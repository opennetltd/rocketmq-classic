use super::*;

impl ConsumerRuntime for PushConsumerRegistration {
    fn reset_offset(
        &self,
        topic: Option<String>,
        offsets: Option<rocketmq_classic_core::runtime::ConsumerOffsetTable>,
    ) -> Option<rocketmq_classic_core::runtime::ConsumerOperationFuture<'_, ()>> {
        let consumer = self.consumer.upgrade();
        Some(Box::pin(async move {
            let consumer = consumer.ok_or_else(|| {
                RuntimeError::ConsumerOperation("push consumer is unavailable".into())
            })?;
            consumer
                .reset_consumer_offsets_shared(topic.as_deref(), offsets)
                .await
                .map_err(|error| match error {
                    PushConsumerError::Runtime(error) => error,
                    error => RuntimeError::ConsumerOperation(error.to_string()),
                })
        }))
    }

    fn consume_message_directly(
        &self,
        message: Option<MessageExt>,
        broker_name: Option<String>,
    ) -> Option<
        rocketmq_classic_core::runtime::ConsumerOperationFuture<
            '_,
            Option<ConsumeMessageDirectlyResult>,
        >,
    > {
        let consumer = self.consumer.upgrade();
        Some(Box::pin(async move {
            let consumer = consumer.ok_or_else(|| {
                RuntimeError::ConsumerOperation("push consumer is unavailable".into())
            })?;
            let message = message.ok_or_else(|| {
                RuntimeError::ConsumerOperation("null direct-consume message".into())
            })?;
            consumer
                .consume_message_directly(message, broker_name)
                .await
                .map(Some)
                .ok_or_else(|| {
                    RuntimeError::ConsumerOperation("consume message service is unavailable".into())
                })
        }))
    }

    fn consumer_running_info(&self) -> Result<ConsumerRunningInfoBody, RuntimeError> {
        let consumer = self.consumer.upgrade().ok_or_else(|| {
            RuntimeError::ConsumerOperation("push consumer is unavailable".into())
        })?;
        let runtime = self.runtime.upgrade().ok_or_else(|| {
            RuntimeError::ConsumerOperation("consumer runtime is unavailable".into())
        })?;
        Ok(consumer.running_info_body(&runtime, None, consumer.get_offset_store()))
    }

    fn consumer_status(
        &self,
        topic: Option<&str>,
    ) -> Result<HashMap<MessageQueue, i64>, RuntimeError> {
        // The raw implementation setter is live after startup. Read its
        // current field rather than the store captured at registration.
        let store = match self.consumer.upgrade() {
            Some(consumer) => consumer.get_offset_store(),
            None => self.offset_store.upgrade(),
        };
        store
            .map(|store| store.clone_offset_table(topic))
            .ok_or(RuntimeError::InvalidOffsetTable(
                "consumer offset store is null",
            ))
    }

    fn inbound_processor(
        &self,
        operation: ConsumerOperation,
    ) -> Option<Arc<dyn rocketmq_classic_remoting::client::RequestProcessor>> {
        match operation {
            ConsumerOperation::ResetOffset => {
                Some(Arc::new(crate::admin::ResetOffsetProcessor::new(
                    self.consumer.clone(),
                    self.runtime.clone(),
                )))
            }
            ConsumerOperation::ConsumerStatus => {
                Some(Arc::new(crate::admin::ConsumerStatusProcessor::for_push(
                    self.consumer.clone(),
                    self.offset_store.clone(),
                )))
            }
            ConsumerOperation::ConsumeDirectly => {
                Some(Arc::new(crate::admin::DirectConsumeProcessor::new(
                    self.consumer.clone(),
                    self.runtime.clone(),
                )))
            }
            ConsumerOperation::RunningInfo => Some(Arc::new(
                crate::admin::ConsumerRunningInfoProcessor::for_push(
                    self.consumer.clone(),
                    self.offset_store.clone(),
                    self.runtime.clone(),
                ),
            )),
        }
    }

    fn pull_message(
        &self,
        request: Arc<dyn rocketmq_classic_core::runtime::MessageRequest>,
    ) -> Option<ConsumerFuture<'_>> {
        let request = request.as_any().downcast_ref::<PullRequest>()?;
        let request = Arc::new(request.clone());
        if let Err(request) = self.pull_rpc_workers.try_submit(request) {
            let _ = self.pull_scheduler.schedule(
                request.clone(),
                PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL,
            );
            self.pull_scheduler.finish(&request);
        }
        Some(Box::pin(async {}))
    }

    fn heartbeat_data(&self) -> ConsumerData {
        let (message_model, consume_from_where, unit_mode) = self
            .consumer
            .upgrade()
            .map(|consumer| {
                (
                    consumer.message_model(),
                    consumer.consume_from_where(),
                    consumer.is_unit_mode(),
                )
            })
            .unwrap_or((self.message_model, self.consume_from_where, self.unit_mode));
        #[allow(clippy::mutable_key_type)]
        let subscription_data_set = read_lock(&self.subscriptions)
            .values()
            .cloned()
            .map(|subscription| Some(SharedValue::from(subscription)))
            .collect::<ConsumerSubscriptionDataSet>();
        ConsumerData {
            group_name: self
                .consumer
                .upgrade()
                .map(|consumer| consumer.group_name())
                .unwrap_or_else(|| Some(self.group.clone())),
            consume_type: Some(ConsumeType::ConsumePassively),
            message_model: Some(message_model),
            consume_from_where: Some(consume_from_where),
            subscription_data_set: Some(SharedValue::from(subscription_data_set)),
            unit_mode,
        }
    }

    fn is_subscribe_topic_need_update(&self, topic: &str) -> bool {
        read_lock(&self.subscriptions).contains_key(topic)
    }

    fn update_topic_subscribe_info(&self, topic: &str, queues: BTreeSet<MessageQueue>) {
        let previous = write_lock(&self.topic_queues).insert(topic.to_owned(), queues.clone());
        if previous.as_ref() != Some(&queues) {
            if let Some(runtime) = self.runtime.upgrade() {
                runtime.rebalance_immediately();
            }
        }
    }

    fn persist_consumer_offset(&self) -> Option<ConsumerFuture<'_>> {
        let store = self.offset_store.upgrade()?;
        let consumer = self.consumer.upgrade()?;
        Some(Box::pin(async move {
            if consumer.service_state() != Some(ConsumerServiceState::Running) {
                return;
            }
            let queues = read_lock(&consumer.process_queues)
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            let _ = store.persist_all(&queues).await;
        }))
    }

    fn try_rebalance(&self) -> Option<Pin<Box<dyn Future<Output = bool> + Send + '_>>> {
        let consumer = self.consumer.clone();
        Some(Box::pin(async move {
            if let Some(consumer) = consumer.upgrade() {
                consumer.rebalance_service.rebalance(&consumer).await
            } else {
                true
            }
        }))
    }
}
