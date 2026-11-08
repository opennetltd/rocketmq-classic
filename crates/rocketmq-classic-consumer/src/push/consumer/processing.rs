use super::*;

impl PushConsumer {
    pub(super) async fn pull_context(&self) -> Option<PullContext> {
        let state = self.state.lock().await;
        match &*state {
            State::Started {
                runtime,
                offset_store,
                pull_scheduler,
                consume_service,
                ..
            } => {
                let runtime = match self
                    .implementation_client_factory
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
                {
                    Some(runtime) => runtime?,
                    None => runtime.upgrade()?,
                };
                Some(PullContext {
                    runtime,
                    offset_store: offset_store.clone(),
                    scheduler: pull_scheduler.clone(),
                    consume_service: consume_service.clone(),
                })
            }
            State::Created | State::Shutdown => None,
        }
    }

    pub(super) async fn current_orderly_lock_settings(&self) -> Option<OrderlyLockSettings> {
        match &*self.state.lock().await {
            State::Started {
                orderly_lock_settings,
                ..
            } => *orderly_lock_settings,
            State::Created | State::Shutdown => None,
        }
    }

    pub(crate) async fn run_pull_worker_request(self: Arc<Self>, request: Arc<PullRequest>) {
        let Some(context) = self.pull_context().await else {
            return;
        };
        let queue = request.queue().clone();
        let generation = request.generation();
        let process_queue = request.process_queue();
        if !request.is_current() {
            context.scheduler.cancel(&queue, generation);
            return;
        }
        let _ = process_queue.set_last_pull_timestamp_for_assignment(generation, epoch_millis());

        if self.paused.load(Ordering::Acquire) {
            let _ = context
                .scheduler
                .schedule(request, PULL_TIME_DELAY_MILLIS_WHEN_SUSPEND);
            return;
        }

        let orderly_lock_settings = self.current_orderly_lock_settings().await;
        if orderly_lock_settings.is_some()
            && self.config.message_model()
                == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
            && !process_queue
                .is_lock_fresh(
                    generation,
                    orderly_lock_settings
                        .expect("orderly lock settings")
                        .max_live_time_millis,
                )
                .unwrap_or(false)
        {
            let locked = self
                .lock_orderly_process_queues(&context.runtime, std::slice::from_ref(&process_queue))
                .await
                .contains(&queue);
            let delay = if locked {
                10
            } else {
                ORDERLY_LOCK_RETRY_DELAY_MILLIS
            };
            let _ = context.scheduler.schedule(request, delay);
            return;
        }

        if orderly_lock_settings.is_some()
            && self.config.message_model()
                == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
            && !request.previously_locked()
        {
            match self
                .compute_initial_offset(&context.runtime, &context.offset_store, &queue)
                .await
            {
                Ok(offset) => {
                    request.set_next_offset(offset);
                    request.set_previously_locked(true);
                }
                Err(error) => {
                    tracing::warn!(
                        consumer_group = %request.consumer_group(),
                        queue_id = queue.queue_id(),
                        generation,
                        error = %error,
                        "push failed to refresh offset on the first orderly pull after lock"
                    );
                    let _ = context
                        .scheduler
                        .schedule(request, self.pull_time_delay_millis_when_exception());
                    return;
                }
            }
        }

        let thresholds = self.active_pull_queue_thresholds();
        if let Ok(snapshot) = process_queue.snapshot(generation) {
            if let Some(reason) = flow_control_reason(&snapshot, thresholds) {
                tracing::trace!(
                    consumer_group = %request.consumer_group(),
                    queue_id = queue.queue_id(),
                    generation,
                    ?reason,
                    cached_count = snapshot.cached_count,
                    cached_bytes = snapshot.cached_bytes,
                    max_span = snapshot.max_span,
                    "push pull paused by queue flow control"
                );
                let _ = context
                    .scheduler
                    .schedule(request, PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL);
                return;
            }
        }

        let Some(topic) = queue.topic() else {
            let _ = context
                .scheduler
                .schedule(request, self.pull_time_delay_millis_when_exception());
            return;
        };
        let subscription = read_lock(&self.subscriptions).get(topic).cloned();
        let Some(subscription) = subscription else {
            tracing::warn!(
                topic,
                queue_id = queue.queue_id(),
                "push pull has no subscription"
            );
            let _ = context
                .scheduler
                .schedule(request, self.pull_time_delay_millis_when_exception());
            return;
        };

        match self
            .fetch_push_result(&context, &request, &subscription)
            .await
        {
            Ok(result) => {
                self.apply_pull_result(request, context, result).await;
            }
            Err(error) => {
                tracing::debug!(
                    consumer_group = %request.consumer_group(),
                    topic,
                    queue_id = queue.queue_id(),
                    generation,
                    delay_millis = error.delay_millis,
                    error = %error.reason,
                    "push pull request will be retried"
                );
                let _ = context.scheduler.schedule(request, error.delay_millis);
            }
        }
    }

    pub(super) async fn fetch_push_result(
        &self,
        context: &PullContext,
        request: &PullRequest,
        subscription: &SubscriptionData,
    ) -> Result<crate::pull::ProcessedPullResult, PullAttemptError> {
        let queue = request.queue();
        let topic = queue
            .topic()
            .filter(|topic| !topic.is_empty())
            .ok_or_else(|| self.pull_retry_error("message queue has no topic"))?;
        let broker_name = queue
            .broker_name()
            .filter(|broker| !broker.is_empty())
            .ok_or_else(|| self.pull_retry_error("message queue has no broker name"))?;
        let deadline =
            Instant::now() + Duration::from_millis(CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND as u64);
        let suggested_broker_id = self.pull_api.recalculate_pull_from_which_node(queue);
        let mut route = context
            .runtime
            .route_snapshot()
            .map_err(|error| self.pull_retry_error(error.to_string()))?;
        let mut broker = route
            .subscribe_address(broker_name, suggested_broker_id, false)
            .map_err(|error| self.pull_retry_error(error.to_string()))?;
        if broker.is_none() {
            context
                .runtime
                .update_topic_route(topic, deadline)
                .await
                .map_err(|error| self.pull_retry_error(error.to_string()))?;
            route = context
                .runtime
                .route_snapshot()
                .map_err(|error| self.pull_retry_error(error.to_string()))?;
            broker = route
                .subscribe_address(broker_name, suggested_broker_id, false)
                .map_err(|error| self.pull_retry_error(error.to_string()))?;
        }
        let broker = broker.ok_or_else(|| {
            self.pull_retry_error(format!("broker {broker_name} was not found in route"))
        })?;

        let class_filter = subscription.is_class_filter_mode();
        let sub_expression = if self.config.post_subscription_when_pull() && !class_filter {
            subscription.get_sub_string().map(str::to_owned)
        } else {
            None
        };
        let commit_offset = if self.config.message_model()
            == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
        {
            match context
                .offset_store
                .read_offset(queue, ReadOffsetType::ReadFromMemory)
                .await
                .map_err(|error| self.pull_retry_error(error.to_string()))?
            {
                OffsetRead::Found(offset) if offset > 0 => offset,
                OffsetRead::Found(_) | OffsetRead::NotFound => 0,
            }
        } else {
            0
        };
        let mut sys_flag = build_pull_sys_flag(
            commit_offset > 0,
            true,
            sub_expression.is_some(),
            class_filter,
        );
        let commit_offset = if broker.slave {
            sys_flag = clear_commit_offset_flag(sys_flag);
            0
        } else {
            commit_offset
        };
        let broker_address = if class_filter {
            let route_data = route
                .raw_routes
                .get(topic)
                .ok_or_else(|| self.pull_retry_error("class-filter route was not found"))?
                .snapshot()
                .map_err(|error| self.pull_retry_error(error.to_string()))?;
            resolve_pull_broker_address(&route_data, topic, &broker.address, sys_flag)
                .map_err(|error| self.pull_retry_error(error.to_string()))?
                .ok_or_else(|| self.pull_retry_error("class-filter broker was not found"))?
        } else {
            broker.address
        };
        let mut client_config = self.config.client_config().clone();
        let header = {
            let mut __phase18_header_1 = PullMessageRequestHeader::default();
            __phase18_header_1.set_consumer_group(Some(request.consumer_group().to_owned()));
            __phase18_header_1.set_topic(Some(topic.to_owned()));
            __phase18_header_1.set_queue_id(Some(queue.queue_id()));
            __phase18_header_1.set_queue_offset(Some(request.next_offset()));
            __phase18_header_1.set_max_msg_nums(Some(self.config.pull_batch_size()));
            __phase18_header_1.set_sys_flag(Some(sys_flag));
            __phase18_header_1.set_commit_offset(Some(commit_offset));
            __phase18_header_1.set_suspend_timeout_millis(Some(BROKER_SUSPEND_MAX_TIME_MILLIS));
            __phase18_header_1.set_subscription(sub_expression);
            __phase18_header_1.set_sub_version(Some(subscription.get_sub_version()));
            __phase18_header_1
                .set_expression_type(subscription.get_expression_type().map(str::to_owned));
            __phase18_header_1.set_max_msg_bytes(Some(self.config.pull_batch_size_in_bytes()));
            __phase18_header_1.set_broker_name(Some(broker_name.to_owned()));
            __phase18_header_1
        };
        let pull_started = Instant::now();
        let pull_result = context
            .runtime
            .api()
            .pull_message(&broker_address, &header, deadline)
            .await
            .map_err(|error| self.pull_retry_error_from_rpc(error))?;
        let mut processed = self
            .pull_api
            .process_pull_result(
                queue,
                pull_result,
                subscription,
                DecodeOptions {
                    read_body: client_config.decode_read_body(),
                    decompress_body: client_config.decode_decompress_body(),
                    ..DecodeOptions::default()
                },
            )
            .map_err(|error| self.pull_retry_error(error.to_string()))?;
        if let Some(messages) = processed.messages.as_mut() {
            for message in messages {
                let message_topic = message.topic().to_owned();
                let topic = client_config
                    .without_namespace(Some(&message_topic))
                    .map_err(|error| self.pull_retry_error(error.to_string()))?;
                if let Some(topic) = topic {
                    message.set_topic(topic);
                }
            }
        }
        if processed.result.status == PullStatus::Found {
            let message_count = processed.messages.as_ref().map_or(0, Vec::len);
            self.record_pull_result_stats(request, topic, pull_started.elapsed(), message_count);
        }
        Ok(processed)
    }

    pub(super) fn record_pull_result_stats(
        &self,
        request: &PullRequest,
        topic: &str,
        elapsed: Duration,
        message_count: usize,
    ) {
        let Some(stats) = self.stats_manager() else {
            return;
        };
        stats.inc_pull_rt(request.consumer_group(), topic, elapsed_millis(elapsed));
        if message_count > 0 {
            stats.inc_pull_tps(request.consumer_group(), topic, metric_count(message_count));
        }
    }

    pub(super) async fn apply_pull_result(
        self: &Arc<Self>,
        request: Arc<PullRequest>,
        context: PullContext,
        processed: crate::pull::ProcessedPullResult,
    ) {
        let queue = request.queue().clone();
        let generation = request.generation();
        let process_queue = request.process_queue();
        if !request.is_current() {
            context.scheduler.cancel(&queue, generation);
            return;
        }
        let next_offset = processed.result.next_begin_offset;
        request.set_next_offset(next_offset);
        match processed.result.status {
            PullStatus::Found => {
                let Some(messages) = processed.messages else {
                    let _ = context
                        .scheduler
                        .schedule(request, self.pull_time_delay_millis_when_exception());
                    return;
                };
                if messages.is_empty() {
                    let _ = context.scheduler.schedule(request, 0);
                    return;
                }
                if !request.is_current() {
                    return;
                }
                let outcome = match process_queue.insert_messages(generation, messages) {
                    Ok(outcome) => outcome,
                    Err(_) => return,
                };
                if outcome.dispatch_to_consume {
                    tracing::trace!(
                        topic = ?queue.topic(),
                        queue_id = queue.queue_id(),
                        generation,
                        inserted_count = outcome.inserted_count,
                        "push process queue has ready messages"
                    );
                    self.submit_ready_messages(&context, process_queue, generation);
                }
                let delay = self.config.pull_interval_millis();
                let _ = context.scheduler.schedule(request, delay);
            }
            PullStatus::NoNewMessage | PullStatus::NoMatchedMessage => {
                if let Ok(snapshot) = process_queue.snapshot(generation) {
                    if snapshot.cached_count == 0 {
                        context
                            .offset_store
                            .update_offset(&queue, next_offset, true);
                    }
                }
                let _ = context.scheduler.schedule(request, 0);
            }
            PullStatus::OffsetIllegal => {
                if process_queue.drop_assignment(generation) == Ok(true) {
                    write_lock(&self.offset_corrections_pending).insert(queue.clone());
                    context.scheduler.cancel(&queue, generation);
                    self.schedule_offset_recovery(
                        context.runtime,
                        context.offset_store,
                        queue,
                        generation,
                        next_offset,
                    );
                }
            }
        }
    }

    pub(super) fn submit_consume_batch(
        self: &Arc<Self>,
        context: &PullContext,
        process_queue: Arc<ProcessQueue>,
        generation: u64,
    ) {
        if let Some(service) = self.custom_consume_message_service() {
            let batch = match process_queue.take_batch(
                generation,
                self.config.consume_message_batch_max_size().max(1) as usize,
            ) {
                Ok(Some(batch)) => batch,
                Ok(None) | Err(_) => return,
            };
            service.submit_consume_request(
                batch.messages().to_vec(),
                process_queue,
                batch.queue().clone(),
                true,
            );
            return;
        }
        if self.consume_message_service_is_null() {
            return;
        }
        let ConsumeService::Concurrent(consume_service) = &context.consume_service else {
            return;
        };
        let batch = match process_queue.take_batch(
            generation,
            self.config.consume_message_batch_max_size().max(1) as usize,
        ) {
            Ok(Some(batch)) => batch,
            Ok(None) | Err(_) => return,
        };
        let request = ConsumeRequest {
            process_queue,
            batch,
        };
        match consume_service.try_submit(request) {
            Ok(()) => {}
            Err(ConsumeSubmitError::Full(request)) => {
                self.requeue_unsubmitted_batch(&request);
                self.schedule_consume_retry(
                    context.runtime.clone(),
                    request.process_queue.clone(),
                    request.batch.generation(),
                    PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL,
                );
            }
            Err(ConsumeSubmitError::Closed(request)) => {
                self.requeue_unsubmitted_batch(&request);
            }
        }
    }

    pub(crate) async fn clean_expired_process_queue_messages_at(
        &self,
        now_millis: i64,
    ) -> Result<(), ExpiredMessageCleanupError> {
        if self.is_consume_orderly() {
            return Ok(());
        }
        let timeout_millis = self
            .config
            .consume_timeout_minutes()
            .wrapping_mul(60)
            .wrapping_mul(1_000);
        let process_queues = read_lock(&self.process_queues)
            .values()
            .cloned()
            .collect::<Vec<_>>();

        for process_queue in process_queues {
            let generation = process_queue.generation();
            let attempts = process_queue
                .snapshot(generation)
                .map_or(0, |snapshot| snapshot.cached_count.clamp(0, 16) as usize);
            for _ in 0..attempts {
                let (offset, message) = match process_queue.expired_head_message(
                    generation,
                    now_millis,
                    timeout_millis,
                ) {
                    Ok(Some(expired)) => expired,
                    Ok(None) | Err(ProcessQueueError::StaleGeneration) => break,
                    Err(ProcessQueueError::InvalidConsumeStartTimestamp { offset, value }) => {
                        return Err(ExpiredMessageCleanupError::InvalidConsumeStartTimestamp {
                            offset,
                            value,
                        });
                    }
                    Err(_) => break,
                };
                match self.send_message_back(&message, 3).await {
                    Ok(()) => {
                        let _ = process_queue.remove_head_message(generation, offset, now_millis);
                    }
                    Err(error) => {
                        tracing::warn!(
                            queue = %process_queue.queue(),
                            offset,
                            error = %error,
                            "failed to send expired concurrent-consume message back"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn submit_ready_messages(
        self: &Arc<Self>,
        context: &PullContext,
        process_queue: Arc<ProcessQueue>,
        generation: u64,
    ) {
        if matches!(&context.consume_service, ConsumeService::Orderly(_)) {
            if let Some(service) = self.custom_consume_message_service() {
                let messages = process_queue
                    .take_messages(i32::MAX)
                    .expect("the maximum batch size is nonnegative");
                if !messages.is_empty() {
                    service.submit_consume_request(
                        messages,
                        process_queue.clone(),
                        process_queue.queue().clone(),
                        true,
                    );
                }
                return;
            }
            if self.consume_message_service_is_null() {
                return;
            }
        }
        match &context.consume_service {
            ConsumeService::Concurrent(_) => {
                self.submit_consume_batch(context, process_queue, generation);
            }
            ConsumeService::Orderly(service) => {
                let request = OrderlyConsumeRequest {
                    process_queue,
                    generation,
                };
                match service.try_submit(request) {
                    Ok(()) => {}
                    Err(OrderlySubmitError::Full(request)) => {
                        self.schedule_orderly_work_retry(
                            context.runtime.clone(),
                            request.process_queue,
                            request.generation,
                            100,
                        );
                    }
                    Err(OrderlySubmitError::Closed(request)) => {
                        let _ = request
                            .process_queue
                            .release_consume_dispatch(request.generation);
                    }
                }
            }
        }
    }

    fn schedule_orderly_work_retry(
        self: &Arc<Self>,
        runtime: Arc<ClientRuntime>,
        process_queue: Arc<ProcessQueue>,
        generation: u64,
        delay_millis: i64,
    ) {
        let consumer = Arc::downgrade(self);
        let task_process_queue = process_queue.clone();
        let task = async move {
            let Some(consumer) = consumer.upgrade() else {
                return;
            };
            let Some(context) = consumer.pull_context().await else {
                return;
            };
            consumer.submit_ready_messages(&context, process_queue, generation);
        };
        if let Err(error) = runtime.execute_task_later(task, delay_millis) {
            let _ = task_process_queue.release_consume_dispatch(generation);
            tracing::error!(generation, error = %error, "failed to schedule orderly consume retry");
        }
    }

    fn requeue_unsubmitted_batch(&self, request: &ConsumeRequest) {
        let _ = request.process_queue.complete_batch(
            request.batch.generation(),
            request.batch.id(),
            &[],
        );
    }

    fn schedule_consume_retry(
        self: &Arc<Self>,
        runtime: Arc<ClientRuntime>,
        process_queue: Arc<ProcessQueue>,
        generation: u64,
        delay_millis: i64,
    ) {
        let consumer = Arc::downgrade(self);
        let task = async move {
            let Some(consumer) = consumer.upgrade() else {
                return;
            };
            let Some(context) = consumer.pull_context().await else {
                return;
            };
            consumer.submit_consume_batch(&context, process_queue, generation);
        };
        if let Err(error) = runtime.execute_task_later(task, delay_millis) {
            tracing::debug!(
                generation,
                error = %error,
                "failed to schedule push consume submission retry"
            );
        }
    }

    pub(crate) async fn run_consume_request(
        self: Arc<Self>,
        request: ConsumeRequest,
        listener: Arc<dyn MessageListenerConcurrently>,
    ) {
        let Some(context) = self.pull_context().await else {
            self.requeue_unsubmitted_batch(&request);
            return;
        };
        let generation = request.batch.generation();
        if !request
            .process_queue
            .snapshot(generation)
            .is_ok_and(|snapshot| !snapshot.dropped)
        {
            return;
        }

        let mut messages = request
            .batch
            .messages()
            .iter()
            .map(|message| message.as_ref().clone())
            .collect::<Vec<_>>();
        self.try_reset_pop_retry_topic(&mut messages, &self.consume_stats_group());
        self.reset_retry_and_namespace(
            &mut messages,
            &self.config.consumer_group().unwrap_or_default(),
        );
        let mut consume_context =
            self.new_consume_message_context(messages, request.batch.queue().clone());
        self.consume_message_hooks
            .execute_before(&mut consume_context);
        let consume_start_timestamp_millis = epoch_millis();
        let _ = request.process_queue.mark_batch_started(
            generation,
            request.batch.id(),
            consume_start_timestamp_millis,
        );
        if let Some(messages) = consume_context.messages.as_mut() {
            for message in messages {
                let _ = message.set_system_property(
                    PROPERTY_CONSUME_START_TIMESTAMP,
                    consume_start_timestamp_millis.to_string(),
                );
            }
        }
        let callback_context = ConsumeConcurrentlyContext::new(request.batch.queue().clone());
        let fallback_context = callback_context.clone();
        let listener_messages = consume_context.messages.clone().unwrap_or_default();
        let callback_started = Instant::now();
        let mut callback = AbortOnDrop(tokio::spawn(async move {
            listener
                .consume_message(listener_messages, callback_context)
                .await
        }));
        // Java classifies consume timeout after the listener returns. The
        // timeout is an observation for hooks and statistics, not a reason to
        // cancel the callback.
        let callback_result = (&mut callback.0).await;
        let callback_timed_out =
            consume_timeout_exceeded(callback_started, self.config.consume_timeout_minutes());
        let (status, returned_context, consume_context_type) = match callback_result {
            Ok((status, returned_context)) => {
                let context_type = if status.is_none() {
                    "RETURNNULL"
                } else if callback_timed_out {
                    "TIME_OUT"
                } else {
                    match status {
                        Some(ConsumeConcurrentlyStatus::ConsumeSuccess) => "SUCCESS",
                        Some(ConsumeConcurrentlyStatus::ReconsumeLater) => "FAILED",
                        None => unreachable!("null status was handled above"),
                    }
                };
                (status, returned_context, context_type)
            }
            Err(error) => {
                tracing::error!(
                    topic = ?request.batch.queue().topic(),
                    queue_id = request.batch.queue().queue_id(),
                    generation,
                    error = %error,
                    "push concurrent listener failed"
                );
                (None, fallback_context, "EXCEPTION")
            }
        };
        consume_context
            .properties
            .get_or_insert_with(HashMap::new)
            .insert(
                "CONSUME_CONTEXT_TYPE".to_owned(),
                consume_context_type.to_owned(),
            );
        consume_context.status = Some(
            match status {
                Some(ConsumeConcurrentlyStatus::ConsumeSuccess) => "CONSUME_SUCCESS",
                Some(ConsumeConcurrentlyStatus::ReconsumeLater) | None => "RECONSUME_LATER",
            }
            .to_owned(),
        );
        consume_context.success = status == Some(ConsumeConcurrentlyStatus::ConsumeSuccess);
        consume_context.access_channel = self.config.client_config().access_channel();
        self.consume_message_hooks
            .execute_after(&mut consume_context);
        if let (Some(topic), Some(stats)) = (request.batch.queue().topic(), self.stats_manager()) {
            stats.inc_consume_rt(
                &self.consume_stats_group(),
                topic,
                elapsed_millis(callback_started.elapsed()),
            );
        }
        self.complete_consume_request(request, context, status, returned_context)
            .await;
    }

    pub(crate) async fn run_orderly_consume_request(
        self: Arc<Self>,
        request: OrderlyConsumeRequest,
        listener: Arc<dyn MessageListenerOrderly>,
    ) {
        let process_queue = request.process_queue;
        let generation = request.generation;
        let Some(context) = self.pull_context().await else {
            let _ = process_queue.release_consume_dispatch(generation);
            return;
        };
        let gate = process_queue.orderly_consume_gate();
        let _consume_guard = gate.write_owned().await;
        if !process_queue
            .snapshot(generation)
            .is_ok_and(|snapshot| !snapshot.dropped)
        {
            return;
        }

        let queue = process_queue.queue().clone();
        let orderly_settings = self.current_orderly_lock_settings().await;
        let orderly_clustering = orderly_settings.is_some()
            && self.config.message_model()
                == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering;
        let started_at = Instant::now();
        loop {
            if !process_queue
                .snapshot(generation)
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                return;
            }
            if orderly_settings
                .is_some_and(|settings| settings.max_continuous_consume_exceeded(started_at))
            {
                self.schedule_orderly_work_retry(
                    context.runtime.clone(),
                    process_queue.clone(),
                    generation,
                    10,
                );
                return;
            }

            if orderly_clustering {
                let Some(settings) = orderly_settings else {
                    return;
                };
                let lock_fresh = process_queue
                    .is_lock_fresh(generation, settings.max_live_time_millis)
                    .unwrap_or(false);
                if !lock_fresh {
                    self.schedule_orderly_lock_retry(
                        context.runtime.clone(),
                        process_queue.clone(),
                        generation,
                        10,
                    );
                    return;
                }
            }

            let batch = match process_queue.take_batch(
                generation,
                self.config.consume_message_batch_max_size().max(1) as usize,
            ) {
                Ok(Some(batch)) => batch,
                Ok(None) | Err(_) => return,
            };
            let mut messages = batch
                .messages()
                .iter()
                .map(|message| message.as_ref().clone())
                .collect::<Vec<_>>();
            self.reset_retry_and_namespace_for_group(
                &mut messages,
                &self.config.consumer_group().unwrap_or_default(),
            );
            let mut consume_context =
                self.new_consume_message_context(messages.clone(), queue.clone());
            self.consume_message_hooks
                .execute_before(&mut consume_context);
            messages = consume_context.messages.clone().unwrap_or_default();
            let listener_messages = messages.clone();
            let callback_context = ConsumeOrderlyContext::new(queue.clone());
            let fallback_context = callback_context.clone();
            let listener = listener.clone();
            let callback_started = Instant::now();
            let mut callback = AbortOnDrop(tokio::spawn(async move {
                listener
                    .consume_message(listener_messages, callback_context)
                    .await
            }));
            // Java's consumeTimeout only classifies a completed callback for
            // statistics; it does not interrupt the listener. Keep the queue
            // gate held until the callback actually returns.
            let (status, returned_context, consume_context_type) = match (&mut callback.0).await {
                Ok((status, returned_context)) => {
                    let context_type = match status {
                        Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment) => "FAILED",
                        None => "RETURNNULL",
                        Some(_) => "SUCCESS",
                    };
                    (status, returned_context, context_type)
                }
                Err(error) => {
                    tracing::error!(
                        topic = ?queue.topic(),
                        queue_id = queue.queue_id(),
                        generation,
                        error = %error,
                        "push orderly listener failed"
                    );
                    (None, fallback_context, "EXCEPTION")
                }
            };

            let consume_timed_out =
                consume_timeout_exceeded(callback_started, self.config.consume_timeout_minutes());
            let consume_context_type =
                orderly_consume_return_type(consume_context_type, status, consume_timed_out);
            consume_context
                .properties
                .get_or_insert_with(HashMap::new)
                .insert(
                    "CONSUME_CONTEXT_TYPE".to_owned(),
                    consume_context_type.to_owned(),
                );
            consume_context.status = Some(
                match status {
                    Some(ConsumeOrderlyStatus::Success) => "SUCCESS",
                    Some(ConsumeOrderlyStatus::Rollback) => "ROLLBACK",
                    Some(ConsumeOrderlyStatus::Commit) => "COMMIT",
                    Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment) | None => {
                        "SUSPEND_CURRENT_QUEUE_A_MOMENT"
                    }
                }
                .to_owned(),
            );
            consume_context.success = matches!(
                status,
                Some(ConsumeOrderlyStatus::Success | ConsumeOrderlyStatus::Commit)
            );
            consume_context.access_channel = self.config.client_config().access_channel();
            self.consume_message_hooks
                .execute_after(&mut consume_context);
            if let (Some(topic), Some(stats)) = (queue.topic(), self.stats_manager()) {
                stats.inc_consume_rt(
                    &self.consume_stats_group(),
                    topic,
                    elapsed_millis(callback_started.elapsed()),
                );
            }

            if !process_queue
                .snapshot(generation)
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                return;
            }
            if orderly_clustering {
                let Some(settings) = orderly_settings else {
                    return;
                };
                if !process_queue
                    .is_lock_fresh(generation, settings.max_live_time_millis)
                    .unwrap_or(false)
                {
                    let completion = process_queue.complete_orderly_batch(
                        generation,
                        batch.id(),
                        OrderlyCompletion::RollbackAll,
                    );
                    if completion.is_ok() {
                        self.schedule_orderly_lock_retry(
                            context.runtime.clone(),
                            process_queue.clone(),
                            generation,
                            10,
                        );
                    }
                    return;
                }
            }

            let status = status.unwrap_or(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
            if let Some(topic) = queue.topic() {
                let message_count = batch.offsets().len();
                let (acknowledged, failed) = if returned_context.auto_commit() {
                    match status {
                        ConsumeOrderlyStatus::Success
                        | ConsumeOrderlyStatus::Commit
                        | ConsumeOrderlyStatus::Rollback => (Some(message_count), None),
                        ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => {
                            (None, Some(message_count))
                        }
                    }
                } else {
                    match status {
                        ConsumeOrderlyStatus::Success => (Some(message_count), None),
                        ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => {
                            (None, Some(message_count))
                        }
                        ConsumeOrderlyStatus::Commit | ConsumeOrderlyStatus::Rollback => {
                            (None, None)
                        }
                    }
                };
                if let Some(stats) = self.stats_manager() {
                    let consumer_group = self.consume_stats_group();
                    if let Some(acknowledged) = acknowledged {
                        stats.inc_consume_ok_tps(
                            &consumer_group,
                            topic,
                            metric_count(acknowledged),
                        );
                    }
                    if let Some(failed) = failed {
                        stats.inc_consume_failed_tps(&consumer_group, topic, metric_count(failed));
                    }
                }
            }
            let retry_required = if status == ConsumeOrderlyStatus::SuspendCurrentQueueAMoment {
                self.check_orderly_reconsume_times(
                    &context,
                    &queue,
                    &process_queue,
                    &batch,
                    &messages,
                    generation,
                )
                .await
            } else {
                false
            };
            let disposition =
                orderly_disposition(returned_context.auto_commit(), Some(status), retry_required);
            let completion = match disposition {
                OrderlyDisposition::Commit => OrderlyCompletion::CommitAll,
                OrderlyDisposition::Hold => OrderlyCompletion::HoldCurrent,
                OrderlyDisposition::Rollback => OrderlyCompletion::RollbackAll,
                OrderlyDisposition::Retry => OrderlyCompletion::RequeueCurrent,
            };
            let outcome =
                match process_queue.complete_orderly_batch(generation, batch.id(), completion) {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        tracing::debug!(
                            topic = ?queue.topic(),
                            queue_id = queue.queue_id(),
                            generation,
                            error = %error,
                            "orderly consume result no longer owns its queue batch"
                        );
                        return;
                    }
                };
            if disposition == OrderlyDisposition::Commit {
                if let Some(offset) = outcome.next_commit_offset {
                    if process_queue
                        .snapshot(generation)
                        .is_ok_and(|snapshot| !snapshot.dropped)
                    {
                        context.offset_store.update_offset(&queue, offset, false);
                    }
                }
            }

            match disposition {
                OrderlyDisposition::Rollback => {
                    self.schedule_orderly_work_retry(
                        context.runtime.clone(),
                        process_queue.clone(),
                        generation,
                        suspend_delay_millis(
                            returned_context.suspend_current_queue_time_millis(),
                            self.config.suspend_current_queue_time_millis(),
                        ) as i64,
                    );
                    return;
                }
                OrderlyDisposition::Retry => {
                    self.schedule_orderly_work_retry(
                        context.runtime.clone(),
                        process_queue.clone(),
                        generation,
                        suspend_delay_millis(
                            returned_context.suspend_current_queue_time_millis(),
                            self.config.suspend_current_queue_time_millis(),
                        ) as i64,
                    );
                    return;
                }
                OrderlyDisposition::Commit | OrderlyDisposition::Hold => {
                    if !outcome.has_ready_messages {
                        return;
                    }
                }
            }
        }
    }

    pub(super) fn reset_retry_and_namespace_for_group(
        &self,
        messages: &mut [MessageExt],
        consumer_group: &str,
    ) {
        let mut config = self.config.client_config().clone();
        let retry_topic = format!("%RETRY%{consumer_group}");
        for message in messages {
            // Java reads RETRY_TOPIC even for ordinary/null topics, which
            // initializes a previously-null properties map.
            let origin_topic = message.get_property(PROPERTY_RETRY_TOPIC);
            if message.topic_nullable() == Some(retry_topic.as_str()) {
                if let Some(origin_topic) = origin_topic {
                    message.set_topic(origin_topic);
                }
            }
            if let Ok(topic) = config.without_namespace(message.topic_nullable()) {
                message.set_topic_nullable(topic);
            }
        }
    }

    async fn check_orderly_reconsume_times(
        &self,
        context: &PullContext,
        queue: &MessageQueue,
        process_queue: &Arc<ProcessQueue>,
        batch: &ConsumeBatch,
        messages: &[MessageExt],
        generation: u64,
    ) -> bool {
        let maximum = self.config.effective_orderly_max_reconsume_times();
        let mut retry_required = false;
        for (offset, message) in batch.offsets().iter().copied().zip(messages) {
            if message.reconsume_times() >= maximum {
                if !self
                    .send_message_back_for_consume(
                        &context.runtime,
                        queue,
                        message,
                        0,
                        SendBackOptions {
                            broker_name_override: None,
                            address_broker_name_override: None,
                            consumer_group_override: None,
                            max_reconsume_times_override: None,
                            timeout_millis: CONSUMER_SEND_BACK_TIMEOUT_MILLIS,
                            orderly: true,
                        },
                    )
                    .await
                {
                    if process_queue
                        .increment_reconsume_times(generation, offset)
                        .is_err()
                    {
                        return true;
                    }
                    retry_required = true;
                }
            } else {
                if process_queue
                    .increment_reconsume_times(generation, offset)
                    .is_err()
                {
                    return true;
                }
                retry_required = true;
            }
        }
        retry_required
    }

    fn schedule_orderly_lock_retry(
        self: &Arc<Self>,
        runtime: Arc<ClientRuntime>,
        process_queue: Arc<ProcessQueue>,
        generation: u64,
        delay_millis: i64,
    ) {
        let consumer = Arc::downgrade(self);
        let task_runtime = runtime.clone();
        let task_queue = process_queue.clone();
        let task = async move {
            let Some(consumer) = consumer.upgrade() else {
                return;
            };
            let Some(context) = consumer.pull_context().await else {
                return;
            };
            if !process_queue
                .snapshot(generation)
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                return;
            }
            let locked = consumer
                .lock_orderly_process_queues(&context.runtime, std::slice::from_ref(&process_queue))
                .await
                .contains(process_queue.queue());
            consumer.schedule_orderly_work_retry(
                context.runtime,
                process_queue,
                generation,
                if locked {
                    10
                } else {
                    ORDERLY_LOCK_RETRY_DELAY_MILLIS
                },
            );
        };
        if let Err(error) = task_runtime.execute_task_later(task, delay_millis) {
            let _ = task_queue.release_consume_dispatch(generation);
            tracing::error!(generation, error = %error, "failed to schedule orderly lock retry");
        }
    }

    pub(super) async fn complete_consume_request(
        self: &Arc<Self>,
        request: ConsumeRequest,
        context: PullContext,
        status: Option<ConsumeConcurrentlyStatus>,
        callback_context: ConsumeConcurrentlyContext,
    ) {
        let process_queue = request.process_queue;
        let batch = request.batch;
        let generation = batch.generation();
        let queue = batch.queue().clone();
        if !process_queue
            .snapshot(generation)
            .is_ok_and(|snapshot| !snapshot.dropped)
        {
            return;
        }

        let batch_len = batch.offsets().len();
        if batch_len == 0 {
            let _ = process_queue.complete_batch(generation, batch.id(), &[]);
            return;
        }
        let ack_index = match status {
            Some(ConsumeConcurrentlyStatus::ConsumeSuccess) => callback_context
                .ack_index()
                .clamp(-1, batch_len.saturating_sub(1) as i32),
            Some(ConsumeConcurrentlyStatus::ReconsumeLater) | None => -1,
        };
        let acknowledged = (ack_index + 1).max(0) as usize;
        if let Some(topic) = queue.topic() {
            let successful_result = status == Some(ConsumeConcurrentlyStatus::ConsumeSuccess);
            if let Some(stats) = self.stats_manager() {
                let consumer_group = self.consume_stats_group();
                if successful_result {
                    stats.inc_consume_ok_tps(&consumer_group, topic, metric_count(acknowledged));
                }
                stats.inc_consume_failed_tps(
                    &consumer_group,
                    topic,
                    metric_count(batch_len.saturating_sub(acknowledged)),
                );
            }
        }
        let mut terminal_offsets = batch.offsets()[..acknowledged].to_vec();
        let mut local_retry_needed = false;
        if self.config.message_model()
            == rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting
        {
            terminal_offsets.extend_from_slice(&batch.offsets()[acknowledged..]);
        } else {
            for (offset, message) in batch.offsets()[acknowledged..]
                .iter()
                .copied()
                .zip(&batch.messages()[acknowledged..])
            {
                if !process_queue
                    .snapshot(generation)
                    .is_ok_and(|snapshot| !snapshot.dropped)
                {
                    return;
                }
                if self
                    .send_message_back_for_consume(
                        &context.runtime,
                        &queue,
                        message.as_ref(),
                        callback_context.delay_level_when_next_consume(),
                        SendBackOptions {
                            broker_name_override: None,
                            address_broker_name_override: None,
                            consumer_group_override: None,
                            max_reconsume_times_override: None,
                            timeout_millis: CONSUMER_SEND_BACK_TIMEOUT_MILLIS,
                            orderly: false,
                        },
                    )
                    .await
                {
                    terminal_offsets.push(offset);
                } else if process_queue
                    .increment_reconsume_times(generation, offset)
                    .is_err()
                {
                    return;
                } else {
                    local_retry_needed = true;
                }
            }
        }

        let completion =
            match process_queue.complete_batch(generation, batch.id(), &terminal_offsets) {
                Ok(completion) => completion,
                Err(_) => return,
            };
        if let Some(offset) = completion.next_commit_offset {
            if process_queue
                .snapshot(generation)
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                context.offset_store.update_offset(&queue, offset, true);
            }
        }
        if completion.has_ready_messages {
            if local_retry_needed {
                self.schedule_consume_retry(
                    context.runtime,
                    process_queue,
                    generation,
                    CONSUME_RETRY_DELAY_MILLIS,
                );
            } else {
                self.submit_ready_messages(&context, process_queue, generation);
            }
        }
    }

    pub(crate) async fn send_message_back_for_consume(
        &self,
        runtime: &Arc<ClientRuntime>,
        queue: &MessageQueue,
        message: &rocketmq_classic_common::message::MessageExt,
        delay_level: i32,
        options: SendBackOptions<'_>,
    ) -> bool {
        let mut client_config = self.config.client_config().clone();
        let consumer_group = options
            .consumer_group_override
            .filter(|group| !group.trim().is_empty())
            .map(str::to_owned)
            .or_else(|| self.config.consumer_group());
        let origin_topic = client_config
            .with_namespace(Some(message.topic()))
            .ok()
            .flatten();
        let Some(consumer_group) = consumer_group else {
            tracing::error!("push send-back has no consumer group");
            return false;
        };
        let Some(origin_topic) = origin_topic else {
            tracing::error!("push send-back has no origin topic");
            return false;
        };
        let Some(retry_group) = client_config
            .with_namespace(self.config.consumer_group().as_deref())
            .ok()
            .flatten()
        else {
            tracing::error!("push send-back has no retry consumer group");
            return false;
        };
        let max_reconsume_times = options.max_reconsume_times_override.unwrap_or_else(|| {
            if options.orderly {
                self.config.effective_orderly_max_reconsume_times()
            } else {
                self.config.effective_max_reconsume_times()
            }
        });
        if options.orderly {
            let sequence = self.next_retry_message_id.fetch_add(1, Ordering::Relaxed);
            let retry_topic = format!("%RETRY%{retry_group}");
            let retry_message = self.build_retry_message(
                runtime,
                message,
                retry_topic,
                message.topic().to_owned(),
                sequence,
                max_reconsume_times,
            );
            return match self.send_retry_message(runtime, &retry_message).await {
                Ok(()) => true,
                Err(error) => {
                    tracing::error!(
                        topic = ?queue.topic(),
                        queue_id = queue.queue_id(),
                        offset = message.queue_offset(),
                        error = %error,
                        "orderly retry-topic send failed"
                    );
                    false
                }
            };
        }
        let broker_name = match options.broker_name_override {
            Some(broker_name) => broker_name,
            None => message.broker_name().or(queue.broker_name()),
        }
        .map(str::to_owned);
        let header = {
            let mut __phase18_header_2 = ConsumerSendMsgBackRequestHeader::default();
            __phase18_header_2.set_group(consumer_group.clone());
            __phase18_header_2.set_broker_name(broker_name.clone());
            __phase18_header_2.set_origin_topic(origin_topic.clone());
            __phase18_header_2.set_offset(message.commit_log_offset());
            __phase18_header_2.set_delay_level(delay_level);
            __phase18_header_2.set_origin_msg_id(message.message_id().map(str::to_owned));
            __phase18_header_2.set_max_reconsume_times(Some(max_reconsume_times));
            __phase18_header_2.set_unit_mode(self.config.unit_mode());
            __phase18_header_2
        };
        let store_host = message
            .store_host()
            .and_then(MessageHostAddress::socket_address_string);

        match self
            .try_send_message_back_to_broker(
                runtime,
                &header,
                broker_name.as_deref(),
                options.address_broker_name_override,
                store_host.as_deref(),
                options.timeout_millis,
            )
            .await
        {
            Ok(()) => return true,
            Err(error) => tracing::debug!(
                topic = ?queue.topic(),
                queue_id = queue.queue_id(),
                offset = message.queue_offset(),
                error = %error,
                "push send-back Broker attempt failed; trying retry-topic publication"
            ),
        }

        let sequence = self.next_retry_message_id.fetch_add(1, Ordering::Relaxed);
        let retry_topic = format!("%RETRY%{retry_group}");
        let retry_message = self.build_retry_message(
            runtime,
            message,
            retry_topic,
            origin_topic,
            sequence,
            max_reconsume_times,
        );
        match self.send_retry_message(runtime, &retry_message).await {
            Ok(()) => true,
            Err(error) => {
                tracing::error!(
                    topic = ?queue.topic(),
                    queue_id = queue.queue_id(),
                    offset = message.queue_offset(),
                    error = %error,
                    "push send-back and retry-topic fallback both failed"
                );
                false
            }
        }
    }

    async fn try_send_message_back_to_broker(
        &self,
        runtime: &Arc<ClientRuntime>,
        header: &ConsumerSendMsgBackRequestHeader,
        broker_name: Option<&str>,
        address_broker_name_override: Option<Option<&str>>,
        store_host: Option<&str>,
        timeout_millis: u64,
    ) -> Result<(), String> {
        let address_broker_name = address_broker_name_override.unwrap_or(broker_name);
        #[cfg(test)]
        {
            let mut scripted = self
                .scripted_retry_transport
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            scripted.broker_headers.push(header.clone());
            scripted.broker_targets.push((
                address_broker_name.map(str::to_owned),
                store_host.map(str::to_owned),
            ));
            scripted.broker_timeouts_millis.push(timeout_millis);
            if let Some(result) = scripted.broker_results.pop_front() {
                return result;
            }
        }

        let address = if broker_name.is_some() {
            let broker_name = address_broker_name
                .ok_or_else(|| "logical broker has no route endpoint".to_owned())?;
            let route = runtime
                .route_snapshot()
                .map_err(|error| error.to_string())?;
            route
                .publish_address(broker_name)
                .ok_or_else(|| format!("broker {broker_name} has no master address"))?
                .to_owned()
        } else {
            store_host
                .filter(|address| !address.trim().is_empty())
                .map(str::to_owned)
                .ok_or_else(|| "message has no broker name or store host".to_owned())?
        };
        let address =
            broker_vip_channel(self.config.client_config().vip_channel_enabled(), &address)
                .map_err(|error| error.to_string())?;
        let deadline = Instant::now() + Duration::from_millis(timeout_millis);
        runtime
            .api()
            .consumer_send_message_back(&address, header, deadline)
            .await
            .map_err(|error| error.to_string())
    }

    fn build_retry_message(
        &self,
        runtime: &ClientRuntime,
        original: &rocketmq_classic_common::message::MessageExt,
        retry_topic: String,
        origin_topic: String,
        sequence: u64,
        max_reconsume_times: i32,
    ) -> Message {
        let reconsume_times = original.reconsume_times().saturating_add(1);
        let mut retry = Message::new(retry_topic, original.body().to_vec());
        retry.set_flag(original.flag());
        for (key, value) in original.nullable_property_entries() {
            if key.as_deref() != Some(PROPERTY_TRANSACTION_PREPARED) {
                let _ = retry.set_property_nullable(key, value);
            }
        }
        let origin_message_id = original
            .get_property(PROPERTY_ORIGIN_MESSAGE_ID)
            .filter(|value| !value.trim().is_empty())
            .or_else(|| original.message_id().map(str::to_owned));
        let _ = retry.set_system_property(PROPERTY_RETRY_TOPIC, origin_topic);
        if let Some(origin_message_id) = origin_message_id {
            let _ = retry.set_system_property(PROPERTY_ORIGIN_MESSAGE_ID, origin_message_id);
        }
        let _ = retry.set_system_property(PROPERTY_RECONSUME_TIME, reconsume_times.to_string());
        let _ = retry.set_system_property(
            PROPERTY_MAX_RECONSUME_TIMES,
            max_reconsume_times.to_string(),
        );
        let _ = retry.set_system_property(
            PROPERTY_DELAY_TIME_LEVEL,
            3i32.saturating_add(original.reconsume_times()).to_string(),
        );
        if retry
            .properties()
            .get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .is_none_or(|value| value.trim().is_empty())
        {
            let unique_id = format!("{}-{}-{}", runtime.client_id(), epoch_millis(), sequence);
            let _ = retry.set_system_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, unique_id);
        }
        retry
    }

    async fn send_retry_message(
        &self,
        runtime: &Arc<ClientRuntime>,
        retry: &Message,
    ) -> Result<(), String> {
        #[cfg(test)]
        {
            let (_, _, _, encoded_properties) = retry_wire_properties(retry);
            let mut scripted = self
                .scripted_retry_transport
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            scripted.retry_messages.push(retry.clone());
            scripted
                .retry_wire_properties
                .push(encoded_properties.clone());
            if let Some(results) = scripted.retry_attempt_scripts.pop_front() {
                let mut attempts = 0;
                let mut last_error = None;
                for result in results.into_iter().take(INNER_PRODUCER_SYNC_ATTEMPTS) {
                    attempts += 1;
                    match result {
                        Ok(()) => {
                            scripted.retry_attempt_counts.push(attempts);
                            return Ok(());
                        }
                        Err(error) => last_error = Some(error),
                    }
                }
                scripted.retry_attempt_counts.push(attempts);
                return Err(last_error
                    .unwrap_or_else(|| "scripted retry publisher had no send result".to_owned()));
            }
            if let Some(result) = scripted.retry_results.pop_front() {
                scripted.retry_attempt_counts.push(1);
                return result;
            }
        }

        let producer = self.inner_producer(runtime)?;
        let mut retry = retry.clone_message();
        producer
            .send_internal_with_runtime(
                runtime,
                &mut retry,
                Duration::from_millis(INNER_PRODUCER_SEND_TIMEOUT_MILLIS),
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub(super) fn inner_producer(
        &self,
        runtime: &Arc<ClientRuntime>,
    ) -> Result<Arc<rocketmq_classic_producer::Producer>, String> {
        let matches_runtime = |owner: &Weak<ClientRuntime>| {
            owner
                .upgrade()
                .is_some_and(|owner| Arc::ptr_eq(&owner, runtime))
        };
        {
            let cached = read_lock(&self.inner_producer);
            if let Some((owner, producer)) = cached.as_ref() {
                if matches_runtime(owner) {
                    return Ok(producer.clone());
                }
            }
        }

        let mut cached = write_lock(&self.inner_producer);
        if let Some((owner, producer)) = cached.as_ref() {
            if matches_runtime(owner) {
                return Ok(producer.clone());
            }
        }

        let mut config = rocketmq_classic_producer::ProducerConfig::new("CLIENT_INNER_PRODUCER")
            .map_err(|error| error.to_string())?;
        *config.client_config_mut() = self.config.client_config().clone();
        config.set_tls_config(self.config.tls_config().cloned());
        let producer = runtime
            .get_or_insert_extension(Arc::new(rocketmq_classic_producer::Producer::new(config)));
        *cached = Some((Arc::downgrade(runtime), producer.clone()));
        Ok(producer)
    }

    fn schedule_offset_recovery(
        self: &Arc<Self>,
        runtime: Arc<ClientRuntime>,
        offset_store: Arc<dyn OffsetStore>,
        queue: MessageQueue,
        generation: u64,
        corrected_offset: i64,
    ) {
        offset_store.update_and_freeze_offset(&queue, corrected_offset);
        let consumer = Arc::downgrade(self);
        let retry_runtime = runtime.clone();
        let retry_store = offset_store.clone();
        let retry_queue = queue.clone();
        let task_retry_runtime = retry_runtime.clone();
        let task_retry_store = retry_store.clone();
        let task_retry_queue = retry_queue.clone();
        let task = async move {
            match offset_store.persist(&queue).await {
                Ok(()) => {
                    if let Some(consumer) = consumer.upgrade() {
                        let _ = consumer
                            .remove_process_queue_generation(&queue, generation)
                            .await;
                    }
                }
                Err(error) => {
                    tracing::error!(
                        queue = ?queue,
                        generation,
                        error = %error,
                        "failed to persist corrected push pull offset"
                    );
                    if let Some(consumer) = consumer.upgrade() {
                        consumer.schedule_offset_recovery_retry(
                            task_retry_runtime,
                            task_retry_store,
                            task_retry_queue,
                            generation,
                            corrected_offset,
                        );
                    }
                }
            }
        };
        if let Err(error) = runtime.execute_task(task) {
            tracing::error!(
                queue = ?retry_queue,
                generation,
                error = %error,
                "failed to schedule corrected push offset persistence"
            );
            self.schedule_offset_recovery_retry(
                retry_runtime,
                retry_store,
                retry_queue,
                generation,
                corrected_offset,
            );
        }
    }

    fn schedule_offset_recovery_retry(
        self: &Arc<Self>,
        runtime: Arc<ClientRuntime>,
        offset_store: Arc<dyn OffsetStore>,
        queue: MessageQueue,
        generation: u64,
        corrected_offset: i64,
    ) {
        let consumer = Arc::downgrade(self);
        let task_runtime = runtime.clone();
        let task_queue = queue.clone();
        if let Err(error) = runtime.execute_task_later(
            async move {
                if let Some(consumer) = consumer.upgrade() {
                    consumer.schedule_offset_recovery(
                        task_runtime,
                        offset_store,
                        task_queue,
                        generation,
                        corrected_offset,
                    );
                }
            },
            PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION,
        ) {
            tracing::error!(
                queue = ?queue,
                generation,
                error = %error,
                "failed to reschedule corrected push offset persistence"
            );
        }
    }
}
