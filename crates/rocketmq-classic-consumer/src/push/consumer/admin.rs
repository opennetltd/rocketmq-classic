use super::*;

impl PushConsumer {
    /// Searches a queue by Java's default lower timestamp boundary.
    pub async fn search_offset(
        &self,
        input_queue: &MessageQueue,
        timestamp_millis: i64,
    ) -> Result<i64, PushConsumerError> {
        let (runtime, queue, broker_name, address, deadline) =
            self.prepare_admin_queue(input_queue).await?;
        let mut header = SearchOffsetRequestHeader::default();
        header.set_topic(Some(
            queue
                .topic()
                .ok_or(PushConsumerError::InvalidQueue)?
                .to_owned(),
        ));
        header.set_queue_id(Some(queue.queue_id()));
        header.set_timestamp(Some(timestamp_millis));
        header.set_boundary_type(Some(BoundaryType::Lower));
        header.set_broker_name(Some(broker_name));
        Ok(runtime
            .api()
            .search_offset(&address, &header, deadline)
            .await?)
    }

    /// Resets each currently subscribed topic to the Broker offsets selected
    /// by Java's `DefaultMQPushConsumerImpl.resetOffsetByTimeStamp` flow.
    pub async fn reset_offset_by_timestamp(
        &self,
        timestamp_millis: i64,
    ) -> Result<(), PushConsumerError> {
        let topics = read_lock(&self.subscriptions)
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        for topic in topics {
            let queues = read_lock(&self.topic_queues)
                .get(&topic)
                .cloned()
                .unwrap_or_default();
            if queues.is_empty() {
                continue;
            }
            let mut offsets = Vec::with_capacity(queues.len());
            for queue in queues {
                let offset = self.search_offset(&queue, timestamp_millis).await?;
                offsets.push((queue, Some(offset)));
            }
            self.reset_consumer_offsets(&topic, offsets).await?;
        }
        Ok(())
    }

    pub async fn get_max_offset(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<i64, PushConsumerError> {
        let (runtime, queue, broker_name, address, deadline) =
            self.prepare_admin_queue(input_queue).await?;
        let mut header = GetMaxOffsetRequestHeader::default();
        header.set_topic(Some(
            queue
                .topic()
                .ok_or(PushConsumerError::InvalidQueue)?
                .to_owned(),
        ));
        header.set_queue_id(Some(queue.queue_id()));
        header.set_committed(true);
        header.set_broker_name(Some(broker_name));
        Ok(runtime
            .api()
            .get_max_offset(&address, &header, deadline)
            .await?)
    }

    pub async fn get_min_offset(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<i64, PushConsumerError> {
        let (runtime, queue, broker_name, address, deadline) =
            self.prepare_admin_queue(input_queue).await?;
        let mut header = GetMinOffsetRequestHeader::default();
        header.set_topic(Some(
            queue
                .topic()
                .ok_or(PushConsumerError::InvalidQueue)?
                .to_owned(),
        ));
        header.set_queue_id(Some(queue.queue_id()));
        header.set_broker_name(Some(broker_name));
        Ok(runtime
            .api()
            .get_min_offset(&address, &header, deadline)
            .await?)
    }

    pub async fn get_earliest_msg_store_time(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<i64, PushConsumerError> {
        let (runtime, queue, broker_name, address, deadline) =
            self.prepare_admin_queue(input_queue).await?;
        let mut header = GetEarliestMsgStoretimeRequestHeader::default();
        header.set_topic(Some(
            queue
                .topic()
                .ok_or(PushConsumerError::InvalidQueue)?
                .to_owned(),
        ));
        header.set_queue_id(Some(queue.queue_id()));
        header.set_broker_name(Some(broker_name));
        Ok(runtime
            .api()
            .get_earliest_msg_storetime(&address, &header, deadline)
            .await?)
    }

    pub async fn create_topic(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        _attributes: Option<&BTreeMap<String, String>>,
    ) -> Result<(), PushConsumerError> {
        self.create_topic_with_sys_flag(key, new_topic, queue_num, 0, None)
            .await
    }

    pub async fn create_topic_with_sys_flag(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        _attributes: Option<&BTreeMap<String, String>>,
    ) -> Result<(), PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let mut client_config = self.config.client_config().clone();
        let topic = client_config
            .with_namespace(Some(new_topic))?
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidAdminTopic(
                "The specified topic is blank.".into(),
            ))?;
        crate::admin::validate_topic(&topic).map_err(PushConsumerError::InvalidAdminTopic)?;
        crate::admin::create_topic(&context.runtime, key, &topic, queue_num, topic_sys_flag)
            .await?;
        Ok(())
    }

    pub async fn fetch_subscribe_message_queues(
        &self,
        input_topic: &str,
    ) -> Result<BTreeSet<MessageQueue>, PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let mut client_config = self.config.client_config().clone();
        let topic = client_config
            .with_namespace(Some(input_topic))?
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidAdminTopic(
                "The specified topic is blank.".into(),
            ))?;
        let timeout = Duration::from_millis(
            i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64,
        );
        let queues =
            crate::admin::fetch_subscribe_queues(&context.runtime, &topic, timeout).await?;
        self.parse_subscribe_message_queues(&queues)
    }

    /// Removes this consumer's namespace from queue topics returned by the
    /// implementation's raw `parseSubscribeMessageQueues` entrypoint.
    pub fn parse_subscribe_message_queues(
        &self,
        queue_set: &BTreeSet<MessageQueue>,
    ) -> Result<BTreeSet<MessageQueue>, PushConsumerError> {
        let mut client_config = self.config.client_config().clone();
        queue_set
            .iter()
            .map(|queue| {
                let topic = client_config.without_namespace(queue.topic())?;
                Ok(MessageQueue::new_nullable(
                    topic,
                    queue.broker_name().map(str::to_owned),
                    queue.queue_id(),
                ))
            })
            .collect::<Result<BTreeSet<_>, rocketmq_classic_core::config::ConfigError>>()
            .map_err(PushConsumerError::from)
    }
}
