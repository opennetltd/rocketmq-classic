use super::*;

impl PushConsumer {
    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Result<QueryResult, PushConsumerError> {
        self.query_messages(topic, key, max_num, begin_timestamp, end_timestamp, false)
            .await
    }

    /// Returns the consume timestamp span for every Broker in a freshly
    /// queried route, retaining route order and Java's per-Broker failure
    /// behavior.
    pub async fn query_consume_time_span(
        &self,
        topic: &str,
    ) -> Result<Vec<Option<QueueTimeSpan>>, PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let route = context
            .runtime
            .api()
            .get_topic_route(topic, Instant::now() + CONSUME_TIME_SPAN_TIMEOUT)
            .await?
            .ok_or_else(
                || rocketmq_classic_core::error::RpcError::MalformedResponse {
                    request_code: request::GET_ROUTEINFO_BY_TOPIC,
                    peer: topic.to_owned(),
                    reason: "missing route response".into(),
                },
            )?;
        let brokers = route.broker_datas_snapshot().ok_or(
            rocketmq_classic_core::runtime::RuntimeError::InvalidRoute(
                "null BrokerData list in consume-time-span route",
            ),
        )?;
        let mut header = QueryConsumeTimeSpanRequestHeader::default();
        header.set_topic(Some(topic.to_owned()));
        header.set_group(self.group_name());
        let mut spans = Vec::new();
        for broker in brokers {
            let broker = broker.ok_or_else(|| {
                rocketmq_classic_core::error::RpcError::MalformedResponse {
                    request_code: request::QUERY_CONSUME_TIME_SPAN,
                    peer: topic.to_owned(),
                    reason: "null BrokerData in consume-time-span route".into(),
                }
            })?;
            let address = broker.java_select_broker_addr().map_err(|error| {
                rocketmq_classic_core::error::RpcError::MalformedResponse {
                    request_code: request::QUERY_CONSUME_TIME_SPAN,
                    peer: topic.to_owned(),
                    reason: error.to_string(),
                }
            })?;
            let address = address.ok_or_else(|| {
                rocketmq_classic_core::error::RpcError::MalformedResponse {
                    request_code: request::QUERY_CONSUME_TIME_SPAN,
                    peer: topic.to_owned(),
                    reason: "selected Broker address is null".into(),
                }
            })?;
            let request_address =
                broker_vip_channel(self.config.client_config().vip_channel_enabled(), &address)
                    .map_err(
                        |error| rocketmq_classic_core::error::RpcError::MalformedResponse {
                            request_code: request::QUERY_CONSUME_TIME_SPAN,
                            peer: address.clone(),
                            reason: error.to_string(),
                        },
                    )?;
            let response = context
                .runtime
                .api()
                .query_consume_time_span(
                    &request_address,
                    &header,
                    Instant::now() + CONSUME_TIME_SPAN_TIMEOUT,
                )
                .await?;
            let mut response = response.ok_or_else(|| {
                rocketmq_classic_core::error::RpcError::MalformedResponse {
                    request_code: request::QUERY_CONSUME_TIME_SPAN,
                    peer: address,
                    reason: "null consumeTimeSpanSet".into(),
                }
            })?;
            spans.append(&mut response);
        }
        Ok(spans)
    }

    /// Views a message by its physical ID, falling back to Java's unique-key
    /// query when parsing or the physical lookup fails.
    pub async fn view_message(
        &self,
        topic: &str,
        message_id: &str,
    ) -> Result<Option<MessageExt>, PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let mut client_config = self.config.client_config().clone();
        let namespaced_topic = client_config
            .with_namespace(Some(topic))?
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidQueue)?;
        let decoded = decode_message_id(message_id).ok().and_then(|decoded| {
            decoded
                .address()?
                .socket_addr()
                .map(|address| (address, decoded.offset()))
        });
        if let Some((address, offset)) = decoded {
            let header = {
                let mut __phase18_header_0 = ViewMessageRequestHeader::default();
                __phase18_header_0.set_topic(Some(namespaced_topic));
                __phase18_header_0.set_offset(Some(offset));
                __phase18_header_0
            };
            let timeout_millis =
                i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64;
            if let Ok(message) = context
                .runtime
                .api()
                .view_message(
                    &address.to_string(),
                    &header,
                    Instant::now() + Duration::from_millis(timeout_millis),
                )
                .await
            {
                return Ok(Some(message));
            }
        }

        let result = self
            .query_messages(
                topic,
                message_id,
                32,
                unique_key_lookup_begin_timestamp(),
                i64::MAX,
                true,
            )
            .await?;
        Ok(first_query_message(result))
    }

    #[allow(clippy::too_many_arguments)]
    async fn query_messages(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
        unique_key: bool,
    ) -> Result<QueryResult, PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let mut client_config = self.config.client_config().clone();
        let topic = client_config
            .with_namespace(Some(topic))?
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidQueue)?;
        let query = if unique_key {
            AdminQueryRequest::by_unique_key(&topic, key, max_num, begin_timestamp, end_timestamp)
        } else {
            AdminQueryRequest::by_key(&topic, key, max_num, begin_timestamp, end_timestamp)
        };
        let timeout_millis =
            i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64;
        let deadline = Instant::now() + Duration::from_millis(timeout_millis).saturating_mul(4);
        let result = context.runtime.query_message(&query, deadline).await?;
        if let Some(messages) = result.get_message_list() {
            for message in messages.snapshot().into_iter().flatten() {
                message.with_mut(|message| {
                    if let Some(topic) = client_config.without_namespace(Some(message.topic()))? {
                        message.set_topic(topic);
                    }
                    Ok::<(), PushConsumerError>(())
                })?;
            }
        }
        Ok(result)
    }
}
