use base64::{
    Engine as _,
    engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
};
use rocketmq_classic_protocol::header::RecallMessageRequestHeader;
use tokio::time::Instant;

use crate::{Producer, ProducerError, validation};

/// Java's `RecallMessageHandle.HandleV1` payload for delayed-message recall.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecallMessageHandle {
    topic: String,
    broker_name: String,
    timestamp: String,
    message_id: String,
}

impl RecallMessageHandle {
    /// Builds Java's URL-safe Base64 v1 recall handle.
    pub fn build_handle(
        topic: &str,
        broker_name: &str,
        timestamp: &str,
        message_id: &str,
    ) -> String {
        URL_SAFE.encode(format!("v1 {topic} {broker_name} {timestamp} {message_id}"))
    }

    /// Decodes Java's v1 handle. Java accepts extra tokens and uses only the
    /// first five space-separated fields, so the decoder preserves that rule.
    pub fn decode_handle(handle: &str) -> Result<Self, ProducerError> {
        if handle.is_empty() {
            return Err(ProducerError::InvalidRecallHandle);
        }
        let encoded = handle.as_bytes();
        let bytes = URL_SAFE
            .decode(encoded)
            .or_else(|_| URL_SAFE_NO_PAD.decode(encoded))
            .map_err(|_| ProducerError::InvalidRecallHandle)?;
        let raw = String::from_utf8_lossy(&bytes);
        let mut fields: Vec<&str> = raw.split(' ').collect();
        while fields.last() == Some(&"") {
            fields.pop();
        }
        if fields.len() < 5 || fields[0] != "v1" {
            return Err(ProducerError::InvalidRecallHandle);
        }
        Ok(Self {
            topic: fields[1].to_owned(),
            broker_name: fields[2].to_owned(),
            timestamp: fields[3].to_owned(),
            message_id: fields[4].to_owned(),
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn version(&self) -> &'static str {
        "v1"
    }

    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    pub fn timestamp(&self) -> &str {
        &self.timestamp
    }

    pub fn message_id(&self) -> &str {
        &self.message_id
    }
}

impl Producer {
    /// Recalls a delayed message by sending Java's `RECALL_MESSAGE` request.
    /// A successful response without an extension-field map is `Ok(None)`;
    /// a present map missing `msgId` is a malformed response.
    pub async fn recall_message(
        &self,
        topic: &str,
        recall_handle: &str,
    ) -> Result<Option<String>, ProducerError> {
        let runtime = self.started_runtime().await?;
        let mut client_config = self.config.client_config().clone();
        let topic = client_config
            .with_namespace(Some(topic))?
            .ok_or(ProducerError::BlankTopic)?;
        validation::validate_topic(&topic)?;
        if is_retry_or_dlq_topic(&topic) {
            return Err(ProducerError::UnsupportedRecallTopic);
        }
        let decoded_handle = RecallMessageHandle::decode_handle(recall_handle)?;

        let deadline = Instant::now()
            + tokio::time::Duration::from_millis(self.config.send_timeout_millis().max(0) as u64);
        self.remember_publish_topic(&topic);
        let _ = runtime.update_topic_route(&topic, deadline).await?;
        let route = runtime.route_snapshot()?;
        if !route.raw_routes.contains_key(&topic) {
            let _ = runtime
                .update_topic_route_from_default(
                    &topic,
                    self.config.default_topic_queue_nums(),
                    deadline,
                )
                .await?;
        }

        let route = runtime.route_snapshot()?;
        let broker_addr = if let Some(address) = route.publish_address(decoded_handle.broker_name())
        {
            address.to_owned()
        } else {
            runtime
                .find_broker_address_by_topic(&topic)?
                .ok_or(ProducerError::RecallBrokerAddressNotFound)?
        };

        let header = {
            let mut __phase18_header_0 = RecallMessageRequestHeader::default();
            __phase18_header_0.set_producer_group(self.producer_group());
            __phase18_header_0.set_topic(topic);
            __phase18_header_0.set_recall_handle(recall_handle.to_owned());
            __phase18_header_0.set_broker_name(Some(decoded_handle.broker_name().to_owned()));
            __phase18_header_0
        };
        Ok(runtime
            .api()
            .recall_message(&broker_addr, &header, deadline)
            .await?)
    }
}

fn is_retry_or_dlq_topic(topic: &str) -> bool {
    topic.starts_with("%RETRY%") || topic.starts_with("%DLQ%")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_core::runtime::ClientRuntimeManager;
    use std::sync::Arc;

    #[test]
    fn java_handle_builder_and_decoder_round_trip_url_safe_payloads() {
        let handle = RecallMessageHandle::build_handle("topic", "broker-a", "123", "msg-id");
        let decoded = RecallMessageHandle::decode_handle(&handle).unwrap();
        assert_eq!(decoded.topic(), "topic");
        assert_eq!(decoded.broker_name(), "broker-a");
        assert_eq!(decoded.timestamp(), "123");
        assert_eq!(decoded.message_id(), "msg-id");
    }

    #[test]
    fn java_handle_decoder_accepts_extra_tokens_and_unpadded_url_base64() {
        let padded = URL_SAFE.encode("v1 topic broker 10 message ignored");
        let unpadded = padded.trim_end_matches('=');
        let decoded = RecallMessageHandle::decode_handle(unpadded).unwrap();
        assert_eq!(decoded.message_id(), "message");
    }

    #[test]
    fn malformed_recall_handles_match_java_error_text() {
        for value in ["", "!", &URL_SAFE.encode("v2 topic broker 1 id")] {
            assert!(matches!(
                RecallMessageHandle::decode_handle(value),
                Err(ProducerError::InvalidRecallHandle)
            ));
        }
        assert_eq!(
            ProducerError::InvalidRecallHandle.to_string(),
            "recall handle is invalid"
        );
    }

    #[test]
    fn retry_and_dlq_recall_topics_are_rejected() {
        assert!(is_retry_or_dlq_topic("%RETRY%group"));
        assert!(is_retry_or_dlq_topic("%DLQ%group"));
        assert!(!is_retry_or_dlq_topic("orders"));
    }

    #[tokio::test]
    async fn producer_recall_checks_lifecycle_topic_and_handle_before_routing() {
        let manager = ClientRuntimeManager::new();
        let producer = Arc::new(Producer::with_runtime_manager(
            crate::ProducerConfig::new("recall-producer").unwrap(),
            manager,
        ));
        assert!(matches!(
            producer.recall_message("%RETRY%group", "bad-handle").await,
            Err(ProducerError::NotStarted)
        ));

        producer.start().await.unwrap();
        assert!(matches!(
            producer.recall_message("%RETRY%group", "bad-handle").await,
            Err(ProducerError::UnsupportedRecallTopic)
        ));
        assert!(matches!(
            producer.recall_message("orders", "bad-handle").await,
            Err(ProducerError::InvalidRecallHandle)
        ));
    }
}
