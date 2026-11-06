use std::{any::Any, fmt, sync::Arc};

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_core::runtime::MessageRequest;
use rocketmq_classic_protocol::{SharedValue, body::MessageRequestMode};

use crate::pop::PopProcessQueue;

/// Java-compatible value corresponding to `client.impl.consumer.PopRequest`.
///
/// Queue and POP-process-queue handles preserve the references retained by
/// Java setters. Java `equals` and `hashCode` intentionally consider only the
/// topic, consumer group, and message queue.
pub struct PopRequest {
    topic: Option<String>,
    consumer_group: Option<String>,
    message_queue: Option<SharedValue<MessageQueue>>,
    pop_process_queue: Option<Arc<PopProcessQueue>>,
    locked_first: bool,
    init_mode: i32,
}

impl PopRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_locked_first(&self) -> bool {
        self.locked_first
    }

    pub fn set_locked_first(&mut self, locked_first: bool) {
        self.locked_first = locked_first;
    }

    pub fn consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    pub fn set_consumer_group(&mut self, consumer_group: Option<String>) {
        self.consumer_group = consumer_group;
    }

    pub fn message_queue(&self) -> Option<SharedValue<MessageQueue>> {
        self.message_queue.clone()
    }

    pub fn set_message_queue(&mut self, message_queue: Option<SharedValue<MessageQueue>>) {
        self.message_queue = message_queue;
    }

    pub fn topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, topic: Option<String>) {
        self.topic = topic;
    }

    pub fn pop_process_queue(&self) -> Option<Arc<PopProcessQueue>> {
        self.pop_process_queue.clone()
    }

    pub fn set_pop_process_queue(&mut self, pop_process_queue: Option<Arc<PopProcessQueue>>) {
        self.pop_process_queue = pop_process_queue;
    }

    pub fn init_mode(&self) -> i32 {
        self.init_mode
    }

    pub fn set_init_mode(&mut self, init_mode: i32) {
        self.init_mode = init_mode;
    }

    pub const fn message_request_mode(&self) -> MessageRequestMode {
        MessageRequestMode::Pop
    }

    /// Returns the pinned Java `PopRequest.hashCode` value.
    pub fn java_hash_code(&self) -> i32 {
        let mut result = 1i32;
        result = result
            .wrapping_mul(31)
            .wrapping_add(java_optional_string_hash(&self.topic));
        result = result
            .wrapping_mul(31)
            .wrapping_add(java_optional_string_hash(&self.consumer_group));
        result.wrapping_mul(31).wrapping_add(
            self.message_queue
                .as_ref()
                .map_or(0, |queue| queue.with(MessageQueue::java_hash_code)),
        )
    }
}

impl Default for PopRequest {
    fn default() -> Self {
        Self {
            topic: None,
            consumer_group: None,
            message_queue: None,
            pop_process_queue: None,
            locked_first: false,
            // org.apache.rocketmq.common.constant.ConsumeInitMode.MAX
            init_mode: 1,
        }
    }
}

impl PartialEq for PopRequest {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic
            && self.consumer_group == other.consumer_group
            && match (&self.message_queue, &other.message_queue) {
                (None, None) => true,
                (Some(left), Some(right)) => left.with(|left| right.with(|right| left == right)),
                _ => false,
            }
    }
}

impl Eq for PopRequest {}

impl MessageRequest for PopRequest {
    fn consumer_group(&self) -> Option<&str> {
        self.consumer_group()
    }

    fn mode(&self) -> MessageRequestMode {
        self.message_request_mode()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl fmt::Display for PopRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message_queue = self.message_queue.as_ref().map_or_else(
            || "null".to_owned(),
            |queue| queue.with(ToString::to_string),
        );
        write!(
            formatter,
            "PopRequest [topic={}, consumerGroup={}, messageQueue={}]",
            self.topic.as_deref().unwrap_or("null"),
            self.consumer_group.as_deref().unwrap_or("null"),
            message_queue
        )
    }
}

impl fmt::Debug for PopRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

fn java_optional_string_hash(value: &Option<String>) -> i32 {
    value.as_deref().map_or(0, |value| {
        value.encode_utf16().fold(0i32, |hash, unit| {
            hash.wrapping_mul(31).wrapping_add(i32::from(unit))
        })
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocketmq_classic_common::message::MessageQueue;
    use rocketmq_classic_core::runtime::MessageRequest;
    use rocketmq_classic_protocol::{SharedValue, body::MessageRequestMode};

    use super::PopRequest;
    use crate::PopProcessQueue;

    #[test]
    fn pop_request_value_api_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/consumer/facade-api.json"))
                .expect("valid pinned Java consumer facade fixture");
        let expected = &fixture["implementationBehaviors"]["popRequest"];
        let defaults = &expected["defaults"];

        let empty = PopRequest::new();
        assert_eq!(empty.topic(), defaults["topic"].as_str());
        assert_eq!(empty.consumer_group(), defaults["consumerGroup"].as_str());
        assert!(empty.message_queue().is_none());
        assert!(empty.pop_process_queue().is_none());
        assert_eq!(
            empty.is_locked_first(),
            defaults["lockedFirst"].as_bool().unwrap()
        );
        assert_eq!(
            empty.init_mode(),
            defaults["initMode"].as_i64().unwrap() as i32
        );
        assert_eq!(empty.message_request_mode(), MessageRequestMode::Pop);
        assert_eq!(MessageRequest::consumer_group(&empty), None);
        assert_eq!(
            empty.java_hash_code(),
            defaults["hashCode"].as_i64().unwrap() as i32
        );
        assert_eq!(empty.to_string(), defaults["toString"].as_str().unwrap());

        let populated = &expected["populated"];
        let mut request = PopRequest::new();
        let queue = SharedValue::new(MessageQueue::new("orders-😀", "broker-雪", 17));
        let process_queue = Arc::new(PopProcessQueue::new());
        request.set_topic(Some("orders-😀".into()));
        request.set_consumer_group(Some("group-雪".into()));
        request.set_message_queue(Some(queue.clone()));
        request.set_pop_process_queue(Some(process_queue.clone()));
        request.set_locked_first(true);
        request.set_init_mode(-7);

        assert_eq!(request.topic(), populated["topic"].as_str());
        assert_eq!(
            request.consumer_group(),
            populated["consumerGroup"].as_str()
        );
        assert!(
            request
                .message_queue()
                .is_some_and(|stored| stored.ptr_eq(&queue))
        );
        assert!(
            request
                .pop_process_queue()
                .is_some_and(|stored| Arc::ptr_eq(&stored, &process_queue))
        );
        assert_eq!(
            request.is_locked_first(),
            populated["lockedFirst"].as_bool().unwrap()
        );
        assert_eq!(
            request.init_mode(),
            populated["initMode"].as_i64().unwrap() as i32
        );
        assert_eq!(request.message_request_mode(), MessageRequestMode::Pop);
        assert_eq!(MessageRequest::consumer_group(&request), Some("group-雪"));
        assert_eq!(MessageRequest::mode(&request), MessageRequestMode::Pop);
        assert!(MessageRequest::as_any(&request).is::<PopRequest>());
        assert_eq!(
            request.java_hash_code(),
            populated["hashCode"].as_i64().unwrap() as i32
        );
        assert_eq!(request.to_string(), populated["toString"].as_str().unwrap());

        let mut equal_by_value = PopRequest::new();
        equal_by_value.set_topic(Some("orders-😀".into()));
        equal_by_value.set_consumer_group(Some("group-雪".into()));
        equal_by_value.set_message_queue(Some(SharedValue::new(MessageQueue::new(
            "orders-😀",
            "broker-雪",
            17,
        ))));
        equal_by_value.set_pop_process_queue(Some(Arc::new(PopProcessQueue::new())));
        assert_eq!(request, equal_by_value);
        assert_eq!(
            equal_by_value.java_hash_code(),
            populated["equalHashCode"].as_i64().unwrap() as i32
        );
        assert!(
            populated["equalsIgnoresProcessQueueAndFlags"]
                .as_bool()
                .unwrap()
        );

        queue.with_mut(|queue| queue.set_topic(Some("orders-mutated".into())));
        assert_eq!(
            queue.with(|queue| queue.topic().map(str::to_owned)),
            populated["topicAfterMessageQueueMutation"]
                .as_str()
                .map(str::to_owned)
        );
        assert_eq!(
            request.to_string(),
            populated["toStringAfterMessageQueueMutation"]
                .as_str()
                .unwrap()
        );
    }
}
