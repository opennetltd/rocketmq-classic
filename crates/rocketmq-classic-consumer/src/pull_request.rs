use std::{any::Any, fmt, sync::Arc};

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_core::runtime::MessageRequest;
use rocketmq_classic_protocol::{SharedValue, body::MessageRequestMode};

use crate::process_queue::ProcessQueue;

/// Java-compatible value corresponding to `client.impl.consumer.PullRequest`.
///
/// The process-queue and message-queue handles retain Java setter/getter
/// references. Java equality and hashing use only the consumer group and
/// message queue; the cursor and lock flag remain mutable operational state.
#[derive(Default)]
pub struct PullRequest {
    consumer_group: Option<String>,
    message_queue: Option<SharedValue<MessageQueue>>,
    process_queue: Option<Arc<ProcessQueue>>,
    next_offset: i64,
    previously_locked: bool,
}

impl PullRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn previously_locked(&self) -> bool {
        self.previously_locked
    }

    pub fn set_previously_locked(&mut self, previously_locked: bool) {
        self.previously_locked = previously_locked;
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

    pub fn next_offset(&self) -> i64 {
        self.next_offset
    }

    pub fn set_next_offset(&mut self, next_offset: i64) {
        self.next_offset = next_offset;
    }

    pub fn process_queue(&self) -> Option<Arc<ProcessQueue>> {
        self.process_queue.clone()
    }

    pub fn set_process_queue(&mut self, process_queue: Option<Arc<ProcessQueue>>) {
        self.process_queue = process_queue;
    }

    pub const fn message_request_mode(&self) -> MessageRequestMode {
        MessageRequestMode::Pull
    }

    /// Returns the pinned Java `PullRequest.hashCode` value.
    pub fn java_hash_code(&self) -> i32 {
        let result = 31i32.wrapping_add(java_optional_string_hash(&self.consumer_group));
        result.wrapping_mul(31).wrapping_add(
            self.message_queue
                .as_ref()
                .map_or(0, |queue| queue.with(MessageQueue::java_hash_code)),
        )
    }
}

impl PartialEq for PullRequest {
    fn eq(&self, other: &Self) -> bool {
        self.consumer_group == other.consumer_group
            && match (&self.message_queue, &other.message_queue) {
                (None, None) => true,
                (Some(left), Some(right)) => left.with(|left| right.with(|right| left == right)),
                _ => false,
            }
    }
}

impl Eq for PullRequest {}

impl MessageRequest for PullRequest {
    fn consumer_group(&self) -> Option<&str> {
        PullRequest::consumer_group(self)
    }

    fn mode(&self) -> MessageRequestMode {
        self.message_request_mode()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl fmt::Display for PullRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message_queue = self.message_queue.as_ref().map_or_else(
            || "null".to_owned(),
            |queue| queue.with(ToString::to_string),
        );
        write!(
            formatter,
            "PullRequest [consumerGroup={}, messageQueue={}, nextOffset={}]",
            self.consumer_group.as_deref().unwrap_or("null"),
            message_queue,
            self.next_offset
        )
    }
}

impl fmt::Debug for PullRequest {
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

    use super::PullRequest;
    use crate::ProcessQueue;

    #[test]
    fn pull_request_value_api_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/consumer/facade-api.json"))
                .expect("valid pinned Java consumer facade fixture");
        let expected = &fixture["implementationBehaviors"]["pullRequest"];
        let defaults = &expected["defaults"];

        let empty = PullRequest::new();
        assert_eq!(empty.consumer_group(), defaults["consumerGroup"].as_str());
        assert!(empty.message_queue().is_none());
        assert!(empty.process_queue().is_none());
        assert_eq!(
            empty.next_offset(),
            defaults["nextOffset"].as_i64().unwrap()
        );
        assert_eq!(
            empty.previously_locked(),
            defaults["previouslyLocked"].as_bool().unwrap()
        );
        assert_eq!(empty.message_request_mode(), MessageRequestMode::Pull);
        assert_eq!(MessageRequest::consumer_group(&empty), None);
        assert_eq!(MessageRequest::mode(&empty), MessageRequestMode::Pull);
        assert!(MessageRequest::as_any(&empty).is::<PullRequest>());
        assert_eq!(
            empty.java_hash_code(),
            defaults["hashCode"].as_i64().unwrap() as i32
        );
        assert_eq!(empty.to_string(), defaults["toString"].as_str().unwrap());

        let populated = &expected["populated"];
        let mut request = PullRequest::new();
        let queue = SharedValue::new(MessageQueue::new("orders-😀", "broker-雪", 17));
        let process_queue = Arc::new(ProcessQueue::new());
        request.set_consumer_group(Some("group-雪".into()));
        request.set_message_queue(Some(queue.clone()));
        request.set_process_queue(Some(process_queue.clone()));
        request.set_next_offset(-42);
        request.set_previously_locked(true);

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
                .process_queue()
                .is_some_and(|stored| Arc::ptr_eq(&stored, &process_queue))
        );
        assert_eq!(
            request.next_offset(),
            populated["nextOffset"].as_i64().unwrap()
        );
        assert_eq!(
            request.previously_locked(),
            populated["previouslyLocked"].as_bool().unwrap()
        );
        assert_eq!(request.message_request_mode(), MessageRequestMode::Pull);
        assert_eq!(
            request.java_hash_code(),
            populated["hashCode"].as_i64().unwrap() as i32
        );
        assert_eq!(request.to_string(), populated["toString"].as_str().unwrap());

        let mut equal_by_value = PullRequest::new();
        equal_by_value.set_consumer_group(Some("group-雪".into()));
        equal_by_value.set_message_queue(Some(SharedValue::new(MessageQueue::new(
            "orders-😀",
            "broker-雪",
            17,
        ))));
        equal_by_value.set_process_queue(Some(Arc::new(ProcessQueue::new())));
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
