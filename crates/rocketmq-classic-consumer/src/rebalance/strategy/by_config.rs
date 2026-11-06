use std::sync::RwLock;

use rocketmq_classic_common::message::MessageQueue;

use super::AllocateMessageQueueStrategy;
use crate::rebalance::RebalanceError;

/// Java `AllocateMessageQueueByConfig` (`getName() == "CONFIG"`).
///
/// Java returns its configured list directly, without validating or filtering
/// it against the queue/client arguments. `None` corresponds to Java `null`;
/// the rebalance caller treats that as an empty assignment.
#[derive(Default)]
pub struct AllocateMessageQueueByConfig {
    message_queue_list: RwLock<Option<Vec<MessageQueue>>>,
}

impl AllocateMessageQueueByConfig {
    pub fn message_queue_list(&self) -> Option<Vec<MessageQueue>> {
        self.message_queue_list
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn set_message_queue_list(&self, message_queue_list: Option<Vec<MessageQueue>>) {
        *self
            .message_queue_list
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = message_queue_list;
    }
}

impl AllocateMessageQueueStrategy for AllocateMessageQueueByConfig {
    fn allocate(
        &self,
        _consumer_group: &str,
        _current_client_id: &str,
        _queues: &[MessageQueue],
        _consumer_ids: &[String],
    ) -> Result<Vec<MessageQueue>, RebalanceError> {
        Ok(self.message_queue_list().unwrap_or_default())
    }

    fn name(&self) -> &str {
        "CONFIG"
    }
}
