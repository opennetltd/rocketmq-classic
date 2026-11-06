//! Allocation strategies ported from the public Java 5.5.1 client API.

mod averagely;
mod averagely_by_circle;
mod by_config;
mod by_machine_room;
mod consistent_hash;

use rocketmq_classic_common::message::MessageQueue;

use super::RebalanceError;

pub use averagely::AllocateMessageQueueAveragely;
pub use averagely_by_circle::AllocateMessageQueueAveragelyByCircle;
pub use by_config::AllocateMessageQueueByConfig;
pub use by_machine_room::AllocateMessageQueueByMachineRoom;
pub use consistent_hash::{AllocateMessageQueueConsistentHash, HashFunction};

/// Assigns the sorted topic queues among sorted consumer IDs.
///
/// Callers sort both slices with Java-compatible ordering before invoking a
/// strategy, matching `RebalanceImpl.rebalanceByTopic`.
pub trait AllocateMessageQueueStrategy: Send + Sync {
    fn allocate(
        &self,
        consumer_group: &str,
        current_client_id: &str,
        queues: &[MessageQueue],
        consumer_ids: &[String],
    ) -> Result<Vec<MessageQueue>, RebalanceError>;

    fn name(&self) -> &str;
}

pub(super) fn current_client_index(
    current_client_id: &str,
    queues: &[MessageQueue],
    consumer_ids: &[String],
) -> Result<Option<usize>, RebalanceError> {
    if current_client_id.is_empty() {
        return Err(RebalanceError::EmptyCurrentClient);
    }
    if queues.is_empty() {
        return Err(RebalanceError::EmptyMessageQueueList);
    }
    if consumer_ids.is_empty() {
        return Err(RebalanceError::EmptyConsumerIdList);
    }
    Ok(consumer_ids
        .iter()
        .position(|consumer_id| consumer_id == current_client_id))
}
