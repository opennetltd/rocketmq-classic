use rocketmq_classic_common::message::MessageQueue;

use super::{AllocateMessageQueueStrategy, current_client_index};
use crate::rebalance::RebalanceError;

/// Java `AllocateMessageQueueAveragelyByCircle` (`getName() == "AVG_BY_CIRCLE"`).
#[derive(Debug, Default, Clone, Copy)]
pub struct AllocateMessageQueueAveragelyByCircle;

impl AllocateMessageQueueStrategy for AllocateMessageQueueAveragelyByCircle {
    fn allocate(
        &self,
        _consumer_group: &str,
        current_client_id: &str,
        queues: &[MessageQueue],
        consumer_ids: &[String],
    ) -> Result<Vec<MessageQueue>, RebalanceError> {
        let Some(index) = current_client_index(current_client_id, queues, consumer_ids)? else {
            return Ok(Vec::new());
        };
        Ok(queues
            .iter()
            .enumerate()
            .filter(|(queue_index, _)| queue_index % consumer_ids.len() == index)
            .map(|(_, queue)| queue.clone())
            .collect())
    }

    fn name(&self) -> &str {
        "AVG_BY_CIRCLE"
    }
}
