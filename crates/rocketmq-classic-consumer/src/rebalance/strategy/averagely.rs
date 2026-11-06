use rocketmq_classic_common::message::MessageQueue;

use super::{AllocateMessageQueueStrategy, current_client_index};
use crate::rebalance::RebalanceError;

/// Java `AllocateMessageQueueAveragely` (`getName() == "AVG"`).
#[derive(Debug, Default, Clone, Copy)]
pub struct AllocateMessageQueueAveragely;

impl AllocateMessageQueueStrategy for AllocateMessageQueueAveragely {
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

        let queue_count = queues.len();
        let consumer_count = consumer_ids.len();
        let remainder = queue_count % consumer_count;
        let average_size = if queue_count <= consumer_count {
            1
        } else {
            queue_count / consumer_count + usize::from(remainder > 0 && index < remainder)
        };
        let start_index = if remainder > 0 && index < remainder {
            index * average_size
        } else {
            index * average_size + remainder
        };
        let range = average_size.min(queue_count.saturating_sub(start_index));
        Ok(queues
            .iter()
            .skip(start_index)
            .take(range)
            .cloned()
            .collect())
    }

    fn name(&self) -> &str {
        "AVG"
    }
}
