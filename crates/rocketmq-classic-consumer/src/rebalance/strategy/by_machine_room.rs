use std::{collections::HashSet, sync::RwLock};

use rocketmq_classic_common::message::MessageQueue;

use super::{AllocateMessageQueueStrategy, current_client_index};
use crate::rebalance::RebalanceError;

/// Java `AllocateMessageQueueByMachineRoom` (`getName() == "MACHINE_ROOM"`).
#[derive(Default)]
pub struct AllocateMessageQueueByMachineRoom {
    consumeridcs: RwLock<Option<HashSet<String>>>,
}

impl AllocateMessageQueueByMachineRoom {
    pub fn consumeridcs(&self) -> Option<HashSet<String>> {
        self.consumeridcs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn set_consumeridcs(&self, consumeridcs: Option<HashSet<String>>) {
        *self
            .consumeridcs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = consumeridcs;
    }
}

impl AllocateMessageQueueStrategy for AllocateMessageQueueByMachineRoom {
    fn allocate(
        &self,
        _consumer_group: &str,
        current_client_id: &str,
        queues: &[MessageQueue],
        consumer_ids: &[String],
    ) -> Result<Vec<MessageQueue>, RebalanceError> {
        let Some(current_index) = current_client_index(current_client_id, queues, consumer_ids)?
        else {
            return Ok(Vec::new());
        };
        let consumeridcs = self
            .consumeridcs()
            .ok_or(RebalanceError::MissingMachineRoomConfiguration)?;
        let eligible = queues
            .iter()
            .filter(|queue| {
                queue.broker_name().is_some_and(|broker_name| {
                    java_room_parts(broker_name)
                        .is_some_and(|(room, _)| consumeridcs.contains(room))
                })
            })
            .collect::<Vec<_>>();

        let consumer_count = consumer_ids.len();
        let quotient = eligible.len() / consumer_count;
        let remainder = eligible.len() % consumer_count;
        let start = quotient * current_index;
        let end = start + quotient;
        let mut result = eligible[start..end]
            .iter()
            .map(|queue| (*queue).clone())
            .collect::<Vec<_>>();
        if remainder > current_index {
            result.push((*eligible[current_index + quotient * consumer_count]).clone());
        }
        Ok(result)
    }

    fn name(&self) -> &str {
        "MACHINE_ROOM"
    }
}

/// Java `String.split("@")` discards trailing empty segments but retains a
/// leading empty segment when the delimiter is at the beginning.
fn java_room_parts(value: &str) -> Option<(&str, &str)> {
    let mut parts = value.split('@').collect::<Vec<_>>();
    if !value.is_empty() {
        while parts.last() == Some(&"") {
            parts.pop();
        }
    }
    (parts.len() == 2).then(|| (parts[0], parts[1]))
}
