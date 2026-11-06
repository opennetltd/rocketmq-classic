use std::{collections::BTreeMap, sync::Arc};

use md5::{Digest, Md5};
use rocketmq_classic_common::message::MessageQueue;

use super::{AllocateMessageQueueStrategy, current_client_index};
use crate::rebalance::RebalanceError;

/// Java `HashFunction`, used by the consistent-hash router.
pub trait HashFunction: Send + Sync {
    fn hash(&self, key: &str) -> i64;
}

impl<F> HashFunction for F
where
    F: Fn(&str) -> i64 + Send + Sync,
{
    fn hash(&self, key: &str) -> i64 {
        self(key)
    }
}

/// Java `AllocateMessageQueueConsistentHash` (`getName() == "CONSISTENT_HASH"`).
#[derive(Clone)]
pub struct AllocateMessageQueueConsistentHash {
    virtual_node_count: usize,
    hash_function: Arc<dyn HashFunction>,
}

impl AllocateMessageQueueConsistentHash {
    /// Creates the Java default of ten virtual nodes with its MD5 hash.
    pub fn new() -> Self {
        Self {
            virtual_node_count: 10,
            hash_function: Arc::new(Md5Hash),
        }
    }

    pub fn with_virtual_node_count(virtual_node_count: i32) -> Result<Self, RebalanceError> {
        Self::with_hash_function(virtual_node_count, Arc::new(Md5Hash))
    }

    pub fn with_hash_function(
        virtual_node_count: i32,
        hash_function: Arc<dyn HashFunction>,
    ) -> Result<Self, RebalanceError> {
        let virtual_node_count = usize::try_from(virtual_node_count)
            .map_err(|_| RebalanceError::InvalidVirtualNodeCount(virtual_node_count))?;
        Ok(Self {
            virtual_node_count,
            hash_function,
        })
    }

    pub const fn virtual_node_count(&self) -> usize {
        self.virtual_node_count
    }
}

impl Default for AllocateMessageQueueConsistentHash {
    fn default() -> Self {
        Self::new()
    }
}

impl AllocateMessageQueueStrategy for AllocateMessageQueueConsistentHash {
    fn allocate(
        &self,
        _consumer_group: &str,
        current_client_id: &str,
        queues: &[MessageQueue],
        consumer_ids: &[String],
    ) -> Result<Vec<MessageQueue>, RebalanceError> {
        let Some(_) = current_client_index(current_client_id, queues, consumer_ids)? else {
            return Ok(Vec::new());
        };

        let mut ring = BTreeMap::<i64, &str>::new();
        for consumer_id in consumer_ids {
            for replica in 0..self.virtual_node_count {
                let virtual_key = format!("{consumer_id}-{replica}");
                ring.insert(self.hash_function.hash(&virtual_key), consumer_id);
            }
        }

        let mut result = Vec::new();
        for queue in queues {
            let point = self.hash_function.hash(&queue.to_string());
            let owner = ring
                .range(point..)
                .next()
                .or_else(|| ring.first_key_value())
                .map(|(_, consumer_id)| *consumer_id);
            if owner == Some(current_client_id) {
                result.push(queue.clone());
            }
        }
        Ok(result)
    }

    fn name(&self) -> &str {
        "CONSISTENT_HASH"
    }
}

struct Md5Hash;

impl HashFunction for Md5Hash {
    fn hash(&self, key: &str) -> i64 {
        let digest = Md5::digest(key.as_bytes());
        i64::from(u32::from_be_bytes([
            digest[0], digest[1], digest[2], digest[3],
        ]))
    }
}
