//! Java 5.5.1 queue assignment and Push rebalance lifecycle.

mod service;
pub mod strategy;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RebalanceError {
    #[error("current client id is empty")]
    EmptyCurrentClient,
    #[error("message queue list is null or empty")]
    EmptyMessageQueueList,
    #[error("consumer id list is null or empty")]
    EmptyConsumerIdList,
    #[error("machine room consumer IDs have not been configured")]
    MissingMachineRoomConfiguration,
    #[error("consistent hash virtual node count must be nonnegative, got {0}")]
    InvalidVirtualNodeCount(i32),
    #[error("allocation strategy failed: {0}")]
    Custom(String),
}

pub use strategy::{
    AllocateMessageQueueAveragely, AllocateMessageQueueAveragelyByCircle,
    AllocateMessageQueueByConfig, AllocateMessageQueueByMachineRoom,
    AllocateMessageQueueConsistentHash, AllocateMessageQueueStrategy, HashFunction,
};

pub(crate) use service::RebalanceService;
