//! Consumer facade compatibility boundary.
//!
//! The Java Push and Pull facades expose unique-key lookup through
//! `viewMessage`; they do not publish a separate query method.
//!
//! ```compile_fail
//! use rocketmq_classic_consumer::{PullConsumer, PushConsumer};
//!
//! fn extra_java_api(push: &PushConsumer, pull: &PullConsumer) {
//!     let _ = push.query_message_by_unique_key("orders", "id", 32, 0, 1);
//!     let _ = pull.query_message_by_unique_key("orders", "id", 32, 0, 1);
//! }
//! ```

mod admin;
pub mod config;
mod consume;
pub mod error;
pub mod lite;
mod mq_helper;
pub mod offset;
pub mod pop;
pub mod pop_request;
pub mod process_queue;
pub mod pull;
pub mod pull_request;
pub mod push;
pub mod rebalance;
mod service_state;
mod stack;
#[cfg(test)]
mod test_support;

pub use config::PullConsumerConfig;
pub use consume::{
    ConsumeMessageContext, ConsumeMessageHook, ConsumeMessageService, ConsumeMessageServiceFuture,
};
pub use error::{LitePullConsumerError, PullConsumerError, PushConsumerError};
pub use lite::{LitePullConsumer, LitePullConsumerConfig, TopicMessageQueueChangeListener};
pub use mq_helper::MqHelper;
pub use pop::{
    PopProcessQueue, PopProcessQueueError, PopReceipt, PopReceiptError, PopResult, PopStatus,
};
pub use pop_request::PopRequest;
pub use process_queue::{ExpiredMessageCleanupError, ProcessQueue};
pub use pull_request::PullRequest;
pub use push::{
    ConcurrentListenerFuture, ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus,
    ConsumeOrderlyContext, ConsumeOrderlyStatus, MessageListenerConcurrently,
    MessageListenerOrderly, MessageQueueListener, OrderlyListenerFuture, PushConsumer,
    PushConsumerConfig, PushMessageListener,
};
pub use rocketmq_classic_core::api::admin::QueryResult;
pub use rocketmq_classic_core::runtime::ConsumerStatsManager;
pub use service_state::ConsumerServiceState;
