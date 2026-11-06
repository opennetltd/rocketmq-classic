//! Producer facade compatibility boundary.
//!
//! Unique-key lookup is part of Java's `viewMessage` fallback, not a separate
//! `DefaultMQProducer` public method.
//!
//! ```compile_fail
//! use rocketmq_classic_producer::Producer;
//!
//! fn extra_java_api(producer: &Producer) {
//!     let _ = producer.query_message_by_unique_key("orders", "id", 32, 0, 1);
//! }
//! ```

mod accumulator;
mod admin;
mod backpressure;
mod batch;
mod compressor;
mod config;
mod error;
mod executor;
mod fault;
mod hooks;
mod producer;
mod publish_info;
mod recall;
mod request_reply;
mod retry_response_codes;
mod selector;
mod send;
mod service_state;
mod topic_list;
mod trace;
mod transaction;
pub mod validation;

pub use accumulator::ProduceAccumulator;
pub use backpressure::BackpressureReadGuard;
pub use batch::MessageBatch;
pub use compressor::ProducerCompressor;
pub use config::{DEFAULT_PRODUCER_GROUP, ProducerConfig};
pub use error::ProducerError;
pub use executor::{AsyncSenderExecutor, AsyncSenderTask};
pub use fault::{FaultItem, FaultItemCompareError, ProducerLongArray};
pub use hooks::{
    CheckForbiddenContext, CheckForbiddenHook, CommunicationMode, EndTransactionContext,
    EndTransactionHook, HookException, SendMessageContext, SendMessageHook, SendMessageType,
};
pub use producer::Producer;
pub use publish_info::{
    NullablePublishInfoQueueFilters, ProducerPublishQueueList, ProducerQueueIndex,
    PublishInfoQueueFilter, TopicPublishInfo, TopicPublishInfoEntryHandle,
    TopicPublishInfoEntrySetView, TopicPublishInfoKeySetView, TopicPublishInfoTable,
    TopicPublishInfoTableError, TopicPublishInfoValuesView,
};
pub use recall::RecallMessageHandle;
pub use request_reply::{
    ReplyMessageError, RequestCallback, RequestFutureHolder, create_reply_message,
};
pub use retry_response_codes::RetryResponseCodes;
pub use rocketmq_classic_common::sysflag::message::CompressionType;
pub use rocketmq_classic_core::api::admin::QueryResult;
pub use rocketmq_classic_core::api::producer::{SendResult, SendStatus};
pub use rocketmq_classic_core::runtime::{
    RequestFutureError, RequestFutureLatch, RequestFutureTable, RequestResponseCallback,
    RequestResponseFuture,
};
pub use rocketmq_classic_remoting::client::{CallbackExecutor, RpcHook};
pub use service_state::ProducerServiceState;
pub use topic_list::ProducerTopicList;
pub use trace::{TraceDispatcher, TraceDispatcherError, TraceProducerKind};
#[allow(deprecated)]
pub use transaction::{
    LocalTransactionState, TransactionCallbackError, TransactionCheckListener, TransactionExecutor,
    TransactionListener, TransactionProducer, TransactionSendResult,
};
