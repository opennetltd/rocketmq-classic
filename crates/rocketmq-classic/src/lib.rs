//! Public RocketMQ Classic Rust facade.
//!
//! Internal Java-property bridging is kept in implementation crates and is
//! not exposed by this facade.
//!
//! ```compile_fail
//! use rocketmq_classic::utils::system_properties::get_property;
//! # fn main() {}
//! ```

pub use rocketmq_classic_common::message::{
    Message, MessageExt, MessageExtSubtype, MessageQueue, SharedMessageBody,
    SharedMessageProperties,
};
pub use rocketmq_classic_common::sysflag::message::CompressionType;
pub use rocketmq_classic_common::topic::{TopicValidator, TopicValidatorError, ValidateResult};
pub use rocketmq_classic_common::{PermName, PermissionParseError};
pub use rocketmq_classic_consumer::config::DEFAULT_CONSUMER_GROUP;
pub use rocketmq_classic_consumer::pull::{BoundaryType, MessageSelector, PullConsumer};
pub use rocketmq_classic_consumer::{
    ConcurrentListenerFuture, ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus,
    ConsumeMessageContext, ConsumeMessageHook, ConsumeMessageService, ConsumeMessageServiceFuture,
    ConsumeOrderlyContext, ConsumeOrderlyStatus, ConsumerServiceState, ConsumerStatsManager,
    LitePullConsumer, LitePullConsumerConfig, LitePullConsumerError, MessageListenerConcurrently,
    MessageListenerOrderly, MessageQueueListener, MqHelper, OrderlyListenerFuture, PopProcessQueue,
    PopRequest, PopResult, PopStatus, PullConsumerConfig, PullConsumerError, PullRequest,
    PushConsumer, PushConsumerConfig, PushConsumerError, PushMessageListener,
    TopicMessageQueueChangeListener, offset, pull,
};
pub use rocketmq_classic_core::api::extensions::NotifyResult;
pub use rocketmq_classic_core::auth::{
    AclClientRpcHook, AclException, AclSigner, AclUtils, AclUtilsError, SessionCredentials,
    SigningAlgorithm,
};
pub use rocketmq_classic_core::route::TopicRouteDataHandle;
pub use rocketmq_classic_core::trace::{
    LocalTransactionState, MessageType, TraceBean, TraceContext, TraceType,
};
pub use rocketmq_classic_core::{RpcHook, config::ClientConfig};
pub use rocketmq_classic_producer::validation;
#[allow(deprecated)]
pub use rocketmq_classic_producer::{
    AsyncSenderExecutor, AsyncSenderTask, BackpressureReadGuard, CallbackExecutor,
    CheckForbiddenContext, CheckForbiddenHook, CommunicationMode, EndTransactionContext,
    EndTransactionHook, FaultItem, FaultItemCompareError, HookException, MessageBatch,
    NullablePublishInfoQueueFilters, ProduceAccumulator, Producer, ProducerCompressor,
    ProducerConfig, ProducerError, ProducerLongArray, ProducerPublishQueueList, ProducerQueueIndex,
    ProducerServiceState, ProducerTopicList, PublishInfoQueueFilter, QueryResult,
    RecallMessageHandle, RetryResponseCodes, SendMessageContext, SendMessageHook, SendMessageType,
    SendResult, SendStatus, TopicPublishInfo, TopicPublishInfoEntryHandle,
    TopicPublishInfoEntrySetView, TopicPublishInfoKeySetView, TopicPublishInfoTable,
    TopicPublishInfoTableError, TopicPublishInfoValuesView, TransactionCheckListener,
    TransactionExecutor, TransactionListener, TransactionProducer, TransactionSendResult,
};
pub use rocketmq_classic_producer::{DEFAULT_PRODUCER_GROUP, TransactionCallbackError};
pub use rocketmq_classic_producer::{
    ReplyMessageError, RequestCallback, RequestFutureError, RequestFutureHolder,
    RequestFutureLatch, RequestFutureTable, RequestResponseCallback, RequestResponseFuture,
    create_reply_message,
};
pub use rocketmq_classic_producer::{TraceDispatcher, TraceDispatcherError, TraceProducerKind};
pub use rocketmq_classic_protocol::NamespaceUtil;
pub use rocketmq_classic_protocol::route::TopicRouteData;
pub use rocketmq_classic_remoting::client::{
    PrivateKeyDecryptionStrategy, TlsConfig, TlsMode, TransportMode, register_decryption_strategy,
};
