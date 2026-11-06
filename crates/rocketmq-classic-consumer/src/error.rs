use rocketmq_classic_core::{config::ConfigError, error::RpcError, runtime::RuntimeError};

use crate::{
    offset::OffsetStoreError,
    pop::{PopProcessError, PopReceiptError},
    pull::ProcessPullError,
    service_state::ConsumerServiceState,
};

#[derive(Debug, thiserror::Error)]
pub enum PullConsumerError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    Rpc(#[from] RpcError),
    #[error(transparent)]
    OffsetStore(#[from] OffsetStoreError),
    #[error(transparent)]
    PullProcessing(#[from] ProcessPullError),
    #[error(transparent)]
    AdminQuery(#[from] rocketmq_classic_core::api::admin::AdminQueryError),
    #[error(transparent)]
    Push(#[from] PushConsumerError),
    #[error("consumer group is blank or contains characters not accepted by RocketMQ")]
    InvalidConsumerGroup,
    #[error("consumer group must not be DEFAULT_CONSUMER")]
    DefaultConsumerGroup,
    #[error(
        "consumer timeout while suspended ({consumer_timeout_millis} ms) must be at least the broker suspend timeout ({broker_suspend_millis} ms)"
    )]
    InvalidSuspendTimeout {
        consumer_timeout_millis: i64,
        broker_suspend_millis: i64,
    },
    #[error("pull consumer has not been started")]
    NotStarted,
    #[error("pull consumer has already been started")]
    AlreadyStarted,
    #[error("pull consumer has been shut down")]
    Shutdown,
    #[error("consumer service state does not permit this operation: {0:?}")]
    ServiceStateNotReady(Option<ConsumerServiceState>),
    #[error("pull API wrapper is unavailable")]
    PullApiWrapperUnavailable,
    #[error("no initMode for Pull")]
    PullConsumeInitModeUnsupported,
    #[error("offset store must be configured before startup")]
    OffsetStoreAfterStartup,
    #[error("message queue must have a topic and broker name")]
    InvalidQueue,
    #[error("Can not find Message Queue for this topic, {0} Namesrv return empty")]
    EmptyMessageQueues(String),
    #[error("Unknow why, Can not find Message Queue for this topic, {0}")]
    NoPublishMessageQueues(String),
    #[error("Can not find Message Queue for this topic, {topic}")]
    PublishMessageQueuesFailed {
        topic: String,
        #[source]
        source: RuntimeError,
    },
    #[error("invalid topic for administrative creation: {0}")]
    InvalidAdminTopic(String),
    #[error("pull offset must be nonnegative")]
    InvalidOffset,
    #[error("max message count must be positive")]
    InvalidMaxMessageCount,
    #[error("max message bytes must be positive")]
    InvalidMaxMessageBytes,
    #[error("pull timeout must be nonnegative and representable")]
    InvalidTimeout,
    #[error("SQL92 pull expression must not be empty")]
    EmptySqlExpression,
    #[error("invalid pull subscription: {0}")]
    InvalidSubscription(String),
    #[error("Broker send-back and retry-topic fallback both failed")]
    SendMessageBackFailed,
    #[error("broker `{broker}` was not found in the current route")]
    BrokerNotFound { broker: String },
    #[error(
        "broker `{broker}` version {version} does not support message filtering by {expression_type}"
    )]
    UnsupportedExpressionBroker {
        broker: String,
        version: i32,
        expression_type: String,
    },
    #[error(
        "class-filter pull has no filter server for broker address `{broker_address}` and topic `{topic}`"
    )]
    FilterServerNotFound {
        broker_address: String,
        topic: String,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum LitePullConsumerError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Pull(#[from] PullConsumerError),
    #[error("Lite pull consumer has not been started")]
    NotStarted,
    #[error("Lite pull consumer has already been started")]
    AlreadyStarted,
    #[error("Lite pull consumer startup has already failed")]
    StartFailed,
    #[error("Lite pull consumer has been shut down")]
    Shutdown,
    #[error("subscription and explicit assignment are mutually exclusive")]
    SubscriptionModeConflict,
    #[error("message queues must not be empty")]
    EmptyAssignment,
    #[error("poll timeout must not be negative")]
    NegativePollTimeout,
    #[error("message queue is not currently assigned")]
    QueueNotAssigned,
    #[error("seek offset {offset} is outside [{minimum}, {maximum}]")]
    InvalidSeekOffset {
        offset: i64,
        minimum: i64,
        maximum: i64,
    },
    #[error("assignment tag expression must not be blank")]
    EmptyAssignmentExpression,
    #[error("assignment tag expression can only be set before startup")]
    AssignmentExpressionAfterStartup,
    #[error("Lite pull consumer queue generation counter is exhausted")]
    GenerationExhausted,
    #[error("consumeTimestamp must be a valid yyyyMMddHHmmss date/time")]
    InvalidConsumeTimestamp,
    #[error("Lite consumeFromWhere value {0:?} is not supported by Java 5.5.1")]
    InvalidConsumeFromWhere(rocketmq_classic_protocol::heartbeat::ConsumeFromWhere),
    #[error("pullThreadNums must not be negative")]
    InvalidPullThreadCount,
    #[error("topicMetadataCheckIntervalMillis must be positive")]
    InvalidTopicMetadataCheckInterval,
}

#[derive(Debug, thiserror::Error)]
pub enum PushConsumerError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    Rpc(#[from] RpcError),
    #[error(transparent)]
    AdminQuery(#[from] rocketmq_classic_core::api::admin::AdminQueryError),
    #[error(transparent)]
    OffsetStore(#[from] OffsetStoreError),
    #[error("push pull scheduler error: {0}")]
    PullScheduler(String),
    #[error("invalid orderly queue-lock settings: {0}")]
    OrderlyLockSettings(String),
    #[error("invalid pull idle-time setting: {0}")]
    PullIdleTimeSettings(String),
    #[error(transparent)]
    PullProcessing(#[from] ProcessPullError),
    #[error(transparent)]
    PopProcessing(#[from] PopProcessError),
    #[error(transparent)]
    PopReceipt(#[from] PopReceiptError),
    #[error("POP message is missing its receipt token")]
    MissingPopReceipt,
    #[error("POP receipt Broker `{0}` is not present in the current route")]
    PopBrokerNotFound(String),
    #[error("push consumer requires a message queue with a topic and broker name")]
    InvalidQueue,
    #[error("invalid topic for administrative creation: {0}")]
    InvalidAdminTopic(String),
    #[error("push pull offset must be nonnegative")]
    InvalidOffset,
    #[error("queue topic `{0}` has no registered subscription")]
    MissingSubscriptionForQueue(String),
    #[error("queue `{0}` is waiting for corrected offset persistence")]
    OffsetCorrectionPending(String),
    #[error("queue `{0}` is still registered while its prior assignment is being removed")]
    ProcessQueueRemovalPending(String),
    #[error("push consumer assignment generation counter is exhausted")]
    GenerationExhausted,
    #[error("consumer group is blank or contains characters not accepted by RocketMQ")]
    InvalidConsumerGroup,
    #[error("consumer group must not be DEFAULT_CONSUMER")]
    DefaultConsumerGroup,
    #[error("consumeTimestamp must be a valid yyyyMMddHHmmss date/time")]
    InvalidConsumeTimestamp,
    #[error("{name} value {value} is outside [{minimum}, {maximum}]")]
    InvalidSetting {
        name: &'static str,
        value: i64,
        minimum: i64,
        maximum: i64,
    },
    #[error("consumeThreadMin ({minimum}) is larger than consumeThreadMax ({maximum})")]
    InvalidThreadBounds { minimum: i32, maximum: i32 },
    #[error("invalid subscription: {0}")]
    InvalidSubscription(String),
    #[error("a concurrent or orderly message listener must be registered before startup")]
    MissingMessageListener,
    #[error("push consumer has already been started")]
    AlreadyStarted,
    #[error("push consumer has been shut down")]
    Shutdown,
    #[error("consumer service state does not permit this operation: {0:?}")]
    ServiceStateNotReady(Option<ConsumerServiceState>),
    #[error("offset store must be configured before startup")]
    OffsetStoreAfterStartup,
    #[error("push consumer has not been started")]
    NotStarted,
    #[error("push consumer consume-message service is unavailable")]
    ConsumeMessageServiceUnavailable,
    #[error("push consumer offset store is not available")]
    OffsetStoreUnavailable,
    #[error("operation requires the push consumer to be created or stopped")]
    ConfigurationAfterStartup,
    #[error("push message send-back and retry-topic fallback both failed")]
    SendMessageBackFailed,
    #[error("one or more orderly queue locks could not be confirmed released")]
    OrderlyUnlockFailed,
    #[cfg(test)]
    #[error("test injected startup failure at {0}")]
    TestStartupFault(&'static str),
}
