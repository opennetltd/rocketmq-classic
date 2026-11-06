mod admin;
mod client_check;
mod control;
mod hooks;
mod inbound;
#[path = "runtime.rs"]
mod instance;
mod manager;
mod pull;
mod rebalance;
mod registry;
mod request_future;
pub(super) mod service_thread;
pub mod stats;
mod tables;
mod tasks;

/// Java 5.5.1 `RebalanceService.getServiceName()` result.
pub const REBALANCE_SERVICE_NAME: &str = "RebalanceService";
/// Java 5.5.1 `PullMessageService.getServiceName()` result.
pub const PULL_MESSAGE_SERVICE_NAME: &str = "PullMessageService";
/// Java 5.5.1 `ServiceThread.getJoinTime()` result in milliseconds.
pub const SERVICE_THREAD_JOIN_TIME_MILLIS: u64 = 90_000;

pub use control::RuntimeState;
pub use instance::ClientRuntime;
pub use manager::{
    ClientRuntimeManager, FactoryEntryHandle, FactoryEntrySetView, FactoryKeySetView, FactoryTable,
    FactoryValuesView,
};
pub use pull::MessageRequest;
pub use registry::{
    BrokerOffsetTable, ConsumerOffsetTable, ConsumerOperation, ConsumerOperationFuture,
    ConsumerRuntime, GroupLease, PendingReplyCompletion, PendingReplyControl, PendingReplyRequest,
    ProducerRuntime, RequestFutureTable,
};
pub use request_future::{
    RequestFutureError, RequestFutureLatch, RequestResponseCallback, RequestResponseFuture,
    RequestSendFailureCause, RequestTimeoutCause,
};
pub use service_thread::RuntimeServiceThread;
pub use stats::ConsumerStatsManager;
pub use tables::{
    ConsumerEntryHandle, ConsumerEntrySetView, ConsumerKeySetView, ConsumerTable,
    ConsumerValuesView, ProducerEntryHandle, ProducerEntrySetView, ProducerKeySetView,
    ProducerTable, ProducerValuesView,
};
pub use tasks::{
    RuntimeScheduledCall, RuntimeScheduledTaskHandle, RuntimeScheduledTaskOutcome,
    RuntimeScheduledTaskWaitError, RuntimeScheduler,
};

/// Callback aliases used by nullable live runtime table adapters.
pub type TableKeyViewPredicate<'a> = dyn FnMut(&str) -> bool + 'a;
pub type TableValueViewPredicate<'a, T> = dyn FnMut(&T) -> bool + 'a;
pub type TableEntryViewPredicate<'a, T> = dyn FnMut(&str, &T) -> bool + 'a;
pub type TableComputeIfAbsent<'a, T> = dyn FnMut(&str) -> Option<T> + 'a;
pub type TableComputeIfPresent<'a, T> = dyn FnMut(&str, &T) -> Option<T> + 'a;
pub type TableCompute<'a, T> = dyn FnMut(&str, Option<&T>) -> Option<T> + 'a;
pub type TableMerge<'a, T> = dyn FnMut(&T, &T) -> Option<T> + 'a;
pub type TableForEach<'a, T> = dyn FnMut(&str, &T) + 'a;
pub type TableReplaceAll<'a, T> = dyn FnMut(&str, &T) -> T + 'a;
pub type TableReplaceAllOptionalResult<'a, T> = dyn FnMut(&str, &T) -> Option<T> + 'a;
/// Nullable key/value fields supplied as one candidate to a live table view.
pub type NullableTableEntry<T> = (Option<String>, Option<std::sync::Arc<T>>);

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("consumer operation failed: {0}")]
    ConsumerOperation(String),
    #[error(
        "Check client in broker error, maybe because you use {expression_type} to filter message, but server has not been upgraded to support!This error would not affect the launch of consumer, but may has impact on message receiving if you have use the new features which are not supported by server, please check the log!"
    )]
    ClientCheck {
        expression_type: String,
        #[source]
        source: Box<RuntimeError>,
    },
    #[error("NameServer addressing callback registration panicked")]
    AddressingCallbackFailed,
    #[error("heartbeat encoding failed: {0}")]
    HeartbeatEncoding(rocketmq_classic_protocol::heartbeat::HeartbeatFingerprintError),
    #[error("invalid heartbeat consumer data: {0}")]
    InvalidHeartbeatData(&'static str),
    #[error("periodic interval must be positive")]
    InvalidSchedule,
    #[error("cannot invoke an empty task set")]
    EmptyTaskSet,
    #[error("no invoked task returned a value")]
    NoSuccessfulTask,
    #[error("invoked tasks did not produce a value before the deadline")]
    InvocationTimeout,
    #[error("periodic callback panicked")]
    TaskCallbackFailed,
    #[error("runtime operation is invalid in state {0:?}")]
    InvalidState(RuntimeState),
    #[error("owned runtime control task failed")]
    ControlTaskFailed,
    #[error("inbound request executor failed: {0}")]
    InboundRequestExecutor(String),
    #[error("runtime startup failed")]
    #[cfg(test)]
    StartupFailure,
    #[error("client group already registered: {group}")]
    DuplicateGroup { group: String },
    #[error("request/reply correlation is already pending: {correlation_id}")]
    DuplicateCorrelation { correlation_id: String },
    #[error("request/reply admission did not become available before the request deadline")]
    RequestReplyAdmissionTimeout,
    #[error("invalid topic route: {0}")]
    InvalidRoute(&'static str),
    #[error("recursive update to a topic route table key during remapping")]
    RecursiveRouteTableMutation,
    #[error("invalid broker offset table: {0}")]
    InvalidOffsetTable(&'static str),
    #[error("VIP broker address has no port separator")]
    InvalidBrokerAddress,
    #[error("VIP broker port is not a Java signed integer")]
    InvalidBrokerPort,
    #[error("runtime RPC failed: {0}")]
    Rpc(#[from] crate::error::RpcError),
    #[error("runtime registry lock was poisoned")]
    RegistryPoisoned,
    #[error("runtime construction failed: {0}")]
    Transport(#[from] rocketmq_classic_remoting::TransportError),
}
