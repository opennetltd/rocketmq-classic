mod admin;
mod assignment;
mod processing;
mod query;
mod rebalance;
mod runtime;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{
        Arc, OnceLock, RwLock, Weak,
        atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, Ordering},
    },
    task::Poll,
    time::Duration,
};

#[cfg(test)]
use std::collections::VecDeque;

#[cfg(test)]
use rocketmq_classic_common::message::encode_nullable_property_entries;
use rocketmq_classic_common::{
    message::{
        DecodeOptions, Message, MessageExt, MessageHostAddress, MessageQueue, constants::*,
        decode_message_id,
    },
    sysflag::pull::{build_pull_sys_flag, clear_commit_offset_flag},
};
use rocketmq_classic_core::api::extensions::AckStatus;
use rocketmq_classic_core::api::{
    admin::{AdminQueryRequest, QueryResult},
    consumer::PullStatus,
};
use rocketmq_classic_core::route::broker_vip_channel;
use rocketmq_classic_core::runtime::{
    ClientRuntime, ClientRuntimeManager, ConsumerOperation, ConsumerRuntime, GroupLease,
    RuntimeError,
};
use rocketmq_classic_protocol::{
    SharedValue,
    body::{
        ConsumeMessageDirectlyResult, ConsumeMessageResult, ConsumerConsumeStatus,
        ConsumerPopProcessQueueInfo, ConsumerProcessQueueInfo, ConsumerRunningInfoBody,
        LockBatchRequestBody, MessageQueueAssignment, MessageRequestMode, QueueTimeSpan,
        UnlockBatchRequestBody,
    },
    codes::{request, response},
    header::{
        BoundaryType, ConsumerSendMsgBackRequestHeader, GetEarliestMsgStoretimeRequestHeader,
        GetMaxOffsetRequestHeader, GetMinOffsetRequestHeader, PopMessageRequestHeader,
        PullMessageRequestHeader, QueryConsumeTimeSpanRequestHeader, SearchOffsetRequestHeader,
        ViewMessageRequestHeader,
    },
    heartbeat::{
        ConsumeFromWhere, ConsumeType, ConsumerData, ConsumerSubscriptionDataSet, SubscriptionData,
    },
};
use rocketmq_classic_remoting::client::RpcHook;
use tokio::time::Instant;

#[cfg(test)]
use std::sync::Mutex;

use crate::{
    ConsumerStatsManager,
    admin::{first_query_message, unique_key_lookup_begin_timestamp},
    consume::{
        ConcurrentConsumeService, ConsumeMessageContext, ConsumeMessageHook,
        ConsumeMessageHookRegistry, ConsumeMessageService, ConsumeMessageServiceFuture,
        ConsumeRequest, ConsumeService, ConsumeSubmitError, ConsumeTraceHook,
        ORDERLY_LOCK_RETRY_DELAY_MILLIS, ORDERLY_LOCK_RPC_TIMEOUT_MILLIS, OrderlyConsumeRequest,
        OrderlyConsumeService, OrderlyDisposition, OrderlyLockSettings, OrderlySubmitError,
        orderly_disposition, suspend_delay_millis,
    },
    error::PushConsumerError,
    offset::{LocalFileOffsetStore, OffsetStore, OffsetStoreError, RemoteBrokerOffsetStore},
    offset::{OffsetRead, ReadOffsetType},
    pop::{PopProcessQueue, PopReceipt, PopStatus, process_pop_reply},
    process_queue::{
        ConsumeBatch, ExpiredMessageCleanupError, LockAttemptResult, OrderlyCompletion,
        ProcessQueue, ProcessQueueError,
    },
    pull::{
        FilterMessageContext, FilterMessageHook, PullAPIWrapper,
        build_class_filter_subscription_data, build_subscription, resolve_pull_broker_address,
    },
    rebalance::RebalanceService,
    service_state::ConsumerServiceState,
};

use super::{
    config::{PushConsumerConfig, parse_java_timestamp_millis},
    listener::{
        ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, ConsumeOrderlyContext,
        ConsumeOrderlyStatus, MessageListenerConcurrently, MessageListenerOrderly,
        MessageQueueListener, PushMessageListener,
    },
    pull_service::{
        EffectiveQueueThresholds, PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL,
        PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL, PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION,
        PULL_TIME_DELAY_MILLIS_WHEN_SUSPEND, PullRequest, PullRpcWorkers, PullScheduler,
        effective_queue_thresholds, flow_control_reason,
    },
};

type ConsumerFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

fn elapsed_millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn metric_count(count: usize) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

#[cfg(test)]
fn retry_wire_properties(
    retry: &Message,
) -> (BTreeMap<String, String>, Option<i32>, Option<i32>, String) {
    let mut properties = retry.properties().clone();
    let reconsume_times = properties
        .remove(PROPERTY_RECONSUME_TIME)
        .and_then(|value| value.parse::<i32>().ok());
    let max_reconsume_times = properties
        .remove(PROPERTY_MAX_RECONSUME_TIMES)
        .and_then(|value| value.parse::<i32>().ok());
    let mut property_entries = properties
        .iter()
        .map(|(key, value)| (Some(key.clone()), Some(value.clone())))
        .collect::<Vec<_>>();
    property_entries.extend(
        retry
            .nullable_property_entries()
            .filter(|(key, _)| key.is_none()),
    );
    let encoded_properties = encode_nullable_property_entries(Some(&property_entries));
    (
        properties,
        reconsume_times,
        max_reconsume_times,
        encoded_properties,
    )
}

#[derive(Clone, Copy)]
pub(crate) struct SendBackOptions<'a> {
    pub(crate) broker_name_override: Option<Option<&'a str>>,
    pub(crate) address_broker_name_override: Option<Option<&'a str>>,
    pub(crate) consumer_group_override: Option<&'a str>,
    pub(crate) max_reconsume_times_override: Option<i32>,
    pub(crate) timeout_millis: u64,
    pub(crate) orderly: bool,
}

#[cfg(test)]
fn retryable_inner_producer_send_error(error: &rocketmq_classic_core::error::RpcError) -> bool {
    match error {
        rocketmq_classic_core::error::RpcError::Response { code, .. } => matches!(
            *code,
            response::TOPIC_NOT_EXIST
                | response::SERVICE_NOT_AVAILABLE
                | response::SYSTEM_ERROR
                | response::SYSTEM_BUSY
                | response::NO_PERMISSION
                | response::NO_BUYER_ID
                | response::NOT_IN_CURRENT_UNIT
                | response::GO_AWAY
        ),
        _ => true,
    }
}

#[cfg(test)]
fn select_retry_publish_queue<'a>(
    queues: &'a [MessageQueue],
    start_index: u64,
    last_broker: Option<&str>,
) -> Option<&'a MessageQueue> {
    if queues.is_empty() {
        return None;
    }
    let queue_count = u64::try_from(queues.len()).ok()?;
    let start = usize::try_from(start_index % queue_count).ok()?;
    let selected = if let Some(last_broker) = last_broker {
        (0..queues.len())
            .map(|offset| &queues[(start + offset) % queues.len()])
            .find(|queue| queue.broker_name() != Some(last_broker))
    } else {
        None
    };
    selected.or_else(|| queues.get(start))
}

#[cfg(test)]
type ScriptedLockResult = Result<Option<HashSet<Option<MessageQueue>>>, String>;
#[cfg(test)]
type ScriptedLockWaiter = tokio::sync::oneshot::Receiver<ScriptedLockResult>;

#[cfg(test)]
#[derive(Default)]
pub(crate) struct ScriptedRetryTransport {
    pub(crate) broker_results: VecDeque<Result<(), String>>,
    pub(crate) retry_results: VecDeque<Result<(), String>>,
    pub(crate) retry_attempt_scripts: VecDeque<Vec<Result<(), String>>>,
    pub(crate) retry_attempt_counts: Vec<usize>,
    pub(crate) broker_headers: Vec<ConsumerSendMsgBackRequestHeader>,
    pub(crate) broker_targets: Vec<(Option<String>, Option<String>)>,
    pub(crate) broker_timeouts_millis: Vec<u64>,
    pub(crate) retry_messages: Vec<Message>,
    pub(crate) retry_wire_properties: Vec<String>,
}

#[cfg(test)]
#[derive(Default)]
struct ScriptedLockTransport {
    lock_results: VecDeque<ScriptedLockResult>,
    lock_waiters: VecDeque<ScriptedLockWaiter>,
    unlock_results: VecDeque<Result<(), String>>,
    lock_bodies: Vec<LockBatchRequestBody>,
    unlock_bodies: Vec<UnlockBatchRequestBody>,
    lock_requested: Arc<tokio::sync::Notify>,
}

#[derive(Default)]
struct OrderlyLockBatchResult {
    locked: HashSet<MessageQueue>,
    lock_times: HashMap<MessageQueue, (i64, Instant)>,
    responded: HashSet<MessageQueue>,
    uncertain: HashSet<MessageQueue>,
}

/// Java `POP` long-polling wait; the POP path keeps this value.
const BROKER_SUSPEND_MAX_TIME_MILLIS: i64 = 20_000;
/// Java simple-consumer `PULL_BROKER_SUSPEND_MAX_TIME_MILLIS`, used for the
/// Push Pull request's `suspendTimeoutMillis`.
const PULL_BROKER_SUSPEND_MAX_TIME_MILLIS: i64 = 15_000;
const CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND: i64 = 30_000;
const CONSUME_RETRY_DELAY_MILLIS: i64 = 5_000;
const CONSUMER_SEND_BACK_TIMEOUT_MILLIS: u64 = 5_000;
const INNER_PRODUCER_SEND_TIMEOUT_MILLIS: u64 = 3_000;
#[cfg(test)]
const INNER_PRODUCER_SYNC_ATTEMPTS: usize = 3;
const CONSUME_TIME_SPAN_TIMEOUT: Duration = Duration::from_millis(3_000);
const RESET_OFFSET_MAX_WAIT: Duration = Duration::from_secs(10);
const ORDERLY_RESET_REMOVE_LOCK_WAIT: Duration = Duration::from_millis(500);

struct PullContext {
    runtime: Arc<ClientRuntime>,
    offset_store: Arc<dyn OffsetStore>,
    scheduler: Arc<PullScheduler>,
    consume_service: ConsumeService,
}

struct PopConsumeBatchContext {
    process_queue: Arc<PopProcessQueue>,
    runtime: Arc<ClientRuntime>,
    queue: MessageQueue,
    route_topic: String,
    consumer_group: String,
    stats_consumer_group: String,
}

struct DesiredAssignment {
    topic: String,
    all_queues: Option<HashSet<MessageQueue>>,
    assigned_queues: HashSet<MessageQueue>,
    pull_queues: HashSet<MessageQueue>,
    pop_queues: HashSet<MessageQueue>,
}

struct BrokerAssignmentQueues {
    assigned: HashSet<MessageQueue>,
    pull: HashSet<MessageQueue>,
    pop: HashSet<MessageQueue>,
}

fn partition_broker_assignments(
    assignments: impl IntoIterator<Item = Option<MessageQueueAssignment>>,
) -> Option<BrokerAssignmentQueues> {
    let mut partition = BrokerAssignmentQueues {
        assigned: HashSet::new(),
        pull: HashSet::new(),
        pop: HashSet::new(),
    };
    for assignment in assignments {
        let assignment = assignment?;
        let Some(queue) = assignment.message_queue().map(|queue| queue.snapshot()) else {
            continue;
        };
        partition.assigned.insert(queue.clone());
        if assignment.mode() == Some(MessageRequestMode::Pop) {
            partition.pop.insert(queue);
        } else {
            // Java treats a missing mode the same as PULL during rebalance.
            partition.pull.insert(queue);
        }
    }
    Some(partition)
}

#[derive(Debug, PartialEq, Eq)]
enum InitialOffsetPlan {
    Stored(i64),
    Fixed(i64),
    MaxOffset,
    SearchTimestamp(i64),
}

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn await_pop_worker_handles(
    handles: &mut [tokio::task::JoinHandle<()>],
    completed: &mut [bool],
    wait: Duration,
) -> bool {
    let join_all = std::future::poll_fn(|context| {
        for (handle, completed) in handles.iter_mut().zip(completed.iter_mut()) {
            if !*completed && Pin::new(handle).poll(context).is_ready() {
                *completed = true;
            }
        }
        if completed.iter().all(|completed| *completed) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    });
    tokio::time::timeout(wait, join_all).await.is_ok()
}

async fn shutdown_pop_worker_handles(
    mut handles: Vec<tokio::task::JoinHandle<()>>,
    await_termination_millis: i64,
) {
    let wait = Duration::from_millis(await_termination_millis.max(0) as u64);
    let mut completed = vec![false; handles.len()];
    if await_pop_worker_handles(&mut handles, &mut completed, wait).await {
        return;
    }

    for (handle, completed) in handles.iter().zip(&completed) {
        if !completed {
            handle.abort();
        }
    }
    let _ = await_pop_worker_handles(&mut handles, &mut completed, wait).await;
}

fn consume_timeout_exceeded(started_at: Instant, timeout_minutes: i64) -> bool {
    let elapsed_millis = i64::try_from(started_at.elapsed().as_millis()).unwrap_or(i64::MAX);
    let timeout_millis = timeout_minutes.wrapping_mul(60).wrapping_mul(1_000);
    elapsed_millis >= timeout_millis
}

fn orderly_consume_return_type(
    callback_return_type: &'static str,
    status: Option<ConsumeOrderlyStatus>,
    timed_out: bool,
) -> &'static str {
    if timed_out && status.is_some() {
        "TIME_OUT"
    } else {
        callback_return_type
    }
}

struct StartupServices {
    consume_service: ConsumeService,
    orderly_lock_settings: Option<OrderlyLockSettings>,
    renew_orderly_locks: bool,
    pull_scheduler: Arc<PullScheduler>,
    pull_rpc_workers: Arc<PullRpcWorkers>,
    consumer: Weak<PushConsumer>,
}

#[derive(Clone)]
struct RunningInfoRuntimeSnapshot {
    nameserver_address: String,
}

impl RunningInfoRuntimeSnapshot {
    fn from_runtime(runtime: &ClientRuntime) -> Self {
        let mut nameserver_address = String::new();
        for address in runtime
            .remoting()
            .get_name_server_address_list()
            .unwrap_or_default()
        {
            nameserver_address.push_str(&address);
            nameserver_address.push(';');
        }
        Self { nameserver_address }
    }
}

struct PullAttemptError {
    delay_millis: i64,
    reason: String,
}

impl PullAttemptError {
    fn retry_with_delay(delay_millis: i64, reason: impl Into<String>) -> Self {
        Self {
            delay_millis,
            reason: reason.into(),
        }
    }

    fn from_rpc_with_delay(
        error: rocketmq_classic_core::error::RpcError,
        exception_delay_millis: i64,
    ) -> Self {
        let delay_millis = match &error {
            rocketmq_classic_core::error::RpcError::Response { code, .. }
                if *code == response::FLOW_CONTROL =>
            {
                PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL
            }
            _ => exception_delay_millis,
        };
        Self {
            delay_millis,
            reason: error.to_string(),
        }
    }
}

enum State {
    Created,
    Started {
        runtime: Weak<ClientRuntime>,
        _lease: GroupLease,
        offset_store: Arc<dyn OffsetStore>,
        consume_service: ConsumeService,
        orderly_lock_settings: Option<OrderlyLockSettings>,
        pull_scheduler: Arc<PullScheduler>,
        pull_rpc_workers: Arc<PullRpcWorkers>,
    },
    Shutdown,
}

#[derive(Clone)]
enum StatsManagerBinding {
    Unbound,
    Runtime(Arc<ConsumerStatsManager>),
    Null,
}

#[derive(Clone)]
enum ConsumeMessageServiceBinding {
    Unbound,
    Builtin(Arc<dyn ConsumeMessageService>),
    Custom(Arc<dyn ConsumeMessageService>),
    Null,
}

struct BuiltinConsumeMessageService {
    owner: Weak<PushConsumer>,
    service: ConsumeService,
    orderly_lock_settings: Option<OrderlyLockSettings>,
}

impl ConsumeMessageService for BuiltinConsumeMessageService {
    fn start(&self) -> ConsumeMessageServiceFuture<'_, ()> {
        let owner = self.owner.clone();
        let service = self.service.clone();
        let orderly_lock_settings = self.orderly_lock_settings;
        Box::pin(async move {
            if let Some(owner) = owner.upgrade() {
                let renew_orderly_locks = orderly_lock_settings.is_some()
                    && owner.config.message_model()
                        == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering;
                service
                    .start(
                        Arc::downgrade(&owner),
                        orderly_lock_settings,
                        renew_orderly_locks,
                    )
                    .await;
            }
        })
    }

    fn shutdown(&self, await_termination_millis: i64) -> ConsumeMessageServiceFuture<'_, ()> {
        let service = self.service.clone();
        Box::pin(async move {
            service.shutdown(await_termination_millis).await;
        })
    }

    fn update_core_pool_size(&self, core_pool_size: i32) {
        if self.service.update_core_pool_size(core_pool_size) {
            if let Some(owner) = self.owner.upgrade() {
                owner
                    .consume_core_pool_size
                    .store(core_pool_size, Ordering::Release);
            }
        }
    }

    fn inc_core_pool_size(&self) {}

    fn dec_core_pool_size(&self) {}

    fn core_pool_size(&self) -> i32 {
        self.owner.upgrade().map_or(0, |owner| {
            owner.consume_core_pool_size.load(Ordering::Acquire)
        })
    }

    fn consume_message_directly(
        &self,
        message: MessageExt,
        broker_name: Option<String>,
    ) -> ConsumeMessageServiceFuture<'_, ConsumeMessageDirectlyResult> {
        let owner = self.owner.clone();
        let service = self.service.clone();
        Box::pin(async move {
            match owner.upgrade() {
                Some(owner) => owner
                    .consume_message_directly_with_service(message, broker_name, service)
                    .await
                    .unwrap_or_default(),
                None => ConsumeMessageDirectlyResult::default(),
            }
        })
    }

    fn submit_consume_request(
        &self,
        messages: Vec<Arc<MessageExt>>,
        process_queue: Arc<ProcessQueue>,
        _message_queue: MessageQueue,
        dispatch_to_consume: bool,
    ) {
        if !dispatch_to_consume && matches!(&self.service, ConsumeService::Orderly(_)) {
            return;
        }
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let message_snapshots = messages
            .iter()
            .map(|message| message.as_ref().clone())
            .collect::<Vec<_>>();
        if let Err(error) = process_queue.put_message(&message_snapshots) {
            tracing::error!(%error, "failed to add consumed messages to process queue");
            return;
        }
        let process_queue_generation = process_queue.generation();
        tokio::spawn(async move {
            if let Some(context) = owner.pull_context().await {
                owner.submit_ready_messages(&context, process_queue, process_queue_generation);
            }
        });
    }

    fn submit_pop_consume_request(
        &self,
        _messages: Vec<MessageExt>,
        _process_queue: Arc<PopProcessQueue>,
        _message_queue: MessageQueue,
    ) {
        panic!("the primary consume-message service does not accept POP requests");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupStage {
    RuntimeCreated,
    OffsetStoreLoaded,
    ConsumeServiceStarted,
    PullSchedulerStarted,
    PullRpcWorkersStarted,
    ConsumerRegistered,
    RuntimeStarted,
    RoutesUpdated,
    BrokerChecked,
    HeartbeatSent,
}

impl StartupStage {
    #[cfg(test)]
    const fn name(self) -> &'static str {
        match self {
            Self::RuntimeCreated => "runtime-created",
            Self::OffsetStoreLoaded => "offset-store-loaded",
            Self::ConsumeServiceStarted => "consume-service-started",
            Self::PullSchedulerStarted => "pull-scheduler-started",
            Self::PullRpcWorkersStarted => "pull-rpc-workers-started",
            Self::ConsumerRegistered => "consumer-registered",
            Self::RuntimeStarted => "runtime-started",
            Self::RoutesUpdated => "routes-updated",
            Self::BrokerChecked => "broker-checked",
            Self::HeartbeatSent => "heartbeat-sent",
        }
    }

    #[cfg(test)]
    const fn all() -> [Self; 10] {
        [
            Self::RuntimeCreated,
            Self::OffsetStoreLoaded,
            Self::ConsumeServiceStarted,
            Self::PullSchedulerStarted,
            Self::PullRpcWorkersStarted,
            Self::ConsumerRegistered,
            Self::RuntimeStarted,
            Self::RoutesUpdated,
            Self::BrokerChecked,
            Self::HeartbeatSent,
        ]
    }
}

/// Java-compatible Push startup, assignment lifecycle, listener modes, and POP flow.
///
/// Java's nullable consumer-group setter is represented by
/// `set_consumer_group(Option<String>)`.
///
/// ```compile_fail
/// use rocketmq_classic_consumer::PushConsumer;
///
/// fn removed_nullable_alias(consumer: &PushConsumer) {
///     consumer.set_consumer_group_nullable(None);
/// }
/// ```
pub struct PushConsumer {
    config: PushConsumerConfig,
    manager: ClientRuntimeManager,
    rpc_hook: Option<Arc<dyn RpcHook>>,
    consume_message_hooks: ConsumeMessageHookRegistry,
    runtime_stats: RwLock<StatsManagerBinding>,
    consume_message_service: RwLock<ConsumeMessageServiceBinding>,
    consume_stats_group: RwLock<Option<String>>,
    trace_dispatcher: OnceLock<Arc<rocketmq_classic_producer::TraceDispatcher>>,
    inner_producer: RwLock<
        Option<(
            Weak<ClientRuntime>,
            Arc<rocketmq_classic_producer::Producer>,
        )>,
    >,
    state: tokio::sync::Mutex<State>,
    implementation_client_factory: std::sync::Mutex<Option<Option<Arc<ClientRuntime>>>>,
    rebalance_client_factory: std::sync::Mutex<Option<Option<Arc<ClientRuntime>>>>,
    owner: std::sync::Mutex<Weak<Self>>,
    service_state: RwLock<Option<ConsumerServiceState>>,
    running_info_runtime_snapshot: RwLock<Option<RunningInfoRuntimeSnapshot>>,
    offset_store: std::sync::Mutex<Option<Arc<dyn OffsetStore>>>,
    listener: RwLock<Option<PushMessageListener>>,
    pop_listener: RwLock<Option<PushMessageListener>>,
    subscription_config: RwLock<HashMap<String, String>>,
    subscriptions: Arc<RwLock<HashMap<String, SubscriptionData>>>,
    topic_queues: Arc<RwLock<HashMap<String, BTreeSet<MessageQueue>>>>,
    process_queues: Arc<RwLock<HashMap<MessageQueue, Arc<ProcessQueue>>>>,
    pending_orderly_unlocks: Arc<RwLock<HashSet<MessageQueue>>>,
    rebalance_service: RebalanceService,
    offset_corrections_pending: Arc<RwLock<HashSet<MessageQueue>>>,
    pop_process_queues: Arc<RwLock<HashMap<MessageQueue, Arc<PopProcessQueue>>>>,
    pop_workers: std::sync::Mutex<HashMap<MessageQueue, tokio::task::JoinHandle<()>>>,
    pop_retired_workers: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pop_consume_workers: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pop_consume_semaphore: Arc<tokio::sync::Semaphore>,
    next_generation: AtomicU64,
    next_retry_message_id: AtomicU64,
    pull_api: PullAPIWrapper,
    paused: AtomicBool,
    pull_time_delay_millis_when_exception: AtomicI64,
    pull_max_idle_time_millis: AtomicI64,
    pop_pull_max_idle_time_millis: AtomicI64,
    consume_orderly: AtomicBool,
    consumer_start_timestamp: AtomicI64,
    consume_core_pool_size: AtomicI32,
    consume_service_initialized: AtomicBool,
    consume_service_max_threads: AtomicI32,
    #[cfg(test)]
    startup_failure: Mutex<Option<StartupStage>>,
    #[cfg(test)]
    pub(crate) scripted_retry_transport: Mutex<ScriptedRetryTransport>,
    #[cfg(test)]
    scripted_lock_transport: Mutex<ScriptedLockTransport>,
    #[cfg(test)]
    rebalance_membership: Mutex<HashMap<String, VecDeque<Option<Vec<String>>>>>,
}

impl PushConsumer {
    pub fn new(config: PushConsumerConfig) -> Self {
        Self::with_manager(config, ClientRuntimeManager::shared_default())
    }

    fn with_manager(config: PushConsumerConfig, manager: ClientRuntimeManager) -> Self {
        Self::with_manager_and_hook(config, manager, None)
    }

    /// Creates a push consumer whose remoting requests use the supplied RPC hook.
    pub fn new_with_rpc_hook(
        config: PushConsumerConfig,
        rpc_hook: Option<Arc<dyn RpcHook>>,
    ) -> Self {
        Self::with_manager_and_hook(config, ClientRuntimeManager::shared_default(), rpc_hook)
    }

    fn with_manager_and_hook(
        config: PushConsumerConfig,
        manager: ClientRuntimeManager,
        rpc_hook: Option<Arc<dyn RpcHook>>,
    ) -> Self {
        let consume_service_max_threads = config.consume_thread_max();
        let pop_consume_limit = usize::try_from(consume_service_max_threads.max(1)).unwrap_or(1);
        let consume_core_pool_size = config.consume_thread_min();
        let pull_time_delay_millis_when_exception = config
            .client_config()
            .pull_time_delay_millis_when_exception();
        let pull_api = PullAPIWrapper::new(config.unit_mode());
        let rebalance_service = RebalanceService::new(
            config.allocate_message_queue_strategy(),
            config.message_model(),
        );
        Self {
            config,
            manager,
            rpc_hook,
            consume_message_hooks: ConsumeMessageHookRegistry::default(),
            runtime_stats: RwLock::new(StatsManagerBinding::Unbound),
            consume_message_service: RwLock::new(ConsumeMessageServiceBinding::Unbound),
            consume_stats_group: RwLock::new(None),
            trace_dispatcher: OnceLock::new(),
            inner_producer: RwLock::new(None),
            state: tokio::sync::Mutex::new(State::Created),
            implementation_client_factory: std::sync::Mutex::new(None),
            rebalance_client_factory: std::sync::Mutex::new(None),
            owner: std::sync::Mutex::new(Weak::new()),
            service_state: RwLock::new(Some(ConsumerServiceState::CreateJust)),
            running_info_runtime_snapshot: RwLock::new(None),
            offset_store: std::sync::Mutex::new(None),
            listener: RwLock::new(None),
            pop_listener: RwLock::new(None),
            subscription_config: RwLock::new(HashMap::new()),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            topic_queues: Arc::new(RwLock::new(HashMap::new())),
            process_queues: Arc::new(RwLock::new(HashMap::new())),
            pending_orderly_unlocks: Arc::new(RwLock::new(HashSet::new())),
            rebalance_service,
            offset_corrections_pending: Arc::new(RwLock::new(HashSet::new())),
            pop_process_queues: Arc::new(RwLock::new(HashMap::new())),
            pop_workers: std::sync::Mutex::new(HashMap::new()),
            pop_retired_workers: std::sync::Mutex::new(Vec::new()),
            pop_consume_workers: std::sync::Mutex::new(Vec::new()),
            pop_consume_semaphore: Arc::new(tokio::sync::Semaphore::new(pop_consume_limit)),
            next_generation: AtomicU64::new(1),
            next_retry_message_id: AtomicU64::new(0),
            pull_api,
            paused: AtomicBool::new(false),
            pull_time_delay_millis_when_exception: AtomicI64::new(
                pull_time_delay_millis_when_exception,
            ),
            pull_max_idle_time_millis: AtomicI64::new(
                crate::process_queue::PULL_MAX_IDLE_TIME_MILLIS,
            ),
            pop_pull_max_idle_time_millis: AtomicI64::new(
                crate::process_queue::PULL_MAX_IDLE_TIME_MILLIS,
            ),
            consume_orderly: AtomicBool::new(false),
            consumer_start_timestamp: AtomicI64::new(0),
            consume_core_pool_size: AtomicI32::new(consume_core_pool_size),
            consume_service_initialized: AtomicBool::new(false),
            consume_service_max_threads: AtomicI32::new(consume_service_max_threads.max(1)),
            #[cfg(test)]
            startup_failure: Mutex::new(None),
            #[cfg(test)]
            scripted_retry_transport: Mutex::new(ScriptedRetryTransport::default()),
            #[cfg(test)]
            scripted_lock_transport: Mutex::new(ScriptedLockTransport::default()),
            #[cfg(test)]
            rebalance_membership: Mutex::new(HashMap::new()),
        }
    }

    pub fn config(&self) -> &PushConsumerConfig {
        &self.config
    }

    /// Returns the current Java-compatible facade consumer group, including
    /// the nullable state accepted by `DefaultMQPushConsumer`.
    pub fn get_consumer_group(&self) -> Option<String> {
        self.config.consumer_group()
    }

    /// Updates the live facade consumer group.
    /// `None` represents the null value accepted by Java's
    /// `DefaultMQPushConsumer.setConsumerGroup(String)`.
    pub fn set_consumer_group(&self, value: Option<String>) {
        self.config.set_consumer_group(value);
    }

    fn consume_stats_group(&self) -> String {
        read_lock(&self.consume_stats_group)
            .clone()
            .unwrap_or_else(|| self.config.consumer_group().unwrap_or_default())
    }

    fn stats_manager(&self) -> Option<Arc<ConsumerStatsManager>> {
        match &*read_lock(&self.runtime_stats) {
            StatsManagerBinding::Unbound => None,
            StatsManagerBinding::Runtime(manager) => Some(manager.clone()),
            StatsManagerBinding::Null => {
                panic!("consumer stats manager is unavailable without a client runtime")
            }
        }
    }

    fn bind_stats_manager(&self, manager: Arc<ConsumerStatsManager>) {
        *write_lock(&self.runtime_stats) = StatsManagerBinding::Runtime(manager);
    }

    /// Returns the runtime-shared manager corresponding to Java's
    /// `DefaultMQPushConsumerImpl.getConsumerStatsManager()`.
    ///
    /// The manager is unavailable before a client runtime has been created.
    pub fn get_consumer_stats_manager(&self) -> Option<Arc<ConsumerStatsManager>> {
        match &*read_lock(&self.runtime_stats) {
            StatsManagerBinding::Runtime(manager) => Some(manager.clone()),
            StatsManagerBinding::Unbound | StatsManagerBinding::Null => None,
        }
    }

    /// Returns the Java implementation's current consume-message service.
    pub fn get_consume_message_service(&self) -> Option<Arc<dyn ConsumeMessageService>> {
        match read_lock(&self.consume_message_service).clone() {
            ConsumeMessageServiceBinding::Builtin(service)
            | ConsumeMessageServiceBinding::Custom(service) => Some(service),
            ConsumeMessageServiceBinding::Unbound | ConsumeMessageServiceBinding::Null => None,
        }
    }

    /// Replaces the consume-message service, matching Java's direct nullable
    /// implementation-field setter. A custom service receives subsequently
    /// submitted normal Pull work and direct-consume calls; POP has its own
    /// service in the Java implementation.
    pub fn set_consume_message_service(&self, service: Option<Arc<dyn ConsumeMessageService>>) {
        let binding = service.map_or(ConsumeMessageServiceBinding::Null, |service| {
            ConsumeMessageServiceBinding::Custom(service)
        });
        *write_lock(&self.consume_message_service) = binding;
    }

    fn custom_consume_message_service(&self) -> Option<Arc<dyn ConsumeMessageService>> {
        match read_lock(&self.consume_message_service).clone() {
            ConsumeMessageServiceBinding::Custom(service) => Some(service),
            ConsumeMessageServiceBinding::Unbound
            | ConsumeMessageServiceBinding::Builtin(_)
            | ConsumeMessageServiceBinding::Null => None,
        }
    }

    fn consume_message_service_is_null(&self) -> bool {
        matches!(
            &*read_lock(&self.consume_message_service),
            ConsumeMessageServiceBinding::Null
        )
    }

    fn bind_builtin_consume_message_service(&self, service: Arc<dyn ConsumeMessageService>) {
        *write_lock(&self.consume_message_service) = ConsumeMessageServiceBinding::Builtin(service);
    }

    /// Returns the retained implementation handle exposed by Java's
    /// deprecated `DefaultMQPushConsumer.getDefaultMQPushConsumerImpl()`.
    /// `PushConsumer` owns both the facade behavior and implementation state,
    /// so the Rust handle is a clone of this same `Arc`.
    pub fn get_default_mq_push_consumer_impl(self: &Arc<Self>) -> Arc<Self> {
        self.clone()
    }

    /// Returns the rebalance compatibility handle. Assignment state and
    /// rebalance behavior live on this owner in Rust, so it aliases `self`.
    pub fn get_rebalance_impl(&self) -> &Self {
        self
    }

    /// Returns the runtime stored by the Push implementation, matching
    /// `DefaultMQPushConsumerImpl.getmQClientFactory`.
    pub async fn get_implementation_client_runtime(&self) -> Option<Arc<ClientRuntime>> {
        if let Some(runtime) = self
            .implementation_client_factory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            return runtime;
        }
        match &*self.state.lock().await {
            State::Started { runtime, .. } => runtime.upgrade(),
            State::Created | State::Shutdown => None,
        }
    }

    /// Replaces the Push implementation's runtime reference, matching
    /// `DefaultMQPushConsumerImpl.setmQClientFactory`. Registration and
    /// lifecycle ownership remain with the runtime manager used by `start`.
    pub fn set_implementation_client_runtime(&self, runtime: Option<Arc<ClientRuntime>>) {
        let stats_binding = runtime
            .as_ref()
            .map_or(StatsManagerBinding::Null, |runtime| {
                StatsManagerBinding::Runtime(runtime.consumer_stats_manager())
            });
        *self
            .implementation_client_factory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(runtime);
        *write_lock(&self.runtime_stats) = stats_binding;
    }

    /// Creates the public queue type returned by Java's
    /// `RebalancePushImpl.createPopProcessQueue()`.
    pub fn create_pop_process_queue(&self) -> PopProcessQueue {
        PopProcessQueue::new()
    }

    /// Creates the public process queue returned by Java rebalance.
    pub fn create_process_queue(&self) -> ProcessQueue {
        ProcessQueue::new()
    }

    /// Matches `RebalancePushImpl.clientRebalance(topic)` for the current
    /// consumer settings. Push uses broker assignments only when client
    /// rebalance is disabled and consumption is neither orderly nor broadcast.
    pub fn client_rebalance(&self, _topic: &str) -> bool {
        self.config.client_rebalance()
            || self.consume_orderly.load(Ordering::Acquire)
            || self.config.message_model()
                == rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting
    }

    pub fn get_allocate_message_queue_strategy(
        &self,
    ) -> Arc<dyn crate::rebalance::AllocateMessageQueueStrategy> {
        self.config.allocate_message_queue_strategy()
    }

    pub fn set_allocate_message_queue_strategy(
        &self,
        strategy: Arc<dyn crate::rebalance::AllocateMessageQueueStrategy>,
    ) {
        self.config
            .set_allocate_message_queue_strategy(strategy.clone());
        self.rebalance_service.set_strategy(strategy);
    }

    pub fn set_message_model(
        &self,
        message_model: rocketmq_classic_protocol::heartbeat::MessageModel,
    ) {
        self.config.set_message_model(message_model);
        self.rebalance_service.set_message_model(message_model);
    }

    /// Returns Java `RebalancePushImpl.getConsumeInitMode()` (`MIN = 0`,
    /// `MAX = 1`) for the configured consume origin.
    pub fn consume_init_mode(&self) -> i32 {
        if self.config.consume_from_where() == ConsumeFromWhere::ConsumeFromFirstOffset {
            0
        } else {
            1
        }
    }

    /// Java's deprecated helper logs offset lookup failures and returns `-1`.
    pub async fn compute_pull_from_where(&self, queue: &MessageQueue) -> i64 {
        self.compute_pull_from_where_with_exception(queue)
            .await
            .unwrap_or(-1)
    }

    /// Computes a queue's initial offset with the exception behavior used by
    /// Java's `RebalancePushImpl.computePullFromWhereWithException`.
    pub async fn compute_pull_from_where_with_exception(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<i64, PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let queue = self
            .config
            .client_config()
            .clone()
            .queue_with_namespace(input_queue)?;
        self.compute_initial_offset(&context.runtime, &context.offset_store, &queue)
            .await
    }

    /// Removes any retained offset before a new queue assignment is activated.
    pub async fn remove_dirty_offset(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<(), PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let queue = self
            .config
            .client_config()
            .clone()
            .queue_with_namespace(input_queue)?;
        context.offset_store.remove_offset(&queue);
        Ok(())
    }

    /// Java's base implementation accepts POP queue removal without additional
    /// cleanup; Push workers own the actual POP assignment lifecycle.
    pub const fn remove_unnecessary_pop_process_queue(
        &self,
        _queue: &MessageQueue,
        _process_queue: &PopProcessQueue,
    ) -> bool {
        true
    }

    fn active_pull_queue_thresholds(&self) -> EffectiveQueueThresholds {
        EffectiveQueueThresholds {
            message_count: self.config.pull_threshold_for_queue(),
            size_mib: self.config.pull_threshold_size_for_queue_mib(),
            max_span: self.config.consume_concurrently_max_span(),
        }
    }

    fn update_pull_thresholds_after_rebalance(&self) {
        let process_queue_count = read_lock(&self.process_queues).len();
        if process_queue_count == 0 {
            // Java leaves the last per-queue value in place when its process
            // queue table becomes empty.
            return;
        }
        let thresholds = effective_queue_thresholds(&self.config, process_queue_count);
        self.config
            .set_pull_threshold_for_queue(thresholds.message_count);
        self.config
            .set_pull_threshold_size_for_queue_mib(thresholds.size_mib);
    }

    /// Returns Java's `DefaultMQPushConsumerImpl.getServiceState()` value.
    pub fn service_state(&self) -> Option<ConsumerServiceState> {
        *read_lock(&self.service_state)
    }

    /// Mirrors the deprecated Java implementation setter, including null.
    pub fn set_service_state(&self, service_state: Option<ConsumerServiceState>) {
        *write_lock(&self.service_state) = service_state;
    }

    /// Returns Java's effective implementation retry limit (`-1` becomes 16).
    pub fn get_max_reconsume_times(&self) -> i32 {
        self.config.effective_max_reconsume_times()
    }

    /// Returns the current Java-compatible pull/POP exception retry delay.
    pub fn pull_time_delay_millis_when_exception(&self) -> i64 {
        self.pull_time_delay_millis_when_exception
            .load(Ordering::Acquire)
    }

    /// Mirrors Java's implementation setter; the value is read by subsequent
    /// pull and POP failure paths without restarting the consumer.
    pub fn set_pull_time_delay_millis_when_exception(&self, delay_millis: i64) {
        self.pull_time_delay_millis_when_exception
            .store(delay_millis, Ordering::Release);
    }

    fn pull_retry_error(&self, reason: impl Into<String>) -> PullAttemptError {
        PullAttemptError::retry_with_delay(self.pull_time_delay_millis_when_exception(), reason)
    }

    fn pull_retry_error_from_rpc(
        &self,
        error: rocketmq_classic_core::error::RpcError,
    ) -> PullAttemptError {
        PullAttemptError::from_rpc_with_delay(error, self.pull_time_delay_millis_when_exception())
    }

    pub fn group_name(&self) -> Option<String> {
        self.config.consumer_group()
    }

    pub fn message_model(&self) -> rocketmq_classic_protocol::heartbeat::MessageModel {
        self.config.message_model()
    }

    pub fn consume_type(&self) -> ConsumeType {
        ConsumeType::ConsumePassively
    }

    pub fn consume_from_where(&self) -> ConsumeFromWhere {
        self.config.consume_from_where()
    }

    pub fn is_unit_mode(&self) -> bool {
        self.config.unit_mode()
    }

    /// Returns the queue-assignment listener configured on the Push consumer.
    pub fn get_message_queue_listener(&self) -> Option<MessageQueueListener> {
        self.config.message_queue_listener()
    }

    /// Returns the message trace dispatcher after trace initialization.
    ///
    /// This corresponds to Java `DefaultMQPushConsumer.getTraceDispatcher()`.
    pub fn trace_dispatcher(&self) -> Option<Arc<rocketmq_classic_producer::TraceDispatcher>> {
        self.trace_dispatcher.get().cloned()
    }

    pub fn get_offset_store(&self) -> Option<Arc<dyn OffsetStore>> {
        self.offset_store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Returns the factory currently stored by the rebalance implementation.
    pub async fn get_client_runtime(&self) -> Option<Arc<ClientRuntime>> {
        if let Some(runtime) = self
            .rebalance_client_factory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            return runtime;
        }
        match &*self.state.lock().await {
            State::Started { runtime, .. } => runtime.upgrade(),
            State::Created | State::Shutdown => None,
        }
    }

    /// Replaces the runtime held by the rebalance implementation, matching
    /// `RebalanceImpl.setmQClientFactory`. The consumer's own runtime and
    /// registrations remain managed by `ClientRuntimeManager`.
    pub fn set_client_runtime(&self, runtime: Option<Arc<ClientRuntime>>) {
        *self
            .rebalance_client_factory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(runtime);
    }

    pub async fn set_offset_store(
        &self,
        store: Option<Arc<dyn OffsetStore>>,
    ) -> Result<(), PushConsumerError> {
        // Java's DefaultMQPushConsumerImpl.setOffsetStore directly replaces
        // the field and does not guard on service lifecycle state.
        let _state = self.state.lock().await;
        *self
            .offset_store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = store;
        Ok(())
    }

    /// Updates the selected queue offset without applying the increase-only
    /// rule used by pull completion, matching Java's raw implementation API.
    pub fn update_consume_offset(
        &self,
        queue: &MessageQueue,
        offset: i64,
    ) -> Result<(), PushConsumerError> {
        let store = self
            .get_offset_store()
            .ok_or(PushConsumerError::OffsetStoreUnavailable)?;
        store.update_offset(queue, offset, false);
        Ok(())
    }

    /// Persists offsets for currently assigned pull queues, matching
    /// `DefaultMQPushConsumerImpl.persistConsumerOffset`.
    pub async fn persist_consumer_offset(&self) -> Result<(), PushConsumerError> {
        let service_state = self.service_state();
        if service_state != Some(ConsumerServiceState::Running) {
            return Err(PushConsumerError::ServiceStateNotReady(service_state));
        }
        let store = self
            .get_offset_store()
            .ok_or(PushConsumerError::OffsetStoreUnavailable)?;
        let queues = read_lock(&self.process_queues)
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        store.persist_all(&queues).await?;
        Ok(())
    }

    pub async fn register_message_listener(
        &self,
        listener: PushMessageListener,
    ) -> Result<(), PushConsumerError> {
        // Java updates the facade and implementation listener references even
        // after startup; already-created consume workers retain their listener.
        *write_lock(&self.listener) = Some(listener);
        Ok(())
    }

    /// Registers a Java-compatible hook around each delivered consume batch.
    pub fn register_consume_message_hook(&self, hook: Arc<dyn ConsumeMessageHook>) {
        self.consume_message_hooks.register(hook);
    }

    /// Registers a Java-compatible hook for decoded Pull and POP messages.
    pub fn register_filter_message_hook(&self, hook: Arc<dyn FilterMessageHook>) {
        self.pull_api.register_filter_message_hook(hook);
    }

    /// Reports whether Java's Push implementation has a consume hook.
    pub fn has_hook(&self) -> bool {
        !self.consume_message_hooks.is_empty()
    }

    /// Runs the registered Push consume hooks before the listener callback.
    pub fn execute_hook_before(&self, context: &mut ConsumeMessageContext) {
        self.consume_message_hooks.execute_before(context);
    }

    /// Runs the registered Push consume hooks after the listener callback.
    pub fn execute_hook_after(&self, context: &mut ConsumeMessageContext) {
        self.consume_message_hooks.execute_after(context);
    }

    fn new_consume_message_context(
        &self,
        messages: Vec<MessageExt>,
        queue: MessageQueue,
    ) -> ConsumeMessageContext {
        let mut client_config = self.config.client_config().clone();
        let namespace = client_config
            .get_namespace()
            .expect("the consumer namespace was validated before consume workers start")
            .map(str::to_owned);
        let consumer_group = self.config.consumer_group();
        ConsumeMessageContext::new(consumer_group, Some(messages), queue, namespace)
            .with_properties(HashMap::new())
    }

    /// Sets the configured message listener. This mirrors Java's
    /// `DefaultMQPushConsumer.setMessageListener` entry point.
    pub async fn set_message_listener(
        &self,
        listener: PushMessageListener,
    ) -> Result<(), PushConsumerError> {
        self.register_message_listener(listener).await
    }

    pub fn get_message_listener(&self) -> Option<PushMessageListener> {
        read_lock(&self.listener).clone()
    }

    pub async fn subscribe(
        &self,
        topic: &str,
        sub_expression: &str,
    ) -> Result<(), PushConsumerError> {
        self.subscribe_with_expression_type(topic, Some("TAG"), sub_expression)
            .await
    }

    pub async fn subscribe_with_expression_type(
        &self,
        topic: &str,
        expression_type: Option<&str>,
        expression: &str,
    ) -> Result<(), PushConsumerError> {
        let runtime = {
            let state = self.state.lock().await;
            match &*state {
                State::Created => None,
                State::Started { runtime, .. } => {
                    Some(runtime.upgrade().ok_or(PushConsumerError::NotStarted)?)
                }
                State::Shutdown => return Err(PushConsumerError::Shutdown),
            }
        };
        let mut client_config = self.config.client_config().clone();
        let topic = client_config.with_namespace(Some(topic))?.ok_or(
            PushConsumerError::InvalidSubscription("topic is empty".to_owned()),
        )?;
        let subscription = build_subscription(&topic, Some(expression), expression_type)
            .map_err(|error| PushConsumerError::InvalidSubscription(error.to_string()))?;
        write_lock(&self.subscriptions).insert(topic, subscription);
        if let Some(runtime) = runtime {
            // Java updates its local subscription table, then makes one
            // best-effort heartbeat attempt. Subscription does not trigger a
            // rebalance, and heartbeat failures are contained by Java.
            let _ = runtime.send_heartbeat_to_all_brokers().await;
        }
        Ok(())
    }

    /// Subscribes using Java's class-filter overload. The class and source stay
    /// on the local `SubscriptionData`; the source is omitted from heartbeat JSON.
    pub async fn subscribe_with_class_filter(
        &self,
        topic: &str,
        full_class_name: &str,
        filter_class_source: &str,
    ) -> Result<(), PushConsumerError> {
        let runtime = {
            let state = self.state.lock().await;
            match &*state {
                State::Created => None,
                State::Started { runtime, .. } => {
                    Some(runtime.upgrade().ok_or(PushConsumerError::NotStarted)?)
                }
                State::Shutdown => return Err(PushConsumerError::Shutdown),
            }
        };
        let mut client_config = self.config.client_config().clone();
        let topic = client_config.with_namespace(Some(topic))?.ok_or(
            PushConsumerError::InvalidSubscription("topic is empty".to_owned()),
        )?;
        let subscription = build_class_filter_subscription_data(
            topic.clone(),
            full_class_name,
            filter_class_source,
        );
        write_lock(&self.subscriptions).insert(topic, subscription);
        if let Some(runtime) = runtime {
            let _ = runtime.send_heartbeat_to_all_brokers().await;
        }
        Ok(())
    }

    pub async fn unsubscribe(&self, topic: &str) -> Result<bool, PushConsumerError> {
        let state = self.state.lock().await;
        match &*state {
            State::Created | State::Started { .. } => {}
            State::Shutdown => return Err(PushConsumerError::Shutdown),
        }
        drop(state);
        // Java's facade passes the argument through without applying the
        // namespace here, unlike subscribe().
        Ok(write_lock(&self.subscriptions).remove(topic).is_some())
    }

    /// Returns Java `MQConsumerInner.subscriptions()` values.
    // Java returns mutable `SubscriptionData` keys in a HashSet; retain that
    // method-level public contract despite Clippy's mutable-key warning.
    #[allow(clippy::mutable_key_type)]
    pub fn subscriptions(&self) -> HashSet<SubscriptionData> {
        read_lock(&self.subscriptions).values().cloned().collect()
    }

    /// Returns the live table used by the Java implementation accessor.
    pub fn get_subscription_inner(&self) -> Arc<RwLock<HashMap<String, SubscriptionData>>> {
        self.subscriptions.clone()
    }

    /// Returns the live POP process-queue table exposed by Java rebalance.
    pub fn get_pop_process_queue_table(
        &self,
    ) -> Arc<RwLock<HashMap<MessageQueue, Arc<PopProcessQueue>>>> {
        self.pop_process_queues.clone()
    }

    /// Returns the live process-queue table exposed by Java rebalance.
    pub fn get_process_queue_table(&self) -> Arc<RwLock<HashMap<MessageQueue, Arc<ProcessQueue>>>> {
        self.process_queues.clone()
    }

    /// Matches Java's `RebalanceImpl.removeProcessQueue` while preserving Rust's
    /// generation fence and offset-cleanup result.
    pub async fn remove_process_queue(&self, input_queue: &MessageQueue) -> bool {
        let Ok(queue) = self
            .config
            .client_config()
            .clone()
            .queue_with_namespace(input_queue)
        else {
            return false;
        };
        let Some(process_queue) = read_lock(&self.process_queues).get(&queue).cloned() else {
            return false;
        };
        process_queue.set_dropped(true);
        let Some(context) = self.pull_context().await else {
            let mut queues = write_lock(&self.process_queues);
            if queues
                .get(&queue)
                .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
            {
                queues.remove(&queue);
                let _ = process_queue.drop_assignment(process_queue.generation());
                return true;
            }
            return false;
        };
        let removed = self
            .remove_process_queue_safely(&context, &queue, &process_queue)
            .await;
        if removed {
            context.scheduler.cancel(&queue, process_queue.generation());
            context.runtime.rebalance_immediately();
        }
        removed
    }

    /// Matches the subclass queue cleanup invoked by Java rebalance after a
    /// process queue is dropped.
    pub async fn remove_unnecessary_message_queue(
        &self,
        input_queue: &MessageQueue,
        process_queue: &Arc<ProcessQueue>,
    ) -> Result<bool, PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let queue = self
            .config
            .client_config()
            .clone()
            .queue_with_namespace(input_queue)?;
        process_queue.set_dropped(true);
        let removed = self
            .remove_process_queue_safely(&context, &queue, process_queue)
            .await;
        if removed {
            context.scheduler.cancel(&queue, process_queue.generation());
        }
        Ok(removed)
    }

    /// Marks and clears the two assignment tables, as Java's rebalance
    /// implementation does during destruction.
    pub async fn destroy(&self) {
        if let Some(context) = self.pull_context().await {
            for (queue, process_queue) in read_lock(&self.process_queues).iter() {
                let generation = process_queue.generation();
                let _ = process_queue.drop_assignment(generation);
                context.scheduler.cancel(queue, generation);
            }
        } else {
            for process_queue in read_lock(&self.process_queues).values() {
                let _ = process_queue.drop_assignment(process_queue.generation());
            }
        }
        write_lock(&self.process_queues).clear();
        for process_queue in write_lock(&self.pop_process_queues).values() {
            process_queue.drop_assignment();
        }
        write_lock(&self.pop_process_queues).clear();
        let mut pop_workers = std::mem::take(
            &mut *self
                .pop_workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
        .into_values()
        .collect::<Vec<_>>();
        pop_workers.extend(std::mem::take(
            &mut *self
                .pop_retired_workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        ));
        for worker in pop_workers {
            worker.abort();
        }
    }

    /// Matches Java's single-queue rebalance lock operation. The returned
    /// response updates local lock state only for queues the Broker confirms.
    pub async fn lock(&self, input_queue: &MessageQueue) -> bool {
        let Some(runtime) = self.get_client_runtime().await else {
            return false;
        };
        let Ok(queue) = self
            .config
            .client_config()
            .clone()
            .queue_with_namespace(input_queue)
        else {
            return false;
        };
        let result = self
            .lock_orderly_queues_raw(&runtime, std::slice::from_ref(&queue))
            .await;
        for (locked_queue, (timestamp, _)) in &result.lock_times {
            if let Some(process_queue) = read_lock(&self.process_queues).get(locked_queue) {
                process_queue.set_locked(true);
                process_queue.set_last_lock_timestamp(*timestamp);
            }
        }
        result.locked.contains(&queue)
    }

    /// Locks every live process queue and applies only Broker-confirmed
    /// responses, matching Java's rebalance `lockAll` behavior.
    pub async fn lock_all(&self) {
        let Some(runtime) = self.get_client_runtime().await else {
            return;
        };
        let queues = read_lock(&self.process_queues)
            .iter()
            .filter_map(|(queue, process_queue)| {
                (!process_queue.is_dropped()).then_some(queue.clone())
            })
            .collect::<Vec<_>>();
        let result = self.lock_orderly_queues_raw(&runtime, &queues).await;
        for queue in result.responded {
            if let Some(process_queue) = read_lock(&self.process_queues).get(&queue) {
                let locked = result.locked.contains(&queue);
                process_queue.set_locked(locked);
                if let Some((timestamp, _)) = result.lock_times.get(&queue) {
                    process_queue.set_last_lock_timestamp(*timestamp);
                }
            }
        }
    }

    /// Unlocks one queue using Java's `oneway` request option and clears local
    /// lock state only after the request is accepted.
    pub async fn unlock(&self, input_queue: &MessageQueue, oneway: bool) {
        let Some(runtime) = self.get_client_runtime().await else {
            return;
        };
        let Ok(queue) = self
            .config
            .client_config()
            .clone()
            .queue_with_namespace(input_queue)
        else {
            return;
        };
        write_lock(&self.pending_orderly_unlocks).insert(queue.clone());
        let unlocked = self
            .unlock_orderly_queues_raw(&runtime, std::slice::from_ref(&queue), oneway)
            .await;
        if unlocked.contains(&queue) {
            write_lock(&self.pending_orderly_unlocks).remove(&queue);
            if let Some(process_queue) = read_lock(&self.process_queues).get(&queue) {
                process_queue.set_locked(false);
            }
        }
    }

    /// Unlocks all live queues and updates their local state for confirmed
    /// Broker responses.
    pub async fn unlock_all(&self, oneway: bool) {
        let Some(runtime) = self.get_client_runtime().await else {
            return;
        };
        let queues = read_lock(&self.process_queues)
            .iter()
            .filter_map(|(queue, process_queue)| {
                (!process_queue.is_dropped()).then_some(queue.clone())
            })
            .collect::<Vec<_>>();
        write_lock(&self.pending_orderly_unlocks).extend(queues.iter().cloned());
        let unlocked = self
            .unlock_orderly_queues_raw(&runtime, &queues, oneway)
            .await;
        for queue in &unlocked {
            write_lock(&self.pending_orderly_unlocks).remove(queue);
            if let Some(process_queue) = read_lock(&self.process_queues).get(queue) {
                process_queue.set_locked(false);
            }
        }
    }

    /// Runs Java's Push queue-change side effects: update the subscription
    /// version and thresholds, send a heartbeat, and invoke the listener.
    pub async fn message_queue_changed(
        &self,
        topic: impl Into<String>,
        all_queues: Option<&HashSet<MessageQueue>>,
        divided_queues: &HashSet<MessageQueue>,
    ) -> Result<bool, PushConsumerError> {
        let runtime = self
            .get_client_runtime()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let assignment = DesiredAssignment {
            topic: topic.into(),
            all_queues: all_queues.cloned(),
            assigned_queues: divided_queues.clone(),
            pull_queues: HashSet::new(),
            pop_queues: HashSet::new(),
        };
        Ok(self
            .notify_assignment_changed(Some(&runtime), &assignment)
            .await)
    }

    /// Returns the live topic-to-queue table exposed by Java rebalance.
    pub fn get_topic_subscribe_info_table(
        &self,
    ) -> Arc<RwLock<HashMap<String, BTreeSet<MessageQueue>>>> {
        self.topic_queues.clone()
    }

    /// Returns the legacy `topic -> tag expression` configuration map.
    pub fn get_subscription(&self) -> HashMap<String, String> {
        read_lock(&self.subscription_config).clone()
    }

    /// Replaces the legacy subscription configuration map, applying the
    /// consumer namespace to each topic as the Java setter does.
    pub async fn set_subscription(
        &self,
        subscriptions: HashMap<String, String>,
    ) -> Result<(), PushConsumerError> {
        let mut client_config = self.config.client_config().clone();
        let mut namespaced = HashMap::with_capacity(subscriptions.len());
        for (topic, expression) in subscriptions {
            let topic = client_config.with_namespace(Some(&topic))?.ok_or_else(|| {
                PushConsumerError::InvalidSubscription("topic is empty".to_owned())
            })?;
            namespaced.insert(topic, expression);
        }
        *write_lock(&self.subscription_config) = namespaced;
        Ok(())
    }

    /// Restores ordinary topic names from POP retry-topic names for this
    /// consumer group, matching Java's `tryResetPopRetryTopic` helper.
    pub fn try_reset_pop_retry_topic(&self, messages: &mut [MessageExt], consumer_group: &str) {
        let prefix = format!("%RETRY%{consumer_group}_");
        for message in messages {
            if let Some(topic) = message.topic().strip_prefix(&prefix).map(str::to_owned) {
                if !topic.is_empty() {
                    message.set_topic(topic);
                }
            }
        }
    }

    /// Restores the original topic on group retry messages, then removes this
    /// consumer's namespace, matching Java's `resetRetryAndNamespace` helper.
    pub fn reset_retry_and_namespace(&self, messages: &mut [MessageExt], consumer_group: &str) {
        self.reset_retry_and_namespace_for_pop(messages, consumer_group);
    }

    /// Reports whether a subscribed topic still needs route data, matching
    /// `MQConsumerInner.isSubscribeTopicNeedUpdate`.
    pub fn is_subscribe_topic_need_update(&self, topic: &str) -> bool {
        read_lock(&self.subscriptions).contains_key(topic)
            && !read_lock(&self.topic_queues).contains_key(topic)
    }

    /// Stores route queues for a subscribed topic and wakes the runtime's
    /// rebalance worker when the route set changes.
    pub async fn update_topic_subscribe_info(&self, topic: &str, queues: BTreeSet<MessageQueue>) {
        if !read_lock(&self.subscriptions).contains_key(topic) {
            return;
        }
        let previous = write_lock(&self.topic_queues).insert(topic.to_owned(), queues.clone());
        if previous.as_ref() != Some(&queues) {
            if let Some(context) = self.pull_context().await {
                context.runtime.rebalance_immediately();
            }
        }
    }

    /// Runs one rebalance pass immediately, unless Push consumption is paused.
    pub async fn do_rebalance(self: &Arc<Self>) {
        if !self.paused.load(Ordering::Acquire) {
            let _ = self.rebalance_service.rebalance(self).await;
        }
    }

    /// Runs one rebalance pass and returns its result, matching Java's
    /// `tryRebalance`; paused consumers return `false`.
    pub async fn try_rebalance(self: &Arc<Self>) -> bool {
        if self.paused.load(Ordering::Acquire) {
            return false;
        }
        self.rebalance_service.rebalance(self).await
    }

    /// Runs the Java `RebalanceImpl.doRebalance(isOrder)` flow and returns
    /// whether every subscribed topic balanced successfully.
    pub async fn do_rebalance_with_order(self: &Arc<Self>, is_order: bool) -> bool {
        if self.paused.load(Ordering::Acquire) {
            return false;
        }
        self.rebalance_service
            .rebalance_with_order(self, Some(is_order))
            .await
    }

    pub async fn suspend(&self) {
        if self.paused.swap(true, Ordering::AcqRel) {
            return;
        }
        if let State::Started { pull_scheduler, .. } = &*self.state.lock().await {
            pull_scheduler.pause_all();
        }
    }

    pub async fn resume(&self) {
        self.paused.store(false, Ordering::Release);
        {
            let state = self.state.lock().await;
            if let State::Started { pull_scheduler, .. } = &*state {
                pull_scheduler.resume_all();
            }
        }
        self.rebalance_after_resume().await;
    }

    async fn rebalance_after_resume(&self) {
        let owner = self
            .owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .upgrade();
        if let Some(owner) = owner {
            owner.do_rebalance().await;
        }
    }

    pub(crate) async fn reset_consumer_offsets(
        &self,
        topic: &str,
        offsets: Vec<(MessageQueue, Option<i64>)>,
    ) -> Result<(), PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let group = self.config.consumer_group();
        context
            .runtime
            .reset_offset(
                Some(topic),
                group.as_deref(),
                Some(SharedValue::new(
                    offsets
                        .into_iter()
                        .map(|(queue, offset)| (Some(queue), offset))
                        .collect(),
                )),
            )
            .await?;
        Ok(())
    }

    async fn reset_consumer_offsets_shared(
        &self,
        topic: Option<&str>,
        offsets: Option<rocketmq_classic_core::runtime::ConsumerOffsetTable>,
    ) -> Result<(), PushConsumerError> {
        let mut context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        struct ResumeReset<'a> {
            consumer: &'a PushConsumer,
            scheduler: Arc<PullScheduler>,
            runtime: Arc<ClientRuntime>,
            armed: bool,
        }
        impl Drop for ResumeReset<'_> {
            fn drop(&mut self) {
                if self.armed && self.consumer.paused.swap(false, Ordering::AcqRel) {
                    self.scheduler.resume_all();
                    self.runtime.rebalance_immediately();
                }
            }
        }
        let mut resume = ResumeReset {
            consumer: self,
            scheduler: context.scheduler.clone(),
            runtime: context.runtime.clone(),
            armed: true,
        };
        self.paused.store(true, Ordering::Release);
        context.scheduler.pause_all();
        let result = async {
            // Java first marks every matching live ProcessQueue dropped and
            // clears it, including rows whose nullable offset has no value.
            // Offset-table rows without a live ProcessQueue are ignored.
            let process_queues = read_lock(&self.process_queues)
                .iter()
                .map(|(queue, process_queue)| (queue.clone(), process_queue.clone()))
                .collect::<Vec<_>>();
            for (queue, process_queue) in process_queues {
                let topic = topic
                    .ok_or_else(|| RuntimeError::ConsumerOperation("null reset topic".into()))?;
                if queue.topic() != Some(topic) {
                    continue;
                }
                let offsets = offsets.as_ref().ok_or_else(|| {
                    RuntimeError::ConsumerOperation("null reset offset table".into())
                })?;
                if !offsets.with(|offsets| offsets.contains_key(&Some(queue.clone()))) {
                    continue;
                }
                let generation = process_queue.generation();
                process_queue
                    .drop_assignment(generation)
                    .map_err(|error| PushConsumerError::PullScheduler(error.to_string()))?;
                context.scheduler.cancel(&queue, generation);
                process_queue.clear();
            }

            let consume_orderly = matches!(&context.consume_service, ConsumeService::Orderly(_));
            let orderly_clustering = consume_orderly
                && self.config.message_model()
                    == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering;
            if !consume_orderly {
                // MQClientInstance.resetOffset drains concurrent callbacks for
                // its complete process-queue snapshot before writing offsets.
                tokio::time::sleep(RESET_OFFSET_MAX_WAIT).await;
            }

            // Java obtains a fresh key-set iterator and calls get(mq) again
            // after waiting. Keep caller-map updates and newly assigned queues
            // visible rather than reusing the first traversal's values.
            let process_queues = read_lock(&self.process_queues)
                .iter()
                .map(|(queue, process_queue)| (queue.clone(), process_queue.clone()))
                .collect::<Vec<_>>();
            for (queue, process_queue) in process_queues {
                let offsets = offsets.as_ref().ok_or_else(|| {
                    RuntimeError::ConsumerOperation("null reset offset table".into())
                })?;
                let offset =
                    offsets.with(|offsets| offsets.get(&Some(queue.clone())).copied().flatten());
                let topic = topic
                    .ok_or_else(|| RuntimeError::ConsumerOperation("null reset topic".into()))?;
                if queue.topic() != Some(topic) {
                    continue;
                }
                let Some(offset) = offset else {
                    continue;
                };
                if consume_orderly {
                    // Java's waitResetOffsetReady ignores a failed 10-second
                    // write-lock attempt and continues with the reset.
                    let gate = process_queue.orderly_consume_gate();
                    if let Ok(guard) =
                        tokio::time::timeout(RESET_OFFSET_MAX_WAIT, gate.write_owned()).await
                    {
                        drop(guard);
                    }
                }

                // Dropping the generation fences late callbacks and pull
                // replies; ordinary updates then match Java's reset path and
                // leave the next assignment free to advance this offset.
                let Some(offset_store) = self.get_offset_store() else {
                    tracing::warn!(queue = %queue, "reset offset failed: null offset store");
                    continue;
                };
                offset_store.update_offset(&queue, offset, false);
                let Some(offset_store) = self.get_offset_store() else {
                    tracing::warn!(queue = %queue, "reset persistence failed: null offset store");
                    continue;
                };
                context.offset_store = offset_store;
                if let Err(error) = context.offset_store.persist(&queue).await {
                    tracing::warn!(queue = %queue, error = %error, "reset offset failed");
                    continue;
                }

                let removed = if orderly_clustering {
                    if let Err(error) = self.try_remove_orderly_queue_after_reset(&context, &queue, &process_queue)
                        .await {
                        tracing::warn!(queue = %queue, error = %error, "reset offset removal failed");
                        continue;
                    }
                    let mut queues = write_lock(&self.process_queues);
                    if queues
                        .get(&queue)
                        .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
                    {
                        queues.remove(&queue);
                        true
                    } else {
                        false
                    }
                } else {
                    let is_current = read_lock(&self.process_queues)
                        .get(&queue)
                        .is_some_and(|current| Arc::ptr_eq(current, &process_queue));
                    if is_current {
                        let Some(offset_store) = self.get_offset_store() else {
                            tracing::warn!(queue = %queue, "reset removal failed: null offset store");
                            continue;
                        };
                        // Custom stores can call back into consumer inspection.
                        // Never invoke them under the process-queue map lock.
                        offset_store.remove_offset(&queue);
                        let mut queues = write_lock(&self.process_queues);
                        if queues.get(&queue).is_some_and(|current| Arc::ptr_eq(current, &process_queue)) {
                            queues.remove(&queue);
                            true
                        } else { false }
                    } else {
                        false
                    }
                };
                if removed {
                    write_lock(&self.offset_corrections_pending).remove(&queue);
                }
            }
            Ok(())
        }
        .await;
        self.paused.store(false, Ordering::Release);
        context.scheduler.resume_all();
        // Only cancellation before the finally action needs Drop recovery.
        // A rebalance callback can suspend again; do not undo that on return.
        resume.armed = false;
        self.rebalance_after_resume().await;
        result
    }

    async fn try_remove_orderly_queue_after_reset(
        &self,
        context: &PullContext,
        queue: &MessageQueue,
        process_queue: &Arc<ProcessQueue>,
    ) -> Result<(), PushConsumerError> {
        let generation = process_queue.generation();
        let orderly_settings = self.current_orderly_lock_settings().await;
        let force_unlock = orderly_settings.is_some_and(|settings| {
            process_queue
                .is_lock_older_than(generation, settings.unlock_delay_millis)
                .unwrap_or(false)
        });
        let gate = process_queue.orderly_consume_gate();
        let _consume_guard = if force_unlock {
            None
        } else {
            match tokio::time::timeout(ORDERLY_RESET_REMOVE_LOCK_WAIT, gate.write_owned()).await {
                Ok(guard) => Some(guard),
                Err(_) => {
                    let _ = process_queue.increment_try_unlock_times(generation);
                    return Ok(());
                }
            }
        };

        // RebalancePushImpl persists before and again inside the orderly
        // unlock/removal attempt. A lock/unlock miss still permits local
        // removal; a thrown store error leaves it registered like Java's catch.
        let offset_store = self
            .get_offset_store()
            .ok_or(PushConsumerError::OffsetStoreUnavailable)?;
        offset_store.persist(queue).await?;
        self.get_offset_store()
            .ok_or(PushConsumerError::OffsetStoreUnavailable)?
            .remove_offset(queue);
        let _ = process_queue.record_unlocked(generation);
        write_lock(&self.pending_orderly_unlocks).insert(queue.clone());
        let unlocked = self
            .unlock_orderly_queues_raw(&context.runtime, std::slice::from_ref(queue), false)
            .await
            .contains(queue);
        if unlocked {
            write_lock(&self.pending_orderly_unlocks).remove(queue);
        } else {
            let _ = process_queue.increment_try_unlock_times(generation);
        }
        Ok(())
    }

    /// Returns the local equivalent of Java's
    /// `DefaultMQPushConsumerImpl.consumerRunningInfo()` snapshot.
    pub async fn consumer_running_info(
        &self,
    ) -> Result<ConsumerRunningInfoBody, PushConsumerError> {
        let (runtime_snapshot, offset_store) = match &*self.state.lock().await {
            State::Started {
                runtime,
                offset_store,
                ..
            } => {
                let runtime = runtime.upgrade().ok_or(PushConsumerError::NotStarted)?;
                let runtime_snapshot = RunningInfoRuntimeSnapshot::from_runtime(&runtime);
                *write_lock(&self.running_info_runtime_snapshot) = Some(runtime_snapshot.clone());
                (runtime_snapshot, Some(offset_store.clone()))
            }
            State::Created => return Err(PushConsumerError::NotStarted),
            State::Shutdown => (
                read_lock(&self.running_info_runtime_snapshot)
                    .clone()
                    .ok_or(PushConsumerError::Shutdown)?,
                None,
            ),
        };
        Ok(self.running_info_body_with_runtime_snapshot(&runtime_snapshot, None, offset_store))
    }

    /// Submits an owner task to the shared runtime's PullMessageService worker.
    ///
    /// This is the Rust owner equivalent of Java's raw implementation
    /// `executeTask(Runnable)` entrypoint. Tasks share the client runtime and
    /// are retired with that runtime's lifecycle.
    pub async fn execute_task(
        &self,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), PushConsumerError> {
        self.active_client_runtime().await?.execute_task(task)?;
        Ok(())
    }

    /// Schedules an owner task on the shared runtime after the requested
    /// delay. Non-positive delays are immediately eligible, matching Java's
    /// scheduled-executor behavior for negative and zero delays.
    pub async fn execute_task_later(
        &self,
        task: impl Future<Output = ()> + Send + 'static,
        delay_millis: i64,
    ) -> Result<(), PushConsumerError> {
        self.active_client_runtime()
            .await?
            .execute_task_later(task, delay_millis)?;
        Ok(())
    }

    async fn active_client_runtime(&self) -> Result<Arc<ClientRuntime>, PushConsumerError> {
        match &*self.state.lock().await {
            State::Started { runtime, .. } => match self
                .implementation_client_factory
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
            {
                Some(runtime) => runtime.ok_or(PushConsumerError::NotStarted),
                None => runtime.upgrade().ok_or(PushConsumerError::NotStarted),
            },
            State::Created => Err(PushConsumerError::NotStarted),
            State::Shutdown => Err(PushConsumerError::Shutdown),
        }
    }

    pub(crate) fn running_info_body(
        &self,
        runtime: &ClientRuntime,
        jstack: Option<String>,
        offset_store: Option<Arc<dyn OffsetStore>>,
    ) -> ConsumerRunningInfoBody {
        let runtime_snapshot = RunningInfoRuntimeSnapshot::from_runtime(runtime);
        self.running_info_body_with_runtime_snapshot(&runtime_snapshot, jstack, offset_store)
    }

    fn running_info_body_with_runtime_snapshot(
        &self,
        runtime_snapshot: &RunningInfoRuntimeSnapshot,
        jstack: Option<String>,
        offset_store: Option<Arc<dyn OffsetStore>>,
    ) -> ConsumerRunningInfoBody {
        let consume_core_pool_size = self
            .get_consume_message_service()
            .expect("consume-message service is unavailable")
            .core_pool_size();
        let mut properties = crate::admin::push_running_info_properties(
            &self.config,
            &runtime_snapshot.nameserver_address,
            self.consumer_start_timestamp.load(Ordering::Acquire),
            self.consume_orderly.load(Ordering::Acquire),
            consume_core_pool_size,
        );
        properties.insert(
            "pullThresholdForQueue".into(),
            self.config.pull_threshold_for_queue().to_string(),
        );
        properties.insert(
            "pullThresholdSizeForQueue".into(),
            self.config.pull_threshold_size_for_queue_mib().to_string(),
        );
        let mut body = ConsumerRunningInfoBody::default();
        body.set_properties(Some(SharedValue::new(properties)));
        body.set_subscription_set(Some(SharedValue::new(
            read_lock(&self.subscriptions).values().cloned().collect(),
        )));
        body.set_jstack(jstack);

        let status_table = body
            .get_subscription_set()
            .unwrap()
            .with(|subscription_set| subscription_set.clone())
            .into_iter()
            .filter_map(|subscription| {
                subscription.topic.as_ref().map(|topic| {
                    (
                        topic.clone(),
                        self.stats_manager().map_or_else(
                            ConsumerConsumeStatus::default,
                            |manager| {
                                manager.consume_status(
                                    &self.config.consumer_group().unwrap_or_default(),
                                    topic,
                                )
                            },
                        ),
                    )
                })
            })
            .collect();
        body.set_status_table(Some(SharedValue::new(status_table)));

        let offset_table = offset_store
            .map(|offset_store| offset_store.clone_offset_table(None))
            .unwrap_or_default();
        let process_queues = read_lock(&self.process_queues)
            .iter()
            .map(|(queue, process_queue)| {
                (
                    queue.clone(),
                    process_queue.clone(),
                    process_queue.generation(),
                )
            })
            .collect::<Vec<_>>();
        let mq_table = process_queues
            .into_iter()
            .filter_map(|(queue, process_queue, generation)| {
                let snapshot = process_queue.snapshot(generation).ok()?;
                let mut info = ConsumerProcessQueueInfo::default();
                info.set_commit_offset(offset_table.get(&queue).copied().unwrap_or(-1));
                info.set_cached_msg_min_offset(snapshot.minimum_offset.unwrap_or(0));
                info.set_cached_msg_max_offset(snapshot.maximum_offset.unwrap_or(0));
                info.set_cached_msg_count(
                    i32::try_from(snapshot.cached_map_count).unwrap_or(i32::MAX),
                );
                info.set_cached_msg_size_in_mib(
                    i32::try_from(snapshot.cached_bytes / (1024 * 1024)).unwrap_or(i32::MAX),
                );
                info.set_transaction_msg_min_offset(
                    snapshot.transaction_minimum_offset.unwrap_or(0),
                );
                info.set_transaction_msg_max_offset(
                    snapshot.transaction_maximum_offset.unwrap_or(0),
                );
                info.set_transaction_msg_count(
                    i32::try_from(snapshot.transaction_count).unwrap_or(i32::MAX),
                );
                info.set_locked(snapshot.locked);
                info.set_try_unlock_times(snapshot.try_unlock_times);
                info.set_last_lock_timestamp(snapshot.last_lock_timestamp_millis);
                info.set_droped(snapshot.dropped);
                info.set_last_pull_timestamp(snapshot.last_pull_timestamp_millis);
                info.set_last_consume_timestamp(snapshot.last_consume_timestamp_millis);
                Some((queue.clone(), info))
            })
            .collect();
        body.set_mq_table(Some(SharedValue::new(mq_table)));

        let pop_process_queues = read_lock(&self.pop_process_queues)
            .iter()
            .map(|(queue, process_queue)| (queue.clone(), process_queue.clone()))
            .collect::<Vec<_>>();
        let mq_pop_table = pop_process_queues
            .into_iter()
            .map(|(queue, process_queue)| {
                let wait_ack_count = process_queue.wait_ack_count();
                let mut info = ConsumerPopProcessQueueInfo::default();
                info.set_wait_ack_count(wait_ack_count);
                info.set_droped(process_queue.is_dropped());
                info.set_last_pop_timestamp(process_queue.last_pop_timestamp());
                (queue, info)
            })
            .collect();
        body.set_mq_pop_table(Some(SharedValue::new(mq_pop_table)));

        body
    }

    pub(crate) async fn consume_message_directly(
        &self,
        message: MessageExt,
        broker_name: Option<String>,
    ) -> Option<ConsumeMessageDirectlyResult> {
        let context = self.pull_context().await?;
        if let Some(service) = self.custom_consume_message_service() {
            return Some(service.consume_message_directly(message, broker_name).await);
        }
        if self.consume_message_service_is_null() {
            return None;
        }
        self.consume_message_directly_using_service(message, broker_name, context.consume_service)
            .await
    }

    async fn consume_message_directly_with_service(
        &self,
        message: MessageExt,
        broker_name: Option<String>,
        service: ConsumeService,
    ) -> Option<ConsumeMessageDirectlyResult> {
        self.pull_context().await?;
        self.consume_message_directly_using_service(message, broker_name, service)
            .await
    }

    async fn consume_message_directly_using_service(
        &self,
        mut message: MessageExt,
        broker_name: Option<String>,
        service: ConsumeService,
    ) -> Option<ConsumeMessageDirectlyResult> {
        let started = std::time::Instant::now();
        let orderly = matches!(&service, ConsumeService::Orderly(_));
        let queue = MessageQueue::new_nullable(
            message.topic_nullable().map(str::to_owned),
            broker_name.clone(),
            message.queue_id(),
        );
        if !orderly {
            message.set_broker_name(broker_name);
        }
        let mut messages = vec![message];
        self.reset_retry_and_namespace_for_group(&mut messages, &self.consume_stats_group());

        let mut result = ConsumeMessageDirectlyResult::default();
        match service.listener() {
            PushMessageListener::Concurrently(listener) if !orderly => {
                result.set_order(false);
                result.set_auto_commit(true);
                let mut callback = AbortOnDrop(tokio::spawn(
                    listener.consume_message(messages, ConsumeConcurrentlyContext::new(queue)),
                ));
                match (&mut callback.0).await {
                    Ok((Some(ConsumeConcurrentlyStatus::ConsumeSuccess), _)) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrSuccess));
                    }
                    Ok((Some(ConsumeConcurrentlyStatus::ReconsumeLater), _)) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrLater));
                    }
                    Ok((None, _)) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrReturnNull));
                    }
                    Err(error) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrThrowException));
                        result.set_remark(Some(error.to_string()));
                    }
                }
            }
            PushMessageListener::Orderly(listener) if orderly => {
                result.set_order(true);
                let mut callback = AbortOnDrop(tokio::spawn(
                    listener.consume_message(messages, ConsumeOrderlyContext::new(queue)),
                ));
                match (&mut callback.0).await {
                    Ok((Some(ConsumeOrderlyStatus::Success), context)) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrSuccess));
                        result.set_auto_commit(context.auto_commit());
                    }
                    Ok((Some(ConsumeOrderlyStatus::Rollback), context)) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrRollback));
                        result.set_auto_commit(context.auto_commit());
                    }
                    Ok((Some(ConsumeOrderlyStatus::Commit), context)) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrCommit));
                        result.set_auto_commit(context.auto_commit());
                    }
                    Ok((Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment), context)) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrLater));
                        result.set_auto_commit(context.auto_commit());
                    }
                    Ok((None, context)) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrReturnNull));
                        result.set_auto_commit(context.auto_commit());
                    }
                    Err(error) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrThrowException));
                        result.set_remark(Some(error.to_string()));
                    }
                }
            }
            _ => return None,
        }
        result.set_spent_time_mills(started.elapsed().as_millis().min(i64::MAX as u128) as i64);
        Some(result)
    }

    pub fn is_pause(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    /// Mirrors Java's direct `setPause` field mutation without scheduling a
    /// rebalance. Use `suspend` or `resume` for the lifecycle operations.
    pub fn set_pause(&self, pause: bool) {
        self.paused.store(pause, Ordering::Release);
    }

    pub fn is_consume_orderly(&self) -> bool {
        self.consume_orderly.load(Ordering::Acquire)
    }

    /// Mirrors Java's direct `setConsumeOrderly` implementation-state setter.
    pub fn set_consume_orderly(&self, consume_orderly: bool) {
        self.consume_orderly
            .store(consume_orderly, Ordering::Release);
    }

    /// Mirrors Java 5.5.1's void `updateCorePoolSize` operation.
    ///
    /// Values rejected by Java's live facade bounds are ignored. If a raised
    /// `consumeThreadMax` permits a size beyond the executor's construction-time
    /// maximum, Java throws; Rust reports that as
    /// [`PushConsumerError::InvalidSetting`]. Before service creation, Java
    /// fails through a null service; Rust reports that state as
    /// [`PushConsumerError::NotStarted`]. Java retains the consume service after
    /// shutdown, so accepted values still update its tracked core-pool size
    /// after its workers have stopped.
    pub async fn update_core_pool_size(
        &self,
        core_pool_size: i32,
    ) -> Result<(), PushConsumerError> {
        if let Some(service) = self.custom_consume_message_service() {
            if core_pool_size > 0
                && core_pool_size <= i16::MAX as i32
                && core_pool_size < self.config.consume_thread_max()
            {
                service.update_core_pool_size(core_pool_size);
            }
            return Ok(());
        }
        if self.consume_message_service_is_null() {
            return Err(PushConsumerError::ConsumeMessageServiceUnavailable);
        }
        let state = self.state.lock().await;
        match &*state {
            State::Created if !self.consume_service_initialized.load(Ordering::Acquire) => {
                Err(PushConsumerError::NotStarted)
            }
            State::Created | State::Shutdown => {
                if core_pool_size <= 0
                    || core_pool_size > i16::MAX as i32
                    || core_pool_size >= self.config.consume_thread_max()
                {
                    return Ok(());
                }
                let executor_max = self.consume_service_max_threads.load(Ordering::Acquire);
                if core_pool_size > executor_max {
                    return Err(PushConsumerError::InvalidSetting {
                        name: "corePoolSize",
                        value: i64::from(core_pool_size),
                        minimum: 1,
                        maximum: i64::from(executor_max),
                    });
                }
                self.consume_core_pool_size
                    .store(core_pool_size, Ordering::Release);
                Ok(())
            }
            State::Started {
                consume_service, ..
            } => {
                if core_pool_size <= 0
                    || core_pool_size > i16::MAX as i32
                    || core_pool_size >= self.config.consume_thread_max()
                {
                    return Ok(());
                }
                let executor_max = consume_service.maximum_threads();
                if core_pool_size > executor_max {
                    return Err(PushConsumerError::InvalidSetting {
                        name: "corePoolSize",
                        value: i64::from(core_pool_size),
                        minimum: 1,
                        maximum: i64::from(executor_max),
                    });
                }
                if consume_service.update_core_pool_size(core_pool_size) {
                    self.consume_core_pool_size
                        .store(core_pool_size, Ordering::Release);
                }
                Ok(())
            }
        }
    }

    /// Mirrors `DefaultMQPushConsumerImpl.adjustThreadPool` using current queue
    /// accumulation and the installed service's grow/shrink hooks.
    pub fn adjust_thread_pool(&self) {
        let service = self
            .get_consume_message_service()
            .expect("consume-message service is unavailable");
        let accumulation = read_lock(&self.process_queues)
            .values()
            .fold(0_i64, |sum, process_queue| {
                sum.wrapping_add(process_queue.get_msg_acc_cnt())
            });
        let threshold = self.config.adjust_thread_pool_nums_threshold();
        let decrease_threshold = (threshold as f64 * 0.8) as i64;
        if accumulation >= threshold {
            service.inc_core_pool_size();
        }
        if accumulation < decrease_threshold {
            service.dec_core_pool_size();
        }
    }

    /// Sends a consumed message back to its originating Broker and uses the
    /// Java-compatible retry-topic fallback if the Broker rejects the request.
    pub async fn send_message_back(
        &self,
        message: &MessageExt,
        delay_level: i32,
    ) -> Result<(), PushConsumerError> {
        self.send_message_back_with_broker(message, delay_level, None)
            .await
    }

    /// Sends a consumed message back to the named Broker, matching Java's
    /// three-argument `sendMessageBack` overload.
    pub async fn send_message_back_to_broker(
        &self,
        message: &MessageExt,
        delay_level: i32,
        broker_name: Option<&str>,
    ) -> Result<(), PushConsumerError> {
        self.send_message_back_with_broker(message, delay_level, Some(broker_name))
            .await
    }

    async fn send_message_back_with_broker(
        &self,
        message: &MessageExt,
        delay_level: i32,
        broker_name_override: Option<Option<&str>>,
    ) -> Result<(), PushConsumerError> {
        let runtime = {
            let state = self.state.lock().await;
            match &*state {
                State::Started { runtime, .. } => {
                    runtime.upgrade().ok_or(PushConsumerError::NotStarted)?
                }
                State::Created => return Err(PushConsumerError::NotStarted),
                State::Shutdown => return Err(PushConsumerError::Shutdown),
            }
        };
        let effective_broker = match broker_name_override {
            Some(broker_name) => broker_name.map(str::to_owned),
            None => message.broker_name().map(str::to_owned),
        };
        let queue = MessageQueue::new_nullable(
            Some(message.topic().to_owned()),
            effective_broker,
            message.queue_id(),
        );
        if self
            .send_message_back_for_consume(
                &runtime,
                &queue,
                message,
                delay_level,
                SendBackOptions {
                    broker_name_override,
                    address_broker_name_override: None,
                    consumer_group_override: None,
                    max_reconsume_times_override: None,
                    timeout_millis: CONSUMER_SEND_BACK_TIMEOUT_MILLIS,
                    orderly: false,
                },
            )
            .await
        {
            Ok(())
        } else {
            Err(PushConsumerError::SendMessageBackFailed)
        }
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), PushConsumerError> {
        *self
            .owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::downgrade(self);
        let mut state = self.state.lock().await;
        match &*state {
            State::Started { .. } => return Err(PushConsumerError::AlreadyStarted),
            State::Shutdown => return Err(PushConsumerError::Shutdown),
            State::Created => {}
        }
        let service_state = self.service_state();
        if service_state != Some(ConsumerServiceState::CreateJust) {
            return Err(PushConsumerError::ServiceStateNotReady(service_state));
        }
        self.set_service_state(Some(ConsumerServiceState::StartFailed));

        let mut client_config = self.config.client_config().clone();
        let consumer_group =
            client_config.with_namespace(self.config.consumer_group().as_deref())?;
        self.config.set_consumer_group(consumer_group.clone());
        let consumer_group = consumer_group.ok_or(PushConsumerError::InvalidConsumerGroup)?;
        self.config.validate(&consumer_group)?;
        let pull_max_idle_time_millis = crate::process_queue::process_queue_pull_max_idle_time()
            .map_err(PushConsumerError::PullIdleTimeSettings)?;
        self.pull_max_idle_time_millis
            .store(pull_max_idle_time_millis, Ordering::Release);
        let pop_pull_max_idle_time_millis = crate::pop::pop_process_queue_pull_max_idle_time()
            .map_err(PushConsumerError::PullIdleTimeSettings)?;
        self.pop_pull_max_idle_time_millis
            .store(pop_pull_max_idle_time_millis, Ordering::Release);
        self.pull_api.set_unit_mode(self.config.unit_mode());
        let pop_consume_limit =
            usize::try_from(self.config.consume_thread_max().max(1)).unwrap_or(1);
        let available_pop_permits = self.pop_consume_semaphore.available_permits();
        match pop_consume_limit.cmp(&available_pop_permits) {
            std::cmp::Ordering::Greater => self
                .pop_consume_semaphore
                .add_permits(pop_consume_limit - available_pop_permits),
            std::cmp::Ordering::Less => {
                self.pop_consume_semaphore
                    .forget_permits(available_pop_permits - pop_consume_limit);
            }
            std::cmp::Ordering::Equal => {}
        }
        let listener = read_lock(&self.listener)
            .clone()
            .ok_or(PushConsumerError::MissingMessageListener)?;
        *write_lock(&self.pop_listener) = Some(listener.clone());
        let orderly_lock_settings = match &listener {
            PushMessageListener::Concurrently(_) => None,
            PushMessageListener::Orderly(_) => Some(
                OrderlyLockSettings::from_java_properties()
                    .map_err(PushConsumerError::OrderlyLockSettings)?,
            ),
        };
        self.consume_orderly
            .store(orderly_lock_settings.is_some(), Ordering::Release);
        *write_lock(&self.consume_stats_group) = Some(consumer_group.clone());
        let service_max_threads = self.config.consume_thread_max();
        let consume_service = match listener {
            PushMessageListener::Concurrently(listener) => {
                ConsumeService::Concurrent(Arc::new(ConcurrentConsumeService::new(
                    self.config.consume_thread_min(),
                    service_max_threads,
                    listener,
                )))
            }
            PushMessageListener::Orderly(listener) => {
                ConsumeService::Orderly(Arc::new(OrderlyConsumeService::new(
                    self.config.consume_thread_min(),
                    service_max_threads,
                    listener,
                )))
            }
        };
        self.bind_builtin_consume_message_service(Arc::new(BuiltinConsumeMessageService {
            owner: Arc::downgrade(self),
            service: consume_service.clone(),
            orderly_lock_settings,
        }));
        self.consume_service_max_threads
            .store(service_max_threads.max(1), Ordering::Release);
        self.consume_service_initialized
            .store(true, Ordering::Release);
        let pull_scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());

        self.copy_subscriptions(&consumer_group)?;
        self.rebalance_service
            .set_strategy(self.config.allocate_message_queue_strategy());
        self.rebalance_service
            .set_message_model(self.config.message_model());

        let runtime = self
            .manager
            .get_or_create_with_hook_and_tls_config(
                &client_config,
                self.rpc_hook.clone(),
                self.config.tls_config().cloned(),
            )
            .await?;
        self.bind_stats_manager(runtime.consumer_stats_manager());
        self.set_implementation_client_runtime(Some(runtime.clone()));
        self.set_client_runtime(Some(runtime.clone()));
        if let Err(error) = self.checkpoint(StartupStage::RuntimeCreated) {
            let _ = runtime.shutdown().await;
            return Err(error);
        }
        let offset_store = match self.select_offset_store(&runtime, &consumer_group) {
            Ok(store) => store,
            Err(error) => {
                let _ = runtime.shutdown().await;
                return Err(error);
            }
        };
        let services = StartupServices {
            consume_service: consume_service.clone(),
            orderly_lock_settings,
            renew_orderly_locks: orderly_lock_settings.is_some()
                && self.config.message_model()
                    == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering,
            pull_scheduler: pull_scheduler.clone(),
            pull_rpc_workers: pull_rpc_workers.clone(),
            consumer: Arc::downgrade(self),
        };
        self.consumer_start_timestamp
            .store(epoch_millis(), Ordering::Release);
        let startup = self
            .start_registered(&runtime, &consumer_group, offset_store.clone(), &services)
            .await;
        match startup {
            Ok(lease) => {
                *write_lock(&self.running_info_runtime_snapshot) =
                    Some(RunningInfoRuntimeSnapshot::from_runtime(&runtime));
                *state = State::Started {
                    runtime: Arc::downgrade(&runtime),
                    _lease: lease,
                    offset_store,
                    consume_service,
                    orderly_lock_settings,
                    pull_scheduler,
                    pull_rpc_workers,
                };
                self.set_service_state(Some(ConsumerServiceState::Running));
                self.start_trace_dispatcher().await;
                runtime.rebalance_immediately();
                Ok(())
            }
            Err((error, lease)) => {
                pull_scheduler.stop().await;
                pull_rpc_workers
                    .stop(self.config.await_termination_millis_when_shutdown())
                    .await;
                consume_service
                    .shutdown(self.config.await_termination_millis_when_shutdown())
                    .await;
                self.consume_orderly.store(false, Ordering::Release);
                if let Some(lease) = lease.filter(|_| {
                    runtime.state() != rocketmq_classic_core::runtime::RuntimeState::Failed
                }) {
                    let _ = runtime
                        .unregister_consumer(&lease, self.unregister_deadline())
                        .await;
                }
                let _ = runtime.shutdown().await;
                if matches!(
                    &error,
                    PushConsumerError::Runtime(
                        rocketmq_classic_core::runtime::RuntimeError::DuplicateGroup { .. }
                    )
                ) {
                    self.set_service_state(Some(ConsumerServiceState::CreateJust));
                }
                Err(error)
            }
        }
    }

    async fn start_trace_dispatcher(&self) {
        let client_config = self.config.client_config();
        if !client_config.enable_trace() {
            return;
        }
        let dispatcher = match rocketmq_classic_producer::TraceDispatcher::new(
            client_config,
            &self.config.consumer_group().unwrap_or_default(),
            rocketmq_classic_producer::TraceProducerKind::Consume,
            client_config.trace_topic(),
            self.manager.clone(),
            self.rpc_hook.clone(),
        ) {
            Ok(dispatcher) => Arc::new(dispatcher),
            Err(error) => {
                tracing::error!(error = %error, "consume trace dispatcher initialization failed");
                return;
            }
        };
        if let Err(error) = dispatcher.start().await {
            tracing::error!(error = %error, "consume trace dispatcher start failed");
            return;
        }
        self.consume_message_hooks
            .register(Arc::new(ConsumeTraceHook::new(dispatcher.clone())));
        let _ = self.trace_dispatcher.set(dispatcher);
    }

    async fn shutdown_trace_dispatcher(&self) {
        if let Some(dispatcher) = self.trace_dispatcher.get() {
            if let Err(error) = dispatcher.shutdown().await {
                tracing::warn!(error = %error, "consume trace dispatcher shutdown failed");
            }
        }
    }

    async fn start_registered(
        &self,
        runtime: &Arc<ClientRuntime>,
        consumer_group: &str,
        offset_store: Arc<dyn OffsetStore>,
        services: &StartupServices,
    ) -> Result<GroupLease, (PushConsumerError, Option<GroupLease>)> {
        if let Err(error) = offset_store.load().await {
            return Err((error.into(), None));
        }
        if let Err(error) = self.checkpoint(StartupStage::OffsetStoreLoaded) {
            return Err((error, None));
        }
        services
            .consume_service
            .start(
                services.consumer.clone(),
                services.orderly_lock_settings,
                services.renew_orderly_locks,
            )
            .await;
        if let Err(error) = self.checkpoint(StartupStage::ConsumeServiceStarted) {
            return Err((error, None));
        }
        services
            .pull_scheduler
            .start(Arc::downgrade(runtime))
            .map_err(|error| (PushConsumerError::PullScheduler(error.to_string()), None))?;
        if let Err(error) = self.checkpoint(StartupStage::PullSchedulerStarted) {
            return Err((error, None));
        }
        if let Err(error) = services
            .pull_rpc_workers
            .start(services.consumer.clone(), services.pull_scheduler.clone())
        {
            return Err((PushConsumerError::PullScheduler(error.to_string()), None));
        }
        if let Err(error) = self.checkpoint(StartupStage::PullRpcWorkersStarted) {
            return Err((error, None));
        }
        let owner: Arc<dyn ConsumerRuntime> = Arc::new(PushConsumerRegistration {
            group: consumer_group.to_owned(),
            message_model: self.config.message_model(),
            consume_from_where: self.config.consume_from_where(),
            unit_mode: self.config.unit_mode(),
            subscriptions: self.subscriptions.clone(),
            topic_queues: self.topic_queues.clone(),
            offset_store: Arc::downgrade(&offset_store),
            pull_scheduler: services.pull_scheduler.clone(),
            pull_rpc_workers: services.pull_rpc_workers.clone(),
            consumer: services.consumer.clone(),
            runtime: Arc::downgrade(runtime),
        });
        let lease = match runtime.register_consumer(consumer_group, owner).await {
            Ok(lease) => lease,
            Err(error) => return Err((error.into(), None)),
        };
        if let Err(error) = self.checkpoint(StartupStage::ConsumerRegistered) {
            return Err((error, Some(lease)));
        }
        if let Err(error) = runtime.start().await {
            return Err((error.into(), Some(lease)));
        }
        if let Err(error) = self.checkpoint(StartupStage::RuntimeStarted) {
            return Err((error, Some(lease)));
        }
        if let Err(error) = runtime.update_all_topic_routes().await {
            return Err((error.into(), Some(lease)));
        }
        if let Err(error) = self.checkpoint(StartupStage::RoutesUpdated) {
            return Err((error, Some(lease)));
        }
        if let Err(error) = runtime.check_client_in_broker().await {
            return Err((error.into(), Some(lease)));
        }
        if let Err(error) = self.checkpoint(StartupStage::BrokerChecked) {
            return Err((error, Some(lease)));
        }
        match runtime.send_heartbeat_to_all_brokers().await {
            Ok(should_rebalance) => {
                if should_rebalance {
                    runtime.rebalance_immediately();
                }
            }
            Err(error) => return Err((error.into(), Some(lease))),
        }
        if let Err(error) = self.checkpoint(StartupStage::HeartbeatSent) {
            return Err((error, Some(lease)));
        }
        Ok(lease)
    }

    fn checkpoint(&self, stage: StartupStage) -> Result<(), PushConsumerError> {
        #[cfg(test)]
        if *self
            .startup_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            == Some(stage)
        {
            return Err(PushConsumerError::TestStartupFault(stage.name()));
        }
        #[cfg(not(test))]
        let _ = stage;
        Ok(())
    }

    fn copy_subscriptions(&self, consumer_group: &str) -> Result<(), PushConsumerError> {
        let configured = read_lock(&self.subscription_config).clone();
        let configured = configured
            .iter()
            .map(|(topic, expression)| {
                build_subscription(topic, Some(expression), Some("TAG"))
                    .map(|subscription| (topic.clone(), subscription))
                    .map_err(|error| PushConsumerError::InvalidSubscription(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut subscriptions = write_lock(&self.subscriptions);
        subscriptions.extend(configured);
        if self.config.message_model()
            != rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting
        {
            let retry_topic = format!("%RETRY%{consumer_group}");
            let mut subscription =
                SubscriptionData::new(Some(retry_topic.clone()), Some("*".to_owned()));
            subscription.set_sub_version(epoch_millis());
            subscriptions.insert(retry_topic, subscription);
        }
        Ok(())
    }

    fn subscribe_pop_retry_topic(&self, topic: &str, consumer_group: &str) {
        let retry_topic = format!("%RETRY%{consumer_group}_{topic}");
        match build_subscription(&retry_topic, Some("*"), Some("TAG")) {
            Ok(mut subscription) => {
                subscription.set_sub_version(epoch_millis());
                write_lock(&self.subscriptions).insert(retry_topic, subscription);
            }
            Err(error) => {
                tracing::warn!(topic, error = %error, "failed to subscribe POP retry topic")
            }
        }
    }

    fn unsubscribe_pop_retry_topic(&self, topic: &str, consumer_group: &str) {
        let retry_topic = format!("%RETRY%{consumer_group}_{topic}");
        write_lock(&self.subscriptions).remove(&retry_topic);
    }

    fn reconcile_pop_retry_topic_subscription(
        &self,
        topic: &str,
        consumer_group: &str,
        pull_queues: &HashSet<MessageQueue>,
        pop_queues: &HashSet<MessageQueue>,
    ) {
        if topic.starts_with("%RETRY%") {
            return;
        }
        if pop_queues.is_empty() && !pull_queues.is_empty() {
            self.subscribe_pop_retry_topic(topic, consumer_group);
        } else if !pop_queues.is_empty() && pull_queues.is_empty() {
            self.unsubscribe_pop_retry_topic(topic, consumer_group);
        }
    }

    fn select_offset_store(
        &self,
        runtime: &Arc<ClientRuntime>,
        consumer_group: &str,
    ) -> Result<Arc<dyn OffsetStore>, PushConsumerError> {
        let mut configured = self
            .offset_store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(store) = configured.clone() {
            return Ok(store);
        }
        let store: Arc<dyn OffsetStore> = match self.config.message_model() {
            rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting => Arc::new(
                LocalFileOffsetStore::new(runtime.client_id(), consumer_group)?,
            ),
            rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
            | rocketmq_classic_protocol::heartbeat::MessageModel::LiteSelective => Arc::new(
                RemoteBrokerOffsetStore::new(runtime.clone(), consumer_group),
            ),
        };
        *configured = Some(store.clone());
        Ok(store)
    }

    async fn pop_loop(
        self: Arc<Self>,
        process_queue: Arc<PopProcessQueue>,
        consumer_group: String,
        subscription: SubscriptionData,
    ) {
        let queue = process_queue.queue().clone();
        let Some(topic) = queue.topic().map(str::to_owned) else {
            return;
        };
        loop {
            if process_queue.is_dropped() || !self.rebalance_is_started().await {
                return;
            }
            if !read_lock(&self.pop_process_queues)
                .get(&queue)
                .is_some_and(|current| {
                    current.generation() == process_queue.generation()
                        && Arc::ptr_eq(current, &process_queue)
                })
            {
                return;
            }
            let Some(context) = self.pull_context().await else {
                return;
            };
            process_queue.set_last_pop_timestamp(epoch_millis());
            if self.paused.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(
                    PULL_TIME_DELAY_MILLIS_WHEN_SUSPEND as u64,
                ))
                .await;
                continue;
            }
            if pop_queue_wait_ack_flow_controlled(
                process_queue.wait_ack_count(),
                self.config.pop_threshold_for_queue(),
            ) {
                tokio::time::sleep(Duration::from_millis(
                    PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL as u64,
                ))
                .await;
                continue;
            }

            let broker_name = queue.broker_name().unwrap_or_default();
            let broker_address = match self
                .resolve_pop_broker_address(&context.runtime, &topic, broker_name)
                .await
            {
                Ok(Some((_, address))) => address,
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(
                        self.pull_time_delay_millis_when_exception().max(0) as u64,
                    ))
                    .await;
                    continue;
                }
                Err(error) => {
                    tracing::warn!(queue = %queue, error = %error, "push POP broker lookup failed");
                    tokio::time::sleep(Duration::from_millis(
                        self.pull_time_delay_millis_when_exception().max(0) as u64,
                    ))
                    .await;
                    continue;
                }
            };
            let poll_time = BROKER_SUSPEND_MAX_TIME_MILLIS;
            let request = {
                let mut __phase18_header_4 = PopMessageRequestHeader::default();
                __phase18_header_4.set_consumer_group(Some(consumer_group.clone()));
                __phase18_header_4.set_topic(Some(topic.clone()));
                __phase18_header_4.set_queue_id(queue.queue_id());
                __phase18_header_4.set_max_msg_nums(self.config.pop_batch_nums());
                __phase18_header_4.set_invisible_time(self.config.pop_invisible_time_millis());
                __phase18_header_4.set_born_time(epoch_millis());
                __phase18_header_4.set_init_mode(self.consume_init_mode());
                __phase18_header_4
                    .set_exp_type(subscription.get_expression_type().map(str::to_owned));
                __phase18_header_4.set_exp(subscription.get_sub_string().map(str::to_owned));
                __phase18_header_4.set_order(Some(false));
                __phase18_header_4.set_attempt_id(None);
                __phase18_header_4
            };
            let deadline = Instant::now()
                + Duration::from_millis(poll_time.saturating_add(10_000).max(0) as u64);
            let pop_started = Instant::now();
            let reply = context
                .runtime
                .api()
                .pop_message(&broker_address, &request, deadline)
                .await;
            let reply = match reply {
                Ok(reply) => reply,
                Err(error) => {
                    if matches!(
                        &error,
                        rocketmq_classic_core::error::RpcError::Response {
                            code: response::FLOW_CONTROL,
                            ..
                        }
                    ) {
                        tokio::time::sleep(Duration::from_millis(
                            PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL as u64,
                        ))
                        .await;
                    } else {
                        tracing::debug!(queue = %queue, error = %error, "push POP request failed");
                        tokio::time::sleep(Duration::from_millis(
                            self.pull_time_delay_millis_when_exception().max(0) as u64,
                        ))
                        .await;
                    }
                    continue;
                }
            };
            let result = match process_pop_reply(
                reply,
                &request,
                broker_name,
                DecodeOptions {
                    read_body: self.config.client_config().decode_read_body(),
                    decompress_body: self.config.client_config().decode_decompress_body(),
                    ..DecodeOptions::default()
                },
            ) {
                Ok(result) => result,
                Err(error) => {
                    tracing::warn!(queue = %queue, error = %error, "push POP response processing failed");
                    tokio::time::sleep(Duration::from_millis(
                        self.pull_time_delay_millis_when_exception().max(0) as u64,
                    ))
                    .await;
                    continue;
                }
            };
            if result.status() == &PopStatus::PollingFull {
                tokio::time::sleep(Duration::from_millis(
                    self.pull_time_delay_millis_when_exception().max(0) as u64,
                ))
                .await;
                continue;
            }
            if result.status() != &PopStatus::Found {
                continue;
            }
            let messages = result.messages().unwrap_or_default().to_vec();
            if let Some(stats) = self.stats_manager() {
                stats.inc_pull_rt(
                    &consumer_group,
                    &topic,
                    elapsed_millis(pop_started.elapsed()),
                );
                if !messages.is_empty() {
                    stats.inc_pull_tps(&consumer_group, &topic, metric_count(messages.len()));
                }
            }
            let (messages, discarded) = self.filter_pop_messages(&subscription, messages);
            for message in discarded {
                if let Err(error) = self
                    .ack_pop_message(&context.runtime, &message, &topic, &consumer_group)
                    .await
                {
                    tracing::debug!(queue = %queue, error = %error, "failed to ACK a filtered POP message");
                }
            }
            if messages.is_empty() {
                continue;
            }
            process_queue.inc_found_messages(messages.len());
            self.dispatch_pop_messages(
                process_queue.clone(),
                context.runtime.clone(),
                queue.clone(),
                topic.clone(),
                consumer_group.clone(),
                messages,
            )
            .await;
            if self.config.pull_interval_millis() > 0 {
                tokio::time::sleep(Duration::from_millis(
                    self.config.pull_interval_millis() as u64
                ))
                .await;
            }
        }
    }

    async fn resolve_pop_broker_address(
        &self,
        runtime: &Arc<ClientRuntime>,
        topic: &str,
        broker_name: &str,
    ) -> Result<Option<(String, String)>, PushConsumerError> {
        let mut target_broker = broker_name.to_owned();
        let mut route = runtime.route_snapshot()?;
        if broker_name.starts_with("%LMQ%") {
            let logical_queue = MessageQueue::new(topic, broker_name, 0);
            if let Some(broker) = route.broker_name(&logical_queue)? {
                target_broker = broker.to_owned();
            }
        }
        let mut address = route.subscribe_address(&target_broker, 0, true)?;
        if address.is_none() {
            let timeout = i64::from(self.config.client_config().mq_client_api_timeout()).max(0);
            runtime
                .update_topic_route(
                    topic,
                    Instant::now() + Duration::from_millis(timeout as u64),
                )
                .await?;
            route = runtime.route_snapshot()?;
            address = route.subscribe_address(&target_broker, 0, true)?;
        }
        let Some(address) = address else {
            return Ok(None);
        };
        let address = broker_vip_channel(
            self.config.client_config().vip_channel_enabled(),
            &address.address,
        )?;
        Ok(Some((target_broker, address)))
    }

    fn filter_pop_messages(
        &self,
        subscription: &SubscriptionData,
        messages: Vec<MessageExt>,
    ) -> (Vec<MessageExt>, Vec<MessageExt>) {
        let tags = subscription
            .get_tags_set()
            .map(|tags| tags.snapshot())
            .unwrap_or_default();
        let max_reconsume_times = if self.config.max_reconsume_times() == -1 {
            16
        } else {
            self.config.max_reconsume_times()
        };
        let mut accepted = Vec::with_capacity(messages.len());
        let mut discarded = Vec::new();
        for message in messages {
            let tag_matches = subscription.is_class_filter_mode()
                || tags.is_empty()
                || message
                    .tags()
                    .is_some_and(|tag| tags.contains(&Some(tag.to_owned())));
            if !tag_matches {
                discarded.push(message);
            } else {
                accepted.push(message);
            }
        }

        if self.pull_api.has_hook() {
            let before_hooks = accepted.clone();
            let mut context = FilterMessageContext {
                unit_mode: self.config.unit_mode(),
                messages: accepted,
            };
            self.pull_api.execute_hook(&mut context);
            for message in before_hooks {
                if !context.messages.contains(&message) {
                    discarded.push(message);
                }
            }
            accepted = context.messages;
        }

        accepted.retain(|message| {
            if message.reconsume_times() > max_reconsume_times {
                discarded.push(message.clone());
                false
            } else {
                true
            }
        });
        (accepted, discarded)
    }

    async fn dispatch_pop_messages(
        self: &Arc<Self>,
        process_queue: Arc<PopProcessQueue>,
        runtime: Arc<ClientRuntime>,
        queue: MessageQueue,
        route_topic: String,
        consumer_group: String,
        messages: Vec<MessageExt>,
    ) {
        let Some(listener) = read_lock(&self.pop_listener).clone() else {
            process_queue.dec_found_messages(messages.len());
            return;
        };
        let batch_size = self.config.consume_message_batch_max_size().max(1) as usize;
        for batch in messages.chunks(batch_size) {
            let Ok(permit) = self.pop_consume_semaphore.clone().acquire_owned().await else {
                return;
            };
            let consumer = self.clone();
            let process_queue = process_queue.clone();
            let runtime = runtime.clone();
            let queue = queue.clone();
            let route_topic = route_topic.clone();
            let consumer_group = consumer_group.clone();
            let stats_consumer_group = self.consume_stats_group();
            let listener = listener.clone();
            let messages = batch.to_vec();
            let batch_context = PopConsumeBatchContext {
                process_queue,
                runtime,
                queue,
                route_topic,
                consumer_group,
                stats_consumer_group,
            };
            let worker = tokio::spawn(async move {
                let _permit = permit;
                consumer
                    .consume_pop_batch(batch_context, listener, messages)
                    .await;
            });
            let mut workers = self
                .pop_consume_workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            workers.retain(|worker| !worker.is_finished());
            workers.push(worker);
        }
    }

    async fn consume_pop_batch(
        self: Arc<Self>,
        batch_context: PopConsumeBatchContext,
        listener: PushMessageListener,
        mut messages: Vec<MessageExt>,
    ) {
        let PopConsumeBatchContext {
            process_queue,
            runtime,
            queue,
            route_topic,
            consumer_group,
            stats_consumer_group,
        } = batch_context;
        if process_queue.is_dropped() {
            return;
        }
        if matches!(&listener, PushMessageListener::Orderly(_)) {
            return;
        }
        if pop_batch_is_expired(&messages, epoch_millis()) {
            process_queue.dec_found_messages(messages.len());
            tracing::debug!(
                queue = %queue,
                message_count = messages.len(),
                "push POP batch expired before listener dispatch"
            );
            return;
        }
        self.reset_retry_and_namespace(&mut messages, &consumer_group);
        let record_consume_stats = matches!(&listener, PushMessageListener::Concurrently(_));
        let mut consume_context = self.new_consume_message_context(messages.clone(), queue.clone());
        self.consume_message_hooks
            .execute_before(&mut consume_context);
        messages = consume_context.messages.clone().unwrap_or_default();
        let consume_started = Instant::now();
        let (ack_index, delay_level, orderly_delay_millis, status, success, return_type) =
            match listener {
                PushMessageListener::Concurrently(listener) => {
                    let context = ConsumeConcurrentlyContext::new(queue.clone());
                    let listener_messages = messages.clone();
                    let callback_started = Instant::now();
                    let mut callback = AbortOnDrop(tokio::spawn(async move {
                        listener.consume_message(listener_messages, context).await
                    }));
                    match (&mut callback.0).await {
                        Ok((Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)) => (
                            context
                                .ack_index()
                                .min(i32::try_from(messages.len()).unwrap_or(i32::MAX) - 1),
                            context.delay_level_when_next_consume(),
                            None,
                            Some("CONSUME_SUCCESS"),
                            true,
                            if callback_started.elapsed() >= pop_invisible_timeout(&messages) {
                                "TIME_OUT"
                            } else {
                                "SUCCESS"
                            },
                        ),
                        Ok((Some(ConsumeConcurrentlyStatus::ReconsumeLater), context)) => (
                            -1,
                            context.delay_level_when_next_consume(),
                            None,
                            Some("RECONSUME_LATER"),
                            false,
                            if callback_started.elapsed() >= pop_invisible_timeout(&messages) {
                                "TIME_OUT"
                            } else {
                                "FAILED"
                            },
                        ),
                        Ok((None, context)) => (
                            -1,
                            context.delay_level_when_next_consume(),
                            None,
                            Some("RECONSUME_LATER"),
                            false,
                            "RETURNNULL",
                        ),
                        Err(error) => {
                            tracing::warn!(queue = %queue, error = %error, "push POP concurrent listener failed");
                            (-1, 0, None, Some("RECONSUME_LATER"), false, "EXCEPTION")
                        }
                    }
                }
                PushMessageListener::Orderly(listener) => {
                    let context = ConsumeOrderlyContext::new(queue.clone());
                    let listener_messages = messages.clone();
                    let callback_started = Instant::now();
                    let mut callback = AbortOnDrop(tokio::spawn(async move {
                        listener.consume_message(listener_messages, context).await
                    }));
                    match (&mut callback.0).await {
                        Ok((
                            Some(ConsumeOrderlyStatus::Success | ConsumeOrderlyStatus::Commit),
                            context,
                        )) if context.auto_commit() => (
                            i32::try_from(messages.len()).unwrap_or(i32::MAX) - 1,
                            0,
                            None,
                            Some("SUCCESS"),
                            true,
                            if callback_started.elapsed() >= pop_invisible_timeout(&messages) {
                                "TIME_OUT"
                            } else {
                                "SUCCESS"
                            },
                        ),
                        Ok((Some(status), context)) => (
                            -1,
                            0,
                            Some(context.suspend_current_queue_time_millis()),
                            Some(orderly_status_name(status)),
                            false,
                            if callback_started.elapsed() >= pop_invisible_timeout(&messages) {
                                "TIME_OUT"
                            } else if status == ConsumeOrderlyStatus::SuspendCurrentQueueAMoment {
                                "FAILED"
                            } else {
                                "SUCCESS"
                            },
                        ),
                        Ok((None, context)) => (
                            -1,
                            0,
                            Some(context.suspend_current_queue_time_millis()),
                            Some("SUSPEND_CURRENT_QUEUE_A_MOMENT"),
                            false,
                            "RETURNNULL",
                        ),
                        Err(error) => {
                            tracing::warn!(queue = %queue, error = %error, "push POP orderly listener failed");
                            (
                                -1,
                                0,
                                None,
                                Some("SUSPEND_CURRENT_QUEUE_A_MOMENT"),
                                false,
                                "EXCEPTION",
                            )
                        }
                    }
                }
            };
        consume_context
            .properties
            .get_or_insert_with(HashMap::new)
            .insert("CONSUME_CONTEXT_TYPE".to_owned(), return_type.to_owned());
        consume_context.status = status.map(str::to_owned);
        consume_context.success = success;
        consume_context.access_channel = self.config.client_config().access_channel();
        self.consume_message_hooks
            .execute_after(&mut consume_context);

        let message_count = messages.len();
        if record_consume_stats {
            if let (Some(topic), Some(stats)) = (queue.topic(), self.stats_manager()) {
                stats.inc_consume_rt(
                    &stats_consumer_group,
                    topic,
                    elapsed_millis(consume_started.elapsed()),
                );
                if message_count > 0 && return_type != "TIME_OUT" && !process_queue.is_dropped() {
                    if success {
                        let acknowledged = usize::try_from(ack_index.saturating_add(1))
                            .unwrap_or(0)
                            .min(message_count);
                        stats.inc_consume_ok_tps(
                            &stats_consumer_group,
                            topic,
                            metric_count(acknowledged),
                        );
                        stats.inc_consume_failed_tps(
                            &stats_consumer_group,
                            topic,
                            metric_count(message_count.saturating_sub(acknowledged)),
                        );
                    } else {
                        stats.inc_consume_failed_tps(
                            &stats_consumer_group,
                            topic,
                            metric_count(message_count),
                        );
                    }
                }
            }
        }

        let max_reconsume_times = if self.config.max_reconsume_times() == -1 {
            16
        } else {
            self.config.max_reconsume_times()
        };
        let delay_table = pop_delay_seconds();
        for (index, mut message) in messages.into_iter().enumerate() {
            let acknowledged = i32::try_from(index).unwrap_or(i32::MAX) <= ack_index;
            process_queue.ack();
            if acknowledged {
                if let Err(error) = self
                    .ack_pop_message(&runtime, &message, &route_topic, &consumer_group)
                    .await
                {
                    tracing::debug!(queue = %queue, error = %error, "push POP ACK failed");
                }
                continue;
            }

            if message.reconsume_times() >= max_reconsume_times {
                let max_delay_millis = i64::from(*delay_table.last().unwrap_or(&7_200)) * 1_000;
                if epoch_millis().saturating_sub(message.born_timestamp()) > max_delay_millis * 2 {
                    let _ = self
                        .ack_pop_message(&runtime, &message, &route_topic, &consumer_group)
                        .await;
                    continue;
                }
                let age_millis = epoch_millis().saturating_sub(message.born_timestamp());
                let delay_index = delay_table
                    .iter()
                    .enumerate()
                    .rev()
                    .find(|(_, delay)| age_millis >= i64::from(**delay) * 1_000)
                    .map_or(delay_table.len() - 1, |(index, _)| {
                        (index + 1).min(delay_table.len() - 1)
                    });
                self.change_pop_invisible_time(
                    &runtime,
                    &mut message,
                    &route_topic,
                    &consumer_group,
                    i64::from(delay_table[delay_index]) * 1_000,
                )
                .await;
                continue;
            }

            let invisible_time = if let Some(delay_millis) = orderly_delay_millis {
                delay_millis.clamp(10, 30_000)
            } else {
                let delay_index = if delay_level == 0 {
                    usize::try_from(message.reconsume_times().max(0)).unwrap_or(0)
                } else {
                    usize::try_from(delay_level.max(0)).unwrap_or(0)
                };
                i64::from(
                    *delay_table
                        .get(delay_index)
                        .unwrap_or_else(|| delay_table.last().unwrap()),
                ) * 1_000
            };
            self.change_pop_invisible_time(
                &runtime,
                &mut message,
                &route_topic,
                &consumer_group,
                invisible_time,
            )
            .await;
        }
    }

    fn reset_retry_and_namespace_for_pop(&self, messages: &mut [MessageExt], consumer_group: &str) {
        let mut config = self.config.client_config().clone();
        let retry_topic = format!("%RETRY%{consumer_group}");
        for message in messages {
            if message.topic() == retry_topic {
                if let Some(origin_topic) = message.get_property(PROPERTY_RETRY_TOPIC) {
                    message.set_topic(origin_topic);
                }
            }
            if let Ok(Some(topic)) = config.without_namespace(Some(message.topic())) {
                message.set_topic(topic);
            }
        }
    }

    async fn ack_pop_message(
        &self,
        runtime: &Arc<ClientRuntime>,
        message: &MessageExt,
        route_topic: &str,
        consumer_group: &str,
    ) -> Result<AckStatus, PushConsumerError> {
        let raw = message
            .get_property(PROPERTY_POP_CK)
            .ok_or(PushConsumerError::MissingPopReceipt)?;
        let receipt = PopReceipt::parse(raw)?;
        let address = self
            .resolve_pop_broker_address(runtime, route_topic, receipt.broker_name())
            .await?
            .ok_or_else(|| PushConsumerError::PopBrokerNotFound(receipt.broker_name().to_owned()))?
            .1;
        let header = receipt.ack_header(message.topic(), consumer_group);
        Ok(runtime
            .api()
            .ack_message(
                &address,
                &header,
                Instant::now() + Duration::from_millis(3_000),
            )
            .await?)
    }

    async fn change_pop_invisible_time(
        &self,
        runtime: &Arc<ClientRuntime>,
        message: &mut MessageExt,
        route_topic: &str,
        consumer_group: &str,
        invisible_time: i64,
    ) {
        let Some(raw) = message.properties().get(PROPERTY_POP_CK).cloned() else {
            return;
        };
        let Ok(receipt) = PopReceipt::parse(raw) else {
            return;
        };
        let Ok(Some((_, address))) = self
            .resolve_pop_broker_address(runtime, route_topic, receipt.broker_name())
            .await
        else {
            return;
        };
        let header =
            receipt.change_invisible_header(message.topic(), consumer_group, invisible_time);
        match runtime
            .api()
            .change_invisible_time(
                &address,
                receipt.broker_name(),
                &header,
                Instant::now() + Duration::from_millis(3_000),
            )
            .await
        {
            Ok(result) if result.status == AckStatus::Ok => {
                if let Some(extra_info) = result.extra_info {
                    let _ = message.set_system_property(PROPERTY_POP_CK, extra_info);
                }
            }
            Ok(_) => {}
            Err(error) => tracing::debug!(error = %error, "push POP invisibility update failed"),
        }
    }

    pub async fn shutdown(&self) -> Result<(), PushConsumerError> {
        self.shutdown_with_await_termination_millis(
            self.config.await_termination_millis_when_shutdown(),
        )
        .await
    }

    /// Shuts down using the supplied Java `shutdown(long)` wait in milliseconds.
    ///
    /// [`shutdown`](Self::shutdown) uses the configured
    /// `awaitTerminationMillisWhenShutdown` value, matching the public consumer
    /// facade. This method lets callers provide the implementation overload's
    /// per-call wait value.
    pub async fn shutdown_with_await_termination_millis(
        &self,
        await_termination_millis: i64,
    ) -> Result<(), PushConsumerError> {
        let (
            runtime,
            offset_store,
            consume_service,
            orderly_lock_settings,
            pull_scheduler,
            pull_rpc_workers,
        ) = {
            let mut state = self.state.lock().await;
            match self.service_state() {
                Some(ConsumerServiceState::Running) => {}
                Some(ConsumerServiceState::CreateJust)
                | Some(ConsumerServiceState::StartFailed)
                | Some(ConsumerServiceState::ShutdownAlready) => return Ok(()),
                None => return Err(PushConsumerError::ServiceStateNotReady(None)),
            }
            if !matches!(&*state, State::Started { .. }) {
                return Err(PushConsumerError::ServiceStateNotReady(
                    self.service_state(),
                ));
            }
            match std::mem::replace(&mut *state, State::Shutdown) {
                State::Started {
                    runtime,
                    _lease: _,
                    offset_store,
                    consume_service,
                    orderly_lock_settings,
                    pull_scheduler,
                    pull_rpc_workers,
                } => (
                    runtime.upgrade(),
                    offset_store,
                    consume_service,
                    orderly_lock_settings,
                    pull_scheduler,
                    pull_rpc_workers,
                ),
                State::Created | State::Shutdown => unreachable!("state checked before move"),
            }
        };
        if let Some(runtime) = runtime.as_ref() {
            *write_lock(&self.running_info_runtime_snapshot) =
                Some(RunningInfoRuntimeSnapshot::from_runtime(runtime));
        }
        for process_queue in write_lock(&self.pop_process_queues).values() {
            process_queue.drop_assignment();
        }
        write_lock(&self.pop_process_queues).clear();
        let mut pop_workers = self
            .pop_workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain()
            .map(|(_, worker)| worker)
            .collect::<Vec<_>>();
        pop_workers.extend(
            self.pop_retired_workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .drain(..),
        );
        for worker in &pop_workers {
            worker.abort();
        }
        for worker in pop_workers {
            let _ = worker.await;
        }
        let pop_consume_workers = self
            .pop_consume_workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
            .collect::<Vec<_>>();
        shutdown_pop_worker_handles(pop_consume_workers, await_termination_millis).await;
        let process_queues = read_lock(&self.process_queues)
            .iter()
            .map(|(queue, process_queue)| (queue.clone(), process_queue.clone()))
            .collect::<Vec<_>>();
        for (queue, process_queue) in &process_queues {
            let generation = process_queue.generation();
            let _ = process_queue.drop_assignment(generation);
            pull_scheduler.cancel(queue, generation);
        }
        pull_scheduler.stop().await;
        pull_rpc_workers.stop(await_termination_millis).await;
        if let Some(service) = self.custom_consume_message_service() {
            service.shutdown(await_termination_millis).await;
        }
        consume_service.shutdown(await_termination_millis).await;
        let queues = process_queues
            .iter()
            .map(|(queue, _)| queue.clone())
            .collect::<Vec<_>>();
        let persist = offset_store.persist_all(&queues).await;
        let orderly_clustering = orderly_lock_settings.is_some()
            && self.config.message_model()
                == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering;
        let unlock_confirmed = if orderly_clustering && persist.is_ok() {
            if let Some(runtime) = runtime.as_ref() {
                let process_queues = process_queues
                    .iter()
                    .map(|(_, process_queue)| process_queue.clone())
                    .collect::<Vec<_>>();
                self.unlock_orderly_process_queues(runtime, &process_queues)
                    .await
            } else {
                false
            }
        } else {
            !orderly_clustering
        };
        write_lock(&self.process_queues).clear();
        let (unregister, shutdown) = if let Some(runtime) = runtime {
            (
                runtime
                    .unregister_consumer_by_group_nullable(
                        self.config.consumer_group().as_deref(),
                        self.unregister_deadline(),
                    )
                    .await,
                runtime.shutdown().await,
            )
        } else {
            (Ok(true), Ok(true))
        };
        self.shutdown_trace_dispatcher().await;
        persist?;
        if orderly_clustering && !unlock_confirmed {
            return Err(PushConsumerError::OrderlyUnlockFailed);
        }
        unregister?;
        shutdown?;
        self.set_service_state(Some(ConsumerServiceState::ShutdownAlready));
        Ok(())
    }

    pub async fn is_running(&self) -> bool {
        self.service_state() == Some(ConsumerServiceState::Running)
    }

    fn unregister_deadline(&self) -> Instant {
        let timeout = self.config.client_config().mq_client_api_timeout().max(0) as u64;
        Instant::now() + Duration::from_millis(timeout)
    }
}

struct PushConsumerRegistration {
    group: String,
    message_model: rocketmq_classic_protocol::heartbeat::MessageModel,
    consume_from_where: rocketmq_classic_protocol::heartbeat::ConsumeFromWhere,
    unit_mode: bool,
    subscriptions: Arc<RwLock<HashMap<String, SubscriptionData>>>,
    topic_queues: Arc<RwLock<HashMap<String, BTreeSet<MessageQueue>>>>,
    pull_scheduler: Arc<PullScheduler>,
    pull_rpc_workers: Arc<PullRpcWorkers>,
    offset_store: Weak<dyn OffsetStore>,
    consumer: Weak<PushConsumer>,
    runtime: Weak<ClientRuntime>,
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(i64::MAX as u128) as i64
        })
}

fn pop_delay_seconds() -> &'static [i32] {
    &[
        10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1_200, 1_800, 3_600, 7_200,
    ]
}

fn pop_batch_is_expired(messages: &[MessageExt], now_millis: i64) -> bool {
    let Some(raw_receipt) = messages
        .first()
        .and_then(|message| message.get_property(PROPERTY_POP_CK))
    else {
        return true;
    };
    let Ok(receipt) = PopReceipt::parse(raw_receipt) else {
        return true;
    };
    if receipt.pop_time() <= 0 || receipt.invisible_time() <= 0 {
        return true;
    }

    now_millis.saturating_sub(receipt.pop_time()) >= receipt.invisible_time()
}

fn pop_queue_wait_ack_flow_controlled(wait_ack_count: i32, threshold: i32) -> bool {
    i64::from(wait_ack_count) > i64::from(threshold)
}

fn pop_invisible_timeout(messages: &[MessageExt]) -> Duration {
    messages
        .first()
        .and_then(|message| message.get_property(PROPERTY_POP_CK))
        .and_then(|raw| PopReceipt::parse(raw).ok())
        .map(|receipt| Duration::from_millis(receipt.invisible_time().max(0) as u64))
        .unwrap_or(Duration::MAX)
}

fn orderly_status_name(status: ConsumeOrderlyStatus) -> &'static str {
    match status {
        ConsumeOrderlyStatus::Success => "SUCCESS",
        ConsumeOrderlyStatus::Rollback => "ROLLBACK",
        ConsumeOrderlyStatus::Commit => "COMMIT",
        ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => "SUSPEND_CURRENT_QUEUE_A_MOMENT",
    }
}

fn java_string_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

fn initial_offset_plan(
    consume_from_where: ConsumeFromWhere,
    retry_topic: bool,
    stored: OffsetRead,
    timestamp: &str,
) -> Result<InitialOffsetPlan, PushConsumerError> {
    match stored {
        OffsetRead::Found(offset) if offset >= 0 => return Ok(InitialOffsetPlan::Stored(offset)),
        OffsetRead::Found(-1) | OffsetRead::NotFound => {}
        OffsetRead::Found(_) => return Err(PushConsumerError::InvalidOffset),
    }
    match consume_from_where {
        ConsumeFromWhere::ConsumeFromFirstOffset => Ok(InitialOffsetPlan::Fixed(0)),
        ConsumeFromWhere::ConsumeFromTimestamp if retry_topic => Ok(InitialOffsetPlan::MaxOffset),
        ConsumeFromWhere::ConsumeFromTimestamp => {
            let timestamp = parse_java_timestamp_millis(timestamp)
                .ok_or(PushConsumerError::InvalidConsumeTimestamp)?;
            Ok(InitialOffsetPlan::SearchTimestamp(timestamp))
        }
        ConsumeFromWhere::ConsumeFromLastOffset
        | ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst
        | ConsumeFromWhere::ConsumeFromMinOffset
        | ConsumeFromWhere::ConsumeFromMaxOffset
            if retry_topic =>
        {
            Ok(InitialOffsetPlan::Fixed(0))
        }
        ConsumeFromWhere::ConsumeFromLastOffset
        | ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst
        | ConsumeFromWhere::ConsumeFromMinOffset
        | ConsumeFromWhere::ConsumeFromMaxOffset => Ok(InitialOffsetPlan::MaxOffset),
    }
}

#[cfg(test)]
mod tests;
