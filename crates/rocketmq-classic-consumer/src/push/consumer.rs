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

const BROKER_SUSPEND_MAX_TIME_MILLIS: i64 = 20_000;
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

    /// Searches a queue by Java's default lower timestamp boundary.
    pub async fn search_offset(
        &self,
        input_queue: &MessageQueue,
        timestamp_millis: i64,
    ) -> Result<i64, PushConsumerError> {
        let (runtime, queue, broker_name, address, deadline) =
            self.prepare_admin_queue(input_queue).await?;
        let mut header = SearchOffsetRequestHeader::default();
        header.set_topic(Some(
            queue
                .topic()
                .ok_or(PushConsumerError::InvalidQueue)?
                .to_owned(),
        ));
        header.set_queue_id(Some(queue.queue_id()));
        header.set_timestamp(Some(timestamp_millis));
        header.set_boundary_type(Some(BoundaryType::Lower));
        header.set_broker_name(Some(broker_name));
        Ok(runtime
            .api()
            .search_offset(&address, &header, deadline)
            .await?)
    }

    /// Resets each currently subscribed topic to the Broker offsets selected
    /// by Java's `DefaultMQPushConsumerImpl.resetOffsetByTimeStamp` flow.
    pub async fn reset_offset_by_timestamp(
        &self,
        timestamp_millis: i64,
    ) -> Result<(), PushConsumerError> {
        let topics = read_lock(&self.subscriptions)
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        for topic in topics {
            let queues = read_lock(&self.topic_queues)
                .get(&topic)
                .cloned()
                .unwrap_or_default();
            if queues.is_empty() {
                continue;
            }
            let mut offsets = Vec::with_capacity(queues.len());
            for queue in queues {
                let offset = self.search_offset(&queue, timestamp_millis).await?;
                offsets.push((queue, Some(offset)));
            }
            self.reset_consumer_offsets(&topic, offsets).await?;
        }
        Ok(())
    }

    pub async fn get_max_offset(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<i64, PushConsumerError> {
        let (runtime, queue, broker_name, address, deadline) =
            self.prepare_admin_queue(input_queue).await?;
        let mut header = GetMaxOffsetRequestHeader::default();
        header.set_topic(Some(
            queue
                .topic()
                .ok_or(PushConsumerError::InvalidQueue)?
                .to_owned(),
        ));
        header.set_queue_id(Some(queue.queue_id()));
        header.set_committed(true);
        header.set_broker_name(Some(broker_name));
        Ok(runtime
            .api()
            .get_max_offset(&address, &header, deadline)
            .await?)
    }

    pub async fn get_min_offset(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<i64, PushConsumerError> {
        let (runtime, queue, broker_name, address, deadline) =
            self.prepare_admin_queue(input_queue).await?;
        let mut header = GetMinOffsetRequestHeader::default();
        header.set_topic(Some(
            queue
                .topic()
                .ok_or(PushConsumerError::InvalidQueue)?
                .to_owned(),
        ));
        header.set_queue_id(Some(queue.queue_id()));
        header.set_broker_name(Some(broker_name));
        Ok(runtime
            .api()
            .get_min_offset(&address, &header, deadline)
            .await?)
    }

    pub async fn get_earliest_msg_store_time(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<i64, PushConsumerError> {
        let (runtime, queue, broker_name, address, deadline) =
            self.prepare_admin_queue(input_queue).await?;
        let mut header = GetEarliestMsgStoretimeRequestHeader::default();
        header.set_topic(Some(
            queue
                .topic()
                .ok_or(PushConsumerError::InvalidQueue)?
                .to_owned(),
        ));
        header.set_queue_id(Some(queue.queue_id()));
        header.set_broker_name(Some(broker_name));
        Ok(runtime
            .api()
            .get_earliest_msg_storetime(&address, &header, deadline)
            .await?)
    }

    pub async fn create_topic(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        _attributes: Option<&BTreeMap<String, String>>,
    ) -> Result<(), PushConsumerError> {
        self.create_topic_with_sys_flag(key, new_topic, queue_num, 0, None)
            .await
    }

    pub async fn create_topic_with_sys_flag(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        _attributes: Option<&BTreeMap<String, String>>,
    ) -> Result<(), PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let mut client_config = self.config.client_config().clone();
        let topic = client_config
            .with_namespace(Some(new_topic))?
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidAdminTopic(
                "The specified topic is blank.".into(),
            ))?;
        crate::admin::validate_topic(&topic).map_err(PushConsumerError::InvalidAdminTopic)?;
        crate::admin::create_topic(&context.runtime, key, &topic, queue_num, topic_sys_flag)
            .await?;
        Ok(())
    }

    pub async fn fetch_subscribe_message_queues(
        &self,
        input_topic: &str,
    ) -> Result<BTreeSet<MessageQueue>, PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let mut client_config = self.config.client_config().clone();
        let topic = client_config
            .with_namespace(Some(input_topic))?
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidAdminTopic(
                "The specified topic is blank.".into(),
            ))?;
        let timeout = Duration::from_millis(
            i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64,
        );
        let queues =
            crate::admin::fetch_subscribe_queues(&context.runtime, &topic, timeout).await?;
        self.parse_subscribe_message_queues(&queues)
    }

    /// Removes this consumer's namespace from queue topics returned by the
    /// implementation's raw `parseSubscribeMessageQueues` entrypoint.
    pub fn parse_subscribe_message_queues(
        &self,
        queue_set: &BTreeSet<MessageQueue>,
    ) -> Result<BTreeSet<MessageQueue>, PushConsumerError> {
        let mut client_config = self.config.client_config().clone();
        queue_set
            .iter()
            .map(|queue| {
                let topic = client_config.without_namespace(queue.topic())?;
                Ok(MessageQueue::new_nullable(
                    topic,
                    queue.broker_name().map(str::to_owned),
                    queue.queue_id(),
                ))
            })
            .collect::<Result<BTreeSet<_>, rocketmq_classic_core::config::ConfigError>>()
            .map_err(PushConsumerError::from)
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

    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Result<QueryResult, PushConsumerError> {
        self.query_messages(topic, key, max_num, begin_timestamp, end_timestamp, false)
            .await
    }

    /// Returns the consume timestamp span for every Broker in a freshly
    /// queried route, retaining route order and Java's per-Broker failure
    /// behavior.
    pub async fn query_consume_time_span(
        &self,
        topic: &str,
    ) -> Result<Vec<Option<QueueTimeSpan>>, PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let route = context
            .runtime
            .api()
            .get_topic_route(topic, Instant::now() + CONSUME_TIME_SPAN_TIMEOUT)
            .await?
            .ok_or_else(
                || rocketmq_classic_core::error::RpcError::MalformedResponse {
                    request_code: request::GET_ROUTEINFO_BY_TOPIC,
                    peer: topic.to_owned(),
                    reason: "missing route response".into(),
                },
            )?;
        let brokers = route.broker_datas_snapshot().ok_or(
            rocketmq_classic_core::runtime::RuntimeError::InvalidRoute(
                "null BrokerData list in consume-time-span route",
            ),
        )?;
        let mut header = QueryConsumeTimeSpanRequestHeader::default();
        header.set_topic(Some(topic.to_owned()));
        header.set_group(self.group_name());
        let mut spans = Vec::new();
        for broker in brokers {
            let broker = broker.ok_or_else(|| {
                rocketmq_classic_core::error::RpcError::MalformedResponse {
                    request_code: request::QUERY_CONSUME_TIME_SPAN,
                    peer: topic.to_owned(),
                    reason: "null BrokerData in consume-time-span route".into(),
                }
            })?;
            let address = broker.java_select_broker_addr().map_err(|error| {
                rocketmq_classic_core::error::RpcError::MalformedResponse {
                    request_code: request::QUERY_CONSUME_TIME_SPAN,
                    peer: topic.to_owned(),
                    reason: error.to_string(),
                }
            })?;
            let address = address.ok_or_else(|| {
                rocketmq_classic_core::error::RpcError::MalformedResponse {
                    request_code: request::QUERY_CONSUME_TIME_SPAN,
                    peer: topic.to_owned(),
                    reason: "selected Broker address is null".into(),
                }
            })?;
            let request_address =
                broker_vip_channel(self.config.client_config().vip_channel_enabled(), &address)
                    .map_err(
                        |error| rocketmq_classic_core::error::RpcError::MalformedResponse {
                            request_code: request::QUERY_CONSUME_TIME_SPAN,
                            peer: address.clone(),
                            reason: error.to_string(),
                        },
                    )?;
            let response = context
                .runtime
                .api()
                .query_consume_time_span(
                    &request_address,
                    &header,
                    Instant::now() + CONSUME_TIME_SPAN_TIMEOUT,
                )
                .await?;
            let mut response = response.ok_or_else(|| {
                rocketmq_classic_core::error::RpcError::MalformedResponse {
                    request_code: request::QUERY_CONSUME_TIME_SPAN,
                    peer: address,
                    reason: "null consumeTimeSpanSet".into(),
                }
            })?;
            spans.append(&mut response);
        }
        Ok(spans)
    }

    /// Views a message by its physical ID, falling back to Java's unique-key
    /// query when parsing or the physical lookup fails.
    pub async fn view_message(
        &self,
        topic: &str,
        message_id: &str,
    ) -> Result<Option<MessageExt>, PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let mut client_config = self.config.client_config().clone();
        let namespaced_topic = client_config
            .with_namespace(Some(topic))?
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidQueue)?;
        let decoded = decode_message_id(message_id).ok().and_then(|decoded| {
            decoded
                .address()?
                .socket_addr()
                .map(|address| (address, decoded.offset()))
        });
        if let Some((address, offset)) = decoded {
            let header = {
                let mut __phase18_header_0 = ViewMessageRequestHeader::default();
                __phase18_header_0.set_topic(Some(namespaced_topic));
                __phase18_header_0.set_offset(Some(offset));
                __phase18_header_0
            };
            let timeout_millis =
                i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64;
            if let Ok(message) = context
                .runtime
                .api()
                .view_message(
                    &address.to_string(),
                    &header,
                    Instant::now() + Duration::from_millis(timeout_millis),
                )
                .await
            {
                return Ok(Some(message));
            }
        }

        let result = self
            .query_messages(
                topic,
                message_id,
                32,
                unique_key_lookup_begin_timestamp(),
                i64::MAX,
                true,
            )
            .await?;
        Ok(first_query_message(result))
    }

    #[allow(clippy::too_many_arguments)]
    async fn query_messages(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
        unique_key: bool,
    ) -> Result<QueryResult, PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let mut client_config = self.config.client_config().clone();
        let topic = client_config
            .with_namespace(Some(topic))?
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidQueue)?;
        let query = if unique_key {
            AdminQueryRequest::by_unique_key(&topic, key, max_num, begin_timestamp, end_timestamp)
        } else {
            AdminQueryRequest::by_key(&topic, key, max_num, begin_timestamp, end_timestamp)
        };
        let timeout_millis =
            i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64;
        let deadline = Instant::now() + Duration::from_millis(timeout_millis).saturating_mul(4);
        let result = context.runtime.query_message(&query, deadline).await?;
        if let Some(messages) = result.get_message_list() {
            for message in messages.snapshot().into_iter().flatten() {
                message.with_mut(|message| {
                    if let Some(topic) = client_config.without_namespace(Some(message.topic()))? {
                        message.set_topic(topic);
                    }
                    Ok::<(), PushConsumerError>(())
                })?;
            }
        }
        Ok(result)
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

    /// Activates an assignment without a Broker lock for private pipeline tests.
    #[cfg(test)]
    pub(crate) async fn activate_process_queue(
        &self,
        input_queue: &MessageQueue,
        initial_offset: i64,
    ) -> Result<Arc<ProcessQueue>, PushConsumerError> {
        self.activate_process_queue_with_lock(input_queue, initial_offset, None)
            .await
    }

    async fn activate_process_queue_with_lock(
        &self,
        input_queue: &MessageQueue,
        initial_offset: i64,
        initialization_time: Option<(i64, Instant)>,
    ) -> Result<Arc<ProcessQueue>, PushConsumerError> {
        if initial_offset < 0 {
            return Err(PushConsumerError::InvalidOffset);
        }
        let mut client_config = self.config.client_config().clone();
        let queue = client_config.queue_with_namespace(input_queue)?;
        let topic = queue
            .topic()
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidQueue)?;
        if queue.broker_name().is_none_or(str::is_empty) {
            return Err(PushConsumerError::InvalidQueue);
        }
        if !read_lock(&self.subscriptions).contains_key(topic) {
            return Err(PushConsumerError::MissingSubscriptionForQueue(
                topic.to_owned(),
            ));
        }
        let consumer_group = self.consume_stats_group();
        if read_lock(&self.offset_corrections_pending).contains(&queue) {
            return Err(PushConsumerError::OffsetCorrectionPending(
                queue.to_string(),
            ));
        }
        let (scheduler, offset_store) = match &*self.state.lock().await {
            State::Created => return Err(PushConsumerError::NotStarted),
            State::Shutdown => return Err(PushConsumerError::Shutdown),
            State::Started {
                pull_scheduler,
                offset_store,
                ..
            } => (pull_scheduler.clone(), offset_store.clone()),
        };
        if let Some(existing) = read_lock(&self.process_queues).get(&queue).cloned() {
            if existing
                .snapshot(existing.generation())
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                return Ok(existing);
            }
            return Err(PushConsumerError::ProcessQueueRemovalPending(
                queue.to_string(),
            ));
        }
        let generation = self
            .next_generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| PushConsumerError::GenerationExhausted)?;
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), generation));
        process_queue.set_locked(true);
        if let Some((timestamp_millis, initialized_at)) = initialization_time {
            let _ = process_queue.set_lock_state_at(
                generation,
                true,
                timestamp_millis,
                Some(initialized_at),
            );
        }
        {
            let mut queues = write_lock(&self.process_queues);
            if let Some(existing) = queues.get(&queue) {
                if existing
                    .snapshot(existing.generation())
                    .is_ok_and(|snapshot| !snapshot.dropped)
                {
                    return Ok(existing.clone());
                }
                return Err(PushConsumerError::ProcessQueueRemovalPending(
                    queue.to_string(),
                ));
            }
            queues.insert(queue.clone(), process_queue.clone());
        }
        offset_store.update_offset(&queue, initial_offset, false);
        let request = Arc::new(PullRequest::new(
            consumer_group,
            process_queue.clone(),
            initial_offset,
        ));
        if self.paused.load(Ordering::Acquire) {
            scheduler.pause(request.queue(), request.generation());
        }
        if let Err(error) = scheduler.schedule(request, 0) {
            scheduler.cancel(&queue, generation);
            let mut queues = write_lock(&self.process_queues);
            if queues
                .get(&queue)
                .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
            {
                queues.remove(&queue);
                offset_store.remove_offset(&queue);
            }
            return Err(PushConsumerError::PullScheduler(error.to_string()));
        }
        Ok(process_queue)
    }

    pub(crate) async fn remove_process_queue_generation(
        &self,
        input_queue: &MessageQueue,
        generation: u64,
    ) -> bool {
        let mut client_config = self.config.client_config().clone();
        let Ok(queue) = client_config.queue_with_namespace(input_queue) else {
            return false;
        };
        let (scheduler, offset_store, runtime) = match &*self.state.lock().await {
            State::Started {
                runtime,
                offset_store,
                pull_scheduler,
                ..
            } => (
                pull_scheduler.clone(),
                offset_store.clone(),
                runtime.upgrade(),
            ),
            State::Created | State::Shutdown => return false,
        };
        let process_queue = {
            let mut queues = write_lock(&self.process_queues);
            if queues
                .get(&queue)
                .is_none_or(|process_queue| process_queue.generation() != generation)
            {
                return false;
            }
            queues.remove(&queue)
        };
        if let Some(process_queue) = process_queue {
            let _ = process_queue.drop_assignment(generation);
            scheduler.cancel(&queue, generation);
            offset_store.remove_offset(&queue);
            write_lock(&self.offset_corrections_pending).remove(&queue);
            if let Some(runtime) = runtime {
                runtime.rebalance_immediately();
            }
            true
        } else {
            false
        }
    }

    async fn pull_context(&self) -> Option<PullContext> {
        let state = self.state.lock().await;
        match &*state {
            State::Started {
                runtime,
                offset_store,
                pull_scheduler,
                consume_service,
                ..
            } => {
                let runtime = match self
                    .implementation_client_factory
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
                {
                    Some(runtime) => runtime?,
                    None => runtime.upgrade()?,
                };
                Some(PullContext {
                    runtime,
                    offset_store: offset_store.clone(),
                    scheduler: pull_scheduler.clone(),
                    consume_service: consume_service.clone(),
                })
            }
            State::Created | State::Shutdown => None,
        }
    }

    async fn current_orderly_lock_settings(&self) -> Option<OrderlyLockSettings> {
        match &*self.state.lock().await {
            State::Started {
                orderly_lock_settings,
                ..
            } => *orderly_lock_settings,
            State::Created | State::Shutdown => None,
        }
    }

    pub(crate) async fn run_pull_worker_request(self: Arc<Self>, request: Arc<PullRequest>) {
        let Some(context) = self.pull_context().await else {
            return;
        };
        let queue = request.queue().clone();
        let generation = request.generation();
        let process_queue = request.process_queue();
        if !request.is_current() {
            context.scheduler.cancel(&queue, generation);
            return;
        }
        let _ = process_queue.set_last_pull_timestamp_for_assignment(generation, epoch_millis());

        if self.paused.load(Ordering::Acquire) {
            let _ = context
                .scheduler
                .schedule(request, PULL_TIME_DELAY_MILLIS_WHEN_SUSPEND);
            return;
        }

        let orderly_lock_settings = self.current_orderly_lock_settings().await;
        if orderly_lock_settings.is_some()
            && self.config.message_model()
                == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
            && !process_queue
                .is_lock_fresh(
                    generation,
                    orderly_lock_settings
                        .expect("orderly lock settings")
                        .max_live_time_millis,
                )
                .unwrap_or(false)
        {
            let locked = self
                .lock_orderly_process_queues(&context.runtime, std::slice::from_ref(&process_queue))
                .await
                .contains(&queue);
            let delay = if locked {
                10
            } else {
                ORDERLY_LOCK_RETRY_DELAY_MILLIS
            };
            let _ = context.scheduler.schedule(request, delay);
            return;
        }

        if orderly_lock_settings.is_some()
            && self.config.message_model()
                == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
            && !request.previously_locked()
        {
            match self
                .compute_initial_offset(&context.runtime, &context.offset_store, &queue)
                .await
            {
                Ok(offset) => {
                    request.set_next_offset(offset);
                    request.set_previously_locked(true);
                }
                Err(error) => {
                    tracing::warn!(
                        consumer_group = %request.consumer_group(),
                        queue_id = queue.queue_id(),
                        generation,
                        error = %error,
                        "push failed to refresh offset on the first orderly pull after lock"
                    );
                    let _ = context
                        .scheduler
                        .schedule(request, self.pull_time_delay_millis_when_exception());
                    return;
                }
            }
        }

        let thresholds = self.active_pull_queue_thresholds();
        if let Ok(snapshot) = process_queue.snapshot(generation) {
            if let Some(reason) = flow_control_reason(&snapshot, thresholds) {
                tracing::trace!(
                    consumer_group = %request.consumer_group(),
                    queue_id = queue.queue_id(),
                    generation,
                    ?reason,
                    cached_count = snapshot.cached_count,
                    cached_bytes = snapshot.cached_bytes,
                    max_span = snapshot.max_span,
                    "push pull paused by queue flow control"
                );
                let _ = context
                    .scheduler
                    .schedule(request, PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL);
                return;
            }
        }

        let Some(topic) = queue.topic() else {
            let _ = context
                .scheduler
                .schedule(request, self.pull_time_delay_millis_when_exception());
            return;
        };
        let subscription = read_lock(&self.subscriptions).get(topic).cloned();
        let Some(subscription) = subscription else {
            tracing::warn!(
                topic,
                queue_id = queue.queue_id(),
                "push pull has no subscription"
            );
            let _ = context
                .scheduler
                .schedule(request, self.pull_time_delay_millis_when_exception());
            return;
        };

        match self
            .fetch_push_result(&context, &request, &subscription)
            .await
        {
            Ok(result) => {
                self.apply_pull_result(request, context, result).await;
            }
            Err(error) => {
                tracing::debug!(
                    consumer_group = %request.consumer_group(),
                    topic,
                    queue_id = queue.queue_id(),
                    generation,
                    delay_millis = error.delay_millis,
                    error = %error.reason,
                    "push pull request will be retried"
                );
                let _ = context.scheduler.schedule(request, error.delay_millis);
            }
        }
    }

    async fn fetch_push_result(
        &self,
        context: &PullContext,
        request: &PullRequest,
        subscription: &SubscriptionData,
    ) -> Result<crate::pull::ProcessedPullResult, PullAttemptError> {
        let queue = request.queue();
        let topic = queue
            .topic()
            .filter(|topic| !topic.is_empty())
            .ok_or_else(|| self.pull_retry_error("message queue has no topic"))?;
        let broker_name = queue
            .broker_name()
            .filter(|broker| !broker.is_empty())
            .ok_or_else(|| self.pull_retry_error("message queue has no broker name"))?;
        let deadline =
            Instant::now() + Duration::from_millis(CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND as u64);
        let suggested_broker_id = self.pull_api.recalculate_pull_from_which_node(queue);
        let mut route = context
            .runtime
            .route_snapshot()
            .map_err(|error| self.pull_retry_error(error.to_string()))?;
        let mut broker = route
            .subscribe_address(broker_name, suggested_broker_id, false)
            .map_err(|error| self.pull_retry_error(error.to_string()))?;
        if broker.is_none() {
            context
                .runtime
                .update_topic_route(topic, deadline)
                .await
                .map_err(|error| self.pull_retry_error(error.to_string()))?;
            route = context
                .runtime
                .route_snapshot()
                .map_err(|error| self.pull_retry_error(error.to_string()))?;
            broker = route
                .subscribe_address(broker_name, suggested_broker_id, false)
                .map_err(|error| self.pull_retry_error(error.to_string()))?;
        }
        let broker = broker.ok_or_else(|| {
            self.pull_retry_error(format!("broker {broker_name} was not found in route"))
        })?;

        let class_filter = subscription.is_class_filter_mode();
        let sub_expression = if self.config.post_subscription_when_pull() && !class_filter {
            subscription.get_sub_string().map(str::to_owned)
        } else {
            None
        };
        let commit_offset = if self.config.message_model()
            == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
        {
            match context
                .offset_store
                .read_offset(queue, ReadOffsetType::ReadFromMemory)
                .await
                .map_err(|error| self.pull_retry_error(error.to_string()))?
            {
                OffsetRead::Found(offset) if offset > 0 => offset,
                OffsetRead::Found(_) | OffsetRead::NotFound => 0,
            }
        } else {
            0
        };
        let mut sys_flag = build_pull_sys_flag(
            commit_offset > 0,
            true,
            sub_expression.is_some(),
            class_filter,
        );
        let commit_offset = if broker.slave {
            sys_flag = clear_commit_offset_flag(sys_flag);
            0
        } else {
            commit_offset
        };
        let broker_address = if class_filter {
            let route_data = route
                .raw_routes
                .get(topic)
                .ok_or_else(|| self.pull_retry_error("class-filter route was not found"))?
                .snapshot()
                .map_err(|error| self.pull_retry_error(error.to_string()))?;
            resolve_pull_broker_address(&route_data, topic, &broker.address, sys_flag)
                .map_err(|error| self.pull_retry_error(error.to_string()))?
                .ok_or_else(|| self.pull_retry_error("class-filter broker was not found"))?
        } else {
            broker.address
        };
        let mut client_config = self.config.client_config().clone();
        let header = {
            let mut __phase18_header_1 = PullMessageRequestHeader::default();
            __phase18_header_1.set_consumer_group(Some(request.consumer_group().to_owned()));
            __phase18_header_1.set_topic(Some(topic.to_owned()));
            __phase18_header_1.set_queue_id(Some(queue.queue_id()));
            __phase18_header_1.set_queue_offset(Some(request.next_offset()));
            __phase18_header_1.set_max_msg_nums(Some(self.config.pull_batch_size()));
            __phase18_header_1.set_sys_flag(Some(sys_flag));
            __phase18_header_1.set_commit_offset(Some(commit_offset));
            __phase18_header_1.set_suspend_timeout_millis(Some(BROKER_SUSPEND_MAX_TIME_MILLIS));
            __phase18_header_1.set_subscription(sub_expression);
            __phase18_header_1.set_sub_version(Some(subscription.get_sub_version()));
            __phase18_header_1
                .set_expression_type(subscription.get_expression_type().map(str::to_owned));
            __phase18_header_1.set_max_msg_bytes(Some(self.config.pull_batch_size_in_bytes()));
            __phase18_header_1.set_broker_name(Some(broker_name.to_owned()));
            __phase18_header_1
        };
        let pull_started = Instant::now();
        let pull_result = context
            .runtime
            .api()
            .pull_message(&broker_address, &header, deadline)
            .await
            .map_err(|error| self.pull_retry_error_from_rpc(error))?;
        let mut processed = self
            .pull_api
            .process_pull_result(
                queue,
                pull_result,
                subscription,
                DecodeOptions {
                    read_body: client_config.decode_read_body(),
                    decompress_body: client_config.decode_decompress_body(),
                    ..DecodeOptions::default()
                },
            )
            .map_err(|error| self.pull_retry_error(error.to_string()))?;
        if let Some(messages) = processed.messages.as_mut() {
            for message in messages {
                let message_topic = message.topic().to_owned();
                let topic = client_config
                    .without_namespace(Some(&message_topic))
                    .map_err(|error| self.pull_retry_error(error.to_string()))?;
                if let Some(topic) = topic {
                    message.set_topic(topic);
                }
            }
        }
        if processed.result.status == PullStatus::Found {
            let message_count = processed.messages.as_ref().map_or(0, Vec::len);
            self.record_pull_result_stats(request, topic, pull_started.elapsed(), message_count);
        }
        Ok(processed)
    }

    fn record_pull_result_stats(
        &self,
        request: &PullRequest,
        topic: &str,
        elapsed: Duration,
        message_count: usize,
    ) {
        let Some(stats) = self.stats_manager() else {
            return;
        };
        stats.inc_pull_rt(request.consumer_group(), topic, elapsed_millis(elapsed));
        if message_count > 0 {
            stats.inc_pull_tps(request.consumer_group(), topic, metric_count(message_count));
        }
    }

    async fn apply_pull_result(
        self: &Arc<Self>,
        request: Arc<PullRequest>,
        context: PullContext,
        processed: crate::pull::ProcessedPullResult,
    ) {
        let queue = request.queue().clone();
        let generation = request.generation();
        let process_queue = request.process_queue();
        if !request.is_current() {
            context.scheduler.cancel(&queue, generation);
            return;
        }
        let next_offset = processed.result.next_begin_offset;
        request.set_next_offset(next_offset);
        match processed.result.status {
            PullStatus::Found => {
                let Some(messages) = processed.messages else {
                    let _ = context
                        .scheduler
                        .schedule(request, self.pull_time_delay_millis_when_exception());
                    return;
                };
                if messages.is_empty() {
                    let _ = context.scheduler.schedule(request, 0);
                    return;
                }
                if !request.is_current() {
                    return;
                }
                let outcome = match process_queue.insert_messages(generation, messages) {
                    Ok(outcome) => outcome,
                    Err(_) => return,
                };
                if outcome.dispatch_to_consume {
                    tracing::trace!(
                        topic = ?queue.topic(),
                        queue_id = queue.queue_id(),
                        generation,
                        inserted_count = outcome.inserted_count,
                        "push process queue has ready messages"
                    );
                    self.submit_ready_messages(&context, process_queue, generation);
                }
                let delay = self.config.pull_interval_millis();
                let _ = context.scheduler.schedule(request, delay);
            }
            PullStatus::NoNewMessage | PullStatus::NoMatchedMessage => {
                if let Ok(snapshot) = process_queue.snapshot(generation) {
                    if snapshot.cached_count == 0 {
                        context
                            .offset_store
                            .update_offset(&queue, next_offset, true);
                    }
                }
                let _ = context.scheduler.schedule(request, 0);
            }
            PullStatus::OffsetIllegal => {
                if process_queue.drop_assignment(generation) == Ok(true) {
                    write_lock(&self.offset_corrections_pending).insert(queue.clone());
                    context.scheduler.cancel(&queue, generation);
                    self.schedule_offset_recovery(
                        context.runtime,
                        context.offset_store,
                        queue,
                        generation,
                        next_offset,
                    );
                }
            }
        }
    }

    fn submit_consume_batch(
        self: &Arc<Self>,
        context: &PullContext,
        process_queue: Arc<ProcessQueue>,
        generation: u64,
    ) {
        if let Some(service) = self.custom_consume_message_service() {
            let batch = match process_queue.take_batch(
                generation,
                self.config.consume_message_batch_max_size().max(1) as usize,
            ) {
                Ok(Some(batch)) => batch,
                Ok(None) | Err(_) => return,
            };
            service.submit_consume_request(
                batch.messages().to_vec(),
                process_queue,
                batch.queue().clone(),
                true,
            );
            return;
        }
        if self.consume_message_service_is_null() {
            return;
        }
        let ConsumeService::Concurrent(consume_service) = &context.consume_service else {
            return;
        };
        let batch = match process_queue.take_batch(
            generation,
            self.config.consume_message_batch_max_size().max(1) as usize,
        ) {
            Ok(Some(batch)) => batch,
            Ok(None) | Err(_) => return,
        };
        let request = ConsumeRequest {
            process_queue,
            batch,
        };
        match consume_service.try_submit(request) {
            Ok(()) => {}
            Err(ConsumeSubmitError::Full(request)) => {
                self.requeue_unsubmitted_batch(&request);
                self.schedule_consume_retry(
                    context.runtime.clone(),
                    request.process_queue.clone(),
                    request.batch.generation(),
                    PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL,
                );
            }
            Err(ConsumeSubmitError::Closed(request)) => {
                self.requeue_unsubmitted_batch(&request);
            }
        }
    }

    pub(crate) async fn clean_expired_process_queue_messages_at(
        &self,
        now_millis: i64,
    ) -> Result<(), ExpiredMessageCleanupError> {
        if self.is_consume_orderly() {
            return Ok(());
        }
        let timeout_millis = self
            .config
            .consume_timeout_minutes()
            .wrapping_mul(60)
            .wrapping_mul(1_000);
        let process_queues = read_lock(&self.process_queues)
            .values()
            .cloned()
            .collect::<Vec<_>>();

        for process_queue in process_queues {
            let generation = process_queue.generation();
            let attempts = process_queue
                .snapshot(generation)
                .map_or(0, |snapshot| snapshot.cached_count.clamp(0, 16) as usize);
            for _ in 0..attempts {
                let (offset, message) = match process_queue.expired_head_message(
                    generation,
                    now_millis,
                    timeout_millis,
                ) {
                    Ok(Some(expired)) => expired,
                    Ok(None) | Err(ProcessQueueError::StaleGeneration) => break,
                    Err(ProcessQueueError::InvalidConsumeStartTimestamp { offset, value }) => {
                        return Err(ExpiredMessageCleanupError::InvalidConsumeStartTimestamp {
                            offset,
                            value,
                        });
                    }
                    Err(_) => break,
                };
                match self.send_message_back(&message, 3).await {
                    Ok(()) => {
                        let _ = process_queue.remove_head_message(generation, offset, now_millis);
                    }
                    Err(error) => {
                        tracing::warn!(
                            queue = %process_queue.queue(),
                            offset,
                            error = %error,
                            "failed to send expired concurrent-consume message back"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn submit_ready_messages(
        self: &Arc<Self>,
        context: &PullContext,
        process_queue: Arc<ProcessQueue>,
        generation: u64,
    ) {
        if matches!(&context.consume_service, ConsumeService::Orderly(_)) {
            if let Some(service) = self.custom_consume_message_service() {
                let messages = process_queue
                    .take_messages(i32::MAX)
                    .expect("the maximum batch size is nonnegative");
                if !messages.is_empty() {
                    service.submit_consume_request(
                        messages,
                        process_queue.clone(),
                        process_queue.queue().clone(),
                        true,
                    );
                }
                return;
            }
            if self.consume_message_service_is_null() {
                return;
            }
        }
        match &context.consume_service {
            ConsumeService::Concurrent(_) => {
                self.submit_consume_batch(context, process_queue, generation);
            }
            ConsumeService::Orderly(service) => {
                let request = OrderlyConsumeRequest {
                    process_queue,
                    generation,
                };
                match service.try_submit(request) {
                    Ok(()) => {}
                    Err(OrderlySubmitError::Full(request)) => {
                        self.schedule_orderly_work_retry(
                            context.runtime.clone(),
                            request.process_queue,
                            request.generation,
                            100,
                        );
                    }
                    Err(OrderlySubmitError::Closed(request)) => {
                        let _ = request
                            .process_queue
                            .release_consume_dispatch(request.generation);
                    }
                }
            }
        }
    }

    fn schedule_orderly_work_retry(
        self: &Arc<Self>,
        runtime: Arc<ClientRuntime>,
        process_queue: Arc<ProcessQueue>,
        generation: u64,
        delay_millis: i64,
    ) {
        let consumer = Arc::downgrade(self);
        let task_process_queue = process_queue.clone();
        let task = async move {
            let Some(consumer) = consumer.upgrade() else {
                return;
            };
            let Some(context) = consumer.pull_context().await else {
                return;
            };
            consumer.submit_ready_messages(&context, process_queue, generation);
        };
        if let Err(error) = runtime.execute_task_later(task, delay_millis) {
            let _ = task_process_queue.release_consume_dispatch(generation);
            tracing::error!(generation, error = %error, "failed to schedule orderly consume retry");
        }
    }

    fn requeue_unsubmitted_batch(&self, request: &ConsumeRequest) {
        let _ = request.process_queue.complete_batch(
            request.batch.generation(),
            request.batch.id(),
            &[],
        );
    }

    fn schedule_consume_retry(
        self: &Arc<Self>,
        runtime: Arc<ClientRuntime>,
        process_queue: Arc<ProcessQueue>,
        generation: u64,
        delay_millis: i64,
    ) {
        let consumer = Arc::downgrade(self);
        let task = async move {
            let Some(consumer) = consumer.upgrade() else {
                return;
            };
            let Some(context) = consumer.pull_context().await else {
                return;
            };
            consumer.submit_consume_batch(&context, process_queue, generation);
        };
        if let Err(error) = runtime.execute_task_later(task, delay_millis) {
            tracing::debug!(
                generation,
                error = %error,
                "failed to schedule push consume submission retry"
            );
        }
    }

    pub(crate) async fn run_consume_request(
        self: Arc<Self>,
        request: ConsumeRequest,
        listener: Arc<dyn MessageListenerConcurrently>,
    ) {
        let Some(context) = self.pull_context().await else {
            self.requeue_unsubmitted_batch(&request);
            return;
        };
        let generation = request.batch.generation();
        if !request
            .process_queue
            .snapshot(generation)
            .is_ok_and(|snapshot| !snapshot.dropped)
        {
            return;
        }

        let mut messages = request
            .batch
            .messages()
            .iter()
            .map(|message| message.as_ref().clone())
            .collect::<Vec<_>>();
        self.try_reset_pop_retry_topic(&mut messages, &self.consume_stats_group());
        self.reset_retry_and_namespace(
            &mut messages,
            &self.config.consumer_group().unwrap_or_default(),
        );
        let mut consume_context =
            self.new_consume_message_context(messages, request.batch.queue().clone());
        self.consume_message_hooks
            .execute_before(&mut consume_context);
        let consume_start_timestamp_millis = epoch_millis();
        let _ = request.process_queue.mark_batch_started(
            generation,
            request.batch.id(),
            consume_start_timestamp_millis,
        );
        if let Some(messages) = consume_context.messages.as_mut() {
            for message in messages {
                let _ = message.set_system_property(
                    PROPERTY_CONSUME_START_TIMESTAMP,
                    consume_start_timestamp_millis.to_string(),
                );
            }
        }
        let callback_context = ConsumeConcurrentlyContext::new(request.batch.queue().clone());
        let fallback_context = callback_context.clone();
        let listener_messages = consume_context.messages.clone().unwrap_or_default();
        let callback_started = Instant::now();
        let mut callback = AbortOnDrop(tokio::spawn(async move {
            listener
                .consume_message(listener_messages, callback_context)
                .await
        }));
        // Java classifies consume timeout after the listener returns. The
        // timeout is an observation for hooks and statistics, not a reason to
        // cancel the callback.
        let callback_result = (&mut callback.0).await;
        let callback_timed_out =
            consume_timeout_exceeded(callback_started, self.config.consume_timeout_minutes());
        let (status, returned_context, consume_context_type) = match callback_result {
            Ok((status, returned_context)) => {
                let context_type = if status.is_none() {
                    "RETURNNULL"
                } else if callback_timed_out {
                    "TIME_OUT"
                } else {
                    match status {
                        Some(ConsumeConcurrentlyStatus::ConsumeSuccess) => "SUCCESS",
                        Some(ConsumeConcurrentlyStatus::ReconsumeLater) => "FAILED",
                        None => unreachable!("null status was handled above"),
                    }
                };
                (status, returned_context, context_type)
            }
            Err(error) => {
                tracing::error!(
                    topic = ?request.batch.queue().topic(),
                    queue_id = request.batch.queue().queue_id(),
                    generation,
                    error = %error,
                    "push concurrent listener failed"
                );
                (None, fallback_context, "EXCEPTION")
            }
        };
        consume_context
            .properties
            .get_or_insert_with(HashMap::new)
            .insert(
                "CONSUME_CONTEXT_TYPE".to_owned(),
                consume_context_type.to_owned(),
            );
        consume_context.status = Some(
            match status {
                Some(ConsumeConcurrentlyStatus::ConsumeSuccess) => "CONSUME_SUCCESS",
                Some(ConsumeConcurrentlyStatus::ReconsumeLater) | None => "RECONSUME_LATER",
            }
            .to_owned(),
        );
        consume_context.success = status == Some(ConsumeConcurrentlyStatus::ConsumeSuccess);
        consume_context.access_channel = self.config.client_config().access_channel();
        self.consume_message_hooks
            .execute_after(&mut consume_context);
        if let (Some(topic), Some(stats)) = (request.batch.queue().topic(), self.stats_manager()) {
            stats.inc_consume_rt(
                &self.consume_stats_group(),
                topic,
                elapsed_millis(callback_started.elapsed()),
            );
        }
        self.complete_consume_request(request, context, status, returned_context)
            .await;
    }

    pub(crate) async fn run_orderly_consume_request(
        self: Arc<Self>,
        request: OrderlyConsumeRequest,
        listener: Arc<dyn MessageListenerOrderly>,
    ) {
        let process_queue = request.process_queue;
        let generation = request.generation;
        let Some(context) = self.pull_context().await else {
            let _ = process_queue.release_consume_dispatch(generation);
            return;
        };
        let gate = process_queue.orderly_consume_gate();
        let _consume_guard = gate.write_owned().await;
        if !process_queue
            .snapshot(generation)
            .is_ok_and(|snapshot| !snapshot.dropped)
        {
            return;
        }

        let queue = process_queue.queue().clone();
        let orderly_settings = self.current_orderly_lock_settings().await;
        let orderly_clustering = orderly_settings.is_some()
            && self.config.message_model()
                == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering;
        let started_at = Instant::now();
        loop {
            if !process_queue
                .snapshot(generation)
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                return;
            }
            if orderly_settings
                .is_some_and(|settings| settings.max_continuous_consume_exceeded(started_at))
            {
                self.schedule_orderly_work_retry(
                    context.runtime.clone(),
                    process_queue.clone(),
                    generation,
                    10,
                );
                return;
            }

            if orderly_clustering {
                let Some(settings) = orderly_settings else {
                    return;
                };
                let lock_fresh = process_queue
                    .is_lock_fresh(generation, settings.max_live_time_millis)
                    .unwrap_or(false);
                if !lock_fresh {
                    self.schedule_orderly_lock_retry(
                        context.runtime.clone(),
                        process_queue.clone(),
                        generation,
                        10,
                    );
                    return;
                }
            }

            let batch = match process_queue.take_batch(
                generation,
                self.config.consume_message_batch_max_size().max(1) as usize,
            ) {
                Ok(Some(batch)) => batch,
                Ok(None) | Err(_) => return,
            };
            let mut messages = batch
                .messages()
                .iter()
                .map(|message| message.as_ref().clone())
                .collect::<Vec<_>>();
            self.reset_retry_and_namespace_for_group(
                &mut messages,
                &self.config.consumer_group().unwrap_or_default(),
            );
            let mut consume_context =
                self.new_consume_message_context(messages.clone(), queue.clone());
            self.consume_message_hooks
                .execute_before(&mut consume_context);
            messages = consume_context.messages.clone().unwrap_or_default();
            let listener_messages = messages.clone();
            let callback_context = ConsumeOrderlyContext::new(queue.clone());
            let fallback_context = callback_context.clone();
            let listener = listener.clone();
            let callback_started = Instant::now();
            let mut callback = AbortOnDrop(tokio::spawn(async move {
                listener
                    .consume_message(listener_messages, callback_context)
                    .await
            }));
            // Java's consumeTimeout only classifies a completed callback for
            // statistics; it does not interrupt the listener. Keep the queue
            // gate held until the callback actually returns.
            let (status, returned_context, consume_context_type) = match (&mut callback.0).await {
                Ok((status, returned_context)) => {
                    let context_type = match status {
                        Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment) => "FAILED",
                        None => "RETURNNULL",
                        Some(_) => "SUCCESS",
                    };
                    (status, returned_context, context_type)
                }
                Err(error) => {
                    tracing::error!(
                        topic = ?queue.topic(),
                        queue_id = queue.queue_id(),
                        generation,
                        error = %error,
                        "push orderly listener failed"
                    );
                    (None, fallback_context, "EXCEPTION")
                }
            };

            let consume_timed_out =
                consume_timeout_exceeded(callback_started, self.config.consume_timeout_minutes());
            let consume_context_type =
                orderly_consume_return_type(consume_context_type, status, consume_timed_out);
            consume_context
                .properties
                .get_or_insert_with(HashMap::new)
                .insert(
                    "CONSUME_CONTEXT_TYPE".to_owned(),
                    consume_context_type.to_owned(),
                );
            consume_context.status = Some(
                match status {
                    Some(ConsumeOrderlyStatus::Success) => "SUCCESS",
                    Some(ConsumeOrderlyStatus::Rollback) => "ROLLBACK",
                    Some(ConsumeOrderlyStatus::Commit) => "COMMIT",
                    Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment) | None => {
                        "SUSPEND_CURRENT_QUEUE_A_MOMENT"
                    }
                }
                .to_owned(),
            );
            consume_context.success = matches!(
                status,
                Some(ConsumeOrderlyStatus::Success | ConsumeOrderlyStatus::Commit)
            );
            consume_context.access_channel = self.config.client_config().access_channel();
            self.consume_message_hooks
                .execute_after(&mut consume_context);
            if let (Some(topic), Some(stats)) = (queue.topic(), self.stats_manager()) {
                stats.inc_consume_rt(
                    &self.consume_stats_group(),
                    topic,
                    elapsed_millis(callback_started.elapsed()),
                );
            }

            if !process_queue
                .snapshot(generation)
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                return;
            }
            if orderly_clustering {
                let Some(settings) = orderly_settings else {
                    return;
                };
                if !process_queue
                    .is_lock_fresh(generation, settings.max_live_time_millis)
                    .unwrap_or(false)
                {
                    let completion = process_queue.complete_orderly_batch(
                        generation,
                        batch.id(),
                        OrderlyCompletion::RollbackAll,
                    );
                    if completion.is_ok() {
                        self.schedule_orderly_lock_retry(
                            context.runtime.clone(),
                            process_queue.clone(),
                            generation,
                            10,
                        );
                    }
                    return;
                }
            }

            let status = status.unwrap_or(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
            if let Some(topic) = queue.topic() {
                let message_count = batch.offsets().len();
                let (acknowledged, failed) = if returned_context.auto_commit() {
                    match status {
                        ConsumeOrderlyStatus::Success
                        | ConsumeOrderlyStatus::Commit
                        | ConsumeOrderlyStatus::Rollback => (Some(message_count), None),
                        ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => {
                            (None, Some(message_count))
                        }
                    }
                } else {
                    match status {
                        ConsumeOrderlyStatus::Success => (Some(message_count), None),
                        ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => {
                            (None, Some(message_count))
                        }
                        ConsumeOrderlyStatus::Commit | ConsumeOrderlyStatus::Rollback => {
                            (None, None)
                        }
                    }
                };
                if let Some(stats) = self.stats_manager() {
                    let consumer_group = self.consume_stats_group();
                    if let Some(acknowledged) = acknowledged {
                        stats.inc_consume_ok_tps(
                            &consumer_group,
                            topic,
                            metric_count(acknowledged),
                        );
                    }
                    if let Some(failed) = failed {
                        stats.inc_consume_failed_tps(&consumer_group, topic, metric_count(failed));
                    }
                }
            }
            let retry_required = if status == ConsumeOrderlyStatus::SuspendCurrentQueueAMoment {
                self.check_orderly_reconsume_times(
                    &context,
                    &queue,
                    &process_queue,
                    &batch,
                    &messages,
                    generation,
                )
                .await
            } else {
                false
            };
            let disposition =
                orderly_disposition(returned_context.auto_commit(), Some(status), retry_required);
            let completion = match disposition {
                OrderlyDisposition::Commit => OrderlyCompletion::CommitAll,
                OrderlyDisposition::Hold => OrderlyCompletion::HoldCurrent,
                OrderlyDisposition::Rollback => OrderlyCompletion::RollbackAll,
                OrderlyDisposition::Retry => OrderlyCompletion::RequeueCurrent,
            };
            let outcome =
                match process_queue.complete_orderly_batch(generation, batch.id(), completion) {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        tracing::debug!(
                            topic = ?queue.topic(),
                            queue_id = queue.queue_id(),
                            generation,
                            error = %error,
                            "orderly consume result no longer owns its queue batch"
                        );
                        return;
                    }
                };
            if disposition == OrderlyDisposition::Commit {
                if let Some(offset) = outcome.next_commit_offset {
                    if process_queue
                        .snapshot(generation)
                        .is_ok_and(|snapshot| !snapshot.dropped)
                    {
                        context.offset_store.update_offset(&queue, offset, false);
                    }
                }
            }

            match disposition {
                OrderlyDisposition::Rollback => {
                    self.schedule_orderly_work_retry(
                        context.runtime.clone(),
                        process_queue.clone(),
                        generation,
                        suspend_delay_millis(
                            returned_context.suspend_current_queue_time_millis(),
                            self.config.suspend_current_queue_time_millis(),
                        ) as i64,
                    );
                    return;
                }
                OrderlyDisposition::Retry => {
                    self.schedule_orderly_work_retry(
                        context.runtime.clone(),
                        process_queue.clone(),
                        generation,
                        suspend_delay_millis(
                            returned_context.suspend_current_queue_time_millis(),
                            self.config.suspend_current_queue_time_millis(),
                        ) as i64,
                    );
                    return;
                }
                OrderlyDisposition::Commit | OrderlyDisposition::Hold => {
                    if !outcome.has_ready_messages {
                        return;
                    }
                }
            }
        }
    }

    fn reset_retry_and_namespace_for_group(
        &self,
        messages: &mut [MessageExt],
        consumer_group: &str,
    ) {
        let mut config = self.config.client_config().clone();
        let retry_topic = format!("%RETRY%{consumer_group}");
        for message in messages {
            // Java reads RETRY_TOPIC even for ordinary/null topics, which
            // initializes a previously-null properties map.
            let origin_topic = message.get_property(PROPERTY_RETRY_TOPIC);
            if message.topic_nullable() == Some(retry_topic.as_str()) {
                if let Some(origin_topic) = origin_topic {
                    message.set_topic(origin_topic);
                }
            }
            if let Ok(topic) = config.without_namespace(message.topic_nullable()) {
                message.set_topic_nullable(topic);
            }
        }
    }

    async fn check_orderly_reconsume_times(
        &self,
        context: &PullContext,
        queue: &MessageQueue,
        process_queue: &Arc<ProcessQueue>,
        batch: &ConsumeBatch,
        messages: &[MessageExt],
        generation: u64,
    ) -> bool {
        let maximum = self.config.effective_orderly_max_reconsume_times();
        let mut retry_required = false;
        for (offset, message) in batch.offsets().iter().copied().zip(messages) {
            if message.reconsume_times() >= maximum {
                if !self
                    .send_message_back_for_consume(
                        &context.runtime,
                        queue,
                        message,
                        0,
                        SendBackOptions {
                            broker_name_override: None,
                            address_broker_name_override: None,
                            consumer_group_override: None,
                            max_reconsume_times_override: None,
                            timeout_millis: CONSUMER_SEND_BACK_TIMEOUT_MILLIS,
                            orderly: true,
                        },
                    )
                    .await
                {
                    if process_queue
                        .increment_reconsume_times(generation, offset)
                        .is_err()
                    {
                        return true;
                    }
                    retry_required = true;
                }
            } else {
                if process_queue
                    .increment_reconsume_times(generation, offset)
                    .is_err()
                {
                    return true;
                }
                retry_required = true;
            }
        }
        retry_required
    }

    fn schedule_orderly_lock_retry(
        self: &Arc<Self>,
        runtime: Arc<ClientRuntime>,
        process_queue: Arc<ProcessQueue>,
        generation: u64,
        delay_millis: i64,
    ) {
        let consumer = Arc::downgrade(self);
        let task_runtime = runtime.clone();
        let task_queue = process_queue.clone();
        let task = async move {
            let Some(consumer) = consumer.upgrade() else {
                return;
            };
            let Some(context) = consumer.pull_context().await else {
                return;
            };
            if !process_queue
                .snapshot(generation)
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                return;
            }
            let locked = consumer
                .lock_orderly_process_queues(&context.runtime, std::slice::from_ref(&process_queue))
                .await
                .contains(process_queue.queue());
            consumer.schedule_orderly_work_retry(
                context.runtime,
                process_queue,
                generation,
                if locked {
                    10
                } else {
                    ORDERLY_LOCK_RETRY_DELAY_MILLIS
                },
            );
        };
        if let Err(error) = task_runtime.execute_task_later(task, delay_millis) {
            let _ = task_queue.release_consume_dispatch(generation);
            tracing::error!(generation, error = %error, "failed to schedule orderly lock retry");
        }
    }

    async fn complete_consume_request(
        self: &Arc<Self>,
        request: ConsumeRequest,
        context: PullContext,
        status: Option<ConsumeConcurrentlyStatus>,
        callback_context: ConsumeConcurrentlyContext,
    ) {
        let process_queue = request.process_queue;
        let batch = request.batch;
        let generation = batch.generation();
        let queue = batch.queue().clone();
        if !process_queue
            .snapshot(generation)
            .is_ok_and(|snapshot| !snapshot.dropped)
        {
            return;
        }

        let batch_len = batch.offsets().len();
        if batch_len == 0 {
            let _ = process_queue.complete_batch(generation, batch.id(), &[]);
            return;
        }
        let ack_index = match status {
            Some(ConsumeConcurrentlyStatus::ConsumeSuccess) => callback_context
                .ack_index()
                .clamp(-1, batch_len.saturating_sub(1) as i32),
            Some(ConsumeConcurrentlyStatus::ReconsumeLater) | None => -1,
        };
        let acknowledged = (ack_index + 1).max(0) as usize;
        if let Some(topic) = queue.topic() {
            let successful_result = status == Some(ConsumeConcurrentlyStatus::ConsumeSuccess);
            if let Some(stats) = self.stats_manager() {
                let consumer_group = self.consume_stats_group();
                if successful_result {
                    stats.inc_consume_ok_tps(&consumer_group, topic, metric_count(acknowledged));
                }
                stats.inc_consume_failed_tps(
                    &consumer_group,
                    topic,
                    metric_count(batch_len.saturating_sub(acknowledged)),
                );
            }
        }
        let mut terminal_offsets = batch.offsets()[..acknowledged].to_vec();
        let mut local_retry_needed = false;
        if self.config.message_model()
            == rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting
        {
            terminal_offsets.extend_from_slice(&batch.offsets()[acknowledged..]);
        } else {
            for (offset, message) in batch.offsets()[acknowledged..]
                .iter()
                .copied()
                .zip(&batch.messages()[acknowledged..])
            {
                if !process_queue
                    .snapshot(generation)
                    .is_ok_and(|snapshot| !snapshot.dropped)
                {
                    return;
                }
                if self
                    .send_message_back_for_consume(
                        &context.runtime,
                        &queue,
                        message.as_ref(),
                        callback_context.delay_level_when_next_consume(),
                        SendBackOptions {
                            broker_name_override: None,
                            address_broker_name_override: None,
                            consumer_group_override: None,
                            max_reconsume_times_override: None,
                            timeout_millis: CONSUMER_SEND_BACK_TIMEOUT_MILLIS,
                            orderly: false,
                        },
                    )
                    .await
                {
                    terminal_offsets.push(offset);
                } else if process_queue
                    .increment_reconsume_times(generation, offset)
                    .is_err()
                {
                    return;
                } else {
                    local_retry_needed = true;
                }
            }
        }

        let completion =
            match process_queue.complete_batch(generation, batch.id(), &terminal_offsets) {
                Ok(completion) => completion,
                Err(_) => return,
            };
        if let Some(offset) = completion.next_commit_offset {
            if process_queue
                .snapshot(generation)
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                context.offset_store.update_offset(&queue, offset, true);
            }
        }
        if completion.has_ready_messages {
            if local_retry_needed {
                self.schedule_consume_retry(
                    context.runtime,
                    process_queue,
                    generation,
                    CONSUME_RETRY_DELAY_MILLIS,
                );
            } else {
                self.submit_ready_messages(&context, process_queue, generation);
            }
        }
    }

    pub(crate) async fn send_message_back_for_consume(
        &self,
        runtime: &Arc<ClientRuntime>,
        queue: &MessageQueue,
        message: &rocketmq_classic_common::message::MessageExt,
        delay_level: i32,
        options: SendBackOptions<'_>,
    ) -> bool {
        let mut client_config = self.config.client_config().clone();
        let consumer_group = options
            .consumer_group_override
            .filter(|group| !group.trim().is_empty())
            .map(str::to_owned)
            .or_else(|| self.config.consumer_group());
        let origin_topic = client_config
            .with_namespace(Some(message.topic()))
            .ok()
            .flatten();
        let Some(consumer_group) = consumer_group else {
            tracing::error!("push send-back has no consumer group");
            return false;
        };
        let Some(origin_topic) = origin_topic else {
            tracing::error!("push send-back has no origin topic");
            return false;
        };
        let Some(retry_group) = client_config
            .with_namespace(self.config.consumer_group().as_deref())
            .ok()
            .flatten()
        else {
            tracing::error!("push send-back has no retry consumer group");
            return false;
        };
        let max_reconsume_times = options.max_reconsume_times_override.unwrap_or_else(|| {
            if options.orderly {
                self.config.effective_orderly_max_reconsume_times()
            } else {
                self.config.effective_max_reconsume_times()
            }
        });
        if options.orderly {
            let sequence = self.next_retry_message_id.fetch_add(1, Ordering::Relaxed);
            let retry_topic = format!("%RETRY%{retry_group}");
            let retry_message = self.build_retry_message(
                runtime,
                message,
                retry_topic,
                message.topic().to_owned(),
                sequence,
                max_reconsume_times,
            );
            return match self.send_retry_message(runtime, &retry_message).await {
                Ok(()) => true,
                Err(error) => {
                    tracing::error!(
                        topic = ?queue.topic(),
                        queue_id = queue.queue_id(),
                        offset = message.queue_offset(),
                        error = %error,
                        "orderly retry-topic send failed"
                    );
                    false
                }
            };
        }
        let broker_name = match options.broker_name_override {
            Some(broker_name) => broker_name,
            None => message.broker_name().or(queue.broker_name()),
        }
        .map(str::to_owned);
        let header = {
            let mut __phase18_header_2 = ConsumerSendMsgBackRequestHeader::default();
            __phase18_header_2.set_group(consumer_group.clone());
            __phase18_header_2.set_broker_name(broker_name.clone());
            __phase18_header_2.set_origin_topic(origin_topic.clone());
            __phase18_header_2.set_offset(message.commit_log_offset());
            __phase18_header_2.set_delay_level(delay_level);
            __phase18_header_2.set_origin_msg_id(message.message_id().map(str::to_owned));
            __phase18_header_2.set_max_reconsume_times(Some(max_reconsume_times));
            __phase18_header_2.set_unit_mode(self.config.unit_mode());
            __phase18_header_2
        };
        let store_host = message
            .store_host()
            .and_then(MessageHostAddress::socket_address_string);

        match self
            .try_send_message_back_to_broker(
                runtime,
                &header,
                broker_name.as_deref(),
                options.address_broker_name_override,
                store_host.as_deref(),
                options.timeout_millis,
            )
            .await
        {
            Ok(()) => return true,
            Err(error) => tracing::debug!(
                topic = ?queue.topic(),
                queue_id = queue.queue_id(),
                offset = message.queue_offset(),
                error = %error,
                "push send-back Broker attempt failed; trying retry-topic publication"
            ),
        }

        let sequence = self.next_retry_message_id.fetch_add(1, Ordering::Relaxed);
        let retry_topic = format!("%RETRY%{retry_group}");
        let retry_message = self.build_retry_message(
            runtime,
            message,
            retry_topic,
            origin_topic,
            sequence,
            max_reconsume_times,
        );
        match self.send_retry_message(runtime, &retry_message).await {
            Ok(()) => true,
            Err(error) => {
                tracing::error!(
                    topic = ?queue.topic(),
                    queue_id = queue.queue_id(),
                    offset = message.queue_offset(),
                    error = %error,
                    "push send-back and retry-topic fallback both failed"
                );
                false
            }
        }
    }

    async fn try_send_message_back_to_broker(
        &self,
        runtime: &Arc<ClientRuntime>,
        header: &ConsumerSendMsgBackRequestHeader,
        broker_name: Option<&str>,
        address_broker_name_override: Option<Option<&str>>,
        store_host: Option<&str>,
        timeout_millis: u64,
    ) -> Result<(), String> {
        let address_broker_name = address_broker_name_override.unwrap_or(broker_name);
        #[cfg(test)]
        {
            let mut scripted = self
                .scripted_retry_transport
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            scripted.broker_headers.push(header.clone());
            scripted.broker_targets.push((
                address_broker_name.map(str::to_owned),
                store_host.map(str::to_owned),
            ));
            scripted.broker_timeouts_millis.push(timeout_millis);
            if let Some(result) = scripted.broker_results.pop_front() {
                return result;
            }
        }

        let address = if broker_name.is_some() {
            let broker_name = address_broker_name
                .ok_or_else(|| "logical broker has no route endpoint".to_owned())?;
            let route = runtime
                .route_snapshot()
                .map_err(|error| error.to_string())?;
            route
                .publish_address(broker_name)
                .ok_or_else(|| format!("broker {broker_name} has no master address"))?
                .to_owned()
        } else {
            store_host
                .filter(|address| !address.trim().is_empty())
                .map(str::to_owned)
                .ok_or_else(|| "message has no broker name or store host".to_owned())?
        };
        let address =
            broker_vip_channel(self.config.client_config().vip_channel_enabled(), &address)
                .map_err(|error| error.to_string())?;
        let deadline = Instant::now() + Duration::from_millis(timeout_millis);
        runtime
            .api()
            .consumer_send_message_back(&address, header, deadline)
            .await
            .map_err(|error| error.to_string())
    }

    fn build_retry_message(
        &self,
        runtime: &ClientRuntime,
        original: &rocketmq_classic_common::message::MessageExt,
        retry_topic: String,
        origin_topic: String,
        sequence: u64,
        max_reconsume_times: i32,
    ) -> Message {
        let reconsume_times = original.reconsume_times().saturating_add(1);
        let mut retry = Message::new(retry_topic, original.body().to_vec());
        retry.set_flag(original.flag());
        for (key, value) in original.nullable_property_entries() {
            if key.as_deref() != Some(PROPERTY_TRANSACTION_PREPARED) {
                let _ = retry.set_property_nullable(key, value);
            }
        }
        let origin_message_id = original
            .get_property(PROPERTY_ORIGIN_MESSAGE_ID)
            .filter(|value| !value.trim().is_empty())
            .or_else(|| original.message_id().map(str::to_owned));
        let _ = retry.set_system_property(PROPERTY_RETRY_TOPIC, origin_topic);
        if let Some(origin_message_id) = origin_message_id {
            let _ = retry.set_system_property(PROPERTY_ORIGIN_MESSAGE_ID, origin_message_id);
        }
        let _ = retry.set_system_property(PROPERTY_RECONSUME_TIME, reconsume_times.to_string());
        let _ = retry.set_system_property(
            PROPERTY_MAX_RECONSUME_TIMES,
            max_reconsume_times.to_string(),
        );
        let _ = retry.set_system_property(
            PROPERTY_DELAY_TIME_LEVEL,
            3i32.saturating_add(original.reconsume_times()).to_string(),
        );
        if retry
            .properties()
            .get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .is_none_or(|value| value.trim().is_empty())
        {
            let unique_id = format!("{}-{}-{}", runtime.client_id(), epoch_millis(), sequence);
            let _ = retry.set_system_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, unique_id);
        }
        retry
    }

    async fn send_retry_message(
        &self,
        runtime: &Arc<ClientRuntime>,
        retry: &Message,
    ) -> Result<(), String> {
        #[cfg(test)]
        {
            let (_, _, _, encoded_properties) = retry_wire_properties(retry);
            let mut scripted = self
                .scripted_retry_transport
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            scripted.retry_messages.push(retry.clone());
            scripted
                .retry_wire_properties
                .push(encoded_properties.clone());
            if let Some(results) = scripted.retry_attempt_scripts.pop_front() {
                let mut attempts = 0;
                let mut last_error = None;
                for result in results.into_iter().take(INNER_PRODUCER_SYNC_ATTEMPTS) {
                    attempts += 1;
                    match result {
                        Ok(()) => {
                            scripted.retry_attempt_counts.push(attempts);
                            return Ok(());
                        }
                        Err(error) => last_error = Some(error),
                    }
                }
                scripted.retry_attempt_counts.push(attempts);
                return Err(last_error
                    .unwrap_or_else(|| "scripted retry publisher had no send result".to_owned()));
            }
            if let Some(result) = scripted.retry_results.pop_front() {
                scripted.retry_attempt_counts.push(1);
                return result;
            }
        }

        let producer = self.inner_producer(runtime)?;
        let mut retry = retry.clone_message();
        producer
            .send_internal_with_runtime(
                runtime,
                &mut retry,
                Duration::from_millis(INNER_PRODUCER_SEND_TIMEOUT_MILLIS),
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn inner_producer(
        &self,
        runtime: &Arc<ClientRuntime>,
    ) -> Result<Arc<rocketmq_classic_producer::Producer>, String> {
        let matches_runtime = |owner: &Weak<ClientRuntime>| {
            owner
                .upgrade()
                .is_some_and(|owner| Arc::ptr_eq(&owner, runtime))
        };
        {
            let cached = read_lock(&self.inner_producer);
            if let Some((owner, producer)) = cached.as_ref() {
                if matches_runtime(owner) {
                    return Ok(producer.clone());
                }
            }
        }

        let mut cached = write_lock(&self.inner_producer);
        if let Some((owner, producer)) = cached.as_ref() {
            if matches_runtime(owner) {
                return Ok(producer.clone());
            }
        }

        let mut config = rocketmq_classic_producer::ProducerConfig::new("CLIENT_INNER_PRODUCER")
            .map_err(|error| error.to_string())?;
        *config.client_config_mut() = self.config.client_config().clone();
        config.set_tls_config(self.config.tls_config().cloned());
        let producer = runtime
            .get_or_insert_extension(Arc::new(rocketmq_classic_producer::Producer::new(config)));
        *cached = Some((Arc::downgrade(runtime), producer.clone()));
        Ok(producer)
    }

    fn schedule_offset_recovery(
        self: &Arc<Self>,
        runtime: Arc<ClientRuntime>,
        offset_store: Arc<dyn OffsetStore>,
        queue: MessageQueue,
        generation: u64,
        corrected_offset: i64,
    ) {
        offset_store.update_and_freeze_offset(&queue, corrected_offset);
        let consumer = Arc::downgrade(self);
        let retry_runtime = runtime.clone();
        let retry_store = offset_store.clone();
        let retry_queue = queue.clone();
        let task_retry_runtime = retry_runtime.clone();
        let task_retry_store = retry_store.clone();
        let task_retry_queue = retry_queue.clone();
        let task = async move {
            match offset_store.persist(&queue).await {
                Ok(()) => {
                    if let Some(consumer) = consumer.upgrade() {
                        let _ = consumer
                            .remove_process_queue_generation(&queue, generation)
                            .await;
                    }
                }
                Err(error) => {
                    tracing::error!(
                        queue = ?queue,
                        generation,
                        error = %error,
                        "failed to persist corrected push pull offset"
                    );
                    if let Some(consumer) = consumer.upgrade() {
                        consumer.schedule_offset_recovery_retry(
                            task_retry_runtime,
                            task_retry_store,
                            task_retry_queue,
                            generation,
                            corrected_offset,
                        );
                    }
                }
            }
        };
        if let Err(error) = runtime.execute_task(task) {
            tracing::error!(
                queue = ?retry_queue,
                generation,
                error = %error,
                "failed to schedule corrected push offset persistence"
            );
            self.schedule_offset_recovery_retry(
                retry_runtime,
                retry_store,
                retry_queue,
                generation,
                corrected_offset,
            );
        }
    }

    fn schedule_offset_recovery_retry(
        self: &Arc<Self>,
        runtime: Arc<ClientRuntime>,
        offset_store: Arc<dyn OffsetStore>,
        queue: MessageQueue,
        generation: u64,
        corrected_offset: i64,
    ) {
        let consumer = Arc::downgrade(self);
        let task_runtime = runtime.clone();
        let task_queue = queue.clone();
        if let Err(error) = runtime.execute_task_later(
            async move {
                if let Some(consumer) = consumer.upgrade() {
                    consumer.schedule_offset_recovery(
                        task_runtime,
                        offset_store,
                        task_queue,
                        generation,
                        corrected_offset,
                    );
                }
            },
            PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION,
        ) {
            tracing::error!(
                queue = ?queue,
                generation,
                error = %error,
                "failed to reschedule corrected push offset persistence"
            );
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

    pub(crate) async fn rebalance_once(
        self: &Arc<Self>,
        strategy: &dyn crate::rebalance::AllocateMessageQueueStrategy,
        is_order: Option<bool>,
    ) -> bool {
        let Some(context) = self.pull_context().await else {
            return true;
        };
        self.pop_retired_workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|worker| !worker.is_finished());
        let rebalance_runtime = self.get_client_runtime().await;
        let mut balanced = true;
        let mut subscriptions = read_lock(&self.subscriptions)
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        subscriptions.sort_by(|left, right| java_string_cmp(left, right));
        let message_model = self.rebalance_service.message_model();
        let is_order = is_order.unwrap_or_else(|| self.consume_orderly.load(Ordering::Acquire));
        let subscribed_topics = subscriptions.iter().cloned().collect::<HashSet<_>>();
        let topic_queues = read_lock(&self.topic_queues).clone();
        let consumer_group = self.consume_stats_group();

        let mut assignments = Vec::new();
        let mut authoritative_topics = HashSet::new();
        let mut desired_push_queues = HashSet::new();
        let mut desired_pop_queues = HashSet::new();
        let mut missing_route_notifications = Vec::new();
        for topic in &subscriptions {
            if !self.rebalance_is_started().await {
                return true;
            }
            if message_model == rocketmq_classic_protocol::heartbeat::MessageModel::LiteSelective
                && self.client_rebalance(topic)
            {
                // Java's LiteSelective rebalance uses the default no-local-allocation branch;
                // broker assignment is used when client-side rebalance is disabled.
                continue;
            }

            if message_model == rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting {
                let Some(queue_snapshot) = topic_queues.get(topic) else {
                    missing_route_notifications.push(topic.clone());
                    continue;
                };
                let mut all_queues = queue_snapshot.iter().cloned().collect::<Vec<_>>();
                all_queues.sort();
                let all_queue_set = all_queues.iter().cloned().collect::<HashSet<_>>();
                authoritative_topics.insert(topic.clone());
                desired_push_queues.extend(all_queues.iter().cloned());
                assignments.push(DesiredAssignment {
                    topic: topic.clone(),
                    all_queues: Some(all_queue_set.clone()),
                    assigned_queues: all_queue_set.clone(),
                    pull_queues: all_queue_set,
                    pop_queues: HashSet::new(),
                });
                continue;
            }

            if self.client_rebalance(topic) {
                // Java selects local assignment from clientRebalance(topic);
                // `is_order` controls the Broker lock request, not selection.
                // The pinned client handles missing and empty routes before
                // querying membership, notifying empty sets for non-retry topics.
                let Some(queue_snapshot) =
                    topic_queues.get(topic).filter(|queues| !queues.is_empty())
                else {
                    if !topic.starts_with("%RETRY%") {
                        missing_route_notifications.push(topic.clone());
                    }
                    continue;
                };
                let Some(rebalance_runtime) = rebalance_runtime.as_ref() else {
                    balanced = false;
                    continue;
                };
                let consumer_ids = self
                    .find_consumer_id_list_for_rebalance(rebalance_runtime, topic, &consumer_group)
                    .await;
                let mut all_queues = queue_snapshot.iter().cloned().collect::<Vec<_>>();
                all_queues.sort();
                let all_queue_set = all_queues.iter().cloned().collect::<HashSet<_>>();
                let consumer_ids = match consumer_ids {
                    Ok(Some(consumer_ids)) if !consumer_ids.is_empty() => consumer_ids,
                    Ok(Some(_)) | Ok(None) => {
                        // Java's rebalanceByTopic leaves its default `true`
                        // result unchanged when membership is null or empty.
                        // It also leaves the current assignments untouched.
                        continue;
                    }
                    Err(error) => {
                        tracing::warn!(topic, error = %error, "push rebalance consumer list lookup failed");
                        balanced = false;
                        continue;
                    }
                };
                let mut consumer_ids = match consumer_ids.into_iter().collect::<Option<Vec<_>>>() {
                    Some(consumer_ids) => consumer_ids,
                    None => {
                        balanced = false;
                        continue;
                    }
                };
                consumer_ids.sort_by(|left, right| java_string_cmp(left, right));
                let allocation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    strategy.allocate(
                        &consumer_group,
                        rebalance_runtime.client_id(),
                        &all_queues,
                        &consumer_ids,
                    )
                }));
                let allocated = match allocation {
                    Ok(Ok(allocated)) => allocated,
                    Ok(Err(error)) => {
                        tracing::warn!(
                            topic,
                            strategy = strategy.name(),
                            error = %error,
                            "push queue allocation failed"
                        );
                        balanced = false;
                        continue;
                    }
                    Err(_) => {
                        tracing::warn!(
                            topic,
                            strategy = strategy.name(),
                            "push queue allocation panicked"
                        );
                        balanced = false;
                        continue;
                    }
                };
                let assigned = allocated.into_iter().collect::<HashSet<_>>();
                desired_push_queues.extend(assigned.iter().cloned());
                assignments.push(DesiredAssignment {
                    topic: topic.clone(),
                    all_queues: Some(all_queue_set),
                    assigned_queues: assigned.clone(),
                    pull_queues: assigned,
                    pop_queues: HashSet::new(),
                });
                authoritative_topics.insert(topic.clone());
                continue;
            } else {
                // Broker assignment does not depend on the local route table.
                let Some(rebalance_runtime) = rebalance_runtime.as_ref() else {
                    balanced = false;
                    continue;
                };
                let result = rebalance_runtime
                    .query_assignment(
                        topic,
                        &consumer_group,
                        strategy.name(),
                        message_model,
                        3_000,
                    )
                    .await;
                #[allow(clippy::mutable_key_type)]
                let assignment_set = match result {
                    Ok(Some(assignment_set)) => assignment_set,
                    Ok(None) => {
                        balanced = false;
                        continue;
                    }
                    Err(error) => {
                        tracing::warn!(topic, error = %error, "push broker assignment lookup failed");
                        balanced = false;
                        continue;
                    }
                };
                let Some(BrokerAssignmentQueues {
                    assigned,
                    pull: pull_queues,
                    pop: pop_queues,
                }) = partition_broker_assignments(assignment_set)
                else {
                    balanced = false;
                    continue;
                };
                self.reconcile_pop_retry_topic_subscription(
                    topic,
                    &consumer_group,
                    &pull_queues,
                    &pop_queues,
                );
                desired_push_queues.extend(pull_queues.iter().cloned());
                desired_pop_queues.extend(pop_queues.iter().cloned());
                authoritative_topics.insert(topic.clone());
                assignments.push(DesiredAssignment {
                    topic: topic.clone(),
                    all_queues: None,
                    assigned_queues: assigned,
                    pull_queues,
                    pop_queues,
                });
            };
        }

        let current_queues = read_lock(&self.process_queues)
            .iter()
            .map(|(queue, process_queue)| (queue.clone(), process_queue.clone()))
            .collect::<Vec<_>>();
        let mut changed_queues = HashSet::new();
        for (queue, process_queue) in current_queues {
            if !self.rebalance_is_started().await {
                return true;
            }
            let Some(topic) = queue.topic() else {
                balanced = false;
                continue;
            };
            let generation = process_queue.generation();
            let snapshot = match process_queue.snapshot(generation) {
                Ok(snapshot) => snapshot,
                Err(_) => {
                    balanced = false;
                    continue;
                }
            };
            if !subscribed_topics.contains(topic) {
                // Java truncates queues for removed subscriptions separately
                // from assignment cleanup: it retains their stored offsets.
                let _ = process_queue.drop_assignment(generation);
                context.scheduler.cancel(&queue, generation);
                let mut queues = write_lock(&self.process_queues);
                if queues
                    .get(&queue)
                    .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
                {
                    queues.remove(&queue);
                }
                continue;
            }
            let should_remove = authoritative_topics.contains(topic)
                && (!desired_push_queues.contains(&queue)
                    || process_queue.is_pull_expired_with_timeout(
                        self.pull_max_idle_time_millis.load(Ordering::Acquire),
                    ));
            if !should_remove {
                continue;
            }
            if !snapshot.dropped {
                if process_queue.drop_assignment(generation).is_err() {
                    balanced = false;
                    continue;
                }
                context.scheduler.cancel(&queue, generation);
            }
            match self
                .remove_process_queue_safely(&context, &queue, &process_queue)
                .await
            {
                true => {
                    changed_queues.insert(queue);
                }
                false => balanced = false,
            }
        }

        let current_pop_queues = read_lock(&self.pop_process_queues)
            .iter()
            .map(|(queue, process_queue)| (queue.clone(), process_queue.clone()))
            .collect::<Vec<_>>();
        for (queue, process_queue) in current_pop_queues {
            if !self.rebalance_is_started().await {
                return true;
            }
            let Some(topic) = queue.topic() else {
                balanced = false;
                continue;
            };
            let should_remove = !subscribed_topics.contains(topic)
                || (authoritative_topics.contains(topic)
                    && (!desired_pop_queues.contains(&queue)
                        || process_queue.is_pull_expired_with_timeout(
                            self.pop_pull_max_idle_time_millis.load(Ordering::Acquire),
                        )));
            if !should_remove {
                continue;
            }
            process_queue.drop_assignment();
            let removed = {
                let mut queues = write_lock(&self.pop_process_queues);
                if queues
                    .get(&queue)
                    .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
                {
                    queues.remove(&queue);
                    true
                } else {
                    false
                }
            };
            if !removed {
                continue;
            }
            let worker = self
                .pop_workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&queue);
            if let Some(worker) = worker {
                if !worker.is_finished() {
                    self.pop_retired_workers
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(worker);
                }
            }
            changed_queues.insert(queue);
        }

        let mut additions = desired_push_queues.into_iter().collect::<Vec<_>>();
        additions.sort();
        let lock_new_queues = is_order
            && message_model != rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting;
        for queue in additions {
            if !self.rebalance_is_started().await {
                return true;
            }
            let Some(topic) = queue.topic() else {
                balanced = false;
                continue;
            };
            if !subscribed_topics.contains(topic) {
                continue;
            }
            if let Some(existing) = read_lock(&self.process_queues).get(&queue).cloned() {
                if existing
                    .snapshot(existing.generation())
                    .is_ok_and(|snapshot| !snapshot.dropped)
                {
                    continue;
                }
                // A dropped generation stays registered until its persisted
                // offset cleanup succeeds; a replacement waits for that retry.
                balanced = false;
                continue;
            }
            if read_lock(&self.offset_corrections_pending).contains(&queue) {
                balanced = false;
                continue;
            }
            let orderly_locked = if lock_new_queues {
                let Some(rebalance_runtime) = rebalance_runtime.as_ref() else {
                    balanced = false;
                    continue;
                };
                let lock_result = self
                    .lock_orderly_queues_raw(rebalance_runtime, std::slice::from_ref(&queue))
                    .await;
                if lock_result.uncertain.contains(&queue) {
                    self.unlock_orderly_queues_tracking(
                        rebalance_runtime,
                        std::slice::from_ref(&queue),
                    )
                    .await;
                }
                if !lock_result.locked.contains(&queue) {
                    balanced = false;
                    continue;
                }
                let Some(_) = lock_result.lock_times.get(&queue) else {
                    balanced = false;
                    continue;
                };
                if !self.rebalance_is_started().await {
                    self.unlock_orderly_queues_tracking(
                        rebalance_runtime,
                        std::slice::from_ref(&queue),
                    )
                    .await;
                    return true;
                }
                true
            } else {
                false
            };
            context.offset_store.remove_offset(&queue);
            let queue_initialization_time = (epoch_millis(), Instant::now());
            let Some(rebalance_runtime) = rebalance_runtime.as_ref() else {
                balanced = false;
                continue;
            };
            let initial_offset = match self
                .compute_initial_offset(rebalance_runtime, &context.offset_store, &queue)
                .await
            {
                Ok(offset) => offset,
                Err(error) => {
                    tracing::warn!(queue = %queue, error = %error, "push start offset lookup failed");
                    if orderly_locked {
                        self.unlock_orderly_queues_tracking(
                            rebalance_runtime,
                            std::slice::from_ref(&queue),
                        )
                        .await;
                    }
                    balanced = false;
                    continue;
                }
            };
            match self
                .activate_process_queue_with_lock(
                    &queue,
                    initial_offset,
                    Some(queue_initialization_time),
                )
                .await
            {
                Ok(_) => {
                    changed_queues.insert(queue);
                }
                Err(error) => {
                    tracing::warn!(queue = %queue, error = %error, "push queue activation failed");
                    if orderly_locked {
                        self.unlock_orderly_queues_tracking(
                            rebalance_runtime,
                            std::slice::from_ref(&queue),
                        )
                        .await;
                    }
                    balanced = false;
                }
            }
        }

        let mut pop_additions = desired_pop_queues.into_iter().collect::<Vec<_>>();
        pop_additions.sort();
        for queue in pop_additions {
            if !self.rebalance_is_started().await {
                return true;
            }
            let Some(topic) = queue.topic() else {
                balanced = false;
                continue;
            };
            if !subscribed_topics.contains(topic) {
                continue;
            }
            if read_lock(&self.process_queues)
                .get(&queue)
                .is_some_and(|process_queue| {
                    process_queue
                        .snapshot(process_queue.generation())
                        .is_ok_and(|snapshot| !snapshot.dropped)
                })
            {
                balanced = false;
                continue;
            }
            let subscription = match read_lock(&self.subscriptions).get(topic).cloned() {
                Some(subscription) => subscription,
                None => {
                    balanced = false;
                    continue;
                }
            };
            let previous_worker = {
                let mut workers = self
                    .pop_workers
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if workers
                    .get(&queue)
                    .is_some_and(|worker| !worker.is_finished())
                    && read_lock(&self.pop_process_queues).contains_key(&queue)
                {
                    continue;
                }
                workers.remove(&queue)
            };
            if let Some(worker) = previous_worker.filter(|worker| !worker.is_finished()) {
                self.pop_retired_workers
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(worker);
            }

            if read_lock(&self.pop_process_queues).contains_key(&queue) {
                // Java's putIfAbsent does not replace a registered POP queue
                // just because its dropped flag is set. Removal is authorized
                // only by an authoritative assignment update above.
                balanced = false;
                continue;
            }
            let generation = match self.next_generation.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |value| value.checked_add(1),
            ) {
                Ok(generation) => generation,
                Err(_) => {
                    balanced = false;
                    continue;
                }
            };
            let process_queue = Arc::new(PopProcessQueue::new_for_assignment(
                queue.clone(),
                generation,
            ));
            {
                let mut queues = write_lock(&self.pop_process_queues);
                if queues.contains_key(&queue) {
                    balanced = false;
                    continue;
                }
                queues.insert(queue.clone(), process_queue.clone());
            }
            let consumer = self.clone();
            let pop_queue = process_queue.clone();
            let group = consumer_group.clone();
            let worker = tokio::spawn(async move {
                consumer.pop_loop(pop_queue, group, subscription).await;
            });
            self.pop_workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(queue.clone(), worker);
            changed_queues.insert(queue);
        }

        for assignment in &assignments {
            let expected = assignment
                .pull_queues
                .iter()
                .filter(|queue| queue.topic() == Some(assignment.topic.as_str()))
                .cloned()
                .collect::<HashSet<_>>();
            let working = read_lock(&self.process_queues)
                .iter()
                .filter(|(queue, process_queue)| {
                    queue.topic() == Some(assignment.topic.as_str())
                        && process_queue
                            .snapshot(process_queue.generation())
                            .is_ok_and(|snapshot| !snapshot.dropped)
                })
                .map(|(queue, _)| queue.clone())
                .collect::<HashSet<_>>();
            if expected != working {
                balanced = false;
            }
            let expected_pop = assignment
                .pop_queues
                .iter()
                .filter(|queue| queue.topic() == Some(assignment.topic.as_str()))
                .cloned()
                .collect::<HashSet<_>>();
            let working_pop = read_lock(&self.pop_process_queues)
                .iter()
                .filter(|(queue, process_queue)| {
                    queue.topic() == Some(assignment.topic.as_str()) && !process_queue.is_dropped()
                })
                .map(|(queue, _)| queue.clone())
                .collect::<HashSet<_>>();
            if expected_pop != working_pop {
                balanced = false;
            }
            let queue_changed = changed_queues.iter().any(|queue| {
                queue.topic() == Some(assignment.topic.as_str())
                    || assignment.assigned_queues.contains(queue)
            });
            if queue_changed
                && !self
                    .notify_assignment_changed(rebalance_runtime.as_ref(), assignment)
                    .await
            {
                balanced = false;
            }
        }
        for topic in missing_route_notifications {
            if !self.rebalance_is_started().await {
                return true;
            }
            let empty = HashSet::new();
            let assignment = DesiredAssignment {
                topic,
                all_queues: Some(empty.clone()),
                assigned_queues: empty.clone(),
                pull_queues: empty,
                pop_queues: HashSet::new(),
            };
            if !self
                .notify_assignment_changed(rebalance_runtime.as_ref(), &assignment)
                .await
            {
                balanced = false;
            }
        }
        balanced
    }

    async fn rebalance_is_started(&self) -> bool {
        matches!(&*self.state.lock().await, State::Started { .. })
    }

    async fn find_consumer_id_list_for_rebalance(
        &self,
        runtime: &Arc<ClientRuntime>,
        topic: &str,
        consumer_group: &str,
    ) -> Result<Option<Vec<Option<String>>>, rocketmq_classic_core::runtime::RuntimeError> {
        #[cfg(test)]
        {
            let scripted = self
                .rebalance_membership
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_mut(topic)
                .and_then(VecDeque::pop_front);
            if let Some(scripted) = scripted {
                return Ok(scripted.map(|ids| ids.into_iter().map(Some).collect()));
            }
        }
        runtime.find_consumer_id_list(topic, consumer_group).await
    }

    async fn remove_process_queue_safely(
        &self,
        context: &PullContext,
        queue: &MessageQueue,
        process_queue: &Arc<ProcessQueue>,
    ) -> bool {
        let generation = process_queue.generation();
        if !self.rebalance_is_started().await {
            return false;
        }
        let orderly_clustering = self.current_orderly_lock_settings().await.filter(|_| {
            self.config.message_model()
                == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
        });
        let _consume_guard = if orderly_clustering.is_some() {
            let gate = process_queue.orderly_consume_gate();
            match tokio::time::timeout(Duration::from_millis(500), gate.write_owned()).await {
                Ok(guard) => Some(guard),
                Err(_) => {
                    let force_unlock = orderly_clustering.is_some_and(|settings| {
                        process_queue
                            .is_lock_older_than(generation, settings.unlock_delay_millis)
                            .unwrap_or(false)
                    });
                    if force_unlock {
                        None
                    } else {
                        let _ = process_queue.increment_try_unlock_times(generation);
                        return false;
                    }
                }
            }
        } else {
            None
        };
        if let Err(error) = context.offset_store.persist(queue).await {
            tracing::warn!(queue = %queue, generation, error = %error, "push queue offset cleanup deferred");
            return false;
        }
        if orderly_clustering.is_some()
            && !self
                .unlock_orderly_process_queues(
                    &context.runtime,
                    std::slice::from_ref(process_queue),
                )
                .await
        {
            return false;
        }
        let mut queues = write_lock(&self.process_queues);
        if !queues
            .get(queue)
            .is_some_and(|current| Arc::ptr_eq(current, process_queue))
        {
            return false;
        }
        context.offset_store.remove_offset(queue);
        queues.remove(queue);
        write_lock(&self.offset_corrections_pending).remove(queue);
        true
    }

    async fn compute_initial_offset(
        &self,
        runtime: &Arc<ClientRuntime>,
        offset_store: &Arc<dyn OffsetStore>,
        queue: &MessageQueue,
    ) -> Result<i64, PushConsumerError> {
        let stored = offset_store
            .read_offset(queue, ReadOffsetType::ReadFromStore)
            .await?;
        let retry_topic = queue
            .topic()
            .is_some_and(|topic| topic.starts_with("%RETRY%"));
        let consume_timestamp = self.config.consume_timestamp();
        let plan = initial_offset_plan(
            self.config.consume_from_where(),
            retry_topic,
            stored,
            &consume_timestamp,
        )?;
        let offset = match plan {
            InitialOffsetPlan::Stored(offset) | InitialOffsetPlan::Fixed(offset) => offset,
            InitialOffsetPlan::MaxOffset => self.query_max_offset(runtime, queue).await?,
            InitialOffsetPlan::SearchTimestamp(timestamp) => {
                self.query_search_offset(runtime, queue, timestamp).await?
            }
        };
        if offset < 0 {
            return Err(PushConsumerError::InvalidOffset);
        }
        Ok(offset)
    }

    async fn query_max_offset(
        &self,
        runtime: &Arc<ClientRuntime>,
        queue: &MessageQueue,
    ) -> Result<i64, PushConsumerError> {
        let topic = queue.topic().ok_or(PushConsumerError::InvalidQueue)?;
        let broker_name = queue.broker_name().ok_or(PushConsumerError::InvalidQueue)?;
        let deadline = self.offset_rpc_deadline();
        let address = self
            .find_offset_broker(runtime, topic, broker_name, deadline)
            .await?;
        let mut header = GetMaxOffsetRequestHeader::default();
        header.set_topic(Some(topic.to_owned()));
        header.set_queue_id(Some(queue.queue_id()));
        header.set_committed(true);
        header.set_broker_name(Some(broker_name.to_owned()));
        Ok(runtime
            .api()
            .get_max_offset(&address, &header, deadline)
            .await?)
    }

    async fn query_search_offset(
        &self,
        runtime: &Arc<ClientRuntime>,
        queue: &MessageQueue,
        timestamp: i64,
    ) -> Result<i64, PushConsumerError> {
        let topic = queue.topic().ok_or(PushConsumerError::InvalidQueue)?;
        let broker_name = queue.broker_name().ok_or(PushConsumerError::InvalidQueue)?;
        let deadline = self.offset_rpc_deadline();
        let address = self
            .find_offset_broker(runtime, topic, broker_name, deadline)
            .await?;
        let mut header = SearchOffsetRequestHeader::default();
        header.set_topic(Some(topic.to_owned()));
        header.set_lite_topic(None);
        header.set_queue_id(Some(queue.queue_id()));
        header.set_timestamp(Some(timestamp));
        header.set_boundary_type(Some(BoundaryType::Lower));
        header.set_broker_name(Some(broker_name.to_owned()));
        Ok(runtime
            .api()
            .search_offset(&address, &header, deadline)
            .await?)
    }

    async fn find_offset_broker(
        &self,
        runtime: &Arc<ClientRuntime>,
        topic: &str,
        broker_name: &str,
        deadline: Instant,
    ) -> Result<String, PushConsumerError> {
        let mut route = runtime.route_snapshot()?;
        let mut address = route.publish_address(broker_name).map(str::to_owned);
        if address.is_none() {
            runtime.update_topic_route(topic, deadline).await?;
            route = runtime.route_snapshot()?;
            address = route.publish_address(broker_name).map(str::to_owned);
        }
        address.ok_or_else(|| OffsetStoreError::BrokerNotFound(broker_name.to_owned()).into())
    }

    async fn prepare_admin_queue(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<(Arc<ClientRuntime>, MessageQueue, String, String, Instant), PushConsumerError>
    {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let queue = self
            .config
            .client_config()
            .clone()
            .queue_with_namespace(input_queue)?;
        let topic = queue
            .topic()
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidQueue)?;
        let broker_name = queue
            .broker_name()
            .filter(|broker| !broker.is_empty())
            .ok_or(PushConsumerError::InvalidQueue)?
            .to_owned();
        let deadline = self.offset_rpc_deadline();
        let address = self
            .find_offset_broker(&context.runtime, topic, &broker_name, deadline)
            .await?;
        Ok((context.runtime, queue, broker_name, address, deadline))
    }

    async fn request_orderly_lock_batch(
        &self,
        runtime: &Arc<ClientRuntime>,
        broker_name: &str,
        topic: &str,
        body: &LockBatchRequestBody,
    ) -> Result<Option<HashSet<Option<MessageQueue>>>, String> {
        #[cfg(test)]
        {
            let (waiter, result, requested) = {
                let mut scripted = self
                    .scripted_lock_transport
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                scripted.lock_bodies.push(body.clone());
                (
                    scripted.lock_waiters.pop_front(),
                    scripted.lock_results.pop_front(),
                    scripted.lock_requested.clone(),
                )
            };
            requested.notify_one();
            if let Some(waiter) = waiter {
                return waiter
                    .await
                    .map_err(|_| "scripted lock response sender dropped".to_owned())?;
            }
            if let Some(result) = result {
                return result;
            }
        }

        let deadline = Instant::now() + Duration::from_millis(ORDERLY_LOCK_RPC_TIMEOUT_MILLIS);
        let address = self
            .find_offset_broker(runtime, topic, broker_name, deadline)
            .await
            .map_err(|error| error.to_string())?;
        let address =
            broker_vip_channel(self.config.client_config().vip_channel_enabled(), &address)
                .map_err(|error| error.to_string())?;
        runtime
            .api()
            .lock_batch_mq(&address, body, deadline)
            .await
            .map_err(|error| error.to_string())
    }

    async fn request_orderly_unlock_batch(
        &self,
        runtime: &Arc<ClientRuntime>,
        broker_name: &str,
        topic: &str,
        body: &UnlockBatchRequestBody,
        oneway: bool,
    ) -> Result<(), String> {
        #[cfg(test)]
        {
            let mut scripted = self
                .scripted_lock_transport
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            scripted.unlock_bodies.push(body.clone());
            if let Some(result) = scripted.unlock_results.pop_front() {
                return result;
            }
        }

        let deadline = Instant::now() + Duration::from_millis(ORDERLY_LOCK_RPC_TIMEOUT_MILLIS);
        let address = self
            .find_offset_broker(runtime, topic, broker_name, deadline)
            .await
            .map_err(|error| error.to_string())?;
        let address =
            broker_vip_channel(self.config.client_config().vip_channel_enabled(), &address)
                .map_err(|error| error.to_string())?;
        runtime
            .api()
            .unlock_batch_mq(&address, body, oneway, deadline)
            .await
            .map_err(|error| error.to_string())
    }

    fn orderly_group_fields(
        &self,
        runtime: &ClientRuntime,
    ) -> Result<(String, String), PushConsumerError> {
        let group = self.consume_stats_group();
        if group.is_empty() {
            return Err(PushConsumerError::InvalidConsumerGroup);
        }
        Ok((group, runtime.client_id().to_owned()))
    }

    async fn lock_orderly_queues_raw(
        &self,
        runtime: &Arc<ClientRuntime>,
        queues: &[MessageQueue],
    ) -> OrderlyLockBatchResult {
        let mut result = OrderlyLockBatchResult::default();
        let mut groups = BTreeMap::<String, (String, HashSet<Option<MessageQueue>>)>::new();
        for queue in queues {
            let (Some(broker), Some(topic)) = (queue.broker_name(), queue.topic()) else {
                continue;
            };
            let group = groups
                .entry(broker.to_owned())
                .or_insert_with(|| (topic.to_owned(), HashSet::new()));
            group.1.insert(Some(queue.clone()));
        }
        let (consumer_group, client_id) = match self.orderly_group_fields(runtime) {
            Ok(fields) => fields,
            Err(error) => {
                tracing::error!(error = %error, "cannot build orderly lock request identity");
                result.uncertain.extend(queues.iter().cloned());
                return result;
            }
        };

        for (broker_name, (topic, requested)) in groups {
            let requested_queues = requested
                .iter()
                .filter_map(|queue| queue.as_ref().cloned())
                .collect::<Vec<_>>();
            let mut body = LockBatchRequestBody::default();
            body.set_consumer_group(Some(consumer_group.clone()));
            body.set_client_id(Some(client_id.clone()));
            body.set_only_this_broker(false);
            body.set_mq_set(Some(SharedValue::new(
                requested
                    .iter()
                    .cloned()
                    .map(|queue| queue.map(SharedValue::new))
                    .collect(),
            )));
            match self
                .request_orderly_lock_batch(runtime, &broker_name, &topic, &body)
                .await
            {
                Ok(Some(locked_queues)) => {
                    result.responded.extend(requested_queues.iter().cloned());
                    let received_at = Instant::now();
                    let received_at_millis = epoch_millis();
                    for queue in requested_queues {
                        if locked_queues.contains(&Some(queue.clone())) {
                            result.locked.insert(queue.clone());
                            result
                                .lock_times
                                .insert(queue, (received_at_millis, received_at));
                        }
                    }
                }
                // Java's LockBatchResponseBody getter is nullable. Its caller
                // treats a null set as an exceptional response and preserves
                // the previous ProcessQueue lock state.
                Ok(None) => {}
                Err(error) => {
                    result.uncertain.extend(requested_queues);
                    tracing::warn!(
                        broker = %broker_name,
                        error,
                        "orderly batch lock request failed"
                    );
                }
            }
        }
        result
    }

    async fn unlock_orderly_queues_raw(
        &self,
        runtime: &Arc<ClientRuntime>,
        queues: &[MessageQueue],
        oneway: bool,
    ) -> HashSet<MessageQueue> {
        let mut groups = BTreeMap::<String, (String, HashSet<Option<MessageQueue>>)>::new();
        for queue in queues {
            let (Some(broker), Some(topic)) = (queue.broker_name(), queue.topic()) else {
                continue;
            };
            let group = groups
                .entry(broker.to_owned())
                .or_insert_with(|| (topic.to_owned(), HashSet::new()));
            group.1.insert(Some(queue.clone()));
        }
        let (consumer_group, client_id) = match self.orderly_group_fields(runtime) {
            Ok(fields) => fields,
            Err(error) => {
                tracing::error!(error = %error, "cannot build orderly unlock request identity");
                return HashSet::new();
            }
        };

        let mut unlocked = HashSet::new();
        for (broker_name, (topic, requested)) in groups {
            let mut body = UnlockBatchRequestBody::default();
            body.set_consumer_group(Some(consumer_group.clone()));
            body.set_client_id(Some(client_id.clone()));
            body.set_only_this_broker(false);
            body.set_mq_set(Some(SharedValue::new(
                requested
                    .iter()
                    .cloned()
                    .map(|queue| queue.map(SharedValue::new))
                    .collect(),
            )));
            match self
                .request_orderly_unlock_batch(runtime, &broker_name, &topic, &body, oneway)
                .await
            {
                Ok(()) => {
                    unlocked.extend(requested.into_iter().flatten());
                }
                Err(error) => {
                    tracing::warn!(
                        broker = %broker_name,
                        error,
                        "orderly batch unlock request failed"
                    );
                }
            }
        }
        unlocked
    }

    async fn lock_orderly_process_queues(
        &self,
        runtime: &Arc<ClientRuntime>,
        process_queues: &[Arc<ProcessQueue>],
    ) -> HashSet<MessageQueue> {
        let mut attempts = Vec::new();
        for process_queue in process_queues {
            let generation = process_queue.generation();
            if !process_queue
                .snapshot(generation)
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                continue;
            }
            if let Ok(attempt_id) = process_queue.begin_lock_attempt(generation) {
                attempts.push((
                    process_queue.queue().clone(),
                    process_queue.clone(),
                    generation,
                    attempt_id,
                ));
            }
        }
        let queues = attempts
            .iter()
            .map(|(queue, _, _, _)| queue.clone())
            .collect::<Vec<_>>();
        let lock_result = self.lock_orderly_queues_raw(runtime, &queues).await;
        let mut applied_locked = HashSet::new();
        let mut stale_locked = Vec::new();
        for (queue, process_queue, generation, attempt_id) in attempts {
            if !lock_result.responded.contains(&queue) {
                continue;
            }
            let returned_locked = lock_result.locked.contains(&queue);
            let applied = if returned_locked {
                let Some((timestamp_millis, lock_instant)) =
                    lock_result.lock_times.get(&queue).copied()
                else {
                    continue;
                };
                process_queue.complete_lock_attempt_at(
                    generation,
                    attempt_id,
                    true,
                    timestamp_millis,
                    lock_instant,
                )
            } else {
                process_queue.complete_lock_attempt(generation, attempt_id, false)
            };
            match applied {
                Ok(LockAttemptResult::Applied) if returned_locked => {
                    applied_locked.insert(queue);
                }
                Ok(LockAttemptResult::Dropped) if returned_locked => {
                    stale_locked.push((queue, process_queue, generation));
                }
                Ok(LockAttemptResult::Applied | LockAttemptResult::Superseded)
                | Ok(LockAttemptResult::Dropped)
                | Err(_) => {}
            }
        }
        if !stale_locked.is_empty() {
            let queues = stale_locked
                .iter()
                .map(|(queue, _, _)| queue.clone())
                .collect::<Vec<_>>();
            let unlocked = self
                .unlock_orderly_queues_raw(runtime, &queues, false)
                .await;
            for (queue, process_queue, generation) in stale_locked {
                if unlocked.contains(&queue) {
                    let _ = process_queue.record_unlocked(generation);
                    write_lock(&self.pending_orderly_unlocks).remove(&queue);
                } else {
                    write_lock(&self.pending_orderly_unlocks).insert(queue);
                }
            }
        }
        applied_locked
    }

    async fn unlock_orderly_queues_tracking(
        &self,
        runtime: &Arc<ClientRuntime>,
        queues: &[MessageQueue],
    ) -> bool {
        if queues.is_empty() {
            return true;
        }
        write_lock(&self.pending_orderly_unlocks).extend(queues.iter().cloned());
        let unlocked = self.unlock_orderly_queues_raw(runtime, queues, false).await;
        let mut pending = write_lock(&self.pending_orderly_unlocks);
        for queue in &unlocked {
            pending.remove(queue);
        }
        queues.iter().all(|queue| unlocked.contains(queue))
    }

    async fn unlock_orderly_process_queues(
        &self,
        runtime: &Arc<ClientRuntime>,
        process_queues: &[Arc<ProcessQueue>],
    ) -> bool {
        let mut targets = process_queues
            .iter()
            .map(|process_queue| (process_queue.queue().clone(), Some(process_queue.clone())))
            .collect::<HashMap<_, _>>();
        {
            let pending = read_lock(&self.pending_orderly_unlocks);
            for queue in pending.iter() {
                targets.entry(queue.clone()).or_insert(None);
            }
        }
        let queues = targets.keys().cloned().collect::<Vec<_>>();
        if queues.is_empty() {
            return true;
        }
        write_lock(&self.pending_orderly_unlocks).extend(queues.iter().cloned());
        let unlocked = self
            .unlock_orderly_queues_raw(runtime, &queues, false)
            .await;
        for (queue, process_queue) in targets {
            if unlocked.contains(&queue) {
                if let Some(process_queue) = process_queue {
                    let _ = process_queue.record_unlocked(process_queue.generation());
                }
                write_lock(&self.pending_orderly_unlocks).remove(&queue);
            } else if let Some(process_queue) = process_queue {
                let _ = process_queue.increment_try_unlock_times(process_queue.generation());
            }
        }
        queues.iter().all(|queue| unlocked.contains(queue))
    }

    async fn retry_pending_orderly_unlocks(&self, runtime: &Arc<ClientRuntime>) {
        let queues = read_lock(&self.pending_orderly_unlocks)
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let unlocked = self
            .unlock_orderly_queues_raw(runtime, &queues, false)
            .await;
        let mut pending = write_lock(&self.pending_orderly_unlocks);
        for queue in unlocked {
            pending.remove(&queue);
        }
    }

    pub(crate) async fn renew_orderly_locks(self: &Arc<Self>) {
        let Some(context) = self.pull_context().await else {
            return;
        };
        if self.current_orderly_lock_settings().await.is_none()
            || self.config.message_model()
                != rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
        {
            return;
        }
        let process_queues = read_lock(&self.process_queues)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        self.lock_orderly_process_queues(&context.runtime, &process_queues)
            .await;
        self.retry_pending_orderly_unlocks(&context.runtime).await;
    }

    fn offset_rpc_deadline(&self) -> Instant {
        Instant::now()
            + Duration::from_millis(
                self.config.client_config().mq_client_api_timeout().max(0) as u64
            )
    }

    async fn notify_assignment_changed(
        &self,
        runtime: Option<&Arc<ClientRuntime>>,
        assignment: &DesiredAssignment,
    ) -> bool {
        if let Some(subscription) = write_lock(&self.subscriptions).get_mut(&assignment.topic) {
            subscription.set_sub_version(epoch_millis());
        }
        self.update_pull_thresholds_after_rebalance();
        let Some(runtime) = runtime else {
            return false;
        };
        let mut successful = true;
        if let Err(error) = runtime.send_heartbeat_to_all_brokers().await {
            tracing::warn!(topic = %assignment.topic, error = %error, "push subscription heartbeat after rebalance failed");
            successful = false;
        }
        if let Some(listener) = self.config.message_queue_listener() {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                listener(
                    &assignment.topic,
                    assignment.all_queues.as_ref(),
                    &assignment.assigned_queues,
                );
            }));
            if outcome.is_err() {
                tracing::warn!(topic = %assignment.topic, "push message queue listener panicked");
                successful = false;
            }
        }
        successful
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

impl ConsumerRuntime for PushConsumerRegistration {
    fn reset_offset(
        &self,
        topic: Option<String>,
        offsets: Option<rocketmq_classic_core::runtime::ConsumerOffsetTable>,
    ) -> Option<rocketmq_classic_core::runtime::ConsumerOperationFuture<'_, ()>> {
        let consumer = self.consumer.upgrade();
        Some(Box::pin(async move {
            let consumer = consumer.ok_or_else(|| {
                RuntimeError::ConsumerOperation("push consumer is unavailable".into())
            })?;
            consumer
                .reset_consumer_offsets_shared(topic.as_deref(), offsets)
                .await
                .map_err(|error| match error {
                    PushConsumerError::Runtime(error) => error,
                    error => RuntimeError::ConsumerOperation(error.to_string()),
                })
        }))
    }

    fn consume_message_directly(
        &self,
        message: Option<MessageExt>,
        broker_name: Option<String>,
    ) -> Option<
        rocketmq_classic_core::runtime::ConsumerOperationFuture<
            '_,
            Option<ConsumeMessageDirectlyResult>,
        >,
    > {
        let consumer = self.consumer.upgrade();
        Some(Box::pin(async move {
            let consumer = consumer.ok_or_else(|| {
                RuntimeError::ConsumerOperation("push consumer is unavailable".into())
            })?;
            let message = message.ok_or_else(|| {
                RuntimeError::ConsumerOperation("null direct-consume message".into())
            })?;
            consumer
                .consume_message_directly(message, broker_name)
                .await
                .map(Some)
                .ok_or_else(|| {
                    RuntimeError::ConsumerOperation("consume message service is unavailable".into())
                })
        }))
    }

    fn consumer_running_info(&self) -> Result<ConsumerRunningInfoBody, RuntimeError> {
        let consumer = self.consumer.upgrade().ok_or_else(|| {
            RuntimeError::ConsumerOperation("push consumer is unavailable".into())
        })?;
        let runtime = self.runtime.upgrade().ok_or_else(|| {
            RuntimeError::ConsumerOperation("consumer runtime is unavailable".into())
        })?;
        Ok(consumer.running_info_body(&runtime, None, consumer.get_offset_store()))
    }

    fn consumer_status(
        &self,
        topic: Option<&str>,
    ) -> Result<HashMap<MessageQueue, i64>, RuntimeError> {
        // The raw implementation setter is live after startup. Read its
        // current field rather than the store captured at registration.
        let store = match self.consumer.upgrade() {
            Some(consumer) => consumer.get_offset_store(),
            None => self.offset_store.upgrade(),
        };
        store
            .map(|store| store.clone_offset_table(topic))
            .ok_or(RuntimeError::InvalidOffsetTable(
                "consumer offset store is null",
            ))
    }

    fn inbound_processor(
        &self,
        operation: ConsumerOperation,
    ) -> Option<Arc<dyn rocketmq_classic_remoting::client::RequestProcessor>> {
        match operation {
            ConsumerOperation::ResetOffset => {
                Some(Arc::new(crate::admin::ResetOffsetProcessor::new(
                    self.consumer.clone(),
                    self.runtime.clone(),
                )))
            }
            ConsumerOperation::ConsumerStatus => {
                Some(Arc::new(crate::admin::ConsumerStatusProcessor::for_push(
                    self.consumer.clone(),
                    self.offset_store.clone(),
                )))
            }
            ConsumerOperation::ConsumeDirectly => {
                Some(Arc::new(crate::admin::DirectConsumeProcessor::new(
                    self.consumer.clone(),
                    self.runtime.clone(),
                )))
            }
            ConsumerOperation::RunningInfo => Some(Arc::new(
                crate::admin::ConsumerRunningInfoProcessor::for_push(
                    self.consumer.clone(),
                    self.offset_store.clone(),
                    self.runtime.clone(),
                ),
            )),
        }
    }

    fn pull_message(
        &self,
        request: Arc<dyn rocketmq_classic_core::runtime::MessageRequest>,
    ) -> Option<ConsumerFuture<'_>> {
        let request = request.as_any().downcast_ref::<PullRequest>()?;
        let request = Arc::new(request.clone());
        if let Err(request) = self.pull_rpc_workers.try_submit(request) {
            let _ = self.pull_scheduler.schedule(
                request.clone(),
                PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL,
            );
            self.pull_scheduler.finish(&request);
        }
        Some(Box::pin(async {}))
    }

    fn heartbeat_data(&self) -> ConsumerData {
        let (message_model, consume_from_where, unit_mode) = self
            .consumer
            .upgrade()
            .map(|consumer| {
                (
                    consumer.message_model(),
                    consumer.consume_from_where(),
                    consumer.is_unit_mode(),
                )
            })
            .unwrap_or((self.message_model, self.consume_from_where, self.unit_mode));
        #[allow(clippy::mutable_key_type)]
        let subscription_data_set = read_lock(&self.subscriptions)
            .values()
            .cloned()
            .map(|subscription| Some(SharedValue::from(subscription)))
            .collect::<ConsumerSubscriptionDataSet>();
        ConsumerData {
            group_name: self
                .consumer
                .upgrade()
                .map(|consumer| consumer.group_name())
                .unwrap_or_else(|| Some(self.group.clone())),
            consume_type: Some(ConsumeType::ConsumePassively),
            message_model: Some(message_model),
            consume_from_where: Some(consume_from_where),
            subscription_data_set: Some(SharedValue::from(subscription_data_set)),
            unit_mode,
        }
    }

    fn is_subscribe_topic_need_update(&self, topic: &str) -> bool {
        read_lock(&self.subscriptions).contains_key(topic)
    }

    fn update_topic_subscribe_info(&self, topic: &str, queues: BTreeSet<MessageQueue>) {
        let previous = write_lock(&self.topic_queues).insert(topic.to_owned(), queues.clone());
        if previous.as_ref() != Some(&queues) {
            if let Some(runtime) = self.runtime.upgrade() {
                runtime.rebalance_immediately();
            }
        }
    }

    fn persist_consumer_offset(&self) -> Option<ConsumerFuture<'_>> {
        let store = self.offset_store.upgrade()?;
        let consumer = self.consumer.upgrade()?;
        Some(Box::pin(async move {
            if consumer.service_state() != Some(ConsumerServiceState::Running) {
                return;
            }
            let queues = read_lock(&consumer.process_queues)
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            let _ = store.persist_all(&queues).await;
        }))
    }

    fn try_rebalance(&self) -> Option<Pin<Box<dyn Future<Output = bool> + Send + '_>>> {
        let consumer = self.consumer.clone();
        Some(Box::pin(async move {
            if let Some(consumer) = consumer.upgrade() {
                consumer.rebalance_service.rebalance(&consumer).await
            } else {
                true
            }
        }))
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
