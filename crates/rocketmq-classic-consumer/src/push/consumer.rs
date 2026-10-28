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

    /// Internal assignment activation used by Phase 11 and private pipeline
    /// tests. It is deliberately not part of the public Push API.
    #[allow(dead_code)] // Activated by Phase 11; exercised through private tests in this phase.
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
mod tests {
    use super::*;
    use crate::offset::OffsetStoreFuture;
    use crate::push::listener::MessageQueueListener;
    use crate::push::pull_service::PullFlowControlReason;
    use bytes::BytesMut;
    use rocketmq_classic_common::message::{
        Message, MessageExt, MessageExtSubtype, MessageHostAddress,
    };
    use rocketmq_classic_core::api::consumer::PullResultExt;
    use rocketmq_classic_protocol::body::GetConsumerStatusBody;
    use rocketmq_classic_protocol::codes::{request, response};
    use rocketmq_classic_protocol::header::HeaderMap;
    use rocketmq_classic_remoting::client::RequestProcessor;
    use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        time::timeout,
    };

    #[test]
    fn orderly_timeout_preserves_java_null_and_exception_priority() {
        assert_eq!(
            orderly_consume_return_type("RETURNNULL", None, true),
            "RETURNNULL"
        );
        assert_eq!(
            orderly_consume_return_type("EXCEPTION", None, true),
            "EXCEPTION"
        );
        assert_eq!(
            orderly_consume_return_type("SUCCESS", Some(ConsumeOrderlyStatus::Success), true),
            "TIME_OUT"
        );
    }

    #[test]
    fn inner_producer_retry_response_codes_match_java_551_defaults() {
        for code in [
            response::TOPIC_NOT_EXIST,
            response::SERVICE_NOT_AVAILABLE,
            response::SYSTEM_ERROR,
            response::SYSTEM_BUSY,
            response::NO_PERMISSION,
            response::NO_BUYER_ID,
            response::NOT_IN_CURRENT_UNIT,
            response::GO_AWAY,
        ] {
            let error = rocketmq_classic_core::error::RpcError::Response {
                request_code: request::SEND_MESSAGE,
                peer: "broker-a".to_owned(),
                code,
                remark: None,
            };
            assert!(retryable_inner_producer_send_error(&error), "code {code}");
        }

        let error = rocketmq_classic_core::error::RpcError::Response {
            request_code: request::SEND_MESSAGE,
            peer: "broker-a".to_owned(),
            code: response::MESSAGE_ILLEGAL,
            remark: None,
        };
        assert!(!retryable_inner_producer_send_error(&error));
    }

    #[test]
    fn retry_publish_queue_avoids_last_broker_until_it_must_fallback() {
        let queues = [
            MessageQueue::new("orders", "broker-a", 0),
            MessageQueue::new("orders", "broker-b", 0),
            MessageQueue::new("orders", "broker-c", 0),
        ];

        assert_eq!(
            select_retry_publish_queue(&queues, 0, None).and_then(MessageQueue::broker_name),
            Some("broker-a")
        );
        assert_eq!(
            select_retry_publish_queue(&queues, 0, Some("broker-a"))
                .and_then(MessageQueue::broker_name),
            Some("broker-b")
        );
        assert_eq!(
            select_retry_publish_queue(&queues, 2, Some("broker-a"))
                .and_then(MessageQueue::broker_name),
            Some("broker-c")
        );
        assert_eq!(
            select_retry_publish_queue(&queues[..1], 0, Some("broker-a"))
                .and_then(MessageQueue::broker_name),
            Some("broker-a")
        );
    }

    #[tokio::test]
    async fn inner_producer_is_shared_by_runtime_and_replaced_on_runtime_change() {
        let config = PushConsumerConfig::new("inner-producer-owner-one").unwrap();
        let runtime_config = config.client_config().clone();
        let manager = ClientRuntimeManager::new();
        let runtime = manager.get_or_create(&runtime_config).await.unwrap();
        let first = PushConsumer::with_manager(config, manager.clone());
        let second = PushConsumer::with_manager(
            PushConsumerConfig::new("inner-producer-owner-two").unwrap(),
            manager,
        );

        let first_producer = first.inner_producer(&runtime).unwrap();
        let second_producer = second.inner_producer(&runtime).unwrap();
        assert!(Arc::ptr_eq(&first_producer, &second_producer));

        let other_manager = ClientRuntimeManager::new();
        let other_runtime = other_manager.get_or_create(&runtime_config).await.unwrap();
        let replacement = first.inner_producer(&other_runtime).unwrap();
        let shared_replacement = second.inner_producer(&other_runtime).unwrap();
        assert!(!Arc::ptr_eq(&first_producer, &replacement));
        assert!(Arc::ptr_eq(&replacement, &shared_replacement));
    }

    #[test]
    fn trace_dispatcher_accessor_is_empty_before_start() {
        let config = PushConsumerConfig::new("trace-dispatcher-accessor").unwrap();
        let consumer = PushConsumer::new(config);
        assert!(consumer.trace_dispatcher().is_none());
    }

    #[tokio::test]
    async fn push_owner_shutdown_stops_its_trace_dispatcher() {
        let config = PushConsumerConfig::new("push-trace-owner-shutdown").unwrap();
        let manager = ClientRuntimeManager::new();
        let dispatcher = Arc::new(
            rocketmq_classic_producer::TraceDispatcher::new(
                config.client_config(),
                "push-trace-owner-shutdown",
                rocketmq_classic_producer::TraceProducerKind::Consume,
                None,
                manager.clone(),
                None,
            )
            .unwrap(),
        );
        let consumer = PushConsumer::with_manager(config, manager);
        assert!(consumer.trace_dispatcher.set(dispatcher.clone()).is_ok());

        consumer.shutdown_trace_dispatcher().await;

        assert!(
            !dispatcher.append(rocketmq_classic_core::trace::TraceContext::new(
                rocketmq_classic_core::trace::TraceType::Recall,
            ))
        );
        dispatcher.shutdown().await.unwrap();
    }

    #[test]
    fn raw_push_implementation_accessor_retains_identity() {
        let group = "phase18-push-implementation-accessor";
        let config = PushConsumerConfig::new(group).unwrap();
        let consumer = Arc::new(PushConsumer::new(config));

        let first = consumer.get_default_mq_push_consumer_impl();
        let second = consumer.get_default_mq_push_consumer_impl();
        let first_rebalance = consumer.get_rebalance_impl();
        let second_rebalance = consumer.get_rebalance_impl();

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        assert_eq!(fixture["implementationNonNull"], true);
        assert_eq!(fixture["implementationIdentityStable"], true);
        assert_eq!(fixture["rebalanceImplementationNonNull"], true);
        assert_eq!(fixture["rebalanceImplementationIdentityStable"], true);
        assert_eq!(fixture["configuredConsumerGroup"], group);
        assert_eq!(fixture["rawFacadeConsumerIdentityStable"], true);
        assert_eq!(fixture["rawFacadeConsumerGroup"], group);
        assert_eq!(fixture["clientFactoryInitiallyNull"], true);
        assert_eq!(fixture["clientFactorySetterPreservesIdentity"], true);
        assert_eq!(fixture["consumeMessageServiceInitiallyNull"], true);
        assert_eq!(
            fixture["consumeMessageServiceSetterPreservesIdentity"],
            true
        );
        assert_eq!(
            fixture["consumerStatsManagerGetterPreservesFactoryIdentity"],
            true
        );
        assert_eq!(
            fixture["rawImplementationPublicMethodSignatures"]
                .as_array()
                .unwrap()
                .len(),
            71,
            "pinned Java 5.5.1 raw implementation method inventory changed"
        );
        assert_eq!(fixture["effectiveMaxReconsumeTimesDefault"], 16);
        assert!(Arc::ptr_eq(&consumer, &first));
        assert!(Arc::ptr_eq(&first, &second));
        assert!(std::ptr::eq(first.config(), second.config()));
        assert!(std::ptr::eq(first_rebalance, consumer.as_ref()));
        assert!(std::ptr::eq(second_rebalance, first_rebalance));
        assert_eq!(first.config().consumer_group(), Some(group.to_owned()));
        assert_eq!(first.get_max_reconsume_times(), 16);
        assert!(first.get_consumer_stats_manager().is_none());

        let custom_config = PushConsumerConfig::new("phase18-push-max-reconsume-custom").unwrap();
        custom_config.set_max_reconsume_times(7);
        let custom_consumer = PushConsumer::new(custom_config);
        assert_eq!(fixture["effectiveMaxReconsumeTimesCustom"], 7);
        assert_eq!(custom_consumer.get_max_reconsume_times(), 7);
    }

    #[tokio::test]
    async fn rebalance_client_factory_setter_replaces_nullable_runtime_reference() {
        let mut config = PushConsumerConfig::new("rebalance-factory-setter").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = PushConsumer::with_manager(config, manager.clone());
        assert!(consumer.get_client_runtime().await.is_none());

        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        consumer.set_client_runtime(Some(runtime.clone()));
        assert!(Arc::ptr_eq(
            &consumer.get_client_runtime().await.unwrap(),
            &runtime
        ));

        consumer.set_client_runtime(None);
        assert!(consumer.get_client_runtime().await.is_none());
        assert!(!runtime.client_id().is_empty());
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn implementation_factory_setter_is_independent_of_rebalance_factory() {
        let mut config = PushConsumerConfig::new("implementation-factory-setter").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let consumer = Arc::new(PushConsumer::with_manager(
            config,
            ClientRuntimeManager::new(),
        ));
        assert!(consumer.get_implementation_client_runtime().await.is_none());

        let implementation_manager = ClientRuntimeManager::new();
        let rebalance_manager = ClientRuntimeManager::new();
        let implementation_runtime = implementation_manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let rebalance_runtime = rebalance_manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();

        consumer.set_implementation_client_runtime(Some(implementation_runtime.clone()));
        consumer.set_client_runtime(Some(rebalance_runtime.clone()));
        let stats = consumer.get_consumer_stats_manager().unwrap();
        assert!(Arc::ptr_eq(
            &stats,
            &implementation_runtime.consumer_stats_manager()
        ));
        assert!(Arc::ptr_eq(
            &consumer.get_implementation_client_runtime().await.unwrap(),
            &implementation_runtime
        ));
        assert!(Arc::ptr_eq(
            &consumer.get_client_runtime().await.unwrap(),
            &rebalance_runtime
        ));

        consumer.set_implementation_client_runtime(None);
        assert!(consumer.get_implementation_client_runtime().await.is_none());
        assert!(consumer.get_consumer_stats_manager().is_none());
        assert!(Arc::ptr_eq(
            &consumer.get_client_runtime().await.unwrap(),
            &rebalance_runtime
        ));
        implementation_runtime.shutdown().await.unwrap();
        rebalance_runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn raw_push_listener_accessors_match_java_registration_flow() {
        let config = PushConsumerConfig::new("phase18-push-implementation-accessor").unwrap();
        let consumer = Arc::new(PushConsumer::new(config));
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();

        assert!(consumer.get_message_listener().is_none());
        assert_eq!(fixture["rawMessageListenerInitiallyNull"], true);

        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            },
        );
        consumer
            .register_message_listener(PushMessageListener::Concurrently(listener.clone()))
            .await
            .unwrap();
        let listener_getter_preserves_identity = match consumer.get_message_listener() {
            Some(PushMessageListener::Concurrently(current)) => Arc::ptr_eq(&listener, &current),
            _ => false,
        };
        assert_eq!(fixture["rawMessageListenerGetterPreservesIdentity"], true);
        assert!(listener_getter_preserves_identity);
    }

    #[test]
    fn push_live_runtime_settings_drive_owner_flow_helpers() {
        let consumer = PushConsumer::new(PushConsumerConfig::new("push-live-runtime").unwrap());
        consumer
            .config()
            .set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        consumer
            .config()
            .set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
        consumer.config().set_unit_mode(true);
        consumer.config().set_max_reconsume_times(9);
        consumer.config().set_pull_threshold_for_queue(10);
        consumer.config().set_pull_threshold_size_for_queue_mib(100);
        consumer.config().set_consume_concurrently_max_span(1);

        assert_eq!(
            consumer.message_model(),
            rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting
        );
        assert_eq!(consumer.consume_init_mode(), 0);
        assert!(consumer.is_unit_mode());
        assert_eq!(consumer.get_max_reconsume_times(), 9);

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = ProcessQueue::new_for_assignment(queue, 1);
        process_queue
            .insert_messages(1, (0..5).map(message).collect())
            .unwrap();
        let thresholds = consumer.active_pull_queue_thresholds();
        assert_eq!(thresholds.message_count, 10);
        assert_eq!(thresholds.size_mib, 100);
        assert_eq!(thresholds.max_span, 1);
        assert_eq!(
            flow_control_reason(&process_queue.snapshot(1).unwrap(), thresholds),
            Some(PullFlowControlReason::MaxSpan)
        );
    }

    #[test]
    fn push_topic_thresholds_match_java_global_process_queue_count() {
        let config = PushConsumerConfig::new("phase18-push-queue-thresholds").unwrap();
        config.set_pull_threshold_for_queue(777);
        config.set_pull_threshold_size_for_queue_mib(77);
        config.set_pull_threshold_for_topic(20);
        config.set_pull_threshold_size_for_topic_mib(12);
        let consumer = PushConsumer::new(config);
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        let expected = |name: &str| {
            let values = fixture[name].as_array().unwrap();
            (
                values[0].as_i64().unwrap() as i32,
                values[1].as_i64().unwrap() as i32,
            )
        };
        let active = || {
            (
                consumer.config.pull_threshold_for_queue(),
                consumer.config.pull_threshold_size_for_queue_mib(),
            )
        };

        consumer.update_pull_thresholds_after_rebalance();
        assert_eq!(active(), expected("pushQueueThresholdZeroAssignment"));

        let queues = [
            MessageQueue::new("orders", "broker-a", 0),
            MessageQueue::new("orders", "broker-a", 1),
            MessageQueue::new("events", "broker-b", 0),
            MessageQueue::new("events", "broker-b", 1),
        ];
        write_lock(&consumer.process_queues).insert(
            queues[0].clone(),
            Arc::new(ProcessQueue::new_for_assignment(queues[0].clone(), 1)),
        );
        consumer.update_pull_thresholds_after_rebalance();
        assert_eq!(active(), expected("pushQueueThresholdSingleAssignment"));

        for (generation, queue) in queues.iter().enumerate().skip(1) {
            write_lock(&consumer.process_queues).insert(
                queue.clone(),
                Arc::new(ProcessQueue::new_for_assignment(
                    queue.clone(),
                    generation as u64 + 1,
                )),
            );
        }
        consumer.update_pull_thresholds_after_rebalance();
        assert_eq!(
            active(),
            expected("pushQueueThresholdFourAssignmentsAcrossTopics")
        );
        let backlog = ProcessQueue::new_for_assignment(queues[0].clone(), 100);
        backlog
            .insert_messages(100, (0..6).map(message).collect())
            .unwrap();
        assert_eq!(
            flow_control_reason(
                &backlog.snapshot(100).unwrap(),
                consumer.active_pull_queue_thresholds(),
            ),
            Some(PullFlowControlReason::MessageCount),
            "the scaled global threshold must drive the actual queue flow-control decision"
        );

        write_lock(&consumer.process_queues).remove(&queues[3]);
        consumer.update_pull_thresholds_after_rebalance();
        assert_eq!(
            active(),
            expected("pushQueueThresholdThreeAssignmentsAcrossTopics")
        );

        write_lock(&consumer.process_queues).clear();
        consumer.update_pull_thresholds_after_rebalance();
        assert_eq!(active(), expected("pushQueueThresholdZeroAfterAssignment"));
        assert_eq!(fixture["pushQueueThresholdHeartbeatCalls"], 5);
    }

    #[tokio::test]
    async fn push_adjust_thread_pool_matches_java_default_service_noop() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        assert_eq!(fixture["adjustThreadPoolDecreaseCalls"], 1);
        assert_eq!(fixture["adjustThreadPoolCorePoolSizeBefore"], 7);
        assert_eq!(fixture["adjustThreadPoolCorePoolSizeAfter"], 7);

        let mut config = PushConsumerConfig::new("phase18-adjust-pool").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_adjust_thread_pool_nums_threshold(10);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        install_started_consumer(
            &consumer,
            &manager,
            Arc::new(RecordingOffsetStore::default()),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        consumer.consume_core_pool_size.store(7, Ordering::Release);
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 1));
        process_queue.set_msg_acc_cnt(10);
        write_lock(&consumer.process_queues).insert(queue, process_queue.clone());
        consumer.adjust_thread_pool();
        assert_eq!(consumer.consume_core_pool_size.load(Ordering::Acquire), 7);
        process_queue.set_msg_acc_cnt(0);
        consumer.adjust_thread_pool();
        assert_eq!(consumer.consume_core_pool_size.load(Ordering::Acquire), 7);
        consumer.shutdown().await.unwrap();
    }

    #[test]
    fn push_rebalance_consume_init_mode_matches_java_for_every_policy() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        let cases = [
            (
                ConsumeFromWhere::ConsumeFromLastOffset,
                "CONSUME_FROM_LAST_OFFSET",
            ),
            (
                ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst,
                "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST",
            ),
            (
                ConsumeFromWhere::ConsumeFromMinOffset,
                "CONSUME_FROM_MIN_OFFSET",
            ),
            (
                ConsumeFromWhere::ConsumeFromMaxOffset,
                "CONSUME_FROM_MAX_OFFSET",
            ),
            (
                ConsumeFromWhere::ConsumeFromFirstOffset,
                "CONSUME_FROM_FIRST_OFFSET",
            ),
            (
                ConsumeFromWhere::ConsumeFromTimestamp,
                "CONSUME_FROM_TIMESTAMP",
            ),
        ];

        for (policy, java_name) in cases {
            let config = PushConsumerConfig::new("phase18-pop-init-mode").unwrap();
            config.set_consume_from_where(policy);
            let consumer = PushConsumer::new(config);
            assert_eq!(
                i64::from(consumer.consume_init_mode()),
                fixture["pushConsumeInitModes"][java_name].as_i64().unwrap(),
                "consume init mode for {java_name}"
            );
        }
    }

    #[tokio::test]
    async fn query_consume_time_span_queries_fresh_route_in_order_and_uses_vip_addresses() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        assert_eq!(fixture["queryConsumeTimeSpanNullGroupOmitted"], true);
        let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let nameserver_address = nameserver.local_addr().unwrap().to_string();
        let first_broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_address = first_broker.local_addr().unwrap();
        let first_vip_address = format!(
            "{}:{}",
            first_address.ip(),
            first_address.port().checked_add(2).unwrap()
        );
        let second_broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_address = second_broker.local_addr().unwrap();
        let second_vip_address = format!(
            "{}:{}",
            second_address.ip(),
            second_address.port().checked_add(2).unwrap()
        );
        let route_body = serde_json::to_vec(&serde_json::json!({
            "queueDatas": [],
            "brokerDatas": [
                {"cluster": "cluster-a", "brokerName": "broker-z", "brokerAddrs": {"0": first_vip_address}},
                {"cluster": "cluster-a", "brokerName": "broker-a", "brokerAddrs": {"0": second_vip_address}}
            ],
            "filterServerTable": {}
        }))
        .unwrap();
        let nameserver_peer = tokio::spawn(async move {
            let (mut stream, _) = nameserver.accept().await.unwrap();
            let incoming = read_remoting_command(&mut stream).await;
            assert_eq!(incoming.code, request::GET_ROUTEINFO_BY_TOPIC);
            assert_eq!(
                incoming
                    .ext_fields
                    .as_ref()
                    .and_then(|fields| fields.get("topic"))
                    .map(String::as_str),
                Some("orders")
            );
            write_remoting_reply_with_body(
                &mut stream,
                &incoming,
                response::SUCCESS,
                None,
                Some(route_body),
            )
            .await;
        });

        let first_peer = tokio::spawn(async move {
            let (mut stream, _) = first_broker.accept().await.unwrap();
            let incoming = read_remoting_command(&mut stream).await;
            assert_eq!(incoming.code, request::QUERY_CONSUME_TIME_SPAN);
            let fields = incoming.ext_fields.as_ref().unwrap();
            assert_eq!(fields.get("topic").map(String::as_str), Some("orders"));
            assert!(!fields.contains_key("group"));
            write_remoting_reply_with_body(
                &mut stream,
                &incoming,
                response::SUCCESS,
                None,
                Some(
                    br#"{"consumeTimeSpanSet":[{"messageQueue":{"topic":"orders","brokerName":"broker-z","queueId":3},"minTimeStamp":1,"maxTimeStamp":9,"consumeTimeStamp":5,"delayTime":4}]}"#.to_vec(),
                ),
            )
            .await;
        });
        let second_peer = tokio::spawn(async move {
            let (mut stream, _) = second_broker.accept().await.unwrap();
            let incoming = read_remoting_command(&mut stream).await;
            assert_eq!(incoming.code, request::QUERY_CONSUME_TIME_SPAN);
            let fields = incoming.ext_fields.as_ref().unwrap();
            assert_eq!(fields.get("topic").map(String::as_str), Some("orders"));
            assert!(!fields.contains_key("group"));
            write_remoting_reply_with_body(
                &mut stream,
                &incoming,
                response::SUCCESS,
                None,
                Some(
                    br#"{"consumeTimeSpanSet":[{"messageQueue":{"topic":"orders","brokerName":"broker-a","queueId":1},"minTimeStamp":2,"maxTimeStamp":10,"consumeTimeStamp":7,"delayTime":3}]}"#.to_vec(),
                ),
            )
            .await;
        });

        let mut config = PushConsumerConfig::new("consume-time-span-group").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(nameserver_address));
        config.client_config_mut().set_vip_channel_enabled(true);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store,
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        runtime.remoting().start().unwrap();
        consumer.set_consumer_group(None);

        let spans = consumer.query_consume_time_span("orders").await.unwrap();
        nameserver_peer.await.unwrap();
        first_peer.await.unwrap();
        second_peer.await.unwrap();
        assert_eq!(spans.len(), 2);
        assert_eq!(
            spans
                .iter()
                .map(|span| {
                    span.as_ref()
                        .unwrap()
                        .get_message_queue()
                        .unwrap()
                        .with(|queue| queue.broker_name().unwrap().to_owned())
                })
                .collect::<Vec<_>>(),
            ["broker-z".to_owned(), "broker-a".to_owned()]
        );
        assert_eq!(spans[0].as_ref().unwrap().get_delay_time(), 4);
        assert_eq!(spans[1].as_ref().unwrap().get_delay_time(), 3);

        consumer.set_consumer_group(Some("consume-time-span-group".into()));
        consumer.shutdown().await.unwrap();
        let _ = runtime.shutdown().await;
    }

    #[test]
    fn push_raw_consume_hook_entrypoints_match_java() {
        struct Hook(Arc<std::sync::Mutex<Vec<String>>>);

        impl ConsumeMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "push-raw-hook-fixture"
            }

            fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
                self.0.lock().unwrap().push(format!(
                    "before:{}",
                    context.status.as_deref().unwrap_or("null")
                ));
                context.status = Some("hook-before".to_owned());
            }

            fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
                self.0.lock().unwrap().push(format!(
                    "after:{}:{}",
                    context.status.as_deref().unwrap_or("null"),
                    context.success
                ));
            }
        }

        let config = PushConsumerConfig::new("phase18-push-implementation-accessor").unwrap();
        let consumer = PushConsumer::new(config);
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        assert!(!consumer.has_hook());
        assert_eq!(fixture["hasConsumeHookBeforeRegistration"], false);

        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        consumer.register_consume_message_hook(Arc::new(Hook(events.clone())));
        assert!(consumer.has_hook());
        let mut context = ConsumeMessageContext::new(
            None,
            None,
            MessageQueue::new("orders", "broker-a", 0),
            None,
        );
        context.status = Some("seed".to_owned());
        consumer.execute_hook_before(&mut context);
        context.success = true;
        consumer.execute_hook_after(&mut context);

        let events = events.lock().unwrap().clone();
        assert_eq!(fixture["hasConsumeHookAfterRegistration"], true);
        assert_eq!(fixture["directHookConsumerGroupNull"], true);
        assert_eq!(
            fixture["directHookEvents"],
            serde_json::json!(["before:seed", "after:hook-before:true"])
        );
        assert_eq!(events, ["before:seed", "after:hook-before:true"]);
        assert_eq!(context.status.as_deref(), Some("hook-before"));
        assert_eq!(context.consumer_group, None);
    }

    #[test]
    fn push_consume_message_context_preserves_a_live_null_group() {
        let mut config = PushConsumerConfig::new("push-null-consume-context").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let consumer = PushConsumer::new(config);
        consumer.set_consumer_group(None);

        let context = consumer.new_consume_message_context(
            Vec::new(),
            MessageQueue::new("tenant%orders", "broker-a", 0),
        );
        assert_eq!(context.consumer_group, None);
        assert_eq!(context.namespace.as_deref(), Some("tenant"));
    }

    #[test]
    fn push_raw_filter_hook_and_topic_helpers_match_java() {
        struct FilterHook(Arc<std::sync::Mutex<Vec<String>>>);

        impl FilterMessageHook for FilterHook {
            fn hook_name(&self) -> &str {
                "push-raw-filter-hook-fixture"
            }

            fn filter_message(&self, context: &mut FilterMessageContext) {
                self.0.lock().unwrap().push(format!(
                    "{}:{}",
                    context.unit_mode,
                    context.messages.len()
                ));
                context.messages.truncate(1);
            }
        }

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        let group = "phase18-push-topic-helpers";
        let mut config = PushConsumerConfig::new(group).unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let consumer = PushConsumer::new(config);

        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        consumer.register_filter_message_hook(Arc::new(FilterHook(events.clone())));
        assert!(consumer.pull_api.has_hook());
        assert_eq!(
            fixture["pushPopFilterHookEvents"],
            serde_json::json!(["false:2"])
        );
        let first = crate::test_support::message_ext_from(Message::new("orders", b"one".to_vec()));
        let second = crate::test_support::message_ext_from(Message::new("orders", b"two".to_vec()));
        let (accepted, discarded) =
            consumer.filter_pop_messages(&SubscriptionData::default(), vec![first, second]);
        assert_eq!(*events.lock().unwrap(), ["false:2"]);
        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted[0].body().as_ref(), b"one");
        assert_eq!(discarded.len(), 1);
        assert_eq!(discarded[0].body().as_ref(), b"two");

        let queues = BTreeSet::from([
            MessageQueue::new("tenant%orders", "broker-a", 1),
            MessageQueue::new("orders", "broker-b", 2),
            MessageQueue::new("other%events", "broker-c", 3),
        ]);
        let parsed = consumer
            .parse_subscribe_message_queues(&queues)
            .unwrap()
            .iter()
            .map(|queue| {
                format!(
                    "{}|{}|{}",
                    queue.topic().unwrap_or_default(),
                    queue.broker_name().unwrap_or_default(),
                    queue.queue_id()
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(fixture["parsedSubscribeQueues"], serde_json::json!(parsed));

        let mut pop_retry_messages = [
            crate::test_support::message_ext_from(Message::new(
                "%RETRY%phase18-push-topic-helpers_orders",
                b"".to_vec(),
            )),
            crate::test_support::message_ext_from(Message::new(
                "%RETRY%other_orders",
                b"".to_vec(),
            )),
            crate::test_support::message_ext_from(Message::new("orders", b"".to_vec())),
        ];
        consumer.try_reset_pop_retry_topic(&mut pop_retry_messages, group);
        let pop_retry_topics = pop_retry_messages
            .iter()
            .map(|message| message.topic())
            .collect::<Vec<_>>();
        assert_eq!(
            fixture["resetPopRetryTopics"],
            serde_json::json!(pop_retry_topics)
        );

        let mut reset_messages = [
            {
                let mut message = crate::test_support::message_ext_from(Message::new(
                    "%RETRY%phase18-push-topic-helpers",
                    b"".to_vec(),
                ));
                message
                    .set_system_property(PROPERTY_RETRY_TOPIC, "tenant%orders")
                    .unwrap();
                message
            },
            crate::test_support::message_ext_from(Message::new("tenant%events", b"".to_vec())),
            crate::test_support::message_ext_from(Message::new("other%events", b"".to_vec())),
        ];
        consumer.reset_retry_and_namespace(&mut reset_messages, group);
        let reset_topics = reset_messages
            .iter()
            .map(|message| message.topic())
            .collect::<Vec<_>>();
        assert_eq!(
            fixture["resetRetryAndNamespaceTopics"],
            serde_json::json!(reset_topics)
        );
    }

    #[tokio::test]
    async fn push_raw_rebalance_route_entrypoints_match_java() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        let consumer = Arc::new(PushConsumer::new(
            PushConsumerConfig::new("phase18-push-implementation-accessor").unwrap(),
        ));
        consumer.subscribe("orders", "*").await.unwrap();

        assert_eq!(
            consumer.is_subscribe_topic_need_update("orders"),
            fixture["subscribeInfoNeededBeforeRoute"]
        );
        assert!(!consumer.is_subscribe_topic_need_update("unregistered"));
        assert_eq!(fixture["unregisteredTopicNeedsRoute"], false);
        consumer
            .update_topic_subscribe_info(
                "orders",
                BTreeSet::from([MessageQueue::new("orders", "broker-a", 1)]),
            )
            .await;
        assert_eq!(
            consumer.is_subscribe_topic_need_update("orders"),
            fixture["subscribeInfoNeededAfterRoute"]
        );
        consumer.suspend().await;
        assert_eq!(
            consumer.try_rebalance().await,
            fixture["tryRebalanceWhilePaused"]
        );
        consumer.do_rebalance().await;
        consumer.resume().await;
    }

    #[tokio::test]
    async fn push_raw_offset_store_accessors_match_java_null_and_identity() {
        let config = PushConsumerConfig::new("phase18-push-implementation-accessor").unwrap();
        let consumer = PushConsumer::new(config);
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        assert_eq!(
            consumer.get_offset_store().is_none(),
            fixture["offsetStoreInitiallyNull"]
        );

        let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
        consumer
            .set_offset_store(Some(store.clone()))
            .await
            .unwrap();
        let retained = consumer.get_offset_store().unwrap();
        assert!(Arc::ptr_eq(&store, &retained));
        assert_eq!(fixture["offsetStoreSetterPreservesIdentity"], true);

        consumer.set_offset_store(None).await.unwrap();
        assert!(consumer.get_offset_store().is_none());
        assert_eq!(fixture["offsetStoreSetterAcceptsNull"], true);
    }

    #[tokio::test]
    async fn push_raw_offset_entrypoints_match_java_and_persist_only_active_pull_queue() {
        let config = PushConsumerConfig::new("phase18-push-implementation-accessor").unwrap();
        let consumer = Arc::new(PushConsumer::new(config));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        consumer
            .set_offset_store(Some(store_dyn.clone()))
            .await
            .unwrap();
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        let assigned = MessageQueue::new("orders", "broker-a", 2);
        let unassigned = MessageQueue::new("orders", "broker-b", 3);

        consumer.update_consume_offset(&assigned, 41).unwrap();
        consumer.update_consume_offset(&unassigned, 99).unwrap();
        assert_eq!(
            store.updates.lock().unwrap().as_slice(),
            &[(assigned.clone(), 41, false), (unassigned, 99, false)]
        );
        assert_eq!(
            fixture["offsetUpdates"],
            serde_json::json!(["orders|broker-a|2|41|false", "orders|broker-b|3|99|false"])
        );

        assert!(matches!(
            consumer.persist_consumer_offset().await,
            Err(PushConsumerError::ServiceStateNotReady(Some(
                ConsumerServiceState::CreateJust
            )))
        ));
        assert!(store.persist_calls.lock().unwrap().is_empty());

        write_lock(&consumer.process_queues).insert(
            assigned.clone(),
            Arc::new(ProcessQueue::new_for_assignment(assigned.clone(), 1)),
        );
        consumer.set_service_state(Some(ConsumerServiceState::Running));
        consumer.persist_consumer_offset().await.unwrap();
        assert_eq!(
            store.persist_calls.lock().unwrap().as_slice(),
            &[vec![assigned.clone()]]
        );
        assert_eq!(
            fixture["persistedQueues"],
            serde_json::json!(["orders|broker-a|2"])
        );

        let registration = PushConsumerRegistration {
            group: consumer.group_name().unwrap_or_default(),
            message_model: consumer.message_model(),
            consume_from_where: consumer.consume_from_where(),
            unit_mode: consumer.is_unit_mode(),
            subscriptions: consumer.subscriptions.clone(),
            topic_queues: consumer.topic_queues.clone(),
            pull_scheduler: Arc::new(PullScheduler::new()),
            pull_rpc_workers: Arc::new(PullRpcWorkers::new()),
            offset_store: Arc::downgrade(&store_dyn),
            consumer: Arc::downgrade(&consumer),
            runtime: std::sync::Weak::new(),
        };
        consumer
            .config()
            .set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        consumer
            .config()
            .set_consume_from_where(ConsumeFromWhere::ConsumeFromTimestamp);
        consumer.config().set_unit_mode(true);
        let heartbeat = ConsumerRuntime::heartbeat_data(&registration);
        assert_eq!(
            heartbeat.message_model,
            Some(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting)
        );
        assert_eq!(
            heartbeat.consume_from_where,
            Some(ConsumeFromWhere::ConsumeFromTimestamp)
        );
        assert!(heartbeat.unit_mode);
        ConsumerRuntime::persist_consumer_offset(&registration)
            .expect("running consumer registers periodic offset persistence")
            .await;
        assert_eq!(
            store.persist_calls.lock().unwrap().as_slice(),
            &[vec![assigned.clone()], vec![assigned]]
        );
    }

    #[test]
    fn push_facade_group_getter_and_heartbeat_follow_live_java_group() {
        let oracle: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        let oracle = &oracle["pushGroupNameMutation"];
        assert_eq!(oracle["defaultConstructorGroup"], "DEFAULT_CONSUMER");

        let config = PushConsumerConfig::new("push-group-before").unwrap();
        let consumer = Arc::new(PushConsumer::new(config));
        let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
        let registration = PushConsumerRegistration {
            group: "push-group-before".to_owned(),
            message_model: consumer.message_model(),
            consume_from_where: consumer.consume_from_where(),
            unit_mode: consumer.is_unit_mode(),
            subscriptions: consumer.subscriptions.clone(),
            topic_queues: consumer.topic_queues.clone(),
            pull_scheduler: Arc::new(PullScheduler::new()),
            pull_rpc_workers: Arc::new(PullRpcWorkers::new()),
            offset_store: Arc::downgrade(&store),
            consumer: Arc::downgrade(&consumer),
            runtime: std::sync::Weak::new(),
        };

        assert_eq!(
            consumer.get_consumer_group().as_deref(),
            Some("push-group-before")
        );
        assert_eq!(consumer.group_name().as_deref(), Some("push-group-before"));
        assert_eq!(oracle["facadeGroupBefore"], "push-group-before");
        assert_eq!(oracle["implementationGroupBefore"], "push-group-before");
        assert_eq!(
            ConsumerRuntime::heartbeat_data(&registration)
                .group_name
                .as_deref(),
            Some(oracle["heartbeatGroupBefore"].as_str().unwrap())
        );

        consumer.set_consumer_group(Some("push-group-after".into()));
        assert_eq!(
            consumer.get_consumer_group().as_deref(),
            Some("push-group-after")
        );
        assert_eq!(consumer.group_name().as_deref(), Some("push-group-after"));
        assert_eq!(oracle["facadeGroupAfter"], "push-group-after");
        assert_eq!(oracle["implementationGroupAfter"], "push-group-after");
        assert_eq!(
            ConsumerRuntime::heartbeat_data(&registration)
                .group_name
                .as_deref(),
            Some(oracle["heartbeatGroupAfter"].as_str().unwrap())
        );

        consumer.set_consumer_group(None);
        assert_eq!(consumer.get_consumer_group(), None);
        assert_eq!(consumer.group_name(), None);
        assert_eq!(oracle["facadeGroupNull"], true);
        assert_eq!(oracle["implementationGroupNull"], true);
        assert_eq!(
            ConsumerRuntime::heartbeat_data(&registration).group_name,
            None
        );
        assert_eq!(oracle["heartbeatGroupNull"], true);
        assert_eq!(
            oracle["originalRegistrationRemainsAfterCurrentGroupUnregister"],
            true
        );
    }

    #[tokio::test]
    async fn push_start_namespaces_and_retains_group_before_consumer_validation() {
        let mut config = PushConsumerConfig::new("push-start-group").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let consumer = Arc::new(PushConsumer::new(config));
        assert_eq!(consumer.group_name().as_deref(), Some("push-start-group"));

        assert!(matches!(
            consumer.start().await,
            Err(PushConsumerError::MissingMessageListener)
        ));
        assert_eq!(
            consumer.group_name().as_deref(),
            Some("tenant%push-start-group")
        );
        assert_eq!(
            consumer.config().consumer_group().as_deref(),
            Some("tenant%push-start-group")
        );

        let default = PushConsumerConfig::new_default().unwrap();
        assert_eq!(
            default.consumer_group().as_deref(),
            Some("DEFAULT_CONSUMER")
        );
        assert!(matches!(
            default.validate("DEFAULT_CONSUMER"),
            Err(PushConsumerError::DefaultConsumerGroup)
        ));
    }

    #[tokio::test]
    async fn push_raw_consumer_inner_accessors_match_java() {
        let config = PushConsumerConfig::new("phase18-push-implementation-accessor").unwrap();
        let consumer = PushConsumer::new(config);
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();

        assert_eq!(
            consumer.group_name(),
            Some(fixture["groupName"].as_str().unwrap().to_owned())
        );
        assert_eq!(fixture["messageModel"], "CLUSTERING");
        assert_eq!(
            consumer.message_model(),
            rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
        );
        assert_eq!(fixture["consumeType"], "CONSUME_PASSIVELY");
        assert_eq!(consumer.consume_type(), ConsumeType::ConsumePassively);
        assert_eq!(fixture["consumeFromWhere"], "CONSUME_FROM_LAST_OFFSET");
        assert_eq!(
            consumer.consume_from_where(),
            ConsumeFromWhere::ConsumeFromLastOffset
        );
        assert_eq!(
            consumer.is_unit_mode(),
            fixture["unitMode"].as_bool().unwrap()
        );
        assert_eq!(
            consumer.get_message_queue_listener().is_none(),
            fixture["messageQueueListenerInitiallyNull"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(
            consumer.is_pause(),
            fixture["pauseBeforeSet"].as_bool().unwrap()
        );
        consumer.set_pause(true);
        assert!(consumer.is_pause());
        assert_eq!(fixture["pauseAfterSetTrue"], true);
        consumer.set_pause(false);
        assert!(!consumer.is_pause());
        assert_eq!(fixture["pauseAfterSetFalse"], false);
        assert_eq!(
            consumer.is_consume_orderly(),
            fixture["consumeOrderlyBeforeSet"].as_bool().unwrap()
        );
        consumer.set_consume_orderly(true);
        assert!(consumer.is_consume_orderly());
        assert_eq!(fixture["consumeOrderlyAfterSetTrue"], true);
        consumer.set_consume_orderly(false);
        assert!(!consumer.is_consume_orderly());
        assert_eq!(fixture["consumeOrderlyAfterSetFalse"], false);
        assert_eq!(
            consumer.pull_time_delay_millis_when_exception(),
            fixture["pullDelayBeforeSet"].as_i64().unwrap()
        );
        consumer.set_pull_time_delay_millis_when_exception(73);
        assert_eq!(
            consumer.pull_time_delay_millis_when_exception(),
            fixture["pullDelayAfterSet"].as_i64().unwrap()
        );
        assert_eq!(
            consumer.pull_retry_error("custom delay").delay_millis,
            fixture["pullDelayAfterSet"].as_i64().unwrap()
        );
        assert_eq!(
            consumer
                .pull_retry_error_from_rpc(rocketmq_classic_core::error::RpcError::Unsupported(
                    "custom delay",
                ))
                .delay_millis,
            fixture["pullDelayAfterSet"].as_i64().unwrap()
        );
        assert_eq!(
            consumer
                .pull_retry_error_from_rpc(rocketmq_classic_core::error::RpcError::Response {
                    request_code: 11,
                    peer: "broker-a".to_owned(),
                    code: response::FLOW_CONTROL,
                    remark: None,
                })
                .delay_millis,
            PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL
        );
        assert_eq!(
            consumer.subscriptions().len(),
            fixture["subscriptionCountBefore"].as_u64().unwrap() as usize
        );

        let table = consumer.get_subscription_inner();
        assert!(Arc::ptr_eq(&table, &consumer.get_subscription_inner()));
        consumer.subscribe("orders", "*").await.unwrap();
        assert_eq!(fixture["subscriptionInnerIdentityStable"], true);
        let mut topics = consumer
            .subscriptions()
            .into_iter()
            .filter_map(|subscription| subscription.topic)
            .collect::<Vec<_>>();
        topics.sort();
        assert_eq!(
            serde_json::json!(topics),
            fixture["subscriptionTopicsAfterSubscribe"]
        );
        assert_eq!(table.read().unwrap().len(), topics.len());
    }

    #[tokio::test]
    async fn push_service_state_accessors_match_java_null_and_start_guard() {
        let group = "phase18-push-implementation-accessor";
        let config = PushConsumerConfig::new(group).unwrap();
        let consumer = Arc::new(PushConsumer::new(config));

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        assert_eq!(
            consumer.service_state(),
            Some(ConsumerServiceState::CreateJust)
        );
        assert_eq!(fixture["initialServiceState"], "CREATE_JUST");

        consumer.set_service_state(None);
        assert_eq!(consumer.service_state(), None);
        assert_eq!(fixture["serviceStateSetterAcceptsNull"], true);
        assert!(matches!(
            consumer.start().await,
            Err(PushConsumerError::ServiceStateNotReady(None))
        ));

        consumer.set_service_state(Some(ConsumerServiceState::StartFailed));
        assert_eq!(fixture["startFailedStateBlocksStart"], true);
        assert!(matches!(
            consumer.start().await,
            Err(PushConsumerError::ServiceStateNotReady(Some(
                ConsumerServiceState::StartFailed
            )))
        ));
        assert_eq!(
            consumer.service_state(),
            Some(ConsumerServiceState::StartFailed)
        );

        consumer.set_service_state(Some(ConsumerServiceState::Running));
        assert!(consumer.is_running().await);
        consumer.set_service_state(Some(ConsumerServiceState::CreateJust));
        assert!(!consumer.is_running().await);
        assert!(matches!(
            consumer.start().await,
            Err(PushConsumerError::MissingMessageListener)
        ));
        assert_eq!(
            consumer.service_state(),
            Some(ConsumerServiceState::StartFailed)
        );
    }

    #[derive(Default)]
    struct RecordingOffsetStore {
        update_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
        persist_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
        remove_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
        reset_events: Mutex<BTreeMap<i32, Vec<String>>>,
        fail_persist_queues: Mutex<HashSet<MessageQueue>>,
        fail_read_queues: Mutex<HashSet<MessageQueue>>,
        values: Mutex<HashMap<MessageQueue, i64>>,
        persisted: Mutex<HashMap<MessageQueue, i64>>,
        frozen: Mutex<HashSet<MessageQueue>>,
        updates: Mutex<Vec<(MessageQueue, i64, bool)>>,
        reads: Mutex<Vec<(MessageQueue, ReadOffsetType)>>,
        persist_calls: Mutex<Vec<Vec<MessageQueue>>>,
        fail_persist: Mutex<bool>,
        persist_started: Mutex<Option<Arc<tokio::sync::Barrier>>>,
        persist_release: Mutex<Option<Arc<tokio::sync::Barrier>>>,
    }

    impl RecordingOffsetStore {
        fn seed_persisted(&self, queue: MessageQueue, offset: i64) {
            self.persisted.lock().unwrap().insert(queue, offset);
        }

        fn set_persist_failure(&self, fail: bool) {
            *self.fail_persist.lock().unwrap() = fail;
        }

        fn set_persist_barriers(
            &self,
            started: Arc<tokio::sync::Barrier>,
            release: Arc<tokio::sync::Barrier>,
        ) {
            *self.persist_started.lock().unwrap() = Some(started);
            *self.persist_release.lock().unwrap() = Some(release);
        }
    }

    impl OffsetStore for RecordingOffsetStore {
        fn load(&self) -> OffsetStoreFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn update_offset(&self, queue: &MessageQueue, offset: i64, increase_only: bool) {
            self.reset_events
                .lock()
                .unwrap()
                .entry(queue.queue_id())
                .or_default()
                .push(format!("update:{offset}"));
            self.updates
                .lock()
                .unwrap()
                .push((queue.clone(), offset, increase_only));
            if self.frozen.lock().unwrap().contains(queue) {
                return;
            }
            let mut values = self.values.lock().unwrap();
            if !increase_only || values.get(queue).is_none_or(|current| offset > *current) {
                values.insert(queue.clone(), offset);
            }
            drop(values);
            let hook = self.update_hook.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook();
            }
        }

        fn update_and_freeze_offset(&self, queue: &MessageQueue, offset: i64) {
            self.updates
                .lock()
                .unwrap()
                .push((queue.clone(), offset, false));
            self.frozen.lock().unwrap().insert(queue.clone());
            self.values.lock().unwrap().insert(queue.clone(), offset);
        }

        fn read_offset<'a>(
            &'a self,
            queue: &'a MessageQueue,
            mode: ReadOffsetType,
        ) -> OffsetStoreFuture<'a, OffsetRead> {
            Box::pin(async move {
                self.reads.lock().unwrap().push((queue.clone(), mode));
                if self.fail_read_queues.lock().unwrap().contains(queue) {
                    return Err(OffsetStoreError::Runtime(
                        "injected offset read failure".into(),
                    ));
                }
                let memory = self.values.lock().unwrap().get(queue).copied();
                let stored = self.persisted.lock().unwrap().get(queue).copied();
                Ok(match mode {
                    ReadOffsetType::ReadFromMemory => memory,
                    ReadOffsetType::ReadFromStore => stored,
                    ReadOffsetType::MemoryFirstThenStore => memory.or(stored),
                }
                .map_or(OffsetRead::NotFound, OffsetRead::Found))
            })
        }

        fn persist<'a>(&'a self, queue: &'a MessageQueue) -> OffsetStoreFuture<'a, ()> {
            Box::pin(async move {
                self.reset_events
                    .lock()
                    .unwrap()
                    .entry(queue.queue_id())
                    .or_default()
                    .push("persist".into());
                let started = self.persist_started.lock().unwrap().clone();
                if let Some(started) = started {
                    started.wait().await;
                }
                let release = self.persist_release.lock().unwrap().clone();
                if let Some(release) = release {
                    release.wait().await;
                }
                if *self.fail_persist.lock().unwrap()
                    || self.fail_persist_queues.lock().unwrap().contains(queue)
                {
                    return Err(OffsetStoreError::Runtime("injected persist failure".into()));
                }
                if let Some(offset) = self.values.lock().unwrap().get(queue).copied() {
                    self.persisted.lock().unwrap().insert(queue.clone(), offset);
                }
                let hook = self.persist_hook.lock().unwrap().clone();
                if let Some(hook) = hook {
                    hook();
                }
                Ok(())
            })
        }

        fn persist_all<'a>(&'a self, queues: &'a [MessageQueue]) -> OffsetStoreFuture<'a, ()> {
            Box::pin(async move {
                self.persist_calls.lock().unwrap().push(queues.to_vec());
                if *self.fail_persist.lock().unwrap() {
                    return Err(OffsetStoreError::Runtime("injected persist failure".into()));
                }
                let values = self.values.lock().unwrap();
                let mut persisted = self.persisted.lock().unwrap();
                for queue in queues {
                    if let Some(offset) = values.get(queue) {
                        persisted.insert(queue.clone(), *offset);
                    }
                }
                Ok(())
            })
        }

        fn remove_offset(&self, queue: &MessageQueue) {
            self.reset_events
                .lock()
                .unwrap()
                .entry(queue.queue_id())
                .or_default()
                .push("remove".into());
            self.values.lock().unwrap().remove(queue);
            self.frozen.lock().unwrap().remove(queue);
            let hook = self.remove_hook.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook();
            }
        }

        fn clone_offset_table(&self, topic: Option<&str>) -> HashMap<MessageQueue, i64> {
            self.values
                .lock()
                .unwrap()
                .iter()
                .filter(|(queue, _)| topic.is_none_or(|topic| queue.topic() == Some(topic)))
                .map(|(queue, offset)| (queue.clone(), *offset))
                .collect()
        }

        fn update_consume_offset_to_broker<'a>(
            &'a self,
            _queue: &'a MessageQueue,
            _offset: i64,
            _oneway: bool,
        ) -> OffsetStoreFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    fn pull_result(status: PullStatus, next_begin_offset: i64) -> crate::pull::ProcessedPullResult {
        crate::pull::ProcessedPullResult {
            result: PullResultExt {
                status,
                next_begin_offset,
                min_offset: 0,
                max_offset: next_begin_offset,
                suggest_which_broker_id: 0,
                offset_delta: None,
                raw_messages: None,
            },
            messages: (status == PullStatus::Found).then(Vec::new),
        }
    }

    fn message(offset: i64) -> MessageExt {
        let mut message =
            crate::test_support::message_ext_from(Message::new("orders", b"body".to_vec()));
        message.set_queue_offset(offset);
        message
    }

    struct FixedStoreHostSubtype {
        store_host: MessageHostAddress,
        setter_calls: AtomicUsize,
    }

    impl MessageExtSubtype for FixedStoreHostSubtype {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn store_host(&self) -> Option<Option<&MessageHostAddress>> {
            Some(Some(&self.store_host))
        }

        fn set_store_host(&self, _store_host: Option<MessageHostAddress>) -> bool {
            self.setter_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            true
        }
    }

    fn pop_receipt_message(receipt: Option<&str>) -> MessageExt {
        let mut message =
            crate::test_support::message_ext_from(Message::new("orders", b"body".to_vec()));
        if let Some(receipt) = receipt {
            message
                .set_system_property(PROPERTY_POP_CK, receipt)
                .unwrap();
        }
        message
    }

    async fn read_remoting_command<R>(stream: &mut R) -> RemotingCommand
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let mut length = [0; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut bytes = BytesMut::from(&length[..]);
        bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
        stream.read_exact(&mut bytes[4..]).await.unwrap();
        frame::decode_next(&mut bytes).unwrap().unwrap()
    }

    async fn write_remoting_reply<W>(
        stream: &mut W,
        request: &RemotingCommand,
        code: i32,
        fields: Option<HeaderMap>,
    ) where
        W: tokio::io::AsyncWrite + Unpin,
    {
        write_remoting_reply_with_body(stream, request, code, fields, None).await;
    }

    async fn write_remoting_reply_with_body<W>(
        stream: &mut W,
        request: &RemotingCommand,
        code: i32,
        fields: Option<HeaderMap>,
        body: Option<Vec<u8>>,
    ) where
        W: tokio::io::AsyncWrite + Unpin,
    {
        let mut reply = RemotingCommand {
            code,
            opaque: request.opaque,
            ext_fields: fields.map(Into::into),
            body,
            ..Default::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
            .await
            .unwrap();
    }

    fn pop_test_route(address: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "queueDatas": [{
                "brokerName": "broker-a",
                "readQueueNums": 1,
                "writeQueueNums": 1,
                "perm": 6,
                "topicSysFlag": 0
            }],
            "brokerDatas": [{
                "cluster": "cluster",
                "brokerName": "broker-a",
                "brokerAddrs": {"0": address}
            }],
            "filterServerTable": {}
        }))
        .unwrap()
    }

    fn pop_test_message_record() -> Vec<u8> {
        let now = epoch_millis();
        let mut record = Vec::new();
        record.extend_from_slice(&0i32.to_be_bytes());
        record.extend_from_slice(&(-626_843_481i32).to_be_bytes());
        record.extend_from_slice(&0i32.to_be_bytes());
        record.extend_from_slice(&0i32.to_be_bytes());
        record.extend_from_slice(&0i32.to_be_bytes());
        record.extend_from_slice(&10i64.to_be_bytes());
        record.extend_from_slice(&10i64.to_be_bytes());
        record.extend_from_slice(&0i32.to_be_bytes());
        record.extend_from_slice(&now.to_be_bytes());
        record.extend_from_slice(&[127, 0, 0, 1, 0, 0, 0, 0]);
        record.extend_from_slice(&now.to_be_bytes());
        record.extend_from_slice(&[127, 0, 0, 1, 0, 0, 0, 0]);
        record.extend_from_slice(&0i32.to_be_bytes());
        record.extend_from_slice(&0i64.to_be_bytes());
        record.extend_from_slice(&1i32.to_be_bytes());
        record.push(b'x');
        record.push(6);
        record.extend_from_slice(b"orders");
        record.extend_from_slice(&0i16.to_be_bytes());
        let size = i32::try_from(record.len()).unwrap();
        record[..4].copy_from_slice(&size.to_be_bytes());
        record
    }

    fn pop_test_message_record_with_receipt(receipt: &str) -> Vec<u8> {
        let properties =
            encode_nullable_property_entries(Some(&[(Some(PROPERTY_POP_CK), Some(receipt))]))
                .into_bytes();
        let mut record = pop_test_message_record();
        record.truncate(record.len() - 2);
        record.extend_from_slice(&i16::try_from(properties.len()).unwrap().to_be_bytes());
        record.extend_from_slice(&properties);
        let size = i32::try_from(record.len()).unwrap();
        record[..4].copy_from_slice(&size.to_be_bytes());
        record
    }

    fn pop_test_message_record_with_tag_and_receipt(tag: &str, receipt: &str) -> Vec<u8> {
        let properties = encode_nullable_property_entries(Some(&[
            (Some(PROPERTY_POP_CK), Some(receipt)),
            (Some(PROPERTY_TAGS), Some(tag)),
        ]))
        .into_bytes();
        let mut record = pop_test_message_record();
        record.truncate(record.len() - 2);
        record.extend_from_slice(&i16::try_from(properties.len()).unwrap().to_be_bytes());
        record.extend_from_slice(&properties);
        let size = i32::try_from(record.len()).unwrap();
        record[..4].copy_from_slice(&size.to_be_bytes());
        record
    }

    async fn accept_remoting_request(
        listener: &TcpListener,
        current: &mut Option<TcpStream>,
    ) -> RemotingCommand {
        loop {
            if current.is_none() {
                *current = Some(listener.accept().await.unwrap().0);
            }
            let stream = current.as_mut().unwrap();
            match timeout(Duration::from_millis(250), read_remoting_command(stream)).await {
                Ok(command) => return command,
                Err(_) => *current = None,
            }
        }
    }

    #[test]
    fn pop_batch_expiry_uses_receipt_invisibility_boundary_and_rejects_invalid_receipts() {
        let message = pop_receipt_message(Some("10 1000 50 2 0 broker-a 0 10"));
        assert!(!pop_batch_is_expired(std::slice::from_ref(&message), 1_049));
        assert!(pop_batch_is_expired(std::slice::from_ref(&message), 1_050));

        assert!(pop_batch_is_expired(&[], 1_049));
        assert!(pop_batch_is_expired(&[pop_receipt_message(None)], 1_049));
        for receipt in [
            "invalid receipt",
            "10 invalid 50 2 0 broker-a 0 10",
            "10 1000 50",
            "10 0 50 2 0 broker-a 0 10",
            "10 1000 0 2 0 broker-a 0 10",
        ] {
            assert!(
                pop_batch_is_expired(&[pop_receipt_message(Some(receipt))], 1_049),
                "receipt should be treated as expired: {receipt}"
            );
        }
    }

    #[tokio::test]
    async fn expired_pop_batch_skips_listener_and_releases_found_message_count() {
        let mut config = PushConsumerConfig::new("expired-pop-batch").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config.client_config())
            .await
            .unwrap();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(PopProcessQueue::new_for_assignment(queue.clone(), 1));
        process_queue.inc_found_messages(1);
        let listener_called = Arc::new(AtomicBool::new(false));
        let listener = PushMessageListener::concurrently({
            let listener_called = listener_called.clone();
            move |_messages, context| {
                listener_called.store(true, Ordering::Release);
                std::future::ready((Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context))
            }
        });

        consumer
            .clone()
            .consume_pop_batch(
                PopConsumeBatchContext {
                    process_queue: process_queue.clone(),
                    runtime: runtime.clone(),
                    queue,
                    route_topic: "orders".to_owned(),
                    consumer_group: "expired-pop-batch".to_owned(),
                    stats_consumer_group: "expired-pop-batch".to_owned(),
                },
                listener,
                vec![pop_receipt_message(Some("10 1000 50 2 0 broker-a 0 10"))],
            )
            .await;

        assert!(!listener_called.load(Ordering::Acquire));
        assert_eq!(process_queue.wait_ack_count(), 0);
        let _ = runtime.shutdown().await;
    }

    #[tokio::test]
    async fn pop_orderly_path_skips_listener_hooks_and_listener_result_side_effects() {
        let mut config = PushConsumerConfig::new("pop-orderly-stats-group").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_pop_threshold_for_queue(96);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config.client_config())
            .await
            .unwrap();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(PopProcessQueue::new_for_assignment(queue.clone(), 2));
        process_queue.inc_found_messages(96);
        // pop_loop accounts the newly returned message before dispatch.
        process_queue.inc_found_messages(1);
        let receipt = format!("10 {} 60000 2 0 broker-a 0 10", epoch_millis());
        let listener_called = Arc::new(AtomicBool::new(false));
        let listener = PushMessageListener::orderly({
            let listener_called = listener_called.clone();
            move |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                listener_called.store(true, Ordering::Release);
                async move { (Some(ConsumeOrderlyStatus::Success), context) }
            }
        });
        let hook_calls = Arc::new(Mutex::new(Vec::new()));
        struct Hook(Arc<Mutex<Vec<&'static str>>>);
        impl ConsumeMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "pop-orderly-parity"
            }

            fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {
                self.0.lock().unwrap().push("before");
            }

            fn consume_message_after(&self, _context: &mut ConsumeMessageContext) {
                self.0.lock().unwrap().push("after");
            }
        }
        consumer.register_consume_message_hook(Arc::new(Hook(hook_calls.clone())));

        consumer
            .clone()
            .consume_pop_batch(
                PopConsumeBatchContext {
                    process_queue: process_queue.clone(),
                    runtime: runtime.clone(),
                    queue,
                    route_topic: "orders".to_owned(),
                    consumer_group: "pop-orderly-stats-group".to_owned(),
                    stats_consumer_group: "pop-orderly-stats-group".to_owned(),
                },
                listener,
                vec![pop_receipt_message(Some(&receipt))],
            )
            .await;

        assert!(!listener_called.load(Ordering::Acquire));
        assert!(hook_calls.lock().unwrap().is_empty());
        assert_eq!(process_queue.wait_ack_count(), 97);
        assert!(pop_queue_wait_ack_flow_controlled(
            process_queue.wait_ack_count(),
            consumer.config.pop_threshold_for_queue()
        ));
        assert!(!pop_queue_wait_ack_flow_controlled(96, 96));
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn pop_worker_shutdown_cancels_its_listener_child_and_prevents_late_ack() {
        struct NotifyOnDrop(Arc<tokio::sync::Notify>);
        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }

        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let cancelled = Arc::new(tokio::sync::Notify::new());
        let late_side_effect = Arc::new(AtomicBool::new(false));
        let listener = PushMessageListener::concurrently({
            let started = started.clone();
            let release = release.clone();
            let cancelled = cancelled.clone();
            let late_side_effect = late_side_effect.clone();
            move |_messages, context| {
                let started = started.clone();
                let release = release.clone();
                let cancelled = cancelled.clone();
                let late_side_effect = late_side_effect.clone();
                async move {
                    let _drop_signal = NotifyOnDrop(cancelled);
                    started.notify_one();
                    release.notified().await;
                    late_side_effect.store(true, Ordering::Release);
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            }
        });
        let mut config = PushConsumerConfig::new("pop-child-cancellation").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            Arc::new(RecordingOffsetStore::default()),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        *write_lock(&consumer.pop_listener) = Some(listener);

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(PopProcessQueue::new_for_assignment(queue.clone(), 1));
        process_queue.inc_found_messages(1);
        let receipt = format!("10 {} 60000 2 0 broker-a 0 10", epoch_millis());
        consumer
            .dispatch_pop_messages(
                process_queue.clone(),
                runtime.clone(),
                queue,
                "orders".to_owned(),
                "pop-child-cancellation".to_owned(),
                vec![pop_receipt_message(Some(&receipt))],
            )
            .await;
        timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("POP listener did not start");

        consumer
            .shutdown_with_await_termination_millis(100)
            .await
            .unwrap();
        timeout(Duration::from_secs(1), cancelled.notified())
            .await
            .expect("aborting the POP worker must drop its listener child");
        release.notify_waiters();
        tokio::task::yield_now().await;

        assert!(!late_side_effect.load(Ordering::Acquire));
        assert_eq!(process_queue.wait_ack_count(), 1);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_direct_consume_drops_its_listener_child() {
        struct NotifyOnDrop(Arc<tokio::sync::Notify>);
        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }

        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let cancelled = Arc::new(tokio::sync::Notify::new());
        let late_side_effect = Arc::new(AtomicBool::new(false));
        let listener: Arc<dyn MessageListenerConcurrently> = {
            let started = started.clone();
            let release = release.clone();
            let cancelled = cancelled.clone();
            let late_side_effect = late_side_effect.clone();
            Arc::new(
                move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                    let started = started.clone();
                    let release = release.clone();
                    let cancelled = cancelled.clone();
                    let late_side_effect = late_side_effect.clone();
                    async move {
                        let _drop_signal = NotifyOnDrop(cancelled);
                        started.notify_one();
                        release.notified().await;
                        late_side_effect.store(true, Ordering::Release);
                        (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                    }
                },
            )
        };
        let service =
            ConsumeService::Concurrent(Arc::new(ConcurrentConsumeService::new(1, 1, listener)));
        let config = PushConsumerConfig::new("direct-consume-cancellation").unwrap();
        let consumer = Arc::new(PushConsumer::with_manager(
            config,
            ClientRuntimeManager::new(),
        ));
        let task_consumer = consumer.clone();
        let task = tokio::spawn(async move {
            task_consumer
                .consume_message_directly_using_service(
                    retry_test_message(10),
                    Some("broker-a".to_owned()),
                    service,
                )
                .await
        });
        timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("direct listener did not start");

        task.abort();
        let _ = task.await;
        timeout(Duration::from_secs(1), cancelled.notified())
            .await
            .expect("cancelling direct consume must drop its listener child");
        release.notify_waiters();
        tokio::task::yield_now().await;

        assert!(!late_side_effect.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn pop_consume_dispatches_ack_redelivery_and_replacement_receipt() {
        let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let nameserver_address = nameserver.local_addr().unwrap().to_string();
        let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let broker_address = broker_listener.local_addr().unwrap().to_string();
        let route = pop_test_route(&broker_address);

        let nameserver_peer = tokio::spawn(async move {
            let (mut stream, _) = nameserver.accept().await.unwrap();
            let incoming = read_remoting_command(&mut stream).await;
            assert_eq!(incoming.code, request::GET_ROUTEINFO_BY_TOPIC);
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: incoming.opaque,
                body: Some(route),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let now = epoch_millis();
        let initial_receipt = format!("10 {now} 60000 2 0 broker-a 0 10");
        let redelivery_receipt = format!("10 {now} 30000 2 0 broker-a 0 10");
        let replacement_receipt = format!("10 {} 45000 3 0 broker-a 0 10", now + 1);
        let expected_receipts = [
            initial_receipt.clone(),
            initial_receipt.clone(),
            redelivery_receipt.clone(),
            replacement_receipt.clone(),
        ];
        let broker_peer = tokio::spawn(async move {
            let mut stream = None;
            let expected_codes = [
                request::ACK_MESSAGE,
                request::CHANGE_MESSAGE_INVISIBLETIME,
                request::CHANGE_MESSAGE_INVISIBLETIME,
                request::ACK_MESSAGE,
            ];
            for (index, expected_code) in expected_codes.into_iter().enumerate() {
                let incoming = accept_remoting_request(&broker_listener, &mut stream).await;
                assert_eq!(incoming.code, expected_code);
                let fields = incoming.ext_fields.as_ref().unwrap();
                assert_eq!(
                    fields.get("consumerGroup"),
                    Some(&"pop-lifecycle-group".into())
                );
                assert_eq!(fields.get("topic"), Some(&"orders".into()));
                assert_eq!(fields.get("queueId"), Some(&"0".into()));
                assert_eq!(fields.get("offset"), Some(&"10".into()));
                assert_eq!(fields.get("extraInfo"), Some(&expected_receipts[index]));
                if expected_code == request::CHANGE_MESSAGE_INVISIBLETIME {
                    let (pop_time, invisible_time, revive_qid) = if index == 1 {
                        (now, 30_000, 2)
                    } else {
                        (now + 1, 45_000, 3)
                    };
                    assert_eq!(
                        fields.get("invisibleTime"),
                        Some(&(if index == 1 { "10000" } else { "30000" }).to_owned())
                    );
                    write_remoting_reply(
                        stream.as_mut().unwrap(),
                        &incoming,
                        response::SUCCESS,
                        Some(HeaderMap::from([
                            ("popTime".into(), pop_time.to_string()),
                            ("invisibleTime".into(), invisible_time.to_string()),
                            ("reviveQid".into(), revive_qid.to_string()),
                        ])),
                    )
                    .await;
                } else {
                    write_remoting_reply(
                        stream.as_mut().unwrap(),
                        &incoming,
                        response::SUCCESS,
                        None,
                    )
                    .await;
                }
            }
        });

        let mut config = PushConsumerConfig::new("pop-lifecycle-group").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(nameserver_address));
        config
            .client_config_mut()
            .set_instance_name(Some("pop-lifecycle-client".into()));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let pop_hook_events = Arc::new(Mutex::new(Vec::new()));
        struct PopHook(Arc<Mutex<Vec<String>>>);
        impl ConsumeMessageHook for PopHook {
            fn hook_name(&self) -> &str {
                "pop-consume-hook"
            }

            fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {
                self.0.lock().unwrap().push("before".into());
            }

            fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
                self.0.lock().unwrap().push(format!(
                    "{}:{}",
                    context.status.as_deref().unwrap_or("missing"),
                    context.success
                ));
            }
        }
        consumer.register_consume_message_hook(Arc::new(PopHook(pop_hook_events.clone())));
        let runtime = manager
            .get_or_create(consumer.config.client_config())
            .await
            .unwrap();
        consumer.bind_stats_manager(runtime.consumer_stats_manager());
        runtime.remoting().start().unwrap();
        runtime
            .update_topic_route("orders", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        nameserver_peer.await.unwrap();
        *write_lock(&consumer.consume_stats_group) = Some("pop-lifecycle-group".to_owned());
        consumer.set_consumer_group(Some("pop-lifecycle-live-group".into()));

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let successful_queue = Arc::new(PopProcessQueue::new_for_assignment(queue.clone(), 1));
        successful_queue.inc_found_messages(1);
        let listener_calls = Arc::new(AtomicUsize::new(0));
        let successful_listener = PushMessageListener::concurrently({
            let listener_calls = listener_calls.clone();
            move |_messages, context| {
                listener_calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready((Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context))
            }
        });
        consumer
            .clone()
            .consume_pop_batch(
                PopConsumeBatchContext {
                    process_queue: successful_queue.clone(),
                    runtime: runtime.clone(),
                    queue: queue.clone(),
                    route_topic: "orders".into(),
                    consumer_group: "pop-lifecycle-group".into(),
                    stats_consumer_group: consumer.consume_stats_group(),
                },
                successful_listener,
                vec![pop_receipt_message(Some(&initial_receipt))],
            )
            .await;
        assert_eq!(listener_calls.load(Ordering::SeqCst), 1);
        assert_eq!(successful_queue.wait_ack_count(), 0);
        tokio::time::sleep(Duration::from_secs(11)).await;
        assert!(
            consumer
                .stats_manager()
                .unwrap()
                .consume_status("pop-lifecycle-group", "orders")
                .get_consume_ok_tps()
                > 0.0
        );
        assert_eq!(
            consumer
                .stats_manager()
                .unwrap()
                .consume_status("pop-lifecycle-live-group", "orders")
                .get_consume_ok_tps(),
            0.0
        );

        let retry_queue = Arc::new(PopProcessQueue::new_for_assignment(queue.clone(), 2));
        retry_queue.inc_found_messages(1);
        let retry_listener = PushMessageListener::concurrently(|_messages, context| {
            std::future::ready((Some(ConsumeConcurrentlyStatus::ReconsumeLater), context))
        });
        consumer
            .clone()
            .consume_pop_batch(
                PopConsumeBatchContext {
                    process_queue: retry_queue.clone(),
                    runtime: runtime.clone(),
                    queue: queue.clone(),
                    route_topic: "orders".into(),
                    consumer_group: "pop-lifecycle-group".into(),
                    stats_consumer_group: consumer.consume_stats_group(),
                },
                retry_listener,
                vec![pop_receipt_message(Some(&initial_receipt))],
            )
            .await;
        assert_eq!(retry_queue.wait_ack_count(), 0);
        assert_eq!(
            pop_hook_events.lock().unwrap().as_slice(),
            [
                "before",
                "CONSUME_SUCCESS:true",
                "before",
                "RECONSUME_LATER:false"
            ]
        );

        let mut changed_message = pop_receipt_message(Some(&redelivery_receipt));
        consumer
            .change_pop_invisible_time(
                &runtime,
                &mut changed_message,
                "orders",
                "pop-lifecycle-group",
                30_000,
            )
            .await;
        assert_eq!(
            changed_message.properties().get(PROPERTY_POP_CK),
            Some(&replacement_receipt)
        );
        assert_eq!(
            consumer
                .ack_pop_message(&runtime, &changed_message, "orders", "pop-lifecycle-group")
                .await
                .unwrap(),
            AckStatus::Ok
        );

        broker_peer.await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn broker_assignment_error_preserves_worker_until_empty_assignment() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/consumer-operations.json"
        ))
        .unwrap();
        let pop_rebalance = fixture["popRebalance"].as_array().unwrap();
        assert_eq!(pop_rebalance.len(), 4);
        let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let nameserver_address = nameserver.local_addr().unwrap().to_string();
        let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let broker_address = broker_listener.local_addr().unwrap().to_string();
        let route = pop_test_route(&broker_address);
        let nameserver_peer = tokio::spawn(async move {
            let (mut stream, _) = nameserver.accept().await.unwrap();
            let incoming = read_remoting_command(&mut stream).await;
            assert_eq!(incoming.code, request::GET_ROUTEINFO_BY_TOPIC);
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: incoming.opaque,
                body: Some(route),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let pop_request_count = Arc::new(AtomicUsize::new(0));
        let peer_pop_request_count = pop_request_count.clone();
        let broker_peer = tokio::spawn(async move {
            let mut stream = None;
            let mut assignment_calls = 0;
            loop {
                let incoming = accept_remoting_request(&broker_listener, &mut stream).await;
                match incoming.code {
                    request::QUERY_ASSIGNMENT => {
                        let body: serde_json::Value = serde_json::from_slice(
                            incoming.body.as_deref().expect("assignment request body"),
                        )
                        .unwrap();
                        assert_eq!(body["topic"], "orders");
                        assert_eq!(body["consumerGroup"], "pop-assignment-flow");
                        assignment_calls += 1;
                        if assignment_calls == 3 {
                            write_remoting_reply(
                                stream.as_mut().unwrap(),
                                &incoming,
                                response::SYSTEM_ERROR,
                                None,
                            )
                            .await;
                            continue;
                        }
                        let assignments = if assignment_calls <= 4 {
                            serde_json::json!([{
                                "messageQueue": {
                                    "topic": "orders",
                                    "brokerName": "broker-a",
                                    "queueId": 0
                                },
                                "mode": "POP"
                            }])
                        } else {
                            serde_json::json!([])
                        };
                        write_remoting_reply_with_body(
                            stream.as_mut().unwrap(),
                            &incoming,
                            response::SUCCESS,
                            None,
                            Some(
                                serde_json::to_vec(&serde_json::json!({
                                    "messageQueueAssignments": assignments
                                }))
                                .unwrap(),
                            ),
                        )
                        .await;
                    }
                    request::POP_MESSAGE => {
                        let fields = incoming.ext_fields.as_ref().unwrap();
                        assert_eq!(fields.get("maxMsgNums"), Some(&"7".to_owned()));
                        assert_eq!(fields.get("invisibleTime"), Some(&"12345".to_owned()));
                        if peer_pop_request_count.fetch_add(1, Ordering::SeqCst) == 0 {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            write_remoting_reply_with_body(
                                stream.as_mut().unwrap(),
                                &incoming,
                                response::SUCCESS,
                                Some(HeaderMap::from([
                                    ("popTime".into(), epoch_millis().to_string()),
                                    ("invisibleTime".into(), "60000".into()),
                                    ("reviveQid".into(), "2".into()),
                                    ("restNum".into(), "0".into()),
                                ])),
                                Some(pop_test_message_record()),
                            )
                            .await;
                        } else {
                            write_remoting_reply(
                                stream.as_mut().unwrap(),
                                &incoming,
                                response::PULL_NOT_FOUND,
                                None,
                            )
                            .await;
                        }
                    }
                    request::HEART_BEAT
                    | request::UNREGISTER_CLIENT
                    | request::CHECK_CLIENT_CONFIG => {
                        write_remoting_reply(
                            stream.as_mut().unwrap(),
                            &incoming,
                            response::SUCCESS,
                            None,
                        )
                        .await;
                    }
                    code => panic!("unexpected scripted Broker request code {code}"),
                }
            }
        });

        let mut config = PushConsumerConfig::new("pop-assignment-flow").unwrap();
        config.set_client_rebalance(false);
        config
            .client_config_mut()
            .set_namesrv_addr(Some(nameserver_address));
        config
            .client_config_mut()
            .set_instance_name(Some("pop-assignment-client".into()));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            Arc::new(RecordingOffsetStore::default()),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        consumer.config().set_pop_batch_nums(7);
        consumer.config().set_pop_invisible_time_millis(12_345);
        runtime.remoting().start().unwrap();
        runtime
            .update_topic_route("orders", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        nameserver_peer.await.unwrap();

        let queue = MessageQueue::new("orders", "broker-a", 0);
        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        timeout(Duration::from_secs(3), async {
            while pop_request_count.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("assigned POP worker did not issue POP_MESSAGE");
        timeout(Duration::from_secs(12), async {
            while consumer
                .stats_manager()
                .unwrap()
                .consume_status("pop-assignment-flow", "orders")
                .get_pull_rt()
                <= 0.0
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("POP_FOUND must record pull latency in consumer statistics");
        let status = consumer
            .stats_manager()
            .unwrap()
            .consume_status("pop-assignment-flow", "orders");
        assert!(
            status.get_pull_rt() > 0.0,
            "unexpected POP stats: {status:?}"
        );
        assert!(
            status.get_pull_tps() > 0.0,
            "unexpected POP stats: {status:?}"
        );
        let process_queue = read_lock(&consumer.pop_process_queues)
            .get(&queue)
            .cloned()
            .expect("Broker POP assignment must activate its queue");
        assert!(!process_queue.is_dropped());
        assert!(
            consumer
                .pop_workers
                .lock()
                .unwrap()
                .get(&queue)
                .is_some_and(|worker| !worker.is_finished())
        );

        process_queue.set_last_pop_timestamp(epoch_millis() - 120_001);
        consumer
            .pull_max_idle_time_millis
            .store(i64::MAX, Ordering::Release);
        consumer
            .pop_pull_max_idle_time_millis
            .store(-1, Ordering::Release);
        let requests_before_replacement = pop_request_count.load(Ordering::SeqCst);
        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        assert_eq!(
            process_queue.is_dropped(),
            pop_rebalance[0]["oldDropped"].as_bool().unwrap()
        );
        let replacement = read_lock(&consumer.pop_process_queues)
            .get(&queue)
            .cloned()
            .expect("authoritative POP assignment must replace an expired queue");
        assert_eq!(
            !Arc::ptr_eq(&process_queue, &replacement),
            pop_rebalance[0]["replaced"].as_bool().unwrap()
        );
        assert_eq!(
            read_lock(&consumer.pop_process_queues).contains_key(&queue),
            pop_rebalance[0]["present"].as_bool().unwrap()
        );
        timeout(Duration::from_secs(3), async {
            while pop_request_count.load(Ordering::SeqCst) <= requests_before_replacement {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("replacement POP worker did not start");

        assert!(
            consumer
                .pop_workers
                .lock()
                .unwrap()
                .get(&queue)
                .is_some_and(|worker| !worker.is_finished())
        );

        consumer
            .pop_pull_max_idle_time_millis
            .store(-1, Ordering::Release);
        assert!(!consumer.rebalance_service.rebalance(&consumer).await);
        assert!(!replacement.is_dropped());
        assert!(Arc::ptr_eq(
            &replacement,
            &read_lock(&consumer.pop_process_queues)[&queue]
        ));
        assert_eq!(
            replacement.is_dropped(),
            pop_rebalance[1]["dropped"].as_bool().unwrap()
        );
        assert_eq!(
            read_lock(&consumer.pop_process_queues).contains_key(&queue),
            pop_rebalance[1]["present"].as_bool().unwrap()
        );
        assert_eq!(
            Arc::ptr_eq(
                &replacement,
                &read_lock(&consumer.pop_process_queues)[&queue]
            ),
            pop_rebalance[1]["same"].as_bool().unwrap()
        );
        assert!(
            consumer
                .pop_workers
                .lock()
                .unwrap()
                .get(&queue)
                .is_some_and(|worker| !worker.is_finished())
        );

        consumer
            .pop_pull_max_idle_time_millis
            .store(120_000, Ordering::Release);
        replacement.set_last_pop_timestamp(epoch_millis());
        replacement.drop_assignment();
        consumer.rebalance_service.rebalance(&consumer).await;
        assert_eq!(
            replacement.is_dropped(),
            pop_rebalance[2]["dropped"].as_bool().unwrap()
        );
        assert_eq!(
            read_lock(&consumer.pop_process_queues).contains_key(&queue),
            pop_rebalance[2]["present"].as_bool().unwrap()
        );
        assert_eq!(
            Arc::ptr_eq(
                &replacement,
                &read_lock(&consumer.pop_process_queues)[&queue]
            ),
            pop_rebalance[2]["same"].as_bool().unwrap()
        );

        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        assert_eq!(
            replacement.is_dropped(),
            pop_rebalance[3]["dropped"].as_bool().unwrap()
        );
        assert_eq!(
            read_lock(&consumer.pop_process_queues).contains_key(&queue),
            pop_rebalance[3]["present"].as_bool().unwrap()
        );
        assert!(!consumer.pop_workers.lock().unwrap().contains_key(&queue));
        let pop_count_after_removal = pop_request_count.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            pop_request_count.load(Ordering::SeqCst),
            pop_count_after_removal,
            "removed POP workers must not issue more Broker requests"
        );

        consumer.shutdown().await.unwrap();
        broker_peer.abort();
    }

    #[tokio::test]
    async fn assigned_pull_queue_expiry_matches_java_replacement_and_retention() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/consumer-operations.json"
        ))
        .unwrap();
        let pull_rebalance = fixture["pullRebalance"].as_array().unwrap();
        assert_eq!(pull_rebalance.len(), 4);

        let mut config = PushConsumerConfig::new("pull-assignment-expiry").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.topic_queues)
            .insert("orders".to_owned(), BTreeSet::from([queue.clone()]));
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            store,
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        let expired = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 91));
        expired.set_last_pull_timestamp(epoch_millis() - 120_001);
        write_lock(&consumer.process_queues).insert(queue.clone(), expired.clone());
        consumer
            .pull_max_idle_time_millis
            .store(-1, Ordering::Release);
        consumer.rebalance_service.rebalance(&consumer).await;
        let replacement = read_lock(&consumer.process_queues)
            .get(&queue)
            .cloned()
            .expect("assigned PULL queue must replace an expired process queue");
        assert_eq!(
            expired.is_dropped(),
            pull_rebalance[0]["oldDropped"].as_bool().unwrap()
        );
        assert_eq!(
            read_lock(&consumer.process_queues).contains_key(&queue),
            pull_rebalance[0]["present"].as_bool().unwrap()
        );
        assert_eq!(
            !Arc::ptr_eq(&expired, &replacement),
            pull_rebalance[0]["replaced"].as_bool().unwrap()
        );

        consumer
            .pull_max_idle_time_millis
            .store(i64::MAX, Ordering::Release);
        replacement.set_last_pull_timestamp(epoch_millis());
        consumer.rebalance_service.rebalance(&consumer).await;
        assert_eq!(
            replacement.is_dropped(),
            pull_rebalance[1]["dropped"].as_bool().unwrap()
        );
        assert_eq!(
            read_lock(&consumer.process_queues).contains_key(&queue),
            pull_rebalance[1]["present"].as_bool().unwrap()
        );
        assert_eq!(
            Arc::ptr_eq(&replacement, &read_lock(&consumer.process_queues)[&queue]),
            pull_rebalance[1]["same"].as_bool().unwrap()
        );

        replacement
            .drop_assignment(replacement.generation())
            .unwrap();
        consumer.rebalance_service.rebalance(&consumer).await;
        assert_eq!(
            replacement.is_dropped(),
            pull_rebalance[2]["dropped"].as_bool().unwrap()
        );
        assert_eq!(
            read_lock(&consumer.process_queues).contains_key(&queue),
            pull_rebalance[2]["present"].as_bool().unwrap()
        );
        assert_eq!(
            Arc::ptr_eq(&replacement, &read_lock(&consumer.process_queues)[&queue]),
            pull_rebalance[2]["same"].as_bool().unwrap()
        );

        write_lock(&consumer.topic_queues).insert("orders".to_owned(), BTreeSet::new());
        consumer.rebalance_service.rebalance(&consumer).await;
        assert_eq!(
            replacement.is_dropped(),
            pull_rebalance[3]["dropped"].as_bool().unwrap()
        );
        assert_eq!(
            read_lock(&consumer.process_queues).contains_key(&queue),
            pull_rebalance[3]["present"].as_bool().unwrap()
        );

        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn removed_pop_queue_finishes_inflight_found_callback_before_retiring() {
        let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let nameserver_address = nameserver.local_addr().unwrap().to_string();
        let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let broker_address = broker_listener.local_addr().unwrap().to_string();
        let route = pop_test_route(&broker_address);
        let nameserver_peer = tokio::spawn(async move {
            let (mut stream, _) = nameserver.accept().await.unwrap();
            let incoming = read_remoting_command(&mut stream).await;
            assert_eq!(incoming.code, request::GET_ROUTEINFO_BY_TOPIC);
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: incoming.opaque,
                body: Some(route),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let (pop_received, pop_received_signal) = tokio::sync::oneshot::channel();
        let (release_pop, mut release_pop_signal) = tokio::sync::oneshot::channel();
        let (ack_received, ack_received_signal) = tokio::sync::oneshot::channel();
        let filtered_receipt = format!("17 {} 60000 2 0 broker-a 0 10", epoch_millis());
        let accepted_receipt = format!("18 {} 60000 2 0 broker-a 0 11", epoch_millis());
        let mut found_record = pop_test_message_record_with_receipt(&filtered_receipt);
        found_record.extend_from_slice(&pop_test_message_record_with_tag_and_receipt(
            "blue",
            &accepted_receipt,
        ));
        let ack_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peer_ack_count = ack_count.clone();
        let broker_peer = tokio::spawn(async move {
            let (request_sender, mut request_receiver) = tokio::sync::mpsc::unbounded_channel::<(
                RemotingCommand,
                Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
            )>();
            let mut readers = tokio::task::JoinSet::new();
            let mut assignment_calls = 0;
            let mut held_pop = None;
            let mut pop_received = Some(pop_received);
            let mut ack_received = Some(ack_received);
            loop {
                tokio::select! {
                    accepted = broker_listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let (mut reader, writer) = stream.into_split();
                        let writer = Arc::new(tokio::sync::Mutex::new(writer));
                        let sender = request_sender.clone();
                        let task_writer = writer.clone();
                        readers.spawn(async move {
                            loop {
                                let incoming = read_remoting_command(&mut reader).await;
                                if sender.send((incoming, task_writer.clone())).is_err() {
                                    break;
                                }
                            }
                        });
                    }
                    Some((incoming, writer)) = request_receiver.recv() => {
                        match incoming.code {
                            request::QUERY_ASSIGNMENT => {
                                let body: serde_json::Value = serde_json::from_slice(
                                    incoming.body.as_deref().expect("assignment request body"),
                                )
                                .unwrap();
                                assert_eq!(body["topic"], "orders");
                                assert_eq!(body["consumerGroup"], "pop-inflight-drop");
                                assignment_calls += 1;
                                let assignments = if assignment_calls == 1 {
                                    serde_json::json!([{
                                        "messageQueue": {
                                            "topic": "orders",
                                            "brokerName": "broker-a",
                                            "queueId": 0
                                        },
                                        "mode": "POP"
                                    }])
                                } else {
                                    serde_json::json!([])
                                };
                                let mut writer = writer.lock().await;
                                write_remoting_reply_with_body(
                                    &mut *writer,
                                    &incoming,
                                    response::SUCCESS,
                                    None,
                                    Some(
                                        serde_json::to_vec(&serde_json::json!({
                                            "messageQueueAssignments": assignments
                                        }))
                                        .unwrap(),
                                    ),
                                )
                                .await;
                            }
                            request::POP_MESSAGE => {
                                let fields = incoming.ext_fields.as_ref().unwrap();
                                assert_eq!(fields.get("maxMsgNums"), Some(&"7".to_owned()));
                                held_pop = Some((incoming, writer));
                                pop_received.take().unwrap().send(()).unwrap();
                            }
                            request::ACK_MESSAGE => {
                                peer_ack_count.fetch_add(1, Ordering::SeqCst);
                                let fields = incoming.ext_fields.as_ref().unwrap();
                                assert_eq!(
                                    fields.get("consumerGroup"),
                                    Some(&"pop-inflight-drop".to_owned())
                                );
                                assert_eq!(fields.get("topic"), Some(&"orders".to_owned()));
                                assert_eq!(fields.get("extraInfo"), Some(&filtered_receipt));
                                let mut writer = writer.lock().await;
                                write_remoting_reply(
                                    &mut *writer,
                                    &incoming,
                                    response::SUCCESS,
                                    None,
                                )
                                .await;
                                if let Some(ack_received) = ack_received.take() {
                                    ack_received.send(()).unwrap();
                                }
                            }
                            request::HEART_BEAT
                            | request::UNREGISTER_CLIENT
                            | request::CHECK_CLIENT_CONFIG => {
                                let mut writer = writer.lock().await;
                                write_remoting_reply(
                                    &mut *writer,
                                    &incoming,
                                    response::SUCCESS,
                                    None,
                                )
                                .await;
                            }
                            code => panic!("unexpected scripted Broker request code {code}"),
                        }
                    }
                    release = &mut release_pop_signal, if held_pop.is_some() => {
                        release.unwrap();
                        let (request, writer) = held_pop.take().unwrap();
                        let mut writer = writer.lock().await;
                        write_remoting_reply_with_body(
                            &mut *writer,
                            &request,
                            response::SUCCESS,
                            Some(HeaderMap::from([
                                ("popTime".into(), epoch_millis().to_string()),
                                ("invisibleTime".into(), "60000".into()),
                                ("reviveQid".into(), "2".into()),
                                ("restNum".into(), "0".into()),
                            ])),
                            Some(found_record.clone()),
                        )
                        .await;
                    }
                    Some(_) = readers.join_next(), if !readers.is_empty() => {}
                }
            }
        });

        let mut config = PushConsumerConfig::new("pop-inflight-drop").unwrap();
        config.set_client_rebalance(false);
        config
            .client_config_mut()
            .set_namesrv_addr(Some(nameserver_address));
        config
            .client_config_mut()
            .set_instance_name(Some("pop-inflight-drop-client".into()));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let mut subscription =
            SubscriptionData::new(Some("orders".to_owned()), Some("blue".to_owned()));
        subscription.set_tags_set_owned(Some(HashSet::from([Some("blue".to_owned())])));
        write_lock(&consumer.subscriptions).insert("orders".to_owned(), subscription);
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            Arc::new(RecordingOffsetStore::default()),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let listener_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        *write_lock(&consumer.pop_listener) = Some(PushMessageListener::concurrently({
            let listener_calls = listener_calls.clone();
            move |_messages, context| {
                listener_calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready((Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context))
            }
        }));
        consumer.config().set_pop_batch_nums(7);
        consumer.config().set_pop_invisible_time_millis(60_000);
        runtime.remoting().start().unwrap();
        runtime
            .update_topic_route("orders", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        nameserver_peer.await.unwrap();

        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        timeout(Duration::from_secs(3), pop_received_signal)
            .await
            .expect("assigned POP worker did not issue POP_MESSAGE")
            .unwrap();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = read_lock(&consumer.pop_process_queues)
            .get(&queue)
            .cloned()
            .expect("the initial POP assignment must exist");

        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        assert!(process_queue.is_dropped());
        assert!(!read_lock(&consumer.pop_process_queues).contains_key(&queue));
        assert!(!consumer.pop_workers.lock().unwrap().contains_key(&queue));
        assert_eq!(consumer.pop_retired_workers.lock().unwrap().len(), 1);
        release_pop.send(()).unwrap();
        timeout(Duration::from_secs(5), ack_received_signal)
            .await
            .expect("FOUND callback must ACK filtered messages after assignment removal")
            .unwrap();
        timeout(Duration::from_secs(3), async {
            while consumer
                .pop_retired_workers
                .lock()
                .unwrap()
                .iter()
                .any(|worker| !worker.is_finished())
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("retired POP worker must exit after processing its in-flight response");
        timeout(Duration::from_secs(3), async {
            while consumer
                .pop_consume_workers
                .lock()
                .unwrap()
                .iter()
                .any(|worker| !worker.is_finished())
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("accepted message consume request must finish on the dropped queue");
        assert_eq!(listener_calls.load(Ordering::SeqCst), 0);
        assert_eq!(ack_count.load(Ordering::SeqCst), 1);

        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
        broker_peer.abort();
    }

    #[tokio::test]
    async fn rebalance_truncates_unsubscribed_pop_queue_and_retires_inflight_worker() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/consumer-operations.json"
        ))
        .unwrap();
        let expected = &fixture["popUnsubscribedTruncation"];

        let mut config = PushConsumerConfig::new("pop-unsubscribed-truncation").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            Arc::new(RecordingOffsetStore::default()),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(PopProcessQueue::new_for_assignment(queue.clone(), 1));
        let worker_stopped = Arc::new(AtomicBool::new(false));
        struct WorkerDropSignal(Arc<AtomicBool>);
        impl Drop for WorkerDropSignal {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let (finish_worker, worker_finished) = tokio::sync::oneshot::channel();
        let drop_signal = WorkerDropSignal(worker_stopped.clone());
        let worker = tokio::spawn(async move {
            let _drop_signal = drop_signal;
            let _ = worker_finished.await;
        });
        write_lock(&consumer.pop_process_queues).insert(queue.clone(), process_queue.clone());
        consumer
            .pop_workers
            .lock()
            .unwrap()
            .insert(queue.clone(), worker);

        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        assert_eq!(
            process_queue.is_dropped(),
            expected["dropped"].as_bool().unwrap()
        );
        assert_eq!(
            read_lock(&consumer.pop_process_queues).contains_key(&queue),
            expected["present"].as_bool().unwrap()
        );
        assert!(!consumer.pop_workers.lock().unwrap().contains_key(&queue));
        assert!(!worker_stopped.load(Ordering::Acquire));
        assert_eq!(consumer.pop_retired_workers.lock().unwrap().len(), 1);
        finish_worker.send(()).unwrap();
        timeout(Duration::from_secs(1), async {
            while !worker_stopped.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("retired POP worker must finish after its in-flight operation");

        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn rebalance_switch_to_pull_removes_pop_queue_and_retires_inflight_worker() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/consumer-operations.json"
        ))
        .unwrap();
        let expected = &fixture["popToPullTransition"];

        let mut config = PushConsumerConfig::new("pop-to-pull").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let queue = MessageQueue::new("orders", "broker-a", 0);
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.topic_queues)
            .insert("orders".to_owned(), BTreeSet::from([queue.clone()]));
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            Arc::new(RecordingOffsetStore::default()),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        let pull_process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 2));
        let pop_process_queue = Arc::new(PopProcessQueue::new_for_assignment(queue.clone(), 3));
        write_lock(&consumer.process_queues).insert(queue.clone(), pull_process_queue.clone());
        write_lock(&consumer.pop_process_queues).insert(queue.clone(), pop_process_queue.clone());
        let worker_stopped = Arc::new(AtomicBool::new(false));
        struct WorkerDropSignal(Arc<AtomicBool>);
        impl Drop for WorkerDropSignal {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let (finish_worker, worker_finished) = tokio::sync::oneshot::channel();
        let drop_signal = WorkerDropSignal(worker_stopped.clone());
        let worker = tokio::spawn(async move {
            let _drop_signal = drop_signal;
            let _ = worker_finished.await;
        });
        consumer
            .pop_workers
            .lock()
            .unwrap()
            .insert(queue.clone(), worker);

        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        assert_eq!(
            pop_process_queue.is_dropped(),
            expected["popDropped"].as_bool().unwrap()
        );
        assert_eq!(
            read_lock(&consumer.pop_process_queues).contains_key(&queue),
            expected["popPresent"].as_bool().unwrap()
        );
        let retained_pull_queue = read_lock(&consumer.process_queues)
            .get(&queue)
            .cloned()
            .expect("PULL assignment retains its active process queue");
        assert_eq!(
            read_lock(&consumer.process_queues).contains_key(&queue),
            expected["pullPresent"].as_bool().unwrap()
        );
        assert_eq!(
            Arc::ptr_eq(&pull_process_queue, &retained_pull_queue),
            expected["pullSame"].as_bool().unwrap()
        );
        assert!(!retained_pull_queue.is_dropped());
        assert!(!consumer.pop_workers.lock().unwrap().contains_key(&queue));
        assert!(!worker_stopped.load(Ordering::Acquire));
        assert_eq!(consumer.pop_retired_workers.lock().unwrap().len(), 1);
        finish_worker.send(()).unwrap();
        timeout(Duration::from_secs(1), async {
            while !worker_stopped.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("retired POP worker must finish after its in-flight operation");

        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    fn retry_test_message(offset: i64) -> MessageExt {
        // Java 5.5.1 producer-produced `ipv4-v1` record from
        // fixtures/messages/records.json (SHA-256 2F93B0A748E645D8AD2CEA027ABDF2C356ED16B74CEAF70A63D97B0A04861BD0CF).
        use base64::Engine;
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/messages/records.json"))
                .unwrap();
        let decoded = &fixture["cases"][0]["decoded"];
        let properties: HashMap<String, String> =
            serde_json::from_value(decoded["properties"].clone()).unwrap();
        let mut body = Message::new(
            decoded["topic"].as_str().unwrap(),
            base64::engine::general_purpose::STANDARD
                .decode(decoded["bodyBase64"].as_str().unwrap())
                .unwrap(),
        );
        body.set_flag(decoded["flag"].as_i64().unwrap() as i32);
        for (key, value) in properties {
            body.set_system_property(key, value).unwrap();
        }
        let mut message = crate::test_support::message_ext_from(body);
        message.set_queue_offset(offset);
        message.set_broker_name(Some("broker-a".into()));
        message.set_commit_log_offset(offset + 100);
        message.set_message_id(Some(format!("broker-msg-{offset}")));
        message.set_reconsume_times(2);
        message
            .set_system_property(PROPERTY_ORIGIN_MESSAGE_ID, "origin-msg")
            .unwrap();
        message
            .set_system_property(PROPERTY_RECONSUME_TIME, "2")
            .unwrap();
        message
            .set_system_property(PROPERTY_MAX_RECONSUME_TIMES, "7")
            .unwrap();
        message
            .set_system_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "source-uniq")
            .unwrap();
        message
            .set_system_property(PROPERTY_TRANSACTION_PREPARED, "true")
            .unwrap();
        message
            .set_system_property(PROPERTY_KEYS, "order-1")
            .unwrap();
        message
    }

    fn retry_topic_message(topic: &str, origin_topic: Option<&str>, offset: i64) -> MessageExt {
        let mut message = crate::test_support::message_ext_from(Message::new(topic, Vec::new()));
        message.set_queue_offset(offset);
        if let Some(origin_topic) = origin_topic {
            message
                .set_system_property(PROPERTY_RETRY_TOPIC, origin_topic)
                .unwrap();
        }
        message
    }

    fn consume_request(
        queue: MessageQueue,
        generation: u64,
        messages: Vec<MessageExt>,
    ) -> ConsumeRequest {
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue, generation));
        process_queue.insert_messages(generation, messages).unwrap();
        let batch = process_queue
            .take_batch(generation, usize::MAX)
            .unwrap()
            .unwrap();
        ConsumeRequest {
            process_queue,
            batch,
        }
    }

    struct ConsumerGroupHookRecorder {
        before: Arc<std::sync::Mutex<Vec<Option<String>>>>,
        after: Arc<std::sync::Mutex<Vec<Option<String>>>>,
    }

    impl ConsumeMessageHook for ConsumerGroupHookRecorder {
        fn hook_name(&self) -> &str {
            "consumer-group-hook-recorder"
        }

        fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
            self.before
                .lock()
                .unwrap()
                .push(context.consumer_group.clone());
        }

        fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
            self.after
                .lock()
                .unwrap()
                .push(context.consumer_group.clone());
        }
    }

    fn idle_consume_service() -> Arc<ConcurrentConsumeService> {
        Arc::new(ConcurrentConsumeService::new(
            1,
            1,
            Arc::new(
                |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                },
            ),
        ))
    }

    #[derive(Default)]
    struct RecordingConsumeMessageService {
        direct_calls: AtomicUsize,
        pull_submissions: AtomicUsize,
        pop_submissions: AtomicUsize,
        shutdown_calls: AtomicUsize,
        submitted_offsets: Mutex<Vec<i64>>,
    }

    impl ConsumeMessageService for RecordingConsumeMessageService {
        fn start(&self) -> ConsumeMessageServiceFuture<'_, ()> {
            Box::pin(async {})
        }

        fn shutdown(&self, _await_termination_millis: i64) -> ConsumeMessageServiceFuture<'_, ()> {
            self.shutdown_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {})
        }

        fn update_core_pool_size(&self, _core_pool_size: i32) {}

        fn inc_core_pool_size(&self) {}

        fn dec_core_pool_size(&self) {}

        fn core_pool_size(&self) -> i32 {
            1
        }

        fn consume_message_directly(
            &self,
            _message: MessageExt,
            _broker_name: Option<String>,
        ) -> ConsumeMessageServiceFuture<'_, ConsumeMessageDirectlyResult> {
            self.direct_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { ConsumeMessageDirectlyResult::default() })
        }

        fn submit_consume_request(
            &self,
            messages: Vec<Arc<MessageExt>>,
            _process_queue: Arc<ProcessQueue>,
            _message_queue: MessageQueue,
            _dispatch_to_consume: bool,
        ) {
            self.pull_submissions.fetch_add(1, Ordering::SeqCst);
            self.submitted_offsets
                .lock()
                .unwrap()
                .extend(messages.iter().map(|message| message.queue_offset()));
        }

        fn submit_pop_consume_request(
            &self,
            _messages: Vec<MessageExt>,
            _process_queue: Arc<PopProcessQueue>,
            _message_queue: MessageQueue,
        ) {
            self.pop_submissions.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn replacement_consume_service_receives_push_work_but_not_pop_work() {
        let mut config = PushConsumerConfig::new("replacement-consume-message-service").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            Arc::new(RecordingOffsetStore::default()),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        let replacement = Arc::new(RecordingConsumeMessageService::default());
        let replacement_api: Arc<dyn ConsumeMessageService> = replacement.clone();
        consumer.set_consume_message_service(Some(replacement_api));

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 40));
        process_queue
            .insert_messages(40, vec![retry_test_message(50)])
            .unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let context = consumer.pull_context().await.unwrap();
        consumer.submit_consume_batch(&context, process_queue.clone(), 40);

        assert_eq!(replacement.pull_submissions.load(Ordering::SeqCst), 1);
        assert_eq!(*replacement.submitted_offsets.lock().unwrap(), [50]);
        assert_eq!(process_queue.snapshot(40).unwrap().in_flight_count, 1);

        consumer
            .consume_message_directly(retry_test_message(51), Some("broker-a".into()))
            .await
            .unwrap();
        assert_eq!(replacement.direct_calls.load(Ordering::SeqCst), 1);

        let pop_queue = Arc::new(PopProcessQueue::new_for_assignment(queue.clone(), 1));
        pop_queue.inc_found_messages(1);
        consumer
            .dispatch_pop_messages(
                pop_queue.clone(),
                context.runtime,
                queue,
                "orders".into(),
                "replacement-consume-message-service".into(),
                vec![retry_test_message(52)],
            )
            .await;
        assert_eq!(replacement.pop_submissions.load(Ordering::SeqCst), 0);
        assert_eq!(pop_queue.wait_ack_count(), 0);

        consumer.shutdown().await.unwrap();
        assert_eq!(replacement.shutdown_calls.load(Ordering::SeqCst), 1);
        consumer.set_consume_message_service(None);
        assert!(matches!(
            consumer.update_core_pool_size(2).await,
            Err(PushConsumerError::ConsumeMessageServiceUnavailable)
        ));
    }

    async fn install_started_consumer(
        consumer: &Arc<PushConsumer>,
        manager: &ClientRuntimeManager,
        offset_store: Arc<dyn OffsetStore>,
        consume_service: Arc<ConcurrentConsumeService>,
        pull_scheduler: Arc<PullScheduler>,
        pull_rpc_workers: Arc<PullRpcWorkers>,
    ) -> Arc<ClientRuntime> {
        install_started_consumer_with_service(
            consumer,
            manager,
            offset_store,
            ConsumeService::Concurrent(consume_service),
            None,
            pull_scheduler,
            pull_rpc_workers,
        )
        .await
    }

    async fn install_started_consumer_with_service(
        consumer: &Arc<PushConsumer>,
        manager: &ClientRuntimeManager,
        offset_store: Arc<dyn OffsetStore>,
        consume_service: ConsumeService,
        orderly_lock_settings: Option<OrderlyLockSettings>,
        pull_scheduler: Arc<PullScheduler>,
        pull_rpc_workers: Arc<PullRpcWorkers>,
    ) -> Arc<ClientRuntime> {
        *consumer.owner.lock().unwrap() = Arc::downgrade(consumer);
        consumer.bind_builtin_consume_message_service(Arc::new(BuiltinConsumeMessageService {
            owner: Arc::downgrade(consumer),
            service: consume_service.clone(),
            orderly_lock_settings,
        }));
        consumer.consume_orderly.store(
            matches!(&consume_service, ConsumeService::Orderly(_)),
            Ordering::Release,
        );
        consumer
            .consume_service_max_threads
            .store(consume_service.maximum_threads(), Ordering::Release);
        consumer
            .consume_service_initialized
            .store(true, Ordering::Release);
        let mut client_config = consumer.config.client_config().clone();
        let consumer_group = client_config
            .with_namespace(consumer.config.consumer_group().as_deref())
            .unwrap()
            .unwrap();
        consumer
            .config
            .set_consumer_group(Some(consumer_group.clone()));
        *write_lock(&consumer.consume_stats_group) = Some(consumer_group.clone());
        consumer
            .rebalance_service
            .set_strategy(consumer.config.allocate_message_queue_strategy());
        consumer
            .rebalance_service
            .set_message_model(consumer.config.message_model());
        let runtime = manager
            .get_or_create(consumer.config.client_config())
            .await
            .unwrap();
        consumer.bind_stats_manager(runtime.consumer_stats_manager());
        let owner: Arc<dyn ConsumerRuntime> = Arc::new(PushConsumerRegistration {
            group: consumer_group.clone(),
            message_model: consumer.config.message_model(),
            consume_from_where: consumer.config.consume_from_where(),
            unit_mode: consumer.config.unit_mode(),
            subscriptions: consumer.subscriptions.clone(),
            topic_queues: consumer.topic_queues.clone(),
            pull_scheduler: pull_scheduler.clone(),
            pull_rpc_workers: pull_rpc_workers.clone(),
            offset_store: Arc::downgrade(&offset_store),
            consumer: Arc::downgrade(consumer),
            runtime: Arc::downgrade(&runtime),
        });
        let lease = runtime
            .register_consumer(&consumer_group, owner)
            .await
            .unwrap();
        *consumer.offset_store.lock().unwrap() = Some(offset_store.clone());
        *consumer.state.lock().await = State::Started {
            runtime: Arc::downgrade(&runtime),
            _lease: lease,
            offset_store,
            consume_service: consume_service.clone(),
            orderly_lock_settings,
            pull_scheduler: pull_scheduler.clone(),
            pull_rpc_workers: pull_rpc_workers.clone(),
        };
        consumer.set_service_state(Some(ConsumerServiceState::Running));
        consume_service
            .start(
                Arc::downgrade(consumer),
                orderly_lock_settings,
                orderly_lock_settings.is_some()
                    && consumer.config.message_model()
                        == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering,
            )
            .await;
        pull_scheduler.start(Arc::downgrade(&runtime)).unwrap();
        pull_rpc_workers
            .start(Arc::downgrade(consumer), pull_scheduler)
            .unwrap();
        runtime
    }

    #[tokio::test(start_paused = true)]
    async fn raw_task_entrypoints_use_the_owner_runtime_and_lifecycle() {
        let mut config = PushConsumerConfig::new("push-task-entrypoints").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();

        assert_eq!(fixture["executeTaskRuns"], 1);
        assert_eq!(fixture["executeTaskServiceCalls"], 1);
        assert_eq!(fixture["executeTaskLaterRuns"], 1);
        assert_eq!(fixture["executeTaskLaterDelayMillis"][0], 17);

        assert!(matches!(
            consumer.execute_task(async {}).await,
            Err(PushConsumerError::NotStarted)
        ));
        assert!(matches!(
            consumer.execute_task_later(async {}, 1).await,
            Err(PushConsumerError::NotStarted)
        ));

        install_started_consumer(
            &consumer,
            &manager,
            Arc::new(RecordingOffsetStore::default()),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        let immediate_ran = Arc::new(AtomicBool::new(false));
        let task_flag = immediate_ran.clone();
        consumer
            .execute_task(async move {
                task_flag.store(true, Ordering::Release);
            })
            .await
            .unwrap();
        for _ in 0..64 {
            if immediate_ran.load(Ordering::Acquire) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(immediate_ran.load(Ordering::Acquire));

        let delayed_ran = Arc::new(AtomicBool::new(false));
        let delayed_flag = delayed_ran.clone();
        consumer
            .execute_task_later(
                async move {
                    delayed_flag.store(true, Ordering::Release);
                },
                25,
            )
            .await
            .unwrap();
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!delayed_ran.load(Ordering::Acquire));
        tokio::time::advance(Duration::from_millis(24)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!delayed_ran.load(Ordering::Acquire));
        tokio::time::advance(Duration::from_millis(1)).await;
        for _ in 0..64 {
            if delayed_ran.load(Ordering::Acquire) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(delayed_ran.load(Ordering::Acquire));

        let negative_delay_ran = Arc::new(AtomicBool::new(false));
        let negative_delay_flag = negative_delay_ran.clone();
        consumer
            .execute_task_later(
                async move {
                    negative_delay_flag.store(true, Ordering::Release);
                },
                -1,
            )
            .await
            .unwrap();
        for _ in 0..64 {
            if negative_delay_ran.load(Ordering::Acquire) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(negative_delay_ran.load(Ordering::Acquire));

        consumer.shutdown().await.unwrap();
        assert!(matches!(
            consumer.execute_task(async {}).await,
            Err(PushConsumerError::Shutdown)
        ));
    }

    async fn install_started_orderly_consumer(
        consumer: &Arc<PushConsumer>,
        manager: &ClientRuntimeManager,
        offset_store: Arc<dyn OffsetStore>,
        listener: Arc<dyn MessageListenerOrderly>,
        orderly_lock_settings: Option<OrderlyLockSettings>,
    ) -> Arc<ClientRuntime> {
        let orderly_lock_settings = Some(orderly_lock_settings.unwrap_or_else(|| {
            OrderlyLockSettings::from_values(30_000, 20_000)
                .expect("Java default orderly lock settings are valid")
        }));
        let service = Arc::new(OrderlyConsumeService::new(
            consumer.config.consume_thread_min(),
            consumer.config.consume_thread_max(),
            listener,
        ));
        install_started_consumer_with_service(
            consumer,
            manager,
            offset_store,
            ConsumeService::Orderly(service),
            orderly_lock_settings,
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await
    }

    fn script_rebalance_membership(
        consumer: &PushConsumer,
        topic: &str,
        results: impl IntoIterator<Item = Option<Vec<String>>>,
    ) {
        consumer
            .rebalance_membership
            .lock()
            .unwrap()
            .entry(topic.to_owned())
            .or_default()
            .extend(results);
    }

    #[test]
    fn java_string_order_and_consume_timestamp_parsing_match_utility_semantics() {
        assert_eq!(
            java_string_cmp("\u{10000}", "\u{e000}"),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            java_string_cmp("client-a", "client-b"),
            std::cmp::Ordering::Less
        );
        assert!(parse_java_timestamp_millis("20240229010203").is_some());
        assert!(parse_java_timestamp_millis("20230229010203").is_some());
        assert!(parse_java_timestamp_millis("2024022901020x").is_some());
        assert!(parse_java_timestamp_millis("+20240229010203").is_none());
    }

    #[test]
    fn java_initial_offset_fixtures_match_policy_and_retry_topic_selection() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/rebalance/initial-offsets.json"
        ))
        .unwrap();
        let max_offset = fixture["mockMaxOffset"].as_i64().unwrap();
        let search_offset = fixture["mockSearchOffset"].as_i64().unwrap();
        let mut checked = 0;
        for case in fixture["cases"].as_array().unwrap() {
            checked += 1;
            let policy = match case["consumeFromWhere"].as_str().unwrap() {
                "CONSUME_FROM_LAST_OFFSET" => ConsumeFromWhere::ConsumeFromLastOffset,
                "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST" => {
                    ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst
                }
                "CONSUME_FROM_MIN_OFFSET" => ConsumeFromWhere::ConsumeFromMinOffset,
                "CONSUME_FROM_MAX_OFFSET" => ConsumeFromWhere::ConsumeFromMaxOffset,
                "CONSUME_FROM_FIRST_OFFSET" => ConsumeFromWhere::ConsumeFromFirstOffset,
                "CONSUME_FROM_TIMESTAMP" => ConsumeFromWhere::ConsumeFromTimestamp,
                other => panic!("unknown fixture consume policy {other}"),
            };
            let topic = case["topic"].as_str().unwrap();
            let stored_offset = case["storedOffset"].as_i64().unwrap();
            let stored = if stored_offset == -1 {
                OffsetRead::NotFound
            } else {
                OffsetRead::Found(stored_offset)
            };
            let plan = initial_offset_plan(
                policy,
                topic.starts_with("%RETRY%"),
                stored,
                case["consumeTimestamp"].as_str().unwrap(),
            );
            if case.get("errorCode").is_some() {
                assert!(matches!(plan, Err(PushConsumerError::InvalidOffset)));
                continue;
            }
            let plan = plan.unwrap();
            let (query, actual_offset) = match plan {
                InitialOffsetPlan::Stored(offset) => ("NONE", offset),
                InitialOffsetPlan::Fixed(offset) => ("NONE", offset),
                InitialOffsetPlan::MaxOffset => ("MAX_OFFSET", max_offset),
                InitialOffsetPlan::SearchTimestamp(_) => ("SEARCH_OFFSET", search_offset),
            };
            assert_eq!(query, case["query"].as_str().unwrap(), "{}", case["id"]);
            assert_eq!(
                actual_offset,
                case["result"].as_i64().unwrap(),
                "{}",
                case["id"]
            );
        }
        assert_eq!(checked, 24);
    }

    #[tokio::test]
    async fn initial_offsets_prefer_store_and_never_turn_query_failures_into_zero() {
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let policies = [
            ConsumeFromWhere::ConsumeFromLastOffset,
            ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst,
            ConsumeFromWhere::ConsumeFromMinOffset,
            ConsumeFromWhere::ConsumeFromMaxOffset,
            ConsumeFromWhere::ConsumeFromFirstOffset,
            ConsumeFromWhere::ConsumeFromTimestamp,
        ];
        for policy in policies {
            let mut config = PushConsumerConfig::new("initial-offset-stored").unwrap();
            config.client_config_mut().set_namesrv_addr(None);
            let manager = ClientRuntimeManager::new();
            let consumer = PushConsumer::with_manager(config, manager.clone());
            consumer.config().set_consume_from_where(policy);
            let runtime = manager
                .get_or_create(consumer.config.client_config())
                .await
                .unwrap();
            let store = Arc::new(RecordingOffsetStore::default());
            store.seed_persisted(queue.clone(), 73);
            let store_dyn: Arc<dyn OffsetStore> = store.clone();
            assert_eq!(
                consumer
                    .compute_initial_offset(&runtime, &store_dyn, &queue)
                    .await
                    .unwrap(),
                73,
                "stored offset must take precedence for {policy:?}"
            );
            let _ = runtime.shutdown().await;
        }

        for (policy, topic, expected) in [
            (ConsumeFromWhere::ConsumeFromFirstOffset, "orders", Some(0)),
            (
                ConsumeFromWhere::ConsumeFromLastOffset,
                "%RETRY%group",
                Some(0),
            ),
            (
                ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst,
                "%RETRY%group",
                Some(0),
            ),
            (
                ConsumeFromWhere::ConsumeFromMinOffset,
                "%RETRY%group",
                Some(0),
            ),
            (
                ConsumeFromWhere::ConsumeFromMaxOffset,
                "%RETRY%group",
                Some(0),
            ),
            (ConsumeFromWhere::ConsumeFromTimestamp, "%RETRY%group", None),
            (ConsumeFromWhere::ConsumeFromLastOffset, "orders", None),
            (ConsumeFromWhere::ConsumeFromTimestamp, "orders", None),
        ] {
            let mut config = PushConsumerConfig::new("initial-offset-missing").unwrap();
            config.client_config_mut().set_namesrv_addr(None);
            let manager = ClientRuntimeManager::new();
            let consumer = PushConsumer::with_manager(config, manager.clone());
            consumer.config().set_consume_from_where(policy);
            consumer.config().set_consume_timestamp("20240229010203");
            let runtime = manager
                .get_or_create(consumer.config.client_config())
                .await
                .unwrap();
            let store = Arc::new(RecordingOffsetStore::default());
            let store_dyn: Arc<dyn OffsetStore> = store.clone();
            let queue = MessageQueue::new(topic, "broker-a", 0);
            let result = consumer
                .compute_initial_offset(&runtime, &store_dyn, &queue)
                .await;
            match expected {
                Some(expected) => assert_eq!(result.unwrap(), expected, "{policy:?} {topic}"),
                None => assert!(
                    result.is_err(),
                    "{policy:?} {topic} must retain the query error"
                ),
            }
            assert!(store.values.lock().unwrap().is_empty());
            let _ = runtime.shutdown().await;
        }
    }

    #[tokio::test]
    async fn broadcasting_rebalance_activates_every_queue_with_local_offsets_and_notifies() {
        let changes = Arc::new(Mutex::new(Vec::new()));
        let recorded_changes = changes.clone();
        let queue_listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
            recorded_changes.lock().unwrap().push((
                topic.to_owned(),
                all.cloned(),
                assigned.clone(),
            ));
        });
        let mut config = PushConsumerConfig::new("rebalance-broadcast").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_client_rebalance(false);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        consumer
            .config()
            .set_message_queue_listener(Some(queue_listener));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let scheduler = Arc::new(PullScheduler::new());
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            store_dyn,
            idle_consume_service(),
            scheduler,
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let first = MessageQueue::new("orders", "broker-a", 0);
        let second = MessageQueue::new("orders", "broker-a", 1);
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.topic_queues).insert(
            "orders".to_owned(),
            BTreeSet::from([first.clone(), second.clone()]),
        );
        store.seed_persisted(first.clone(), 17);
        store.seed_persisted(second.clone(), 29);
        consumer.paused.store(true, Ordering::Release);

        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        {
            let queues = read_lock(&consumer.process_queues);
            assert_eq!(queues.len(), 2);
            assert!(queues.contains_key(&first) && queues.contains_key(&second));
        }
        assert_eq!(store.values.lock().unwrap().get(&first), Some(&17));
        assert_eq!(store.values.lock().unwrap().get(&second), Some(&29));
        {
            let changes = changes.lock().unwrap();
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].0, "orders");
            assert_eq!(
                changes[0].1,
                Some(HashSet::from([first.clone(), second.clone()]))
            );
            assert_eq!(changes[0].2, HashSet::from([first, second]));
        }
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn missing_clustering_route_notifies_empty_and_keeps_assignment_without_membership_lookup()
     {
        let changes = Arc::new(Mutex::new(Vec::new()));
        let recorded_changes = changes.clone();
        let queue_listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
            recorded_changes.lock().unwrap().push((
                topic.to_owned(),
                all.cloned(),
                assigned.clone(),
            ));
        });
        let mut config = PushConsumerConfig::new("rebalance-route-missing").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_queue_listener(Some(queue_listener));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 37));
        store.update_offset(&queue, 45, false);
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            store.clone(),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        script_rebalance_membership(&consumer, "orders", [Some(vec!["peer-a".to_owned()])]);
        consumer.paused.store(true, Ordering::Release);

        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        assert!(
            read_lock(&consumer.process_queues)
                .get(&queue)
                .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
        );
        assert!(!process_queue.snapshot(37).unwrap().dropped);
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&45));
        assert_eq!(
            consumer
                .rebalance_membership
                .lock()
                .unwrap()
                .get("orders")
                .unwrap()
                .len(),
            1,
            "the pinned Java client handles a missing route before membership lookup"
        );
        {
            let changes = changes.lock().unwrap();
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].0, "orders");
            assert_eq!(changes[0].1, Some(HashSet::new()));
            assert!(changes[0].2.is_empty());
        }
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn missing_broadcasting_route_notifies_empty_and_returns_balanced() {
        let changes = Arc::new(Mutex::new(Vec::new()));
        let recorded_changes = changes.clone();
        let queue_listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
            recorded_changes.lock().unwrap().push((
                topic.to_owned(),
                all.cloned(),
                assigned.clone(),
            ));
        });
        let mut config = PushConsumerConfig::new("rebalance-broadcast-route-missing").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_message_queue_listener(Some(queue_listener));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            Arc::new(RecordingOffsetStore::default()),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        consumer.paused.store(true, Ordering::Release);

        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        {
            let changes = changes.lock().unwrap();
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].0, "orders");
            assert_eq!(changes[0].1, Some(HashSet::new()));
            assert!(changes[0].2.is_empty());
        }
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn empty_clustering_route_notifies_empty_and_keeps_assignment_without_membership_lookup()
    {
        let changes = Arc::new(Mutex::new(Vec::new()));
        let recorded_changes = changes.clone();
        let queue_listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
            recorded_changes.lock().unwrap().push((
                topic.to_owned(),
                all.cloned(),
                assigned.clone(),
            ));
        });
        let mut config = PushConsumerConfig::new("rebalance-route-empty").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_queue_listener(Some(queue_listener));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 37));
        store.update_offset(&queue, 45, false);
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.topic_queues).insert("orders".to_owned(), BTreeSet::new());
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            store.clone(),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        script_rebalance_membership(&consumer, "orders", [Some(vec!["peer-a".to_owned()])]);
        consumer.paused.store(true, Ordering::Release);

        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        assert!(
            read_lock(&consumer.process_queues)
                .get(&queue)
                .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
        );
        assert!(!process_queue.snapshot(37).unwrap().dropped);
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&45));
        assert_eq!(
            consumer
                .rebalance_membership
                .lock()
                .unwrap()
                .get("orders")
                .unwrap()
                .len(),
            1,
            "the pinned Java client handles an empty route before membership lookup"
        );
        {
            let changes = changes.lock().unwrap();
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].0, "orders");
            assert_eq!(changes[0].1, Some(HashSet::new()));
            assert!(changes[0].2.is_empty());
        }
        consumer.shutdown().await.unwrap();
    }

    #[test]
    fn broker_assignment_partition_preserves_mixed_pull_pop_modes() {
        let pull_queue = MessageQueue::new("orders", "broker-a", 0);
        let pop_queue = MessageQueue::new("orders", "broker-a", 1);
        let default_pull_queue = MessageQueue::new("orders", "broker-a", 2);
        let make_assignment = |queue: Option<MessageQueue>, mode: Option<MessageRequestMode>| {
            let mut assignment = MessageQueueAssignment::default();
            assignment.set_message_queue(queue.map(SharedValue::new));
            assignment.set_mode(mode);
            Some(assignment)
        };
        let assignments = [
            make_assignment(Some(pull_queue.clone()), Some(MessageRequestMode::Pull)),
            make_assignment(Some(pop_queue.clone()), Some(MessageRequestMode::Pop)),
            make_assignment(Some(default_pull_queue.clone()), None),
            make_assignment(None, Some(MessageRequestMode::Pop)),
        ];

        let partition = partition_broker_assignments(assignments).unwrap();
        assert_eq!(
            partition.assigned,
            HashSet::from([
                pull_queue.clone(),
                pop_queue.clone(),
                default_pull_queue.clone()
            ])
        );
        assert_eq!(
            partition.pull,
            HashSet::from([pull_queue, default_pull_queue])
        );
        assert_eq!(partition.pop, HashSet::from([pop_queue]));
    }

    #[test]
    fn broker_assignment_with_a_null_entry_is_not_authoritative() {
        assert!(partition_broker_assignments([None]).is_none());
    }

    #[test]
    fn pop_to_pull_transition_uses_the_java_retry_topic_and_retains_its_route() {
        let mut config = PushConsumerConfig::new("retry-transition").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let consumer = PushConsumer::new(config);
        let retry_topic = "%RETRY%retry-transition_orders";

        consumer.subscribe_pop_retry_topic("orders", "retry-transition");
        assert!(read_lock(&consumer.subscriptions).contains_key(retry_topic));
        write_lock(&consumer.topic_queues).insert(
            retry_topic.to_owned(),
            BTreeSet::from([MessageQueue::new(retry_topic, "broker-a", 0)]),
        );

        consumer.unsubscribe_pop_retry_topic("orders", "retry-transition");
        assert!(!read_lock(&consumer.subscriptions).contains_key(retry_topic));
        assert!(read_lock(&consumer.topic_queues).contains_key(retry_topic));
    }

    #[test]
    fn broker_assignment_modes_match_java_pop_retry_subscription_transitions() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/consumer-operations.json"
        ))
        .unwrap();
        let rows = fixture["popRetryTransitions"].as_array().unwrap();
        assert_eq!(rows.len(), 5);

        for row in rows {
            let group = row["group"].as_str().unwrap();
            let topic = row["topic"].as_str().unwrap();
            let retry_topic = row["retryTopic"].as_str().unwrap();
            let mut config = PushConsumerConfig::new(group).unwrap();
            config.client_config_mut().set_namesrv_addr(None);
            let consumer = PushConsumer::new(config);
            if row["initialSubscription"].as_bool().unwrap() {
                consumer.subscribe_pop_retry_topic(topic, group);
            }
            write_lock(&consumer.topic_queues).insert(
                retry_topic.to_owned(),
                BTreeSet::from([MessageQueue::new(retry_topic, "broker-a", 7)]),
            );

            let pull_queues = row["pullQueueIds"]
                .as_array()
                .unwrap()
                .iter()
                .map(|queue_id| {
                    MessageQueue::new(topic, "broker-a", queue_id.as_i64().unwrap() as i32)
                })
                .collect::<HashSet<_>>();
            let pop_queues = row["popQueueIds"]
                .as_array()
                .unwrap()
                .iter()
                .map(|queue_id| {
                    MessageQueue::new(topic, "broker-a", queue_id.as_i64().unwrap() as i32)
                })
                .collect::<HashSet<_>>();
            consumer.reconcile_pop_retry_topic_subscription(
                topic,
                group,
                &pull_queues,
                &pop_queues,
            );

            assert_eq!(
                read_lock(&consumer.subscriptions).contains_key(retry_topic),
                row["subscriptionPresent"].as_bool().unwrap(),
                "{} subscription",
                row["case"].as_str().unwrap()
            );
            assert_eq!(
                read_lock(&consumer.topic_queues).contains_key(retry_topic),
                row["routePresent"].as_bool().unwrap(),
                "{} route",
                row["case"].as_str().unwrap()
            );
        }
    }

    #[tokio::test]
    async fn lite_selective_rebalance_leaves_assignments_for_pop_handling() {
        let changes = Arc::new(Mutex::new(Vec::new()));
        let recorded_changes = changes.clone();
        let queue_listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
            recorded_changes.lock().unwrap().push((
                topic.to_owned(),
                all.cloned(),
                assigned.clone(),
            ));
        });
        let mut config = PushConsumerConfig::new("rebalance-lite-selective").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::LiteSelective);
        config.set_message_queue_listener(Some(queue_listener));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 41));
        store.update_offset(&queue, 52, false);
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.topic_queues)
            .insert("orders".to_owned(), BTreeSet::from([queue.clone()]));
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            store,
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        script_rebalance_membership(&consumer, "orders", [Some(vec!["peer-a".to_owned()])]);
        consumer.paused.store(true, Ordering::Release);

        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        assert!(
            read_lock(&consumer.process_queues)
                .get(&queue)
                .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
        );
        assert_eq!(
            consumer
                .rebalance_membership
                .lock()
                .unwrap()
                .get("orders")
                .unwrap()
                .len(),
            1,
            "LiteSelective must not enter the clustering allocator"
        );
        assert!(changes.lock().unwrap().is_empty());

        consumer.config().set_client_rebalance(false);
        assert!(
            !consumer
                .rebalance_service
                .rebalance_with_order(&consumer, Some(true))
                .await
        );
        assert_eq!(
            consumer
                .rebalance_membership
                .lock()
                .unwrap()
                .get("orders")
                .unwrap()
                .len(),
            1,
            "isOrder does not turn off broker assignment for LiteSelective"
        );
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unavailable_membership_keeps_the_previous_queue_assignment() {
        let mut config = PushConsumerConfig::new("rebalance-membership-unavailable").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 31));
        store.update_offset(&queue, 45, false);
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.topic_queues)
            .insert("orders".to_owned(), BTreeSet::from([queue.clone()]));
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            store.clone(),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        assert!(!consumer.rebalance_service.rebalance(&consumer).await);
        assert!(
            read_lock(&consumer.process_queues)
                .get(&queue)
                .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
        );
        assert!(!process_queue.snapshot(31).unwrap().dropped);
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&45));
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn client_rebalance_false_uses_broker_assignment_unless_orderly_forces_local() {
        let mut config = PushConsumerConfig::new("rebalance-client-disabled").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        consumer.config().set_client_rebalance(false);
        let store = Arc::new(RecordingOffsetStore::default());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.seed_persisted(queue.clone(), 19);
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.topic_queues)
            .insert("orders".to_owned(), BTreeSet::from([queue.clone()]));
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store,
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let client_id = runtime.client_id().to_owned();
        script_rebalance_membership(
            &consumer,
            "orders",
            [Some(vec![client_id.clone()]), Some(vec![client_id])],
        );
        consumer.paused.store(true, Ordering::Release);

        assert!(!consumer.rebalance_service.rebalance(&consumer).await);
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
        assert_eq!(
            consumer
                .rebalance_membership
                .lock()
                .unwrap()
                .get("orders")
                .unwrap()
                .len(),
            2,
            "broker assignment mode must not request the local membership list"
        );

        assert!(
            !consumer
                .rebalance_service
                .rebalance_with_order(&consumer, Some(true))
                .await
        );
        assert_eq!(
            consumer
                .rebalance_membership
                .lock()
                .unwrap()
                .get("orders")
                .unwrap()
                .len(),
            2,
            "the doRebalance isOrder argument does not select local assignment"
        );

        consumer.config().set_client_rebalance(true);
        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        let process_queue = read_lock(&consumer.process_queues)
            .get(&queue)
            .cloned()
            .expect("local assignment creates the process queue");
        assert!(process_queue.is_locked());

        consumer.config().set_client_rebalance(false);
        consumer.consume_orderly.store(true, Ordering::Release);
        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn clustering_membership_join_leave_and_unavailable_cycles_converge() {
        let mut config = PushConsumerConfig::new("rebalance-membership-churn").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let queues = (0..4)
            .map(|queue_id| MessageQueue::new("orders", "broker-a", queue_id))
            .collect::<Vec<_>>();
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.topic_queues)
            .insert("orders".to_owned(), queues.iter().cloned().collect());
        for (index, queue) in queues.iter().enumerate() {
            store.seed_persisted(queue.clone(), 100 + index as i64);
        }
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            store.clone(),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let runtime = match &*consumer.state.lock().await {
            State::Started { runtime, .. } => runtime.upgrade().unwrap(),
            State::Created | State::Shutdown => unreachable!(),
        };
        consumer.paused.store(true, Ordering::Release);
        let current_id = runtime.client_id().to_owned();
        let all = queues.clone();
        let expected = |ids: &[&str]| {
            let mut ids = ids.iter().map(|id| (*id).to_owned()).collect::<Vec<_>>();
            ids.sort_by(|left, right| java_string_cmp(left, right));
            consumer
                .config
                .allocate_message_queue_strategy()
                .allocate(
                    consumer.config.consumer_group().as_deref().unwrap(),
                    &current_id,
                    &all,
                    &ids,
                )
                .unwrap()
                .into_iter()
                .collect::<HashSet<_>>()
        };

        let peer = "peer-z";
        script_rebalance_membership(
            &consumer,
            "orders",
            [
                Some(vec![current_id.clone(), peer.to_owned()]),
                Some(vec![current_id.clone()]),
                Some(vec![
                    "client-a".to_owned(),
                    "client-b".to_owned(),
                    current_id.clone(),
                ]),
                None,
                Some(Vec::new()),
                Some(vec![peer.to_owned()]),
                Some(vec![current_id.clone()]),
            ],
        );
        let expected_sets = [
            expected(&[&current_id, peer]),
            expected(&[&current_id]),
            expected(&["client-a", "client-b", &current_id]),
            expected(&["client-a", "client-b", &current_id]),
            expected(&["client-a", "client-b", &current_id]),
            HashSet::new(),
            expected(&[&current_id]),
        ];
        for (round, expected) in expected_sets.iter().enumerate() {
            let balanced = consumer.rebalance_service.rebalance(&consumer).await;
            assert!(
                balanced,
                "membership round {round} should match Java result"
            );
            let working = read_lock(&consumer.process_queues)
                .iter()
                .filter(|(queue, process_queue)| {
                    queue.topic() == Some("orders")
                        && process_queue
                            .snapshot(process_queue.generation())
                            .is_ok_and(|snapshot| !snapshot.dropped)
                })
                .map(|(queue, _)| queue.clone())
                .collect::<HashSet<_>>();
            assert_eq!(&working, expected, "membership round {round}");
        }
        assert_eq!(store.persisted.lock().unwrap().len(), queues.len());
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn queue_removal_fences_a_listener_already_in_flight() {
        let mut config = PushConsumerConfig::new("rebalance-listener-race").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let scheduler = Arc::new(PullScheduler::new());
        let consume_service = idle_consume_service();
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store_dyn.clone(),
            consume_service.clone(),
            scheduler.clone(),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.update_offset(&queue, 50, false);
        let request = consume_request(queue.clone(), 72, vec![message(50)]);
        let process_queue = request.process_queue.clone();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let listener_started = Arc::new(tokio::sync::Barrier::new(2));
        let listener_release = Arc::new(tokio::sync::Barrier::new(2));
        let started = listener_started.clone();
        let release = listener_release.clone();
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let started = started.clone();
                let release = release.clone();
                async move {
                    started.wait().await;
                    release.wait().await;
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        );
        let running_consumer = consumer.clone();
        let listener_task = tokio::spawn(async move {
            running_consumer
                .run_consume_request(request, listener)
                .await;
        });
        listener_started.wait().await;
        process_queue.drop_assignment(72).unwrap();
        scheduler.cancel(&queue, 72);
        let context = PullContext {
            runtime: runtime.clone(),
            offset_store: store_dyn,
            scheduler,
            consume_service: ConsumeService::Concurrent(consume_service),
        };
        assert!(
            consumer
                .remove_process_queue_safely(&context, &queue, &process_queue)
                .await
        );
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
        listener_release.wait().await;
        listener_task.await.unwrap();
        assert_eq!(store.values.lock().unwrap().get(&queue), None);
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&50));
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_reset_matches_java_live_maps_nulls_and_per_queue_failures() {
        struct NonPush;
        impl ConsumerRuntime for NonPush {
            fn heartbeat_data(&self) -> ConsumerData {
                ConsumerData::default()
            }
        }
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/consumer-operations.json"
        ))
        .unwrap();
        let cases = fixture["resetOffset"].as_array().unwrap();
        assert_eq!(cases.len(), 15);
        let retention_cases = fixture["rebalanceRetention"].as_array().unwrap();
        assert_eq!(retention_cases.len(), 3);
        for case in cases.iter().chain(retention_cases) {
            let name = case["name"].as_str().unwrap();
            let mut config = PushConsumerConfig::new(format!("reset-{name}")).unwrap();
            config
                .client_config_mut()
                .set_namesrv_addr(Some(String::new()));
            config.set_message_model(
                rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting,
            );
            let manager = ClientRuntimeManager::new();
            let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
            let store = Arc::new(RecordingOffsetStore::default());
            let store_dyn: Arc<dyn OffsetStore> = store.clone();
            let runtime = if case["orderly"].as_bool().unwrap() {
                install_started_orderly_consumer(
                    &consumer,
                    &manager,
                    store_dyn,
                    Arc::new(EmptyOrderlyListener),
                    None,
                )
                .await
            } else {
                install_started_consumer(
                    &consumer,
                    &manager,
                    store_dyn,
                    idle_consume_service(),
                    Arc::new(PullScheduler::new()),
                    Arc::new(PullRpcWorkers::new()),
                )
                .await
            };
            let queue0 = MessageQueue::new("orders", "broker-a", 0);
            let queue1 = MessageQueue::new("orders", "broker-a", 1);
            if name.starts_with("subscribed-null-value-") || name == "subscribed-pull-expired" {
                write_lock(&consumer.subscriptions).insert(
                    "orders".into(),
                    SubscriptionData::new(Some("orders".into()), Some("*".into())),
                );
                if name.ends_with("-assigned") || name == "subscribed-pull-expired" {
                    write_lock(&consumer.topic_queues)
                        .insert("orders".into(), BTreeSet::from([queue0.clone()]));
                }
            }
            let mut queues = BTreeMap::new();
            if name != "empty-push-null-inputs" {
                let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue0.clone(), 100));
                if name == "subscribed-pull-expired" {
                    process_queue
                        .set_last_pull_timestamp_for_assignment(100, epoch_millis() - 120_001)
                        .unwrap();
                    store
                        .fail_read_queues
                        .lock()
                        .unwrap()
                        .insert(queue0.clone());
                    assert!(process_queue.is_pull_expired_with_timeout(120_000));
                    assert!(read_lock(&consumer.subscriptions).contains_key("orders"));
                    assert!(
                        read_lock(&consumer.topic_queues)
                            .get("orders")
                            .is_some_and(|queues| queues.contains(&queue0))
                    );
                    assert_eq!(
                        consumer.rebalance_service.message_model_for_test(),
                        rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting
                    );
                }
                write_lock(&consumer.process_queues).insert(queue0.clone(), process_queue.clone());
                queues.insert(0, process_queue);
                store.values.lock().unwrap().insert(queue0.clone(), 50);
            }
            if name == "per-queue-failure" {
                let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue1.clone(), 101));
                write_lock(&consumer.process_queues).insert(queue1.clone(), process_queue.clone());
                queues.insert(1, process_queue);
                store.values.lock().unwrap().insert(queue1.clone(), 60);
                store
                    .fail_persist_queues
                    .lock()
                    .unwrap()
                    .insert(queue0.clone());
            }
            let owner = if name == "pull-group" {
                Arc::new(NonPush) as Arc<dyn ConsumerRuntime>
            } else {
                runtime
                    .select_consumer(&format!("reset-{name}"))
                    .unwrap()
                    .unwrap()
            };
            runtime
                .consumer_table()
                .put("selected", owner)
                .await
                .unwrap();
            let offsets = case["offsets"].as_array().map(|rows| {
                SharedValue::new(
                    rows.iter()
                        .map(|row| {
                            let queue = row["queueId"]
                                .as_i64()
                                .map(|id| MessageQueue::new("orders", "broker-a", id as i32));
                            (queue, row["offset"].as_i64())
                        })
                        .collect::<BTreeMap<_, _>>(),
                )
            });
            let reset_runtime = runtime.clone();
            let reset_offsets = offsets.clone();
            let topic = case["topic"].as_str().map(str::to_owned);
            let group = case["group"].as_str().map(str::to_owned);
            let reset = tokio::spawn(async move {
                reset_runtime
                    .reset_offset(topic.as_deref(), group.as_deref(), reset_offsets)
                    .await
            });
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }
            if name.starts_with("live-") {
                assert!(consumer.is_pause());
                assert!(queues[&0].snapshot(100).unwrap().dropped);
                if name == "live-map-update" {
                    offsets.as_ref().unwrap().with_mut(|offsets| {
                        offsets.insert(Some(queue0.clone()), Some(77));
                    });
                } else {
                    let process_queue =
                        Arc::new(ProcessQueue::new_for_assignment(queue1.clone(), 101));
                    write_lock(&consumer.process_queues)
                        .insert(queue1.clone(), process_queue.clone());
                    queues.insert(1, process_queue);
                    store.values.lock().unwrap().insert(queue1.clone(), 60);
                }
            }
            tokio::time::advance(RESET_OFFSET_MAX_WAIT).await;
            let result = reset.await.unwrap();
            if name == "subscribed-pull-expired" {
                assert!(!consumer.is_pause());
            }
            assert_eq!(
                result.is_err(),
                !case["error"].is_null(),
                "{name}: {result:?}"
            );
            let rows = queues.iter().map(|(id, process_queue)| {
                let queue = if *id == 0 { &queue0 } else { &queue1 };
                serde_json::json!({"id": id, "present": read_lock(&consumer.process_queues).contains_key(queue),
                    "dropped": process_queue.snapshot(process_queue.generation()).unwrap().dropped,
                    "value": store.values.lock().unwrap().get(queue).copied(),
                    "persisted": store.persisted.lock().unwrap().get(queue).copied()})
            }).collect::<Vec<_>>();
            assert_eq!(serde_json::json!(rows), case["queues"], "{name}");
            assert_eq!(
                serde_json::to_value(&*store.reset_events.lock().unwrap()).unwrap(),
                case["events"],
                "{name}"
            );
            assert_eq!(
                consumer.is_pause(),
                case["pausedAfter"].as_bool().unwrap(),
                "{name}"
            );
            runtime.consumer_table().remove("selected").await.unwrap();
            consumer.shutdown().await.unwrap();
            runtime.shutdown().await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn reset_store_handoff_uses_live_fields_and_calls_remove_outside_queue_lock() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/consumer-operations.json"
        ))
        .unwrap();
        let expected = &fixture["resetStoreHandoff"];
        let mut config = PushConsumerConfig::new("reset-store-handoff").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let first = Arc::new(RecordingOffsetStore::default());
        let second = Arc::new(RecordingOffsetStore::default());
        let third = Arc::new(RecordingOffsetStore::default());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        for (store, offset) in [(&first, 50), (&second, 60), (&third, 70)] {
            store.values.lock().unwrap().insert(queue.clone(), offset);
        }
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            first.clone(),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 100));
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let weak = Arc::downgrade(&consumer);
        let store = second.clone();
        *first.update_hook.lock().unwrap() = Some(Arc::new(move || {
            *weak.upgrade().unwrap().offset_store.lock().unwrap() = Some(store.clone());
        }));
        let weak = Arc::downgrade(&consumer);
        let store = third.clone();
        *second.persist_hook.lock().unwrap() = Some(Arc::new(move || {
            *weak.upgrade().unwrap().offset_store.lock().unwrap() = Some(store.clone());
        }));
        let weak = Arc::downgrade(&consumer);
        let expected_queue = queue.clone();
        *third.remove_hook.lock().unwrap() = Some(Arc::new(move || {
            let consumer = weak.upgrade().unwrap();
            let queues = consumer
                .process_queues
                .try_read()
                .expect("custom store callback must run outside queue map locks");
            assert!(queues.contains_key(&expected_queue));
        }));
        runtime
            .reset_offset(
                Some("orders"),
                Some("reset-store-handoff"),
                Some(SharedValue::new(BTreeMap::from([(
                    Some(queue.clone()),
                    Some(7),
                )]))),
            )
            .await
            .unwrap();
        assert!(expected["error"].is_null());
        assert_eq!(
            serde_json::json!([
                first.values.lock().unwrap().get(&queue).copied(),
                second.values.lock().unwrap().get(&queue).copied(),
                third.values.lock().unwrap().get(&queue).copied(),
            ]),
            expected["storeValues"]
        );
        assert_eq!(
            second.persisted.lock().unwrap().get(&queue).copied(),
            expected["queues"][0]["persisted"].as_i64()
        );
        assert_eq!(
            read_lock(&consumer.process_queues).contains_key(&queue),
            expected["queues"][0]["present"].as_bool().unwrap()
        );
        assert_eq!(
            process_queue.snapshot(100).unwrap().dropped,
            expected["queues"][0]["dropped"].as_bool().unwrap()
        );
        assert_eq!(first.reset_events.lock().unwrap()[&0], vec!["update:7"]);
        assert_eq!(second.reset_events.lock().unwrap()[&0], vec!["persist"]);
        assert_eq!(third.reset_events.lock().unwrap()[&0], vec!["remove"]);
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn resume_completes_each_target_round_without_rebalancing_siblings() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/consumer-operations.json"
        ))
        .unwrap();
        let manager = ClientRuntimeManager::new();
        let mut owners = Vec::new();
        let mut runtimes = Vec::new();
        let mut stores = Vec::new();
        for group in ["resume-selected", "resume-sibling"] {
            let mut config = PushConsumerConfig::new(group).unwrap();
            config
                .client_config_mut()
                .set_namesrv_addr(Some(String::new()));
            let owner = Arc::new(PushConsumer::with_manager(config, manager.clone()));
            let store = Arc::new(RecordingOffsetStore::default());
            runtimes.push(
                install_started_consumer(
                    &owner,
                    &manager,
                    store.clone(),
                    idle_consume_service(),
                    Arc::new(PullScheduler::new()),
                    Arc::new(PullRpcWorkers::new()),
                )
                .await,
            );
            owners.push(owner);
            stores.push(store);
        }
        assert!(Arc::ptr_eq(&runtimes[0], &runtimes[1]));
        let queue = MessageQueue::new("orders", "broker-a", 0);
        write_lock(&owners[1].process_queues).insert(
            queue.clone(),
            Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 200)),
        );
        let cases = fixture["resume"].as_array().unwrap();
        assert_eq!(cases.len(), 2);
        for (round, expected) in cases.iter().enumerate() {
            let process_queue = Arc::new(ProcessQueue::new_for_assignment(
                queue.clone(),
                round as u64 + 100,
            ));
            write_lock(&owners[0].process_queues).insert(queue.clone(), process_queue.clone());
            if round == 0 {
                owners[0].suspend().await;
            }
            owners[0].resume().await;
            assert_eq!(
                serde_json::json!({
                    "present": read_lock(&owners[0].process_queues).contains_key(&queue),
                    "dropped": process_queue.snapshot(process_queue.generation()).unwrap().dropped,
                    "paused": owners[0].is_pause(),
                    "siblingPresent": read_lock(&owners[1].process_queues).contains_key(&queue),
                }),
                *expected,
                "resume round {round}"
            );
            assert!(
                stores
                    .iter()
                    .all(|store| store.reset_events.lock().unwrap().is_empty())
            );
        }
        for owner in owners {
            owner.shutdown().await.unwrap();
        }
        runtimes[0].shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn reset_finally_preserves_a_rebalance_callback_suspend_without_holding_state() {
        let mut config = PushConsumerConfig::new("reset-resume-callback").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            Arc::new(RecordingOffsetStore::default()),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        write_lock(&consumer.subscriptions).insert(
            "orders".into(),
            SubscriptionData::new(Some("orders".into()), Some("*".into())),
        );
        let owner = Arc::downgrade(&consumer);
        consumer
            .config()
            .set_message_queue_listener(Some(Arc::new(move |_, _, _| {
                let owner = owner.upgrade().unwrap();
                assert!(
                    owner.state.try_lock().is_ok(),
                    "state must be available to callbacks"
                );
                owner.paused.store(true, Ordering::Release);
            })));
        consumer
            .reset_consumer_offsets_shared(None, None)
            .await
            .unwrap();
        assert!(
            consumer.is_pause(),
            "finally must retain the callback's new suspend"
        );
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn rebalance_truncates_unsubscribed_queues_without_store_side_effects() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/consumer-operations.json"
        ))
        .unwrap();
        let expected = &fixture["unsubscribedTruncation"];
        let mut config = PushConsumerConfig::new("unsubscribed-truncation").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store.clone(),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 100));
        // Match the Java probe's nullable-reset precondition at entry to its
        // real doRebalance: dropped queue, retained offset and no subscription.
        process_queue.drop_assignment(100).unwrap();
        process_queue.clear();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        store.values.lock().unwrap().insert(queue.clone(), 50);
        consumer.do_rebalance().await;
        assert_eq!(
            read_lock(&consumer.process_queues).contains_key(&queue),
            expected["queues"][0]["present"].as_bool().unwrap()
        );
        assert_eq!(
            process_queue.snapshot(100).unwrap().dropped,
            expected["queues"][0]["dropped"].as_bool().unwrap()
        );
        assert_eq!(
            store.values.lock().unwrap().get(&queue).copied(),
            expected["queues"][0]["value"].as_i64()
        );
        assert_eq!(
            store.persisted.lock().unwrap().get(&queue).copied(),
            expected["queues"][0]["persisted"].as_i64()
        );
        assert_eq!(
            serde_json::to_value(&*store.reset_events.lock().unwrap()).unwrap(),
            expected["events"]
        );
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_runtime_reset_resumes_consumer_and_releases_the_monitor() {
        let mut config = PushConsumerConfig::new("cancelled-runtime-reset").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store,
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let reset_runtime = runtime.clone();
        let reset = tokio::spawn(async move {
            reset_runtime
                .reset_offset(None, Some("cancelled-runtime-reset"), None)
                .await
        });
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert!(consumer.is_pause());
        reset.abort();
        assert!(reset.await.unwrap_err().is_cancelled());
        assert!(!consumer.is_pause());
        // A cancelled caller must not leave the monitor or scheduler paused.
        assert!(
            runtime
                .register_consumer_nullable(None, None)
                .await
                .unwrap()
                .is_none()
        );
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn reset_offset_fences_a_slow_callback_and_persists_only_the_new_value() {
        let mut config = PushConsumerConfig::new("reset-offset-listener-race").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let scheduler = Arc::new(PullScheduler::new());
        let consume_service = idle_consume_service();
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store_dyn.clone(),
            consume_service.clone(),
            scheduler,
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.update_offset(&queue, 50, false);
        let request = consume_request(queue.clone(), 73, vec![message(50)]);
        let process_queue = request.process_queue.clone();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let listener_started = Arc::new(tokio::sync::Barrier::new(2));
        let listener_release = Arc::new(tokio::sync::Barrier::new(2));
        let started = listener_started.clone();
        let release = listener_release.clone();
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let started = started.clone();
                let release = release.clone();
                async move {
                    started.wait().await;
                    release.wait().await;
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        );
        let running_consumer = consumer.clone();
        let listener_task = tokio::spawn(async move {
            running_consumer
                .run_consume_request(request, listener)
                .await;
        });
        listener_started.wait().await;

        let reset_consumer = consumer.clone();
        let reset_queue = queue.clone();
        let reset = tokio::spawn(async move {
            reset_consumer
                .reset_consumer_offsets("orders", vec![(reset_queue, Some(7))])
                .await
        });
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert!(process_queue.snapshot(73).unwrap().dropped);
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&50));
        assert_eq!(store.persisted.lock().unwrap().get(&queue), None);
        tokio::time::advance(RESET_OFFSET_MAX_WAIT).await;
        tokio::task::yield_now().await;
        reset.await.unwrap().unwrap();
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
        assert_eq!(store.values.lock().unwrap().get(&queue), None);
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&7));

        listener_release.wait().await;
        listener_task.await.unwrap();
        assert_eq!(store.values.lock().unwrap().get(&queue), None);
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn orderly_reset_waits_java_lock_window_then_removes_the_dropped_queue() {
        let mut config = PushConsumerConfig::new("reset-offset-orderly-lock-window").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            store_dyn,
            Arc::new(EmptyOrderlyListener),
            None,
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.update_offset(&queue, 50, false);
        store.persist(&queue).await.unwrap();
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 73));
        process_queue
            .set_lock_state(73, true, epoch_millis())
            .unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());

        let consume_gate = process_queue.orderly_consume_gate();
        let _consume_guard = consume_gate.write_owned().await;
        let reset_consumer = consumer.clone();
        let reset_queue = queue.clone();
        let reset = tokio::spawn(async move {
            reset_consumer
                .reset_consumer_offsets("orders", vec![(reset_queue, Some(7))])
                .await
        });
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert!(process_queue.snapshot(73).unwrap().dropped);

        tokio::time::advance(Duration::from_millis(9_999)).await;
        tokio::task::yield_now().await;
        assert!(
            !reset.is_finished(),
            "Java waits up to 10 seconds for orderly consumption"
        );
        assert!(read_lock(&consumer.process_queues).contains_key(&queue));

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(
            !reset.is_finished(),
            "Java's queue-removal path gets a further 500ms lock attempt"
        );
        tokio::time::advance(Duration::from_millis(500)).await;
        tokio::task::yield_now().await;

        assert!(reset.await.unwrap().is_ok());
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&7));
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&7));
        store.update_offset(&queue, 8, true);
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&8));
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_reset_commits_then_unlocks_and_removes_when_the_gate_is_available() {
        let mut config = PushConsumerConfig::new("reset-offset-orderly-removal").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            store_dyn,
            Arc::new(EmptyOrderlyListener),
            None,
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.update_offset(&queue, 50, false);
        store.persist(&queue).await.unwrap();
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 74));
        process_queue
            .set_lock_state(74, true, epoch_millis())
            .unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        consumer
            .scripted_lock_transport
            .lock()
            .unwrap()
            .unlock_results
            .push_back(Ok(()));

        consumer
            .reset_consumer_offsets("orders", vec![(queue.clone(), Some(7))])
            .await
            .unwrap();

        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
        assert_eq!(store.values.lock().unwrap().get(&queue), None);
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&7));
        assert_eq!(
            consumer
                .scripted_lock_transport
                .lock()
                .unwrap()
                .unlock_bodies
                .len(),
            1
        );
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn reset_offset_ignores_queues_without_an_active_process_queue() {
        let mut config = PushConsumerConfig::new("reset-offset-unassigned-queue").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            store_dyn,
            Arc::new(EmptyOrderlyListener),
            None,
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 1);
        store.update_offset(&queue, 50, false);
        store.persist(&queue).await.unwrap();

        consumer
            .reset_consumer_offsets("orders", vec![(queue.clone(), Some(7))])
            .await
            .unwrap();

        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&50));
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&50));
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn reset_offset_with_null_value_retains_offset_through_unsubscribed_truncation() {
        let mut config = PushConsumerConfig::new("reset-offset-null-value").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            store_dyn,
            Arc::new(EmptyOrderlyListener),
            None,
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.update_offset(&queue, 50, false);
        store.persist(&queue).await.unwrap();
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 73));
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());

        consumer
            .reset_consumer_offsets("orders", vec![(queue.clone(), None)])
            .await
            .unwrap();

        assert!(process_queue.snapshot(73).unwrap().dropped);
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&50));
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&50));
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn reset_offset_fences_a_late_pull_response_before_persisting_the_new_value() {
        let mut config = PushConsumerConfig::new("reset-offset-pull-race").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let scheduler = Arc::new(PullScheduler::new());
        let consume_service = idle_consume_service();
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store_dyn.clone(),
            consume_service.clone(),
            scheduler.clone(),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.update_offset(&queue, 50, false);
        store.persist(&queue).await.unwrap();
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 73));
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let stale_request = Arc::new(PullRequest::new(
            "reset-offset-pull-race",
            process_queue.clone(),
            50,
        ));
        let persist_started = Arc::new(tokio::sync::Barrier::new(2));
        let persist_release = Arc::new(tokio::sync::Barrier::new(2));
        store.set_persist_barriers(persist_started.clone(), persist_release.clone());

        let reset_consumer = consumer.clone();
        let reset_queue = queue.clone();
        let reset_task = tokio::spawn(async move {
            reset_consumer
                .reset_consumer_offsets("orders", vec![(reset_queue, Some(7))])
                .await
        });
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert!(process_queue.snapshot(73).unwrap().dropped);
        tokio::time::advance(RESET_OFFSET_MAX_WAIT).await;
        tokio::task::yield_now().await;
        persist_started.wait().await;
        assert!(process_queue.snapshot(73).unwrap().dropped);
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&7));
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&50));

        let mut late_result = pull_result(PullStatus::Found, 51);
        late_result.messages = Some(vec![message(50)]);
        consumer
            .apply_pull_result(
                stale_request,
                PullContext {
                    runtime: runtime.clone(),
                    offset_store: store_dyn,
                    scheduler,
                    consume_service: ConsumeService::Concurrent(consume_service),
                },
                late_result,
            )
            .await;
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&7));
        assert_eq!(
            store.updates.lock().unwrap().as_slice(),
            &[(queue.clone(), 50, false), (queue.clone(), 7, false)]
        );

        persist_release.wait().await;
        reset_task.await.unwrap().unwrap();
        assert_eq!(store.values.lock().unwrap().get(&queue), None);
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&7));
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn direct_consume_invokes_listener_without_advancing_regular_offsets() {
        let mut config = PushConsumerConfig::new("direct-consume-offset-isolation").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.update_offset(&queue, 50, false);
        store.persist(&queue).await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let listener: Arc<dyn MessageListenerConcurrently> = {
            let calls = calls.clone();
            Arc::new(
                move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                    let calls = calls.clone();
                    async move {
                        assert_eq!(messages.len(), 1);
                        assert_eq!(messages[0].topic(), "orders");
                        assert_eq!(context.message_queue().topic(), Some("orders"));
                        assert_eq!(context.message_queue().broker_name(), Some("broker-a"));
                        calls.fetch_add(1, Ordering::AcqRel);
                        (Some(ConsumeConcurrentlyStatus::ReconsumeLater), context)
                    }
                },
            )
        };
        let consume_service = Arc::new(ConcurrentConsumeService::new(1, 1, listener));
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store_dyn,
            consume_service.clone(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        let result = consumer
            .consume_message_directly(message(812), Some("broker-a".into()))
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert_eq!(
            result.get_consume_result(),
            Some(ConsumeMessageResult::CrLater)
        );
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&50));
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&50));
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));

        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn independent_runtime_owners_with_same_client_id_isolate_consumer_stats() {
        let mut first_config = PushConsumerConfig::new("stats-isolation-group").unwrap();
        first_config.client_config_mut().set_namesrv_addr(None);
        first_config
            .client_config_mut()
            .set_client_ip(Some("198.51.100.41".into()));
        first_config
            .client_config_mut()
            .set_instance_name(Some("stats-isolation-instance".into()));
        let mut second_config = PushConsumerConfig::new("stats-isolation-group").unwrap();
        second_config.client_config_mut().set_namesrv_addr(None);
        second_config
            .client_config_mut()
            .set_client_ip(Some("198.51.100.41".into()));
        second_config
            .client_config_mut()
            .set_instance_name(Some("stats-isolation-instance".into()));

        let first = PushConsumer::with_manager(first_config, ClientRuntimeManager::new());
        let second = PushConsumer::with_manager(second_config, ClientRuntimeManager::new());
        let first_runtime = first
            .manager
            .get_or_create(first.config.client_config())
            .await
            .unwrap();
        let second_runtime = second
            .manager
            .get_or_create(second.config.client_config())
            .await
            .unwrap();
        assert_eq!(first_runtime.client_id(), second_runtime.client_id());
        first.bind_stats_manager(first_runtime.consumer_stats_manager());
        second.bind_stats_manager(second_runtime.consumer_stats_manager());
        let first_stats = first.get_consumer_stats_manager().unwrap();
        let second_stats = second.get_consumer_stats_manager().unwrap();
        assert!(!Arc::ptr_eq(&first_stats, &second_stats));
        let java_fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        assert_eq!(
            java_fixture["consumerStatsManagersAreInstanceScoped"].as_bool(),
            Some(true)
        );

        first_stats.inc_pull_rt("stats-isolation-group", "stats-isolation-topic", 3);
        first_stats.inc_pull_tps("stats-isolation-group", "stats-isolation-topic", 4);

        let second_status =
            second_stats.consume_status("stats-isolation-group", "stats-isolation-topic");
        assert_eq!(second_status.get_pull_tps(), 0.0);

        first_runtime.shutdown().await.unwrap();
        second_runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn push_consumers_sharing_one_runtime_share_consumer_stats_manager_identity() {
        let client_ip = Some("198.51.100.42".to_owned());
        let instance_name = Some("shared-stats-runtime".to_owned());
        let mut first_config = PushConsumerConfig::new("shared-stats-group-a").unwrap();
        first_config.client_config_mut().set_namesrv_addr(None);
        first_config
            .client_config_mut()
            .set_client_ip(client_ip.clone());
        first_config
            .client_config_mut()
            .set_instance_name(instance_name.clone());
        let mut second_config = PushConsumerConfig::new("shared-stats-group-b").unwrap();
        second_config.client_config_mut().set_namesrv_addr(None);
        second_config.client_config_mut().set_client_ip(client_ip);
        second_config
            .client_config_mut()
            .set_instance_name(instance_name);

        let manager = ClientRuntimeManager::new();
        let first = Arc::new(PushConsumer::with_manager(first_config, manager.clone()));
        let second = Arc::new(PushConsumer::with_manager(second_config, manager.clone()));
        assert!(first.get_consumer_stats_manager().is_none());
        assert!(second.get_consumer_stats_manager().is_none());
        let first_runtime = install_started_consumer(
            &first,
            &manager,
            Arc::new(RecordingOffsetStore::default()),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let second_runtime = install_started_consumer(
            &second,
            &manager,
            Arc::new(RecordingOffsetStore::default()),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        assert!(Arc::ptr_eq(&first_runtime, &second_runtime));
        let first_stats = first.get_consumer_stats_manager().unwrap();
        let second_stats = second.get_consumer_stats_manager().unwrap();
        assert!(Arc::ptr_eq(&first_stats, &second_stats));

        first_stats.inc_pull_rt("shared-stats-group-a", "orders", 7);
        first_stats.inc_pull_tps("shared-stats-group-a", "orders", 3);
        assert_eq!(
            second_stats
                .consume_status("shared-stats-group-b", "orders")
                .get_pull_tps(),
            0.0
        );

        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
        first_runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_consume_stats_keep_the_start_group_after_facade_mutation() {
        let mut config = PushConsumerConfig::new("consume-stats-snapshot-group").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store,
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["consumeServiceGroupBeforeMutation"],
            "phase18-consume-service-group"
        );
        assert_eq!(
            fixture["consumeServiceFacadeGroupAfterMutation"],
            "phase18-consume-service-group-mutated"
        );
        for key in [
            "concurrentConsumeServiceGroup",
            "orderlyConsumeServiceGroup",
            "popConcurrentConsumeServiceGroup",
            "popOrderlyConsumeServiceGroup",
        ] {
            assert_eq!(fixture[key], fixture["consumeServiceGroupBeforeMutation"]);
        }

        consumer.set_consumer_group(Some("consume-stats-live-group".into()));
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let request = consume_request(queue, 191, vec![message(1)]);
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            },
        );
        consumer
            .clone()
            .run_consume_request(request, listener)
            .await;
        tokio::time::sleep(Duration::from_secs(11)).await;

        assert!(
            consumer
                .stats_manager()
                .unwrap()
                .consume_status("consume-stats-snapshot-group", "orders")
                .get_consume_ok_tps()
                > 0.0
        );
        assert_eq!(
            consumer
                .stats_manager()
                .unwrap()
                .consume_status("consume-stats-live-group", "orders")
                .get_consume_ok_tps(),
            0.0
        );
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_consume_retry_topics_match_java_after_facade_group_mutation() {
        let group = "phase18-consume-retry-snapshot";
        let live_group = "phase18-consume-retry-live";
        let mut config = PushConsumerConfig::new(group).unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store,
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        consumer.set_consumer_group(Some(live_group.to_owned()));
        let hook_before_groups = Arc::new(std::sync::Mutex::new(Vec::new()));
        let hook_after_groups = Arc::new(std::sync::Mutex::new(Vec::new()));
        consumer.register_consume_message_hook(Arc::new(ConsumerGroupHookRecorder {
            before: hook_before_groups.clone(),
            after: hook_after_groups.clone(),
        }));

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        let received_topics = Arc::new(std::sync::Mutex::new(Vec::new()));
        let listener: Arc<dyn MessageListenerConcurrently> = {
            let received_topics = received_topics.clone();
            Arc::new(
                move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                    let received_topics = received_topics.clone();
                    async move {
                        *received_topics.lock().unwrap() = messages
                            .iter()
                            .map(|message| message.topic().to_owned())
                            .collect();
                        (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                    }
                },
            )
        };
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let request = consume_request(
            queue,
            193,
            vec![
                retry_topic_message(
                    "%RETRY%tenant%phase18-consume-retry-snapshot_orders",
                    None,
                    1,
                ),
                retry_topic_message("%RETRY%phase18-consume-retry-live", Some("tenant%retry"), 2),
                retry_topic_message("tenant%events", None, 3),
            ],
        );
        consumer
            .clone()
            .run_consume_request(request, listener)
            .await;

        assert_eq!(
            fixture["concurrentConsumeCallbackTopics"],
            serde_json::json!(*received_topics.lock().unwrap())
        );
        assert_eq!(
            fixture["consumeHookBeforeGroups"][0],
            serde_json::json!(hook_before_groups.lock().unwrap()[0])
        );
        assert_eq!(
            fixture["consumeHookAfterGroups"][0],
            serde_json::json!(hook_after_groups.lock().unwrap()[0])
        );
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn direct_consume_retry_topics_use_the_service_group_snapshot() {
        let group = "phase18-consume-retry-snapshot";
        let mut config = PushConsumerConfig::new(group).unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let received_topics = Arc::new(std::sync::Mutex::new(Vec::new()));
        let listener: Arc<dyn MessageListenerConcurrently> = {
            let received_topics = received_topics.clone();
            Arc::new(
                move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                    let received_topics = received_topics.clone();
                    async move {
                        *received_topics.lock().unwrap() = messages
                            .iter()
                            .map(|message| message.topic().to_owned())
                            .collect();
                        (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                    }
                },
            )
        };
        let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store,
            Arc::new(ConcurrentConsumeService::new(1, 1, listener)),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        consumer.set_consumer_group(Some("phase18-consume-retry-live".into()));

        let result = consumer
            .consume_message_directly(
                retry_topic_message(
                    "%RETRY%tenant%phase18-consume-retry-snapshot",
                    Some("tenant%direct-concurrent"),
                    4,
                ),
                Some("broker-a".to_owned()),
            )
            .await
            .unwrap();
        assert_eq!(
            result.get_consume_result(),
            Some(ConsumeMessageResult::CrSuccess)
        );

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["concurrentConsumeDirectTopics"],
            serde_json::json!(*received_topics.lock().unwrap())
        );
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_consume_retry_topics_match_java_snapshot_and_live_group_paths() {
        let group = "phase18-consume-retry-snapshot";
        let live_group = "phase18-consume-retry-live";
        let mut config = PushConsumerConfig::new(group).unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let received_topics = Arc::new(std::sync::Mutex::new(Vec::new()));
        let listener: Arc<dyn MessageListenerOrderly> = {
            let received_topics = received_topics.clone();
            Arc::new(
                move |messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                    let received_topics = received_topics.clone();
                    async move {
                        received_topics.lock().unwrap().push(
                            messages
                                .iter()
                                .map(|message| message.topic().to_owned())
                                .collect::<Vec<_>>(),
                        );
                        (Some(ConsumeOrderlyStatus::Success), context)
                    }
                },
            )
        };
        let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
        let runtime =
            install_started_orderly_consumer(&consumer, &manager, store, listener.clone(), None)
                .await;
        consumer.set_consumer_group(Some(live_group.to_owned()));
        let hook_before_groups = Arc::new(std::sync::Mutex::new(Vec::new()));
        let hook_after_groups = Arc::new(std::sync::Mutex::new(Vec::new()));
        consumer.register_consume_message_hook(Arc::new(ConsumerGroupHookRecorder {
            before: hook_before_groups.clone(),
            after: hook_after_groups.clone(),
        }));

        let direct_result = consumer
            .consume_message_directly(
                retry_topic_message(
                    "%RETRY%tenant%phase18-consume-retry-snapshot",
                    Some("tenant%direct-orderly"),
                    5,
                ),
                Some("broker-a".to_owned()),
            )
            .await
            .unwrap();
        assert_eq!(
            direct_result.get_consume_result(),
            Some(ConsumeMessageResult::CrSuccess)
        );

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue, 194));
        process_queue
            .insert_messages(
                194,
                vec![retry_topic_message(
                    "%RETRY%phase18-consume-retry-live",
                    Some("tenant%orderly-live"),
                    6,
                )],
            )
            .unwrap();
        consumer
            .clone()
            .run_orderly_consume_request(
                OrderlyConsumeRequest {
                    process_queue,
                    generation: 194,
                },
                listener,
            )
            .await;

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        {
            let received_topics = received_topics.lock().unwrap();
            assert_eq!(
                fixture["orderlyConsumeDirectTopics"],
                serde_json::json!(received_topics[0])
            );
            assert_eq!(
                fixture["orderlyConsumeCallbackTopics"],
                serde_json::json!(received_topics[1])
            );
        }
        assert_eq!(
            fixture["consumeHookBeforeGroups"][1],
            serde_json::json!(hook_before_groups.lock().unwrap()[0])
        );
        assert_eq!(
            fixture["consumeHookAfterGroups"][1],
            serde_json::json!(hook_after_groups.lock().unwrap()[0])
        );
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_consume_stats_keep_the_start_group_after_facade_mutation() {
        let mut config = PushConsumerConfig::new("orderly-stats-snapshot-group").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
        let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
                (Some(ConsumeOrderlyStatus::Success), context)
            },
        );
        let runtime =
            install_started_orderly_consumer(&consumer, &manager, store, listener.clone(), None)
                .await;

        consumer.set_consumer_group(Some("orderly-stats-live-group".into()));
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue, 192));
        process_queue
            .insert_messages(192, vec![message(1)])
            .unwrap();
        consumer
            .clone()
            .run_orderly_consume_request(
                OrderlyConsumeRequest {
                    process_queue,
                    generation: 192,
                },
                listener,
            )
            .await;
        tokio::time::sleep(Duration::from_secs(11)).await;

        assert!(
            consumer
                .stats_manager()
                .unwrap()
                .consume_status("orderly-stats-snapshot-group", "orders")
                .get_consume_ok_tps()
                > 0.0
        );
        assert_eq!(
            consumer
                .stats_manager()
                .unwrap()
                .consume_status("orderly-stats-live-group", "orders")
                .get_consume_ok_tps(),
            0.0
        );
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn running_info_returns_queue_snapshot_and_java_config_properties() {
        let mut config = PushConsumerConfig::new("running-info-snapshot").unwrap();
        config.set_pull_threshold_for_queue(777);
        config.set_pull_threshold_size_for_queue_mib(77);
        config.set_pull_threshold_for_topic(20);
        config.set_pull_threshold_size_for_topic_mib(12);
        config.client_config_mut().set_namesrv_addr(None);
        config
            .client_config_mut()
            .set_socks_proxy_config(Some(
                r#"{"10.1.0.0/16":{"addr":"proxy.internal:1080","username":"reader","password":"secret"}}"#.into(),
            ));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let raw_running_info_fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        consumer.subscribe("orders", "blue").await.unwrap();
        consumer.subscribe("empty", "*").await.unwrap();
        let before_start_running_info = consumer.consumer_running_info().await;
        assert!(
            raw_running_info_fixture["runningInfoBeforeStartThrowsNullPointerException"]
                .as_bool()
                .unwrap()
        );
        assert!(matches!(
            before_start_running_info,
            Err(PushConsumerError::NotStarted)
        ));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.update_offset(&queue, 17, false);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 19));
        process_queue
            .insert_messages(19, vec![message(11)])
            .unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue);
        consumer.update_pull_thresholds_after_rebalance();

        let pop_queue = MessageQueue::new("orders", "broker-a", 2);
        let pop_process_queue =
            Arc::new(PopProcessQueue::new_for_assignment(pop_queue.clone(), 23));
        pop_process_queue.inc_found_messages(3);
        pop_process_queue.set_last_pop_timestamp(1_700_000_000_005);
        write_lock(&consumer.pop_process_queues).insert(pop_queue.clone(), pop_process_queue);

        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store_dyn.clone(),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        consumer
            .consumer_start_timestamp
            .store(1_700_000_000_000, Ordering::Release);
        let stats = consumer.stats_manager().unwrap();
        stats.inc_pull_rt("running-info-snapshot", "orders", 500);
        stats.inc_pull_tps("running-info-snapshot", "orders", 2);
        stats.inc_consume_rt("running-info-snapshot", "orders", 8);
        stats.inc_consume_ok_tps("running-info-snapshot", "orders", 3);
        stats.inc_consume_failed_tps("running-info-snapshot", "orders", 1);
        tokio::time::sleep(Duration::from_secs(11)).await;

        consumer.consume_core_pool_size.store(7, Ordering::Release);
        let raw_running_info = consumer.consumer_running_info().await.unwrap();
        assert_eq!(
            raw_running_info
                .get_subscription_set()
                .unwrap()
                .with(Vec::len),
            raw_running_info_fixture["rawRunningInfoSubscriptionCount"]
                .as_u64()
                .unwrap() as usize
        );
        assert_eq!(
            raw_running_info.get_mq_table().unwrap().with(Vec::len),
            raw_running_info_fixture["rawRunningInfoQueueCount"]
                .as_u64()
                .unwrap() as usize
        );
        assert_eq!(
            raw_running_info.get_mq_pop_table().unwrap().with(Vec::len),
            raw_running_info_fixture["rawRunningInfoPopQueueCount"]
                .as_u64()
                .unwrap() as usize
        );
        assert_eq!(
            raw_running_info
                .get_status_table()
                .unwrap()
                .with(BTreeMap::len),
            raw_running_info_fixture["rawRunningInfoStatusCount"]
                .as_u64()
                .unwrap() as usize
        );
        let properties = raw_running_info.get_properties().unwrap().snapshot();
        assert_eq!(
            properties["PROP_THREADPOOL_CORE_SIZE"],
            raw_running_info_fixture["rawRunningInfoCorePoolSize"]
                .as_str()
                .unwrap()
        );
        assert_eq!(
            properties[ConsumerRunningInfoBody::PROP_CONSUME_ORDERLY],
            "false"
        );
        assert!(!properties.contains_key("PROP_CONSUME_ORDERLY"));

        let processor = crate::admin::ConsumerRunningInfoProcessor::for_push(
            Arc::downgrade(&consumer),
            Arc::downgrade(&store_dyn),
            Arc::downgrade(&runtime),
        );
        let response = processor
            .process(RemotingCommand {
                ext_fields: Some(
                    BTreeMap::from([
                        ("consumerGroup".into(), "running-info-snapshot".into()),
                        (
                            "clientId".into(),
                            "client-id-is-not-used-for-filtering".into(),
                        ),
                        ("jstackEnable".into(), "false".into()),
                    ])
                    .into(),
                ),
                ..Default::default()
            })
            .unwrap()
            .unwrap();
        assert_eq!(response.code, response::SUCCESS);
        let bytes = response.body.unwrap();
        let json = ConsumerRunningInfoBody::decode_java_json(std::str::from_utf8(&bytes).unwrap())
            .unwrap();
        assert!(json.get("jstack").is_none());
        let properties = json["properties"].as_object().unwrap();
        assert_eq!(properties["PROP_CONSUME_TYPE"], "CONSUME_PASSIVELY");
        assert_eq!(properties["PROP_CONSUMER_START_TIMESTAMP"], "1700000000000");
        assert_eq!(
            properties["pullThresholdForQueue"],
            raw_running_info_fixture["pushQueueThresholdSingleAssignment"][0]
                .as_i64()
                .unwrap()
                .to_string()
        );
        assert_eq!(
            properties["pullThresholdSizeForQueue"],
            raw_running_info_fixture["pushQueueThresholdSingleAssignment"][1]
                .as_i64()
                .unwrap()
                .to_string()
        );
        assert_eq!(
            properties["socksProxyConfig"],
            r#"{"10.1.0.0/16":{"addr":"proxy.internal:1080","username":"reader","password":"secret"}}"#
        );
        assert!(properties.get("tlsConfig").is_none());
        assert_eq!(json["subscriptionSet"].as_array().unwrap().len(), 2);
        assert_eq!(json["statusTable"]["orders"]["pullRT"], 500.0);
        assert!(json["statusTable"]["orders"]["pullTPS"].as_f64().unwrap() > 0.0);
        assert_eq!(json["statusTable"]["orders"]["consumeRT"], 8.0);
        assert!(
            json["statusTable"]["orders"]["consumeOKTPS"]
                .as_f64()
                .unwrap()
                > 0.0
        );
        assert!(
            json["statusTable"]["orders"]["consumeFailedTPS"]
                .as_f64()
                .unwrap()
                > 0.0
        );
        assert_eq!(json["statusTable"]["orders"]["consumeFailedMsgs"], 0);
        let encoded_queue = r#"{"brokerName":"broker-a","queueId":0,"topic":"orders"}"#;
        let encoded_pop_queue = r#"{"brokerName":"broker-a","queueId":2,"topic":"orders"}"#;
        assert_eq!(
            json["mqTable"][encoded_queue]["commitOffset"],
            serde_json::Value::from(17)
        );
        assert_eq!(json["mqTable"][encoded_queue]["cachedMsgMinOffset"], 11);
        assert_eq!(json["mqTable"][encoded_queue]["cachedMsgMaxOffset"], 11);
        assert_eq!(json["mqTable"][encoded_queue]["cachedMsgCount"], 1);
        assert_eq!(json["mqPopTable"][encoded_pop_queue]["waitAckCount"], 3);
        assert_eq!(
            json["mqPopTable"][encoded_pop_queue]["lastPopTimestamp"],
            1_700_000_000_005i64
        );
        assert!(
            std::str::from_utf8(&bytes)
                .unwrap()
                .contains("socksProxyConfig")
        );

        let response = processor
            .process(RemotingCommand {
                ext_fields: Some(
                    BTreeMap::from([
                        ("consumerGroup".into(), "running-info-snapshot".into()),
                        ("clientId".into(), "ignored-client-id".into()),
                        ("jstackEnable".into(), "true".into()),
                    ])
                    .into(),
                ),
                ..Default::default()
            })
            .unwrap()
            .unwrap();
        let body = ConsumerRunningInfoBody::decode_java_json(
            std::str::from_utf8(response.body.as_deref().unwrap()).unwrap(),
        )
        .unwrap();
        assert!(
            body["jstack"]
                .as_str()
                .is_some_and(|stack| !stack.is_empty())
        );
        assert!(
            body["jstack"]
                .as_str()
                .is_some_and(|stack| stack.contains("running_info_returns_queue_snapshot"))
        );

        consumer.set_consumer_group(None);
        let null_group_running_info =
            consumer.running_info_body(&runtime, None, Some(store_dyn.clone()));
        assert!(
            !null_group_running_info
                .get_properties()
                .unwrap()
                .snapshot()
                .contains_key("consumerGroup")
        );
        consumer.set_consumer_group(Some("running-info-snapshot".into()));

        consumer.shutdown().await.unwrap();
        let shutdown_running_info = consumer.consumer_running_info().await.unwrap();
        assert_eq!(
            shutdown_running_info.get_mq_table().unwrap().with(Vec::len),
            raw_running_info_fixture["rawRunningInfoAfterShutdownQueueCount"]
                .as_u64()
                .unwrap() as usize
        );
        assert_eq!(
            shutdown_running_info
                .get_mq_pop_table()
                .unwrap()
                .with(Vec::len),
            raw_running_info_fixture["rawRunningInfoAfterShutdownPopQueueCount"]
                .as_u64()
                .unwrap() as usize
        );
        assert_eq!(
            shutdown_running_info
                .get_status_table()
                .unwrap()
                .with(BTreeMap::len),
            raw_running_info_fixture["rawRunningInfoAfterShutdownStatusCount"]
                .as_u64()
                .unwrap() as usize
        );
        assert_eq!(
            shutdown_running_info.get_properties().unwrap().snapshot()["PROP_THREADPOOL_CORE_SIZE"],
            raw_running_info_fixture["rawRunningInfoAfterShutdownCorePoolSize"]
                .as_str()
                .unwrap()
        );
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn consumer_status_stays_responsive_during_a_slow_direct_consume_callback() {
        let mut config = PushConsumerConfig::new("status-during-direct-consume").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.update_offset(&queue, 42, false);
        let listener_started = Arc::new(tokio::sync::Barrier::new(2));
        let listener_release = Arc::new(tokio::sync::Barrier::new(2));
        let started = listener_started.clone();
        let release = listener_release.clone();
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let started = started.clone();
                let release = release.clone();
                async move {
                    started.wait().await;
                    release.wait().await;
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        );
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store_dyn.clone(),
            Arc::new(ConcurrentConsumeService::new(1, 1, listener)),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        let direct_consume = {
            let consumer = consumer.clone();
            tokio::spawn(async move {
                consumer
                    .consume_message_directly(message(99), Some("broker-a".into()))
                    .await
                    .unwrap()
            })
        };
        listener_started.wait().await;
        let status_processor =
            crate::admin::ConsumerStatusProcessor::new(Arc::downgrade(&store_dyn));
        let response = tokio::time::timeout(Duration::from_secs(1), async {
            status_processor.process(RemotingCommand {
                ext_fields: Some(
                    BTreeMap::from([
                        ("topic".into(), "orders".into()),
                        ("group".into(), "status-during-direct-consume".into()),
                    ])
                    .into(),
                ),
                ..Default::default()
            })
        })
        .await
        .expect("status snapshot must not wait for a consumer callback")
        .unwrap()
        .unwrap();
        assert_eq!(response.code, response::SUCCESS);
        let body = GetConsumerStatusBody::decode_java_json(
            std::str::from_utf8(response.body.as_deref().unwrap()).unwrap(),
        )
        .unwrap();
        body.get_message_queue_table()
            .unwrap()
            .with(|entries| assert_eq!(entries, &vec![(queue.clone(), Some(42))]));

        listener_release.wait().await;
        assert_eq!(
            direct_consume.await.unwrap().get_consume_result(),
            Some(ConsumeMessageResult::CrSuccess)
        );
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn queue_removal_fences_a_pull_response_already_in_flight() {
        let mut config = PushConsumerConfig::new("rebalance-pull-race").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let scheduler = Arc::new(PullScheduler::new());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store_dyn.clone(),
            idle_consume_service(),
            scheduler.clone(),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 73));
        store.update_offset(&queue, 50, false);
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let request = Arc::new(PullRequest::new(
            "rebalance-pull-race",
            process_queue.clone(),
            50,
        ));
        scheduler.schedule(request.clone(), 0).unwrap();
        let active_request = scheduler.take_due().unwrap();
        let response_ready = Arc::new(tokio::sync::Barrier::new(2));
        let response_release = Arc::new(tokio::sync::Barrier::new(2));
        let task_ready = response_ready.clone();
        let task_release = response_release.clone();
        let pull_consumer = consumer.clone();
        let pull_store = store_dyn.clone();
        let pull_scheduler = scheduler.clone();
        let pull_runtime = runtime.clone();
        let updates_before = store.updates.lock().unwrap().len();
        let pull_task = tokio::spawn(async move {
            task_ready.wait().await;
            task_release.wait().await;
            let context = PullContext {
                runtime: pull_runtime,
                offset_store: pull_store,
                scheduler: pull_scheduler,
                consume_service: ConsumeService::Concurrent(idle_consume_service()),
            };
            let mut result = pull_result(PullStatus::Found, 51);
            result.messages = Some(vec![message(50)]);
            pull_consumer
                .apply_pull_result(request, context, result)
                .await;
        });
        response_ready.wait().await;

        process_queue.drop_assignment(73).unwrap();
        scheduler.cancel(&queue, 73);
        let removal_context = PullContext {
            runtime,
            offset_store: store_dyn,
            scheduler: scheduler.clone(),
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
        };
        assert!(
            consumer
                .remove_process_queue_safely(&removal_context, &queue, &process_queue)
                .await
        );
        response_release.wait().await;
        pull_task.await.unwrap();
        scheduler.finish(&active_request);

        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
        assert_eq!(process_queue.snapshot(73).unwrap().cached_count, 0);
        assert_eq!(store.updates.lock().unwrap().len(), updates_before);
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&50));
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn failed_offset_persist_retains_the_dropped_generation_for_retry() {
        let mut config = PushConsumerConfig::new("rebalance-remove-retry").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store.clone(),
            idle_consume_service(),
            scheduler.clone(),
            pull_rpc_workers,
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 2);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 44));
        process_queue.drop_assignment(44).unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        store.update_offset(&queue, 81, false);
        let context = PullContext {
            runtime: runtime.clone(),
            offset_store: store.clone(),
            scheduler,
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
        };

        store.set_persist_failure(true);
        assert!(
            !consumer
                .remove_process_queue_safely(&context, &queue, &process_queue)
                .await
        );
        assert!(
            read_lock(&consumer.process_queues)
                .get(&queue)
                .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
        );
        assert!(process_queue.snapshot(44).unwrap().dropped);
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&81));

        store.set_persist_failure(false);
        assert!(
            consumer
                .remove_process_queue_safely(&context, &queue, &process_queue)
                .await
        );
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&81));
        assert!(!store.values.lock().unwrap().contains_key(&queue));
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn activation_fences_old_generations_and_rolls_back_scheduler_failure() {
        let mut config = PushConsumerConfig::new("rebalance-activation").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let scheduler = Arc::new(PullScheduler::new());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store.clone(),
            idle_consume_service(),
            scheduler.clone(),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let original = consumer.activate_process_queue(&queue, 10).await.unwrap();
        assert!(Arc::ptr_eq(
            &original,
            &consumer.activate_process_queue(&queue, 999).await.unwrap()
        ));
        original.drop_assignment(original.generation()).unwrap();
        assert!(matches!(
            consumer.activate_process_queue(&queue, 20).await,
            Err(PushConsumerError::ProcessQueueRemovalPending(_))
        ));
        assert!(
            read_lock(&consumer.process_queues)
                .get(&queue)
                .is_some_and(|current| Arc::ptr_eq(current, &original))
        );

        let context = PullContext {
            runtime: runtime.clone(),
            offset_store: store.clone(),
            scheduler: scheduler.clone(),
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
        };
        assert!(
            consumer
                .remove_process_queue_safely(&context, &queue, &original)
                .await
        );
        let replacement = consumer.activate_process_queue(&queue, 20).await.unwrap();
        assert!(replacement.generation() > original.generation());

        let failed_queue = MessageQueue::new("orders", "broker-a", 1);
        scheduler.stop().await;
        assert!(matches!(
            consumer.activate_process_queue(&failed_queue, 30).await,
            Err(PushConsumerError::PullScheduler(_))
        ));
        assert!(!read_lock(&consumer.process_queues).contains_key(&failed_queue));
        assert!(!store.values.lock().unwrap().contains_key(&failed_queue));
        consumer.shutdown().await.unwrap();
    }

    #[test]
    fn broker_flow_control_uses_the_java_retry_delay() {
        let consumer = PushConsumer::new(PushConsumerConfig::new("pull-delay-config").unwrap());
        let flow_control =
            consumer.pull_retry_error_from_rpc(rocketmq_classic_core::error::RpcError::Response {
                request_code: 11,
                peer: "broker-a".to_owned(),
                code: response::FLOW_CONTROL,
                remark: None,
            });
        assert_eq!(
            flow_control.delay_millis,
            PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL
        );

        let failure = consumer.pull_retry_error_from_rpc(
            rocketmq_classic_core::error::RpcError::Unsupported("test failure"),
        );
        assert_eq!(failure.delay_millis, 1_000);
    }

    #[tokio::test(start_paused = true)]
    async fn push_pull_outcomes_insert_found_messages_and_correct_only_drained_offsets() {
        let mut config = PushConsumerConfig::new("pull-outcomes").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let scheduler = Arc::new(PullScheduler::new());
        let consume_service = idle_consume_service();
        let context = || PullContext {
            runtime: runtime.clone(),
            offset_store: offset_store.clone(),
            scheduler: scheduler.clone(),
            consume_service: ConsumeService::Concurrent(consume_service.clone()),
        };

        let empty_queue = MessageQueue::new("orders", "broker-a", 0);
        let empty_process_queue =
            Arc::new(ProcessQueue::new_for_assignment(empty_queue.clone(), 1));
        let empty_request = Arc::new(PullRequest::new("pull-outcomes", empty_process_queue, 10));
        scheduler.schedule(empty_request.clone(), 0).unwrap();
        let active_empty_request = scheduler.take_due().unwrap();
        consumer
            .apply_pull_result(
                empty_request.clone(),
                context(),
                pull_result(PullStatus::NoNewMessage, 20),
            )
            .await;
        scheduler.finish(&active_empty_request);
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(empty_queue.clone(), 20, true)]
        );
        let follow_up = scheduler.take_due().unwrap();
        scheduler.finish(&follow_up);

        let unresolved_queue = MessageQueue::new("orders", "broker-a", 1);
        let unresolved_process_queue = Arc::new(ProcessQueue::new_for_assignment(
            unresolved_queue.clone(),
            2,
        ));
        unresolved_process_queue
            .insert_messages(2, vec![message(30)])
            .unwrap();
        let unresolved_request = Arc::new(PullRequest::new(
            "pull-outcomes",
            unresolved_process_queue.clone(),
            30,
        ));
        scheduler.schedule(unresolved_request.clone(), 0).unwrap();
        let active_unresolved_request = scheduler.take_due().unwrap();
        consumer
            .apply_pull_result(
                unresolved_request.clone(),
                context(),
                pull_result(PullStatus::NoMatchedMessage, 35),
            )
            .await;
        scheduler.finish(&active_unresolved_request);
        assert_eq!(offset_store.updates.lock().unwrap().len(), 1);
        assert_eq!(
            unresolved_process_queue.snapshot(2).unwrap().cached_count,
            1
        );
        let follow_up = scheduler.take_due().unwrap();
        scheduler.finish(&follow_up);

        let found_queue = MessageQueue::new("orders", "broker-a", 2);
        let found_process_queue = Arc::new(ProcessQueue::new_for_assignment(found_queue, 3));
        let found_request = Arc::new(PullRequest::new(
            "pull-outcomes",
            found_process_queue.clone(),
            40,
        ));
        scheduler.schedule(found_request.clone(), 0).unwrap();
        let active_found_request = scheduler.take_due().unwrap();
        let mut found = pull_result(PullStatus::Found, 42);
        found.messages = Some(vec![message(40)]);
        consumer.config().set_pull_interval_millis(25);
        consumer
            .apply_pull_result(found_request.clone(), context(), found)
            .await;
        scheduler.finish(&active_found_request);
        let snapshot = found_process_queue.snapshot(3).unwrap();
        assert_eq!(snapshot.cached_count, 1);
        assert_eq!(found_request.next_offset(), 42);
        assert!(scheduler.take_due().is_none());
        tokio::time::advance(Duration::from_millis(25)).await;
        assert_eq!(scheduler.take_due().unwrap().next_offset(), 42);
    }

    #[tokio::test]
    async fn push_pull_request_uses_live_batch_settings_from_consumer_config() {
        let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let nameserver_address = nameserver.local_addr().unwrap().to_string();
        let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let broker_address = broker_listener.local_addr().unwrap().to_string();
        let broker_route = pop_test_route(&broker_address);

        let nameserver_peer = tokio::spawn(async move {
            let (mut stream, _) = nameserver.accept().await.unwrap();
            let incoming = read_remoting_command(&mut stream).await;
            assert_eq!(incoming.code, request::GET_ROUTEINFO_BY_TOPIC);
            write_remoting_reply_with_body(
                &mut stream,
                &incoming,
                response::SUCCESS,
                None,
                Some(broker_route),
            )
            .await;
        });

        let broker_peer = tokio::spawn(async move {
            let mut stream = None;
            let incoming = accept_remoting_request(&broker_listener, &mut stream).await;
            assert_eq!(incoming.code, request::PULL_MESSAGE);
            let fields = incoming.ext_fields.as_ref().unwrap();
            assert_eq!(
                fields.get("consumerGroup"),
                Some(&"push-live-pull-batch".to_owned())
            );
            assert_eq!(fields.get("maxMsgNums"), Some(&"7".to_owned()));
            assert_eq!(fields.get("maxMsgBytes"), Some(&"12345".to_owned()));
            assert_eq!(fields.get("subscription"), Some(&"tagA".to_owned()));
            assert_eq!(fields.get("commitOffset"), Some(&"42".to_owned()));
            assert!(
                rocketmq_classic_common::sysflag::pull::has_commit_offset_flag(
                    fields.get("sysFlag").unwrap().parse().unwrap()
                )
            );
            write_remoting_reply(
                stream.as_mut().unwrap(),
                &incoming,
                response::PULL_NOT_FOUND,
                Some(HeaderMap::from([
                    ("nextBeginOffset".into(), "0".into()),
                    ("minOffset".into(), "0".into()),
                    ("maxOffset".into(), "0".into()),
                    ("suggestWhichBrokerId".into(), "0".into()),
                ])),
            )
            .await;
        });

        let mut config = PushConsumerConfig::new("push-live-pull-batch").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(nameserver_address));
        config
            .client_config_mut()
            .set_instance_name(Some("push-live-pull-batch".into()));
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_pull_batch_size(3);
        config.set_pull_batch_size_in_bytes(4_096);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        runtime.remoting().start().unwrap();
        runtime
            .update_topic_route("orders", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        nameserver_peer.await.unwrap();

        consumer.config().set_pull_batch_size(7);
        consumer.config().set_pull_batch_size_in_bytes(12_345);
        consumer
            .config()
            .set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
        consumer.config().set_post_subscription_when_pull(true);
        assert_eq!(consumer.config().pull_batch_size(), 7);
        assert_eq!(consumer.config().pull_batch_size_in_bytes(), 12_345);
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let offset_store = Arc::new(RecordingOffsetStore::default());
        offset_store.update_offset(&queue, 42, false);

        let request = PullRequest::new(
            "push-live-pull-batch",
            Arc::new(ProcessQueue::new_for_assignment(queue, 1)),
            0,
        );
        consumer.set_consumer_group(Some("push-live-pull-mutated-group".into()));
        let context = PullContext {
            runtime: runtime.clone(),
            offset_store,
            scheduler: Arc::new(PullScheduler::new()),
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
        };
        let subscription = SubscriptionData::new(Some("orders".into()), Some("tagA".into()));

        let pull_result = consumer
            .fetch_push_result(&context, &request, &subscription)
            .await;
        assert!(pull_result.is_ok());
        broker_peer.await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn activated_pull_request_keeps_the_start_group_after_facade_mutation() {
        let mut config = PushConsumerConfig::new("pull-request-group-snapshot").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        consumer.subscribe("orders", "*").await.unwrap();
        let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store,
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        consumer.set_consumer_group(Some("pull-request-live-group".into()));

        let queue = MessageQueue::new("orders", "broker-a", 0);
        consumer.activate_process_queue(&queue, 0).await.unwrap();
        let scheduler = match &*consumer.state.lock().await {
            State::Started { pull_scheduler, .. } => pull_scheduler.clone(),
            State::Created | State::Shutdown => unreachable!(),
        };
        let request = scheduler.take_due().unwrap();
        assert_eq!(request.consumer_group(), "pull-request-group-snapshot");

        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn first_orderly_locked_pull_refreshes_offset_once_like_java() {
        let mut config = PushConsumerConfig::new("orderly-first-lock-offset").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.seed_persisted(queue.clone(), 15);
        store.update_offset(&queue, 8, false);
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
                (Some(ConsumeOrderlyStatus::Success), context)
            },
        );
        let runtime =
            install_started_orderly_consumer(&consumer, &manager, store_dyn, listener, None).await;
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 101));
        process_queue
            .set_lock_state(101, true, epoch_millis())
            .unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        consumer
            .scripted_lock_transport
            .lock()
            .unwrap()
            .unlock_results
            .push_back(Ok(()));
        let request = Arc::new(PullRequest::new(
            "orderly-first-lock-offset",
            process_queue,
            8,
        ));
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["orderlyPullStatsGroup"],
            "phase18-orderly-pull-offset"
        );
        assert_eq!(
            fixture["orderlyPullFacadeGroupAfterMutation"],
            "phase18-orderly-pull-offset-mutated"
        );

        consumer
            .clone()
            .run_pull_worker_request(request.clone())
            .await;
        assert_eq!(
            request.next_offset(),
            fixture["orderlyFirstLockedPullOffset"].as_i64().unwrap()
        );
        assert!(request.previously_locked());
        assert_eq!(fixture["orderlyFirstLockedPullPreviouslyLocked"], true);
        assert_eq!(
            store.reads.lock().unwrap().as_slice(),
            &[(queue.clone(), ReadOffsetType::ReadFromStore)]
        );

        store.seed_persisted(queue.clone(), 21);
        consumer
            .clone()
            .run_pull_worker_request(request.clone())
            .await;
        assert_eq!(request.next_offset(), 15);
        assert_eq!(
            fixture["orderlySecondLockedPullOffset"].as_i64().unwrap(),
            request.next_offset()
        );
        assert_eq!(
            store.reads.lock().unwrap().as_slice(),
            &[(queue, ReadOffsetType::ReadFromStore)]
        );
        assert_eq!(fixture["orderlySecondLockedPullStoreReads"], 1);

        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn pull_result_stats_use_the_request_group_snapshot() {
        let config = PushConsumerConfig::new("pull-stats-live-group").unwrap();
        let manager = ClientRuntimeManager::new();
        let consumer = PushConsumer::with_manager(config, manager.clone());
        let runtime = manager
            .get_or_create(consumer.config.client_config())
            .await
            .unwrap();
        consumer.bind_stats_manager(runtime.consumer_stats_manager());
        let request_group = "pull-stats-request-group";
        let request = PullRequest::new(
            request_group,
            Arc::new(ProcessQueue::new_for_assignment(
                MessageQueue::new("orders", "broker-a", 0),
                1,
            )),
            0,
        );
        consumer.set_consumer_group(Some("pull-stats-mutated-group".into()));
        consumer.record_pull_result_stats(&request, "orders", Duration::from_millis(2), 1);
        std::thread::sleep(Duration::from_secs(11));

        assert!(
            consumer
                .stats_manager()
                .unwrap()
                .consume_status(request_group, "orders")
                .get_pull_tps()
                > 0.0
        );
        assert_eq!(
            consumer
                .stats_manager()
                .unwrap()
                .consume_status("pull-stats-mutated-group", "orders")
                .get_pull_tps(),
            0.0
        );
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn push_pull_rejects_stale_results_and_fences_offset_illegal_recovery() {
        let config = PushConsumerConfig::new("pull-generation-fence").unwrap();
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let scheduler = Arc::new(PullScheduler::new());
        let consume_service = idle_consume_service();
        let context = || PullContext {
            runtime: runtime.clone(),
            offset_store: offset_store.clone(),
            scheduler: scheduler.clone(),
            consume_service: ConsumeService::Concurrent(consume_service.clone()),
        };

        let stale_queue = MessageQueue::new("orders", "broker-a", 3);
        let stale_process_queue = Arc::new(ProcessQueue::new_for_assignment(stale_queue, 4));
        let stale_request = Arc::new(PullRequest::new(
            "pull-generation-fence",
            stale_process_queue.clone(),
            50,
        ));
        scheduler.schedule(stale_request.clone(), 0).unwrap();
        let active_stale_request = scheduler.take_due().unwrap();
        stale_process_queue.drop_assignment(4).unwrap();
        let mut stale_result = pull_result(PullStatus::Found, 51);
        stale_result.messages = Some(vec![message(50)]);
        consumer
            .apply_pull_result(stale_request, context(), stale_result)
            .await;
        scheduler.finish(&active_stale_request);
        assert_eq!(stale_process_queue.snapshot(4).unwrap().cached_count, 0);
        assert!(offset_store.updates.lock().unwrap().is_empty());

        let illegal_queue = MessageQueue::new("orders", "broker-a", 4);
        let illegal_process_queue =
            Arc::new(ProcessQueue::new_for_assignment(illegal_queue.clone(), 5));
        let illegal_request = Arc::new(PullRequest::new(
            "pull-generation-fence",
            illegal_process_queue.clone(),
            60,
        ));
        consumer
            .apply_pull_result(
                illegal_request,
                context(),
                pull_result(PullStatus::OffsetIllegal, 55),
            )
            .await;
        assert!(illegal_process_queue.snapshot(5).unwrap().dropped);
        assert!(read_lock(&consumer.offset_corrections_pending).contains(&illegal_queue));
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(illegal_queue, 55, false)]
        );

        let _ = runtime.shutdown().await;
    }

    #[tokio::test]
    async fn concurrent_ack_index_sends_only_the_unacknowledged_tail_back() {
        let config = PushConsumerConfig::new("consume-ack-index").unwrap();
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let scheduler = Arc::new(PullScheduler::new());
        let context = PullContext {
            runtime: runtime.clone(),
            offset_store: offset_store.clone(),
            scheduler,
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
        };
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let request = consume_request(
            queue.clone(),
            31,
            vec![retry_test_message(10), retry_test_message(11)],
        );
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .broker_results
            .push_back(Ok(()));
        let mut callback_context = ConsumeConcurrentlyContext::new(queue.clone());
        callback_context.set_ack_index(0);
        consumer
            .clone()
            .complete_consume_request(
                request,
                context,
                Some(ConsumeConcurrentlyStatus::ConsumeSuccess),
                callback_context,
            )
            .await;

        {
            let scripted = consumer.scripted_retry_transport.lock().unwrap();
            assert_eq!(scripted.broker_headers.len(), 1);
            assert_eq!(scripted.broker_headers[0].get_origin_topic(), "orders");
            assert_eq!(scripted.broker_headers[0].get_offset(), 111);
            assert_eq!(
                scripted.broker_headers[0].get_origin_msg_id(),
                Some("broker-msg-11")
            );
            assert_eq!(
                scripted.broker_headers[0].get_max_reconsume_times(),
                Some(16)
            );
            assert!(scripted.retry_messages.is_empty());
        }
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue, 12, true)]
        );
        let _ = runtime.shutdown().await;
    }

    #[tokio::test]
    async fn expired_concurrent_messages_match_java_process_queue_cleanup() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/expired-messages.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["provenance"]["sourceCommit"],
            "efa1be59d6fc747ad220f036c86030f05f0419f9"
        );
        let null_consumer_error = ProcessQueue::new()
            .clean_expired_msg_nullable(None)
            .await
            .expect_err("Java dereferences the consumer before scanning messages");
        assert!(matches!(
            &null_consumer_error,
            ExpiredMessageCleanupError::NullPushConsumer
        ));
        assert_eq!(
            null_consumer_error.to_string(),
            fixture["nullConsumerErrorClass"].as_str().unwrap()
        );

        let config = PushConsumerConfig::new("expired-message-cleanup").unwrap();
        config.set_consume_timeout_minutes(1);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            Arc::new(RecordingOffsetStore::default()),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        let make_message = |offset: i64, body_size: usize, started_at: i64| {
            let mut message = Message::new("orders", vec![b'x'; body_size]);
            message
                .set_system_property(PROPERTY_CONSUME_START_TIMESTAMP, started_at.to_string())
                .unwrap();
            let mut message = crate::test_support::message_ext_from(message);
            message.set_queue_offset(offset);
            message.set_broker_name(Some("broker-a".to_owned()));
            message.set_commit_log_offset(offset);
            message
        };
        let now_millis = 106_001;
        let direct_clean_now = epoch_millis();

        let expired_queue = MessageQueue::new("orders", "broker-a", 0);
        let expired_process_queue =
            Arc::new(ProcessQueue::new_for_assignment(expired_queue.clone(), 101));
        expired_process_queue
            .insert_messages(
                101,
                vec![
                    make_message(10, 2, direct_clean_now - 60_001),
                    // Keep this entry within the timeout window with margin;
                    // cleanup reads the wall clock again after its async send.
                    make_message(11, 3, direct_clean_now - 59_000),
                ],
            )
            .unwrap();
        write_lock(&consumer.process_queues).insert(expired_queue, expired_process_queue.clone());
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .broker_results
            .push_back(Ok(()));
        expired_process_queue
            .clean_expired_msg(&consumer)
            .await
            .unwrap();
        let expired_snapshot = expired_process_queue.snapshot(101).unwrap();
        assert_eq!(
            expired_snapshot.cached_count,
            fixture["expiredHead"]["cachedCount"]
        );
        assert_eq!(
            expired_snapshot.cached_bytes,
            fixture["expiredHead"]["cachedBytes"]
        );
        assert_eq!(expired_snapshot.minimum_offset, Some(11));
        assert_eq!(expired_snapshot.maximum_offset, Some(11));

        let failed_queue = MessageQueue::new("orders", "broker-a", 1);
        let failed_process_queue =
            Arc::new(ProcessQueue::new_for_assignment(failed_queue.clone(), 102));
        failed_process_queue
            .insert_messages(
                102,
                vec![make_message(20, 2, 46_000), make_message(21, 3, 46_001)],
            )
            .unwrap();
        write_lock(&consumer.process_queues)
            .insert(failed_queue.clone(), failed_process_queue.clone());
        {
            let mut scripted = consumer.scripted_retry_transport.lock().unwrap();
            scripted.broker_results.extend(
                std::iter::repeat_with(|| Err("scripted send-back failure".into())).take(2),
            );
            scripted
                .retry_results
                .extend(std::iter::repeat_with(|| Err("scripted retry failure".into())).take(2));
        }
        failed_process_queue
            .clean_expired_msg(&consumer)
            .await
            .unwrap();
        let failed_snapshot = failed_process_queue.snapshot(102).unwrap();
        assert_eq!(
            failed_snapshot.cached_count,
            fixture["sendBackFailure"]["cachedCount"]
        );
        assert_eq!(
            failed_snapshot.cached_bytes,
            fixture["sendBackFailure"]["cachedBytes"]
        );
        assert_eq!(failed_snapshot.minimum_offset, Some(20));
        assert_eq!(failed_snapshot.maximum_offset, Some(21));
        write_lock(&consumer.process_queues).remove(&failed_queue);

        let limit_queue = MessageQueue::new("orders", "broker-a", 2);
        let limit_process_queue =
            Arc::new(ProcessQueue::new_for_assignment(limit_queue.clone(), 103));
        let limit_messages = (0..18)
            .map(|index| make_message(60 + index, 2 + index as usize, 46_000))
            .collect();
        limit_process_queue
            .insert_messages(103, limit_messages)
            .unwrap();
        write_lock(&consumer.process_queues)
            .insert(limit_queue.clone(), limit_process_queue.clone());
        {
            let mut scripted = consumer.scripted_retry_transport.lock().unwrap();
            scripted.broker_results.extend(
                std::iter::repeat_with(|| Err("scripted send-back failure".into())).take(16),
            );
            scripted
                .retry_results
                .extend(std::iter::repeat_with(|| Err("scripted retry failure".into())).take(16));
        }
        consumer
            .clean_expired_process_queue_messages_at(now_millis)
            .await
            .unwrap();
        let limit_snapshot = limit_process_queue.snapshot(103).unwrap();
        assert_eq!(
            limit_snapshot.cached_count,
            fixture["attemptLimit"]["cachedCount"]
        );
        assert_eq!(
            limit_snapshot.cached_bytes,
            fixture["attemptLimit"]["cachedBytes"]
        );
        assert_eq!(limit_snapshot.minimum_offset, Some(60));
        assert_eq!(limit_snapshot.maximum_offset, Some(77));
        write_lock(&consumer.process_queues).remove(&limit_queue);

        let orderly_queue = MessageQueue::new("orders", "broker-a", 3);
        let orderly_process_queue =
            Arc::new(ProcessQueue::new_for_assignment(orderly_queue.clone(), 104));
        orderly_process_queue
            .insert_messages(104, vec![make_message(30, 2, 46_000)])
            .unwrap();
        write_lock(&consumer.process_queues).insert(orderly_queue, orderly_process_queue.clone());
        consumer.set_consume_orderly(true);
        consumer
            .clean_expired_process_queue_messages_at(now_millis)
            .await
            .unwrap();
        consumer.set_consume_orderly(false);
        let orderly_snapshot = orderly_process_queue.snapshot(104).unwrap();
        assert_eq!(
            orderly_snapshot.cached_count,
            fixture["orderlySkipped"]["cachedCount"]
        );
        assert_eq!(
            orderly_snapshot.cached_bytes,
            fixture["orderlySkipped"]["cachedBytes"]
        );
        assert_eq!(orderly_snapshot.minimum_offset, Some(30));

        {
            let scripted = consumer.scripted_retry_transport.lock().unwrap();
            let attempts = scripted
                .broker_headers
                .iter()
                .map(|header| format!("{}:{}", header.get_offset(), header.get_delay_level()))
                .collect::<Vec<_>>();
            assert_eq!(attempts.len(), 19);
            assert_eq!(attempts[0], "10:3");
            assert_eq!(attempts[1..3], ["20:3", "20:3"]);
            assert!(attempts[3..19].iter().all(|attempt| attempt == "60:3"));
            for (name, range) in [
                ("expiredHead", 0..1),
                ("sendBackFailure", 1..3),
                ("attemptLimit", 3..19),
                ("orderlySkipped", 19..19),
            ] {
                let fixture_attempts = fixture[name]["attempts"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|attempt| attempt.as_str().unwrap().to_owned())
                    .collect::<Vec<_>>();
                let rust_attempts = attempts[range].to_vec();
                assert_eq!(rust_attempts, fixture_attempts, "{name}");
            }
        }

        let malformed_queue = MessageQueue::new("orders", "broker-a", 4);
        let malformed_process_queue = Arc::new(ProcessQueue::new_for_assignment(
            malformed_queue.clone(),
            105,
        ));
        let mut malformed_message = make_message(40, 4, 0);
        malformed_message
            .set_system_property(PROPERTY_CONSUME_START_TIMESTAMP, "not-a-long".to_owned())
            .unwrap();
        malformed_process_queue
            .insert_messages(105, vec![malformed_message])
            .unwrap();
        let direct_error = malformed_process_queue
            .clean_expired_msg(&consumer)
            .await
            .unwrap_err();
        assert!(matches!(
            &direct_error,
            ExpiredMessageCleanupError::InvalidConsumeStartTimestamp { offset: 40, value }
                if value == "not-a-long"
        ));
        assert_eq!(
            fixture["malformedTimestamp"]["errorClass"],
            "java.lang.NumberFormatException"
        );
        let malformed_snapshot = malformed_process_queue.snapshot(105).unwrap();
        assert_eq!(
            malformed_snapshot.cached_count,
            fixture["malformedTimestamp"]["cachedCount"]
        );
        assert_eq!(
            malformed_snapshot.cached_bytes,
            fixture["malformedTimestamp"]["cachedBytes"]
        );
        assert_eq!(malformed_snapshot.minimum_offset, Some(40));
        assert_eq!(malformed_snapshot.maximum_offset, Some(40));

        write_lock(&consumer.process_queues).clear();
        write_lock(&consumer.process_queues)
            .insert(malformed_queue, malformed_process_queue.clone());
        let scheduled_error = consumer
            .clean_expired_process_queue_messages_at(now_millis)
            .await
            .unwrap_err();
        assert_eq!(scheduled_error, direct_error);
        assert!(
            fixture["malformedTimestamp"]["attempts"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            malformed_process_queue.snapshot(105).unwrap().cached_count,
            1
        );

        let empty_queue = MessageQueue::new("orders", "broker-a", 5);
        let empty_process_queue = Arc::new(ProcessQueue::new_for_assignment(empty_queue, 106));
        let mut empty_message = make_message(50, 4, 0);
        empty_message
            .set_system_property(PROPERTY_CONSUME_START_TIMESTAMP, String::new())
            .unwrap();
        empty_process_queue
            .insert_messages(106, vec![empty_message])
            .unwrap();
        empty_process_queue
            .clean_expired_msg(&consumer)
            .await
            .unwrap();
        let empty_snapshot = empty_process_queue.snapshot(106).unwrap();
        assert_eq!(
            empty_snapshot.cached_count,
            fixture["emptyTimestamp"]["cachedCount"]
        );
        assert_eq!(
            empty_snapshot.cached_bytes,
            fixture["emptyTimestamp"]["cachedBytes"]
        );
        assert_eq!(empty_snapshot.minimum_offset, Some(50));
        assert!(fixture["emptyTimestamp"]["errorClass"].is_null());
        assert!(
            fixture["emptyTimestamp"]["attempts"]
                .as_array()
                .unwrap()
                .is_empty()
        );

        let missing_queue = MessageQueue::new("orders", "broker-a", 6);
        let missing_process_queue = Arc::new(ProcessQueue::new_for_assignment(missing_queue, 107));
        let missing_message = Message::new("orders", vec![b'x'; 4]);
        let mut missing_message = crate::test_support::message_ext_from(missing_message);
        missing_message.set_queue_offset(51);
        missing_message.set_broker_name(Some("broker-a".to_owned()));
        missing_process_queue
            .insert_messages(107, vec![missing_message])
            .unwrap();
        missing_process_queue
            .clean_expired_msg(&consumer)
            .await
            .unwrap();
        let missing_snapshot = missing_process_queue.snapshot(107).unwrap();
        assert_eq!(
            missing_snapshot.cached_count,
            fixture["missingTimestamp"]["cachedCount"]
        );
        assert_eq!(
            missing_snapshot.cached_bytes,
            fixture["missingTimestamp"]["cachedBytes"]
        );
        assert_eq!(missing_snapshot.minimum_offset, Some(51));
        assert!(fixture["missingTimestamp"]["errorClass"].is_null());
        assert!(
            fixture["missingTimestamp"]["attempts"]
                .as_array()
                .unwrap()
                .is_empty()
        );

        let orderly_malformed_queue = MessageQueue::new("orders", "broker-a", 7);
        let orderly_malformed_process_queue = Arc::new(ProcessQueue::new_for_assignment(
            orderly_malformed_queue,
            108,
        ));
        let mut orderly_malformed_message = make_message(52, 4, 0);
        orderly_malformed_message
            .set_system_property(PROPERTY_CONSUME_START_TIMESTAMP, "not-a-long".to_owned())
            .unwrap();
        orderly_malformed_process_queue
            .insert_messages(108, vec![orderly_malformed_message])
            .unwrap();
        consumer.set_consume_orderly(true);
        orderly_malformed_process_queue
            .clean_expired_msg(&consumer)
            .await
            .unwrap();
        consumer.set_consume_orderly(false);
        let orderly_malformed_snapshot = orderly_malformed_process_queue.snapshot(108).unwrap();
        assert_eq!(
            orderly_malformed_snapshot.cached_count,
            fixture["orderlyMalformedTimestamp"]["cachedCount"]
        );
        assert_eq!(orderly_malformed_snapshot.minimum_offset, Some(52));
        assert!(fixture["orderlyMalformedTimestamp"]["errorClass"].is_null());
        assert!(
            fixture["orderlyMalformedTimestamp"]["attempts"]
                .as_array()
                .unwrap()
                .is_empty()
        );

        let unicode_queue = MessageQueue::new("orders", "broker-a", 8);
        let unicode_process_queue = Arc::new(ProcessQueue::new_for_assignment(unicode_queue, 109));
        let mut unicode_message = make_message(40, 4, 0);
        unicode_message
            .set_system_property(PROPERTY_CONSUME_START_TIMESTAMP, "٤٦٠٠٠".to_owned())
            .unwrap();
        unicode_process_queue
            .insert_messages(109, vec![unicode_message])
            .unwrap();
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .broker_results
            .push_back(Ok(()));
        unicode_process_queue
            .clean_expired_msg(&consumer)
            .await
            .unwrap();
        let unicode_snapshot = unicode_process_queue.snapshot(109).unwrap();
        assert_eq!(
            unicode_snapshot.cached_count,
            fixture["unicodeTimestamp"]["cachedCount"]
        );
        assert_eq!(
            unicode_snapshot.cached_bytes,
            fixture["unicodeTimestamp"]["cachedBytes"]
        );
        assert_eq!(unicode_snapshot.minimum_offset, None);
        assert_eq!(
            fixture["unicodeTimestamp"]["attempts"],
            serde_json::json!(["40:3"])
        );

        consumer.shutdown().await.unwrap();
        let _ = runtime.shutdown().await;
    }

    #[tokio::test]
    async fn concurrent_consume_start_timestamp_allows_expiry_while_listener_is_running() {
        let config = PushConsumerConfig::new("expired-message-in-flight").unwrap();
        config.set_consume_timeout_minutes(1);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store.clone(),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 104));
        process_queue
            .insert_messages(104, vec![retry_test_message(40)])
            .unwrap();
        let batch = process_queue.take_batch(104, 1).unwrap().unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let request = ConsumeRequest {
            process_queue: process_queue.clone(),
            batch,
        };

        let listener_started = Arc::new(tokio::sync::Notify::new());
        let continue_listener = Arc::new(tokio::sync::Notify::new());
        let observed_start = Arc::new(Mutex::new(None::<String>));
        let listener: Arc<dyn MessageListenerConcurrently> = {
            let listener_started = listener_started.clone();
            let continue_listener = continue_listener.clone();
            let observed_start = observed_start.clone();
            Arc::new(
                move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                    let listener_started = listener_started.clone();
                    let continue_listener = continue_listener.clone();
                    let observed_start = observed_start.clone();
                    async move {
                        *observed_start.lock().unwrap() = messages.first().and_then(|message| {
                            message.get_property(PROPERTY_CONSUME_START_TIMESTAMP)
                        });
                        listener_started.notify_one();
                        continue_listener.notified().await;
                        (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                    }
                },
            )
        };

        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .broker_results
            .push_back(Ok(()));
        let consume = {
            let consumer = consumer.clone();
            tokio::spawn(async move { consumer.run_consume_request(request, listener).await })
        };
        listener_started.notified().await;
        let timestamp = observed_start
            .lock()
            .unwrap()
            .as_deref()
            .unwrap()
            .parse::<i64>()
            .unwrap();
        assert_eq!(
            process_queue
                .expired_head_message(104, timestamp + 60_001, 60_000)
                .unwrap()
                .map(|(offset, _)| offset),
            Some(40)
        );
        consumer
            .clean_expired_process_queue_messages_at(timestamp + 60_001)
            .await
            .unwrap();
        assert_eq!(process_queue.snapshot(104).unwrap().cached_count, 0);

        continue_listener.notify_one();
        consume.await.unwrap();
        assert!(offset_store.updates.lock().unwrap().is_empty());
        assert_eq!(process_queue.snapshot(104).unwrap().cached_count, 0);

        consumer.shutdown().await.unwrap();
        let _ = runtime.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_consume_service_runs_expiry_sweep_after_java_initial_delay() {
        let config = PushConsumerConfig::new("expired-message-schedule").unwrap();
        config.set_consume_timeout_minutes(1);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            Arc::new(RecordingOffsetStore::default()),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 105));
        let mut expired = retry_test_message(50);
        expired
            .set_system_property(
                PROPERTY_CONSUME_START_TIMESTAMP,
                (epoch_millis() - 61_000).to_string(),
            )
            .unwrap();
        process_queue.insert_messages(105, vec![expired]).unwrap();
        write_lock(&consumer.process_queues).insert(queue, process_queue.clone());
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .broker_results
            .push_back(Ok(()));

        tokio::time::advance(Duration::from_secs(60)).await;
        for _ in 0..32 {
            if process_queue.snapshot(105).unwrap().cached_count == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(process_queue.snapshot(105).unwrap().cached_count, 0);
        assert_eq!(
            consumer
                .scripted_retry_transport
                .lock()
                .unwrap()
                .broker_headers
                .len(),
            1
        );

        consumer.shutdown().await.unwrap();
        let _ = runtime.shutdown().await;
    }

    #[tokio::test]
    async fn broker_send_back_failure_publishes_java_compatible_retry_metadata() {
        let mut config = PushConsumerConfig::new("consume-retry-fallback").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let context = PullContext {
            runtime: runtime.clone(),
            offset_store: offset_store.clone(),
            scheduler: Arc::new(PullScheduler::new()),
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
        };
        let queue = MessageQueue::new("orders", "broker-a", 1);
        let request = consume_request(queue.clone(), 32, vec![retry_test_message(20)]);
        {
            let mut scripted = consumer.scripted_retry_transport.lock().unwrap();
            scripted
                .broker_results
                .push_back(Err("broker refused send-back".into()));
            scripted.retry_attempt_scripts.push_back(vec![
                Err("inner producer first Broker failed".into()),
                Err("inner producer second Broker failed".into()),
                Ok(()),
            ]);
        }
        let callback_context = ConsumeConcurrentlyContext::new(queue.clone());
        consumer
            .clone()
            .complete_consume_request(
                request,
                context,
                Some(ConsumeConcurrentlyStatus::ReconsumeLater),
                callback_context,
            )
            .await;

        {
            let scripted = consumer.scripted_retry_transport.lock().unwrap();
            assert_eq!(scripted.retry_attempt_counts, [3]);
            let retry = &scripted.retry_messages[0];
            assert_eq!(retry.topic(), "%RETRY%tenant%consume-retry-fallback");
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_RETRY_TOPIC)
                    .map(String::as_str),
                Some("tenant%orders")
            );
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_ORIGIN_MESSAGE_ID)
                    .map(String::as_str),
                Some("origin-msg")
            );
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_RECONSUME_TIME)
                    .map(String::as_str),
                Some("3")
            );
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_MAX_RECONSUME_TIMES)
                    .map(String::as_str),
                Some("16")
            );
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_DELAY_TIME_LEVEL)
                    .map(String::as_str),
                Some("5")
            );
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                    .map(String::as_str),
                Some("source-uniq")
            );
            assert_eq!(
                retry.properties().get(PROPERTY_KEYS).map(String::as_str),
                Some("order-1")
            );
            assert!(
                !retry
                    .properties()
                    .contains_key(PROPERTY_TRANSACTION_PREPARED)
            );
        }
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue, 21, true)]
        );
        let _ = runtime.shutdown().await;
    }

    #[tokio::test]
    async fn send_back_uses_live_facade_group_and_namespaced_retry_topic() {
        let mut config = PushConsumerConfig::new("phase18-send-back-group-snapshot").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            Arc::new(RecordingOffsetStore::default()),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        consumer.set_consumer_group(Some("phase18-consume-retry-live".into()));

        let offset_store = Arc::new(RecordingOffsetStore::default());
        let context = PullContext {
            runtime: runtime.clone(),
            offset_store,
            scheduler: Arc::new(PullScheduler::new()),
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
        };
        let queue = MessageQueue::new("tenant%orders", "broker-a", 1);
        let mut retry_input = retry_test_message(90);
        retry_input.set_property_nullable(None, Some("value".to_owned()));
        let request = consume_request(queue.clone(), 195, vec![retry_input]);
        {
            let mut scripted = consumer.scripted_retry_transport.lock().unwrap();
            scripted
                .broker_results
                .push_back(Err("broker refused send-back".into()));
            scripted.retry_results.push_back(Ok(()));
        }
        consumer
            .clone()
            .complete_consume_request(
                request,
                context,
                Some(ConsumeConcurrentlyStatus::ReconsumeLater),
                ConsumeConcurrentlyContext::new(queue),
            )
            .await;

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        let message_fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/messages/records.json"))
                .unwrap();
        {
            let scripted = consumer.scripted_retry_transport.lock().unwrap();
            assert_eq!(scripted.broker_headers.len(), 1);
            assert_eq!(
                Some(scripted.broker_headers[0].get_group()),
                fixture["brokerSendBackConsumerGroups"][0].as_str()
            );
            assert_eq!(scripted.retry_messages.len(), 1);
            assert_eq!(
                scripted.retry_messages[0].topic(),
                fixture["sendBackFallbackRetryTopics"][0].as_str().unwrap()
            );
            assert_eq!(
                scripted.retry_messages[0]
                    .get_nullable_property(None)
                    .as_deref(),
                Some("value")
            );
            assert_eq!(scripted.retry_wire_properties.len(), 1);
            assert!(
                scripted.retry_wire_properties[0].contains(
                    message_fixture["messagePublicApi"]["nullKeyPropertyEncoded"]
                        .as_str()
                        .unwrap()
                )
            );
        }

        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn public_send_back_overloads_use_message_and_explicit_broker_names() {
        let config = PushConsumerConfig::new("public-send-back").unwrap();
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store,
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        {
            let mut scripted = consumer.scripted_retry_transport.lock().unwrap();
            scripted
                .broker_results
                .extend([Ok(()), Ok(()), Ok(()), Ok(())]);
        }

        let message = retry_test_message(7);
        consumer.send_message_back(&message, 2).await.unwrap();
        consumer
            .send_message_back_to_broker(&message, 3, Some("broker-override"))
            .await
            .unwrap();
        let mut store_host_message = retry_test_message(8);
        store_host_message.set_broker_name(None);
        store_host_message.set_store_host(Some(MessageHostAddress::new("127.0.0.1", 10911)));
        consumer
            .send_message_back_to_broker(&store_host_message, 4, None)
            .await
            .unwrap();
        let mut custom_store_host_message = retry_test_message(9);
        let custom_store_host_subtype = Arc::new(FixedStoreHostSubtype {
            store_host: MessageHostAddress::new("127.0.0.1", 10912),
            setter_calls: AtomicUsize::new(0),
        });
        custom_store_host_message.set_runtime_subtype(custom_store_host_subtype.clone());
        custom_store_host_message.set_broker_name(None);
        custom_store_host_message.set_store_host(Some(MessageHostAddress::new("127.0.0.1", 10911)));
        consumer
            .send_message_back_to_broker(&custom_store_host_message, 5, None)
            .await
            .unwrap();

        {
            let scripted = consumer.scripted_retry_transport.lock().unwrap();
            assert_eq!(scripted.broker_headers.len(), 4);
            assert_eq!(
                scripted.broker_headers[0].get_broker_name(),
                Some("broker-a")
            );
            assert_eq!(scripted.broker_headers[0].get_delay_level(), 2);
            assert_eq!(
                scripted.broker_headers[1].get_broker_name(),
                Some("broker-override")
            );
            assert_eq!(scripted.broker_headers[1].get_delay_level(), 3);
            assert_eq!(scripted.broker_headers[2].get_broker_name(), None);
            assert_eq!(scripted.broker_headers[2].get_delay_level(), 4);
            assert_eq!(
                scripted.broker_targets[2],
                (None, Some("127.0.0.1:10911".to_owned()))
            );
            assert_eq!(scripted.broker_headers[3].get_broker_name(), None);
            assert_eq!(scripted.broker_headers[3].get_delay_level(), 5);
            assert_eq!(
                scripted.broker_targets[3],
                (None, Some("127.0.0.1:10912".to_owned()))
            );
            assert!(scripted.retry_messages.is_empty());
        }
        assert_eq!(
            custom_store_host_subtype
                .setter_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        consumer.shutdown().await.unwrap();
        let _ = runtime.shutdown().await;
    }

    #[tokio::test]
    async fn failed_send_back_retains_and_increments_messages_for_redelivery() {
        let config = PushConsumerConfig::new("consume-retry-retention").unwrap();
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let context = PullContext {
            runtime: runtime.clone(),
            offset_store: offset_store.clone(),
            scheduler: Arc::new(PullScheduler::new()),
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
        };
        let queue = MessageQueue::new("orders", "broker-a", 2);
        let request = consume_request(queue.clone(), 33, vec![retry_test_message(30)]);
        let process_queue = request.process_queue.clone();
        {
            let mut scripted = consumer.scripted_retry_transport.lock().unwrap();
            scripted
                .broker_results
                .push_back(Err("broker refused send-back".into()));
            scripted
                .retry_results
                .push_back(Err("retry-topic publish failed".into()));
        }
        consumer
            .clone()
            .complete_consume_request(
                request,
                context,
                Some(ConsumeConcurrentlyStatus::ReconsumeLater),
                ConsumeConcurrentlyContext::new(queue.clone()),
            )
            .await;

        let snapshot = process_queue.snapshot(33).unwrap();
        assert_eq!(snapshot.cached_count, 1);
        assert_eq!(snapshot.in_flight_count, 0);
        let redelivery = process_queue.take_batch(33, 1).unwrap().unwrap();
        assert_eq!(redelivery.messages()[0].reconsume_times(), 3);
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue, 30, true)]
        );
        let _ = runtime.shutdown().await;
    }

    #[tokio::test]
    async fn concurrent_listener_success_commits_the_complete_fixture_batch() {
        let mut config = PushConsumerConfig::new("consume-success").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let consume_service = idle_consume_service();
        let scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            consume_service,
            scheduler,
            pull_rpc_workers,
        )
        .await;

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let request = consume_request(
            queue.clone(),
            50,
            vec![retry_test_message(60), retry_test_message(61)],
        );
        let process_queue = request.process_queue.clone();
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
                assert_eq!(messages.len(), 2);
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            },
        );
        consumer
            .clone()
            .run_consume_request(request, listener)
            .await;

        let snapshot = process_queue.snapshot(50).unwrap();
        assert_eq!(snapshot.cached_count, 0);
        assert_eq!(snapshot.in_flight_count, 0);
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue, 62, true)]
        );
        let _ = runtime.shutdown().await;
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn consume_message_hooks_wrap_the_listener_and_share_context_state() {
        struct Hook(Arc<std::sync::Mutex<Vec<String>>>);

        impl ConsumeMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "consume-hook-test"
            }

            fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
                assert!(!context.success);
                assert!(context.status.is_none());
                assert!(context.access_channel.is_none());
                assert_eq!(
                    context.consumer_group.as_deref(),
                    Some("tenant%consume-hook-lifecycle")
                );
                assert_eq!(context.namespace.as_deref(), Some("tenant"));
                assert_eq!(context.properties, Some(HashMap::new()));
                context
                    .properties
                    .as_mut()
                    .unwrap()
                    .insert("hook-state".into(), "retained".into());
                self.0.lock().unwrap().push("before".into());
            }

            fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
                assert!(context.success);
                assert_eq!(context.status.as_deref(), Some("CONSUME_SUCCESS"));
                assert_eq!(
                    context.access_channel,
                    Some(rocketmq_classic_core::config::AccessChannel::Local)
                );
                assert_eq!(
                    context
                        .properties
                        .as_ref()
                        .and_then(|properties| properties.get("hook-state"))
                        .map(String::as_str),
                    Some("retained")
                );
                self.0.lock().unwrap().push("after".into());
            }
        }

        let mut config = PushConsumerConfig::new("consume-hook-lifecycle").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".into()));
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        consumer.register_consume_message_hook(Arc::new(Hook(events.clone())));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let request = consume_request(queue, 60, vec![retry_test_message(70)]);
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
                assert_eq!(messages.len(), 1);
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            },
        );
        consumer
            .clone()
            .run_consume_request(request, listener)
            .await;

        assert_eq!(events.lock().unwrap().as_slice(), ["before", "after"]);
        let _ = runtime.shutdown().await;
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_consume_timeout_classifies_after_listener_returns_without_cancelling_it() {
        struct Hook(Arc<std::sync::Mutex<Vec<String>>>);

        impl ConsumeMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "consume-timeout-hook"
            }

            fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {
                self.0.lock().unwrap().push("before".into());
            }

            fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
                let return_type = context
                    .properties
                    .as_ref()
                    .and_then(|properties| properties.get("CONSUME_CONTEXT_TYPE"))
                    .map(String::as_str)
                    .unwrap_or("missing");
                self.0.lock().unwrap().push(format!(
                    "after:{}:{}:{return_type}",
                    context.status.as_deref().unwrap_or("missing"),
                    context.success
                ));
            }
        }

        struct CallbackDrop(Arc<AtomicBool>);

        impl Drop for CallbackDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let listener: Arc<dyn MessageListenerConcurrently> = {
            let started = started.clone();
            let release = release.clone();
            let dropped = dropped.clone();
            Arc::new(
                move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                    let started = started.clone();
                    let release = release.clone();
                    let dropped = dropped.clone();
                    async move {
                        let _drop_signal = CallbackDrop(dropped);
                        started.notify_one();
                        release.notified().await;
                        (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                    }
                },
            )
        };

        let mut config = PushConsumerConfig::new("consume-timeout-hook").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_consume_timeout_minutes(2);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        consumer.register_consume_message_hook(Arc::new(Hook(events.clone())));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        let request = consume_request(
            MessageQueue::new("orders", "broker-a", 0),
            61,
            vec![retry_test_message(71)],
        );
        let task = tokio::spawn({
            let consumer = consumer.clone();
            async move { consumer.run_consume_request(request, listener).await }
        });
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();

        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        consumer.config().set_consume_timeout_minutes(1);
        assert!(
            !dropped.load(Ordering::Acquire),
            "consume timeout must classify a completed callback, not cancel it"
        );
        assert_eq!(events.lock().unwrap().as_slice(), ["before"]);

        release.notify_one();
        task.await.unwrap();
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["before", "after:CONSUME_SUCCESS:true:TIME_OUT"]
        );
        let _ = runtime.shutdown().await;
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_listener_without_status_sends_fixture_back_for_retry() {
        let mut config = PushConsumerConfig::new("consume-null-status").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        consumer.config().set_max_reconsume_times(2);
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let consume_service = idle_consume_service();
        let scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            consume_service,
            scheduler,
            pull_rpc_workers,
        )
        .await;
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .broker_results
            .push_back(Ok(()));

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let request = consume_request(queue.clone(), 51, vec![retry_test_message(70)]);
        let process_queue = request.process_queue.clone();
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
                (None, context)
            },
        );
        consumer
            .clone()
            .run_consume_request(request, listener)
            .await;

        assert_eq!(process_queue.snapshot(51).unwrap().cached_count, 0);
        {
            let scripted = consumer.scripted_retry_transport.lock().unwrap();
            assert_eq!(scripted.broker_headers.len(), 1);
            assert_eq!(scripted.broker_headers[0].get_offset(), 170);
            assert_eq!(
                scripted.broker_headers[0].get_max_reconsume_times(),
                Some(2)
            );
        }
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue, 71, true)]
        );
        let _ = runtime.shutdown().await;
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_listener_panic_is_isolated_and_retried() {
        let mut config = PushConsumerConfig::new("consume-listener-panic").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let consume_service = idle_consume_service();
        let scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            consume_service,
            scheduler,
            pull_rpc_workers,
        )
        .await;
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .broker_results
            .push_back(Ok(()));

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let request = consume_request(queue.clone(), 52, vec![retry_test_message(80)]);
        let process_queue = request.process_queue.clone();
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            |_messages: Vec<MessageExt>, _context: ConsumeConcurrentlyContext| async move {
                panic!("scripted concurrent-listener panic")
            },
        );
        consumer
            .clone()
            .run_consume_request(request, listener)
            .await;

        assert_eq!(process_queue.snapshot(52).unwrap().cached_count, 0);
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue, 81, true)]
        );
        let _ = runtime.shutdown().await;
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn private_fixed_assignment_retries_java_fixture_with_broker_limit() {
        let mut config = PushConsumerConfig::new("consume-fixed-assignment").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        consumer.subscribe("orders", "*").await.unwrap();
        consumer.suspend().await;
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let consume_service = idle_consume_service();
        let scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            consume_service,
            scheduler,
            pull_rpc_workers,
        )
        .await;
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .broker_results
            .push_back(Ok(()));

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = consumer.activate_process_queue(&queue, 90).await.unwrap();
        let generation = process_queue.generation();
        process_queue
            .insert_messages(generation, vec![retry_test_message(90)])
            .unwrap();
        let batch = process_queue
            .take_batch(generation, usize::MAX)
            .unwrap()
            .unwrap();
        consumer
            .clone()
            .complete_consume_request(
                ConsumeRequest {
                    process_queue: process_queue.clone(),
                    batch,
                },
                consumer.pull_context().await.unwrap(),
                Some(ConsumeConcurrentlyStatus::ReconsumeLater),
                ConsumeConcurrentlyContext::new(queue.clone()),
            )
            .await;

        assert_eq!(process_queue.snapshot(generation).unwrap().cached_count, 0);
        {
            let scripted = consumer.scripted_retry_transport.lock().unwrap();
            assert_eq!(scripted.broker_headers.len(), 1);
            assert_eq!(scripted.broker_headers[0].get_offset(), 190);
            assert_eq!(
                scripted.broker_headers[0].get_origin_msg_id(),
                Some("broker-msg-90")
            );
            assert_eq!(
                scripted.broker_headers[0].get_max_reconsume_times(),
                Some(16)
            );
        }
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue.clone(), 90, false), (queue, 91, true)]
        );
        let _ = runtime.shutdown().await;
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn broadcasting_drops_failed_messages_without_clustering_retry() {
        let config = PushConsumerConfig::new("consume-broadcast-failure").unwrap();
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let context = PullContext {
            runtime: runtime.clone(),
            offset_store: offset_store.clone(),
            scheduler: Arc::new(PullScheduler::new()),
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
        };
        let queue = MessageQueue::new("orders", "broker-a", 3);
        let request = consume_request(queue.clone(), 34, vec![retry_test_message(40)]);
        let process_queue = request.process_queue.clone();
        consumer
            .clone()
            .complete_consume_request(
                request,
                context,
                Some(ConsumeConcurrentlyStatus::ReconsumeLater),
                ConsumeConcurrentlyContext::new(queue.clone()),
            )
            .await;

        assert_eq!(process_queue.snapshot(34).unwrap().cached_count, 0);
        {
            let scripted = consumer.scripted_retry_transport.lock().unwrap();
            assert!(scripted.broker_headers.is_empty());
            assert!(scripted.retry_messages.is_empty());
        }
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue, 41, true)]
        );
        let _ = runtime.shutdown().await;
    }

    #[tokio::test]
    async fn startup_rejects_missing_listener_and_starts_orderly_listener() {
        let consumer = Arc::new(PushConsumer::with_manager(
            PushConsumerConfig::new("missing-listener").unwrap(),
            ClientRuntimeManager::new(),
        ));
        assert!(matches!(
            consumer.start().await,
            Err(PushConsumerError::MissingMessageListener)
        ));
        assert!(!consumer.is_running().await);

        let mut config = PushConsumerConfig::new("orderly-listener").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let consumer = Arc::new(PushConsumer::with_manager(
            config,
            ClientRuntimeManager::new(),
        ));
        consumer
            .register_message_listener(PushMessageListener::Orderly(Arc::new(EmptyOrderlyListener)))
            .await
            .unwrap();
        consumer.start().await.unwrap();
        assert!(consumer.is_running().await);
        assert!(consumer.is_consume_orderly());
        consumer.shutdown().await.unwrap();
    }

    struct NamedAllocationStrategy(&'static str);

    impl crate::rebalance::AllocateMessageQueueStrategy for NamedAllocationStrategy {
        fn allocate(
            &self,
            _consumer_group: &str,
            _current_client_id: &str,
            queues: &[MessageQueue],
            _consumer_ids: &[String],
        ) -> Result<Vec<MessageQueue>, crate::rebalance::RebalanceError> {
            Ok(queues.to_vec())
        }

        fn name(&self) -> &str {
            self.0
        }
    }

    #[tokio::test]
    async fn startup_snapshots_the_current_rebalance_settings() {
        let mut config = PushConsumerConfig::new("rebalance-strategy-snapshot").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let consumer = Arc::new(PushConsumer::with_manager(
            config,
            ClientRuntimeManager::new(),
        ));
        consumer
            .config()
            .set_allocate_message_queue_strategy(Arc::new(NamedAllocationStrategy(
                "configured-before-start",
            )));
        consumer
            .config()
            .set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        consumer
            .register_message_listener(PushMessageListener::concurrently(
                |_messages: Vec<MessageExt>, context| async move {
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                },
            ))
            .await
            .unwrap();

        consumer.start().await.unwrap();
        assert_eq!(
            consumer.rebalance_service.strategy_name(),
            "configured-before-start"
        );
        assert_eq!(
            consumer.rebalance_service.message_model_for_test(),
            rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting
        );

        consumer
            .config()
            .set_allocate_message_queue_strategy(Arc::new(NamedAllocationStrategy(
                "configured-after-start",
            )));
        consumer
            .config()
            .set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
        assert_eq!(
            consumer.rebalance_service.strategy_name(),
            "configured-before-start",
            "Java installs the configured strategy into RebalanceImpl during start"
        );
        assert_eq!(
            consumer.rebalance_service.message_model_for_test(),
            rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting,
            "Java keeps RebalanceImpl's message-model snapshot after start"
        );
        assert_eq!(
            consumer.message_model(),
            rocketmq_classic_protocol::heartbeat::MessageModel::Clustering,
            "the facade getter still reflects the live consumer setting"
        );
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn startup_registers_push_consumer_and_shutdown_releases_it() {
        let mut config = PushConsumerConfig::new("push-lifecycle").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        consumer
            .register_message_listener(PushMessageListener::concurrently(
                |_messages: Vec<rocketmq_classic_common::message::MessageExt>, context| async move {
                    (
                        Some(super::super::listener::ConsumeConcurrentlyStatus::ConsumeSuccess),
                        context,
                    )
                },
            ))
            .await
            .unwrap();
        consumer.subscribe("orders", "*").await.unwrap();
        consumer.subscribe("temporary", "*").await.unwrap();
        assert!(!consumer.unsubscribe("temporary").await.unwrap());
        assert!(consumer.unsubscribe("tenant%temporary").await.unwrap());
        assert!(!consumer.unsubscribe("tenant%temporary").await.unwrap());
        let subscription_table = consumer.get_subscription_inner();
        {
            let subscriptions = read_lock(&subscription_table);
            assert_eq!(
                subscriptions
                    .get("tenant%orders")
                    .and_then(|subscription| subscription.sub_string.as_deref()),
                Some("*")
            );
        }

        consumer.start().await.unwrap();
        assert!(consumer.is_running().await);
        let (consume_service, pull_scheduler, pull_rpc_workers) =
            match &*consumer.state.lock().await {
                State::Started {
                    consume_service,
                    pull_scheduler,
                    pull_rpc_workers,
                    ..
                } => (
                    consume_service.clone(),
                    pull_scheduler.clone(),
                    pull_rpc_workers.clone(),
                ),
                _ => panic!("consumer should own a started consume service"),
            };
        assert_eq!(consume_service.worker_count(), 20);
        assert_eq!(pull_scheduler.worker_count(), 1);
        assert_eq!(pull_rpc_workers.worker_count(), 4);
        assert!(consumer.update_core_pool_size(4).await.is_ok());
        assert_eq!(consume_service.worker_count(), 4);
        assert!(consumer.update_core_pool_size(20).await.is_ok());
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let registration = runtime
            .select_consumer("tenant%push-lifecycle")
            .unwrap()
            .expect("started consumer should be registered");
        let heartbeat_topics = registration
            .heartbeat_data()
            .subscription_data_set
            .expect("push consumers publish subscriptions")
            .snapshot()
            .into_iter()
            .flatten()
            .filter_map(|subscription| subscription.snapshot().topic)
            .collect::<std::collections::BTreeSet<_>>();
        assert!(heartbeat_topics.contains("tenant%orders"));
        assert!(heartbeat_topics.contains("%RETRY%tenant%push-lifecycle"));
        consumer.subscribe("late", "*").await.unwrap();
        assert!(
            runtime
                .select_consumer("tenant%push-lifecycle")
                .unwrap()
                .unwrap()
                .heartbeat_data()
                .subscription_data_set
                .unwrap()
                .snapshot()
                .iter()
                .any(|subscription| {
                    subscription.as_ref().is_some_and(|subscription| {
                        subscription.snapshot().topic.as_deref() == Some("tenant%late")
                    })
                })
        );
        let running_offset_store = consumer.get_offset_store();
        assert!(running_offset_store.is_some());
        consumer.set_offset_store(None).await.unwrap();
        assert!(consumer.get_offset_store().is_none());
        consumer
            .set_offset_store(running_offset_store)
            .await
            .unwrap();
        assert!(consumer.get_offset_store().is_some());
        assert!(matches!(
            consumer.start().await,
            Err(PushConsumerError::AlreadyStarted)
        ));
        assert!(consumer.unsubscribe("tenant%orders").await.unwrap());

        consumer.shutdown().await.unwrap();
        assert!(!consumer.is_running().await);
        assert_eq!(consume_service.worker_count(), 0);
        assert_eq!(pull_scheduler.worker_count(), 0);
        assert_eq!(pull_rpc_workers.worker_count(), 0);
        assert!(matches!(
            consumer.start().await,
            Err(PushConsumerError::Shutdown)
        ));
    }

    #[tokio::test]
    async fn update_core_pool_size_matches_java_void_and_lifecycle_semantics() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        assert_eq!(fixture["updateCorePoolSizeReturnType"], "void");
        assert_eq!(
            fixture["updateCorePoolSizeNullServiceThrowsNullPointerException"],
            true
        );
        assert_eq!(
            fixture["concurrentRaisedMaxUpdateThrowsIllegalArgumentException"],
            true
        );
        assert_eq!(
            fixture["orderlyRaisedMaxUpdateThrowsIllegalArgumentException"],
            true
        );
        assert_eq!(
            fixture["concurrentShutdownRaisedMaxUpdateThrowsIllegalArgumentException"],
            true
        );
        assert_eq!(
            fixture["orderlyShutdownRaisedMaxUpdateThrowsIllegalArgumentException"],
            true
        );

        fn config(
            group: &str,
            model: rocketmq_classic_protocol::heartbeat::MessageModel,
        ) -> PushConsumerConfig {
            let mut config = PushConsumerConfig::new(group).unwrap();
            config.client_config_mut().set_namesrv_addr(None);
            config.set_consume_thread_min(2);
            config.set_consume_thread_max(8);
            config.set_message_model(model);
            config
        }

        let created = PushConsumer::new(config(
            "push-core-pool-created",
            rocketmq_classic_protocol::heartbeat::MessageModel::Clustering,
        ));
        assert!(matches!(
            created.update_core_pool_size(4).await,
            Err(PushConsumerError::NotStarted)
        ));
        assert!(matches!(
            created.update_core_pool_size(0).await,
            Err(PushConsumerError::NotStarted)
        ));

        let concurrent_manager = ClientRuntimeManager::new();
        let concurrent = Arc::new(PushConsumer::with_manager(
            config(
                "push-core-pool-concurrent",
                rocketmq_classic_protocol::heartbeat::MessageModel::Clustering,
            ),
            concurrent_manager.clone(),
        ));
        let concurrent_listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            },
        );
        let concurrent_service = Arc::new(ConcurrentConsumeService::new(2, 8, concurrent_listener));
        let concurrent_offset_store: Arc<dyn OffsetStore> =
            Arc::new(RecordingOffsetStore::default());
        let concurrent_runtime = install_started_consumer_with_service(
            &concurrent,
            &concurrent_manager,
            concurrent_offset_store,
            ConsumeService::Concurrent(concurrent_service.clone()),
            None,
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        assert_eq!(
            concurrent_service.worker_count(),
            fixture["concurrentCorePoolSizeBefore"]
        );
        assert!(concurrent.update_core_pool_size(6).await.is_ok());
        assert_eq!(
            concurrent_service.worker_count(),
            fixture["concurrentCorePoolSizeAfterValid"]
        );
        assert!(concurrent.update_core_pool_size(8).await.is_ok());
        assert!(concurrent.update_core_pool_size(0).await.is_ok());
        assert_eq!(
            concurrent_service.worker_count(),
            fixture["concurrentCorePoolSizeAfterInvalid"]
        );
        concurrent.config().set_consume_thread_max(10);
        assert!(matches!(
            concurrent.update_core_pool_size(9).await,
            Err(PushConsumerError::InvalidSetting {
                name: "corePoolSize",
                value: 9,
                minimum: 1,
                maximum: 8,
            })
        ));
        assert_eq!(
            concurrent_service.worker_count(),
            fixture["concurrentCorePoolSizeAfterRaisedMaxFailure"]
        );
        assert!(concurrent.update_core_pool_size(8).await.is_ok());
        assert_eq!(
            concurrent_service.worker_count(),
            fixture["concurrentCorePoolSizeAtExecutorMaximum"]
        );
        concurrent.shutdown().await.unwrap();
        assert_eq!(
            concurrent_service.worker_count(),
            0,
            "shutdown retires all Rust workers"
        );
        assert!(matches!(
            concurrent.update_core_pool_size(9).await,
            Err(PushConsumerError::InvalidSetting {
                name: "corePoolSize",
                value: 9,
                minimum: 1,
                maximum: 8,
            })
        ));
        assert!(concurrent.update_core_pool_size(7).await.is_ok());
        assert_eq!(
            concurrent.consume_core_pool_size.load(Ordering::Acquire),
            fixture["concurrentCorePoolSizeAfterShutdownUpdate"]
        );
        drop(concurrent_runtime);

        let orderly_manager = ClientRuntimeManager::new();
        let orderly = Arc::new(PushConsumer::with_manager(
            config(
                "push-core-pool-orderly",
                rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting,
            ),
            orderly_manager.clone(),
        ));
        let orderly_listener: Arc<dyn MessageListenerOrderly> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
                (Some(ConsumeOrderlyStatus::Success), context)
            },
        );
        let orderly_offset_store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
        let orderly_runtime = install_started_orderly_consumer(
            &orderly,
            &orderly_manager,
            orderly_offset_store,
            orderly_listener,
            None,
        )
        .await;
        let orderly_service = match &*orderly.state.lock().await {
            State::Started {
                consume_service, ..
            } => consume_service.clone(),
            _ => panic!("orderly consumer should own a started consume service"),
        };
        assert_eq!(
            orderly_service.worker_count(),
            fixture["orderlyCorePoolSizeBefore"]
        );
        assert!(orderly.update_core_pool_size(6).await.is_ok());
        assert_eq!(
            orderly_service.worker_count(),
            fixture["orderlyCorePoolSizeAfterValid"]
        );
        assert!(orderly.update_core_pool_size(8).await.is_ok());
        assert!(orderly.update_core_pool_size(-1).await.is_ok());
        assert_eq!(
            orderly_service.worker_count(),
            fixture["orderlyCorePoolSizeAfterInvalid"]
        );
        orderly.config().set_consume_thread_max(10);
        assert!(matches!(
            orderly.update_core_pool_size(9).await,
            Err(PushConsumerError::InvalidSetting {
                name: "corePoolSize",
                value: 9,
                minimum: 1,
                maximum: 8,
            })
        ));
        assert_eq!(
            orderly_service.worker_count(),
            fixture["orderlyCorePoolSizeAfterRaisedMaxFailure"]
        );
        assert!(orderly.update_core_pool_size(8).await.is_ok());
        assert_eq!(
            orderly_service.worker_count(),
            fixture["orderlyCorePoolSizeAtExecutorMaximum"]
        );
        orderly.shutdown().await.unwrap();
        assert_eq!(
            orderly_service.worker_count(),
            0,
            "shutdown retires all Rust workers"
        );
        assert!(matches!(
            orderly.update_core_pool_size(9).await,
            Err(PushConsumerError::InvalidSetting {
                name: "corePoolSize",
                value: 9,
                minimum: 1,
                maximum: 8,
            })
        ));
        assert!(orderly.update_core_pool_size(7).await.is_ok());
        assert_eq!(
            orderly.consume_core_pool_size.load(Ordering::Acquire),
            fixture["orderlyCorePoolSizeAfterShutdownUpdate"]
        );
        drop(orderly_runtime);
    }

    #[tokio::test]
    async fn push_shutdown_unregisters_the_current_java_group() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        let expected_retained = fixture["pushGroupNameMutation"]
            ["originalRegistrationRemainsAfterCurrentGroupUnregister"]
            .as_bool()
            .unwrap();

        let mut config = PushConsumerConfig::new("push-shutdown-group-before").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
        let listener: Arc<dyn super::super::listener::MessageListenerConcurrently> = Arc::new(
            |_messages: Vec<rocketmq_classic_common::message::MessageExt>, context| async move {
                (
                    Some(super::super::listener::ConsumeConcurrentlyStatus::ConsumeSuccess),
                    context,
                )
            },
        );
        let consume_service = Arc::new(ConcurrentConsumeService::new(1, 1, listener));
        let pull_scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store,
            consume_service,
            pull_scheduler,
            pull_rpc_workers,
        )
        .await;

        assert!(
            runtime
                .select_consumer("push-shutdown-group-before")
                .unwrap()
                .is_some()
        );
        consumer.set_consumer_group(Some("push-shutdown-group-after".into()));
        consumer.shutdown().await.unwrap();

        assert!(
            runtime
                .select_consumer("push-shutdown-group-after")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            runtime
                .select_consumer("push-shutdown-group-before")
                .unwrap()
                .is_some(),
            expected_retained
        );
    }

    #[tokio::test]
    async fn duplicate_group_registration_rolls_back_without_running_state() {
        let manager = ClientRuntimeManager::new();
        let make_consumer = || {
            let mut config = PushConsumerConfig::new("same-group").unwrap();
            config.client_config_mut().set_namesrv_addr(None);
            Arc::new(PushConsumer::with_manager(config, manager.clone()))
        };
        let first = make_consumer();
        let second = make_consumer();
        let listener = || {
            PushMessageListener::concurrently(
                |_messages: Vec<rocketmq_classic_common::message::MessageExt>, context| async move {
                    (
                        Some(super::super::listener::ConsumeConcurrentlyStatus::ConsumeSuccess),
                        context,
                    )
                },
            )
        };
        first.register_message_listener(listener()).await.unwrap();
        second.register_message_listener(listener()).await.unwrap();

        first.start().await.unwrap();
        assert_eq!(first.service_state(), Some(ConsumerServiceState::Running));
        assert!(matches!(
            second.start().await,
            Err(PushConsumerError::Runtime(_))
        ));
        assert_eq!(
            second.service_state(),
            Some(ConsumerServiceState::CreateJust)
        );
        assert!(!second.is_running().await);
        assert!(first.is_running().await);
        first.shutdown().await.unwrap();
        assert_eq!(
            first.service_state(),
            Some(ConsumerServiceState::ShutdownAlready)
        );
        assert!(!first.is_running().await);
    }

    #[tokio::test]
    async fn startup_rolls_back_each_runtime_registration_and_route_stage() {
        for stage in StartupStage::all() {
            let mut config = PushConsumerConfig::new(format!("rollback-{}", stage.name())).unwrap();
            config.client_config_mut().set_namesrv_addr(None);
            let manager = ClientRuntimeManager::new();
            let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
            consumer
                .register_message_listener(PushMessageListener::concurrently(
                    |_messages: Vec<rocketmq_classic_common::message::MessageExt>, context| async move {
                        (
                            Some(super::super::listener::ConsumeConcurrentlyStatus::ConsumeSuccess),
                            context,
                        )
                    },
                ))
                .await
                .unwrap();
            *consumer
                .startup_failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(stage);

            assert!(matches!(
                consumer.start().await,
                Err(PushConsumerError::TestStartupFault(_))
            ));
            assert!(!consumer.is_running().await);
            let runtime = manager
                .get_or_create(consumer.config().client_config())
                .await
                .unwrap();
            assert!(
                runtime
                    .select_consumer(consumer.config().consumer_group().as_deref().unwrap())
                    .unwrap()
                    .is_none()
            );
            runtime.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn factory_start_failure_preserves_java_push_consumer_registration() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/consumer/lifecycle.json"))
                .unwrap();
        let java = &fixture["pushFactoryStartFailure"];
        assert_eq!(java["exception"], "IllegalArgumentException");
        assert_eq!(java["consumerRegistered"], true);
        assert_eq!(java["consumerServiceState"], "START_FAILED");
        assert_eq!(java["factoryServiceState"], "START_FAILED");

        let group = "push-factory-start-failure";
        let mut config = PushConsumerConfig::new(group).unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        config.client_config_mut().set_poll_name_server_interval(0);
        let manager = ClientRuntimeManager::new();
        let client_config = config.client_config().clone();
        let runtime = manager.get_or_create(&client_config).await.unwrap();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        consumer
            .register_message_listener(PushMessageListener::concurrently(
                |_messages: Vec<rocketmq_classic_common::message::MessageExt>, context| async move {
                    (
                        Some(super::super::listener::ConsumeConcurrentlyStatus::ConsumeSuccess),
                        context,
                    )
                },
            ))
            .await
            .unwrap();

        assert!(matches!(
            consumer.start().await,
            Err(PushConsumerError::Runtime(
                rocketmq_classic_core::runtime::RuntimeError::InvalidSchedule
            ))
        ));
        assert_eq!(
            consumer.service_state(),
            Some(ConsumerServiceState::StartFailed)
        );
        assert!(!consumer.is_running().await);
        assert_eq!(
            runtime.state(),
            rocketmq_classic_core::runtime::RuntimeState::Failed
        );
        assert!(runtime.select_consumer(group).unwrap().is_some());
        assert!(Arc::ptr_eq(
            &runtime,
            &manager.get_or_create(&client_config).await.unwrap()
        ));
        assert!(
            runtime
                .unregister_consumer_by_group(
                    group,
                    tokio::time::Instant::now() + Duration::from_secs(1),
                )
                .await
                .unwrap()
        );
    }

    struct EmptyOrderlyListener;
    impl super::super::listener::MessageListenerOrderly for EmptyOrderlyListener {
        fn consume_message(
            &self,
            _messages: Vec<rocketmq_classic_common::message::MessageExt>,
            context: super::super::listener::ConsumeOrderlyContext,
        ) -> super::super::listener::OrderlyListenerFuture {
            Box::pin(async move {
                (
                    Some(super::super::listener::ConsumeOrderlyStatus::Success),
                    context,
                )
            })
        }
    }

    #[test]
    fn clustering_start_injects_retry_subscription_and_broadcast_does_not() {
        let clustering = PushConsumer::with_manager(
            PushConsumerConfig::new("orders").unwrap(),
            ClientRuntimeManager::new(),
        );
        clustering.copy_subscriptions("orders").unwrap();
        assert!(read_lock(&clustering.get_subscription_inner()).contains_key("%RETRY%orders"));

        let config = PushConsumerConfig::new("broadcast-orders").unwrap();
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        let broadcasting = PushConsumer::with_manager(config, ClientRuntimeManager::new());
        broadcasting.copy_subscriptions("broadcast-orders").unwrap();
        assert!(
            !read_lock(&broadcasting.get_subscription_inner())
                .keys()
                .any(|topic| topic.starts_with("%RETRY%"))
        );
    }

    #[tokio::test]
    async fn class_filter_subscription_matches_java_facade_and_heartbeat_flow() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        let mut config = PushConsumerConfig::new("phase18-class-filter-subscribe").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));

        let before_class = fixture["classFilterBeforeStartClassName"].as_str().unwrap();
        let before_source = fixture["classFilterBeforeStartSource"].as_str().unwrap();
        consumer
            .subscribe_with_class_filter("class-filter-orders", before_class, before_source)
            .await
            .unwrap();

        let subscription_table = consumer.get_subscription_inner();
        let before_start = read_lock(&subscription_table)
            .get("tenant%class-filter-orders")
            .cloned()
            .expect("class-filter subscription is stored before start");
        assert_eq!(
            before_start.get_topic(),
            fixture["classFilterBeforeStartTopic"].as_str()
        );
        assert_eq!(
            before_start.get_sub_string(),
            fixture["classFilterBeforeStartClassName"].as_str()
        );
        assert_eq!(
            before_start.is_class_filter_mode(),
            fixture["classFilterBeforeStartMode"].as_bool().unwrap()
        );
        assert_eq!(
            before_start.get_filter_class_source(),
            fixture["classFilterBeforeStartSource"].as_str()
        );
        let before_wire = serde_json::to_value(&before_start).unwrap();
        assert_eq!(before_wire["topic"], fixture["classFilterBeforeStartTopic"]);
        assert_eq!(
            before_wire["subString"],
            fixture["classFilterBeforeStartClassName"]
        );
        assert_eq!(
            before_wire["classFilterMode"],
            fixture["classFilterBeforeStartMode"]
        );
        assert!(before_wire.get("filterClassSource").is_none());
        assert_eq!(
            fixture["classFilterSourceSerialized"],
            serde_json::json!(false)
        );

        consumer
            .register_message_listener(PushMessageListener::concurrently(
                |_messages: Vec<rocketmq_classic_common::message::MessageExt>, context| async move {
                    (
                        Some(super::super::listener::ConsumeConcurrentlyStatus::ConsumeSuccess),
                        context,
                    )
                },
            ))
            .await
            .unwrap();
        consumer.start().await.unwrap();
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let class_topic = fixture["classFilterBeforeStartTopic"].as_str().unwrap();
        let registration = runtime
            .select_consumer("tenant%phase18-class-filter-subscribe")
            .unwrap()
            .expect("started consumer is registered");
        let heartbeat_subscription = registration
            .heartbeat_data()
            .subscription_data_set
            .expect("Push heartbeat carries subscriptions")
            .snapshot()
            .into_iter()
            .flatten()
            .find_map(|subscription| {
                let subscription = subscription.snapshot();
                (subscription.topic.as_deref() == Some(class_topic)).then_some(subscription)
            })
            .expect("class-filter subscription is present in heartbeat state");
        assert_eq!(
            heartbeat_subscription.sub_string.as_deref(),
            fixture["classFilterBeforeStartClassName"].as_str()
        );
        assert_eq!(
            heartbeat_subscription.filter_class_source.as_deref(),
            fixture["classFilterBeforeStartSource"].as_str()
        );

        let after_class = fixture["classFilterAfterFactoryClassName"]
            .as_str()
            .unwrap();
        let after_source = fixture["classFilterAfterFactorySource"].as_str().unwrap();
        consumer
            .subscribe_with_class_filter("class-filter-orders", after_class, after_source)
            .await
            .unwrap();
        let updated_registration = runtime
            .select_consumer("tenant%phase18-class-filter-subscribe")
            .unwrap()
            .expect("started consumer remains registered");
        let updated_heartbeat_subscription = updated_registration
            .heartbeat_data()
            .subscription_data_set
            .expect("Push heartbeat carries subscriptions")
            .snapshot()
            .into_iter()
            .flatten()
            .find_map(|subscription| {
                let subscription = subscription.snapshot();
                (subscription.topic.as_deref() == Some(class_topic)).then_some(subscription)
            })
            .expect("class-filter update reaches heartbeat state");
        assert_eq!(
            updated_heartbeat_subscription.topic.as_deref(),
            fixture["classFilterAfterFactoryTopic"].as_str()
        );
        assert_eq!(
            updated_heartbeat_subscription.sub_string.as_deref(),
            fixture["classFilterAfterFactoryClassName"].as_str()
        );
        assert_eq!(
            updated_heartbeat_subscription.class_filter_mode,
            fixture["classFilterAfterFactoryMode"].as_bool().unwrap()
        );
        assert_eq!(
            updated_heartbeat_subscription
                .filter_class_source
                .as_deref(),
            fixture["classFilterAfterFactorySource"].as_str()
        );
        assert_eq!(
            fixture["classFilterAfterFactoryHeartbeatCalls"],
            serde_json::json!(1)
        );
        let mut rust_wire = serde_json::to_value(&updated_heartbeat_subscription).unwrap();
        let mut java_wire = fixture["classFilterAfterFactoryWireData"].clone();
        assert!(rust_wire.get("filterClassSource").is_none());
        rust_wire.as_object_mut().unwrap().remove("subVersion");
        java_wire.as_object_mut().unwrap().remove("subVersion");
        assert_eq!(rust_wire, java_wire);

        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn legacy_subscription_map_is_namespaced_and_copied_into_heartbeat_state() {
        let mut config = PushConsumerConfig::new("orders").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let consumer = PushConsumer::with_manager(config, ClientRuntimeManager::new());
        consumer
            .set_subscription(HashMap::from([(
                "orders".to_owned(),
                "gold || silver".to_owned(),
            )]))
            .await
            .unwrap();

        assert_eq!(
            consumer.get_subscription().get("tenant%orders"),
            Some(&"gold || silver".to_owned())
        );
        consumer.copy_subscriptions("tenant%orders").unwrap();
        assert_eq!(
            read_lock(&consumer.get_subscription_inner())
                .get("tenant%orders")
                .and_then(SubscriptionData::get_sub_string),
            Some("gold || silver")
        );
    }

    #[test]
    fn retry_and_dlq_messages_restore_logical_topics_for_orderly_and_pop() {
        let mut config = PushConsumerConfig::new("namespace-consumer").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let consumer = PushConsumer::with_manager(config, ClientRuntimeManager::new());
        let mut client_config = consumer.config.client_config().clone();
        let namespaced_group = client_config
            .with_namespace(consumer.config.consumer_group().as_deref())
            .unwrap()
            .unwrap();

        let make_messages = || {
            let mut retry = retry_test_message(1);
            retry.set_topic(format!("%RETRY%{namespaced_group}"));
            retry
                .set_system_property(PROPERTY_RETRY_TOPIC, "tenant%orders")
                .unwrap();

            let mut dlq = retry_test_message(2);
            dlq.set_topic(format!(
                "%DLQ%tenant%{}",
                consumer.config().consumer_group().unwrap()
            ));

            let mut normal = retry_test_message(3);
            normal.set_topic("tenant%orders");
            vec![retry, dlq, normal]
        };

        let mut orderly_messages = make_messages();
        consumer.reset_retry_and_namespace_for_group(&mut orderly_messages, &namespaced_group);
        assert_eq!(
            orderly_messages
                .iter()
                .map(|message| message.topic())
                .collect::<Vec<_>>(),
            ["orders", "%DLQ%namespace-consumer", "orders"]
        );

        let mut pop_messages = make_messages();
        consumer.reset_retry_and_namespace_for_pop(&mut pop_messages, &namespaced_group);
        assert_eq!(
            pop_messages
                .iter()
                .map(|message| message.topic())
                .collect::<Vec<_>>(),
            ["orders", "%DLQ%namespace-consumer", "orders"]
        );
    }

    #[tokio::test]
    async fn shutdown_aborts_a_slow_listener_without_committing_its_batch() {
        struct NotifyOnDrop(Arc<tokio::sync::Notify>);
        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }

        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let cancelled = Arc::new(tokio::sync::Notify::new());
        let listener: Arc<dyn MessageListenerConcurrently> = {
            let started = started.clone();
            let release = release.clone();
            let cancelled = cancelled.clone();
            Arc::new(
                move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                    let started = started.clone();
                    let release = release.clone();
                    let cancelled = cancelled.clone();
                    async move {
                        let _drop_signal = NotifyOnDrop(cancelled);
                        started.notify_one();
                        release.notified().await;
                        (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                    }
                },
            )
        };
        let mut config = PushConsumerConfig::new("slow-consumer-shutdown").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_consume_thread_min(1);
        config.set_consume_thread_max(1);
        config.set_await_termination_millis_when_shutdown(10);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let consume_service = Arc::new(ConcurrentConsumeService::new(1, 1, listener));
        let scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            consume_service.clone(),
            scheduler.clone(),
            pull_rpc_workers.clone(),
        )
        .await;

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 40));
        process_queue
            .insert_messages(40, vec![retry_test_message(50)])
            .unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        offset_store.update_offset(&queue, 50, false);
        let context = consumer.pull_context().await.unwrap();
        consumer.submit_consume_batch(&context, process_queue.clone(), 40);
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();

        consumer.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), cancelled.notified())
            .await
            .unwrap();
        let snapshot = process_queue.snapshot(40).unwrap();
        assert!(snapshot.dropped);
        assert_eq!(snapshot.cached_count, 1);
        assert_eq!(snapshot.in_flight_count, 1);
        assert_eq!(offset_store.values.lock().unwrap().get(&queue), Some(&50));
        assert_eq!(consume_service.worker_count(), 0);
        assert_eq!(scheduler.worker_count(), 0);
        assert_eq!(pull_rpc_workers.worker_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn pop_shutdown_wait_budget_is_independent_of_worker_count() {
        async fn elapsed_for(worker_count: usize) -> Duration {
            let handles = (0..worker_count)
                .map(|_| tokio::spawn(std::future::pending::<()>()))
                .collect();
            tokio::task::yield_now().await;
            let started = Instant::now();
            shutdown_pop_worker_handles(handles, 100).await;
            started.elapsed()
        }

        let one_worker = elapsed_for(1).await;
        let twenty_workers = elapsed_for(20).await;
        let service_budget = Duration::from_millis(200);
        assert!(
            one_worker <= service_budget,
            "one worker waited {one_worker:?}"
        );
        assert!(
            twenty_workers <= service_budget,
            "twenty workers waited {twenty_workers:?}"
        );
        assert!(
            twenty_workers <= one_worker + Duration::from_millis(10),
            "wait grew with worker count: one={one_worker:?}, twenty={twenty_workers:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_with_explicit_wait_overrides_configured_wait() {
        struct NotifyOnDrop(Arc<tokio::sync::Notify>);
        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }

        let started = Arc::new(tokio::sync::Notify::new());
        let completed = Arc::new(AtomicBool::new(false));
        let cancelled = Arc::new(tokio::sync::Notify::new());
        let listener: Arc<dyn MessageListenerConcurrently> = {
            let started = started.clone();
            let completed = completed.clone();
            let cancelled = cancelled.clone();
            Arc::new(
                move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                    let started = started.clone();
                    let completed = completed.clone();
                    let cancelled = cancelled.clone();
                    async move {
                        let _drop_signal = NotifyOnDrop(cancelled);
                        started.notify_one();
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        completed.store(true, Ordering::Release);
                        (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                    }
                },
            )
        };
        let mut config = PushConsumerConfig::new("shutdown-explicit-wait-override").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_consume_thread_min(1);
        config.set_consume_thread_max(1);
        config.set_await_termination_millis_when_shutdown(0);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let consume_service = Arc::new(ConcurrentConsumeService::new(1, 1, listener));
        let scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            consume_service.clone(),
            scheduler.clone(),
            pull_rpc_workers.clone(),
        )
        .await;

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 40));
        process_queue
            .insert_messages(40, vec![retry_test_message(50)])
            .unwrap();
        write_lock(&consumer.process_queues).insert(queue, process_queue.clone());
        consumer.submit_consume_batch(&consumer.pull_context().await.unwrap(), process_queue, 40);
        started.notified().await;

        let shutdown_consumer = consumer.clone();
        let shutdown = tokio::spawn(async move {
            shutdown_consumer
                .shutdown_with_await_termination_millis(100)
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;
        assert!(!shutdown.is_finished());
        tokio::time::advance(Duration::from_millis(50)).await;
        shutdown.await.unwrap();

        assert!(completed.load(Ordering::Acquire));
        assert_eq!(consume_service.worker_count(), 0);
        assert_eq!(scheduler.worker_count(), 0);
        assert_eq!(pull_rpc_workers.worker_count(), 0);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), cancelled.notified())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn orderly_batch_lock_applies_partial_results_grouped_by_broker() {
        let mut config = PushConsumerConfig::new("orderly-partial-lock").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let queues = [
            MessageQueue::new("orders", "broker-a", 0),
            MessageQueue::new("orders", "broker-a", 1),
            MessageQueue::new("orders", "broker-b", 0),
        ];
        let process_queues = queues
            .iter()
            .enumerate()
            .map(|(index, queue)| {
                let process_queue = Arc::new(ProcessQueue::new_for_assignment(
                    queue.clone(),
                    80 + index as u64,
                ));
                if index == 1 {
                    process_queue
                        .set_lock_state(81, true, epoch_millis())
                        .unwrap();
                }
                process_queue
            })
            .collect::<Vec<_>>();
        {
            let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
            scripted.lock_results.extend([
                Ok(Some(HashSet::from([Some(queues[0].clone())]))),
                Ok(Some(HashSet::new())),
            ]);
        }

        let locked = consumer
            .lock_orderly_process_queues(&runtime, &process_queues)
            .await;
        assert_eq!(locked, HashSet::from([queues[0].clone()]));
        assert!(process_queues[0].is_lock_fresh(80, 30_000).unwrap());
        assert!(!process_queues[1].snapshot(81).unwrap().locked);
        assert!(!process_queues[2].snapshot(82).unwrap().locked);

        {
            let scripted = consumer.scripted_lock_transport.lock().unwrap();
            assert_eq!(scripted.lock_bodies.len(), 2);
            let mut grouped_queue_counts = scripted
                .lock_bodies
                .iter()
                .map(|body| body.get_mq_set().unwrap().with(|set| set.len()))
                .collect::<Vec<_>>();
            grouped_queue_counts.sort_unstable();
            assert_eq!(grouped_queue_counts, vec![1, 2]);
        }
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_lock_requests_keep_the_rebalance_group_snapshot() {
        let group = "phase18-consume-service-group";
        let mut config = PushConsumerConfig::new(group).unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
                (Some(ConsumeOrderlyStatus::Success), context)
            },
        );
        let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
        let runtime =
            install_started_orderly_consumer(&consumer, &manager, store, listener, None).await;
        consumer.set_consumer_group(Some("phase18-consume-service-group-mutated".into()));

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        assert_eq!(fixture["rebalanceConsumerGroupAfterFacadeMutation"], group);
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 195));
        consumer
            .scripted_lock_transport
            .lock()
            .unwrap()
            .lock_results
            .push_back(Ok(Some(HashSet::from([Some(queue.clone())]))));

        assert_eq!(
            consumer
                .lock_orderly_process_queues(&runtime, &[process_queue])
                .await,
            HashSet::from([queue])
        );
        assert_eq!(
            consumer.scripted_lock_transport.lock().unwrap().lock_bodies[0].get_consumer_group(),
            fixture["rebalanceConsumerGroupAfterFacadeMutation"].as_str()
        );

        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_lock_errors_and_nullable_sets_preserve_previous_state() {
        let mut config = PushConsumerConfig::new("orderly-lock-uncertain").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue, 83));
        process_queue
            .set_lock_state(83, true, epoch_millis())
            .unwrap();
        let queues = std::slice::from_ref(&process_queue);

        consumer
            .scripted_lock_transport
            .lock()
            .unwrap()
            .lock_results
            .push_back(Err("scripted timeout".into()));
        consumer.lock_orderly_process_queues(&runtime, queues).await;
        assert!(process_queue.snapshot(83).unwrap().locked);
        assert!(process_queue.is_lock_fresh(83, 30_000).unwrap());

        consumer
            .scripted_lock_transport
            .lock()
            .unwrap()
            .lock_results
            .push_back(Ok(None));
        consumer.lock_orderly_process_queues(&runtime, queues).await;
        assert!(process_queue.snapshot(83).unwrap().locked);
        assert!(process_queue.is_lock_fresh(83, 30_000).unwrap());

        consumer
            .scripted_lock_transport
            .lock()
            .unwrap()
            .lock_results
            .push_back(Ok(Some(HashSet::new())));
        consumer.lock_orderly_process_queues(&runtime, queues).await;
        assert!(!process_queue.snapshot(83).unwrap().locked);
        assert!(!process_queue.is_lock_fresh(83, 30_000).unwrap());
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn older_orderly_renewal_response_cannot_overwrite_the_newer_attempt() {
        let mut config = PushConsumerConfig::new("orderly-lock-race").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 84));
        let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
        let (second_sender, second_receiver) = tokio::sync::oneshot::channel();
        let requested = {
            let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
            scripted
                .lock_waiters
                .extend([first_receiver, second_receiver]);
            scripted.lock_requested.clone()
        };

        let first_notice = requested.notified();
        let first_consumer = consumer.clone();
        let first_runtime = runtime.clone();
        let first_queue = process_queue.clone();
        let first = tokio::spawn(async move {
            first_consumer
                .lock_orderly_process_queues(&first_runtime, &[first_queue])
                .await
        });
        first_notice.await;

        let second_notice = requested.notified();
        let second_consumer = consumer.clone();
        let second_runtime = runtime.clone();
        let second_queue = process_queue.clone();
        let second = tokio::spawn(async move {
            second_consumer
                .lock_orderly_process_queues(&second_runtime, &[second_queue])
                .await
        });
        second_notice.await;

        first_sender
            .send(Ok(Some(HashSet::from([Some(queue.clone())]))))
            .unwrap();
        assert!(first.await.unwrap().is_empty());
        assert!(!process_queue.snapshot(84).unwrap().locked);

        second_sender
            .send(Ok(Some(HashSet::from([Some(queue.clone())]))))
            .unwrap();
        assert_eq!(second.await.unwrap(), HashSet::from([queue]));
        assert!(process_queue.snapshot(84).unwrap().locked);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn orderly_batch_lock_age_starts_at_each_broker_response() {
        let mut config = PushConsumerConfig::new("orderly-lock-response-time").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let queue_a = MessageQueue::new("orders", "broker-a", 0);
        let queue_b = MessageQueue::new("orders", "broker-b", 0);
        let process_queue_a = Arc::new(ProcessQueue::new_for_assignment(queue_a.clone(), 94));
        let process_queue_b = Arc::new(ProcessQueue::new_for_assignment(queue_b.clone(), 95));
        let (response_a, waiter_a) = tokio::sync::oneshot::channel();
        let (response_b, waiter_b) = tokio::sync::oneshot::channel();
        let requested = {
            let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
            scripted.lock_waiters.extend([waiter_a, waiter_b]);
            scripted.lock_requested.clone()
        };

        let first_request = requested.notified();
        let task_consumer = consumer.clone();
        let task_runtime = runtime.clone();
        let task_queues = [process_queue_a.clone(), process_queue_b.clone()];
        let lock_task = tokio::spawn(async move {
            task_consumer
                .lock_orderly_process_queues(&task_runtime, &task_queues)
                .await
        });
        first_request.await;
        response_a
            .send(Ok(Some(HashSet::from([Some(queue_a.clone())]))))
            .unwrap();

        let second_request = requested.notified();
        second_request.await;
        tokio::time::advance(Duration::from_millis(30_001)).await;
        response_b
            .send(Ok(Some(HashSet::from([Some(queue_b.clone())]))))
            .unwrap();

        assert_eq!(
            lock_task.await.unwrap(),
            HashSet::from([queue_a.clone(), queue_b.clone()])
        );
        assert!(!process_queue_a.is_lock_fresh(94, 30_000).unwrap());
        assert!(process_queue_b.is_lock_fresh(95, 30_000).unwrap());
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn late_orderly_lock_for_dropped_generation_is_unlocked() {
        let mut config = PushConsumerConfig::new("orderly-dropped-lock-race").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 85));
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();
        let requested = {
            let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
            scripted.lock_waiters.push_back(response_receiver);
            scripted.unlock_results.push_back(Ok(()));
            scripted.lock_requested.clone()
        };

        let notice = requested.notified();
        let task_consumer = consumer.clone();
        let task_runtime = runtime.clone();
        let task_queue = process_queue.clone();
        let task = tokio::spawn(async move {
            task_consumer
                .lock_orderly_process_queues(&task_runtime, &[task_queue])
                .await
        });
        notice.await;
        process_queue.drop_assignment(85).unwrap();
        response_sender
            .send(Ok(Some(HashSet::from([Some(queue.clone())]))))
            .unwrap();

        assert!(task.await.unwrap().is_empty());
        assert!(process_queue.snapshot(85).unwrap().dropped);
        {
            let scripted = consumer.scripted_lock_transport.lock().unwrap();
            assert_eq!(scripted.unlock_bodies.len(), 1);
        }
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_success_holds_until_commit_and_rollback_replays_in_order() {
        let mut config = PushConsumerConfig::new("orderly-status-flow").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_consume_message_batch_max_size(1);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let hook_events = Arc::new(Mutex::new(Vec::new()));
        struct Hook(Arc<Mutex<Vec<String>>>);
        impl ConsumeMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "orderly-consume-hook"
            }

            fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {
                self.0.lock().unwrap().push("before".into());
            }

            fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
                self.0.lock().unwrap().push(format!(
                    "{}:{}",
                    context.status.as_deref().unwrap_or("missing"),
                    context.success
                ));
            }
        }
        consumer.register_consume_message_hook(Arc::new(Hook(hook_events.clone())));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let outcomes = Arc::new(Mutex::new(VecDeque::from([
            (Some(ConsumeOrderlyStatus::Success), false, -1),
            (Some(ConsumeOrderlyStatus::Rollback), false, 30_000),
            (Some(ConsumeOrderlyStatus::Success), true, -1),
            (Some(ConsumeOrderlyStatus::Success), true, -1),
        ])));
        let seen = Arc::new(Mutex::new(Vec::<Vec<i64>>::new()));
        let listener: Arc<dyn MessageListenerOrderly> = {
            let outcomes = outcomes.clone();
            let seen = seen.clone();
            Arc::new(
                move |messages: Vec<MessageExt>, mut context: ConsumeOrderlyContext| {
                    let (status, auto_commit, suspend_millis) =
                        outcomes.lock().unwrap().pop_front().unwrap();
                    let offsets = messages
                        .iter()
                        .map(MessageExt::queue_offset)
                        .collect::<Vec<_>>();
                    let seen = seen.clone();
                    async move {
                        seen.lock().unwrap().push(offsets);
                        context.set_auto_commit(auto_commit);
                        context.set_suspend_current_queue_time_millis(suspend_millis);
                        (status, context)
                    }
                },
            )
        };
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            listener.clone(),
            None,
        )
        .await;

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 86));
        process_queue
            .insert_messages(86, vec![retry_test_message(10), retry_test_message(11)])
            .unwrap();
        let request = OrderlyConsumeRequest {
            process_queue: process_queue.clone(),
            generation: 86,
        };
        consumer
            .clone()
            .run_orderly_consume_request(request, listener.clone())
            .await;
        assert_eq!(process_queue.snapshot(86).unwrap().cached_count, 2);
        assert!(offset_store.updates.lock().unwrap().is_empty());

        let request = OrderlyConsumeRequest {
            process_queue: process_queue.clone(),
            generation: 86,
        };
        consumer
            .clone()
            .run_orderly_consume_request(request, listener)
            .await;
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            &[vec![10], vec![11], vec![10], vec![11]]
        );
        assert_eq!(process_queue.snapshot(86).unwrap().cached_count, 0);
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue.clone(), 11, false), (queue, 12, false)]
        );
        assert_eq!(
            hook_events.lock().unwrap().as_slice(),
            [
                "before",
                "SUCCESS:true",
                "before",
                "ROLLBACK:false",
                "before",
                "SUCCESS:true",
                "before",
                "SUCCESS:true"
            ]
        );
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_clustering_rebalance_locks_before_activating_pull_queue() {
        let mut config = PushConsumerConfig::new("orderly-lock-before-pull").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
        config.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.topic_queues)
            .insert("orders".to_owned(), BTreeSet::from([queue.clone()]));
        let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
                (Some(ConsumeOrderlyStatus::Success), context)
            },
        );
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime =
            install_started_orderly_consumer(&consumer, &manager, offset_store_dyn, listener, None)
                .await;
        let client_id = runtime.client_id().to_owned();
        script_rebalance_membership(
            &consumer,
            "orders",
            [Some(vec![client_id.clone()]), Some(vec![client_id])],
        );
        {
            let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
            scripted.lock_results.extend([
                Ok(Some(HashSet::new())),
                Ok(Some(HashSet::from([Some(queue.clone())]))),
            ]);
            scripted.unlock_results.push_back(Ok(()));
        }
        consumer.paused.store(true, Ordering::Release);

        assert!(
            !consumer
                .rebalance_once(&crate::rebalance::AllocateMessageQueueAveragely, None)
                .await
        );
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));

        consumer.paused.store(false, Ordering::Release);
        assert!(consumer.do_rebalance_with_order(true).await);
        let process_queue = read_lock(&consumer.process_queues)
            .get(&queue)
            .cloned()
            .expect("successful Broker lock permits assignment activation");
        assert!(
            process_queue
                .snapshot(process_queue.generation())
                .unwrap()
                .locked
        );
        assert_eq!(offset_store.values.lock().unwrap().get(&queue), Some(&0));
        {
            let scripted = consumer.scripted_lock_transport.lock().unwrap();
            assert_eq!(scripted.lock_bodies.len(), 2);
            assert!(scripted.lock_bodies.iter().all(|body| {
                body.get_mq_set().is_some_and(|set| {
                    set.with(|queues| queues.contains(&Some(SharedValue::new(queue.clone()))))
                })
            }));
        }

        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn orderly_max_continuous_consume_property_yields_between_batches() {
        let mut config = PushConsumerConfig::new("orderly-continuous-limit").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_consume_message_batch_max_size(1);
        config.set_consume_thread_min(1);
        config.set_consume_thread_max(1);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let callback_count = Arc::new(AtomicUsize::new(0));
        let listener: Arc<dyn MessageListenerOrderly> = {
            let callback_count = callback_count.clone();
            Arc::new(
                move |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                    let callback_count = callback_count.clone();
                    async move {
                        callback_count.fetch_add(1, Ordering::SeqCst);
                        tokio::time::advance(Duration::from_millis(1)).await;
                        (Some(ConsumeOrderlyStatus::Success), context)
                    }
                },
            )
        };
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let settings = OrderlyLockSettings::from_all_values(30_000, 20_000, 20_000, 0).unwrap();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            listener.clone(),
            Some(settings),
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 95));
        process_queue
            .insert_messages(95, vec![retry_test_message(400), retry_test_message(401)])
            .unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        offset_store.update_offset(&queue, 400, false);

        consumer
            .clone()
            .run_orderly_consume_request(
                OrderlyConsumeRequest {
                    process_queue: process_queue.clone(),
                    generation: 95,
                },
                listener,
            )
            .await;

        assert_eq!(callback_count.load(Ordering::SeqCst), 1);
        assert_eq!(process_queue.snapshot(95).unwrap().cached_count, 1);
        assert_eq!(
            process_queue.snapshot(95).unwrap().next_commit_offset,
            Some(401)
        );
        assert_eq!(
            offset_store.updates.lock().unwrap().last(),
            Some(&(queue, 401, false))
        );
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_retry_limit_publishes_retry_topic_and_failed_publish_requeues() {
        let mut config = PushConsumerConfig::new("orderly-retry-limit").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_max_reconsume_times(2);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
                (
                    Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment),
                    context,
                )
            },
        );
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            listener.clone(),
            None,
        )
        .await;
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .retry_results
            .push_back(Ok(()));

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 87));
        process_queue
            .insert_messages(87, vec![retry_test_message(20)])
            .unwrap();
        consumer
            .clone()
            .run_orderly_consume_request(
                OrderlyConsumeRequest {
                    process_queue: process_queue.clone(),
                    generation: 87,
                },
                listener.clone(),
            )
            .await;
        assert_eq!(process_queue.snapshot(87).unwrap().cached_count, 0);
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue, 21, false)]
        );
        {
            let scripted = consumer.scripted_retry_transport.lock().unwrap();
            assert!(scripted.broker_headers.is_empty());
            assert_eq!(scripted.retry_messages.len(), 1);
            let retry = &scripted.retry_messages[0];
            assert_eq!(retry.topic(), "%RETRY%orderly-retry-limit");
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_RETRY_TOPIC)
                    .map(String::as_str),
                Some("orders")
            );
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_RECONSUME_TIME)
                    .map(String::as_str),
                Some("3")
            );
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_MAX_RECONSUME_TIMES)
                    .map(String::as_str),
                Some("2")
            );
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_DELAY_TIME_LEVEL)
                    .map(String::as_str),
                Some("5")
            );
            assert!(
                !retry
                    .properties()
                    .contains_key(PROPERTY_TRANSACTION_PREPARED)
            );
        }
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();

        let mut config = PushConsumerConfig::new("orderly-retry-fallback-failure").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_max_reconsume_times(2);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            listener.clone(),
            None,
        )
        .await;
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .retry_results
            .push_back(Err("retry publish rejected".into()));
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 88));
        process_queue
            .insert_messages(88, vec![retry_test_message(30)])
            .unwrap();
        consumer
            .clone()
            .run_orderly_consume_request(
                OrderlyConsumeRequest {
                    process_queue: process_queue.clone(),
                    generation: 88,
                },
                listener,
            )
            .await;
        assert_eq!(process_queue.snapshot(88).unwrap().cached_count, 1);
        assert!(offset_store.updates.lock().unwrap().is_empty());
        let redelivery = process_queue.take_batch(88, 1).unwrap().unwrap();
        assert_eq!(redelivery.messages()[0].reconsume_times(), 3);
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_callbacks_on_same_queue_are_serialized_in_offset_order() {
        let mut config = PushConsumerConfig::new("orderly-serialized-queue").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_consume_thread_min(2);
        config.set_consume_thread_max(2);
        config.set_consume_message_batch_max_size(1);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let first_started = Arc::new(tokio::sync::Notify::new());
        let second_started = Arc::new(tokio::sync::Notify::new());
        let release_first = Arc::new(tokio::sync::Notify::new());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let listener: Arc<dyn MessageListenerOrderly> = {
            let first_started = first_started.clone();
            let second_started = second_started.clone();
            let release_first = release_first.clone();
            let seen = seen.clone();
            Arc::new(
                move |messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                    let offset = messages[0].queue_offset();
                    let first_started = first_started.clone();
                    let second_started = second_started.clone();
                    let release_first = release_first.clone();
                    let seen = seen.clone();
                    async move {
                        seen.lock().unwrap().push(offset);
                        if offset == 100 {
                            first_started.notify_one();
                            release_first.notified().await;
                        } else {
                            second_started.notify_one();
                        }
                        (Some(ConsumeOrderlyStatus::Success), context)
                    }
                },
            )
        };
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store;
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            listener.clone(),
            None,
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue, 96));
        process_queue
            .insert_messages(96, vec![retry_test_message(100), retry_test_message(101)])
            .unwrap();

        let first_notice = first_started.notified();
        let first_consumer = consumer.clone();
        let first_queue = process_queue.clone();
        let first_listener = listener.clone();
        let first = tokio::spawn(async move {
            first_consumer
                .run_orderly_consume_request(
                    OrderlyConsumeRequest {
                        process_queue: first_queue,
                        generation: 96,
                    },
                    first_listener,
                )
                .await
        });
        first_notice.await;

        let second_notice = second_started.notified();
        let second_consumer = consumer.clone();
        let second_queue = process_queue.clone();
        let second_listener = listener.clone();
        let second = tokio::spawn(async move {
            second_consumer
                .run_orderly_consume_request(
                    OrderlyConsumeRequest {
                        process_queue: second_queue,
                        generation: 96,
                    },
                    second_listener,
                )
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), second_notice)
                .await
                .is_err()
        );

        release_first.notify_one();
        tokio::time::timeout(Duration::from_secs(1), second_started.notified())
            .await
            .expect("the next callback should run after the first releases the queue gate");
        first.await.unwrap();
        second.await.unwrap();
        assert_eq!(seen.lock().unwrap().as_slice(), &[100, 101]);
        assert_eq!(process_queue.snapshot(96).unwrap().cached_count, 0);
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_callbacks_on_different_queues_make_progress_independently() {
        let mut config = PushConsumerConfig::new("orderly-independent-queues").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_consume_thread_min(2);
        config.set_consume_thread_max(2);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let slow_started = Arc::new(tokio::sync::Notify::new());
        let release_slow = Arc::new(tokio::sync::Notify::new());
        let fast_finished = Arc::new(tokio::sync::Notify::new());
        let listener: Arc<dyn MessageListenerOrderly> = {
            let slow_started = slow_started.clone();
            let release_slow = release_slow.clone();
            let fast_finished = fast_finished.clone();
            Arc::new(
                move |messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                    let offset = messages[0].queue_offset();
                    let slow_started = slow_started.clone();
                    let release_slow = release_slow.clone();
                    let fast_finished = fast_finished.clone();
                    async move {
                        if offset == 100 {
                            slow_started.notify_one();
                            release_slow.notified().await;
                        } else {
                            fast_finished.notify_one();
                        }
                        (Some(ConsumeOrderlyStatus::Success), context)
                    }
                },
            )
        };
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            listener.clone(),
            None,
        )
        .await;

        let queue_a = MessageQueue::new("orders", "broker-a", 0);
        let queue_b = MessageQueue::new("orders", "broker-a", 1);
        let process_queue_a = Arc::new(ProcessQueue::new_for_assignment(queue_a, 89));
        let process_queue_b = Arc::new(ProcessQueue::new_for_assignment(queue_b, 90));
        process_queue_a
            .insert_messages(89, vec![retry_test_message(100)])
            .unwrap();
        process_queue_b
            .insert_messages(90, vec![retry_test_message(200)])
            .unwrap();

        let slow_notice = slow_started.notified();
        let task_consumer = consumer.clone();
        let task_listener = listener.clone();
        let task_a = process_queue_a.clone();
        let slow = tokio::spawn(async move {
            task_consumer
                .run_orderly_consume_request(
                    OrderlyConsumeRequest {
                        process_queue: task_a,
                        generation: 89,
                    },
                    task_listener,
                )
                .await
        });
        slow_notice.await;

        let fast_notice = fast_finished.notified();
        let task_consumer = consumer.clone();
        let task_listener = listener.clone();
        let task_b = process_queue_b.clone();
        let fast = tokio::spawn(async move {
            task_consumer
                .run_orderly_consume_request(
                    OrderlyConsumeRequest {
                        process_queue: task_b,
                        generation: 90,
                    },
                    task_listener,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), fast_notice)
            .await
            .expect("the second queue must not wait on the first queue's callback");
        fast.await.unwrap();
        release_slow.notify_one();
        slow.await.unwrap();

        assert_eq!(process_queue_a.snapshot(89).unwrap().cached_count, 0);
        assert_eq!(process_queue_b.snapshot(90).unwrap().cached_count, 0);
        assert_eq!(offset_store.updates.lock().unwrap().len(), 2);
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn orderly_lock_loss_during_callback_rolls_back_without_committing() {
        let mut config = PushConsumerConfig::new("orderly-lock-loss").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_consume_thread_min(1);
        config.set_consume_thread_max(1);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let callback_started = Arc::new(tokio::sync::Notify::new());
        let release_callback = Arc::new(tokio::sync::Notify::new());
        let listener: Arc<dyn MessageListenerOrderly> = {
            let callback_started = callback_started.clone();
            let release_callback = release_callback.clone();
            Arc::new(
                move |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                    let callback_started = callback_started.clone();
                    let release_callback = release_callback.clone();
                    async move {
                        callback_started.notify_one();
                        release_callback.notified().await;
                        (Some(ConsumeOrderlyStatus::Success), context)
                    }
                },
            )
        };
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let settings = OrderlyLockSettings::from_values(30_000, 20_000).unwrap();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            listener.clone(),
            Some(settings),
        )
        .await;

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 91));
        process_queue
            .insert_messages(91, vec![retry_test_message(300)])
            .unwrap();
        process_queue
            .set_lock_state(91, true, epoch_millis())
            .unwrap();
        let callback_notice = callback_started.notified();
        let task_consumer = consumer.clone();
        let task_listener = listener.clone();
        let task_queue = process_queue.clone();
        let task = tokio::spawn(async move {
            task_consumer
                .run_orderly_consume_request(
                    OrderlyConsumeRequest {
                        process_queue: task_queue,
                        generation: 91,
                    },
                    task_listener,
                )
                .await
        });
        callback_notice.await;
        tokio::time::advance(Duration::from_millis(30_001)).await;
        release_callback.notify_one();
        task.await.unwrap();

        let snapshot = process_queue.snapshot(91).unwrap();
        assert_eq!(snapshot.cached_count, 1);
        assert_eq!(snapshot.in_flight_count, 0);
        assert!(snapshot.next_commit_offset == Some(300));
        assert!(offset_store.updates.lock().unwrap().is_empty());
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn orderly_lock_renewal_uses_java_initial_delay_and_interval() {
        let mut config = PushConsumerConfig::new("orderly-lock-renewal").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_consume_thread_min(1);
        config.set_consume_thread_max(1);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
        let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
                (Some(ConsumeOrderlyStatus::Success), context)
            },
        );
        let settings = OrderlyLockSettings::from_values(30_000, 20_000).unwrap();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store,
            listener,
            Some(settings),
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 92));
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let requested = {
            let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
            scripted.lock_results.extend([
                Ok(Some(HashSet::from([Some(queue.clone())]))),
                Ok(Some(HashSet::new())),
            ]);
            scripted.unlock_results.push_back(Ok(()));
            scripted.lock_requested.clone()
        };

        let first_notice = requested.notified();
        tokio::time::advance(Duration::from_millis(999)).await;
        tokio::task::yield_now().await;
        assert!(
            consumer
                .scripted_lock_transport
                .lock()
                .unwrap()
                .lock_bodies
                .is_empty()
        );
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::time::timeout(Duration::from_secs(1), first_notice)
            .await
            .expect("first Java renewal starts after 1000ms");
        assert!(process_queue.snapshot(92).unwrap().locked);

        let second_notice = requested.notified();
        tokio::time::advance(Duration::from_millis(20_000)).await;
        tokio::time::timeout(Duration::from_secs(1), second_notice)
            .await
            .expect("subsequent Java renewals use the configured interval");
        assert!(!process_queue.snapshot(92).unwrap().locked);
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn orderly_rebalance_bounds_slow_callback_wait_and_never_commits_dropped_work() {
        let mut config = PushConsumerConfig::new("orderly-slow-rebalance").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
        config.set_consume_thread_min(1);
        config.set_consume_thread_max(1);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        consumer
            .scripted_lock_transport
            .lock()
            .unwrap()
            .unlock_results
            .push_back(Ok(()));
        let callback_started = Arc::new(tokio::sync::Notify::new());
        let release_callback = Arc::new(tokio::sync::Notify::new());
        let listener: Arc<dyn MessageListenerOrderly> = {
            let callback_started = callback_started.clone();
            let release_callback = release_callback.clone();
            Arc::new(
                move |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                    let callback_started = callback_started.clone();
                    let release_callback = release_callback.clone();
                    async move {
                        callback_started.notify_one();
                        release_callback.notified().await;
                        (Some(ConsumeOrderlyStatus::Success), context)
                    }
                },
            )
        };
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            listener.clone(),
            None,
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 93));
        process_queue
            .set_lock_state(93, true, epoch_millis())
            .unwrap();
        process_queue
            .insert_messages(93, vec![retry_test_message(400)])
            .unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        offset_store.update_offset(&queue, 400, false);
        let context = consumer.pull_context().await.unwrap();

        let callback_notice = callback_started.notified();
        let task_consumer = consumer.clone();
        let task_listener = listener.clone();
        let task_queue = process_queue.clone();
        let task = tokio::spawn(async move {
            task_consumer
                .run_orderly_consume_request(
                    OrderlyConsumeRequest {
                        process_queue: task_queue,
                        generation: 93,
                    },
                    task_listener,
                )
                .await
        });
        callback_notice.await;
        process_queue.drop_assignment(93).unwrap();
        let removal_started = Instant::now();
        assert!(
            !consumer
                .remove_process_queue_safely(&context, &queue, &process_queue)
                .await
        );
        assert!(Instant::now().duration_since(removal_started) <= Duration::from_millis(500));
        assert!(read_lock(&consumer.process_queues).contains_key(&queue));
        tokio::time::advance(Duration::from_millis(20_001)).await;
        assert!(
            consumer
                .remove_process_queue_safely(&context, &queue, &process_queue)
                .await
        );
        assert_eq!(
            offset_store.persisted.lock().unwrap().get(&queue),
            Some(&400)
        );
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));

        release_callback.notify_one();
        task.await.unwrap();
        assert_eq!(process_queue.snapshot(93).unwrap().in_flight_count, 1);
        assert_eq!(offset_store.updates.lock().unwrap().len(), 1);
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_shutdown_persists_offsets_and_reports_unconfirmed_unlock() {
        let mut config = PushConsumerConfig::new("orderly-unlock-failure").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
                (Some(ConsumeOrderlyStatus::Success), context)
            },
        );
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime =
            install_started_orderly_consumer(&consumer, &manager, offset_store_dyn, listener, None)
                .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 94));
        process_queue
            .set_lock_state(94, true, epoch_millis())
            .unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        offset_store.update_offset(&queue, 500, false);
        let unassigned_queue = MessageQueue::new("stale-orders", "broker-b", 1);
        offset_store.update_offset(&unassigned_queue, 900, false);
        consumer
            .scripted_lock_transport
            .lock()
            .unwrap()
            .unlock_results
            .push_back(Err("scripted unlock timeout".to_owned()));

        assert!(matches!(
            consumer.shutdown().await,
            Err(PushConsumerError::OrderlyUnlockFailed)
        ));
        assert_eq!(
            offset_store.persisted.lock().unwrap().get(&queue),
            Some(&500)
        );
        assert!(
            !offset_store
                .persisted
                .lock()
                .unwrap()
                .contains_key(&unassigned_queue)
        );
        assert_eq!(
            offset_store.persist_calls.lock().unwrap().as_slice(),
            &[vec![queue.clone()]]
        );
        assert!(process_queue.snapshot(94).unwrap().locked);
        assert!(read_lock(&consumer.pending_orderly_unlocks).contains(&queue));
        assert_eq!(
            consumer
                .scripted_lock_transport
                .lock()
                .unwrap()
                .unlock_bodies
                .len(),
            1
        );
        runtime.shutdown().await.unwrap();
    }
}
