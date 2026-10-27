mod config;
mod trace;

pub use config::*;
pub use trace::{
    LiteTraceBean, LiteTraceContext, LiteTraceDispatcher, LiteTraceError, LiteTraceKind,
};

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_core::error::RpcError;
use rocketmq_classic_protocol::{
    SharedValue,
    codes::response,
    heartbeat::{ConsumeFromWhere, ConsumerSubscriptionDataSet},
};
use rocketmq_classic_remoting::client::RpcHook;
use tokio::{
    task::JoinHandle,
    time::{Instant, sleep, timeout_at},
};

use crate::{
    config::PullConsumerConfig,
    consume::{ConsumeMessageContext, ConsumeMessageHook, ConsumeMessageHookRegistry},
    error::{LitePullConsumerError, PullConsumerError},
    pull::{MessageSelector, ProcessedPullResult, PullConsumer, build_subscription},
    push::{
        MessageQueueListener, parse_java_timestamp_millis,
        pull_service::PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL,
    },
};

pub type TopicMessageQueueChangeListener = Arc<dyn Fn(&str, &BTreeSet<MessageQueue>) + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SubscriptionMode {
    None,
    Subscribe,
    Assign,
}

struct QueueState {
    generation: u64,
    fetched_offset: Option<i64>,
    delivered_offset: Option<i64>,
    paused: bool,
}

struct BufferedBatch {
    queue: MessageQueue,
    generation: u64,
    messages: Vec<rocketmq_classic_common::message::MessageExt>,
    body_bytes: usize,
}

#[derive(Debug, PartialEq, Eq)]
enum LiteInitialOffsetAction {
    Value(i64),
    MaxOffset,
    SearchTimestamp(i64),
}

fn lite_pull_error_delay_millis(
    error: &LitePullConsumerError,
    configured_delay_millis: i64,
) -> u64 {
    if matches!(
        error,
        LitePullConsumerError::Pull(PullConsumerError::Rpc(RpcError::Response {
            code: response::FLOW_CONTROL,
            ..
        }))
    ) {
        PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL.max(0) as u64
    } else {
        configured_delay_millis.max(0) as u64
    }
}

fn lite_initial_offset_action(
    policy: ConsumeFromWhere,
    topic: &str,
    stored_offset: i64,
    consume_timestamp: &str,
) -> Result<LiteInitialOffsetAction, LitePullConsumerError> {
    if stored_offset >= 0 {
        return Ok(LiteInitialOffsetAction::Value(stored_offset));
    }
    if stored_offset < -1 {
        return Ok(LiteInitialOffsetAction::Value(-1));
    }

    let retry_topic = topic.starts_with("%RETRY%");
    match policy {
        ConsumeFromWhere::ConsumeFromLastOffset => {
            if retry_topic {
                Ok(LiteInitialOffsetAction::Value(0))
            } else {
                Ok(LiteInitialOffsetAction::MaxOffset)
            }
        }
        ConsumeFromWhere::ConsumeFromFirstOffset => Ok(LiteInitialOffsetAction::Value(0)),
        ConsumeFromWhere::ConsumeFromTimestamp if retry_topic => {
            Ok(LiteInitialOffsetAction::MaxOffset)
        }
        ConsumeFromWhere::ConsumeFromTimestamp => {
            let timestamp = parse_java_timestamp_millis(consume_timestamp)
                .ok_or(LitePullConsumerError::InvalidConsumeTimestamp)?;
            Ok(LiteInitialOffsetAction::SearchTimestamp(timestamp))
        }
        _ => Ok(LiteInitialOffsetAction::Value(-1)),
    }
}

#[derive(Default)]
struct MessageBuffer {
    batches: VecDeque<BufferedBatch>,
    messages: usize,
    body_bytes: usize,
}

/// Lite Pull consumer with Java-compatible subscribe/assign exclusivity,
/// queue-scoped prefetch, and separate fetched/delivered/committed positions.
pub struct LitePullConsumer {
    config: LitePullConsumerConfig,
    consumer_group: RwLock<Option<String>>,
    consume_from_where: RwLock<ConsumeFromWhere>,
    consume_timestamp: RwLock<String>,
    pull_time_delay_millis_when_exception: i64,
    rpc_hook: Option<Arc<dyn RpcHook>>,
    consume_message_hooks: ConsumeMessageHookRegistry,
    engine: RwLock<Option<Arc<PullConsumer>>>,
    lifecycle: tokio::sync::Mutex<Lifecycle>,
    lifecycle_gate: tokio::sync::Mutex<()>,
    start_invoked: AtomicBool,
    running: AtomicBool,
    auto_commit: AtomicBool,
    pull_thread_nums: AtomicI32,
    auto_commit_interval_millis: AtomicI64,
    pull_batch_size: AtomicI32,
    pull_threshold_for_all: AtomicI64,
    consume_max_span: AtomicI32,
    pull_threshold_for_queue: AtomicI32,
    pull_threshold_size_for_queue: AtomicI32,
    poll_timeout_millis: AtomicI64,
    consumer_pull_timeout_millis: AtomicI64,
    consumer_timeout_millis_when_suspend: AtomicI64,
    topic_metadata_check_interval_millis: AtomicI64,
    connect_broker_by_user: AtomicBool,
    default_broker_id: AtomicI64,
    mode: RwLock<SubscriptionMode>,
    subscriptions: RwLock<BTreeMap<String, MessageSelector>>,
    assigned: RwLock<BTreeSet<MessageQueue>>,
    assigned_public: RwLock<BTreeSet<MessageQueue>>,
    assign_expressions: RwLock<HashMap<String, String>>,
    queue_states: Mutex<HashMap<MessageQueue, QueueState>>,
    workers: Mutex<HashMap<MessageQueue, JoinHandle<()>>>,
    pull_permits: Arc<tokio::sync::Semaphore>,
    monitor: Mutex<Option<JoinHandle<()>>>,
    buffer: Mutex<MessageBuffer>,
    ready: tokio::sync::Notify,
    reconcile: tokio::sync::Notify,
    reconcile_gate: tokio::sync::Mutex<()>,
    next_generation: AtomicU64,
    next_auto_commit: Mutex<Option<Instant>>,
    heartbeat_subscriptions: Arc<RwLock<ConsumerSubscriptionDataSet>>,
    queue_change_listeners: RwLock<HashMap<String, TopicMessageQueueChangeListener>>,
    last_topic_queues: Mutex<HashMap<String, BTreeSet<MessageQueue>>>,
    message_queue_listener: RwLock<Option<MessageQueueListener>>,
    trace_dispatcher: RwLock<Option<Arc<LiteTraceDispatcher>>>,
}

enum Lifecycle {
    Created,
    Starting,
    Started,
    StartFailed,
    Shutdown,
}

impl LitePullConsumer {
    pub fn new(config: LitePullConsumerConfig) -> Result<Self, LitePullConsumerError> {
        Self::new_with_rpc_hook(config, None)
    }

    /// Creates a Lite consumer whose remoting requests use the supplied Java-compatible RPC hook.
    pub fn new_with_rpc_hook(
        config: LitePullConsumerConfig,
        rpc_hook: Option<Arc<dyn RpcHook>>,
    ) -> Result<Self, LitePullConsumerError> {
        let consumer_group = config.get_consumer_group();
        let consume_from_where = config.consume_from_where();
        let consume_timestamp = config.consume_timestamp().to_owned();
        let pull_time_delay_millis_when_exception = config
            .client_config()
            .pull_time_delay_millis_when_exception();
        let auto_commit = config.auto_commit();
        let pull_thread_nums = config.pull_thread_nums();
        let auto_commit_interval_millis = config.auto_commit_interval_millis();
        let pull_batch_size = config.pull_batch_size();
        let pull_threshold_for_all = config.pull_threshold_for_all();
        let consume_max_span = config.consume_max_span();
        let pull_threshold_for_queue = config.pull_threshold_for_queue();
        let pull_threshold_size_for_queue = config.pull_threshold_size_for_queue();
        let poll_timeout_millis = config.poll_timeout_millis();
        let consumer_pull_timeout_millis = config.consumer_pull_timeout_millis();
        let consumer_timeout_millis_when_suspend = config.consumer_timeout_millis_when_suspend();
        let topic_metadata_check_interval_millis = config.topic_metadata_check_interval_millis();
        let connect_broker_by_user = config.connect_broker_by_user();
        let default_broker_id = config.default_broker_id();
        let message_queue_listener = config.message_queue_listener();
        // Pull workers are created only after start, so settings changed after
        // construction but before start can size the semaphore there.
        let pull_permits = Arc::new(tokio::sync::Semaphore::new(0));
        Ok(Self {
            config,
            consumer_group: RwLock::new(consumer_group),
            consume_from_where: RwLock::new(consume_from_where),
            consume_timestamp: RwLock::new(consume_timestamp),
            pull_time_delay_millis_when_exception,
            rpc_hook,
            consume_message_hooks: ConsumeMessageHookRegistry::default(),
            engine: RwLock::new(None),
            lifecycle: tokio::sync::Mutex::new(Lifecycle::Created),
            lifecycle_gate: tokio::sync::Mutex::new(()),
            start_invoked: AtomicBool::new(false),
            running: AtomicBool::new(false),
            auto_commit: AtomicBool::new(auto_commit),
            pull_thread_nums: AtomicI32::new(pull_thread_nums),
            auto_commit_interval_millis: AtomicI64::new(auto_commit_interval_millis),
            pull_batch_size: AtomicI32::new(pull_batch_size),
            pull_threshold_for_all: AtomicI64::new(pull_threshold_for_all),
            consume_max_span: AtomicI32::new(consume_max_span),
            pull_threshold_for_queue: AtomicI32::new(pull_threshold_for_queue),
            pull_threshold_size_for_queue: AtomicI32::new(pull_threshold_size_for_queue),
            poll_timeout_millis: AtomicI64::new(poll_timeout_millis),
            consumer_pull_timeout_millis: AtomicI64::new(consumer_pull_timeout_millis),
            consumer_timeout_millis_when_suspend: AtomicI64::new(
                consumer_timeout_millis_when_suspend,
            ),
            topic_metadata_check_interval_millis: AtomicI64::new(
                topic_metadata_check_interval_millis,
            ),
            connect_broker_by_user: AtomicBool::new(connect_broker_by_user),
            default_broker_id: AtomicI64::new(default_broker_id),
            mode: RwLock::new(SubscriptionMode::None),
            subscriptions: RwLock::new(BTreeMap::new()),
            assigned: RwLock::new(BTreeSet::new()),
            assigned_public: RwLock::new(BTreeSet::new()),
            assign_expressions: RwLock::new(HashMap::new()),
            queue_states: Mutex::new(HashMap::new()),
            workers: Mutex::new(HashMap::new()),
            pull_permits,
            monitor: Mutex::new(None),
            buffer: Mutex::new(MessageBuffer::default()),
            ready: tokio::sync::Notify::new(),
            reconcile: tokio::sync::Notify::new(),
            reconcile_gate: tokio::sync::Mutex::new(()),
            next_generation: AtomicU64::new(1),
            next_auto_commit: Mutex::new(None),
            heartbeat_subscriptions: Arc::new(RwLock::new(ConsumerSubscriptionDataSet::new())),
            queue_change_listeners: RwLock::new(HashMap::new()),
            last_topic_queues: Mutex::new(HashMap::new()),
            message_queue_listener: RwLock::new(message_queue_listener),
            trace_dispatcher: RwLock::new(None),
        })
    }

    /// Returns the construction-time configuration snapshot. Settings with
    /// Java-compatible live setters are read from the corresponding consumer
    /// accessors after construction.
    pub fn config(&self) -> &LitePullConsumerConfig {
        &self.config
    }

    pub fn get_consumer_group(&self) -> Option<String> {
        self.consumer_group
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn set_consumer_group(&self, value: impl Into<String>) {
        self.set_consumer_group_nullable(Some(value.into()));
    }

    pub fn set_consumer_group_nullable(&self, value: Option<String>) {
        *self
            .consumer_group
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = value;
    }

    pub fn consume_from_where(&self) -> ConsumeFromWhere {
        *self
            .consume_from_where
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn set_consume_from_where(
        &self,
        value: ConsumeFromWhere,
    ) -> Result<(), LitePullConsumerError> {
        if !matches!(
            value,
            ConsumeFromWhere::ConsumeFromFirstOffset
                | ConsumeFromWhere::ConsumeFromLastOffset
                | ConsumeFromWhere::ConsumeFromTimestamp
        ) {
            return Err(LitePullConsumerError::InvalidConsumeFromWhere(value));
        }
        *self
            .consume_from_where
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = value;
        Ok(())
    }

    pub fn consume_timestamp(&self) -> String {
        self.consume_timestamp
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn set_consume_timestamp(&self, value: impl Into<String>) {
        *self
            .consume_timestamp
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = value.into();
    }

    /// Installs an internal hook around each non-empty poll result.
    pub(crate) fn register_consume_message_hook(&self, hook: Arc<dyn ConsumeMessageHook>) {
        self.consume_message_hooks.register(hook);
    }

    pub fn is_auto_commit(&self) -> bool {
        self.auto_commit.load(Ordering::Acquire)
    }

    pub fn pull_thread_nums(&self) -> i32 {
        self.pull_thread_nums.load(Ordering::Acquire)
    }

    pub fn set_pull_thread_nums(&self, value: i32) {
        self.pull_thread_nums.store(value, Ordering::Release);
    }

    pub fn auto_commit_interval_millis(&self) -> i64 {
        self.auto_commit_interval_millis.load(Ordering::Acquire)
    }

    pub fn set_auto_commit_interval_millis(&self, value: i64) {
        if value >= 1_000 {
            self.auto_commit_interval_millis
                .store(value, Ordering::Release);
        }
    }

    pub fn pull_batch_size(&self) -> i32 {
        self.pull_batch_size.load(Ordering::Acquire)
    }

    pub fn set_pull_batch_size(&self, value: i32) {
        self.pull_batch_size.store(value, Ordering::Release);
    }

    pub fn pull_threshold_for_all(&self) -> i64 {
        self.pull_threshold_for_all.load(Ordering::Acquire)
    }

    pub fn set_pull_threshold_for_all(&self, value: i64) {
        self.pull_threshold_for_all.store(value, Ordering::Release);
    }

    pub fn consume_max_span(&self) -> i32 {
        self.consume_max_span.load(Ordering::Acquire)
    }

    pub fn set_consume_max_span(&self, value: i32) {
        self.consume_max_span.store(value, Ordering::Release);
    }

    pub fn pull_threshold_for_queue(&self) -> i32 {
        self.pull_threshold_for_queue.load(Ordering::Acquire)
    }

    pub fn set_pull_threshold_for_queue(&self, value: i32) {
        self.pull_threshold_for_queue
            .store(value, Ordering::Release);
    }

    pub fn pull_threshold_size_for_queue(&self) -> i32 {
        self.pull_threshold_size_for_queue.load(Ordering::Acquire)
    }

    pub fn set_pull_threshold_size_for_queue(&self, value: i32) {
        self.pull_threshold_size_for_queue
            .store(value, Ordering::Release);
    }

    pub fn poll_timeout_millis(&self) -> i64 {
        self.poll_timeout_millis.load(Ordering::Acquire)
    }

    pub fn set_poll_timeout_millis(&self, value: i64) {
        self.poll_timeout_millis.store(value, Ordering::Release);
    }

    pub fn consumer_pull_timeout_millis(&self) -> i64 {
        self.consumer_pull_timeout_millis.load(Ordering::Acquire)
    }

    pub fn set_consumer_pull_timeout_millis(&self, value: i64) {
        self.consumer_pull_timeout_millis
            .store(value, Ordering::Release);
    }

    pub fn consumer_timeout_millis_when_suspend(&self) -> i64 {
        self.consumer_timeout_millis_when_suspend
            .load(Ordering::Acquire)
    }

    pub fn set_consumer_timeout_millis_when_suspend(&self, value: i64) {
        self.consumer_timeout_millis_when_suspend
            .store(value, Ordering::Release);
    }

    pub fn topic_metadata_check_interval_millis(&self) -> i64 {
        self.topic_metadata_check_interval_millis
            .load(Ordering::Acquire)
    }

    pub fn set_topic_metadata_check_interval_millis(&self, value: i64) {
        self.topic_metadata_check_interval_millis
            .store(value, Ordering::Release);
    }

    pub fn message_queue_listener(&self) -> Option<MessageQueueListener> {
        self.message_queue_listener
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn set_message_queue_listener(&self, listener: Option<MessageQueueListener>) {
        *self
            .message_queue_listener
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = listener.clone();
        if let Some(engine) = self.engine() {
            engine.set_message_queue_listener(listener);
        }
    }

    pub async fn is_running(&self) -> bool {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        self.running.load(Ordering::Acquire)
    }

    /// Returns the consume trace dispatcher after tracing has been enabled at start.
    pub fn trace_dispatcher(&self) -> Option<Arc<LiteTraceDispatcher>> {
        self.trace_dispatcher
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn normalized_topic(&self, topic: &str) -> Result<String, LitePullConsumerError> {
        self.config
            .client_config()
            .clone()
            .with_namespace(Some(topic))?
            .ok_or(LitePullConsumerError::Pull(
                PullConsumerError::InvalidSubscription("topic is empty".into()),
            ))
    }

    fn normalized_queue(
        &self,
        queue: &MessageQueue,
    ) -> Result<MessageQueue, LitePullConsumerError> {
        let mut client_config = self.config.client_config().clone();
        Ok(client_config.queue_with_namespace(queue)?)
    }

    fn select_mode(&self, mode: SubscriptionMode) -> Result<(), LitePullConsumerError> {
        let mut current = self
            .mode
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *current == SubscriptionMode::None {
            *current = mode;
        } else if *current != mode {
            return Err(LitePullConsumerError::SubscriptionModeConflict);
        }
        Ok(())
    }

    pub async fn subscribe(&self, topic: &str) -> Result<(), LitePullConsumerError> {
        self.subscribe_with_selector(topic, &MessageSelector::by_tag("*"))
            .await
    }

    pub async fn subscribe_with_expression(
        &self,
        topic: &str,
        expression: &str,
    ) -> Result<(), LitePullConsumerError> {
        self.subscribe_with_selector(topic, &MessageSelector::by_tag(expression))
            .await
    }

    pub async fn subscribe_with_selector(
        &self,
        topic: &str,
        selector: &MessageSelector,
    ) -> Result<(), LitePullConsumerError> {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        self.subscribe_with_selector_locked(topic, selector, None)
            .await
    }

    pub async fn subscribe_with_listener(
        &self,
        topic: &str,
        selector: &MessageSelector,
        listener: MessageQueueListener,
    ) -> Result<(), LitePullConsumerError> {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        self.subscribe_with_selector_locked(topic, selector, Some(listener))
            .await
    }

    async fn subscribe_with_selector_locked(
        &self,
        topic: &str,
        selector: &MessageSelector,
        listener: Option<MessageQueueListener>,
    ) -> Result<(), LitePullConsumerError> {
        if topic.is_empty() {
            return Err(PullConsumerError::InvalidSubscription(
                "Topic can not be null or empty.".into(),
            )
            .into());
        }
        self.select_mode(SubscriptionMode::Subscribe)?;
        let normalized_topic = self.normalized_topic(topic)?;
        build_subscription(
            &normalized_topic,
            Some(selector.expression()),
            Some(selector.expression_type()),
        )
        .map_err(|error| PullConsumerError::InvalidSubscription(error.to_string()))?;
        self.subscriptions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(normalized_topic.clone(), selector.clone());

        if let Some(engine) = self.engine() {
            engine.register_topic(&normalized_topic)?;
            engine.add_register_subscriptions(&normalized_topic, Some(selector))?;
        }
        if let Some(listener) = listener {
            *self
                .message_queue_listener
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(listener.clone());
            if let Some(engine) = self.engine() {
                engine.set_message_queue_listener(Some(listener));
            }
        }
        if self.running.load(Ordering::Acquire) {
            if let Some(engine) = self.engine() {
                if let Some(runtime) = engine.get_client_runtime().await {
                    // Java's sendHeartbeatToAllBrokerWithLock() logs and
                    // suppresses errors and lock contention. Its V1 fallback
                    // is serial even when concurrent heartbeat is configured.
                    let _ = runtime
                        .send_heartbeat_to_all_brokers_with_rebalance(false)
                        .await;
                }
                let subscribed_topics = self
                    .subscriptions
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>();
                for topic in subscribed_topics {
                    match engine.refresh_topic_route(&topic).await {
                        Ok(())
                        | Err(PullConsumerError::Runtime(
                            rocketmq_classic_core::runtime::RuntimeError::Rpc(
                                rocketmq_classic_core::error::RpcError::Response { .. },
                            ),
                        )) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
            }
        }
        self.reconcile.notify_one();
        Ok(())
    }

    pub async fn unsubscribe(&self, topic: &str) -> Result<bool, LitePullConsumerError> {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        let _reconcile_gate = self.reconcile_gate.lock().await;
        let normalized_topic = self.normalized_topic(topic)?;
        let removed = self
            .subscriptions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&normalized_topic)
            .is_some();
        let removed_queues = {
            let mut states = self
                .queue_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let queues = states
                .keys()
                .filter(|queue| queue.topic() == Some(normalized_topic.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            for queue in &queues {
                states.remove(queue);
            }
            queues
        };
        let mut removed_workers = Vec::new();
        {
            let mut workers = self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for queue in &removed_queues {
                if let Some(worker) = workers.remove(queue) {
                    worker.abort();
                    removed_workers.push(worker);
                }
            }
        }
        for queue in &removed_queues {
            self.remove_queue_batches(queue);
        }
        for worker in removed_workers {
            let _ = worker.await;
        }
        self.last_topic_queues
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&normalized_topic);
        if let Some(engine) = self.engine() {
            let _ = engine.unregister_topic(&normalized_topic)?;
        }
        self.reconcile.notify_one();
        Ok(removed)
    }

    pub async fn assign<I>(&self, message_queues: I) -> Result<(), LitePullConsumerError>
    where
        I: IntoIterator<Item = MessageQueue>,
    {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        let public_queues = message_queues.into_iter().collect::<BTreeSet<_>>();
        let queues = public_queues
            .iter()
            .map(|queue| self.normalized_queue(queue))
            .collect::<Result<BTreeSet<_>, _>>()?;
        if queues.is_empty() {
            return Err(LitePullConsumerError::EmptyAssignment);
        }
        self.select_mode(SubscriptionMode::Assign)?;
        *self
            .assigned
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = queues;
        *self
            .assigned_public
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = public_queues;
        self.reconcile.notify_one();
        Ok(())
    }

    pub async fn set_sub_expression_for_assign(
        &self,
        topic: &str,
        expression: &str,
    ) -> Result<(), LitePullConsumerError> {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        if expression.trim().is_empty() {
            return Err(LitePullConsumerError::EmptyAssignmentExpression);
        }
        if self.start_invoked.load(Ordering::Acquire) {
            return Err(LitePullConsumerError::AssignmentExpressionAfterStartup);
        }
        self.select_mode(SubscriptionMode::Assign)?;
        let topic = self.normalized_topic(topic)?;
        self.assign_expressions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(topic, expression.to_owned());
        Ok(())
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), LitePullConsumerError> {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        self.start_invoked.store(true, Ordering::Release);
        {
            let mut state = self.lifecycle.lock().await;
            match &*state {
                Lifecycle::Started | Lifecycle::Starting => {
                    return Err(LitePullConsumerError::AlreadyStarted);
                }
                Lifecycle::StartFailed => return Err(LitePullConsumerError::StartFailed),
                Lifecycle::Shutdown => return Err(LitePullConsumerError::Shutdown),
                Lifecycle::Created => *state = Lifecycle::Starting,
            }
        }
        let raw_consumer_group = match self.get_consumer_group() {
            Some(group) => group,
            None => {
                *self.lifecycle.lock().await = Lifecycle::StartFailed;
                return Err(PullConsumerError::InvalidConsumerGroup.into());
            }
        };
        let effective_group = match self
            .config
            .client_config()
            .clone()
            .with_namespace(Some(&raw_consumer_group))
        {
            Ok(Some(group)) => group,
            Ok(None) => {
                *self.lifecycle.lock().await = Lifecycle::StartFailed;
                return Err(PullConsumerError::InvalidConsumerGroup.into());
            }
            Err(error) => {
                *self.lifecycle.lock().await = Lifecycle::StartFailed;
                return Err(error.into());
            }
        };
        let group_validation = crate::config::validate_group(&effective_group).and_then(|()| {
            if effective_group == crate::config::DEFAULT_CONSUMER_GROUP {
                Err(PullConsumerError::DefaultConsumerGroup)
            } else {
                Ok(())
            }
        });
        if let Err(error) = group_validation {
            *self.lifecycle.lock().await = Lifecycle::StartFailed;
            return Err(error.into());
        }
        let consumer_timeout_millis = self
            .consumer_timeout_millis_when_suspend
            .load(Ordering::Acquire);
        let broker_suspend_millis = self.config.broker_suspend_max_time_millis();
        if consumer_timeout_millis < broker_suspend_millis {
            *self.lifecycle.lock().await = Lifecycle::StartFailed;
            return Err(PullConsumerError::InvalidSuspendTimeout {
                consumer_timeout_millis,
                broker_suspend_millis,
            }
            .into());
        }
        let pull_thread_nums = self.pull_thread_nums.load(Ordering::Acquire);
        if pull_thread_nums < 0 {
            *self.lifecycle.lock().await = Lifecycle::StartFailed;
            return Err(LitePullConsumerError::InvalidPullThreadCount);
        }
        self.pull_permits
            .add_permits(usize::try_from(pull_thread_nums.max(1)).unwrap_or(1));
        let trace_dispatcher = if self.config.enable_msg_trace() {
            match LiteTraceDispatcher::new(
                self.config.client_config(),
                &raw_consumer_group,
                self.config.customized_trace_topic(),
                self.rpc_hook.clone(),
            ) {
                Ok(dispatcher) => {
                    let dispatcher = Arc::new(dispatcher);
                    *self
                        .trace_dispatcher
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        Some(dispatcher.clone());
                    Some(dispatcher)
                }
                Err(error) => {
                    tracing::error!(error = %error, "Lite consume trace dispatcher initialization failed");
                    None
                }
            }
        } else {
            None
        };
        if let Some(dispatcher) = trace_dispatcher.as_ref() {
            self.register_consume_message_hook(Arc::new(trace::LiteConsumeTraceHook::new(
                dispatcher.clone(),
                &raw_consumer_group,
                self.config.client_config().clone(),
            )));
        }
        let engine = match self.prepare_engine().await {
            Ok(engine) => engine,
            Err(error) => {
                self.record_start_failure(&error).await;
                return Err(error);
            }
        };
        if let Err(error) = engine.start().await {
            let error = LitePullConsumerError::Pull(error);
            self.record_start_failure(&error).await;
            return Err(error);
        }
        // Java registers LitePull's metadata task immediately after the client
        // runtime starts, before the remaining post-start route and Broker
        // checks. Keep its initial delay anchored to that point even though
        // this Rust task is spawned after post-start initialization.
        let metadata_schedule_started_at = Instant::now();
        let topic_metadata_check_interval_millis = self
            .topic_metadata_check_interval_millis
            .load(Ordering::Acquire);
        if topic_metadata_check_interval_millis <= 0 {
            *self
                .engine
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(engine);
            *self.lifecycle.lock().await = Lifecycle::StartFailed;
            return Err(LitePullConsumerError::InvalidTopicMetadataCheckInterval);
        }
        let metadata_period = Duration::from_millis(topic_metadata_check_interval_millis as u64);
        {
            let mut lifecycle = self.lifecycle.lock().await;
            if !matches!(*lifecycle, Lifecycle::Starting) {
                drop(lifecycle);
                let _ = engine.shutdown().await;
                return Err(LitePullConsumerError::Shutdown);
            }
            *self
                .engine
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(engine.clone());
            self.running.store(true, Ordering::Release);
            *lifecycle = Lifecycle::Started;
        }
        if *self
            .mode
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            == SubscriptionMode::Subscribe
        {
            let subscription_topics = self
                .subscriptions
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            for topic in subscription_topics {
                match engine.refresh_topic_route(&topic).await {
                    Ok(())
                    | Err(PullConsumerError::Runtime(
                        rocketmq_classic_core::runtime::RuntimeError::Rpc(
                            rocketmq_classic_core::error::RpcError::Response { .. },
                        ),
                    )) => {}
                    Err(error) => {
                        self.shutdown_after_start_failure(&engine).await;
                        return Err(error.into());
                    }
                }
            }
        }
        let listener_topics = self
            .queue_change_listeners
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for topic in listener_topics {
            match engine.fetch_message_queues(&topic).await {
                Ok(queues) => {
                    self.last_topic_queues
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(topic, queues);
                }
                Err(error) => {
                    self.shutdown_after_start_failure(&engine).await;
                    return Err(error.into());
                }
            }
        }
        if let Err(error) = engine.check_client_in_broker().await {
            self.shutdown_after_start_failure(&engine).await;
            return Err(error.into());
        }
        self.sync_workers().await;
        if let Some(dispatcher) = trace_dispatcher {
            let name_server_address = self.config.client_config().get_namesrv_addr();
            let access_channel = self
                .config
                .client_config()
                .access_channel()
                .unwrap_or_default();
            if let Err(error) = dispatcher.start(name_server_address, access_channel).await {
                tracing::warn!(error = %error, "Lite consume trace dispatcher start failed");
            }
        }
        let consumer = self.clone();
        let task = tokio::spawn(async move {
            let mut next_metadata_refresh = metadata_schedule_started_at + Duration::from_secs(10);
            loop {
                if !consumer.running.load(Ordering::Acquire) {
                    break;
                }
                consumer.sync_workers().await;
                if Instant::now() >= next_metadata_refresh {
                    let _lifecycle_gate = consumer.lifecycle_gate.lock().await;
                    if consumer.running.load(Ordering::Acquire) {
                        if let Some(engine) = consumer.engine() {
                            let topics = consumer
                                .queue_change_listeners
                                .read()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .keys()
                                .cloned()
                                .collect::<Vec<_>>();
                            for topic in topics {
                                consumer.update_topic_queue_change(&engine, &topic).await;
                            }
                        }
                    }
                    next_metadata_refresh += metadata_period;
                }
                tokio::select! {
                    _ = consumer.reconcile.notified() => {}
                    _ = sleep(Duration::from_millis(200)) => {}
                }
            }
        });
        *self
            .monitor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(task);
        Ok(())
    }

    async fn shutdown_after_start_failure(&self, engine: &PullConsumer) {
        self.running.store(false, Ordering::Release);
        self.ready.notify_waiters();
        self.reconcile.notify_waiters();
        let workers = {
            let mut workers = self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *workers)
        };
        for (_, worker) in workers {
            worker.abort();
            let _ = worker.await;
        }
        self.queue_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        *self.lifecycle.lock().await = Lifecycle::Shutdown;
        let _ = engine.shutdown().await;
    }

    async fn record_start_failure(&self, error: &LitePullConsumerError) {
        let duplicate_group = matches!(
            error,
            LitePullConsumerError::Pull(PullConsumerError::Runtime(
                rocketmq_classic_core::runtime::RuntimeError::DuplicateGroup { .. }
            ))
        );
        if duplicate_group {
            self.start_invoked.store(false, Ordering::Release);
        }
        *self.lifecycle.lock().await = if duplicate_group {
            Lifecycle::Created
        } else {
            Lifecycle::StartFailed
        };
    }

    async fn prepare_engine(&self) -> Result<Arc<PullConsumer>, LitePullConsumerError> {
        let consumer_group = self
            .get_consumer_group()
            .ok_or(PullConsumerError::InvalidConsumerGroup)?;
        let mut pull_config = PullConsumerConfig::new(consumer_group)?;
        *pull_config.client_config_mut() = self.config.client_config().clone();
        pull_config.set_tls_config(self.config.tls_config().cloned());
        pull_config
            .set_broker_suspend_max_time_millis(self.config.broker_suspend_max_time_millis());
        pull_config.set_consumer_timeout_millis_when_suspend(
            self.consumer_timeout_millis_when_suspend
                .load(Ordering::Acquire),
        );
        pull_config.set_consumer_pull_timeout_millis(
            self.consumer_pull_timeout_millis.load(Ordering::Acquire),
        );
        pull_config.set_message_model(self.config.message_model());
        pull_config.set_consume_from_where(self.config.consume_from_where());
        pull_config.set_unit_mode(self.config.unit_mode());
        pull_config
            .set_allocate_message_queue_strategy(self.config.allocate_message_queue_strategy());
        pull_config.set_message_queue_listener(
            self.message_queue_listener
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        );
        let engine = Arc::new(PullConsumer::new_with_rpc_hook(
            pull_config,
            self.rpc_hook.clone(),
        ));
        engine.configure_pull_api(
            self.connect_broker_by_user.load(Ordering::Acquire),
            self.default_broker_id.load(Ordering::Acquire),
        );
        engine.set_additional_heartbeat_subscriptions(self.heartbeat_subscriptions.clone());
        if let Some(store) = self.config.offset_store() {
            engine.set_offset_store(Some(store)).await?;
        }
        if *self
            .mode
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            == SubscriptionMode::Subscribe
        {
            for (topic, selector) in self
                .subscriptions
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
            {
                engine.register_topic(topic)?;
                engine.add_register_subscriptions(topic, Some(selector))?;
            }
        }
        Ok(engine)
    }

    pub async fn shutdown(&self) -> Result<(), LitePullConsumerError> {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        self.running.store(false, Ordering::Release);
        self.ready.notify_waiters();
        self.reconcile.notify_waiters();
        {
            let mut state = self.lifecycle.lock().await;
            if matches!(
                &*state,
                Lifecycle::Created | Lifecycle::StartFailed | Lifecycle::Shutdown
            ) {
                return Ok(());
            }
            *state = Lifecycle::Shutdown;
        }

        let monitor = self
            .monitor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(task) = monitor {
            task.abort();
            let _ = task.await;
        }
        let workers = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain()
            .map(|(_, worker)| worker)
            .collect::<Vec<_>>();
        for worker in &workers {
            worker.abort();
        }
        for worker in workers {
            let _ = worker.await;
        }
        if let Some(engine) = self.engine() {
            engine.shutdown().await?;
        }
        if let Some(dispatcher) = self.trace_dispatcher() {
            if let Err(error) = dispatcher.shutdown().await {
                tracing::warn!(error = %error, "Lite consume trace dispatcher shutdown failed");
            }
        }
        Ok(())
    }

    pub async fn poll(
        &self,
        timeout_millis: Option<i64>,
    ) -> Result<Vec<rocketmq_classic_common::message::MessageExt>, LitePullConsumerError> {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        if !self.running.load(Ordering::Acquire) {
            return Err(LitePullConsumerError::NotStarted);
        }
        let timeout_millis =
            timeout_millis.unwrap_or_else(|| self.poll_timeout_millis.load(Ordering::Acquire));
        if timeout_millis < 0 {
            return Err(LitePullConsumerError::NegativePollTimeout);
        }
        self.maybe_auto_commit().await;
        let deadline = Instant::now() + Duration::from_millis(timeout_millis as u64);

        loop {
            let notified = self.ready.notified();
            if let Some((queue, batch)) = self.take_ready_batch_with_queue() {
                if self.consume_message_hooks.is_empty() {
                    return Ok(batch);
                }
                let mut client_config = self.config.client_config().clone();
                let namespace = client_config.get_namespace()?.map(str::to_owned);
                let consumer_group = match self.get_consumer_group() {
                    Some(raw_consumer_group) => {
                        client_config.with_namespace(Some(&raw_consumer_group))?
                    }
                    None => None,
                };
                let mut consume_context =
                    ConsumeMessageContext::new(consumer_group, Some(batch), queue, namespace);
                self.consume_message_hooks
                    .execute_before(&mut consume_context);
                consume_context.status = Some("CONSUME_SUCCESS".to_owned());
                consume_context.success = true;
                consume_context.access_channel = client_config.access_channel();
                self.consume_message_hooks
                    .execute_after(&mut consume_context);
                return Ok(consume_context.messages.unwrap_or_default());
            }
            if timeout_millis == 0 {
                return Ok(Vec::new());
            }
            if timeout_at(deadline, notified).await.is_err() {
                return Ok(Vec::new());
            }
        }
    }

    pub async fn assignment(&self) -> Result<BTreeSet<MessageQueue>, LitePullConsumerError> {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        let mode = *self
            .mode
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if mode == SubscriptionMode::Assign {
            return Ok(self
                .assigned_public
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone());
        }
        Ok(self
            .queue_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect())
    }

    pub async fn fetch_message_queues(
        &self,
        topic: &str,
    ) -> Result<BTreeSet<MessageQueue>, LitePullConsumerError> {
        let engine = self.engine().ok_or(LitePullConsumerError::NotStarted)?;
        Ok(engine.fetch_message_queues(topic).await?)
    }

    pub async fn offset_for_timestamp(
        &self,
        queue: &MessageQueue,
        timestamp_millis: i64,
    ) -> Result<i64, LitePullConsumerError> {
        let engine = self.engine().ok_or(LitePullConsumerError::NotStarted)?;
        Ok(engine.search_offset(queue, timestamp_millis).await?)
    }

    pub async fn committed(&self, queue: &MessageQueue) -> Result<i64, LitePullConsumerError> {
        let engine = self.engine().ok_or(LitePullConsumerError::NotStarted)?;
        Ok(engine.fetch_consume_offset(queue, false).await?)
    }

    pub async fn seek(
        self: &Arc<Self>,
        queue: &MessageQueue,
        offset: i64,
    ) -> Result<(), LitePullConsumerError> {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        let queue = self.normalized_queue(queue)?;
        self.require_assigned(&queue)?;
        let engine = self.engine().ok_or(LitePullConsumerError::NotStarted)?;
        let minimum = engine.get_min_offset(&queue).await?;
        let maximum = engine.get_max_offset(&queue).await?;
        if offset < minimum || offset > maximum {
            return Err(LitePullConsumerError::InvalidSeekOffset {
                offset,
                minimum,
                maximum,
            });
        }
        let _reconcile = self.reconcile_gate.lock().await;

        let generation = self
            .next_generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| LitePullConsumerError::GenerationExhausted)?;
        {
            let mut states = self
                .queue_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(state) = states.get_mut(&queue) else {
                return Err(LitePullConsumerError::QueueNotAssigned);
            };
            state.generation = generation;
            state.fetched_offset = Some(offset);
            state.delivered_offset = Some(offset);
        }
        self.remove_queue_batches(&queue);
        self.abort_queue_worker(&queue).await;
        self.spawn_queue_worker(queue, generation);
        self.ready.notify_waiters();
        Ok(())
    }

    pub async fn seek_to_begin(
        self: &Arc<Self>,
        queue: &MessageQueue,
    ) -> Result<(), LitePullConsumerError> {
        let engine = self.engine().ok_or(LitePullConsumerError::NotStarted)?;
        let offset = engine.get_min_offset(queue).await?;
        self.seek(queue, offset).await
    }

    pub async fn seek_to_end(
        self: &Arc<Self>,
        queue: &MessageQueue,
    ) -> Result<(), LitePullConsumerError> {
        let engine = self.engine().ok_or(LitePullConsumerError::NotStarted)?;
        let offset = engine.get_max_offset(queue).await?;
        self.seek(queue, offset).await
    }

    pub fn pause<I>(&self, message_queues: I)
    where
        I: IntoIterator<Item = MessageQueue>,
    {
        let mut states = self
            .queue_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for queue in message_queues
            .into_iter()
            .filter_map(|queue| self.normalized_queue(&queue).ok())
        {
            if let Some(state) = states.get_mut(&queue) {
                state.paused = true;
            }
        }
    }

    pub fn resume<I>(&self, message_queues: I)
    where
        I: IntoIterator<Item = MessageQueue>,
    {
        let mut states = self
            .queue_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for queue in message_queues
            .into_iter()
            .filter_map(|queue| self.normalized_queue(&queue).ok())
        {
            if let Some(state) = states.get_mut(&queue) {
                state.paused = false;
            }
        }
        self.reconcile.notify_one();
    }

    pub async fn commit(&self) -> Result<(), LitePullConsumerError> {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        self.commit_all_best_effort().await;
        Ok(())
    }

    pub async fn commit_sync(&self) -> Result<(), LitePullConsumerError> {
        self.commit().await
    }

    pub async fn commit_queues<I>(
        &self,
        message_queues: I,
        persist: bool,
    ) -> Result<(), LitePullConsumerError>
    where
        I: IntoIterator<Item = MessageQueue>,
    {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        let queues = message_queues
            .into_iter()
            .map(|queue| self.normalized_queue(&queue))
            .collect::<Result<Vec<_>, _>>()?;
        if queues.is_empty() {
            return Ok(());
        }
        let offsets = {
            let states = self
                .queue_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            queues
                .iter()
                .filter_map(|queue| {
                    states.get(queue).and_then(|state| {
                        state.delivered_offset.map(|offset| (queue.clone(), offset))
                    })
                })
                .collect::<Vec<_>>()
        };
        self.apply_commits(offsets, &queues, persist).await
    }

    pub async fn commit_offsets<I>(
        &self,
        offsets: I,
        persist: bool,
    ) -> Result<(), LitePullConsumerError>
    where
        I: IntoIterator<Item = (MessageQueue, i64)>,
    {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        let offsets = offsets
            .into_iter()
            .map(|(queue, offset)| Ok((self.normalized_queue(&queue)?, offset)))
            .collect::<Result<Vec<_>, LitePullConsumerError>>()?;
        if offsets.is_empty() {
            return Ok(());
        }
        let queues = offsets
            .iter()
            .map(|(queue, _)| queue.clone())
            .collect::<Vec<_>>();
        self.apply_commits(offsets, &queues, persist).await
    }

    async fn apply_commits(
        &self,
        offsets: Vec<(MessageQueue, i64)>,
        persist_queues: &[MessageQueue],
        persist: bool,
    ) -> Result<(), LitePullConsumerError> {
        let offsets = offsets
            .into_iter()
            .filter(|(queue, offset)| *offset != -1 && self.is_assigned(queue))
            .collect::<Vec<_>>();
        if offsets.is_empty() && !persist {
            return Ok(());
        }
        let engine = self.engine().ok_or(LitePullConsumerError::NotStarted)?;
        for (queue, offset) in offsets {
            engine.update_consume_offset(&queue, offset).await?;
        }
        if persist {
            engine.persist_offsets(persist_queues).await?;
        }
        Ok(())
    }

    pub async fn commit_sync_offsets<I>(
        &self,
        offsets: I,
        persist: bool,
    ) -> Result<(), LitePullConsumerError>
    where
        I: IntoIterator<Item = (MessageQueue, i64)>,
    {
        self.commit_offsets(offsets, persist).await
    }

    pub fn set_auto_commit(&self, enabled: bool) {
        self.auto_commit.store(enabled, Ordering::Release);
    }

    pub fn is_connect_broker_by_user(&self) -> bool {
        self.connect_broker_by_user.load(Ordering::Acquire)
    }

    pub fn set_connect_broker_by_user(&self, enabled: bool) {
        self.connect_broker_by_user
            .store(enabled, Ordering::Release);
        self.configure_engine_pull_api();
    }

    pub fn default_broker_id(&self) -> i64 {
        self.default_broker_id.load(Ordering::Acquire)
    }

    pub fn set_default_broker_id(&self, broker_id: i64) {
        self.default_broker_id.store(broker_id, Ordering::Release);
        self.configure_engine_pull_api();
    }

    pub fn build_subscriptions_for_heartbeat(
        &self,
        selectors: &HashMap<String, Option<MessageSelector>>,
    ) -> Result<(), LitePullConsumerError> {
        {
            let mut subscriptions = self
                .heartbeat_subscriptions
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            subscriptions.clear();
            for (topic, selector) in selectors {
                let selector = selector.as_ref().ok_or_else(|| {
                    PullConsumerError::InvalidSubscription("message selector is null".into())
                })?;
                subscriptions.insert(Some(SharedValue::from(
                    build_subscription(
                        topic,
                        Some(selector.expression()),
                        Some(selector.expression_type()),
                    )
                    .map_err(|error| PullConsumerError::InvalidSubscription(error.to_string()))?,
                )));
            }
        }
        if let Some(engine) = self.engine() {
            engine.set_additional_heartbeat_subscriptions(self.heartbeat_subscriptions.clone());
        }
        Ok(())
    }

    pub fn subscriptions_for_heartbeat(&self) -> Arc<RwLock<ConsumerSubscriptionDataSet>> {
        self.heartbeat_subscriptions.clone()
    }

    /// Registers a queue metadata listener. When the consumer is already
    /// running, the current route is fetched before this call succeeds, as in
    /// Java LitePullConsumer; a failed fetch leaves the listener registered.
    pub async fn register_topic_message_queue_change_listener(
        &self,
        topic: &str,
        listener: TopicMessageQueueChangeListener,
    ) -> Result<(), LitePullConsumerError> {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        let topic = self.normalized_topic(topic)?;
        let _gate = self.reconcile_gate.lock().await;
        self.queue_change_listeners
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(topic.clone(), listener);
        if self.running.load(Ordering::Acquire) {
            self.reconcile.notify_one();
            if let Some(engine) = self.engine() {
                let queues = engine.fetch_message_queues(&topic).await?;
                self.last_topic_queues
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(topic, queues);
            }
        }
        self.reconcile.notify_one();
        Ok(())
    }

    pub async fn update_name_server_address(
        &self,
        address: impl Into<String>,
    ) -> Result<(), LitePullConsumerError> {
        let engine = self.engine().ok_or(LitePullConsumerError::NotStarted)?;
        engine.update_name_server_address(&address.into()).await?;
        Ok(())
    }

    fn engine(&self) -> Option<Arc<PullConsumer>> {
        self.engine
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn configure_engine_pull_api(&self) {
        if let Some(engine) = self.engine() {
            engine.configure_pull_api(
                self.connect_broker_by_user.load(Ordering::Acquire),
                self.default_broker_id.load(Ordering::Acquire),
            );
        }
    }

    fn require_assigned(&self, queue: &MessageQueue) -> Result<(), LitePullConsumerError> {
        if self.is_assigned(queue) {
            Ok(())
        } else {
            Err(LitePullConsumerError::QueueNotAssigned)
        }
    }

    fn is_assigned(&self, queue: &MessageQueue) -> bool {
        self.queue_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(queue)
    }

    async fn sync_workers(self: &Arc<Self>) {
        if !self.running.load(Ordering::Acquire) {
            return;
        }
        let Ok(_gate) = self.reconcile_gate.try_lock() else {
            return;
        };
        let mode = *self
            .mode
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut desired = BTreeSet::new();
        if mode == SubscriptionMode::Assign {
            desired = self
                .assigned
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
        } else if mode == SubscriptionMode::Subscribe {
            if let Some(engine) = self.engine() {
                let topics = self
                    .subscriptions
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>();
                for topic in topics {
                    if let Ok(queues) = engine.fetch_message_queues_in_balance(&topic).await {
                        desired.extend(queues);
                    }
                }
            }
        }
        let current = self
            .queue_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let removed = current.difference(&desired).cloned().collect::<Vec<_>>();
        let added = desired.difference(&current).cloned().collect::<Vec<_>>();
        let mut removed_workers = Vec::new();
        {
            let mut states = self
                .queue_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for queue in &removed {
                states.remove(queue);
            }
            for queue in &added {
                let generation = self.next_generation.fetch_add(1, Ordering::AcqRel);
                states.insert(
                    queue.clone(),
                    QueueState {
                        generation,
                        fetched_offset: None,
                        delivered_offset: None,
                        paused: false,
                    },
                );
            }
            let mut workers = self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for queue in &removed {
                if let Some(worker) = workers.remove(queue) {
                    worker.abort();
                    removed_workers.push(worker);
                }
            }
            for queue in &current {
                if workers.get(queue).is_some_and(JoinHandle::is_finished) {
                    if let Some(worker) = workers.remove(queue) {
                        removed_workers.push(worker);
                    }
                }
            }
        }
        for worker in removed_workers {
            let _ = worker.await;
        }
        for queue in removed {
            self.remove_queue_batches(&queue);
        }
        for queue in desired {
            let generation = self
                .queue_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&queue)
                .map(|state| state.generation);
            let has_worker = self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&queue);
            if let Some(generation) = generation.filter(|_| !has_worker) {
                self.spawn_queue_worker(queue, generation);
            }
        }
    }

    async fn update_topic_queue_change(&self, engine: &PullConsumer, topic: &str) {
        let listeners = self
            .queue_change_listeners
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(topic)
            .cloned();
        let Some(listener) = listeners else {
            return;
        };
        let Ok(queues) = engine.fetch_message_queues(topic).await else {
            return;
        };
        let changed = {
            let mut known = self
                .last_topic_queues
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if known.get(topic) == Some(&queues) {
                false
            } else {
                known.insert(topic.to_owned(), queues.clone());
                true
            }
        };
        if changed {
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| listener(topic, &queues)));
        }
    }

    fn spawn_queue_worker(self: &Arc<Self>, queue: MessageQueue, generation: u64) {
        let consumer = self.clone();
        let worker_queue = queue.clone();
        let handle = tokio::spawn(async move {
            consumer.pull_loop(worker_queue, generation).await;
        });
        self.workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(queue, handle);
    }

    async fn abort_queue_worker(&self, queue: &MessageQueue) {
        let worker = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(queue);
        if let Some(worker) = worker {
            worker.abort();
            let _ = worker.await;
        }
    }

    async fn wait_after_pull_error(&self, error: &LitePullConsumerError) {
        sleep(Duration::from_millis(lite_pull_error_delay_millis(
            error,
            self.pull_time_delay_millis_when_exception,
        )))
        .await;
    }

    async fn pull_loop(self: Arc<Self>, queue: MessageQueue, generation: u64) {
        loop {
            if !self.running.load(Ordering::Acquire) {
                return;
            }
            let (paused, mut offset) = {
                let states = self
                    .queue_states
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let Some(state) = states
                    .get(&queue)
                    .filter(|state| state.generation == generation)
                else {
                    return;
                };
                (state.paused, state.fetched_offset)
            };
            if paused || self.is_flow_controlled(&queue) {
                sleep(Duration::from_millis(50)).await;
                continue;
            }
            let Some(engine) = self.engine() else {
                return;
            };
            if offset.is_none() {
                match self.initial_offset(&engine, &queue).await {
                    Ok(value) if value >= 0 => {
                        offset = Some(value);
                        if let Some(state) = self
                            .queue_states
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .get_mut(&queue)
                            .filter(|state| state.generation == generation)
                        {
                            state.fetched_offset = Some(value);
                            state.delivered_offset.get_or_insert(value);
                        } else {
                            return;
                        }
                    }
                    Err(error) => {
                        self.wait_after_pull_error(&error).await;
                        continue;
                    }
                    Ok(_) => {
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            }
            let selector = self.selector_for_queue(&queue);
            let Some(offset) = offset else {
                continue;
            };
            let Ok(_permit) = self.pull_permits.clone().acquire_owned().await else {
                return;
            };
            let result = engine
                .lite_pull_with_selector(
                    &queue,
                    &selector,
                    offset,
                    self.pull_batch_size.load(Ordering::Acquire),
                    self.consumer_timeout_millis_when_suspend
                        .load(Ordering::Acquire),
                )
                .await;
            match result {
                Ok(result) => match self.accept_pull_result(&queue, generation, result) {
                    None => return,
                    Some(false) => sleep(Duration::from_millis(10)).await,
                    Some(true) => {}
                },
                Err(error) => {
                    let error = LitePullConsumerError::Pull(error);
                    self.wait_after_pull_error(&error).await;
                }
            }
        }
    }

    fn selector_for_queue(&self, queue: &MessageQueue) -> MessageSelector {
        let topic = queue.topic().unwrap_or_default();
        if *self
            .mode
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            == SubscriptionMode::Assign
        {
            return self
                .assign_expressions
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(topic)
                .map(MessageSelector::by_tag)
                .unwrap_or_else(|| MessageSelector::by_tag("*"));
        }
        self.subscriptions
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(topic)
            .cloned()
            .unwrap_or_else(|| MessageSelector::by_tag("*"))
    }

    async fn initial_offset(
        &self,
        engine: &PullConsumer,
        queue: &MessageQueue,
    ) -> Result<i64, LitePullConsumerError> {
        let stored = engine.fetch_consume_offset(queue, false).await?;
        match lite_initial_offset_action(
            self.consume_from_where(),
            queue.topic().unwrap_or_default(),
            stored,
            &self.consume_timestamp(),
        )? {
            LiteInitialOffsetAction::Value(offset) => Ok(offset),
            LiteInitialOffsetAction::MaxOffset => Ok(engine.get_max_offset(queue).await?),
            LiteInitialOffsetAction::SearchTimestamp(timestamp) => {
                // Java preserves SEARCH_OFFSET's result, including -1.
                Ok(engine.search_offset(queue, timestamp).await?)
            }
        }
    }

    fn accept_pull_result(
        &self,
        queue: &MessageQueue,
        generation: u64,
        mut result: ProcessedPullResult,
    ) -> Option<bool> {
        let next_offset = result.result.next_begin_offset;
        if next_offset < 0 {
            return Some(false);
        }
        let messages = result.messages.take().unwrap_or_default();
        let body_bytes = messages
            .iter()
            .map(|message| message.body().len())
            .sum::<usize>();
        let mut states = self
            .queue_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = states
            .get_mut(queue)
            .filter(|state| state.generation == generation)?;
        state.fetched_offset = Some(next_offset);
        drop(states);

        let had_messages = !messages.is_empty();
        if had_messages {
            let mut buffer = self
                .buffer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            buffer.messages = buffer.messages.saturating_add(messages.len());
            buffer.body_bytes = buffer.body_bytes.saturating_add(body_bytes);
            buffer.batches.push_back(BufferedBatch {
                queue: queue.clone(),
                generation,
                messages,
                body_bytes,
            });
            drop(buffer);
            self.ready.notify_one();
        }
        Some(had_messages)
    }

    fn is_flow_controlled(&self, queue: &MessageQueue) -> bool {
        let buffer = self
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let queued_requests = buffer.batches.len() as i64;
        if queued_requests.saturating_mul(i64::from(self.pull_batch_size.load(Ordering::Acquire)))
            > self.pull_threshold_for_all.load(Ordering::Acquire)
        {
            return true;
        }
        let mut queue_count = 0usize;
        let mut queue_bytes = 0usize;
        let mut minimum_offset = None;
        let mut maximum_offset = None;
        for batch in &buffer.batches {
            if &batch.queue == queue {
                queue_count = queue_count.saturating_add(batch.messages.len());
                queue_bytes = queue_bytes.saturating_add(batch.body_bytes);
                for offset in batch.messages.iter().map(|message| message.queue_offset()) {
                    minimum_offset =
                        Some(minimum_offset.map_or(offset, |value: i64| value.min(offset)));
                    maximum_offset =
                        Some(maximum_offset.map_or(offset, |value: i64| value.max(offset)));
                }
            }
        }
        if i64::try_from(queue_count).unwrap_or(i64::MAX)
            > i64::from(self.pull_threshold_for_queue.load(Ordering::Acquire))
        {
            return true;
        }
        let queue_size_mib = queue_bytes / (1024 * 1024);
        if i64::try_from(queue_size_mib).unwrap_or(i64::MAX)
            > i64::from(self.pull_threshold_size_for_queue.load(Ordering::Acquire))
        {
            return true;
        }
        minimum_offset
            .zip(maximum_offset)
            .is_some_and(|(minimum, maximum)| {
                maximum.saturating_sub(minimum)
                    > i64::from(self.consume_max_span.load(Ordering::Acquire))
            })
    }

    #[cfg(test)]
    fn take_ready_batch(&self) -> Option<Vec<rocketmq_classic_common::message::MessageExt>> {
        self.take_ready_batch_with_queue()
            .map(|(_, messages)| messages)
    }

    fn take_ready_batch_with_queue(
        &self,
    ) -> Option<(
        MessageQueue,
        Vec<rocketmq_classic_common::message::MessageExt>,
    )> {
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while let Some(batch) = buffer.batches.pop_front() {
            buffer.messages = buffer.messages.saturating_sub(batch.messages.len());
            buffer.body_bytes = buffer.body_bytes.saturating_sub(batch.body_bytes);
            let mut states = self
                .queue_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(state) = states
                .get_mut(&batch.queue)
                .filter(|state| state.generation == batch.generation)
            else {
                continue;
            };
            if let Some(last_offset) = batch
                .messages
                .iter()
                .map(|message| message.queue_offset())
                .max()
            {
                state.delivered_offset = Some(last_offset.saturating_add(1));
            }
            return Some((batch.queue, batch.messages));
        }
        None
    }

    fn remove_queue_batches(&self, queue: &MessageQueue) {
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut retained = VecDeque::with_capacity(buffer.batches.len());
        buffer.messages = 0;
        buffer.body_bytes = 0;
        while let Some(batch) = buffer.batches.pop_front() {
            if &batch.queue != queue {
                buffer.messages = buffer.messages.saturating_add(batch.messages.len());
                buffer.body_bytes = buffer.body_bytes.saturating_add(batch.body_bytes);
                retained.push_back(batch);
            }
        }
        buffer.batches = retained;
    }

    async fn maybe_auto_commit(&self) {
        if !self.auto_commit.load(Ordering::Acquire) {
            return;
        }
        let now = Instant::now();
        let due = {
            let mut deadline = self
                .next_auto_commit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if deadline.is_none_or(|deadline| now >= deadline) {
                *deadline = Some(
                    now + Duration::from_millis(
                        self.auto_commit_interval_millis
                            .load(Ordering::Acquire)
                            .max(0) as u64,
                    ),
                );
                true
            } else {
                false
            }
        };
        if due {
            self.commit_all_best_effort().await;
        }
    }

    async fn commit_all_best_effort(&self) {
        let offsets = self
            .queue_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter_map(|(queue, state)| {
                state.delivered_offset.map(|offset| (queue.clone(), offset))
            })
            .collect::<Vec<_>>();
        for (queue, offset) in offsets {
            let persist_queues = [queue.clone()];
            if let Err(error) = self
                .apply_commits(vec![(queue, offset)], &persist_queues, false)
                .await
            {
                tracing::debug!(error = %error, "Lite automatic offset commit failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_common::message::Message;
    use rocketmq_classic_core::api::consumer::{PullResultExt, PullStatus};
    use rocketmq_classic_core::config::AccessChannel;
    use rocketmq_classic_protocol::codes::LanguageCode;

    fn consumer() -> LitePullConsumer {
        LitePullConsumer::new(LitePullConsumerConfig::new("lite-test").unwrap()).unwrap()
    }

    #[test]
    fn lite_initial_offset_matrix_matches_java_missing_and_unavailable_branches() {
        let timestamp = "20200101000000";
        let regular = "orders";
        let retry = "%RETRY%group";
        let cases = [
            (
                ConsumeFromWhere::ConsumeFromLastOffset,
                -1,
                regular,
                LiteInitialOffsetAction::MaxOffset,
            ),
            (
                ConsumeFromWhere::ConsumeFromLastOffset,
                -1,
                retry,
                LiteInitialOffsetAction::Value(0),
            ),
            (
                ConsumeFromWhere::ConsumeFromFirstOffset,
                -1,
                regular,
                LiteInitialOffsetAction::Value(0),
            ),
            (
                ConsumeFromWhere::ConsumeFromFirstOffset,
                -1,
                retry,
                LiteInitialOffsetAction::Value(0),
            ),
            (
                ConsumeFromWhere::ConsumeFromTimestamp,
                -1,
                regular,
                LiteInitialOffsetAction::SearchTimestamp(1_577_836_800_000),
            ),
            (
                ConsumeFromWhere::ConsumeFromTimestamp,
                -1,
                retry,
                LiteInitialOffsetAction::MaxOffset,
            ),
            (
                ConsumeFromWhere::ConsumeFromTimestamp,
                73,
                retry,
                LiteInitialOffsetAction::Value(73),
            ),
        ];

        for (policy, stored_offset, topic, expected) in cases {
            assert_eq!(
                lite_initial_offset_action(policy, topic, stored_offset, timestamp).unwrap(),
                expected,
                "policy={policy:?}, stored={stored_offset}, topic={topic}"
            );
        }
        for policy in [
            ConsumeFromWhere::ConsumeFromLastOffset,
            ConsumeFromWhere::ConsumeFromFirstOffset,
            ConsumeFromWhere::ConsumeFromTimestamp,
        ] {
            assert_eq!(
                lite_initial_offset_action(policy, regular, -2, timestamp).unwrap(),
                LiteInitialOffsetAction::Value(-1),
                "unavailable stored offset with {policy:?}"
            );
        }
        assert!(matches!(
            lite_initial_offset_action(
                ConsumeFromWhere::ConsumeFromTimestamp,
                regular,
                -1,
                "invalid"
            ),
            Err(LitePullConsumerError::InvalidConsumeTimestamp)
        ));
        assert_eq!(
            lite_initial_offset_action(
                ConsumeFromWhere::ConsumeFromTimestamp,
                retry,
                -1,
                "invalid"
            )
            .unwrap(),
            LiteInitialOffsetAction::MaxOffset
        );
    }

    #[test]
    fn lite_consumer_reads_live_policy_and_timestamp_after_construction() {
        let consumer = consumer();
        consumer
            .set_consume_from_where(ConsumeFromWhere::ConsumeFromTimestamp)
            .unwrap();
        consumer.set_consume_timestamp("20210102030405");

        assert_eq!(
            lite_initial_offset_action(
                consumer.consume_from_where(),
                "orders",
                -1,
                &consumer.consume_timestamp(),
            )
            .unwrap(),
            LiteInitialOffsetAction::SearchTimestamp(1_609_556_645_000)
        );
    }

    fn response_error(code: i32) -> LitePullConsumerError {
        PullConsumerError::Rpc(RpcError::Response {
            request_code: 11,
            peer: "broker-a".to_owned(),
            code,
            remark: None,
        })
        .into()
    }

    #[test]
    fn lite_pull_retry_delay_selects_configured_default_and_flow_control_branches() {
        let configured_error = response_error(response::SYSTEM_ERROR);
        let flow_control_error = response_error(response::FLOW_CONTROL);
        assert_eq!(
            lite_pull_error_delay_millis(&configured_error, 4_500),
            4_500
        );
        assert_eq!(
            lite_pull_error_delay_millis(&configured_error, 1_000),
            1_000
        );
        assert_eq!(lite_pull_error_delay_millis(&flow_control_error, 4_500), 20);
    }

    #[tokio::test(start_paused = true)]
    async fn lite_pull_retry_delay_waits_for_configured_duration_and_is_cancellable() {
        let mut config = LitePullConsumerConfig::new("lite-retry-delay").unwrap();
        config
            .client_config_mut()
            .set_pull_time_delay_millis_when_exception(4_500);
        let consumer = Arc::new(LitePullConsumer::new(config).unwrap());
        let error = response_error(response::SYSTEM_ERROR);
        let retry = {
            let consumer = consumer.clone();
            tokio::spawn(async move { consumer.wait_after_pull_error(&error).await })
        };
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(4_499)).await;
        tokio::task::yield_now().await;
        assert!(!retry.is_finished());
        tokio::time::advance(Duration::from_millis(1)).await;
        retry.await.unwrap();

        let pending = {
            let consumer = consumer.clone();
            let error = response_error(response::SYSTEM_ERROR);
            tokio::spawn(async move { consumer.wait_after_pull_error(&error).await })
        };
        tokio::task::yield_now().await;
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn lite_pull_retry_delay_uses_one_second_default_and_twenty_ms_for_flow_control() {
        let default_consumer = Arc::new(consumer());
        let default_error = response_error(response::SYSTEM_ERROR);
        let default_retry = {
            let consumer = default_consumer.clone();
            tokio::spawn(async move { consumer.wait_after_pull_error(&default_error).await })
        };
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(999)).await;
        tokio::task::yield_now().await;
        assert!(!default_retry.is_finished());
        tokio::time::advance(Duration::from_millis(1)).await;
        default_retry.await.unwrap();

        let configured_consumer = Arc::new({
            let mut config = LitePullConsumerConfig::new("lite-flow-control-delay").unwrap();
            config
                .client_config_mut()
                .set_pull_time_delay_millis_when_exception(4_500);
            LitePullConsumer::new(config).unwrap()
        });
        let flow_error = response_error(response::FLOW_CONTROL);
        let flow_retry = {
            let consumer = configured_consumer.clone();
            tokio::spawn(async move { consumer.wait_after_pull_error(&flow_error).await })
        };
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(19)).await;
        tokio::task::yield_now().await;
        assert!(!flow_retry.is_finished());
        tokio::time::advance(Duration::from_millis(1)).await;
        flow_retry.await.unwrap();
    }

    #[tokio::test]
    async fn lite_nullable_group_is_preserved_until_start_validation() {
        let consumer = Arc::new(
            LitePullConsumer::new(LitePullConsumerConfig::new_nullable(None).unwrap()).unwrap(),
        );
        assert_eq!(consumer.get_consumer_group(), None);
        assert!(matches!(
            consumer.start().await,
            Err(LitePullConsumerError::Pull(
                PullConsumerError::InvalidConsumerGroup
            ))
        ));
        assert!(matches!(
            consumer.start().await,
            Err(LitePullConsumerError::StartFailed)
        ));

        consumer.set_consumer_group("lite-after-construction");
        assert_eq!(
            consumer.get_consumer_group().as_deref(),
            Some("lite-after-construction")
        );
        consumer.set_consumer_group_nullable(None);
        assert_eq!(consumer.get_consumer_group(), None);
    }

    #[tokio::test]
    async fn lite_group_start_validation_distinguishes_null_empty_default_and_namespaced() {
        for (group, expected_error) in [
            (None, PullConsumerError::InvalidConsumerGroup.to_string()),
            (
                Some(String::new()),
                PullConsumerError::InvalidConsumerGroup.to_string(),
            ),
            (
                Some(crate::config::DEFAULT_CONSUMER_GROUP.to_owned()),
                PullConsumerError::DefaultConsumerGroup.to_string(),
            ),
        ] {
            let consumer = Arc::new(
                LitePullConsumer::new(LitePullConsumerConfig::new_nullable(group).unwrap())
                    .unwrap(),
            );
            let error = consumer.start().await.unwrap_err();
            assert_eq!(error.to_string(), expected_error);
            assert!(matches!(
                consumer.start().await,
                Err(LitePullConsumerError::StartFailed)
            ));
        }

        let mut config = LitePullConsumerConfig::new("valid-lite-group").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let consumer = LitePullConsumer::new(config).unwrap();
        let mut client_config = consumer.config().client_config().clone();
        let effective_group = client_config
            .with_namespace(consumer.get_consumer_group().as_deref())
            .unwrap()
            .unwrap();
        assert_eq!(effective_group, "tenant%valid-lite-group");
        assert!(crate::config::validate_group(&effective_group).is_ok());
    }

    #[tokio::test]
    async fn inherited_java_client_config_is_copied_to_the_pull_engine() {
        let mut config = LitePullConsumerConfig::new("lite-client-config-copy").unwrap();
        let client = config.client_config_mut();
        client.set_namesrv_addr(Some("nameserver:9876".into()));
        client.set_client_ip(Some("192.0.2.10".into()));
        client.set_instance_name(Some("lite-client-config-copy-instance".into()));
        client.set_client_callback_executor_threads(3);
        client.set_namespace(Some("tenant".into()));
        client.set_namespace_v2(Some("tenant-v2".into()));
        client.set_access_channel(Some(AccessChannel::Cloud));
        client.set_poll_name_server_interval(31);
        client.set_heartbeat_broker_interval(32);
        client.set_persist_consumer_offset_interval(33);
        client.set_pull_time_delay_millis_when_exception(34);
        client.set_trace_msg_batch_num(35);
        client.set_unit_mode(true);
        client.set_unit_name(Some("unit-a".into()));
        client.set_decode_read_body(false);
        client.set_decode_decompress_body(false);
        client.set_vip_channel_enabled(true);
        client.set_use_heartbeat_v2(true);
        client.set_use_tls(true);
        client.set_socks_proxy_config(Some("{}".into()));
        client.set_mq_client_api_timeout(36);
        client.set_detect_timeout(37);
        client.set_detect_interval(38);
        client.set_language(Some(LanguageCode::Rust));
        client.set_enable_stream_request_type(false);
        client.set_send_latency_enable(true);
        client.set_start_detector_enable(true);
        client.set_enable_heartbeat_channel_event_listener(false);
        client.set_enable_concurrent_heartbeat(true);
        client.set_concurrent_heartbeat_thread_pool_size(4);
        client.set_enable_trace(true);
        client.set_trace_topic(Some("client-trace-topic".into()));
        client.set_max_page_size_in_get_metadata(39);
        let expected = config.client_config().clone();

        let consumer = LitePullConsumer::new(config).unwrap();
        let engine = consumer.prepare_engine().await.unwrap();
        assert_eq!(engine.config().client_config(), &expected);
        assert!(engine.config().unit_mode());
        assert!(consumer.config().enable_msg_trace());
        assert_eq!(
            consumer.config().customized_trace_topic(),
            Some("client-trace-topic")
        );
    }

    #[tokio::test]
    async fn factory_start_failure_preserves_java_lite_consumer_lifecycle() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/consumer/lifecycle.json"))
                .unwrap();
        let java = &fixture["litePullFactoryStartFailure"];
        assert_eq!(java["exception"], "IllegalArgumentException");
        assert_eq!(java["retryException"], "MQClientException");
        assert_eq!(java["consumerRegistered"], true);
        assert_eq!(java["consumerServiceState"], "START_FAILED");
        assert_eq!(java["factoryServiceState"], "START_FAILED");

        let group = format!(
            "lite-factory-start-failure-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut config = LitePullConsumerConfig::new(&group).unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        config
            .client_config_mut()
            .set_instance_name(Some(group.clone()));
        config.client_config_mut().set_poll_name_server_interval(0);
        let client_config = config.client_config().clone();
        let client_id = client_config.build_client_id();
        let manager = rocketmq_classic_core::runtime::ClientRuntimeManager::shared_default();
        let expected_runtime = manager.get_or_create(&client_config).await.unwrap();
        let consumer = Arc::new(LitePullConsumer::new(config).unwrap());

        assert!(matches!(
            consumer.start().await,
            Err(LitePullConsumerError::Pull(PullConsumerError::Runtime(
                rocketmq_classic_core::runtime::RuntimeError::InvalidSchedule
            )))
        ));
        assert!(matches!(
            *consumer.lifecycle.lock().await,
            Lifecycle::StartFailed
        ));
        assert!(!consumer.is_running().await);

        let runtime = manager
            .factory_table()
            .get(&client_id)
            .unwrap()
            .expect("LitePull runtime remains in the shared factory table");
        assert!(Arc::ptr_eq(&expected_runtime, &runtime));
        assert_eq!(
            runtime.state(),
            rocketmq_classic_core::runtime::RuntimeState::Failed
        );
        assert!(runtime.select_consumer(&group).unwrap().is_some());

        assert!(matches!(
            consumer.start().await,
            Err(LitePullConsumerError::StartFailed)
        ));
        consumer.shutdown().await.unwrap();
        assert!(matches!(
            *consumer.lifecycle.lock().await,
            Lifecycle::StartFailed
        ));

        assert!(
            runtime
                .unregister_consumer_by_group(
                    &group,
                    tokio::time::Instant::now() + Duration::from_secs(1),
                )
                .await
                .unwrap()
        );
        runtime.shutdown().await.unwrap();
        manager
            .remove_client_factory(runtime.client_id(), &runtime)
            .unwrap();
    }

    #[tokio::test]
    async fn metadata_schedule_failure_preserves_started_java_factory() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/consumer/lifecycle.json"))
                .unwrap();
        let java = &fixture["litePullMetadataScheduleFailure"];
        assert_eq!(java["exception"], "IllegalArgumentException");
        assert_eq!(java["retryException"], "MQClientException");
        assert_eq!(java["consumerRegistered"], true);
        assert_eq!(java["consumerServiceState"], "START_FAILED");
        assert_eq!(java["factoryServiceState"], "RUNNING");

        let group = format!(
            "lite-metadata-schedule-failure-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut config = LitePullConsumerConfig::new(&group).unwrap();
        config.set_topic_metadata_check_interval_millis(0);
        config
            .client_config_mut()
            .set_instance_name(Some(group.clone()));
        let client_config = config.client_config().clone();
        let client_id = client_config.build_client_id();
        let manager = rocketmq_classic_core::runtime::ClientRuntimeManager::shared_default();
        let expected_runtime = manager.get_or_create(&client_config).await.unwrap();
        let consumer = Arc::new(LitePullConsumer::new(config).unwrap());

        assert!(matches!(
            consumer.start().await,
            Err(LitePullConsumerError::InvalidTopicMetadataCheckInterval)
        ));
        assert!(matches!(
            *consumer.lifecycle.lock().await,
            Lifecycle::StartFailed
        ));
        assert!(!consumer.is_running().await);
        assert!(consumer.engine().is_some());

        let runtime = manager
            .factory_table()
            .get(&client_id)
            .unwrap()
            .expect("LitePull runtime remains in the shared factory table");
        assert!(Arc::ptr_eq(&expected_runtime, &runtime));
        assert_eq!(
            runtime.state(),
            rocketmq_classic_core::runtime::RuntimeState::Running
        );
        assert!(runtime.select_consumer(&group).unwrap().is_some());

        assert!(matches!(
            consumer.start().await,
            Err(LitePullConsumerError::StartFailed)
        ));
        consumer.shutdown().await.unwrap();
        assert!(matches!(
            *consumer.lifecycle.lock().await,
            Lifecycle::StartFailed
        ));

        assert!(
            runtime
                .unregister_consumer_by_group(
                    &group,
                    tokio::time::Instant::now() + Duration::from_secs(1),
                )
                .await
                .unwrap()
        );
        runtime.shutdown().await.unwrap();
        manager
            .remove_client_factory(runtime.client_id(), &runtime)
            .unwrap();
    }

    #[tokio::test]
    async fn lite_config_validation_precedes_pull_executor_creation() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/consumer/lifecycle.json"))
                .unwrap();
        let java = &fixture["litePullConfigValidationPrecedesExecutor"];
        assert_eq!(java["exception"], "MQClientException");
        assert_eq!(java["retryException"], "MQClientException");
        assert_eq!(java["timeoutValidationReached"], true);
        assert_eq!(java["consumerRegistered"], false);
        assert_eq!(java["consumerServiceState"], "START_FAILED");
        assert_eq!(java["factoryServiceState"], "");

        let group = format!(
            "lite-config-order-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut config = LitePullConsumerConfig::new(&group).unwrap();
        config.set_consumer_timeout_millis_when_suspend(19_999);
        config.set_pull_thread_nums(-1);
        config
            .client_config_mut()
            .set_instance_name(Some(group.clone()));
        let client_id = config.client_config().build_client_id();
        let manager = rocketmq_classic_core::runtime::ClientRuntimeManager::shared_default();
        let consumer = Arc::new(LitePullConsumer::new(config).unwrap());

        assert!(matches!(
            consumer.start().await,
            Err(LitePullConsumerError::Pull(
                PullConsumerError::InvalidSuspendTimeout { .. }
            ))
        ));
        assert!(matches!(
            *consumer.lifecycle.lock().await,
            Lifecycle::StartFailed
        ));
        assert!(consumer.engine().is_none());
        assert!(manager.factory_table().get(&client_id).unwrap().is_none());
        assert!(matches!(
            consumer.start().await,
            Err(LitePullConsumerError::StartFailed)
        ));
    }

    #[tokio::test]
    async fn listener_route_failure_shuts_down_java_lite_consumer_startup() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/consumer/lifecycle.json"))
                .unwrap();
        let java = &fixture["litePullListenerRouteFailure"];
        assert_eq!(java["exception"], "MQClientException");
        assert_eq!(java["retryException"], "MQClientException");
        assert_eq!(java["consumerRegistered"], false);
        assert_eq!(java["consumerServiceState"], "SHUTDOWN_ALREADY");
        assert_eq!(java["factoryServiceState"], "SHUTDOWN_ALREADY");

        let group = format!(
            "lite-listener-route-failure-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut config = LitePullConsumerConfig::new(&group).unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some("127.0.0.1:1".into()));
        config
            .client_config_mut()
            .set_instance_name(Some(group.clone()));
        config.client_config_mut().set_mq_client_api_timeout(100);
        let client_config = config.client_config().clone();
        let client_id = client_config.build_client_id();
        let manager = rocketmq_classic_core::runtime::ClientRuntimeManager::shared_default();
        let runtime = manager.get_or_create(&client_config).await.unwrap();
        let consumer = Arc::new(LitePullConsumer::new(config).unwrap());
        consumer
            .register_topic_message_queue_change_listener(
                "listener-route-failure",
                Arc::new(|_, _| {}),
            )
            .await
            .unwrap();

        let start_error = consumer.start().await.unwrap_err();
        assert!(matches!(start_error, LitePullConsumerError::Pull(_)));
        assert!(matches!(
            *consumer.lifecycle.lock().await,
            Lifecycle::Shutdown
        ));
        assert!(!consumer.is_running().await);

        assert_eq!(
            runtime.state(),
            rocketmq_classic_core::runtime::RuntimeState::Stopped
        );
        assert!(runtime.select_consumer(&group).unwrap().is_none());
        if let Some(mapped) = manager.factory_table().get(&client_id).unwrap() {
            assert!(Arc::ptr_eq(&runtime, &mapped));
        }
        assert!(matches!(
            consumer.start().await,
            Err(LitePullConsumerError::Shutdown)
        ));

        manager
            .remove_client_factory(runtime.client_id(), &runtime)
            .unwrap();
    }

    #[tokio::test]
    async fn lite_poll_consume_hooks_run_around_delivery_with_queue_metadata() {
        struct Hook(Arc<Mutex<Vec<String>>>);

        impl ConsumeMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "lite-consume-hook"
            }

            fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
                assert_eq!(context.message_queue.topic(), Some("orders"));
                assert!(!context.success);
                assert!(context.properties.is_none());
                assert!(context.access_channel.is_none());
                assert_eq!(context.consumer_group, None);
                assert_eq!(context.namespace.as_deref(), Some("tenant"));
                context.properties = Some(std::collections::HashMap::new());
                context
                    .properties
                    .as_mut()
                    .unwrap()
                    .insert("shared".into(), "yes".into());
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
                        .and_then(|properties| properties.get("shared"))
                        .map(String::as_str),
                    Some("yes")
                );
                self.0.lock().unwrap().push("after".into());
            }
        }

        let mut config = LitePullConsumerConfig::new("lite-hook").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".into()));
        let consumer = LitePullConsumer::new(config).unwrap();
        consumer.running.store(true, Ordering::Release);
        consumer.auto_commit.store(false, Ordering::Release);
        let events = Arc::new(Mutex::new(Vec::new()));
        consumer.register_consume_message_hook(Arc::new(Hook(events.clone())));
        consumer.set_consumer_group_nullable(None);
        let queue = MessageQueue::new("orders", "broker-a", 0);
        consumer.queue_states.lock().unwrap().insert(
            queue.clone(),
            QueueState {
                generation: 7,
                fetched_offset: Some(1),
                delivered_offset: None,
                paused: false,
            },
        );
        let mut message =
            crate::test_support::message_ext_from(Message::new("orders", b"body".to_vec()));
        message.set_queue_offset(0);
        consumer
            .buffer
            .lock()
            .unwrap()
            .batches
            .push_back(BufferedBatch {
                queue,
                generation: 7,
                messages: vec![message],
                body_bytes: 4,
            });
        consumer.buffer.lock().unwrap().messages = 1;
        consumer.buffer.lock().unwrap().body_bytes = 4;

        let messages = consumer.poll(Some(0)).await.unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(events.lock().unwrap().as_slice(), ["before", "after"]);
    }

    #[test]
    fn post_construction_setters_update_java_live_runtime_settings() {
        let consumer = consumer();
        consumer.set_pull_thread_nums(3);
        consumer.set_auto_commit_interval_millis(2_000);
        consumer.set_auto_commit_interval_millis(999);
        consumer.set_pull_batch_size(11);
        consumer.set_pull_threshold_for_all(23);
        consumer.set_consume_max_span(29);
        consumer.set_pull_threshold_for_queue(31);
        consumer.set_pull_threshold_size_for_queue(37);
        consumer.set_poll_timeout_millis(41);
        consumer.set_consumer_pull_timeout_millis(43);
        consumer.set_consumer_timeout_millis_when_suspend(45);
        consumer.set_topic_metadata_check_interval_millis(47);
        consumer.set_message_queue_listener(Some(Arc::new(|_, _, _| {})));

        assert_eq!(consumer.pull_thread_nums(), 3);
        assert_eq!(consumer.auto_commit_interval_millis(), 2_000);
        assert_eq!(consumer.pull_batch_size(), 11);
        assert_eq!(consumer.pull_threshold_for_all(), 23);
        assert_eq!(consumer.consume_max_span(), 29);
        assert_eq!(consumer.pull_threshold_for_queue(), 31);
        assert_eq!(consumer.pull_threshold_size_for_queue(), 37);
        assert_eq!(consumer.poll_timeout_millis(), 41);
        assert_eq!(consumer.consumer_pull_timeout_millis(), 43);
        assert_eq!(consumer.consumer_timeout_millis_when_suspend(), 45);
        assert_eq!(consumer.topic_metadata_check_interval_millis(), 47);
        assert!(consumer.message_queue_listener().is_some());
    }

    #[tokio::test]
    async fn auto_commit_and_commit_all_ignore_per_queue_errors_like_java() {
        let consumer = consumer();
        consumer.running.store(true, Ordering::Release);
        let queue = MessageQueue::new("orders", "broker-a", 0);
        consumer.queue_states.lock().unwrap().insert(
            queue,
            QueueState {
                generation: 1,
                fetched_offset: Some(5),
                delivered_offset: Some(5),
                paused: false,
            },
        );

        assert!(consumer.poll(Some(0)).await.unwrap().is_empty());
        consumer.commit().await.unwrap();
        assert!(
            consumer
                .next_auto_commit
                .lock()
                .unwrap()
                .is_some_and(|deadline| deadline > Instant::now())
        );
    }

    #[tokio::test]
    async fn empty_unassigned_and_unset_commits_are_noops_before_start_like_java() {
        let consumer = consumer();
        let queue = MessageQueue::new("orders", "broker-a", 0);

        consumer.commit_sync().await.unwrap();
        consumer
            .commit_queues([queue.clone()], false)
            .await
            .unwrap();
        consumer
            .commit_offsets([(queue.clone(), -1)], false)
            .await
            .unwrap();
        consumer.commit_offsets([(queue, 42)], false).await.unwrap();
    }

    #[test]
    fn heartbeat_builder_mutates_the_retained_subscription_set_in_place() {
        let consumer = consumer();
        let retained = consumer.subscriptions_for_heartbeat();
        let mut selectors = HashMap::new();
        selectors.insert("orders".to_owned(), Some(MessageSelector::by_tag("paid")));
        selectors.insert(
            "invoices".to_owned(),
            Some(MessageSelector::by_sql("amount > 0")),
        );
        consumer
            .build_subscriptions_for_heartbeat(&selectors)
            .unwrap();
        assert_eq!(retained.read().unwrap().len(), 2);
        consumer
            .build_subscriptions_for_heartbeat(&HashMap::new())
            .unwrap();
        assert!(Arc::ptr_eq(
            &retained,
            &consumer.subscriptions_for_heartbeat()
        ));
        assert!(retained.read().unwrap().is_empty());
    }

    #[tokio::test]
    async fn default_subscribe_uses_java_tag_all_subscription() {
        let consumer = consumer();
        consumer.subscribe("orders").await.unwrap();
        assert_eq!(
            consumer.subscriptions.read().unwrap().get("orders"),
            Some(&MessageSelector::by_tag("*"))
        );
    }

    #[test]
    fn heartbeat_builder_rejects_null_selectors_like_java() {
        let consumer = consumer();
        let selectors = HashMap::from([("orders".to_owned(), None)]);
        assert!(matches!(
            consumer.build_subscriptions_for_heartbeat(&selectors),
            Err(LitePullConsumerError::Pull(
                PullConsumerError::InvalidSubscription(_)
            ))
        ));
        assert!(
            consumer
                .subscriptions_for_heartbeat()
                .read()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn delivered_offset_advances_on_poll_without_moving_fetched_position() {
        let consumer = consumer();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        consumer.queue_states.lock().unwrap().insert(
            queue.clone(),
            QueueState {
                generation: 7,
                fetched_offset: Some(21),
                delivered_offset: Some(10),
                paused: false,
            },
        );
        let mut first =
            crate::test_support::message_ext_from(Message::new("orders", b"a".to_vec()));
        first.set_queue_offset(19);
        let mut second =
            crate::test_support::message_ext_from(Message::new("orders", b"b".to_vec()));
        second.set_queue_offset(20);
        let body_bytes = 2;
        consumer
            .buffer
            .lock()
            .unwrap()
            .batches
            .push_back(BufferedBatch {
                queue: queue.clone(),
                generation: 7,
                messages: vec![first, second],
                body_bytes,
            });
        consumer.buffer.lock().unwrap().messages = 2;
        consumer.buffer.lock().unwrap().body_bytes = body_bytes;

        let batch = consumer.take_ready_batch().unwrap();
        assert_eq!(batch.len(), 2);
        let states = consumer.queue_states.lock().unwrap();
        let state = states.get(&queue).unwrap();
        assert_eq!(state.fetched_offset, Some(21));
        assert_eq!(state.delivered_offset, Some(21));
    }

    #[tokio::test(start_paused = true)]
    async fn lite_accepts_the_complete_pull_batch_before_advancing_fetched_offset() {
        let consumer = consumer();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        consumer.queue_states.lock().unwrap().insert(
            queue.clone(),
            QueueState {
                generation: 7,
                fetched_offset: None,
                delivered_offset: None,
                paused: false,
            },
        );
        let messages = (0..24)
            .map(|offset| {
                let mut message =
                    crate::test_support::message_ext_from(Message::new("orders", b"body".to_vec()));
                message.set_queue_offset(offset);
                message
            })
            .collect::<Vec<_>>();
        let result = ProcessedPullResult {
            result: PullResultExt {
                status: PullStatus::Found,
                next_begin_offset: 24,
                min_offset: 0,
                max_offset: 24,
                suggest_which_broker_id: 0,
                offset_delta: None,
                raw_messages: None,
            },
            messages: Some(messages),
        };

        let accepted_at = Instant::now();
        assert_eq!(consumer.accept_pull_result(&queue, 7, result), Some(true));
        assert_eq!(Instant::now(), accepted_at);
        let states = consumer.queue_states.lock().unwrap();
        assert_eq!(states.get(&queue).unwrap().fetched_offset, Some(24));
        drop(states);
        let buffer = consumer.buffer.lock().unwrap();
        assert_eq!(buffer.messages, 24);
        assert_eq!(buffer.batches.front().unwrap().messages.len(), 24);
    }

    #[test]
    fn flow_control_uses_java_request_count_message_and_whole_mib_boundaries() {
        let config = LitePullConsumerConfig::new("lite-flow-control").unwrap();
        let consumer = LitePullConsumer::new(config).unwrap();
        consumer.set_pull_threshold_for_all(20);
        consumer.set_pull_threshold_for_queue(2);
        consumer.set_pull_threshold_size_for_queue(1);
        consumer.set_consume_max_span(5);
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let batch = |offsets: &[i64], body_bytes| BufferedBatch {
            queue: queue.clone(),
            generation: 1,
            messages: offsets
                .iter()
                .map(|offset| {
                    let mut message =
                        crate::test_support::message_ext_from(Message::new("orders", Vec::new()));
                    message.set_queue_offset(*offset);
                    message
                })
                .collect(),
            body_bytes,
        };

        let mut buffer = consumer.buffer.lock().unwrap();
        buffer.batches.push_back(batch(&[2, 7], 1_999_999));
        drop(buffer);
        assert!(!consumer.is_flow_controlled(&queue));

        let mut buffer = consumer.buffer.lock().unwrap();
        buffer.batches.push_back(batch(&[8], 1));
        drop(buffer);
        assert!(consumer.is_flow_controlled(&queue));

        consumer.buffer.lock().unwrap().batches.clear();
        consumer
            .buffer
            .lock()
            .unwrap()
            .batches
            .push_back(batch(&[2], 2 * 1024 * 1024));
        assert!(consumer.is_flow_controlled(&queue));
    }

    #[test]
    fn a_late_pull_result_from_an_old_generation_cannot_advance_offsets() {
        let consumer = consumer();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        consumer.queue_states.lock().unwrap().insert(
            queue.clone(),
            QueueState {
                generation: 8,
                fetched_offset: Some(21),
                delivered_offset: Some(18),
                paused: false,
            },
        );
        let result = ProcessedPullResult {
            result: PullResultExt {
                status: PullStatus::NoNewMessage,
                next_begin_offset: 25,
                min_offset: 0,
                max_offset: 30,
                suggest_which_broker_id: 0,
                offset_delta: None,
                raw_messages: None,
            },
            messages: None,
        };
        assert_eq!(consumer.accept_pull_result(&queue, 7, result), None);
        let states = consumer.queue_states.lock().unwrap();
        let state = states.get(&queue).unwrap();
        assert_eq!(state.fetched_offset, Some(21));
        assert_eq!(state.delivered_offset, Some(18));
    }
}
