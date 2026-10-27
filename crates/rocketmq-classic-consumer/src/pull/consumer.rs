use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex, RwLock, Weak,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};

#[cfg(test)]
use std::collections::VecDeque;

use rocketmq_classic_common::{
    message::{DecodeOptions, MessageExt, MessageQueue, decode_message_id},
    sysflag::pull::{build_pull_sys_flag_with_lite, clear_commit_offset_flag},
};
use rocketmq_classic_core::{
    api::admin::{AdminQueryRequest, QueryResult},
    route::{BrokerAddress, RouteSnapshot},
    runtime::{
        ClientRuntime, ClientRuntimeManager, ConsumerOperation, ConsumerRuntime, GroupLease,
        RuntimeError,
    },
};
use rocketmq_classic_protocol::{
    SharedValue,
    body::{ConsumerRunningInfoBody, LockBatchRequestBody, UnlockBatchRequestBody},
    header::{
        BoundaryType, GetEarliestMsgStoretimeRequestHeader, GetMaxOffsetRequestHeader,
        GetMinOffsetRequestHeader, PullMessageRequestHeader, SearchOffsetRequestHeader,
        ViewMessageRequestHeader,
    },
    heartbeat::{
        ConsumeFromWhere, ConsumeType, ConsumerData, ConsumerSubscriptionDataSet, MessageModel,
        SubscriptionCodeSet, SubscriptionData, SubscriptionTagSet,
    },
};
use rocketmq_classic_remoting::client::RpcHook;
use tokio::time::Instant;

use crate::{
    admin::{first_query_message, unique_key_lookup_begin_timestamp},
    config::PullConsumerConfig,
    consume::{ConsumeMessageContext, ConsumeMessageHook, ConsumeMessageHookRegistry},
    error::PullConsumerError,
    offset::{LocalFileOffsetStore, OffsetStore, ReadOffsetType, RemoteBrokerOffsetStore},
    pop::PopProcessQueue,
    process_queue::ProcessQueue,
    pull::{
        FilterMessageHook, ProcessedPullResult, PullAPIWrapper, SharedFilterMessageHooks,
        resolve_pull_broker_address,
    },
    push::{MessageQueueListener, PushConsumer, PushConsumerConfig, SendBackOptions},
    rebalance::AllocateMessageQueueStrategy,
    service_state::ConsumerServiceState,
};

const MAX_PULL_MESSAGE_BYTES: i32 = i32::MAX;
const FIRST_BROKER_SUPPORTING_EXPRESSION_FILTER: i32 = 232;
const PULL_SEND_BACK_TIMEOUT_MILLIS: u64 = 3_000;
const LOGICAL_QUEUE_MOCK_BROKER_PREFIX: &str = "__syslo__";
static NEXT_PULL_PROCESS_QUEUE_GENERATION: AtomicU64 = AtomicU64::new(1);

type RegisteredSubscriptions = Arc<RwLock<HashSet<SubscriptionData>>>;

/// Selector API corresponding to Java `MessageSelector`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageSelector {
    expression_type: String,
    expression: String,
}

impl MessageSelector {
    pub fn by_sql(expression: impl Into<String>) -> Self {
        Self {
            expression_type: "SQL92".into(),
            expression: expression.into(),
        }
    }

    pub fn by_tag(expression: impl Into<String>) -> Self {
        Self {
            expression_type: "TAG".into(),
            expression: expression.into(),
        }
    }

    pub fn expression_type(&self) -> &str {
        &self.expression_type
    }

    pub fn expression(&self) -> &str {
        &self.expression
    }
}

enum State {
    Created,
    Started {
        runtime: Weak<ClientRuntime>,
        lease: GroupLease,
        offset_store: Arc<dyn OffsetStore>,
        registration: Arc<PullConsumerRegistration>,
    },
    Shutdown,
}

/// Explicit Pull Consumer facade. Pulling returns decoded messages but leaves
/// the offset unchanged; applications choose when to update and persist it.
///
/// Topic registration follows the Java listener and topic-set methods; helper
/// methods that combine registration and unregistration are not public API.
///
/// ```compile_fail
/// use rocketmq_classic_consumer::PullConsumer;
/// fn register_single_topic(consumer: &PullConsumer) {
///     let _ = consumer.register_topic("orders");
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_consumer::PullConsumer;
/// fn unregister_single_topic(consumer: &PullConsumer) {
///     let _ = consumer.unregister_topic("orders");
/// }
/// ```
pub struct PullConsumer {
    config: PullConsumerConfig,
    current_group_name: RwLock<String>,
    broker_suspend_max_time_millis: AtomicI64,
    consumer_timeout_millis_when_suspend: AtomicI64,
    consumer_pull_timeout_millis: AtomicI64,
    rebalance_message_model: Arc<RwLock<MessageModel>>,
    rebalance_strategy: Arc<RwLock<Arc<dyn AllocateMessageQueueStrategy>>>,
    manager: ClientRuntimeManager,
    rpc_hook: Option<Arc<dyn RpcHook>>,
    consume_message_hooks: ConsumeMessageHookRegistry,
    state: tokio::sync::Mutex<State>,
    rebalance_client_factory: Mutex<Option<Option<Arc<ClientRuntime>>>>,
    service_state: RwLock<Option<ConsumerServiceState>>,
    pull_api: RwLock<Option<Arc<PullAPIWrapper>>>,
    pull_api_hooks: SharedFilterMessageHooks,
    pull_connect_broker_by_user: AtomicBool,
    pull_default_broker_id: AtomicI64,
    offset_store: Mutex<Option<Arc<dyn OffsetStore>>>,
    retry_helper: Mutex<Option<Arc<PushConsumer>>>,
    subscriptions: Arc<RwLock<HashMap<String, SubscriptionData>>>,
    register_subscriptions: RegisteredSubscriptions,
    additional_heartbeat_subscriptions: Arc<RwLock<ConsumerSubscriptionDataSet>>,
    topic_queues: Arc<RwLock<HashMap<String, BTreeSet<MessageQueue>>>>,
    process_queues: Arc<RwLock<HashMap<MessageQueue, Arc<ProcessQueue>>>>,
    pop_process_queues: Arc<RwLock<HashMap<MessageQueue, Arc<PopProcessQueue>>>>,
    register_topics: Arc<RwLock<BTreeSet<String>>>,
    in_balance_queues: Arc<RwLock<BTreeSet<MessageQueue>>>,
    message_queue_listener: Arc<RwLock<Option<MessageQueueListener>>>,
    enable_rebalance: Arc<AtomicBool>,
    rebalance_active: Arc<AtomicBool>,
    runtime_for_wakeup: RwLock<Option<Weak<ClientRuntime>>>,
    consumer_start_timestamp: AtomicI64,
}

impl PullConsumer {
    pub fn new(config: PullConsumerConfig) -> Self {
        Self::with_manager(config, ClientRuntimeManager::shared_default())
    }

    fn with_manager(config: PullConsumerConfig, manager: ClientRuntimeManager) -> Self {
        Self::with_manager_and_hook(config, manager, None)
    }

    /// Creates a pull consumer whose remoting requests use the supplied RPC hook.
    pub fn new_with_rpc_hook(
        config: PullConsumerConfig,
        rpc_hook: Option<Arc<dyn RpcHook>>,
    ) -> Self {
        Self::with_manager_and_hook(config, ClientRuntimeManager::shared_default(), rpc_hook)
    }

    fn with_manager_and_hook(
        config: PullConsumerConfig,
        manager: ClientRuntimeManager,
        rpc_hook: Option<Arc<dyn RpcHook>>,
    ) -> Self {
        let current_group_name = config.consumer_group().to_owned();
        let broker_suspend_max_time_millis = config.broker_suspend_max_time_millis();
        let consumer_timeout_millis_when_suspend = config.consumer_timeout_millis_when_suspend();
        let consumer_pull_timeout_millis = config.consumer_pull_timeout_millis();
        let rebalance_message_model = Arc::new(RwLock::new(config.message_model()));
        let rebalance_strategy = Arc::new(RwLock::new(config.allocate_message_queue_strategy()));
        let message_queue_listener = config.message_queue_listener();
        let enable_rebalance = config.enable_rebalance();
        Self {
            config,
            current_group_name: RwLock::new(current_group_name),
            broker_suspend_max_time_millis: AtomicI64::new(broker_suspend_max_time_millis),
            consumer_timeout_millis_when_suspend: AtomicI64::new(
                consumer_timeout_millis_when_suspend,
            ),
            consumer_pull_timeout_millis: AtomicI64::new(consumer_pull_timeout_millis),
            rebalance_message_model,
            rebalance_strategy,
            manager,
            rpc_hook,
            consume_message_hooks: ConsumeMessageHookRegistry::default(),
            state: tokio::sync::Mutex::new(State::Created),
            rebalance_client_factory: Mutex::new(None),
            service_state: RwLock::new(Some(ConsumerServiceState::CreateJust)),
            pull_api: RwLock::new(None),
            pull_api_hooks: Arc::new(RwLock::new(Vec::new())),
            pull_connect_broker_by_user: AtomicBool::new(false),
            pull_default_broker_id: AtomicI64::new(0),
            offset_store: Mutex::new(None),
            retry_helper: Mutex::new(None),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            register_subscriptions: Arc::new(RwLock::new(HashSet::new())),
            additional_heartbeat_subscriptions: Arc::new(RwLock::new(
                ConsumerSubscriptionDataSet::new(),
            )),
            topic_queues: Arc::new(RwLock::new(HashMap::new())),
            process_queues: Arc::new(RwLock::new(HashMap::new())),
            pop_process_queues: Arc::new(RwLock::new(HashMap::new())),
            register_topics: Arc::new(RwLock::new(BTreeSet::new())),
            in_balance_queues: Arc::new(RwLock::new(BTreeSet::new())),
            message_queue_listener: Arc::new(RwLock::new(message_queue_listener)),
            enable_rebalance: Arc::new(AtomicBool::new(enable_rebalance)),
            rebalance_active: Arc::new(AtomicBool::new(false)),
            runtime_for_wakeup: RwLock::new(None),
            consumer_start_timestamp: AtomicI64::new(epoch_millis()),
        }
    }

    pub fn config(&self) -> &PullConsumerConfig {
        &self.config
    }

    /// Returns the consumer group as exposed by Java's implementation getter.
    /// It is raw before startup and namespace-wrapped once startup begins.
    pub fn group_name(&self) -> String {
        read_lock(&self.current_group_name).clone()
    }

    pub fn get_consumer_group(&self) -> String {
        self.group_name()
    }

    pub fn get_consumer_group_nullable(&self) -> Option<String> {
        self.config.get_consumer_group().map(|_| self.group_name())
    }

    /// Pull configuration can be changed before startup, matching Java's
    /// rebalance group setter while keeping the Rust owner borrow-safe.
    pub fn set_consumer_group(&mut self, group: impl Into<String>) {
        let group = group.into();
        self.config.set_consumer_group(group.clone());
        *write_lock(&self.current_group_name) = group;
    }

    pub fn set_consumer_group_nullable(&mut self, group: Option<String>) {
        self.config.set_consumer_group_nullable(group.clone());
        *write_lock(&self.current_group_name) = group.unwrap_or_default();
    }

    pub fn broker_suspend_max_time_millis(&self) -> i64 {
        self.broker_suspend_max_time_millis.load(Ordering::Acquire)
    }

    pub fn set_broker_suspend_max_time_millis(&self, value: i64) {
        self.broker_suspend_max_time_millis
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

    pub fn consumer_pull_timeout_millis(&self) -> i64 {
        self.consumer_pull_timeout_millis.load(Ordering::Acquire)
    }

    pub fn set_consumer_pull_timeout_millis(&self, value: i64) {
        self.consumer_pull_timeout_millis
            .store(value, Ordering::Release);
    }

    pub fn message_model(&self) -> MessageModel {
        *read_lock(&self.rebalance_message_model)
    }

    pub fn set_message_model(&self, message_model: MessageModel) {
        *write_lock(&self.rebalance_message_model) = message_model;
        if let Some(runtime) = read_lock(&self.runtime_for_wakeup)
            .as_ref()
            .and_then(Weak::upgrade)
        {
            runtime.rebalance_immediately();
        }
    }

    pub fn get_allocate_message_queue_strategy(&self) -> Arc<dyn AllocateMessageQueueStrategy> {
        read_lock(&self.rebalance_strategy).clone()
    }

    pub fn set_allocate_message_queue_strategy(
        &self,
        strategy: Arc<dyn AllocateMessageQueueStrategy>,
    ) {
        *write_lock(&self.rebalance_strategy) = strategy;
        if let Some(runtime) = read_lock(&self.runtime_for_wakeup)
            .as_ref()
            .and_then(Weak::upgrade)
        {
            runtime.rebalance_immediately();
        }
    }

    async fn rebalance_broker_address(
        &self,
        runtime: &ClientRuntime,
        topic: &str,
        broker: &str,
        deadline: Instant,
    ) -> Result<String, PullConsumerError> {
        let mut route = runtime.route_snapshot()?;
        if let Some(address) = route.publish_address(broker) {
            return Ok(address.to_owned());
        }
        runtime.update_topic_route(topic, deadline).await?;
        route = runtime.route_snapshot()?;
        route
            .publish_address(broker)
            .map(str::to_owned)
            .ok_or_else(|| PullConsumerError::BrokerNotFound {
                broker: broker.to_owned(),
            })
    }

    /// Requests the Java rebalance lock for one queue and applies returned
    /// lock state to any matching local ProcessQueue.
    pub async fn lock(&self, input_queue: &MessageQueue) -> Result<bool, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(input_queue)?;
        let topic = queue.topic().ok_or(PullConsumerError::InvalidQueue)?;
        let broker = queue.broker_name().ok_or(PullConsumerError::InvalidQueue)?;
        let deadline = Instant::now() + Duration::from_millis(1_000);
        let address = self
            .rebalance_broker_address(&runtime, topic, broker, deadline)
            .await?;
        let mut body = LockBatchRequestBody::default();
        body.set_consumer_group(Some(self.group_name()));
        body.set_client_id(Some(runtime.client_id().to_owned()));
        body.set_only_this_broker(false);
        body.set_mq_set(Some(SharedValue::new(HashSet::from([Some(
            SharedValue::new(queue.clone()),
        )]))));
        let Some(locked_queues) = runtime
            .api()
            .lock_batch_mq(&address, &body, deadline)
            .await?
        else {
            return Ok(false);
        };
        for locked_queue in locked_queues.iter().flatten() {
            if let Some(process_queue) = read_lock(&self.process_queues).get(locked_queue) {
                process_queue.set_locked(true);
                process_queue.set_last_lock_timestamp(epoch_millis());
            }
        }
        Ok(locked_queues.contains(&Some(queue)))
    }

    /// Locks all non-dropped Pull process queues in one request per Broker.
    pub async fn lock_all(&self) {
        let Ok((runtime, _)) = self.started_runtime().await else {
            return;
        };
        let mut groups =
            BTreeMap::<String, (String, HashSet<Option<SharedValue<MessageQueue>>>)>::new();
        for (queue, process_queue) in read_lock(&self.process_queues).iter() {
            if process_queue.is_dropped() {
                continue;
            }
            let (Some(broker), Some(topic)) = (queue.broker_name(), queue.topic()) else {
                continue;
            };
            groups
                .entry(broker.to_owned())
                .or_insert_with(|| (topic.to_owned(), HashSet::new()))
                .1
                .insert(Some(SharedValue::new(queue.clone())));
        }
        for (broker, (topic, queues)) in groups {
            let deadline = Instant::now() + Duration::from_millis(1_000);
            let Ok(address) = self
                .rebalance_broker_address(&runtime, &topic, &broker, deadline)
                .await
            else {
                continue;
            };
            let mut body = LockBatchRequestBody::default();
            body.set_consumer_group(Some(self.group_name()));
            body.set_client_id(Some(runtime.client_id().to_owned()));
            body.set_only_this_broker(false);
            body.set_mq_set(Some(SharedValue::new(queues.clone())));
            let Ok(Some(locked_queues)) =
                runtime.api().lock_batch_mq(&address, &body, deadline).await
            else {
                continue;
            };
            for queue in queues.into_iter().flatten() {
                let queue = queue.snapshot();
                if let Some(process_queue) = read_lock(&self.process_queues).get(&queue) {
                    let locked = locked_queues.contains(&Some(queue));
                    process_queue.set_locked(locked);
                    if locked {
                        process_queue.set_last_lock_timestamp(epoch_millis());
                    }
                }
            }
        }
    }

    /// Releases one queue lock, retaining Java's optional one-way request path.
    pub async fn unlock(
        &self,
        input_queue: &MessageQueue,
        oneway: bool,
    ) -> Result<(), PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(input_queue)?;
        let topic = queue.topic().ok_or(PullConsumerError::InvalidQueue)?;
        let broker = queue.broker_name().ok_or(PullConsumerError::InvalidQueue)?;
        let deadline = Instant::now() + Duration::from_millis(1_000);
        let address = self
            .rebalance_broker_address(&runtime, topic, broker, deadline)
            .await?;
        let mut body = UnlockBatchRequestBody::default();
        body.set_consumer_group(Some(self.group_name()));
        body.set_client_id(Some(runtime.client_id().to_owned()));
        body.set_only_this_broker(false);
        body.set_mq_set(Some(SharedValue::new(HashSet::from([Some(
            SharedValue::new(queue.clone()),
        )]))));
        runtime
            .api()
            .unlock_batch_mq(&address, &body, oneway, deadline)
            .await?;
        if let Some(process_queue) = read_lock(&self.process_queues).get(&queue) {
            process_queue.set_locked(false);
        }
        Ok(())
    }

    /// Releases all non-dropped Pull process queue locks in one request per
    /// Broker.
    pub async fn unlock_all(&self, oneway: bool) {
        let Ok((runtime, _)) = self.started_runtime().await else {
            return;
        };
        let mut groups =
            BTreeMap::<String, (String, HashSet<Option<SharedValue<MessageQueue>>>)>::new();
        for (queue, process_queue) in read_lock(&self.process_queues).iter() {
            if process_queue.is_dropped() {
                continue;
            }
            let (Some(broker), Some(topic)) = (queue.broker_name(), queue.topic()) else {
                continue;
            };
            groups
                .entry(broker.to_owned())
                .or_insert_with(|| (topic.to_owned(), HashSet::new()))
                .1
                .insert(Some(SharedValue::new(queue.clone())));
        }
        for (broker, (topic, queues)) in groups {
            let deadline = Instant::now() + Duration::from_millis(1_000);
            let Ok(address) = self
                .rebalance_broker_address(&runtime, &topic, &broker, deadline)
                .await
            else {
                continue;
            };
            let mut body = UnlockBatchRequestBody::default();
            body.set_consumer_group(Some(self.group_name()));
            body.set_client_id(Some(runtime.client_id().to_owned()));
            body.set_only_this_broker(false);
            body.set_mq_set(Some(SharedValue::new(queues.clone())));
            if runtime
                .api()
                .unlock_batch_mq(&address, &body, oneway, deadline)
                .await
                .is_ok()
            {
                for queue in queues.into_iter().flatten() {
                    if let Some(process_queue) =
                        read_lock(&self.process_queues).get(&queue.snapshot())
                    {
                        process_queue.set_locked(false);
                    }
                }
            }
        }
    }

    pub fn is_unit_mode(&self) -> bool {
        self.config.unit_mode()
    }

    pub const fn consume_type(&self) -> ConsumeType {
        ConsumeType::ConsumeActively
    }

    /// Matches `RebalancePullImpl.clientRebalance`: explicit Pull always uses
    /// the client's local assignment path.
    pub const fn client_rebalance(&self, _topic: &str) -> bool {
        true
    }

    /// Matches the deprecated Java `RebalancePullImpl.computePullFromWhere`.
    pub const fn compute_pull_from_where(&self, _queue: &MessageQueue) -> i64 {
        0
    }

    /// Matches Java's exception-capable Pull offset helper, which also returns
    /// zero for explicit Pull consumers.
    pub const fn compute_pull_from_where_with_exception(
        &self,
        _queue: &MessageQueue,
    ) -> Result<i64, PullConsumerError> {
        Ok(0)
    }

    /// Pull rebalance does not define a POP consume init mode in Java.
    pub const fn get_consume_init_mode(&self) -> Result<i32, PullConsumerError> {
        Err(PullConsumerError::PullConsumeInitModeUnsupported)
    }

    pub const fn consume_from_where(&self) -> ConsumeFromWhere {
        ConsumeFromWhere::ConsumeFromLastOffset
    }

    pub fn is_subscribe_topic_need_update(&self, topic: &str) -> bool {
        pull_subscription_info_needs_update(&self.subscriptions, &self.topic_queues, topic)
    }

    pub fn update_topic_subscribe_info(&self, topic: &str, subscribe_info: BTreeSet<MessageQueue>) {
        pull_update_topic_subscribe_info(
            &self.subscriptions,
            &self.topic_queues,
            topic,
            subscribe_info,
        );
    }

    /// Runs this consumer's rebalance immediately, as Java's raw
    /// `DefaultMQPullConsumerImpl.doRebalance` does. The async result reports
    /// lifecycle failures; individual balancing failures remain reflected by
    /// the runtime's retained assignment.
    pub async fn do_rebalance(&self) -> Result<(), PullConsumerError> {
        let registration = self.started_registration().await?;
        let _ = registration.rebalance_once().await;
        Ok(())
    }

    /// Runs this consumer's rebalance and returns Java's `tryRebalance` result.
    pub async fn try_rebalance(&self) -> Result<bool, PullConsumerError> {
        let registration = self.started_registration().await?;
        Ok(registration.rebalance_once().await)
    }

    /// Returns Java's `RebalanceImpl.doRebalance(isOrder)` result. Pull does
    /// not use ordered dispatch, so both values share the same assignment pass.
    pub async fn do_rebalance_with_order(
        &self,
        _is_order: bool,
    ) -> Result<bool, PullConsumerError> {
        self.try_rebalance().await
    }

    /// Returns the pull-consumer implementation represented by this facade.
    ///
    /// Java's deprecated `DefaultMQPullConsumer.getDefaultMQPullConsumerImpl`
    /// returns a separate implementation object. Rust keeps the implementation
    /// and facade in one owner, so this compatibility accessor returns `self`.
    #[deprecated(note = "PullConsumer is the implementation; use it directly")]
    pub fn get_default_mq_pull_consumer_impl(&self) -> &Self {
        self
    }

    /// Returns this consumer's rebalance state through a stable owner reference.
    ///
    /// Java exposes a separate `RebalancePullImpl` from its raw implementation
    /// accessor. Rust keeps the configuration, registration, routes, and
    /// assignment lifecycle in the `PullConsumer` owner, so the rebalance
    /// compatibility handle aliases that owner.
    pub fn get_rebalance_impl(&self) -> &Self {
        self
    }

    /// Returns the live subscription table exposed by Java rebalance.
    pub fn get_subscription_inner(&self) -> Arc<RwLock<HashMap<String, SubscriptionData>>> {
        self.subscriptions.clone()
    }

    /// Returns the live topic-to-queue table exposed by Java rebalance.
    pub fn get_topic_subscribe_info_table(
        &self,
    ) -> Arc<RwLock<HashMap<String, BTreeSet<MessageQueue>>>> {
        self.topic_queues.clone()
    }

    /// Matches Java's `RebalancePullImpl.createPopProcessQueue()`, which
    /// returns null because explicit Pull does not create POP assignments.
    pub const fn create_pop_process_queue(&self) -> Option<PopProcessQueue> {
        None
    }

    /// Creates the public process queue returned by Java pull rebalance.
    pub fn create_process_queue(&self) -> ProcessQueue {
        ProcessQueue::new()
    }

    /// Removes stale offset state before explicit Pull adds a new assignment.
    pub async fn remove_dirty_offset(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<(), PullConsumerError> {
        let store = self
            .get_offset_store()
            .ok_or(PullConsumerError::NotStarted)?;
        let queue = self.queue_with_namespace(input_queue)?;
        store.remove_offset(&queue);
        Ok(())
    }

    /// Persists and clears offsets when Java removes an explicit Pull queue.
    pub async fn remove_unnecessary_message_queue(
        &self,
        input_queue: &MessageQueue,
        _process_queue: &Arc<ProcessQueue>,
    ) -> Result<bool, PullConsumerError> {
        let (_, store) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(input_queue)?;
        store.persist(&queue).await?;
        store.remove_offset(&queue);
        Ok(true)
    }

    /// Pull's base rebalance implementation accepts POP cleanup without work.
    pub const fn remove_unnecessary_pop_process_queue(
        &self,
        _queue: &MessageQueue,
        _process_queue: &PopProcessQueue,
    ) -> bool {
        true
    }

    /// Invokes the configured Pull queue listener with Java's exception
    /// isolation behavior.
    pub fn message_queue_changed(
        &self,
        topic: &str,
        all_queues: Option<&HashSet<MessageQueue>>,
        divided_queues: &HashSet<MessageQueue>,
    ) {
        if let Some(listener) = self.message_queue_listener() {
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                listener(topic, all_queues, divided_queues);
            }));
        }
    }

    /// Removes a Pull assignment, marks its ProcessQueue dropped and clears
    /// the retained offset state.
    pub async fn remove_process_queue(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<bool, PullConsumerError> {
        let queue = self.queue_with_namespace(input_queue)?;
        let process_queue = write_lock(&self.process_queues).remove(&queue);
        let Some(process_queue) = process_queue else {
            return Ok(false);
        };
        process_queue.set_dropped(true);
        let _ = process_queue.drop_assignment(process_queue.generation());
        write_lock(&self.in_balance_queues).remove(&queue);
        if let Some(store) = self.get_offset_store() {
            store.persist(&queue).await?;
            store.remove_offset(&queue);
        }
        if let Some(runtime) = read_lock(&self.runtime_for_wakeup)
            .as_ref()
            .and_then(Weak::upgrade)
        {
            runtime.rebalance_immediately();
        }
        Ok(true)
    }

    /// Drops and clears Pull assignment tables without shutting down the shared
    /// client runtime, matching Java rebalance destruction.
    pub fn destroy(&self) {
        self.rebalance_active.store(false, Ordering::Release);
        for process_queue in write_lock(&self.process_queues).values() {
            let _ = process_queue.drop_assignment(process_queue.generation());
        }
        write_lock(&self.process_queues).clear();
        for process_queue in write_lock(&self.pop_process_queues).values() {
            process_queue.drop_assignment();
        }
        write_lock(&self.pop_process_queues).clear();
        write_lock(&self.in_balance_queues).clear();
    }

    pub fn get_process_queue_table(&self) -> Arc<RwLock<HashMap<MessageQueue, Arc<ProcessQueue>>>> {
        self.process_queues.clone()
    }

    pub fn get_pop_process_queue_table(
        &self,
    ) -> Arc<RwLock<HashMap<MessageQueue, Arc<PopProcessQueue>>>> {
        self.pop_process_queues.clone()
    }

    pub(crate) fn running_info_body(
        &self,
        runtime: &ClientRuntime,
        jstack: Option<String>,
    ) -> ConsumerRunningInfoBody {
        let subscriptions = read_lock(&self.subscriptions)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut body = ConsumerRunningInfoBody::default();
        body.set_properties(Some(SharedValue::new(
            crate::admin::pull_running_info_properties(
                &self.config,
                runtime,
                self.consumer_start_timestamp.load(Ordering::Acquire),
            ),
        )));
        body.set_subscription_set(Some(SharedValue::new(subscriptions)));
        body.set_jstack(jstack);
        body
    }

    /// Returns the local `ConsumerRunningInfo` snapshot exposed by Java's
    /// `DefaultMQPullConsumerImpl.consumerRunningInfo` entrypoint.
    pub fn consumer_running_info(&self) -> ConsumerRunningInfoBody {
        let mut properties = crate::admin::pull_consumer_running_info_properties(
            &self.config,
            self.consumer_start_timestamp(),
        );
        properties.insert(
            "defaultMQPullConsumerImpl".into(),
            "<implementation>".into(),
        );
        properties.insert(
            "allocateMessageQueueStrategy".into(),
            self.config
                .allocate_message_queue_strategy()
                .name()
                .to_owned(),
        );
        let topics = self.register_topics();
        properties.insert(
            "registerTopics".into(),
            format!(
                "[{}]",
                topics
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        );
        let mut registered_subscriptions = self
            .register_subscriptions()
            .iter()
            .map(SubscriptionData::java_to_string)
            .collect::<Vec<_>>();
        registered_subscriptions.sort();
        properties.insert(
            "registerSubscriptions".into(),
            format!("[{}]", registered_subscriptions.join(", ")),
        );
        if self.message_queue_listener().is_some() {
            properties.insert("messageQueueListener".into(), "<configured>".into());
        }
        if self.get_offset_store().is_some() {
            properties.insert("offsetStore".into(), "<configured>".into());
        }
        let mut body = ConsumerRunningInfoBody::default();
        body.set_properties(Some(SharedValue::new(properties)));
        body.set_subscription_set(Some(SharedValue::new(
            self.subscriptions().into_iter().collect(),
        )));
        body
    }

    /// Java initializes this timestamp when the implementation is constructed,
    /// before `start()` is called.
    pub fn consumer_start_timestamp(&self) -> i64 {
        self.consumer_start_timestamp.load(Ordering::Acquire)
    }

    pub fn service_state(&self) -> Option<ConsumerServiceState> {
        *read_lock(&self.service_state)
    }

    /// Mirrors Java's deprecated raw implementation setter, including null.
    pub fn set_service_state(&self, service_state: Option<ConsumerServiceState>) {
        *write_lock(&self.service_state) = service_state;
    }

    /// Registers a Java-compatible hook around each pull result.
    pub fn register_consume_message_hook(&self, hook: Arc<dyn ConsumeMessageHook>) {
        self.consume_message_hooks.register(hook);
    }

    pub fn execute_hook_before(&self, context: &mut ConsumeMessageContext) {
        self.consume_message_hooks.execute_before(context);
    }

    pub fn execute_hook_after(&self, context: &mut ConsumeMessageContext) {
        self.consume_message_hooks.execute_after(context);
    }

    pub fn register_filter_message_hook(&self, hook: Arc<dyn FilterMessageHook>) {
        write_lock(&self.pull_api_hooks).push(hook.clone());
    }

    pub(crate) fn configure_pull_api(&self, connect_broker_by_user: bool, default_broker_id: i64) {
        self.pull_connect_broker_by_user
            .store(connect_broker_by_user, Ordering::Release);
        self.pull_default_broker_id
            .store(default_broker_id, Ordering::Release);
        if let Some(pull_api) = self.pull_api_wrapper() {
            pull_api.set_connect_broker_by_user(connect_broker_by_user);
            pull_api.set_default_broker_id(default_broker_id);
        }
    }

    /// Returns the current Pull API wrapper, or `None` before Java creates it
    /// during startup (or after a caller clears it through the raw accessor).
    pub fn pull_api_wrapper(&self) -> Option<Arc<PullAPIWrapper>> {
        read_lock(&self.pull_api).clone()
    }

    /// Replaces the wrapper exposed by the deprecated Java implementation
    /// accessor. Startup constructs a fresh wrapper, as Java does.
    pub fn set_pull_api_wrapper(&self, pull_api: Option<Arc<PullAPIWrapper>>) {
        *write_lock(&self.pull_api) = pull_api;
    }

    fn initialize_pull_api_wrapper(&self) {
        let pull_api = Arc::new(PullAPIWrapper::new(self.config.unit_mode()));
        pull_api
            .set_connect_broker_by_user(self.pull_connect_broker_by_user.load(Ordering::Acquire));
        pull_api.set_default_broker_id(self.pull_default_broker_id.load(Ordering::Acquire));
        pull_api.set_filter_message_hooks(Arc::clone(&self.pull_api_hooks));
        *write_lock(&self.pull_api) = Some(pull_api);
    }

    fn current_pull_api_wrapper(&self) -> Result<Arc<PullAPIWrapper>, PullConsumerError> {
        self.pull_api_wrapper()
            .ok_or(PullConsumerError::PullApiWrapperUnavailable)
    }

    pub(crate) fn set_additional_heartbeat_subscriptions(
        &self,
        subscriptions: Arc<RwLock<ConsumerSubscriptionDataSet>>,
    ) {
        *write_lock(&self.additional_heartbeat_subscriptions) = subscriptions
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
    }

    pub fn message_queue_listener(&self) -> Option<MessageQueueListener> {
        read_lock(&self.message_queue_listener).clone()
    }

    pub fn set_message_queue_listener(&self, listener: Option<MessageQueueListener>) {
        *write_lock(&self.message_queue_listener) = listener;
    }

    pub fn enable_rebalance(&self) -> bool {
        self.enable_rebalance.load(Ordering::Acquire)
    }

    pub fn set_enable_rebalance(&self, enabled: bool) {
        self.enable_rebalance.store(enabled, Ordering::Release);
        self.wakeup_rebalance();
    }

    /// Returns the configured or model-selected offset store, if available.
    /// Like Java `getOffsetStore`, this is `None` until a store is configured
    /// or startup selects the default implementation.
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

    /// Select a user-supplied OffsetStore before startup. `None` restores the
    /// Java default selected from the message model.
    pub async fn set_offset_store(
        &self,
        offset_store: Option<Arc<dyn OffsetStore>>,
    ) -> Result<(), PullConsumerError> {
        let state = self.state.lock().await;
        if !matches!(&*state, State::Created) {
            return Err(match &*state {
                State::Started { .. } => PullConsumerError::OffsetStoreAfterStartup,
                State::Shutdown => PullConsumerError::Shutdown,
                State::Created => unreachable!(),
            });
        }
        *self
            .offset_store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = offset_store;
        Ok(())
    }

    pub(crate) fn register_topic(&self, topic: &str) -> Result<(), PullConsumerError> {
        let topic = self.topic_with_namespace(topic)?;
        let registered = write_lock(&self.register_topics).insert(topic.clone());
        write_lock(&self.subscriptions).insert(topic.clone(), subscription_all(&topic));
        if registered {
            self.wakeup_rebalance();
        }
        Ok(())
    }

    pub(crate) fn unregister_topic(&self, topic: &str) -> Result<bool, PullConsumerError> {
        let topic = self.topic_with_namespace(topic)?;
        let removed_registration = write_lock(&self.register_topics).remove(&topic);
        let removed_subscription = write_lock(&self.subscriptions).remove(&topic).is_some();
        let removed_registered_subscriptions = {
            let mut subscriptions = write_lock(&self.register_subscriptions);
            let before = subscriptions.len();
            subscriptions.retain(|subscription| subscription.get_topic() != Some(topic.as_str()));
            subscriptions.len() != before
        };
        if removed_registration || removed_subscription || removed_registered_subscriptions {
            self.wakeup_rebalance();
        }
        Ok(removed_registration || removed_subscription || removed_registered_subscriptions)
    }

    pub fn register_message_queue_listener(
        &self,
        topic: &str,
        listener: Option<MessageQueueListener>,
    ) -> Result<(), PullConsumerError> {
        if let Some(listener) = listener {
            self.set_message_queue_listener(Some(listener));
        }
        self.register_topic(topic)
    }

    pub fn register_topics(&self) -> BTreeSet<String> {
        read_lock(&self.register_topics).clone()
    }

    // Java exposes a Set<SubscriptionData>; its shared tag/code fields are
    // mutable even though set equality and version behavior must be retained.
    #[allow(clippy::mutable_key_type)]
    pub fn register_subscriptions(&self) -> HashSet<SubscriptionData> {
        read_lock(&self.register_subscriptions).clone()
    }

    /// Returns configured subscription data, matching
    /// `DefaultMQPullConsumerImpl.subscriptions`. Explicit subscriptions take
    /// precedence over the wildcard entries built from registered topics.
    // Preserve Java's Set<SubscriptionData> shape for this accessor.
    #[allow(clippy::mutable_key_type)]
    pub fn subscriptions(&self) -> HashSet<SubscriptionData> {
        let registered_subscriptions = read_lock(&self.register_subscriptions);
        if !registered_subscriptions.is_empty() {
            return registered_subscriptions.clone();
        }

        read_lock(&self.register_topics)
            .iter()
            .map(|topic| {
                let mut subscription = subscription_all(topic);
                subscription.set_sub_version(0);
                subscription
            })
            .collect()
    }

    pub fn add_register_subscriptions(
        &self,
        topic: &str,
        selector: Option<&MessageSelector>,
    ) -> Result<(), PullConsumerError> {
        let topic = self.topic_with_namespace(topic)?;
        let default_selector = MessageSelector::by_tag("*");
        let selector = selector.unwrap_or(&default_selector);
        let subscription = build_subscription(
            &topic,
            Some(selector.expression()),
            Some(selector.expression_type()),
        )
        .map_err(|error| PullConsumerError::InvalidSubscription(error.to_string()))?;
        write_lock(&self.register_subscriptions).insert(subscription);
        self.wakeup_rebalance();
        Ok(())
    }

    pub fn clear_register_subscriptions(&self) {
        write_lock(&self.register_subscriptions).clear();
        self.wakeup_rebalance();
    }

    pub fn set_register_topics<I, S>(&self, topics: I) -> Result<(), PullConsumerError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut normalized = BTreeSet::new();
        for topic in topics {
            normalized.insert(self.topic_with_namespace(topic.as_ref())?);
        }
        let old = std::mem::replace(&mut *write_lock(&self.register_topics), normalized.clone());
        let mut subscriptions = write_lock(&self.subscriptions);
        for topic in old.difference(&normalized) {
            subscriptions.remove(topic);
        }
        for topic in normalized {
            subscriptions.insert(topic.clone(), subscription_all(&topic));
        }
        drop(subscriptions);
        self.wakeup_rebalance();
        Ok(())
    }

    pub async fn fetch_message_queues_in_balance(
        &self,
        topic: &str,
    ) -> Result<BTreeSet<MessageQueue>, PullConsumerError> {
        let _ = self.started_runtime().await?;
        let topic = self.topic_with_namespace(topic)?;
        let queues = read_lock(&self.in_balance_queues)
            .iter()
            .filter(|queue| queue.topic() == Some(topic.as_str()))
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut client_config = self.config.client_config().clone();
        queues
            .into_iter()
            .map(|queue| {
                let topic = client_config.without_namespace(queue.topic())?;
                Ok(MessageQueue::new_nullable(
                    topic,
                    queue.broker_name().map(str::to_owned),
                    queue.queue_id(),
                ))
            })
            .collect::<Result<BTreeSet<_>, rocketmq_classic_core::config::ConfigError>>()
            .map_err(PullConsumerError::from)
    }

    /// Removes this consumer's namespace from subscribe-queue topics, matching
    /// `DefaultMQPullConsumerImpl.parseSubscribeMessageQueues`.
    pub fn parse_subscribe_message_queues(
        &self,
        queue_set: &BTreeSet<MessageQueue>,
    ) -> Result<BTreeSet<MessageQueue>, PullConsumerError> {
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
            .map_err(PullConsumerError::from)
    }

    /// Removes this consumer's namespace from message topics, matching
    /// `DefaultMQPullConsumerImpl.resetTopic`. A missing or empty list is a
    /// no-op. `MessageExt` topics are non-null in Rust, unlike Java's mutable
    /// message model.
    pub fn reset_topic(
        &self,
        messages: Option<&mut [MessageExt]>,
    ) -> Result<(), PullConsumerError> {
        let Some(messages) = messages else {
            return Ok(());
        };
        if messages.is_empty() {
            return Ok(());
        }

        let mut client_config = self.config.client_config().clone();
        for message in messages {
            if let Some(topic) = client_config.without_namespace(Some(message.topic()))? {
                message.set_topic(topic);
            }
        }
        Ok(())
    }

    /// Adds a wildcard subscription for a topic only when one is not already
    /// present, matching `DefaultMQPullConsumerImpl.subscriptionAutomatically`.
    pub fn subscription_automatically(&self, topic: &str) {
        write_lock(&self.subscriptions)
            .entry(topic.to_owned())
            .or_insert_with(|| subscription_all(topic));
    }

    /// Removes only the raw implementation subscription entry for `topic`,
    /// matching `DefaultMQPullConsumerImpl.unsubscribe`.
    pub fn unsubscribe(&self, topic: &str) {
        write_lock(&self.subscriptions).remove(topic);
    }

    /// Refreshes the route and returns the writable queues with the namespace
    /// removed, matching Pull's Java implementation accessor.
    pub async fn fetch_publish_message_queues(
        &self,
        input_topic: &str,
    ) -> Result<Vec<MessageQueue>, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        // The raw Java implementation entry point accepts the Broker topic as
        // supplied. The public DefaultMQPullConsumer wrapper does not expose
        // this method and therefore does not add a namespace before delegation.
        let topic = input_topic.to_owned();
        let timeout = Duration::from_millis(
            i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64,
        );
        let queues = runtime
            .get_topic_publish_queues(&topic, Instant::now() + timeout)
            .await
            .map_err(|source| PullConsumerError::PublishMessageQueuesFailed {
                topic: topic.clone(),
                source,
            })?;
        let Some(queues) = queues else {
            return Err(PullConsumerError::NoPublishMessageQueues(topic));
        };
        if queues.is_empty() {
            return Err(PullConsumerError::NoPublishMessageQueues(topic));
        }
        let mut client_config = self.config.client_config().clone();
        queues
            .iter()
            .map(|queue| {
                let topic = client_config.without_namespace(queue.topic())?;
                Ok(MessageQueue::new_nullable(
                    topic,
                    queue.broker_name().map(str::to_owned),
                    queue.queue_id(),
                ))
            })
            .collect::<Result<Vec<_>, rocketmq_classic_core::config::ConfigError>>()
            .map_err(PullConsumerError::from)
    }

    /// Returns the route's full subscribe-queue set, independently of this
    /// consumer's current rebalance allocation.
    pub async fn fetch_message_queues(
        &self,
        input_topic: &str,
    ) -> Result<BTreeSet<MessageQueue>, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let topic = self.topic_with_namespace(input_topic)?;
        let timeout = i64::from(self.config.client_config().mq_client_api_timeout()).max(0);
        let queues = runtime
            .get_topic_subscribe_queues(
                &topic,
                Instant::now() + Duration::from_millis(timeout as u64),
            )
            .await?
            .filter(|queues| !queues.is_empty())
            .ok_or_else(|| PullConsumerError::EmptyMessageQueues(topic.clone()))?;
        // Java MQAdminImpl.fetchSubscribeMessageQueues queries NameServer on
        // each call and does not publish this route into the shared cache.
        let mut client_config = self.config.client_config().clone();
        queues
            .into_iter()
            .map(|queue| {
                let topic = client_config.without_namespace(queue.topic())?;
                Ok(MessageQueue::new_nullable(
                    topic,
                    queue.broker_name().map(str::to_owned),
                    queue.queue_id(),
                ))
            })
            .collect::<Result<BTreeSet<_>, rocketmq_classic_core::config::ConfigError>>()
            .map_err(PullConsumerError::from)
    }

    pub async fn refresh_topic_route(&self, input_topic: &str) -> Result<(), PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let topic = self.topic_with_namespace(input_topic)?;
        let timeout = i64::from(self.config.client_config().mq_client_api_timeout()).max(0);
        runtime
            .update_topic_route(
                &topic,
                Instant::now() + Duration::from_millis(timeout as u64),
            )
            .await?;
        Ok(())
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), PullConsumerError> {
        let mut state = self.state.lock().await;
        let service_state = self.service_state();
        if service_state != Some(ConsumerServiceState::CreateJust) {
            return Err(PullConsumerError::ServiceStateNotReady(service_state));
        }
        match &*state {
            State::Started { .. } => return Err(PullConsumerError::AlreadyStarted),
            State::Shutdown => return Err(PullConsumerError::Shutdown),
            State::Created => {}
        }
        self.set_service_state(Some(ConsumerServiceState::StartFailed));
        let mut client_config = self.config.client_config().clone();
        let raw_consumer_group = self
            .config
            .consumer_group_option()
            .ok_or(PullConsumerError::InvalidConsumerGroup)?;
        let consumer_group = client_config
            .with_namespace(Some(raw_consumer_group))?
            .ok_or(PullConsumerError::InvalidConsumerGroup)?;
        *write_lock(&self.current_group_name) = consumer_group.clone();
        crate::config::validate_pull_settings(
            &consumer_group,
            self.consumer_timeout_millis_when_suspend(),
            self.broker_suspend_max_time_millis(),
        )?;

        let runtime = self
            .manager
            .get_or_create_with_hook_and_tls_config(
                &client_config,
                self.rpc_hook.clone(),
                self.config.tls_config().cloned(),
            )
            .await?;
        self.set_client_runtime(Some(runtime.clone()));
        self.initialize_pull_api_wrapper();
        let offset_store = self.select_offset_store(&runtime, &consumer_group)?;
        offset_store.load().await?;

        let registration = Arc::new(PullConsumerRegistration {
            group: consumer_group.clone(),
            message_model: self.config.message_model(),
            rebalance_message_model: self.rebalance_message_model.clone(),
            consume_from_where: self.config.consume_from_where(),
            unit_mode: self.config.unit_mode(),
            route_timeout_millis: i64::from(client_config.mq_client_api_timeout()),
            enable_rebalance: self.enable_rebalance.clone(),
            rebalance_active: self.rebalance_active.clone(),
            strategy: self.rebalance_strategy.clone(),
            message_queue_listener: self.message_queue_listener.clone(),
            register_topics: self.register_topics.clone(),
            subscriptions: self.subscriptions.clone(),
            register_subscriptions: self.register_subscriptions.clone(),
            additional_heartbeat_subscriptions: self.additional_heartbeat_subscriptions.clone(),
            topic_queues: self.topic_queues.clone(),
            in_balance_queues: self.in_balance_queues.clone(),
            offset_store: Arc::downgrade(&offset_store),
            runtime: Arc::downgrade(&runtime),
            consumer: Arc::downgrade(self),
            rebalance_execution: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            rebalance_membership: Mutex::new(HashMap::new()),
        });
        let owner: Arc<dyn ConsumerRuntime> = registration.clone();
        let lease = match runtime.register_consumer(&consumer_group, owner).await {
            Ok(lease) => lease,
            Err(error) => {
                if matches!(
                    error,
                    rocketmq_classic_core::runtime::RuntimeError::DuplicateGroup { .. }
                ) {
                    self.set_service_state(Some(ConsumerServiceState::CreateJust));
                }
                return Err(error.into());
            }
        };
        self.rebalance_active.store(true, Ordering::Release);
        if let Err(error) = runtime.start().await {
            self.rebalance_active.store(false, Ordering::Release);
            if runtime.state() != rocketmq_classic_core::runtime::RuntimeState::Failed {
                let _ = runtime
                    .unregister_consumer(&lease, self.unregister_deadline())
                    .await;
            }
            return Err(error.into());
        }
        *state = State::Started {
            runtime: Arc::downgrade(&runtime),
            lease,
            offset_store,
            registration,
        };
        self.set_service_state(Some(ConsumerServiceState::Running));
        *write_lock(&self.runtime_for_wakeup) = Some(Arc::downgrade(&runtime));
        runtime.rebalance_immediately();
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), PullConsumerError> {
        let mut state = self.state.lock().await;
        match self.service_state() {
            Some(ConsumerServiceState::Running) => {}
            Some(ConsumerServiceState::CreateJust)
            | Some(ConsumerServiceState::StartFailed)
            | Some(ConsumerServiceState::ShutdownAlready) => return Ok(()),
            None => return Err(PullConsumerError::ServiceStateNotReady(None)),
        }
        self.rebalance_active.store(false, Ordering::Release);
        let (runtime, lease, offset_store) = match &*state {
            State::Created | State::Shutdown => {
                return Err(PullConsumerError::ServiceStateNotReady(
                    self.service_state(),
                ));
            }
            State::Started {
                runtime,
                lease,
                offset_store,
                ..
            } => {
                let Some(runtime) = runtime.upgrade() else {
                    *state = State::Shutdown;
                    self.set_service_state(Some(ConsumerServiceState::ShutdownAlready));
                    return Ok(());
                };
                (runtime, lease.clone(), offset_store.clone())
            }
        };

        let queues = read_lock(&self.in_balance_queues)
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        // Java shutdown swallows a persistence error, then still unregisters.
        let _ = offset_store.persist_all(&queues).await;
        runtime
            .unregister_consumer(&lease, self.unregister_deadline())
            .await?;
        runtime.shutdown().await?;
        *state = State::Shutdown;
        self.set_service_state(Some(ConsumerServiceState::ShutdownAlready));
        *write_lock(&self.runtime_for_wakeup) = None;
        Ok(())
    }

    pub async fn is_running(&self) -> bool {
        self.service_state() == Some(ConsumerServiceState::Running)
    }

    pub async fn update_name_server_address(
        &self,
        addresses: &str,
    ) -> Result<(), PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        runtime.update_name_server_address_list(addresses);
        Ok(())
    }

    pub(crate) async fn check_client_in_broker(&self) -> Result<(), PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        runtime.check_client_in_broker().await?;
        Ok(())
    }

    pub async fn pull(
        &self,
        queue: &MessageQueue,
        subscription: Option<&str>,
        offset: i64,
        max_message_count: i32,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_timeout(
            queue,
            subscription,
            offset,
            max_message_count,
            self.consumer_pull_timeout_millis(),
        )
        .await
    }

    pub async fn pull_with_timeout(
        &self,
        queue: &MessageQueue,
        subscription: Option<&str>,
        offset: i64,
        max_message_count: i32,
        timeout_millis: i64,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_expression(
            queue,
            subscription,
            Some("TAG"),
            offset,
            max_message_count,
            MAX_PULL_MESSAGE_BYTES,
            timeout_millis,
            false,
        )
        .await
    }

    /// Pull with a caller-selected maximum response size, in bytes.
    ///
    /// The default pull methods use `i32::MAX`, matching Java's unbounded
    /// `maxSizeInBytes` default. A custom size must be positive.
    pub async fn pull_with_max_size(
        &self,
        queue: &MessageQueue,
        subscription: Option<&str>,
        offset: i64,
        max_message_count: i32,
        max_size_in_bytes: i32,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_max_size_and_timeout(
            queue,
            subscription,
            offset,
            max_message_count,
            max_size_in_bytes,
            self.consumer_pull_timeout_millis(),
        )
        .await
    }

    pub async fn pull_with_max_size_and_timeout(
        &self,
        queue: &MessageQueue,
        subscription: Option<&str>,
        offset: i64,
        max_message_count: i32,
        max_size_in_bytes: i32,
        timeout_millis: i64,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_expression(
            queue,
            subscription,
            Some("TAG"),
            offset,
            max_message_count,
            max_size_in_bytes,
            timeout_millis,
            false,
        )
        .await
    }

    pub async fn pull_with_selector(
        &self,
        queue: &MessageQueue,
        selector: &MessageSelector,
        offset: i64,
        max_message_count: i32,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_selector_with_timeout(
            queue,
            selector,
            offset,
            max_message_count,
            self.consumer_pull_timeout_millis(),
        )
        .await
    }

    pub async fn pull_selector_with_timeout(
        &self,
        queue: &MessageQueue,
        selector: &MessageSelector,
        offset: i64,
        max_message_count: i32,
        timeout_millis: i64,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_expression(
            queue,
            Some(&selector.expression),
            Some(&selector.expression_type),
            offset,
            max_message_count,
            MAX_PULL_MESSAGE_BYTES,
            timeout_millis,
            false,
        )
        .await
    }

    pub async fn pull_block_if_not_found(
        &self,
        queue: &MessageQueue,
        subscription: Option<&str>,
        offset: i64,
        max_message_count: i32,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_expression(
            queue,
            subscription,
            Some("TAG"),
            offset,
            max_message_count,
            MAX_PULL_MESSAGE_BYTES,
            self.consumer_pull_timeout_millis(),
            true,
        )
        .await
    }

    pub async fn pull_block_with_selector(
        &self,
        queue: &MessageQueue,
        selector: &MessageSelector,
        offset: i64,
        max_message_count: i32,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_expression(
            queue,
            Some(&selector.expression),
            Some(&selector.expression_type),
            offset,
            max_message_count,
            MAX_PULL_MESSAGE_BYTES,
            self.consumer_pull_timeout_millis(),
            true,
        )
        .await
    }

    pub(crate) async fn lite_pull_with_selector(
        &self,
        queue: &MessageQueue,
        selector: &MessageSelector,
        offset: i64,
        max_message_count: i32,
        timeout_millis: i64,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_expression_mode(
            queue,
            Some(&selector.expression),
            Some(&selector.expression_type),
            offset,
            max_message_count,
            MAX_PULL_MESSAGE_BYTES,
            timeout_millis,
            true,
            true,
        )
        .await
    }

    /// Search a queue by a signed Java millisecond timestamp. A successful
    /// response of `-1` is preserved as a valid empty-queue offset.
    pub async fn search_offset(
        &self,
        queue: &MessageQueue,
        timestamp_millis: i64,
    ) -> Result<i64, PullConsumerError> {
        self.search_offset_with_boundary(queue, timestamp_millis, BoundaryType::Lower)
            .await
    }

    pub async fn search_offset_with_boundary(
        &self,
        input_queue: &MessageQueue,
        timestamp_millis: i64,
        boundary_type: BoundaryType,
    ) -> Result<i64, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(input_queue)?;
        let deadline = self.offset_rpc_deadline()?;
        let (broker_name, address) = find_publish_broker(&runtime, &queue, deadline).await?;
        let mut header = SearchOffsetRequestHeader::default();
        header.set_topic(Some(
            queue
                .topic()
                .ok_or(PullConsumerError::InvalidQueue)?
                .to_owned(),
        ));
        header.set_queue_id(Some(queue.queue_id()));
        header.set_timestamp(Some(timestamp_millis));
        header.set_boundary_type(Some(boundary_type));
        header.set_broker_name(Some(broker_name));
        Ok(runtime
            .api()
            .search_offset(&address, &header, deadline)
            .await?)
    }

    pub async fn get_max_offset(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<i64, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(input_queue)?;
        let deadline = self.offset_rpc_deadline()?;
        let (broker_name, address) = find_publish_broker(&runtime, &queue, deadline).await?;
        let mut header = GetMaxOffsetRequestHeader::default();
        header.set_topic(Some(
            queue
                .topic()
                .ok_or(PullConsumerError::InvalidQueue)?
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
    ) -> Result<i64, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(input_queue)?;
        let deadline = self.offset_rpc_deadline()?;
        let (broker_name, address) = find_publish_broker(&runtime, &queue, deadline).await?;
        let mut header = GetMinOffsetRequestHeader::default();
        header.set_topic(Some(
            queue
                .topic()
                .ok_or(PullConsumerError::InvalidQueue)?
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
    ) -> Result<i64, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(input_queue)?;
        let deadline = self.offset_rpc_deadline()?;
        let (broker_name, address) = find_publish_broker(&runtime, &queue, deadline).await?;
        let mut header = GetEarliestMsgStoretimeRequestHeader::default();
        header.set_topic(Some(
            queue
                .topic()
                .ok_or(PullConsumerError::InvalidQueue)?
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
    ) -> Result<(), PullConsumerError> {
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
    ) -> Result<(), PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let mut client_config = self.config.client_config().clone();
        let topic = client_config
            .with_namespace(Some(new_topic))?
            .filter(|topic| !topic.is_empty())
            .ok_or(PullConsumerError::InvalidAdminTopic(
                "The specified topic is blank.".into(),
            ))?;
        crate::admin::validate_topic(&topic).map_err(PullConsumerError::InvalidAdminTopic)?;
        crate::admin::create_topic(&runtime, key, &topic, queue_num, topic_sys_flag).await?;
        Ok(())
    }

    pub async fn fetch_subscribe_message_queues(
        &self,
        input_topic: &str,
    ) -> Result<BTreeSet<MessageQueue>, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let mut client_config = self.config.client_config().clone();
        let topic = self.topic_with_namespace(input_topic)?;
        let timeout = Duration::from_millis(
            i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64,
        );
        let queues = crate::admin::fetch_subscribe_queues(&runtime, &topic, timeout).await?;
        if queues.is_empty() {
            return Err(crate::error::PullConsumerError::EmptyMessageQueues(topic));
        }
        queues
            .into_iter()
            .map(|queue| {
                let topic = client_config.without_namespace(queue.topic())?;
                Ok(MessageQueue::new_nullable(
                    topic,
                    queue.broker_name().map(str::to_owned),
                    queue.queue_id(),
                ))
            })
            .collect::<Result<BTreeSet<_>, rocketmq_classic_core::config::ConfigError>>()
            .map_err(PullConsumerError::from)
    }

    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Result<QueryResult, PullConsumerError> {
        self.query_messages(topic, key, max_num, begin_timestamp, end_timestamp, false)
            .await
    }

    /// Views a message by its physical ID, falling back to Java's unique-key
    /// query when parsing or the physical lookup fails.
    pub async fn view_message(
        &self,
        topic: &str,
        message_id: &str,
    ) -> Result<Option<MessageExt>, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let decoded = decode_message_id(message_id).ok().and_then(|decoded| {
            decoded
                .address()?
                .socket_addr()
                .map(|address| (address, decoded.offset()))
        });
        if let Some((address, offset)) = decoded {
            let header = {
                let mut __phase18_header_0 = ViewMessageRequestHeader::default();
                __phase18_header_0.set_topic(Some(topic.to_owned()));
                __phase18_header_0.set_offset(Some(offset));
                __phase18_header_0
            };
            let timeout_millis =
                i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64;
            if let Ok(message) = runtime
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
    ) -> Result<QueryResult, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let topic = self.topic_with_namespace(topic)?;
        let query = if unique_key {
            AdminQueryRequest::by_unique_key(&topic, key, max_num, begin_timestamp, end_timestamp)
        } else {
            AdminQueryRequest::by_key(&topic, key, max_num, begin_timestamp, end_timestamp)
        };
        let timeout_millis =
            i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64;
        let deadline = Instant::now() + Duration::from_millis(timeout_millis).saturating_mul(4);
        let result = runtime.query_message(&query, deadline).await?;
        if let Some(messages) = result.get_message_list() {
            for message in messages.snapshot().into_iter().flatten() {
                message
                    .with_mut(|message| self.reset_topic(Some(std::slice::from_mut(message))))?;
            }
        }
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    async fn pull_with_expression(
        &self,
        input_queue: &MessageQueue,
        expression: Option<&str>,
        expression_type: Option<&str>,
        offset: i64,
        max_message_count: i32,
        max_size_in_bytes: i32,
        timeout_millis: i64,
        suspend: bool,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_expression_mode(
            input_queue,
            expression,
            expression_type,
            offset,
            max_message_count,
            max_size_in_bytes,
            timeout_millis,
            suspend,
            false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn pull_with_expression_mode(
        &self,
        input_queue: &MessageQueue,
        expression: Option<&str>,
        expression_type: Option<&str>,
        offset: i64,
        max_message_count: i32,
        max_size_in_bytes: i32,
        timeout_millis: i64,
        suspend: bool,
        lite_pull: bool,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        validate_pull_request(
            input_queue,
            offset,
            max_message_count,
            max_size_in_bytes,
            timeout_millis,
        )?;

        let mut client_config = self.config.client_config().clone();
        let queue = client_config.queue_with_namespace(input_queue)?;
        let topic = queue
            .topic()
            .filter(|topic| !topic.is_empty())
            .ok_or(PullConsumerError::InvalidQueue)?;
        let broker_name = queue
            .broker_name()
            .filter(|broker| !broker.is_empty())
            .ok_or(PullConsumerError::InvalidQueue)?
            .to_owned();

        let subscription = build_subscription(topic, expression, expression_type)?;
        let is_tag = is_tag_type(subscription.get_expression_type());
        self.subscription_automatically(topic);
        let sub_version = if is_tag {
            0
        } else {
            subscription.get_sub_version()
        };
        let mut sys_flag = build_pull_sys_flag_with_lite(
            false,
            suspend,
            true,
            subscription.is_class_filter_mode(),
            lite_pull,
        );
        let pull_timeout_millis = if suspend && !lite_pull {
            self.consumer_timeout_millis_when_suspend()
        } else {
            timeout_millis
        };
        let deadline = request_deadline(pull_timeout_millis)?;

        let pull_api = self.current_pull_api_wrapper()?;
        let suggested_broker_id = pull_api.recalculate_pull_from_which_node(&queue);
        let (route, broker) =
            find_pull_broker(&runtime, &queue, suggested_broker_id, deadline).await?;
        if !is_tag && broker.version < FIRST_BROKER_SUPPORTING_EXPRESSION_FILTER {
            return Err(PullConsumerError::UnsupportedExpressionBroker {
                broker: broker_name,
                version: broker.version,
                expression_type: subscription
                    .get_expression_type()
                    .unwrap_or_default()
                    .to_owned(),
            });
        }
        if broker.slave {
            sys_flag = clear_commit_offset_flag(sys_flag);
        }

        let broker_address = if subscription.is_class_filter_mode() {
            let route_data = route
                .raw_routes
                .get(topic)
                .ok_or_else(|| PullConsumerError::FilterServerNotFound {
                    broker_address: broker.address.clone(),
                    topic: topic.to_owned(),
                })?
                .snapshot()?;
            resolve_pull_broker_address(&route_data, topic, &broker.address, sys_flag)
                .map_err(|_| PullConsumerError::FilterServerNotFound {
                    broker_address: broker.address.clone(),
                    topic: topic.to_owned(),
                })?
                .ok_or_else(|| PullConsumerError::FilterServerNotFound {
                    broker_address: broker.address.clone(),
                    topic: topic.to_owned(),
                })?
        } else {
            broker.address.clone()
        };

        let header = {
            let mut __phase18_header_1 = PullMessageRequestHeader::default();
            __phase18_header_1.set_consumer_group(Some(self.effective_consumer_group()?));
            __phase18_header_1.set_topic(Some(topic.to_owned()));
            __phase18_header_1.set_queue_id(Some(queue.queue_id()));
            __phase18_header_1.set_queue_offset(Some(offset));
            __phase18_header_1.set_max_msg_nums(Some(max_message_count));
            __phase18_header_1.set_sys_flag(Some(sys_flag));
            __phase18_header_1.set_commit_offset(Some(0));
            __phase18_header_1
                .set_suspend_timeout_millis(Some(self.broker_suspend_max_time_millis()));
            __phase18_header_1.set_subscription(subscription.get_sub_string().map(str::to_owned));
            __phase18_header_1.set_sub_version(Some(sub_version));
            __phase18_header_1
                .set_expression_type(subscription.get_expression_type().map(str::to_owned));
            __phase18_header_1.set_max_msg_bytes(Some(max_size_in_bytes));
            __phase18_header_1.set_broker_name(Some(broker_name));
            __phase18_header_1
        };
        let pull_result = runtime
            .api()
            .pull_message(&broker_address, &header, deadline)
            .await?;
        let options = DecodeOptions {
            read_body: client_config.decode_read_body(),
            decompress_body: client_config.decode_decompress_body(),
            ..DecodeOptions::default()
        };
        let mut processed =
            pull_api.process_pull_result(&queue, pull_result, &subscription, options)?;
        self.reset_topic(processed.messages.as_deref_mut())?;
        self.execute_consume_message_hooks(input_queue, &mut processed)?;
        Ok(processed)
    }

    fn execute_consume_message_hooks(
        &self,
        queue: &MessageQueue,
        processed: &mut ProcessedPullResult,
    ) -> Result<(), PullConsumerError> {
        let mut client_config = self.config.client_config().clone();
        let mut consume_context = ConsumeMessageContext::new(
            Some(self.effective_consumer_group()?),
            processed.messages.take(),
            queue.clone(),
            client_config.get_namespace()?.map(str::to_owned),
        );
        self.consume_message_hooks
            .execute_before(&mut consume_context);
        consume_context.status = Some("CONSUME_SUCCESS".to_owned());
        consume_context.success = true;
        consume_context.access_channel = client_config.access_channel();
        self.consume_message_hooks
            .execute_after(&mut consume_context);
        processed.messages = consume_context.messages;
        Ok(())
    }

    pub async fn update_consume_offset(
        &self,
        queue: &MessageQueue,
        offset: i64,
    ) -> Result<(), PullConsumerError> {
        let (_, store) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(queue)?;
        store.update_offset(&queue, offset, false);
        Ok(())
    }

    /// Sends a pulled message back using its Broker name, matching the
    /// deprecated Java two-argument `sendMessageBack` overload.
    pub async fn send_message_back(
        &self,
        message: &mut MessageExt,
        delay_level: i32,
    ) -> Result<(), PullConsumerError> {
        self.send_message_back_with_options(message, delay_level, None, None)
            .await
    }

    /// Sends a pulled message back using an explicit Broker name. `None`
    /// follows Java's null-name path and uses the message store host.
    pub async fn send_message_back_to_broker(
        &self,
        message: &mut MessageExt,
        delay_level: i32,
        broker_name: Option<&str>,
    ) -> Result<(), PullConsumerError> {
        self.send_message_back_with_options(message, delay_level, Some(broker_name), None)
            .await
    }

    /// Sends a pulled message back with Java's deprecated custom-group
    /// overload. A missing or blank group uses this consumer's group.
    pub async fn send_message_back_with_group(
        &self,
        message: &mut MessageExt,
        delay_level: i32,
        broker_name: Option<&str>,
        consumer_group: Option<&str>,
    ) -> Result<(), PullConsumerError> {
        self.send_message_back_with_options(message, delay_level, Some(broker_name), consumer_group)
            .await
    }

    async fn send_message_back_with_options(
        &self,
        message: &mut MessageExt,
        delay_level: i32,
        broker_name_override: Option<Option<&str>>,
        consumer_group_override: Option<&str>,
    ) -> Result<(), PullConsumerError> {
        let original_topic = message.topic().to_owned();
        let mut client_config = self.config.client_config().clone();
        let restored_topic = client_config
            .without_namespace(Some(&original_topic))?
            .unwrap_or_else(|| original_topic.clone());

        let result = async {
            let (runtime, _) = self.started_runtime().await?;
            let namespaced_topic = self.topic_with_namespace(&original_topic)?;
            let mut namespaced_message = message.clone();
            namespaced_message.set_topic(namespaced_topic.clone());
            let effective_broker = match broker_name_override {
                Some(broker_name) => broker_name.map(str::to_owned),
                None => namespaced_message.broker_name().map(str::to_owned),
            };
            let queue = MessageQueue::new_nullable(
                Some(namespaced_topic),
                effective_broker.clone(),
                namespaced_message.queue_id(),
            );
            let route_queue = MessageQueue::new_nullable(
                Some(namespaced_message.topic().to_owned()),
                namespaced_message.broker_name().map(str::to_owned),
                namespaced_message.queue_id(),
            );
            let address_broker_name_override = if effective_broker
                .as_deref()
                .is_some_and(|name| name.starts_with(LOGICAL_QUEUE_MOCK_BROKER_PREFIX))
            {
                let route_snapshot = runtime.route_snapshot()?;
                resolve_pull_send_back_address_broker_name_override(
                    &route_snapshot,
                    effective_broker.as_deref(),
                    &route_queue,
                )?
            } else {
                None
            };
            let helper = self.retry_helper()?;
            let effective_consumer_group = self.effective_consumer_group()?;
            let broker_consumer_group = consumer_group_override
                .filter(|group| !group.trim().is_empty())
                .unwrap_or(&effective_consumer_group);
            if helper
                .send_message_back_for_consume(
                    &runtime,
                    &queue,
                    &namespaced_message,
                    delay_level,
                    SendBackOptions {
                        broker_name_override,
                        address_broker_name_override: address_broker_name_override
                            .as_ref()
                            .map(|broker| broker.as_deref()),
                        consumer_group_override: Some(broker_consumer_group),
                        max_reconsume_times_override: Some(self.config.max_reconsume_times()),
                        timeout_millis: PULL_SEND_BACK_TIMEOUT_MILLIS,
                        orderly: false,
                    },
                )
                .await
            {
                Ok(())
            } else {
                Err(PullConsumerError::SendMessageBackFailed)
            }
        }
        .await;

        message.set_topic(restored_topic);
        result
    }

    pub async fn fetch_consume_offset(
        &self,
        queue: &MessageQueue,
        from_store: bool,
    ) -> Result<i64, PullConsumerError> {
        self.require_running_service_state()?;
        let store = self.offset_store_for_implementation_call().await?;
        let queue = self.queue_with_namespace(queue)?;
        self.read_consume_offset(store.as_ref(), &queue, from_store)
            .await
    }

    /// Reads the offset through Java's raw `DefaultMQPullConsumerImpl` entrypoint.
    /// The caller supplies the queue exactly as the implementation receives it;
    /// unlike [`Self::fetch_consume_offset`], this method does not add a namespace.
    pub async fn fetch_consume_offset_raw(
        &self,
        queue: &MessageQueue,
        from_store: bool,
    ) -> Result<i64, PullConsumerError> {
        self.require_running_service_state()?;
        let store = self.offset_store_for_implementation_call().await?;
        self.read_consume_offset(store.as_ref(), queue, from_store)
            .await
    }

    async fn read_consume_offset(
        &self,
        store: &dyn OffsetStore,
        queue: &MessageQueue,
        from_store: bool,
    ) -> Result<i64, PullConsumerError> {
        let mode = if from_store {
            ReadOffsetType::ReadFromStore
        } else {
            ReadOffsetType::MemoryFirstThenStore
        };
        Ok(store.read_offset(queue, mode).await?.java_value())
    }

    pub async fn persist_offset(&self, queue: &MessageQueue) -> Result<(), PullConsumerError> {
        let (_, store) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(queue)?;
        store.persist(&queue).await?;
        Ok(())
    }

    pub async fn persist_offsets(
        &self,
        input_queues: &[MessageQueue],
    ) -> Result<(), PullConsumerError> {
        let (_, store) = self.started_runtime().await?;
        let queues = input_queues
            .iter()
            .map(|queue| self.queue_with_namespace(queue))
            .collect::<Result<Vec<_>, _>>()?;
        store.persist_all(&queues).await?;
        Ok(())
    }

    pub async fn persist_consumer_offset(&self) -> Result<(), PullConsumerError> {
        let (_, store) = self.started_runtime().await?;
        let queues = read_lock(&self.in_balance_queues)
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        store.persist_all(&queues).await?;
        Ok(())
    }

    pub async fn update_consume_offset_to_broker(
        &self,
        queue: &MessageQueue,
        offset: i64,
        oneway: bool,
    ) -> Result<(), PullConsumerError> {
        let store = self.offset_store_for_implementation_call().await?;
        store
            .update_consume_offset_to_broker(queue, offset, oneway)
            .await?;
        Ok(())
    }

    fn select_offset_store(
        &self,
        runtime: &Arc<ClientRuntime>,
        consumer_group: &str,
    ) -> Result<Arc<dyn OffsetStore>, PullConsumerError> {
        let mut selected_store = self
            .offset_store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(store) = selected_store.clone() {
            return Ok(store);
        }
        let store: Arc<dyn OffsetStore> = match self.config.message_model() {
            MessageModel::Broadcasting => Arc::new(LocalFileOffsetStore::new(
                runtime.client_id(),
                consumer_group,
            )?),
            MessageModel::Clustering | MessageModel::LiteSelective => Arc::new(
                RemoteBrokerOffsetStore::new(runtime.clone(), consumer_group),
            ),
        };
        *selected_store = Some(store.clone());
        Ok(store)
    }

    async fn started_runtime(
        &self,
    ) -> Result<(Arc<ClientRuntime>, Arc<dyn OffsetStore>), PullConsumerError> {
        self.require_running_service_state()?;
        let state = self.state.lock().await;
        match &*state {
            State::Created => Err(PullConsumerError::NotStarted),
            State::Shutdown => Err(PullConsumerError::Shutdown),
            State::Started {
                runtime,
                offset_store,
                ..
            } => runtime
                .upgrade()
                .map(|runtime| (runtime, offset_store.clone()))
                .ok_or(PullConsumerError::Shutdown),
        }
    }

    async fn offset_store_for_implementation_call(
        &self,
    ) -> Result<Arc<dyn OffsetStore>, PullConsumerError> {
        if let Some(store) = self.get_offset_store() {
            return Ok(store);
        }
        let state = self.state.lock().await;
        match &*state {
            State::Created => Err(PullConsumerError::NotStarted),
            State::Started { offset_store, .. } => Ok(offset_store.clone()),
            State::Shutdown => Err(PullConsumerError::Shutdown),
        }
    }

    async fn started_registration(
        &self,
    ) -> Result<Arc<PullConsumerRegistration>, PullConsumerError> {
        self.require_running_service_state()?;
        let state = self.state.lock().await;
        match &*state {
            State::Created => Err(PullConsumerError::NotStarted),
            State::Shutdown => Err(PullConsumerError::Shutdown),
            State::Started { registration, .. } => Ok(registration.clone()),
        }
    }

    fn queue_with_namespace(
        &self,
        queue: &MessageQueue,
    ) -> Result<MessageQueue, PullConsumerError> {
        Ok(self
            .config
            .client_config()
            .clone()
            .queue_with_namespace(queue)?)
    }

    fn require_running_service_state(&self) -> Result<(), PullConsumerError> {
        match self.service_state() {
            Some(ConsumerServiceState::Running) => Ok(()),
            Some(ConsumerServiceState::CreateJust) => Err(PullConsumerError::NotStarted),
            Some(ConsumerServiceState::ShutdownAlready) => Err(PullConsumerError::Shutdown),
            state => Err(PullConsumerError::ServiceStateNotReady(state)),
        }
    }

    fn topic_with_namespace(&self, topic: &str) -> Result<String, PullConsumerError> {
        if topic.is_empty() {
            return Err(PullConsumerError::InvalidQueue);
        }
        self.config
            .client_config()
            .clone()
            .with_namespace(Some(topic))?
            .filter(|topic| !topic.is_empty())
            .ok_or(PullConsumerError::InvalidQueue)
    }

    fn wakeup_rebalance(&self) {
        if let Some(runtime) = read_lock(&self.runtime_for_wakeup)
            .as_ref()
            .and_then(Weak::upgrade)
        {
            runtime.rebalance_immediately();
        }
    }

    fn effective_consumer_group(&self) -> Result<String, PullConsumerError> {
        let mut client_config = self.config.client_config().clone();
        client_config
            .with_namespace(Some(
                self.config
                    .consumer_group_option()
                    .ok_or(PullConsumerError::InvalidConsumerGroup)?,
            ))?
            .ok_or(PullConsumerError::InvalidConsumerGroup)
    }

    fn retry_helper(&self) -> Result<Arc<PushConsumer>, PullConsumerError> {
        let mut retry_helper = self
            .retry_helper
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(helper) = retry_helper.as_ref() {
            return Ok(helper.clone());
        }

        let consumer_group = self
            .config
            .consumer_group_option()
            .ok_or(PullConsumerError::InvalidConsumerGroup)?;
        let mut config = PushConsumerConfig::new(consumer_group)?;
        *config.client_config_mut() = self.config.client_config().clone();
        config.set_tls_config(self.config.tls_config().cloned());
        config.set_unit_mode(self.config.unit_mode());
        config.set_max_reconsume_times(self.config.max_reconsume_times());
        let helper = Arc::new(PushConsumer::new(config));
        *retry_helper = Some(helper.clone());
        Ok(helper)
    }

    fn offset_rpc_deadline(&self) -> Result<Instant, PullConsumerError> {
        request_deadline(i64::from(
            self.config.client_config().mq_client_api_timeout(),
        ))
    }

    fn unregister_deadline(&self) -> Instant {
        Instant::now()
            + Duration::from_millis(
                self.config.client_config().mq_client_api_timeout().max(0) as u64
            )
    }
}

fn resolve_pull_send_back_address_broker_name_override(
    route: &RouteSnapshot,
    broker_name: Option<&str>,
    message_queue: &MessageQueue,
) -> Result<Option<Option<String>>, PullConsumerError> {
    if !broker_name.is_some_and(|name| name.starts_with(LOGICAL_QUEUE_MOCK_BROKER_PREFIX)) {
        return Ok(None);
    }
    Ok(Some(route.broker_name(message_queue)?.map(str::to_owned)))
}

fn pull_subscription_info_needs_update(
    subscriptions: &RwLock<HashMap<String, SubscriptionData>>,
    topic_queues: &RwLock<HashMap<String, BTreeSet<MessageQueue>>>,
    topic: &str,
) -> bool {
    read_lock(subscriptions).contains_key(topic) && !read_lock(topic_queues).contains_key(topic)
}

fn pull_update_topic_subscribe_info(
    subscriptions: &RwLock<HashMap<String, SubscriptionData>>,
    topic_queues: &RwLock<HashMap<String, BTreeSet<MessageQueue>>>,
    topic: &str,
    queues: BTreeSet<MessageQueue>,
) -> bool {
    if !read_lock(subscriptions).contains_key(topic) {
        return false;
    }
    write_lock(topic_queues).insert(topic.to_owned(), queues);
    true
}

struct PullConsumerRegistration {
    group: String,
    message_model: MessageModel,
    rebalance_message_model: Arc<RwLock<MessageModel>>,
    consume_from_where: ConsumeFromWhere,
    unit_mode: bool,
    route_timeout_millis: i64,
    enable_rebalance: Arc<AtomicBool>,
    rebalance_active: Arc<AtomicBool>,
    strategy: Arc<RwLock<Arc<dyn AllocateMessageQueueStrategy>>>,
    message_queue_listener: Arc<RwLock<Option<MessageQueueListener>>>,
    register_topics: Arc<RwLock<BTreeSet<String>>>,
    subscriptions: Arc<RwLock<HashMap<String, SubscriptionData>>>,
    register_subscriptions: RegisteredSubscriptions,
    additional_heartbeat_subscriptions: Arc<RwLock<ConsumerSubscriptionDataSet>>,
    topic_queues: Arc<RwLock<HashMap<String, BTreeSet<MessageQueue>>>>,
    in_balance_queues: Arc<RwLock<BTreeSet<MessageQueue>>>,
    offset_store: Weak<dyn OffsetStore>,
    runtime: Weak<ClientRuntime>,
    consumer: Weak<PullConsumer>,
    rebalance_execution: tokio::sync::Mutex<()>,
    #[cfg(test)]
    rebalance_membership: Mutex<HashMap<String, VecDeque<Option<Vec<String>>>>>,
}

impl ConsumerRuntime for PullConsumerRegistration {
    fn consumer_running_info(&self) -> Result<ConsumerRunningInfoBody, RuntimeError> {
        let consumer = self.consumer.upgrade().ok_or_else(|| {
            RuntimeError::ConsumerOperation("pull consumer is unavailable".into())
        })?;
        Ok(consumer.consumer_running_info())
    }

    fn consumer_status(
        &self,
        topic: Option<&str>,
    ) -> Result<HashMap<MessageQueue, i64>, RuntimeError> {
        self.offset_store
            .upgrade()
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
            ConsumerOperation::ConsumerStatus => Some(Arc::new(
                crate::admin::ConsumerStatusProcessor::new(self.offset_store.clone()),
            )),
            ConsumerOperation::RunningInfo => Some(Arc::new(
                crate::admin::ConsumerRunningInfoProcessor::for_pull(
                    self.consumer.clone(),
                    self.runtime.clone(),
                ),
            )),
            _ => None,
        }
    }

    fn heartbeat_data(&self) -> ConsumerData {
        // Java sends mutable SubscriptionData values in a HashSet; preserve
        // that identity/equality behavior for the heartbeat wire model.
        let registered_subscriptions = read_lock(&self.register_subscriptions);
        #[allow(clippy::mutable_key_type)]
        let mut subscription_data_set = if registered_subscriptions.is_empty() {
            read_lock(&self.register_topics)
                .iter()
                .map(|topic| {
                    let mut subscription = subscription_all(topic);
                    subscription.set_sub_version(0);
                    Some(SharedValue::from(subscription))
                })
                .collect::<ConsumerSubscriptionDataSet>()
        } else {
            registered_subscriptions
                .iter()
                .cloned()
                .map(|subscription| Some(SharedValue::from(subscription)))
                .collect::<ConsumerSubscriptionDataSet>()
        };
        subscription_data_set.extend(
            read_lock(&self.additional_heartbeat_subscriptions)
                .iter()
                .cloned(),
        );
        ConsumerData {
            group_name: Some(self.group.clone()),
            consume_type: Some(ConsumeType::ConsumeActively),
            message_model: Some(self.message_model),
            consume_from_where: Some(self.consume_from_where),
            subscription_data_set: Some(SharedValue::from(subscription_data_set)),
            unit_mode: self.unit_mode,
        }
    }

    fn is_subscribe_topic_need_update(&self, topic: &str) -> bool {
        pull_subscription_info_needs_update(&self.subscriptions, &self.topic_queues, topic)
    }

    fn update_topic_subscribe_info(&self, topic: &str, queues: BTreeSet<MessageQueue>) {
        if pull_update_topic_subscribe_info(&self.subscriptions, &self.topic_queues, topic, queues)
        {
            if let Some(runtime) = self.runtime.upgrade() {
                runtime.rebalance_immediately();
            }
        }
    }

    fn persist_consumer_offset(
        &self,
    ) -> Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>> {
        let store = self.offset_store.upgrade()?;
        let queues = read_lock(&self.in_balance_queues)
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        Some(Box::pin(async move {
            let _ = store.persist_all(&queues).await;
        }))
    }

    fn try_rebalance(
        &self,
    ) -> Option<std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>>> {
        Some(Box::pin(self.rebalance_once()))
    }
}

impl PullConsumerRegistration {
    async fn find_consumer_id_list(
        &self,
        runtime: &ClientRuntime,
        topic: &str,
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
        runtime.find_consumer_id_list(topic, &self.group).await
    }

    async fn rebalance_once(&self) -> bool {
        if !self.rebalance_active.load(Ordering::Acquire)
            || !self.enable_rebalance.load(Ordering::Acquire)
        {
            return true;
        }
        let rebalance_factory = self.consumer.upgrade().map(|consumer| {
            consumer
                .rebalance_client_factory
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        });
        let runtime = match rebalance_factory {
            Some(Some(runtime)) => runtime,
            Some(None) | None => self.runtime.upgrade(),
        };
        let Some(runtime) = runtime else {
            return true;
        };
        let _execution = self.rebalance_execution.lock().await;
        if !self.rebalance_active.load(Ordering::Acquire)
            || !self.enable_rebalance.load(Ordering::Acquire)
        {
            return true;
        }

        let message_model = *read_lock(&self.rebalance_message_model);
        let strategy = read_lock(&self.strategy).clone();
        let subscribed = read_lock(&self.subscriptions)
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let previous = read_lock(&self.in_balance_queues).clone();
        let mut topics = subscribed.clone();
        topics.extend(
            previous
                .iter()
                .filter_map(|queue| queue.topic().map(str::to_owned)),
        );
        let mut topics = topics.into_iter().collect::<Vec<_>>();
        topics.sort_by(|left, right| java_string_cmp(left, right));
        let mut balanced = true;

        for topic in topics {
            if !self.rebalance_active.load(Ordering::Acquire) {
                return true;
            }
            let is_subscribed = subscribed.contains(&topic);
            if !is_subscribed {
                let changed = self
                    .reconcile_topic(&topic, Some(HashSet::new()), HashSet::new())
                    .await;
                balanced &= changed;
                continue;
            }
            if message_model == MessageModel::LiteSelective {
                continue;
            }

            let mut queue_snapshot = read_lock(&self.topic_queues).get(&topic).cloned();
            if queue_snapshot.is_none() {
                let deadline =
                    Instant::now() + Duration::from_millis(self.route_timeout_millis.max(0) as u64);
                if let Err(error) = runtime.update_topic_route(&topic, deadline).await {
                    tracing::warn!(topic, error = %error, "pull rebalance route lookup failed");
                    if message_model == MessageModel::Broadcasting
                        || (message_model == MessageModel::Clustering
                            && !topic.starts_with("%RETRY%"))
                    {
                        self.notify_message_queue_listener(
                            &topic,
                            Some(HashSet::new()),
                            HashSet::new(),
                        );
                    }
                    balanced = false;
                    continue;
                }
                queue_snapshot = read_lock(&self.topic_queues).get(&topic).cloned();
            }

            match message_model {
                MessageModel::Broadcasting => {
                    let Some(queue_snapshot) = queue_snapshot else {
                        self.notify_message_queue_listener(
                            &topic,
                            Some(HashSet::new()),
                            HashSet::new(),
                        );
                        balanced = false;
                        continue;
                    };
                    let all_queues = queue_snapshot.into_iter().collect::<Vec<_>>();
                    let all = all_queues.iter().cloned().collect::<HashSet<_>>();
                    balanced &= self.reconcile_topic(&topic, Some(all.clone()), all).await;
                }
                MessageModel::Clustering => {
                    let Some(queue_snapshot) = queue_snapshot.filter(|queues| !queues.is_empty())
                    else {
                        if !topic.starts_with("%RETRY%") {
                            self.notify_message_queue_listener(
                                &topic,
                                Some(HashSet::new()),
                                HashSet::new(),
                            );
                        }
                        balanced = false;
                        continue;
                    };
                    let mut all_queues = queue_snapshot.into_iter().collect::<Vec<_>>();
                    all_queues.sort_by(java_message_queue_cmp);
                    let all = all_queues.iter().cloned().collect::<HashSet<_>>();
                    let consumer_ids = match self.find_consumer_id_list(&runtime, &topic).await {
                        Ok(Some(consumer_ids)) if !consumer_ids.is_empty() => consumer_ids,
                        Ok(Some(_)) | Ok(None) => {
                            balanced = false;
                            continue;
                        }
                        Err(error) => {
                            tracing::warn!(topic, error = %error, "pull rebalance consumer list lookup failed");
                            balanced = false;
                            continue;
                        }
                    };
                    let Some(mut consumer_ids) =
                        consumer_ids.into_iter().collect::<Option<Vec<_>>>()
                    else {
                        balanced = false;
                        continue;
                    };
                    consumer_ids.sort_by(|left, right| java_string_cmp(left, right));
                    let allocation = std::panic::catch_unwind(AssertUnwindSafe(|| {
                        strategy.allocate(
                            &self.group,
                            runtime.client_id(),
                            &all_queues,
                            &consumer_ids,
                        )
                    }));
                    let assigned = match allocation {
                        Ok(Ok(assigned)) => assigned.into_iter().collect::<HashSet<_>>(),
                        Ok(Err(error)) => {
                            tracing::warn!(topic, strategy = strategy.name(), error = %error, "pull queue allocation failed");
                            balanced = false;
                            continue;
                        }
                        Err(_) => {
                            tracing::warn!(
                                topic,
                                strategy = strategy.name(),
                                "pull queue allocation panicked"
                            );
                            balanced = false;
                            continue;
                        }
                    };
                    balanced &= self.reconcile_topic(&topic, Some(all), assigned).await;
                }
                MessageModel::LiteSelective => {}
            }
        }
        balanced
    }

    async fn reconcile_topic(
        &self,
        topic: &str,
        all_queues: Option<HashSet<MessageQueue>>,
        desired: HashSet<MessageQueue>,
    ) -> bool {
        let current = read_lock(&self.in_balance_queues)
            .iter()
            .filter(|queue| queue.topic() == Some(topic))
            .cloned()
            .collect::<HashSet<_>>();
        let removed = current.difference(&desired).cloned().collect::<Vec<_>>();
        let added = desired.difference(&current).cloned().collect::<Vec<_>>();
        let has_changes = !removed.is_empty() || !added.is_empty();
        let mut changed = false;
        let mut balanced = true;

        if let Some(store) = self.offset_store.upgrade() {
            for queue in removed {
                if !self.rebalance_active.load(Ordering::Acquire) {
                    return false;
                }
                if let Err(error) = store.persist(&queue).await {
                    tracing::warn!(queue = %queue, error = %error, "pull queue offset persistence failed during rebalance removal");
                    balanced = false;
                    continue;
                }
                if !self.rebalance_active.load(Ordering::Acquire) {
                    return false;
                }
                store.remove_offset(&queue);
                if write_lock(&self.in_balance_queues).remove(&queue) {
                    changed = true;
                    if let Some(consumer) = self.consumer.upgrade() {
                        if let Some(process_queue) =
                            write_lock(&consumer.process_queues).remove(&queue)
                        {
                            process_queue.set_dropped(true);
                        }
                        write_lock(&consumer.pop_process_queues).remove(&queue);
                    }
                }
            }

            for queue in added {
                if !self.rebalance_active.load(Ordering::Acquire) {
                    return false;
                }
                store.remove_offset(&queue);
                if write_lock(&self.in_balance_queues).insert(queue.clone()) {
                    changed = true;
                    if let Some(consumer) = self.consumer.upgrade() {
                        let generation = NEXT_PULL_PROCESS_QUEUE_GENERATION
                            .fetch_add(1, Ordering::AcqRel)
                            .max(1);
                        write_lock(&consumer.process_queues).insert(
                            queue.clone(),
                            Arc::new(ProcessQueue::new_for_assignment(queue.clone(), generation)),
                        );
                    }
                }
            }
        } else if has_changes {
            balanced = false;
        }

        if changed && self.rebalance_active.load(Ordering::Acquire) {
            let assigned = read_lock(&self.in_balance_queues)
                .iter()
                .filter(|queue| queue.topic() == Some(topic))
                .cloned()
                .collect::<HashSet<_>>();
            self.notify_message_queue_listener(topic, all_queues, assigned);
        }
        balanced
    }

    fn notify_message_queue_listener(
        &self,
        topic: &str,
        all_queues: Option<HashSet<MessageQueue>>,
        assigned: HashSet<MessageQueue>,
    ) {
        if !self.rebalance_active.load(Ordering::Acquire) {
            return;
        }
        let listener = read_lock(&self.message_queue_listener).clone();
        if let Some(listener) = listener {
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                listener(topic, all_queues.as_ref(), &assigned);
            }));
        }
    }
}

fn java_string_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

fn java_message_queue_cmp(left: &MessageQueue, right: &MessageQueue) -> std::cmp::Ordering {
    java_string_cmp(
        left.topic().unwrap_or_default(),
        right.topic().unwrap_or_default(),
    )
    .then_with(|| {
        java_string_cmp(
            left.broker_name().unwrap_or_default(),
            right.broker_name().unwrap_or_default(),
        )
    })
    .then_with(|| left.queue_id().cmp(&right.queue_id()))
}

fn subscription_all(topic: &str) -> SubscriptionData {
    let mut subscription = SubscriptionData::new(Some(topic.to_owned()), Some("*".to_owned()));
    subscription.set_sub_version(epoch_millis());
    subscription
}

pub(crate) fn build_subscription(
    topic: &str,
    expression: Option<&str>,
    expression_type: Option<&str>,
) -> Result<SubscriptionData, PullConsumerError> {
    let expression = expression.unwrap_or("");
    let is_string_tag_api = expression_type == Some("TAG");
    let expression_type = expression_type.filter(|kind| !kind.trim().is_empty());
    if expression_type.is_some_and(|kind| kind != "TAG") && expression.is_empty() {
        return Err(PullConsumerError::EmptySqlExpression);
    }
    let mut subscription = SubscriptionData::new(
        Some(topic.to_owned()),
        Some(if expression.is_empty() || expression == "*" {
            "*".to_owned()
        } else {
            expression.to_owned()
        }),
    );
    subscription.set_sub_version(epoch_millis());
    subscription.set_expression_type(expression_type.map(str::to_owned));

    let tags_api = is_string_tag_api || expression_type.is_none_or(|kind| kind == "TAG");
    if tags_api {
        let (mut tags, mut codes) = (SubscriptionTagSet::new(), SubscriptionCodeSet::new());
        if !expression.is_empty() && expression != "*" {
            for tag in expression
                .split("||")
                .map(str::trim)
                .filter(|tag| !tag.is_empty())
            {
                tags.insert(Some(tag.to_owned()));
                codes.insert(Some(java_string_hash(tag)));
            }
        }
        subscription.set_tags_set_owned(Some(tags));
        subscription.set_code_set_owned(Some(codes));
    }
    Ok(subscription)
}

fn is_tag_type(expression_type: Option<&str>) -> bool {
    expression_type
        .is_none_or(|expression_type| expression_type.is_empty() || expression_type == "TAG")
}

fn validate_pull_request(
    queue: &MessageQueue,
    offset: i64,
    max_message_count: i32,
    max_size_in_bytes: i32,
    timeout_millis: i64,
) -> Result<(), PullConsumerError> {
    if queue.topic().is_none_or(str::is_empty) || queue.broker_name().is_none_or(str::is_empty) {
        return Err(PullConsumerError::InvalidQueue);
    }
    if offset < 0 {
        return Err(PullConsumerError::InvalidOffset);
    }
    if max_message_count <= 0 {
        return Err(PullConsumerError::InvalidMaxMessageCount);
    }
    if max_size_in_bytes <= 0 {
        return Err(PullConsumerError::InvalidMaxMessageBytes);
    }
    request_deadline(timeout_millis)?;
    Ok(())
}

fn java_string_hash(value: &str) -> i32 {
    value.encode_utf16().fold(0i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

fn epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

fn request_deadline(timeout_millis: i64) -> Result<Instant, PullConsumerError> {
    let timeout_millis =
        u64::try_from(timeout_millis).map_err(|_| PullConsumerError::InvalidTimeout)?;
    Instant::now()
        .checked_add(Duration::from_millis(timeout_millis))
        .ok_or(PullConsumerError::InvalidTimeout)
}

async fn find_pull_broker(
    runtime: &ClientRuntime,
    queue: &MessageQueue,
    broker_id: i64,
    deadline: Instant,
) -> Result<(Arc<RouteSnapshot>, BrokerAddress), PullConsumerError> {
    let topic = queue.topic().ok_or(PullConsumerError::InvalidQueue)?;
    let broker_name = queue.broker_name().ok_or(PullConsumerError::InvalidQueue)?;
    let mut route = runtime.route_snapshot()?;
    let mut broker = route.subscribe_address(broker_name, broker_id, false)?;
    if broker.is_none() {
        runtime.update_topic_route(topic, deadline).await?;
        route = runtime.route_snapshot()?;
        broker = route.subscribe_address(broker_name, broker_id, false)?;
    }
    broker
        .map(|broker| (route, broker))
        .ok_or_else(|| PullConsumerError::BrokerNotFound {
            broker: broker_name.to_owned(),
        })
}

async fn find_publish_broker(
    runtime: &ClientRuntime,
    queue: &MessageQueue,
    deadline: Instant,
) -> Result<(String, String), PullConsumerError> {
    let topic = queue
        .topic()
        .filter(|topic| !topic.is_empty())
        .ok_or(PullConsumerError::InvalidQueue)?;
    let broker_name = queue
        .broker_name()
        .filter(|broker| !broker.is_empty())
        .ok_or(PullConsumerError::InvalidQueue)?
        .to_owned();
    let mut route = runtime.route_snapshot()?;
    let mut address = route.publish_address(&broker_name).map(str::to_owned);
    if address.is_none() {
        runtime.update_topic_route(topic, deadline).await?;
        route = runtime.route_snapshot()?;
        address = route.publish_address(&broker_name).map(str::to_owned);
    }
    address
        .map(|address| (broker_name.clone(), address))
        .ok_or(PullConsumerError::BrokerNotFound {
            broker: broker_name,
        })
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offset::OffsetRead;
    use rocketmq_classic_common::message::Message;
    use rocketmq_classic_common::message::constants::{
        PROPERTY_MAX_RECONSUME_TIMES, PROPERTY_RECONSUME_TIME, PROPERTY_RETRY_TOPIC,
    };
    use rocketmq_classic_core::api::consumer::PullStatus;
    use rocketmq_classic_core::config::AccessChannel;
    use rocketmq_classic_core::route::TopicSnapshot;
    use rocketmq_classic_remoting::{RemotingCommand, client::RequestProcessor};
    use std::collections::{BTreeMap, HashMap, HashSet};

    #[tokio::test]
    async fn rebalance_client_factory_setter_replaces_nullable_runtime_reference() {
        let mut config = PullConsumerConfig::new("pull-rebalance-factory-setter").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = PullConsumer::with_manager(config, manager.clone());
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
        runtime.shutdown().await.unwrap();
    }

    #[test]
    fn pull_timeouts_remain_live_after_facade_construction() {
        let config = PullConsumerConfig::new("pull-live-timeouts").unwrap();
        let consumer = PullConsumer::new(config);
        assert_eq!(consumer.broker_suspend_max_time_millis(), 20_000);
        assert_eq!(consumer.consumer_timeout_millis_when_suspend(), 30_000);
        assert_eq!(consumer.consumer_pull_timeout_millis(), 10_000);

        consumer.set_broker_suspend_max_time_millis(12_000);
        consumer.set_consumer_timeout_millis_when_suspend(15_000);
        consumer.set_consumer_pull_timeout_millis(7_500);
        assert_eq!(consumer.broker_suspend_max_time_millis(), 12_000);
        assert_eq!(consumer.consumer_timeout_millis_when_suspend(), 15_000);
        assert_eq!(consumer.consumer_pull_timeout_millis(), 7_500);
        assert_eq!(consumer.config().broker_suspend_max_time_millis(), 20_000);
        assert_eq!(
            consumer.config().consumer_timeout_millis_when_suspend(),
            30_000
        );
        assert_eq!(consumer.config().consumer_pull_timeout_millis(), 10_000);
    }

    #[tokio::test]
    async fn pull_nullable_group_is_preserved_until_start_validation() {
        let consumer = Arc::new(PullConsumer::new(
            PullConsumerConfig::new_nullable(None).unwrap(),
        ));
        assert_eq!(consumer.get_consumer_group_nullable(), None);
        assert!(matches!(
            consumer.start().await,
            Err(PullConsumerError::InvalidConsumerGroup)
        ));

        for (group, expected_error) in [
            (
                Some(String::new()),
                PullConsumerError::InvalidConsumerGroup.to_string(),
            ),
            (
                Some(crate::config::DEFAULT_CONSUMER_GROUP.to_owned()),
                PullConsumerError::DefaultConsumerGroup.to_string(),
            ),
        ] {
            let consumer = Arc::new(PullConsumer::new(
                PullConsumerConfig::new_nullable(group).unwrap(),
            ));
            assert_eq!(
                consumer.start().await.unwrap_err().to_string(),
                expected_error
            );
        }

        let mut namespaced_config = PullConsumerConfig::new("valid-pull-group").unwrap();
        namespaced_config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let namespaced_consumer = PullConsumer::new(namespaced_config);
        assert_eq!(
            namespaced_consumer.effective_consumer_group().unwrap(),
            "tenant%valid-pull-group"
        );

        let mut empty = PullConsumer::new(PullConsumerConfig::new("").unwrap());
        assert_eq!(empty.get_consumer_group_nullable().as_deref(), Some(""));
        empty.set_consumer_group_nullable(Some("pull-after-construction".to_owned()));
        assert_eq!(
            empty.get_consumer_group_nullable().as_deref(),
            Some("pull-after-construction")
        );
        empty.set_consumer_group_nullable(None);
        assert_eq!(empty.get_consumer_group_nullable(), None);
    }

    #[test]
    #[allow(deprecated)]
    fn deprecated_java_impl_accessor_returns_the_same_pull_consumer() {
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/pull-implementation-accessor.json"
        ))
        .unwrap();
        let group = expected["consumerGroup"].as_str().unwrap();
        let mut config = PullConsumerConfig::new(group).unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".into()));
        assert_eq!(
            config.max_reconsume_times(),
            expected["maxReconsumeTimesDefault"].as_i64().unwrap() as i32
        );
        let mutated_max_reconsume_times =
            expected["maxReconsumeTimesAfterMutation"].as_i64().unwrap() as i32;
        config.set_max_reconsume_times(mutated_max_reconsume_times);
        assert_eq!(config.max_reconsume_times(), mutated_max_reconsume_times);
        let consumer = PullConsumer::new(config);
        assert!(consumer.pull_api_wrapper().is_none());
        let pull_api_wrapper = Arc::new(PullAPIWrapper::new(false));
        consumer.set_pull_api_wrapper(Some(pull_api_wrapper.clone()));
        assert!(Arc::ptr_eq(
            &consumer.pull_api_wrapper().unwrap(),
            &pull_api_wrapper
        ));
        consumer.set_pull_api_wrapper(None);
        assert!(consumer.pull_api_wrapper().is_none());
        let implementation = consumer.get_default_mq_pull_consumer_impl();
        let rebalance_implementation = consumer.get_rebalance_impl();

        assert!(std::ptr::eq(implementation, &consumer));
        assert!(std::ptr::eq(
            consumer.get_default_mq_pull_consumer_impl(),
            implementation
        ));
        assert!(std::ptr::eq(rebalance_implementation, &consumer));
        assert!(std::ptr::eq(
            consumer.get_rebalance_impl(),
            rebalance_implementation
        ));
        assert_eq!(consumer.config().consumer_group(), group);
        assert!(consumer.get_offset_store().is_none());
        assert!(expected["accessorNonNull"].as_bool().unwrap());
        assert!(expected["sameImplementationAcrossCalls"].as_bool().unwrap());
        assert!(
            expected["rebalanceImplementationPresent"]
                .as_bool()
                .unwrap()
        );
        assert!(
            expected["rebalanceImplementationIdentityStable"]
                .as_bool()
                .unwrap()
        );
        assert!(expected["offsetStoreIsNullBeforeStart"].as_bool().unwrap());
        assert!(
            expected["pullApiWrapperIsNullBeforeStart"]
                .as_bool()
                .unwrap()
        );
        assert!(
            expected["pullAPIWrapperSetterRetainsIdentity"]
                .as_bool()
                .unwrap()
        );
        assert!(
            expected["pullAPIWrapperSetterAcceptsNull"]
                .as_bool()
                .unwrap()
        );

        let parse_input = expected["parseSubscribeMessageQueuesInput"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| {
                let value = value.as_str().unwrap();
                let (topic, tail) = value.split_once('|').unwrap();
                let (broker, queue_id) = tail.rsplit_once('|').unwrap();
                MessageQueue::new_nullable(
                    (topic != "null").then(|| topic.to_owned()),
                    (broker != "null").then(|| broker.to_owned()),
                    queue_id.parse().unwrap(),
                )
            })
            .collect::<BTreeSet<_>>();
        let parsed = consumer
            .parse_subscribe_message_queues(&parse_input)
            .unwrap();
        let mut parsed_keys = parsed
            .iter()
            .map(|queue| {
                format!(
                    "{}|{}|{}",
                    queue.topic().unwrap_or("null"),
                    queue.broker_name().unwrap_or("null"),
                    queue.queue_id()
                )
            })
            .collect::<Vec<_>>();
        parsed_keys.sort();
        assert_eq!(
            parsed_keys,
            expected["parseSubscribeMessageQueuesOutput"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        );

        consumer.reset_topic(None).unwrap();
        assert!(expected["resetTopicNullListNoOp"].as_bool().unwrap());
        let mut empty_messages: Vec<MessageExt> = Vec::new();
        consumer.reset_topic(Some(&mut empty_messages)).unwrap();
        assert_eq!(
            empty_messages
                .iter()
                .map(|message| message.topic().to_owned())
                .collect::<Vec<_>>(),
            expected["resetTopicEmptyListOutput"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        );
        let mut reset_messages = expected["resetTopicInput"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| {
                crate::test_support::message_ext_from(Message::new(
                    value.as_str().unwrap(),
                    Vec::new(),
                ))
            })
            .collect::<Vec<_>>();
        consumer.reset_topic(Some(&mut reset_messages)).unwrap();
        assert_eq!(
            reset_messages
                .iter()
                .map(|message| message.topic().to_owned())
                .collect::<Vec<_>>(),
            expected["resetTopicOutput"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        );

        let mut no_namespace_config = PullConsumerConfig::new(group).unwrap();
        no_namespace_config.client_config_mut().set_namespace(None);
        let no_namespace_consumer = PullConsumer::new(no_namespace_config);
        let mut no_namespace_messages = expected["resetTopicNoNamespaceInput"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| {
                crate::test_support::message_ext_from(Message::new(
                    value.as_str().unwrap(),
                    Vec::new(),
                ))
            })
            .collect::<Vec<_>>();
        no_namespace_consumer
            .reset_topic(Some(&mut no_namespace_messages))
            .unwrap();
        assert_eq!(
            no_namespace_messages
                .iter()
                .map(|message| message.topic().to_owned())
                .collect::<Vec<_>>(),
            expected["resetTopicNoNamespaceOutput"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn raw_pull_subscription_helpers_match_java_accessor_fixture() {
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/pull-implementation-accessor.json"
        ))
        .unwrap();
        let mut config = PullConsumerConfig::new("phase18-pull-implementation-accessor").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".into()));
        let consumer = PullConsumer::new(config);

        let automatic_topic = expected["subscriptionAutomaticallyInputTopic"]
            .as_str()
            .unwrap();
        consumer.subscription_automatically(automatic_topic);
        let automatic = read_lock(&consumer.subscriptions)
            .get(automatic_topic)
            .cloned()
            .unwrap();
        assert_eq!(
            automatic.get_topic(),
            expected["subscriptionAutomaticallyTopic"].as_str()
        );
        assert_eq!(
            automatic.get_sub_string(),
            expected["subscriptionAutomaticallyExpression"].as_str()
        );
        assert_eq!(
            automatic.get_expression_type(),
            expected["subscriptionAutomaticallyExpressionType"].as_str()
        );
        assert_eq!(
            automatic.is_class_filter_mode(),
            expected["subscriptionAutomaticallyClassFilterMode"]
                .as_bool()
                .unwrap()
        );
        assert!(automatic.get_sub_version() > 0);
        assert!(
            expected["subscriptionAutomaticallyVersionPositive"]
                .as_bool()
                .unwrap()
        );
        assert!(automatic.get_tags_set().unwrap().snapshot().is_empty());
        assert!(automatic.get_code_set().unwrap().snapshot().is_empty());
        assert!(
            expected["subscriptionAutomaticallyTags"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert!(
            expected["subscriptionAutomaticallyCodes"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert!(
            expected["subscriptionAutomaticallyDoesNotNamespace"]
                .as_bool()
                .unwrap()
        );

        consumer.subscription_automatically(automatic_topic);
        assert_eq!(
            read_lock(&consumer.subscriptions).get(automatic_topic),
            Some(&automatic)
        );
        assert!(
            expected["subscriptionAutomaticallyIdempotent"]
                .as_bool()
                .unwrap()
        );

        let empty_topic = expected["subscriptionAutomaticallyEmptyTopicInput"]
            .as_str()
            .unwrap();
        consumer.subscription_automatically(empty_topic);
        let empty_topic_subscription = read_lock(&consumer.subscriptions)
            .get(empty_topic)
            .cloned()
            .unwrap();
        assert_eq!(
            empty_topic_subscription.get_topic(),
            expected["subscriptionAutomaticallyEmptyTopicOutput"].as_str()
        );

        let existing_topic = expected["subscriptionAutomaticallyExistingTopic"]
            .as_str()
            .unwrap();
        let mut existing = SubscriptionData::new(
            Some(existing_topic.to_owned()),
            Some(
                expected["subscriptionAutomaticallyExistingExpression"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            ),
        );
        existing.set_expression_type(Some(
            expected["subscriptionAutomaticallyExistingExpressionType"]
                .as_str()
                .unwrap()
                .to_owned(),
        ));
        existing.set_sub_version(
            expected["subscriptionAutomaticallyExistingVersion"]
                .as_i64()
                .unwrap(),
        );
        write_lock(&consumer.subscriptions).insert(existing_topic.to_owned(), existing.clone());
        consumer.subscription_automatically(existing_topic);
        assert_eq!(
            read_lock(&consumer.subscriptions).get(existing_topic),
            Some(&existing)
        );
        assert!(
            expected["subscriptionAutomaticallyCustomEntryPreserved"]
                .as_bool()
                .unwrap()
        );

        let registered_topic = "registered-topic";
        consumer.register_topic(registered_topic).unwrap();
        let namespaced_registered_topic = expected["unsubscribeRegisteredTopic"].as_str().unwrap();
        assert!(
            consumer
                .register_topics()
                .contains(namespaced_registered_topic)
        );
        consumer.unsubscribe(automatic_topic);
        consumer.unsubscribe(empty_topic);
        consumer.unsubscribe(namespaced_registered_topic);
        consumer.unsubscribe("missing-topic");
        let subscriptions = read_lock(&consumer.subscriptions);
        assert!(!subscriptions.contains_key(automatic_topic));
        assert!(!subscriptions.contains_key(empty_topic));
        assert!(!subscriptions.contains_key(namespaced_registered_topic));
        assert!(subscriptions.contains_key(existing_topic));
        assert!(
            consumer
                .register_topics()
                .contains(namespaced_registered_topic)
        );
        assert!(expected["unsubscribeRemovedExactTopic"].as_bool().unwrap());
        assert!(expected["unsubscribeRemovedEmptyTopic"].as_bool().unwrap());
        assert!(expected["unsubscribeLeavesOtherTopic"].as_bool().unwrap());
        assert!(
            expected["unsubscribeLeavesFacadeRegistration"]
                .as_bool()
                .unwrap()
        );
    }

    #[test]
    fn raw_pull_state_getters_and_subscriptions_match_java_fixture() {
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/pull-implementation-accessor.json"
        ))
        .unwrap();
        let mut config = PullConsumerConfig::new("phase18-pull-implementation-accessor").unwrap();
        assert_eq!(config.message_model(), MessageModel::Clustering);
        assert!(!config.unit_mode());
        config
            .client_config_mut()
            .set_namespace(Some("tenant".into()));
        config.set_message_model(MessageModel::Broadcasting);
        config.set_unit_mode(true);
        let consumer = PullConsumer::new(config);

        assert_eq!(
            consumer.group_name(),
            expected["groupNameBeforeStart"].as_str().unwrap()
        );
        assert_eq!(
            consumer.message_model().name(),
            expected["messageModelAfterMutation"].as_str().unwrap()
        );
        assert_eq!(
            consumer.is_unit_mode(),
            expected["unitModeAfterMutation"].as_bool().unwrap()
        );
        assert_eq!(
            consumer.consume_type().name(),
            expected["consumeType"].as_str().unwrap()
        );
        assert_eq!(
            consumer.consume_from_where().name(),
            expected["consumeFromWhere"].as_str().unwrap()
        );
        assert_eq!(
            expected["messageModelBeforeMutation"].as_str().unwrap(),
            MessageModel::Clustering.name()
        );
        assert!(!expected["unitModeBeforeMutation"].as_bool().unwrap());

        let rows = |subscriptions: HashSet<SubscriptionData>| {
            let mut rows = subscriptions
                .into_iter()
                .map(|subscription| {
                    let tags = subscription
                        .get_tags_set()
                        .map(|set| {
                            let mut values = set
                                .snapshot()
                                .into_iter()
                                .map(|value| value.unwrap_or_else(|| "null".to_owned()))
                                .collect::<Vec<_>>();
                            values.sort();
                            format!("[{}]", values.join(", "))
                        })
                        .unwrap_or_else(|| "null".to_owned());
                    let codes = subscription
                        .get_code_set()
                        .map(|set| {
                            let mut values = set
                                .snapshot()
                                .into_iter()
                                .map(|value| {
                                    value.map_or_else(
                                        || "null".to_owned(),
                                        |value| value.to_string(),
                                    )
                                })
                                .collect::<Vec<_>>();
                            values.sort();
                            format!("[{}]", values.join(", "))
                        })
                        .unwrap_or_else(|| "null".to_owned());
                    let version = if subscription.get_sub_version() == 0 {
                        "0"
                    } else {
                        "nonzero"
                    };
                    format!(
                        "{}|{}|{}|{}|{}|{}|{}",
                        subscription.get_topic().unwrap_or("null"),
                        subscription.get_sub_string().unwrap_or("null"),
                        subscription.get_expression_type().unwrap_or("null"),
                        subscription.is_class_filter_mode(),
                        version,
                        tags,
                        codes
                    )
                })
                .collect::<Vec<_>>();
            rows.sort();
            rows
        };
        let expected_rows = |field: &str| {
            expected[field]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        };

        assert_eq!(
            rows(consumer.subscriptions()),
            expected_rows("subscriptionsInitially")
        );
        consumer.register_topic("registered-topic").unwrap();
        assert_eq!(
            rows(consumer.subscriptions()),
            expected_rows("subscriptionsFromTopics")
        );
        let selector = MessageSelector::by_tag("red");
        consumer
            .add_register_subscriptions("explicit-topic", Some(&selector))
            .unwrap();
        assert_eq!(consumer.register_subscriptions().len(), 1);
        assert_eq!(
            rows(consumer.subscriptions()),
            expected_rows("subscriptionsFromExplicit")
        );

        let mut first_version =
            build_subscription("duplicate-topic", Some("red"), Some("TAG")).unwrap();
        first_version.set_sub_version(11);
        let mut second_version = first_version.clone();
        second_version.set_sub_version(12);
        {
            let mut registered = write_lock(&consumer.register_subscriptions);
            registered.clear();
            registered.insert(first_version);
            registered.insert(second_version);
        }
        assert_eq!(consumer.register_subscriptions().len(), 2);
        assert_eq!(
            rows(consumer.subscriptions()),
            expected_rows("subscriptionsWithSameSpecDifferentVersions")
        );
    }

    #[test]
    fn pull_sendback_resolves_logical_broker_through_topic_endpoints() {
        let logical_queue = MessageQueue::new("tenant%orders", "__syslo__logical", 2);
        let topic = TopicSnapshot {
            route: Arc::new(rocketmq_classic_protocol::route::TopicRouteData::default()),
            publish_queues: Vec::new(),
            subscribe_queues: BTreeSet::new(),
            endpoints: BTreeMap::from([(logical_queue.clone(), "broker-a".to_owned())]),
            order_topic: false,
        };
        let mut route = RouteSnapshot::default();
        route
            .topics
            .insert("tenant%orders".to_owned(), Arc::new(topic));

        assert_eq!(
            resolve_pull_send_back_address_broker_name_override(
                &route,
                Some("__syslo__logical"),
                &logical_queue,
            )
            .unwrap(),
            Some(Some("broker-a".to_owned()))
        );
        assert_eq!(
            resolve_pull_send_back_address_broker_name_override(
                &route,
                Some("broker-a"),
                &logical_queue,
            )
            .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn pull_sendback_matches_java_retry_fallback_and_restores_message_topic() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/pull-implementation-accessor.json"
        ))
        .unwrap();
        let mut config = PullConsumerConfig::new("pull-sendback").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        config.client_config_mut().set_namesrv_addr(None);
        config.set_max_reconsume_times(-3);
        let consumer = Arc::new(PullConsumer::with_manager(
            config,
            ClientRuntimeManager::new(),
        ));
        consumer.register_topic("orders").unwrap();
        consumer.start().await.unwrap();

        let helper = consumer.retry_helper().unwrap();
        {
            let mut scripted = helper.scripted_retry_transport.lock().unwrap();
            scripted
                .broker_results
                .extend([Ok(()), Err("broker refused send-back".into())]);
            scripted.retry_results.push_back(Ok(()));
        }

        let mut message =
            crate::test_support::message_ext_from(Message::new("orders", b"payload".to_vec()));
        message.set_broker_name(Some("broker-a".to_owned()));
        message.set_queue_id(2);
        message.set_commit_log_offset(71);
        message.set_message_id(Some("origin-message-id".to_owned()));
        message.set_reconsume_times(4);

        consumer.send_message_back(&mut message, 2).await.unwrap();
        assert_eq!(message.topic(), "orders");
        consumer
            .send_message_back_with_group(
                &mut message,
                5,
                Some("broker-override"),
                Some("override-group"),
            )
            .await
            .unwrap();
        assert_eq!(message.topic(), "orders");

        {
            let scripted = helper.scripted_retry_transport.lock().unwrap();
            assert_eq!(scripted.broker_headers.len(), 2);
            assert_eq!(scripted.broker_timeouts_millis, vec![3_000, 3_000]);
            assert_eq!(
                fixture["pullSendBackBrokerGroups"][0].as_str(),
                Some(scripted.broker_headers[0].get_group())
            );
            assert_eq!(
                scripted.broker_headers[0].get_origin_topic(),
                "tenant%orders"
            );
            assert_eq!(
                scripted.broker_headers[0].get_broker_name(),
                Some("broker-a")
            );
            assert_eq!(scripted.broker_headers[0].get_offset(), 71);
            assert_eq!(
                scripted.broker_headers[0].get_max_reconsume_times(),
                Some(-3)
            );
            assert_eq!(
                fixture["pullSendBackBrokerGroups"][1].as_str(),
                Some(scripted.broker_headers[1].get_group())
            );
            assert_eq!(
                scripted.broker_headers[1].get_broker_name(),
                Some("broker-override")
            );
            assert_eq!(scripted.broker_headers[1].get_delay_level(), 5);
            let retry = &scripted.retry_messages[0];
            assert_eq!(
                retry.topic(),
                fixture["pullSendBackFallbackRetryTopics"][0]
                    .as_str()
                    .unwrap()
            );
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
                    .get(PROPERTY_MAX_RECONSUME_TIMES)
                    .map(String::as_str),
                Some("-3")
            );
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_RECONSUME_TIME)
                    .map(String::as_str),
                Some("5")
            );
        }

        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn pull_running_info_uses_group_and_reports_active_consumer_properties() {
        let mut config = PullConsumerConfig::new("pull-running-info").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PullConsumer::with_manager(config, manager.clone()));
        consumer.register_topic("orders").unwrap();
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        consumer
            .consumer_start_timestamp
            .store(1_700_000_000_000, Ordering::Release);

        let processor = crate::admin::ConsumerRunningInfoProcessor::for_pull(
            Arc::downgrade(&consumer),
            Arc::downgrade(&runtime),
        );
        let response = processor
            .process(RemotingCommand {
                ext_fields: Some(
                    BTreeMap::from([
                        ("consumerGroup".into(), "pull-running-info".into()),
                        ("clientId".into(), "client-id-is-ignored".into()),
                        ("jstackEnable".into(), "false".into()),
                    ])
                    .into(),
                ),
                ..Default::default()
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            response.code,
            rocketmq_classic_protocol::codes::response::SUCCESS
        );
        let body = ConsumerRunningInfoBody::decode_java_json(
            std::str::from_utf8(response.body.as_deref().unwrap()).unwrap(),
        )
        .unwrap();
        assert_eq!(body["properties"]["PROP_CONSUME_TYPE"], "CONSUME_ACTIVELY");
        assert_eq!(body["properties"]["consumerPullTimeoutMillis"], "10000");
        assert_eq!(
            body["properties"]["PROP_CONSUMER_START_TIMESTAMP"],
            "1700000000000"
        );
        assert_eq!(body["subscriptionSet"].as_array().unwrap().len(), 1);
        assert_eq!(body["mqTable"], serde_json::json!({}));
        assert_eq!(body["mqPopTable"], serde_json::json!({}));
        assert!(body.get("jstack").is_none());

        runtime.shutdown().await.unwrap();
    }

    #[test]
    fn pull_consume_hooks_receive_empty_pull_context_with_java_success_state() {
        struct Hook(Arc<Mutex<Vec<String>>>);

        impl ConsumeMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "pull-consume-hook"
            }

            fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
                assert!(context.messages.is_none());
                assert!(!context.success);
                assert!(context.properties.is_none());
                assert!(context.access_channel.is_none());
                assert_eq!(
                    context.consumer_group.as_deref(),
                    Some("tenant%pull-hook-lifecycle")
                );
                assert_eq!(context.namespace.as_deref(), Some("tenant"));
                context.properties = Some(HashMap::new());
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
                assert_eq!(context.access_channel, Some(AccessChannel::Local));
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

        let mut config = PullConsumerConfig::new("pull-hook-lifecycle").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".into()));
        let consumer = PullConsumer::new(config);
        let events = Arc::new(Mutex::new(Vec::new()));
        consumer.register_consume_message_hook(Arc::new(Hook(events.clone())));
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let mut processed = ProcessedPullResult {
            result: rocketmq_classic_core::api::consumer::PullResultExt {
                status: PullStatus::NoNewMessage,
                next_begin_offset: 0,
                min_offset: 0,
                max_offset: 0,
                suggest_which_broker_id: 0,
                offset_delta: None,
                raw_messages: None,
            },
            messages: None,
        };

        consumer
            .execute_consume_message_hooks(&queue, &mut processed)
            .unwrap();

        assert_eq!(events.lock().unwrap().as_slice(), ["before", "after"]);
        assert!(processed.messages.is_none());
    }

    #[test]
    fn pull_raw_hook_entrypoints_share_the_supplied_context() {
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/pull-implementation-accessor.json"
        ))
        .unwrap();
        struct Hook(Arc<Mutex<Vec<String>>>);

        impl ConsumeMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "pull-raw-hook-entrypoint"
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

        let consumer =
            PullConsumer::new(PullConsumerConfig::new("pull-raw-hook-entrypoint").unwrap());
        let events = Arc::new(Mutex::new(Vec::new()));
        consumer.register_consume_message_hook(Arc::new(Hook(events.clone())));
        let mut context = ConsumeMessageContext::new(
            Some("pull-raw-hook-entrypoint".to_owned()),
            None,
            MessageQueue::new("orders", "broker-a", 0),
            None,
        );
        context.status = Some("seed".to_owned());

        consumer.execute_hook_before(&mut context);
        assert_eq!(context.status.as_deref(), Some("hook-before"));
        context.success = true;
        consumer.execute_hook_after(&mut context);

        let expected_events = expected["directHookEvents"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event.as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(*events.lock().unwrap(), expected_events);
    }

    #[derive(Default)]
    struct TestOffsetStore {
        values: Mutex<HashMap<MessageQueue, i64>>,
        persisted: Mutex<Vec<MessageQueue>>,
        calls: Mutex<Vec<String>>,
    }

    impl OffsetStore for TestOffsetStore {
        fn load(&self) -> crate::offset::OffsetStoreFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn update_offset(&self, queue: &MessageQueue, offset: i64, _increase_only: bool) {
            self.values.lock().unwrap().insert(queue.clone(), offset);
        }

        fn update_and_freeze_offset(&self, queue: &MessageQueue, offset: i64) {
            self.update_offset(queue, offset, false);
        }

        fn read_offset<'a>(
            &'a self,
            queue: &'a MessageQueue,
            mode: ReadOffsetType,
        ) -> crate::offset::OffsetStoreFuture<'a, OffsetRead> {
            Box::pin(async move {
                let mode = match mode {
                    ReadOffsetType::ReadFromMemory => "READ_FROM_MEMORY",
                    ReadOffsetType::ReadFromStore => "READ_FROM_STORE",
                    ReadOffsetType::MemoryFirstThenStore => "MEMORY_FIRST_THEN_STORE",
                };
                self.calls.lock().unwrap().push(format!(
                    "readOffset|{}|{}",
                    queue_key_for_fixture(queue),
                    mode
                ));
                Ok(self
                    .values
                    .lock()
                    .unwrap()
                    .get(queue)
                    .copied()
                    .map_or(OffsetRead::NotFound, OffsetRead::Found))
            })
        }

        fn persist<'a>(
            &'a self,
            queue: &'a MessageQueue,
        ) -> crate::offset::OffsetStoreFuture<'a, ()> {
            Box::pin(async move {
                self.persisted.lock().unwrap().push(queue.clone());
                Ok(())
            })
        }

        fn persist_all<'a>(
            &'a self,
            queues: &'a [MessageQueue],
        ) -> crate::offset::OffsetStoreFuture<'a, ()> {
            Box::pin(async move {
                self.persisted
                    .lock()
                    .unwrap()
                    .extend(queues.iter().cloned());
                Ok(())
            })
        }

        fn remove_offset(&self, queue: &MessageQueue) {
            self.values.lock().unwrap().remove(queue);
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
            queue: &'a MessageQueue,
            offset: i64,
            oneway: bool,
        ) -> crate::offset::OffsetStoreFuture<'a, ()> {
            Box::pin(async move {
                self.calls.lock().unwrap().push(format!(
                    "updateConsumeOffsetToBroker|{}|{offset}|{oneway}",
                    queue_key_for_fixture(queue)
                ));
                Ok(())
            })
        }
    }

    fn queue_key_for_fixture(queue: &MessageQueue) -> String {
        format!(
            "{}|{}|{}",
            queue.topic().unwrap_or("null"),
            queue.broker_name().unwrap_or("null"),
            queue.queue_id()
        )
    }

    #[test]
    fn string_tag_subscription_matches_java_filter_api_and_hashing() {
        let all = build_subscription("orders", None, Some("TAG")).unwrap();
        assert_eq!(all.get_sub_string(), Some("*"));
        assert_eq!(all.get_tags_set().unwrap().snapshot(), HashSet::new());

        let tags = build_subscription("orders", Some("red || blue || red"), Some("TAG")).unwrap();
        assert_eq!(tags.get_sub_string(), Some("red || blue || red"));
        assert_eq!(
            tags.get_tags_set().unwrap().snapshot(),
            HashSet::from([Some("red".to_owned()), Some("blue".to_owned())])
        );
        assert_eq!(
            java_string_hash("blue"),
            "blue".chars().fold(0i32, |hash, ch| {
                hash.wrapping_mul(31).wrapping_add(ch as i32)
            })
        );
    }

    #[test]
    fn selector_requires_sql_expression_but_tag_null_means_all() {
        assert!(matches!(
            build_subscription("orders", Some(""), Some("SQL92")),
            Err(PullConsumerError::EmptySqlExpression)
        ));
        let selector = MessageSelector::by_tag("");
        let all = build_subscription("orders", Some(&selector.expression), Some("TAG")).unwrap();
        assert_eq!(all.get_sub_string(), Some("*"));
    }

    #[test]
    fn expression_type_tag_detection_matches_java() {
        assert!(is_tag_type(None));
        assert!(is_tag_type(Some("")));
        assert!(is_tag_type(Some("TAG")));
        assert!(!is_tag_type(Some("SQL92")));
        assert!(!is_tag_type(Some("CUSTOM")));
    }

    #[test]
    fn namespace_is_applied_to_the_consumer_group_and_registered_topic() {
        let mut config = PullConsumerConfig::new("orders-group").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let consumer = PullConsumer::new(config);

        assert_eq!(
            consumer.effective_consumer_group().unwrap(),
            "tenant%orders-group"
        );
        consumer.register_topic("orders").unwrap();
        assert!(read_lock(&consumer.subscriptions).contains_key("tenant%orders"));
        assert_eq!(
            consumer.register_topics(),
            BTreeSet::from(["tenant%orders".to_owned()])
        );
    }

    #[test]
    fn pull_rebalance_settings_default_to_java_values_and_registration_sets_listener() {
        let mut config = PullConsumerConfig::new("pull-rebalance-settings").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        assert!(config.enable_rebalance());
        assert_eq!(config.allocate_message_queue_strategy().name(), "AVG");
        assert!(config.message_queue_listener().is_none());

        let consumer = PullConsumer::new(config);
        let changes = Arc::new(Mutex::new(Vec::new()));
        let recorded_changes = changes.clone();
        let listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
            recorded_changes.lock().unwrap().push((
                topic.to_owned(),
                all.cloned(),
                assigned.clone(),
            ));
        });
        consumer
            .register_message_queue_listener("orders", Some(listener.clone()))
            .unwrap();

        assert_eq!(
            consumer.register_topics(),
            BTreeSet::from(["tenant%orders".to_owned()])
        );
        assert!(Arc::ptr_eq(
            consumer.message_queue_listener().as_ref().unwrap(),
            &listener
        ));
        let selector = MessageSelector::by_sql("color = 'blue'");
        consumer
            .add_register_subscriptions("events", Some(&selector))
            .unwrap();
        #[allow(clippy::mutable_key_type)]
        let registered_subscriptions = consumer.register_subscriptions();
        assert_eq!(registered_subscriptions.len(), 1);
        let registered_subscription = registered_subscriptions.iter().next().unwrap();
        assert_eq!(registered_subscription.get_topic(), Some("tenant%events"));
        assert_eq!(registered_subscription.get_expression_type(), Some("SQL92"));
        consumer.clear_register_subscriptions();
        assert!(consumer.register_subscriptions().is_empty());
        consumer.set_enable_rebalance(false);
        assert!(!consumer.enable_rebalance());
    }

    #[test]
    fn pull_subscription_info_callbacks_follow_java_subscription_and_route_tables() {
        let config = PullConsumerConfig::new("pull-subscribe-info-callbacks").unwrap();
        let offset_store = Arc::new(TestOffsetStore::default());
        let offset_store_trait: Arc<dyn OffsetStore> = offset_store;
        let subscriptions = Arc::new(RwLock::new(HashMap::from([
            (
                "registered-topic".to_owned(),
                subscription_all("registered-topic"),
            ),
            (
                "automatic-topic".to_owned(),
                subscription_all("automatic-topic"),
            ),
        ])));
        let registration = PullConsumerRegistration {
            group: "pull-subscribe-info-callbacks".to_owned(),
            message_model: MessageModel::Clustering,
            rebalance_message_model: Arc::new(RwLock::new(MessageModel::Clustering)),
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            unit_mode: false,
            route_timeout_millis: 100,
            enable_rebalance: Arc::new(AtomicBool::new(true)),
            rebalance_active: Arc::new(AtomicBool::new(false)),
            strategy: Arc::new(RwLock::new(config.allocate_message_queue_strategy())),
            message_queue_listener: Arc::new(RwLock::new(None)),
            register_topics: Arc::new(RwLock::new(BTreeSet::from(["registered-topic".to_owned()]))),
            subscriptions: subscriptions.clone(),
            register_subscriptions: Arc::new(RwLock::new(HashSet::new())),
            additional_heartbeat_subscriptions: Arc::new(RwLock::new(
                ConsumerSubscriptionDataSet::new(),
            )),
            topic_queues: Arc::new(RwLock::new(HashMap::new())),
            in_balance_queues: Arc::new(RwLock::new(BTreeSet::new())),
            offset_store: Arc::downgrade(&offset_store_trait),
            runtime: Weak::new(),
            consumer: Weak::new(),
            rebalance_execution: tokio::sync::Mutex::new(()),
            rebalance_membership: Mutex::new(HashMap::new()),
        };

        assert!(!registration.is_subscribe_topic_need_update("unregistered-topic"));
        assert!(registration.is_subscribe_topic_need_update("registered-topic"));
        assert!(registration.is_subscribe_topic_need_update("automatic-topic"));

        let queue = MessageQueue::new("unregistered-topic", "broker-a", 0);
        registration
            .update_topic_subscribe_info("unregistered-topic", BTreeSet::from([queue.clone()]));
        assert!(!read_lock(&registration.topic_queues).contains_key("unregistered-topic"));

        let subscribed_queue = MessageQueue::new("registered-topic", "broker-a", 0);
        registration.update_topic_subscribe_info(
            "registered-topic",
            BTreeSet::from([subscribed_queue.clone()]),
        );
        assert_eq!(
            read_lock(&registration.topic_queues).get("registered-topic"),
            Some(&BTreeSet::from([subscribed_queue]))
        );
        assert!(!registration.is_subscribe_topic_need_update("registered-topic"));
    }

    #[tokio::test]
    async fn pull_persist_consumer_offset_uses_only_current_assignment() {
        let mut config = PullConsumerConfig::new("pull-persist-assigned-offsets").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config
            .client_config_mut()
            .set_persist_consumer_offset_interval(60_000);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PullConsumer::with_manager(config, manager));
        let store = Arc::new(TestOffsetStore::default());
        let store_trait: Arc<dyn OffsetStore> = store.clone();
        consumer.set_offset_store(Some(store_trait)).await.unwrap();
        consumer.start().await.unwrap();

        let assigned = MessageQueue::new("orders", "broker-a", 0);
        let unassigned = MessageQueue::new("orders", "broker-a", 1);
        store.update_offset(&assigned, 10, false);
        store.update_offset(&unassigned, 20, false);
        write_lock(&consumer.in_balance_queues).insert(assigned.clone());

        consumer.persist_consumer_offset().await.unwrap();
        assert_eq!(*store.persisted.lock().unwrap(), vec![assigned.clone()]);

        store.persisted.lock().unwrap().clear();
        consumer.shutdown().await.unwrap();
        assert_eq!(*store.persisted.lock().unwrap(), vec![assigned]);
    }

    #[tokio::test]
    async fn pull_raw_rebalance_entrypoints_update_and_use_subscription_info() {
        let mut config = PullConsumerConfig::new("pull-raw-rebalance-entrypoints").unwrap();
        config.set_message_model(MessageModel::Broadcasting);
        config.set_enable_rebalance(false);
        config.client_config_mut().set_namesrv_addr(None);
        config
            .client_config_mut()
            .set_persist_consumer_offset_interval(60_000);
        let consumer = Arc::new(PullConsumer::with_manager(
            config,
            ClientRuntimeManager::new(),
        ));
        consumer.register_topic("orders").unwrap();
        consumer.start().await.unwrap();

        assert!(consumer.is_subscribe_topic_need_update("orders"));
        assert!(!consumer.is_subscribe_topic_need_update("not-subscribed"));
        let assigned = MessageQueue::new("orders", "broker-a", 0);
        consumer.update_topic_subscribe_info("orders", BTreeSet::from([assigned.clone()]));
        consumer.update_topic_subscribe_info(
            "not-subscribed",
            BTreeSet::from([MessageQueue::new("not-subscribed", "broker-a", 0)]),
        );
        assert!(!consumer.is_subscribe_topic_need_update("orders"));
        assert!(!read_lock(&consumer.topic_queues).contains_key("not-subscribed"));

        assert!(consumer.try_rebalance().await.unwrap());
        assert!(read_lock(&consumer.in_balance_queues).is_empty());
        consumer.set_enable_rebalance(true);
        assert!(consumer.try_rebalance().await.unwrap());
        assert_eq!(
            read_lock(&consumer.in_balance_queues).clone(),
            BTreeSet::from([assigned.clone()])
        );
        consumer.do_rebalance().await.unwrap();
        assert_eq!(
            read_lock(&consumer.in_balance_queues).clone(),
            BTreeSet::from([assigned])
        );
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn pull_raw_offset_entrypoints_match_java_queue_and_lifecycle_boundaries() {
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/pull-implementation-accessor.json"
        ))
        .unwrap();
        let mut config = PullConsumerConfig::new("phase18-pull-implementation-accessor").unwrap();
        config.set_message_model(MessageModel::Broadcasting);
        config.set_enable_rebalance(false);
        config
            .client_config_mut()
            .set_namespace(Some("tenant".into()));
        config.client_config_mut().set_namesrv_addr(None);
        config
            .client_config_mut()
            .set_persist_consumer_offset_interval(60_000);
        let consumer = Arc::new(PullConsumer::with_manager(
            config,
            ClientRuntimeManager::new(),
        ));
        let store = Arc::new(TestOffsetStore::default());
        let store_trait: Arc<dyn OffsetStore> = store.clone();
        consumer.set_offset_store(Some(store_trait)).await.unwrap();

        let raw_queue = MessageQueue::new("raw-offset-topic", "broker-offset", 3);
        let namespaced_queue = MessageQueue::new("tenant%raw-offset-topic", "broker-offset", 3);
        store.update_offset(&raw_queue, 73, false);
        store.update_offset(&namespaced_queue, 73, false);

        // The raw Java implementation update call has no running-state check
        // and forwards the supplied queue directly to OffsetStore.
        consumer
            .update_consume_offset_to_broker(&raw_queue, 37, true)
            .await
            .unwrap();
        consumer.set_service_state(Some(ConsumerServiceState::Running));
        assert_eq!(
            consumer
                .fetch_consume_offset_raw(&raw_queue, false)
                .await
                .unwrap(),
            73
        );
        assert_eq!(
            consumer
                .fetch_consume_offset(&raw_queue, true)
                .await
                .unwrap(),
            73
        );
        consumer.set_service_state(Some(ConsumerServiceState::CreateJust));
        assert!(matches!(
            consumer.fetch_consume_offset_raw(&raw_queue, false).await,
            Err(PullConsumerError::NotStarted)
        ));

        consumer.start().await.unwrap();

        let expected_calls = expected["rawOffsetStoreCalls"]
            .as_array()
            .unwrap()
            .iter()
            .map(|call| call.as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(*store.calls.lock().unwrap(), expected_calls);
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn pull_consumer_running_info_and_start_timestamp_match_java_fixture() {
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/pull-implementation-accessor.json"
        ))
        .unwrap();
        let mut config = PullConsumerConfig::new("phase18-pull-implementation-accessor").unwrap();
        config.set_message_model(MessageModel::Broadcasting);
        config.set_unit_mode(true);
        config.set_max_reconsume_times(-3);
        config
            .client_config_mut()
            .set_namespace(Some("tenant".into()));
        config.client_config_mut().set_namesrv_addr(None);
        config
            .client_config_mut()
            .set_client_callback_executor_threads(3);
        config
            .client_config_mut()
            .set_concurrent_heartbeat_thread_pool_size(8);
        config.client_config_mut().set_use_tls(false);
        let consumer = Arc::new(PullConsumer::with_manager(
            config,
            ClientRuntimeManager::new(),
        ));
        assert!(
            consumer
                .config()
                .client_config()
                .enable_stream_request_type()
        );

        let start_timestamp = consumer.consumer_start_timestamp();
        assert!(start_timestamp > 0);
        assert!(
            expected["consumerStartTimestampPositive"]
                .as_bool()
                .unwrap()
        );
        let info = consumer.consumer_running_info();
        let mut normalized_properties = info.get_properties().unwrap().snapshot();
        if normalized_properties.contains_key("clientIP") {
            normalized_properties.insert("clientIP".into(), "<present>".into());
        }
        normalized_properties.insert("PROP_CONSUMER_START_TIMESTAMP".into(), "<timestamp>".into());
        assert_eq!(
            serde_json::to_value(&normalized_properties).unwrap(),
            expected["directRunningInfoProperties"]
        );
        assert_eq!(
            info.get_subscription_set().unwrap().with(Vec::len),
            expected["directRunningInfoSubscriptions"]
                .as_array()
                .unwrap()
                .len()
        );
        assert!(info.get_mq_table().unwrap().with(Vec::is_empty));
        assert!(info.get_mq_pop_table().unwrap().with(Vec::is_empty));
        assert!(info.get_status_table().unwrap().with(BTreeMap::is_empty));
        assert!(
            info.get_user_consumer_info()
                .unwrap()
                .with(BTreeMap::is_empty)
        );
        assert!(info.get_jstack().is_none());

        consumer.start().await.unwrap();
        assert_eq!(consumer.consumer_start_timestamp(), start_timestamp);
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn pull_service_state_accessor_matches_java_mutability_and_start_guard() {
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/pull-implementation-accessor.json"
        ))
        .unwrap();
        let group = expected["consumerGroup"].as_str().unwrap();
        let consumer = Arc::new(PullConsumer::new(PullConsumerConfig::new(group).unwrap()));
        assert_eq!(
            consumer.service_state().unwrap().as_java_name(),
            expected["serviceStateBeforeStart"].as_str().unwrap()
        );
        consumer.set_service_state(None);
        assert!(consumer.service_state().is_none());
        assert!(expected["serviceStateSetterAcceptsNull"].as_bool().unwrap());

        consumer.set_service_state(Some(ConsumerServiceState::StartFailed));
        assert!(matches!(
            consumer.start().await,
            Err(PullConsumerError::ServiceStateNotReady(Some(
                ConsumerServiceState::StartFailed
            )))
        ));
        assert_eq!(
            consumer.service_state().unwrap().as_java_name(),
            expected["serviceStateAfterRejectedStart"].as_str().unwrap()
        );
        assert!(expected["serviceStateSetterBlocksStart"].as_bool().unwrap());

        consumer.set_service_state(Some(ConsumerServiceState::Running));
        assert_eq!(
            consumer.service_state().unwrap().as_java_name(),
            expected["serviceStateAfterSetter"].as_str().unwrap()
        );
        assert!(consumer.is_running().await);
        consumer.set_service_state(Some(ConsumerServiceState::CreateJust));
        assert!(!consumer.is_running().await);
    }

    #[tokio::test]
    async fn factory_start_failure_preserves_java_pull_consumer_registration() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/consumer/lifecycle.json"))
                .unwrap();
        let java = &fixture["pullFactoryStartFailure"];
        assert_eq!(java["exception"], "IllegalArgumentException");
        assert_eq!(java["consumerRegistered"], true);
        assert_eq!(java["consumerServiceState"], "START_FAILED");
        assert_eq!(java["factoryServiceState"], "START_FAILED");

        let group = "pull-factory-start-failure";
        let mut config = PullConsumerConfig::new(group).unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        config.client_config_mut().set_poll_name_server_interval(0);
        let manager = ClientRuntimeManager::new();
        let client_config = config.client_config().clone();
        let runtime = manager.get_or_create(&client_config).await.unwrap();
        let consumer = Arc::new(PullConsumer::with_manager(config, manager.clone()));

        assert!(matches!(
            consumer.start().await,
            Err(PullConsumerError::Runtime(
                rocketmq_classic_core::runtime::RuntimeError::InvalidSchedule
            ))
        ));
        assert_eq!(
            consumer.service_state(),
            Some(ConsumerServiceState::StartFailed)
        );
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

    #[tokio::test]
    async fn pull_runtime_rebalance_assigns_all_broadcast_queues_and_persists_removals() {
        let mut config = PullConsumerConfig::new("pull-rebalance-broadcast").unwrap();
        config.set_message_model(MessageModel::Broadcasting);
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let runtime = manager.get_or_create(config.client_config()).await.unwrap();
        let store = Arc::new(TestOffsetStore::default());
        let store_trait: Arc<dyn OffsetStore> = store.clone();
        let changes = Arc::new(Mutex::new(Vec::new()));
        let recorded_changes = changes.clone();
        let listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
            recorded_changes.lock().unwrap().push((
                topic.to_owned(),
                all.cloned(),
                assigned.clone(),
            ));
        });
        let first = MessageQueue::new("orders", "broker-a", 0);
        let second = MessageQueue::new("orders", "broker-a", 1);
        let unassigned = MessageQueue::new("payments", "broker-a", 0);
        store.update_offset(&first, 11, false);
        store.update_offset(&second, 22, false);
        store.update_offset(&unassigned, 33, false);
        let pull_owner = Arc::new(PullConsumer::new(
            PullConsumerConfig::new("pull-rebalance-broadcast").unwrap(),
        ));
        let registration = Arc::new(PullConsumerRegistration {
            group: "pull-rebalance-broadcast".to_owned(),
            message_model: MessageModel::Broadcasting,
            rebalance_message_model: Arc::new(RwLock::new(MessageModel::Broadcasting)),
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            unit_mode: false,
            route_timeout_millis: 100,
            enable_rebalance: Arc::new(AtomicBool::new(true)),
            rebalance_active: Arc::new(AtomicBool::new(true)),
            strategy: Arc::new(RwLock::new(config.allocate_message_queue_strategy())),
            message_queue_listener: Arc::new(RwLock::new(Some(listener))),
            register_topics: Arc::new(RwLock::new(BTreeSet::from(["orders".to_owned()]))),
            subscriptions: Arc::new(RwLock::new(HashMap::from([(
                "orders".to_owned(),
                subscription_all("orders"),
            )]))),
            register_subscriptions: Arc::new(RwLock::new(HashSet::new())),
            additional_heartbeat_subscriptions: Arc::new(RwLock::new(
                ConsumerSubscriptionDataSet::new(),
            )),
            topic_queues: Arc::new(RwLock::new(HashMap::from([(
                "orders".to_owned(),
                BTreeSet::from([first.clone(), second.clone()]),
            )]))),
            in_balance_queues: Arc::new(RwLock::new(BTreeSet::new())),
            offset_store: Arc::downgrade(&store_trait),
            runtime: Arc::downgrade(&runtime),
            consumer: Arc::downgrade(&pull_owner),
            rebalance_execution: tokio::sync::Mutex::new(()),
            rebalance_membership: Mutex::new(HashMap::new()),
        });
        write_lock(&registration.additional_heartbeat_subscriptions)
            .insert(Some(SharedValue::from(subscription_all("heartbeat-only"))));
        let heartbeat_topics = registration
            .heartbeat_data()
            .subscription_data_set
            .unwrap()
            .snapshot()
            .into_iter()
            .flatten()
            .map(|subscription| {
                subscription.with(|subscription| subscription.get_topic().unwrap().to_owned())
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            heartbeat_topics,
            BTreeSet::from(["orders".into(), "heartbeat-only".into()])
        );
        let owner: Arc<dyn ConsumerRuntime> = registration.clone();
        let lease = runtime
            .register_consumer("pull-rebalance-broadcast", owner)
            .await
            .unwrap();

        assert!(runtime.do_rebalance().await.unwrap());
        assert_eq!(
            read_lock(&registration.in_balance_queues).clone(),
            BTreeSet::from([first.clone(), second.clone()])
        );
        assert!(read_lock(&pull_owner.process_queues).contains_key(&first));
        assert!(read_lock(&pull_owner.process_queues).contains_key(&second));
        let removed_process_queue = read_lock(&pull_owner.process_queues)[&first].clone();
        assert_eq!(
            store.values.lock().unwrap().get(&unassigned),
            Some(&33),
            "rebalance leaves the unrelated offset untouched"
        );
        assert!(!store.values.lock().unwrap().contains_key(&first));
        assert!(!store.values.lock().unwrap().contains_key(&second));
        {
            let changes_guard = changes.lock().unwrap();
            assert!(changes_guard.iter().any(|(topic, all, assigned)| {
                topic == "orders"
                    && all.as_ref().is_some_and(|queues| queues.len() == 2)
                    && assigned.len() == 2
            }));
        }

        assert!(runtime.do_rebalance().await.unwrap());
        assert_eq!(
            changes.lock().unwrap().len(),
            1,
            "unchanged assignments do not notify"
        );

        let persist = ConsumerRuntime::persist_consumer_offset(&*registration).unwrap();
        persist.await;
        assert_eq!(
            store.persisted.lock().unwrap().as_slice(),
            &[first.clone(), second.clone()],
            "periodic persistence skips offsets outside the current assignment"
        );
        store.persisted.lock().unwrap().clear();

        write_lock(&registration.topic_queues)
            .insert("orders".to_owned(), BTreeSet::from([second.clone()]));
        store.update_offset(&first, 33, false);
        registration
            .enable_rebalance
            .store(false, Ordering::Release);
        assert!(runtime.do_rebalance().await.unwrap());
        assert_eq!(
            read_lock(&registration.in_balance_queues).clone(),
            BTreeSet::from([first.clone(), second.clone()]),
            "disabled rebalance retains the existing assignment"
        );
        assert!(store.persisted.lock().unwrap().is_empty());
        registration.enable_rebalance.store(true, Ordering::Release);
        assert!(runtime.do_rebalance().await.unwrap());
        assert_eq!(
            read_lock(&registration.in_balance_queues).clone(),
            BTreeSet::from([second.clone()])
        );
        assert!(!read_lock(&pull_owner.process_queues).contains_key(&first));
        assert!(removed_process_queue.is_dropped());
        assert!(read_lock(&pull_owner.process_queues).contains_key(&second));
        assert_eq!(*store.persisted.lock().unwrap(), vec![first.clone()]);
        assert!(!store.values.lock().unwrap().contains_key(&first));
        assert_eq!(changes.lock().unwrap().len(), 2);

        runtime
            .unregister_consumer(&lease, Instant::now() + Duration::from_millis(100))
            .await
            .unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn pull_rebalance_uses_implementation_subscription_map_and_membership() {
        let mut config = PullConsumerConfig::new("pull-rebalance-cluster").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let runtime = manager.get_or_create(config.client_config()).await.unwrap();
        let store = Arc::new(TestOffsetStore::default());
        let store_trait: Arc<dyn OffsetStore> = store.clone();
        let first = MessageQueue::new("orders", "broker-a", 0);
        let second = MessageQueue::new("orders", "broker-a", 1);
        let registration = Arc::new(PullConsumerRegistration {
            group: "pull-rebalance-cluster".to_owned(),
            message_model: MessageModel::Clustering,
            rebalance_message_model: Arc::new(RwLock::new(MessageModel::Clustering)),
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            unit_mode: false,
            route_timeout_millis: 100,
            enable_rebalance: Arc::new(AtomicBool::new(true)),
            rebalance_active: Arc::new(AtomicBool::new(true)),
            strategy: Arc::new(RwLock::new(config.allocate_message_queue_strategy())),
            message_queue_listener: Arc::new(RwLock::new(None)),
            register_topics: Arc::new(RwLock::new(BTreeSet::new())),
            subscriptions: Arc::new(RwLock::new(HashMap::from([(
                "orders".to_owned(),
                subscription_all("orders"),
            )]))),
            register_subscriptions: Arc::new(RwLock::new(HashSet::new())),
            additional_heartbeat_subscriptions: Arc::new(RwLock::new(
                ConsumerSubscriptionDataSet::new(),
            )),
            topic_queues: Arc::new(RwLock::new(HashMap::from([(
                "orders".to_owned(),
                BTreeSet::from([first.clone(), second.clone()]),
            )]))),
            in_balance_queues: Arc::new(RwLock::new(BTreeSet::new())),
            offset_store: Arc::downgrade(&store_trait),
            runtime: Arc::downgrade(&runtime),
            consumer: Weak::new(),
            rebalance_execution: tokio::sync::Mutex::new(()),
            rebalance_membership: Mutex::new(HashMap::from([(
                "orders".to_owned(),
                VecDeque::from([Some(vec![runtime.client_id().to_owned()]), Some(Vec::new())]),
            )])),
        });
        let owner: Arc<dyn ConsumerRuntime> = registration.clone();
        let lease = runtime
            .register_consumer("pull-rebalance-cluster", owner)
            .await
            .unwrap();

        assert!(runtime.do_rebalance().await.unwrap());
        let assigned = read_lock(&registration.in_balance_queues).clone();
        assert_eq!(assigned, BTreeSet::from([first, second]));

        write_lock(&registration.topic_queues).insert("orders".to_owned(), BTreeSet::new());
        assert!(!runtime.do_rebalance().await.unwrap());
        assert_eq!(read_lock(&registration.in_balance_queues).clone(), assigned);
        assert_eq!(
            registration
                .rebalance_membership
                .lock()
                .unwrap()
                .get("orders")
                .unwrap()
                .len(),
            1,
            "empty route data is handled before membership lookup"
        );

        write_lock(&registration.topic_queues).insert(
            "orders".to_owned(),
            BTreeSet::from([
                MessageQueue::new("orders", "broker-a", 0),
                MessageQueue::new("orders", "broker-a", 1),
            ]),
        );
        assert!(!runtime.do_rebalance().await.unwrap());
        assert_eq!(read_lock(&registration.in_balance_queues).clone(), assigned);

        runtime
            .unregister_consumer(&lease, Instant::now() + Duration::from_millis(100))
            .await
            .unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[test]
    fn pull_input_validation_rejects_invalid_queue_offset_count_and_timeout() {
        let queue = MessageQueue::new("orders", "broker-a", 0);
        assert!(validate_pull_request(&queue, 0, 1, 1, 0).is_ok());
        assert!(matches!(
            validate_pull_request(&queue, -1, 1, 1, 0),
            Err(PullConsumerError::InvalidOffset)
        ));
        assert!(matches!(
            validate_pull_request(&queue, 0, 0, 1, 0),
            Err(PullConsumerError::InvalidMaxMessageCount)
        ));
        assert!(matches!(
            validate_pull_request(&queue, 0, 1, 0, 0),
            Err(PullConsumerError::InvalidMaxMessageBytes)
        ));
        assert!(matches!(
            validate_pull_request(&queue, 0, 1, -1, 0),
            Err(PullConsumerError::InvalidMaxMessageBytes)
        ));
        assert!(matches!(
            validate_pull_request(&queue, 0, 1, 1, -1),
            Err(PullConsumerError::InvalidTimeout)
        ));
        let invalid_queue = MessageQueue::new("", "broker-a", 0);
        assert!(matches!(
            validate_pull_request(&invalid_queue, 0, 1, 1, 0),
            Err(PullConsumerError::InvalidQueue)
        ));
    }

    #[tokio::test]
    async fn startup_registration_and_shutdown_obey_consumer_lifecycle() {
        let mut config = PullConsumerConfig::new("pull-lifecycle").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(MessageModel::Broadcasting);
        let changes = Arc::new(Mutex::new(Vec::new()));
        let recorded_changes = changes.clone();
        config.set_message_queue_listener(Some(Arc::new(move |topic, all, assigned| {
            recorded_changes.lock().unwrap().push((
                topic.to_owned(),
                all.cloned(),
                assigned.clone(),
            ));
        })));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PullConsumer::with_manager(config, manager));
        let store = Arc::new(TestOffsetStore::default());

        assert_eq!(consumer.group_name(), "pull-lifecycle");
        assert_eq!(
            consumer.service_state(),
            Some(ConsumerServiceState::CreateJust)
        );
        assert!(consumer.get_offset_store().is_none());
        assert!(matches!(
            consumer.fetch_message_queues_in_balance("orders").await,
            Err(PullConsumerError::NotStarted)
        ));
        consumer
            .set_offset_store(Some(store.clone()))
            .await
            .unwrap();
        consumer.register_topic("orders").unwrap();
        consumer.start().await.unwrap();
        assert_eq!(
            consumer.service_state(),
            Some(ConsumerServiceState::Running)
        );
        assert_eq!(consumer.group_name(), "tenant%pull-lifecycle");
        let runtime = consumer.started_runtime().await.unwrap().0;
        write_lock(&consumer.topic_queues).insert(
            "tenant%orders".to_owned(),
            BTreeSet::from([
                MessageQueue::new("tenant%orders", "broker-a", 1),
                MessageQueue::new("tenant%orders", "broker-a", 2),
            ]),
        );
        changes.lock().unwrap().clear();
        assert!(runtime.do_rebalance().await.unwrap());
        assert_eq!(
            consumer
                .fetch_message_queues_in_balance("orders")
                .await
                .unwrap(),
            BTreeSet::from([
                MessageQueue::new("orders", "broker-a", 1),
                MessageQueue::new("orders", "broker-a", 2),
            ])
        );
        {
            let changes = changes.lock().unwrap();
            assert!(changes.iter().any(|(topic, all, assigned)| {
                topic == "tenant%orders"
                    && all.as_ref().is_some_and(|queues| queues.len() == 2)
                    && assigned.len() == 2
            }));
        }
        assert!(consumer.get_offset_store().is_some());
        assert!(consumer.is_running().await);
        assert!(matches!(
            consumer.start().await,
            Err(PullConsumerError::ServiceStateNotReady(Some(
                ConsumerServiceState::Running
            )))
        ));
        assert!(matches!(
            consumer.set_offset_store(None).await,
            Err(PullConsumerError::OffsetStoreAfterStartup)
        ));

        consumer.shutdown().await.unwrap();
        assert_eq!(
            consumer.service_state(),
            Some(ConsumerServiceState::ShutdownAlready)
        );
        assert!(consumer.get_offset_store().is_some());
        assert!(!consumer.is_running().await);
        assert!(matches!(
            consumer
                .pull(&MessageQueue::new("orders", "broker-a", 0), None, 0, 1)
                .await,
            Err(PullConsumerError::Shutdown)
        ));
        assert!(matches!(
            consumer.fetch_message_queues_in_balance("orders").await,
            Err(PullConsumerError::Shutdown)
        ));
    }
}
