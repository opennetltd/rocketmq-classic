use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc, Mutex as StdMutex, OnceLock, RwLock, Weak,
        atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering},
    },
    time::{Duration, Instant as StdInstant},
};

use rocketmq_classic_core::{
    route::TopicSnapshot,
    runtime::{ClientRuntime, ClientRuntimeManager, GroupLease, ProducerRuntime},
    unique_id::UniqueIdGenerator,
};
use rocketmq_classic_remoting::client::{CallbackExecutor, RpcHook};

use crate::request_reply::RequestFutureHolder;
use crate::{
    AsyncSenderExecutor, CheckForbiddenContext, EndTransactionContext, EndTransactionHook,
    ProducerCompressor, ProducerConfig, ProducerError, ProducerLongArray, ProducerServiceState,
    RetryResponseCodes, SendMessageContext, SendMessageHook, TopicPublishInfo,
    TopicPublishInfoTable,
    accumulator::AutoBatchAccumulator,
    backpressure::{AdjustableSemaphore, BackpressureReadGuard},
    config::AccumulatorSettings,
    executor::TokioAsyncSenderExecutor,
    fault::FaultStrategy,
    selector::RoundRobinSelector,
    topic_list::ProducerTopicList,
    validation,
};

use tokio::task::JoinHandle;

enum State {
    Created,
    Started {
        runtime: Weak<ClientRuntime>,
        lease: GroupLease,
    },
    Shutdown,
}

pub(crate) struct SharedAutoBatchAccumulator {
    pub(crate) engine: AutoBatchAccumulator,
    pub(crate) settings: RwLock<AccumulatorSettings>,
    pub(crate) settings_changed: tokio::sync::watch::Sender<u64>,
}

type AccumulatorRegistryKey = (usize, String);
type AccumulatorRegistry = StdMutex<HashMap<AccumulatorRegistryKey, AccumulatorRegistryEntry>>;

struct AccumulatorRegistryEntry {
    _manager_token: Arc<()>,
    accumulator: Arc<SharedAutoBatchAccumulator>,
}

impl SharedAutoBatchAccumulator {
    fn new() -> Self {
        let (settings_changed, _) = tokio::sync::watch::channel(0);
        Self {
            engine: AutoBatchAccumulator::default(),
            settings: RwLock::new(AccumulatorSettings::default()),
            settings_changed,
        }
    }

    fn for_client(manager: &ClientRuntimeManager, client_id: String) -> Arc<Self> {
        static ACCUMULATORS: OnceLock<AccumulatorRegistry> = OnceLock::new();
        let mut accumulators = ACCUMULATORS
            .get_or_init(|| StdMutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let manager_token = manager.cache_identity_token();
        let key = (Arc::as_ptr(&manager_token) as usize, client_id);
        if let Some(entry) = accumulators.get(&key) {
            return entry.accumulator.clone();
        }
        let accumulator = Arc::new(Self::new());
        accumulators.insert(
            key,
            AccumulatorRegistryEntry {
                _manager_token: manager_token,
                accumulator: accumulator.clone(),
            },
        );
        accumulator
    }

    fn notify_settings_changed(&self) {
        self.settings_changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }
}

/// Java-compatible producer lifecycle and send surface.
pub struct Producer {
    pub(crate) config: ProducerConfig,
    producer_group: RwLock<Option<String>>,
    send_message_with_vip_channel: AtomicBool,
    topics: RwLock<Option<ProducerTopicList>>,
    started_topics: OnceLock<Option<Vec<String>>>,
    manager: ClientRuntimeManager,
    rpc_hook: Option<Arc<dyn RpcHook>>,
    state: tokio::sync::Mutex<State>,
    service_state: RwLock<Option<ProducerServiceState>>,
    detector_task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    pub(crate) auto_batch_accumulator: OnceLock<Arc<SharedAutoBatchAccumulator>>,
    accumulator_initialized: AtomicBool,
    batch_max_delay_millis: AtomicI32,
    batch_max_bytes: AtomicI64,
    total_batch_max_bytes: AtomicI64,
    pub(crate) auto_batch_tasks: tokio::sync::Mutex<Vec<JoinHandle<()>>>,
    pub(crate) self_weak: OnceLock<Weak<Producer>>,
    pub(crate) request_future_scheduler_id: Arc<()>,
    pub(crate) transaction: OnceLock<Arc<crate::transaction::TransactionRuntime>>,
    pub(crate) trace_dispatcher: OnceLock<Arc<crate::trace::TraceDispatcher>>,
    recall_trace_hook: tokio::sync::Mutex<Option<Arc<dyn RpcHook>>>,
    pub(crate) request_callback_tasks: tokio::sync::Mutex<Vec<JoinHandle<()>>>,
    pub(crate) check_forbidden_hooks: crate::hooks::CheckForbiddenHookList,
    pub(crate) send_message_hooks: crate::hooks::SendMessageHookList,
    pub(crate) end_transaction_hooks: crate::hooks::EndTransactionHookList,
    pub(crate) backpressure_send_num: Arc<AdjustableSemaphore>,
    pub(crate) backpressure_send_size: Arc<AdjustableSemaphore>,
    publish_info: TopicPublishInfoTable,
    default_async_sender_executor: Arc<dyn AsyncSenderExecutor>,
    async_sender_executor: RwLock<Option<Arc<dyn AsyncSenderExecutor>>>,
    pub(crate) selector: RoundRobinSelector,
    pub(crate) fault: FaultStrategy,
    pub(crate) ids: UniqueIdGenerator,
    #[cfg(test)]
    pub(crate) scripted_transport: std::sync::Mutex<ScriptedTransport>,
    #[cfg(test)]
    pub(crate) scripted_end_transactions: std::sync::Mutex<
        Vec<(
            String,
            rocketmq_classic_protocol::header::EndTransactionRequestHeader,
            Option<String>,
        )>,
    >,
    #[cfg(test)]
    pub(crate) scripted_end_failure: std::sync::atomic::AtomicBool,
}

impl Producer {
    /// Registers a Java-compatible pre-send forbidden check.
    pub fn register_check_forbidden_hook(&self, hook: Arc<dyn crate::CheckForbiddenHook>) {
        self.check_forbidden_hooks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(hook);
    }

    /// Returns whether a pre-send forbidden hook is registered.
    pub fn has_check_forbidden_hook(&self) -> bool {
        !self
            .check_forbidden_hooks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    }

    /// Runs forbidden hooks in registration order and stops at the first error.
    pub fn execute_check_forbidden_hook(
        &self,
        context: &mut CheckForbiddenContext<'_>,
    ) -> Result<(), ProducerError> {
        Self::execute_check_forbidden_hook_snapshot(&self.check_forbidden_hook_snapshot(), context)
    }

    pub(crate) fn execute_check_forbidden_hook_snapshot(
        hooks: &[Arc<dyn crate::CheckForbiddenHook>],
        context: &mut CheckForbiddenContext<'_>,
    ) -> Result<(), ProducerError> {
        for hook in hooks {
            hook.check_forbidden(context)?;
        }
        Ok(())
    }

    pub(crate) fn check_forbidden_hook_snapshot(&self) -> Vec<Arc<dyn crate::CheckForbiddenHook>> {
        self.check_forbidden_hooks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Registers a Java-compatible hook for every outbound producer attempt.
    pub fn register_send_message_hook(&self, hook: Arc<dyn crate::SendMessageHook>) {
        self.send_message_hooks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(hook);
    }

    /// Returns whether a send-message hook is registered.
    pub fn has_send_message_hook(&self) -> bool {
        !self
            .send_message_hooks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    }

    /// Runs before-send callbacks in registration order, isolating each panic.
    pub fn execute_send_message_hook_before(&self, context: &mut SendMessageContext<'_>) {
        Self::execute_send_message_hook_before_snapshot(
            &self.send_message_hook_snapshot(),
            context,
        );
    }

    /// Runs after-send callbacks in registration order, isolating each panic.
    pub fn execute_send_message_hook_after(&self, context: &mut SendMessageContext<'_>) {
        Self::execute_send_message_hook_after_snapshot(&self.send_message_hook_snapshot(), context);
    }

    pub(crate) fn execute_send_message_hook_before_snapshot(
        hooks: &[Arc<dyn SendMessageHook>],
        context: &mut SendMessageContext<'_>,
    ) {
        for hook in hooks {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                hook.send_message_before(context);
            }));
        }
    }

    pub(crate) fn execute_send_message_hook_after_snapshot(
        hooks: &[Arc<dyn SendMessageHook>],
        context: &mut SendMessageContext<'_>,
    ) {
        for hook in hooks {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                hook.send_message_after(context);
            }));
        }
    }

    /// Registers a hook invoked before each transaction completion request.
    pub fn register_end_transaction_hook(&self, hook: Arc<dyn EndTransactionHook>) {
        self.end_transaction_hooks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(hook);
    }

    /// Returns whether any end-transaction hooks are registered.
    pub fn has_end_transaction_hook(&self) -> bool {
        !self
            .end_transaction_hooks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    }

    /// Runs registered hooks in registration order. A panicking hook is
    /// isolated so later hooks still receive the same context, matching Java.
    pub fn execute_end_transaction_hook(&self, context: &mut EndTransactionContext<'_>) {
        let hooks = self
            .end_transaction_hooks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for hook in hooks {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                hook.end_transaction(context);
            }));
        }
    }

    /// Builds the Java implementation hook context and runs it before an
    /// end-transaction RPC. The RPC header is prepared by the caller first.
    pub fn do_execute_end_transaction_hook(
        &self,
        message: &mut rocketmq_classic_common::message::Message,
        message_id: &str,
        broker_addr: &str,
        transaction_state: crate::LocalTransactionState,
        from_transaction_check: bool,
    ) {
        let transaction_id = message.transaction_id().map(str::to_owned);
        let mut context = EndTransactionContext {
            producer_group: self.producer_group(),
            message,
            broker_addr: broker_addr.to_owned(),
            message_id: message_id.to_owned(),
            transaction_id,
            transaction_state,
            from_transaction_check,
        };
        self.execute_end_transaction_hook(&mut context);
    }

    pub(crate) fn send_message_hook_snapshot(&self) -> Vec<Arc<dyn crate::SendMessageHook>> {
        self.send_message_hooks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn new(config: ProducerConfig) -> Self {
        Self::new_with_rpc_hook(config, None)
    }

    /// Creates a producer whose remoting requests use the supplied RPC hook.
    pub fn new_with_rpc_hook(config: ProducerConfig, rpc_hook: Option<Arc<dyn RpcHook>>) -> Self {
        let manager = ClientRuntimeManager::shared_default();
        Self::with_runtime_manager_and_hook(config, manager, rpc_hook)
    }

    #[cfg(test)]
    pub(crate) fn with_runtime_manager(
        config: ProducerConfig,
        manager: ClientRuntimeManager,
    ) -> Self {
        Self::with_runtime_manager_and_hook(config, manager, None)
    }

    pub(crate) fn with_runtime_manager_and_hook(
        config: ProducerConfig,
        manager: ClientRuntimeManager,
        rpc_hook: Option<Arc<dyn RpcHook>>,
    ) -> Self {
        let ids = UniqueIdGenerator::new(config.client_config().client_ip());
        let producer_group = config.producer_group().map(str::to_owned);
        let send_message_with_vip_channel = config.send_message_with_vip_channel();
        let topics = config.topics();
        let batch_max_delay_millis = config.batch_max_delay_millis().unwrap_or(-1);
        let batch_max_bytes = config.batch_max_bytes().unwrap_or(-1);
        let total_batch_max_bytes = config.total_batch_max_bytes_input().unwrap_or(-1);
        let backpressure_send_num = Arc::new(AdjustableSemaphore::new(
            config.backpressure_async_send_num(),
        ));
        let backpressure_send_size = Arc::new(AdjustableSemaphore::new(
            config.backpressure_async_send_size(),
        ));
        let fault = FaultStrategy::new(
            config.latency_max_millis().to_vec(),
            config.not_available_duration_millis().to_vec(),
        );
        fault.set_send_latency_fault_enable(config.send_latency_fault_enable());
        fault.set_start_detector_enable(config.start_detector_enable());
        Self {
            ids,
            config,
            producer_group: RwLock::new(producer_group),
            send_message_with_vip_channel: AtomicBool::new(send_message_with_vip_channel),
            topics: RwLock::new(topics),
            started_topics: OnceLock::new(),
            manager,
            rpc_hook,
            state: tokio::sync::Mutex::new(State::Created),
            service_state: RwLock::new(Some(ProducerServiceState::CreateJust)),
            detector_task: tokio::sync::Mutex::new(None),
            auto_batch_accumulator: OnceLock::new(),
            accumulator_initialized: AtomicBool::new(false),
            batch_max_delay_millis: AtomicI32::new(batch_max_delay_millis),
            batch_max_bytes: AtomicI64::new(batch_max_bytes),
            total_batch_max_bytes: AtomicI64::new(total_batch_max_bytes),
            auto_batch_tasks: tokio::sync::Mutex::new(Vec::new()),
            self_weak: OnceLock::new(),
            request_future_scheduler_id: Arc::new(()),
            transaction: OnceLock::new(),
            trace_dispatcher: OnceLock::new(),
            recall_trace_hook: tokio::sync::Mutex::new(None),
            request_callback_tasks: tokio::sync::Mutex::new(Vec::new()),
            check_forbidden_hooks: Default::default(),
            send_message_hooks: Default::default(),
            end_transaction_hooks: Default::default(),
            backpressure_send_num,
            backpressure_send_size,
            publish_info: TopicPublishInfoTable::default(),
            default_async_sender_executor: Arc::new(TokioAsyncSenderExecutor),
            async_sender_executor: RwLock::new(None),
            selector: RoundRobinSelector::default(),
            fault,
            #[cfg(test)]
            scripted_transport: std::sync::Mutex::new(ScriptedTransport::default()),
            #[cfg(test)]
            scripted_end_transactions: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            scripted_end_failure: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn config(&self) -> &ProducerConfig {
        &self.config
    }

    /// Borrows the Java `DefaultMQProducerImpl` view exposed by
    /// `DefaultMQProducer.getDefaultMQProducerImpl()`.
    ///
    /// The Rust `Producer` owns both facade and implementation behavior, so
    /// this returns the same owner without allocating a second handle.
    pub fn default_mq_producer_impl(&self) -> &Self {
        self
    }

    /// Borrows the Java `DefaultMQProducer` facade returned by
    /// `DefaultMQProducerImpl.getDefaultMQProducer()`.
    pub fn default_mq_producer(&self) -> &Self {
        self
    }

    /// Returns the shared mutable topic list, preserving Java's getter alias.
    /// Mutations after start are visible through this handle but do not alter
    /// the runtime topic snapshot captured at start.
    pub fn topics(&self) -> Option<ProducerTopicList> {
        self.topics
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Retains the supplied topic list, matching Java's
    /// `DefaultMQProducer.setTopics` reference behavior. The runtime captures
    /// a separate topic snapshot when the producer starts.
    pub fn set_topics(&self, value: Option<ProducerTopicList>) {
        *self
            .topics
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = value;
    }

    /// Returns whether this producer rewrites Broker addresses to VIP ports.
    pub fn send_message_with_vip_channel(&self) -> bool {
        self.send_message_with_vip_channel.load(Ordering::SeqCst)
    }

    /// Sets the per-producer VIP channel flag used by later sends.
    pub fn set_send_message_with_vip_channel(&self, value: bool) {
        self.send_message_with_vip_channel
            .store(value, Ordering::SeqCst);
    }

    /// Returns the topic key used by later route lookups.
    pub fn create_topic_key(&self) -> String {
        self.config.create_topic_key()
    }

    /// Changes the topic key used by later route lookups.
    pub fn set_create_topic_key(&self, value: impl Into<String>) {
        self.config.set_create_topic_key_runtime(value);
    }

    /// Returns the queue count used when a route is created from the default topic.
    pub fn default_topic_queue_nums(&self) -> i32 {
        self.config.default_topic_queue_nums()
    }

    /// Changes the queue count used by subsequent default-topic route lookups.
    pub fn set_default_topic_queue_nums(&self, value: i32) {
        self.config.set_default_topic_queue_nums_runtime(value);
    }

    /// Returns the timeout used by send methods without an explicit timeout.
    pub fn send_timeout_millis(&self) -> i32 {
        self.config.send_timeout_millis()
    }

    /// Changes the timeout used by subsequent send methods without an explicit timeout.
    pub fn set_send_timeout_millis(&self, value: i32) {
        self.config.set_send_timeout_millis_runtime(value);
    }

    /// Returns the maximum timeout for an individual synchronous retry request.
    pub fn send_msg_max_timeout_per_request(&self) -> i32 {
        self.config.send_msg_max_timeout_per_request()
    }

    /// Changes the maximum timeout used by later synchronous retry requests.
    pub fn set_send_msg_max_timeout_per_request(&self, value: i32) {
        self.config
            .set_send_msg_max_timeout_per_request_runtime(value);
    }

    /// Returns the synchronous send retry count.
    pub fn retry_times_when_send_failed(&self) -> i32 {
        self.config.retry_times_when_send_failed()
    }

    /// Changes the retry count used by subsequent synchronous sends.
    pub fn set_retry_times_when_send_failed(&self, value: i32) {
        self.config.set_retry_times_when_send_failed_runtime(value);
    }

    /// Returns the future-mode send retry count.
    pub fn retry_times_when_send_async_failed(&self) -> i32 {
        self.config.retry_times_when_send_async_failed()
    }

    /// Changes the retry count used by subsequent future-mode sends.
    pub fn set_retry_times_when_send_async_failed(&self, value: i32) {
        self.config
            .set_retry_times_when_send_async_failed_runtime(value);
    }

    /// Returns whether a later send retries another Broker after a non-OK store result.
    pub fn retry_another_broker_when_not_store_ok(&self) -> bool {
        self.config.retry_another_broker_when_not_store_ok()
    }

    /// Changes the retry behavior for subsequent send attempts.
    pub fn set_retry_another_broker_when_not_store_ok(&self, value: bool) {
        self.config
            .set_retry_another_broker_when_not_store_ok_runtime(value);
    }

    /// Returns the current maximum message size in bytes.
    pub fn max_message_size(&self) -> i32 {
        self.config.max_message_size()
    }

    /// Changes the message-size limit used by subsequent validation.
    pub fn set_max_message_size(&self, value: i32) {
        self.config.set_max_message_size_runtime(value);
    }

    /// Returns the shared producer route table corresponding to Java's
    /// `DefaultMQProducerImpl.getTopicPublishInfoTable()`.
    pub fn get_topic_publish_info_table(&self) -> TopicPublishInfoTable {
        self.publish_info.clone()
    }

    /// Returns the Java implementation lifecycle state, including a caller-set
    /// null value.
    pub fn service_state(&self) -> Option<ProducerServiceState> {
        *self
            .service_state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Sets the Java implementation lifecycle state. The setter mirrors the
    /// nullable Java field; normal Producer lifecycle methods update it as
    /// startup and shutdown progress.
    pub fn set_service_state(&self, value: Option<ProducerServiceState>) {
        *self
            .service_state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = value;
    }

    /// Returns the producer's client unit-mode setting.
    pub fn is_unit_mode(&self) -> bool {
        self.config.client_config().unit_mode()
    }

    /// Returns whether auto-batching is enabled after the accumulator has
    /// been initialized. Java reports `false` before that lifecycle step.
    pub fn auto_batch(&self) -> bool {
        self.accumulator_initialized.load(Ordering::SeqCst) && self.config.auto_batch()
    }

    /// Changes auto-batching for later sends. The setting becomes observable
    /// through [`Self::auto_batch`] once the accumulator is initialized.
    pub fn set_auto_batch(&self, value: bool) {
        self.config.set_auto_batch_runtime(value);
    }

    /// Returns the effective accumulator hold delay, or zero before
    /// initialization, matching Java `getBatchMaxDelayMs()`.
    pub fn batch_max_delay_millis(&self) -> i32 {
        if !self.accumulator_initialized.load(Ordering::SeqCst) {
            return 0;
        }
        self.shared_auto_batch_accumulator()
            .settings
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .max_delay_millis
    }

    /// Updates the live accumulator hold delay. Before initialization the
    /// value is retained for `init_produce_accumulator`; afterward it must be
    /// in Java's inclusive 1–30,000 ms range.
    pub fn set_batch_max_delay_millis(&self, value: i32) -> Result<(), ProducerError> {
        self.batch_max_delay_millis.store(value, Ordering::SeqCst);
        if !self.accumulator_initialized.load(Ordering::SeqCst) {
            return Ok(());
        }
        if !(1..=30_000).contains(&value) {
            return Err(ProducerError::InvalidBatchMaxDelay(value));
        }
        let accumulator = self.shared_auto_batch_accumulator();
        accumulator
            .settings
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .max_delay_millis = value;
        accumulator.notify_settings_changed();
        Ok(())
    }

    /// Returns the effective per-batch byte threshold, or zero before
    /// accumulator initialization, matching Java `getBatchMaxBytes()`.
    pub fn batch_max_bytes(&self) -> i64 {
        if !self.accumulator_initialized.load(Ordering::SeqCst) {
            return 0;
        }
        self.shared_auto_batch_accumulator()
            .settings
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .max_bytes as i64
    }

    /// Updates the live per-batch byte threshold. After initialization Java
    /// accepts values from 1 byte through 2 MiB.
    pub fn set_batch_max_bytes(&self, value: i64) -> Result<(), ProducerError> {
        self.batch_max_bytes.store(value, Ordering::SeqCst);
        if !self.accumulator_initialized.load(Ordering::SeqCst) {
            return Ok(());
        }
        if !(1..=2 * 1024 * 1024).contains(&value) {
            return Err(ProducerError::InvalidBatchMaxBytes(value));
        }
        let accumulator = self.shared_auto_batch_accumulator();
        accumulator
            .settings
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .max_bytes = value as usize;
        accumulator.notify_settings_changed();
        Ok(())
    }

    /// Returns Java's `getTotalBatchMaxBytes()` value. That getter reports
    /// the per-batch threshold; the actual aggregate cap is controlled by
    /// `set_total_batch_max_bytes`.
    pub fn total_batch_max_bytes(&self) -> i64 {
        self.batch_max_bytes()
    }

    /// Updates the live aggregate accumulator byte cap. Java accepts any
    /// positive signed-long value.
    pub fn set_total_batch_max_bytes(&self, value: i64) -> Result<(), ProducerError> {
        self.total_batch_max_bytes.store(value, Ordering::SeqCst);
        if !self.accumulator_initialized.load(Ordering::SeqCst) {
            return Ok(());
        }
        if value <= 0 {
            return Err(ProducerError::InvalidTotalBatchMaxBytes(value));
        }
        let value = usize::try_from(value).unwrap_or(usize::MAX);
        let accumulator = self.shared_auto_batch_accumulator();
        accumulator
            .settings
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .total_max_bytes = value;
        accumulator.notify_settings_changed();
        Ok(())
    }

    fn configured_accumulator_settings(&self) -> Result<AccumulatorSettings, ProducerError> {
        ProducerConfig::accumulator_settings_from_raw(
            self.batch_max_delay_millis.load(Ordering::SeqCst),
            self.batch_max_bytes.load(Ordering::SeqCst),
            self.total_batch_max_bytes.load(Ordering::SeqCst),
        )
    }

    pub(crate) fn effective_accumulator_settings(&self) -> AccumulatorSettings {
        *self
            .shared_auto_batch_accumulator()
            .settings
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn subscribe_accumulator_settings(&self) -> tokio::sync::watch::Receiver<u64> {
        self.shared_auto_batch_accumulator()
            .settings_changed
            .subscribe()
    }

    fn shared_auto_batch_accumulator(&self) -> &Arc<SharedAutoBatchAccumulator> {
        self.auto_batch_accumulator
            .get()
            .expect("auto-batch accumulator must be initialized")
    }

    pub(crate) fn auto_batch_engine(&self) -> &AutoBatchAccumulator {
        &self.shared_auto_batch_accumulator().engine
    }

    /// Initializes the client-ID-owned accumulator. Java caches this helper
    /// by `buildMQClientId`, so Producers sharing a client identity also share
    /// pending batches and their live thresholds.
    pub fn init_produce_accumulator(&self) -> Result<(), ProducerError> {
        let accumulator = self.auto_batch_accumulator.get_or_init(|| {
            SharedAutoBatchAccumulator::for_client(
                &self.manager,
                self.config.client_config().build_client_id(),
            )
        });
        self.accumulator_initialized.store(true, Ordering::SeqCst);
        let mut settings = accumulator
            .settings
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut changed = false;

        let delay = self.batch_max_delay_millis.load(Ordering::SeqCst);
        if delay > -1 {
            if !(1..=30_000).contains(&delay) {
                return Err(ProducerError::InvalidBatchMaxDelay(delay));
            }
            if settings.max_delay_millis != delay {
                settings.max_delay_millis = delay;
                changed = true;
            }
        }
        let max_bytes = self.batch_max_bytes.load(Ordering::SeqCst);
        if max_bytes > -1 {
            if !(1..=2 * 1024 * 1024).contains(&max_bytes) {
                return Err(ProducerError::InvalidBatchMaxBytes(max_bytes));
            }
            if settings.max_bytes != max_bytes as usize {
                settings.max_bytes = max_bytes as usize;
                changed = true;
            }
        }
        let total_max_bytes = self.total_batch_max_bytes.load(Ordering::SeqCst);
        if total_max_bytes > -1 {
            if total_max_bytes <= 0 {
                return Err(ProducerError::InvalidTotalBatchMaxBytes(total_max_bytes));
            }
            let total_max_bytes = usize::try_from(total_max_bytes).unwrap_or(usize::MAX);
            if settings.total_max_bytes != total_max_bytes {
                settings.total_max_bytes = total_max_bytes;
                changed = true;
            }
        }
        drop(settings);
        if changed {
            accumulator.notify_settings_changed();
        }
        Ok(())
    }

    /// Returns a compression backend snapshot for the current configured type.
    pub fn compressor(&self) -> ProducerCompressor {
        ProducerCompressor::new(self.config.compress_type())
    }

    /// Changes the backend used by later sends. Previously returned
    /// [`ProducerCompressor`] values retain their original type.
    pub fn set_compress_type(
        &self,
        value: rocketmq_classic_common::sysflag::message::CompressionType,
    ) {
        self.config.set_compress_type_runtime(value);
    }

    /// Returns the minimum body size that the producer compresses.
    pub fn compress_threshold(&self) -> i32 {
        self.config.compress_threshold()
    }

    /// Changes the compression threshold for later sends.
    pub fn set_compress_threshold(&self, value: i32) {
        self.config.set_compress_threshold_runtime(value);
    }

    /// Returns the compression level used by later sends.
    pub fn compress_level(&self) -> i32 {
        self.config.compress_level()
    }

    /// Changes the compression level used by later sends.
    pub fn set_compress_level(&self, value: i32) {
        self.config.set_compress_level_runtime(value);
    }

    /// Returns whether later sends use latency and reachability based Broker
    /// selection.
    pub fn send_latency_fault_enable(&self) -> bool {
        self.fault.send_latency_fault_enable()
    }

    /// Changes latency and reachability based Broker selection for later sends.
    pub fn set_send_latency_fault_enable(&self, value: bool) {
        self.fault.set_send_latency_fault_enable(value);
    }

    /// Returns the current latency thresholds used by the Producer fault
    /// strategy.
    pub fn latency_max_millis(&self) -> ProducerLongArray {
        self.fault.latency_max_millis()
    }

    /// Changes the latency thresholds used for subsequent fault updates.
    /// Passing `None` preserves Java's nullable-array setting; a later fault
    /// update reports the corresponding Java null-pointer failure if it reads
    /// that table.
    pub fn set_latency_max_millis(&self, value: impl Into<ProducerLongArray>) {
        self.fault.set_latency_max_millis(value.into());
    }

    /// Returns the current Broker unavailability durations used by the
    /// Producer fault strategy.
    pub fn not_available_duration_millis(&self) -> ProducerLongArray {
        self.fault.not_available_duration_millis()
    }

    /// Changes the Broker unavailability durations used for subsequent fault
    /// updates. Passing `None` preserves Java's nullable-array setting; a
    /// matching latency threshold later reports the corresponding Java
    /// null-pointer failure if it reads that table.
    pub fn set_not_available_duration_millis(&self, value: impl Into<ProducerLongArray>) {
        self.fault.set_not_available_duration_millis(value.into());
    }

    /// Returns whether periodic Broker reachability checks are enabled.
    pub fn start_detector_enable(&self) -> bool {
        self.fault.start_detector_enable()
    }

    /// Changes whether the owned periodic task probes due Brokers.
    pub fn set_start_detector_enable(&self, value: bool) {
        self.fault.set_start_detector_enable(value);
    }

    /// Returns the current async backpressure mode. The value can be changed
    /// while the producer is running, like Java's
    /// `isEnableBackpressureForAsyncMode` setting.
    pub fn enable_backpressure_for_async_mode(&self) -> bool {
        self.config.enable_backpressure_for_async_mode()
    }

    /// Changes whether later future-mode sends acquire async backpressure.
    pub fn set_enable_backpressure_for_async_mode(&self, value: bool) {
        self.config
            .set_enable_backpressure_for_async_mode_runtime(value);
    }

    /// Returns the current maximum number of in-flight async sends.
    pub fn backpressure_async_send_num(&self) -> i32 {
        self.config.backpressure_async_send_num()
    }

    /// Returns the currently available async-send count permits, matching
    /// Java's `DefaultMQProducerImpl.getSemaphoreAsyncSendNumAvailablePermits`.
    pub fn semaphore_async_send_num_available_permits(&self) -> i32 {
        self.backpressure_send_num.available_permits() as i32
    }

    /// Replaces the raw async-send count semaphore's available permits, matching
    /// `DefaultMQProducerImpl.setSemaphoreAsyncSendNum`. This does not change
    /// the configured facade limit returned by `backpressure_async_send_num`.
    pub fn set_semaphore_async_send_num(&self, available: i32) {
        self.backpressure_send_num
            .reset_available_permits(available);
    }

    /// Resizes the live async-send count limit while preserving permits held
    /// by sends already in flight. Values below Java's minimum clamp to 10.
    pub fn set_backpressure_async_send_num(&self, value: i32) {
        let value = value.max(10);
        self.backpressure_send_num.resize_and(value, || {
            self.config.set_backpressure_async_send_num_runtime(value);
        });
    }

    /// Returns the current maximum number of bytes held by async sends.
    pub fn backpressure_async_send_size(&self) -> i32 {
        self.config.backpressure_async_send_size()
    }

    /// Returns the currently available async-send byte permits, matching
    /// Java's `DefaultMQProducerImpl.getSemaphoreAsyncSendSizeAvailablePermits`.
    pub fn semaphore_async_send_size_available_permits(&self) -> i32 {
        self.backpressure_send_size.available_permits() as i32
    }

    /// Replaces the raw async-send byte semaphore's available permits, matching
    /// `DefaultMQProducerImpl.setSemaphoreAsyncSendSize`. This does not change
    /// the configured facade limit returned by `backpressure_async_send_size`.
    pub fn set_semaphore_async_send_size(&self, available: i32) {
        self.backpressure_send_size
            .reset_available_permits(available);
    }

    /// Resizes the live async-send byte limit while preserving permits held by
    /// sends already in flight. Values below Java's minimum clamp to 1 MiB.
    pub fn set_backpressure_async_send_size(&self, value: i32) {
        let value = value.max(1024 * 1024);
        self.backpressure_send_size.resize_and(value, || {
            self.config.set_backpressure_async_send_size_runtime(value);
        });
    }

    /// Mirrors Java's internal permit-accounting setter. It updates the
    /// reported limit without resizing the semaphore; normal callers should
    /// use `set_backpressure_async_send_num`.
    pub fn set_backpressure_async_send_num_inside_adjust(&self, value: i32) {
        self.config
            .set_backpressure_async_send_num_inside_adjust(value);
    }

    /// Mirrors Java's internal permit-accounting setter. It updates the
    /// reported limit without resizing the semaphore; normal callers should
    /// use `set_backpressure_async_send_size`.
    pub fn set_backpressure_async_send_size_inside_adjust(&self, value: i32) {
        self.config
            .set_backpressure_async_send_size_inside_adjust(value);
    }

    /// Acquires the Java-compatible count-limit read lock. The lock is
    /// released when the returned guard is dropped.
    pub fn acquire_backpressure_for_async_send_num_lock(&self) -> BackpressureReadGuard {
        self.backpressure_send_num.read_lock()
    }

    /// Acquires the Java-compatible byte-limit read lock. The lock is
    /// released when the returned guard is dropped.
    pub fn acquire_backpressure_for_async_send_size_lock(&self) -> BackpressureReadGuard {
        self.backpressure_send_size.read_lock()
    }

    /// Returns a handle to the live retry response-code set used by this
    /// producer's synchronous retry decisions.
    pub fn retry_response_codes(&self) -> RetryResponseCodes {
        self.config.retry_response_codes()
    }

    /// Adds a code to the live synchronous retry set.
    pub fn add_retry_response_code(&self, value: i32) {
        self.config.retry_response_codes().insert(value);
    }

    /// Sets the executor used for future-mode send attempts. Passing `None`
    /// restores the producer's Tokio-backed default executor.
    pub fn set_async_sender_executor(&self, executor: Option<Arc<dyn AsyncSenderExecutor>>) {
        *self
            .async_sender_executor
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = executor;
    }

    /// Returns the effective asynchronous sender executor. If no override was
    /// set, this is the producer's Tokio-backed default executor.
    pub fn async_sender_executor(&self) -> Arc<dyn AsyncSenderExecutor> {
        self.async_sender_executor
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or_else(|| self.default_async_sender_executor.clone())
    }

    /// Sets the callback executor on the shared remoting client, corresponding
    /// to Java `DefaultMQProducer.setCallbackExecutor`. The producer must be
    /// started because Java stores this executor on its initialized client.
    pub async fn set_callback_executor(
        &self,
        executor: Option<Arc<dyn CallbackExecutor>>,
    ) -> Result<(), ProducerError> {
        let runtime = self.started_runtime().await?;
        runtime.remoting().set_callback_executor(executor);
        Ok(())
    }

    /// Returns the message trace dispatcher after trace initialization.
    ///
    /// This corresponds to Java `DefaultMQProducer.getTraceDispatcher()`.
    pub fn trace_dispatcher(&self) -> Option<Arc<crate::TraceDispatcher>> {
        self.trace_dispatcher.get().cloned()
    }

    /// Returns the Java-compatible producer group, namespace-wrapped at start.
    pub fn producer_group(&self) -> Option<String> {
        self.producer_group
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Sets the producer group. As in Java, a post-start change is used by
    /// subsequent requests without changing the group's runtime registration.
    pub fn set_producer_group(&self, value: Option<String>) {
        *self
            .producer_group
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = value;
    }

    pub(crate) async fn started_runtime(&self) -> Result<Arc<ClientRuntime>, ProducerError> {
        self.started_registration()
            .await
            .map(|(runtime, _)| runtime)
    }

    pub(crate) async fn started_registration(
        &self,
    ) -> Result<(Arc<ClientRuntime>, GroupLease), ProducerError> {
        let state = self.state.lock().await;
        match &*state {
            State::Created => Err(ProducerError::NotStarted),
            State::Shutdown => Err(ProducerError::Shutdown),
            State::Started { runtime, lease } => runtime
                .upgrade()
                .map(|runtime| (runtime, lease.clone()))
                .ok_or(ProducerError::Shutdown),
        }
    }

    pub(crate) async fn track_request_callback_task(&self, task: JoinHandle<()>) {
        let state = self.state.lock().await;
        if !matches!(&*state, State::Started { .. }) || task.is_finished() {
            drop(task);
            return;
        }
        let mut tasks = self.request_callback_tasks.lock().await;
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
    }

    async fn retire_request_callback_tasks(&self) {
        let tasks = {
            let mut tasks = self.request_callback_tasks.lock().await;
            std::mem::take(&mut *tasks)
        };
        // Finished tasks can be joined immediately. An unfinished callback is
        // detached so user code cannot hold producer shutdown open; its task
        // owns the producer until it returns and still delivers its terminal
        // request result.
        for task in tasks {
            if task.is_finished() {
                let _ = task.await;
            }
        }
    }

    fn unregister_deadline(&self) -> tokio::time::Instant {
        let millis = self.config.client.mq_client_api_timeout().max(0) as u64;
        tokio::time::Instant::now() + tokio::time::Duration::from_millis(millis)
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), ProducerError> {
        self.start_with_factory(true).await
    }

    /// Starts the producer and optionally starts its shared client runtime.
    ///
    /// Pass `false` when the runtime lifecycle is managed by an enclosing
    /// client factory, matching Java's `DefaultMQProducerImpl.start(boolean)`.
    pub async fn start_with_factory(
        self: &Arc<Self>,
        start_factory: bool,
    ) -> Result<(), ProducerError> {
        let mut state = self.state.lock().await;
        let service_state = self.service_state();
        match service_state {
            Some(ProducerServiceState::CreateJust) => {}
            Some(ProducerServiceState::Running) => return Err(ProducerError::AlreadyStarted),
            Some(ProducerServiceState::ShutdownAlready) => return Err(ProducerError::Shutdown),
            Some(ProducerServiceState::StartFailed) | None => {
                return Err(ProducerError::ServiceStateNotReady(service_state));
            }
        }
        match &*state {
            State::Started { .. } => return Err(ProducerError::AlreadyStarted),
            State::Shutdown => return Err(ProducerError::Shutdown),
            State::Created => {}
        }
        self.set_service_state(Some(ProducerServiceState::StartFailed));
        let _ = self.self_weak.set(Arc::downgrade(self));
        let mut client_config = self.config.client_config().clone();
        let group = client_config.with_namespace(self.producer_group().as_deref())?;
        *self
            .producer_group
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = group.clone();
        validation::validate_group(group.as_deref().unwrap_or(""))?;
        self.configured_accumulator_settings()?;
        let _ = self
            .started_topics
            .set(self.topics().map(|topics| topics.snapshot()));

        let mut runtime_client_config = self.config.client_config().clone();
        runtime_client_config.set_vip_channel_enabled(self.send_message_with_vip_channel());

        let runtime = self
            .manager
            .get_or_create_with_hook_and_tls_config(
                &runtime_client_config,
                self.rpc_hook.clone(),
                self.config.tls_config().cloned(),
            )
            .await?;
        self.init_produce_accumulator()?;
        let owner: Arc<dyn ProducerRuntime> = self.clone();
        let producer_group = self.producer_group().ok_or(ProducerError::BlankGroup)?;
        let lease = match runtime.register_producer(&producer_group, owner).await {
            Ok(lease) => lease,
            Err(error) => {
                if matches!(
                    &error,
                    rocketmq_classic_core::runtime::RuntimeError::DuplicateGroup { .. }
                ) {
                    self.set_service_state(Some(ProducerServiceState::CreateJust));
                }
                return Err(error.into());
            }
        };
        if start_factory {
            if let Err(error) = runtime.start().await {
                return Err(error.into());
            }
        }
        self.resume_auto_batch_tasks().await?;
        self.initialize_configured_topic_routes(&runtime).await?;
        *state = State::Started {
            runtime: Arc::downgrade(&runtime),
            lease,
        };
        self.start_trace_dispatcher(&runtime, &producer_group).await;
        let task = self.spawn_detector_task(Arc::downgrade(&runtime));
        *self.detector_task.lock().await = Some(task);
        self.set_service_state(Some(ProducerServiceState::Running));
        // Java sends a heartbeat after every Producer start, including when
        // the enclosing client factory was already running and its start is
        // intentionally skipped.
        let _ = runtime.send_heartbeat_to_all_brokers().await;
        RequestFutureHolder::get_instance()
            .start_scheduled_task(self)
            .await;
        Ok(())
    }

    async fn initialize_configured_topic_routes(
        &self,
        runtime: &Arc<ClientRuntime>,
    ) -> Result<(), ProducerError> {
        let Some(topics) = self.started_topics.get().cloned().flatten() else {
            return Ok(());
        };
        let timeout = Duration::from_millis(
            self.config.client_config().mq_client_api_timeout().max(0) as u64,
        );
        let mut client_config = self.config.client_config().clone();
        for topic in topics {
            let topic = client_config.with_namespace(Some(&topic))?.unwrap_or(topic);
            self.remember_publish_topic(&topic);
            let cached_info = self.cached_publish_info_value(&topic);
            if !cached_info.as_ref().is_some_and(TopicPublishInfo::ok) {
                self.update_startup_topic_route(runtime, &topic, timeout, false)
                    .await?;
            }

            let route_available = self
                .cached_publish_info_value(&topic)
                .is_some_and(|info| info.have_topic_router_info() || info.ok());
            if !route_available {
                self.update_startup_topic_route(runtime, &topic, timeout, true)
                    .await?;
            }
        }
        Ok(())
    }

    async fn update_startup_topic_route(
        &self,
        runtime: &ClientRuntime,
        topic: &str,
        timeout: Duration,
        use_default_topic: bool,
    ) -> Result<(), ProducerError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let result = if use_default_topic {
            runtime
                .update_topic_route_from_default(topic, self.default_topic_queue_nums(), deadline)
                .await
        } else {
            runtime.update_topic_route(topic, deadline).await
        };
        match result {
            Ok(_) => Ok(()),
            Err(error)
                if matches!(
                    &error,
                    rocketmq_classic_core::runtime::RuntimeError::Rpc(
                        rocketmq_classic_core::error::RpcError::Transport(_)
                    )
                ) =>
            {
                Err(error.into())
            }
            Err(error) => {
                tracing::warn!(topic, error = %error, "producer startup topic route update failed");
                Ok(())
            }
        }
    }

    async fn start_trace_dispatcher(&self, runtime: &Arc<ClientRuntime>, producer_group: &str) {
        if !self.config.client.enable_trace() {
            return;
        }
        let dispatcher = match crate::trace::TraceDispatcher::new(
            &self.config.client,
            producer_group,
            crate::TraceProducerKind::Produce,
            self.config.client.trace_topic(),
            self.manager.clone(),
            self.rpc_hook.clone(),
        ) {
            Ok(dispatcher) => Arc::new(dispatcher),
            Err(error) => {
                tracing::error!(error = %error, "message trace dispatcher initialization failed");
                return;
            }
        };
        if let Err(error) = dispatcher.start().await {
            tracing::error!(error = %error, "message trace dispatcher start failed");
            return;
        }
        self.register_send_message_hook(Arc::new(crate::trace::ProducerSendTraceHook::new(
            dispatcher.clone(),
        )));
        self.register_end_transaction_hook(Arc::new(
            crate::trace::ProducerEndTransactionTraceHook::new(
                dispatcher.clone(),
                self.config.client.clone(),
            ),
        ));
        let _ = self.trace_dispatcher.set(dispatcher);
        if crate::trace::recall_trace_property_enabled() {
            self.register_recall_trace_hook(
                runtime,
                self.trace_dispatcher
                    .get()
                    .expect("dispatcher was just set")
                    .clone(),
            )
            .await;
        }
    }

    pub async fn shutdown(&self) -> Result<(), ProducerError> {
        self.shutdown_with_factory(true).await
    }

    /// Shuts down the producer and optionally its shared client runtime.
    ///
    /// Pass `false` when an enclosing client factory owns runtime shutdown,
    /// matching Java's `DefaultMQProducerImpl.shutdown(boolean)`.
    pub async fn shutdown_with_factory(&self, shutdown_factory: bool) -> Result<(), ProducerError> {
        match self.service_state() {
            Some(ProducerServiceState::Running) => {}
            Some(
                ProducerServiceState::CreateJust
                | ProducerServiceState::StartFailed
                | ProducerServiceState::ShutdownAlready,
            ) => return Ok(()),
            None => return Err(ProducerError::ServiceStateNotReady(None)),
        }
        let mut state = self.state.lock().await;
        match &*state {
            State::Created => {
                return Err(ProducerError::ServiceStateNotReady(Some(
                    ProducerServiceState::Running,
                )));
            }
            State::Shutdown => return Err(ProducerError::Shutdown),
            State::Started { .. } => {}
        }
        let previous = std::mem::replace(&mut *state, State::Shutdown);
        match previous {
            State::Created | State::Shutdown => {
                self.remove_recall_trace_hook(None).await;
                self.stop_auto_batch().await;
                self.stop_detector_task().await;
                self.retire_request_callback_tasks().await;
                self.shutdown_trace_dispatcher().await;
                Ok(())
            }
            State::Started { runtime, lease } => {
                let Some(runtime_arc) = runtime.upgrade() else {
                    RequestFutureHolder::get_instance().shutdown(self).await;
                    self.remove_recall_trace_hook(None).await;
                    self.stop_auto_batch().await;
                    self.stop_detector_task().await;
                    self.retire_request_callback_tasks().await;
                    self.shutdown_trace_dispatcher().await;
                    self.set_service_state(Some(ProducerServiceState::ShutdownAlready));
                    return Ok(());
                };
                let deadline = self.unregister_deadline();
                let current_group = self.producer_group();
                match runtime_arc
                    .unregister_producer_by_group_nullable(current_group.as_deref(), deadline)
                    .await
                {
                    Ok(_) => {}
                    Err(error) => {
                        *state = State::Started { runtime, lease };
                        return Err(error.into());
                    }
                }
                if shutdown_factory {
                    match runtime_arc.shutdown().await {
                        Ok(_) => {}
                        Err(error) => {
                            *state = State::Started {
                                runtime: Arc::downgrade(&runtime_arc),
                                lease,
                            };
                            return Err(error.into());
                        }
                    }
                }
                self.remove_recall_trace_hook(Some(&runtime_arc)).await;
                RequestFutureHolder::get_instance().shutdown(self).await;
                self.stop_auto_batch().await;
                self.stop_detector_task().await;
                self.retire_request_callback_tasks().await;
                self.shutdown_trace_dispatcher().await;
                self.set_service_state(Some(ProducerServiceState::ShutdownAlready));
                Ok(())
            }
        }
    }

    async fn shutdown_trace_dispatcher(&self) {
        if let Some(dispatcher) = self.trace_dispatcher.get() {
            if let Err(error) = dispatcher.shutdown().await {
                tracing::warn!(error = %error, "message trace dispatcher shutdown failed");
            }
        }
    }

    /// Stops an owned internal trace producer after its worker was cancelled.
    /// This intentionally omits trace-dispatcher shutdown to avoid recursive
    /// worker ownership during forced cleanup.
    #[doc(hidden)]
    pub async fn shutdown_owned_trace_producer(
        &self,
        cleanup_deadline: tokio::time::Instant,
    ) -> Result<(), ProducerError> {
        match self.service_state() {
            Some(ProducerServiceState::Running) => {}
            Some(
                ProducerServiceState::CreateJust
                | ProducerServiceState::StartFailed
                | ProducerServiceState::ShutdownAlready,
            ) => return Ok(()),
            None => return Err(ProducerError::ServiceStateNotReady(None)),
        }

        let mut state = self.state.lock().await;
        match &*state {
            State::Created => {
                return Err(ProducerError::ServiceStateNotReady(Some(
                    ProducerServiceState::Running,
                )));
            }
            State::Shutdown => return Err(ProducerError::Shutdown),
            State::Started { .. } => {}
        }
        let (runtime_arc, registered_group) = match &*state {
            State::Started { runtime, lease } => {
                (runtime.upgrade(), Some(lease.group().to_owned()))
            }
            State::Created | State::Shutdown => (None, None),
        };
        let Some(runtime_arc) = runtime_arc else {
            RequestFutureHolder::get_instance().shutdown(self).await;
            self.remove_recall_trace_hook(None).await;
            self.stop_auto_batch().await;
            self.stop_detector_task().await;
            self.retire_request_callback_tasks().await;
            *state = State::Shutdown;
            self.set_service_state(Some(ProducerServiceState::ShutdownAlready));
            return Ok(());
        };

        let deadline = self.unregister_deadline().min(cleanup_deadline);
        let registered_group = registered_group.expect("started producer retains its group lease");
        runtime_arc
            .unregister_producer_by_group(&registered_group, deadline)
            .await
            .map_err(ProducerError::from)?;
        self.remove_recall_trace_hook(Some(&runtime_arc)).await;
        RequestFutureHolder::get_instance().shutdown(self).await;
        self.stop_auto_batch().await;
        self.stop_detector_task().await;
        self.retire_request_callback_tasks().await;
        runtime_arc.shutdown().await?;
        *state = State::Shutdown;
        self.set_service_state(Some(ProducerServiceState::ShutdownAlready));
        Ok(())
    }

    async fn remove_recall_trace_hook(&self, runtime: Option<&ClientRuntime>) {
        let hook = self.recall_trace_hook.lock().await.take();
        if let (Some(runtime), Some(hook)) = (runtime, hook.as_ref()) {
            runtime.remoting().remove_rpc_hook(hook);
        }
    }

    async fn register_recall_trace_hook(
        &self,
        runtime: &ClientRuntime,
        dispatcher: Arc<crate::trace::TraceDispatcher>,
    ) {
        let hook: Arc<dyn RpcHook> =
            Arc::new(crate::trace::RecallMessageTraceHook::new(dispatcher));
        runtime.remoting().add_rpc_hook(hook.clone());
        *self.recall_trace_hook.lock().await = Some(hook);
    }

    fn spawn_detector_task(self: &Arc<Self>, runtime: Weak<ClientRuntime>) -> JoinHandle<()> {
        let producer = Arc::downgrade(self);
        tokio::spawn(async move {
            let period = Duration::from_secs(3);
            let mut interval =
                tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            loop {
                interval.tick().await;
                let Some(producer) = producer.upgrade() else {
                    break;
                };
                let Some(runtime) = runtime.upgrade() else {
                    break;
                };
                if producer.start_detector_enable() {
                    producer.detect_brokers(&runtime).await;
                }
            }
        })
    }

    pub(crate) async fn ensure_internal_detector_task(
        self: &Arc<Self>,
        runtime: &Arc<ClientRuntime>,
    ) {
        if !self.start_detector_enable() {
            return;
        }
        let mut task = self.detector_task.lock().await;
        if task.as_ref().is_some_and(|task| !task.is_finished()) {
            return;
        }
        if let Some(finished) = task.take() {
            let _ = finished.await;
        }
        *task = Some(self.spawn_detector_task(Arc::downgrade(runtime)));
    }

    async fn stop_detector_task(&self) {
        if let Some(task) = self.detector_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn detector_task_running_for_test(&self) -> bool {
        self.detector_task
            .lock()
            .await
            .as_ref()
            .is_some_and(|task| !task.is_finished())
    }

    async fn stop_auto_batch(&self) {
        if let Some(accumulator) = self.auto_batch_accumulator.get() {
            if accumulator.engine.stop().await {
                accumulator.notify_settings_changed();
            }
        }
        let mut tasks = self.auto_batch_tasks.lock().await;
        for task in tasks.drain(..) {
            task.abort();
            let _ = task.await;
        }
    }

    async fn detect_brokers(&self, runtime: &ClientRuntime) {
        if !self.start_detector_enable() {
            return;
        }
        let interval = Duration::from_millis(self.config.client.detect_interval().max(0) as u64);
        let due_brokers = self
            .fault
            .take_due_for_detection(StdInstant::now(), interval);
        if due_brokers.is_empty() {
            return;
        }
        let topic = self.publish_info.keys().into_iter().next();
        let Some(topic) = topic else {
            return;
        };
        let Ok(snapshot) = runtime.route_snapshot() else {
            return;
        };
        for broker in due_brokers {
            let Some(address) = snapshot.publish_address(&broker) else {
                self.fault.remove(&broker);
                continue;
            };
            let vip_enabled = runtime
                .config()
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .vip_channel_enabled();
            let Ok(address) =
                rocketmq_classic_core::route::broker_vip_channel(vip_enabled, address)
            else {
                continue;
            };
            let deadline = tokio::time::Instant::now()
                + tokio::time::Duration::from_millis(
                    self.config.client.detect_timeout().max(0) as u64
                );
            if runtime
                .api()
                .probe_broker(&address, &topic, deadline)
                .await
                .is_ok()
            {
                self.fault.mark_reachable(&broker);
            }
        }
    }

    pub(crate) fn cached_publish_info(&self, topic: &str) -> Option<Arc<TopicSnapshot>> {
        self.publish_info.get(topic)?.snapshot().ok().map(Arc::new)
    }

    pub(crate) fn cached_publish_info_value(&self, topic: &str) -> Option<TopicPublishInfo> {
        self.publish_info.get(topic)
    }

    pub(crate) fn remember_publish_topic(&self, topic: &str) {
        self.publish_info
            .put_if_absent(topic.to_owned(), TopicPublishInfo::default());
    }

    pub(crate) fn cache_publish_info(&self, topic: &str, info: Arc<TopicSnapshot>) {
        self.remember_publish_topic(topic);
        self.publish_info.put(
            topic.to_owned(),
            TopicPublishInfo::from_snapshot((*info).clone(), true),
        );
    }
}

impl ProducerRuntime for Producer {
    fn transaction_processor(
        &self,
    ) -> Option<Arc<dyn rocketmq_classic_remoting::client::RequestProcessor>> {
        self.transaction
            .get()
            .map(crate::transaction::TransactionRuntime::request_processor)
    }

    fn publish_topics(&self) -> BTreeSet<String> {
        let mut topics = self.publish_info.keys();
        let mut client_config = self.config.client_config().clone();
        let configured_topics = self
            .started_topics
            .get()
            .cloned()
            .unwrap_or_else(|| self.topics().map(|topics| topics.snapshot()));
        topics.extend(
            configured_topics
                .unwrap_or_default()
                .iter()
                .filter_map(|topic| {
                    client_config
                        .with_namespace(Some(topic))
                        .ok()
                        .flatten()
                        .or_else(|| Some(topic.clone()))
                }),
        );
        topics
    }

    fn is_publish_topic_need_update(&self, topic: &str) -> bool {
        self.cached_publish_info(topic)
            .is_none_or(|info| info.publish_queues.is_empty())
    }

    fn update_topic_publish_info(&self, topic: &str, info: Arc<TopicSnapshot>) {
        self.cache_publish_info(topic, info);
    }
}

#[cfg(test)]
use rocketmq_classic_core::api::producer::{SendKind, SendMode, SendResult};

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct ScriptedSendAttempt {
    pub(crate) broker: String,
    pub(crate) address: String,
    pub(crate) producer_group: Option<String>,
    pub(crate) queue: rocketmq_classic_common::message::MessageQueue,
    pub(crate) default_topic: String,
    pub(crate) default_topic_queue_nums: i32,
    pub(crate) body: Vec<u8>,
    pub(crate) topic: String,
    pub(crate) properties: Option<String>,
    pub(crate) sys_flag: i32,
    pub(crate) unique_id: String,
    pub(crate) mode: SendMode,
    pub(crate) batch: bool,
    pub(crate) send_kind: SendKind,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct ScriptedTransport {
    pub(crate) responses: std::collections::VecDeque<Result<Option<SendResult>, ProducerError>>,
    pub(crate) attempts: Vec<ScriptedSendAttempt>,
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ConstructorRpcHook;

    impl RpcHook for ConstructorRpcHook {}

    #[test]
    fn rpc_hook_constructor_path_retains_the_supplied_owner() {
        let config = ProducerConfig::new("producer-rpc-hook-constructor").unwrap();
        let hook: Arc<dyn RpcHook> = Arc::new(ConstructorRpcHook);
        let producer = Producer::with_runtime_manager_and_hook(
            config,
            ClientRuntimeManager::new(),
            Some(hook.clone()),
        );

        assert!(Arc::ptr_eq(
            producer.rpc_hook.as_ref().expect("RPC hook is retained"),
            &hook
        ));
    }

    #[test]
    fn configured_publish_topics_are_namespace_wrapped_once() {
        let mut config = ProducerConfig::new("namespace-publish-topics").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".into()));
        config.set_topics(Some(
            vec![
                "orders".into(),
                "tenant%already".into(),
                "%RETRY%workers".into(),
                "RMQ_SYS_TRACE_TOPIC".into(),
            ]
            .into(),
        ));
        let producer = Producer::with_runtime_manager(config, ClientRuntimeManager::new());

        assert_eq!(
            producer.publish_topics(),
            BTreeSet::from([
                "tenant%orders".to_owned(),
                "tenant%already".to_owned(),
                "%RETRY%tenant%workers".to_owned(),
                "RMQ_SYS_TRACE_TOPIC".to_owned(),
            ])
        );
    }

    #[test]
    fn publish_topics_include_cached_dynamic_topics_and_configured_topics() {
        let mut config = ProducerConfig::new("namespace-publish-topics").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".into()));
        config.set_topics(Some(vec!["configured".into()].into()));
        let producer = Producer::with_runtime_manager(config, ClientRuntimeManager::new());
        let route = Arc::new(
            TopicSnapshot::from_route_data(
                "tenant%sent-before",
                &rocketmq_classic_protocol::route::TopicRouteData::default(),
            )
            .unwrap(),
        );
        producer.update_topic_publish_info("tenant%sent-before", route);

        assert_eq!(
            producer.publish_topics(),
            BTreeSet::from([
                "tenant%configured".to_owned(),
                "tenant%sent-before".to_owned(),
            ])
        );
    }

    #[tokio::test]
    async fn topic_setter_after_start_changes_getter_but_not_runtime_topics() {
        use bytes::BytesMut;
        use rocketmq_classic_protocol::codes::{request, response};
        use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::{TcpListener, TcpStream},
        };

        async fn read_command(stream: &mut TcpStream) -> Option<RemotingCommand> {
            let mut length = [0; 4];
            if stream.read_exact(&mut length).await.is_err() {
                return None;
            }
            let mut bytes = BytesMut::from(&length[..]);
            bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut bytes[4..]).await.ok()?;
            frame::decode_next(&mut bytes).ok().flatten()
        }

        let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let nameserver_address = nameserver.local_addr().unwrap().to_string();
        let nameserver_peer = tokio::spawn(async move {
            let (mut stream, _) = nameserver.accept().await.unwrap();
            while let Some(command) = read_command(&mut stream).await {
                assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
                let mut reply = RemotingCommand {
                    code: response::TOPIC_NOT_EXIST,
                    opaque: command.opaque,
                    ..Default::default()
                };
                reply.mark_response();
                stream
                    .write_all(&frame::encode(&reply).unwrap())
                    .await
                    .unwrap();
            }
        });

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let expected = &fixture["runtimeSetterFlows"];
        let mut config = ProducerConfig::new("topics-live-start-snapshot").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(nameserver_address));
        config.set_topics(Some(vec!["startup-topic".into()].into()));
        let producer = Arc::new(Producer::with_runtime_manager(
            config,
            ClientRuntimeManager::new(),
        ));
        producer.start().await.unwrap();

        let topics = ProducerTopicList::from(vec!["later-topic".to_owned()]);
        let caller_alias = topics.clone();
        producer.set_topics(Some(topics));
        caller_alias.push("caller-added-topic");

        assert_eq!(
            producer.topics().map(|topics| topics.snapshot()),
            Some(
                expected["topicsAfterSetter"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|topic| topic.as_str().unwrap().to_owned())
                    .collect()
            )
        );
        assert_eq!(
            expected["topicListIdentityAfterSetter"].as_bool(),
            Some(true)
        );
        assert_eq!(
            producer.publish_topics(),
            expected["publishTopicsAfterSetter"]
                .as_array()
                .unwrap()
                .iter()
                .map(|topic| topic.as_str().unwrap().to_owned())
                .collect()
        );
        producer.shutdown().await.unwrap();
        nameserver_peer.abort();
    }

    #[tokio::test]
    async fn invalid_group_fails_before_creating_or_registering_runtime() {
        let manager = ClientRuntimeManager::new();
        let config = ProducerConfig::new("bad.group").unwrap();
        let producer = Arc::new(Producer::with_runtime_manager(config, manager.clone()));

        assert!(matches!(
            producer.start().await,
            Err(ProducerError::IllegalGroup)
        ));
        assert_eq!(manager.factory_table().len().unwrap(), 0);
    }

    #[tokio::test]
    async fn invalid_accumulator_settings_fail_before_runtime_creation() {
        let manager = ClientRuntimeManager::new();
        let mut config = ProducerConfig::new("accumulator-producer").unwrap();
        config.set_batch_max_delay_millis(0);
        let producer = Arc::new(Producer::with_runtime_manager(config, manager.clone()));

        assert!(matches!(
            producer.start().await,
            Err(ProducerError::InvalidBatchMaxDelay(0))
        ));
        assert_eq!(
            producer.service_state(),
            Some(ProducerServiceState::StartFailed)
        );
        assert!(matches!(
            producer.start().await,
            Err(ProducerError::ServiceStateNotReady(Some(
                ProducerServiceState::StartFailed
            )))
        ));
        assert_eq!(manager.factory_table().len().unwrap(), 0);
    }

    #[tokio::test]
    async fn runtime_start_failure_preserves_java_factory_registration() {
        let java_fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/lifecycle.json"))
                .unwrap();
        let java_failure = &java_fixture["startupFactoryStartFailure"];
        assert_eq!(java_failure["exception"], "IllegalArgumentException");
        assert_eq!(java_failure["producerRegistered"], true);
        assert_eq!(java_failure["producerServiceState"], "START_FAILED");
        assert_eq!(java_failure["factoryServiceState"], "START_FAILED");

        let manager = ClientRuntimeManager::new();
        let mut config = ProducerConfig::new("start-rollback-producer").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        config.client_config_mut().set_poll_name_server_interval(0);
        let runtime = manager.get_or_create(config.client_config()).await.unwrap();
        let producer = Arc::new(Producer::with_runtime_manager(
            config.clone(),
            manager.clone(),
        ));

        assert!(matches!(
            producer.start().await,
            Err(ProducerError::Runtime(
                rocketmq_classic_core::runtime::RuntimeError::InvalidSchedule
            ))
        ));
        assert_eq!(runtime.producer_table().len().unwrap(), 1);
        assert!(
            runtime
                .select_producer("start-rollback-producer")
                .unwrap()
                .is_some()
        );
        assert_eq!(
            producer.service_state(),
            Some(ProducerServiceState::StartFailed)
        );
        assert!(matches!(
            producer.started_runtime().await,
            Err(ProducerError::NotStarted)
        ));
        assert!(Arc::ptr_eq(
            &runtime,
            &manager.get_or_create(config.client_config()).await.unwrap()
        ));
        assert!(
            runtime
                .unregister_producer_by_group(
                    "start-rollback-producer",
                    tokio::time::Instant::now() + Duration::from_secs(1),
                )
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn repeated_start_is_rejected_and_shutdown_is_idempotent() {
        let manager = ClientRuntimeManager::new();
        let mut config = ProducerConfig::new("detector-producer").unwrap();
        config.client_config_mut().set_start_detector_enable(true);
        let producer = Arc::new(Producer::with_runtime_manager(config, manager));

        assert_eq!(
            producer.service_state(),
            Some(ProducerServiceState::CreateJust)
        );
        producer.start().await.unwrap();
        assert_eq!(
            producer.service_state(),
            Some(ProducerServiceState::Running)
        );
        assert!(matches!(
            producer.start().await,
            Err(ProducerError::AlreadyStarted)
        ));
        assert!(producer.detector_task.lock().await.is_some());
        producer.shutdown().await.unwrap();
        assert_eq!(
            producer.service_state(),
            Some(ProducerServiceState::ShutdownAlready)
        );
        assert!(producer.detector_task.lock().await.is_none());
        producer.shutdown().await.unwrap();
        assert!(matches!(
            producer.start().await,
            Err(ProducerError::Shutdown)
        ));
    }

    #[tokio::test]
    async fn duplicate_group_registration_resets_the_public_service_state() {
        let manager = ClientRuntimeManager::new();
        let config = ProducerConfig::new("duplicate-producer-state").unwrap();
        let first = Arc::new(Producer::with_runtime_manager(
            config.clone(),
            manager.clone(),
        ));
        let second = Arc::new(Producer::with_runtime_manager(config, manager));

        first.start().await.unwrap();
        assert!(matches!(
            second.start().await,
            Err(ProducerError::Runtime(
                rocketmq_classic_core::runtime::RuntimeError::DuplicateGroup { .. }
            ))
        ));
        assert_eq!(
            second.service_state(),
            Some(ProducerServiceState::CreateJust)
        );
        first.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn runtime_detector_toggle_skips_disabled_due_work_and_joins_on_shutdown() {
        let manager = ClientRuntimeManager::new();
        let producer = Arc::new(Producer::with_runtime_manager(
            ProducerConfig::new("runtime-detector-toggle").unwrap(),
            manager,
        ));

        assert!(!producer.start_detector_enable());
        producer.start().await.unwrap();
        assert!(producer.detector_task.lock().await.is_some());
        let runtime = producer.started_runtime().await.unwrap();
        producer
            .fault
            .update(
                "disabled-detector-broker",
                Duration::from_millis(100),
                false,
                true,
                StdInstant::now(),
            )
            .unwrap();
        producer.detect_brokers(&runtime).await;
        assert_eq!(
            producer.fault.take_due_for_detection(
                StdInstant::now() + Duration::from_secs(1),
                Duration::from_secs(3)
            ),
            ["disabled-detector-broker"]
        );
        producer.set_start_detector_enable(true);
        assert!(producer.start_detector_enable());
        producer.set_start_detector_enable(false);
        assert!(!producer.start_detector_enable());

        producer.shutdown().await.unwrap();
        assert!(producer.detector_task.lock().await.is_none());
    }

    #[tokio::test]
    async fn shutdown_before_start_keeps_producer_startable() {
        let manager = ClientRuntimeManager::new();
        let config = ProducerConfig::new("shutdown-before-start").unwrap();
        let producer = Arc::new(Producer::with_runtime_manager(config, manager));

        producer.shutdown().await.unwrap();
        assert_eq!(
            producer.service_state(),
            Some(ProducerServiceState::CreateJust)
        );
        assert!(matches!(
            producer.started_runtime().await,
            Err(ProducerError::NotStarted)
        ));
        producer.start().await.unwrap();
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn factory_lifecycle_overloads_leave_runtime_control_to_the_caller() {
        let manager = ClientRuntimeManager::new();
        let mut config = ProducerConfig::new("factory-controlled-producer").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        let producer = Arc::new(Producer::with_runtime_manager(config, manager));

        producer.start_with_factory(false).await.unwrap();
        let (runtime, _) = producer.started_registration().await.unwrap();
        assert_eq!(
            runtime.state(),
            rocketmq_classic_core::runtime::RuntimeState::Created
        );
        runtime.start().await.unwrap();

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/lifecycle.json"))
                .unwrap();
        assert_eq!(
            fixture["sameGroup"]["clientFactoryServiceStateAfterShutdown"],
            "RUNNING"
        );

        producer.shutdown_with_factory(false).await.unwrap();
        assert_eq!(
            runtime.state(),
            rocketmq_classic_core::runtime::RuntimeState::Running
        );
        assert_eq!(
            producer.service_state(),
            Some(ProducerServiceState::ShutdownAlready)
        );
        assert!(runtime.shutdown().await.unwrap());
    }

    #[tokio::test]
    async fn producer_start_sends_a_heartbeat_to_cached_brokers() {
        use bytes::BytesMut;
        use rocketmq_classic_protocol::codes::{request, response};
        use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::{TcpListener, TcpStream},
        };

        async fn read_command(stream: &mut TcpStream) -> RemotingCommand {
            let mut length = [0; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut bytes = BytesMut::from(&length[..]);
            bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut bytes[4..]).await.unwrap();
            frame::decode_next(&mut bytes).unwrap().unwrap()
        }

        async fn write_response(
            stream: &mut TcpStream,
            command: &RemotingCommand,
            code: i32,
            body: Option<Vec<u8>>,
        ) {
            let mut reply = RemotingCommand {
                code,
                opaque: command.opaque,
                body,
                ..RemotingCommand::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        }

        let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let broker_address = broker.local_addr().unwrap().to_string();
        let broker_peer = tokio::spawn(async move {
            let (mut stream, _) = broker.accept().await.unwrap();
            let command = read_command(&mut stream).await;
            assert_eq!(command.code, request::HEART_BEAT);
            let heartbeat: serde_json::Value =
                serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
            write_response(&mut stream, &command, response::SUCCESS, None).await;
            heartbeat
        });

        let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let nameserver_address = nameserver.local_addr().unwrap().to_string();
        let route = serde_json::to_vec(&serde_json::json!({
            "queueDatas": [{
                "brokerName": "startup-broker",
                "readQueueNums": 1,
                "writeQueueNums": 1,
                "perm": 6,
                "topicSysFlag": 0
            }],
            "brokerDatas": [{
                "cluster": "DefaultCluster",
                "brokerName": "startup-broker",
                "brokerAddrs": {"0": broker_address}
            }],
            "filterServerTable": {}
        }))
        .unwrap();
        let nameserver_peer = tokio::spawn(async move {
            let (mut stream, _) = nameserver.accept().await.unwrap();
            let command = read_command(&mut stream).await;
            assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
            write_response(&mut stream, &command, response::SUCCESS, Some(route)).await;
        });

        let manager = ClientRuntimeManager::new();
        let mut config = ProducerConfig::new("startup-heartbeat-producer").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(nameserver_address));
        config
            .client_config_mut()
            .set_enable_heartbeat_channel_event_listener(false);
        let runtime = manager.get_or_create(config.client_config()).await.unwrap();
        runtime.start().await.unwrap();
        assert!(
            runtime
                .update_topic_route(
                    "existing-topic",
                    tokio::time::Instant::now() + Duration::from_secs(2)
                )
                .await
                .unwrap()
        );
        nameserver_peer.await.unwrap();

        let producer = Arc::new(Producer::with_runtime_manager(config, manager));
        producer.start_with_factory(false).await.unwrap();
        let heartbeat = tokio::time::timeout(Duration::from_secs(2), broker_peer)
            .await
            .unwrap()
            .unwrap();
        assert!(
            heartbeat["producerDataSet"]
                .as_array()
                .unwrap()
                .iter()
                .any(|producer| producer["groupName"] == "startup-heartbeat-producer")
        );

        producer.shutdown_with_factory(false).await.unwrap();
        assert!(runtime.shutdown().await.unwrap());
    }

    #[tokio::test]
    async fn producer_start_skips_route_lookup_for_cached_publish_info() {
        use tokio::net::TcpListener;

        let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let nameserver_address = nameserver.local_addr().unwrap().to_string();
        let nameserver_peer = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(250), nameserver.accept())
                .await
                .is_ok()
        });

        let mut config = ProducerConfig::new("startup-cached-route-producer").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(nameserver_address));
        config.set_topics(Some(vec!["startup-cached-topic".into()].into()));
        let producer = Arc::new(Producer::with_runtime_manager(
            config,
            ClientRuntimeManager::new(),
        ));
        let cached_info = TopicPublishInfo::default();
        cached_info.set_message_queue_list(Some(crate::ProducerPublishQueueList::new(vec![
            rocketmq_classic_common::message::MessageQueue::new(
                "startup-cached-topic",
                "cached-broker",
                0,
            ),
        ])));
        producer
            .get_topic_publish_info_table()
            .put("startup-cached-topic", cached_info);

        producer.start_with_factory(false).await.unwrap();
        assert!(!nameserver_peer.await.unwrap());
        producer.shutdown_with_factory(false).await.unwrap();
    }

    #[tokio::test]
    async fn producer_start_preserves_java_registration_after_route_transport_failure() {
        use bytes::BytesMut;
        use rocketmq_classic_protocol::codes::request;
        use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
        use tokio::{
            io::AsyncReadExt,
            net::{TcpListener, TcpStream},
        };

        let java_fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/lifecycle.json"))
                .unwrap();
        let java_failure = &java_fixture["startupRouteTransportFailure"];
        assert_eq!(java_failure["exception"], "IllegalStateException");
        assert_eq!(java_failure["cause"], "RemotingSendRequestException");
        assert_eq!(java_failure["requestCount"], 1);
        assert_eq!(java_failure["producerRegistered"], true);
        assert_eq!(java_failure["producerServiceState"], "START_FAILED");

        async fn read_command(stream: &mut TcpStream) -> RemotingCommand {
            let mut length = [0; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut bytes = BytesMut::from(&length[..]);
            bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut bytes[4..]).await.unwrap();
            frame::decode_next(&mut bytes).unwrap().unwrap()
        }

        let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let nameserver_address = nameserver.local_addr().unwrap().to_string();
        let nameserver_peer = tokio::spawn(async move {
            let (mut stream, _) = nameserver.accept().await.unwrap();
            let command = read_command(&mut stream).await;
            assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
            // Closing without a remoting response models a transport failure.
        });

        let manager = ClientRuntimeManager::new();
        let mut config = ProducerConfig::new("startup-route-transport-failure").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(nameserver_address));
        config.set_topics(Some(vec!["startup-topic".into()].into()));
        let runtime = manager.get_or_create(config.client_config()).await.unwrap();
        runtime.start().await.unwrap();

        let producer = Arc::new(Producer::with_runtime_manager(config, manager));
        assert!(producer.start_with_factory(false).await.is_err());
        nameserver_peer.await.unwrap();
        assert_eq!(
            producer.service_state(),
            Some(ProducerServiceState::StartFailed)
        );
        assert!(
            runtime
                .select_producer("startup-route-transport-failure")
                .unwrap()
                .is_some()
        );
        assert!(
            runtime
                .unregister_producer_by_group(
                    "startup-route-transport-failure",
                    tokio::time::Instant::now() + Duration::from_secs(2),
                )
                .await
                .unwrap()
        );
        assert!(runtime.shutdown().await.unwrap());
    }

    #[tokio::test]
    async fn shutdown_unregisters_the_live_group_like_pinned_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/lifecycle.json"))
                .unwrap();
        let java_missing = &fixture["changedToMissingGroup"];
        assert_eq!(java_missing["shutdownException"], "");
        assert_eq!(java_missing["originalGroupRemains"], true);
        assert_eq!(java_missing["currentGroupRemains"], false);
        assert_eq!(java_missing["serviceState"], "SHUTDOWN_ALREADY");
        let java = &fixture["changedToOtherProducerGroup"];
        assert_eq!(java["shutdownException"], "");
        assert_eq!(java["originalGroupRemains"], true);
        assert_eq!(java["otherGroupRemains"], false);
        assert_eq!(java["serviceState"], "SHUTDOWN_ALREADY");

        let missing_manager = ClientRuntimeManager::new();
        let mut missing_config = ProducerConfig::new("original-group").unwrap();
        missing_config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        let missing = Arc::new(Producer::with_runtime_manager(
            missing_config,
            missing_manager,
        ));
        missing.start().await.unwrap();
        let (missing_runtime, missing_lease) = missing.started_registration().await.unwrap();
        missing.set_producer_group(Some("missing-current-group".into()));
        missing.shutdown().await.unwrap();
        assert!(
            missing_runtime
                .select_producer("original-group")
                .unwrap()
                .is_some()
        );
        assert!(
            missing_runtime
                .select_producer("missing-current-group")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            missing_runtime.state(),
            rocketmq_classic_core::runtime::RuntimeState::Running
        );
        assert!(
            missing_runtime
                .unregister_producer(&missing_lease, tokio::time::Instant::now())
                .await
                .unwrap()
        );
        assert!(missing_runtime.shutdown().await.unwrap());

        let manager = ClientRuntimeManager::new();
        let mut first_config = ProducerConfig::new("original-group-collision").unwrap();
        first_config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        let mut second_config = ProducerConfig::new("other-group").unwrap();
        *second_config.client_config_mut() = first_config.client_config().clone();
        let first = Arc::new(Producer::with_runtime_manager(
            first_config,
            manager.clone(),
        ));
        let second = Arc::new(Producer::with_runtime_manager(second_config, manager));
        first.start().await.unwrap();
        second.start().await.unwrap();
        let (runtime, first_lease) = first.started_registration().await.unwrap();
        let (second_runtime, _) = second.started_registration().await.unwrap();
        assert!(Arc::ptr_eq(&runtime, &second_runtime));
        assert!(
            runtime
                .select_producer("original-group-collision")
                .unwrap()
                .is_some()
        );
        assert!(runtime.select_producer("other-group").unwrap().is_some());

        first.set_producer_group(Some("other-group".into()));
        first.shutdown().await.unwrap();

        assert!(
            runtime
                .select_producer("original-group-collision")
                .unwrap()
                .is_some()
        );
        assert!(runtime.select_producer("other-group").unwrap().is_none());
        assert_eq!(
            runtime.state(),
            rocketmq_classic_core::runtime::RuntimeState::Running
        );
        assert_eq!(
            first.service_state(),
            Some(ProducerServiceState::ShutdownAlready)
        );
        assert_eq!(second.service_state(), Some(ProducerServiceState::Running));

        second.shutdown().await.unwrap();
        assert!(
            runtime
                .unregister_producer(&first_lease, tokio::time::Instant::now())
                .await
                .unwrap()
        );
        assert!(runtime.shutdown().await.unwrap());
    }

    #[tokio::test]
    async fn enabled_trace_starts_and_stops_its_owned_dispatcher() {
        let manager = ClientRuntimeManager::new();
        let mut config = ProducerConfig::new("trace-lifecycle-producer").unwrap();
        config.client_config_mut().set_enable_trace(true);
        let producer = Arc::new(Producer::with_runtime_manager(config, manager));

        producer.start().await.unwrap();
        assert!(producer.trace_dispatcher().is_some());
        let dispatcher = producer.trace_dispatcher().unwrap();
        let trace_producer = dispatcher.trace_producer().unwrap();
        assert_eq!(producer.send_message_hook_snapshot().len(), 1);
        assert_eq!(
            producer
                .end_transaction_hooks
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .map(|hook| hook.hook_name())
                .collect::<Vec<_>>(),
            ["EndTransactionTraceHook"]
        );
        assert!(
            dispatcher.append(rocketmq_classic_core::trace::TraceContext {
                trace_type: Some(rocketmq_classic_core::trace::TraceType::Recall),
                timestamp_millis: 1,
                region_id: None,
                region_name: Some(String::new()),
                group_name: None,
                cost_time_millis: 0,
                success: true,
                request_id: None,
                context_code: 0,
                access_channel: None,
                trace_beans: Some(vec![rocketmq_classic_core::trace::TraceBean::default()]),
            })
        );
        dispatcher.flush().await.unwrap();
        producer.shutdown().await.unwrap();
        let trace = rocketmq_classic_core::trace::TraceContext {
            trace_type: Some(rocketmq_classic_core::trace::TraceType::Recall),
            timestamp_millis: 1,
            region_id: Some("region-a".into()),
            region_name: Some(String::new()),
            group_name: Some("trace-lifecycle-producer".into()),
            cost_time_millis: 0,
            success: true,
            request_id: None,
            context_code: 0,
            access_channel: None,
            trace_beans: Some(vec![]),
        };
        assert!(!dispatcher.append(trace));
        assert!(Arc::ptr_eq(
            &trace_producer,
            &dispatcher.trace_producer().unwrap()
        ));
    }

    #[tokio::test]
    async fn recall_trace_rpc_hook_is_registered_and_removed_with_its_producer() {
        let manager = ClientRuntimeManager::new();
        let config = ProducerConfig::new("recall-trace-lifecycle").unwrap();
        let runtime = manager.get_or_create(config.client_config()).await.unwrap();
        let producer = Arc::new(Producer::with_runtime_manager(
            config.clone(),
            manager.clone(),
        ));
        let dispatcher = Arc::new(
            crate::trace::TraceDispatcher::new(
                config.client_config(),
                config
                    .producer_group()
                    .expect("the trace producer fixture has a configured group"),
                crate::TraceProducerKind::Produce,
                None,
                manager,
                None,
            )
            .unwrap(),
        );
        let hooks_before = runtime.remoting().get_rpc_hooks().len();

        producer
            .register_recall_trace_hook(&runtime, dispatcher)
            .await;
        assert_eq!(runtime.remoting().get_rpc_hooks().len(), hooks_before + 1);
        producer.remove_recall_trace_hook(Some(&runtime)).await;
        assert_eq!(runtime.remoting().get_rpc_hooks().len(), hooks_before);
    }
}
