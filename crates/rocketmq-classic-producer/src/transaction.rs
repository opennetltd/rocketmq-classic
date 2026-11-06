use std::{
    any::Any,
    ops::Deref,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, PoisonError, RwLock, Weak,
        atomic::{AtomicI32, Ordering},
    },
    time::Duration,
};

use rocketmq_classic_common::{
    message::{DecodeOptions, Message, MessageExt, constants::*, decode_one},
    sysflag::message as message_sys_flag,
};
use rocketmq_classic_core::api::producer::{SendResult, SendStatus};
pub use rocketmq_classic_core::trace::LocalTransactionState;
use rocketmq_classic_protocol::{
    codes::request,
    header::{CheckTransactionStateRequestHeader, CommandHeader, EndTransactionRequestHeader},
};
use rocketmq_classic_remoting::{RemotingCommand, client::RequestProcessor};
use tokio::{
    runtime::Handle,
    sync::{Mutex as AsyncMutex, Semaphore, mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};

use crate::{DEFAULT_PRODUCER_GROUP, Producer, ProducerConfig, ProducerError};

const DEFAULT_CHECK_THREAD_POOL_MIN_SIZE: i32 = 1;
const DEFAULT_CHECK_THREAD_POOL_MAX_SIZE: i32 = 1;
const DEFAULT_CHECK_REQUEST_HOLD_MAX: i32 = 2_000;
const CUSTOM_EXECUTOR_QUEUE_CAPACITY: usize = DEFAULT_CHECK_REQUEST_HOLD_MAX as usize;
const CHECK_WORKER_KEEPALIVE: Duration = Duration::from_secs(60);
#[cfg(not(test))]
const CHECK_END_TRANSACTION_TIMEOUT: Duration = Duration::from_millis(3_000);

fn transaction_flag(state: LocalTransactionState) -> i32 {
    match state {
        LocalTransactionState::Commit => message_sys_flag::TRANSACTION_COMMIT_TYPE,
        LocalTransactionState::Rollback => message_sys_flag::TRANSACTION_ROLLBACK_TYPE,
        LocalTransactionState::Unknown => message_sys_flag::TRANSACTION_NOT_TYPE,
    }
}

/// Error returned by a Rust transaction callback, adapting a Java callback
/// exception to the callback's typed `Result` contract. Java's throwable
/// message is read-only, so the stored message is exposed through `message()`.
///
/// ```compile_fail
/// use rocketmq_classic_producer::TransactionCallbackError;
/// let error = TransactionCallbackError::new("callback failed");
/// let _ = error.0;
/// ```
///
/// It does not expose structural value equality or cloning by message; Java
/// `Throwable` inherits identity-based equality.
///
/// ```compile_fail
/// use rocketmq_classic_producer::TransactionCallbackError;
/// fn requires_value_traits<T: Clone + PartialEq + Eq>() {}
/// requires_value_traits::<TransactionCallbackError>();
/// ```
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct TransactionCallbackError(String);

impl TransactionCallbackError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    pub fn message(&self) -> &str {
        &self.0
    }
}

/// Transaction callbacks run outside the remoting reader. A callback error,
/// panic, or null-like `None` result resolves to `Unknown`.
pub trait TransactionListener: Send + Sync + 'static {
    fn execute_local_transaction(
        &self,
        message: &mut Message,
        argument: Option<&(dyn Any + Send + Sync)>,
    ) -> Result<Option<LocalTransactionState>, TransactionCallbackError>;

    fn check_local_transaction(
        &self,
        message: &mut MessageExt,
    ) -> Result<Option<LocalTransactionState>, TransactionCallbackError>;
}

/// Deprecated Java-compatible transaction check callback.
#[deprecated(note = "set a TransactionListener and implement check_local_transaction")]
pub trait TransactionCheckListener: Send + Sync + 'static {
    fn check_local_transaction_state(
        &self,
        message: &mut MessageExt,
    ) -> Result<Option<LocalTransactionState>, TransactionCallbackError>;
}

/// A caller-supplied callback executor, corresponding to Java's ExecutorService.
/// `execute` should enqueue the task and return promptly. The producer calls
/// `shutdown` when it shuts down, matching TransactionMQProducer ownership.
pub trait TransactionExecutor: Send + Sync + 'static {
    fn execute(&self, task: Box<dyn FnOnce() + Send + 'static>) -> Result<(), String>;

    fn shutdown(&self) {}
}

/// Rust transaction-send result containing the complete ordinary send result
/// plus the local transaction decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionSendResult {
    pub send_result: SendResult,
    pub local_transaction_state: LocalTransactionState,
}

impl Deref for TransactionSendResult {
    type Target = SendResult;

    fn deref(&self) -> &Self::Target {
        &self.send_result
    }
}

/// Transaction producer API corresponding to Java `TransactionMQProducer`.
/// Ordinary producer methods remain available through `Deref<Target = Producer>`.
pub struct TransactionProducer {
    producer: Arc<Producer>,
    transaction: Arc<TransactionRuntime>,
}

impl TransactionProducer {
    pub fn new(config: ProducerConfig) -> Self {
        Self::new_with_rpc_hook(config, None)
    }

    /// Creates a transaction producer whose remoting requests use the supplied RPC hook.
    pub fn new_with_rpc_hook(
        config: ProducerConfig,
        rpc_hook: Option<Arc<dyn rocketmq_classic_remoting::client::RpcHook>>,
    ) -> Self {
        let producer = Arc::new(Producer::new_with_rpc_hook(config, rpc_hook));
        let transaction = Arc::new(TransactionRuntime::new(Arc::downgrade(&producer)));
        let _ = producer.transaction.set(transaction.clone());
        Self {
            producer,
            transaction,
        }
    }

    pub fn with_default_group() -> Result<Self, ProducerError> {
        Ok(Self::new(ProducerConfig::new(DEFAULT_PRODUCER_GROUP)?))
    }

    pub async fn start(&self) -> Result<(), ProducerError> {
        self.transaction.start_pool()?;
        match self.producer.start().await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.transaction.shutdown_pool(Duration::from_secs(1)).await;
                Err(error)
            }
        }
    }

    pub async fn shutdown(&self) -> Result<(), ProducerError> {
        self.transaction.shutdown_pool(Duration::from_secs(3)).await;
        self.producer.shutdown().await
    }

    /// Namespaces and prepares `message` before attempting the half send, as
    /// Java 5.5.1 does. The caller message therefore retains `TRAN_MSG` and
    /// `PGROUP` after a transport failure.
    pub async fn send_message_in_transaction(
        &self,
        message: &mut Message,
        argument: Option<Arc<dyn Any + Send + Sync>>,
    ) -> Result<TransactionSendResult, ProducerError> {
        let listener = self
            .transaction
            .transaction_listener()
            .ok_or(ProducerError::TransactionListenerMissing)?;

        let mut client_config = self.producer.config().client_config().clone();
        if let Some(topic) = client_config.with_namespace(Some(message.topic()))? {
            message.set_topic(topic);
        }
        if delayed_transaction_property(message) {
            return Err(ProducerError::DelayedTransactionalMessage);
        }
        crate::validation::validate_message(Some(message), self.producer.config())?;

        let _ = message.set_system_property(PROPERTY_TRANSACTION_PREPARED, "true");
        if let Some(group) = self.producer.producer_group() {
            let _ = message.set_system_property(PROPERTY_PRODUCER_GROUP, group);
        } else {
            message.clear_system_property(PROPERTY_PRODUCER_GROUP);
        }

        let timeout =
            Duration::from_millis(self.producer.config().send_timeout_millis().max(0) as u64);
        let send_result = self
            .producer
            .send_with_timeout(message, timeout)
            .await
            .map_err(|error| ProducerError::TransactionSend(Box::new(error)))?;

        if message
            .properties()
            .get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .is_none()
        {
            let _ = message.set_system_property(
                PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                send_result.msg_id.clone(),
            );
        }
        if let Some(transaction_id) = send_result.transaction_id.as_deref() {
            let _ = message.set_system_property("__transactionId__", transaction_id);
        }
        if let Some(transaction_id) = message
            .get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .filter(|value| !value.is_empty())
        {
            message.set_transaction_id(Some(transaction_id));
        }

        let (local_state, callback_error) = match send_result.status {
            SendStatus::SendOk => {
                let backup_message = message.clone();
                let mut callback_message = backup_message.clone();
                let callback_argument = argument.clone();
                let outcome = tokio::task::spawn_blocking(move || {
                    let (state, error) = call_local_transaction(
                        listener,
                        &mut callback_message,
                        callback_argument.as_deref(),
                    );
                    (callback_message, state, error)
                })
                .await
                .unwrap_or_else(|_| {
                    (
                        backup_message,
                        LocalTransactionState::Unknown,
                        Some("transaction callback panicked".into()),
                    )
                });
                *message = outcome.0;
                (outcome.1, outcome.2)
            }
            SendStatus::FlushDiskTimeout
            | SendStatus::FlushSlaveTimeout
            | SendStatus::SlaveNotAvailable => (LocalTransactionState::Rollback, None),
        };

        let _ = self
            .send_initial_end_transaction(
                message,
                &send_result,
                local_state,
                callback_error.as_deref(),
            )
            .await;

        Ok(TransactionSendResult {
            send_result,
            local_transaction_state: local_state,
        })
    }

    pub fn transaction_listener(&self) -> Option<Arc<dyn TransactionListener>> {
        self.transaction.transaction_listener()
    }

    pub fn set_transaction_listener(&self, listener: Option<Arc<dyn TransactionListener>>) {
        *self
            .transaction
            .listener
            .write()
            .unwrap_or_else(PoisonError::into_inner) = listener;
    }

    #[allow(deprecated)]
    pub fn transaction_check_listener(&self) -> Option<Arc<dyn TransactionCheckListener>> {
        self.transaction
            .legacy_listener
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    #[allow(deprecated)]
    #[deprecated(note = "use TransactionListener::check_local_transaction")]
    pub fn set_transaction_check_listener(
        &self,
        listener: Option<Arc<dyn TransactionCheckListener>>,
    ) {
        *self
            .transaction
            .legacy_listener
            .write()
            .unwrap_or_else(PoisonError::into_inner) = listener;
    }

    pub fn check_thread_pool_min_size(&self) -> i32 {
        self.transaction.min_size.load(Ordering::Relaxed)
    }

    #[deprecated(note = "Java deprecated this setting; use a custom transaction executor")]
    pub fn set_check_thread_pool_min_size(&self, value: i32) {
        self.transaction.min_size.store(value, Ordering::Relaxed);
    }

    pub fn check_thread_pool_max_size(&self) -> i32 {
        self.transaction.max_size.load(Ordering::Relaxed)
    }

    #[deprecated(note = "Java deprecated this setting; use a custom transaction executor")]
    pub fn set_check_thread_pool_max_size(&self, value: i32) {
        self.transaction.max_size.store(value, Ordering::Relaxed);
    }

    pub fn check_request_hold_max(&self) -> i32 {
        self.transaction.hold_max.load(Ordering::Relaxed)
    }

    #[deprecated(note = "Java deprecated this setting; use a custom transaction executor")]
    pub fn set_check_request_hold_max(&self, value: i32) {
        self.transaction.hold_max.store(value, Ordering::Relaxed);
    }

    pub fn executor_service(&self) -> Option<Arc<dyn TransactionExecutor>> {
        self.transaction
            .executor
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn set_executor_service(&self, executor: Option<Arc<dyn TransactionExecutor>>) {
        *self
            .transaction
            .executor
            .write()
            .unwrap_or_else(PoisonError::into_inner) = executor;
    }

    async fn send_initial_end_transaction(
        &self,
        message: &mut Message,
        send_result: &SendResult,
        state: LocalTransactionState,
        callback_error: Option<&str>,
    ) -> Result<(), ProducerError> {
        let offset_id = send_result
            .offset_msg_id
            .as_deref()
            .filter(|offset_id| !offset_id.is_empty())
            .unwrap_or(&send_result.msg_id);
        let commit_log_offset = decode_message_id_offset(offset_id)
            .ok_or_else(|| ProducerError::TransactionMetadata(offset_id.to_owned()))?;
        let runtime = self.producer.started_runtime().await?;
        let cached_address = self
            .producer
            .cached_publish_info(&send_result.topic)
            .and_then(|info| {
                info.endpoints
                    .iter()
                    .find(|(queue, _)| {
                        queue.broker_name() == Some(send_result.broker_name.as_str())
                            && queue.queue_id() == send_result.queue_id
                    })
                    .map(|(_, address)| address.clone())
            });
        let broker_addr = cached_address
            .or(runtime
                .route_snapshot()?
                .publish_address(&send_result.broker_name)
                .map(str::to_owned))
            .ok_or_else(|| ProducerError::NoRoute(message.topic().to_owned()))?;
        let header = {
            let mut __phase18_header_0 = EndTransactionRequestHeader::default();
            __phase18_header_0.set_topic(Some(message.topic().to_owned()));
            __phase18_header_0.set_producer_group(self.producer.producer_group());
            __phase18_header_0.set_tran_state_table_offset(Some(send_result.queue_offset));
            __phase18_header_0.set_commit_log_offset(Some(commit_log_offset));
            __phase18_header_0.set_commit_or_rollback(Some(transaction_flag(state)));
            __phase18_header_0.set_from_transaction_check(Some(false));
            __phase18_header_0.set_msg_id(Some(send_result.msg_id.clone()));
            __phase18_header_0.set_transaction_id(send_result.transaction_id.clone());
            __phase18_header_0.set_broker_name(Some(send_result.broker_name.clone()));
            __phase18_header_0
        };
        let remark =
            callback_error.map(|error| format!("executeLocalTransactionBranch exception: {error}"));
        self.producer.do_execute_end_transaction_hook(
            message,
            header.get_msg_id().unwrap_or_default(),
            &broker_addr,
            state,
            false,
        );
        #[cfg(test)]
        {
            self.producer
                .scripted_end_transactions
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((broker_addr, header, remark));
            if self
                .producer
                .scripted_end_failure
                .swap(false, Ordering::SeqCst)
            {
                return Err(ProducerError::TransactionMetadata(
                    "scripted end transaction transport failure".into(),
                ));
            }
            Ok(())
        }
        #[cfg(not(test))]
        {
            let deadline = Instant::now()
                + Duration::from_millis(self.producer.config().send_timeout_millis().max(0) as u64);
            runtime
                .api()
                .end_transaction_oneway(&broker_addr, &header, remark, deadline)
                .await?;
            Ok(())
        }
    }
}

impl Deref for TransactionProducer {
    type Target = Producer;

    fn deref(&self) -> &Self::Target {
        &self.producer
    }
}

pub(crate) struct TransactionRuntime {
    producer: Weak<Producer>,
    listener: RwLock<Option<Arc<dyn TransactionListener>>>,
    #[allow(deprecated)]
    legacy_listener: RwLock<Option<Arc<dyn TransactionCheckListener>>>,
    min_size: AtomicI32,
    max_size: AtomicI32,
    hold_max: AtomicI32,
    executor: RwLock<Option<Arc<dyn TransactionExecutor>>>,
    pool: Mutex<Option<CheckPool>>,
}

struct CheckPool {
    sender: Option<mpsc::Sender<CheckJob>>,
    receiver: Arc<AsyncMutex<mpsc::Receiver<CheckJob>>>,
    worker_count: usize,
    min_workers: usize,
    max_workers: usize,
    handle: Handle,
    workers: Vec<JoinHandle<()>>,
    completion_tasks: Vec<JoinHandle<()>>,
    custom_executor_permits: Option<Arc<Semaphore>>,
    executor: Option<Arc<dyn TransactionExecutor>>,
}

struct CheckJob {
    remote: String,
    command: RemotingCommand,
}

struct TransactionRequestProcessor {
    transaction: Weak<TransactionRuntime>,
}

impl TransactionRuntime {
    fn new(producer: Weak<Producer>) -> Self {
        Self {
            producer,
            listener: RwLock::new(None),
            legacy_listener: RwLock::new(None),
            min_size: AtomicI32::new(DEFAULT_CHECK_THREAD_POOL_MIN_SIZE),
            max_size: AtomicI32::new(DEFAULT_CHECK_THREAD_POOL_MAX_SIZE),
            hold_max: AtomicI32::new(DEFAULT_CHECK_REQUEST_HOLD_MAX),
            executor: RwLock::new(None),
            pool: Mutex::new(None),
        }
    }

    fn transaction_listener(&self) -> Option<Arc<dyn TransactionListener>> {
        self.listener
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn request_processor(self: &Arc<Self>) -> Arc<dyn RequestProcessor> {
        Arc::new(TransactionRequestProcessor {
            transaction: Arc::downgrade(self),
        })
    }

    fn start_pool(self: &Arc<Self>) -> Result<(), ProducerError> {
        let mut pool_slot = self.pool.lock().unwrap_or_else(PoisonError::into_inner);
        if pool_slot.is_some() {
            return Ok(());
        }
        let executor = self
            .executor
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let min = self.min_size.load(Ordering::Relaxed);
        let max = self.max_size.load(Ordering::Relaxed);
        let hold = self.hold_max.load(Ordering::Relaxed);
        if executor.is_none() && (min < 0 || max <= 0 || max < min || hold <= 0) {
            return Err(ProducerError::InvalidTransactionPoolSettings { min, max, hold });
        }
        let capacity =
            if executor.is_some() {
                CUSTOM_EXECUTOR_QUEUE_CAPACITY
            } else {
                usize::try_from(hold)
                    .map_err(|_| ProducerError::InvalidTransactionPoolSettings { min, max, hold })?
            };
        let handle =
            Handle::try_current().map_err(|_| ProducerError::TransactionCheckQueueUnavailable)?;
        let (sender, receiver) = mpsc::channel(capacity);
        let custom_executor_permits = executor
            .as_ref()
            .map(|_| Arc::new(Semaphore::new(CUSTOM_EXECUTOR_QUEUE_CAPACITY)));
        *pool_slot = Some(CheckPool {
            sender: Some(sender),
            receiver: Arc::new(AsyncMutex::new(receiver)),
            worker_count: 0,
            min_workers: if executor.is_some() {
                1
            } else {
                usize::try_from(min).unwrap_or_default()
            },
            max_workers: if executor.is_some() {
                1
            } else {
                usize::try_from(max).unwrap_or_default()
            },
            handle,
            workers: Vec::new(),
            completion_tasks: Vec::new(),
            custom_executor_permits,
            executor,
        });
        Ok(())
    }

    fn try_submit(self: &Arc<Self>, remote: String, command: RemotingCommand) -> bool {
        let mut pool_slot = self.pool.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(pool) = pool_slot.as_mut() else {
            return false;
        };
        let Some(sender) = pool.sender.as_ref() else {
            return false;
        };
        let job = CheckJob { remote, command };
        match sender.try_send(job) {
            Ok(()) => {
                if pool.worker_count < pool.min_workers || pool.worker_count == 0 {
                    self.spawn_worker(pool, None);
                }
                true
            }
            Err(mpsc::error::TrySendError::Full(job)) if pool.worker_count < pool.max_workers => {
                self.spawn_worker(pool, Some(job));
                true
            }
            Err(_) => false,
        }
    }

    fn spawn_worker(self: &Arc<Self>, pool: &mut CheckPool, first: Option<CheckJob>) {
        pool.workers.retain(|worker| !worker.is_finished());
        pool.worker_count += 1;
        let transaction = self.clone();
        let receiver = pool.receiver.clone();
        let handle = pool.handle.spawn(async move {
            transaction.check_worker(receiver, first).await;
        });
        pool.workers.push(handle);
    }

    fn retire_idle_worker(&self) -> bool {
        let mut pool_slot = self.pool.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(pool) = pool_slot.as_mut() else {
            return true;
        };
        if pool.worker_count > pool.min_workers {
            pool.worker_count -= 1;
            true
        } else {
            false
        }
    }

    fn finish_worker(&self) {
        if let Some(pool) = self
            .pool
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            pool.worker_count = pool.worker_count.saturating_sub(1);
        }
    }

    fn track_completion_task(&self, task: JoinHandle<()>) {
        let mut pool_slot = self.pool.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(pool) = pool_slot.as_mut() else {
            task.abort();
            return;
        };
        pool.completion_tasks
            .retain(|completed| !completed.is_finished());
        pool.completion_tasks.push(task);
    }

    async fn check_worker(
        self: Arc<Self>,
        receiver: Arc<AsyncMutex<mpsc::Receiver<CheckJob>>>,
        mut first: Option<CheckJob>,
    ) {
        loop {
            let job = if let Some(job) = first.take() {
                Some(job)
            } else {
                match tokio::time::timeout(CHECK_WORKER_KEEPALIVE, async {
                    receiver.lock().await.recv().await
                })
                .await
                {
                    Ok(job) => job,
                    Err(_) if self.retire_idle_worker() => return,
                    Err(_) => continue,
                }
            };
            let Some(job) = job else {
                self.finish_worker();
                return;
            };
            self.process_check(job).await;
        }
    }

    async fn process_check(self: &Arc<Self>, job: CheckJob) {
        let Some(producer) = self.producer.upgrade() else {
            return;
        };
        let Some(fields) = job.command.ext_fields.as_ref() else {
            return;
        };
        let Ok(header) = CheckTransactionStateRequestHeader::decode_fields(fields) else {
            return;
        };
        let Some(body) = job.command.body.as_deref() else {
            return;
        };
        let Ok((mut message, _consumed)) = decode_one(body, DecodeOptions::default()) else {
            return;
        };
        let mut client_config = producer.config().client_config().clone();
        if let Ok(Some(topic)) = client_config.without_namespace(Some(message.topic())) {
            message.set_topic(topic);
        }
        if let Some(transaction_id) = message.get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) {
            message.set_transaction_id(Some(transaction_id));
        }

        #[allow(deprecated)]
        let legacy = self
            .legacy_listener
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let current = self.transaction_listener();
        if legacy.is_none() && current.is_none() {
            return;
        }
        let (executor, handle, custom_executor_permits) = {
            let pool_slot = self.pool.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(pool) = pool_slot.as_ref() else {
                return;
            };
            (
                pool.executor.clone(),
                pool.handle.clone(),
                pool.custom_executor_permits.clone(),
            )
        };

        if let Some(executor) = executor {
            let Some(custom_executor_permits) = custom_executor_permits else {
                return;
            };
            let Ok(custom_executor_permit) = custom_executor_permits.acquire_owned().await else {
                return;
            };
            let (reply, receive) = oneshot::channel();
            let callback_message_backup = message.clone();
            let task = Box::new(move || {
                let result = call_check_transaction(legacy, current, message);
                let _ = reply.send(result);
            });
            if executor.execute(task).is_err() {
                let transaction = Arc::clone(self);
                let remote = job.remote;
                let completion = handle.spawn(async move {
                    transaction
                        .send_check_end(
                            &producer,
                            &remote,
                            &header,
                            LocalTransactionState::Unknown,
                            Some(callback_message_backup),
                            Some("transaction executor rejected check"),
                        )
                        .await;
                    drop(custom_executor_permit);
                });
                self.track_completion_task(completion);
                return;
            }
            let transaction = Arc::clone(self);
            let remote = job.remote;
            let completion = handle.spawn(async move {
                match receive.await {
                    Ok(outcome) => {
                        transaction
                            .send_check_end(
                                &producer,
                                &remote,
                                &header,
                                outcome.state,
                                Some(outcome.message),
                                outcome.error.as_deref(),
                            )
                            .await;
                    }
                    Err(_) => {
                        transaction
                            .send_check_end(
                                &producer,
                                &remote,
                                &header,
                                LocalTransactionState::Unknown,
                                Some(callback_message_backup),
                                Some("transaction executor dropped check callback"),
                            )
                            .await;
                    }
                }
                drop(custom_executor_permit);
            });
            self.track_completion_task(completion);
        } else {
            let backup = message.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                call_check_transaction(legacy, current, message)
            })
            .await
            .unwrap_or(CheckCallbackOutcome {
                state: LocalTransactionState::Unknown,
                message: backup,
                error: Some("transaction check callback panicked".into()),
            });
            self.send_check_end(
                &producer,
                &job.remote,
                &header,
                outcome.state,
                Some(outcome.message),
                outcome.error.as_deref(),
            )
            .await;
        }
    }

    async fn send_check_end(
        &self,
        producer: &Arc<Producer>,
        remote: &str,
        check_header: &CheckTransactionStateRequestHeader,
        state: LocalTransactionState,
        message: Option<MessageExt>,
        callback_error: Option<&str>,
    ) {
        if remote.is_empty() {
            return;
        }
        let Some(mut message) = message else {
            return;
        };
        let unique_key = message
            .properties()
            .get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .cloned()
            .unwrap_or_else(|| message.message_id().unwrap_or_default().to_owned());
        let header = {
            let mut __phase18_header_1 = EndTransactionRequestHeader::default();
            __phase18_header_1.set_topic(check_header.get_topic().map(str::to_owned));
            __phase18_header_1.set_producer_group(producer.producer_group());
            __phase18_header_1
                .set_tran_state_table_offset(Some(check_header.get_tran_state_table_offset()));
            __phase18_header_1.set_commit_log_offset(Some(check_header.get_commit_log_offset()));
            __phase18_header_1.set_commit_or_rollback(Some(transaction_flag(state)));
            __phase18_header_1.set_from_transaction_check(Some(true));
            __phase18_header_1.set_msg_id(Some(unique_key));
            __phase18_header_1
                .set_transaction_id(check_header.get_transaction_id().map(str::to_owned));
            __phase18_header_1.set_broker_name(check_header.get_broker_name().map(str::to_owned));
            __phase18_header_1
        };
        let remark =
            callback_error.map(|error| format!("checkLocalTransactionState Exception: {error}"));
        producer.do_execute_end_transaction_hook(
            &mut message,
            header.get_msg_id().unwrap_or_default(),
            remote,
            state,
            true,
        );
        #[cfg(test)]
        {
            producer
                .scripted_end_transactions
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((remote.to_owned(), header, remark));
        }
        #[cfg(not(test))]
        {
            let deadline = Instant::now() + CHECK_END_TRANSACTION_TIMEOUT;
            let Ok(runtime) = producer.started_runtime().await else {
                return;
            };
            let _ = runtime
                .api()
                .end_transaction_oneway(remote, &header, remark, deadline)
                .await;
        }
    }

    async fn shutdown_pool(&self, timeout: Duration) {
        let (workers, executor) = {
            let mut pool_slot = self.pool.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(pool) = pool_slot.as_mut() else {
                return;
            };
            pool.sender.take();
            if let Some(permits) = &pool.custom_executor_permits {
                permits.close();
            }
            (std::mem::take(&mut pool.workers), pool.executor.clone())
        };
        let deadline = Instant::now() + timeout;
        let mut workers = workers;
        for worker in &mut workers {
            if tokio::time::timeout_at(deadline, &mut *worker)
                .await
                .is_err()
            {
                worker.abort();
            }
        }
        let mut completion_tasks = {
            let mut pool_slot = self.pool.lock().unwrap_or_else(PoisonError::into_inner);
            pool_slot
                .as_mut()
                .map(|pool| std::mem::take(&mut pool.completion_tasks))
                .unwrap_or_default()
        };
        for task in &mut completion_tasks {
            if tokio::time::timeout_at(deadline, &mut *task).await.is_err() {
                task.abort();
            }
        }
        if let Some(executor) = executor {
            executor.shutdown();
        }
    }
}

impl RequestProcessor for TransactionRequestProcessor {
    fn process(&self, command: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
        self.process_with_context("", command)
    }

    fn process_with_context(
        &self,
        remote: &str,
        command: RemotingCommand,
    ) -> Result<Option<RemotingCommand>, String> {
        if command.code != request::CHECK_TRANSACTION_STATE {
            return Ok(None);
        }
        if let Some(transaction) = self.transaction.upgrade() {
            let _ = transaction.try_submit(remote.to_owned(), command);
        }
        // The Java handler is oneway and returns null. Overload or shutdown
        // drops this check so the Broker can retry it later.
        Ok(None)
    }
}

struct CheckCallbackOutcome {
    state: LocalTransactionState,
    message: MessageExt,
    error: Option<String>,
}

fn call_local_transaction(
    listener: Arc<dyn TransactionListener>,
    message: &mut Message,
    argument: Option<&(dyn Any + Send + Sync)>,
) -> (LocalTransactionState, Option<String>) {
    match catch_unwind(AssertUnwindSafe(|| {
        listener.execute_local_transaction(message, argument)
    })) {
        Ok(Ok(Some(state))) => (state, None),
        Ok(Ok(None)) => (LocalTransactionState::Unknown, None),
        Ok(Err(error)) => (LocalTransactionState::Unknown, Some(error.to_string())),
        Err(payload) => (
            LocalTransactionState::Unknown,
            Some(panic_description(payload.as_ref())),
        ),
    }
}

#[allow(deprecated)]
fn call_check_transaction(
    legacy: Option<Arc<dyn TransactionCheckListener>>,
    current: Option<Arc<dyn TransactionListener>>,
    mut message: MessageExt,
) -> CheckCallbackOutcome {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if let Some(listener) = legacy.as_ref() {
            listener.check_local_transaction_state(&mut message)
        } else if let Some(listener) = current.as_ref() {
            listener.check_local_transaction(&mut message)
        } else {
            Ok(None)
        }
    }));
    let (state, error) = match result {
        Ok(Ok(Some(state))) => (state, None),
        Ok(Ok(None)) => (LocalTransactionState::Unknown, None),
        Ok(Err(error)) => (LocalTransactionState::Unknown, Some(error.to_string())),
        Err(payload) => (
            LocalTransactionState::Unknown,
            Some(panic_description(payload.as_ref())),
        ),
    };
    CheckCallbackOutcome {
        state,
        message,
        error,
    }
}

fn panic_description(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|text| (*text).to_owned())
        })
        .unwrap_or_else(|| "transaction callback panicked".into())
}

fn delayed_transaction_property(message: &Message) -> bool {
    [
        PROPERTY_DELAY_TIME_LEVEL,
        PROPERTY_TIMER_DELAY_MS,
        PROPERTY_TIMER_DELAY_SEC,
        PROPERTY_TIMER_DELIVER_MS,
    ]
    .into_iter()
    .any(|property| message.properties().contains_key(property))
}

fn decode_message_id_offset(message_id: &str) -> Option<i64> {
    if message_id.len() < 16
        || message_id.len() % 2 != 0
        || !message_id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    let offset = u64::from_str_radix(&message_id[message_id.len() - 16..], 16).ok()?;
    Some(offset as i64)
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use crate::{EndTransactionContext, EndTransactionHook};
    use base64::Engine as _;
    use rocketmq_classic_common::message::{MessageQueue, decode_properties};
    use rocketmq_classic_core::{route::TopicSnapshot, runtime::ClientRuntimeManager};
    use rocketmq_classic_protocol::header::HeaderMap;
    use rocketmq_classic_remoting::client::RpcHook;
    use std::{
        collections::BTreeMap,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        time::Duration,
    };

    const OFFSET_MSG_ID: &str = "7F00000100002A9F0000000000003039";
    const MESSAGE_ID: &str = "MSG-001";

    struct TestListener {
        local_state: Option<LocalTransactionState>,
        check_state: Option<LocalTransactionState>,
        local_calls: AtomicUsize,
        check_calls: AtomicUsize,
        local_error: bool,
        local_panic: bool,
        check_error: bool,
        check_panic: bool,
        check_message: Mutex<Option<(String, Option<String>)>>,
    }

    impl TestListener {
        fn new(local_state: Option<LocalTransactionState>) -> Self {
            Self {
                local_state,
                check_state: Some(LocalTransactionState::Commit),
                local_calls: AtomicUsize::new(0),
                check_calls: AtomicUsize::new(0),
                local_error: false,
                local_panic: false,
                check_error: false,
                check_panic: false,
                check_message: Mutex::new(None),
            }
        }
    }

    #[derive(Clone, Default)]
    struct EndHookProbe {
        events: Arc<Mutex<Vec<String>>>,
        contexts: Arc<Mutex<Vec<serde_json::Value>>>,
    }

    impl EndHookProbe {
        fn install(&self, producer: &TransactionProducer) {
            producer.register_end_transaction_hook(Arc::new(FirstEndHook(self.clone())));
            producer.register_end_transaction_hook(Arc::new(PanickingEndHook(self.clone())));
            producer.register_end_transaction_hook(Arc::new(LastEndHook(self.clone())));
        }
    }

    fn end_hook_phase(context: &EndTransactionContext<'_>) -> &'static str {
        if context.from_transaction_check {
            "check"
        } else {
            "local"
        }
    }

    fn end_hook_state(context: &EndTransactionContext<'_>) -> &'static str {
        match context.transaction_state {
            LocalTransactionState::Commit => "COMMIT_MESSAGE",
            LocalTransactionState::Rollback => "ROLLBACK_MESSAGE",
            LocalTransactionState::Unknown => "UNKNOW",
        }
    }

    struct FirstEndHook(EndHookProbe);

    impl EndTransactionHook for FirstEndHook {
        fn hook_name(&self) -> &str {
            "fixture-first"
        }

        fn end_transaction(&self, context: &mut EndTransactionContext<'_>) {
            let phase = end_hook_phase(context);
            self.0
                .events
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(format!("first:{phase}"));
            let _ = context
                .message
                .set_system_property("END_HOOK_MARKER", phase);
        }
    }

    struct PanickingEndHook(EndHookProbe);

    impl EndTransactionHook for PanickingEndHook {
        fn hook_name(&self) -> &str {
            "fixture-throws"
        }

        fn end_transaction(&self, context: &mut EndTransactionContext<'_>) {
            let phase = end_hook_phase(context);
            self.0
                .events
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(format!("throws:{phase}"));
            panic!("fixture hook failure");
        }
    }

    struct LastEndHook(EndHookProbe);

    impl EndTransactionHook for LastEndHook {
        fn hook_name(&self) -> &str {
            "fixture-last"
        }

        fn end_transaction(&self, context: &mut EndTransactionContext<'_>) {
            let phase = end_hook_phase(context);
            self.0
                .events
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(format!("last:{phase}"));
            let mut observed = serde_json::json!({
                "producerGroup": context.producer_group.as_deref(),
                "topic": context.message.topic(),
                "brokerAddr": context.broker_addr.as_str(),
                "msgId": context.message_id.as_str(),
                "transactionIdPresent": context.transaction_id.is_some(),
                "transactionState": end_hook_state(context),
                "fromTransactionCheck": context.from_transaction_check,
                "messageMutation": context.message.properties().get("END_HOOK_MARKER"),
            });
            if let Some(transaction_id) = &context.transaction_id {
                observed["transactionId"] = serde_json::json!(transaction_id);
            }
            self.0
                .contexts
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(observed);
        }
    }

    impl TransactionListener for TestListener {
        fn execute_local_transaction(
            &self,
            message: &mut Message,
            _argument: Option<&(dyn Any + Send + Sync)>,
        ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
            self.local_calls.fetch_add(1, Ordering::SeqCst);
            if self.local_panic {
                panic!("local callback panic");
            }
            if self.local_error {
                return Err(TransactionCallbackError::new("local callback error"));
            }
            assert_eq!(
                message
                    .properties()
                    .get(PROPERTY_TRANSACTION_PREPARED)
                    .map(String::as_str),
                Some("true")
            );
            assert_eq!(
                message
                    .properties()
                    .get(PROPERTY_PRODUCER_GROUP)
                    .map(String::as_str),
                Some("transaction-producer")
            );
            let unique_id = message
                .get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                .expect("Java half-send retains the generated or caller-provided ID");
            assert!(!unique_id.is_empty());
            assert_eq!(message.transaction_id(), Some(unique_id.as_str()));
            Ok(self.local_state)
        }

        fn check_local_transaction(
            &self,
            message: &mut MessageExt,
        ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
            self.check_calls.fetch_add(1, Ordering::SeqCst);
            *self
                .check_message
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some((
                message.topic().to_owned(),
                message.transaction_id().map(str::to_owned),
            ));
            if self.check_panic {
                panic!("check callback panic");
            }
            if self.check_error {
                return Err(TransactionCallbackError::new("check callback error"));
            }
            Ok(self.check_state)
        }
    }

    #[allow(deprecated)]
    struct LegacyRollbackListener {
        calls: AtomicUsize,
        check_message: Mutex<Option<(String, Option<String>)>>,
    }

    #[allow(deprecated)]
    impl TransactionCheckListener for LegacyRollbackListener {
        fn check_local_transaction_state(
            &self,
            message: &mut MessageExt,
        ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self
                .check_message
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some((
                message.topic().to_owned(),
                message.transaction_id().map(str::to_owned),
            ));
            Ok(Some(LocalTransactionState::Rollback))
        }
    }

    struct InlineExecutor {
        submitted: AtomicUsize,
        shutdown: AtomicBool,
    }

    impl TransactionExecutor for InlineExecutor {
        fn execute(&self, task: Box<dyn FnOnce() + Send + 'static>) -> Result<(), String> {
            self.submitted.fetch_add(1, Ordering::SeqCst);
            task();
            Ok(())
        }

        fn shutdown(&self) {
            self.shutdown.store(true, Ordering::SeqCst);
        }
    }

    struct RejectingExecutor {
        shutdown: AtomicBool,
    }

    impl TransactionExecutor for RejectingExecutor {
        fn execute(&self, _task: Box<dyn FnOnce() + Send + 'static>) -> Result<(), String> {
            Err("executor is full".into())
        }

        fn shutdown(&self) {
            self.shutdown.store(true, Ordering::SeqCst);
        }
    }

    fn transaction_producer(config: ProducerConfig) -> TransactionProducer {
        let manager = ClientRuntimeManager::new();
        let producer = Arc::new(Producer::with_runtime_manager(config, manager));
        let transaction = Arc::new(TransactionRuntime::new(Arc::downgrade(&producer)));
        let _ = producer.transaction.set(transaction.clone());
        TransactionProducer {
            producer,
            transaction,
        }
    }

    async fn started_producer(
        group: &str,
        topic: &str,
        listener: Option<Arc<dyn TransactionListener>>,
    ) -> TransactionProducer {
        let config = ProducerConfig::new(group).unwrap();
        let producer = transaction_producer(config);
        producer.set_transaction_listener(listener);
        producer.start().await.unwrap();
        let queue = MessageQueue::new(topic, "broker-a", 0);
        let route = Arc::new(
            serde_json::from_str::<rocketmq_classic_protocol::route::TopicRouteData>("{}").unwrap(),
        );
        producer.producer.cache_publish_info(
            topic,
            Arc::new(TopicSnapshot {
                route,
                publish_queues: vec![queue.clone()],
                subscribe_queues: Default::default(),
                endpoints: BTreeMap::from([(queue, "10.0.0.1:10911".into())]),
                order_topic: false,
            }),
        );
        producer
    }

    fn scripted_result(status: SendStatus, topic: &str) -> SendResult {
        SendResult {
            status,
            msg_id: MESSAGE_ID.into(),
            offset_msg_id: Some(OFFSET_MSG_ID.into()),
            topic: topic.into(),
            broker_name: "broker-a".into(),
            queue_id: 0,
            queue_offset: 271,
            transaction_id: Some("TX-001".into()),
            recall_handle: None,
            raw_resp_body: None,
            region_id: "DefaultRegion".into(),
            trace_on: true,
        }
    }

    fn fixture_fields(value: &serde_json::Value) -> HeaderMap {
        value
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
            .collect()
    }

    fn transaction_fixture() -> serde_json::Value {
        serde_json::from_str(include_str!("../../../fixtures/producer/transaction.json")).unwrap()
    }

    fn check_command() -> RemotingCommand {
        let transaction = transaction_fixture();
        let fields = fixture_fields(&transaction["transactionFlow"]["brokerCheckRequest"]);
        let body = transaction["transactionFlow"]["brokerCheckMessage"]["body"]
            .as_str()
            .unwrap();
        RemotingCommand {
            code: request::CHECK_TRANSACTION_STATE,
            ext_fields: Some(fields.into()),
            body: Some(
                base64::engine::general_purpose::STANDARD
                    .decode(body)
                    .unwrap(),
            ),
            ..Default::default()
        }
    }

    fn remove_unique_key(record: &mut Vec<u8>) {
        let properties_length_offset = (4..record.len() - 2)
            .find(|offset| {
                let length =
                    u16::from_be_bytes(record[*offset..*offset + 2].try_into().unwrap()) as usize;
                offset + 2 + length == record.len()
            })
            .unwrap();
        let properties_start = properties_length_offset + 2;
        let properties_length = record.len() - properties_start;
        let needle = b"UNIQ_KEY\x01id\x02";
        let relative_offset = record[properties_start..]
            .windows(needle.len())
            .position(|window| window == needle)
            .unwrap();
        let start = properties_start + relative_offset;
        record.drain(start..start + needle.len());
        let new_properties_length = (properties_length - needle.len()) as u16;
        record[properties_length_offset..properties_length_offset + 2]
            .copy_from_slice(&new_properties_length.to_be_bytes());
        let record_length = record.len() as i32;
        record[..4].copy_from_slice(&record_length.to_be_bytes());
    }

    fn end_transactions(
        producer: &TransactionProducer,
    ) -> Vec<(String, EndTransactionRequestHeader, Option<String>)> {
        producer
            .producer
            .scripted_end_transactions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    async fn wait_for_end_transactions(producer: &TransactionProducer, target: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if end_transactions(producer).len() >= target {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn java_rpc_hook_constructor_path_registers_hook_on_client_runtime() {
        let mut config = ProducerConfig::new("transaction-rpc-hook-test").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config
            .client_config_mut()
            .set_instance_name(Some("transaction-rpc-hook-test".into()));
        let hook: Arc<dyn RpcHook> = Arc::new(|_: &str, _: &RemotingCommand| {});
        let producer = TransactionProducer::new_with_rpc_hook(config, Some(hook.clone()));

        producer.start().await.unwrap();
        let runtime = producer.producer.started_runtime().await.unwrap();
        assert!(
            runtime
                .remoting()
                .get_rpc_hooks()
                .iter()
                .any(|registered| Arc::ptr_eq(registered, &hook))
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn p14_v01_half_message_statuses_and_initial_end_fields_match_java_fixture() {
        let cases = [
            (
                SendStatus::SendOk,
                LocalTransactionState::Commit,
                "commit",
                true,
            ),
            (
                SendStatus::SendOk,
                LocalTransactionState::Rollback,
                "rollback",
                true,
            ),
            (
                SendStatus::SendOk,
                LocalTransactionState::Unknown,
                "unknown",
                true,
            ),
            (
                SendStatus::FlushDiskTimeout,
                LocalTransactionState::Rollback,
                "rollback",
                false,
            ),
            (
                SendStatus::FlushSlaveTimeout,
                LocalTransactionState::Rollback,
                "rollback",
                false,
            ),
            (
                SendStatus::SlaveNotAvailable,
                LocalTransactionState::Rollback,
                "rollback",
                false,
            ),
        ];
        let fixture = transaction_fixture();
        for (status, callback_state, fixture_state, callback_expected) in cases {
            let listener = Arc::new(TestListener::new(Some(callback_state)));
            let producer =
                started_producer("transaction-producer", "orders", Some(listener.clone())).await;
            producer
                .producer
                .scripted_transport
                .lock()
                .unwrap()
                .responses
                .push_back(Ok(Some(scripted_result(status, "orders"))));
            let mut message = Message::new("orders", b"half-message".to_vec());
            let result = producer
                .send_message_in_transaction(&mut message, None)
                .await
                .unwrap();

            let expected_state = if status == SendStatus::SendOk {
                callback_state
            } else {
                LocalTransactionState::Rollback
            };
            assert_eq!(result.local_transaction_state, expected_state);
            assert_eq!(
                listener.local_calls.load(Ordering::SeqCst),
                usize::from(callback_expected)
            );
            assert_eq!(
                message
                    .properties()
                    .get(PROPERTY_TRANSACTION_PREPARED)
                    .map(String::as_str),
                Some("true")
            );
            assert_eq!(
                message
                    .properties()
                    .get(PROPERTY_PRODUCER_GROUP)
                    .map(String::as_str),
                Some("transaction-producer")
            );
            let unique_id = message
                .get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                .expect("Java half-send retains the generated or caller-provided ID");
            assert!(matches!(unique_id.len(), 32 | 56));
            assert_eq!(message.transaction_id(), Some(unique_id.as_str()));

            {
                let attempts = producer.producer.scripted_transport.lock().unwrap();
                let attempt = attempts.attempts.last().unwrap();
                assert_eq!(
                    attempt.sys_flag & message_sys_flag::TRANSACTION_PREPARED_TYPE,
                    message_sys_flag::TRANSACTION_PREPARED_TYPE
                );
                let properties = decode_properties(attempt.properties.as_deref().unwrap());
                assert_eq!(
                    properties
                        .get(PROPERTY_TRANSACTION_PREPARED)
                        .map(String::as_str),
                    Some("true")
                );
                assert_eq!(
                    properties.get(PROPERTY_PRODUCER_GROUP).map(String::as_str),
                    Some("transaction-producer")
                );
            }

            let recorded = end_transactions(&producer);
            assert_eq!(recorded.len(), 1);
            assert_eq!(recorded[0].0, "10.0.0.1:10911");
            let expected =
                fixture_fields(&fixture["transactionFlow"]["initialEndTransaction"][fixture_state]);
            let mut actual = HeaderMap::new();
            recorded[0].1.encode_fields(&mut actual);
            assert_eq!(actual, expected);
            producer.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn p18_end_transaction_hooks_match_java_context_order_and_exception_isolation() {
        let mut listener = TestListener::new(Some(LocalTransactionState::Commit));
        listener.check_state = Some(LocalTransactionState::Rollback);
        let producer =
            started_producer("transaction-producer", "orders", Some(Arc::new(listener))).await;
        let probe = EndHookProbe::default();
        assert!(!producer.has_end_transaction_hook());
        probe.install(&producer);
        assert!(producer.has_end_transaction_hook());

        producer
            .producer
            .scripted_transport
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .responses
            .push_back(Ok(Some(scripted_result(SendStatus::SendOk, "orders"))));
        let mut message = Message::new("orders", b"half-message".to_vec());
        message
            .set_system_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, MESSAGE_ID)
            .unwrap();
        producer
            .send_message_in_transaction(&mut message, None)
            .await
            .unwrap();

        producer
            .transaction
            .request_processor()
            .process_with_context("127.0.0.1:10911", check_command())
            .unwrap();
        wait_for_end_transactions(&producer, 2).await;

        let mut manual_message = Message::new("orders", b"manual-message".to_vec());
        let mut manual_context = EndTransactionContext {
            producer_group: Some("direct-producer".into()),
            message: &mut manual_message,
            broker_addr: "192.0.2.1:10911".into(),
            message_id: "manual-msg".into(),
            transaction_id: Some("TX-MANUAL".into()),
            transaction_state: LocalTransactionState::Unknown,
            from_transaction_check: true,
        };
        producer.execute_end_transaction_hook(&mut manual_context);

        let expected = transaction_fixture()["transactionFlow"]["endTransactionHooks"].clone();
        let events = probe
            .events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let contexts = probe
            .contexts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        assert_eq!(serde_json::json!(events), expected["events"]);
        assert_eq!(serde_json::json!(contexts), expected["contexts"]);
        assert_eq!(
            message
                .properties()
                .get("END_HOOK_MARKER")
                .map(String::as_str),
            Some("local")
        );
        assert_eq!(
            manual_message
                .properties()
                .get("END_HOOK_MARKER")
                .map(String::as_str),
            expected["finalMessageMarker"].as_str()
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn p14_v01_callback_error_and_panic_resolve_unknown_and_end_failure_keeps_result() {
        for (error, panic) in [(true, false), (false, true)] {
            let mut listener = TestListener::new(Some(LocalTransactionState::Commit));
            listener.local_error = error;
            listener.local_panic = panic;
            let listener = Arc::new(listener);
            let producer =
                started_producer("transaction-producer", "orders", Some(listener.clone())).await;
            producer
                .producer
                .scripted_transport
                .lock()
                .unwrap()
                .responses
                .push_back(Ok(Some(scripted_result(SendStatus::SendOk, "orders"))));
            producer
                .producer
                .scripted_end_failure
                .store(true, Ordering::SeqCst);
            let mut message = Message::new("orders", b"half-message".to_vec());
            let result = producer
                .send_message_in_transaction(&mut message, None)
                .await
                .unwrap();
            assert_eq!(
                result.local_transaction_state,
                LocalTransactionState::Unknown
            );
            let (_, header, remark) = end_transactions(&producer).pop().unwrap();
            assert_eq!(
                header.get_commit_or_rollback(),
                Some(message_sys_flag::TRANSACTION_NOT_TYPE)
            );
            assert_eq!(header.get_msg_id(), Some(MESSAGE_ID));
            assert!(
                remark
                    .unwrap()
                    .starts_with("executeLocalTransactionBranch exception:")
            );
            producer.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn p14_v01_failed_half_send_mutates_prepared_message_but_never_calls_listener_or_end() {
        let listener = Arc::new(TestListener::new(Some(LocalTransactionState::Commit)));
        let producer =
            started_producer("transaction-producer", "orders", Some(listener.clone())).await;
        producer
            .producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Err(ProducerError::Timeout));
        let mut message = Message::new("orders", b"half-message".to_vec());
        assert!(matches!(
            producer
                .send_message_in_transaction(&mut message, None)
                .await,
            Err(ProducerError::TransactionSend(_))
        ));
        assert_eq!(listener.local_calls.load(Ordering::SeqCst), 0);
        assert!(end_transactions(&producer).is_empty());
        assert_eq!(
            message
                .properties()
                .get(PROPERTY_TRANSACTION_PREPARED)
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            message
                .properties()
                .get(PROPERTY_PRODUCER_GROUP)
                .map(String::as_str),
            Some("transaction-producer")
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn p14_v01_commit_log_offset_uses_offset_message_id_then_falls_back() {
        for (offset_msg_id, result_msg_id, expected_offset) in [
            (OFFSET_MSG_ID, MESSAGE_ID, 12345),
            ("", "7F00000100002A9F000000000000303A", 12346),
        ] {
            let listener = Arc::new(TestListener::new(Some(LocalTransactionState::Commit)));
            let producer = started_producer("transaction-producer", "orders", Some(listener)).await;
            let mut result = scripted_result(SendStatus::SendOk, "orders");
            result.msg_id = result_msg_id.into();
            result.offset_msg_id = Some(offset_msg_id.into());
            producer
                .producer
                .scripted_transport
                .lock()
                .unwrap()
                .responses
                .push_back(Ok(Some(result)));
            let mut message = Message::new("orders", b"body".to_vec());
            let _ = producer
                .send_message_in_transaction(&mut message, None)
                .await
                .unwrap();
            assert_eq!(
                end_transactions(&producer)[0].1.get_commit_log_offset(),
                Some(expected_offset)
            );
            producer.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn p14_v02_broker_checks_route_listeners_repeat_and_preserve_exact_fields() {
        let listener = Arc::new(TestListener::new(Some(LocalTransactionState::Commit)));
        let producer = started_producer("producer-a", "orders", Some(listener.clone())).await;
        #[allow(deprecated)]
        let legacy = Arc::new(LegacyRollbackListener {
            calls: AtomicUsize::new(0),
            check_message: Mutex::new(None),
        });
        let legacy_api: Arc<dyn TransactionCheckListener> = legacy.clone();
        #[allow(deprecated)]
        producer.set_transaction_check_listener(Some(legacy_api.clone()));
        assert!(Arc::ptr_eq(
            &legacy_api,
            &producer.transaction_check_listener().unwrap()
        ));
        let processor = producer.transaction.request_processor();
        for _ in 0..2 {
            assert!(
                processor
                    .process_with_context("127.0.0.1:10911", check_command())
                    .unwrap()
                    .is_none()
            );
        }
        wait_for_end_transactions(&producer, 2).await;

        assert_eq!(listener.local_calls.load(Ordering::SeqCst), 0);
        assert_eq!(listener.check_calls.load(Ordering::SeqCst), 0);
        assert_eq!(legacy.calls.load(Ordering::SeqCst), 2);
        let (topic, transaction_id) = legacy
            .check_message
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_default();
        assert_eq!(topic, "orders");
        assert_eq!(transaction_id.as_deref(), Some("id"));
        let fixture = transaction_fixture();
        let mut expected = fixture_fields(
            &fixture["transactionFlow"]["brokerCheckEndTransaction"]["withUniqueKey"]["rollback"],
        );
        expected.insert("producerGroup".into(), "producer-a".into());
        expected.insert("msgId".into(), "id".into());
        for (_, header, _) in end_transactions(&producer) {
            assert_eq!(
                header.get_commit_or_rollback(),
                Some(message_sys_flag::TRANSACTION_ROLLBACK_TYPE)
            );
            let mut actual = HeaderMap::new();
            header.encode_fields(&mut actual);
            assert_eq!(actual, expected);
        }
        producer.shutdown().await.unwrap();

        for (state, fixture_state) in [
            (LocalTransactionState::Commit, "commit"),
            (LocalTransactionState::Rollback, "rollback"),
            (LocalTransactionState::Unknown, "unknown"),
        ] {
            let mut callback = TestListener::new(None);
            callback.check_state = Some(state);
            let listener = Arc::new(callback);
            let producer = started_producer("producer-a", "orders", Some(listener.clone())).await;
            producer
                .transaction
                .request_processor()
                .process_with_context("127.0.0.1:10911", check_command())
                .unwrap();
            wait_for_end_transactions(&producer, 1).await;
            assert_eq!(listener.check_calls.load(Ordering::SeqCst), 1);
            let mut expected = fixture_fields(
                &fixture["transactionFlow"]["brokerCheckEndTransaction"]["withUniqueKey"]
                    [fixture_state],
            );
            expected.insert("producerGroup".into(), "producer-a".into());
            expected.insert("msgId".into(), "id".into());
            let mut actual = HeaderMap::new();
            end_transactions(&producer)[0].1.encode_fields(&mut actual);
            assert_eq!(actual, expected);
            producer.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn p14_v02_check_error_and_panic_map_to_unknown_and_missing_unique_key_uses_message_id() {
        for (error, panic) in [(true, false), (false, true)] {
            let mut listener = TestListener::new(Some(LocalTransactionState::Commit));
            listener.check_error = error;
            listener.check_panic = panic;
            let listener = Arc::new(listener);
            let producer = started_producer("producer-a", "orders", Some(listener)).await;
            let processor = producer.transaction.request_processor();
            processor
                .process_with_context("127.0.0.1:10911", check_command())
                .unwrap();
            wait_for_end_transactions(&producer, 1).await;
            let (_, header, remark) = end_transactions(&producer).pop().unwrap();
            assert_eq!(
                header.get_commit_or_rollback(),
                Some(message_sys_flag::TRANSACTION_NOT_TYPE)
            );
            let mut expected = fixture_fields(
                &transaction_fixture()["transactionFlow"]["brokerCheckEndTransaction"]["withUniqueKey"]
                    ["unknown"],
            );
            expected.insert("producerGroup".into(), "producer-a".into());
            expected.insert("msgId".into(), "id".into());
            let mut actual = HeaderMap::new();
            header.encode_fields(&mut actual);
            assert_eq!(actual, expected);
            assert!(
                remark
                    .unwrap()
                    .starts_with("checkLocalTransactionState Exception:")
            );
            producer.shutdown().await.unwrap();
        }

        let listener = Arc::new(TestListener::new(Some(LocalTransactionState::Commit)));
        let producer = started_producer("producer-a", "orders", Some(listener)).await;
        let fixture = transaction_fixture();
        let mut command = check_command();
        remove_unique_key(command.body.as_mut().unwrap());
        let expected_message_id =
            decode_one(command.body.as_deref().unwrap(), DecodeOptions::default())
                .unwrap()
                .0
                .message_id()
                .unwrap()
                .to_owned();
        let mut expected = fixture_fields(
            &fixture["transactionFlow"]["brokerCheckEndTransaction"]["withoutUniqueKey"]["commit"],
        );
        expected.insert("producerGroup".into(), "producer-a".into());
        assert_eq!(expected.get("msgId"), Some(&expected_message_id));
        producer
            .transaction
            .request_processor()
            .process_with_context("127.0.0.1:10911", command)
            .unwrap();
        wait_for_end_transactions(&producer, 1).await;
        let mut actual = HeaderMap::new();
        end_transactions(&producer)[0].1.encode_fields(&mut actual);
        assert_eq!(actual, expected);
        producer.shutdown().await.unwrap();
    }

    struct BlockingListener {
        started: std::sync::mpsc::Sender<usize>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
        calls: AtomicUsize,
    }

    impl TransactionListener for BlockingListener {
        fn execute_local_transaction(
            &self,
            _message: &mut Message,
            _argument: Option<&(dyn Any + Send + Sync)>,
        ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
            Ok(Some(LocalTransactionState::Unknown))
        }

        fn check_local_transaction(
            &self,
            _message: &mut MessageExt,
        ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = self.started.send(call);
            let _ = self
                .release
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .recv();
            Ok(Some(LocalTransactionState::Commit))
        }
    }

    struct ConcurrentBlockingListener {
        started: std::sync::mpsc::Sender<usize>,
        calls: AtomicUsize,
        release: (std::sync::Mutex<bool>, std::sync::Condvar),
    }

    impl TransactionListener for ConcurrentBlockingListener {
        fn execute_local_transaction(
            &self,
            _message: &mut Message,
            _argument: Option<&(dyn Any + Send + Sync)>,
        ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
            Ok(Some(LocalTransactionState::Unknown))
        }

        fn check_local_transaction(
            &self,
            _message: &mut MessageExt,
        ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = self.started.send(call);
            let (released, changed) = &self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = changed.wait(released).unwrap();
            }
            Ok(Some(LocalTransactionState::Commit))
        }
    }

    struct ThreadPerTaskExecutor {
        shutdown: AtomicBool,
    }

    impl TransactionExecutor for ThreadPerTaskExecutor {
        fn execute(&self, task: Box<dyn FnOnce() + Send + 'static>) -> Result<(), String> {
            std::thread::Builder::new()
                .spawn(task)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }

        fn shutdown(&self) {
            self.shutdown.store(true, Ordering::SeqCst);
        }
    }

    async fn next_started(receiver: &std::sync::mpsc::Receiver<usize>) -> usize {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match receiver.try_recv() {
                    Ok(value) => return value,
                    Err(std::sync::mpsc::TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        panic!("callback worker closed")
                    }
                }
            }
        })
        .await
        .unwrap()
    }

    async fn wait_started(receiver: &std::sync::mpsc::Receiver<usize>) -> usize {
        loop {
            match receiver.try_recv() {
                Ok(value) => return value,
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    panic!("callback worker closed")
                }
            }
        }
    }

    #[tokio::test]
    async fn p14_v02_reader_returns_promptly_queue_overflow_drops_and_shutdown_drains() {
        let (started_send, started_receive) = std::sync::mpsc::channel();
        let (release_send, release_receive) = std::sync::mpsc::channel();
        let listener = Arc::new(BlockingListener {
            started: started_send,
            release: Mutex::new(release_receive),
            calls: AtomicUsize::new(0),
        });
        let producer = transaction_producer(ProducerConfig::new("producer-a").unwrap());
        producer.set_transaction_listener(Some(listener.clone()));
        producer.set_check_thread_pool_min_size(1);
        producer.set_check_thread_pool_max_size(1);
        producer.set_check_request_hold_max(1);
        producer.start().await.unwrap();
        let processor = producer.transaction.request_processor();

        assert!(
            processor
                .process_with_context("127.0.0.1:10911", check_command())
                .unwrap()
                .is_none()
        );
        assert_eq!(next_started(&started_receive).await, 1);
        assert!(
            processor
                .process_with_context("127.0.0.1:10911", check_command())
                .unwrap()
                .is_none()
        );
        assert!(
            processor
                .process_with_context("127.0.0.1:10911", check_command())
                .unwrap()
                .is_none()
        );
        release_send.send(()).unwrap();
        assert_eq!(next_started(&started_receive).await, 2);

        let shutdown_producer = Arc::new(producer);
        let shutdown = {
            let producer = shutdown_producer.clone();
            tokio::spawn(async move { producer.shutdown().await })
        };
        release_send.send(()).unwrap();
        shutdown.await.unwrap().unwrap();
        assert_eq!(listener.calls.load(Ordering::SeqCst), 2);
        assert_eq!(end_transactions(&shutdown_producer).len(), 2);
    }

    struct BlockingLocalListener {
        started: std::sync::mpsc::Sender<usize>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
        calls: AtomicUsize,
        completed: AtomicUsize,
    }

    impl TransactionListener for BlockingLocalListener {
        fn execute_local_transaction(
            &self,
            _message: &mut Message,
            _argument: Option<&(dyn Any + Send + Sync)>,
        ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = self.started.send(call);
            let _ = self
                .release
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .recv();
            self.completed.fetch_add(1, Ordering::SeqCst);
            Ok(Some(LocalTransactionState::Commit))
        }

        fn check_local_transaction(
            &self,
            _message: &mut MessageExt,
        ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
            Ok(Some(LocalTransactionState::Unknown))
        }
    }

    #[tokio::test]
    async fn p14_v02_cancellation_after_half_send_leaves_broker_check_recovery() {
        let (started_send, started_receive) = std::sync::mpsc::channel();
        let (release_send, release_receive) = std::sync::mpsc::channel();
        let listener = Arc::new(BlockingLocalListener {
            started: started_send,
            release: Mutex::new(release_receive),
            calls: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
        });
        let producer =
            Arc::new(started_producer("producer-a", "orders", Some(listener.clone())).await);
        producer
            .producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result(SendStatus::SendOk, "orders"))));
        let send = {
            let producer = producer.clone();
            tokio::spawn(async move {
                let mut message = Message::new("orders", b"half-message".to_vec());
                producer
                    .send_message_in_transaction(&mut message, None)
                    .await
            })
        };
        assert_eq!(next_started(&started_receive).await, 1);
        send.abort();
        let _ = send.await;
        release_send.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while listener.completed.load(Ordering::SeqCst) != 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(end_transactions(&producer).is_empty());

        producer
            .transaction
            .request_processor()
            .process_with_context("127.0.0.1:10911", check_command())
            .unwrap();
        wait_for_end_transactions(&producer, 1).await;
        assert_eq!(
            end_transactions(&producer)[0].1.get_commit_or_rollback(),
            Some(message_sys_flag::TRANSACTION_NOT_TYPE)
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn p14_v02_custom_executor_runs_check_and_is_shutdown_by_producer() {
        let listener = Arc::new(TestListener::new(Some(LocalTransactionState::Commit)));
        let producer = started_producer("producer-a", "orders", Some(listener.clone())).await;
        producer.set_check_thread_pool_min_size(-3);
        producer.set_check_thread_pool_max_size(0);
        producer.set_check_request_hold_max(0);
        let executor = Arc::new(InlineExecutor {
            submitted: AtomicUsize::new(0),
            shutdown: AtomicBool::new(false),
        });
        let executor_api: Arc<dyn TransactionExecutor> = executor.clone();
        producer.set_executor_service(Some(executor_api.clone()));
        assert!(Arc::ptr_eq(
            &executor_api,
            &producer.executor_service().unwrap()
        ));
        // Executor injection after start does not replace Java's active executor.
        let processor = producer.transaction.request_processor();
        processor
            .process_with_context("127.0.0.1:10911", check_command())
            .unwrap();
        wait_for_end_transactions(&producer, 1).await;
        assert_eq!(listener.check_calls.load(Ordering::SeqCst), 1);
        assert_eq!(executor.submitted.load(Ordering::SeqCst), 0);
        producer.shutdown().await.unwrap();
        assert!(!executor.shutdown.load(Ordering::SeqCst));

        let producer = transaction_producer(ProducerConfig::new("producer-a").unwrap());
        let executor = Arc::new(InlineExecutor {
            submitted: AtomicUsize::new(0),
            shutdown: AtomicBool::new(false),
        });
        producer.set_check_thread_pool_min_size(-3);
        producer.set_check_thread_pool_max_size(0);
        producer.set_check_request_hold_max(0);
        producer.set_transaction_listener(Some(listener.clone()));
        producer.set_executor_service(Some(executor.clone()));
        producer.start().await.unwrap();
        let processor = producer.transaction.request_processor();
        processor
            .process_with_context("127.0.0.1:10911", check_command())
            .unwrap();
        wait_for_end_transactions(&producer, 1).await;
        assert_eq!(executor.submitted.load(Ordering::SeqCst), 1);
        producer.shutdown().await.unwrap();
        assert!(executor.shutdown.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn p14_v02_custom_executor_receives_concurrent_checks_without_dispatch_serialization() {
        let (started_send, started_receive) = std::sync::mpsc::channel();
        let listener = Arc::new(ConcurrentBlockingListener {
            started: started_send,
            calls: AtomicUsize::new(0),
            release: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
        });
        let executor = Arc::new(ThreadPerTaskExecutor {
            shutdown: AtomicBool::new(false),
        });
        let producer =
            transaction_producer(ProducerConfig::new("producer-custom-parallel").unwrap());
        producer.set_transaction_listener(Some(listener.clone()));
        producer.set_executor_service(Some(executor.clone()));
        producer.start().await.unwrap();
        let processor = producer.transaction.request_processor();

        for _ in 0..2 {
            assert!(
                processor
                    .process_with_context("127.0.0.1:10911", check_command())
                    .unwrap()
                    .is_none()
            );
        }

        let first =
            tokio::time::timeout(Duration::from_secs(2), wait_started(&started_receive)).await;
        let second =
            tokio::time::timeout(Duration::from_millis(500), wait_started(&started_receive)).await;
        {
            let (released, changed) = &listener.release;
            *released.lock().unwrap() = true;
            changed.notify_all();
        }
        assert!(
            first.is_ok(),
            "custom executor did not start its first check"
        );
        assert!(
            second.is_ok(),
            "transaction dispatcher serialized checks submitted to the custom executor"
        );
        wait_for_end_transactions(&producer, 2).await;
        assert_eq!(listener.calls.load(Ordering::SeqCst), 2);
        producer.shutdown().await.unwrap();
        assert!(executor.shutdown.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn p14_v02_custom_executor_rejection_resolves_unknown_and_shuts_down() {
        let listener = Arc::new(TestListener::new(Some(LocalTransactionState::Commit)));
        let producer = transaction_producer(ProducerConfig::new("producer-a").unwrap());
        producer.set_transaction_listener(Some(listener.clone()));
        let executor = Arc::new(RejectingExecutor {
            shutdown: AtomicBool::new(false),
        });
        producer.set_executor_service(Some(executor.clone()));
        producer.start().await.unwrap();
        producer
            .transaction
            .request_processor()
            .process_with_context("127.0.0.1:10911", check_command())
            .unwrap();
        wait_for_end_transactions(&producer, 1).await;

        let (_, header, remark) = end_transactions(&producer).pop().unwrap();
        assert_eq!(
            header.get_commit_or_rollback(),
            Some(message_sys_flag::TRANSACTION_NOT_TYPE)
        );
        assert!(
            remark
                .unwrap()
                .starts_with("checkLocalTransactionState Exception:")
        );
        assert_eq!(listener.check_calls.load(Ordering::SeqCst), 0);
        producer.shutdown().await.unwrap();
        assert!(executor.shutdown.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn p14_v02_idle_check_workers_retire_only_above_the_minimum() {
        let producer = transaction_producer(ProducerConfig::new("producer-a").unwrap());
        producer.set_check_thread_pool_min_size(1);
        producer.set_check_thread_pool_max_size(3);
        producer.transaction.start_pool().unwrap();
        producer
            .transaction
            .pool
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .worker_count = 3;

        assert!(producer.transaction.retire_idle_worker());
        assert!(producer.transaction.retire_idle_worker());
        assert!(!producer.transaction.retire_idle_worker());
        assert_eq!(
            producer
                .transaction
                .pool
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .worker_count,
            1
        );
        producer
            .transaction
            .shutdown_pool(Duration::from_millis(1))
            .await;
    }
}
