use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::{Arc, Mutex, RwLockReadGuard},
};

use rocketmq_classic_remoting::client::{
    ClientConfig as TransportConfig, RemotingClient, RpcHook, TlsConfig, TransportMode,
};

use super::{
    ConsumerRuntime, GroupLease, ProducerRuntime, RequestResponseFuture, RuntimeError,
    RuntimeState,
    control::{Command, ControlPlane},
    manager::ManagerLink,
    registry::{GroupKind, GroupRegistry},
};
use crate::{
    api::MQClientApi,
    config::{ClientConfig, SharedClientConfig, read_shared_client_config},
};

fn callback_executor_threads(configured: i32) -> i32 {
    // NettyRemotingClient uses four public executor threads for any nonpositive
    // value; ClientConfig and sharing metadata still retain the signed input.
    if configured <= 0 { 4 } else { configured }
}

fn epoch_millis(time: std::time::SystemTime) -> i64 {
    match time.duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        // Java's epoch millisecond clock floors a fractional negative instant.
        Err(error) => (-(error.duration().as_nanos().div_ceil(1_000_000) as i128)) as i64,
    }
}

#[cfg(test)]
#[test]
fn constructor_clock_uses_signed_epoch_milliseconds() {
    use std::time::{Duration, UNIX_EPOCH};
    assert_eq!(epoch_millis(UNIX_EPOCH), 0);
    assert_eq!(epoch_millis(UNIX_EPOCH + Duration::from_millis(1234)), 1234);
    assert_eq!(
        epoch_millis(UNIX_EPOCH - Duration::from_millis(1234)),
        -1234
    );
    assert_eq!(epoch_millis(UNIX_EPOCH - Duration::from_nanos(1)), -1);
}

#[test]
fn rpc_timeout_reads_the_live_runtime_configuration() {
    let mut config = ClientConfig::new("live-timeout");
    config.namesrv_addr = Some(String::new());
    config.mq_client_api_timeout = 123;
    let runtime = ClientRuntime::construct(config, &HashMap::new(), None).unwrap();
    assert_eq!(
        runtime.rpc_timeout(),
        tokio::time::Duration::from_millis(123)
    );
    runtime.config().write().unwrap().mq_client_api_timeout = -9;
    assert_eq!(runtime.rpc_timeout(), tokio::time::Duration::ZERO);
}

#[test]
fn runtime_extension_is_shared_by_type_and_isolated_between_runtimes() {
    let first = ClientRuntime::construct(
        ClientConfig::new("runtime-extension-first"),
        &HashMap::new(),
        None,
    )
    .unwrap();
    let second = ClientRuntime::construct(
        ClientConfig::new("runtime-extension-second"),
        &HashMap::new(),
        None,
    )
    .unwrap();

    let first_value = first.get_or_insert_extension(Arc::new(String::from("first")));
    let shared_value = first.get_or_insert_extension(Arc::new(String::from("ignored")));
    let second_value = second.get_or_insert_extension(Arc::new(String::from("second")));

    assert!(Arc::ptr_eq(&first_value, &shared_value));
    assert!(!Arc::ptr_eq(&first_value, &second_value));
    assert_eq!(first_value.as_str(), "first");
    assert_eq!(second_value.as_str(), "second");
}

#[cfg(test)]
#[test]
fn nameserver_refresh_reads_the_current_nullable_config_value() {
    let fixtures = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/rpc-invoked-ext-more");
    for name in [
        "nameserver-refresh-present",
        "nameserver-refresh-null",
        "nameserver-refresh-empty",
    ] {
        let fixture: serde_json::Value =
            serde_json::from_slice(&std::fs::read(fixtures.join(format!("{name}.json"))).unwrap())
                .unwrap();
        let mut config = ClientConfig::new("nameserver-refresh");
        config.namesrv_addr = Some("initial:9876".into());
        let runtime = ClientRuntime::construct(config, &HashMap::new(), None).unwrap();
        let configured = fixture["inputNamesrvAddr"].as_str().map(str::to_owned);
        runtime.config().write().unwrap().namesrv_addr = configured;
        assert_eq!(
            runtime.update_name_server_address_list_from_config(),
            fixture["returnValue"].as_bool().unwrap(),
            "{name}"
        );
        let mut actual = runtime.remoting.get_name_server_address_list().unwrap();
        let mut expected = fixture["resultingAddresses"]
            .as_array()
            .unwrap()
            .iter()
            .map(|address| address.as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected, "{name}");
    }
}

/// A manager-owned transport and typed API. Construction does not start tasks.
pub struct ClientRuntime {
    boot_timestamp: i64,
    config: SharedClientConfig,
    client_id: String,
    remoting: RemotingClient,
    api: MQClientApi,
    routes: crate::route::RouteCache,
    namesrv_gate: tokio::sync::Mutex<()>,
    pub(super) monitor_gate: Arc<tokio::sync::Mutex<()>>,
    pub(super) groups: Arc<GroupRegistry>,
    pub(super) control: ControlPlane,
    pub(super) inbound: Arc<super::inbound::RuntimeProcessor>,
    pub(super) rebalance: Arc<super::rebalance::RebalanceSignal>,
    pub(super) task_scheduler_changed: Arc<tokio::sync::Notify>,
    pub(super) task_scheduler_status: Arc<super::tasks::SchedulerStatus>,
    pub(super) pull: Arc<super::pull::PullService>,
    manager: Mutex<Vec<ManagerLink>>,
    extensions: Mutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
    reply_timeout_task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    consumer_stats_manager: Arc<super::ConsumerStatsManager>,
    pub(super) sharing_properties: HashMap<String, String>,
    pub(super) sharing_hook: Option<Arc<dyn RpcHook>>,
    pub(super) sharing_tls_config: Option<TlsConfig>,
    pub(crate) heartbeat: crate::heartbeat::HeartbeatState,
    pub(crate) concurrent_heartbeat_width: Option<usize>,
    heartbeat_listener: Option<Arc<crate::heartbeat::HeartbeatChannelListener>>,
    addressing: Arc<dyn crate::namesrv::TopAddressing>,
    discovery: Arc<crate::namesrv::DiscoveryState>,
    #[cfg(test)]
    pub(super) periodic_probe: Arc<super::tasks::TaskProbe>,
    #[cfg(test)]
    pub(super) rebalance_probe: Arc<super::tasks::TaskProbe>,
    #[cfg(test)]
    pub(super) pull_probe: Arc<super::tasks::TaskProbe>,
}

impl ClientRuntime {
    /// Returns a Rust-native handle to the client instance's owned scheduler.
    ///
    /// Scheduled work shares the client runtime's serial scheduler and is
    /// cancelled when the runtime shuts down.
    pub fn scheduled_executor_service(self: &Arc<Self>) -> super::tasks::RuntimeScheduler {
        super::tasks::RuntimeScheduler::new(
            Arc::downgrade(self),
            super::tasks::RuntimeSchedulerOwner::Client,
        )
    }

    /// Returns a Rust-native handle to the PullMessageService-owned scheduler.
    pub fn pull_scheduled_executor_service(self: &Arc<Self>) -> super::tasks::RuntimeScheduler {
        super::tasks::RuntimeScheduler::new(
            Arc::downgrade(self),
            super::tasks::RuntimeSchedulerOwner::Pull,
        )
    }

    pub(super) fn service_thread_control(
        &self,
        kind: super::service_thread::RuntimeServiceKind,
    ) -> &super::service_thread::ServiceThreadControl {
        match kind {
            super::service_thread::RuntimeServiceKind::Rebalance => &self.rebalance.thread,
            super::service_thread::RuntimeServiceKind::PullMessage => &self.pull.thread,
        }
    }

    pub(super) fn wakeup_service_thread(&self, kind: super::service_thread::RuntimeServiceKind) {
        if kind == super::service_thread::RuntimeServiceKind::Rebalance {
            self.rebalance.wakeup();
        }
    }

    pub(super) async fn start_service_thread(
        self: &Arc<Self>,
        kind: super::service_thread::RuntimeServiceKind,
    ) -> Result<(), RuntimeError> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.control
            .sender(self)?
            .send(Command::StartServiceThread { kind, reply })
            .map_err(|_| RuntimeError::ControlTaskFailed)?;
        receive.await.map_err(|_| RuntimeError::ControlTaskFailed)?
    }

    pub(super) async fn shutdown_service_thread(
        self: &Arc<Self>,
        kind: super::service_thread::RuntimeServiceKind,
        interrupt: bool,
    ) -> Result<(), RuntimeError> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.control
            .sender(self)?
            .send(Command::ShutdownServiceThread {
                kind,
                interrupt,
                reply,
            })
            .map_err(|_| RuntimeError::ControlTaskFailed)?;
        receive.await.map_err(|_| RuntimeError::ControlTaskFailed)?
    }

    pub(super) fn enqueue_scheduled_task(
        self: &Arc<Self>,
        schedule: super::tasks::UserSchedule,
    ) -> Result<(), RuntimeError> {
        if self.task_scheduler_status.is_shutdown() {
            return Err(RuntimeError::InvalidState(RuntimeState::Stopped));
        }
        let state = self.state();
        if !matches!(
            state,
            RuntimeState::Created | RuntimeState::Starting | RuntimeState::Running
        ) {
            return Err(RuntimeError::InvalidState(state));
        }
        self.control
            .sender(self)?
            .send(Command::ScheduleRuntimeTask(schedule))
            .map_err(|_| RuntimeError::ControlTaskFailed)
    }

    pub(super) async fn shutdown_task_scheduler(
        self: &Arc<Self>,
        immediate: bool,
    ) -> Result<Vec<super::tasks::RuntimeScheduledTaskHandle>, RuntimeError> {
        if self.task_scheduler_status.is_terminated() {
            return Ok(Vec::new());
        }
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.control
            .sender(self)?
            .send(Command::ShutdownRuntimeScheduler { immediate, reply })
            .map_err(|_| RuntimeError::ControlTaskFailed)?;
        receive.await.map_err(|_| RuntimeError::ControlTaskFailed)?
    }

    pub(super) fn enqueue_pull_scheduled_task(
        self: &Arc<Self>,
        schedule: super::tasks::UserSchedule,
    ) -> Result<(), RuntimeError> {
        if self.pull.is_scheduler_shutdown() {
            return Err(RuntimeError::InvalidState(RuntimeState::Stopped));
        }
        self.ensure_pull_scheduler()?;
        self.pull.schedule_user_task(schedule)
    }

    pub(super) async fn shutdown_pull_task_scheduler(
        self: &Arc<Self>,
        immediate: bool,
    ) -> Result<Vec<super::tasks::RuntimeScheduledTaskHandle>, RuntimeError> {
        if self.pull.is_scheduler_terminated() {
            return Ok(Vec::new());
        }
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.control
            .sender(self)?
            .send(Command::ShutdownPullTaskScheduler { immediate, reply })
            .map_err(|_| RuntimeError::ControlTaskFailed)?;
        receive.await.map_err(|_| RuntimeError::ControlTaskFailed)?
    }

    /// Returns the shared consumer statistics manager for this client runtime.
    ///
    /// This is the Rust equivalent of
    /// `MQClientInstance.getConsumerStatsManager()`.
    pub fn consumer_stats_manager(&self) -> Arc<super::ConsumerStatsManager> {
        self.consumer_stats_manager.clone()
    }

    /// Stores one typed owner-scoped extension for each Rust type.
    ///
    /// This is used for Java `MQClientInstance` objects that are shared by all
    /// owners of a `ClientRuntime`, including its internal producer.
    #[doc(hidden)]
    pub fn get_or_insert_extension<T>(&self, value: Arc<T>) -> Arc<T>
    where
        T: Any + Send + Sync,
    {
        let mut extensions = self
            .extensions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let stored = extensions
            .entry(TypeId::of::<T>())
            .or_insert_with(|| value.clone());
        Arc::clone(stored)
            .downcast::<T>()
            .unwrap_or_else(|_| unreachable!("runtime extension type ID must match its value"))
    }

    /// Replaces the remoting client's NameServer address list using the same
    /// comma/semicolon/whitespace splitting used during client construction.
    pub fn update_name_server_address_list(&self, addresses: &str) {
        self.remoting
            .update_name_server_address_list(crate::namesrv::split_addresses(addresses));
    }

    /// Reapplies the retained NameServer setting, matching
    /// `MQClientAPIExt.updateNameServerAddressList()`. Returns false and keeps
    /// the prior transport list when the setting is null.
    pub fn update_name_server_address_list_from_config(&self) -> bool {
        let config = self.config_read();
        let Some(addresses) = config.namesrv_addr.as_deref() else {
            return false;
        };
        self.remoting
            .update_name_server_address_list(crate::namesrv::split_addresses(addresses));
        true
    }

    #[cfg(test)]
    pub(super) fn construct(
        config: ClientConfig,
        properties: &HashMap<String, String>,
        hook: Option<Arc<dyn RpcHook>>,
    ) -> Result<Self, RuntimeError> {
        Self::construct_with_top_addressing(config, properties, hook, None)
    }

    #[cfg(test)]
    pub(super) fn construct_with_top_addressing(
        config: ClientConfig,
        properties: &HashMap<String, String>,
        hook: Option<Arc<dyn RpcHook>>,
        provider: Option<Arc<dyn crate::namesrv::TopAddressing>>,
    ) -> Result<Self, RuntimeError> {
        Self::construct_with_top_addressing_and_tls_config(config, properties, hook, provider, None)
    }

    pub(super) fn construct_with_top_addressing_and_tls_config(
        config: ClientConfig,
        properties: &HashMap<String, String>,
        hook: Option<Arc<dyn RpcHook>>,
        provider: Option<Arc<dyn crate::namesrv::TopAddressing>>,
        tls_config: Option<TlsConfig>,
    ) -> Result<Self, RuntimeError> {
        let boot_timestamp = epoch_millis(std::time::SystemTime::now());
        let shared_config = Arc::new(std::sync::RwLock::new(config.clone()));
        let rebalance = Arc::new(super::rebalance::RebalanceSignal::from_properties(
            properties,
        )?);
        if config.enable_concurrent_heartbeat && config.concurrent_heartbeat_thread_pool_size <= 0 {
            return Err(
                rocketmq_classic_remoting::TransportError::InvalidConfiguration(
                    "concurrent heartbeat thread count must be positive".into(),
                )
                .into(),
            );
        }
        let concurrent_heartbeat_width = config
            .enable_concurrent_heartbeat
            .then_some(config.concurrent_heartbeat_thread_pool_size as usize);
        let callback_executor_threads =
            callback_executor_threads(config.client_callback_executor_threads);
        let transport = if let Some(tls_config) = &tls_config {
            TransportMode::Tls(tls_config.clone())
        } else if config.use_tls {
            let mut properties = properties.clone();
            properties.insert("tls.enable".into(), "true".into());
            TransportMode::from_java_properties(&properties, None)?
        } else {
            TransportMode::Plain
        };
        let heartbeat_listener = config
            .enable_heartbeat_channel_event_listener
            .then(|| Arc::new(crate::heartbeat::HeartbeatChannelListener::default()));
        let remoting = RemotingClient::with_config_and_channel_event_listener(
            TransportConfig {
                transport,
                callback_executor_threads,
                // Java Fastjson accepts null and exactly empty Strings as no
                // proxy map, but rejects whitespace-only Strings. Keep the raw
                // core setting and bridge only those two cases to lower "null".
                socks_proxy_config: config
                    .socks_proxy_config
                    .as_deref()
                    .filter(|json| !json.is_empty())
                    .unwrap_or("null")
                    .into(),
                scan_available_nameserver: false,
                ..TransportConfig::default()
            },
            heartbeat_listener.clone().map(|listener| {
                listener as Arc<dyn rocketmq_classic_remoting::client::ChannelEventListener>
            }),
        )?;
        if let Some(addresses) = config.get_namesrv_addr() {
            remoting.update_name_server_address_list(crate::namesrv::split_addresses(&addresses));
        }
        let sharing_hook = hook.clone();
        super::hooks::register(&remoting, shared_config.clone(), hook);
        let mut addressing = crate::namesrv::DefaultTopAddressing::new(
            crate::namesrv::ws_addr(properties),
            config.unit_name.clone(),
        );
        if let Some(provider) = provider {
            addressing = addressing.with_provider(provider);
        }
        let addressing: Arc<dyn crate::namesrv::TopAddressing> = Arc::new(addressing);
        let discovery = crate::namesrv::DiscoveryState::new(remoting.clone());
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            addressing.register_change_callback(discovery.callback());
        }))
        .map_err(|_| RuntimeError::AddressingCallbackFailed)?;
        Ok(Self {
            boot_timestamp,
            client_id: config.build_client_id(),
            config: shared_config.clone(),
            api: MQClientApi::with_config(remoting.clone(), shared_config),
            remoting,
            routes: crate::route::RouteCache::default(),
            namesrv_gate: tokio::sync::Mutex::new(()),
            monitor_gate: Arc::new(tokio::sync::Mutex::new(())),
            groups: Arc::new(GroupRegistry::default()),
            control: ControlPlane::default(),
            inbound: Arc::default(),
            rebalance,
            task_scheduler_changed: Arc::new(tokio::sync::Notify::new()),
            task_scheduler_status: Arc::default(),
            pull: Arc::default(),
            manager: Mutex::new(Vec::new()),
            extensions: Mutex::new(HashMap::new()),
            reply_timeout_task: tokio::sync::Mutex::new(None),
            consumer_stats_manager: Arc::new(super::ConsumerStatsManager::new_for_runtime()),
            sharing_properties: properties.clone(),
            sharing_hook,
            sharing_tls_config: tls_config,
            heartbeat: crate::heartbeat::HeartbeatState::default(),
            concurrent_heartbeat_width,
            heartbeat_listener,
            addressing,
            discovery,
            #[cfg(test)]
            periodic_probe: Arc::default(),
            #[cfg(test)]
            rebalance_probe: Arc::default(),
            #[cfg(test)]
            pull_probe: Arc::default(),
        })
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Constructor wall-clock milliseconds, corresponding to getBootTimestamp.
    /// It is independent of Tokio's monotonic timer clock and never resets.
    pub fn boot_timestamp(&self) -> i64 {
        self.boot_timestamp
    }

    pub fn producer_table(self: &Arc<Self>) -> super::ProducerTable {
        super::ProducerTable::new(self)
    }
    pub fn consumer_table(self: &Arc<Self>) -> super::ConsumerTable {
        super::ConsumerTable::new(self)
    }

    pub(super) async fn mutate_registry_table(
        self: &Arc<Self>,
        kind: super::tables::TableKind,
        mutation: super::tables::TableMutation,
        gate: Option<tokio::sync::OwnedMutexGuard<()>>,
    ) -> Result<super::tables::TableChange, RuntimeError> {
        let _gate = gate;
        let (reply, receive) = tokio::sync::oneshot::channel();
        let Some(sender) = self.control.existing_sender()? else {
            return self.groups.mutate_table(kind, &mutation);
        };
        if let Err(error) = sender.send(Command::MutateTable {
            kind,
            mutation: mutation.clone(),
            reply,
        }) {
            let Command::MutateTable { mutation, .. } = error.0 else {
                unreachable!("failed registry mutation send returns the sent command")
            };
            return self.groups.mutate_table(kind, &mutation);
        }
        match receive.await {
            Ok(result) => result,
            // A terminal actor can close with queued raw map operations. Its
            // unregister/transport work has ended; retained maps still work.
            Err(_) => self.groups.mutate_table(kind, &mutation),
        }
    }

    /// Converts broker offset-table queue keys using the explicit namespace.
    ///
    /// The optional shared table, queue handles, and offsets represent Java's
    /// nullable map, mutable `MessageQueue` keys, and `Long` values. Queue
    /// mutations are visible through the input table and any retained handles.
    #[allow(clippy::mutable_key_type)] // Java returns mutable MessageQueue objects as HashMap keys.
    pub fn parse_offset_table_from_broker(
        &self,
        offset_table: Option<&super::BrokerOffsetTable>,
        namespace: Option<&str>,
    ) -> Result<
        HashMap<
            Option<
                rocketmq_classic_protocol::SharedValue<
                    rocketmq_classic_common::message::MessageQueue,
                >,
            >,
            Option<i64>,
        >,
        RuntimeError,
    > {
        let offset_table =
            offset_table.ok_or(RuntimeError::InvalidOffsetTable("null offset table"))?;
        let mut converted = HashMap::new();
        if let Some(namespace) = namespace.filter(|value| !value.is_empty()) {
            offset_table.with(|table| -> Result<(), RuntimeError> {
                for (queue, offset) in table {
                    let queue = queue.as_ref().ok_or(RuntimeError::InvalidOffsetTable(
                        "null queue key with namespace",
                    ))?;
                    queue.with_mut(|queue| {
                        let topic =
                            crate::config::namespace::unwrap(Some(namespace), queue.topic());
                        queue.set_topic(topic);
                    });
                    converted.insert(Some(queue.clone()), *offset);
                }
                Ok(())
            })?;
        } else {
            offset_table.with(|table| {
                converted.extend(table.iter().map(|(queue, offset)| (queue.clone(), *offset)));
            });
        }
        Ok(converted)
    }

    pub fn execute_pull_request_immediately(
        &self,
        request: Arc<dyn super::MessageRequest>,
    ) -> Result<(), RuntimeError> {
        self.pull.enqueue(request)
    }
    pub fn execute_pop_pull_request_immediately(
        &self,
        request: Arc<dyn super::MessageRequest>,
    ) -> Result<(), RuntimeError> {
        self.pull.enqueue(request)
    }
    pub fn execute_pull_request_later(
        self: &Arc<Self>,
        request: Arc<dyn super::MessageRequest>,
        delay: i64,
    ) -> Result<(), RuntimeError> {
        if self.pull.thread.is_stopped() {
            return Ok(());
        }
        self.ensure_pull_scheduler()?;
        self.pull.request_later(request, delay)
    }
    pub fn execute_pop_pull_request_later(
        self: &Arc<Self>,
        request: Arc<dyn super::MessageRequest>,
        delay: i64,
    ) -> Result<(), RuntimeError> {
        if self.pull.thread.is_stopped() {
            return Ok(());
        }
        self.ensure_pull_scheduler()?;
        self.pull.request_later(request, delay)
    }
    pub fn execute_task(
        self: &Arc<Self>,
        task: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), RuntimeError> {
        if self.pull.thread.is_stopped() {
            return Ok(());
        }
        self.ensure_pull_scheduler()?;
        self.pull.task_later(Box::pin(task), 0)
    }
    pub fn execute_task_later(
        self: &Arc<Self>,
        task: impl std::future::Future<Output = ()> + Send + 'static,
        delay: i64,
    ) -> Result<(), RuntimeError> {
        if self.pull.thread.is_stopped() {
            return Ok(());
        }
        self.ensure_pull_scheduler()?;
        self.pull.task_later(Box::pin(task), delay)
    }

    fn ensure_pull_scheduler(self: &Arc<Self>) -> Result<(), RuntimeError> {
        let state = self.state();
        if !matches!(
            state,
            RuntimeState::Created | RuntimeState::Starting | RuntimeState::Running
        ) {
            return Err(RuntimeError::InvalidState(state));
        }
        self.control
            .sender(self)?
            .send(Command::StartPullScheduler)
            .map_err(|_| RuntimeError::ControlTaskFailed)
    }

    pub fn rebalance_immediately(&self) {
        self.rebalance.wakeup();
    }

    pub fn rebalance_later(&self, delay_millis: i64) -> Result<(), RuntimeError> {
        if delay_millis <= 0 {
            self.rebalance_immediately();
            return Ok(());
        }
        let state = self.state();
        if matches!(
            state,
            RuntimeState::ShuttingDown | RuntimeState::Stopped | RuntimeState::Failed
        ) {
            return Err(RuntimeError::InvalidState(state));
        }
        self.rebalance
            .schedule(tokio::time::Duration::from_millis(delay_millis as u64))
    }

    pub async fn do_rebalance(&self) -> Result<bool, RuntimeError> {
        use std::task::Poll;
        let mut balanced = true;
        for consumer in self.groups.snapshot()?.consumers {
            // Java contains each owner's Throwable independently. Catch both
            // capability construction and asynchronous polling outside locks.
            let operation =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| consumer.try_rebalance()));
            if let Ok(Some(mut operation)) = operation {
                let result = std::future::poll_fn(|context| {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        operation.as_mut().poll(context)
                    }))
                    .unwrap_or(Poll::Ready(true))
                })
                .await;
                balanced &= result;
            }
        }
        Ok(balanced)
    }

    /// Java `adjustThreadPool`: only owners exposing the push adjustment
    /// capability participate; one owner's failure does not end the round.
    pub async fn adjust_thread_pool(&self) -> Result<(), RuntimeError> {
        use std::task::Poll;
        for consumer in self.groups.snapshot()?.consumers {
            let operation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                consumer.adjust_thread_pool()
            }));
            if let Ok(Some(mut operation)) = operation {
                std::future::poll_fn(|context| {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        operation.as_mut().poll(context)
                    }))
                    .unwrap_or(Poll::Ready(()))
                })
                .await;
            }
        }
        Ok(())
    }

    /// Returns a shared mutable handle to this runtime's retained config.
    /// This mirrors Java `MQClientInstance.getClientConfig()`; constructor-only
    /// transport settings remain captured by the already-created remoting client.
    /// Drop read/write guards before calling runtime methods that may read config.
    pub fn config(&self) -> SharedClientConfig {
        self.config.clone()
    }

    pub(crate) fn config_read(&self) -> RwLockReadGuard<'_, ClientConfig> {
        read_shared_client_config(&self.config)
    }

    pub(crate) fn config_snapshot(&self) -> ClientConfig {
        self.config_read().clone()
    }

    pub fn api(&self) -> &MQClientApi {
        &self.api
    }

    pub fn remoting(&self) -> &RemotingClient {
        &self.remoting
    }

    pub async fn fetch_name_server_addr(&self) -> Option<String> {
        use std::{future::Future, task::Poll};
        let mut fetch = Box::pin(async { self.addressing.fetch_ns_addr().await });
        let addresses = std::future::poll_fn(|context| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                fetch.as_mut().poll(context)
            }))
            .unwrap_or(Poll::Ready(None))
        })
        .await;
        self.discovery.fetched(addresses)
    }

    pub fn name_server_addr(&self) -> Option<String> {
        self.discovery.current()
    }

    pub fn route_snapshot(&self) -> Result<Arc<crate::route::RouteSnapshot>, RuntimeError> {
        self.routes.snapshot()
    }

    /// Retained live raw map. Primitive writes do not convert/publish routes.
    pub fn topic_route_table(&self) -> crate::route::TopicRouteTable {
        crate::route::TopicRouteTable::new(self.routes.clone())
    }

    pub fn find_broker_address_by_topic(
        &self,
        topic: &str,
    ) -> Result<Option<String>, RuntimeError> {
        self.route_snapshot()?.broker_address_by_topic(topic)
    }

    /// Null-tolerant Java entry point for `findBrokerAddrByTopic`.
    pub fn find_broker_address_by_topic_nullable(
        &self,
        topic: Option<&str>,
    ) -> Result<Option<String>, RuntimeError> {
        let topic = topic.ok_or(RuntimeError::InvalidRoute("null broker lookup topic"))?;
        self.find_broker_address_by_topic(topic)
    }

    pub fn get_existing_topic_route(
        &self,
        topic: &str,
    ) -> Result<Option<crate::route::TopicRouteDataHandle>, RuntimeError> {
        Ok(self.route_snapshot()?.raw_routes.get(topic).cloned())
    }

    /// Nullable Java entry point for `getAnExistTopicRouteData`; Java's
    /// concurrent route map rejects a null topic key.
    pub fn get_existing_topic_route_nullable(
        &self,
        topic: Option<&str>,
    ) -> Result<Option<crate::route::TopicRouteDataHandle>, RuntimeError> {
        let topic = topic.ok_or(RuntimeError::InvalidRoute("null route lookup topic"))?;
        self.get_existing_topic_route(topic)
    }

    pub async fn query_topic_route(
        &self,
        topic: &str,
    ) -> Result<Option<crate::route::TopicRouteDataHandle>, RuntimeError> {
        if let Some(route) = self.get_existing_topic_route(topic)? {
            return Ok(Some(route));
        }
        self.update_topic_route(topic, tokio::time::Instant::now() + self.rpc_timeout())
            .await?;
        self.get_existing_topic_route(topic)
    }

    /// Nullable Java entry point for `queryTopicRouteData`; the source first
    /// performs the same null-rejecting cached-route lookup.
    pub async fn query_topic_route_nullable(
        &self,
        topic: Option<&str>,
    ) -> Result<Option<crate::route::TopicRouteDataHandle>, RuntimeError> {
        let topic = topic.ok_or(RuntimeError::InvalidRoute("null route lookup topic"))?;
        self.query_topic_route(topic).await
    }

    async fn lookup_broker_with_route_refresh(
        &self,
        topic: &str,
    ) -> Result<Option<String>, RuntimeError> {
        if let Some(address) = self.find_broker_address_by_topic(topic)? {
            return Ok(Some(address));
        }
        self.update_topic_route(topic, tokio::time::Instant::now() + self.rpc_timeout())
            .await?;
        self.find_broker_address_by_topic(topic)
    }

    /// Runtime lookup follows Java's best-effort consumer-list RPC: route
    /// lookup/refresh errors remain errors, but Broker RPC errors return None.
    pub async fn find_consumer_id_list(
        &self,
        topic: &str,
        group: &str,
    ) -> Result<Option<Vec<Option<String>>>, RuntimeError> {
        self.find_consumer_id_list_nullable(Some(topic), Some(group))
            .await
    }

    /// Nullable Java entry point for `findConsumerIdList`; the source resolves
    /// the topic's broker before entering its best-effort Broker RPC branch.
    pub async fn find_consumer_id_list_nullable(
        &self,
        topic: Option<&str>,
        group: Option<&str>,
    ) -> Result<Option<Vec<Option<String>>>, RuntimeError> {
        let topic = topic.ok_or(RuntimeError::InvalidRoute("null broker lookup topic"))?;
        let Some(address) = self.lookup_broker_with_route_refresh(topic).await? else {
            return Ok(None);
        };
        let Ok(address) =
            crate::route::broker_vip_channel(self.config_read().vip_channel_enabled, &address)
        else {
            return Ok(None);
        };
        let timeout_millis = self.config_read().mq_client_api_timeout;
        Ok(self
            .api
            .get_consumer_list_by_group_with_timeout_millis(&address, group, timeout_millis)
            .await
            .unwrap_or(None))
    }

    /// Assignment keeps Java's explicit signed millisecond timeout and
    /// propagates Broker/transport errors, unlike consumer-list lookup.
    pub async fn query_assignment(
        &self,
        topic: &str,
        consumer_group: &str,
        strategy_name: &str,
        message_model: rocketmq_classic_protocol::heartbeat::MessageModel,
        timeout_millis: i32,
    ) -> Result<
        Option<
            std::collections::HashSet<
                Option<rocketmq_classic_protocol::body::MessageQueueAssignment>,
            >,
        >,
        RuntimeError,
    > {
        self.query_assignment_nullable(
            Some(topic),
            Some(consumer_group),
            Some(strategy_name),
            Some(message_model),
            timeout_millis,
        )
        .await
    }

    /// Nullable Java entry point for `queryAssignment`; the source resolves
    /// the topic's broker before constructing or sending the assignment body.
    pub async fn query_assignment_nullable(
        &self,
        topic: Option<&str>,
        consumer_group: Option<&str>,
        strategy_name: Option<&str>,
        message_model: Option<rocketmq_classic_protocol::heartbeat::MessageModel>,
        timeout_millis: i32,
    ) -> Result<
        Option<
            std::collections::HashSet<
                Option<rocketmq_classic_protocol::body::MessageQueueAssignment>,
            >,
        >,
        RuntimeError,
    > {
        let topic = topic.ok_or(RuntimeError::InvalidRoute("null broker lookup topic"))?;
        let Some(address) = self.lookup_broker_with_route_refresh(topic).await? else {
            return Ok(None);
        };
        let address =
            crate::route::broker_vip_channel(self.config_read().vip_channel_enabled, &address)?;
        let mut body = rocketmq_classic_protocol::body::QueryAssignmentRequestBody::default();
        body.set_topic(Some(topic.into()));
        body.set_consumer_group(consumer_group.map(str::to_owned));
        body.set_client_id(Some(self.client_id.clone()));
        body.set_strategy_name(strategy_name.map(str::to_owned));
        body.set_message_model(message_model);
        Ok(self
            .api
            .query_assignment_with_timeout_millis(&address, &body, timeout_millis)
            .await?
            .get_message_queue_assignments()
            .map(|assignments| {
                assignments
                    .snapshot()
                    .into_iter()
                    .map(|assignment| assignment.map(|assignment| assignment.snapshot()))
                    .collect()
            }))
    }

    pub async fn update_topic_route(
        &self,
        topic: &str,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        self.update_route(topic, None, deadline).await
    }

    /// Queries NameServer and converts the writable queues without publishing
    /// the result into the shared runtime route cache.
    pub async fn get_topic_publish_queues(
        &self,
        topic: &str,
        deadline: tokio::time::Instant,
    ) -> Result<Option<Vec<rocketmq_classic_common::message::MessageQueue>>, RuntimeError> {
        let Some(route) = self.api.get_topic_route(topic, deadline).await? else {
            return Ok(None);
        };
        let snapshot = crate::route::TopicSnapshot::from_route_data(topic, &route)?;
        Ok(Some(snapshot.publish_queues))
    }

    /// Queries NameServer for the current subscribe queues without publishing
    /// the route into the shared runtime cache, like Java MQAdminImpl.
    pub async fn get_topic_subscribe_queues(
        &self,
        topic: &str,
        deadline: tokio::time::Instant,
    ) -> Result<
        Option<std::collections::BTreeSet<rocketmq_classic_common::message::MessageQueue>>,
        RuntimeError,
    > {
        let Some(route) = self.api.get_topic_route(topic, deadline).await? else {
            return Ok(None);
        };
        let snapshot = crate::route::TopicSnapshot::from_route_data(topic, &route)?;
        Ok(Some(snapshot.subscribe_queues))
    }

    /// Nullable Java entry point for `updateTopicRouteInfoFromNameServer`.
    /// Java sends the null topic to NameServer first: a missing route returns
    /// false, while a returned route reaches the null-key map lookup and fails.
    pub async fn update_topic_route_nullable(
        &self,
        topic: Option<&str>,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        let Some(topic) = topic else {
            let Ok(_gate) = tokio::time::timeout(
                tokio::time::Duration::from_millis(3000),
                self.namesrv_gate.lock(),
            )
            .await
            else {
                return Ok(false);
            };
            let Some(_route) = (match self.api.get_topic_route_nullable(None, deadline).await {
                Ok(route) => route,
                Err(error) if MQClientApi::route_error_is_client_exception(&error) => {
                    return Ok(false);
                }
                Err(error) => return Err(error.into()),
            }) else {
                return Ok(false);
            };
            return Err(RuntimeError::InvalidRoute("null topic route key"));
        };
        self.update_topic_route(topic, deadline).await
    }

    /// Refresh the union of current consumer subscriptions and producer
    /// publish topics. Each topic gets Java's fresh configured RPC budget;
    /// an individual refresh failure does not prevent the remaining topics.
    pub async fn update_all_topic_routes(&self) -> Result<(), RuntimeError> {
        let groups = self.groups.snapshot()?;
        let mut topics = std::collections::BTreeSet::new();
        for consumer in groups.consumers {
            if let Some(subscriptions) = consumer
                .heartbeat_data()
                .subscription_data_set
                .map(|subscriptions| subscriptions.snapshot())
            {
                for subscription in subscriptions {
                    let Some(subscription) = subscription else {
                        return Err(RuntimeError::InvalidHeartbeatData("null subscription data"));
                    };
                    let subscription = subscription.snapshot();
                    topics.insert(subscription.topic);
                }
            }
        }
        for (_, producer) in groups.producers {
            topics.extend(producer.publish_topics().into_iter().map(Some));
        }
        for topic in topics {
            let deadline = tokio::time::Instant::now() + self.rpc_timeout();
            if let Some(topic) = topic {
                let _ = self.update_topic_route(&topic, deadline).await;
            } else {
                // Java includes null in its HashSet route union. The request
                // header omits the null topic field on the wire.
                match self.api.get_topic_route_nullable(None, deadline).await {
                    Ok(None) => {}
                    Ok(Some(_)) => {
                        return Err(RuntimeError::InvalidRoute("null topic route key"));
                    }
                    Err(error) if MQClientApi::route_error_is_client_exception(&error) => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Ok(())
    }

    /// Runtime branch for producer default-route fallback. The queue setting
    /// stays producer-owned rather than becoming runtime configuration.
    pub async fn update_topic_route_from_default(
        &self,
        topic: &str,
        default_topic_queue_nums: i32,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        self.update_route(topic, Some(default_topic_queue_nums), deadline)
            .await
    }

    async fn update_route(
        &self,
        topic: &str,
        default_topic_queue_nums: Option<i32>,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        let Ok(_gate) = tokio::time::timeout(
            tokio::time::Duration::from_millis(3000),
            self.namesrv_gate.lock(),
        )
        .await
        else {
            return Ok(false);
        };
        let fetched = if default_topic_queue_nums.is_some() {
            self.api.get_default_topic_route(deadline).await
        } else {
            self.api.get_topic_route(topic, deadline).await
        };
        let Some(route) = (match fetched {
            Ok(route) => route,
            // Java catches MQClientException and returns false, but propagates
            // transport failure. Rejection must not discard an existing route.
            Err(error) if MQClientApi::route_error_is_client_exception(&error) => return Ok(false),
            Err(error) => return Err(error.into()),
        }) else {
            return Ok(false);
        };
        if let Some(limit) = default_topic_queue_nums {
            route
                .queue_datas
                .as_ref()
                .ok_or(RuntimeError::InvalidRoute("null queue data list"))?
                .with_mut(|queues| {
                    for data in queues {
                        let data = data
                            .as_ref()
                            .ok_or(RuntimeError::InvalidRoute("null queue data"))?;
                        data.with_mut(|data| {
                            let count = limit.min(data.read_queue_nums);
                            data.read_queue_nums = count;
                            data.write_queue_nums = count;
                        });
                    }
                    Ok::<_, RuntimeError>(())
                })?;
        }
        let groups = self.groups.snapshot()?;
        let changed = self.routes.route_changed(topic, &route)?;
        let needed = !changed
            && (groups
                .producers
                .iter()
                .any(|(_, producer)| producer.is_publish_topic_need_update(topic))
                || groups
                    .consumers
                    .iter()
                    .any(|consumer| consumer.is_subscribe_topic_need_update(topic)));
        let static_route = route.order_topic_conf.is_none()
            && route
                .topic_queue_mapping_by_broker
                .as_ref()
                .is_some_and(|mappings| mappings.with(|mappings| !mappings.is_empty()));
        if (changed || needed)
            && !groups.consumers.is_empty()
            && route.queue_datas.is_none()
            && !static_route
        {
            return Err(RuntimeError::InvalidRoute("null queue data list"));
        }
        let changed = if needed {
            self.routes.replace_for_owner_update(topic, route, true)?
        } else {
            self.routes.replace(topic, route)?
        };
        if changed || needed {
            if let Some(info) = self.routes.snapshot()?.topics.get(topic).cloned() {
                for (_, producer) in groups.producers {
                    producer.update_topic_publish_info(topic, info.clone());
                }
                for consumer in groups.consumers {
                    consumer.update_topic_subscribe_info(topic, info.subscribe_queues.clone());
                }
            }
        }
        Ok(changed || needed)
    }

    async fn clean_offline_brokers(&self) -> Result<(), RuntimeError> {
        let Ok(_gate) = tokio::time::timeout(
            tokio::time::Duration::from_millis(3000),
            self.namesrv_gate.lock(),
        )
        .await
        else {
            return Ok(());
        };
        self.routes.clean_offline()
    }

    pub(crate) fn record_broker_version(
        &self,
        broker: &str,
        address: &str,
        version: i32,
    ) -> Result<(), RuntimeError> {
        self.routes.record_version(broker, address, version)
    }

    pub(super) async fn run_periodic(
        &self,
        kind: super::tasks::TaskKind,
    ) -> Result<(), RuntimeError> {
        use super::tasks::TaskKind;
        match kind {
            TaskKind::Discovery => {
                self.fetch_name_server_addr().await;
            }
            TaskKind::Route => self.update_all_topic_routes().await?,
            TaskKind::Heartbeat => {
                self.clean_offline_brokers().await?;
                self.send_heartbeat_to_all_brokers().await?;
            }
            TaskKind::Persist => {
                for consumer in self.groups.snapshot()?.consumers {
                    if let Some(operation) = consumer.persist_consumer_offset() {
                        operation.await;
                    }
                }
            }
            TaskKind::Adjust => self.adjust_thread_pool().await?,
            TaskKind::StatsSample(set) => self.consumer_stats_manager.sample(set),
            TaskKind::StatsPrint(set, window) => self.consumer_stats_manager.log(set, window),
        }
        Ok(())
    }

    /// Register a producer group and retain its owner until the mapping is
    /// replaced, removed, or cleared. Owners that refer back to this runtime
    /// must hold a `Weak<ClientRuntime>` to avoid an `Arc` cycle.
    pub async fn register_producer(
        self: &Arc<Self>,
        group: &str,
        handle: Arc<dyn ProducerRuntime>,
    ) -> Result<GroupLease, RuntimeError> {
        let monitor = Some(Arc::new(self.monitor_gate.clone().lock_owned().await));
        let gate = self
            .groups
            .table_mutation_gate(super::tables::TableKind::Producer, group)?
            .lock_owned()
            .await;
        if self.state() == RuntimeState::Failed {
            return self.groups.register_producer(group, handle);
        }
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.control
            .sender(self)?
            .send(
                Command::RegisterProducer {
                    group: group.into(),
                    handle,
                    gate,
                    reply,
                }
                .with_monitor(monitor.clone()),
            )
            .map_err(|_| self.control_error())?;
        let lease = receive.await.map_err(|_| self.control_error())??.accept()?;
        if lease.group() != super::control::INNER_PRODUCER {
            self.ensure_reply_timeout_task().await;
        }
        Ok(lease)
    }

    /// Nullable Java-compatible registration boundary. A null group or owner
    /// matches `MQClientInstance.registerProducer` returning false.
    pub async fn register_producer_nullable(
        self: &Arc<Self>,
        group: Option<&str>,
        handle: Option<Arc<dyn ProducerRuntime>>,
    ) -> Result<Option<GroupLease>, RuntimeError> {
        let (Some(group), Some(handle)) = (group, handle) else {
            let _monitor = self.monitor_gate.lock().await;
            return Ok(None);
        };
        self.register_producer(group, handle).await.map(Some)
    }

    /// Register a consumer group and retain its owner until the mapping is
    /// replaced, removed, or cleared. Owners that refer back to this runtime
    /// must hold a `Weak<ClientRuntime>` to avoid an `Arc` cycle.
    pub async fn register_consumer(
        self: &Arc<Self>,
        group: &str,
        handle: Arc<dyn ConsumerRuntime>,
    ) -> Result<GroupLease, RuntimeError> {
        let monitor = Some(Arc::new(self.monitor_gate.clone().lock_owned().await));
        let gate = self
            .groups
            .table_mutation_gate(super::tables::TableKind::Consumer, group)?
            .lock_owned()
            .await;
        if self.state() == RuntimeState::Failed {
            return self.groups.register_consumer(group, handle);
        }
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.control
            .sender(self)?
            .send(
                Command::RegisterConsumer {
                    group: group.into(),
                    handle,
                    gate,
                    reply,
                }
                .with_monitor(monitor.clone()),
            )
            .map_err(|_| self.control_error())?;
        receive.await.map_err(|_| self.control_error())??.accept()
    }

    /// Nullable Java-compatible registration boundary. A null group or owner
    /// matches `MQClientInstance.registerConsumer` returning false.
    pub async fn register_consumer_nullable(
        self: &Arc<Self>,
        group: Option<&str>,
        handle: Option<Arc<dyn ConsumerRuntime>>,
    ) -> Result<Option<GroupLease>, RuntimeError> {
        let (Some(group), Some(handle)) = (group, handle) else {
            let _monitor = self.monitor_gate.lock().await;
            return Ok(None);
        };
        self.register_consumer(group, handle).await.map(Some)
    }

    pub async fn register_admin(self: &Arc<Self>, group: &str) -> Result<GroupLease, RuntimeError> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.control
            .sender(self)?
            .send(Command::RegisterAdmin {
                group: group.into(),
                reply,
            })
            .map_err(|_| self.control_error())?;
        receive.await.map_err(|_| self.control_error())??.accept()
    }

    /// Retain an admin owner by group, matching Java `registerAdminExt`.
    /// Null group or owner inputs are rejected with `false`; duplicate groups
    /// keep their first owner. Use a `Weak<ClientRuntime>` for owner back-
    /// references so the retained admin cannot form an `Arc` cycle. Admin
    /// operations themselves belong to Phase 17.
    pub fn register_admin_ext(
        &self,
        group: Option<&str>,
        owner: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Result<bool, RuntimeError> {
        self.groups.register_admin_ext(group, owner)
    }

    /// Remove the current admin owner by group, matching Java's unconditional
    /// `unregisterAdminExt` removal. `None` maps Java's null-key failure to a
    /// typed invalid-route error.
    pub fn unregister_admin_ext(&self, group: Option<&str>) -> Result<(), RuntimeError> {
        self.groups.unregister_admin_ext(group)
    }

    pub fn select_producer(
        &self,
        group: &str,
    ) -> Result<Option<Arc<dyn ProducerRuntime>>, RuntimeError> {
        self.groups.select_producer(group)
    }

    /// Nullable Java-compatible selector. A null group matches the
    /// `ConcurrentHashMap.get(null)` failure in `MQClientInstance`.
    pub fn select_producer_nullable(
        &self,
        group: Option<&str>,
    ) -> Result<Option<Arc<dyn ProducerRuntime>>, RuntimeError> {
        let Some(group) = group else {
            return Err(RuntimeError::InvalidRoute("null producer group"));
        };
        self.select_producer(group)
    }

    pub fn select_consumer(
        &self,
        group: &str,
    ) -> Result<Option<Arc<dyn ConsumerRuntime>>, RuntimeError> {
        self.groups.select_consumer(group)
    }

    /// Nullable Java-compatible selector. A null group matches the
    /// `ConcurrentHashMap.get(null)` failure in `MQClientInstance`.
    pub fn select_consumer_nullable(
        &self,
        group: Option<&str>,
    ) -> Result<Option<Arc<dyn ConsumerRuntime>>, RuntimeError> {
        let Some(group) = group else {
            return Err(RuntimeError::InvalidRoute("null consumer group"));
        };
        self.select_consumer(group)
    }

    /// Java `MQClientInstance.getConsumerStatus(topic, group)`: snapshot the
    /// selected Pull/Push consumer's offset store without a lifecycle check.
    /// Topic filtering belongs to that store; no namespace conversion occurs.
    /// A null group fails the registry lookup, while an absent/other consumer
    /// returns an empty map.
    pub fn get_consumer_status(
        &self,
        topic: Option<&str>,
        group: Option<&str>,
    ) -> Result<HashMap<rocketmq_classic_common::message::MessageQueue, i64>, RuntimeError> {
        match self.select_consumer_nullable(group)? {
            Some(consumer) => consumer.consumer_status(topic),
            None => Ok(HashMap::new()),
        }
    }

    pub fn prepare_heartbeat_data(
        &self,
        without_sub: bool,
    ) -> Result<rocketmq_classic_protocol::heartbeat::HeartbeatData, RuntimeError> {
        use rocketmq_classic_protocol::{
            SharedValue,
            heartbeat::{HeartbeatData, ProducerData},
        };
        use std::collections::HashSet;
        let snapshot = self.groups.snapshot()?;
        let heartbeat = HeartbeatData {
            client_id: Some(self.client_id.clone()),
            is_without_sub: without_sub,
            ..Default::default()
        };
        for (group, _) in snapshot.producers {
            heartbeat
                .producer_data_set
                .as_ref()
                .unwrap()
                .with_mut(|producers| {
                    producers.insert(Some(SharedValue::from(ProducerData {
                        group_name: Some(group),
                    })));
                });
        }
        for consumer in snapshot.consumers {
            let mut data = consumer.heartbeat_data();
            if without_sub {
                data.subscription_data_set = Some(SharedValue::from(HashSet::new()));
            } else if data.subscription_data_set.is_none() {
                // Java constructs a fresh ConsumerData and addAll(null) fails.
                return Err(RuntimeError::InvalidHeartbeatData("null subscription set"));
            }
            heartbeat
                .consumer_data_set
                .as_ref()
                .unwrap()
                .with_mut(|consumers| {
                    consumers.insert(Some(SharedValue::from(data)));
                });
        }
        Ok(heartbeat)
    }

    pub async fn unregister_producer(
        self: &Arc<Self>,
        lease: &GroupLease,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        self.request_unregister(lease, GroupKind::Producer, deadline)
            .await
    }

    pub async fn unregister_consumer(
        self: &Arc<Self>,
        lease: &GroupLease,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        self.request_unregister(lease, GroupKind::Consumer, deadline)
            .await
    }

    /// Remove the current producer mapping, irrespective of its registration
    /// identity, and attempt Broker unregister even if the mapping was absent.
    /// Returns whether a binding was removed; lifecycle cleanup uses leases.
    pub async fn unregister_producer_by_group(
        self: &Arc<Self>,
        group: &str,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        self.request_unregister_by_group(group, GroupKind::Producer, deadline)
            .await
    }

    /// Nullable Java-compatible group removal. Java's concurrent table
    /// rejects a null key before attempting Broker unregister.
    pub async fn unregister_producer_by_group_nullable(
        self: &Arc<Self>,
        group: Option<&str>,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        let Some(group) = group else {
            let _monitor = self.monitor_gate.lock().await;
            return Err(RuntimeError::InvalidRoute("null producer group"));
        };
        self.unregister_producer_by_group(group, deadline).await
    }

    /// Consumer counterpart of unregister_producer_by_group. This explicit
    /// source capability does not replace identity-safe owner cleanup.
    pub async fn unregister_consumer_by_group(
        self: &Arc<Self>,
        group: &str,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        self.request_unregister_by_group(group, GroupKind::Consumer, deadline)
            .await
    }

    /// Nullable Java-compatible group removal. Java's concurrent table
    /// rejects a null key before attempting Broker unregister.
    pub async fn unregister_consumer_by_group_nullable(
        self: &Arc<Self>,
        group: Option<&str>,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        let Some(group) = group else {
            let _monitor = self.monitor_gate.lock().await;
            return Err(RuntimeError::InvalidRoute("null consumer group"));
        };
        self.unregister_consumer_by_group(group, deadline).await
    }

    pub async fn unregister_admin(
        self: &Arc<Self>,
        lease: &GroupLease,
    ) -> Result<bool, RuntimeError> {
        self.request_unregister(lease, GroupKind::Admin, tokio::time::Instant::now())
            .await
    }

    pub(super) async fn unregister_group(
        &self,
        lease: &GroupLease,
        kind: GroupKind,
        deadline: tokio::time::Instant,
        gate: Option<tokio::sync::OwnedMutexGuard<()>>,
    ) -> Result<bool, RuntimeError> {
        if !self.groups.unregister(lease, kind)? {
            drop(gate);
            return Ok(false);
        }
        drop(gate);
        if kind == GroupKind::Admin {
            return Ok(true);
        }
        self.unregister_brokers(&lease.group, kind, deadline)
            .await?;
        if kind == GroupKind::Producer {
            self.stop_reply_timeout_task_if_idle().await?;
        }
        Ok(true)
    }

    pub(super) async fn unregister_current_group(
        &self,
        group: &str,
        kind: GroupKind,
        deadline: tokio::time::Instant,
        gate: Option<tokio::sync::OwnedMutexGuard<()>>,
    ) -> Result<bool, RuntimeError> {
        let removed = self.groups.remove_by_group(group, kind)?;
        drop(gate);
        self.unregister_brokers(group, kind, deadline).await?;
        if kind == GroupKind::Producer {
            self.stop_reply_timeout_task_if_idle().await?;
        }
        Ok(removed)
    }

    async fn unregister_brokers(
        &self,
        group: &str,
        kind: GroupKind,
        deadline: tokio::time::Instant,
    ) -> Result<(), RuntimeError> {
        let header = {
            let mut __phase18_header_0 =
                rocketmq_classic_protocol::header::UnregisterClientRequestHeader::default();
            __phase18_header_0.set_client_id(self.client_id.clone());
            __phase18_header_0
                .set_producer_group((kind == GroupKind::Producer).then(|| group.to_owned()));
            __phase18_header_0
                .set_consumer_group((kind == GroupKind::Consumer).then(|| group.to_owned()));
            __phase18_header_0
        };
        let snapshot = self.routes.snapshot()?;
        for brokers in snapshot.broker_addresses.values() {
            for address in brokers.values() {
                let Some(address) = address.as_deref() else {
                    continue;
                };
                // Java best effort: one failed peer must not prevent cleanup of
                // other peers. The phase contract bounds all attempts together.
                let _ = self.api.unregister_client(address, &header, deadline).await;
            }
        }
        Ok(())
    }

    pub fn state(&self) -> RuntimeState {
        self.control.state()
    }

    pub async fn register_reply_request(
        &self,
        lease: &GroupLease,
        correlation_id: &str,
        deadline: tokio::time::Instant,
    ) -> Result<super::PendingReplyRequest, RuntimeError> {
        let state = self.state();
        if state != RuntimeState::Running {
            return Err(RuntimeError::InvalidState(state));
        }
        let pending = self
            .groups
            .register_pending_reply(lease, correlation_id, deadline)
            .await?;
        if lease.group() != super::control::INNER_PRODUCER {
            self.ensure_reply_timeout_task().await;
        }
        Ok(pending)
    }

    #[doc(hidden)]
    pub async fn register_reply_request_with_future(
        &self,
        lease: &GroupLease,
        future: Arc<RequestResponseFuture>,
        deadline: tokio::time::Instant,
    ) -> Result<super::PendingReplyRequest, RuntimeError> {
        let state = self.state();
        if state != RuntimeState::Running {
            return Err(RuntimeError::InvalidState(state));
        }
        let pending = self
            .groups
            .register_pending_future(lease, future, deadline)
            .await?;
        Ok(pending)
    }

    pub(super) fn complete_reply_request(
        &self,
        correlation_id: &str,
        reply: rocketmq_classic_common::message::MessageExt,
    ) -> Result<bool, super::RequestFutureError> {
        self.groups
            .try_complete_pending_reply(correlation_id, reply)
    }

    async fn ensure_reply_timeout_task(&self) {
        let mut task = self.reply_timeout_task.lock().await;
        if task.is_some() {
            return;
        }
        let registry = Arc::downgrade(&self.groups);
        *task = Some(tokio::spawn(async move {
            let period = tokio::time::Duration::from_secs(1);
            let first_scan = tokio::time::Instant::now() + tokio::time::Duration::from_secs(3);
            let mut interval = tokio::time::interval_at(first_scan, period);
            loop {
                interval.tick().await;
                let Some(registry) = registry.upgrade() else {
                    break;
                };
                registry.expire_pending_replies(tokio::time::Instant::now());
            }
        }));
    }

    async fn stop_reply_timeout_task(&self) {
        let task = self.reply_timeout_task.lock().await.take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
    }

    async fn stop_reply_timeout_task_if_idle(&self) -> Result<(), RuntimeError> {
        let mut task = self.reply_timeout_task.lock().await;
        let producer_count = self.groups.table_len(super::tables::TableKind::Producer)?;
        let has_internal_producer = self.groups.table_contains_key(
            super::tables::TableKind::Producer,
            super::control::INNER_PRODUCER,
        )?;
        let external_producer_count =
            producer_count.saturating_sub(usize::from(has_internal_producer));
        if external_producer_count != 0 {
            return Ok(());
        }
        if let Some(task) = task.take() {
            task.abort();
            let _ = task.await;
        }
        Ok(())
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), RuntimeError> {
        let monitor = Some(Arc::new(self.monitor_gate.clone().lock_owned().await));
        if self.state() == RuntimeState::Stopped {
            self.control.join_terminal().await?;
            return Ok(());
        }
        if self.state() == RuntimeState::Failed {
            return Err(RuntimeError::InvalidState(self.state()));
        }
        let (reply, receive) = tokio::sync::oneshot::channel();
        if self
            .control
            .sender(self)?
            .send(Command::Start { reply }.with_monitor(monitor.clone()))
            .is_err()
        {
            if self.state() == RuntimeState::Stopped {
                self.control.join_terminal().await?;
                return Ok(());
            }
            return Err(self.control_error());
        }
        let result = match receive.await {
            Ok(result) => result,
            // Java start() is a no-op once shutdown has completed. A Start
            // queued behind Shutdown can observe the actor exit without a
            // reply, so preserve that same terminal-state outcome.
            Err(_) if self.state() == RuntimeState::Stopped => Ok(()),
            Err(_) => Err(self.control_error()),
        };
        self.control.join_terminal().await?;
        result
    }

    pub async fn shutdown(self: &Arc<Self>) -> Result<bool, RuntimeError> {
        // Java checks user tables before entering synchronized(this).
        let monitor = if self
            .groups
            .has_active_users(super::control::INNER_PRODUCER)?
        {
            None
        } else {
            Some(Arc::new(self.monitor_gate.clone().lock_owned().await))
        };
        if matches!(self.state(), RuntimeState::Stopped | RuntimeState::Failed) {
            self.control.join_terminal().await?;
            return Ok(false);
        }
        let (reply, receive) = tokio::sync::oneshot::channel();
        if self
            .control
            .sender(self)?
            .send(Command::Shutdown { reply }.with_monitor(monitor.clone()))
            .is_err()
        {
            if matches!(self.state(), RuntimeState::Stopped | RuntimeState::Failed) {
                self.control.join_terminal().await?;
                return Ok(false);
            }
            return Err(self.control_error());
        }
        let result = match receive.await {
            Ok(result) => result,
            Err(_) if self.state() == RuntimeState::Stopped => Ok(false),
            Err(_) => Err(self.control_error()),
        };
        self.control.join_terminal().await?;
        if result.as_ref().is_ok_and(|stopped| *stopped) {
            self.stop_reply_timeout_task().await;
        }
        result
    }

    async fn request_unregister_by_group(
        self: &Arc<Self>,
        group: &str,
        kind: GroupKind,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        let monitor = if kind == GroupKind::Admin {
            None
        } else {
            Some(Arc::new(self.monitor_gate.clone().lock_owned().await))
        };
        let table_kind = match kind {
            GroupKind::Producer => Some(super::tables::TableKind::Producer),
            GroupKind::Consumer => Some(super::tables::TableKind::Consumer),
            GroupKind::Admin => None,
        };
        let gate = match table_kind {
            Some(table_kind) => Some(
                self.groups
                    .table_mutation_gate(table_kind, group)?
                    .lock_owned()
                    .await,
            ),
            None => None,
        };
        if matches!(self.state(), RuntimeState::Stopped | RuntimeState::Failed) {
            // Retained maps can still be mutated, but terminal transport must
            // not be restarted for best-effort source unregister attempts.
            let _gate = gate;
            return self.groups.remove_by_group(group, kind);
        }
        let (reply, receive) = tokio::sync::oneshot::channel();
        let sender = self.control.sender(self)?;
        if let Err(error) = sender.send(
            Command::UnregisterByGroup {
                group: group.to_owned(),
                kind,
                deadline,
                gate,
                reply,
            }
            .with_monitor(monitor.clone()),
        ) {
            let (command, _monitor) = error.0.split_monitor();
            let Command::UnregisterByGroup { gate, .. } = command else {
                unreachable!("failed unregister send returns the sent command")
            };
            if matches!(self.state(), RuntimeState::Stopped | RuntimeState::Failed) {
                let _gate = gate;
                return self.groups.remove_by_group(group, kind);
            }
            return Err(self.control_error());
        }
        match receive.await {
            Ok(result) => result,
            Err(_) if matches!(self.state(), RuntimeState::Stopped | RuntimeState::Failed) => {
                let table_kind = match kind {
                    GroupKind::Producer => Some(super::tables::TableKind::Producer),
                    GroupKind::Consumer => Some(super::tables::TableKind::Consumer),
                    GroupKind::Admin => None,
                };
                let _gate = match table_kind {
                    Some(table_kind) => Some(
                        self.groups
                            .table_mutation_gate(table_kind, group)?
                            .lock_owned()
                            .await,
                    ),
                    None => None,
                };
                self.groups.remove_by_group(group, kind)
            }
            Err(_) => Err(self.control_error()),
        }
    }

    async fn request_unregister(
        self: &Arc<Self>,
        lease: &GroupLease,
        kind: GroupKind,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        let monitor = if kind == GroupKind::Admin {
            None
        } else {
            Some(Arc::new(self.monitor_gate.clone().lock_owned().await))
        };
        let table_kind = match kind {
            GroupKind::Producer => Some(super::tables::TableKind::Producer),
            GroupKind::Consumer => Some(super::tables::TableKind::Consumer),
            GroupKind::Admin => None,
        };
        let gate = match table_kind {
            Some(table_kind) => Some(
                self.groups
                    .table_mutation_gate(table_kind, &lease.group)?
                    .lock_owned()
                    .await,
            ),
            None => None,
        };
        if self.state() == RuntimeState::Failed {
            return self.groups.unregister(lease, kind);
        }
        if self.state() == RuntimeState::Stopped {
            return Ok(false);
        }
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.control
            .sender(self)?
            .send(
                Command::Unregister {
                    lease: lease.clone(),
                    kind,
                    deadline,
                    gate,
                    reply: Some(reply),
                }
                .with_monitor(monitor.clone()),
            )
            .map_err(|_| self.control_error())?;
        receive.await.map_err(|_| self.control_error())?
    }

    fn control_error(&self) -> RuntimeError {
        match self.state() {
            RuntimeState::Failed | RuntimeState::Stopped => {
                RuntimeError::InvalidState(self.state())
            }
            _ => RuntimeError::ControlTaskFailed,
        }
    }

    pub(crate) fn rpc_timeout(&self) -> tokio::time::Duration {
        tokio::time::Duration::from_millis(self.config_read().mq_client_api_timeout.max(0) as u64)
    }

    pub(super) fn attach_manager(self: &Arc<Self>, link: ManagerLink) -> Result<(), RuntimeError> {
        {
            let mut links = self
                .manager
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            if !links.iter().any(|existing| existing.same_table(&link)) {
                links.push(link);
            }
        }
        self.inbound.bind(self);
        if let Some(listener) = &self.heartbeat_listener {
            listener.bind(self);
        }
        Ok(())
    }

    pub(super) fn remove_from_manager(self: &Arc<Self>) -> Result<bool, RuntimeError> {
        let links = self
            .manager
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?
            .clone();
        let mut removed = false;
        for link in links {
            removed |= link.remove(self)?;
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, Instant};

    struct RequestReplyProducer;
    impl ProducerRuntime for RequestReplyProducer {}

    #[test]
    fn callback_thread_effective_capacity_matches_invoked_java_executor() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        for java in fixture["callbackThreadsConstruction"].as_array().unwrap() {
            let configured = i32::try_from(java["input"].as_i64().unwrap()).unwrap();
            assert_eq!(
                callback_executor_threads(configured),
                i32::try_from(java["effective"].as_i64().unwrap()).unwrap(),
                "{java}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn request_reply_timeout_worker_is_shared_and_joined_with_runtime() {
        let mut config = ClientConfig::new("reply-timeout-worker");
        config.namesrv_addr = Some(String::new());
        let runtime = Arc::new(ClientRuntime::construct(config, &HashMap::new(), None).unwrap());
        let first = runtime
            .register_producer("first", Arc::new(RequestReplyProducer))
            .await
            .unwrap();
        let first_task_id = runtime
            .reply_timeout_task
            .lock()
            .await
            .as_ref()
            .unwrap()
            .id();
        let second = runtime
            .register_producer("second", Arc::new(RequestReplyProducer))
            .await
            .unwrap();
        assert_eq!(
            runtime
                .reply_timeout_task
                .lock()
                .await
                .as_ref()
                .unwrap()
                .id(),
            first_task_id,
            "producers sharing a runtime share one timeout worker"
        );

        runtime.start().await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        runtime.unregister_producer(&first, deadline).await.unwrap();
        assert!(runtime.reply_timeout_task.lock().await.is_some());
        runtime
            .unregister_producer(&second, deadline)
            .await
            .unwrap();
        assert!(
            runtime.reply_timeout_task.lock().await.is_none(),
            "the shared worker stops when the last producer unregisters"
        );

        let third = runtime
            .register_producer("third", Arc::new(RequestReplyProducer))
            .await
            .unwrap();
        assert!(runtime.reply_timeout_task.lock().await.is_some());
        runtime.unregister_producer(&third, deadline).await.unwrap();
        assert!(runtime.reply_timeout_task.lock().await.is_none());

        let fourth = runtime
            .register_producer("fourth", Arc::new(RequestReplyProducer))
            .await
            .unwrap();
        let pending_deadline = Instant::now() + Duration::from_secs(60);
        let mut pending = runtime
            .register_reply_request(&fourth, "group-unregister-pending", pending_deadline)
            .await
            .unwrap();
        assert!(runtime.reply_timeout_task.lock().await.is_some());
        assert!(
            runtime
                .unregister_producer_by_group("fourth", deadline)
                .await
                .unwrap()
        );
        assert!(runtime.reply_timeout_task.lock().await.is_none());
        assert!(
            tokio::time::timeout(Duration::from_millis(1), pending.receive())
                .await
                .is_err()
        );
        assert!(pending.control().cancel());

        assert!(runtime.shutdown().await.unwrap());
        assert!(runtime.reply_timeout_task.lock().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn route_refresh_and_offline_cleanup_share_three_second_gate_without_blocking_readers() {
        let config = ClientConfig::new("192.0.2.6");
        let runtime =
            Arc::new(ClientRuntime::construct(config.clone(), &HashMap::new(), None).unwrap());
        let other = ClientRuntime::construct(config, &HashMap::new(), None).unwrap();
        let held = runtime.namesrv_gate.lock().await;
        let before = runtime.route_snapshot().unwrap();
        let update = {
            let runtime = runtime.clone();
            tokio::spawn(async move {
                runtime
                    .update_topic_route("topic", Instant::now() + Duration::from_secs(30))
                    .await
            })
        };
        let cleanup = {
            let runtime = runtime.clone();
            tokio::spawn(async move { runtime.clean_offline_brokers().await })
        };
        tokio::task::yield_now().await;
        assert!(other.namesrv_gate.try_lock().is_ok());
        assert!(Arc::ptr_eq(&before, &runtime.route_snapshot().unwrap()));
        tokio::time::advance(Duration::from_millis(2999)).await;
        assert!(!update.is_finished());
        assert!(!cleanup.is_finished());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(!update.await.unwrap().unwrap()); // No transport started/fetch attempted.
        cleanup.await.unwrap().unwrap();
        assert!(Arc::ptr_eq(&before, &runtime.route_snapshot().unwrap()));
        drop(held);
        let acquired = runtime.namesrv_gate.lock().await;
        let cancelled = {
            let runtime = runtime.clone();
            tokio::spawn(async move {
                runtime
                    .update_topic_route("cancelled", Instant::now() + Duration::from_secs(30))
                    .await
            })
        };
        tokio::task::yield_now().await;
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        drop(acquired);
        assert!(runtime.namesrv_gate.try_lock().is_ok());
    }
}
