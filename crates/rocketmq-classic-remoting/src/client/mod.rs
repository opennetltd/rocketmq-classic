mod config;
mod connection;
mod connection_manager;
mod java_number;
mod pending;
mod processor;
mod response_future;
pub mod tls;

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, RwLock, Weak,
        atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering},
    },
};

use rand::{Rng, seq::SliceRandom};
use rocketmq_classic_protocol::codes::JavaEnumLookupError;
use std::task::{Context, Poll, Waker};
use tokio::{
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
    time::{self, Instant},
};

use crate::{command::RemotingCommand, error::TransportError, metrics::RemotingMetricsManager};

pub use config::{ClientConfig, SocksProxyConfig};
pub use connection::TransportMode;
pub use processor::{RequestContext, RequestProcessor};
pub use response_future::{
    ChannelHandle, ResponseFuture, ResponseWaitError, ResponseWaitInterrupt,
    SemaphoreReleaseOnlyOnce,
};
pub use tls::{PrivateKeyDecryptionStrategy, TlsConfig, TlsMode, register_decryption_strategy};

#[cfg(test)]
const LEGACY_OUTBOUND_QUEUE_CAPACITY: usize = 128;
const CHANNEL_EVENT_QUEUE_CAPACITY: usize = 10001;
const NAME_SERVER_SCAN_MAX_IN_FLIGHT: usize = 42;
const SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);

fn semaphore_admission_capacity(configured: i32) -> usize {
    // Java's Semaphore stores permits as a signed int. Its first acquire from
    // Integer.MIN_VALUE wraps to Integer.MAX_VALUE, so the observable admission
    // capacity for that one value is MAX_VALUE + 1. Other negative values never
    // acquire and are represented by a closed Tokio semaphore.
    let capacity = if configured == i32::MIN {
        i32::MAX as usize + 1
    } else {
        configured.max(0) as usize
    };
    capacity.min(tokio::sync::Semaphore::MAX_PERMITS)
}

fn callback_worker_limit(configured: i32) -> usize {
    if configured <= 0 {
        4
    } else {
        configured as usize
    }
}

#[derive(Clone)]
pub struct ProcessorRegistration {
    pub processor: Arc<dyn RequestProcessor>,
    pub executor: Option<Arc<dyn RequestExecutor>>,
    identity: Arc<()>,
}

impl ProcessorRegistration {
    pub fn new(
        processor: Arc<dyn RequestProcessor>,
        executor: Option<Arc<dyn RequestExecutor>>,
    ) -> Self {
        Self {
            processor,
            executor,
            identity: Arc::new(()),
        }
    }
}

/// An internal ownership boundary for a registered inbound slot. Dropping an
/// old owner cannot remove a replacement, even with the same processor handle.
pub struct ProcessorLease {
    inner: Weak<ClientInner>,
    code: i32,
    identity: Arc<()>,
}

impl Drop for ProcessorLease {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            let mut processors = inner.processors.lock().unwrap();
            if processors
                .get(&self.code)
                .is_some_and(|entry| Arc::ptr_eq(&entry.identity, &self.identity))
            {
                processors.remove(&self.code);
            }
        }
    }
}

#[derive(Clone)]
struct CallbackTracking {
    channel_established: Arc<AtomicBool>,
    opaque: Arc<AtomicI32>,
    request: Arc<Mutex<RemotingCommand>>,
    timing: Arc<Mutex<CallbackTiming>>,
}

#[derive(Clone, Copy)]
struct CallbackTiming {
    wrapper_timeout_millis: i64,
    response_begin: Instant,
    response_timeout_millis: i64,
}

#[derive(Clone, Copy)]
struct InvokeOptions {
    admitted: bool,
    retried_go_away: bool,
    run_rpc_hooks: bool,
    close_socket_on_timeout: bool,
    timeout_millis: std::time::Duration,
}

struct CallbackJob {
    future: Arc<ResponseFuture>,
    channel_established: Arc<AtomicBool>,
}

type CallbackTask = Box<dyn FnOnce() + Send + 'static>;

struct PendingEntry {
    generation: u64,
    deadline: Instant,
    address: String,
    request_code: i32,
    response: oneshot::Sender<Result<RemotingCommand, TransportError>>,
    // Sync wait expiry detaches the caller while the response table retains admission ownership.
    retained_admission: Option<tokio::sync::OwnedSemaphorePermit>,
}

struct Startup {
    inbound_receiver: mpsc::UnboundedReceiver<processor::InboundRequest>,
    callback_receiver: mpsc::UnboundedReceiver<CallbackTask>,
    channel_event_receiver: mpsc::Receiver<ChannelEvent>,
}

struct ClientInner {
    connections: Mutex<HashMap<String, Arc<connection::Connection>>>,
    connection_wrapper_locks: Mutex<HashMap<String, Weak<RwLock<()>>>>,
    connecting: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
    retired_connections: Mutex<HashMap<u64, (String, Arc<connection::Connection>)>>,
    pending: Mutex<HashMap<i32, PendingEntry>>,
    opaque: AtomicI32,
    generation: AtomicU64,
    processors: Mutex<HashMap<i32, ProcessorRegistration>>,
    default_processor: Mutex<Option<ProcessorRegistration>>,
    inbound: mpsc::UnboundedSender<processor::InboundRequest>,
    public_executor: Arc<TokioRequestExecutor>,
    shutdown: tokio::sync::watch::Sender<bool>,
    callback_shutdown: tokio::sync::watch::Sender<bool>,
    callback_workers: Mutex<Vec<JoinHandle<()>>>,
    channel_event_stopped: tokio::sync::watch::Sender<bool>,
    channel_event_worker: Mutex<Option<JoinHandle<()>>>,
    shutdown_cleanup_stopped: tokio::sync::watch::Sender<bool>,
    shutdown_started: tokio::sync::OnceCell<()>,
    closed: AtomicBool,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    security: TransportMode,
    use_tls: AtomicBool,
    max_frame_size_override: Option<usize>,
    config: ClientConfig,
    connect_timeout_millis: AtomicI32,
    active_connect_timeout_millis: AtomicI32,
    proxy_connect_timeout_millis: Mutex<HashMap<String, i32>>,
    close_socket_if_timeout: AtomicBool,
    enable_reconnect_for_go_away: AtomicBool,
    disable_callback_executor: AtomicBool,
    client_worker_threads: AtomicI32,
    disable_netty_worker_group: AtomicBool,
    client_channel_max_idle_time_seconds: AtomicI32,
    client_socket_send_buffer_size: AtomicI32,
    client_socket_receive_buffer_size: AtomicI32,
    write_buffer_high_water_mark: AtomicI32,
    write_buffer_low_water_mark: AtomicI32,
    pooled_byte_buffer_allocator_enabled: AtomicBool,
    active_connection_settings: Mutex<Option<config::ConnectionSettings>>,
    proxy_connection_settings: Mutex<HashMap<String, config::ConnectionSettings>>,
    scan_available_nameserver: AtomicBool,
    socks_proxy_routes: config::ProxyRoutes,
    async_admission: Arc<tokio::sync::Semaphore>,
    oneway_admission: Arc<tokio::sync::Semaphore>,
    callback_sender: mpsc::UnboundedSender<CallbackTask>,
    callback_executor: Mutex<Option<Arc<dyn CallbackExecutor>>>,
    rpc_hooks: Mutex<Vec<Arc<dyn RpcHook>>>,
    channel_listener: Option<Arc<dyn ChannelEventListener>>,
    channel_event_sender: mpsc::Sender<ChannelEvent>,
    startup: Mutex<Option<Startup>>,
    runtime: Mutex<Option<tokio::runtime::Handle>>,
    started: AtomicBool,
    request_pipeline: Mutex<Option<Arc<dyn RequestPipeline>>>,
    metrics_manager: RwLock<Option<Arc<RemotingMetricsManager>>>,
    nameservers: Mutex<Option<Vec<String>>>,
    available_nameservers: Mutex<HashSet<String>>,
    chosen_nameserver: Mutex<Option<String>>,
    nameserver_index: AtomicU64,
}

impl ClientInner {
    fn active_connect_timeout_millis(&self) -> i32 {
        self.active_connect_timeout_millis.load(Ordering::Acquire)
    }

    fn scan_interval(&self) -> std::time::Duration {
        config::scan_interval(self.active_connect_timeout_millis())
    }

    fn deadline_for_connect_timeout(
        deadline: Option<Instant>,
        timeout_millis: i32,
    ) -> Option<Instant> {
        if timeout_millis == 0 {
            deadline
        } else {
            let connect_deadline =
                Instant::now() + config::connect_timeout_duration(timeout_millis);
            Some(deadline.map_or(connect_deadline, |deadline| deadline.min(connect_deadline)))
        }
    }

    fn connection_deadline_for_route(
        &self,
        proxy_cidr: Option<&str>,
        deadline: Option<Instant>,
    ) -> Option<Instant> {
        let timeout_millis = match proxy_cidr {
            Some(cidr) => {
                let mut timeouts = self.proxy_connect_timeout_millis.lock().unwrap();
                *timeouts
                    .entry(cidr.to_owned())
                    .or_insert_with(|| self.connect_timeout_millis.load(Ordering::Acquire))
            }
            None => self.active_connect_timeout_millis(),
        };
        Self::deadline_for_connect_timeout(deadline, timeout_millis)
    }

    fn channel_pipeline_settings(&self) -> (std::time::Duration, bool) {
        let max_idle_seconds = self
            .client_channel_max_idle_time_seconds
            .load(Ordering::Acquire);
        let disable_worker_group = self.disable_netty_worker_group.load(Ordering::Acquire);
        (
            std::time::Duration::from_secs(max_idle_seconds.max(0) as u64),
            disable_worker_group,
        )
    }

    fn direct_connection_settings(&self) -> Result<config::ConnectionSettings, TransportError> {
        self.active_connection_settings
            .lock()
            .unwrap()
            .as_ref()
            .copied()
            .ok_or(TransportError::NotStarted)
    }

    fn connection_settings_for_route(
        &self,
        proxy_cidr: Option<&str>,
    ) -> Result<config::ConnectionSettings, TransportError> {
        match proxy_cidr {
            None => self.direct_connection_settings(),
            Some(cidr) => {
                let mut settings = self.proxy_connection_settings.lock().unwrap();
                if let Some(cached) = settings.get(cidr) {
                    return Ok(*cached);
                }
                let captured = config::ConnectionSettings::proxy(
                    self.client_socket_send_buffer_size.load(Ordering::Acquire),
                    self.client_socket_receive_buffer_size
                        .load(Ordering::Acquire),
                )?;
                settings.insert(cidr.to_owned(), captured);
                Ok(captured)
            }
        }
    }

    fn capture_direct_connection_settings(
        &self,
    ) -> Result<config::ConnectionSettings, TransportError> {
        config::ConnectionSettings::direct(
            self.client_socket_send_buffer_size.load(Ordering::Acquire),
            self.client_socket_receive_buffer_size
                .load(Ordering::Acquire),
            self.write_buffer_low_water_mark.load(Ordering::Acquire),
            self.write_buffer_high_water_mark.load(Ordering::Acquire),
            self.pooled_byte_buffer_allocator_enabled
                .load(Ordering::Acquire),
        )
    }

    fn frame_max_length(&self) -> Result<usize, TransportError> {
        match self.max_frame_size_override {
            Some(max_frame_size) => Ok(max_frame_size),
            None => config::java_frame_max_length(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestPipelineError {
    Abort {
        response_code: i32,
        remark: Option<String>,
    },
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RpcHookError {
    Abort {
        response_code: i32,
        remark: Option<String>,
    },
    Failed(String),
}

fn rpc_hook_transport_error(error: RpcHookError) -> TransportError {
    match error {
        RpcHookError::Abort {
            response_code,
            remark,
        } => TransportError::RpcHook(format!(
            "aborted with response code {response_code}: {}",
            remark.unwrap_or_default()
        )),
        RpcHookError::Failed(message) => TransportError::RpcHook(message),
    }
}

fn remaining_timeout_millis(deadline: Instant, now: Instant) -> i64 {
    let (duration, expired) = if deadline >= now {
        (deadline.duration_since(now), false)
    } else {
        (now.duration_since(deadline), true)
    };
    let millis = duration.as_millis().min(i64::MAX as u128) as i64;
    if expired { -millis } else { millis }
}

fn classify_connection_error(remote_addr: &str, error: TransportError) -> TransportError {
    let cause = match error {
        TransportError::Connect(message) | TransportError::Tls(message) => {
            Some(crate::error::ErrorSource::message(message))
        }
        TransportError::ConnectSource(source) | TransportError::TlsSource(source) => Some(source),
        TransportError::Closed => None,
        error => return error,
    };
    TransportError::ConnectRequest {
        remote_addr: remote_addr.to_owned(),
        cause,
    }
}

fn classify_write_error(remote_addr: &str, error: TransportError) -> TransportError {
    let cause = match error {
        TransportError::Send(message) => Some(crate::error::ErrorSource::message(message)),
        TransportError::SendSource(source) => Some(source),
        TransportError::Closed | TransportError::Codec(_) | TransportError::MalformedFrame(_) => {
            None
        }
        error => return error,
    };
    TransportError::SendRequest {
        remote_addr: remote_addr.to_owned(),
        cause,
    }
}

fn run_before_rpc_hooks(
    hooks: &[Arc<dyn RpcHook>],
    remote_addr: &str,
    request: &mut RemotingCommand,
) -> Result<(), RpcHookError> {
    for hook in hooks {
        hook.before_request_mut_result(remote_addr, request)?;
    }
    Ok(())
}

fn run_after_rpc_hooks(
    hooks: &[Arc<dyn RpcHook>],
    remote_addr: &str,
    request: &mut RemotingCommand,
    response: &mut RemotingCommand,
) -> Result<(), RpcHookError> {
    for hook in hooks {
        hook.after_response_mut_result(remote_addr, request, response)?;
    }
    Ok(())
}

/// Routes a processor task to an application-owned executor. Returning `Err` rejects the task.
pub trait RequestExecutor: Send + Sync + 'static {
    fn execute(&self, task: Box<dyn FnOnce() + Send + 'static>) -> Result<(), String>;
}

struct TokioExecutorState {
    closed: AtomicBool,
    admission: Mutex<()>,
}

impl TokioExecutorState {
    fn execute(
        &self,
        sender: &mpsc::UnboundedSender<CallbackTask>,
        task: CallbackTask,
    ) -> Result<(), String> {
        let _admission = self.admission.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            return Err("client public executor is shut down".into());
        }
        sender.send(task).map_err(|error| error.to_string())
    }

    fn shutdown(&self) {
        let _admission = self.admission.lock().unwrap();
        self.closed.store(true, Ordering::Release);
    }
}

struct TokioRequestExecutor {
    sender: mpsc::UnboundedSender<CallbackTask>,
    state: Arc<TokioExecutorState>,
}

impl RequestExecutor for TokioRequestExecutor {
    fn execute(&self, task: CallbackTask) -> Result<(), String> {
        self.state.execute(&self.sender, task)
    }
}

pub trait RequestPipeline: Send + Sync + 'static {
    fn execute(&self, request: &RemotingCommand) -> Result<(), i32>;

    fn execute_detailed(&self, request: &RemotingCommand) -> Result<(), RequestPipelineError> {
        self.execute(request)
            .map_err(|response_code| RequestPipelineError::Abort {
                response_code,
                remark: Some("request pipeline aborted".into()),
            })
    }

    /// Executes with the channel context for the inbound request.
    ///
    /// The default preserves existing implementations. Context-aware pipelines can override this
    /// method to inspect, close, or write through the request's channel.
    fn execute_detailed_with_context(
        &self,
        _context: &RequestContext,
        request: &RemotingCommand,
    ) -> Result<(), RequestPipelineError> {
        self.execute_detailed(request)
    }

    /// Context-aware mutable form of `execute`, preserving existing read-only implementations.
    fn execute_detailed_with_context_mut(
        &self,
        context: &RequestContext,
        request: &mut RemotingCommand,
    ) -> Result<(), RequestPipelineError> {
        self.execute_detailed_with_context(context, request)
    }
}

impl<F> RequestPipeline for F
where
    F: Fn(&RemotingCommand) -> Result<(), i32> + Send + Sync + 'static,
{
    fn execute(&self, request: &RemotingCommand) -> Result<(), i32> {
        self(request)
    }
}

struct PipedRequestPipeline {
    source: Arc<dyn RequestPipeline>,
    existing: Arc<dyn RequestPipeline>,
}

impl RequestPipeline for PipedRequestPipeline {
    fn execute(&self, request: &RemotingCommand) -> Result<(), i32> {
        self.source.execute(request)?;
        self.existing.execute(request)
    }

    fn execute_detailed(&self, request: &RemotingCommand) -> Result<(), RequestPipelineError> {
        self.source.execute_detailed(request)?;
        self.existing.execute_detailed(request)
    }

    fn execute_detailed_with_context(
        &self,
        context: &RequestContext,
        request: &RemotingCommand,
    ) -> Result<(), RequestPipelineError> {
        self.source
            .execute_detailed_with_context(context, request)?;
        self.existing
            .execute_detailed_with_context(context, request)
    }

    fn execute_detailed_with_context_mut(
        &self,
        context: &RequestContext,
        request: &mut RemotingCommand,
    ) -> Result<(), RequestPipelineError> {
        self.source
            .execute_detailed_with_context_mut(context, request)?;
        self.existing
            .execute_detailed_with_context_mut(context, request)
    }
}

/// RPC interception hooks. Hook errors are propagated through the request path. Outbound hook
/// panics propagate from the invoking task; inbound hook panics map to SYSTEM_ERROR.
pub trait RpcHook: Send + Sync + 'static {
    /// Java RPCHook may mutate the command before encoding/signing. Existing
    /// read-only hooks adapt through their original result method by default.
    fn before_request_mut_result(
        &self,
        remote_addr: &str,
        request: &mut RemotingCommand,
    ) -> Result<(), RpcHookError> {
        self.before_request_result(remote_addr, request)
    }

    /// Optional equality key used to reproduce Java `List.contains` deduplication.
    fn equality_key(&self) -> Option<String> {
        None
    }

    fn before_request(&self, _remote_addr: &str, _request: &RemotingCommand) {}
    fn after_response(
        &self,
        _remote_addr: &str,
        _request: &RemotingCommand,
        _response: &RemotingCommand,
    ) {
    }

    fn before_request_result(
        &self,
        remote_addr: &str,
        request: &RemotingCommand,
    ) -> Result<(), RpcHookError> {
        self.before_request(remote_addr, request);
        Ok(())
    }

    fn after_response_result(
        &self,
        remote_addr: &str,
        request: &RemotingCommand,
        response: &RemotingCommand,
    ) -> Result<(), RpcHookError> {
        self.after_response(remote_addr, request, response);
        Ok(())
    }

    /// Runs after a response arrives and may update it before the caller receives it.
    ///
    /// The default preserves hooks that only inspect the response. Java `RPCHook` receives the
    /// mutable `RemotingCommand`, so implementations that need to rewrite response fields can
    /// override this method.
    fn after_response_mut_result(
        &self,
        remote_addr: &str,
        request: &mut RemotingCommand,
        response: &mut RemotingCommand,
    ) -> Result<(), RpcHookError> {
        self.after_response_optional_result(remote_addr, request, Some(response))
    }

    fn after_response_optional_result(
        &self,
        remote_addr: &str,
        request: &RemotingCommand,
        response: Option<&RemotingCommand>,
    ) -> Result<(), RpcHookError> {
        if let Some(response) = response {
            self.after_response_result(remote_addr, request, response)
        } else {
            Ok(())
        }
    }

    /// Runs after an inbound request has been processed and may update its response before it is
    /// written to the request's channel. The default keeps existing read-only hooks working,
    /// including hooks that override `after_response_optional_result`.
    fn after_response_optional_mut_result(
        &self,
        remote_addr: &str,
        request: &mut RemotingCommand,
        response: Option<&mut RemotingCommand>,
    ) -> Result<(), RpcHookError> {
        if let Some(response) = response {
            self.after_response_mut_result(remote_addr, request, response)
        } else {
            self.after_response_optional_result(remote_addr, request, None)
        }
    }
}

impl<F> RpcHook for F
where
    F: Fn(&str, &RemotingCommand) + Send + Sync + 'static,
{
    fn before_request(&self, remote_addr: &str, request: &RemotingCommand) {
        self(remote_addr, request);
    }
}

/// Connection lifecycle notifications corresponding to Java `ChannelEventListener`.
pub trait InvokeCallback: Send + Sync + 'static {
    fn operation_complete(&self, _future: &ResponseFuture) {}
    fn operation_succeed(&self, _response: &RemotingCommand) {}
    fn operation_fail(&self, _error: &TransportError) {}
}

impl<F> InvokeCallback for F
where
    F: Fn(Result<RemotingCommand, TransportError>) + Send + Sync + 'static,
{
    fn operation_succeed(&self, response: &RemotingCommand) {
        self(Ok(response.clone()));
    }

    fn operation_fail(&self, error: &TransportError) {
        self(Err(error.clone()));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NettyEventType {
    Connect,
    Close,
    Idle,
    Exception,
    Active,
}

impl NettyEventType {
    pub const fn values() -> [Self; 5] {
        [
            Self::Connect,
            Self::Close,
            Self::Idle,
            Self::Exception,
            Self::Active,
        ]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
        match name {
            None => Err(JavaEnumLookupError::NullName),
            Some("CONNECT") => Ok(Self::Connect),
            Some("CLOSE") => Ok(Self::Close),
            Some("IDLE") => Ok(Self::Idle),
            Some("EXCEPTION") => Ok(Self::Exception),
            Some("ACTIVE") => Ok(Self::Active),
            Some(other) => Err(JavaEnumLookupError::UnknownName {
                enum_class: "org.apache.rocketmq.remoting.netty.NettyEventType",
                name: other.to_owned(),
            }),
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Connect => "CONNECT",
            Self::Close => "CLOSE",
            Self::Idle => "IDLE",
            Self::Exception => "EXCEPTION",
            Self::Active => "ACTIVE",
        }
    }

    pub const fn ordinal(self) -> i32 {
        match self {
            Self::Connect => 0,
            Self::Close => 1,
            Self::Idle => 2,
            Self::Exception => 3,
            Self::Active => 4,
        }
    }

    pub const fn java_compare_to(self, other: Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl std::fmt::Display for NettyEventType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

/// Routes callbacks to an application-owned executor, corresponding to Java's `setCallbackExecutor`.
pub trait CallbackExecutor: Send + Sync + 'static {
    fn execute(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), Box<dyn FnOnce() + Send + 'static>>;
}

struct ClientCallbackExecutor {
    sender: mpsc::UnboundedSender<CallbackTask>,
}

impl CallbackExecutor for ClientCallbackExecutor {
    fn execute(&self, task: CallbackTask) -> Result<(), CallbackTask> {
        self.sender.send(task).map_err(|error| error.0)
    }
}

/// Owned channel lifecycle information passed through Java-shaped remoting callbacks.
///
/// Connection generation is private transport bookkeeping and is not exposed as an event field:
///
/// ```compile_fail
/// use rocketmq_classic_remoting::client::ChannelEvent;
/// let event: ChannelEvent = loop {};
/// let _generation = event.generation;
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelEvent {
    pub remote_addr: String,
    pub event_type: NettyEventType,
    pub channel: ChannelHandle,
}

pub trait ChannelEventListener: Send + Sync + 'static {
    /// Synchronous callbacks run serially on a Tokio blocking thread behind the bounded event
    /// queue, so a slow observer cannot block transport or a Tokio runtime worker. Queue overflow
    /// drops the event. A synchronous callback already running cannot be forcibly cancelled by
    /// shutdown; it must return for its blocking thread to be reclaimed.
    /// Receives the event with its Rust channel handle.
    fn on_channel_event(&self, event: &ChannelEvent) {
        match event.event_type {
            NettyEventType::Connect => self.on_connect(&event.remote_addr, &event.channel),
            NettyEventType::Close => self.on_close(&event.remote_addr, &event.channel),
            NettyEventType::Exception => self.on_exception(&event.remote_addr, &event.channel),
            NettyEventType::Idle => self.on_idle(&event.remote_addr, &event.channel),
            // Active is delivered through the async adapter below.
            NettyEventType::Active => {}
        }
    }

    fn on_connect(&self, _remote_addr: &str, _channel: &ChannelHandle) {}
    fn on_close(&self, _remote_addr: &str, _channel: &ChannelHandle) {}
    fn on_exception(&self, _remote_addr: &str, _channel: &ChannelHandle) {}
    fn on_idle(&self, _remote_addr: &str, _channel: &ChannelHandle) {}
    fn on_active(&self, _remote_addr: &str, _channel: &ChannelHandle) {}
    /// Borrowed async work stays in the owned event worker; existing sync
    /// listeners use the default adapter without spawning detached tasks. The
    /// worker awaits this callback like Java's synchronous listener, so shutdown
    /// can wait up to 90 seconds and cannot cancel an in-flight callback.
    fn on_active_async<'a>(
        &'a self,
        remote_addr: &'a str,
        channel: &'a ChannelHandle,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            self.on_active(remote_addr, channel);
        })
    }
}

/// Deadline-based RocketMQ remoting client. Call [`start`](Self::start) before using transport operations.
///
/// The peer-returning NameServer helper is an internal adapter, not a public
/// operation. Java's public invocation returns the response command; peer
/// selection metadata stays inside this client unless an owning API needs it
/// for its error context.
///
/// ```compile_fail
/// use rocketmq_classic_remoting::{client::RemotingClient, RemotingCommand};
///
/// async fn peer_address_is_not_a_public_operation(client: &RemotingClient) {
///     let _ = client
///         .invoke_name_server_with_peer(
///             RemotingCommand::default(),
///             tokio::time::Instant::now(),
///         )
///         .await;
/// }
/// ```
#[derive(Clone)]
pub struct RemotingClient {
    inner: Arc<ClientInner>,
}

/// A point-in-time view of the remoting client's retained lifecycle state.
#[cfg(rocketmq_classic_test_support)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemotingLifecycleCounts {
    pub pending_requests: usize,
    pub connections: usize,
    pub owned_tasks: usize,
}

impl RemotingClient {
    pub fn new(security: TransportMode) -> Self {
        Self::with_config(ClientConfig {
            transport: security,
            ..ClientConfig::default()
        })
        .expect("default client configuration is valid")
    }

    pub fn with_config(config: ClientConfig) -> Result<Self, TransportError> {
        Self::with_config_and_channel_event_listener(config, None)
    }

    pub fn with_config_and_channel_event_listener(
        mut config: ClientConfig,
        channel_listener: Option<Arc<dyn ChannelEventListener>>,
    ) -> Result<Self, TransportError> {
        if let TransportMode::Tls(tls) = &mut config.transport {
            tls.reload_property_file_for_context()?;
        }
        config.validate()?;
        if let TransportMode::Tls(tls) = &config.transport {
            // Validate certificate and key material before retaining worker
            // receivers or allowing start() to launch background tasks.
            let _ = tls.connector()?;
        }
        let socks_proxy_routes = config.parse_socks_proxy_config()?;
        let use_tls = matches!(&config.transport, TransportMode::Tls(_));
        let close_socket_if_timeout = config.close_socket_if_timeout;
        let enable_reconnect_for_go_away = config.enable_reconnect_for_go_away;
        let disable_callback_executor = config.disable_callback_executor;
        let scan_available_nameserver = config.scan_available_nameserver;
        let (inbound, inbound_rx) = mpsc::unbounded_channel();
        let (shutdown, _) = tokio::sync::watch::channel(false);
        let (callback_shutdown, _) = tokio::sync::watch::channel(false);
        let (channel_event_stopped, _) = tokio::sync::watch::channel(false);
        let (shutdown_cleanup_stopped, _) = tokio::sync::watch::channel(false);
        let (callback_sender, callback_receiver) = mpsc::unbounded_channel();
        let (channel_event_sender, channel_event_receiver) =
            mpsc::channel(CHANNEL_EVENT_QUEUE_CAPACITY);
        let default_callback_executor: Arc<dyn CallbackExecutor> =
            Arc::new(ClientCallbackExecutor {
                sender: callback_sender.clone(),
            });
        let inner = Arc::new(ClientInner {
            connections: Mutex::new(HashMap::new()),
            connection_wrapper_locks: Mutex::new(HashMap::new()),
            retired_connections: Mutex::new(HashMap::new()),
            connecting: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            opaque: AtomicI32::new(0),
            generation: AtomicU64::new(1),
            processors: Mutex::new(HashMap::new()),
            default_processor: Mutex::new(None),
            inbound,
            shutdown,
            callback_shutdown,
            callback_workers: Mutex::new(Vec::new()),
            channel_event_stopped,
            channel_event_worker: Mutex::new(None),
            shutdown_cleanup_stopped,
            shutdown_started: tokio::sync::OnceCell::new(),
            closed: AtomicBool::new(false),
            tasks: Mutex::new(Vec::new()),
            security: config.transport.clone(),
            use_tls: AtomicBool::new(use_tls),
            max_frame_size_override: None,
            connect_timeout_millis: AtomicI32::new(config.connect_timeout),
            active_connect_timeout_millis: AtomicI32::new(config.connect_timeout),
            proxy_connect_timeout_millis: Mutex::new(HashMap::new()),
            close_socket_if_timeout: AtomicBool::new(close_socket_if_timeout),
            enable_reconnect_for_go_away: AtomicBool::new(enable_reconnect_for_go_away),
            disable_callback_executor: AtomicBool::new(disable_callback_executor),
            client_worker_threads: AtomicI32::new(config.worker_threads),
            disable_netty_worker_group: AtomicBool::new(config.disable_netty_worker_group),
            client_channel_max_idle_time_seconds: AtomicI32::new(config.max_idle_time),
            client_socket_send_buffer_size: AtomicI32::new(config.client_socket_send_buffer_size),
            client_socket_receive_buffer_size: AtomicI32::new(
                config.client_socket_receive_buffer_size,
            ),
            write_buffer_high_water_mark: AtomicI32::new(config.write_buffer_high_water_mark),
            write_buffer_low_water_mark: AtomicI32::new(config.write_buffer_low_water_mark),
            pooled_byte_buffer_allocator_enabled: AtomicBool::new(
                config.pooled_byte_buffer_allocator_enabled,
            ),
            active_connection_settings: Mutex::new(None),
            proxy_connection_settings: Mutex::new(HashMap::new()),
            scan_available_nameserver: AtomicBool::new(scan_available_nameserver),
            async_admission: Arc::new(tokio::sync::Semaphore::new(semaphore_admission_capacity(
                config.async_semaphore_value,
            ))),
            oneway_admission: Arc::new(tokio::sync::Semaphore::new(semaphore_admission_capacity(
                config.oneway_semaphore_value,
            ))),
            callback_sender: callback_sender.clone(),
            callback_executor: Mutex::new(Some(default_callback_executor)),
            public_executor: Arc::new(TokioRequestExecutor {
                sender: callback_sender.clone(),
                state: Arc::new(TokioExecutorState {
                    closed: AtomicBool::new(false),
                    admission: Mutex::new(()),
                }),
            }),
            config,
            rpc_hooks: Mutex::new(Vec::new()),
            channel_listener,
            channel_event_sender,
            startup: Mutex::new(Some(Startup {
                inbound_receiver: inbound_rx,
                callback_receiver,
                channel_event_receiver,
            })),
            runtime: Mutex::new(None),
            started: AtomicBool::new(false),
            request_pipeline: Mutex::new(None),
            metrics_manager: RwLock::new(None),
            nameservers: Mutex::new(None),
            available_nameservers: Mutex::new(HashSet::new()),
            chosen_nameserver: Mutex::new(None),
            nameserver_index: AtomicU64::new(rand::rng().random_range(0..999)),
            socks_proxy_routes,
        });
        Ok(Self { inner })
    }

    /// Returns the number of pending requests, retained connections, and
    /// client-owned worker handles currently held by this remoting client.
    /// This is intended for lifecycle diagnostics after `shutdown()`.
    #[cfg(rocketmq_classic_test_support)]
    pub fn lifecycle_counts(&self) -> RemotingLifecycleCounts {
        let pending_requests = self.inner.pending.lock().unwrap().len();
        let connections = self.inner.connections.lock().unwrap().len()
            + self.inner.retired_connections.lock().unwrap().len();
        let owned_tasks = self
            .inner
            .tasks
            .lock()
            .unwrap()
            .iter()
            .filter(|task| !task.is_finished())
            .count()
            + self
                .inner
                .callback_workers
                .lock()
                .unwrap()
                .iter()
                .filter(|task| !task.is_finished())
                .count()
            + usize::from(
                self.inner
                    .channel_event_worker
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|task| !task.is_finished()),
            );
        RemotingLifecycleCounts {
            pending_requests,
            connections,
            owned_tasks,
        }
    }

    /// Returns the live Java `clientCloseSocketIfTimeout` behavior setting.
    pub fn get_client_close_socket_if_timeout(&self) -> bool {
        self.inner.close_socket_if_timeout.load(Ordering::Acquire)
    }

    /// Updates the Java `clientCloseSocketIfTimeout` behavior for later sync calls.
    pub fn set_client_close_socket_if_timeout(&self, enabled: bool) {
        self.inner
            .close_socket_if_timeout
            .store(enabled, Ordering::Release);
    }

    /// Returns the live Java `enableReconnectForGoAway` behavior setting.
    pub fn get_enable_reconnect_for_go_away(&self) -> bool {
        self.inner
            .enable_reconnect_for_go_away
            .load(Ordering::Acquire)
    }

    /// Updates whether later GO_AWAY responses reconnect and retry the request.
    pub fn set_enable_reconnect_for_go_away(&self, enabled: bool) {
        self.inner
            .enable_reconnect_for_go_away
            .store(enabled, Ordering::Release);
    }

    /// Returns whether Java-equivalent callback executor dispatch is disabled.
    pub fn get_disable_callback_executor(&self) -> bool {
        self.inner.disable_callback_executor.load(Ordering::Acquire)
    }

    /// Updates callback executor dispatch for later callback completions.
    pub fn set_disable_callback_executor(&self, disabled: bool) {
        self.inner
            .disable_callback_executor
            .store(disabled, Ordering::Release);
    }

    /// Returns the worker count used when the client starts its request workers.
    pub fn get_client_worker_threads(&self) -> i32 {
        self.inner.client_worker_threads.load(Ordering::Acquire)
    }

    /// Updates the worker count that `start()` uses to create request workers.
    pub fn set_client_worker_threads(&self, worker_threads: i32) {
        self.inner
            .client_worker_threads
            .store(worker_threads, Ordering::Release);
    }

    /// Returns the worker-group dispatch setting captured by newly created channels.
    pub fn get_disable_netty_worker_group(&self) -> bool {
        self.inner
            .disable_netty_worker_group
            .load(Ordering::Acquire)
    }

    /// Updates the dispatch path captured by each channel created after this call.
    pub fn set_disable_netty_worker_group(&self, disabled: bool) {
        self.inner
            .disable_netty_worker_group
            .store(disabled, Ordering::Release);
    }

    /// Returns the idle timeout captured by newly created channels, in seconds.
    pub fn get_client_channel_max_idle_time_seconds(&self) -> i32 {
        self.inner
            .client_channel_max_idle_time_seconds
            .load(Ordering::Acquire)
    }

    /// Updates the all-idle timeout for channels created after this call.
    pub fn set_client_channel_max_idle_time_seconds(&self, seconds: i32) {
        self.inner
            .client_channel_max_idle_time_seconds
            .store(seconds, Ordering::Release);
    }

    /// Returns whether new connections attempt TLS using the context captured at construction.
    pub fn get_use_tls(&self) -> bool {
        self.inner.use_tls.load(Ordering::Acquire)
    }

    /// Updates whether new connections use the TLS context captured at construction.
    /// Enabling TLS has no effect when the client was constructed without a TLS context.
    pub fn set_use_tls(&self, enabled: bool) {
        self.inner.use_tls.store(enabled, Ordering::Release);
    }

    /// Returns the configured Java client socket send-buffer size in bytes.
    pub fn get_client_socket_send_buffer_size(&self) -> i32 {
        self.inner
            .client_socket_send_buffer_size
            .load(Ordering::Acquire)
    }

    /// Sets the direct Bootstrap value captured by `start()` and the value
    /// captured when each proxy CIDR is first used.
    pub fn set_client_socket_send_buffer_size(&self, size: i32) {
        self.inner
            .client_socket_send_buffer_size
            .store(size, Ordering::Release);
    }

    /// Returns the configured Java client socket receive-buffer size in bytes.
    pub fn get_client_socket_receive_buffer_size(&self) -> i32 {
        self.inner
            .client_socket_receive_buffer_size
            .load(Ordering::Acquire)
    }

    /// Sets the direct Bootstrap value captured by `start()` and the value
    /// captured when each proxy CIDR is first used.
    pub fn set_client_socket_receive_buffer_size(&self, size: i32) {
        self.inner
            .client_socket_receive_buffer_size
            .store(size, Ordering::Release);
    }

    /// Returns the configured Java write-buffer high watermark in bytes.
    pub fn get_write_buffer_high_water_mark(&self) -> i32 {
        self.inner
            .write_buffer_high_water_mark
            .load(Ordering::Acquire)
    }

    /// Sets the direct Bootstrap value captured by `start()`.
    pub fn set_write_buffer_high_water_mark(&self, size: i32) {
        self.inner
            .write_buffer_high_water_mark
            .store(size, Ordering::Release);
    }

    /// Returns the configured Java write-buffer low watermark in bytes.
    pub fn get_write_buffer_low_water_mark(&self) -> i32 {
        self.inner
            .write_buffer_low_water_mark
            .load(Ordering::Acquire)
    }

    /// Sets the direct Bootstrap value captured by `start()`.
    pub fn set_write_buffer_low_water_mark(&self, size: i32) {
        self.inner
            .write_buffer_low_water_mark
            .store(size, Ordering::Release);
    }

    /// Returns whether the Rust per-connection frame reuse pool is enabled.
    pub fn is_pooled_byte_buffer_allocator_enabled(&self) -> bool {
        self.inner
            .pooled_byte_buffer_allocator_enabled
            .load(Ordering::Acquire)
    }

    /// Sets the direct Bootstrap value captured by `start()`.
    pub fn set_pooled_byte_buffer_allocator_enabled(&self, enabled: bool) {
        self.inner
            .pooled_byte_buffer_allocator_enabled
            .store(enabled, Ordering::Release);
    }

    /// Returns the configured Java `connectTimeoutMillis` value.
    pub fn get_connect_timeout_millis(&self) -> i32 {
        self.inner.connect_timeout_millis.load(Ordering::Acquire)
    }

    /// Sets the direct-connect value captured by `start()` and the live value captured
    /// when each proxy CIDR is first used.
    pub fn set_connect_timeout_millis(&self, timeout_millis: i32) {
        self.inner
            .connect_timeout_millis
            .store(timeout_millis, Ordering::Release);
    }

    /// Returns whether NameServer reachability scans are enabled in the config view.
    pub fn get_scan_available_nameserver(&self) -> bool {
        self.inner.scan_available_nameserver.load(Ordering::Acquire)
    }

    /// Updates whether `start()` schedules NameServer reachability scans.
    pub fn set_scan_available_nameserver(&self, enabled: bool) {
        self.inner
            .scan_available_nameserver
            .store(enabled, Ordering::Release);
    }

    #[cfg(test)]
    fn with_limits(security: TransportMode, max_frame_size: usize, initial_opaque: i32) -> Self {
        let use_tls = matches!(&security, TransportMode::Tls(_));
        let config = ClientConfig {
            transport: security.clone(),
            ..ClientConfig::default()
        };
        let (inbound, inbound_rx) = mpsc::unbounded_channel();
        let (shutdown, _) = tokio::sync::watch::channel(false);
        let (callback_shutdown, _) = tokio::sync::watch::channel(false);
        let (channel_event_stopped, _) = tokio::sync::watch::channel(false);
        let (shutdown_cleanup_stopped, _) = tokio::sync::watch::channel(false);
        let (callback_sender, _callback_receiver) = mpsc::unbounded_channel();
        let (channel_event_sender, _channel_event_receiver) =
            mpsc::channel(CHANNEL_EVENT_QUEUE_CAPACITY);
        let inner = Arc::new(ClientInner {
            connections: Mutex::new(HashMap::new()),
            connection_wrapper_locks: Mutex::new(HashMap::new()),
            retired_connections: Mutex::new(HashMap::new()),
            connecting: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            opaque: AtomicI32::new(initial_opaque),
            generation: AtomicU64::new(1),
            processors: Mutex::new(HashMap::new()),
            default_processor: Mutex::new(None),
            inbound,
            shutdown,
            callback_shutdown,
            callback_workers: Mutex::new(Vec::new()),
            channel_event_stopped,
            channel_event_worker: Mutex::new(None),
            shutdown_cleanup_stopped,
            shutdown_started: tokio::sync::OnceCell::new(),
            closed: AtomicBool::new(false),
            tasks: Mutex::new(Vec::new()),
            public_executor: Arc::new(TokioRequestExecutor {
                sender: callback_sender.clone(),
                state: Arc::new(TokioExecutorState {
                    closed: AtomicBool::new(false),
                    admission: Mutex::new(()),
                }),
            }),
            runtime: Mutex::new(Some(tokio::runtime::Handle::current())),
            security: security.clone(),
            use_tls: AtomicBool::new(use_tls),
            max_frame_size_override: Some(max_frame_size),
            connect_timeout_millis: AtomicI32::new(3000),
            active_connect_timeout_millis: AtomicI32::new(3000),
            proxy_connect_timeout_millis: Mutex::new(HashMap::new()),
            close_socket_if_timeout: AtomicBool::new(true),
            enable_reconnect_for_go_away: AtomicBool::new(true),
            disable_callback_executor: AtomicBool::new(false),
            client_worker_threads: AtomicI32::new(4),
            disable_netty_worker_group: AtomicBool::new(false),
            client_channel_max_idle_time_seconds: AtomicI32::new(120),
            client_socket_send_buffer_size: AtomicI32::new(config.client_socket_send_buffer_size),
            client_socket_receive_buffer_size: AtomicI32::new(
                config.client_socket_receive_buffer_size,
            ),
            write_buffer_high_water_mark: AtomicI32::new(config.write_buffer_high_water_mark),
            write_buffer_low_water_mark: AtomicI32::new(config.write_buffer_low_water_mark),
            pooled_byte_buffer_allocator_enabled: AtomicBool::new(
                config.pooled_byte_buffer_allocator_enabled,
            ),
            active_connection_settings: Mutex::new(None),
            proxy_connection_settings: Mutex::new(HashMap::new()),
            scan_available_nameserver: AtomicBool::new(true),
            config,
            socks_proxy_routes: config::ProxyRoutes::default(),
            startup: Mutex::new(None),
            started: AtomicBool::new(true),
            async_admission: Arc::new(tokio::sync::Semaphore::new(65535)),
            oneway_admission: Arc::new(tokio::sync::Semaphore::new(65535)),
            callback_sender,
            callback_executor: Mutex::new(None),
            rpc_hooks: Mutex::new(Vec::new()),
            channel_listener: None,
            channel_event_sender,
            request_pipeline: Mutex::new(None),
            metrics_manager: RwLock::new(None),
            nameservers: Mutex::new(None),
            available_nameservers: Mutex::new(HashSet::new()),
            chosen_nameserver: Mutex::new(None),
            nameserver_index: AtomicU64::new(0),
        });
        let processor_tasks = processor::start(
            Arc::downgrade(&inner),
            inbound_rx,
            usize::try_from(inner.config.worker_threads).expect("validated worker count"),
        );
        for task in processor_tasks {
            inner.spawn(task);
        }
        let scheduler = tokio::spawn(timeout_scheduler(
            Arc::downgrade(&inner),
            inner.shutdown.subscribe(),
        ));
        inner.spawn(scheduler);
        Self { inner }
    }

    /// Starts the client's background workers. Repeated calls are harmless.
    pub fn start(&self) -> Result<(), TransportError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        let mut startup = self.inner.startup.lock().unwrap();
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        if self.inner.started.load(Ordering::Acquire) {
            return Ok(());
        }
        let worker_threads = self.inner.client_worker_threads.load(Ordering::Acquire);
        if worker_threads <= 0 {
            return Err(TransportError::InvalidConfiguration(
                "client worker thread count must fit a positive Java signed integer".into(),
            ));
        }
        let worker_count = usize::try_from(worker_threads).map_err(|_| {
            TransportError::InvalidConfiguration(
                "client worker thread count must fit the platform thread count".into(),
            )
        })?;
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            TransportError::InvalidConfiguration("start requires an active Tokio runtime".into())
        })?;
        let connection_settings = self.inner.capture_direct_connection_settings()?;
        self.inner.active_connect_timeout_millis.store(
            self.inner.connect_timeout_millis.load(Ordering::Acquire),
            Ordering::Release,
        );
        *self.inner.active_connection_settings.lock().unwrap() = Some(connection_settings);
        *self.inner.runtime.lock().unwrap() = Some(runtime);
        let startup_state = startup.take().ok_or_else(|| {
            TransportError::InvalidConfiguration("client startup state is unavailable".into())
        })?;
        for task in processor::start(
            Arc::downgrade(&self.inner),
            startup_state.inbound_receiver,
            worker_count,
        ) {
            self.inner.spawn(task);
        }
        *self.inner.channel_event_worker.lock().unwrap() = Some(start_channel_event_worker(
            self.inner.channel_listener.clone(),
            startup_state.channel_event_receiver,
            self.inner.shutdown.subscribe(),
            self.inner.channel_event_stopped.clone(),
        ));
        let callback_workers = callback_worker_limit(self.inner.config.callback_executor_threads);
        *self.inner.callback_workers.lock().unwrap() = start_callback_workers(
            startup_state.callback_receiver,
            self.inner.callback_shutdown.subscribe(),
            callback_workers,
        );
        self.inner.spawn(tokio::spawn(timeout_scheduler(
            Arc::downgrade(&self.inner),
            self.inner.shutdown.subscribe(),
        )));
        if self.inner.scan_available_nameserver.load(Ordering::Acquire) {
            self.inner.spawn(tokio::spawn(scan_available_name_servers(
                Arc::downgrade(&self.inner),
                self.inner.shutdown.subscribe(),
            )));
        }
        self.inner.started.store(true, Ordering::Release);
        Ok(())
    }

    /// Replaces the configured NameServer list and randomizes its order when changed.
    pub fn update_name_server_address_list(&self, mut addresses: Vec<String>) {
        if addresses.is_empty() {
            return;
        }
        let mut nameservers = self.inner.nameservers.lock().unwrap();
        if nameservers.as_ref().is_some_and(|configured| {
            configured.len() == addresses.len()
                && addresses.iter().all(|address| configured.contains(address))
        }) {
            return;
        }
        let removed = nameservers
            .as_ref()
            .into_iter()
            .flatten()
            .filter(|address| !addresses.contains(address))
            .cloned()
            .collect::<HashSet<_>>();
        addresses.shuffle(&mut rand::rng());
        *nameservers = Some(addresses);
        let removed_chosen = {
            let mut chosen = self.inner.chosen_nameserver.lock().unwrap();
            if chosen
                .as_ref()
                .is_some_and(|address| removed.contains(address))
            {
                chosen.take()
            } else {
                None
            }
        };
        if let Some(address) = removed_chosen {
            self.inner.connection_closed_for_address(&address);
        }
    }

    pub fn get_name_server_address_list(&self) -> Option<Vec<String>> {
        self.inner.nameservers.lock().unwrap().clone()
    }

    /// Provides lock-protected mutable access to the live nullable NameServer list.
    ///
    /// Mutations are visible to later selection and availability scans. The closure receives
    /// `None` before any non-empty update, matching Java's nullable live-list getter; clearing
    /// an existing list preserves the configured-but-empty state. Do not call this client again
    /// from inside the closure.
    pub fn with_name_server_address_list<R>(
        &self,
        operation: impl FnOnce(Option<&mut Vec<String>>) -> R,
    ) -> R {
        let mut nameservers = self.inner.nameservers.lock().unwrap();
        operation(nameservers.as_mut())
    }

    pub fn get_available_name_srv_list(&self) -> Vec<String> {
        self.inner
            .available_nameservers
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect()
    }

    /// Selects the next configured NameServer. A successful invoke marks it available;
    /// connection failures remove it from the available set.
    pub async fn invoke_name_server(
        &self,
        command: RemotingCommand,
        deadline: Instant,
    ) -> Result<RemotingCommand, TransportError> {
        self.invoke_name_server_with_peer(command, deadline)
            .await
            .map(|(response, _)| response)
    }

    /// Invokes a NameServer and returns the address that supplied the response.
    async fn invoke_name_server_with_peer(
        &self,
        command: RemotingCommand,
        deadline: Instant,
    ) -> Result<(RemotingCommand, String), TransportError> {
        self.invoke_name_server_mode(command, deadline, false).await
    }

    /// Java invokeSync(NameServer): retain the original absolute wait deadline.
    pub async fn invoke_name_server_sync_with_peer(
        &self,
        command: RemotingCommand,
        deadline: Instant,
    ) -> Result<(RemotingCommand, String), TransportError> {
        self.invoke_name_server_mode(command, deadline, true).await
    }

    async fn invoke_name_server_mode(
        &self,
        command: RemotingCommand,
        deadline: Instant,
        sync: bool,
    ) -> Result<(RemotingCommand, String), TransportError> {
        if !self.inner.started.load(Ordering::Acquire) {
            return Err(TransportError::NotStarted);
        }
        let addresses = self.get_name_server_address_list().unwrap_or_default();
        if addresses.is_empty() {
            return Err(TransportError::InvalidConfiguration(
                "no NameServer addresses configured".into(),
            ));
        }
        if Instant::now() >= deadline {
            return Err(TransportError::Timeout);
        }
        let chosen = self.inner.chosen_nameserver.lock().unwrap().clone();
        let start = chosen
            .as_ref()
            .and_then(|address| addresses.iter().position(|candidate| candidate == address))
            .unwrap_or_else(|| {
                self.inner
                    .nameserver_index
                    .fetch_add(1, Ordering::Relaxed)
                    .wrapping_add(1) as usize
                    % addresses.len()
            });
        let mut last_error = None;
        for offset in 0..addresses.len() {
            if Instant::now() >= deadline {
                return Err(TransportError::Timeout);
            }
            let address = &addresses[(start + offset) % addresses.len()];
            let address_is_configured = self
                .inner
                .nameservers
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|configured| configured.iter().any(|entry| entry == address));
            if !address_is_configured {
                continue;
            }
            let connection = match self.inner.connection(address, deadline).await {
                Ok(connection) => connection,
                Err(error) => {
                    let nameservers = self.inner.nameservers.lock().unwrap();
                    let replacement_exists = self
                        .inner
                        .connections
                        .lock()
                        .unwrap()
                        .get(address)
                        .is_some_and(|current| !current.is_closed());
                    if nameservers
                        .as_ref()
                        .is_some_and(|configured| configured.iter().any(|entry| entry == address))
                        && !replacement_exists
                    {
                        self.inner
                            .available_nameservers
                            .lock()
                            .unwrap()
                            .remove(address);
                    }
                    last_error = Some(error);
                    continue;
                }
            };
            let selected = {
                let nameservers = self.inner.nameservers.lock().unwrap();
                if nameservers
                    .as_ref()
                    .is_some_and(|configured| configured.iter().any(|entry| entry == address))
                {
                    *self.inner.chosen_nameserver.lock().unwrap() = Some(address.clone());
                    true
                } else {
                    false
                }
            };
            if !selected {
                continue;
            }
            let result = if sync {
                self.invoke_sync_with_deadline(address, command, deadline)
                    .await
            } else {
                self.invoke(address, command, deadline).await
            };
            match result {
                Ok(response) => {
                    let nameservers = self.inner.nameservers.lock().unwrap();
                    let connection_is_current = nameservers
                        .as_ref()
                        .is_some_and(|configured| configured.iter().any(|entry| entry == address))
                        && !connection.is_closed()
                        && self
                            .inner
                            .connections
                            .lock()
                            .unwrap()
                            .get(address)
                            .is_some_and(|current| current.generation == connection.generation);
                    if connection_is_current {
                        self.inner
                            .available_nameservers
                            .lock()
                            .unwrap()
                            .insert(address.clone());
                    }
                    drop(nameservers);
                    return Ok((response, address.clone()));
                }
                Err(error) => {
                    let nameservers = self.inner.nameservers.lock().unwrap();
                    let connection_is_current = nameservers
                        .as_ref()
                        .is_some_and(|configured| configured.iter().any(|entry| entry == address))
                        && !connection.is_closed()
                        && self
                            .inner
                            .connections
                            .lock()
                            .unwrap()
                            .get(address)
                            .is_some_and(|current| current.generation == connection.generation);
                    if connection_is_current {
                        self.inner
                            .available_nameservers
                            .lock()
                            .unwrap()
                            .remove(address);
                        if matches!(
                            error,
                            TransportError::Connect(_)
                                | TransportError::Send(_)
                                | TransportError::Closed
                                | TransportError::Tls(_)
                        ) {
                            let mut chosen = self.inner.chosen_nameserver.lock().unwrap();
                            if chosen.as_deref() == Some(address.as_str()) {
                                *chosen = None;
                            }
                        }
                    }
                    drop(nameservers);
                    return Err(error);
                }
            }
        }
        Err(last_error.unwrap_or(TransportError::Timeout))
    }

    pub fn add_rpc_hook(&self, hook: Arc<dyn RpcHook>) {
        let mut hooks = self.inner.rpc_hooks.lock().unwrap();
        if !hooks.iter().any(|registered| {
            Arc::ptr_eq(registered, &hook)
                || registered
                    .equality_key()
                    .is_some_and(|key| hook.equality_key().as_ref() == Some(&key))
        }) {
            hooks.push(hook);
        }
    }

    /// Returns a snapshot of the registered hooks.
    pub fn get_rpc_hooks(&self) -> Vec<Arc<dyn RpcHook>> {
        self.inner.rpc_hooks.lock().unwrap().clone()
    }

    /// Provides lock-protected access to the live RPC-hook list.
    pub fn with_rpc_hooks<R>(&self, operation: impl FnOnce(&mut Vec<Arc<dyn RpcHook>>) -> R) -> R {
        let mut hooks = self.inner.rpc_hooks.lock().unwrap();
        operation(&mut hooks)
    }

    /// Removes a registered hook by identity or its optional Java-equality key.
    pub fn remove_rpc_hook(&self, hook: &Arc<dyn RpcHook>) -> bool {
        let mut hooks = self.inner.rpc_hooks.lock().unwrap();
        let position = hooks.iter().position(|registered| {
            Arc::ptr_eq(registered, hook)
                || registered
                    .equality_key()
                    .is_some_and(|key| hook.equality_key().as_ref() == Some(&key))
        });
        position.is_some_and(|position| {
            hooks.remove(position);
            true
        })
    }

    pub fn clear_rpc_hooks(&self) {
        self.inner.rpc_hooks.lock().unwrap().clear();
    }

    pub fn get_channel_event_listener(&self) -> Option<Arc<dyn ChannelEventListener>> {
        self.inner.channel_listener.clone()
    }

    pub fn set_callback_executor(&self, executor: Option<Arc<dyn CallbackExecutor>>) {
        *self.inner.callback_executor.lock().unwrap() = Some(executor.unwrap_or_else(|| {
            Arc::new(ClientCallbackExecutor {
                sender: self.inner.callback_sender.clone(),
            })
        }));
    }

    pub fn get_callback_executor(&self) -> Option<Arc<dyn CallbackExecutor>> {
        if self.inner.disable_callback_executor.load(Ordering::Acquire) {
            None
        } else {
            self.inner.callback_executor.lock().unwrap().clone()
        }
    }

    pub fn set_request_pipeline(&self, pipeline: Option<Arc<dyn RequestPipeline>>) {
        *self.inner.request_pipeline.lock().unwrap() = pipeline;
    }

    /// Sets the optional per-client remoting metrics manager.
    pub fn set_remoting_metrics_manager(&self, manager: Option<Arc<RemotingMetricsManager>>) {
        *self.inner.metrics_manager.write().unwrap() = manager;
    }

    /// Returns this client's remoting metrics manager, when configured.
    pub fn get_remoting_metrics_manager(&self) -> Option<Arc<RemotingMetricsManager>> {
        self.inner.metrics_manager.read().unwrap().clone()
    }

    pub fn pipe_request_pipeline(&self, pipeline: Arc<dyn RequestPipeline>) {
        let mut existing = self.inner.request_pipeline.lock().unwrap();
        *existing = Some(match existing.take() {
            Some(existing) => Arc::new(PipedRequestPipeline {
                source: pipeline,
                existing,
            }),
            None => pipeline,
        });
    }

    /// Returns whether the connection to `addr` is below its configured write watermarks.
    /// Unknown addresses are considered writable, matching the Java client contract.
    pub fn is_channel_writable(&self, addr: &str) -> bool {
        if !self.inner.started.load(Ordering::Acquire) {
            return true;
        }
        let Ok(address) = connection_manager::Address::parse(addr) else {
            return true;
        };
        self.inner
            .connections
            .lock()
            .unwrap()
            .get(address.as_str())
            .filter(|connection| !connection.is_closed())
            .is_none_or(|connection| connection.is_writable())
    }

    /// Opens or reuses a connection and reports whether it is active before `deadline`.
    pub async fn is_address_reachable(&self, addr: &str, deadline: Instant) -> bool {
        self.inner.started.load(Ordering::Acquire)
            && self.inner.connection(addr, deadline).await.is_ok()
    }

    /// Closes any currently cached channels for the supplied addresses.
    pub fn close_channels(&self, addresses: &[String]) {
        for address in addresses {
            let key = connection_manager::Address::parse(address)
                .map(|address| address.as_str().to_owned())
                .unwrap_or_else(|_| address.clone());
            self.inner.interrupt_pull_requests(&key);
            self.inner.connection_closed_for_address(&key);
        }
    }

    pub fn invoke_async(
        &self,
        addr: impl Into<String>,
        mut command: RemotingCommand,
        deadline: Instant,
        callback: Arc<dyn InvokeCallback>,
    ) -> Result<(), TransportError> {
        if !self.inner.started.load(Ordering::Acquire) {
            return Err(TransportError::NotStarted);
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        let addr = addr.into();
        let started = Instant::now();
        let timeout_millis = remaining_timeout_millis(deadline, started);
        let callback_timing = Arc::new(Mutex::new(CallbackTiming {
            wrapper_timeout_millis: timeout_millis,
            response_begin: started,
            response_timeout_millis: timeout_millis,
        }));
        let client = self.clone();
        let callback_request_state = Arc::new(Mutex::new(command.clone()));
        let channel_established = Arc::new(AtomicBool::new(false));
        let callback_opaque = Arc::new(AtomicI32::new(command.opaque));
        let task = tokio::spawn(async move {
            let mut channel = None;
            let (result, permit) = match client.inner.connection(&addr, deadline).await {
                Ok(connection) => {
                    channel = Some(connection.channel_handle());
                    channel_established.store(true, Ordering::Release);
                    let wrapper_timeout_millis = remaining_timeout_millis(deadline, Instant::now());
                    callback_timing.lock().unwrap().wrapper_timeout_millis = wrapper_timeout_millis;
                    let hooks = client.inner.rpc_hooks.lock().unwrap().clone();
                    let hook_result = run_before_rpc_hooks(&hooks, &addr, &mut command);
                    *callback_request_state.lock().unwrap() = command.clone();
                    if let Err(error) = hook_result {
                        let future = ResponseFuture::from_result(
                            channel,
                            callback_request_state.lock().unwrap().clone(),
                            Err(rpc_hook_transport_error(error)),
                            wrapper_timeout_millis,
                            Instant::now(),
                            callback.clone(),
                            None,
                        );
                        client.inner.dispatch_callback(CallbackJob {
                            future,
                            channel_established: channel_established.clone(),
                        });
                        return;
                    }
                    match client
                        .inner
                        .acquire_admission(client.inner.async_admission.clone(), deadline)
                        .await
                    {
                        Ok(permit) => {
                            let mut admission = Some(permit);
                            let result = client
                                .invoke_inner(
                                    &addr,
                                    command,
                                    deadline,
                                    InvokeOptions {
                                        admitted: true,
                                        retried_go_away: false,
                                        run_rpc_hooks: true,
                                        close_socket_on_timeout: false,
                                        timeout_millis: std::time::Duration::ZERO,
                                    },
                                    Some(CallbackTracking {
                                        channel_established: channel_established.clone(),
                                        opaque: callback_opaque.clone(),
                                        request: callback_request_state.clone(),
                                        timing: callback_timing.clone(),
                                    }),
                                    &mut admission,
                                )
                                .await;
                            (result, admission)
                        }
                        Err(error) => {
                            let error = match error {
                                TransportError::TimeoutWithContext { phase, timeout, .. } => {
                                    TransportError::timeout(phase, Some(addr.clone()), timeout)
                                }
                                error => error,
                            };
                            (Err(error), None)
                        }
                    }
                }
                Err(error) => (Err(classify_connection_error(&addr, error)), None),
            };
            let (future_timeout_millis, future_begin) = {
                let timing = *callback_timing.lock().unwrap();
                if result.is_ok() {
                    (timing.response_timeout_millis, timing.response_begin)
                } else {
                    (timing.wrapper_timeout_millis, Instant::now())
                }
            };
            let future = ResponseFuture::from_result(
                channel,
                callback_request_state.lock().unwrap().clone(),
                result,
                future_timeout_millis,
                future_begin,
                callback,
                permit,
            );
            client.inner.dispatch_callback(CallbackJob {
                future,
                channel_established: channel_established.clone(),
            });
        });
        self.inner.spawn(task);
        Ok(())
    }

    /// Provides lock-protected access to the live processor table.
    pub fn with_processor_table<R>(
        &self,
        operation: impl FnOnce(&mut HashMap<i32, ProcessorRegistration>) -> R,
    ) -> R {
        let mut processors = self.inner.processors.lock().unwrap();
        operation(&mut processors)
    }

    pub fn register_processor(&self, code: i32, processor: Arc<dyn RequestProcessor>) {
        self.register_processor_with_executor(code, processor, None);
    }

    pub fn register_owned_processor(
        &self,
        code: i32,
        processor: Arc<dyn RequestProcessor>,
    ) -> ProcessorLease {
        self.register_owned_processor_with_executor(code, processor, None)
    }

    pub fn register_owned_processor_with_executor(
        &self,
        code: i32,
        processor: Arc<dyn RequestProcessor>,
        executor: Option<Arc<dyn RequestExecutor>>,
    ) -> ProcessorLease {
        let identity = Arc::new(());
        self.inner.processors.lock().unwrap().insert(
            code,
            ProcessorRegistration {
                processor,
                executor,
                identity: identity.clone(),
            },
        );
        ProcessorLease {
            inner: Arc::downgrade(&self.inner),
            code,
            identity,
        }
    }

    pub fn register_processor_with_executor(
        &self,
        code: i32,
        processor: Arc<dyn RequestProcessor>,
        executor: Option<Arc<dyn RequestExecutor>>,
    ) {
        self.inner.processors.lock().unwrap().insert(
            code,
            ProcessorRegistration {
                processor,
                executor,
                identity: Arc::new(()),
            },
        );
    }

    /// Registers the fallback processor used when no request-code match exists.
    pub fn register_default_processor(&self, processor: Option<Arc<dyn RequestProcessor>>) {
        self.register_default_processor_with_executor(processor, None);
    }

    pub fn register_default_processor_with_executor(
        &self,
        processor: Option<Arc<dyn RequestProcessor>>,
        executor: Option<Arc<dyn RequestExecutor>>,
    ) {
        *self.inner.default_processor.lock().unwrap() =
            processor.map(|processor| ProcessorRegistration {
                processor,
                executor,
                identity: Arc::new(()),
            });
    }

    /// Sends one command and completes with the matching opaque response.
    pub async fn invoke(
        &self,
        addr: &str,
        command: RemotingCommand,
        deadline: Instant,
    ) -> Result<RemotingCommand, TransportError> {
        let mut admission = None;
        self.invoke_inner(
            addr,
            command,
            deadline,
            InvokeOptions {
                admitted: false,
                retried_go_away: false,
                run_rpc_hooks: true,
                close_socket_on_timeout: false,
                timeout_millis: std::time::Duration::ZERO,
            },
            None,
            &mut admission,
        )
        .await
    }

    pub async fn invoke_sync(
        &self,
        addr: &str,
        command: RemotingCommand,
        timeout: std::time::Duration,
    ) -> Result<RemotingCommand, TransportError> {
        let timeout_millis = timeout.as_millis().min(i64::MAX as u128) as i64;
        self.invoke_sync_with_timeout_millis(addr, command, timeout_millis)
            .await
    }

    /// Absolute-deadline form of Java synchronous invocation for typed RPCs.
    pub async fn invoke_sync_with_deadline(
        &self,
        addr: &str,
        command: RemotingCommand,
        deadline: Instant,
    ) -> Result<RemotingCommand, TransportError> {
        let timeout = deadline.saturating_duration_since(Instant::now());
        self.invoke_sync_deadline_inner(addr, command, deadline, timeout)
            .await
    }

    /// Synchronous Java-compatible call with a signed millisecond timeout.
    ///
    /// Java's `NettyRemotingClient.invokeSync` acquires or creates the channel
    /// before it checks whether the request timeout has elapsed. This preserves
    /// that ordering for RocketMQ client methods which receive a signed timeout
    /// value directly. A non-positive timeout can therefore establish a channel,
    /// but it never sends a request frame.
    #[doc(hidden)]
    pub async fn invoke_sync_with_timeout_millis(
        &self,
        addr: &str,
        command: RemotingCommand,
        timeout_millis: i64,
    ) -> Result<RemotingCommand, TransportError> {
        if !self.inner.started.load(Ordering::Acquire) {
            return Err(TransportError::NotStarted);
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }

        let begin = Instant::now();
        self.inner
            .connection_for_scan(addr)
            .await
            .map_err(|error| match error {
                TransportError::Timeout => TransportError::ConnectRequest {
                    remote_addr: addr.to_owned(),
                    cause: None,
                },
                error => classify_connection_error(addr, error),
            })?;

        // Java subtracts whole elapsed milliseconds from its signed long
        // timeout after the channel future completes.
        let elapsed_millis = begin.elapsed().as_millis().min(i64::MAX as u128) as i64;
        let remaining_millis = timeout_millis.saturating_sub(elapsed_millis);
        if remaining_millis <= 0 {
            return Err(TransportError::timeout(
                crate::error::TimeoutPhase::Operation,
                Some(addr.to_owned()),
                (timeout_millis > 0)
                    .then(|| std::time::Duration::from_millis(timeout_millis as u64)),
            ));
        }

        let remaining = std::time::Duration::from_millis(remaining_millis as u64);
        self.invoke_sync_deadline_inner(addr, command, Instant::now() + remaining, remaining)
            .await
    }

    async fn invoke_sync_deadline_inner(
        &self,
        addr: &str,
        command: RemotingCommand,
        deadline: Instant,
        timeout: std::time::Duration,
    ) -> Result<RemotingCommand, TransportError> {
        let mut admission = None;
        self.invoke_inner(
            addr,
            command,
            deadline,
            InvokeOptions {
                admitted: false,
                retried_go_away: false,
                run_rpc_hooks: true,
                close_socket_on_timeout: true,
                timeout_millis: timeout,
            },
            None,
            &mut admission,
        )
        .await
    }

    async fn invoke_inner(
        &self,
        addr: &str,
        mut command: RemotingCommand,
        deadline: Instant,
        options: InvokeOptions,
        callback: Option<CallbackTracking>,
        admission: &mut Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<RemotingCommand, TransportError> {
        if !self.inner.started.load(Ordering::Acquire) {
            return Err(TransportError::NotStarted);
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        let timeout = (!options.timeout_millis.is_zero()).then_some(options.timeout_millis);
        if Instant::now() >= deadline {
            return Err(TransportError::TimeoutWithContext {
                phase: crate::error::TimeoutPhase::Operation,
                remote_addr: Some(addr.to_owned()),
                timeout,
                cause: None,
            });
        }
        let connection = self
            .inner
            .connection(addr, deadline)
            .await
            .map_err(|error| classify_connection_error(addr, error))
            .map_err(|error| match error {
                TransportError::Timeout => TransportError::TimeoutWithContext {
                    phase: crate::error::TimeoutPhase::Connection,
                    remote_addr: Some(addr.to_owned()),
                    timeout,
                    cause: None,
                },
                error => error,
            })?;
        if let Some(callback) = &callback {
            callback.channel_established.store(true, Ordering::Release);
        }
        let timeout_budget = deadline.saturating_duration_since(Instant::now());
        if Instant::now() >= deadline {
            return Err(TransportError::TimeoutWithContext {
                phase: crate::error::TimeoutPhase::Admission,
                remote_addr: Some(addr.to_owned()),
                timeout,
                cause: None,
            });
        }
        if options.run_rpc_hooks && !options.retried_go_away && !options.admitted {
            let hooks = self.inner.rpc_hooks.lock().unwrap().clone();
            run_before_rpc_hooks(&hooks, addr, &mut command).map_err(rpc_hook_transport_error)?;
        }
        let mut request_for_hooks = command.clone();
        if !options.admitted {
            match self
                .inner
                .acquire_admission(self.inner.async_admission.clone(), deadline)
                .await
            {
                Ok(permit) => *admission = Some(permit),
                Err(error) if error.is_timeout() || error == TransportError::TooManyRequests => {
                    let error = if error.is_timeout() {
                        TransportError::TimeoutWithContext {
                            phase: crate::error::TimeoutPhase::Admission,
                            remote_addr: Some(addr.to_owned()),
                            timeout,
                            cause: None,
                        }
                    } else {
                        error
                    };
                    if options.close_socket_on_timeout {
                        connection.close();
                        return Err(TransportError::SendRequest {
                            remote_addr: addr.to_owned(),
                            cause: Some(crate::error::ErrorSource::message(error.to_string())),
                        });
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
        if let Some(callback) = &callback {
            let response_begin = Instant::now();
            let mut timing = callback.timing.lock().unwrap();
            timing.response_begin = response_begin;
            timing.response_timeout_millis = remaining_timeout_millis(deadline, response_begin);
        }
        let pending_deadline = deadline + std::time::Duration::from_secs(1);
        let (opaque, receiver) = self.inner.insert_pending(
            connection.generation,
            pending_deadline,
            connection.address().to_owned(),
            command.code,
        )?;
        command.opaque = opaque;
        if let Some(callback) = &callback {
            callback.opaque.store(opaque, Ordering::Release);
            if !options.retried_go_away {
                callback.request.lock().unwrap().opaque = opaque;
            }
        }
        let mut pending =
            pending::PendingGuard::new(Arc::downgrade(&self.inner), opaque, connection.generation);
        let frame = connection
            .encode_command(&command, crate::codec::frame::JAVA_ENCODER_MAX_FRAME_SIZE)
            .map_err(|error| classify_write_error(connection.address(), error))?;
        connection
            .enqueue_without_deadline(frame)
            .map_err(|error| classify_write_error(connection.address(), error))?;
        let response_deadline = if options.close_socket_on_timeout {
            deadline
        } else {
            pending_deadline
        };
        match time::timeout_at(response_deadline, receiver).await {
            Ok(Ok(Ok(mut response))) => {
                pending.disarm();
                if response.code == rocketmq_classic_protocol::codes::response::GO_AWAY {
                    if self
                        .inner
                        .enable_reconnect_for_go_away
                        .load(Ordering::Acquire)
                        && !options.retried_go_away
                        && self.inner.retire_for_go_away(addr, connection.clone())
                    {
                        let retry_request = RemotingCommand {
                            code: request_for_hooks.code,
                            language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                            ext_fields: request_for_hooks.ext_fields.clone(),
                            body: request_for_hooks.body.clone(),
                            ..RemotingCommand::default()
                        };
                        admission.take();
                        let mut retry = Box::pin(self.invoke_inner(
                            addr,
                            retry_request.clone(),
                            deadline,
                            InvokeOptions {
                                admitted: false,
                                retried_go_away: true,
                                run_rpc_hooks: false,
                                close_socket_on_timeout: options.close_socket_on_timeout,
                                timeout_millis: options.timeout_millis,
                            },
                            callback.clone(),
                            admission,
                        ));
                        let mut result = retry.as_mut().await;
                        if let Ok(response) = &mut result {
                            let hooks = self.inner.rpc_hooks.lock().unwrap().clone();
                            run_after_rpc_hooks(&hooks, addr, &mut request_for_hooks, response)
                                .map_err(rpc_hook_transport_error)?;
                            if let Some(callback) = &callback {
                                let mut request = retry_request;
                                request.opaque = callback.opaque.load(Ordering::Acquire);
                                *callback.request.lock().unwrap() = request;
                            }
                        }
                        return result;
                    }
                    return Err(TransportError::SendRequest {
                        remote_addr: connection.address().to_owned(),
                        cause: Some(if options.retried_go_away {
                            crate::error::ErrorSource::new(crate::error::GoAwayCause {
                                remote_addr: connection.address().to_owned(),
                                retried: true,
                            })
                        } else {
                            crate::error::ErrorSource::new(crate::error::GoAwayCause {
                                remote_addr: connection.address().to_owned(),
                                retried: false,
                            })
                        }),
                    });
                }
                if options.run_rpc_hooks && !options.retried_go_away {
                    let hooks = self.inner.rpc_hooks.lock().unwrap().clone();
                    run_after_rpc_hooks(&hooks, addr, &mut request_for_hooks, &mut response)
                        .map_err(rpc_hook_transport_error)?;
                }
                Ok(response)
            }
            Ok(Ok(Err(error))) => {
                pending.disarm();
                Err(error)
            }
            Ok(Err(_)) => {
                pending.disarm();
                if Instant::now() >= deadline {
                    Err(TransportError::TimeoutWithContext {
                        phase: crate::error::TimeoutPhase::Response,
                        remote_addr: Some(addr.to_owned()),
                        timeout,
                        cause: None,
                    })
                } else {
                    Err(TransportError::Closed)
                }
            }
            Err(_) => {
                if options.close_socket_on_timeout {
                    let mut entries = self.inner.pending.lock().unwrap();
                    if let Some(entry) = entries
                        .get_mut(&opaque)
                        .filter(|entry| entry.generation == connection.generation)
                    {
                        entry.retained_admission = admission.take();
                    }
                } else {
                    self.inner.cancel_pending(opaque, connection.generation);
                }
                if options.close_socket_on_timeout
                    && self.inner.close_socket_if_timeout.load(Ordering::Acquire)
                    && (timeout_budget > std::time::Duration::from_millis(100)
                        || timeout_budget > options.timeout_millis / 4)
                {
                    connection.close();
                }
                pending.disarm();
                Err(TransportError::TimeoutWithContext {
                    phase: crate::error::TimeoutPhase::Response,
                    remote_addr: Some(addr.to_owned()),
                    timeout,
                    cause: None,
                })
            }
        }
    }

    /// Starts connection acquisition at call time and returns after the client-owned operation is
    /// scheduled. As in Java, the caller receives only call-boundary errors; it does not observe
    /// later connection, admission, or socket-write completion, and this call never waits for a
    /// Broker response.
    pub fn invoke_oneway(
        &self,
        addr: &str,
        mut command: RemotingCommand,
        deadline: Instant,
    ) -> Result<(), TransportError> {
        if !self.inner.started.load(Ordering::Acquire) {
            return Err(TransportError::NotStarted);
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        if Instant::now() >= deadline {
            return Err(TransportError::Timeout);
        }
        connection_manager::Address::parse(addr)?;
        let client = self.clone();
        let addr = addr.to_owned();
        let (connection_tx, connection_rx) = tokio::sync::oneshot::channel();
        let connection_client = client.clone();
        let connection_addr = addr.clone();
        let connection_task = async move {
            let result = connection_client
                .inner
                .connection(&connection_addr, deadline)
                .await;
            let _ = connection_tx.send(result);
        };
        if let Some(connection_task) = spawn_after_initial_poll(connection_task) {
            self.inner.spawn(connection_task);
        }
        let task = tokio::spawn(async move {
            let _ = async {
                let connection = connection_rx
                    .await
                    .map_err(|_| TransportError::Closed)?
                    .map_err(|error| classify_connection_error(&addr, error))?;
                let hooks = client.inner.rpc_hooks.lock().unwrap().clone();
                run_before_rpc_hooks(&hooks, &addr, &mut command)
                    .map_err(rpc_hook_transport_error)?;
                command.mark_oneway();
                let _admission = client
                    .inner
                    .acquire_admission(client.inner.oneway_admission.clone(), deadline)
                    .await
                    .map_err(|error| match error {
                        TransportError::TimeoutWithContext { phase, timeout, .. } => {
                            TransportError::timeout(phase, Some(addr.clone()), timeout)
                        }
                        error => error,
                    })?;
                let frame = connection
                    .encode_command(&command, crate::codec::frame::JAVA_ENCODER_MAX_FRAME_SIZE)?;
                let written = connection
                    .enqueue_without_deadline(frame)
                    .map_err(|error| classify_write_error(&addr, error))?;
                written
                    .await
                    .map_err(|_| TransportError::Closed)?
                    .map_err(|error| classify_write_error(&addr, error))
            }
            .await;
        });
        self.inner.spawn(task);
        Ok(())
    }

    /// Closes connections, fails pending calls, and waits for the channel event worker for up to
    /// 90 seconds. Other owned workers and callbacks continue draining in the background.
    pub async fn shutdown(&self) {
        self.shutdown_with_timeout(SHUTDOWN_TIMEOUT).await;
    }

    async fn shutdown_with_timeout(&self, timeout: std::time::Duration) {
        let deadline = Instant::now() + timeout;
        self.inner
            .shutdown_started
            .get_or_init(|| async {
                {
                    let mut startup = self.inner.startup.lock().unwrap();
                    self.inner.shutdown();
                    startup.take();
                }
                let mut connections = {
                    let mut table = self.inner.connections.lock().unwrap();
                    table
                        .drain()
                        .map(|(_, connection)| connection)
                        .collect::<Vec<_>>()
                };
                connections.extend(
                    self.inner
                        .retired_connections
                        .lock()
                        .unwrap()
                        .drain()
                        .map(|(_, (_, connection))| connection),
                );
                for connection in &connections {
                    connection.close();
                }
                self.inner.fail_all_pending();
                let tasks = std::mem::take(&mut *self.inner.tasks.lock().unwrap());
                let event_worker = self.inner.channel_event_worker.lock().unwrap().take();
                if event_worker.is_none() {
                    self.inner.channel_event_stopped.send_replace(true);
                }
                let callback_workers =
                    std::mem::take(&mut *self.inner.callback_workers.lock().unwrap());
                if let Some(runtime) = self.inner.runtime.lock().unwrap().clone() {
                    runtime.spawn(
                        ShutdownCleanup {
                            tasks,
                            connections,
                            event_worker,
                            channel_event_stopped: self.inner.channel_event_stopped.clone(),
                            shutdown_cleanup_stopped: self.inner.shutdown_cleanup_stopped.clone(),
                            callback_shutdown: self.inner.callback_shutdown.clone(),
                            callback_workers,
                            public_executor: self.inner.public_executor.clone(),
                            deadline,
                        }
                        .run(),
                    );
                } else {
                    drop((tasks, connections, event_worker, callback_workers));
                    self.inner.public_executor.state.shutdown();
                    self.inner.callback_shutdown.send_replace(true);
                    self.inner.shutdown_cleanup_stopped.send_replace(true);
                }
            })
            .await;

        let mut channel_event_stopped = self.inner.channel_event_stopped.subscribe();
        if !*channel_event_stopped.borrow() {
            let _ = tokio::time::timeout_at(deadline, channel_event_stopped.changed()).await;
        }
        self.inner.public_executor.state.shutdown();
    }
}

struct ShutdownCleanup {
    tasks: Vec<JoinHandle<()>>,
    connections: Vec<Arc<connection::Connection>>,
    event_worker: Option<JoinHandle<()>>,
    channel_event_stopped: tokio::sync::watch::Sender<bool>,
    shutdown_cleanup_stopped: tokio::sync::watch::Sender<bool>,
    callback_shutdown: tokio::sync::watch::Sender<bool>,
    callback_workers: Vec<JoinHandle<()>>,
    public_executor: Arc<TokioRequestExecutor>,
    deadline: Instant,
}

impl ShutdownCleanup {
    async fn run(self) {
        let Self {
            tasks,
            connections,
            event_worker,
            channel_event_stopped,
            shutdown_cleanup_stopped,
            callback_shutdown,
            callback_workers,
            public_executor,
            deadline,
        } = self;
        let transport_cleanup = async move {
            let mut tasks = tasks;
            let mut timed_out = false;
            for task in &mut tasks {
                if tokio::time::timeout_at(deadline, task).await.is_err() {
                    timed_out = true;
                    break;
                }
            }
            if timed_out {
                for task in &tasks {
                    task.abort();
                }
                for connection in &connections {
                    connection.abort_task().await;
                }
            } else {
                for connection in &connections {
                    if !connection.join_until(deadline).await {
                        timed_out = true;
                        break;
                    }
                }
                if timed_out {
                    for connection in &connections {
                        connection.abort_task().await;
                    }
                }
            }
        };
        let event_worker_cleanup = async move {
            let mut stopped = channel_event_stopped.subscribe();
            if !*stopped.borrow() {
                let _ = tokio::time::timeout_at(deadline, stopped.changed()).await;
            }
            if let Some(worker) = event_worker {
                if *stopped.borrow() {
                    let _ = worker.await;
                }
            }
            public_executor.state.shutdown();
        };
        tokio::join!(transport_cleanup, event_worker_cleanup);

        callback_shutdown.send_replace(true);
        for worker in callback_workers {
            let _ = worker.await;
        }
        shutdown_cleanup_stopped.send_replace(true);
    }
}

impl ClientInner {
    async fn acquire_admission(
        &self,
        semaphore: Arc<tokio::sync::Semaphore>,
        deadline: Instant,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, TransportError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        if let Ok(permit) = semaphore.clone().try_acquire_owned() {
            return Ok(permit);
        }
        if Instant::now() >= deadline {
            return Err(TransportError::TooManyRequests);
        }
        let mut shutdown = self.shutdown.subscribe();
        tokio::select! {
            permit = time::timeout_at(deadline, semaphore.acquire_owned()) => permit
                .map_err(|_| TransportError::timeout(
                    crate::error::TimeoutPhase::Admission,
                    None,
                    Some(deadline.saturating_duration_since(Instant::now())),
                ))?
                .map_err(|_| TransportError::Shutdown),
            changed = shutdown.changed() => {
                let _ = changed;
                Err(TransportError::Shutdown)
            }
        }
    }

    fn notify_channel(&self, event_type: NettyEventType, channel: ChannelHandle) {
        if self.channel_listener.is_some() {
            let _ = self.channel_event_sender.try_send(ChannelEvent {
                remote_addr: channel.remote_addr().to_owned(),
                event_type,
                channel,
            });
        }
    }

    fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        let _ = self.shutdown.send(true);
    }

    fn insert_pending(
        &self,
        generation: u64,
        deadline: Instant,
        address: String,
        request_code: i32,
    ) -> Result<
        (
            i32,
            oneshot::Receiver<Result<RemotingCommand, TransportError>>,
        ),
        TransportError,
    > {
        if deadline <= Instant::now() {
            return Err(TransportError::Timeout);
        }
        let (sender, receiver) = oneshot::channel();
        let mut pending = self.pending.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        for _ in 0..=pending.len() {
            let opaque = self.opaque.fetch_add(1, Ordering::Relaxed);
            if let std::collections::hash_map::Entry::Vacant(entry) = pending.entry(opaque) {
                entry.insert(PendingEntry {
                    generation,
                    deadline,
                    address,
                    request_code,
                    response: sender,
                    retained_admission: None,
                });
                return Ok((opaque, receiver));
            }
        }
        Err(TransportError::OpaqueExhausted)
    }

    fn cancel_pending(&self, opaque: i32, generation: u64) {
        self.finish_pending(opaque, generation, Err(TransportError::Closed));
    }

    fn interrupt_pull_requests(&self, address: &str) {
        let requests = self
            .pending
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(opaque, entry)| {
                (entry.address == address
                    && matches!(
                        entry.request_code,
                        rocketmq_classic_protocol::codes::request::PULL_MESSAGE
                            | rocketmq_classic_protocol::codes::request::LITE_PULL_MESSAGE
                    ))
                .then_some((*opaque, entry.generation))
            })
            .collect::<Vec<_>>();
        for (opaque, generation) in requests {
            self.finish_pending(opaque, generation, Err(TransportError::Interrupted));
        }
    }

    fn expire_pending(&self, opaque: i32, generation: u64) {
        self.finish_pending(opaque, generation, Err(TransportError::Timeout));
    }

    fn complete_response(&self, generation: u64, command: RemotingCommand) {
        self.finish_pending(command.opaque, generation, Ok(command));
    }

    fn finish_pending(
        &self,
        opaque: i32,
        generation: u64,
        result: Result<RemotingCommand, TransportError>,
    ) -> bool {
        let mut pending = self.pending.lock().unwrap();
        if pending
            .get(&opaque)
            .is_some_and(|entry| entry.generation == generation)
        {
            if let Some(entry) = pending.remove(&opaque) {
                let result = if entry.deadline <= Instant::now() && result.is_ok() {
                    Err(TransportError::Timeout)
                } else {
                    result
                };
                let _ = entry.response.send(result);
                return true;
            }
        }
        false
    }

    fn fail_generation(&self, generation: u64, error: TransportError) {
        let ids = {
            let pending = self.pending.lock().unwrap();
            pending
                .iter()
                .filter_map(|(id, item)| {
                    (item.generation == generation).then_some((*id, item.address.clone()))
                })
                .collect::<Vec<_>>()
        };
        for (id, address) in ids {
            let error = match &error {
                TransportError::Closed
                | TransportError::Send(_)
                | TransportError::Codec(_)
                | TransportError::MalformedFrame(_) => TransportError::SendRequest {
                    remote_addr: address,
                    cause: None,
                },
                _ => error.clone(),
            };
            self.finish_pending(id, generation, Err(error));
        }
    }

    fn fail_all_pending(&self) {
        let pending = self
            .pending
            .lock()
            .unwrap()
            .iter()
            .map(|(opaque, entry)| (*opaque, entry.generation))
            .collect::<Vec<_>>();
        for (opaque, generation) in pending {
            self.finish_pending(opaque, generation, Err(TransportError::Shutdown));
        }
    }

    fn dispatch_callback(&self, job: CallbackJob) {
        let task: CallbackTask = Box::new(move || run_callback(job));
        if self.disable_callback_executor.load(Ordering::Acquire) {
            task();
            return;
        }
        if let Some(executor) = self.callback_executor.lock().unwrap().clone() {
            let task = Arc::new(Mutex::new(Some(task)));
            let task_for_executor = task.clone();
            let dispatched: CallbackTask = Box::new(move || {
                if let Some(task) = task_for_executor.lock().unwrap().take() {
                    task();
                }
            });
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                executor.execute(dispatched)
            }));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => {
                    if let Some(task) = task.lock().unwrap().take() {
                        task();
                    }
                }
            }
            return;
        }
        if let Err(error) = self.callback_sender.send(task) {
            (error.0)();
        }
    }

    fn spawn(&self, task: JoinHandle<()>) {
        let mut tasks = self.tasks.lock().unwrap();
        tasks.retain(|task| !task.is_finished());
        if self.closed.load(Ordering::Acquire) {
            task.abort();
        } else {
            tasks.push(task);
        }
    }
}

struct NotifyWake(Arc<tokio::sync::Notify>);

impl std::task::Wake for NotifyWake {
    fn wake(self: Arc<Self>) {
        self.0.notify_one();
    }
}

fn spawn_after_initial_poll<F>(future: F) -> Option<JoinHandle<()>>
where
    F: Future<Output = ()> + Send + 'static,
{
    let mut future = Box::pin(future);
    let notify = Arc::new(tokio::sync::Notify::new());
    let waker = Waker::from(Arc::new(NotifyWake(notify.clone())));
    let mut context = Context::from_waker(&waker);
    match Pin::as_mut(&mut future).poll(&mut context) {
        Poll::Ready(()) => None,
        Poll::Pending => Some(tokio::spawn(async move {
            loop {
                let notified = notify.notified();
                tokio::pin!(notified);
                tokio::select! {
                    _ = &mut notified => {}
                    _ = &mut future => break,
                }
            }
        })),
    }
}

fn run_callback(job: CallbackJob) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        job.future
            .dispatch_async_callback(job.channel_established.load(Ordering::Acquire));
    }));
    job.future.release();
}

fn start_channel_event_worker(
    listener: Option<Arc<dyn ChannelEventListener>>,
    mut receiver: mpsc::Receiver<ChannelEvent>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    worker_stopped: tokio::sync::watch::Sender<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let event = tokio::select! {
                event = receiver.recv() => event,
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() { break }
                    continue;
                },
            };
            let Some(event) = event else {
                break;
            };
            let Some(listener) = &listener else { continue };
            let listener = listener.clone();
            let runtime = tokio::runtime::Handle::current();
            let callback = tokio::task::spawn_blocking(move || {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    listener.on_channel_event(&event)
                }));
                if matches!(event.event_type, NettyEventType::Active) {
                    let operation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        listener.on_active_async(&event.remote_addr, &event.channel)
                    }));
                    if let Ok(operation) = operation {
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            runtime.block_on(operation)
                        }));
                    }
                }
            });
            let _ = callback.await;
        }
        worker_stopped.send_replace(true);
    })
}

fn redact_peer_address(address: &str) -> String {
    let Some((scheme, remainder)) = address.split_once("://") else {
        return address.to_owned();
    };
    let authority_end = remainder.find(['/', '?', '#']).unwrap_or(remainder.len());
    let authority = &remainder[..authority_end];
    let Some((_, host)) = authority.rsplit_once('@') else {
        return address.to_owned();
    };
    let suffix = &remainder[authority_end..];
    format!("{scheme}://[REDACTED]@{host}{suffix}")
}

fn start_callback_workers(
    receiver: mpsc::UnboundedReceiver<CallbackTask>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    worker_count: usize,
) -> Vec<JoinHandle<()>> {
    // Keep the configured value as a concurrency limit, not an eager allocation
    // count; Java accepts i32::MAX core threads without prestarting them.
    let worker_count = worker_count.max(1);
    let worker = tokio::spawn(async move {
        let mut receiver = receiver;
        let mut shutdown = shutdown;
        let mut callbacks = JoinSet::new();
        let mut stopping = false;
        loop {
            if stopping {
                if callbacks.len() >= worker_count {
                    let _ = callbacks.join_next().await;
                    continue;
                }
                match receiver.try_recv() {
                    Ok(task) => {
                        callbacks.spawn_blocking(task);
                    }
                    Err(
                        mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected,
                    ) => {
                        if callbacks.join_next().await.is_none() {
                            return;
                        }
                    }
                }
                continue;
            }
            if callbacks.len() >= worker_count {
                tokio::select! {
                    biased;
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            stopping = true;
                        }
                    }
                    Some(_) = callbacks.join_next() => {}
                }
                continue;
            }
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        stopping = true;
                    }
                }
                Some(_) = callbacks.join_next(), if !callbacks.is_empty() => {}
                task = receiver.recv() => match task {
                    Some(task) => {
                        callbacks.spawn_blocking(task);
                    }
                    None => stopping = true,
                }
            }
        }
    });
    vec![worker]
}

fn name_server_scan_batch(configured: Vec<String>, available_slots: usize) -> Vec<String> {
    if configured.is_empty() || available_slots == 0 {
        return Vec::new();
    }
    // Java submits addresses in configured order to a bounded executor with
    // AbortPolicy. Once its 42 active/queued slots fill, the next submission
    // aborts that scan. Later ticks begin at the start of the configured list.
    configured.into_iter().take(available_slots).collect()
}

fn apply_name_server_scan_result(
    inner: &ClientInner,
    address: String,
    connection: Result<Arc<connection::Connection>, TransportError>,
) {
    if inner.closed.load(Ordering::Acquire) {
        return;
    }
    let address_is_configured = inner
        .nameservers
        .lock()
        .unwrap()
        .as_ref()
        .is_some_and(|configured| configured.iter().any(|entry| entry == &address));
    if !address_is_configured {
        return;
    }
    let current_generation = inner
        .connections
        .lock()
        .unwrap()
        .get(&address)
        .filter(|current| !current.is_closed())
        .map(|current| current.generation);
    let probe_generation =
        connection.map(|connection| (!connection.is_closed()).then_some(connection.generation));
    let nameservers = inner.nameservers.lock().unwrap();
    if nameservers
        .as_ref()
        .is_some_and(|configured| configured.iter().any(|entry| entry == &address))
    {
        match name_server_probe_availability(probe_generation, current_generation) {
            Some(true) => {
                inner.available_nameservers.lock().unwrap().insert(address);
            }
            Some(false) => {
                inner.available_nameservers.lock().unwrap().remove(&address);
            }
            None => {}
        }
    }
}

async fn scan_available_name_servers(
    inner: Weak<ClientInner>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let interval_duration = inner
        .upgrade()
        .map(|inner| inner.scan_interval())
        .unwrap_or_else(|| std::time::Duration::from_secs(3));
    let mut interval = time::interval(interval_duration);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut scans = tokio::task::JoinSet::new();
    let scan_workers = Arc::new(tokio::sync::Semaphore::new(10));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Join completed probes before measuring capacity. Java's
                // executor releases a worker/queue slot as soon as a task
                // completes, without waiting for a later scan tick.
                while let Some(result) = scans.try_join_next() {
                    if let Ok(Some((address, connection))) = result {
                        let Some(inner) = inner.upgrade() else { break };
                        apply_name_server_scan_result(&inner, address, connection);
                    }
                }
                let Some(inner) = inner.upgrade() else { break };
                let configured = inner
                    .nameservers
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_default();
                inner
                    .available_nameservers
                    .lock()
                    .unwrap()
                    .retain(|address| {
                        configured.contains(address)
                    });
                if configured.is_empty() {
                    continue;
                }
                let available_slots = NAME_SERVER_SCAN_MAX_IN_FLIGHT.saturating_sub(scans.len());
                let batch = name_server_scan_batch(configured, available_slots);
                for address in batch {
                    let inner = inner.clone();
                    let scan_workers = scan_workers.clone();
                    scans.spawn(async move {
                        let _permit = scan_workers.acquire_owned().await.ok()?;
                        let connection = inner.connection_for_scan(&address).await;
                        Some((address, connection))
                    });
                }
            }
            result = scans.join_next(), if !scans.is_empty() => {
                if let Some(Ok(Some((address, connection)))) = result {
                    if let Some(inner) = inner.upgrade() {
                        apply_name_server_scan_result(&inner, address, connection);
                    } else {
                        break;
                    }
                }
            }
            result = shutdown.changed() => if result.is_err() || *shutdown.borrow() { break },
        }
    }
    scans.shutdown().await;
}

fn name_server_probe_availability(
    probe_generation: Result<Option<u64>, TransportError>,
    current_generation: Option<u64>,
) -> Option<bool> {
    let Ok(probe_generation) = probe_generation else {
        return None;
    };
    match (probe_generation, current_generation) {
        (Some(probe), Some(current)) if probe == current => Some(true),
        (_, Some(_)) => None,
        (_, None) => Some(false),
    }
}

async fn timeout_scheduler(
    inner: Weak<ClientInner>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut interval = time::interval_at(
        Instant::now() + std::time::Duration::from_secs(3),
        std::time::Duration::from_secs(1),
    );
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let Some(inner) = inner.upgrade() else { return };
                let now = Instant::now();
                let expired = {
                    let pending = inner.pending.lock().unwrap();
                    pending.iter().filter_map(|(opaque, entry)| (entry.deadline <= now).then_some((*opaque, entry.generation))).collect::<Vec<_>>()
                };
                for (opaque, generation) in expired {
                    inner.expire_pending(opaque, generation);
                }
            }
            result = shutdown.changed() => if result.is_err() || *shutdown.borrow() { return },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::frame::DEFAULT_MAX_FRAME_SIZE;

    #[test]
    fn callback_executor_thread_settings_match_java_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();

        for case in fixture["callbackThreadsConstruction"].as_array().unwrap() {
            let configured = i32::try_from(case["input"].as_i64().unwrap()).unwrap();
            let expected = usize::try_from(case["effective"].as_i64().unwrap()).unwrap();
            assert_eq!(case["outcome"], "OK");
            assert_eq!(callback_worker_limit(configured), expected);

            let client = RemotingClient::with_config(ClientConfig {
                callback_executor_threads: configured,
                ..ClientConfig::default()
            })
            .unwrap();
            assert_eq!(client.inner.config.callback_executor_threads, configured);
        }
    }

    #[test]
    fn java_tls_property_file_is_reloaded_for_each_client_context() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/security/tls-settings-defaults.json"
        ))
        .unwrap();
        let expected_modes = fixture["propertyFileReload"]["observedTestModes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_bool().unwrap())
            .collect::<Vec<_>>();
        let expected_auth_server = fixture["propertyFileReload"]["observedAuthServer"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_bool().unwrap())
            .collect::<Vec<_>>();

        let directory = std::env::temp_dir().join(format!(
            "rocketmq-tls-context-reload-{}-{}",
            std::process::id(),
            Instant::now().elapsed().as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let properties_file = directory.join("tls.properties");
        std::fs::write(
            &properties_file,
            "tls.test.mode.enable=true\ntls.client.authServer=false\n",
        )
        .unwrap();
        let properties = HashMap::new();
        let mut tls = TlsConfig::from_java_properties(&properties, Some(&properties_file)).unwrap();
        // This test exercises property reload, not the machine's native CA
        // store. Supply a pinned local trust anchor so it is reproducible on
        // hosts that do not expose system trust roots.
        tls.ca_pem = Some(include_bytes!("../../tests/fixtures/tls/java-server.pem").to_vec());

        let mut observed_modes = Vec::new();
        let mut observed_auth_server = Vec::new();
        for (test_mode, auth_server) in [(true, Some(false)), (false, Some(true)), (true, None)] {
            let mut snapshot = format!("tls.test.mode.enable={test_mode}\n");
            if let Some(auth_server) = auth_server {
                snapshot.push_str(&format!("tls.client.authServer={auth_server}\n"));
            }
            std::fs::write(&properties_file, snapshot).unwrap();
            let client = RemotingClient::with_config(ClientConfig {
                transport: TransportMode::Tls(tls.clone()),
                ..ClientConfig::default()
            })
            .unwrap();
            let TransportMode::Tls(context_config) = &client.inner.config.transport else {
                unreachable!("TLS configuration should be retained by the remoting client");
            };
            observed_modes.push(context_config.test_mode);
            observed_auth_server.push(context_config.auth_server);
        }
        std::fs::remove_dir_all(directory).unwrap();
        assert_eq!(observed_modes, expected_modes);
        assert_eq!(observed_auth_server, expected_auth_server);
    }

    #[tokio::test]
    async fn java_frame_max_length_property_is_applied_to_new_connections() {
        const CHILD_MARKER: &str = "ROCKETMQ_FRAME_MAX_LENGTH_CHILD";
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/netty-frame-limit.json"
        ))
        .unwrap();
        let expected = fixture["configured"].as_u64().unwrap() as usize;

        if std::env::var_os(CHILD_MARKER).is_some() {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                stream
                    .write_all(&(expected as u32).to_be_bytes())
                    .await
                    .unwrap();
                let mut byte = [0];
                assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
            });
            let client = RemotingClient::with_config(ClientConfig::default()).unwrap();
            client.start().unwrap();
            let connection = client
                .inner
                .connection(&address, Instant::now() + std::time::Duration::from_secs(3))
                .await
                .unwrap();
            let oversized = RemotingCommand {
                body: Some(vec![0; expected]),
                ..RemotingCommand::default()
            };
            let outbound = connection
                .encode_command(&oversized, crate::codec::frame::JAVA_ENCODER_MAX_FRAME_SIZE)
                .unwrap();
            assert!(outbound.len() > expected);
            let inbound_rejected =
                tokio::time::timeout(std::time::Duration::from_secs(3), peer).await;
            assert!(
                inbound_rejected.is_ok(),
                "connection did not close after a frame exceeded the configured inbound limit"
            );
            inbound_rejected.unwrap().unwrap();
            assert!(connection.is_closed());
            connection.join().await;
            client.shutdown().await;
            return;
        }

        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("client::tests::java_frame_max_length_property_is_applied_to_new_connections")
            .arg("--nocapture")
            .env(config::JAVA_FRAME_MAX_LENGTH_PROPERTY, expected.to_string())
            .env(CHILD_MARKER, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child test failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn channel_event_peer_address_redacts_uri_userinfo() {
        assert_eq!(
            redact_peer_address("tcp://user:secret@broker.example:10911/path"),
            "tcp://[REDACTED]@broker.example:10911/path"
        );
        assert_eq!(
            redact_peer_address("[2001:db8::1]:10911"),
            "[2001:db8::1]:10911"
        );
    }

    #[test]
    fn netty_event_type_methods_match_invoked_java_enum() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        let cases = fixture["nettyEventTypeMethodCases"].as_array().unwrap();
        let values = NettyEventType::values();
        let expected_names = cases
            .iter()
            .find(|case| case["case"] == "constant")
            .unwrap()["values"]
            .as_array()
            .unwrap()
            .iter()
            .map(|name| name.as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            values.iter().map(|value| value.name()).collect::<Vec<_>>(),
            expected_names
        );
        let mut mutated_values = NettyEventType::values();
        mutated_values[0] = values[values.len() - 1];
        assert_eq!(NettyEventType::values()[0], values[0]);

        for case in cases {
            match case["case"].as_str().unwrap() {
                "constant" => {
                    let name = case["name"].as_str().unwrap();
                    let value = NettyEventType::value_of_name(Some(name)).unwrap();
                    let ordinal = case["ordinal"].as_i64().unwrap() as i32;
                    assert_eq!(value.name(), name, "{case}");
                    assert_eq!(value.to_string(), case["text"].as_str().unwrap(), "{case}");
                    assert_eq!(value.ordinal(), ordinal, "{case}");
                    assert_eq!(value, values[ordinal as usize], "{case}");
                    assert_eq!(case["valueOfName"], name, "{case}");
                    assert_eq!(case["valuesDefensiveCopy"], true, "{case}");
                    let next = values[(ordinal as usize + 1) % values.len()];
                    assert_eq!(
                        value.java_compare_to(next),
                        case["compareNext"].as_i64().unwrap() as i32,
                        "{case}"
                    );
                }
                "lookup" => match NettyEventType::value_of_name(case["input"].as_str()) {
                    Ok(value) => {
                        assert_eq!(case["outcome"], "OK", "{case}");
                        assert_eq!(case["decoded"], value.name(), "{case}");
                    }
                    Err(error) => {
                        let expected = if case["input"].is_null() {
                            "NullPointerException"
                        } else {
                            "IllegalArgumentException"
                        };
                        assert_eq!(case["outcome"], expected, "{case}");
                        assert_eq!(case["message"], error.to_string(), "{case}");
                    }
                },
                other => panic!("unknown Java NettyEventType case: {other}"),
            }
        }
    }

    #[test]
    fn mutable_hooks_are_ordered_and_legacy_hooks_observe_prior_mutation() {
        struct Mutator;
        impl RpcHook for Mutator {
            fn before_request_mut_result(
                &self,
                _: &str,
                request: &mut RemotingCommand,
            ) -> Result<(), RpcHookError> {
                request
                    .ext_fields
                    .get_or_insert_with(Default::default)
                    .insert("mutated".into(), "yes".into());
                Ok(())
            }
        }
        let observed = Arc::new(AtomicBool::new(false));
        let flag = observed.clone();
        let hooks: Vec<Arc<dyn RpcHook>> = vec![
            Arc::new(Mutator),
            Arc::new(move |_: &str, command: &RemotingCommand| {
                assert_eq!(command.ext_fields.as_ref().unwrap()["mutated"], "yes");
                flag.store(true, Ordering::Release);
            }),
        ];
        let mut command = RemotingCommand::default();
        run_before_rpc_hooks(&hooks, "peer", &mut command).unwrap();
        assert!(observed.load(Ordering::Acquire));
    }
    use tokio::sync::Notify;

    #[tokio::test]
    async fn async_active_listener_is_owned_panic_isolated_and_drained_after_shutdown() {
        struct Listener {
            calls: std::sync::atomic::AtomicUsize,
            live: Arc<std::sync::atomic::AtomicUsize>,
            entered: Notify,
            exited: Arc<Notify>,
            release: Arc<Notify>,
        }
        struct Live {
            live: Arc<std::sync::atomic::AtomicUsize>,
            exited: Arc<Notify>,
        }
        impl Drop for Live {
            fn drop(&mut self) {
                self.live.fetch_sub(1, Ordering::SeqCst);
                self.exited.notify_one();
            }
        }
        impl ChannelEventListener for Listener {
            fn on_active_async<'a>(
                &'a self,
                _: &'a str,
                _: &'a ChannelHandle,
            ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
                Box::pin(async move {
                    let call = self.calls.fetch_add(1, Ordering::SeqCst);
                    assert_ne!(call, 0, "scripted active listener panic");
                    self.live.fetch_add(1, Ordering::SeqCst);
                    let _live = Live {
                        live: self.live.clone(),
                        exited: self.exited.clone(),
                    };
                    self.entered.notify_one();
                    self.release.notified().await;
                })
            }
        }
        let release = Arc::new(Notify::new());
        let listener = Arc::new(Listener {
            calls: std::sync::atomic::AtomicUsize::new(0),
            live: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            entered: Notify::new(),
            exited: Arc::new(Notify::new()),
            release: release.clone(),
        });
        let (sender, receiver) = mpsc::channel(2);
        let (stop, shutdown) = tokio::sync::watch::channel(false);
        let (worker_stopped, _) = tokio::sync::watch::channel(false);
        let worker =
            start_channel_event_worker(Some(listener.clone()), receiver, shutdown, worker_stopped);
        sender
            .send(ChannelEvent {
                remote_addr: "broker".into(),
                event_type: NettyEventType::Active,
                channel: ChannelHandle::pending("broker"),
            })
            .await
            .unwrap();
        sender
            .send(ChannelEvent {
                remote_addr: "broker".into(),
                event_type: NettyEventType::Active,
                channel: ChannelHandle::pending("broker"),
            })
            .await
            .unwrap();
        listener.entered.notified().await;
        assert_eq!(listener.calls.load(Ordering::SeqCst), 2);
        assert_eq!(listener.live.load(Ordering::SeqCst), 1);
        stop.send_replace(true);
        let mut worker = worker;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), &mut worker)
                .await
                .is_err()
        );
        release.notify_one();
        worker.await.unwrap();
        if listener.live.load(Ordering::SeqCst) != 0 {
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                listener.exited.notified(),
            )
            .await
            .expect("active listener future did not finish after release");
        }
        assert_eq!(listener.live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn owned_processor_cleanup_preserves_replacement_even_for_same_handle() {
        let client = RemotingClient::new(TransportMode::Plain);
        let processor: Arc<dyn RequestProcessor> = Arc::new(|_: RemotingCommand| Ok(None));
        let old = client.register_owned_processor(39, processor.clone());
        let replacement = client.register_owned_processor(39, processor.clone());
        drop(old);
        assert!(client.with_processor_table(|table| table.contains_key(&39)));
        drop(replacement);
        assert!(!client.with_processor_table(|table| table.contains_key(&39)));
        let lease = client.register_owned_processor(39, processor.clone());
        client.register_processor(39, processor);
        drop(lease);
        assert!(client.with_processor_table(|table| table.contains_key(&39)));
    }

    #[test]
    fn name_server_scan_batch_matches_java_abort_policy_submission_order() {
        let addresses: Vec<_> = (0..50).map(|index| format!("address-{index}")).collect();
        let first = name_server_scan_batch(addresses.clone(), 42);
        assert_eq!(first.len(), 42);
        assert_eq!(first.first().unwrap(), "address-0");
        assert_eq!(first.last().unwrap(), "address-41");

        // The rejected submission aborts that Java scan loop. A later tick
        // begins at the first configured address instead of rotating skipped
        // entries ahead of previously submitted ones.
        let second = name_server_scan_batch(addresses.clone(), 5);
        assert_eq!(second, addresses[..5]);
        assert!(!second.contains(&"address-42".to_owned()));
        assert!(name_server_scan_batch(addresses.clone(), 0).is_empty());
        assert_eq!(name_server_scan_batch(addresses, 42)[0], "address-0");
    }

    use std::sync::atomic::AtomicUsize;

    fn test_command() -> RemotingCommand {
        RemotingCommand::default()
    }

    fn install_test_connection(
        client: &RemotingClient,
        address: &str,
        stream: connection::BoxedStream,
        generation: u64,
    ) -> Arc<connection::Connection> {
        let connection = connection::Connection::spawn(
            stream,
            generation,
            Arc::downgrade(&client.inner),
            client.inner.frame_max_length().unwrap(),
            (64 * 1024, 32 * 1024),
            address.to_owned(),
            false,
        );
        client
            .inner
            .connections
            .lock()
            .unwrap()
            .insert(address.to_owned(), connection.clone());
        connection
    }

    #[tokio::test]
    async fn sync_response_is_read_while_request_write_is_pending() {
        use tokio::io::{AsyncWriteExt, duplex};

        let java_fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/hooks.json")).unwrap();
        assert_eq!(
            java_fixture["outboundWriteFlows"]["syncReturnedBeforeWriteCompletion"],
            true
        );
        let address = "127.0.0.1:10911";
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let (client_stream, mut peer_stream) = duplex(1);
        let connection = install_test_connection(&client, address, Box::new(client_stream), 9901);
        let request = RemotingCommand {
            code: 1,
            body: Some(vec![0; 1024 * 1024]),
            ..RemotingCommand::default()
        };
        let invoke_client = client.clone();
        let mut invoke = tokio::spawn(async move {
            invoke_client
                .invoke_sync_with_deadline(
                    address,
                    request,
                    Instant::now() + std::time::Duration::from_secs(2),
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while client.is_channel_writable(address) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("large request did not stall the tiny socket writer");
        assert!(connection.queued_write_bytes() > 0);

        let mut response = RemotingCommand {
            opaque: 0,
            ..RemotingCommand::default()
        };
        response.mark_response();
        peer_stream
            .write_all(&crate::codec::frame::encode(&response).unwrap())
            .await
            .unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_millis(250), &mut invoke)
            .await
            .expect("response handling waited for the blocked outbound write")
            .unwrap()
            .unwrap();
        assert!(result.is_response());
        assert!(
            connection.queued_write_bytes() > 0,
            "the response must complete before the queued request finishes writing"
        );
        client.shutdown().await;
    }

    #[tokio::test]
    async fn oneway_write_remains_queued_after_the_call_deadline() {
        use bytes::BytesMut;
        use tokio::io::{AsyncReadExt, duplex};

        let java_fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/hooks.json")).unwrap();
        let java_write = &java_fixture["outboundWriteFlows"];
        assert_eq!(java_write["onewayReturnedBeforeWriteCompletion"], true);
        assert_eq!(java_write["onewayPermitsWhileWritePending"], 0);
        assert_eq!(java_write["onewayPermitsAfterWriteCompletion"], 1);
        let address = "127.0.0.1:10912";
        let client = RemotingClient::with_config(ClientConfig {
            oneway_semaphore_value: 1,
            ..ClientConfig::default()
        })
        .unwrap();
        client.start().unwrap();
        const BODY_LEN: usize = 256 * 1024;
        let (client_stream, mut peer_stream) = duplex(4096);
        let connection = install_test_connection(&client, address, Box::new(client_stream), 9902);
        let deadline = Instant::now() + std::time::Duration::from_millis(200);
        client
            .invoke_oneway(
                address,
                RemotingCommand {
                    code: 2,
                    body: Some(vec![0; BODY_LEN]),
                    ..RemotingCommand::default()
                },
                deadline,
            )
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while connection.queued_write_bytes() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("one-way request was not enqueued");
        assert_eq!(client.inner.oneway_admission.available_permits(), 0);
        tokio::time::sleep_until(deadline + std::time::Duration::from_millis(50)).await;

        let sent = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let mut prefix = [0; 4];
            peer_stream.read_exact(&mut prefix).await.unwrap();
            let frame_length = u32::from_be_bytes(prefix) as usize;
            let mut framed = BytesMut::with_capacity(frame_length + 4);
            framed.extend_from_slice(&prefix);
            framed.resize(frame_length + 4, 0);
            peer_stream.read_exact(&mut framed[4..]).await.unwrap();
            crate::codec::frame::decode_next(&mut framed)
                .unwrap()
                .unwrap()
        })
        .await
        .expect("queued one-way frame did not finish after the peer drained it");
        assert!(sent.is_oneway());
        assert_eq!(sent.body.as_ref().unwrap().len(), BODY_LEN);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while client.inner.oneway_admission.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("one-way permit was not released when the queued write completed");
        client.shutdown().await;
    }

    #[tokio::test]
    async fn sync_wait_timeout_retains_pending_admission_until_terminal_cleanup() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for late_reply in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            let (seen_tx, seen_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut len = [0; 4];
                stream.read_exact(&mut len).await.unwrap();
                let mut buffer = bytes::BytesMut::from(&len[..]);
                buffer.resize(u32::from_be_bytes(len) as usize + 4, 0);
                stream.read_exact(&mut buffer[4..]).await.unwrap();
                let request = crate::codec::frame::decode_next(&mut buffer)
                    .unwrap()
                    .unwrap();
                seen_tx.send(request.opaque).unwrap();
                release_rx.await.unwrap();
                if late_reply {
                    let mut reply = test_command();
                    reply.opaque = request.opaque;
                    reply.mark_response();
                    stream
                        .write_all(&crate::codec::frame::encode(&reply).unwrap())
                        .await
                        .unwrap();
                }
            });
            let client = RemotingClient::with_config(ClientConfig {
                close_socket_if_timeout: false,
                async_semaphore_value: 1,
                ..Default::default()
            })
            .unwrap();
            client.start().unwrap();
            let result = client
                .invoke_sync(&addr, test_command(), std::time::Duration::from_millis(100))
                .await;
            assert!(result.unwrap_err().is_timeout());
            let opaque = seen_rx.await.unwrap();
            let generation = {
                let pending = client.inner.pending.lock().unwrap();
                let entry = pending
                    .get(&opaque)
                    .expect("sync timeout must retain pending entry");
                assert!(entry.deadline > Instant::now());
                entry.generation
            };
            assert_eq!(client.inner.async_admission.available_permits(), 0);
            if !late_reply {
                client.inner.expire_pending(opaque, generation);
            }
            release_tx.send(()).unwrap();
            peer.await.unwrap();
            time::timeout(std::time::Duration::from_secs(1), async {
                while !client.inner.pending.lock().unwrap().is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(client.inner.async_admission.available_permits(), 1);
            client.shutdown().await;
            assert!(client.inner.pending.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn oneway_never_registers_pending_even_when_peer_replies() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut frame = bytes::BytesMut::from(&length[..]);
            frame.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut frame[4..]).await.unwrap();
            let request = crate::codec::frame::decode_next(&mut frame)
                .unwrap()
                .unwrap();
            assert!(request.is_oneway());
            seen_tx.send(request.opaque).unwrap();
            reply_rx.await.unwrap();
            let mut response = RemotingCommand {
                opaque: request.opaque,
                ..RemotingCommand::default()
            };
            response.mark_response();
            stream
                .write_all(&crate::codec::frame::encode(&response).unwrap())
                .await
                .unwrap();
        });

        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        client
            .invoke_oneway(
                &address,
                test_command(),
                Instant::now() + std::time::Duration::from_secs(2),
            )
            .unwrap();
        let opaque = seen_rx.await.unwrap();
        assert!(!client.inner.pending.lock().unwrap().contains_key(&opaque));
        assert!(client.inner.pending.lock().unwrap().is_empty());
        reply_tx.send(()).unwrap();
        peer.await.unwrap();
        assert!(client.inner.pending.lock().unwrap().is_empty());
        client.shutdown().await;
    }

    #[tokio::test]
    async fn late_oneway_response_does_not_complete_concurrent_pending_request() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        async fn read_request(stream: &mut tokio::net::TcpStream) -> RemotingCommand {
            let mut length = [0u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut frame = bytes::BytesMut::from(&length[..]);
            frame.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut frame[4..]).await.unwrap();
            crate::codec::frame::decode_next(&mut frame)
                .unwrap()
                .unwrap()
        }

        async fn reply(stream: &mut tokio::net::TcpStream, opaque: i32) {
            let mut response = RemotingCommand {
                opaque,
                ..RemotingCommand::default()
            };
            response.mark_response();
            stream
                .write_all(&crate::codec::frame::encode(&response).unwrap())
                .await
                .unwrap();
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (pending_tx, pending_rx) = oneshot::channel();
        let (oneway_tx, oneway_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let oneway = read_request(&mut stream).await;
            assert!(oneway.is_oneway());
            oneway_tx.send(()).unwrap();
            let pending = read_request(&mut stream).await;
            assert!(!pending.is_oneway());
            assert_ne!(oneway.opaque, pending.opaque);
            pending_tx.send(pending.opaque).unwrap();
            release_rx.await.unwrap();
            reply(&mut stream, oneway.opaque).await;
            let probe = read_request(&mut stream).await;
            reply(&mut stream, probe.opaque).await;
            // The probe reply acknowledges that the reader passed the late oneway reply.
            (pending.opaque, stream)
        });

        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let mut oneway = test_command();
        oneway.opaque = 777;
        client
            .invoke_oneway(
                &address,
                oneway,
                Instant::now() + std::time::Duration::from_secs(10),
            )
            .unwrap();
        oneway_rx.await.unwrap();
        let invoke_client = client.clone();
        let invoke_address = address.clone();
        let pending = tokio::spawn(async move {
            invoke_client
                .invoke(
                    &invoke_address,
                    test_command(),
                    Instant::now() + std::time::Duration::from_secs(10),
                )
                .await
        });
        let opaque = pending_rx.await.unwrap();
        assert!(client.inner.pending.lock().unwrap().contains_key(&opaque));
        release_tx.send(()).unwrap();
        let probe = client
            .invoke(
                &address,
                test_command(),
                Instant::now() + std::time::Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert_ne!(probe.opaque, opaque);
        assert!(
            !pending.is_finished(),
            "late oneway response completed pending invoke"
        );
        assert!(client.inner.pending.lock().unwrap().contains_key(&opaque));
        let (target, mut stream) = peer.await.unwrap();
        assert_eq!(target, opaque);
        reply(&mut stream, opaque).await;
        assert_eq!(pending.await.unwrap().unwrap().opaque, opaque);
        assert!(client.inner.pending.lock().unwrap().is_empty());
        client.shutdown().await;
    }

    #[tokio::test]
    async fn oneway_connection_acquisition_enters_connect_before_return() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        client
            .invoke_oneway(
                &address.to_string(),
                test_command(),
                Instant::now() + std::time::Duration::from_secs(1),
            )
            .unwrap();
        {
            let connecting = client.inner.connecting.lock().unwrap();
            let gate = connecting
                .get(&address.to_string())
                .and_then(Weak::upgrade)
                .expect("acquisition should register its single-flight gate before return");
            assert!(gate.try_lock().is_err(), "dial future should hold the gate");
        }
        client.shutdown().await;
    }

    #[tokio::test]
    async fn spawned_connection_future_is_polled_before_return() {
        let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let polls_for_future = polls.clone();
        let task = spawn_after_initial_poll(async move {
            polls_for_future.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
        })
        .expect("pending future should be spawned");
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        task.abort();
        let _ = task.await;
    }

    #[test]
    fn stale_name_server_probe_does_not_override_current_generation_availability() {
        assert_eq!(
            name_server_probe_availability(Ok(Some(7)), Some(7)),
            Some(true)
        );
        assert_eq!(name_server_probe_availability(Ok(Some(7)), Some(8)), None);
        assert_eq!(name_server_probe_availability(Ok(None), Some(8)), None);
        assert_eq!(
            name_server_probe_availability(Ok(Some(7)), None),
            Some(false)
        );
        assert_eq!(name_server_probe_availability(Ok(None), None), Some(false));
        assert_eq!(
            name_server_probe_availability(
                Err(TransportError::Connect("probe failed".into())),
                Some(8)
            ),
            None
        );
        assert_eq!(
            name_server_probe_availability(
                Err(TransportError::Connect("probe failed".into())),
                None
            ),
            None
        );
    }

    #[tokio::test]
    async fn admission_matches_java_zero_timeout_and_wait_timeout_categories() {
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(0));
        assert_eq!(
            client
                .inner
                .acquire_admission(semaphore.clone(), Instant::now())
                .await
                .unwrap_err(),
            TransportError::TooManyRequests
        );
        assert!(
            client
                .inner
                .acquire_admission(
                    semaphore,
                    Instant::now() + std::time::Duration::from_millis(10),
                )
                .await
                .unwrap_err()
                .is_timeout()
        );
        let available = Arc::new(tokio::sync::Semaphore::new(1));
        let _permit = client
            .inner
            .acquire_admission(available.clone(), Instant::now())
            .await
            .unwrap();
        assert_eq!(
            client
                .inner
                .acquire_admission(available, Instant::now())
                .await
                .unwrap_err(),
            TransportError::TooManyRequests
        );
        client.shutdown().await;
    }

    #[tokio::test]
    async fn signed_semaphore_settings_match_java_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        let defaults = ClientConfig::default();
        assert_eq!(
            fixture["remotingSemaphoreConfig"]["defaults"]["async"].as_i64(),
            Some(i64::from(defaults.async_semaphore_value))
        );
        assert_eq!(
            fixture["remotingSemaphoreConfig"]["defaults"]["oneway"].as_i64(),
            Some(i64::from(defaults.oneway_semaphore_value))
        );

        for case in fixture["remotingSemaphoreConfig"]["cases"]
            .as_array()
            .unwrap()
        {
            let value = i32::try_from(case["input"].as_i64().unwrap()).unwrap();
            assert_eq!(case["async"].as_i64(), Some(i64::from(value)));
            assert_eq!(case["oneway"].as_i64(), Some(i64::from(value)));
            let client = RemotingClient::with_config(ClientConfig {
                async_semaphore_value: value,
                oneway_semaphore_value: value,
                ..ClientConfig::default()
            })
            .unwrap();
            let expected_capacity = if value == i32::MIN {
                i32::MAX as usize + 1
            } else {
                value.max(0) as usize
            }
            .min(tokio::sync::Semaphore::MAX_PERMITS);

            for (semaphore, acquired_key, after_key) in [
                (
                    &client.inner.async_admission,
                    "asyncAcquireImmediate",
                    "asyncAvailableAfterAcquire",
                ),
                (
                    &client.inner.oneway_admission,
                    "onewayAcquireImmediate",
                    "onewayAvailableAfterAcquire",
                ),
            ] {
                assert_eq!(semaphore.available_permits(), expected_capacity);
                let java_acquired = case[acquired_key].as_bool().unwrap();
                if java_acquired {
                    let permit = client
                        .inner
                        .acquire_admission(semaphore.clone(), Instant::now())
                        .await
                        .unwrap();
                    assert_eq!(
                        semaphore.available_permits() as i64,
                        case[after_key].as_i64().unwrap()
                    );
                    drop(permit);
                } else {
                    assert_eq!(
                        client
                            .inner
                            .acquire_admission(semaphore.clone(), Instant::now())
                            .await
                            .unwrap_err(),
                        TransportError::TooManyRequests
                    );
                    assert!(
                        client
                            .inner
                            .acquire_admission(
                                semaphore.clone(),
                                Instant::now() + std::time::Duration::from_millis(2),
                            )
                            .await
                            .unwrap_err()
                            .is_timeout()
                    );
                }
            }
            client.shutdown().await;
        }
    }

    #[test]
    fn client_remoting_defaults_follow_java_system_properties() {
        const CHILD_MARKER: &str = "ROCKETMQ_REMOTING_PROPERTY_CHILD";
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config-properties.json"
        ))
        .unwrap();
        let async_property = config::JAVA_CLIENT_ASYNC_SEMAPHORE_VALUE_PROPERTY;
        let oneway_property = config::JAVA_CLIENT_ONEWAY_SEMAPHORE_VALUE_PROPERTY;
        let worker_property = config::JAVA_CLIENT_WORKER_SIZE_PROPERTY;
        let connect_timeout_property = config::JAVA_CLIENT_CONNECT_TIMEOUT_PROPERTY;
        let max_idle_time_property = config::JAVA_CLIENT_CHANNEL_MAX_IDLE_TIME_PROPERTY;
        let close_socket_property = config::JAVA_CLIENT_CLOSE_SOCKET_IF_TIMEOUT_PROPERTY;
        let send_buffer_property = config::JAVA_CLIENT_SOCKET_SNDBUF_SIZE_PROPERTY;
        let receive_buffer_property = config::JAVA_CLIENT_SOCKET_RCVBUF_SIZE_PROPERTY;
        let high_watermark_property = config::JAVA_CLIENT_WRITE_BUFFER_HIGH_WATER_MARK_PROPERTY;
        let low_watermark_property = config::JAVA_CLIENT_WRITE_BUFFER_LOW_WATER_MARK_PROPERTY;
        let configured = &fixture["remotingSemaphoreValues"];
        let worker_settings = &fixture["remotingWorkerSettings"];
        let timeout_settings = &fixture["remotingTimeoutSettings"];
        let bootstrap_settings = &fixture["remotingBootstrapSettings"];

        match std::env::var(CHILD_MARKER).as_deref() {
            Ok("valid") => {
                let defaults = ClientConfig::default();
                assert_eq!(
                    i64::from(defaults.async_semaphore_value),
                    configured["async"].as_i64().unwrap()
                );
                assert_eq!(
                    i64::from(defaults.oneway_semaphore_value),
                    configured["oneway"].as_i64().unwrap()
                );
                assert_eq!(
                    i64::from(defaults.worker_threads),
                    worker_settings["workerThreads"].as_i64().unwrap()
                );
                assert_eq!(
                    defaults.close_socket_if_timeout,
                    timeout_settings["closeSocketIfTimeout"]
                );
                assert_eq!(
                    i64::from(defaults.connect_timeout),
                    timeout_settings["connectTimeoutMillis"].as_i64().unwrap()
                );
                assert_eq!(
                    i64::from(defaults.max_idle_time),
                    timeout_settings["clientChannelMaxIdleTimeSeconds"]
                        .as_i64()
                        .unwrap()
                );
                assert_eq!(
                    i64::from(defaults.client_socket_send_buffer_size),
                    bootstrap_settings["clientSocketSndBufSize"]
                        .as_i64()
                        .unwrap()
                );
                assert_eq!(
                    i64::from(defaults.client_socket_receive_buffer_size),
                    bootstrap_settings["clientSocketRcvBufSize"]
                        .as_i64()
                        .unwrap()
                );
                assert_eq!(
                    i64::from(defaults.write_buffer_high_water_mark),
                    bootstrap_settings["writeBufferHighWaterMark"]
                        .as_i64()
                        .unwrap()
                );
                assert_eq!(
                    i64::from(defaults.write_buffer_low_water_mark),
                    bootstrap_settings["writeBufferLowWaterMark"]
                        .as_i64()
                        .unwrap()
                );
                return;
            }
            Ok("invalid") => {
                let error = std::panic::catch_unwind(ClientConfig::default).unwrap_err();
                let message = error.downcast_ref::<String>().unwrap();
                assert!(message.contains(async_property));
                return;
            }
            Ok("invalid-bootstrap") => {
                let error = std::panic::catch_unwind(ClientConfig::default).unwrap_err();
                let message = error.downcast_ref::<String>().unwrap();
                assert!(message.contains(send_buffer_property));
                return;
            }
            _ => {}
        }

        use std::process::Command;
        let executable = std::env::current_exe().unwrap();
        let property_values = &fixture["systemProperties"];
        let valid = Command::new(&executable)
            .arg("--exact")
            .arg("client::tests::client_remoting_defaults_follow_java_system_properties")
            .arg("--nocapture")
            .env(CHILD_MARKER, "valid")
            .env(
                async_property,
                property_values[async_property].as_str().unwrap(),
            )
            .env(
                oneway_property,
                property_values[oneway_property].as_str().unwrap(),
            )
            .env(
                worker_property,
                property_values[worker_property].as_str().unwrap(),
            )
            .env(
                close_socket_property,
                property_values[close_socket_property].as_str().unwrap(),
            )
            .env(
                connect_timeout_property,
                property_values[connect_timeout_property].as_str().unwrap(),
            )
            .env(
                max_idle_time_property,
                property_values[max_idle_time_property].as_str().unwrap(),
            )
            .env(
                send_buffer_property,
                property_values[send_buffer_property].as_str().unwrap(),
            )
            .env(
                receive_buffer_property,
                property_values[receive_buffer_property].as_str().unwrap(),
            )
            .env(
                high_watermark_property,
                property_values[high_watermark_property].as_str().unwrap(),
            )
            .env(
                low_watermark_property,
                property_values[low_watermark_property].as_str().unwrap(),
            )
            .output()
            .unwrap();
        assert!(
            valid.status.success(),
            "valid child failed: {}{}",
            String::from_utf8_lossy(&valid.stdout),
            String::from_utf8_lossy(&valid.stderr)
        );

        let invalid = Command::new(&executable)
            .arg("--exact")
            .arg("client::tests::client_remoting_defaults_follow_java_system_properties")
            .arg("--nocapture")
            .env(CHILD_MARKER, "invalid")
            .env(async_property, " 1")
            .env(
                oneway_property,
                property_values[oneway_property].as_str().unwrap(),
            )
            .env(
                worker_property,
                property_values[worker_property].as_str().unwrap(),
            )
            .env(
                close_socket_property,
                property_values[close_socket_property].as_str().unwrap(),
            )
            .env(
                connect_timeout_property,
                property_values[connect_timeout_property].as_str().unwrap(),
            )
            .env(
                max_idle_time_property,
                property_values[max_idle_time_property].as_str().unwrap(),
            )
            .output()
            .unwrap();
        assert!(
            invalid.status.success(),
            "invalid child failed: {}{}",
            String::from_utf8_lossy(&invalid.stdout),
            String::from_utf8_lossy(&invalid.stderr)
        );

        let invalid_bootstrap = Command::new(&executable)
            .arg("--exact")
            .arg("client::tests::client_remoting_defaults_follow_java_system_properties")
            .arg("--nocapture")
            .env(CHILD_MARKER, "invalid-bootstrap")
            .env(
                async_property,
                property_values[async_property].as_str().unwrap(),
            )
            .env(
                oneway_property,
                property_values[oneway_property].as_str().unwrap(),
            )
            .env(
                worker_property,
                property_values[worker_property].as_str().unwrap(),
            )
            .env(
                connect_timeout_property,
                property_values[connect_timeout_property].as_str().unwrap(),
            )
            .env(
                max_idle_time_property,
                property_values[max_idle_time_property].as_str().unwrap(),
            )
            .env(send_buffer_property, " 1")
            .env(
                receive_buffer_property,
                property_values[receive_buffer_property].as_str().unwrap(),
            )
            .env(
                high_watermark_property,
                property_values[high_watermark_property].as_str().unwrap(),
            )
            .env(
                low_watermark_property,
                property_values[low_watermark_property].as_str().unwrap(),
            )
            .output()
            .unwrap();
        assert!(
            invalid_bootstrap.status.success(),
            "invalid bootstrap property child failed: {}{}",
            String::from_utf8_lossy(&invalid_bootstrap.stdout),
            String::from_utf8_lossy(&invalid_bootstrap.stderr)
        );
    }

    #[tokio::test]
    async fn start_and_shutdown_race_leaves_no_owned_tasks_or_connections() {
        for _ in 0..16 {
            let client = RemotingClient::new(TransportMode::Plain);
            let barrier = Arc::new(tokio::sync::Barrier::new(3));
            let start_client = client.clone();
            let start_barrier = barrier.clone();
            let start = tokio::spawn(async move {
                start_barrier.wait().await;
                start_client.start()
            });
            let shutdown_client = client.clone();
            let shutdown_barrier = barrier.clone();
            let shutdown = tokio::spawn(async move {
                shutdown_barrier.wait().await;
                shutdown_client.shutdown().await;
            });
            barrier.wait().await;
            let _ = start.await.unwrap();
            shutdown.await.unwrap();
            assert!(client.inner.tasks.lock().unwrap().is_empty());
            assert!(client.inner.connections.lock().unwrap().is_empty());
            assert!(client.inner.pending.lock().unwrap().is_empty());
            assert_eq!(client.start(), Err(TransportError::Shutdown));
        }
    }

    #[tokio::test]
    async fn pending_terminal_race_and_opaque_wrap_never_replace_live_entries() {
        let client =
            RemotingClient::with_limits(TransportMode::Plain, DEFAULT_MAX_FRAME_SIZE, i32::MAX);
        let generation = 9;
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let (first_id, first_rx) = client
            .inner
            .insert_pending(generation, deadline, "127.0.0.1:1".into(), 0)
            .unwrap();
        let (second_id, second_rx) = client
            .inner
            .insert_pending(generation, deadline, "127.0.0.1:1".into(), 0)
            .unwrap();
        assert_eq!(first_id, i32::MAX);
        assert_eq!(second_id, i32::MIN);

        let (barrier_tx, _) = tokio::sync::broadcast::channel(1);
        let mut response_barrier = barrier_tx.subscribe();
        let mut timeout_barrier = barrier_tx.subscribe();
        let response_inner = client.inner.clone();
        let response = tokio::spawn(async move {
            let _ = response_barrier.recv().await;
            response_inner.finish_pending(first_id, generation, Ok(test_command()))
        });
        let timeout_inner = client.inner.clone();
        let timeout = tokio::spawn(async move {
            let _ = timeout_barrier.recv().await;
            timeout_inner.finish_pending(first_id, generation, Err(TransportError::Timeout))
        });
        barrier_tx.send(()).unwrap();
        let response_won = response.await.unwrap();
        let timeout_won = timeout.await.unwrap();
        assert_ne!(response_won, timeout_won);
        assert_eq!(first_rx.await.is_ok(), response_won);
        assert_eq!(client.inner.pending.lock().unwrap().len(), 1);
        client.inner.cancel_pending(second_id, generation);
        assert!(matches!(second_rx.await, Ok(Err(TransportError::Closed))));
        assert_eq!(client.inner.pending.lock().unwrap().len(), 0);
        client.shutdown().await;
        assert!(client.inner.tasks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn wire_response_timeout_cancellation_and_shutdown_race_leaves_no_pending() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut prefix = [0; 4];
            stream.read_exact(&mut prefix).await.unwrap();
            let frame_length = u32::from_be_bytes(prefix) as usize;
            let mut frame_bytes = bytes::BytesMut::with_capacity(frame_length + 4);
            frame_bytes.extend_from_slice(&prefix);
            frame_bytes.resize(frame_length + 4, 0);
            stream.read_exact(&mut frame_bytes[4..]).await.unwrap();
            let request = crate::codec::frame::decode_next(&mut frame_bytes)
                .unwrap()
                .unwrap();
            request_tx.send(request.opaque).unwrap();
            let _ = release_rx.await;
            let mut response = test_command();
            response.opaque = request.opaque;
            response.mark_response();
            let _ = stream
                .write_all(&crate::codec::frame::encode(&response).unwrap())
                .await;
        });

        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let invoke_client = client.clone();
        let address_string = address.to_string();
        let invoke = tokio::spawn(async move {
            invoke_client
                .invoke(
                    &address_string,
                    test_command(),
                    Instant::now() + std::time::Duration::from_secs(5),
                )
                .await
        });
        let opaque = tokio::time::timeout(std::time::Duration::from_secs(2), request_rx)
            .await
            .expect("invoke frame was not written")
            .unwrap();

        let generation = client
            .inner
            .pending
            .lock()
            .unwrap()
            .get(&opaque)
            .expect("wire request must own a pending entry")
            .generation;
        let barrier = Arc::new(tokio::sync::Barrier::new(5));
        let cancel_barrier = barrier.clone();
        let cancel = tokio::spawn(async move {
            cancel_barrier.wait().await;
            invoke.abort();
        });
        let timeout_inner = client.inner.clone();
        let timeout_barrier = barrier.clone();
        let timeout = tokio::spawn(async move {
            timeout_barrier.wait().await;
            timeout_inner.expire_pending(opaque, generation);
        });
        let shutdown_client = client.clone();
        let shutdown_barrier = barrier.clone();
        let shutdown = tokio::spawn(async move {
            shutdown_barrier.wait().await;
            shutdown_client.shutdown().await;
        });
        let release_barrier = barrier.clone();
        let release = tokio::spawn(async move {
            release_barrier.wait().await;
            let _ = release_tx.send(());
        });
        barrier.wait().await;
        cancel.await.unwrap();
        timeout.await.unwrap();
        release.await.unwrap();
        shutdown.await.unwrap();
        let _ = peer.await;
        assert!(client.inner.pending.lock().unwrap().is_empty());
        assert!(client.inner.tasks.lock().unwrap().is_empty());
        assert!(client.inner.connections.lock().unwrap().is_empty());
        assert!(!client.inner.pending.lock().unwrap().contains_key(&opaque));
    }

    #[tokio::test]
    async fn connection_creation_racing_shutdown_closes_unpublished_or_published_socket() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            let mut byte = [0; 1];
            assert!(
                matches!(
                    tokio::time::timeout(
                        std::time::Duration::from_secs(2),
                        tokio::io::AsyncReadExt::read(&mut stream, &mut byte),
                    )
                    .await,
                    Ok(Ok(0)) | Ok(Err(_))
                ),
                "shutdown left the accepted socket open"
            );
        });
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let connect_client = client.clone();
        let connect = tokio::spawn(async move {
            connect_client
                .inner
                .connection(
                    &address.to_string(),
                    Instant::now() + std::time::Duration::from_secs(5),
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), accepted_rx)
            .await
            .expect("connection attempt did not reach the peer")
            .unwrap();
        tokio::task::yield_now().await;
        client.shutdown().await;
        if let Ok(connection) = connect.await.unwrap() {
            assert!(
                connection.is_closed(),
                "shutdown left a published connection open"
            );
        }
        peer.await.unwrap();
        assert!(client.inner.connections.lock().unwrap().is_empty());
        assert!(client.inner.pending.lock().unwrap().is_empty());
        assert!(client.inner.tasks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn inbound_dispatch_queue_accepts_more_than_legacy_capacity() {
        struct InlineExecutor;

        impl RequestExecutor for InlineExecutor {
            fn execute(&self, task: CallbackTask) -> Result<(), String> {
                task();
                Ok(())
            }
        }

        let client = RemotingClient::new(TransportMode::Plain);

        client.start().unwrap();
        let processed = Arc::new(AtomicUsize::new(0));
        let processed_by_handler = processed.clone();
        client.register_processor_with_executor(
            test_command().code,
            Arc::new(move |_| {
                processed_by_handler.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            }),
            Some(Arc::new(InlineExecutor)),
        );
        for _ in 0..256 {
            client
                .inner
                .inbound
                .send(processor::InboundRequest {
                    generation: 0,
                    remote_addr: "test:0".into(),
                    channel: ChannelHandle::pending("test:0"),
                    command: test_command(),
                })
                .unwrap();
        }
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while processed.load(Ordering::SeqCst) != 256 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("unbounded inbound queue did not dispatch all accepted requests");
        client.shutdown().await;
    }

    #[tokio::test]
    async fn accepted_inbound_requests_are_dispatched_when_shutdown_starts() {
        struct InlineExecutor;

        impl RequestExecutor for InlineExecutor {
            fn execute(&self, task: CallbackTask) -> Result<(), String> {
                task();
                Ok(())
            }
        }

        let client = RemotingClient::new(TransportMode::Plain);

        client.start().unwrap();
        let processed = Arc::new(AtomicUsize::new(0));
        let processed_by_handler = processed.clone();
        client.register_processor_with_executor(
            test_command().code,
            Arc::new(move |_| {
                processed_by_handler.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            }),
            Some(Arc::new(InlineExecutor)),
        );
        client
            .inner
            .inbound
            .send(processor::InboundRequest {
                generation: 0,
                remote_addr: "test:0".into(),
                channel: ChannelHandle::pending("test:0"),
                command: test_command(),
            })
            .unwrap();
        client.inner.shutdown.send(true).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while processed.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("accepted inbound request was dropped during shutdown");
        client.shutdown().await;
    }

    #[tokio::test]
    async fn inbound_shutdown_go_away_precedes_rejection_above_java_551_version_cutoff() {
        struct RejectingProcessor {
            rejects: Arc<AtomicUsize>,
            processed: Arc<AtomicUsize>,
        }

        impl RequestProcessor for RejectingProcessor {
            fn process(&self, _: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
                self.processed.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            }

            fn reject_request(&self) -> bool {
                self.rejects.fetch_add(1, Ordering::SeqCst);
                true
            }
        }

        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let rejects = Arc::new(AtomicUsize::new(0));
        let processed = Arc::new(AtomicUsize::new(0));
        client.register_processor(
            test_command().code,
            Arc::new(RejectingProcessor {
                rejects: rejects.clone(),
                processed: processed.clone(),
            }),
        );

        client.inner.shutdown.send_replace(true);
        for version in [476, 475] {
            let mut command = test_command();
            command.version = version;
            processor::process_one(
                &Arc::downgrade(&client.inner),
                processor::InboundRequest {
                    generation: 0,
                    remote_addr: "test:0".into(),
                    channel: ChannelHandle::pending("test:0"),
                    command,
                },
            );
            assert_eq!(rejects.load(Ordering::SeqCst), usize::from(version == 475));
        }

        client.inner.shutdown.send_replace(false);
        let mut running_command = test_command();
        running_command.version = 476;
        processor::process_one(
            &Arc::downgrade(&client.inner),
            processor::InboundRequest {
                generation: 0,
                remote_addr: "test:0".into(),
                channel: ChannelHandle::pending("test:0"),
                command: running_command,
            },
        );

        assert_eq!(rejects.load(Ordering::SeqCst), 2);
        assert_eq!(processed.load(Ordering::SeqCst), 0);
        client.shutdown().await;
    }

    #[tokio::test]
    async fn callback_workers_drain_late_tasks_after_client_shutdown_starts() {
        let (sender, receiver) = mpsc::unbounded_channel();
        let (shutdown, _shutdown_receiver) = tokio::sync::watch::channel(false);
        let (callback_shutdown, _) = tokio::sync::watch::channel(false);
        let completed = Arc::new(AtomicUsize::new(0));
        for _ in 0..32 {
            let completed = completed.clone();
            sender
                .send(Box::new(move || {
                    completed.fetch_add(1, Ordering::SeqCst);
                }) as CallbackTask)
                .unwrap();
        }
        let workers = start_callback_workers(receiver, callback_shutdown.subscribe(), 2);
        shutdown.send(true).unwrap();
        let late_completed = completed.clone();
        sender
            .send(Box::new(move || {
                late_completed.fetch_add(1, Ordering::SeqCst);
            }) as CallbackTask)
            .unwrap();
        callback_shutdown.send(true).unwrap();
        for worker in workers {
            tokio::time::timeout(std::time::Duration::from_secs(2), worker)
                .await
                .expect("callback worker did not drain accepted tasks")
                .unwrap();
        }
        assert_eq!(completed.load(Ordering::SeqCst), 33);
    }

    #[test]
    fn public_executor_rejects_submissions_after_client_shutdown() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let state = TokioExecutorState {
            closed: AtomicBool::new(false),
            admission: Mutex::new(()),
        };
        state
            .execute(&sender, Box::new(|| {}))
            .expect("running executor accepts tasks");
        assert!(receiver.try_recv().is_ok());
        state.shutdown();
        assert!(state.execute(&sender, Box::new(|| {})).is_err());
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn client_worker_threads_setting_is_read_when_starting() {
        let client = RemotingClient::with_config(ClientConfig {
            worker_threads: 1,
            callback_executor_threads: 1,
            scan_available_nameserver: false,
            ..ClientConfig::default()
        })
        .unwrap();
        assert_eq!(client.get_client_worker_threads(), 1);
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config-properties.json"
        ))
        .unwrap();
        for case in fixture["remotingWorkerSettings"]["setterCases"]
            .as_array()
            .unwrap()
        {
            let input = i32::try_from(case["input"].as_i64().unwrap()).unwrap();
            let expected = i32::try_from(case["getter"].as_i64().unwrap()).unwrap();
            client.set_client_worker_threads(input);
            assert_eq!(client.get_client_worker_threads(), expected);
        }

        client.set_client_worker_threads(-1);
        assert_eq!(client.get_client_worker_threads(), -1);
        assert!(matches!(
            client.start(),
            Err(TransportError::InvalidConfiguration(_))
        ));
        client.set_client_worker_threads(0);
        assert!(matches!(
            client.start(),
            Err(TransportError::InvalidConfiguration(_))
        ));
        client.set_client_worker_threads(3);
        client.start().unwrap();
        assert_eq!(client.get_client_worker_threads(), 3);
        // Processor start owns one dispatcher plus three workers. The remaining
        // tracked task is the timeout scheduler; shutdown joins the event worker separately.
        assert_eq!(client.inner.tasks.lock().unwrap().len(), 5);
        assert!(client.inner.channel_event_worker.lock().unwrap().is_some());
        assert_eq!(client.inner.callback_workers.lock().unwrap().len(), 1);
        client.shutdown().await;
    }

    #[tokio::test]
    async fn connect_timeout_setting_is_snapshotted_when_starting() {
        let client = RemotingClient::with_config(ClientConfig {
            scan_available_nameserver: false,
            ..ClientConfig::default()
        })
        .unwrap();
        assert_eq!(client.get_connect_timeout_millis(), 3000);
        client.set_connect_timeout_millis(25);
        assert_eq!(client.inner.active_connect_timeout_millis(), 3000);

        client.start().unwrap();
        assert_eq!(client.inner.active_connect_timeout_millis(), 25);
        let requested_deadline = Instant::now() + std::time::Duration::from_secs(1);
        let connect_deadline = ClientInner::deadline_for_connect_timeout(
            Some(requested_deadline),
            client.inner.active_connect_timeout_millis(),
        )
        .unwrap();
        assert!(connect_deadline <= requested_deadline);
        assert!(
            connect_deadline.saturating_duration_since(Instant::now())
                <= std::time::Duration::from_millis(25)
        );

        client.set_connect_timeout_millis(99);
        assert_eq!(client.get_connect_timeout_millis(), 99);
        assert_eq!(client.inner.active_connect_timeout_millis(), 25);
        client.shutdown().await;
    }

    #[tokio::test]
    async fn proxy_connect_timeout_is_read_on_first_route_use_and_then_cached() {
        let client = RemotingClient::with_config(ClientConfig {
            scan_available_nameserver: false,
            ..ClientConfig::default()
        })
        .unwrap();
        client.start().unwrap();

        client.set_connect_timeout_millis(125);
        let first = client
            .inner
            .connection_deadline_for_route(
                Some("127.0.0.0/8"),
                Some(Instant::now() + std::time::Duration::from_secs(5)),
            )
            .unwrap();
        assert_eq!(
            client
                .inner
                .proxy_connect_timeout_millis
                .lock()
                .unwrap()
                .get("127.0.0.0/8"),
            Some(&125)
        );
        assert_eq!(client.inner.active_connect_timeout_millis(), 3000);

        client.set_connect_timeout_millis(250);
        let cached = client
            .inner
            .connection_deadline_for_route(
                Some("127.0.0.0/8"),
                Some(Instant::now() + std::time::Duration::from_secs(5)),
            )
            .unwrap();
        assert_eq!(
            client
                .inner
                .proxy_connect_timeout_millis
                .lock()
                .unwrap()
                .get("127.0.0.0/8"),
            Some(&125)
        );
        assert!(first <= Instant::now() + std::time::Duration::from_millis(125));
        assert!(cached <= Instant::now() + std::time::Duration::from_millis(125));

        let second_route = client
            .inner
            .connection_deadline_for_route(
                Some("10.0.0.0/8"),
                Some(Instant::now() + std::time::Duration::from_secs(5)),
            )
            .unwrap();
        assert_eq!(
            client
                .inner
                .proxy_connect_timeout_millis
                .lock()
                .unwrap()
                .get("10.0.0.0/8"),
            Some(&250)
        );
        assert!(second_route <= Instant::now() + std::time::Duration::from_millis(250));
        client.shutdown().await;
    }

    #[tokio::test]
    async fn channel_pipeline_settings_are_read_from_live_values() {
        let client = RemotingClient::with_config(ClientConfig {
            scan_available_nameserver: false,
            ..ClientConfig::default()
        })
        .unwrap();
        client.start().unwrap();
        assert_eq!(client.get_client_channel_max_idle_time_seconds(), 120);
        assert!(!client.get_disable_netty_worker_group());
        assert_eq!(
            client.inner.channel_pipeline_settings(),
            (std::time::Duration::from_secs(120), false)
        );

        client.set_client_channel_max_idle_time_seconds(-1);
        client.set_disable_netty_worker_group(true);
        assert_eq!(client.get_client_channel_max_idle_time_seconds(), -1);
        assert!(client.get_disable_netty_worker_group());
        assert_eq!(
            client.inner.channel_pipeline_settings(),
            (std::time::Duration::ZERO, true)
        );

        client.set_client_channel_max_idle_time_seconds(2);
        client.set_disable_netty_worker_group(false);
        assert_eq!(
            client.inner.channel_pipeline_settings(),
            (std::time::Duration::from_secs(2), false)
        );
        client.shutdown().await;
    }

    #[tokio::test]
    async fn bootstrap_options_use_start_and_proxy_first_use_snapshots() {
        let client = RemotingClient::with_config(ClientConfig {
            scan_available_nameserver: false,
            ..ClientConfig::default()
        })
        .unwrap();
        client.set_client_socket_send_buffer_size(4096);
        client.set_client_socket_receive_buffer_size(8192);
        client.set_write_buffer_low_water_mark(16 * 1024);
        client.set_write_buffer_high_water_mark(48 * 1024);
        client.set_pooled_byte_buffer_allocator_enabled(true);
        client.start().unwrap();

        let direct = client.inner.direct_connection_settings().unwrap();
        assert_eq!(direct.send_buffer_size, 4096);
        assert_eq!(direct.receive_buffer_size, 8192);
        assert_eq!(direct.write_buffer_watermarks, (48 * 1024, 16 * 1024));
        assert!(direct.pooled_byte_buffer_allocator_enabled);

        client.set_client_socket_send_buffer_size(12 * 1024);
        client.set_client_socket_receive_buffer_size(24 * 1024);
        client.set_write_buffer_low_water_mark(4 * 1024);
        client.set_write_buffer_high_water_mark(12 * 1024);
        client.set_pooled_byte_buffer_allocator_enabled(true);

        let first_proxy = client
            .inner
            .connection_settings_for_route(Some("127.0.0.0/8"))
            .unwrap();
        assert_eq!(first_proxy.send_buffer_size, 12 * 1024);
        assert_eq!(first_proxy.receive_buffer_size, 24 * 1024);
        assert_eq!(first_proxy.write_buffer_watermarks, (64 * 1024, 32 * 1024));
        assert!(!first_proxy.pooled_byte_buffer_allocator_enabled);

        client.set_client_socket_send_buffer_size(32 * 1024);
        client.set_client_socket_receive_buffer_size(48 * 1024);
        assert_eq!(
            client
                .inner
                .connection_settings_for_route(Some("127.0.0.0/8"))
                .unwrap(),
            first_proxy
        );
        let second_proxy = client
            .inner
            .connection_settings_for_route(Some("10.0.0.0/8"))
            .unwrap();
        assert_eq!(second_proxy.send_buffer_size, 32 * 1024);
        assert_eq!(second_proxy.receive_buffer_size, 48 * 1024);
        assert_eq!(client.inner.direct_connection_settings().unwrap(), direct);
        client.shutdown().await;
    }

    #[tokio::test]
    async fn signed_socket_settings_follow_java_direct_and_proxy_snapshots() {
        let client = RemotingClient::with_config(ClientConfig {
            scan_available_nameserver: false,
            socks_proxy_config: r#"{"127.0.0.0/8":{"addr":"127.0.0.1:1080"}}"#.into(),
            client_socket_send_buffer_size: -8192,
            client_socket_receive_buffer_size: 0,
            write_buffer_low_water_mark: -3,
            write_buffer_high_water_mark: 12,
            ..ClientConfig::default()
        })
        .unwrap();
        client.start().unwrap();

        let direct = client.inner.direct_connection_settings().unwrap();
        assert_eq!(direct.send_buffer_size, -8192);
        assert_eq!(direct.receive_buffer_size, 0);
        assert_eq!(direct.write_buffer_watermarks, (64 * 1024, 32 * 1024));

        let proxy = client
            .inner
            .connection_settings_for_route(Some("127.0.0.0/8"))
            .unwrap();
        assert_eq!(proxy.send_buffer_size, -8192);
        assert_eq!(proxy.receive_buffer_size, 0);
        assert_eq!(proxy.write_buffer_watermarks, (64 * 1024, 32 * 1024));
        client.shutdown().await;
    }

    #[tokio::test]
    async fn signed_buffer_and_watermark_settings_match_java_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config-properties.json"
        ))
        .unwrap();
        let settings = &fixture["remotingBootstrapSettings"]["setterCases"];
        let client = RemotingClient::with_config(ClientConfig {
            scan_available_nameserver: false,
            ..ClientConfig::default()
        })
        .unwrap();

        for (setting, cases) in settings.as_object().unwrap() {
            for case in cases.as_array().unwrap() {
                let input = i32::try_from(case["input"].as_i64().unwrap()).unwrap();
                let expected = i32::try_from(case["getter"].as_i64().unwrap()).unwrap();
                match setting.as_str() {
                    "clientSocketSndBufSize" => {
                        client.set_client_socket_send_buffer_size(input);
                        assert_eq!(client.get_client_socket_send_buffer_size(), expected);
                    }
                    "clientSocketRcvBufSize" => {
                        client.set_client_socket_receive_buffer_size(input);
                        assert_eq!(client.get_client_socket_receive_buffer_size(), expected);
                    }
                    "writeBufferHighWaterMark" => {
                        client.set_write_buffer_high_water_mark(input);
                        assert_eq!(client.get_write_buffer_high_water_mark(), expected);
                    }
                    "writeBufferLowWaterMark" => {
                        client.set_write_buffer_low_water_mark(input);
                        assert_eq!(client.get_write_buffer_low_water_mark(), expected);
                    }
                    other => panic!("unexpected Java setting fixture: {other}"),
                }
            }
        }
        client.shutdown().await;
    }

    #[tokio::test]
    async fn invalid_live_write_watermarks_leave_start_retryable() {
        let client = RemotingClient::with_config(ClientConfig {
            write_buffer_low_water_mark: 8192,
            write_buffer_high_water_mark: 4096,
            scan_available_nameserver: false,
            ..ClientConfig::default()
        })
        .unwrap();
        assert!(matches!(
            client.start(),
            Err(TransportError::InvalidConfiguration(_))
        ));
        assert!(!client.inner.started.load(Ordering::Acquire));

        client.set_write_buffer_high_water_mark(16 * 1024);
        client.start().unwrap();
        assert_eq!(
            client
                .inner
                .direct_connection_settings()
                .unwrap()
                .write_buffer_watermarks,
            (16 * 1024, 8192)
        );
        client.shutdown().await;
    }

    #[tokio::test]
    async fn worker_group_pool_is_available_for_live_channel_toggles() {
        let config = ClientConfig {
            disable_netty_worker_group: true,
            worker_threads: 2,
            ..ClientConfig::default()
        };
        let (inbound, receiver) = mpsc::unbounded_channel();
        let (shutdown, _) = tokio::sync::watch::channel(false);
        let (callback_shutdown, _) = tokio::sync::watch::channel(false);
        let (channel_event_stopped, _) = tokio::sync::watch::channel(false);
        let (shutdown_cleanup_stopped, _) = tokio::sync::watch::channel(false);
        let (callback_sender, _callback_receiver) = mpsc::unbounded_channel();
        let (channel_event_sender, _channel_event_receiver) =
            mpsc::channel(CHANNEL_EVENT_QUEUE_CAPACITY);
        let executor = Arc::new(TokioRequestExecutor {
            sender: callback_sender.clone(),
            state: Arc::new(TokioExecutorState {
                closed: AtomicBool::new(false),
                admission: Mutex::new(()),
            }),
        });
        let inner = Arc::new(ClientInner {
            connections: Mutex::new(HashMap::new()),
            connection_wrapper_locks: Mutex::new(HashMap::new()),
            retired_connections: Mutex::new(HashMap::new()),
            connecting: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            opaque: AtomicI32::new(0),
            generation: AtomicU64::new(1),
            processors: Mutex::new(HashMap::new()),
            default_processor: Mutex::new(None),
            inbound,
            public_executor: executor,
            shutdown,
            callback_shutdown,
            callback_workers: Mutex::new(Vec::new()),
            channel_event_stopped,
            channel_event_worker: Mutex::new(None),
            shutdown_cleanup_stopped,
            shutdown_started: tokio::sync::OnceCell::new(),
            closed: AtomicBool::new(false),
            tasks: Mutex::new(Vec::new()),
            security: TransportMode::Plain,
            use_tls: AtomicBool::new(false),
            max_frame_size_override: Some(DEFAULT_MAX_FRAME_SIZE),
            close_socket_if_timeout: AtomicBool::new(config.close_socket_if_timeout),
            enable_reconnect_for_go_away: AtomicBool::new(config.enable_reconnect_for_go_away),
            disable_callback_executor: AtomicBool::new(config.disable_callback_executor),
            client_worker_threads: AtomicI32::new(config.worker_threads),
            disable_netty_worker_group: AtomicBool::new(config.disable_netty_worker_group),
            client_channel_max_idle_time_seconds: AtomicI32::new(config.max_idle_time),
            client_socket_send_buffer_size: AtomicI32::new(config.client_socket_send_buffer_size),
            client_socket_receive_buffer_size: AtomicI32::new(
                config.client_socket_receive_buffer_size,
            ),
            write_buffer_high_water_mark: AtomicI32::new(config.write_buffer_high_water_mark),
            write_buffer_low_water_mark: AtomicI32::new(config.write_buffer_low_water_mark),
            pooled_byte_buffer_allocator_enabled: AtomicBool::new(
                config.pooled_byte_buffer_allocator_enabled,
            ),
            active_connection_settings: Mutex::new(None),
            proxy_connection_settings: Mutex::new(HashMap::new()),
            scan_available_nameserver: AtomicBool::new(config.scan_available_nameserver),
            connect_timeout_millis: AtomicI32::new(config.connect_timeout),
            active_connect_timeout_millis: AtomicI32::new(config.connect_timeout),
            proxy_connect_timeout_millis: Mutex::new(HashMap::new()),
            config,
            socks_proxy_routes: config::ProxyRoutes::default(),
            startup: Mutex::new(None),
            runtime: Mutex::new(Some(tokio::runtime::Handle::current())),
            started: AtomicBool::new(true),
            async_admission: Arc::new(tokio::sync::Semaphore::new(8)),
            oneway_admission: Arc::new(tokio::sync::Semaphore::new(8)),
            callback_sender,
            callback_executor: Mutex::new(None),
            rpc_hooks: Mutex::new(Vec::new()),
            channel_listener: None,
            channel_event_sender,
            request_pipeline: Mutex::new(None),
            metrics_manager: RwLock::new(None),
            nameservers: Mutex::new(None),
            available_nameservers: Mutex::new(HashSet::new()),
            chosen_nameserver: Mutex::new(None),
            nameserver_index: AtomicU64::new(0),
        });
        let tasks = processor::start(
            Arc::downgrade(&inner),
            receiver,
            usize::try_from(inner.config.worker_threads).expect("validated worker count"),
        );
        assert_eq!(
            tasks.len(),
            usize::try_from(inner.config.worker_threads).unwrap() + 1
        );
        inner.shutdown.send(true).unwrap();
        for task in tasks {
            task.await.unwrap();
        }
        inner.public_executor.state.shutdown();
    }

    #[tokio::test]
    async fn stale_generation_close_cannot_evict_replacement() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (_first, _) = listener.accept().await.unwrap();
            let (_replacement, _) = listener.accept().await.unwrap();
        });
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let first = client
            .inner
            .connection(
                &address.to_string(),
                Instant::now() + std::time::Duration::from_secs(1),
            )
            .await
            .unwrap();
        let stale_generation = first.generation;
        first.close();
        tokio::time::timeout(std::time::Duration::from_secs(2), first.join())
            .await
            .expect("first connection did not join");
        let replacement = client
            .inner
            .connection(
                &address.to_string(),
                Instant::now() + std::time::Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_ne!(replacement.generation, stale_generation);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            client
                .inner
                .connection_closed(stale_generation, Some(TransportError::Closed));
        })
        .await
        .expect("stale close handler blocked");
        assert!(!replacement.is_closed());
        assert_eq!(
            client
                .inner
                .connections
                .lock()
                .unwrap()
                .get(&address.to_string())
                .unwrap()
                .generation,
            replacement.generation
        );
        peer.await.unwrap();
        client.shutdown().await;
    }

    #[tokio::test]
    async fn cancelled_invoke_guard_removes_only_its_pending_generation() {
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let (opaque, receiver) = client
            .inner
            .insert_pending(5, deadline, "127.0.0.1:1".into(), 0)
            .unwrap();
        let guard = pending::PendingGuard::new(Arc::downgrade(&client.inner), opaque, 5);
        assert_eq!(client.inner.pending.lock().unwrap().len(), 1);
        drop(guard);
        assert_eq!(client.inner.pending.lock().unwrap().len(), 0);
        assert!(matches!(receiver.await, Ok(Err(TransportError::Closed))));
        client.shutdown().await;
    }

    #[tokio::test]
    async fn response_timeout_and_shutdown_race_has_one_terminal_completion() {
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let generation = 21;
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let (opaque, receiver) = client
            .inner
            .insert_pending(generation, deadline, "127.0.0.1:1".into(), 0)
            .unwrap();
        let guard = pending::PendingGuard::new(Arc::downgrade(&client.inner), opaque, generation);
        let barrier = Arc::new(tokio::sync::Barrier::new(5));

        let response_inner = client.inner.clone();
        let response_barrier = barrier.clone();
        let response = tokio::spawn(async move {
            response_barrier.wait().await;
            response_inner.finish_pending(opaque, generation, Ok(test_command()))
        });
        let timeout_inner = client.inner.clone();
        let timeout_barrier = barrier.clone();
        let timeout = tokio::spawn(async move {
            timeout_barrier.wait().await;
            timeout_inner.finish_pending(opaque, generation, Err(TransportError::Timeout))
        });
        let cancel_inner = client.inner.clone();
        let cancel_barrier = barrier.clone();
        let cancel = tokio::spawn(async move {
            cancel_barrier.wait().await;
            cancel_inner.finish_pending(opaque, generation, Err(TransportError::Closed))
        });
        let shutdown_inner = client.inner.clone();
        let shutdown_barrier = barrier.clone();
        let shutdown = tokio::spawn(async move {
            shutdown_barrier.wait().await;
            shutdown_inner.fail_all_pending();
        });
        barrier.wait().await;
        let response_won = response.await.unwrap();
        let timeout_won = timeout.await.unwrap();
        let cancel_won = cancel.await.unwrap();
        shutdown.await.unwrap();
        assert!(
            usize::from(response_won) + usize::from(timeout_won) + usize::from(cancel_won) <= 1
        );
        assert!(matches!(receiver.await, Ok(Ok(_)) | Ok(Err(_))));
        assert_eq!(client.inner.pending.lock().unwrap().len(), 0);
        drop(guard);
        client.shutdown().await;
    }

    #[tokio::test]
    async fn pooled_allocator_reuses_frame_capacity_after_successful_write() {
        let (client_io, mut peer_io) = tokio::io::duplex(4096);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            30,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            (64 * 1024, 32 * 1024),
            "test:0".into(),
            true,
        );
        let command = RemotingCommand {
            body: Some(vec![7; 128]),
            ..test_command()
        };
        let first = connection
            .encode_command(&command, DEFAULT_MAX_FRAME_SIZE)
            .unwrap();
        let first_capacity = first.capacity();
        connection
            .send(first, Instant::now() + std::time::Duration::from_secs(2))
            .await
            .unwrap();
        let mut length = [0; 4];
        tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut length)
            .await
            .unwrap();
        let remaining = u32::from_be_bytes(length) as usize;
        let mut received = vec![0; remaining];
        tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut received)
            .await
            .unwrap();
        assert_eq!(connection.pooled_frame_count(), 1);
        assert!(connection.pooled_frame_capacity() >= first_capacity);

        let second = connection
            .encode_command(&command, DEFAULT_MAX_FRAME_SIZE)
            .unwrap();
        assert!(second.capacity() >= first_capacity);
        connection
            .send(second, Instant::now() + std::time::Duration::from_secs(2))
            .await
            .unwrap();
        tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut length)
            .await
            .unwrap();
        let remaining = u32::from_be_bytes(length) as usize;
        received.resize(remaining, 0);
        tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut received)
            .await
            .unwrap();
        connection.close();
        connection.join().await;
    }

    #[tokio::test]
    async fn pooled_allocator_recycles_frame_after_encode_failure() {
        let (client_io, _peer_io) = tokio::io::duplex(64);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            23,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            (64 * 1024, 32 * 1024),
            "test:0".into(),
            true,
        );
        assert!(matches!(
            connection.encode_command(&test_command(), 1),
            Err(TransportError::Codec(_))
        ));
        assert_eq!(connection.pooled_frame_count(), 1);
        connection.close();
        connection.join().await;
    }

    #[tokio::test]
    async fn pooled_allocator_recycles_frame_after_saturated_write_timeout() {
        let (client_io, mut peer_io) = tokio::io::duplex(1);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            22,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            (64 * 1024, 32 * 1024),
            "test:0".into(),
            true,
        );
        let active_sender = {
            let connection = connection.clone();
            tokio::spawn(async move {
                connection
                    .send(
                        vec![0; 128],
                        Instant::now() + std::time::Duration::from_secs(5),
                    )
                    .await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while connection.queued_write_bytes() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("writer did not dequeue the active frame");
        let mut first_byte = [0];
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut first_byte),
        )
        .await
        .expect("writer did not begin sending the active frame")
        .unwrap();
        connection.try_send(vec![0; 128], Instant::now()).unwrap();
        let result = connection
            .send(
                vec![0; 128],
                Instant::now() + std::time::Duration::from_millis(10),
            )
            .await;
        assert!(matches!(result, Err(TransportError::Timeout)), "{result:?}");
        connection.close();
        connection.join().await;
        assert!(connection.pooled_frame_count() >= 1);
        active_sender.abort();
        let _ = active_sender.await;
    }

    #[test]
    fn asynchronous_write_failure_matches_java_request_fail_classification() {
        let remote_addr = "127.0.0.1:10911";
        let error =
            classify_connection_error(remote_addr, TransportError::Connect("refused".into()));
        assert!(matches!(
            error,
            TransportError::ConnectRequest {
                remote_addr: address,
                cause: Some(cause),
            } if address == remote_addr && cause.to_string() == "refused"
        ));
        let error = classify_connection_error(remote_addr, TransportError::Tls("handshake".into()));
        assert!(matches!(
            error,
            TransportError::ConnectRequest {
                remote_addr: address,
                cause: Some(cause),
            } if address == remote_addr && cause.to_string() == "handshake"
        ));
        assert_eq!(
            classify_connection_error(remote_addr, TransportError::Timeout),
            TransportError::Timeout
        );
        let error = classify_write_error(remote_addr, TransportError::Send("broken pipe".into()));
        assert!(matches!(
            error,
            TransportError::SendRequest {
                remote_addr: address,
                cause: Some(cause),
            } if address == remote_addr && cause.to_string() == "broken pipe"
        ));
        assert_eq!(
            classify_write_error(remote_addr, TransportError::Closed),
            TransportError::SendRequest {
                remote_addr: remote_addr.into(),
                cause: None,
            }
        );
        assert_eq!(
            classify_write_error(
                remote_addr,
                TransportError::Codec(crate::error::CodecError::FrameTooLarge {
                    length: 128,
                    limit: 64,
                }),
            ),
            TransportError::SendRequest {
                remote_addr: remote_addr.into(),
                cause: None,
            }
        );
        assert_eq!(
            classify_write_error(
                remote_addr,
                TransportError::MalformedFrame(crate::error::CodecError::FrameTooLarge {
                    length: 128,
                    limit: 64,
                }),
            ),
            TransportError::SendRequest {
                remote_addr: remote_addr.into(),
                cause: None,
            }
        );
        assert_eq!(
            classify_write_error(remote_addr, TransportError::Timeout),
            TransportError::Timeout
        );
    }

    #[tokio::test]
    async fn pooled_allocator_recycles_frames_after_write_error() {
        let (client_io, peer_io) = tokio::io::duplex(1);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            26,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            (64 * 1024, 32 * 1024),
            "test:0".into(),
            true,
        );
        drop(peer_io);
        let result = connection
            .send(
                vec![0; 128],
                Instant::now() + std::time::Duration::from_secs(2),
            )
            .await;
        assert!(matches!(result, Err(TransportError::SendSource(_))));
        connection.join().await;
        assert_eq!(connection.pooled_frame_count(), 1);
    }

    #[tokio::test]
    async fn pooled_allocator_recycles_frames_after_send_cancellation_and_shutdown() {
        for shutdown in [false, true] {
            let (client_io, mut peer_io) = tokio::io::duplex(1);
            let connection = connection::Connection::spawn(
                Box::new(client_io),
                if shutdown { 25 } else { 24 },
                Weak::new(),
                DEFAULT_MAX_FRAME_SIZE,
                (64 * 1024, 32 * 1024),
                "test:0".into(),
                true,
            );
            let sender = {
                let connection = connection.clone();
                tokio::spawn(async move {
                    connection
                        .send(
                            vec![0; 128],
                            Instant::now() + std::time::Duration::from_secs(5),
                        )
                        .await
                })
            };
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                while connection.queued_write_bytes() == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("writer did not dequeue the frame");
            let mut first_byte = [0];
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut first_byte),
            )
            .await
            .expect("writer did not begin sending the frame")
            .unwrap();
            if shutdown {
                connection.close();
            } else {
                sender.abort();
            }
            let _ = sender.await;
            tokio::time::timeout(std::time::Duration::from_secs(1), connection.join())
                .await
                .expect("connection did not stop");
            assert_eq!(connection.pooled_frame_count(), 1);
        }
    }

    #[tokio::test]
    async fn pooled_allocator_keeps_at_most_four_buffers_per_connection() {
        let (client_io, _peer_io) = tokio::io::duplex(64);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            27,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            (64 * 1024, 32 * 1024),
            "test:0".into(),
            true,
        );
        for _ in 0..8 {
            connection.recycle_frame(Vec::with_capacity(128));
        }
        assert_eq!(connection.pooled_frame_count(), 4);
        connection.close();
        connection.join().await;
    }

    #[tokio::test]
    async fn unbounded_outbound_queue_accepts_more_than_legacy_capacity() {
        let (client_io, mut peer_io) = tokio::io::duplex(1);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            29,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            (64 * 1024, 32 * 1024),
            "test:0".into(),
            true,
        );
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        connection.try_send(vec![0; 128], deadline).unwrap();
        let mut first_byte = [0];
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut first_byte),
        )
        .await
        .expect("writer did not start the active frame")
        .unwrap();

        let queued_frames = super::LEGACY_OUTBOUND_QUEUE_CAPACITY + 32;
        for _ in 0..queued_frames {
            connection.try_send(vec![0; 128], deadline).unwrap();
        }
        assert!(connection.queued_write_bytes() >= queued_frames * 128);
        connection.close();
        connection.join().await;
        assert!(connection.pooled_frame_count() > 0);
    }

    #[tokio::test]
    async fn write_watermarks_follow_queued_bytes_with_hysteresis() {
        let (client_io, mut peer_io) = tokio::io::duplex(64);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            32,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            (8, 3),
            "test:0".into(),
            true,
        );
        assert!(connection.is_writable());
        connection.reserve_write_bytes(8);
        assert!(
            connection.is_writable(),
            "Java stays writable at the high mark"
        );
        connection.reserve_write_bytes(1);
        assert!(!connection.is_writable());
        connection.release_write_bytes(6);
        assert!(
            !connection.is_writable(),
            "Java stays unwritable at the low mark"
        );
        connection.release_write_bytes(1);
        assert!(connection.is_writable());

        let deadline = Instant::now() + std::time::Duration::from_secs(2);
        connection.try_send(vec![0; 9], deadline).unwrap();
        assert!(!connection.is_writable());
        let mut buffer = [0; 9];
        tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut buffer)
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !connection.watermark_is_writable() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        connection.close();
        connection.join().await;
    }

    #[tokio::test]
    async fn response_style_write_waits_until_connection_shutdown() {
        let (client_io, _peer_io) = tokio::io::duplex(1);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            33,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            (64 * 1024, 32 * 1024),
            "test:0".into(),
            true,
        );
        let writer = {
            let connection = connection.clone();
            tokio::spawn(
                async move { connection.send_without_deadline(vec![0; 1024 * 1024]).await },
            )
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !writer.is_finished(),
            "unbounded response write must remain pending"
        );
        connection.close();
        assert_eq!(writer.await.unwrap(), Err(TransportError::Shutdown));
        connection.join().await;
    }

    #[tokio::test]
    async fn saturated_writer_obeys_deadline_and_joins() {
        let (client_io, _peer_io) = tokio::io::duplex(1);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            31,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            (64 * 1024, 32 * 1024),
            "test:0".into(),
            true,
        );
        let deadline = Instant::now() + std::time::Duration::from_millis(100);
        let queued_frames = super::LEGACY_OUTBOUND_QUEUE_CAPACITY + 32;
        for _ in 0..queued_frames {
            connection.try_send(vec![0; 1024], deadline).unwrap();
        }
        assert!(connection.queued_write_bytes() >= queued_frames * 1024);
        assert!(matches!(
            connection
                .send(
                    vec![0; 1024 * 1024],
                    Instant::now() + std::time::Duration::from_millis(20),
                )
                .await,
            Err(TransportError::Timeout)
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), connection.join())
            .await
            .unwrap();
        assert!(connection.is_closed());
    }

    #[tokio::test]
    async fn pending_calls_complete_on_shutdown_and_owned_tasks_join() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_seen_tx, request_seen_rx) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut first_byte = [0; 1];
            tokio::io::AsyncReadExt::read_exact(&mut stream, &mut first_byte)
                .await
                .unwrap();
            request_seen_tx.send(()).unwrap();
            let mut remaining = [0; 64];
            loop {
                if tokio::io::AsyncReadExt::read(&mut stream, &mut remaining)
                    .await
                    .unwrap()
                    == 0
                {
                    break;
                }
            }
        });
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let invoke_client = client.clone();
        let invoke = tokio::spawn(async move {
            invoke_client
                .invoke(
                    &address.to_string(),
                    test_command(),
                    Instant::now() + std::time::Duration::from_secs(2),
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), request_seen_rx)
            .await
            .expect("peer did not receive the pending request")
            .unwrap();
        client.shutdown().await;
        assert!(matches!(
            invoke.await.unwrap(),
            Err(TransportError::Shutdown)
        ));
        assert!(client.inner.tasks.lock().unwrap().is_empty());
        assert!(client.inner.connections.lock().unwrap().is_empty());
        assert!(client.inner.pending.lock().unwrap().is_empty());
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_returns_while_owned_blocking_callback_drains() {
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let started = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Arc::new(Mutex::new(release_rx));
        let callback_started = started.clone();
        let callback_finished = finished.clone();
        let callback_release = release_rx.clone();
        let future = ResponseFuture::from_result(
            None,
            test_command(),
            Err(TransportError::Shutdown),
            1000,
            Instant::now(),
            Arc::new(move |_: Result<RemotingCommand, TransportError>| {
                callback_started.store(true, Ordering::Release);
                callback_release.lock().unwrap().recv().unwrap();
                callback_finished.store(true, Ordering::Release);
            }),
            None,
        );
        client.inner.dispatch_callback(CallbackJob {
            future,
            channel_established: Arc::new(AtomicBool::new(true)),
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();

        let started_at = Instant::now();
        client.shutdown().await;
        assert!(started_at.elapsed() < std::time::Duration::from_millis(250));
        assert!(!finished.load(Ordering::Acquire));
        release_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !finished.load(Ordering::Acquire) {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let mut cleanup_stopped = client.inner.shutdown_cleanup_stopped.subscribe();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !*cleanup_stopped.borrow() {
                cleanup_stopped.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn shutdown_waits_for_channel_event_worker_only_until_java_join_limit() {
        struct SlowListener {
            entered: Notify,
            release: Mutex<std::sync::mpsc::Receiver<()>>,
            finished: AtomicBool,
        }
        impl ChannelEventListener for SlowListener {
            fn on_connect(&self, _: &str, _: &ChannelHandle) {
                self.entered.notify_one();
                self.release.lock().unwrap().recv().unwrap();
                self.finished.store(true, Ordering::Release);
            }
        }

        let (release, release_rx) = std::sync::mpsc::channel();
        let listener = Arc::new(SlowListener {
            entered: Notify::new(),
            release: Mutex::new(release_rx),
            finished: AtomicBool::new(false),
        });
        let client = RemotingClient::with_config_and_channel_event_listener(
            ClientConfig::default(),
            Some(listener.clone()),
        )
        .unwrap();
        client.start().unwrap();
        client
            .inner
            .notify_channel(NettyEventType::Connect, ChannelHandle::pending("peer"));
        listener.entered.notified().await;

        let shutdown_client = client.clone();
        let shutdown = tokio::spawn(async move {
            shutdown_client
                .shutdown_with_timeout(std::time::Duration::from_millis(30))
                .await;
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !client.inner.closed.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let accepted_task_finished = Arc::new(AtomicBool::new(false));
        let accepted_task_finished_in_task = accepted_task_finished.clone();
        client
            .inner
            .public_executor
            .execute(Box::new(move || {
                accepted_task_finished_in_task.store(true, Ordering::Release);
            }))
            .expect("public executor remains open until the channel event join ends");

        let started_at = Instant::now();
        shutdown.await.unwrap();
        assert!(started_at.elapsed() >= std::time::Duration::from_millis(20));
        assert!(started_at.elapsed() < std::time::Duration::from_millis(250));
        assert!(!listener.finished.load(Ordering::Acquire));
        assert!(!*client.inner.channel_event_stopped.subscribe().borrow());
        assert!(
            client
                .inner
                .public_executor
                .execute(Box::new(|| {}))
                .is_err()
        );

        release.send(()).unwrap();
        let mut stopped = client.inner.channel_event_stopped.subscribe();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !*stopped.borrow() {
                stopped.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert!(listener.finished.load(Ordering::Acquire));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !accepted_task_finished.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut cleanup_stopped = client.inner.shutdown_cleanup_stopped.subscribe();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !*cleanup_stopped.borrow() {
                cleanup_stopped.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn shutdown_is_idempotent_and_new_calls_are_rejected() {
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        client.shutdown().await;
        client.shutdown().await;
        assert!(matches!(
            client
                .invoke(
                    "127.0.0.1:1",
                    test_command(),
                    Instant::now() + std::time::Duration::from_secs(1)
                )
                .await,
            Err(TransportError::Shutdown)
        ));
    }
}
