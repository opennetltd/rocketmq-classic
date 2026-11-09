mod config;
mod connection;
mod connection_manager;
mod dispatch;
mod invocation;
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
mod tests;
