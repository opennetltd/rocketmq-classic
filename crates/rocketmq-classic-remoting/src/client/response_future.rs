use std::{
    fmt,
    sync::{
        Arc, Condvar, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use tokio::{sync::OwnedSemaphorePermit, time::Instant as TokioInstant};

use crate::{codec::frame, command::RemotingCommand, error::TransportError};

use super::{InvokeCallback, connection};

/// A Rust handle for the Netty channel exposed by Java remoting callbacks.
///
/// The handle keeps channel identity after close events while allowing live operations when the
/// connection is still active. `write_and_flush` encodes a RocketMQ command before enqueueing it
/// on the connection's owned writer task.
///
/// Connection generation is internal transport bookkeeping and is not part of this public API:
///
/// ```compile_fail
/// use rocketmq_classic_remoting::client::ChannelHandle;
/// let channel: ChannelHandle = loop {};
/// let _generation = channel.generation();
/// ```
#[derive(Clone)]
pub struct ChannelHandle {
    state: Arc<ChannelHandleState>,
}

struct ChannelHandleState {
    remote_addr: String,
    connection: Mutex<Option<Weak<connection::Connection>>>,
    closed_events: tokio::sync::watch::Sender<bool>,
    closed: Arc<AtomicBool>,
    active: AtomicBool,
    writable: Arc<AtomicBool>,
}

impl ChannelHandle {
    pub(super) fn pending(remote_addr: &str) -> Self {
        let (closed_events, _) = tokio::sync::watch::channel(false);
        Self {
            state: Arc::new(ChannelHandleState {
                remote_addr: super::redact_peer_address(remote_addr),
                connection: Mutex::new(None),
                closed_events,
                closed: Arc::new(AtomicBool::new(false)),
                active: AtomicBool::new(false),
                writable: Arc::new(AtomicBool::new(true)),
            }),
        }
    }

    pub(super) fn attach(&self, connection: &Arc<connection::Connection>) {
        *self.state.connection.lock().unwrap() = Some(Arc::downgrade(connection));
        if !self.is_closed() {
            self.state.active.store(true, Ordering::Release);
        }
    }

    pub(super) fn mark_closed(&self) {
        self.state.closed.store(true, Ordering::Release);
        self.state.active.store(false, Ordering::Release);
        self.state.closed_events.send_replace(true);
    }

    pub(super) fn closed_flag(&self) -> Arc<AtomicBool> {
        self.state.closed.clone()
    }

    pub(super) fn writable_flag(&self) -> Arc<AtomicBool> {
        self.state.writable.clone()
    }

    pub(super) fn subscribe_closed(&self) -> tokio::sync::watch::Receiver<bool> {
        self.state.closed_events.subscribe()
    }

    fn connection(&self) -> Option<Arc<connection::Connection>> {
        self.state
            .connection
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)
    }

    /// Returns the peer address recorded for this channel, with URI credentials redacted.
    pub fn remote_addr(&self) -> &str {
        &self.state.remote_addr
    }

    /// Reports whether the channel object remains open, including while connection is pending.
    pub fn is_open(&self) -> bool {
        !self.is_closed()
    }

    /// Reports whether the connection has completed setup and remains active.
    pub fn is_active(&self) -> bool {
        self.state.active.load(Ordering::Acquire) && !self.is_closed()
    }

    /// Reports whether the live connection is below its configured write watermarks.
    pub fn is_writable(&self) -> bool {
        self.is_active() && self.state.writable.load(Ordering::Acquire)
    }

    /// Closes this channel and lets the connection owner complete pending requests.
    pub fn close(&self) -> bool {
        if self.state.closed.swap(true, Ordering::AcqRel) {
            return false;
        }
        self.state.active.store(false, Ordering::Release);
        self.state.closed_events.send_replace(true);
        if let Some(connection) = self.connection() {
            connection.close();
        }
        true
    }

    /// Encodes and writes one command through the live channel's owned writer.
    pub async fn write_and_flush(
        &self,
        command: &RemotingCommand,
        deadline: TokioInstant,
    ) -> Result<(), TransportError> {
        let Some(connection) = self.connection().filter(|_| self.is_active()) else {
            return Err(TransportError::Closed);
        };
        connection.send(frame::encode(command)?, deadline).await
    }

    /// Enqueues a command for this channel without an operation deadline.
    ///
    /// The returned future reports the local socket write result. Dropping it does not cancel the
    /// already-enqueued write; connection shutdown terminates a write that cannot complete.
    pub fn write_and_flush_without_deadline(
        &self,
        command: &RemotingCommand,
    ) -> Result<
        impl std::future::Future<Output = Result<(), TransportError>> + Send + 'static,
        TransportError,
    > {
        let Some(connection) = self.connection().filter(|_| self.is_active()) else {
            return Err(TransportError::Closed);
        };
        let written = connection.enqueue_without_deadline(frame::encode(command)?)?;
        Ok(async move { written.await.map_err(|_| TransportError::Closed)? })
    }

    fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }
}

impl fmt::Debug for ChannelHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChannelHandle")
            .field("remote_addr", &self.remote_addr())
            .field("open", &self.is_open())
            .field("active", &self.is_active())
            .finish()
    }
}

impl PartialEq for ChannelHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
}

impl Eq for ChannelHandle {}

/// Callback metadata and completion state corresponding to Java's public `ResponseFuture`.
///
/// The command objects are owned values. Use the `with_*_mut` methods to mutate a command held
/// by the future, preserving Java's live getter/setter behavior without exposing a lock guard.
///
/// The transport's owned-permit constructor is an internal implementation detail:
///
/// ```compile_fail
/// use rocketmq_classic_remoting::client::ResponseFuture;
/// ResponseFuture::new_with_permit(None, 1, None, 1000, None, None);
/// ```
pub struct ResponseFuture {
    channel: Option<ChannelHandle>,
    opaque: i32,
    request_command: Mutex<Option<RemotingCommand>>,
    response_command: Mutex<Option<RemotingCommand>>,
    timeout_millis: i64,
    begin_timestamp: SystemTime,
    begin_instant: TokioInstant,
    invoke_callback: Option<Arc<dyn InvokeCallback>>,
    cause: Mutex<Option<TransportError>>,
    send_request_ok: AtomicBool,
    interrupted: AtomicBool,
    callback_executed: AtomicBool,
    released: Mutex<Option<PermitRelease>>,
    response_latch: Arc<ResponseLatch>,
}

#[derive(Default)]
struct ResponseLatch {
    complete: Mutex<bool>,
    changed: Condvar,
}

/// A Rust-native interrupt signal for one blocking response wait.
///
/// Pass a clone to the thread that calls `wait_response_interruptible`; calling
/// `interrupt` wakes that wait and causes it to return `ResponseWaitError::Interrupted`.
/// The interruption is consumed by that return, matching Java's clearing of the
/// waiting thread's interrupt flag when `CountDownLatch.await` throws.
#[derive(Clone, Default)]
pub struct ResponseWaitInterrupt {
    state: Arc<ResponseWaitInterruptState>,
}

#[derive(Default)]
struct ResponseWaitInterruptState {
    interrupted: AtomicBool,
    latch: Mutex<Option<Weak<ResponseLatch>>>,
}

impl ResponseWaitInterrupt {
    pub fn new() -> Self {
        Self::default()
    }

    /// Interrupts the currently registered wait, or the next wait if none has started.
    pub fn interrupt(&self) {
        self.state.interrupted.store(true, Ordering::Release);
        if let Some(latch) = self
            .state
            .latch
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)
        {
            let _complete = latch.complete.lock().unwrap();
            latch.changed.notify_all();
        }
    }

    fn register(&self, latch: &Arc<ResponseLatch>) {
        *self.state.latch.lock().unwrap() = Some(Arc::downgrade(latch));
        if self.state.interrupted.load(Ordering::Acquire) {
            let _complete = latch.complete.lock().unwrap();
            latch.changed.notify_all();
        }
    }

    fn consume_interrupt(&self) -> bool {
        self.state.interrupted.swap(false, Ordering::AcqRel)
    }
}

/// Failure returned when a blocking `ResponseFuture` wait is interrupted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseWaitError {
    Interrupted,
}

impl ResponseWaitError {
    pub fn java_exception_name(self) -> &'static str {
        match self {
            Self::Interrupted => "java.lang.InterruptedException",
        }
    }
}

impl fmt::Display for ResponseWaitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Interrupted => "InterruptedException",
        })
    }
}

impl std::error::Error for ResponseWaitError {}

/// Java-compatible release-once wrapper for a semaphore.
///
/// `release` adds one permit at most once. The semaphore handle remains available after release,
/// matching Java's `SemaphoreReleaseOnlyOnce.getSemaphore()` behavior.
pub struct SemaphoreReleaseOnlyOnce {
    semaphore: Option<Arc<tokio::sync::Semaphore>>,
    released: AtomicBool,
}

impl SemaphoreReleaseOnlyOnce {
    /// Retains the supplied semaphore; `None` represents Java's nullable constructor input.
    pub fn new(semaphore: Option<Arc<tokio::sync::Semaphore>>) -> Self {
        Self {
            semaphore,
            released: AtomicBool::new(false),
        }
    }

    /// Adds one semaphore permit once. A missing semaphore is a no-op.
    pub fn release(&self) {
        if let Some(semaphore) = &self.semaphore {
            if !self.released.swap(true, Ordering::AcqRel) {
                semaphore.add_permits(1);
            }
        }
    }

    /// Returns the same semaphore handle passed to `new`.
    pub fn semaphore(&self) -> Option<Arc<tokio::sync::Semaphore>> {
        self.semaphore.clone()
    }
}

enum PermitRelease {
    Owned(Option<OwnedSemaphorePermit>),
    JavaOnce(Arc<SemaphoreReleaseOnlyOnce>),
}

impl ResponseFuture {
    /// Constructs a callback context with the same metadata available from Java's public
    /// `ResponseFuture` constructors. The remoting client normally constructs these internally.
    pub fn new(
        channel: Option<ChannelHandle>,
        opaque: i32,
        request_command: Option<RemotingCommand>,
        timeout_millis: i64,
        invoke_callback: Option<Arc<dyn InvokeCallback>>,
    ) -> Self {
        Self::new_with_permit(
            channel,
            opaque,
            request_command,
            timeout_millis,
            invoke_callback,
            None,
        )
    }

    /// Constructs a future with Java's nullable semaphore-release helper argument.
    pub fn new_with_release_once(
        channel: Option<ChannelHandle>,
        opaque: i32,
        request_command: Option<RemotingCommand>,
        timeout_millis: i64,
        invoke_callback: Option<Arc<dyn InvokeCallback>>,
        release_once: Option<Arc<SemaphoreReleaseOnlyOnce>>,
    ) -> Self {
        Self::new_with_release(
            channel,
            opaque,
            request_command,
            timeout_millis,
            invoke_callback,
            release_once.map(PermitRelease::JavaOnce),
        )
    }

    /// Constructs a future that owns an admission permit released by `release`.
    /// Dropping the future also releases this transport-owned permit through RAII.
    fn new_with_permit(
        channel: Option<ChannelHandle>,
        opaque: i32,
        request_command: Option<RemotingCommand>,
        timeout_millis: i64,
        invoke_callback: Option<Arc<dyn InvokeCallback>>,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        Self::new_with_release(
            channel,
            opaque,
            request_command,
            timeout_millis,
            invoke_callback,
            permit.map(|permit| PermitRelease::Owned(Some(permit))),
        )
    }

    fn new_with_release(
        channel: Option<ChannelHandle>,
        opaque: i32,
        request_command: Option<RemotingCommand>,
        timeout_millis: i64,
        invoke_callback: Option<Arc<dyn InvokeCallback>>,
        permit: Option<PermitRelease>,
    ) -> Self {
        let begin_timestamp = SystemTime::now();
        Self {
            channel,
            opaque,
            request_command: Mutex::new(request_command),
            response_command: Mutex::new(None),
            timeout_millis,
            begin_timestamp,
            begin_instant: TokioInstant::now(),
            invoke_callback,
            cause: Mutex::new(None),
            send_request_ok: AtomicBool::new(true),
            interrupted: AtomicBool::new(false),
            callback_executed: AtomicBool::new(false),
            released: Mutex::new(permit),
            response_latch: Arc::new(ResponseLatch::default()),
        }
    }

    pub(super) fn from_result(
        channel: Option<ChannelHandle>,
        request_command: RemotingCommand,
        result: Result<RemotingCommand, TransportError>,
        timeout_millis: i64,
        begin_instant: TokioInstant,
        invoke_callback: Arc<dyn InvokeCallback>,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Arc<Self> {
        let elapsed = begin_instant.elapsed();
        let begin_timestamp = SystemTime::now().checked_sub(elapsed).unwrap_or(UNIX_EPOCH);
        let (response_command, cause) = match result {
            Ok(response) => (Some(response), None),
            Err(error) => (None, Some(error)),
        };
        Arc::new(Self {
            channel,
            opaque: request_command.opaque,
            request_command: Mutex::new(Some(request_command)),
            response_command: Mutex::new(response_command),
            timeout_millis,
            begin_timestamp,
            begin_instant,
            invoke_callback: Some(invoke_callback),
            cause: Mutex::new(cause),
            // invokeAsyncImpl passes a wrapper-created ResponseFuture to the callback on
            // exceptional completion, so its public sendRequestOK default remains true.
            send_request_ok: AtomicBool::new(true),
            interrupted: AtomicBool::new(false),
            callback_executed: AtomicBool::new(false),
            released: Mutex::new(permit.map(|permit| PermitRelease::Owned(Some(permit)))),
            response_latch: Arc::new(ResponseLatch::default()),
        })
    }

    pub fn channel(&self) -> Option<ChannelHandle> {
        self.channel.clone()
    }

    pub fn opaque(&self) -> i32 {
        self.opaque
    }

    pub fn request_command(&self) -> Option<RemotingCommand> {
        self.request_command.lock().unwrap().clone()
    }

    pub fn with_request_command_mut<R>(
        &self,
        operation: impl FnOnce(Option<&mut RemotingCommand>) -> R,
    ) -> R {
        operation(self.request_command.lock().unwrap().as_mut())
    }

    pub fn response_command(&self) -> Option<RemotingCommand> {
        self.response_command.lock().unwrap().clone()
    }

    pub fn set_response_command(&self, response: Option<RemotingCommand>) {
        *self.response_command.lock().unwrap() = response;
    }

    pub fn with_response_command_mut<R>(
        &self,
        operation: impl FnOnce(Option<&mut RemotingCommand>) -> R,
    ) -> R {
        operation(self.response_command.lock().unwrap().as_mut())
    }

    pub fn cause(&self) -> Option<TransportError> {
        self.cause.lock().unwrap().clone()
    }

    pub fn set_cause(&self, cause: Option<TransportError>) {
        *self.cause.lock().unwrap() = cause;
    }

    pub fn timeout_millis(&self) -> i64 {
        self.timeout_millis
    }

    pub fn is_timeout(&self) -> bool {
        let elapsed_millis = self
            .begin_instant
            .elapsed()
            .as_millis()
            .min(i64::MAX as u128) as i64;
        elapsed_millis > self.timeout_millis
    }

    pub fn begin_timestamp_millis(&self) -> i64 {
        self.begin_timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64
    }

    pub fn invoke_callback(&self) -> Option<Arc<dyn InvokeCallback>> {
        self.invoke_callback.clone()
    }

    pub fn is_send_request_ok(&self) -> bool {
        self.send_request_ok.load(Ordering::Acquire)
    }

    pub fn set_send_request_ok(&self, value: bool) {
        self.send_request_ok.store(value, Ordering::Release);
    }

    pub fn is_interrupted(&self) -> bool {
        self.interrupted.load(Ordering::Acquire)
    }

    /// Stores the response and releases threads waiting in `wait_response`.
    pub fn put_response(&self, response: Option<RemotingCommand>) {
        self.set_response_command(response);
        *self.response_latch.complete.lock().unwrap() = true;
        self.response_latch.changed.notify_all();
    }

    /// Waits up to `timeout` for `put_response`, then returns the current response value.
    pub fn wait_response(&self, timeout_millis: i64) -> Option<RemotingCommand> {
        self.wait_response_inner(timeout_millis, None)
            .expect("an uninterruptible response wait cannot be interrupted")
    }

    /// Waits for a response with a Rust-native external interruption signal.
    ///
    /// The timeout result matches `wait_response`: it returns the current response,
    /// which is `None` when no response arrived. An interruption returns an error
    /// and is consumed by the wait.
    pub fn wait_response_interruptible(
        &self,
        timeout_millis: i64,
        interrupt: &ResponseWaitInterrupt,
    ) -> Result<Option<RemotingCommand>, ResponseWaitError> {
        self.wait_response_inner(timeout_millis, Some(interrupt))
    }

    fn wait_response_inner(
        &self,
        timeout_millis: i64,
        interrupt: Option<&ResponseWaitInterrupt>,
    ) -> Result<Option<RemotingCommand>, ResponseWaitError> {
        if let Some(interrupt) = interrupt {
            interrupt.register(&self.response_latch);
        }
        let timeout = Duration::from_millis(timeout_millis.max(0) as u64);
        let mut complete = self.response_latch.complete.lock().unwrap();
        let started = Instant::now();
        loop {
            if interrupt.is_some_and(ResponseWaitInterrupt::consume_interrupt) {
                return Err(ResponseWaitError::Interrupted);
            }
            if *complete {
                return Ok(self.response_command());
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Ok(self.response_command());
            }
            let (next, _) = self
                .response_latch
                .changed
                .wait_timeout(complete, remaining)
                .unwrap();
            complete = next;
        }
    }

    /// Runs the callback once, following Java `ResponseFuture.executeInvokeCallback` ordering.
    pub fn execute_invoke_callback(&self) {
        if self.callback_executed.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(callback) = &self.invoke_callback else {
            return;
        };
        match self.execute_callback_result() {
            Ok(response) => callback.operation_succeed(&response),
            Err(error) => callback.operation_fail(&error),
        }
        callback.operation_complete(self);
    }

    /// Marks the future interrupted and attempts its callback, as Java does.
    pub fn interrupt(&self) {
        self.interrupted.store(true, Ordering::Release);
        self.execute_invoke_callback();
    }

    /// Releases async admission exactly once.
    pub fn release(&self) {
        let mut released = self.released.lock().unwrap();
        match released.as_mut() {
            Some(PermitRelease::Owned(permit)) => {
                permit.take();
            }
            Some(PermitRelease::JavaOnce(release_once)) => release_once.release(),
            None => {}
        }
    }

    pub(super) fn dispatch_async_callback(&self, channel_established: bool) {
        if self.callback_executed.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(callback) = &self.invoke_callback else {
            return;
        };
        if channel_established {
            callback.operation_complete(self);
        }
        match self.callback_result() {
            Ok(response) => callback.operation_succeed(&response),
            Err(error) => callback.operation_fail(&error),
        }
    }

    fn callback_result(&self) -> Result<RemotingCommand, TransportError> {
        if let Some(response) = self.response_command() {
            return Ok(response);
        }
        self.cause()
            .map_or_else(|| Err(self.default_failure()), Err)
    }

    fn execute_callback_result(&self) -> Result<RemotingCommand, TransportError> {
        if let Some(response) = self.response_command() {
            return Ok(response);
        }
        let cause = self.cause();
        if !self.is_send_request_ok() {
            return Err(TransportError::SendRequest {
                remote_addr: self
                    .channel
                    .as_ref()
                    .map(|channel| channel.remote_addr().to_owned())
                    .unwrap_or_default(),
                cause: cause.map(crate::error::ErrorSource::new),
            });
        }
        if self.is_timeout() {
            return Err(TransportError::timeout_with_cause(
                crate::error::TimeoutPhase::Response,
                self.channel
                    .as_ref()
                    .map(|channel| channel.remote_addr().to_owned()),
                Some(Duration::from_millis(self.timeout_millis.max(0) as u64)),
                cause.map(crate::error::ErrorSource::new),
            ));
        }
        let request = self.request_description();
        Err(match cause {
            Some(cause) => TransportError::RemotingWithCause {
                request,
                cause: crate::error::ErrorSource::new(cause),
            },
            None => TransportError::Remoting(request),
        })
    }

    fn default_failure(&self) -> TransportError {
        TransportError::Remoting(self.request_description())
    }

    fn request_description(&self) -> String {
        format!(
            "{:?}",
            self.request_command()
                .expect("Java ResponseFuture formats its request in generic failure")
        )
    }
}

impl fmt::Debug for ResponseFuture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResponseFuture")
            .field("response_command", &self.response_command())
            .field("send_request_ok", &self.is_send_request_ok())
            .field("cause", &self.cause())
            .field("opaque", &self.opaque)
            .field("timeout_millis", &self.timeout_millis)
            .field("begin_timestamp", &self.begin_timestamp)
            .finish()
    }
}

impl fmt::Display for ResponseFuture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let response = self
            .response_command()
            .map_or_else(|| "null".to_owned(), |response| response.to_string());
        let cause = self
            .cause()
            .map_or_else(|| "null".to_owned(), |cause| cause.to_string());
        let callback = if self.invoke_callback.is_some() {
            "<Rust callback>"
        } else {
            "null"
        };
        let latch_count = if *self.response_latch.complete.lock().unwrap() {
            0
        } else {
            1
        };
        write!(
            formatter,
            "ResponseFuture [responseCommand={response}, sendRequestOK={}, cause={cause}, opaque={}, timeoutMillis={}, invokeCallback={callback}, beginTimestamp={}, countDownLatch=CountDownLatch [Count = {latch_count}]]",
            self.is_send_request_ok(),
            self.opaque,
            self.timeout_millis,
            self.begin_timestamp_millis(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use rocketmq_classic_protocol::codes::SerializeType;

    use super::{ChannelHandle, ResponseFuture, ResponseWaitInterrupt, SemaphoreReleaseOnlyOnce};
    use crate::{client::InvokeCallback, command::RemotingCommand, error::TransportError};

    struct RecordingCallback(Arc<Mutex<Vec<&'static str>>>);

    impl InvokeCallback for RecordingCallback {
        fn operation_complete(&self, _future: &ResponseFuture) {
            self.0.lock().unwrap().push("complete");
        }

        fn operation_succeed(&self, _response: &RemotingCommand) {
            self.0.lock().unwrap().push("succeed");
        }

        fn operation_fail(&self, _error: &TransportError) {
            self.0.lock().unwrap().push("fail");
        }
    }

    struct FailureCallback(Arc<Mutex<Option<TransportError>>>);

    impl InvokeCallback for FailureCallback {
        fn operation_fail(&self, error: &TransportError) {
            *self.0.lock().unwrap() = Some(error.clone());
        }
    }

    struct RecordingFailureCallback {
        events: Arc<Mutex<Vec<&'static str>>>,
        error: Arc<Mutex<Option<TransportError>>>,
    }

    impl InvokeCallback for RecordingFailureCallback {
        fn operation_complete(&self, _future: &ResponseFuture) {
            self.events.lock().unwrap().push("complete");
        }

        fn operation_fail(&self, error: &TransportError) {
            *self.error.lock().unwrap() = Some(error.clone());
            self.events.lock().unwrap().push("fail");
        }
    }

    #[test]
    fn response_future_accessors_mutations_and_callback_order_match_java_surface() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let callback = Arc::new(RecordingCallback(events.clone()));
        let request = RemotingCommand {
            code: 11,
            opaque: 42,
            ..Default::default()
        };
        let response = RemotingCommand {
            code: 12,
            opaque: 42,
            ..Default::default()
        };
        let channel = ChannelHandle::pending("broker:10911");
        let future = ResponseFuture::new(
            Some(channel.clone()),
            42,
            Some(request),
            1000,
            Some(callback.clone()),
        );

        assert_eq!(future.opaque(), 42);
        assert_eq!(future.timeout_millis(), 1000);
        assert!(future.begin_timestamp_millis() > 0);
        assert!(Arc::ptr_eq(
            &future.invoke_callback().unwrap(),
            &(callback.clone() as Arc<dyn InvokeCallback>)
        ));
        let returned_channel = future.channel().unwrap();
        assert_eq!(returned_channel, channel);
        assert_eq!(returned_channel.remote_addr(), "broker:10911");
        assert_eq!(future.request_command().unwrap().code, 11);
        future.with_request_command_mut(|mut request| request.as_mut().unwrap().code = 13);
        assert_eq!(future.request_command().unwrap().code, 13);
        assert!(future.is_send_request_ok());
        future.set_send_request_ok(false);
        assert!(!future.is_send_request_ok());
        future.set_response_command(Some(response.clone()));
        future.with_response_command_mut(|mut value| {
            value.as_mut().unwrap().remark = Some("seen".into())
        });
        assert_eq!(
            future.response_command().unwrap().remark.as_deref(),
            Some("seen")
        );

        future.execute_invoke_callback();
        future.execute_invoke_callback();
        assert_eq!(*events.lock().unwrap(), vec!["succeed", "complete"]);
    }

    #[test]
    fn response_future_display_preserves_java_to_string_state_fields() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/remoting/response-future-api.json"
        ))
        .unwrap();
        let java = &fixture["responseFutureToStringBehavior"];
        let future = ResponseFuture::new(None, 42, None, 1000, None);
        let display = future.to_string();

        assert_eq!(
            java["responseCommandNull"],
            display.contains("responseCommand=null")
        );
        assert_eq!(java["causeNull"], display.contains("cause=null"));
        assert_eq!(
            java["callbackNull"],
            display.contains("invokeCallback=null")
        );
        assert_eq!(java["opaque"], 42);
        assert_eq!(java["timeoutMillis"], 1000);
        assert!(display.contains("opaque=42"));
        assert!(display.contains("timeoutMillis=1000"));
        assert!(display.contains(&format!(
            "beginTimestamp={}",
            future.begin_timestamp_millis()
        )));
        assert_eq!(java["latchCount"], 1);
        assert!(display.contains("countDownLatch=CountDownLatch [Count = 1]"));

        let mut response =
            RemotingCommand::create_response_command(206, Some("fixture".into())).unwrap();
        response.opaque = 42;
        response.version = 0;
        response.serialization = SerializeType::Json;
        let java_rendering = java["responseCommandRendering"].as_str().unwrap();
        assert_eq!(response.to_string(), java_rendering);
        future.set_response_command(Some(response));
        assert_eq!(java["responseCommandEmbedded"], true);
        assert!(
            future
                .to_string()
                .contains(&format!("responseCommand={java_rendering}, sendRequestOK="))
        );

        future.put_response(Some(RemotingCommand::default()));
        assert_eq!(java["latchCountAfterResponse"], 0);
        assert!(
            future
                .to_string()
                .contains("countDownLatch=CountDownLatch [Count = 0]")
        );
    }

    #[test]
    fn wait_response_timeout_and_external_interrupt_match_java_mapping() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/remoting/response-future-api.json"
        ))
        .unwrap();
        let java = &fixture["responseFutureWaitResponseInterruptionBehavior"];
        assert_eq!(java["exceptionClass"], "java.lang.InterruptedException");
        assert_eq!(java["returnedResponseNull"], true);
        assert_eq!(java["interruptFlagCleared"], true);

        let future = ResponseFuture::new(None, 55, None, 5000, None);
        assert!(future.wait_response(1).is_none());
        assert!(!future.is_interrupted());
        future.interrupt();
        assert!(future.is_interrupted());

        let future = Arc::new(ResponseFuture::new(None, 55, None, 5000, None));
        let interrupt = ResponseWaitInterrupt::new();
        let waiter_interrupt = interrupt.clone();
        let wait_future = future.clone();
        let waiter = std::thread::spawn(move || {
            wait_future.wait_response_interruptible(5000, &waiter_interrupt)
        });
        std::thread::sleep(std::time::Duration::from_millis(10));
        interrupt.interrupt();
        let error = waiter
            .join()
            .unwrap()
            .expect_err("external interruption must terminate the response wait");
        assert_eq!(
            error.java_exception_name(),
            java["exceptionClass"].as_str().unwrap()
        );
        assert!(
            future
                .wait_response_interruptible(1, &interrupt)
                .expect("the interruption is consumed when the wait returns")
                .is_none()
        );

        let completed = ResponseFuture::new(None, 56, None, 5000, None);
        completed.put_response(Some(RemotingCommand {
            code: 17,
            ..Default::default()
        }));
        let already_interrupted = ResponseWaitInterrupt::new();
        already_interrupted.interrupt();
        assert_eq!(
            completed
                .wait_response_interruptible(5000, &already_interrupted)
                .unwrap_err(),
            super::ResponseWaitError::Interrupted
        );
        assert_eq!(
            completed
                .wait_response_interruptible(0, &already_interrupted)
                .unwrap()
                .unwrap()
                .code,
            17
        );
    }

    #[test]
    fn put_response_unblocks_waiters_and_setter_alone_does_not() {
        let response = RemotingCommand {
            code: 19,
            ..Default::default()
        };
        let future = Arc::new(ResponseFuture::new(None, 0, None, 1000, None));
        future.set_response_command(Some(response.clone()));
        let wait_future = future.clone();
        let waiter = std::thread::spawn(move || {
            let started = std::time::Instant::now();
            let response = wait_future.wait_response(25);
            (response, started.elapsed())
        });
        let (received, elapsed) = waiter.join().unwrap();
        let received = received.expect("put_response must wake the waiter with a command");
        assert_eq!(received.code, response.code);
        assert_eq!(received.opaque, response.opaque);
        assert!(elapsed >= std::time::Duration::from_millis(15));

        let future = Arc::new(ResponseFuture::new(None, 0, None, 1000, None));
        let wait_future = future.clone();
        let waiter = std::thread::spawn(move || wait_future.wait_response(1000));
        std::thread::sleep(std::time::Duration::from_millis(10));
        future.put_response(Some(response.clone()));
        let received = waiter
            .join()
            .unwrap()
            .expect("put_response must wake the waiter with a command");
        assert_eq!(received.code, response.code);
        assert_eq!(received.opaque, response.opaque);
    }

    #[test]
    fn interruption_and_release_are_idempotent() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let future = ResponseFuture::new(
            None,
            0,
            Some(RemotingCommand::default()),
            1000,
            Some(Arc::new(RecordingCallback(events.clone()))),
        );
        future.interrupt();
        future.interrupt();
        future.release();
        future.release();
        assert!(future.is_interrupted());
        assert_eq!(*events.lock().unwrap(), vec!["fail", "complete"]);

        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore.clone().try_acquire_owned().unwrap();
        let future = ResponseFuture::from_result(
            None,
            RemotingCommand::default(),
            Err(TransportError::Shutdown),
            1000,
            tokio::time::Instant::now(),
            Arc::new(|_: Result<RemotingCommand, TransportError>| {}),
            Some(permit),
        );
        assert_eq!(semaphore.available_permits(), 0);
        future.release();
        future.release();
        assert_eq!(semaphore.available_permits(), 1);

        let permit = semaphore.clone().try_acquire_owned().unwrap();
        let future = ResponseFuture::new_with_permit(None, 0, None, 1000, None, Some(permit));
        assert_eq!(semaphore.available_permits(), 0);
        future.release();
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn semaphore_release_once_matches_pinned_java_public_api_and_behavior() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/remoting/response-future-api.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["sourceCommit"],
            "efa1be59d6fc747ad220f036c86030f05f0419f9"
        );
        assert_eq!(
            fixture["responseFuturePublicConstructors"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            fixture["responseFuturePublicMethods"]
                .as_array()
                .unwrap()
                .len(),
            20
        );
        assert_eq!(
            fixture["semaphoreReleaseOnlyOncePublicConstructors"][0],
            "org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce(java.util.concurrent.Semaphore)"
        );
        assert_eq!(
            fixture["semaphoreReleaseOnlyOncePublicMethods"],
            serde_json::json!([
                "java.util.concurrent.Semaphore getSemaphore()",
                "void release()"
            ])
        );
        let response_future_constructors = fixture["responseFuturePublicConstructors"]
            .as_array()
            .unwrap();
        assert!(response_future_constructors.iter().any(|signature| {
            signature
                .as_str()
                .unwrap()
                .contains("ResponseFuture(io.netty.channel.Channel,int,long,")
        }));
        assert!(response_future_constructors.iter().any(|signature| {
            signature
                .as_str()
                .unwrap()
                .contains("ResponseFuture(io.netty.channel.Channel,int,org.apache.rocketmq.remoting.protocol.RemotingCommand,")
        }));
        assert_eq!(
            fixture["semaphoreReleaseBehavior"]["permitsAfterRepeatedRelease"],
            1
        );
        assert_eq!(
            fixture["responseFuturePermitReleaseBehavior"]["permitsAfterRepeatedRelease"],
            1
        );

        let semaphore = Arc::new(tokio::sync::Semaphore::new(0));
        let release_once = Arc::new(SemaphoreReleaseOnlyOnce::new(Some(semaphore.clone())));
        let returned = release_once.semaphore().unwrap();
        assert!(Arc::ptr_eq(&returned, &semaphore));
        release_once.release();
        release_once.release();
        assert_eq!(semaphore.available_permits(), 1);

        let future_semaphore = Arc::new(tokio::sync::Semaphore::new(0));
        let future_release_once = Arc::new(SemaphoreReleaseOnlyOnce::new(Some(
            future_semaphore.clone(),
        )));
        let future = ResponseFuture::new_with_release_once(
            None,
            42,
            None,
            1000,
            None,
            Some(future_release_once.clone()),
        );
        assert_eq!(Arc::strong_count(&future_release_once), 2);
        future.release();
        future.release();
        assert_eq!(future.opaque(), 42);
        assert_eq!(future_semaphore.available_permits(), 1);
        assert_eq!(Arc::strong_count(&future_release_once), 2);
        drop(future);
        assert_eq!(Arc::strong_count(&future_release_once), 1);

        let dropped_semaphore = Arc::new(tokio::sync::Semaphore::new(0));
        let dropped_release_once = Arc::new(SemaphoreReleaseOnlyOnce::new(Some(
            dropped_semaphore.clone(),
        )));
        drop(ResponseFuture::new_with_release_once(
            None,
            7,
            None,
            1000,
            None,
            Some(dropped_release_once),
        ));
        assert_eq!(dropped_semaphore.available_permits(), 0);

        let racing_semaphore = Arc::new(tokio::sync::Semaphore::new(0));
        let racing_release_once = Arc::new(SemaphoreReleaseOnlyOnce::new(Some(
            racing_semaphore.clone(),
        )));
        let barrier = Arc::new(std::sync::Barrier::new(9));
        let workers = (0..8)
            .map(|_| {
                let release_once = racing_release_once.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    release_once.release();
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(racing_semaphore.available_permits(), 1);

        let no_semaphore = SemaphoreReleaseOnlyOnce::new(None);
        assert!(no_semaphore.semaphore().is_none());
        no_semaphore.release();
        no_semaphore.release();
    }

    #[test]
    fn direct_callback_failure_matches_java_send_timeout_and_generic_categories() {
        let send_error = Arc::new(Mutex::new(None));
        let send_future = ResponseFuture::new(
            Some(ChannelHandle::pending("broker:10911")),
            0,
            Some(RemotingCommand::default()),
            1000,
            Some(Arc::new(FailureCallback(send_error.clone()))),
        );
        send_future.set_send_request_ok(false);
        send_future.execute_invoke_callback();
        assert_eq!(
            *send_error.lock().unwrap(),
            Some(TransportError::SendRequest {
                remote_addr: "broker:10911".into(),
                cause: None,
            })
        );

        let timeout_error = Arc::new(Mutex::new(None));
        let timeout_future = ResponseFuture::new(
            Some(ChannelHandle::pending("broker:10911")),
            0,
            Some(RemotingCommand::default()),
            -1,
            Some(Arc::new(FailureCallback(timeout_error.clone()))),
        );
        timeout_future.execute_invoke_callback();
        assert!(matches!(
            timeout_error.lock().unwrap().as_ref(),
            Some(TransportError::TimeoutWithContext {
                phase: crate::error::TimeoutPhase::Response,
                remote_addr: Some(remote_addr),
                ..
            }) if remote_addr == "broker:10911"
        ));

        let generic_error = Arc::new(Mutex::new(None));
        let generic_future = ResponseFuture::new(
            None,
            0,
            Some(RemotingCommand {
                code: 42,
                ..Default::default()
            }),
            i64::MAX,
            Some(Arc::new(FailureCallback(generic_error.clone()))),
        );
        generic_future.execute_invoke_callback();
        assert!(matches!(
            generic_error.lock().unwrap().as_ref(),
            Some(TransportError::Remoting(request)) if request.contains("code: 42")
        ));
    }

    #[test]
    fn response_future_callback_failure_causes_and_null_request_match_java_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/remoting/response-future-api.json"
        ))
        .unwrap();
        let java = &fixture["responseFutureCallbackFailureBehavior"];
        for (name, class) in [
            (
                "sendFailure",
                "org.apache.rocketmq.remoting.exception.RemotingSendRequestException",
            ),
            (
                "timeoutFailure",
                "org.apache.rocketmq.remoting.exception.RemotingTimeoutException",
            ),
            (
                "genericFailure",
                "org.apache.rocketmq.remoting.exception.RemotingException",
            ),
        ] {
            assert_eq!(java[name]["failureClass"], class);
            assert_eq!(java[name]["failureCauseSameInstance"], true);
            assert_eq!(
                java[name]["callbackEvents"],
                serde_json::json!(["fail", "complete"])
            );
        }
        assert_eq!(
            java["nullRequestGenericFailure"]["throwable"],
            "java.lang.NullPointerException"
        );
        assert_eq!(
            java["nullRequestGenericFailure"]["callbackEvents"],
            serde_json::json!([])
        );

        let exercise = |timeout_millis, send_request_ok, opaque| {
            let events = Arc::new(Mutex::new(Vec::new()));
            let error = Arc::new(Mutex::new(None));
            let future = ResponseFuture::new(
                Some(ChannelHandle::pending("broker:10911")),
                opaque,
                Some(RemotingCommand {
                    code: 42,
                    ..Default::default()
                }),
                timeout_millis,
                Some(Arc::new(RecordingFailureCallback {
                    events: events.clone(),
                    error: error.clone(),
                })),
            );
            future.set_cause(Some(TransportError::Closed));
            future.set_send_request_ok(send_request_ok);
            future.execute_invoke_callback();
            let events = events.lock().unwrap().clone();
            let error = error.lock().unwrap().clone().unwrap();
            (events, error)
        };

        let (send_events, send_error) = exercise(1000, false, 51);
        assert_eq!(send_events, vec!["fail", "complete"]);
        assert!(matches!(
            send_error,
            TransportError::SendRequest {
                cause: Some(ref cause),
                ..
            } if cause.downcast_ref::<TransportError>() == Some(&TransportError::Closed)
        ));

        let (timeout_events, timeout_error) = exercise(-1, true, 52);
        assert_eq!(timeout_events, vec!["fail", "complete"]);
        assert!(matches!(
            timeout_error,
            TransportError::TimeoutWithContext {
                phase: crate::error::TimeoutPhase::Response,
                cause: Some(ref cause),
                ..
            } if cause.downcast_ref::<TransportError>() == Some(&TransportError::Closed)
        ));

        let (generic_events, generic_error) = exercise(i64::MAX, true, 53);
        assert_eq!(generic_events, vec!["fail", "complete"]);
        assert!(matches!(
            generic_error,
            TransportError::RemotingWithCause { request, cause }
                if request.contains("code: 42")
                    && cause.downcast_ref::<TransportError>() == Some(&TransportError::Closed)
        ));

        let events = Arc::new(Mutex::new(Vec::new()));
        let null_request = ResponseFuture::new(
            Some(ChannelHandle::pending("broker:10911")),
            54,
            None,
            i64::MAX,
            Some(Arc::new(RecordingCallback(events.clone()))),
        );
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                null_request.execute_invoke_callback();
            }))
            .is_err()
        );
        assert!(events.lock().unwrap().is_empty());
    }

    #[test]
    fn channel_handle_exposes_java_channel_identity_address_state_and_close() {
        let channel = ChannelHandle::pending("tls://user:secret@broker:10911");
        assert_eq!(channel, channel.clone());
        assert_ne!(
            channel,
            ChannelHandle::pending("tls://user:secret@broker:10911")
        );
        assert_eq!(channel.remote_addr(), "tls://[REDACTED]@broker:10911");
        assert!(channel.is_open());
        assert!(!channel.is_active());
        assert!(!channel.is_writable());
        assert!(channel.close());
        assert!(!channel.close());
        assert!(!channel.is_open());
    }
}
