use rocketmq_classic_common::message::Message;
use std::{
    error::Error,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::watch, time::Instant};

/// Callback contract attached to a Java-compatible request future.
pub trait RequestResponseCallback: Send + Sync + 'static {
    fn on_success(&self, response: Option<Message>);
    fn on_exception(&self, cause: Option<Arc<dyn Error + Send + Sync>>);
}

#[derive(Debug, thiserror::Error)]
#[error("request timeout, no reply message.")]
pub struct RequestTimeoutCause;

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum RequestFutureError {
    #[error("request response future has no count down latch")]
    MissingCountDownLatch,
}

/// Typed send failure delivered through the Java-compatible request callback.
/// Its message is exposed read-only, like Java `Throwable.getMessage()`.
///
/// ```compile_fail
/// use rocketmq_classic_core::runtime::RequestSendFailureCause;
/// let cause = RequestSendFailureCause::new("send failed");
/// let _ = cause.0;
/// ```
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct RequestSendFailureCause(String);

impl RequestSendFailureCause {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    pub fn message(&self) -> &str {
        &self.0
    }
}

/// Rust latch with CountDownLatch-style countdown and async waiting.
#[derive(Clone)]
pub struct RequestFutureLatch {
    count: watch::Sender<usize>,
}

impl RequestFutureLatch {
    pub fn new(count: usize) -> Self {
        let (count, _) = watch::channel(count);
        Self { count }
    }

    pub fn count(&self) -> usize {
        *self.count.borrow()
    }

    pub fn count_down(&self) {
        self.count.send_modify(|current| {
            if *current > 0 {
                *current -= 1;
            }
        });
    }

    /// Returns `true` when the latch reaches zero before `timeout` elapses.
    pub async fn wait(&self, timeout: Duration) -> bool {
        let mut count = self.count.subscribe();
        if *count.borrow() == 0 {
            return true;
        }
        tokio::time::timeout(timeout, async move {
            while *count.borrow_and_update() != 0 {
                if count.changed().await.is_err() {
                    return false;
                }
            }
            true
        })
        .await
        .unwrap_or(false)
    }

    /// Waits until the latch reaches zero, matching `CountDownLatch.await()`.
    pub async fn wait_forever(&self) {
        let mut count = self.count.subscribe();
        while *count.borrow_and_update() != 0 {
            if count.changed().await.is_err() {
                return;
            }
        }
    }
}

/// State shared by the public request future, the runtime correlation table,
/// and inbound reply/timeout completion paths.
pub struct RequestResponseFuture {
    correlation_id: String,
    timeout_millis: AtomicI64,
    begin_timestamp: i64,
    started_at: Instant,
    count_down_latch: RwLock<Option<Arc<RequestFutureLatch>>>,
    response_msg: Mutex<Option<Message>>,
    send_request_ok: AtomicBool,
    cause: Mutex<Option<Arc<dyn Error + Send + Sync>>>,
    request_callback: Option<Arc<dyn RequestResponseCallback>>,
    callback_executed: AtomicBool,
}

impl RequestResponseFuture {
    pub fn new(
        correlation_id: impl Into<String>,
        timeout_millis: i64,
        request_callback: Option<Arc<dyn RequestResponseCallback>>,
    ) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            timeout_millis: AtomicI64::new(timeout_millis),
            begin_timestamp: epoch_millis(SystemTime::now()),
            started_at: Instant::now(),
            count_down_latch: RwLock::new(Some(Arc::new(RequestFutureLatch::new(1)))),
            response_msg: Mutex::new(None),
            send_request_ok: AtomicBool::new(true),
            cause: Mutex::new(None),
            request_callback,
            callback_executed: AtomicBool::new(false),
        }
    }

    pub fn correlation_id(&self) -> &str {
        &self.correlation_id
    }

    pub fn timeout_millis(&self) -> i64 {
        self.timeout_millis.load(Ordering::SeqCst)
    }

    pub fn set_timeout_millis(&self, timeout_millis: i64) {
        self.timeout_millis.store(timeout_millis, Ordering::SeqCst);
    }

    pub fn request_callback(&self) -> Option<Arc<dyn RequestResponseCallback>> {
        self.request_callback.clone()
    }

    pub fn begin_timestamp(&self) -> i64 {
        self.begin_timestamp
    }

    pub fn count_down_latch(&self) -> Option<Arc<RequestFutureLatch>> {
        self.count_down_latch
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn set_count_down_latch(&self, latch: Option<Arc<RequestFutureLatch>>) {
        *self
            .count_down_latch
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = latch;
    }

    pub fn response_msg(&self) -> Option<Message> {
        self.response_msg
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn set_response_msg(&self, response: Option<Message>) {
        *self
            .response_msg
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = response;
    }

    pub fn send_request_ok(&self) -> bool {
        self.send_request_ok.load(Ordering::SeqCst)
    }

    pub fn set_send_request_ok(&self, send_request_ok: bool) {
        self.send_request_ok
            .store(send_request_ok, Ordering::SeqCst);
    }

    pub fn cause(&self) -> Option<Arc<dyn Error + Send + Sync>> {
        self.cause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn set_cause(&self, cause: Option<Arc<dyn Error + Send + Sync>>) {
        *self
            .cause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = cause;
    }

    /// The pinned Java implementation always returns a null request message.
    pub fn request_msg(&self) -> Option<Message> {
        None
    }

    pub fn is_timeout(&self) -> bool {
        epoch_millis(SystemTime::now()).wrapping_sub(self.begin_timestamp) > self.timeout_millis()
    }

    pub(super) fn is_timeout_at(&self, now: Instant) -> bool {
        let timeout_millis = self.timeout_millis();
        if timeout_millis < 0 {
            return true;
        }
        now.saturating_duration_since(self.started_at).as_millis() > timeout_millis as u128
    }

    pub async fn wait_response_message(
        &self,
        timeout: Duration,
    ) -> Result<Option<Message>, RequestFutureError> {
        let latch = self
            .count_down_latch()
            .ok_or(RequestFutureError::MissingCountDownLatch)?;
        let _ = latch.wait(timeout).await;
        Ok(self.response_msg())
    }

    pub fn put_response_message(
        &self,
        response: Option<Message>,
    ) -> Result<(), RequestFutureError> {
        self.set_response_msg(response);
        let latch = self
            .count_down_latch()
            .ok_or(RequestFutureError::MissingCountDownLatch)?;
        latch.count_down();
        Ok(())
    }

    pub fn execute_request_callback(&self) {
        let Some(callback) = self.request_callback.as_ref() else {
            return;
        };
        if self.callback_executed.swap(true, Ordering::SeqCst) {
            return;
        }
        let cause = self.cause();
        if self.send_request_ok() && cause.is_none() {
            callback.on_success(self.response_msg());
        } else {
            callback.on_exception(cause);
        }
    }
}

fn epoch_millis(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(error) => (-(error.duration().as_nanos().div_ceil(1_000_000) as i128)) as i64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Callback(AtomicUsize);

    impl RequestResponseCallback for Callback {
        fn on_success(&self, _response: Option<Message>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn on_exception(&self, _cause: Option<Arc<dyn Error + Send + Sync>>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn mutable_future_state_and_callback_execution_are_shared() {
        let callback = Arc::new(Callback(AtomicUsize::new(0)));
        let future = RequestResponseFuture::new("correlation", 10, Some(callback.clone()));
        assert_eq!(future.correlation_id(), "correlation");
        assert_eq!(future.timeout_millis(), 10);
        assert!(future.send_request_ok());
        assert!(!future.is_timeout());
        assert_eq!(future.count_down_latch().unwrap().count(), 1);

        let latch = Arc::new(RequestFutureLatch::new(2));
        future.set_count_down_latch(Some(latch.clone()));
        let response = Message::new("future-response", b"response".to_vec());
        future.put_response_message(Some(response.clone())).unwrap();
        assert_eq!(latch.count(), 1);
        let stored = future.response_msg().unwrap();
        assert_eq!(stored.topic(), "future-response");
        assert_eq!(stored.body().as_ref(), b"response");
        latch.count_down();
        assert!(latch.wait(Duration::ZERO).await);
        let waited = future
            .wait_response_message(Duration::ZERO)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(waited.topic(), "future-response");

        future.set_send_request_ok(false);
        future.execute_request_callback();
        future.execute_request_callback();
        assert_eq!(callback.0.load(Ordering::SeqCst), 1);

        future.set_timeout_millis(-1);
        assert!(future.is_timeout());
    }

    #[tokio::test]
    async fn nullable_latch_preserves_java_response_before_countdown_failure() {
        let future = RequestResponseFuture::new("nullable-latch", 10, None);
        future.set_count_down_latch(None);
        assert!(future.count_down_latch().is_none());

        let response = Message::new("nullable-latch", b"stored".to_vec());
        assert_eq!(
            future.put_response_message(Some(response.clone())),
            Err(RequestFutureError::MissingCountDownLatch)
        );
        let stored = future.response_msg().unwrap();
        assert_eq!(stored.topic(), "nullable-latch");
        assert_eq!(stored.body().as_ref(), b"stored");
        assert!(matches!(
            future.wait_response_message(Duration::ZERO).await,
            Err(RequestFutureError::MissingCountDownLatch)
        ));
    }

    #[tokio::test]
    async fn latch_wait_forever_returns_after_countdown() {
        let latch = Arc::new(RequestFutureLatch::new(1));
        let waiter = {
            let latch = latch.clone();
            tokio::spawn(async move { latch.wait_forever().await })
        };
        latch.count_down();
        waiter.await.unwrap();
    }

    #[test]
    fn latch_count_down_is_safe_when_called_concurrently() {
        let latch = Arc::new(RequestFutureLatch::new(64));
        std::thread::scope(|scope| {
            for _ in 0..64 {
                let latch = latch.clone();
                scope.spawn(move || latch.count_down());
            }
        });
        assert_eq!(latch.count(), 0);
    }
}
