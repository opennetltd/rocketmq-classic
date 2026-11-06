use super::{ClientRuntime, RuntimeError};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RuntimeServiceKind {
    Rebalance,
    PullMessage,
}

/// State corresponding to the observable `ServiceThread` fields. The owned
/// Tokio task remains the execution mechanism; this state drives its lifecycle.
pub(super) struct ServiceThreadControl {
    started: AtomicBool,
    stopped: AtomicBool,
    daemon: AtomicBool,
    thread_daemon: AtomicBool,
}

impl Default for ServiceThreadControl {
    fn default() -> Self {
        Self {
            started: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            daemon: AtomicBool::new(false),
            thread_daemon: AtomicBool::new(false),
        }
    }
}

impl ServiceThreadControl {
    pub fn start(&self) -> bool {
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.stopped.store(false, Ordering::Release);
        self.thread_daemon
            .store(self.daemon.load(Ordering::Acquire), Ordering::Release);
        true
    }

    pub fn shutdown(&self) -> bool {
        if self
            .started
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.stopped.store(true, Ordering::Release);
        true
    }

    pub fn make_stop(&self) -> bool {
        if self.started.load(Ordering::Acquire) {
            self.stopped.store(true, Ordering::Release);
            true
        } else {
            false
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    pub fn is_daemon(&self) -> bool {
        self.daemon.load(Ordering::Acquire)
    }

    pub fn set_daemon(&self, daemon: bool) {
        self.daemon.store(daemon, Ordering::Release);
    }

    pub fn thread_is_daemon(&self) -> bool {
        self.thread_daemon.load(Ordering::Acquire)
    }
}

/// Rust handle for a runtime-owned Java `ServiceThread` worker.
/// The handle retains the client runtime, matching each Java service's strong
/// reference to its `MQClientInstance` owner.
///
/// Start and shutdown are asynchronous because the runtime owns and joins the
/// worker task. `shutdown` follows the source's 90-second join window; a daemon
/// setting applies to the next worker start, as in Java.
pub struct RuntimeServiceThread {
    runtime: Arc<ClientRuntime>,
    kind: RuntimeServiceKind,
}

impl RuntimeServiceThread {
    pub(super) fn new(runtime: Arc<ClientRuntime>, kind: RuntimeServiceKind) -> Self {
        Self { runtime, kind }
    }

    pub fn service_name(&self) -> &'static str {
        match self.kind {
            RuntimeServiceKind::Rebalance => super::REBALANCE_SERVICE_NAME,
            RuntimeServiceKind::PullMessage => super::PULL_MESSAGE_SERVICE_NAME,
        }
    }

    pub fn join_time_millis(&self) -> u64 {
        super::SERVICE_THREAD_JOIN_TIME_MILLIS
    }

    pub fn is_stopped(&self) -> bool {
        self.runtime.service_thread_control(self.kind).is_stopped()
    }

    pub fn is_daemon(&self) -> bool {
        self.runtime.service_thread_control(self.kind).is_daemon()
    }

    pub fn set_daemon(&self, daemon: bool) {
        self.runtime
            .service_thread_control(self.kind)
            .set_daemon(daemon);
    }

    /// Sets the worker's stop flag and wakes it without joining it.
    pub fn make_stop(&self) {
        if self.runtime.service_thread_control(self.kind).make_stop() {
            self.runtime.wakeup_service_thread(self.kind);
        }
    }

    /// Wakes a rebalance worker. Pull dispatch waits for queued work and has no
    /// wait-point to unpark, matching `PullMessageService`'s run loop.
    pub fn wakeup(&self) {
        self.runtime.wakeup_service_thread(self.kind);
    }

    pub async fn start(&self) -> Result<(), RuntimeError> {
        self.runtime.start_service_thread(self.kind).await
    }

    /// Shuts down without interrupting a queue wait, matching Java's
    /// no-argument `ServiceThread.shutdown()` overload.
    pub async fn shutdown(&self) -> Result<(), RuntimeError> {
        self.shutdown_with_interrupt(false).await
    }

    /// Shuts down the service and optionally interrupts its blocking wait,
    /// matching Java's `ServiceThread.shutdown(boolean)` overload.
    pub async fn shutdown_with_interrupt(&self, interrupt: bool) -> Result<(), RuntimeError> {
        self.runtime
            .shutdown_service_thread(self.kind, interrupt)
            .await
    }
}

impl Clone for RuntimeServiceThread {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            kind: self.kind,
        }
    }
}

impl ClientRuntime {
    pub fn rebalance_service(self: &Arc<Self>) -> RuntimeServiceThread {
        RuntimeServiceThread::new(self.clone(), RuntimeServiceKind::Rebalance)
    }

    /// Returns the runtime-owned counterpart to
    /// `MQClientInstance.getPullMessageService()`.
    pub fn pull_message_service(self: &Arc<Self>) -> RuntimeServiceThread {
        RuntimeServiceThread::new(self.clone(), RuntimeServiceKind::PullMessage)
    }
}

#[cfg(test)]
mod tests {
    use crate::{config::ClientConfig, runtime::ClientRuntimeManager};
    use std::sync::atomic::Ordering;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn make_stop_wakes_a_waiting_rebalance_worker() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        assert!(
            fixture["serviceThreadMakeStopWakeup"]["returnedAfterMakeStop"]
                .as_bool()
                .unwrap()
        );

        let mut config = ClientConfig::new("service-thread-make-stop-wakeup");
        config.set_namesrv_addr(Some(String::new()));
        let runtime = ClientRuntimeManager::new()
            .get_or_create(&config)
            .await
            .unwrap();
        let service = runtime.rebalance_service();
        service.start().await.unwrap();
        timeout(Duration::from_secs(2), async {
            while runtime.rebalance_probe.live.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("rebalance worker did not start");

        service.make_stop();
        timeout(Duration::from_secs(2), async {
            while runtime.rebalance_probe.live.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("make_stop did not wake the rebalance worker");
        service.shutdown().await.unwrap();
    }
}
