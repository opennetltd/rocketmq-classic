use std::{
    env,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};

use crate::{
    process_queue::ProcessQueue,
    push::{ConsumeOrderlyStatus, MessageListenerOrderly, PushConsumer},
};

pub(crate) const ORDERLY_LOCK_INTERVAL_PROPERTY: &str =
    crate::process_queue::REBALANCE_LOCK_INTERVAL_PROPERTY;
pub(crate) const ORDERLY_UNLOCK_DELAY_PROPERTY: &str = "rocketmq.client.unlockDelayTimeMills";
pub(crate) const ORDERLY_MAX_CONTINUOUS_CONSUME_PROPERTY: &str =
    "rocketmq.client.maxTimeConsumeContinuously";
pub(crate) const ORDERLY_UNLOCK_DELAY_MILLIS: i64 = 20_000;
pub(crate) const ORDERLY_MAX_CONTINUOUS_CONSUME_MILLIS: i64 = 60_000;
pub(crate) const ORDERLY_LOCK_INITIAL_DELAY_MILLIS: u64 = 1_000;
pub(crate) const ORDERLY_LOCK_RPC_TIMEOUT_MILLIS: u64 = 1_000;
pub(crate) const ORDERLY_LOCK_RETRY_DELAY_MILLIS: i64 = 3_000;
const ORDERLY_REQUEST_QUEUE_CAPACITY: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrderlyDisposition {
    Commit,
    Hold,
    Rollback,
    Retry,
}

pub(crate) fn orderly_disposition(
    auto_commit: bool,
    status: Option<ConsumeOrderlyStatus>,
    retry_required: bool,
) -> OrderlyDisposition {
    match (
        auto_commit,
        status.unwrap_or(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment),
    ) {
        (
            true,
            ConsumeOrderlyStatus::Success
            | ConsumeOrderlyStatus::Commit
            | ConsumeOrderlyStatus::Rollback,
        ) => OrderlyDisposition::Commit,
        (false, ConsumeOrderlyStatus::Success) => OrderlyDisposition::Hold,
        (false, ConsumeOrderlyStatus::Commit) => OrderlyDisposition::Commit,
        (false, ConsumeOrderlyStatus::Rollback) => OrderlyDisposition::Rollback,
        (_, ConsumeOrderlyStatus::SuspendCurrentQueueAMoment) if retry_required => {
            OrderlyDisposition::Retry
        }
        (true, ConsumeOrderlyStatus::SuspendCurrentQueueAMoment) => OrderlyDisposition::Commit,
        (false, ConsumeOrderlyStatus::SuspendCurrentQueueAMoment) => OrderlyDisposition::Hold,
    }
}

pub(crate) fn suspend_delay_millis(requested: i64, configured: i64) -> u64 {
    let delay = if requested == -1 {
        configured
    } else {
        requested
    };
    delay.clamp(10, 30_000) as u64
}

/// Rust reads the exact Java system-property names from the process environment
/// because Rust has no JVM-style system-property table. The values are captured
/// once when an orderly consumer starts, matching Java's static ProcessQueue
/// constants being initialized before lock scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OrderlyLockSettings {
    pub(crate) max_live_time_millis: i64,
    pub(crate) interval_millis: i64,
    pub(crate) unlock_delay_millis: i64,
    pub(crate) max_continuous_consume_millis: i64,
}

impl OrderlyLockSettings {
    pub(crate) fn from_java_properties() -> Result<Self, String> {
        let max_live_time_millis = ProcessQueue::rebalance_lock_max_live_time_millis()?;
        let interval_millis = ProcessQueue::rebalance_lock_interval_millis()?;
        let unlock_delay_millis = property_result(
            ORDERLY_UNLOCK_DELAY_PROPERTY,
            ORDERLY_UNLOCK_DELAY_MILLIS,
            env::var(ORDERLY_UNLOCK_DELAY_PROPERTY),
        )?;
        let max_continuous_consume_millis = property_result(
            ORDERLY_MAX_CONTINUOUS_CONSUME_PROPERTY,
            ORDERLY_MAX_CONTINUOUS_CONSUME_MILLIS,
            env::var(ORDERLY_MAX_CONTINUOUS_CONSUME_PROPERTY),
        )?;
        Self::from_all_values(
            max_live_time_millis,
            interval_millis,
            unlock_delay_millis,
            max_continuous_consume_millis,
        )
    }

    #[cfg(test)]
    pub(crate) fn from_values(
        max_live_time_millis: i64,
        interval_millis: i64,
    ) -> Result<Self, String> {
        Self::from_all_values(
            max_live_time_millis,
            interval_millis,
            ORDERLY_UNLOCK_DELAY_MILLIS,
            ORDERLY_MAX_CONTINUOUS_CONSUME_MILLIS,
        )
    }

    pub(crate) fn from_all_values(
        max_live_time_millis: i64,
        interval_millis: i64,
        unlock_delay_millis: i64,
        max_continuous_consume_millis: i64,
    ) -> Result<Self, String> {
        if interval_millis <= 0 {
            return Err(format!(
                "{ORDERLY_LOCK_INTERVAL_PROPERTY} must be positive, got {interval_millis}"
            ));
        }
        Ok(Self {
            max_live_time_millis,
            interval_millis,
            unlock_delay_millis,
            max_continuous_consume_millis,
        })
    }

    pub(crate) fn interval(self) -> Duration {
        Duration::from_millis(self.interval_millis as u64)
    }

    pub(crate) fn max_continuous_consume_exceeded(self, started_at: Instant) -> bool {
        let elapsed_millis = Instant::now()
            .saturating_duration_since(started_at)
            .as_millis();
        i128::try_from(elapsed_millis).unwrap_or(i128::MAX)
            > i128::from(self.max_continuous_consume_millis)
    }
}

fn property_result(
    name: &str,
    default: i64,
    value: Result<String, env::VarError>,
) -> Result<i64, String> {
    match value {
        Ok(value) => crate::process_queue::parse_java_long(&value)
            .ok_or_else(|| format!("invalid Java-compatible long property {name}={value:?}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(value)) => Err(format!(
            "non-Unicode Java-compatible long property {name}={value:?}"
        )),
    }
}

pub(crate) struct OrderlyConsumeRequest {
    pub(crate) process_queue: Arc<ProcessQueue>,
    pub(crate) generation: u64,
}

pub(crate) enum OrderlySubmitError {
    Full(OrderlyConsumeRequest),
    Closed(OrderlyConsumeRequest),
}

struct OrderlyWorker {
    stop: Option<oneshot::Sender<()>>,
    handle: JoinHandle<()>,
}

struct OrderlyWorkerSet {
    sender: Option<mpsc::Sender<OrderlyConsumeRequest>>,
    receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<OrderlyConsumeRequest>>>,
    consumer: std::sync::Weak<PushConsumer>,
    workers: Vec<OrderlyWorker>,
    retiring: Vec<JoinHandle<()>>,
}

/// Owns bounded workers for orderly callbacks and the clustering lock-renewal task.
pub(crate) struct OrderlyConsumeService {
    worker_count: usize,
    maximum_threads: usize,
    listener: Arc<dyn MessageListenerOrderly>,
    workers: Mutex<Option<OrderlyWorkerSet>>,
    renewal: Mutex<Option<JoinHandle<()>>>,
    started: AtomicBool,
}

impl OrderlyConsumeService {
    pub(crate) fn new(
        minimum_threads: i32,
        maximum_threads: i32,
        listener: Arc<dyn MessageListenerOrderly>,
    ) -> Self {
        Self {
            worker_count: (minimum_threads.max(1).min(maximum_threads.max(1))) as usize,
            maximum_threads: maximum_threads.max(1) as usize,
            listener,
            workers: Mutex::new(None),
            renewal: Mutex::new(None),
            started: AtomicBool::new(false),
        }
    }

    pub(crate) fn listener(&self) -> Arc<dyn MessageListenerOrderly> {
        self.listener.clone()
    }

    pub(crate) async fn start(
        &self,
        consumer: std::sync::Weak<PushConsumer>,
        settings: OrderlyLockSettings,
        renew_locks: bool,
    ) {
        if self.started.swap(true, Ordering::AcqRel) {
            return;
        }
        let (sender, receiver) = mpsc::channel(ORDERLY_REQUEST_QUEUE_CAPACITY);
        let receiver = Arc::new(tokio::sync::Mutex::new(receiver));
        let workers = (0..self.worker_count)
            .map(|_| {
                spawn_orderly_worker(receiver.clone(), consumer.clone(), self.listener.clone())
            })
            .collect();
        *self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(OrderlyWorkerSet {
            sender: Some(sender),
            receiver,
            consumer: consumer.clone(),
            workers,
            retiring: Vec::new(),
        });

        if renew_locks {
            let renewal = tokio::spawn(async move {
                let initial =
                    Instant::now() + Duration::from_millis(ORDERLY_LOCK_INITIAL_DELAY_MILLIS);
                let mut ticks = tokio::time::interval_at(initial, settings.interval());
                ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
                loop {
                    ticks.tick().await;
                    let Some(consumer) = consumer.upgrade() else {
                        break;
                    };
                    consumer.renew_orderly_locks().await;
                }
            });
            *self
                .renewal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(renewal);
        }
    }

    pub(crate) fn try_submit(
        &self,
        request: OrderlyConsumeRequest,
    ) -> Result<(), OrderlySubmitError> {
        let workers = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(sender) = workers.as_ref().and_then(|workers| workers.sender.as_ref()) else {
            return Err(OrderlySubmitError::Closed(request));
        };
        sender.try_send(request).map_err(|error| match error {
            mpsc::error::TrySendError::Full(request) => OrderlySubmitError::Full(request),
            mpsc::error::TrySendError::Closed(request) => OrderlySubmitError::Closed(request),
        })
    }

    pub(crate) fn update_core_pool_size(&self, core_pool_size: i32) -> bool {
        if core_pool_size <= 0
            || core_pool_size > i16::MAX as i32
            || core_pool_size as usize > self.maximum_threads
        {
            return false;
        }
        let mut workers = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(workers) = workers.as_mut() else {
            return false;
        };
        workers.retiring.retain(|worker| !worker.is_finished());
        let target = core_pool_size as usize;
        while workers.workers.len() < target {
            workers.workers.push(spawn_orderly_worker(
                workers.receiver.clone(),
                workers.consumer.clone(),
                self.listener.clone(),
            ));
        }
        while workers.workers.len() > target {
            let Some(mut worker) = workers.workers.pop() else {
                break;
            };
            if let Some(stop) = worker.stop.take() {
                let _ = stop.send(());
            }
            workers.retiring.push(worker.handle);
        }
        true
    }

    pub(crate) fn maximum_threads(&self) -> i32 {
        self.maximum_threads as i32
    }

    pub(crate) async fn shutdown(&self, await_termination_millis: i64) {
        if !self.started.swap(false, Ordering::AcqRel) {
            return;
        }
        let renewal = {
            self.renewal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
        };
        if let Some(mut renewal) = renewal {
            let timeout = Duration::from_millis(await_termination_millis.max(0) as u64);
            if timeout.is_zero() || tokio::time::timeout(timeout, &mut renewal).await.is_err() {
                renewal.abort();
            }
            let _ = renewal.await;
        }
        let Some(mut workers) = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        else {
            return;
        };
        workers.sender.take();
        let deadline =
            Instant::now() + Duration::from_millis(await_termination_millis.max(0) as u64);
        for mut worker in workers.workers.drain(..) {
            if let Some(stop) = worker.stop.take() {
                let _ = stop.send(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero()
                || tokio::time::timeout(remaining, &mut worker.handle)
                    .await
                    .is_err()
            {
                worker.handle.abort();
            }
            let _ = worker.handle.await;
        }
        for mut worker in workers.retiring.drain(..) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, &mut worker).await.is_err() {
                worker.abort();
            }
            let _ = worker.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn worker_count(&self) -> usize {
        self.workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map_or(0, |workers| workers.workers.len())
    }
}

impl Drop for OrderlyConsumeService {
    fn drop(&mut self) {
        if let Some(mut workers) = self
            .workers
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            workers.sender.take();
            for worker in workers.workers.drain(..) {
                worker.handle.abort();
            }
            for worker in workers.retiring.drain(..) {
                worker.abort();
            }
        }
        if let Some(renewal) = self
            .renewal
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            renewal.abort();
        }
    }
}

fn spawn_orderly_worker(
    receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<OrderlyConsumeRequest>>>,
    consumer: std::sync::Weak<PushConsumer>,
    listener: Arc<dyn MessageListenerOrderly>,
) -> OrderlyWorker {
    let (stop, mut stop_receiver) = oneshot::channel();
    let handle = tokio::spawn(async move {
        loop {
            let request = tokio::select! {
                biased;
                _ = &mut stop_receiver => break,
                request = async {
                    let mut receiver = receiver.lock().await;
                    receiver.recv().await
                } => request,
            };
            let Some(request) = request else {
                break;
            };
            let Some(consumer) = consumer.upgrade() else {
                continue;
            };
            consumer
                .run_orderly_consume_request(request, listener.clone())
                .await;
        }
    });
    OrderlyWorker {
        stop: Some(stop),
        handle,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_lock_properties_match_java_551_values() {
        assert_eq!(
            crate::process_queue::REBALANCE_LOCK_MAX_LIVE_TIME_MILLIS,
            30_000
        );
        assert_eq!(crate::process_queue::REBALANCE_LOCK_INTERVAL_MILLIS, 20_000);
        assert_eq!(ORDERLY_UNLOCK_DELAY_MILLIS, 20_000);
        assert_eq!(ORDERLY_MAX_CONTINUOUS_CONSUME_MILLIS, 60_000);
        assert_eq!(ORDERLY_LOCK_INITIAL_DELAY_MILLIS, 1_000);
        assert_eq!(ORDERLY_LOCK_RPC_TIMEOUT_MILLIS, 1_000);
        assert_eq!(ORDERLY_LOCK_RETRY_DELAY_MILLIS, 3_000);
        let settings = OrderlyLockSettings::from_values(30_000, 20_000).unwrap();
        assert_eq!(settings.interval(), Duration::from_secs(20));
        assert_eq!(settings.max_live_time_millis, 30_000);
        assert_eq!(settings.unlock_delay_millis, 20_000);
        assert_eq!(settings.max_continuous_consume_millis, 60_000);
    }

    #[test]
    fn configured_lock_settings_preserve_java_values_and_reject_invalid_interval() {
        assert_eq!(
            OrderlyLockSettings::from_values(-1, 1)
                .unwrap()
                .max_live_time_millis,
            -1
        );
        for interval_millis in [0, -1] {
            assert!(OrderlyLockSettings::from_values(30_000, interval_millis).is_err());
        }
    }

    #[test]
    fn property_parser_matches_java_long_parsing_and_defaults() {
        assert_eq!(
            property_result(
                ORDERLY_LOCK_INTERVAL_PROPERTY,
                20_000,
                Err(env::VarError::NotPresent)
            )
            .unwrap(),
            20_000
        );
        assert_eq!(
            property_result(
                ORDERLY_LOCK_INTERVAL_PROPERTY,
                20_000,
                Ok("1234".to_owned())
            )
            .unwrap(),
            1234
        );
        assert!(
            property_result(
                ORDERLY_LOCK_INTERVAL_PROPERTY,
                20_000,
                Ok("not-a-number".to_owned())
            )
            .is_err()
        );
        assert!(
            property_result(
                ORDERLY_LOCK_INTERVAL_PROPERTY,
                20_000,
                Ok("9223372036854775808".to_owned())
            )
            .is_err()
        );
        assert_eq!(
            property_result(
                ORDERLY_LOCK_INTERVAL_PROPERTY,
                20_000,
                Ok("١٢٣٤".to_owned())
            )
            .unwrap(),
            1_234
        );
        assert_eq!(
            property_result(
                ORDERLY_UNLOCK_DELAY_PROPERTY,
                ORDERLY_UNLOCK_DELAY_MILLIS,
                Err(env::VarError::NotPresent)
            )
            .unwrap(),
            20_000
        );
        assert_eq!(
            property_result(
                ORDERLY_MAX_CONTINUOUS_CONSUME_PROPERTY,
                ORDERLY_MAX_CONTINUOUS_CONSUME_MILLIS,
                Ok("12345".to_owned())
            )
            .unwrap(),
            12_345
        );
    }

    #[tokio::test(start_paused = true)]
    async fn continuous_consume_limit_uses_java_strict_boundary() {
        let settings = OrderlyLockSettings::from_all_values(30_000, 20_000, 20_000, 10).unwrap();
        let started_at = Instant::now();
        tokio::time::advance(Duration::from_millis(10)).await;
        assert!(!settings.max_continuous_consume_exceeded(started_at));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(settings.max_continuous_consume_exceeded(started_at));
    }

    #[test]
    fn every_java_orderly_result_matches_both_context_modes() {
        use ConsumeOrderlyStatus::{Commit, Rollback, Success, SuspendCurrentQueueAMoment};
        use OrderlyDisposition::{Commit as CommitBatch, Hold, Retry, Rollback as RollbackBatch};

        let cases = [
            (true, Some(Success), false, CommitBatch),
            (true, Some(Commit), false, CommitBatch),
            (true, Some(Rollback), false, CommitBatch),
            (true, Some(SuspendCurrentQueueAMoment), true, Retry),
            (true, Some(SuspendCurrentQueueAMoment), false, CommitBatch),
            (true, None, true, Retry),
            (false, Some(Success), false, Hold),
            (false, Some(Commit), false, CommitBatch),
            (false, Some(Rollback), false, RollbackBatch),
            (false, Some(SuspendCurrentQueueAMoment), true, Retry),
            (false, Some(SuspendCurrentQueueAMoment), false, Hold),
            (false, None, true, Retry),
        ];
        for (auto_commit, status, retry_required, expected) in cases {
            assert_eq!(
                orderly_disposition(auto_commit, status, retry_required),
                expected
            );
        }
    }

    #[test]
    fn suspend_delay_uses_push_default_and_java_bounds() {
        assert_eq!(suspend_delay_millis(-1, 1_000), 1_000);
        assert_eq!(suspend_delay_millis(0, 1_000), 10);
        assert_eq!(suspend_delay_millis(31_000, 1_000), 30_000);
        assert_eq!(suspend_delay_millis(-2, 1_000), 10);
    }
}
