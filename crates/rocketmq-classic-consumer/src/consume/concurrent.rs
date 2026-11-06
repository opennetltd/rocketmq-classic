use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};

use crate::{
    process_queue::{ConsumeBatch, ProcessQueue},
    push::MessageListenerConcurrently,
};

const CONSUME_REQUEST_QUEUE_CAPACITY: usize = 256;

pub(crate) struct ConsumeRequest {
    pub(crate) process_queue: Arc<ProcessQueue>,
    pub(crate) batch: ConsumeBatch,
}

pub(crate) enum ConsumeSubmitError {
    Full(ConsumeRequest),
    Closed(ConsumeRequest),
}

struct ConsumeWorkerSet {
    sender: Option<mpsc::Sender<ConsumeRequest>>,
    receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<ConsumeRequest>>>,
    consumer: std::sync::Weak<crate::push::PushConsumer>,
    workers: Vec<ConsumeWorker>,
    retiring: Vec<JoinHandle<()>>,
    cleaner_stop: Option<oneshot::Sender<()>>,
    cleaner: Option<JoinHandle<()>>,
}

struct ConsumeWorker {
    stop: Option<oneshot::Sender<()>>,
    handle: JoinHandle<()>,
}

/// Owns a bounded set of concurrent-listener workers and their pending work.
pub(crate) struct ConcurrentConsumeService {
    minimum_threads: usize,
    maximum_threads: usize,
    listener: Arc<dyn MessageListenerConcurrently>,
    workers: Mutex<Option<ConsumeWorkerSet>>,
    started: AtomicBool,
}

impl ConcurrentConsumeService {
    pub fn new(
        minimum_threads: i32,
        maximum_threads: i32,
        listener: Arc<dyn MessageListenerConcurrently>,
    ) -> Self {
        Self {
            minimum_threads: minimum_threads.max(1) as usize,
            maximum_threads: maximum_threads.max(1) as usize,
            listener,
            workers: Mutex::new(None),
            started: AtomicBool::new(false),
        }
    }

    pub(crate) fn listener(&self) -> Arc<dyn MessageListenerConcurrently> {
        self.listener.clone()
    }

    pub async fn start(&self, consumer: std::sync::Weak<crate::push::PushConsumer>) {
        if self.started.swap(true, Ordering::AcqRel) {
            return;
        }
        let worker_count = self.minimum_threads.min(self.maximum_threads);
        let (sender, receiver) = mpsc::channel(CONSUME_REQUEST_QUEUE_CAPACITY);
        let receiver = Arc::new(tokio::sync::Mutex::new(receiver));
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            workers.push(spawn_worker(
                receiver.clone(),
                consumer.clone(),
                self.listener.clone(),
            ));
        }
        let timeout_minutes = consumer
            .upgrade()
            .map_or(15, |consumer| consumer.config().consume_timeout_minutes());
        let period_seconds = u64::try_from(timeout_minutes.max(1))
            .unwrap_or(u64::MAX)
            .saturating_mul(60);
        let period = Duration::from_secs(period_seconds);
        let first_tick = Instant::now() + period;
        let (cleaner_stop, mut cleaner_stop_receiver) = oneshot::channel();
        let cleaner_consumer = consumer.clone();
        let cleaner = tokio::spawn(async move {
            let mut interval = tokio::time::interval_at(first_tick, period);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut cleaner_stop_receiver => break,
                    _ = interval.tick() => {
                        let Some(consumer) = cleaner_consumer.upgrade() else {
                            break;
                        };
                        if let Err(error) = consumer
                            .clean_expired_process_queue_messages_at(current_epoch_millis())
                            .await
                        {
                            tracing::error!(
                                error = %error,
                                "schedule clean expired messages failed"
                            );
                        }
                    }
                }
            }
        });
        *self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ConsumeWorkerSet {
            sender: Some(sender),
            receiver,
            consumer,
            workers,
            retiring: Vec::new(),
            cleaner_stop: Some(cleaner_stop),
            cleaner: Some(cleaner),
        });
    }

    /// Changes the active worker count within the constructed executor
    /// capacity. The facade applies Java's live `consumeThreadMax` guard.
    pub fn update_core_pool_size(&self, core_pool_size: i32) -> bool {
        if core_pool_size <= 0
            || core_pool_size > i16::MAX as i32
            || core_pool_size > self.maximum_threads as i32
        {
            return false;
        }
        let mut worker_set = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(worker_set) = worker_set.as_mut() else {
            return false;
        };
        worker_set.retiring.retain(|worker| !worker.is_finished());

        let target = core_pool_size as usize;
        while worker_set.workers.len() < target {
            worker_set.workers.push(spawn_worker(
                worker_set.receiver.clone(),
                worker_set.consumer.clone(),
                self.listener.clone(),
            ));
        }
        while worker_set.workers.len() > target {
            let Some(mut worker) = worker_set.workers.pop() else {
                break;
            };
            if let Some(stop) = worker.stop.take() {
                let _ = stop.send(());
            }
            worker_set.retiring.push(worker.handle);
        }
        true
    }

    pub(crate) fn maximum_threads(&self) -> i32 {
        self.maximum_threads as i32
    }

    pub(crate) fn try_submit(&self, request: ConsumeRequest) -> Result<(), ConsumeSubmitError> {
        let mut workers = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(sender) = workers.as_mut().and_then(|workers| workers.sender.as_ref()) else {
            return Err(ConsumeSubmitError::Closed(request));
        };
        sender.try_send(request).map_err(|error| match error {
            mpsc::error::TrySendError::Full(request) => ConsumeSubmitError::Full(request),
            mpsc::error::TrySendError::Closed(request) => ConsumeSubmitError::Closed(request),
        })
    }

    pub async fn shutdown(&self, await_termination_millis: i64) {
        if !self.started.swap(false, Ordering::AcqRel) {
            return;
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
        if let Some(stop) = workers.cleaner_stop.take() {
            let _ = stop.send(());
        }
        let deadline =
            Instant::now() + Duration::from_millis(await_termination_millis.max(0) as u64);
        let mut handles = workers
            .workers
            .drain(..)
            .map(|worker| worker.handle)
            .chain(workers.retiring.drain(..))
            .collect::<Vec<_>>();
        if let Some(cleaner) = workers.cleaner.take() {
            handles.push(cleaner);
        }
        for mut worker in handles.drain(..) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, &mut worker).await.is_err() {
                worker.abort();
                let _ = worker.await;
            }
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

impl Drop for ConcurrentConsumeService {
    fn drop(&mut self) {
        if let Some(mut workers) = self
            .workers
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            workers.sender.take();
            if let Some(stop) = workers.cleaner_stop.take() {
                let _ = stop.send(());
            }
            if let Some(cleaner) = workers.cleaner.take() {
                cleaner.abort();
            }
            for worker in workers.workers.drain(..) {
                worker.handle.abort();
            }
            for worker in workers.retiring.drain(..) {
                worker.abort();
            }
        }
    }
}

fn spawn_worker(
    receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<ConsumeRequest>>>,
    consumer: std::sync::Weak<crate::push::PushConsumer>,
    listener: Arc<dyn MessageListenerConcurrently>,
) -> ConsumeWorker {
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
                .run_consume_request(request, listener.clone())
                .await;
        }
    });
    ConsumeWorker {
        stop: Some(stop),
        handle,
    }
}

fn current_epoch_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(i64::MAX as u128) as i64
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::push::{ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus};
    use rocketmq_classic_common::message::MessageExt;

    #[tokio::test]
    async fn service_starts_a_bounded_worker_set_and_joins_it_on_shutdown() {
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            },
        );
        let service = ConcurrentConsumeService::new(3, 7, listener);
        service
            .start(std::sync::Weak::<crate::push::PushConsumer>::new())
            .await;
        assert_eq!(service.worker_count(), 3);
        service.shutdown(100).await;
        assert_eq!(service.worker_count(), 0);
    }

    #[tokio::test]
    async fn core_pool_update_obeys_executor_capacity_and_resizes_owned_workers() {
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            },
        );
        let service = ConcurrentConsumeService::new(2, 5, listener);
        assert!(!service.update_core_pool_size(3));
        service
            .start(std::sync::Weak::<crate::push::PushConsumer>::new())
            .await;
        assert_eq!(service.worker_count(), 2);
        assert!(service.update_core_pool_size(1));
        assert_eq!(service.worker_count(), 1);
        assert!(service.update_core_pool_size(4));
        assert_eq!(service.worker_count(), 4);
        assert!(!service.update_core_pool_size(0));
        assert!(service.update_core_pool_size(5));
        assert_eq!(service.worker_count(), 5);
        assert!(!service.update_core_pool_size(6));
        assert!(!service.update_core_pool_size(i16::MAX as i32 + 1));
        service.shutdown(100).await;
        assert_eq!(service.worker_count(), 0);
    }
}
