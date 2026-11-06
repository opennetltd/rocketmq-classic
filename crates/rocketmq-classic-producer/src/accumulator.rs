use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use rocketmq_classic_common::message::{Message, MessageQueue};
use rocketmq_classic_core::api::producer::{SendMode, SendResult};
use tokio::{
    sync::{Mutex, oneshot},
    time::Instant,
};

use crate::{Producer, ProducerError};

/// Constructor-only public counterpart of Java `ProduceAccumulator`.
///
/// RocketMQ 5.5.1 declares a public `ProduceAccumulator(String)` constructor
/// but no public methods. The Producer owns and operates its batching engine;
/// this value preserves the separately constructible Java API surface without
/// exposing that private engine.
pub struct ProduceAccumulator {
    _instance_name: Option<String>,
}

impl ProduceAccumulator {
    /// Constructs the Java `ProduceAccumulator(String)` counterpart.
    ///
    /// `None` represents a Java null constructor argument.
    pub fn new(instance_name: Option<String>) -> Self {
        Self {
            _instance_name: instance_name,
        }
    }
}

pub(crate) type BatchResponse = Result<SendResult, String>;
pub(crate) type BatchReceiver = oneshot::Receiver<BatchResponse>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BatchKey {
    pub(crate) topic: String,
    pub(crate) queue: Option<MessageQueue>,
    pub(crate) wait_store_msg_ok: bool,
    pub(crate) tag: Option<String>,
    pub(crate) mode: SendMode,
}

#[derive(Clone, Copy)]
pub(crate) struct BatchLimits {
    pub(crate) max_delay: Duration,
    pub(crate) max_bytes: usize,
    pub(crate) total_max_bytes: usize,
}

impl BatchKey {
    pub(crate) fn new(message: &Message, queue: Option<MessageQueue>, mode: SendMode) -> Self {
        Self {
            topic: message.topic().to_owned(),
            queue,
            wait_store_msg_ok: message.is_wait_store_msg_ok(),
            tag: message.tags(),
            mode,
        }
    }
}

struct PendingBatch {
    generation: u64,
    created_at: Instant,
    body_bytes: usize,
    messages: Vec<Message>,
    waiters: Vec<oneshot::Sender<BatchResponse>>,
    owner: Option<Arc<Producer>>,
}

pub(crate) struct ReadyBatch {
    pub(crate) key: BatchKey,
    pub(crate) messages: Vec<Message>,
    pub(crate) waiters: Vec<oneshot::Sender<BatchResponse>>,
    pub(crate) body_bytes: usize,
    pub(crate) owner: Option<Arc<Producer>>,
}

pub(crate) enum EnqueueResult {
    Bypass,
    Queued {
        receiver: BatchReceiver,
        key: BatchKey,
        generation: u64,
        flush_at: Instant,
        schedule_timer: bool,
    },
    Ready {
        receiver: BatchReceiver,
        batch: ReadyBatch,
    },
}

pub(crate) enum PendingBatchStatus {
    Missing,
    WaitUntil(Instant),
    Ready(ReadyBatch),
}

#[derive(Default)]
struct AccumulatorState {
    entries: HashMap<BatchKey, PendingBatch>,
    current_hold_size: usize,
    next_generation: u64,
    running: bool,
    closed: bool,
}

/// Rust-native pending batches. Keys and flush thresholds mirror the pinned
/// Java `ProduceAccumulator`; each waiter receives its message's result.
#[derive(Default)]
pub(crate) struct AutoBatchAccumulator {
    state: Mutex<AccumulatorState>,
    closed: AtomicBool,
    running: AtomicBool,
}

impl AutoBatchAccumulator {
    #[cfg(test)]
    pub(crate) async fn enqueue(
        &self,
        message: Message,
        queue: Option<MessageQueue>,
        mode: SendMode,
        max_delay: Duration,
        max_bytes: usize,
        total_max_bytes: usize,
    ) -> Result<EnqueueResult, ProducerError> {
        self.enqueue_with_owner(
            message,
            queue,
            mode,
            BatchLimits {
                max_delay,
                max_bytes,
                total_max_bytes,
            },
            None,
        )
        .await
    }

    pub(crate) async fn enqueue_owned(
        &self,
        message: Message,
        queue: Option<MessageQueue>,
        mode: SendMode,
        limits: BatchLimits,
        owner: Arc<Producer>,
    ) -> Result<EnqueueResult, ProducerError> {
        self.enqueue_with_owner(message, queue, mode, limits, Some(owner))
            .await
    }

    async fn enqueue_with_owner(
        &self,
        message: Message,
        queue: Option<MessageQueue>,
        mode: SendMode,
        limits: BatchLimits,
        owner: Option<Arc<Producer>>,
    ) -> Result<EnqueueResult, ProducerError> {
        let (sender, receiver) = oneshot::channel();
        let key = BatchKey::new(&message, queue, mode);
        let now = Instant::now();
        let message_size = message.body().len();
        let mut state = self.state.lock().await;
        if state.closed {
            return Err(ProducerError::Shutdown);
        }

        // Java checks the current hold size before adding the next message.
        if state.current_hold_size >= limits.total_max_bytes {
            return Ok(EnqueueResult::Bypass);
        }
        state.current_hold_size = state.current_hold_size.saturating_add(message_size);

        let is_new = !state.entries.contains_key(&key);
        if is_new {
            state.next_generation = state.next_generation.wrapping_add(1);
        }
        let generation = state.next_generation;
        let pending = state
            .entries
            .entry(key.clone())
            .or_insert_with(|| PendingBatch {
                generation,
                created_at: now,
                body_bytes: 0,
                messages: Vec::new(),
                waiters: Vec::new(),
                owner,
            });
        pending.body_bytes = pending.body_bytes.saturating_add(message_size);
        pending.messages.push(message);
        pending.waiters.push(sender);
        let generation = pending.generation;
        let flush_at = pending.created_at + limits.max_delay;
        let should_flush = pending.body_bytes > limits.max_bytes || Instant::now() >= flush_at;

        if should_flush {
            let Some(pending) = state.entries.remove(&key) else {
                state.current_hold_size = state.current_hold_size.saturating_sub(message_size);
                return Err(ProducerError::AutoBatchSend(
                    "accumulator batch disappeared before flush".into(),
                ));
            };
            Ok(EnqueueResult::Ready {
                receiver,
                batch: ReadyBatch {
                    key,
                    messages: pending.messages,
                    waiters: pending.waiters,
                    body_bytes: pending.body_bytes,
                    owner: pending.owner,
                },
            })
        } else {
            Ok(EnqueueResult::Queued {
                receiver,
                key,
                generation,
                flush_at,
                schedule_timer: is_new && state.running,
            })
        }
    }

    pub(crate) async fn recheck(
        &self,
        key: &BatchKey,
        generation: u64,
        max_delay: Duration,
        max_bytes: usize,
    ) -> PendingBatchStatus {
        let mut state = self.state.lock().await;
        let Some(pending) = state
            .entries
            .get(key)
            .filter(|batch| batch.generation == generation)
        else {
            return PendingBatchStatus::Missing;
        };
        let deadline = pending.created_at + max_delay;
        if pending.body_bytes > max_bytes || Instant::now() >= deadline {
            let Some(batch) = state.entries.remove(key) else {
                return PendingBatchStatus::Missing;
            };
            return PendingBatchStatus::Ready(ReadyBatch {
                key: key.clone(),
                messages: batch.messages,
                waiters: batch.waiters,
                body_bytes: batch.body_bytes,
                owner: batch.owner,
            });
        }
        PendingBatchStatus::WaitUntil(deadline)
    }

    pub(crate) async fn release(&self, body_bytes: usize) {
        let mut state = self.state.lock().await;
        state.current_hold_size = state.current_hold_size.saturating_sub(body_bytes);
    }

    #[cfg(test)]
    pub(crate) async fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let mut state = self.state.lock().await;
        state.closed = true;
        state.running = false;
        self.running.store(false, Ordering::Release);
        state.current_hold_size = 0;
        state.entries.clear();
    }

    pub(crate) async fn start(&self) -> bool {
        let mut state = self.state.lock().await;
        if state.closed || state.running {
            return false;
        }
        state.running = true;
        self.running.store(true, Ordering::Release);
        true
    }

    pub(crate) async fn stop(&self) -> bool {
        let mut state = self.state.lock().await;
        if state.closed || !state.running {
            return false;
        }
        state.running = false;
        self.running.store(false, Ordering::Release);
        true
    }

    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub(crate) async fn pending_timers(
        &self,
        max_delay: Duration,
    ) -> Vec<(BatchKey, u64, Instant)> {
        let state = self.state.lock().await;
        state
            .entries
            .iter()
            .map(|(key, batch)| (key.clone(), batch.generation, batch.created_at + max_delay))
            .collect()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_common::message::Message;

    #[tokio::test]
    async fn batches_share_only_matching_route_tag_wait_store_and_mode() {
        let accumulator = AutoBatchAccumulator::default();
        let mut first = Message::new("orders", vec![1; 10]);
        first.set_tags("blue").unwrap();
        let first_result = accumulator
            .enqueue(
                first,
                None,
                SendMode::Sync,
                Duration::from_secs(1),
                1_000,
                10_000,
            )
            .await
            .unwrap();
        let EnqueueResult::Queued {
            key, generation, ..
        } = first_result
        else {
            panic!("first message should wait for batching");
        };

        let mut second = Message::new("orders", vec![2; 10]);
        second.set_tags("blue").unwrap();
        let second_result = accumulator
            .enqueue(
                second,
                None,
                SendMode::Sync,
                Duration::from_secs(1),
                1_000,
                10_000,
            )
            .await
            .unwrap();
        let EnqueueResult::Queued {
            key: second_key, ..
        } = second_result
        else {
            panic!("matching message should join the first batch");
        };
        assert_eq!(key, second_key);

        let mut differently_tagged = Message::new("orders", vec![3; 10]);
        differently_tagged.set_tags("green").unwrap();
        let different_result = accumulator
            .enqueue(
                differently_tagged,
                None,
                SendMode::Sync,
                Duration::from_secs(1),
                1_000,
                10_000,
            )
            .await
            .unwrap();
        let EnqueueResult::Queued {
            key: different_key, ..
        } = different_result
        else {
            panic!("different tag should create a distinct batch");
        };
        assert_ne!(key, different_key);

        let PendingBatchStatus::Ready(ready) = accumulator
            .recheck(&key, generation, Duration::ZERO, 1_000)
            .await
        else {
            panic!("zero hold delay should make the queued batch ready");
        };
        assert_eq!(ready.messages.len(), 2);
        assert_eq!(ready.body_bytes, 20);
        assert_eq!(ready.waiters.len(), 2);
    }

    #[tokio::test]
    async fn byte_threshold_flushes_and_total_hold_limit_bypasses() {
        let accumulator = AutoBatchAccumulator::default();
        let first = accumulator
            .enqueue(
                Message::new("orders", vec![1; 8]),
                None,
                SendMode::Sync,
                Duration::from_secs(1),
                8,
                100,
            )
            .await
            .unwrap();
        assert!(matches!(first, EnqueueResult::Queued { .. }));
        let second = accumulator
            .enqueue(
                Message::new("orders", vec![2; 1]),
                None,
                SendMode::Sync,
                Duration::from_secs(1),
                8,
                100,
            )
            .await
            .unwrap();
        assert!(matches!(second, EnqueueResult::Ready { .. }));

        let full = accumulator
            .enqueue(
                Message::new("payments", vec![3; 1]),
                None,
                SendMode::Sync,
                Duration::from_secs(1),
                8,
                9,
            )
            .await
            .unwrap();
        assert!(matches!(full, EnqueueResult::Bypass));
    }

    #[tokio::test]
    async fn pending_batch_becomes_ready_at_its_configured_hold_deadline() {
        let accumulator = AutoBatchAccumulator::default();
        let EnqueueResult::Queued {
            key,
            generation,
            flush_at,
            ..
        } = accumulator
            .enqueue(
                Message::new("orders", vec![1; 8]),
                None,
                SendMode::Sync,
                Duration::from_millis(100),
                1_000,
                10_000,
            )
            .await
            .unwrap()
        else {
            panic!("message should wait for its hold deadline");
        };

        tokio::time::sleep_until(flush_at).await;
        let PendingBatchStatus::Ready(ready) = accumulator
            .recheck(&key, generation, Duration::from_millis(100), 1_000)
            .await
        else {
            panic!("the batch should be ready at its hold deadline");
        };
        assert_eq!(ready.messages.len(), 1);
        assert_eq!(ready.body_bytes, 8);
    }

    #[tokio::test]
    async fn stopped_accumulator_retains_pending_batches_for_restart() {
        let accumulator = AutoBatchAccumulator::default();
        assert!(accumulator.start().await);
        let limits = Duration::from_millis(100);
        let EnqueueResult::Queued {
            key,
            generation,
            schedule_timer,
            ..
        } = accumulator
            .enqueue(
                Message::new("orders", vec![1]),
                None,
                SendMode::Sync,
                limits,
                100,
                100,
            )
            .await
            .unwrap()
        else {
            panic!("first message should remain queued");
        };
        assert!(schedule_timer);

        assert!(accumulator.stop().await);
        let EnqueueResult::Queued { schedule_timer, .. } = accumulator
            .enqueue(
                Message::new("orders", vec![2]),
                None,
                SendMode::Sync,
                limits,
                100,
                100,
            )
            .await
            .unwrap()
        else {
            panic!("second message should join the stopped batch");
        };
        assert!(!schedule_timer);

        tokio::time::sleep(Duration::from_millis(110)).await;
        assert!(accumulator.start().await);
        let pending = accumulator.pending_timers(limits).await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, key);
        assert_eq!(pending[0].1, generation);

        let PendingBatchStatus::Ready(ready) =
            accumulator.recheck(&key, generation, limits, 100).await
        else {
            panic!("the retained batch should be ready after its hold deadline");
        };
        assert_eq!(ready.messages.len(), 2);
        accumulator.release(ready.body_bytes).await;
        accumulator.close().await;
    }

    #[tokio::test]
    async fn close_fails_pending_waiters_and_rejects_new_messages() {
        let accumulator = AutoBatchAccumulator::default();
        let EnqueueResult::Queued { receiver, .. } = accumulator
            .enqueue(
                Message::new("orders", vec![1]),
                None,
                SendMode::Future,
                Duration::from_secs(1),
                100,
                100,
            )
            .await
            .unwrap()
        else {
            panic!("message should queue");
        };
        accumulator.close().await;
        assert!(receiver.await.is_err());
        assert!(matches!(
            accumulator
                .enqueue(
                    Message::new("orders", vec![1]),
                    None,
                    SendMode::Sync,
                    Duration::from_secs(1),
                    100,
                    100,
                )
                .await,
            Err(ProducerError::Shutdown)
        ));
    }
}
