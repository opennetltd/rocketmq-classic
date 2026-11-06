use std::{
    any::Any,
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_core::runtime::{ClientRuntime, MessageRequest};
use rocketmq_classic_protocol::body::MessageRequestMode;
use tokio::{
    sync::{Notify, mpsc, watch},
    task::JoinHandle,
    time::{Duration, Instant, sleep_until},
};

use crate::{
    process_queue::{ProcessQueue, ProcessQueueSnapshot},
    push::config::PushConsumerConfig,
};

pub(crate) const PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION: i64 = 3_000;
pub(crate) const PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL: i64 = 50;
pub(crate) const PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL: i64 = 20;
pub(crate) const PULL_TIME_DELAY_MILLIS_WHEN_SUSPEND: i64 = 1_000;

const MEBIBYTE: i64 = 1024 * 1024;

/// One logical fetch request tied to a queue assignment. The cursor is mutable
/// because Java reuses its PullRequest after each completed response.
#[derive(Clone)]
pub(crate) struct PullRequest {
    consumer_group: String,
    process_queue: Arc<ProcessQueue>,
    next_offset: Arc<Mutex<i64>>,
    previously_locked: Arc<AtomicBool>,
}

impl PullRequest {
    #[allow(dead_code)] // Constructed by the private Phase 11 assignment activation path.
    pub(crate) fn new(
        consumer_group: impl Into<String>,
        process_queue: Arc<ProcessQueue>,
        next_offset: i64,
    ) -> Self {
        Self {
            consumer_group: consumer_group.into(),
            process_queue,
            next_offset: Arc::new(Mutex::new(next_offset)),
            previously_locked: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn process_queue(&self) -> Arc<ProcessQueue> {
        self.process_queue.clone()
    }

    pub(crate) fn consumer_group(&self) -> &str {
        &self.consumer_group
    }

    pub(crate) fn queue(&self) -> &MessageQueue {
        self.process_queue.queue()
    }

    pub(crate) fn generation(&self) -> u64 {
        self.process_queue.generation()
    }

    pub(crate) fn next_offset(&self) -> i64 {
        *self
            .next_offset
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn set_next_offset(&self, offset: i64) {
        *self
            .next_offset
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = offset;
    }

    pub(crate) fn previously_locked(&self) -> bool {
        self.previously_locked.load(Ordering::Acquire)
    }

    pub(crate) fn set_previously_locked(&self, value: bool) {
        self.previously_locked.store(value, Ordering::Release);
    }

    fn key(&self) -> PullIdentity {
        PullIdentity {
            queue: self.queue().clone(),
            generation: self.generation(),
        }
    }

    pub(crate) fn is_current(&self) -> bool {
        self.process_queue
            .snapshot(self.generation())
            .is_ok_and(|snapshot| !snapshot.dropped)
    }
}

const PULL_RPC_WORKER_COUNT: usize = 4;
const PULL_RPC_QUEUE_CAPACITY: usize = 64;

struct PullRpcWorkerSet {
    sender: Option<mpsc::Sender<Arc<PullRequest>>>,
    cancellation: watch::Sender<bool>,
    workers: Vec<JoinHandle<()>>,
}

/// Bounded pull-RPC workers keep long broker suspends off the shared runtime
/// PullMessageService worker. A queue assignment still has at most one pull in
/// flight because PullScheduler owns that invariant.
pub(crate) struct PullRpcWorkers {
    state: Mutex<Option<PullRpcWorkerSet>>,
}

impl PullRpcWorkers {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(None),
        }
    }

    pub(crate) fn start(
        &self,
        consumer: Weak<crate::push::PushConsumer>,
        scheduler: Arc<PullScheduler>,
    ) -> Result<(), PullSchedulerError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.is_some() {
            return Err(PullSchedulerError::AlreadyStarted);
        }
        let (sender, receiver) = mpsc::channel::<Arc<PullRequest>>(PULL_RPC_QUEUE_CAPACITY);
        let receiver = Arc::new(tokio::sync::Mutex::new(receiver));
        let (cancellation, initial_receiver) = watch::channel(false);
        let mut workers = Vec::with_capacity(PULL_RPC_WORKER_COUNT);
        for _ in 0..PULL_RPC_WORKER_COUNT {
            let mut cancellation = initial_receiver.clone();
            let receiver = receiver.clone();
            let consumer = consumer.clone();
            let scheduler = scheduler.clone();
            workers.push(tokio::spawn(async move {
                loop {
                    if *cancellation.borrow() {
                        break;
                    }
                    let request = tokio::select! {
                        biased;
                        _ = cancellation.changed() => break,
                        request = async { receiver.lock().await.recv().await } => request,
                    };
                    let Some(request) = request else {
                        break;
                    };
                    let Some(consumer) = consumer.upgrade() else {
                        scheduler.finish(&request);
                        continue;
                    };
                    let finish_request = request.clone();
                    tokio::select! {
                        biased;
                        _ = cancellation.changed() => {
                            scheduler.finish(&finish_request);
                            break;
                        }
                        _ = consumer.clone().run_pull_worker_request(request) => {}
                    }
                    scheduler.finish(&finish_request);
                }
            }));
        }
        *state = Some(PullRpcWorkerSet {
            sender: Some(sender),
            cancellation,
            workers,
        });
        Ok(())
    }

    pub(crate) fn try_submit(&self, request: Arc<PullRequest>) -> Result<(), Arc<PullRequest>> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(sender) = state.as_ref().and_then(|workers| workers.sender.as_ref()) else {
            return Err(request);
        };
        sender.try_send(request).map_err(|error| error.into_inner())
    }

    pub(crate) async fn stop(&self, wait_millis: i64) {
        let Some(mut workers) = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        else {
            return;
        };
        workers.cancellation.send_replace(true);
        workers.sender.take();
        let deadline = Instant::now() + Duration::from_millis(wait_millis.max(0) as u64);
        for mut worker in workers.workers.drain(..) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, &mut worker).await.is_err() {
                worker.abort();
                let _ = worker.await;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn worker_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map_or(0, |workers| workers.workers.len())
    }
}

impl Drop for PullRpcWorkers {
    fn drop(&mut self) {
        if let Some(mut workers) = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            workers.cancellation.send_replace(true);
            workers.sender.take();
            for worker in workers.workers.drain(..) {
                worker.abort();
            }
        }
    }
}

impl MessageRequest for PullRequest {
    fn consumer_group(&self) -> Option<&str> {
        Some(&self.consumer_group)
    }

    fn mode(&self) -> MessageRequestMode {
        MessageRequestMode::Pull
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum PullSchedulerError {
    #[error("pull scheduler has already been started")]
    AlreadyStarted,
    #[error("pull scheduler has been stopped")]
    Stopped,
    #[error("pull request belongs to a dropped queue assignment")]
    DroppedAssignment,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PullIdentity {
    queue: MessageQueue,
    generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ScheduledAt {
    deadline: Instant,
    sequence: u64,
}

struct ScheduledPull {
    key: PullIdentity,
    request: Arc<PullRequest>,
}

struct DeferredPull {
    deadline: Instant,
    request: Arc<PullRequest>,
}

#[derive(Default)]
struct ScheduleState {
    stopped: bool,
    all_paused: bool,
    sequence: u64,
    scheduled: BTreeMap<ScheduledAt, ScheduledPull>,
    scheduled_by_key: HashMap<PullIdentity, ScheduledAt>,
    in_flight: HashSet<PullIdentity>,
    deferred: HashMap<PullIdentity, DeferredPull>,
    paused: HashSet<PullIdentity>,
    paused_scheduled: HashMap<PullIdentity, DeferredPull>,
    cancelled_in_flight: HashSet<PullIdentity>,
}

struct PullSchedulerInner {
    state: Mutex<ScheduleState>,
    changed: Notify,
    cancellation: Mutex<Option<watch::Sender<bool>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    started: AtomicBool,
    delayed_pull_count: AtomicU64,
}

/// Coalesces each queue-generation to at most one scheduled or active pull.
/// A single owned due-time worker submits ready requests to the shared runtime
/// PullMessageService; it never spawns a task per queue or pull iteration.
pub(crate) struct PullScheduler {
    inner: Arc<PullSchedulerInner>,
}

impl PullScheduler {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(PullSchedulerInner {
                state: Mutex::new(ScheduleState::default()),
                changed: Notify::new(),
                cancellation: Mutex::new(None),
                worker: Mutex::new(None),
                started: AtomicBool::new(false),
                delayed_pull_count: AtomicU64::new(0),
            }),
        }
    }

    pub(crate) fn start(&self, runtime: Weak<ClientRuntime>) -> Result<(), PullSchedulerError> {
        if self.inner.started.swap(true, Ordering::AcqRel) {
            return Err(PullSchedulerError::AlreadyStarted);
        }
        if self.lock_state().stopped {
            self.inner.started.store(false, Ordering::Release);
            return Err(PullSchedulerError::Stopped);
        }

        let (cancellation, receiver) = watch::channel(false);
        *self
            .inner
            .cancellation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(cancellation);
        let inner = self.inner.clone();
        let worker = tokio::spawn(async move {
            Self::run(inner, runtime, receiver).await;
        });
        *self
            .inner
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(worker);
        Ok(())
    }

    pub(crate) fn schedule(
        &self,
        request: Arc<PullRequest>,
        delay_millis: i64,
    ) -> Result<(), PullSchedulerError> {
        if !request.is_current() {
            return Err(PullSchedulerError::DroppedAssignment);
        }
        let delay_millis = delay_millis.max(0);
        let now = Instant::now();
        let delay = Duration::from_millis(delay_millis as u64);
        let requested_deadline = now
            .checked_add(delay)
            .unwrap_or(now + Duration::from_secs(365 * 24 * 60 * 60));
        let key = request.key();
        {
            let mut state = self.lock_state();
            if state.stopped {
                return Err(PullSchedulerError::Stopped);
            }
            if state.cancelled_in_flight.contains(&key) {
                return Err(PullSchedulerError::DroppedAssignment);
            }
            if state.in_flight.contains(&key) {
                match state.deferred.get_mut(&key) {
                    Some(deferred) => {
                        deferred.deadline = deferred.deadline.min(requested_deadline);
                        deferred.request = request.clone();
                    }
                    None => {
                        state.deferred.insert(
                            key.clone(),
                            DeferredPull {
                                deadline: requested_deadline,
                                request: request.clone(),
                            },
                        );
                    }
                }
            } else if state.all_paused || state.paused.contains(&key) {
                Self::insert_paused(&mut state, key.clone(), request.clone(), requested_deadline);
            } else {
                let previous_schedule = state.scheduled_by_key.remove(&key);
                let deadline = previous_schedule.map_or(requested_deadline, |scheduled_at| {
                    let old = state.scheduled.remove(&scheduled_at);
                    drop(old);
                    scheduled_at.deadline.min(requested_deadline)
                });
                Self::insert_scheduled(&mut state, key.clone(), request.clone(), deadline);
            }
        }
        if delay_millis > 0 {
            self.inner
                .delayed_pull_count
                .fetch_add(1, Ordering::Relaxed);
        }
        self.inner.changed.notify_one();
        tracing::trace!(
            consumer_group = %request.consumer_group(),
            topic = ?request.queue().topic(),
            queue_id = request.queue().queue_id(),
            generation = request.generation(),
            delay_millis,
            delayed_pull_count = self.inner.delayed_pull_count.load(Ordering::Relaxed),
            scheduled_pull_backlog = self.logical_pull_count(),
            "push pull request scheduled"
        );
        Ok(())
    }

    pub(crate) fn cancel(&self, queue: &MessageQueue, generation: u64) {
        Self::cancel_inner(
            &self.inner,
            &PullIdentity {
                queue: queue.clone(),
                generation,
            },
        );
    }

    #[allow(dead_code)] // Used by Phase 11 assignment transitions.
    pub(crate) fn pause(&self, queue: &MessageQueue, generation: u64) {
        let key = PullIdentity {
            queue: queue.clone(),
            generation,
        };
        {
            let mut state = self.lock_state();
            state.paused.insert(key.clone());
            if let Some(scheduled_at) = state.scheduled_by_key.remove(&key) {
                if let Some(scheduled) = state.scheduled.remove(&scheduled_at) {
                    Self::insert_paused(&mut state, key, scheduled.request, scheduled_at.deadline);
                }
            }
        }
        self.inner.changed.notify_one();
    }

    #[allow(dead_code)] // Used by Phase 11 assignment transitions.
    pub(crate) fn resume(&self, queue: &MessageQueue, generation: u64) {
        let key = PullIdentity {
            queue: queue.clone(),
            generation,
        };
        {
            let mut state = self.lock_state();
            state.paused.remove(&key);
            if !state.all_paused && !state.in_flight.contains(&key) {
                if let Some(paused) = state.paused_scheduled.remove(&key) {
                    Self::insert_scheduled(&mut state, key, paused.request, paused.deadline);
                }
            }
        }
        self.inner.changed.notify_one();
    }

    pub(crate) fn pause_all(&self) {
        {
            let mut state = self.lock_state();
            state.all_paused = true;
            let scheduled = std::mem::take(&mut state.scheduled);
            state.scheduled_by_key.clear();
            for (scheduled_at, pull) in scheduled {
                state.paused.insert(pull.key.clone());
                Self::insert_paused(&mut state, pull.key, pull.request, scheduled_at.deadline);
            }
            let in_flight = state.in_flight.iter().cloned().collect::<Vec<_>>();
            state.paused.extend(in_flight);
        }
        self.inner.changed.notify_waiters();
    }

    pub(crate) fn resume_all(&self) {
        {
            let mut state = self.lock_state();
            state.all_paused = false;
            let paused = std::mem::take(&mut state.paused);
            for key in paused {
                if !state.in_flight.contains(&key) {
                    if let Some(request) = state.paused_scheduled.remove(&key) {
                        Self::insert_scheduled(&mut state, key, request.request, Instant::now());
                    }
                }
            }
        }
        self.inner.changed.notify_waiters();
    }

    pub(crate) fn finish(&self, request: &PullRequest) {
        Self::finish_inner(&self.inner, &request.key());
    }

    pub(crate) async fn stop(&self) {
        Self::stop_queue(&self.inner);
        if let Some(cancellation) = self
            .inner
            .cancellation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            cancellation.send_replace(true);
        }
        self.inner.changed.notify_waiters();
        let worker = self
            .inner
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker {
            let _ = worker.await;
        }
        self.inner.started.store(false, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn take_due(&self) -> Option<Arc<PullRequest>> {
        Self::take_due_inner(&self.inner)
    }

    #[cfg(test)]
    fn scheduled_pull_count(&self) -> usize {
        self.lock_state().scheduled.len()
    }

    fn logical_pull_count(&self) -> usize {
        let state = self.lock_state();
        let mut keys = state
            .scheduled_by_key
            .keys()
            .cloned()
            .chain(state.in_flight.iter().cloned())
            .chain(state.deferred.keys().cloned())
            .chain(state.paused_scheduled.keys().cloned())
            .collect::<HashSet<_>>();
        keys.retain(|key| !state.cancelled_in_flight.contains(key));
        keys.len()
    }

    #[cfg(test)]
    fn delayed_pull_count(&self) -> u64 {
        self.inner.delayed_pull_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn worker_count(&self) -> usize {
        usize::from(
            self.inner
                .worker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .is_some_and(|worker| !worker.is_finished()),
        )
    }

    async fn run(
        inner: Arc<PullSchedulerInner>,
        runtime: Weak<ClientRuntime>,
        mut cancellation: watch::Receiver<bool>,
    ) {
        loop {
            if *cancellation.borrow() {
                break;
            }
            let deadline = inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .scheduled
                .first_key_value()
                .map(|(scheduled_at, _)| scheduled_at.deadline);
            if deadline.is_none() {
                tokio::select! {
                    biased;
                    _ = cancellation.changed() => break,
                    _ = inner.changed.notified() => continue,
                }
            } else {
                let deadline = deadline.unwrap_or_else(Instant::now);
                tokio::select! {
                    biased;
                    _ = cancellation.changed() => break,
                    _ = inner.changed.notified() => continue,
                    _ = sleep_until(deadline) => {},
                }
            }

            while !*cancellation.borrow() {
                let Some(request) = Self::take_due_inner(&inner) else {
                    break;
                };
                let key = request.key();
                let Some(runtime) = runtime.upgrade() else {
                    Self::finish_inner(&inner, &key);
                    return;
                };
                let request: Arc<dyn MessageRequest> = request.clone();
                if runtime.execute_pull_request_immediately(request).is_err() {
                    Self::finish_inner(&inner, &key);
                }
            }
        }
    }

    fn take_due_inner(inner: &Arc<PullSchedulerInner>) -> Option<Arc<PullRequest>> {
        loop {
            let (key, request) = {
                let mut state = inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.stopped {
                    return None;
                }
                let (scheduled_at, _) = state.scheduled.first_key_value()?;
                if scheduled_at.deadline > Instant::now() {
                    return None;
                }
                let (_, scheduled) = state.scheduled.pop_first()?;
                let key = scheduled.key;
                state.scheduled_by_key.remove(&key);
                state.in_flight.insert(key.clone());
                (key, scheduled.request)
            };
            if request.is_current() {
                return Some(request);
            }
            Self::cancel_inner(inner, &key);
            Self::finish_inner(inner, &key);
        }
    }

    fn finish_inner(inner: &Arc<PullSchedulerInner>, key: &PullIdentity) {
        {
            let mut state = inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !state.in_flight.remove(key) {
                return;
            }
            if state.cancelled_in_flight.remove(key) || state.stopped {
                state.deferred.remove(key);
                return;
            }
            if let Some(deferred) = state.deferred.remove(key) {
                if state.all_paused || state.paused.contains(key) {
                    Self::insert_paused(
                        &mut state,
                        key.clone(),
                        deferred.request,
                        deferred.deadline,
                    );
                } else {
                    Self::insert_scheduled(
                        &mut state,
                        key.clone(),
                        deferred.request,
                        deferred.deadline,
                    );
                }
            }
        }
        inner.changed.notify_one();
    }

    fn cancel_inner(inner: &Arc<PullSchedulerInner>, key: &PullIdentity) {
        let removed = {
            let mut state = inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let scheduled = state
                .scheduled_by_key
                .remove(key)
                .and_then(|scheduled_at| state.scheduled.remove(&scheduled_at));
            let deferred = state.deferred.remove(key);
            let paused = state.paused.remove(key);
            let paused_scheduled = state.paused_scheduled.remove(key);
            if state.in_flight.contains(key) {
                state.cancelled_in_flight.insert(key.clone());
            }
            (scheduled, deferred, paused, paused_scheduled)
        };
        drop(removed);
        inner.changed.notify_one();
    }

    fn stop_queue(inner: &Arc<PullSchedulerInner>) {
        let pending = {
            let mut state = inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.stopped = true;
            state.all_paused = false;
            state.scheduled_by_key.clear();
            state.in_flight.clear();
            state.cancelled_in_flight.clear();
            state.paused.clear();
            (
                std::mem::take(&mut state.scheduled),
                std::mem::take(&mut state.deferred),
                std::mem::take(&mut state.paused_scheduled),
            )
        };
        drop(pending);
        inner.changed.notify_waiters();
    }

    fn insert_scheduled(
        state: &mut ScheduleState,
        key: PullIdentity,
        request: Arc<PullRequest>,
        deadline: Instant,
    ) {
        let scheduled_at = ScheduledAt {
            deadline,
            sequence: state.sequence,
        };
        state.sequence = state.sequence.wrapping_add(1);
        state.scheduled.insert(
            scheduled_at,
            ScheduledPull {
                key: key.clone(),
                request,
            },
        );
        state.scheduled_by_key.insert(key, scheduled_at);
    }

    fn insert_paused(
        state: &mut ScheduleState,
        key: PullIdentity,
        request: Arc<PullRequest>,
        deadline: Instant,
    ) {
        match state.paused_scheduled.get_mut(&key) {
            Some(paused) => {
                paused.deadline = paused.deadline.min(deadline);
                paused.request = request;
            }
            None => {
                state
                    .paused_scheduled
                    .insert(key, DeferredPull { deadline, request });
            }
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, ScheduleState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Drop for PullScheduler {
    fn drop(&mut self) {
        Self::stop_queue(&self.inner);
        if let Some(cancellation) = self
            .inner
            .cancellation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            cancellation.send_replace(true);
        }
        if let Some(worker) = self
            .inner
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            worker.abort();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EffectiveQueueThresholds {
    pub(crate) message_count: i32,
    pub(crate) size_mib: i32,
    pub(crate) max_span: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PullFlowControlReason {
    MessageCount,
    MessageBytes,
    MaxSpan,
}

pub(crate) fn effective_queue_thresholds(
    config: &PushConsumerConfig,
    process_queue_count: usize,
) -> EffectiveQueueThresholds {
    let scale = |topic_threshold: i32, queue_threshold: i32| {
        if topic_threshold != -1 && process_queue_count > 0 {
            (i64::from(topic_threshold) / process_queue_count.min(i64::MAX as usize) as i64).max(1)
                as i32
        } else {
            queue_threshold
        }
    };
    EffectiveQueueThresholds {
        message_count: scale(
            config.pull_threshold_for_topic(),
            config.pull_threshold_for_queue(),
        ),
        size_mib: scale(
            config.pull_threshold_size_for_topic_mib(),
            config.pull_threshold_size_for_queue_mib(),
        ),
        max_span: config.consume_concurrently_max_span(),
    }
}

pub(crate) fn flow_control_reason(
    queue: &ProcessQueueSnapshot,
    thresholds: EffectiveQueueThresholds,
) -> Option<PullFlowControlReason> {
    if queue.cached_count > thresholds.message_count as i64 {
        return Some(PullFlowControlReason::MessageCount);
    }
    if queue.cached_bytes / MEBIBYTE > thresholds.size_mib as i64 {
        return Some(PullFlowControlReason::MessageBytes);
    }
    if queue.max_span > i64::from(thresholds.max_span) {
        return Some(PullFlowControlReason::MaxSpan);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_common::message::{Message, MessageExt, MessageQueue};
    use std::time::Duration;

    fn process_queue(queue_id: i32, generation: u64) -> Arc<ProcessQueue> {
        Arc::new(ProcessQueue::new_for_assignment(
            MessageQueue::new("orders", "broker-a", queue_id),
            generation,
        ))
    }

    fn request(queue_id: i32, generation: u64, next_offset: i64) -> Arc<PullRequest> {
        Arc::new(PullRequest::new(
            "orders-consumer",
            process_queue(queue_id, generation),
            next_offset,
        ))
    }

    fn message(offset: i64, body_size: usize) -> MessageExt {
        let mut message =
            crate::test_support::message_ext_from(Message::new("orders", vec![b'x'; body_size]));
        message.set_queue_offset(offset);
        message
    }

    #[tokio::test]
    async fn scheduler_start_and_stop_owns_a_single_worker() {
        let scheduler = PullScheduler::new();
        scheduler.start(Weak::<ClientRuntime>::new()).unwrap();
        assert_eq!(scheduler.worker_count(), 1);
        assert_eq!(
            scheduler.start(Weak::<ClientRuntime>::new()),
            Err(PullSchedulerError::AlreadyStarted)
        );

        scheduler.stop().await;
        assert_eq!(scheduler.worker_count(), 0);
        assert_eq!(
            scheduler.start(Weak::<ClientRuntime>::new()),
            Err(PullSchedulerError::Stopped)
        );
    }

    #[tokio::test]
    async fn pull_rpc_workers_have_a_fixed_owned_set_and_join_on_stop() {
        let workers = PullRpcWorkers::new();
        let scheduler = Arc::new(PullScheduler::new());
        workers
            .start(Weak::<crate::push::PushConsumer>::new(), scheduler)
            .unwrap();
        assert_eq!(workers.worker_count(), PULL_RPC_WORKER_COUNT);
        assert_eq!(
            workers.start(
                Weak::<crate::push::PushConsumer>::new(),
                Arc::new(PullScheduler::new())
            ),
            Err(PullSchedulerError::AlreadyStarted)
        );

        workers.stop(100).await;
        assert_eq!(workers.worker_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn scheduler_coalesces_one_due_pull_per_queue_generation() {
        let scheduler = PullScheduler::new();
        let first = request(0, 1, 10);
        let replacement = request(0, 1, 20);
        scheduler.schedule(first, 50).unwrap();
        scheduler.schedule(replacement, 10).unwrap();

        assert_eq!(scheduler.logical_pull_count(), 1);
        assert_eq!(scheduler.scheduled_pull_count(), 1);
        assert_eq!(scheduler.delayed_pull_count(), 2);
        tokio::time::advance(Duration::from_millis(9)).await;
        assert!(scheduler.take_due().is_none());
        tokio::time::advance(Duration::from_millis(1)).await;
        let due = scheduler.take_due().unwrap();
        assert_eq!(due.generation(), 1);
        assert_eq!(due.next_offset(), 20);
        assert_eq!(scheduler.logical_pull_count(), 1);

        scheduler.finish(&due);
        assert_eq!(scheduler.logical_pull_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn active_pull_keeps_one_follow_up_and_cancellation_rejects_stale_work() {
        let scheduler = PullScheduler::new();
        let first = request(2, 7, 30);
        scheduler.schedule(first, 0).unwrap();
        let active = scheduler.take_due().unwrap();
        scheduler.schedule(request(2, 7, 31), 100).unwrap();
        scheduler.schedule(request(2, 7, 32), 0).unwrap();
        assert_eq!(scheduler.logical_pull_count(), 1);
        assert_eq!(scheduler.scheduled_pull_count(), 0);

        scheduler.finish(&active);
        let follow_up = scheduler.take_due().unwrap();
        assert_eq!(follow_up.next_offset(), 32);
        scheduler.cancel(follow_up.queue(), follow_up.generation());
        scheduler.finish(&follow_up);
        assert_eq!(scheduler.logical_pull_count(), 0);

        let delayed = request(3, 8, 40);
        scheduler.schedule(delayed.clone(), 25).unwrap();
        scheduler.cancel(delayed.queue(), delayed.generation());
        tokio::time::advance(Duration::from_millis(25)).await;
        assert!(scheduler.take_due().is_none());
        assert_eq!(scheduler.logical_pull_count(), 0);

        let stale = request(4, 9, 50);
        scheduler.schedule(stale.clone(), 5).unwrap();
        stale.process_queue().drop_assignment(9).unwrap();
        tokio::time::advance(Duration::from_millis(5)).await;
        assert!(scheduler.take_due().is_none());
        assert_eq!(scheduler.logical_pull_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn pause_holds_due_and_follow_up_pulls_until_resume() {
        let scheduler = PullScheduler::new();
        let scheduled = request(5, 15, 60);
        scheduler.schedule(scheduled.clone(), 10).unwrap();
        scheduler.pause(scheduled.queue(), scheduled.generation());
        assert_eq!(scheduler.scheduled_pull_count(), 0);
        assert_eq!(scheduler.logical_pull_count(), 1);

        tokio::time::advance(Duration::from_millis(10)).await;
        assert!(scheduler.take_due().is_none());
        scheduler.resume(scheduled.queue(), scheduled.generation());
        assert_eq!(scheduler.take_due().unwrap().next_offset(), 60);

        let active_request = request(6, 16, 70);
        scheduler.schedule(active_request.clone(), 0).unwrap();
        let active = scheduler.take_due().unwrap();
        scheduler.schedule(request(6, 16, 71), 10).unwrap();
        scheduler.pause(active_request.queue(), active_request.generation());
        scheduler.finish(&active);
        tokio::time::advance(Duration::from_millis(10)).await;
        assert!(scheduler.take_due().is_none());
        scheduler.resume(active_request.queue(), active_request.generation());
        assert_eq!(scheduler.take_due().unwrap().next_offset(), 71);
    }

    #[tokio::test(start_paused = true)]
    async fn global_suspend_holds_pending_requests_and_resume_wakes_them() {
        let scheduler = PullScheduler::new();
        let request = request(7, 17, 80);
        scheduler.schedule(request.clone(), 10).unwrap();
        scheduler.pause_all();
        tokio::time::advance(Duration::from_millis(10)).await;
        assert!(scheduler.take_due().is_none());
        scheduler.resume_all();
        assert_eq!(scheduler.take_due().unwrap().next_offset(), 80);
    }

    #[test]
    fn topic_thresholds_scale_to_process_queues_and_queue_backlog_includes_in_flight() {
        let config = PushConsumerConfig::new("orders-consumer").unwrap();
        config.set_pull_threshold_for_topic(7);
        config.set_pull_threshold_size_for_topic_mib(4);
        let scaled = effective_queue_thresholds(&config, 3);
        assert_eq!(scaled.message_count, 2);
        assert_eq!(scaled.size_mib, 1);
        assert_eq!(effective_queue_thresholds(&config, 0).message_count, 1_000);

        let queue = process_queue(0, 12);
        queue
            .insert_messages(12, vec![message(10, 0), message(11, 0)])
            .unwrap();
        let batch = queue.take_batch(12, 2).unwrap().unwrap();
        config.set_pull_threshold_for_queue(2);
        let thresholds = effective_queue_thresholds(&config, 0);
        assert_eq!(
            flow_control_reason(&queue.snapshot(12).unwrap(), thresholds),
            None
        );
        queue.insert_messages(12, vec![message(12, 0)]).unwrap();
        assert_eq!(
            flow_control_reason(&queue.snapshot(12).unwrap(), thresholds),
            Some(PullFlowControlReason::MessageCount)
        );
        assert_eq!(queue.snapshot(12).unwrap().in_flight_count, 2);
        queue.complete_batch(12, batch.id(), &[10, 11]).unwrap();

        config.set_pull_threshold_for_queue(10);
        config.set_pull_threshold_size_for_queue_mib(1);
        let bytes_queue = process_queue(1, 13);
        bytes_queue
            .insert_messages(13, vec![message(0, 2 * 1024 * 1024)])
            .unwrap();
        assert_eq!(
            flow_control_reason(
                &bytes_queue.snapshot(13).unwrap(),
                effective_queue_thresholds(&config, 0)
            ),
            Some(PullFlowControlReason::MessageBytes)
        );

        config.set_pull_threshold_size_for_queue_mib(100);
        config.set_consume_concurrently_max_span(2);
        let span_queue = process_queue(2, 14);
        span_queue
            .insert_messages(14, vec![message(0, 0), message(3, 0)])
            .unwrap();
        assert_eq!(
            flow_control_reason(
                &span_queue.snapshot(14).unwrap(),
                effective_queue_thresholds(&config, 0)
            ),
            Some(PullFlowControlReason::MaxSpan)
        );
    }
}
