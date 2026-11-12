use std::{
    any::Any,
    collections::{BTreeMap, HashMap, HashSet},
    future::Future,
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_core::runtime::{ClientRuntime, MessageRequest};
use rocketmq_classic_protocol::body::MessageRequestMode;
use tokio::{
    sync::{Notify, watch},
    task::{JoinHandle, JoinSet},
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

/// Releases one admitted queue-generation slot when its owned pull task ends.
/// The guard is built before the task is spawned and moved into the future, so
/// completion, a failed owner upgrade, panic unwinding, cancellation, and an
/// abort before the first poll each release the slot exactly once. It never
/// touches dispatcher state and holds no strong consumer reference.
struct PullCompletionGuard {
    request: Arc<PullRequest>,
    scheduler: Arc<PullScheduler>,
    active: Arc<AtomicUsize>,
}

impl Drop for PullCompletionGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
        self.scheduler.finish(&self.request);
    }
}

struct PullRpcWorkerSet {
    consumer: Weak<crate::push::PushConsumer>,
    scheduler: Arc<PullScheduler>,
    cancellation: watch::Sender<bool>,
    tasks: JoinSet<()>,
    active: Arc<AtomicUsize>,
}

/// Owned per-request pull dispatch. Every admitted request gets one task in the
/// owned `JoinSet`, so a long broker suspend stays off the shared runtime
/// PullMessageService worker without a fixed worker count or queue capacity.
/// `PullScheduler` still owns the one-active-pull-per-queue-generation
/// invariant, and transport admission plus ProcessQueue flow control bound how
/// many long-polls are actually outstanding.
pub(crate) struct PullRpcWorkers {
    state: Mutex<Option<PullRpcWorkerSet>>,
}

impl PullRpcWorkers {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(None),
        }
    }

    /// Install an empty admission set. Unlike `PullScheduler`, a stopped
    /// dispatcher is not restarted by the consumer, so only an existing set is
    /// rejected here.
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
        let (cancellation, _) = watch::channel(false);
        *state = Some(PullRpcWorkerSet {
            consumer,
            scheduler,
            cancellation,
            tasks: JoinSet::new(),
            active: Arc::new(AtomicUsize::new(0)),
        });
        Ok(())
    }

    pub(crate) fn try_submit(&self, request: Arc<PullRequest>) -> Result<(), Arc<PullRequest>> {
        self.submit_with(request, |consumer, request| async move {
            // A retired consumer releases its scheduler slot through the guard
            // instead of issuing a new RPC.
            let Some(consumer) = consumer.upgrade() else {
                return;
            };
            consumer.run_pull_worker_request(request).await;
        })
    }

    /// Admit one already-scheduled request. The caller must have taken the
    /// request from `PullScheduler`; this never bypasses identity admission.
    fn submit_with<Factory, Operation>(
        &self,
        request: Arc<PullRequest>,
        pull: Factory,
    ) -> Result<(), Arc<PullRequest>>
    where
        Factory:
            FnOnce(Weak<crate::push::PushConsumer>, Arc<PullRequest>) -> Operation + Send + 'static,
        Operation: Future<Output = ()> + Send + 'static,
    {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(workers) = state.as_mut() else {
            return Err(request);
        };
        // Reap finished tasks so retained entries do not grow with pull
        // iterations. Join results are observed here, never abandoned.
        while let Some(result) = workers.tasks.try_join_next() {
            report_join(result, "push pull task panicked and was reaped");
        }
        let cancellation = workers.cancellation.subscribe();
        let consumer = workers.consumer.clone();
        let scheduler = workers.scheduler.clone();
        let active = workers.active.clone();
        active.fetch_add(1, Ordering::AcqRel);
        let guard = PullCompletionGuard {
            request: request.clone(),
            scheduler,
            active,
        };
        // No await and no scheduler call happens while the dispatcher lock is
        // held; `spawn` only registers the task.
        workers
            .tasks
            .spawn(run_owned_pull(guard, cancellation, pull(consumer, request)));
        Ok(())
    }

    pub(crate) async fn stop(&self, wait_millis: i64) {
        let mut workers = match self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            Some(workers) => workers,
            None => return,
        };
        // Admission is closed by the take above; cancellation then lets running
        // pulls stop before their next await point.
        workers.cancellation.send_replace(true);
        let deadline = Instant::now() + Duration::from_millis(wait_millis.max(0) as u64);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, workers.tasks.join_next()).await {
                Err(_) => break,
                Ok(None) => return,
                Ok(Some(Ok(()))) => continue,
                Ok(Some(Err(error))) if error.is_cancelled() => continue,
                Ok(Some(Err(error))) => {
                    tracing::warn!(task = %error, "push pull task panicked during shutdown");
                    continue;
                }
            }
        }
        workers.tasks.abort_all();
        // Draining every join result lets each guard run and lets cancellation
        // cleanup finish; the wait budget bounds the join, not the abort drain.
        while let Some(result) = workers.tasks.join_next().await {
            report_join(result, "push pull task panicked during shutdown");
        }
    }

    /// Active admitted requests, not `JoinSet::len()`, which still counts
    /// completed tasks that have not been joined yet.
    #[cfg(test)]
    pub(crate) fn active_admissions(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map_or(0, |workers| workers.active.load(Ordering::Acquire))
    }
}

fn report_join(result: Result<(), tokio::task::JoinError>, message: &str) {
    match result {
        Ok(()) => {}
        Err(error) if error.is_cancelled() => {}
        Err(error) => tracing::warn!(task = %error, "{message}"),
    }
}

/// Run one owned pull request under its pre-created completion guard. The pull
/// future is scoped so it is dropped before the guard, which keeps slot release
/// ordered after any consumer reference the pull future held.
async fn run_owned_pull<Operation>(
    _guard: PullCompletionGuard,
    mut cancellation: watch::Receiver<bool>,
    pull: Operation,
) where
    Operation: Future<Output = ()> + Send,
{
    // A task first polled after cancellation must not issue a new RPC.
    if *cancellation.borrow_and_update() {
        return;
    }
    {
        let pull = std::pin::pin!(pull);
        tokio::select! {
            biased;
            _ = cancellation.changed() => {}
            _ = pull => {}
        }
    }
}

impl Drop for PullRpcWorkers {
    fn drop(&mut self) {
        if let Some(workers) = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            // The owned `JoinSet` aborts outstanding tasks when it drops; the
            // runtime observes their cleanup on its next poll rather than
            // synchronously inside `Drop`.
            workers.cancellation.send_replace(true);
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

    #[cfg(test)]
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
    use tokio::time::timeout;

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

    /// Test-only formatting so admission results can be unwrapped; the
    /// production type intentionally keeps its state private.
    impl std::fmt::Debug for PullRequest {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("PullRequest")
                .field("queue", self.queue())
                .field("generation", &self.generation())
                .field("next_offset", &self.next_offset())
                .finish()
        }
    }

    struct NotifyOnDrop(Arc<Notify>);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    /// Raw scheduler ownership that survives dispatcher `stop` taking its state.
    fn in_flight(scheduler: &PullScheduler) -> Vec<PullIdentity> {
        scheduler
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .in_flight
            .iter()
            .cloned()
            .collect::<Vec<_>>()
    }

    /// Retained witness for the dispatcher's active counter, read before any
    /// `stop` removes the state that owns it.
    fn active_witness(workers: &PullRpcWorkers) -> Arc<AtomicUsize> {
        workers
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .expect("dispatcher should be started")
            .active
            .clone()
    }

    fn admitted(scheduler: &PullScheduler, request: Arc<PullRequest>) -> Arc<PullRequest> {
        scheduler.schedule(request, 0).unwrap();
        scheduler.take_due().expect("request should be due")
    }

    #[tokio::test]
    async fn pull_rpc_workers_admit_every_request_without_a_fixed_width() {
        const QUEUES: usize = 80;
        let workers = PullRpcWorkers::new();
        let scheduler = Arc::new(PullScheduler::new());
        workers
            .start(Weak::<crate::push::PushConsumer>::new(), scheduler.clone())
            .unwrap();
        let active = active_witness(&workers);
        assert_eq!(active.load(Ordering::SeqCst), 0);

        // Every request enters the dispatcher through scheduler identity
        // admission, exactly as the runtime FIFO delivers them.
        for queue_id in 0..QUEUES as i32 {
            let admitted = admitted(&scheduler, request(queue_id, 1, 10));
            workers
                .submit_with(admitted, move |_owner, _request| {
                    std::future::pending::<()>()
                })
                .expect("every eligible queue must be admitted");
        }
        assert_eq!(active.load(Ordering::SeqCst), QUEUES);
        assert_eq!(in_flight(&scheduler).len(), QUEUES);

        // A JoinSet reports no fixed width: the retained task count is the
        // number of live requests, not a pool size.
        let tasks = workers
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .expect("dispatcher should be started")
            .tasks
            .len();
        assert_eq!(tasks, QUEUES);

        // Stop must cancel and join every outstanding task rather than leave a
        // detached tail behind.
        workers.stop(10).await;
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(in_flight(&scheduler).is_empty());
        // Closed admission returns the request to its caller.
        let rejected = request(90, 1, 10);
        let returned = workers.try_submit(rejected.clone()).unwrap_err();
        assert_eq!(returned.queue(), rejected.queue());
        assert_eq!(returned.generation(), rejected.generation());
    }

    #[tokio::test]
    async fn pull_rpc_workers_run_every_admitted_task_concurrently() {
        const QUEUES: usize = 80;
        let workers = PullRpcWorkers::new();
        let scheduler = Arc::new(PullScheduler::new());
        workers
            .start(Weak::<crate::push::PushConsumer>::new(), scheduler.clone())
            .unwrap();
        assert_eq!(
            workers.start(
                Weak::<crate::push::PushConsumer>::new(),
                Arc::new(PullScheduler::new())
            ),
            Err(PullSchedulerError::AlreadyStarted)
        );

        let entered = Arc::new(AtomicUsize::new(0));
        let (release, release_receiver) = watch::channel(false);
        for queue_id in 0..QUEUES as i32 {
            let admitted = admitted(&scheduler, request(queue_id, 1, 10));
            let entered = entered.clone();
            let mut release = release_receiver.clone();
            workers
                .submit_with(admitted, move |_owner, _request| async move {
                    entered.fetch_add(1, Ordering::SeqCst);
                    // Each task blocks until every other task has entered, so a
                    // narrower dispatch width can never reach the count.
                    let _ = release.wait_for(|released| *released).await;
                })
                .unwrap();
        }

        let deadline = Instant::now() + Duration::from_secs(5);
        while entered.load(Ordering::SeqCst) < QUEUES && Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            entered.load(Ordering::SeqCst),
            QUEUES,
            "all admitted pulls must enter concurrently"
        );

        release.send(true).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while workers.active_admissions() > 0 && Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        assert_eq!(workers.active_admissions(), 0);
        assert!(in_flight(&scheduler).is_empty());
        workers.stop(100).await;
    }

    #[tokio::test(start_paused = true)]
    async fn completed_pull_releases_its_slot_and_keeps_one_deferred_follow_up() {
        let workers = PullRpcWorkers::new();
        let scheduler = Arc::new(PullScheduler::new());
        workers
            .start(Weak::<crate::push::PushConsumer>::new(), scheduler.clone())
            .unwrap();
        let active = active_witness(&workers);

        let first = admitted(&scheduler, request(1, 3, 10));
        // A running pull may record exactly one deferred follow-up; only the
        // guard releases the scheduler slot afterwards.
        scheduler.schedule(request(1, 3, 11), 5).unwrap();
        workers
            .submit_with(first.clone(), |_owner, _request| async {})
            .unwrap();
        assert_eq!(active.load(Ordering::SeqCst), 1);
        tokio::task::yield_now().await;

        // The task finished but has not been joined; the separate active
        // counter and the raw scheduler set still prove cleanup happened.
        let deadline = Instant::now() + Duration::from_secs(1);
        while active.load(Ordering::SeqCst) > 0 && Instant::now() < deadline {
            tokio::time::advance(Duration::from_millis(1)).await;
        }
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(in_flight(&scheduler).is_empty());

        tokio::time::advance(Duration::from_millis(5)).await;
        let follow_up = scheduler.take_due().expect("one deferred follow-up");
        assert_eq!(follow_up.next_offset(), 11);
        assert!(scheduler.take_due().is_none());
        workers.stop(100).await;
    }

    #[tokio::test]
    async fn expired_consumer_owner_still_releases_the_admitted_slot() {
        let workers = PullRpcWorkers::new();
        let scheduler = Arc::new(PullScheduler::new());
        workers
            .start(Weak::<crate::push::PushConsumer>::new(), scheduler.clone())
            .unwrap();
        let active = active_witness(&workers);

        let admitted = admitted(&scheduler, request(2, 4, 20));
        // Production path: the weak owner is already gone, so no RPC runs and
        // the guard still releases the slot.
        workers.try_submit(admitted).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while active.load(Ordering::SeqCst) > 0 && Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(in_flight(&scheduler).is_empty());
        workers.stop(100).await;
    }

    #[tokio::test]
    async fn panicking_and_cancelled_pull_tasks_release_their_slots_once() {
        let workers = PullRpcWorkers::new();
        let scheduler = Arc::new(PullScheduler::new());
        workers
            .start(Weak::<crate::push::PushConsumer>::new(), scheduler.clone())
            .unwrap();
        let active = active_witness(&workers);
        let dropped = Arc::new(Notify::new());

        let panicking = admitted(&scheduler, request(3, 5, 30));
        workers
            .submit_with(panicking, |_owner, _request| async {
                panic!("pull task failure")
            })
            .unwrap();
        let cancelled = admitted(&scheduler, request(4, 6, 40));
        let witness = NotifyOnDrop(dropped.clone());
        workers
            .submit_with(cancelled, move |_owner, _request| async move {
                let _witness = witness;
                std::future::pending::<()>().await;
            })
            .unwrap();
        tokio::task::yield_now().await;

        // Stop observes both the panicked and cancelled join results without
        // abandoning the other task.
        workers.stop(50).await;
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(in_flight(&scheduler).is_empty());
        timeout(Duration::from_secs(1), dropped.notified())
            .await
            .expect("cancelled pull future must be dropped");
        // Repeated stop is safe.
        workers.stop(50).await;
    }

    #[tokio::test]
    async fn aborting_a_polled_pending_task_releases_its_slot() {
        let workers = PullRpcWorkers::new();
        let scheduler = Arc::new(PullScheduler::new());
        workers
            .start(Weak::<crate::push::PushConsumer>::new(), scheduler.clone())
            .unwrap();
        let active = active_witness(&workers);
        let dropped = Arc::new(Notify::new());

        let admitted = admitted(&scheduler, request(5, 7, 50));
        let witness = NotifyOnDrop(dropped.clone());
        workers
            .submit_with(admitted, move |_owner, _request| async move {
                let _witness = witness;
                std::future::pending::<()>().await;
            })
            .unwrap();
        // Force the task to be polled and parked before the zero wait budget.
        tokio::task::yield_now().await;
        assert_eq!(active.load(Ordering::SeqCst), 1);

        workers.stop(0).await;
        timeout(Duration::from_secs(1), dropped.notified())
            .await
            .expect("aborted pull future must be dropped");
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(in_flight(&scheduler).is_empty());
    }

    #[tokio::test]
    async fn aborting_before_the_first_poll_releases_every_slot() {
        let workers = PullRpcWorkers::new();
        let scheduler = Arc::new(PullScheduler::new());
        workers
            .start(Weak::<crate::push::PushConsumer>::new(), scheduler.clone())
            .unwrap();
        let active = active_witness(&workers);

        for queue_id in 0..8i32 {
            let admitted = admitted(&scheduler, request(queue_id, 8, 60));
            workers
                .submit_with(admitted, move |_owner, _request| {
                    std::future::pending::<()>()
                })
                .unwrap();
        }
        assert_eq!(active.load(Ordering::SeqCst), 8);

        // No yield between admission and stop: every task is aborted before it
        // is ever polled, so only the pre-created guard can release the slots.
        workers.stop(0).await;
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(in_flight(&scheduler).is_empty());
    }

    #[tokio::test]
    async fn dispatcher_drop_aborts_outstanding_pulls() {
        let workers = Arc::new(PullRpcWorkers::new());
        let scheduler = Arc::new(PullScheduler::new());
        workers
            .start(Weak::<crate::push::PushConsumer>::new(), scheduler.clone())
            .unwrap();
        let active = active_witness(&workers);
        for queue_id in 0..4i32 {
            let admitted = admitted(&scheduler, request(queue_id, 9, 70));
            workers
                .submit_with(admitted, move |_owner, _request| {
                    std::future::pending::<()>()
                })
                .unwrap();
        }
        tokio::task::yield_now().await;
        assert_eq!(active.load(Ordering::SeqCst), 4);

        drop(workers);
        // `Drop` aborts through the owned set; cleanup is observed once the
        // runtime polls the aborts, not synchronously inside `Drop`.
        let deadline = Instant::now() + Duration::from_secs(5);
        while active.load(Ordering::SeqCst) > 0 && Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(in_flight(&scheduler).is_empty());
    }

    #[tokio::test]
    async fn admission_racing_stop_either_owns_or_returns_each_request() {
        for round in 0..8u32 {
            let workers = Arc::new(PullRpcWorkers::new());
            let scheduler = Arc::new(PullScheduler::new());
            workers
                .start(Weak::<crate::push::PushConsumer>::new(), scheduler.clone())
                .unwrap();
            let active = active_witness(&workers);
            let stop = tokio::spawn({
                let workers = workers.clone();
                async move {
                    workers.stop(0).await;
                }
            });

            for queue_id in 0..4i32 {
                let generation = u64::from(round) + 10;
                let admitted = admitted(&scheduler, request(queue_id, generation, 80));
                match workers.submit_with(admitted.clone(), |_owner, _request| {
                    std::future::pending::<()>()
                }) {
                    // Owned by the dispatcher: its guard must run.
                    Ok(()) => {}
                    // Rejected by closed admission: the caller finishes it, as
                    // `pull_message` does.
                    Err(request) => scheduler.finish(&request),
                }
            }
            stop.await.unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            while active.load(Ordering::SeqCst) > 0 && Instant::now() < deadline {
                tokio::task::yield_now().await;
            }
            assert_eq!(active.load(Ordering::SeqCst), 0, "round {round}");
            assert!(in_flight(&scheduler).is_empty(), "round {round}");
        }
    }

    #[tokio::test]
    async fn old_generation_completion_cannot_release_a_new_generation_slot() {
        let workers = PullRpcWorkers::new();
        let scheduler = Arc::new(PullScheduler::new());
        workers
            .start(Weak::<crate::push::PushConsumer>::new(), scheduler.clone())
            .unwrap();

        // Rebalance cancels the old generation and assigns a new one while the
        // retiring request's task is still running.
        let retiring = admitted(&scheduler, request(6, 20, 90));
        scheduler.cancel(retiring.queue(), retiring.generation());
        let current = admitted(&scheduler, request(6, 21, 95));
        workers
            .submit_with(retiring.clone(), |_owner, _request| async {})
            .unwrap();
        // Rapid same-identity follow-ups arrive while the retiring task ends.
        scheduler.schedule(request(6, 21, 96), 0).unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        while in_flight(&scheduler) != vec![current.key()] && Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        assert_eq!(in_flight(&scheduler), vec![current.key()]);
        // The retiring completion released only its own identity.
        assert!(scheduler.take_due().is_none());

        scheduler.finish(&current);
        let follow_up = scheduler.take_due().expect("one deferred follow-up");
        assert_eq!(follow_up.next_offset(), 96);
        assert!(scheduler.take_due().is_none());
        workers.stop(100).await;
    }

    #[tokio::test]
    async fn completed_task_entries_are_reaped_across_rounds() {
        let workers = PullRpcWorkers::new();
        let scheduler = Arc::new(PullScheduler::new());
        workers
            .start(Weak::<crate::push::PushConsumer>::new(), scheduler.clone())
            .unwrap();
        let active = active_witness(&workers);

        for round in 0..200u32 {
            let admitted = admitted(&scheduler, request(7, 11, i64::from(round)));
            workers
                .submit_with(admitted, |_owner, _request| async {})
                .unwrap();
            tokio::task::yield_now().await;
        }
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(in_flight(&scheduler).is_empty());
        let retained = workers
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .expect("dispatcher should be started")
            .tasks
            .len();
        assert!(
            retained < 8,
            "completed task entries must not accumulate per iteration: {retained}"
        );
        workers.stop(100).await;
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
