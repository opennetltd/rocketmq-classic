use super::{
    ClientRuntime, RuntimeError, RuntimeState,
    stats::{StatsItemSetKind, StatsLogWindow, StatsScheduleKind, stats_schedule_specs},
};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicU8, Ordering},
    sync::{Arc, Mutex, Weak},
    task::Poll,
};
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
    time::{Duration, Instant},
};

#[cfg(test)]
#[derive(Default)]
pub(super) struct TaskProbe {
    pub created: std::sync::atomic::AtomicUsize,
    pub live: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
pub(super) struct LiveTask(pub(super) Arc<TaskProbe>);

#[cfg(test)]
impl Drop for LiveTask {
    fn drop(&mut self) {
        self.0
            .live
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum TaskKind {
    Discovery,
    Route,
    Heartbeat,
    Persist,
    Adjust,
    StatsSample(StatsItemSetKind),
    StatsPrint(StatsItemSetKind, StatsLogWindow),
}

#[derive(Clone, Copy)]
pub(super) struct Schedule {
    kind: TaskKind,
    delay: Duration,
    period: Duration,
}

pub(super) type UserFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub(super) type UserTaskFactory = Arc<dyn Fn() -> UserFuture + Send + Sync + 'static>;

const TASK_PENDING: u8 = 0;
const TASK_RUNNING: u8 = 1;
const TASK_FINISHED: u8 = 2;
const TASK_CANCELLED: u8 = 3;
const TASK_FAILED: u8 = 4;

const SCHEDULER_OPEN: u8 = 0;
const SCHEDULER_SHUTDOWN: u8 = 1;
const SCHEDULER_TERMINATED: u8 = 2;

pub(super) struct SchedulerStatus {
    state: AtomicU8,
    terminated: watch::Sender<bool>,
}

impl Default for SchedulerStatus {
    fn default() -> Self {
        let (terminated, _) = watch::channel(false);
        Self {
            state: AtomicU8::new(SCHEDULER_OPEN),
            terminated,
        }
    }
}

impl SchedulerStatus {
    pub(super) fn is_shutdown(&self) -> bool {
        self.state.load(Ordering::Acquire) != SCHEDULER_OPEN
    }

    pub(super) fn is_terminated(&self) -> bool {
        self.state.load(Ordering::Acquire) == SCHEDULER_TERMINATED
    }

    pub(super) fn shutdown(&self) {
        let _ = self.state.compare_exchange(
            SCHEDULER_OPEN,
            SCHEDULER_SHUTDOWN,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub(super) fn terminate(&self) {
        self.state.store(SCHEDULER_TERMINATED, Ordering::Release);
        self.terminated.send_replace(true);
    }

    pub(super) async fn await_termination(&self, timeout: Duration) -> bool {
        let mut terminated = self.terminated.subscribe();
        if self.is_terminated() {
            return true;
        }
        tokio::time::timeout(timeout, async {
            loop {
                if terminated.changed().await.is_err() {
                    return;
                }
                if self.is_terminated() {
                    return;
                }
            }
        })
        .await
        .is_ok()
    }
}

struct SchedulerTerminationGuard(Arc<SchedulerStatus>);

impl Drop for SchedulerTerminationGuard {
    fn drop(&mut self) {
        self.0.terminate();
    }
}

/// A handle to one task accepted by a runtime's client scheduler.
///
/// Dropping the handle leaves the scheduled task running. Call [`cancel`](Self::cancel)
/// to cancel it explicitly. Cloned handles compare equal by scheduled-task
/// identity.
#[derive(Clone)]
pub struct RuntimeScheduledTaskHandle {
    state: Arc<AtomicU8>,
    cancel: Arc<Notify>,
    changed: Arc<Notify>,
    due: Arc<Mutex<Instant>>,
}

/// Failure reported by the asynchronous wait methods on a scheduled-task handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RuntimeScheduledTaskWaitError {
    /// The scheduled work was cancelled before it completed.
    #[error("scheduled task was cancelled")]
    Cancelled,
    /// The scheduled callback panicked.
    #[error("scheduled task callback failed")]
    Failed,
    /// The requested wait deadline elapsed while the task remained pending.
    #[error("timed out waiting for scheduled task")]
    Timeout,
}

/// Result-bearing work submitted to a runtime-owned scheduler.
pub struct RuntimeScheduledCall<T> {
    task: RuntimeScheduledTaskHandle,
    result: tokio::sync::oneshot::Receiver<T>,
}

/// Per-task result returned by [`RuntimeScheduler::invoke_all_with_timeout`].
#[derive(Debug)]
pub enum RuntimeScheduledTaskOutcome<T> {
    /// The callback returned a value before the deadline.
    Completed(T),
    /// The callback panicked, was cancelled, or its runtime stopped.
    FailedOrCancelled,
    /// The callback was still pending when the timed invocation expired.
    CancelledByTimeout,
}

struct ScheduledCallsCancellationGuard {
    tasks: Vec<RuntimeScheduledTaskHandle>,
    armed: bool,
}

impl ScheduledCallsCancellationGuard {
    fn new() -> Self {
        Self {
            tasks: Vec::new(),
            armed: true,
        }
    }

    fn track(&mut self, task: RuntimeScheduledTaskHandle) {
        self.tasks.push(task);
    }

    fn cancel_except(&self, keep: usize) {
        for (index, task) in self.tasks.iter().enumerate() {
            if index != keep {
                task.cancel();
            }
        }
    }

    fn cancel_all(&self) {
        for task in &self.tasks {
            task.cancel();
        }
    }

    fn cancel_unfinished(&self, completed: &[bool]) {
        for (index, task) in self.tasks.iter().enumerate() {
            if !completed.get(index).copied().unwrap_or(false) {
                task.cancel();
            }
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ScheduledCallsCancellationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cancel_all();
        }
    }
}

impl<T> RuntimeScheduledCall<T> {
    /// Cancels the scheduled call if it has not already completed.
    pub fn cancel(&self) -> bool {
        self.task.cancel()
    }

    /// Returns whether the call completed or was cancelled.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    /// Returns whether the call was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.task.is_cancelled()
    }

    /// Returns a clone of the scheduled task handle associated with this call.
    ///
    /// This is useful when matching a call against the queued handles returned
    /// by [`RuntimeScheduler::shutdown_now`].
    pub fn task_handle(&self) -> RuntimeScheduledTaskHandle {
        self.task.clone()
    }

    /// Waits asynchronously for the call's result.
    ///
    /// The receive error indicates cancellation, a panicking callback, or
    /// runtime shutdown before the callback could return a value.
    pub async fn join(self) -> Result<T, tokio::sync::oneshot::error::RecvError> {
        self.result.await
    }

    /// Waits up to `timeout` without cancelling the scheduled callback.
    ///
    /// The mutable borrow keeps the result receiver available if the timeout
    /// expires, so the caller can wait again or use [`join`](Self::join) later.
    pub async fn join_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Result<T, tokio::sync::oneshot::error::RecvError>, tokio::time::error::Elapsed>
    {
        tokio::time::timeout(timeout, &mut self.result).await
    }
}

impl RuntimeScheduledTaskHandle {
    pub(super) fn pending(due: Instant) -> Self {
        Self {
            state: Arc::new(AtomicU8::new(TASK_PENDING)),
            cancel: Arc::new(Notify::new()),
            changed: Arc::new(Notify::new()),
            due: Arc::new(Mutex::new(due)),
        }
    }

    /// Cancels a task that has not already finished or been cancelled.
    pub fn cancel(&self) -> bool {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            if matches!(current, TASK_FINISHED | TASK_FAILED | TASK_CANCELLED) {
                return false;
            }
            match self.state.compare_exchange_weak(
                current,
                TASK_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.cancel.notify_one();
                    self.changed.notify_one();
                    return true;
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Returns whether the task has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::Acquire) == TASK_CANCELLED
    }

    /// Returns whether the task completed or was cancelled.
    pub fn is_finished(&self) -> bool {
        matches!(
            self.state.load(Ordering::Acquire),
            TASK_FINISHED | TASK_FAILED | TASK_CANCELLED
        )
    }

    /// Returns the nonnegative time remaining until the task's next scheduled
    /// invocation. A completed or overdue task returns zero.
    pub fn remaining_delay(&self) -> Duration {
        if self.is_finished() {
            return Duration::ZERO;
        }
        self.due
            .lock()
            .map(|due| due.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::ZERO)
    }

    /// Waits until the task completes, fails, or is cancelled.
    ///
    /// A repeating task remains pending until it is cancelled or its callback
    /// fails, matching `ScheduledFuture::get()` completion behavior.
    pub async fn wait(&self) -> Result<(), RuntimeScheduledTaskWaitError> {
        loop {
            let changed = self.changed.notified();
            match self.state.load(Ordering::Acquire) {
                TASK_FINISHED => return Ok(()),
                TASK_FAILED => return Err(RuntimeScheduledTaskWaitError::Failed),
                TASK_CANCELLED => return Err(RuntimeScheduledTaskWaitError::Cancelled),
                _ => changed.await,
            }
        }
    }

    /// Waits until the task completes or `timeout` expires.
    ///
    /// A timeout leaves the scheduled task running, as Java's timed
    /// `Future.get` does; cancel it separately when that is desired.
    pub async fn wait_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<(), RuntimeScheduledTaskWaitError> {
        if self.is_finished() {
            return self.wait().await;
        }
        tokio::time::timeout(timeout, self.wait())
            .await
            .map_err(|_| RuntimeScheduledTaskWaitError::Timeout)?
    }
}

impl PartialEq for RuntimeScheduledTaskHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
}

impl Eq for RuntimeScheduledTaskHandle {}

#[derive(Clone)]
pub(super) struct UserSchedule {
    pub(super) delay: Duration,
    pub(super) period: Option<Duration>,
    pub(super) fixed_rate: bool,
    pub(super) factory: UserTaskFactory,
    pub(super) state: Arc<AtomicU8>,
    pub(super) cancel: Arc<Notify>,
    pub(super) changed: Arc<Notify>,
    due: Arc<Mutex<Instant>>,
}

impl UserSchedule {
    fn new(
        delay: Duration,
        period: Option<Duration>,
        fixed_rate: bool,
        factory: UserTaskFactory,
        changed: Arc<Notify>,
    ) -> (Self, RuntimeScheduledTaskHandle) {
        let state = Arc::new(AtomicU8::new(TASK_PENDING));
        let cancel = Arc::new(Notify::new());
        let due = Arc::new(Mutex::new(Instant::now() + delay));
        (
            Self {
                delay,
                period,
                fixed_rate,
                factory,
                state: state.clone(),
                cancel: cancel.clone(),
                changed: changed.clone(),
                due: due.clone(),
            },
            RuntimeScheduledTaskHandle {
                state,
                cancel,
                changed,
                due,
            },
        )
    }

    pub(super) fn task_handle(&self) -> RuntimeScheduledTaskHandle {
        RuntimeScheduledTaskHandle {
            state: self.state.clone(),
            cancel: self.cancel.clone(),
            changed: self.changed.clone(),
            due: self.due.clone(),
        }
    }

    pub(super) fn due_at(&self) -> Instant {
        self.due
            .lock()
            .map(|due| *due)
            .unwrap_or_else(|_| Instant::now() + self.delay)
    }

    pub(super) fn update_due(&self, due: Instant) {
        if let Ok(mut current) = self.due.lock() {
            *current = due;
        }
    }

    pub(super) fn reject(&self) {
        let mut current = self.state.load(Ordering::Acquire);
        while matches!(current, TASK_PENDING | TASK_RUNNING) {
            match self.state.compare_exchange_weak(
                current,
                TASK_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        self.changed.notify_one();
        self.cancel.notify_one();
    }

    pub(super) fn fail(&self) {
        let mut current = self.state.load(Ordering::Acquire);
        while matches!(current, TASK_PENDING | TASK_RUNNING) {
            match self.state.compare_exchange_weak(
                current,
                TASK_FAILED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        self.changed.notify_one();
    }

    pub(super) fn try_start(&self) -> bool {
        self.state
            .compare_exchange(
                TASK_PENDING,
                TASK_RUNNING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub(super) fn complete(&self) -> bool {
        let next = if self.period.is_some() {
            TASK_PENDING
        } else {
            TASK_FINISHED
        };
        self.state
            .compare_exchange(TASK_RUNNING, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::Acquire) == TASK_CANCELLED
    }
}

/// Rust-native access to the single-threaded scheduler owned by `MQClientInstance`.
///
/// The handle schedules async work on the runtime's client scheduler, whose
/// lifetime remains owned by `ClientRuntime`.
pub struct RuntimeScheduler {
    runtime: Weak<ClientRuntime>,
    owner: RuntimeSchedulerOwner,
}

#[derive(Clone, Copy)]
pub(super) enum RuntimeSchedulerOwner {
    Client,
    Pull,
}

impl RuntimeScheduler {
    pub(super) fn new(runtime: Weak<ClientRuntime>, owner: RuntimeSchedulerOwner) -> Self {
        Self { runtime, owner }
    }

    /// Stops admission and lets already queued one-shot work drain.
    pub async fn shutdown(&self) -> Result<(), RuntimeError> {
        match self.owner {
            RuntimeSchedulerOwner::Client => self
                .runtime
                .upgrade()
                .ok_or(RuntimeError::InvalidState(RuntimeState::Stopped))?
                .shutdown_task_scheduler(false)
                .await
                .map(|_| ()),
            RuntimeSchedulerOwner::Pull => self
                .runtime
                .upgrade()
                .ok_or(RuntimeError::InvalidState(RuntimeState::Stopped))?
                .shutdown_pull_task_scheduler(false)
                .await
                .map(|_| ()),
        }
    }

    /// Stops admission and returns handles for queued work without cancelling
    /// those tasks, matching `ScheduledExecutorService.shutdownNow()`'s return
    /// list. Running work is cancelled without waiting for this worker to
    /// finish.
    pub async fn shutdown_now(&self) -> Result<Vec<RuntimeScheduledTaskHandle>, RuntimeError> {
        match self.owner {
            RuntimeSchedulerOwner::Client => {
                self.runtime
                    .upgrade()
                    .ok_or(RuntimeError::InvalidState(RuntimeState::Stopped))?
                    .shutdown_task_scheduler(true)
                    .await
            }
            RuntimeSchedulerOwner::Pull => {
                self.runtime
                    .upgrade()
                    .ok_or(RuntimeError::InvalidState(RuntimeState::Stopped))?
                    .shutdown_pull_task_scheduler(true)
                    .await
            }
        }
    }

    /// Returns whether this scheduler has stopped accepting new work.
    pub fn is_shutdown(&self) -> bool {
        self.runtime
            .upgrade()
            .is_none_or(|runtime| match self.owner {
                RuntimeSchedulerOwner::Client => runtime.task_scheduler_status.is_shutdown(),
                RuntimeSchedulerOwner::Pull => runtime.pull.is_scheduler_shutdown(),
            })
    }

    /// Returns whether this scheduler has finished all accepted work.
    pub fn is_terminated(&self) -> bool {
        self.runtime
            .upgrade()
            .is_none_or(|runtime| match self.owner {
                RuntimeSchedulerOwner::Client => runtime.task_scheduler_status.is_terminated(),
                RuntimeSchedulerOwner::Pull => runtime.pull.is_scheduler_terminated(),
            })
    }

    /// Waits asynchronously for termination, returning false when the timeout
    /// expires first.
    pub async fn await_termination(&self, timeout: Duration) -> bool {
        let Some(runtime) = self.runtime.upgrade() else {
            return true;
        };
        match self.owner {
            RuntimeSchedulerOwner::Client => {
                runtime
                    .task_scheduler_status
                    .await_termination(timeout)
                    .await
            }
            RuntimeSchedulerOwner::Pull => runtime.pull.await_scheduler_termination(timeout).await,
        }
    }

    /// Queues a task for immediate execution on the client scheduler.
    pub fn execute(
        &self,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> Result<RuntimeScheduledTaskHandle, RuntimeError> {
        self.schedule_after(Duration::ZERO, task)
    }

    /// Queues a task for execution after `delay`.
    pub fn schedule_after(
        &self,
        delay: Duration,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> Result<RuntimeScheduledTaskHandle, RuntimeError> {
        let pending = Arc::new(Mutex::new(Some(Box::pin(task) as UserFuture)));
        self.enqueue(
            delay,
            None,
            true,
            Arc::new(move || {
                pending
                    .lock()
                    .expect("one-shot scheduled task future lock poisoned")
                    .take()
                    .expect("one-shot scheduled task future already consumed")
            }),
        )
    }

    /// Submits result-bearing work for immediate execution.
    pub fn submit<T, Fut>(&self, task: Fut) -> Result<RuntimeScheduledCall<T>, RuntimeError>
    where
        T: Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        self.schedule_call_after(Duration::ZERO, task)
    }

    /// Schedules every task, then waits for all results in submission order.
    ///
    /// A failed or cancelled callback is represented by its individual
    /// receive error. If this future is cancelled, all accepted tasks are
    /// cancelled as well. Wrap the future in `tokio::time::timeout` to apply a
    /// caller-selected deadline.
    pub async fn invoke_all<T, Fut, Tasks>(
        &self,
        tasks: Tasks,
    ) -> Result<Vec<Result<T, tokio::sync::oneshot::error::RecvError>>, RuntimeError>
    where
        T: Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        Tasks: IntoIterator<Item = Fut>,
    {
        let mut cancellation = ScheduledCallsCancellationGuard::new();
        let mut calls = Vec::new();
        for task in tasks {
            let call = self.submit(task)?;
            cancellation.track(call.task.clone());
            calls.push(call);
        }

        let mut results = Vec::with_capacity(calls.len());
        for call in calls {
            results.push(call.join().await);
        }
        cancellation.disarm();
        Ok(results)
    }

    /// Schedules every task and collects outcomes until `timeout` expires.
    ///
    /// Results retain submission order. Tasks still pending at the deadline
    /// are cancelled and reported as [`RuntimeScheduledTaskOutcome::CancelledByTimeout`].
    /// A timeout of zero cancels all work immediately after it is accepted.
    pub async fn invoke_all_with_timeout<T, Fut, Tasks>(
        &self,
        tasks: Tasks,
        timeout: Duration,
    ) -> Result<Vec<RuntimeScheduledTaskOutcome<T>>, RuntimeError>
    where
        T: Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        Tasks: IntoIterator<Item = Fut>,
    {
        let deadline = Instant::now().checked_add(timeout);
        let mut cancellation = ScheduledCallsCancellationGuard::new();
        let mut calls = Vec::new();
        for task in tasks {
            let call = self.submit(task)?;
            cancellation.track(call.task.clone());
            calls.push(call);
        }

        let mut outcomes = std::iter::repeat_with(|| None)
            .take(calls.len())
            .collect::<Vec<Option<RuntimeScheduledTaskOutcome<T>>>>();
        let mut timed_out = false;
        for (index, call) in calls.into_iter().enumerate() {
            let joined = match deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, call.join()).await,
                None => Ok(call.join().await),
            };
            match joined {
                Ok(Ok(value)) => {
                    outcomes[index] = Some(RuntimeScheduledTaskOutcome::Completed(value));
                }
                Ok(Err(_)) => {
                    outcomes[index] = Some(RuntimeScheduledTaskOutcome::FailedOrCancelled);
                }
                Err(_) => {
                    timed_out = true;
                    let completed = outcomes.iter().map(Option::is_some).collect::<Vec<_>>();
                    cancellation.cancel_unfinished(&completed);
                    break;
                }
            }
        }

        cancellation.disarm();
        Ok(outcomes
            .into_iter()
            .map(|outcome| match (timed_out, outcome) {
                (_, Some(outcome)) => outcome,
                (true, None) => RuntimeScheduledTaskOutcome::CancelledByTimeout,
                (false, None) => RuntimeScheduledTaskOutcome::FailedOrCancelled,
            })
            .collect())
    }

    /// Returns the value from the first task that completes normally.
    ///
    /// Tasks run on this scheduler's single worker, so they are considered in
    /// submission order. Panicking or cancelled tasks are skipped. A returned
    /// value such as `Result::Err` is still a normal task result. If this
    /// future is cancelled, all accepted tasks are cancelled as well. Wrap the
    /// future in `tokio::time::timeout` to apply a caller-selected deadline.
    pub async fn invoke_any<T, Fut, Tasks>(&self, tasks: Tasks) -> Result<T, RuntimeError>
    where
        T: Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        Tasks: IntoIterator<Item = Fut>,
    {
        let mut cancellation = ScheduledCallsCancellationGuard::new();
        let mut calls = Vec::new();
        let any_succeeded = Arc::new(std::sync::atomic::AtomicBool::new(false));
        for task in tasks {
            let any_succeeded = any_succeeded.clone();
            let call = self.submit(async move {
                if any_succeeded.load(Ordering::Acquire) {
                    None
                } else {
                    let result = task.await;
                    any_succeeded.store(true, Ordering::Release);
                    Some(result)
                }
            })?;
            cancellation.track(call.task.clone());
            calls.push(call);
        }
        if calls.is_empty() {
            cancellation.disarm();
            return Err(RuntimeError::EmptyTaskSet);
        }

        for (index, call) in calls.into_iter().enumerate() {
            if let Ok(Some(result)) = call.join().await {
                cancellation.cancel_except(index);
                cancellation.disarm();
                return Ok(result);
            }
        }

        cancellation.disarm();
        Err(RuntimeError::NoSuccessfulTask)
    }

    /// Returns the first normal callback value, cancelling other accepted
    /// work when `timeout` expires. A returned `Err` inside `Result` is a
    /// normal callback value. Empty input returns [`RuntimeError::EmptyTaskSet`].
    pub async fn invoke_any_with_timeout<T, Fut, Tasks>(
        &self,
        tasks: Tasks,
        timeout: Duration,
    ) -> Result<T, RuntimeError>
    where
        T: Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        Tasks: IntoIterator<Item = Fut>,
    {
        let deadline = Instant::now().checked_add(timeout);
        let mut cancellation = ScheduledCallsCancellationGuard::new();
        let mut calls = Vec::new();
        let any_succeeded = Arc::new(std::sync::atomic::AtomicBool::new(false));
        for task in tasks {
            let any_succeeded = any_succeeded.clone();
            let call = self.submit(async move {
                if any_succeeded.load(Ordering::Acquire) {
                    None
                } else {
                    let result = task.await;
                    any_succeeded.store(true, Ordering::Release);
                    Some(result)
                }
            })?;
            cancellation.track(call.task.clone());
            calls.push(call);
        }
        if calls.is_empty() {
            cancellation.disarm();
            return Err(RuntimeError::EmptyTaskSet);
        }

        for (index, call) in calls.into_iter().enumerate() {
            let joined = match deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, call.join()).await,
                None => Ok(call.join().await),
            };
            match joined {
                Ok(Ok(Some(result))) => {
                    cancellation.cancel_except(index);
                    cancellation.disarm();
                    return Ok(result);
                }
                Ok(Ok(None) | Err(_)) => {}
                Err(_) => return Err(RuntimeError::InvocationTimeout),
            }
        }

        cancellation.disarm();
        Err(RuntimeError::NoSuccessfulTask)
    }

    /// Schedules result-bearing work and returns a handle that can await its
    /// result or cancel it before it completes.
    pub fn schedule_call_after<T, Fut>(
        &self,
        delay: Duration,
        task: Fut,
    ) -> Result<RuntimeScheduledCall<T>, RuntimeError>
    where
        T: Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let (result_tx, result) = tokio::sync::oneshot::channel();
        let scheduled = self.schedule_after(delay, async move {
            let value = task.await;
            let _ = result_tx.send(value);
        })?;
        Ok(RuntimeScheduledCall {
            task: scheduled,
            result,
        })
    }

    /// Queues a callback at a fixed rate. Calls run serially on the client
    /// scheduler and retain the prior-deadline catch-up behavior of Java's
    /// `scheduleAtFixedRate`.
    pub fn schedule_at_fixed_rate<F, Fut>(
        &self,
        initial_delay: Duration,
        period: Duration,
        callback: F,
    ) -> Result<RuntimeScheduledTaskHandle, RuntimeError>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if period.is_zero() {
            return Err(RuntimeError::InvalidSchedule);
        }
        let callback = Arc::new(callback);
        self.enqueue(
            initial_delay,
            Some(period),
            true,
            Arc::new(move || Box::pin(callback())),
        )
    }

    /// Queues a callback with a delay measured from the previous completion.
    /// Calls run serially on the client scheduler.
    pub fn schedule_with_fixed_delay<F, Fut>(
        &self,
        initial_delay: Duration,
        delay: Duration,
        callback: F,
    ) -> Result<RuntimeScheduledTaskHandle, RuntimeError>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if delay.is_zero() {
            return Err(RuntimeError::InvalidSchedule);
        }
        let callback = Arc::new(callback);
        self.enqueue(
            initial_delay,
            Some(delay),
            false,
            Arc::new(move || Box::pin(callback())),
        )
    }

    fn enqueue(
        &self,
        delay: Duration,
        period: Option<Duration>,
        fixed_rate: bool,
        factory: UserTaskFactory,
    ) -> Result<RuntimeScheduledTaskHandle, RuntimeError> {
        let runtime = self
            .runtime
            .upgrade()
            .ok_or(RuntimeError::InvalidState(RuntimeState::Stopped))?;
        let changed = match self.owner {
            RuntimeSchedulerOwner::Client => runtime.task_scheduler_changed.clone(),
            RuntimeSchedulerOwner::Pull => runtime.pull.scheduled_notifier(),
        };
        let (schedule, handle) = UserSchedule::new(delay, period, fixed_rate, factory, changed);
        match self.owner {
            RuntimeSchedulerOwner::Client => runtime.enqueue_scheduled_task(schedule)?,
            RuntimeSchedulerOwner::Pull => runtime.enqueue_pull_scheduled_task(schedule)?,
        }
        Ok(handle)
    }
}

#[derive(Clone)]
enum ScheduledJob {
    Periodic(Schedule),
    User(UserSchedule),
}

#[derive(Default)]
struct SchedulerQueue {
    jobs: BTreeMap<(Instant, u64), ScheduledJob>,
    next_order: u64,
    closed: bool,
}

impl SchedulerQueue {
    fn insert(&mut self, due: Instant, job: ScheduledJob) -> u64 {
        let order = self.next_order;
        self.next_order = self.next_order.wrapping_add(1);
        self.jobs.insert((due, order), job);
        order
    }

    fn remove_cancelled_users(&mut self) {
        self.jobs.retain(|_, job| {
            !matches!(job, ScheduledJob::User(schedule) if schedule.state.load(Ordering::Acquire) == TASK_CANCELLED)
        });
    }
}

pub(super) fn schedules(
    config: &crate::config::ClientConfig,
) -> Result<Vec<Schedule>, RuntimeError> {
    let mut result = Vec::new();
    if config.namesrv_addr.is_none() {
        result.push(Schedule {
            kind: TaskKind::Discovery,
            delay: Duration::from_secs(10),
            period: Duration::from_secs(120),
        });
    }
    for (kind, delay, period) in [
        (TaskKind::Route, 10, config.poll_name_server_interval),
        (TaskKind::Heartbeat, 1_000, config.heartbeat_broker_interval),
        (
            TaskKind::Persist,
            10_000,
            config.persist_consumer_offset_interval,
        ),
        (TaskKind::Adjust, 60_000, 60_000),
    ] {
        if period <= 0 {
            return Err(RuntimeError::InvalidSchedule);
        }
        result.push(Schedule {
            kind,
            delay: Duration::from_millis(delay),
            period: Duration::from_millis(period as u64),
        });
    }
    Ok(result)
}

/// `ConsumerStatsManager` constructs five `StatsItemSet`s. Each set registers
/// three sampling jobs and three log jobs with the shared Java client executor.
/// Keep these separate from `schedules`, which represents the jobs registered
/// by `MQClientInstance.startScheduledTask()`.
pub(super) fn stats_schedules() -> Vec<Schedule> {
    stats_schedule_specs()
        .into_iter()
        .map(|spec| Schedule {
            kind: match spec.kind {
                StatsScheduleKind::Sample(set) => TaskKind::StatsSample(set),
                StatsScheduleKind::Print(set, window) => TaskKind::StatsPrint(set, window),
            },
            delay: spec.delay,
            period: spec.period,
        })
        .collect()
}

/// Java's single scheduled executor: fixed-rate deadlines, no overlapping jobs,
/// catch-up after slow work, and one cancellation/join owner.
pub(super) struct OwnedTasks {
    schedules: Arc<Mutex<SchedulerQueue>>,
    changed: Arc<Notify>,
    status: Arc<SchedulerStatus>,
    active: watch::Sender<bool>,
    cancel: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl OwnedTasks {
    pub fn new(runtime: Weak<ClientRuntime>, status: Arc<SchedulerStatus>) -> Self {
        #[cfg(test)]
        let live = runtime.upgrade().map(|runtime| {
            let probe = runtime.periodic_probe.clone();
            probe
                .created
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            probe.live.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            LiveTask(probe)
        });
        let (active, mut activation) = watch::channel(false);
        let (cancel, mut cancellation) = watch::channel(false);
        let mut queue = SchedulerQueue {
            closed: status.is_shutdown(),
            ..SchedulerQueue::default()
        };
        if !queue.closed {
            for schedule in stats_schedules() {
                queue.insert(
                    Instant::now() + schedule.delay,
                    ScheduledJob::Periodic(schedule),
                );
            }
        }
        let schedules = Arc::new(Mutex::new(queue));
        let changed = runtime
            .upgrade()
            .map(|runtime| runtime.task_scheduler_changed.clone())
            .unwrap_or_else(|| Arc::new(Notify::new()));
        let worker_schedules = schedules.clone();
        let worker_changed = changed.clone();
        let worker_status = status.clone();
        let task = tokio::spawn(async move {
            #[cfg(test)]
            let _live = live;
            let _termination = SchedulerTerminationGuard(worker_status.clone());
            let Some(rebalance) = runtime.upgrade().map(|runtime| runtime.rebalance.clone()) else {
                return;
            };
            tokio::select! { biased;
                _ = cancellation.changed() => return,
                _ = activation.changed() => {},
            }
            loop {
                let next = match worker_schedules.lock() {
                    Ok(mut queue) => {
                        queue.remove_cancelled_users();
                        if queue.closed && queue.jobs.is_empty() {
                            return;
                        }
                        queue.jobs.first_key_value().map(|(key, _)| key.0)
                    }
                    Err(_) => return,
                };
                let Some(due) = next else {
                    tokio::select! { biased;
                        _ = cancellation.changed() => return,
                        _ = worker_changed.notified() => continue,
                        _ = rebalance.scheduled.notified() => {
                            rebalance.fire_due();
                            continue;
                        },
                    }
                };
                let wake_due = rebalance.next_due().filter(|wake| *wake < due);
                let next_wake = wake_due.unwrap_or(due);
                tokio::select! { biased;
                    _ = cancellation.changed() => return,
                    _ = worker_changed.notified() => continue,
                    _ = rebalance.scheduled.notified() => {
                        rebalance.fire_due();
                        continue;
                    },
                    _ = tokio::time::sleep_until(next_wake) => {},
                }
                if wake_due.is_some() {
                    rebalance.fire_due();
                    continue;
                }
                let (key, job) = match worker_schedules.lock() {
                    Ok(mut queue) => {
                        queue.remove_cancelled_users();
                        if queue
                            .jobs
                            .first_key_value()
                            .is_some_and(|(key, _)| key.0 <= Instant::now())
                        {
                            queue
                                .jobs
                                .pop_first()
                                .expect("first scheduled job was checked")
                        } else {
                            continue;
                        }
                    }
                    Err(_) => return,
                };
                let Some(runtime) = runtime.upgrade() else {
                    return;
                };
                match job {
                    ScheduledJob::Periodic(schedule) => {
                        let mut operation = Box::pin(runtime.run_periodic(schedule.kind));
                        let caught = std::future::poll_fn(|context| {
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                operation.as_mut().poll(context)
                            }))
                            .unwrap_or(Poll::Ready(Err(RuntimeError::TaskCallbackFailed)))
                        });
                        tokio::select! { biased;
                            _ = cancellation.changed() => return,
                            _ = caught => {},
                        }
                        if let Ok(mut queue) = worker_schedules.lock() {
                            if !queue.closed {
                                queue.jobs.insert(
                                    ((key.0 + schedule.period), key.1),
                                    ScheduledJob::Periodic(schedule),
                                );
                            }
                        } else {
                            return;
                        }
                    }
                    ScheduledJob::User(schedule) => {
                        if schedule
                            .state
                            .compare_exchange(
                                TASK_PENDING,
                                TASK_RUNNING,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_err()
                        {
                            continue;
                        }
                        let mut operation =
                            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                (schedule.factory)()
                            })) {
                                Ok(operation) => operation,
                                Err(_) => {
                                    schedule.fail();
                                    continue;
                                }
                            };
                        let caught =
                            std::future::poll_fn(|context| {
                                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    operation.as_mut().poll(context)
                                })) {
                                    Ok(Poll::Ready(())) => Poll::Ready(true),
                                    Ok(Poll::Pending) => Poll::Pending,
                                    Err(_) => Poll::Ready(false),
                                }
                            });
                        let succeeded = tokio::select! { biased;
                            _ = cancellation.changed() => {
                                schedule.reject();
                                return;
                            },
                            _ = schedule.cancel.notified() => {
                                schedule.changed.notify_one();
                                continue;
                            },
                            succeeded = caught => succeeded,
                        };
                        if !succeeded {
                            schedule.fail();
                            continue;
                        }
                        let next_state = if schedule.period.is_some() {
                            TASK_PENDING
                        } else {
                            TASK_FINISHED
                        };
                        if schedule
                            .state
                            .compare_exchange(
                                TASK_RUNNING,
                                next_state,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            if let Some(period) = schedule.period {
                                if let Ok(mut queue) = worker_schedules.lock() {
                                    if queue.closed {
                                        schedule.reject();
                                    } else {
                                        let next_due = if schedule.fixed_rate {
                                            key.0 + period
                                        } else {
                                            Instant::now() + period
                                        };
                                        schedule.update_due(next_due);
                                        queue.jobs.insert(
                                            (next_due, key.1),
                                            ScheduledJob::User(schedule),
                                        );
                                        worker_changed.notify_one();
                                    }
                                } else {
                                    return;
                                }
                            } else {
                                schedule.changed.notify_one();
                            }
                        } else {
                            schedule.changed.notify_one();
                        }
                    }
                }
            }
        });
        Self {
            schedules,
            changed,
            status,
            active,
            cancel,
            task: Some(task),
        }
    }

    pub fn register(&mut self, schedule: Schedule) -> Result<(), RuntimeError> {
        let mut queue = self
            .schedules
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if queue.closed {
            return Err(RuntimeError::InvalidState(RuntimeState::Stopped));
        }
        queue.insert(
            Instant::now() + schedule.delay,
            ScheduledJob::Periodic(schedule),
        );
        drop(queue);
        self.changed.notify_one();
        Ok(())
    }

    pub fn register_user(&mut self, schedule: UserSchedule) -> Result<(), RuntimeError> {
        let due = schedule.due_at();
        let mut queue = self
            .schedules
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if queue.closed {
            return Err(RuntimeError::InvalidState(RuntimeState::Stopped));
        }
        queue.insert(due, ScheduledJob::User(schedule));
        drop(queue);
        self.changed.notify_one();
        Ok(())
    }

    pub fn shutdown_gracefully(&mut self) -> Result<(), RuntimeError> {
        self.status.shutdown();
        let removed = {
            let mut queue = self
                .schedules
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            queue.closed = true;
            let keys = queue
                .jobs
                .iter()
                .filter_map(|(key, job)| {
                    matches!(
                        job,
                        ScheduledJob::Periodic(_)
                            | ScheduledJob::User(UserSchedule {
                                period: Some(_),
                                ..
                            })
                    )
                    .then_some(*key)
                })
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| queue.jobs.remove(&key))
                .collect::<Vec<_>>()
        };
        for job in removed {
            if let ScheduledJob::User(schedule) = job {
                schedule.reject();
            }
        }
        self.changed.notify_one();
        self.activate();
        Ok(())
    }

    pub async fn shutdown_now(&mut self) -> Result<Vec<RuntimeScheduledTaskHandle>, RuntimeError> {
        self.status.shutdown();
        let pending = {
            let mut queue = self
                .schedules
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            queue.closed = true;
            std::mem::take(&mut queue.jobs)
        };
        let returned = pending
            .into_iter()
            .map(|((due, _), job)| match job {
                ScheduledJob::Periodic(_) => RuntimeScheduledTaskHandle::pending(due),
                ScheduledJob::User(schedule) => schedule.task_handle(),
            })
            .collect();
        self.changed.notify_one();
        self.cancel.send_replace(true);
        Ok(returned)
    }

    pub fn activate(&self) {
        self.active.send_replace(true);
    }

    pub async fn stop(&mut self) {
        self.status.shutdown();
        if let Ok(mut queue) = self.schedules.lock() {
            queue.closed = true;
        }
        self.changed.notify_one();
        self.cancel.send_replace(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        if let Ok(mut queue) = self.schedules.lock() {
            for (_, job) in std::mem::take(&mut queue.jobs) {
                if let ScheduledJob::User(schedule) = job {
                    schedule.reject();
                }
            }
        }
    }
}

impl Drop for OwnedTasks {
    fn drop(&mut self) {
        self.status.shutdown();
        if let Ok(mut queue) = self.schedules.lock() {
            queue.closed = true;
        }
        self.cancel.send_replace(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::ClientConfig,
        runtime::{ConsumerRuntime, MessageRequest, ProducerRuntime},
    };
    use rocketmq_classic_protocol::heartbeat::ConsumerData;
    use std::{
        collections::{BTreeSet, HashMap},
        pin::Pin,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    #[test]
    fn stats_schedules_keep_each_java_item_set_identity() {
        let schedules = stats_schedules();
        assert_eq!(schedules.len(), 30);

        for (set_schedules, set) in schedules.chunks_exact(6).zip(StatsItemSetKind::ALL) {
            for (schedule, period) in set_schedules[..3].iter().zip([10, 600, 3600]) {
                assert!(matches!(schedule.kind, TaskKind::StatsSample(owner) if owner == set));
                assert_eq!(schedule.delay, Duration::ZERO);
                assert_eq!(schedule.period, Duration::from_secs(period));
            }
            for (schedule, (window, period)) in set_schedules[3..].iter().zip([
                (StatsLogWindow::Minute, 60),
                (StatsLogWindow::Hour, 3600),
                (StatsLogWindow::Day, 86_400),
            ]) {
                assert!(matches!(
                    schedule.kind,
                    TaskKind::StatsPrint(owner, actual_window)
                        if owner == set && actual_window == window
                ));
                assert!(schedule.delay > Duration::ZERO);
                assert_eq!(schedule.period, Duration::from_secs(period));
            }
        }
    }

    #[derive(Default)]
    struct Consumer {
        persistence: AtomicUsize,
        adjustment: AtomicUsize,
        heartbeat: AtomicUsize,
        active: AtomicUsize,
        block: AtomicBool,
        panic: AtomicBool,
        release: tokio::sync::Notify,
    }

    struct Active<'a>(&'a AtomicUsize);
    impl Drop for Active<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl ConsumerRuntime for Consumer {
        fn heartbeat_data(&self) -> ConsumerData {
            self.heartbeat.fetch_add(1, Ordering::SeqCst);
            ConsumerData {
                group_name: Some("consumer".into()),
                ..Default::default()
            }
        }
        fn persist_consumer_offset(&self) -> Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>> {
            Some(Box::pin(async move {
                self.persistence.fetch_add(1, Ordering::SeqCst);
                assert_eq!(
                    self.active.fetch_add(1, Ordering::SeqCst),
                    0,
                    "overlapping callback"
                );
                let _active = Active(&self.active);
                assert!(
                    !self.panic.swap(false, Ordering::SeqCst),
                    "scripted callback panic"
                );
                if self.block.load(Ordering::SeqCst) {
                    self.release.notified().await;
                }
            }))
        }
        fn adjust_thread_pool(&self) -> Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>> {
            Some(Box::pin(async move {
                self.adjustment.fetch_add(1, Ordering::SeqCst);
            }))
        }
    }

    #[derive(Default)]
    struct Producer(AtomicUsize);
    impl ProducerRuntime for Producer {
        fn publish_topics(&self) -> BTreeSet<String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            BTreeSet::new()
        }
    }

    fn runtime() -> Arc<ClientRuntime> {
        let mut config = ClientConfig::new("timer");
        config.namesrv_addr = Some(String::new());
        Arc::new(ClientRuntime::construct(config, &HashMap::new(), None).unwrap())
    }

    struct DispatchRequest(Arc<AtomicUsize>);
    impl MessageRequest for DispatchRequest {
        fn consumer_group(&self) -> Option<&str> {
            Some("missing-dispatch-probe")
        }
        fn mode(&self) -> rocketmq_classic_protocol::body::MessageRequestMode {
            rocketmq_classic_protocol::body::MessageRequestMode::Pull
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }
    impl Drop for DispatchRequest {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    async fn assert_pull_dispatcher_remains_available(runtime: &Arc<ClientRuntime>) {
        runtime.start().await.unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        runtime
            .execute_pull_request_immediately(Arc::new(DispatchRequest(dropped.clone())))
            .unwrap();
        for _ in 0..100 {
            if dropped.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn public_and_periodic_adjustment_isolate_each_owner_failure() {
        struct FailingConsumer(bool);
        impl ConsumerRuntime for FailingConsumer {
            fn heartbeat_data(&self) -> ConsumerData {
                ConsumerData::default()
            }
            fn adjust_thread_pool(&self) -> Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>> {
                assert!(!self.0, "capability construction failure");
                Some(Box::pin(async {
                    tokio::task::yield_now().await;
                    panic!("capability polling failure");
                }))
            }
        }
        struct NoAdjustment;
        impl ConsumerRuntime for NoAdjustment {
            fn heartbeat_data(&self) -> ConsumerData {
                ConsumerData::default()
            }
        }
        let runtime = runtime();
        let construct_failure = Arc::new(FailingConsumer(true));
        let poll_failure = Arc::new(FailingConsumer(false));
        let unsupported = Arc::new(NoAdjustment);
        let healthy = Arc::new(Consumer::default());
        runtime
            .groups
            .register_consumer("a", construct_failure.clone())
            .unwrap();
        runtime
            .groups
            .register_consumer("b", poll_failure.clone())
            .unwrap();
        runtime
            .groups
            .register_consumer("c", unsupported.clone())
            .unwrap();
        runtime
            .groups
            .register_consumer("z", healthy.clone())
            .unwrap();
        runtime.adjust_thread_pool().await.unwrap();
        assert_eq!(healthy.adjustment.load(Ordering::SeqCst), 1);
        runtime.run_periodic(TaskKind::Adjust).await.unwrap();
        assert_eq!(healthy.adjustment.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.periodic_probe.created.load(Ordering::SeqCst), 0);
    }

    async fn settle() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }
    async fn advance(milliseconds: u64) {
        tokio::time::advance(Duration::from_millis(milliseconds)).await;
        settle().await;
    }

    #[tokio::test(start_paused = true)]
    async fn public_client_scheduler_runs_before_start_and_cancels_fixed_rate_tasks() {
        let runtime = runtime();
        let scheduler = runtime.scheduled_executor_service();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
        let one_shot = scheduler
            .schedule_after(Duration::from_secs(1), async move {
                let _ = completed_tx.send(());
            })
            .unwrap();

        settle().await;
        advance(999).await;
        assert!(!one_shot.is_finished());
        advance(1).await;
        completed_rx.await.unwrap();
        settle().await;
        assert!(one_shot.is_finished());

        let calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = calls.clone();
        let repeating = scheduler
            .schedule_at_fixed_rate(Duration::ZERO, Duration::from_secs(1), move || {
                let callback_calls = callback_calls.clone();
                async move {
                    callback_calls.fetch_add(1, Ordering::SeqCst);
                }
            })
            .unwrap();
        settle().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(repeating.cancel());
        settle().await;
        advance(5_000).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(repeating.is_finished());

        let delayed_calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = delayed_calls.clone();
        let fixed_delay = scheduler
            .schedule_with_fixed_delay(Duration::from_secs(2), Duration::from_secs(2), move || {
                let callback_calls = callback_calls.clone();
                async move {
                    callback_calls.fetch_add(1, Ordering::SeqCst);
                }
            })
            .unwrap();
        settle().await;
        advance(2_000).await;
        assert_eq!(delayed_calls.load(Ordering::SeqCst), 1);
        advance(1_999).await;
        assert_eq!(delayed_calls.load(Ordering::SeqCst), 1);
        advance(1).await;
        assert_eq!(delayed_calls.load(Ordering::SeqCst), 2);
        assert!(fixed_delay.cancel());

        assert!(matches!(
            scheduler.schedule_with_fixed_delay(Duration::ZERO, Duration::ZERO, || async {}),
            Err(RuntimeError::InvalidSchedule)
        ));

        runtime.start().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn scheduled_calls_return_values_and_report_cancellation() {
        let runtime = runtime();
        let scheduler = runtime.scheduled_executor_service();
        let mut call = scheduler
            .schedule_call_after(Duration::from_secs(2), async { 73_u64 })
            .unwrap();

        settle().await;
        assert!(
            call.join_with_timeout(Duration::from_secs(1))
                .await
                .is_err()
        );
        assert!(!call.is_finished());
        assert!(!call.is_cancelled());
        advance(999).await;
        assert!(!call.is_finished());
        advance(1).await;
        assert_eq!(call.join().await.unwrap(), 73);

        let cancelled = scheduler
            .submit(async { String::from("unreachable") })
            .unwrap();
        assert!(cancelled.cancel());
        settle().await;
        assert!(cancelled.is_finished());
        assert!(cancelled.join().await.is_err());

        runtime.start().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn scheduled_task_handles_report_delay_status_failure_and_wait_results() {
        let runtime = runtime();
        let scheduler = runtime.scheduled_executor_service();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
        let delayed = scheduler
            .schedule_after(Duration::from_secs(5), async move {
                let _ = completed_tx.send(());
            })
            .unwrap();

        assert_eq!(delayed.remaining_delay(), Duration::from_secs(5));
        assert!(!delayed.is_finished());
        assert!(!delayed.is_cancelled());
        assert_eq!(
            delayed.wait_with_timeout(Duration::from_secs(2)).await,
            Err(RuntimeScheduledTaskWaitError::Timeout)
        );
        assert!(!delayed.is_finished());
        assert_eq!(delayed.remaining_delay(), Duration::from_secs(3));
        advance(3_000).await;
        completed_rx.await.unwrap();
        delayed.wait().await.unwrap();
        assert!(delayed.is_finished());
        assert!(!delayed.is_cancelled());
        assert_eq!(delayed.remaining_delay(), Duration::ZERO);

        let cancelled = scheduler
            .schedule_after(Duration::from_secs(60), async {})
            .unwrap();
        assert!(cancelled.cancel());
        assert!(cancelled.is_finished());
        assert!(cancelled.is_cancelled());
        assert_eq!(
            cancelled.wait().await,
            Err(RuntimeScheduledTaskWaitError::Cancelled)
        );

        let failed = scheduler
            .execute(async { panic!("scripted scheduled task failure") })
            .unwrap();
        settle().await;
        assert!(failed.is_finished());
        assert!(!failed.is_cancelled());
        assert_eq!(
            failed.wait().await,
            Err(RuntimeScheduledTaskWaitError::Failed)
        );

        runtime.start().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn invoke_all_returns_results_in_submission_order() {
        let runtime = runtime();
        let scheduler = runtime.scheduled_executor_service();
        let order = Arc::new(AtomicUsize::new(0));
        let tasks = [11_u8, 23, 37].into_iter().map(|value| {
            let order = order.clone();
            async move { (value, order.fetch_add(1, Ordering::SeqCst)) }
        });

        let results = scheduler.invoke_all(tasks).await.unwrap();
        assert_eq!(
            results.into_iter().map(Result::unwrap).collect::<Vec<_>>(),
            [(11, 0), (23, 1), (37, 2)]
        );

        runtime.start().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn invoke_any_skips_failed_tasks_and_cancels_the_remainder() {
        let runtime = runtime();
        let scheduler = runtime.scheduled_executor_service();
        let third_calls = Arc::new(AtomicUsize::new(0));
        let third_task_calls = third_calls.clone();
        let tasks: Vec<Pin<Box<dyn Future<Output = usize> + Send>>> = vec![
            Box::pin(async { panic!("scripted invoke_any failure") }),
            Box::pin(async { 42 }),
            Box::pin(async move { third_task_calls.fetch_add(1, Ordering::SeqCst) }),
        ];

        assert_eq!(scheduler.invoke_any(tasks).await.unwrap(), 42);
        settle().await;
        assert_eq!(third_calls.load(Ordering::SeqCst), 0);

        let empty = std::iter::empty::<std::future::Ready<usize>>();
        assert!(matches!(
            scheduler.invoke_any(empty).await,
            Err(RuntimeError::EmptyTaskSet)
        ));

        let failures: Vec<Pin<Box<dyn Future<Output = usize> + Send>>> = vec![
            Box::pin(async { panic!("first task failed") }),
            Box::pin(async { panic!("second task failed") }),
        ];
        assert!(matches!(
            scheduler.invoke_any(failures).await,
            Err(RuntimeError::NoSuccessfulTask)
        ));

        runtime.start().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_invoke_all_cancels_running_and_queued_tasks() {
        struct DropCount(Arc<AtomicUsize>);
        impl Drop for DropCount {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let runtime = runtime();
        let scheduler = Arc::new(runtime.scheduled_executor_service());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let active_drops = Arc::new(AtomicUsize::new(0));
        let active_task_drops = active_drops.clone();
        let queued_calls = Arc::new(AtomicUsize::new(0));
        let queued_task_calls = queued_calls.clone();
        let tasks: Vec<Pin<Box<dyn Future<Output = usize> + Send>>> = vec![
            Box::pin(async move {
                let _drop_count = DropCount(active_task_drops);
                let _ = started_tx.send(());
                std::future::pending::<usize>().await
            }),
            Box::pin(async move { queued_task_calls.fetch_add(1, Ordering::SeqCst) }),
        ];
        let invocation = {
            let scheduler = scheduler.clone();
            tokio::spawn(async move { scheduler.invoke_all(tasks).await })
        };

        started_rx.await.unwrap();
        invocation.abort();
        assert!(invocation.await.unwrap_err().is_cancelled());
        settle().await;
        assert_eq!(active_drops.load(Ordering::SeqCst), 1);
        assert_eq!(queued_calls.load(Ordering::SeqCst), 0);

        runtime.start().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn timed_invoke_all_retains_completed_results_and_cancels_pending_tasks() {
        struct DropCount(Arc<AtomicUsize>);
        impl Drop for DropCount {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let runtime = runtime();
        let scheduler = Arc::new(runtime.scheduled_executor_service());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let active_drops = Arc::new(AtomicUsize::new(0));
        let active_task_drops = active_drops.clone();
        let queued_calls = Arc::new(AtomicUsize::new(0));
        let queued_task_calls = queued_calls.clone();
        let tasks: Vec<Pin<Box<dyn Future<Output = usize> + Send>>> = vec![
            Box::pin(async { 17 }),
            Box::pin(async move {
                let _drop_count = DropCount(active_task_drops);
                let _ = started_tx.send(());
                std::future::pending::<usize>().await
            }),
            Box::pin(async move { queued_task_calls.fetch_add(1, Ordering::SeqCst) }),
        ];
        let invocation = {
            let scheduler = scheduler.clone();
            tokio::spawn(async move {
                scheduler
                    .invoke_all_with_timeout(tasks, Duration::from_secs(5))
                    .await
            })
        };

        started_rx.await.unwrap();
        advance(5_000).await;
        let outcomes = invocation.await.unwrap().unwrap();
        assert!(matches!(
            outcomes[0],
            RuntimeScheduledTaskOutcome::Completed(17)
        ));
        assert!(matches!(
            outcomes[1],
            RuntimeScheduledTaskOutcome::CancelledByTimeout
        ));
        assert!(matches!(
            outcomes[2],
            RuntimeScheduledTaskOutcome::CancelledByTimeout
        ));
        assert_eq!(active_drops.load(Ordering::SeqCst), 1);
        assert_eq!(queued_calls.load(Ordering::SeqCst), 0);

        let empty = scheduler
            .invoke_all_with_timeout(
                std::iter::empty::<std::future::Ready<usize>>(),
                Duration::ZERO,
            )
            .await
            .unwrap();
        assert!(empty.is_empty());

        runtime.start().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn timed_invoke_any_reports_timeout_and_cancels_accepted_tasks() {
        struct DropCount(Arc<AtomicUsize>);
        impl Drop for DropCount {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let runtime = runtime();
        let scheduler = Arc::new(runtime.scheduled_executor_service());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let drops = Arc::new(AtomicUsize::new(0));
        let task_drops = drops.clone();
        let tasks: Vec<Pin<Box<dyn Future<Output = usize> + Send>>> = vec![Box::pin(async move {
            let _drop_count = DropCount(task_drops);
            let _ = started_tx.send(());
            std::future::pending::<usize>().await
        })];
        let invocation = {
            let scheduler = scheduler.clone();
            tokio::spawn(async move {
                scheduler
                    .invoke_any_with_timeout(tasks, Duration::from_secs(5))
                    .await
            })
        };

        started_rx.await.unwrap();
        advance(5_000).await;
        assert!(matches!(
            invocation.await.unwrap(),
            Err(RuntimeError::InvocationTimeout)
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        let empty = std::iter::empty::<std::future::Ready<usize>>();
        assert!(matches!(
            scheduler
                .invoke_any_with_timeout(empty, Duration::ZERO)
                .await,
            Err(RuntimeError::EmptyTaskSet)
        ));

        runtime.start().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn public_pull_scheduler_uses_its_own_pre_start_queue() {
        let runtime = runtime();
        let scheduler = runtime.pull_scheduled_executor_service();
        let cancelled = scheduler
            .schedule_after(Duration::from_secs(3_600), async {})
            .unwrap();
        settle().await;
        assert!(cancelled.cancel());
        settle().await;
        assert!(runtime.pull.is_empty());

        let task = scheduler
            .schedule_call_after(Duration::from_secs(2), async { 29_u32 })
            .unwrap();

        settle().await;
        advance(1_999).await;
        assert!(!task.is_finished());
        advance(1).await;
        assert_eq!(task.join().await.unwrap(), 29);
        settle().await;

        let batch = scheduler
            .invoke_all([std::future::ready(31_u32), std::future::ready(47)])
            .await
            .unwrap();
        assert_eq!(
            batch.into_iter().map(Result::unwrap).collect::<Vec<_>>(),
            [31, 47]
        );
        assert_eq!(
            scheduler
                .invoke_any([std::future::ready(53_u32)])
                .await
                .unwrap(),
            53
        );
        let timed_batch = scheduler
            .invoke_all_with_timeout([std::future::ready(59_u32)], Duration::from_secs(1))
            .await
            .unwrap();
        assert!(matches!(
            timed_batch.as_slice(),
            [RuntimeScheduledTaskOutcome::Completed(59)]
        ));
        assert_eq!(
            scheduler
                .invoke_any_with_timeout([std::future::ready(61_u32)], Duration::from_secs(1))
                .await
                .unwrap(),
            61
        );

        runtime.start().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn client_scheduler_graceful_shutdown_drains_one_shots_and_terminates() {
        let runtime = runtime();
        let scheduler = runtime.scheduled_executor_service();
        let call = scheduler
            .schedule_call_after(Duration::from_secs(2), async { 41_u8 })
            .unwrap();
        let repeating = scheduler
            .schedule_at_fixed_rate(Duration::from_secs(60), Duration::from_secs(60), || async {
            })
            .unwrap();

        scheduler.shutdown().await.unwrap();
        assert!(scheduler.is_shutdown());
        assert!(!scheduler.is_terminated());
        assert!(!scheduler.await_termination(Duration::ZERO).await);
        assert!(repeating.is_finished());
        assert!(scheduler.execute(async {}).is_err());

        advance(2_000).await;
        assert_eq!(call.join().await.unwrap(), 41);
        settle().await;
        assert!(scheduler.is_terminated());
        assert!(scheduler.await_termination(Duration::ZERO).await);
    }

    #[tokio::test(start_paused = true)]
    async fn client_scheduler_immediate_shutdown_returns_pending_queued_work() {
        let runtime = runtime();
        runtime.start().await.unwrap();
        let scheduler = runtime.scheduled_executor_service();
        let first = scheduler
            .schedule_call_after(Duration::from_secs(60), async { 1_u8 })
            .unwrap();
        let second = scheduler
            .schedule_call_after(Duration::from_secs(60), async { 2_u8 })
            .unwrap();
        let first_handle = first.task_handle();
        let second_handle = second.task_handle();

        let returned = scheduler.shutdown_now().await.unwrap();
        assert_eq!(
            returned.len(),
            stats_schedules().len() + schedules(&runtime.config_snapshot()).unwrap().len() + 2
        );
        assert!(returned.contains(&first_handle));
        assert!(returned.contains(&second_handle));
        assert!(scheduler.is_shutdown());
        settle().await;
        assert!(scheduler.is_terminated());
        assert!(!first.is_finished());
        assert!(!first.is_cancelled());
        assert!(!second.is_finished());
        assert!(!second.is_cancelled());
        for task in &returned {
            assert!(!task.is_finished());
            assert!(task.cancel());
        }
        assert!(first.is_cancelled());
        assert!(second.is_cancelled());
        assert!(first.join().await.is_err());
        assert!(second.join().await.is_err());
        assert!(scheduler.execute(async {}).is_err());
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn client_scheduler_exists_with_stats_jobs_before_runtime_start() {
        let runtime = runtime();
        let scheduler = runtime.scheduled_executor_service();
        let first = scheduler
            .schedule_call_after(Duration::from_secs(60), async { 1_u8 })
            .unwrap();
        let second = scheduler
            .schedule_call_after(Duration::from_secs(60), async { 2_u8 })
            .unwrap();
        let first_handle = first.task_handle();
        let second_handle = second.task_handle();

        let returned = scheduler.shutdown_now().await.unwrap();
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/scheduled-executor-shutdown.json"
        ))
        .unwrap();
        assert_eq!(
            returned.len(),
            fixture["cases"][0]["queuedTaskCount"].as_u64().unwrap() as usize
        );
        assert!(returned.contains(&first_handle));
        assert!(returned.contains(&second_handle));
        assert!(scheduler.is_shutdown());
        for task in &returned {
            assert!(!task.is_finished());
            assert!(!task.is_cancelled());
        }
        assert!(!first.is_finished());
        assert!(!first.is_cancelled());
        assert!(!second.is_finished());
        assert!(!second.is_cancelled());
        for task in &returned {
            assert!(task.cancel());
        }
        assert!(first.is_cancelled());
        assert!(second.is_cancelled());
        assert!(scheduler.await_termination(Duration::ZERO).await);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn consumer_stats_manager_constructor_registers_java_jobs_with_scheduler() {
        let runtime = runtime();
        let scheduler = runtime.scheduled_executor_service();
        let manager = crate::runtime::ConsumerStatsManager::with_scheduler(&scheduler).unwrap();

        let returned = scheduler.shutdown_now().await.unwrap();
        assert_eq!(stats_schedules().len(), 30);
        assert_eq!(returned.len(), stats_schedules().len() * 2);
        assert_eq!(
            manager.consume_status("workers", "orders").get_pull_tps(),
            0.0
        );

        for task in &returned {
            assert!(task.cancel());
        }
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn pull_scheduler_graceful_shutdown_drains_one_shots_and_terminates() {
        let runtime = runtime();
        let scheduler = runtime.pull_scheduled_executor_service();
        let call = scheduler
            .schedule_call_after(Duration::from_secs(2), async { 17_u8 })
            .unwrap();
        let repeating = scheduler
            .schedule_with_fixed_delay(Duration::from_secs(60), Duration::from_secs(60), || async {
            })
            .unwrap();

        scheduler.shutdown().await.unwrap();
        assert!(scheduler.is_shutdown());
        assert!(!scheduler.is_terminated());
        assert!(repeating.is_finished());
        assert!(scheduler.execute(async {}).is_err());

        advance(2_000).await;
        assert_eq!(call.join().await.unwrap(), 17);
        settle().await;
        assert!(scheduler.is_terminated());
        assert!(scheduler.await_termination(Duration::ZERO).await);
        assert_pull_dispatcher_remains_available(&runtime).await;
    }

    #[tokio::test(start_paused = true)]
    async fn pull_scheduler_immediate_shutdown_returns_pending_queued_work() {
        let runtime = runtime();
        let scheduler = runtime.pull_scheduled_executor_service();
        let callbacks = Arc::new(AtomicUsize::new(0));
        let callback_count = callbacks.clone();
        runtime
            .execute_task_later(
                async move {
                    callback_count.fetch_add(1, Ordering::SeqCst);
                },
                60_000,
            )
            .unwrap();
        let request_drops = Arc::new(AtomicUsize::new(0));
        runtime
            .execute_pull_request_later(Arc::new(DispatchRequest(request_drops.clone())), 60_000)
            .unwrap();
        let first = scheduler
            .schedule_call_after(Duration::from_secs(60), async { 1_u8 })
            .unwrap();
        let second = scheduler
            .schedule_call_after(Duration::from_secs(60), async { 2_u8 })
            .unwrap();
        let first_handle = first.task_handle();
        let second_handle = second.task_handle();

        let returned = scheduler.shutdown_now().await.unwrap();
        assert_eq!(returned.len(), 4);
        assert!(returned.contains(&first_handle));
        assert!(returned.contains(&second_handle));
        assert!(scheduler.is_shutdown());
        settle().await;
        assert!(scheduler.is_terminated());
        assert!(!first.is_finished());
        assert!(!first.is_cancelled());
        assert!(!second.is_finished());
        assert!(!second.is_cancelled());
        for task in &returned {
            assert!(!task.is_finished());
            assert!(task.cancel());
        }
        assert!(first.is_cancelled());
        assert!(second.is_cancelled());
        assert_eq!(callbacks.load(Ordering::SeqCst), 0);
        assert_eq!(request_drops.load(Ordering::SeqCst), 1);
        assert!(first.join().await.is_err());
        assert!(second.join().await.is_err());
        assert!(scheduler.execute(async {}).is_err());
        assert_pull_dispatcher_remains_available(&runtime).await;
    }

    #[tokio::test]
    async fn client_scheduler_worker_can_request_immediate_shutdown() {
        let runtime = runtime();
        let scheduler = runtime.scheduled_executor_service();
        let callback_runtime = runtime.clone();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        scheduler
            .execute(async move {
                let _ = entered_tx.send(());
                callback_runtime
                    .scheduled_executor_service()
                    .shutdown_now()
                    .await
                    .unwrap();
            })
            .unwrap();

        entered_rx.await.unwrap();
        assert!(scheduler.await_termination(Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn pull_scheduler_worker_can_request_immediate_shutdown() {
        let runtime = runtime();
        let scheduler = runtime.pull_scheduled_executor_service();
        let callback_runtime = runtime.clone();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        scheduler
            .execute(async move {
                let _ = entered_tx.send(());
                callback_runtime
                    .pull_scheduled_executor_service()
                    .shutdown_now()
                    .await
                    .unwrap();
            })
            .unwrap();

        entered_rx.await.unwrap();
        assert!(scheduler.await_termination(Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn runtime_shutdown_terminates_both_owned_schedulers() {
        let runtime = runtime();
        let client_scheduler = runtime.scheduled_executor_service();
        let pull_scheduler = runtime.pull_scheduled_executor_service();
        runtime.start().await.unwrap();
        assert!(!client_scheduler.is_shutdown());
        assert!(!pull_scheduler.is_shutdown());

        runtime.shutdown().await.unwrap();

        for scheduler in [&client_scheduler, &pull_scheduler] {
            assert!(scheduler.is_shutdown());
            assert!(scheduler.is_terminated());
            assert!(scheduler.await_termination(Duration::ZERO).await);
        }
    }

    fn tasks(runtime: &Arc<ClientRuntime>) -> OwnedTasks {
        let mut tasks = OwnedTasks::new(
            Arc::downgrade(runtime),
            runtime.task_scheduler_status.clone(),
        );
        for schedule in schedules(&runtime.config_snapshot()).unwrap() {
            tasks.register(schedule).unwrap();
        }
        tasks.activate();
        tasks
    }

    #[tokio::test(start_paused = true)]
    async fn defaults_have_java_initial_delays_periods_and_no_ticks_after_stop() {
        let runtime = runtime();
        let producer = Arc::new(Producer::default());
        let consumer = Arc::new(Consumer::default());
        runtime
            .groups
            .register_producer("producer", producer.clone())
            .unwrap();
        runtime
            .groups
            .register_consumer("consumer", consumer.clone())
            .unwrap();
        let mut tasks = tasks(&runtime);
        settle().await;
        advance(9).await;
        assert_eq!(producer.0.load(Ordering::SeqCst), 0);
        advance(1).await;
        assert_eq!(producer.0.load(Ordering::SeqCst), 1);
        assert_eq!(consumer.heartbeat.load(Ordering::SeqCst), 1); // route topic collection
        advance(989).await;
        assert_eq!(consumer.heartbeat.load(Ordering::SeqCst), 1);
        advance(1).await;
        assert_eq!(consumer.heartbeat.load(Ordering::SeqCst), 2);
        advance(8_999).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 0);
        advance(1).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 1);
        advance(5_000).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 2);
        advance(45_000).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 11);
        assert_eq!(consumer.adjustment.load(Ordering::SeqCst), 1);
        assert_eq!(producer.0.load(Ordering::SeqCst), 2);
        assert_eq!(consumer.heartbeat.load(Ordering::SeqCst), 4);
        tasks.stop().await;
        assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 0);
        advance(120_000).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 11);
        assert_eq!(producer.0.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn offset_persistence_skips_missing_capabilities_and_continues_ticking() {
        struct NoPersistence(AtomicUsize);
        impl ConsumerRuntime for NoPersistence {
            fn heartbeat_data(&self) -> ConsumerData {
                ConsumerData::default()
            }
            fn persist_consumer_offset(
                &self,
            ) -> Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>> {
                self.0.fetch_add(1, Ordering::SeqCst);
                None
            }
        }

        let runtime = runtime();
        let no_capability = Arc::new(NoPersistence(AtomicUsize::new(0)));
        let capable = Arc::new(Consumer::default());
        runtime
            .groups
            .register_consumer("a-no-persistence", no_capability.clone())
            .unwrap();
        runtime
            .groups
            .register_consumer("z-persistence", capable.clone())
            .unwrap();
        let mut tasks = tasks(&runtime);
        settle().await;

        advance(10_000).await;
        assert_eq!(no_capability.0.load(Ordering::SeqCst), 1);
        assert_eq!(capable.persistence.load(Ordering::SeqCst), 1);

        advance(5_000).await;
        assert_eq!(no_capability.0.load(Ordering::SeqCst), 2);
        assert_eq!(capable.persistence.load(Ordering::SeqCst), 2);
        tasks.stop().await;
    }

    #[test]
    fn scheduled_task_delays_and_periods_match_invoked_java_registration() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/lookup.json")).unwrap();
        for case in fixture["runtimeScheduledTaskCases"].as_array().unwrap() {
            let mut config = ClientConfig::new("scheduled-task-oracle");
            let mutated = match case["name"].as_str().unwrap() {
                "nameserver-absent" => {
                    config.namesrv_addr = None;
                    false
                }
                "nameserver-configured" => {
                    config.namesrv_addr = Some("127.0.0.1:9876".into());
                    false
                }
                "nameserver-absent-mutated-configured" => {
                    config.namesrv_addr = Some("late-after-construction:9876".into());
                    true
                }
                "nameserver-configured-mutated-absent" => {
                    config.namesrv_addr = None;
                    true
                }
                other => panic!("unexpected Java scheduler case {other}"),
            };
            config.poll_name_server_interval = if mutated { 71 } else { 23 };
            config.heartbeat_broker_interval = if mutated { 83 } else { 47 };
            config.persist_consumer_offset_interval = if mutated { 97 } else { 59 };

            let actual: Vec<_> = schedules(&config)
                .unwrap()
                .iter()
                .map(|schedule| {
                    serde_json::json!({
                        "initialDelayMillis": schedule.delay.as_millis(),
                        "periodMillis": schedule.period.as_millis(),
                    })
                })
                .collect();
            let expected: Vec<_> = case["scheduleAtFixedRateCalls"]
                .as_array()
                .unwrap()
                .iter()
                .map(|call| {
                    let multiplier = match call["unit"].as_str().unwrap() {
                        "MILLISECONDS" => 1,
                        "MINUTES" => 60_000,
                        unit => panic!("unexpected Java scheduler unit {unit}"),
                    };
                    serde_json::json!({
                        "initialDelayMillis": call["initialDelay"].as_u64().unwrap() * multiplier,
                        "periodMillis": call["period"].as_u64().unwrap() * multiplier,
                    })
                })
                .collect();
            assert_eq!(actual, expected, "{}", case["name"]);
        }
    }

    #[test]
    fn empty_nameserver_start_schedule_count_matches_java_probe() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/start.json")).unwrap();
        let java_case = &fixture["configuredToEmpty"];
        let mut config = ClientConfig::new("empty-nameserver-start");
        config.namesrv_addr = Some(String::new());

        assert_eq!(java_case["discoveryCalls"], 0);
        assert_eq!(
            schedules(&config).unwrap().len(),
            java_case["scheduledTaskCount"].as_u64().unwrap() as usize
        );
    }

    #[tokio::test(start_paused = true)]
    async fn slow_callback_serializes_jobs_then_catches_up_from_prior_deadlines() {
        let runtime = runtime();
        let consumer = Arc::new(Consumer::default());
        consumer.block.store(true, Ordering::SeqCst);
        runtime
            .groups
            .register_consumer("consumer", consumer.clone())
            .unwrap();
        let mut tasks = tasks(&runtime);
        settle().await;
        advance(10_000).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 1);
        advance(50_000).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 1);
        assert_eq!(consumer.active.load(Ordering::SeqCst), 1);
        assert_eq!(consumer.adjustment.load(Ordering::SeqCst), 0);
        consumer.block.store(false, Ordering::SeqCst);
        consumer.release.notify_one();
        settle().await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 11);
        assert_eq!(consumer.adjustment.load(Ordering::SeqCst), 1);
        assert_eq!(consumer.active.load(Ordering::SeqCst), 0);
        tasks.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_cancels_and_joins_pending_callback() {
        let runtime = runtime();
        let consumer = Arc::new(Consumer::default());
        consumer.block.store(true, Ordering::SeqCst);
        runtime
            .groups
            .register_consumer("consumer", consumer.clone())
            .unwrap();
        let mut tasks = tasks(&runtime);
        settle().await;
        advance(10_000).await;
        assert_eq!(consumer.active.load(Ordering::SeqCst), 1);
        tasks.stop().await;
        assert_eq!(consumer.active.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 0);
        advance(120_000).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_rebalance_wakeup_waits_behind_the_single_scheduled_callback() {
        let runtime = runtime();
        let consumer = Arc::new(Consumer::default());
        consumer.block.store(true, Ordering::SeqCst);
        runtime
            .groups
            .register_consumer("consumer", consumer.clone())
            .unwrap();
        let mut tasks = tasks(&runtime);
        settle().await;
        advance(10_000).await;
        assert_eq!(consumer.active.load(Ordering::SeqCst), 1);
        runtime.rebalance_later(1_000).unwrap();
        advance(1_000).await;
        assert!(runtime.rebalance.next_due().is_some());
        consumer.block.store(false, Ordering::SeqCst);
        consumer.release.notify_one();
        settle().await;
        assert!(runtime.rebalance.next_due().is_none());
        tasks.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn panicking_callback_does_not_kill_later_ticks() {
        let runtime = runtime();
        let consumer = Arc::new(Consumer::default());
        consumer.panic.store(true, Ordering::SeqCst);
        runtime
            .groups
            .register_consumer("consumer", consumer.clone())
            .unwrap();
        let mut tasks = tasks(&runtime);
        settle().await;
        advance(10_000).await;
        advance(50_000).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 11);
        assert_eq!(consumer.adjustment.load(Ordering::SeqCst), 1);
        tasks.stop().await;
    }

    #[test]
    fn nonpositive_period_is_rejected_before_task_creation() {
        for period in [0, -1] {
            let mut config = ClientConfig::new("timer");
            config.poll_name_server_interval = period;
            assert!(matches!(
                schedules(&config),
                Err(RuntimeError::InvalidSchedule)
            ));
        }
    }
}
