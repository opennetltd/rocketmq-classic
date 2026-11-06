use super::tasks::{RuntimeScheduledTaskHandle, UserSchedule};
use super::{ClientRuntime, RuntimeError, RuntimeState};
use rocketmq_classic_protocol::body::MessageRequestMode;
use std::{
    any::Any,
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
};
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
    time::{Duration, Instant},
};

/// Routing-only contract; concrete PullRequest/PopRequest state belongs to
/// consumer phases, not a second Phase 6 consumer scheduler.
pub trait MessageRequest: Any + Send + Sync {
    /// Java's `MessageRequest.getConsumerGroup()` may return null.
    fn consumer_group(&self) -> Option<&str>;
    fn mode(&self) -> MessageRequestMode;
    fn as_any(&self) -> &dyn Any;
}
type Task = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
struct ScheduledTaskGuard(Option<UserSchedule>);
impl Drop for ScheduledTaskGuard {
    fn drop(&mut self) {
        if let Some(task) = self.0.as_ref() {
            task.reject();
        }
    }
}
struct PullSchedulerTerminationGuard(Arc<PullService>);
impl Drop for PullSchedulerTerminationGuard {
    fn drop(&mut self) {
        if self.0.is_scheduler_shutdown() {
            self.0.mark_scheduler_terminated();
        }
    }
}
enum Job {
    Request(Arc<dyn MessageRequest>),
    Task(Task),
    ScheduledTask(UserSchedule),
}
#[derive(Default)]
struct Queues {
    stopped: bool,
    scheduler_closed: bool,
    immediate: VecDeque<Arc<dyn MessageRequest>>,
    scheduled: BTreeMap<(Instant, u64), Job>,
    sequence: u64,
}
pub(super) struct PullService {
    pub thread: super::service_thread::ServiceThreadControl,
    queues: Mutex<Queues>,
    immediate: Notify,
    pub(super) interrupt_wait: Notify,
    scheduled: Arc<Notify>,
    scheduler_terminated: AtomicBool,
    scheduler_terminated_watch: watch::Sender<bool>,
}

impl Default for PullService {
    fn default() -> Self {
        let (scheduler_terminated_watch, _) = watch::channel(false);
        Self {
            thread: super::service_thread::ServiceThreadControl::default(),
            queues: Mutex::new(Queues::default()),
            immediate: Notify::new(),
            interrupt_wait: Notify::new(),
            scheduled: Arc::default(),
            scheduler_terminated: AtomicBool::new(false),
            scheduler_terminated_watch,
        }
    }
}

impl PullService {
    pub fn open_dispatch(&self) {
        if let Ok(mut queues) = self.queues.lock() {
            queues.stopped = false;
        }
    }

    pub fn scheduled_notifier(&self) -> Arc<Notify> {
        self.scheduled.clone()
    }

    pub fn is_scheduler_shutdown(&self) -> bool {
        self.queues
            .lock()
            .map_or(true, |queues| queues.scheduler_closed)
    }

    pub fn is_scheduler_terminated(&self) -> bool {
        self.scheduler_terminated.load(Ordering::Acquire)
    }

    pub async fn await_scheduler_termination(&self, timeout: Duration) -> bool {
        let mut terminated = self.scheduler_terminated_watch.subscribe();
        if self.is_scheduler_terminated() {
            return true;
        }
        tokio::time::timeout(timeout, async {
            loop {
                if terminated.changed().await.is_err() {
                    return;
                }
                if self.is_scheduler_terminated() {
                    return;
                }
            }
        })
        .await
        .is_ok()
    }

    pub fn mark_scheduler_terminated(&self) {
        self.scheduler_terminated.store(true, Ordering::Release);
        self.scheduler_terminated_watch.send_replace(true);
    }

    pub fn shutdown_scheduler(
        &self,
        immediate: bool,
    ) -> Result<Vec<RuntimeScheduledTaskHandle>, RuntimeError> {
        let (jobs, returned) = {
            let mut queues = self
                .queues
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            queues.scheduler_closed = true;
            if immediate {
                let jobs = std::mem::take(&mut queues.scheduled);
                let returned = jobs
                    .iter()
                    .map(|((due, _), job)| match job {
                        Job::ScheduledTask(task) => task.task_handle(),
                        Job::Request(_) | Job::Task(_) => RuntimeScheduledTaskHandle::pending(*due),
                    })
                    .collect();
                (jobs.into_values().collect::<Vec<_>>(), returned)
            } else {
                let keys = queues
                    .scheduled
                    .iter()
                    .filter_map(|(key, job)| {
                        matches!(job, Job::ScheduledTask(task) if task.period.is_some())
                            .then_some(*key)
                    })
                    .collect::<Vec<_>>();
                let jobs = keys
                    .into_iter()
                    .filter_map(|key| queues.scheduled.remove(&key))
                    .collect::<Vec<_>>();
                (jobs, Vec::new())
            }
        };
        if !immediate {
            for job in jobs {
                if let Job::ScheduledTask(task) = job {
                    task.reject();
                }
            }
        }
        self.scheduled.notify_one();
        Ok(returned)
    }

    fn discard_cancelled_user_tasks(&self) {
        let cancelled = self.queues.lock().ok().map(|mut queues| {
            let keys = queues
                .scheduled
                .iter()
                .filter_map(|(key, job)| {
                    matches!(job, Job::ScheduledTask(task) if task.is_cancelled()).then_some(*key)
                })
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| queues.scheduled.remove(&key))
                .collect::<Vec<_>>()
        });
        if let Some(cancelled) = cancelled {
            for job in cancelled {
                if let Job::ScheduledTask(task) = job {
                    task.reject();
                }
            }
        }
    }

    pub fn enqueue(&self, request: Arc<dyn MessageRequest>) -> Result<(), RuntimeError> {
        let mut queues = self
            .queues
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if queues.stopped {
            return Err(RuntimeError::InvalidState(RuntimeState::Stopped));
        }
        queues.immediate.push_back(request);
        self.immediate.notify_one();
        Ok(())
    }
    fn schedule(&self, job: Job, delay: i64) -> Result<(), RuntimeError> {
        let mut queues = self
            .queues
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if queues.stopped || queues.scheduler_closed {
            return Err(RuntimeError::InvalidState(RuntimeState::Stopped));
        }
        let key = (
            Instant::now() + Duration::from_millis(delay.max(0) as u64),
            queues.sequence,
        );
        queues.sequence = queues.sequence.wrapping_add(1);
        queues.scheduled.insert(key, job);
        self.scheduled.notify_one();
        Ok(())
    }
    pub fn request_later(
        &self,
        request: Arc<dyn MessageRequest>,
        delay: i64,
    ) -> Result<(), RuntimeError> {
        self.schedule(Job::Request(request), delay)
    }
    pub fn task_later(&self, task: Task, delay: i64) -> Result<(), RuntimeError> {
        self.schedule(Job::Task(task), delay)
    }
    pub fn schedule_user_task(&self, task: UserSchedule) -> Result<(), RuntimeError> {
        let mut queues = self
            .queues
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if queues.stopped || queues.scheduler_closed {
            return Err(RuntimeError::InvalidState(RuntimeState::Stopped));
        }
        let due = task.due_at();
        let sequence = queues.sequence;
        queues.sequence = queues.sequence.wrapping_add(1);
        queues
            .scheduled
            .insert((due, sequence), Job::ScheduledTask(task));
        self.scheduled.notify_one();
        Ok(())
    }
    pub fn close(&self) {
        // User destructors/futures must drop outside the queue mutex.
        let mut old = self.queues.lock().ok().map(|mut queues| {
            std::mem::replace(
                &mut *queues,
                Queues {
                    stopped: true,
                    scheduler_closed: true,
                    ..Queues::default()
                },
            )
        });
        if let Some(queues) = old.as_mut() {
            for (_, job) in std::mem::take(&mut queues.scheduled) {
                if let Job::ScheduledTask(task) = job {
                    task.reject();
                }
            }
        }
        drop(old);
    }
    pub fn stop_accepting(&self) {
        let pending = self.queues.lock().ok().map(|mut queues| {
            queues.stopped = true;
            queues.scheduler_closed = true;
            let pending = std::mem::take(&mut queues.immediate);
            let repeating_keys = queues
                .scheduled
                .iter()
                .filter_map(|(key, job)| {
                    matches!(job, Job::ScheduledTask(task) if task.period.is_some()).then_some(*key)
                })
                .collect::<Vec<_>>();
            let repeating = repeating_keys
                .into_iter()
                .filter_map(|key| queues.scheduled.remove(&key))
                .collect::<Vec<_>>();
            (pending, repeating)
        });
        if let Some((pending, repeating)) = pending {
            drop(pending);
            for job in repeating {
                if let Job::ScheduledTask(task) = job {
                    task.reject();
                }
            }
        }
        self.scheduled.notify_one();
    }
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        let queues = self.queues.lock().unwrap();
        queues.immediate.is_empty() && queues.scheduled.is_empty()
    }
}
pub(super) struct OwnedPullWorker {
    scheduled: bool,
    active: watch::Sender<bool>,
    cancel: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}
impl OwnedPullWorker {
    pub fn new(runtime: Weak<ClientRuntime>, service: Arc<PullService>, scheduled: bool) -> Self {
        #[cfg(test)]
        let live = runtime.upgrade().map(|runtime| {
            let probe = runtime.pull_probe.clone();
            probe
                .created
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            probe.live.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            super::tasks::LiveTask(probe)
        });
        let (active, mut activation) = watch::channel(false);
        let (cancel, mut cancellation) = watch::channel(false);
        let task = tokio::spawn(async move {
            #[cfg(test)]
            let _live = live;
            let _termination = scheduled.then(|| PullSchedulerTerminationGuard(service.clone()));
            tokio::select! { biased; _ = cancellation.changed() => return, _ = activation.changed() => {}, }
            let mut pull_take_woken = false;
            loop {
                if !scheduled && !pull_take_woken && service.thread.is_stopped() {
                    return;
                }
                pull_take_woken = false;
                service.discard_cancelled_user_tasks();
                if scheduled
                    && service.queues.lock().ok().is_some_and(|queues| {
                        queues.scheduler_closed && queues.scheduled.is_empty()
                    })
                {
                    return;
                }
                let work = if scheduled {
                    let due = service.queues.lock().ok().and_then(|queues| {
                        queues.scheduled.first_key_value().map(|(key, _)| key.0)
                    });
                    tokio::select! { biased;
                        _ = cancellation.changed() => return,
                        _ = service.scheduled.notified() => continue,
                        _ = async { if let Some(due) = due { tokio::time::sleep_until(due).await; } else { std::future::pending::<()>().await; } } => {},
                    }
                    service.queues.lock().ok().and_then(|mut queues| {
                        if queues
                            .scheduled
                            .first_key_value()
                            .is_some_and(|(key, _)| key.0 <= Instant::now())
                        {
                            queues
                                .scheduled
                                .pop_first()
                                .map(|(key, job)| (Some(key), job))
                        } else {
                            None
                        }
                    })
                } else {
                    let request = service
                        .queues
                        .lock()
                        .ok()
                        .and_then(|mut queues| queues.immediate.pop_front());
                    if request.is_none() {
                        tokio::select! { biased;
                            _ = cancellation.changed() => return,
                            _ = service.interrupt_wait.notified() => {
                                if service.thread.is_stopped() {
                                    return;
                                }
                            },
                            _ = service.immediate.notified() => {
                                // A queue wake that races `makeStop()` is like
                                // Java's already-blocked `take()`: process the
                                // one item it yields, then observe the stop flag.
                                pull_take_woken = true;
                                continue;
                            },
                        }
                    }
                    request.map(|request| (None, Job::Request(request)))
                };
                let Some((key, work)) = work else {
                    continue;
                };
                let operation = async {
                    match work {
                        Job::Request(request) if scheduled => {
                            let _ = service.enqueue(request);
                        }
                        Job::ScheduledTask(task) => {
                            let mut guard = ScheduledTaskGuard(Some(task));
                            let task = guard.0.as_ref().expect("scheduled task guard is active");
                            if !task.try_start() {
                                return;
                            }
                            let mut operation =
                                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    (task.factory)()
                                })) {
                                    Ok(operation) => operation,
                                    Err(_) => {
                                        task.fail();
                                        return;
                                    }
                                };
                            let caught = std::future::poll_fn(|context| {
                                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    operation.as_mut().poll(context)
                                })) {
                                    Ok(std::task::Poll::Ready(())) => std::task::Poll::Ready(true),
                                    Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
                                    Err(_) => std::task::Poll::Ready(false),
                                }
                            });
                            let succeeded = tokio::select! { biased;
                                _ = task.cancel.notified() => {
                                    task.changed.notify_one();
                                    return;
                                },
                                succeeded = caught => succeeded,
                            };
                            let task = guard.0.take().expect("scheduled task guard is active");
                            if !succeeded {
                                task.fail();
                                return;
                            }
                            if task.complete() {
                                if let Some(period) = task.period {
                                    let due = if task.fixed_rate {
                                        key.expect("scheduled task has a deadline").0 + period
                                    } else {
                                        Instant::now() + period
                                    };
                                    task.update_due(due);
                                    let task = task;
                                    match service.queues.lock() {
                                        Ok(mut queues) if !queues.scheduler_closed => {
                                            queues.scheduled.insert(
                                                (
                                                    due,
                                                    key.expect("scheduled task has a deadline").1,
                                                ),
                                                Job::ScheduledTask(task),
                                            );
                                            service.scheduled.notify_one();
                                        }
                                        _ => task.reject(),
                                    }
                                } else {
                                    task.changed.notify_one();
                                }
                            } else {
                                task.changed.notify_one();
                            }
                        }
                        Job::Request(request) => {
                            let Some(runtime) = runtime.upgrade() else {
                                return;
                            };
                            if let Ok(Some(consumer)) =
                                runtime.select_consumer_nullable(request.consumer_group())
                            {
                                let operation = match request.mode() {
                                    MessageRequestMode::Pull => consumer.pull_message(request),
                                    MessageRequestMode::Pop => consumer.pop_message(request),
                                };
                                if let Some(operation) = operation {
                                    operation.await;
                                }
                            }
                        }
                        Job::Task(task) => task.await,
                    }
                };
                let mut operation = Box::pin(operation);
                let caught = std::future::poll_fn(|context| {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        operation.as_mut().poll(context)
                    }))
                    .unwrap_or(Poll::Ready(()))
                });
                tokio::select! { biased; _ = cancellation.changed() => return, _ = caught => {}, }
                if !scheduled && service.thread.is_stopped() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        });
        Self {
            scheduled,
            active,
            cancel,
            task: Some(task),
        }
    }
    pub fn activate(&self) {
        self.active.send_replace(true);
    }
    pub fn request_stop(&self) {
        self.cancel.send_replace(true);
    }
    pub fn is_scheduled(&self) -> bool {
        self.scheduled
    }
    pub async fn join_with_timeout(&mut self, timeout: Duration) -> bool {
        let Some(task) = self.task.as_mut() else {
            return true;
        };
        if tokio::time::timeout(timeout, task).await.is_ok() {
            self.task.take();
            true
        } else {
            false
        }
    }
    pub async fn stop(&mut self) {
        self.request_stop();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
    pub async fn finish_gracefully(&mut self) {
        // PullMessageService's executor gets a 1000ms graceful window. Rust
        // cancellation then drops/joins pending work, rather than detaching it.
        if let Some(task) = self.task.as_mut() {
            if tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .is_ok()
            {
                self.task.take();
                return;
            }
        }
        self.stop().await;
    }
}
impl Drop for OwnedPullWorker {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Request(Arc<AtomicUsize>);
    impl MessageRequest for Request {
        fn consumer_group(&self) -> Option<&str> {
            Some("unactivated")
        }
        fn mode(&self) -> MessageRequestMode {
            MessageRequestMode::Pull
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }
    impl Drop for Request {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Reenter(Weak<PullService>, Arc<AtomicUsize>);
    impl Drop for Reenter {
        fn drop(&mut self) {
            let service = self.0.upgrade().unwrap();
            assert!(
                service.queues.try_lock().is_ok(),
                "user destructor ran under queue lock"
            );
            self.1.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn close_drops_queue_and_scheduled_owners_outside_mutex_and_rejects_late_work() {
        let service = Arc::new(PullService::default());
        let dropped = Arc::new(AtomicUsize::new(0));
        service.enqueue(Arc::new(Request(dropped.clone()))).unwrap();
        let guard = Reenter(Arc::downgrade(&service), dropped.clone());
        service
            .task_later(
                Box::pin(async move {
                    let _guard = guard;
                }),
                1_000,
            )
            .unwrap();
        service.close();
        assert!(service.is_empty());
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
        assert!(service.enqueue(Arc::new(Request(dropped.clone()))).is_err());
        assert!(service.task_later(Box::pin(async {}), 0).is_err());
    }

    #[tokio::test]
    async fn missing_capability_drops_request_without_claiming_consumer_execution() {
        struct Consumer;
        impl super::super::ConsumerRuntime for Consumer {
            fn heartbeat_data(&self) -> rocketmq_classic_protocol::heartbeat::ConsumerData {
                Default::default()
            }
        }
        let mut config = crate::config::ClientConfig::new("unactivated-pull");
        config.namesrv_addr = Some(String::new());
        let runtime = super::super::ClientRuntimeManager::new()
            .get_or_create(&config)
            .await
            .unwrap();
        let owner = Arc::new(Consumer);
        let lease = runtime
            .register_consumer("unactivated", owner.clone())
            .await
            .unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        runtime
            .execute_pull_request_immediately(Arc::new(Request(dropped.clone())))
            .unwrap();
        runtime.start().await.unwrap();
        for _ in 0..30 {
            tokio::task::yield_now().await;
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        runtime
            .unregister_consumer(&lease, Instant::now())
            .await
            .unwrap();
        runtime.shutdown().await.unwrap();
        assert_eq!(runtime.pull_probe.live.load(Ordering::SeqCst), 0);
    }
}
