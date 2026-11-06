use super::{
    ClientRuntime, ConsumerRuntime, GroupLease, ProducerRuntime, RuntimeError, registry::GroupKind,
};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicU8, Ordering},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{Duration, Instant},
};

pub(super) const INNER_PRODUCER: &str = "CLIENT_INNER_PRODUCER";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RuntimeState {
    Created,
    Starting,
    Running,
    ShuttingDown,
    Stopped,
    Failed,
}

// One admission guard remains held until both caller post-processing and the
// owned command/abandoned-registration cleanup have finished.
pub(super) type MonitorGuard = Arc<tokio::sync::OwnedMutexGuard<()>>;

pub(super) enum Command {
    Monitored {
        command: Box<Command>,
        monitor: MonitorGuard,
    },
    MutateTable {
        kind: super::tables::TableKind,
        mutation: super::tables::TableMutation,
        reply: oneshot::Sender<Result<super::tables::TableChange, RuntimeError>>,
    },
    RegisterProducer {
        group: String,
        handle: Arc<dyn ProducerRuntime>,
        gate: tokio::sync::OwnedMutexGuard<()>,
        reply: oneshot::Sender<Result<PendingLease, RuntimeError>>,
    },
    RegisterConsumer {
        group: String,
        handle: Arc<dyn ConsumerRuntime>,
        gate: tokio::sync::OwnedMutexGuard<()>,
        reply: oneshot::Sender<Result<PendingLease, RuntimeError>>,
    },
    RegisterAdmin {
        group: String,
        reply: oneshot::Sender<Result<PendingLease, RuntimeError>>,
    },
    UnregisterByGroup {
        group: String,
        kind: GroupKind,
        deadline: Instant,
        gate: Option<tokio::sync::OwnedMutexGuard<()>>,
        reply: oneshot::Sender<Result<bool, RuntimeError>>,
    },
    Unregister {
        lease: GroupLease,
        kind: GroupKind,
        deadline: Instant,
        gate: Option<tokio::sync::OwnedMutexGuard<()>>,
        reply: Option<oneshot::Sender<Result<bool, RuntimeError>>>,
    },
    Start {
        reply: oneshot::Sender<Result<(), RuntimeError>>,
    },
    StartServiceThread {
        kind: super::service_thread::RuntimeServiceKind,
        reply: oneshot::Sender<Result<(), RuntimeError>>,
    },
    ShutdownServiceThread {
        kind: super::service_thread::RuntimeServiceKind,
        interrupt: bool,
        reply: oneshot::Sender<Result<(), RuntimeError>>,
    },
    StartPullScheduler,
    ScheduleRuntimeTask(super::tasks::UserSchedule),
    ShutdownRuntimeScheduler {
        immediate: bool,
        reply: oneshot::Sender<Result<Vec<super::tasks::RuntimeScheduledTaskHandle>, RuntimeError>>,
    },
    ShutdownPullTaskScheduler {
        immediate: bool,
        reply: oneshot::Sender<Result<Vec<super::tasks::RuntimeScheduledTaskHandle>, RuntimeError>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<bool, RuntimeError>>,
    },
}

impl Command {
    pub(super) fn with_monitor(self, monitor: Option<MonitorGuard>) -> Self {
        match monitor {
            Some(monitor) => Self::Monitored {
                command: Box::new(self),
                monitor,
            },
            None => self,
        }
    }

    pub(super) fn split_monitor(self) -> (Self, Option<MonitorGuard>) {
        match self {
            Self::Monitored { command, monitor } => (*command, Some(monitor)),
            command => (command, None),
        }
    }
}

/// A response cancelled before the receiving future can accept the lease must
/// undo its registration. Drop enqueues cleanup into the existing owned worker.
pub(super) struct PendingLease {
    monitor: Option<MonitorGuard>,
    lease: Option<GroupLease>,
    sender: mpsc::UnboundedSender<Command>,
    groups: Arc<super::registry::GroupRegistry>,
    runtime_handle: tokio::runtime::Handle,
    timeout: Duration,
}

impl PendingLease {
    pub fn accept(mut self) -> Result<GroupLease, RuntimeError> {
        self.lease.take().ok_or(RuntimeError::RegistryPoisoned)
    }
}

impl Drop for PendingLease {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            let monitor = self.monitor.take();
            let kind = lease.kind;
            let deadline = Instant::now() + self.timeout;
            let table_kind = match kind {
                GroupKind::Producer => Some(super::tables::TableKind::Producer),
                GroupKind::Consumer => Some(super::tables::TableKind::Consumer),
                GroupKind::Admin => None,
            };
            let gate = table_kind.and_then(|table_kind| {
                self.groups
                    .table_mutation_gate(table_kind, &lease.group)
                    .ok()
            });
            let Some(gate) = gate else {
                let _ = self.sender.send(
                    Command::Unregister {
                        lease,
                        kind,
                        deadline,
                        gate: None,
                        reply: None,
                    }
                    .with_monitor(monitor),
                );
                return;
            };
            match gate.clone().try_lock_owned() {
                Ok(gate) => {
                    let _ = self.sender.send(
                        Command::Unregister {
                            lease,
                            kind,
                            deadline,
                            gate: Some(gate),
                            reply: None,
                        }
                        .with_monitor(monitor),
                    );
                }
                Err(_) => {
                    let wait_gate = gate;
                    let sender = self.sender.clone();
                    self.runtime_handle.spawn(async move {
                        let gate = wait_gate.lock_owned().await;
                        let _ = sender.send(
                            Command::Unregister {
                                lease,
                                kind,
                                deadline,
                                gate: Some(gate),
                                reply: None,
                            }
                            .with_monitor(monitor),
                        );
                    });
                }
            }
        }
    }
}

pub(super) struct ControlPlane {
    state: AtomicU8,
    sender: Mutex<Option<mpsc::UnboundedSender<Command>>>,
    task: Mutex<Option<JoinHandle<()>>>,
    join_gate: tokio::sync::Mutex<()>,
    #[cfg(test)]
    pub fault: AtomicU8,
}

impl Default for ControlPlane {
    fn default() -> Self {
        Self {
            state: AtomicU8::new(RuntimeState::Created as u8),
            sender: Mutex::new(None),
            task: Mutex::new(None),
            join_gate: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            fault: AtomicU8::new(0),
        }
    }
}

impl Drop for ControlPlane {
    fn drop(&mut self) {
        if let Ok(task) = self.task.get_mut() {
            if let Some(task) = task.take() {
                task.abort();
            }
        }
    }
}

impl ControlPlane {
    pub fn state(&self) -> RuntimeState {
        match self.state.load(Ordering::Acquire) {
            0 => RuntimeState::Created,
            1 => RuntimeState::Starting,
            2 => RuntimeState::Running,
            3 => RuntimeState::ShuttingDown,
            4 => RuntimeState::Stopped,
            _ => RuntimeState::Failed,
        }
    }

    fn set_state(&self, state: RuntimeState) {
        self.state.store(state as u8, Ordering::Release);
    }

    pub fn sender(
        &self,
        runtime: &Arc<ClientRuntime>,
    ) -> Result<mpsc::UnboundedSender<Command>, RuntimeError> {
        let mut sender = self
            .sender
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if sender.as_ref().is_some_and(|sender| !sender.is_closed()) {
            return Ok(sender
                .as_ref()
                .expect("checked live control sender")
                .clone());
        }
        *sender = None;
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            rocketmq_classic_remoting::TransportError::InvalidConfiguration(
                "runtime lifecycle requires an active Tokio runtime".into(),
            )
        })?;
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = handle.spawn(run(Arc::downgrade(runtime), tx.downgrade(), rx));
        if !matches!(self.state(), RuntimeState::Stopped | RuntimeState::Failed) {
            *self
                .task
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)? = Some(worker);
        }
        *sender = Some(tx.clone());
        Ok(tx)
    }

    pub(super) fn existing_sender(
        &self,
    ) -> Result<Option<mpsc::UnboundedSender<Command>>, RuntimeError> {
        Ok(self
            .sender
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?
            .clone())
    }

    pub async fn join_terminal(&self) -> Result<(), RuntimeError> {
        if !matches!(self.state(), RuntimeState::Stopped | RuntimeState::Failed) {
            return Ok(());
        }
        // Poll the owned handle in place. Cancelling a joining caller must not
        // take/drop (detach) the only handle. The async gate serializes wakers.
        let _join = self.join_gate.lock().await;
        std::future::poll_fn(|context| {
            use std::{future::Future, pin::Pin, task::Poll};
            let mut owned = match self.task.lock() {
                Ok(owned) => owned,
                Err(_) => return Poll::Ready(Err(RuntimeError::RegistryPoisoned)),
            };
            let Some(task) = owned.as_mut() else {
                return Poll::Ready(Ok(()));
            };
            match Pin::new(task).poll(context) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    owned.take();
                    Poll::Ready(result.map_err(|_| RuntimeError::ControlTaskFailed))
                }
            }
        })
        .await
    }

    fn checkpoint(&self, _step: u8) -> Result<(), RuntimeError> {
        #[cfg(test)]
        if self.fault.load(Ordering::Acquire) == _step {
            return Err(RuntimeError::StartupFailure);
        }
        Ok(())
    }
}

struct InnerProducer;
impl ProducerRuntime for InnerProducer {}

async fn run(
    runtime: Weak<ClientRuntime>,
    sender: mpsc::WeakUnboundedSender<Command>,
    mut receiver: mpsc::UnboundedReceiver<Command>,
) {
    let mut inner_producer: Option<Arc<dyn ProducerRuntime>> = None;
    let mut inner_lease: Option<GroupLease> = None;
    let mut periodic = runtime.upgrade().map(|runtime| {
        super::tasks::OwnedTasks::new(
            Arc::downgrade(&runtime),
            runtime.task_scheduler_status.clone(),
        )
    });
    let mut inbound = Vec::new();
    let mut rebalance: Vec<super::rebalance::OwnedRebalance> = Vec::new();
    let mut pull_workers: Vec<super::pull::OwnedPullWorker> = Vec::new();
    while let Some(command) = receiver.recv().await {
        // The command owns admission through completion, even when its caller
        // cancels the reply future while an unregister RPC remains in flight.
        let (command, monitor) = command.split_monitor();
        let Some(runtime) = runtime.upgrade() else {
            return;
        };
        let state = runtime.control.state();
        let allowed = matches!(state, RuntimeState::Created | RuntimeState::Running);
        match command {
            Command::Monitored { .. } => unreachable!("monitor envelopes are never nested"),
            Command::StartServiceThread { kind, reply } => {
                let service_start_allowed =
                    allowed || matches!(state, RuntimeState::Stopped | RuntimeState::Failed);
                let result = if !service_start_allowed {
                    Err(RuntimeError::InvalidState(state))
                } else {
                    let control = runtime.service_thread_control(kind);
                    if control.start() {
                        match kind {
                            super::service_thread::RuntimeServiceKind::Rebalance => {
                                let worker = super::rebalance::OwnedRebalance::new(
                                    Arc::downgrade(&runtime),
                                    runtime.rebalance.clone(),
                                );
                                worker.activate();
                                rebalance.push(worker);
                            }
                            super::service_thread::RuntimeServiceKind::PullMessage => {
                                runtime.pull.open_dispatch();
                                let worker = super::pull::OwnedPullWorker::new(
                                    Arc::downgrade(&runtime),
                                    runtime.pull.clone(),
                                    false,
                                );
                                worker.activate();
                                pull_workers.push(worker);
                            }
                        }
                    }
                    Ok(())
                };
                let _ = reply.send(result);
            }
            Command::ShutdownServiceThread {
                kind,
                interrupt,
                reply,
            } => {
                let control = runtime.service_thread_control(kind);
                let was_started = control.shutdown();
                let daemon = control.thread_is_daemon();
                let result = match kind {
                    super::service_thread::RuntimeServiceKind::Rebalance => {
                        if was_started {
                            runtime.rebalance.wakeup();
                            if !daemon {
                                if let Some(worker) = rebalance.last_mut() {
                                    let _ = worker
                                        .join_with_timeout(Duration::from_millis(
                                            super::SERVICE_THREAD_JOIN_TIME_MILLIS,
                                        ))
                                        .await;
                                }
                            }
                        }
                        Ok(())
                    }
                    super::service_thread::RuntimeServiceKind::PullMessage => {
                        if was_started && interrupt {
                            runtime.pull.interrupt_wait.notify_one();
                        }
                        if was_started && !daemon {
                            if let Some(worker) = pull_workers
                                .iter_mut()
                                .rfind(|worker| !worker.is_scheduled())
                            {
                                let _ = worker
                                    .join_with_timeout(Duration::from_millis(
                                        super::SERVICE_THREAD_JOIN_TIME_MILLIS,
                                    ))
                                    .await;
                            }
                        }
                        let scheduler_result = runtime.pull.shutdown_scheduler(false);
                        if scheduler_result.is_ok() {
                            if let Some(worker) =
                                pull_workers.iter_mut().find(|worker| worker.is_scheduled())
                            {
                                worker.finish_gracefully().await;
                            } else if !runtime.pull.is_scheduler_terminated() {
                                runtime.pull.mark_scheduler_terminated();
                            }
                        }
                        scheduler_result.map(|_| ())
                    }
                };
                let _ = reply.send(result);
            }
            Command::MutateTable {
                kind,
                mutation,
                reply,
            } => {
                if !reply.is_closed() {
                    let _ = reply.send(runtime.groups.mutate_table(kind, &mutation));
                }
            }
            Command::StartPullScheduler => {
                if matches!(
                    state,
                    RuntimeState::Created | RuntimeState::Starting | RuntimeState::Running
                ) && !runtime.pull.is_scheduler_shutdown()
                    && !pull_workers.iter().any(|worker| worker.is_scheduled())
                {
                    let worker = super::pull::OwnedPullWorker::new(
                        Arc::downgrade(&runtime),
                        runtime.pull.clone(),
                        true,
                    );
                    worker.activate();
                    pull_workers.push(worker);
                }
            }
            Command::ScheduleRuntimeTask(schedule) => {
                if !runtime.task_scheduler_status.is_shutdown()
                    && matches!(
                        state,
                        RuntimeState::Created | RuntimeState::Starting | RuntimeState::Running
                    )
                {
                    if periodic.is_none() {
                        periodic = Some(super::tasks::OwnedTasks::new(
                            Arc::downgrade(&runtime),
                            runtime.task_scheduler_status.clone(),
                        ));
                    }
                    let result = periodic
                        .as_mut()
                        .expect("client scheduler was initialized")
                        .register_user(schedule.clone());
                    if result.is_ok() {
                        periodic
                            .as_ref()
                            .expect("client scheduler was initialized")
                            .activate();
                    } else {
                        schedule.reject();
                    }
                } else {
                    schedule.reject();
                }
            }
            Command::ShutdownRuntimeScheduler { immediate, reply } => {
                let result = if immediate {
                    if let Some(tasks) = periodic.as_mut() {
                        tasks.shutdown_now().await
                    } else {
                        runtime.task_scheduler_status.shutdown();
                        runtime.task_scheduler_status.terminate();
                        Ok(Vec::new())
                    }
                } else if let Some(tasks) = periodic.as_mut() {
                    tasks.shutdown_gracefully().map(|()| Vec::new())
                } else {
                    runtime.task_scheduler_status.shutdown();
                    runtime.task_scheduler_status.terminate();
                    Ok(Vec::new())
                };
                let _ = reply.send(result);
            }
            Command::ShutdownPullTaskScheduler { immediate, reply } => {
                let result = match runtime.pull.shutdown_scheduler(immediate) {
                    Ok(cancelled) => {
                        if immediate {
                            if let Some(worker) =
                                pull_workers.iter().find(|worker| worker.is_scheduled())
                            {
                                worker.request_stop();
                            } else {
                                runtime.pull.mark_scheduler_terminated();
                            }
                        } else if !pull_workers.iter().any(|worker| worker.is_scheduled()) {
                            runtime.pull.mark_scheduler_terminated();
                        }
                        Ok(cancelled)
                    }
                    Err(error) => Err(error),
                };
                let _ = reply.send(result);
            }
            Command::RegisterProducer {
                group,
                handle,
                gate: _gate,
                reply,
            } => {
                if reply.is_closed() {
                    continue;
                }
                let result = if allowed {
                    runtime.groups.register_producer(&group, handle)
                } else {
                    Err(RuntimeError::InvalidState(state))
                };
                deliver(result, reply, &sender, &runtime, monitor.clone());
            }
            Command::RegisterConsumer {
                group,
                handle,
                gate: _gate,
                reply,
            } => {
                if reply.is_closed() {
                    continue;
                }
                let result = if allowed {
                    runtime.groups.register_consumer(&group, handle)
                } else {
                    Err(RuntimeError::InvalidState(state))
                };
                deliver(result, reply, &sender, &runtime, monitor.clone());
            }
            Command::RegisterAdmin { group, reply } => {
                if reply.is_closed() {
                    continue;
                }
                let result = if allowed {
                    runtime.groups.register_admin(&group)
                } else {
                    Err(RuntimeError::InvalidState(state))
                };
                deliver(result, reply, &sender, &runtime, monitor.clone());
            }
            Command::UnregisterByGroup {
                group,
                kind,
                deadline,
                gate,
                reply,
            } => {
                if !reply.is_closed() {
                    let result = runtime
                        .unregister_current_group(&group, kind, deadline, gate)
                        .await;
                    let _ = reply.send(result);
                }
            }
            Command::Unregister {
                lease,
                kind,
                deadline,
                gate,
                reply,
            } => {
                let result = runtime.unregister_group(&lease, kind, deadline, gate).await;
                if let Some(reply) = reply {
                    let _ = reply.send(result);
                }
            }
            Command::Start { reply } => {
                if reply.is_closed() {
                    continue;
                }
                if state == RuntimeState::Running {
                    let _ = reply.send(Ok(()));
                    continue;
                }
                if state != RuntimeState::Created {
                    let _ = reply.send(Err(RuntimeError::InvalidState(state)));
                    continue;
                }
                runtime.control.set_state(RuntimeState::Starting);
                let producer: Arc<dyn ProducerRuntime> = Arc::new(InnerProducer);
                let result = async {
                    inner_lease = Some(
                        runtime
                            .groups
                            .register_producer(INNER_PRODUCER, producer.clone())?,
                    );
                    runtime.control.checkpoint(1)?;
                    let inbound_executor = runtime.inbound.request_executor()?;
                    for (index, code) in super::inbound::CODES.into_iter().enumerate() {
                        inbound.push(runtime.remoting().register_owned_processor_with_executor(
                            code,
                            runtime.inbound.clone(),
                            Some(inbound_executor.clone()),
                        ));
                        runtime.control.checkpoint(10 + index as u8)?;
                    }
                    let needs_name_server_discovery = runtime.config_read().namesrv_addr.is_none();
                    if needs_name_server_discovery {
                        runtime.fetch_name_server_addr().await;
                    }
                    runtime.remoting().start()?;
                    runtime.control.checkpoint(2)?;
                    let config = runtime.config_snapshot();
                    let specs = super::tasks::schedules(&config)?;
                    if periodic.is_none() {
                        periodic = Some(super::tasks::OwnedTasks::new(
                            Arc::downgrade(&runtime),
                            runtime.task_scheduler_status.clone(),
                        ));
                    }
                    for (index, schedule) in specs.into_iter().enumerate() {
                        if let Some(tasks) = periodic.as_mut() {
                            tasks.register(schedule)?;
                        }
                        runtime.control.checkpoint(3 + index as u8)?;
                    }
                    let start_pull_service = runtime.pull.thread.start();
                    let start_rebalance_service = runtime.rebalance.thread.start();
                    for (scheduled, checkpoint) in [(false, 21), (true, 22)] {
                        if scheduled && runtime.pull.is_scheduler_shutdown() {
                            runtime.control.checkpoint(checkpoint)?;
                            continue;
                        }
                        let should_start = if scheduled {
                            !pull_workers.iter().any(|worker| worker.is_scheduled())
                        } else {
                            start_pull_service
                        };
                        if should_start {
                            let worker = super::pull::OwnedPullWorker::new(
                                Arc::downgrade(&runtime),
                                runtime.pull.clone(),
                                scheduled,
                            );
                            pull_workers.push(worker);
                        }
                        runtime.control.checkpoint(checkpoint)?;
                    }
                    if start_rebalance_service {
                        rebalance.push(super::rebalance::OwnedRebalance::new(
                            Arc::downgrade(&runtime),
                            runtime.rebalance.clone(),
                        ));
                    }
                    runtime.control.checkpoint(20)?;
                    Ok(())
                }
                .await;
                match result {
                    Ok(()) => {
                        inner_producer = Some(producer);
                        runtime.control.set_state(RuntimeState::Running);
                        if let Some(tasks) = periodic.as_ref() {
                            tasks.activate();
                        }
                        for worker in &rebalance {
                            worker.activate();
                        }
                        for worker in &pull_workers {
                            worker.activate();
                        }
                        let _ = reply.send(Ok(()));
                    }
                    Err(error) => {
                        runtime.pull.close();
                        runtime.rebalance.close_delayed();
                        runtime.pull.thread.shutdown();
                        runtime.rebalance.thread.shutdown();
                        for worker in rebalance.iter_mut().rev() {
                            worker.stop().await;
                        }
                        for worker in pull_workers.iter_mut().rev() {
                            worker.stop().await;
                        }
                        runtime.pull.mark_scheduler_terminated();
                        if let Some(tasks) = periodic.as_mut() {
                            tasks.stop().await;
                        }
                        runtime.remoting().shutdown().await;
                        let _ = runtime.inbound.shutdown_executor().await;
                        inbound.clear();
                        if let Some(lease) = inner_lease.take() {
                            let _ = runtime.groups.unregister(&lease, GroupKind::Producer);
                        }
                        runtime.control.set_state(RuntimeState::Failed);
                        let _ = reply.send(Err(error));
                        break;
                    }
                }
            }
            Command::Shutdown { reply } => {
                if runtime
                    .groups
                    .has_active_users(INNER_PRODUCER)
                    .unwrap_or(true)
                    || state != RuntimeState::Running
                {
                    let _ = reply.send(Ok(false));
                    continue;
                }
                runtime.control.set_state(RuntimeState::ShuttingDown);
                runtime.rebalance.close_delayed();
                runtime.pull.stop_accepting();
                runtime.rebalance.thread.shutdown();
                runtime.pull.thread.shutdown();
                for worker in rebalance.iter_mut().rev() {
                    worker.stop().await;
                }
                for worker in pull_workers.iter_mut().rev() {
                    if worker.is_scheduled() {
                        worker.finish_gracefully().await;
                    } else {
                        worker.stop().await;
                    }
                }
                runtime.pull.close();
                runtime.pull.mark_scheduler_terminated();
                if let Some(tasks) = periodic.as_mut() {
                    tasks.stop().await;
                }
                if let Some(lease) = inner_lease.take() {
                    let deadline = Instant::now() + runtime.rpc_timeout();
                    let _ = runtime
                        .unregister_group(&lease, GroupKind::Producer, deadline, None)
                        .await;
                }
                inner_producer.take();
                runtime.remoting().shutdown().await;
                let executor_result = runtime.inbound.shutdown_executor().await;
                inbound.clear();
                runtime.control.set_state(RuntimeState::Stopped);
                let removal_result = runtime.remove_from_manager().map(|_| true);
                let result = executor_result.and(removal_result);
                let _ = reply.send(result);
                break;
            }
        }
    }
}

fn deliver(
    result: Result<GroupLease, RuntimeError>,
    reply: oneshot::Sender<Result<PendingLease, RuntimeError>>,
    sender: &mpsc::WeakUnboundedSender<Command>,
    runtime: &ClientRuntime,
    monitor: Option<MonitorGuard>,
) {
    let result = result.and_then(|lease| {
        let sender = sender.upgrade().ok_or(RuntimeError::ControlTaskFailed)?;
        Ok(PendingLease {
            monitor,
            lease: Some(lease),
            sender,
            groups: runtime.groups.clone(),
            runtime_handle: tokio::runtime::Handle::current(),
            timeout: runtime.rpc_timeout(),
        })
    });
    let _ = reply.send(result); // Undelivered PendingLease enqueues identity cleanup.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::ClientConfig, runtime::ClientRuntimeManager};

    #[tokio::test]
    async fn undelivered_registration_retains_monitor_until_identity_cleanup_finishes() {
        struct Producer;
        impl ProducerRuntime for Producer {}
        let mut config = ClientConfig::new("unaccepted-monitor");
        config.set_namesrv_addr(Some(String::new()));
        let runtime = ClientRuntimeManager::new()
            .get_or_create(&config)
            .await
            .unwrap();
        let sender = runtime.control.sender(&runtime).unwrap();
        let lease = runtime
            .groups
            .register_producer("unaccepted", Arc::new(Producer))
            .unwrap();
        let key_gate = runtime
            .groups
            .table_mutation_gate(super::super::tables::TableKind::Producer, "unaccepted")
            .unwrap()
            .lock_owned()
            .await;
        let monitor = Arc::new(runtime.monitor_gate.clone().lock_owned().await);
        let (reply, receive) = oneshot::channel();
        drop(receive);
        deliver(
            Ok(lease),
            reply,
            &sender.downgrade(),
            &runtime,
            Some(monitor.clone()),
        );
        drop(monitor);
        // Cleanup is deliberately waiting for this key gate. The abandoned
        // reply must keep monitor ownership while the accepted entry exists.
        assert!(runtime.monitor_gate.try_lock().is_err());
        let reset_runtime = runtime.clone();
        let reset = tokio::spawn(async move {
            reset_runtime
                .reset_offset(None, Some("missing"), None)
                .await
        });
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert!(!reset.is_finished());
        drop(key_gate);
        reset.await.unwrap().unwrap();
        assert!(runtime.select_producer("unaccepted").unwrap().is_none());
        runtime.shutdown().await.unwrap();
    }

    struct BlankAddressing;
    impl crate::namesrv::TopAddressing for BlankAddressing {
        fn fetch_ns_addr(
            &self,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<String>> + Send + '_>>
        {
            Box::pin(async { Some(" ".into()) })
        }
    }

    #[tokio::test]
    async fn discovery_schedule_failure_joins_all_owned_work() {
        for point in [
            1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22,
        ] {
            let manager = ClientRuntimeManager::with_top_addressing(Arc::new(BlankAddressing));
            let config = ClientConfig::new("discovery-failure");
            let runtime = manager.get_or_create(&config).await.unwrap();
            runtime.rebalance_later(60_000).unwrap();
            runtime.execute_task_later(async {}, 60_000).unwrap();
            runtime.control.fault.store(point, Ordering::SeqCst);
            assert!(matches!(
                runtime.start().await,
                Err(RuntimeError::StartupFailure)
            ));
            assert_eq!(runtime.state(), RuntimeState::Failed);
            assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 0);
            assert_eq!(runtime.rebalance_probe.live.load(Ordering::SeqCst), 0);
            assert_eq!(runtime.pull_probe.live.load(Ordering::SeqCst), 0);
            assert!(runtime.pull.is_empty());
            assert!(runtime.rebalance.next_due().is_none());
            assert!(runtime.control.task.lock().unwrap().is_none());
            assert_eq!(
                runtime.remoting().with_processor_table(|table| table.len()),
                0
            );
            assert!(
                runtime
                    .prepare_heartbeat_data(false)
                    .unwrap()
                    .producer_data_set
                    .as_ref()
                    .unwrap()
                    .with(|producers| producers.is_empty())
            );
            let replacement = manager.get_or_create(&config).await.unwrap();
            assert!(Arc::ptr_eq(&runtime, &replacement));
        }
    }

    #[tokio::test]
    async fn concurrent_starters_own_exactly_one_periodic_worker_and_join_it() {
        let manager = ClientRuntimeManager::new();
        let mut config = ClientConfig::new("one-task-set");
        config.namesrv_addr = Some(String::new());
        let runtime = manager.get_or_create(&config).await.unwrap();
        let mut calls = Vec::new();
        for _ in 0..64 {
            let runtime = runtime.clone();
            calls.push(tokio::spawn(async move {
                runtime.start().await.unwrap();
            }));
        }
        for call in calls {
            call.await.unwrap();
        }
        assert_eq!(runtime.periodic_probe.created.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.rebalance_probe.created.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.rebalance_probe.live.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.pull_probe.created.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.pull_probe.live.load(Ordering::SeqCst), 2);
        assert!(runtime.shutdown().await.unwrap());
        assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.rebalance_probe.live.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.pull_probe.live.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn p6_v01_running_first_lookups_share_identity_task_set_and_actual_socket() {
        use bytes::BytesMut;
        use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        const CALLS: usize = 64;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut identities = std::collections::HashSet::new();
            for _ in 0..CALLS {
                let mut length = [0; 4];
                tokio::select! {
                    result = socket.read_exact(&mut length) => { result.unwrap(); }
                    result = listener.accept() => panic!("duplicate pooled socket: {result:?}"),
                }
                let mut bytes = BytesMut::from(&length[..]);
                bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
                socket.read_exact(&mut bytes[4..]).await.unwrap();
                let command = frame::decode_next(&mut bytes).unwrap().unwrap();
                assert_eq!(command.code, 999_999);
                assert!(identities.insert(command.opaque));
                let mut reply = RemotingCommand {
                    opaque: command.opaque,
                    ..Default::default()
                };
                reply.mark_response();
                socket
                    .write_all(&frame::encode(&reply).unwrap())
                    .await
                    .unwrap();
            }
            let mut byte = [0];
            tokio::select! {
                result = socket.read(&mut byte) => assert_eq!(result.unwrap(), 0),
                result = listener.accept() => panic!("late duplicate pooled socket: {result:?}"),
            }
        });
        let manager = ClientRuntimeManager::new();
        let mut config = ClientConfig::new("p6-v01-running");
        config.namesrv_addr = Some(String::new());
        let barrier = Arc::new(tokio::sync::Barrier::new(CALLS));
        let mut calls = Vec::new();
        for _ in 0..CALLS {
            let manager = manager.clone();
            let config = config.clone();
            let address = address.clone();
            let barrier = barrier.clone();
            calls.push(tokio::spawn(async move {
                barrier.wait().await;
                let runtime = manager.get_or_create(&config).await.unwrap();
                runtime.start().await.unwrap();
                let reply = runtime
                    .remoting()
                    .invoke_sync(
                        &address,
                        RemotingCommand {
                            code: 999_999,
                            ..Default::default()
                        },
                        Duration::from_secs(5),
                    )
                    .await
                    .unwrap();
                assert_eq!(reply.code, 0);
                runtime
            }));
        }
        let runtime = calls.remove(0).await.unwrap();
        for call in calls {
            assert!(Arc::ptr_eq(&runtime, &call.await.unwrap()));
        }
        assert_eq!(runtime.state(), RuntimeState::Running);
        assert_eq!(runtime.periodic_probe.created.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.rebalance_probe.created.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.rebalance_probe.live.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.pull_probe.created.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.pull_probe.live.load(Ordering::SeqCst), 2);
        assert_eq!(
            runtime.remoting().with_processor_table(|table| table.len()),
            8
        );
        assert!(runtime.shutdown().await.unwrap());
        assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.rebalance_probe.live.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.pull_probe.live.load(Ordering::SeqCst), 0);
        tokio::time::timeout(Duration::from_secs(5), peer)
            .await
            .unwrap()
            .unwrap();
    }

    // Keep an executor runnable while waiting on real loopback I/O so paused
    // Tokio time advances only at the test's explicit clock boundaries.
    async fn frozen_io<F: std::future::Future>(future: F) -> F::Output {
        let started = std::time::Instant::now();
        tokio::pin!(future);
        loop {
            tokio::select! {
                biased;
                result = &mut future => return result,
                _ = tokio::task::yield_now() => {
                    assert!(started.elapsed() < Duration::from_secs(5), "scripted I/O stalled");
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn p6_u01_periodic_subscription_wire_and_unregister_keep_shared_user_running() {
        use bytes::BytesMut;
        use rocketmq_classic_protocol::SharedValue;
        use rocketmq_classic_protocol::{
            codes::{LanguageCode, request},
            heartbeat::{ConsumerData, HeartbeatData, ProducerData, SubscriptionData},
        };
        use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
        use std::collections::{BTreeMap, HashSet};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::{TcpListener, TcpStream},
        };

        struct Consumer(Mutex<SubscriptionData>);
        impl ConsumerRuntime for Consumer {
            fn heartbeat_data(&self) -> ConsumerData {
                ConsumerData {
                    group_name: Some("consumer".into()),
                    subscription_data_set: Some(SharedValue::from(HashSet::from([Some(
                        SharedValue::from(self.0.lock().unwrap().clone()),
                    )]))),
                    unit_mode: true,
                    ..Default::default()
                }
            }
        }
        async fn read(socket: &mut TcpStream) -> Option<RemotingCommand> {
            let mut length = [0; 4];
            match socket.read_exact(&mut length).await {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return None,
                Err(error) => panic!("scripted peer read: {error}"),
            }
            let mut bytes = BytesMut::from(&length[..]);
            bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
            socket.read_exact(&mut bytes[4..]).await.unwrap();
            frame::decode_next(&mut bytes).unwrap()
        }
        async fn respond(
            socket: &mut TcpStream,
            command: &RemotingCommand,
            version: i32,
            body: Option<Vec<u8>>,
        ) {
            let mut reply = RemotingCommand {
                opaque: command.opaque,
                version,
                body,
                ..Default::default()
            };
            reply.mark_response();
            socket
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        }
        fn assert_heartbeat(command: &RemotingCommand, consumer: &Consumer, producers: &[&str]) {
            assert_eq!(command.code, request::HEART_BEAT);
            assert_eq!(command.language, Some(LanguageCode::Java));
            let heartbeat: HeartbeatData =
                serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
            let expected = HeartbeatData {
                client_id: Some("p6-u01@DEFAULT".into()),
                producer_data_set: Some(SharedValue::from(
                    producers
                        .iter()
                        .map(|group| {
                            Some(SharedValue::from(ProducerData {
                                group_name: Some((*group).into()),
                            }))
                        })
                        .collect::<HashSet<_>>(),
                )),
                consumer_data_set: Some(SharedValue::from(HashSet::from([Some(
                    SharedValue::from(consumer.heartbeat_data()),
                )]))),
                ..Default::default()
            };
            fn sort_json_arrays(value: &mut serde_json::Value) {
                match value {
                    serde_json::Value::Array(rows) => {
                        for row in rows.iter_mut() {
                            sort_json_arrays(row);
                        }
                        rows.sort_by_key(serde_json::Value::to_string);
                    }
                    serde_json::Value::Object(fields) => {
                        for value in fields.values_mut() {
                            sort_json_arrays(value);
                        }
                    }
                    _ => {}
                }
            }
            let mut actual_json = serde_json::to_value(&heartbeat).unwrap();
            let mut expected_json = serde_json::to_value(&expected).unwrap();
            sort_json_arrays(&mut actual_json);
            sort_json_arrays(&mut expected_json);
            assert_eq!(actual_json, expected_json);
            let json: serde_json::Value =
                serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
            assert_eq!(json["clientID"], "p6-u01@DEFAULT");
            assert_eq!(json["withoutSub"], false);
            assert!(json.get("clientId").is_none());
        }
        fn assert_unregister(command: &RemotingCommand, field: &str, group: &str) {
            assert_eq!(command.code, request::UNREGISTER_CLIENT);
            assert_eq!(command.language, Some(LanguageCode::Java));
            assert!(command.body.is_none());
            assert_eq!(
                command.ext_fields,
                Some(
                    BTreeMap::from([
                        ("clientID".into(), "p6-u01@DEFAULT".into()),
                        (field.into(), group.into()),
                    ])
                    .into()
                )
            );
        }

        let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let broker_address = broker.local_addr().unwrap().to_string();
        let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let nameserver_address = nameserver.local_addr().unwrap().to_string();
        let route = serde_json::to_vec(&serde_json::json!({
            "queueDatas": [], "brokerDatas": [{"cluster":"cluster","brokerName":"broker","brokerAddrs":{"0":broker_address}}], "filterServerTable":{}
        })).unwrap();
        let name_peer = tokio::spawn(async move {
            let (mut socket, _) = nameserver.accept().await.unwrap();
            while let Some(command) = read(&mut socket).await {
                assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
                respond(&mut socket, &command, 0, Some(route.clone())).await;
            }
        });
        let (observed, mut events) = mpsc::unbounded_channel();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = broker.accept().await.unwrap();
            let mut heartbeats = 0;
            loop {
                let command = tokio::select! {
                    command = read(&mut socket) => command,
                    result = broker.accept() => panic!("shared user opened another socket: {result:?}"),
                };
                let Some(command) = command else {
                    break;
                };
                assert!(matches!(
                    command.code,
                    request::HEART_BEAT | request::UNREGISTER_CLIENT
                ));
                heartbeats += usize::from(command.code == request::HEART_BEAT);
                respond(&mut socket, &command, 550 + heartbeats as i32, None).await;
                observed.send(command).unwrap();
            }
        });
        let manager = ClientRuntimeManager::new();
        let mut config = ClientConfig::new("p6-u01");
        config.namesrv_addr = Some(nameserver_address);
        // Channel-active behavior has its own wire gate; isolate scheduled ticks.
        config.enable_heartbeat_channel_event_listener = false;
        let runtime = frozen_io(manager.get_or_create(&config)).await.unwrap();
        let producer = Arc::new(InnerProducer);
        let consumer = Arc::new(Consumer(Mutex::new(SubscriptionData {
            topic: Some("topic".into()),
            sub_string: Some("A".into()),
            sub_version: 41,
            tags_set: Some(SharedValue::from(HashSet::from([Some("A".into())]))),
            code_set: Some(SharedValue::from(HashSet::from([Some(65)]))),
            ..Default::default()
        })));
        let producer_lease = frozen_io(runtime.register_producer("producer", producer.clone()))
            .await
            .unwrap();
        let consumer_lease = frozen_io(runtime.register_consumer("consumer", consumer.clone()))
            .await
            .unwrap();
        let other_user = frozen_io(manager.get_or_create(&config)).await.unwrap();
        assert!(Arc::ptr_eq(&runtime, &other_user));
        frozen_io(runtime.start()).await.unwrap();
        frozen_io(runtime.update_topic_route("topic", Instant::now() + Duration::from_secs(3)))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(999)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(events.try_recv().is_err());
        tokio::time::advance(Duration::from_millis(1)).await;
        let command = frozen_io(events.recv()).await.unwrap();
        assert_heartbeat(&command, &consumer, &["producer", INNER_PRODUCER]);
        frozen_io(async {
            while runtime
                .route_snapshot()
                .unwrap()
                .broker_versions
                .get(&("broker".into(), broker_address.clone()))
                != Some(&551)
            {
                tokio::task::yield_now().await;
            }
        })
        .await;
        frozen_io(
            runtime.unregister_producer(&producer_lease, Instant::now() + Duration::from_secs(3)),
        )
        .await
        .unwrap();
        assert_unregister(
            &frozen_io(events.recv()).await.unwrap(),
            "producerGroup",
            "producer",
        );
        assert!(!frozen_io(runtime.shutdown()).await.unwrap());
        assert_eq!(other_user.state(), RuntimeState::Running);
        assert_eq!(runtime.periodic_probe.created.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.rebalance_probe.live.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.pull_probe.live.load(Ordering::SeqCst), 2);
        *consumer.0.lock().unwrap() = SubscriptionData {
            topic: Some("topic".into()),
            sub_string: Some("B".into()),
            sub_version: 42,
            tags_set: Some(SharedValue::from(HashSet::from([Some("B".into())]))),
            code_set: Some(SharedValue::from(HashSet::from([Some(66)]))),
            ..Default::default()
        };
        tokio::time::advance(Duration::from_millis(29_999)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(events.try_recv().is_err());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_heartbeat(
            &frozen_io(events.recv()).await.unwrap(),
            &consumer,
            &[INNER_PRODUCER],
        );
        frozen_io(
            runtime.unregister_consumer(&consumer_lease, Instant::now() + Duration::from_secs(3)),
        )
        .await
        .unwrap();
        assert_unregister(
            &frozen_io(events.recv()).await.unwrap(),
            "consumerGroup",
            "consumer",
        );
        assert!(frozen_io(other_user.shutdown()).await.unwrap());
        assert_unregister(
            &frozen_io(events.recv()).await.unwrap(),
            "producerGroup",
            INNER_PRODUCER,
        );
        frozen_io(peer).await.unwrap();
        frozen_io(name_peer).await.unwrap();
        assert_eq!(runtime.state(), RuntimeState::Stopped);
        assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.rebalance_probe.live.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.pull_probe.live.load(Ordering::SeqCst), 0);
        tokio::time::advance(Duration::from_secs(120)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(events.try_recv().is_err());
    }

    #[tokio::test]
    async fn every_initial_startup_failure_keeps_factory_tables_and_cleans_transport_control() {
        for point in [1, 2, 3, 4, 5, 6, 10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22] {
            let manager = ClientRuntimeManager::new();
            let mut config = ClientConfig::new(format!("failure-{point}"));
            config.namesrv_addr = Some(String::new());
            let runtime = manager.get_or_create(&config).await.unwrap();
            let producer: Arc<dyn ProducerRuntime> = Arc::new(InnerProducer);
            let lease = runtime
                .register_producer("producer", producer.clone())
                .await
                .unwrap();
            assert!(matches!(
                runtime
                    .register_reply_request(
                        &lease,
                        "pre-start-request",
                        Instant::now() + Duration::from_secs(60)
                    )
                    .await,
                Err(RuntimeError::InvalidState(RuntimeState::Created))
            ));
            runtime.control.fault.store(point, Ordering::Release);
            runtime.rebalance_later(60_000).unwrap();
            runtime.execute_task_later(async {}, 60_000).unwrap();
            assert!(matches!(
                runtime.start().await,
                Err(RuntimeError::StartupFailure)
            ));
            assert_eq!(runtime.state(), RuntimeState::Failed);
            assert!(runtime.control.task.lock().unwrap().is_none());
            assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 0);
            assert_eq!(runtime.rebalance_probe.live.load(Ordering::SeqCst), 0);
            assert_eq!(runtime.pull_probe.live.load(Ordering::SeqCst), 0);
            assert!(runtime.pull.is_empty());
            assert!(runtime.rebalance.next_due().is_none());
            assert_eq!(
                runtime.remoting().with_processor_table(|table| table.len()),
                0
            );
            assert!(runtime.select_producer("producer").unwrap().is_some());
            assert!(
                runtime
                    .unregister_producer(&lease, Instant::now())
                    .await
                    .unwrap()
            );
            assert!(matches!(
                runtime.start().await,
                Err(RuntimeError::InvalidState(RuntimeState::Failed))
            ));
            assert!(Arc::ptr_eq(
                &runtime,
                &manager.get_or_create(&config).await.unwrap()
            ));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn p6_v03_every_failure_closes_pending_socket_and_drops_owned_work() {
        use bytes::BytesMut;
        use rocketmq_classic_remoting::{RemotingCommand, TransportError, codec::frame};
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        use tokio::{io::AsyncReadExt, net::TcpListener};

        struct Consumer;
        impl ConsumerRuntime for Consumer {
            fn heartbeat_data(&self) -> rocketmq_classic_protocol::heartbeat::ConsumerData {
                Default::default()
            }
        }
        struct Deferred(Arc<AtomicUsize>);
        impl Drop for Deferred {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        // Source-owned effects: group, eight inbound slots, transport,
        // four/five schedules, two pull workers and rebalance worker.
        for discovery in [false, true] {
            let mut points = vec![1, 2, 3, 4, 5, 6, 10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22];
            if discovery {
                points.push(7);
            }
            // Also exercise real scheduling failures, rather than only the
            // private injected error: zero/negative for each public period.
            points.extend(30..36);
            for point in points {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap().to_string();
                let (seen, received) = oneshot::channel();
                let peer = tokio::spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut length = [0; 4];
                    socket.read_exact(&mut length).await.unwrap();
                    let mut bytes = BytesMut::from(&length[..]);
                    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
                    socket.read_exact(&mut bytes[4..]).await.unwrap();
                    let command = frame::decode_next(&mut bytes).unwrap().unwrap();
                    assert_eq!(command.code, 999_999);
                    seen.send(()).unwrap(); // Deliberately never answer this RPC.
                    let mut byte = [0];
                    tokio::select! {
                        result = socket.read(&mut byte) => assert_eq!(result.unwrap(), 0),
                        result = listener.accept() => panic!("rollback opened an extra socket: {result:?}"),
                    }
                });
                let manager = ClientRuntimeManager::with_top_addressing(Arc::new(BlankAddressing));
                let mut config = ClientConfig::new(format!("p6-v03-{discovery}-{point}"));
                config.namesrv_addr = (!discovery).then(String::new);
                config.enable_heartbeat_channel_event_listener = false;
                if point >= 30 {
                    let invalid = if point % 2 == 0 { 0 } else { -1 };
                    match (point - 30) / 2 {
                        0 => config.poll_name_server_interval = invalid,
                        1 => config.heartbeat_broker_interval = invalid,
                        _ => config.persist_consumer_offset_interval = invalid,
                    }
                }
                let runtime = manager.get_or_create(&config).await.unwrap();
                let producer: Arc<dyn ProducerRuntime> = Arc::new(InnerProducer);
                let consumer: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
                let producer_lease = runtime
                    .register_producer("producer", producer.clone())
                    .await
                    .unwrap();
                let consumer_lease = runtime
                    .register_consumer("consumer", consumer.clone())
                    .await
                    .unwrap();
                let admin_lease = runtime.register_admin("admin").await.unwrap();
                let dropped = Arc::new(AtomicUsize::new(0));
                let executed = Arc::new(AtomicBool::new(false));
                let marker = Deferred(dropped.clone());
                let execution = executed.clone();
                runtime
                    .execute_task_later(
                        async move {
                            let _marker = marker;
                            execution.store(true, Ordering::SeqCst);
                        },
                        60_000,
                    )
                    .unwrap();
                runtime.rebalance_later(60_000).unwrap();
                let snapshot = runtime.route_snapshot().unwrap();
                // Exercise rollback of an already active lower socket/request,
                // not only bookkeeping on a transport with no connections.
                runtime.remoting().start().unwrap();
                let request_runtime = runtime.clone();
                let pending = tokio::spawn(async move {
                    request_runtime
                        .remoting()
                        .invoke_sync(
                            &address,
                            RemotingCommand {
                                code: 999_999,
                                ..Default::default()
                            },
                            Duration::from_secs(60),
                        )
                        .await
                });
                tokio::time::timeout(Duration::from_secs(5), received)
                    .await
                    .unwrap()
                    .unwrap();
                runtime
                    .control
                    .fault
                    .store(if point >= 30 { 0 } else { point }, Ordering::Release);
                let result = tokio::time::timeout(Duration::from_secs(5), runtime.start())
                    .await
                    .unwrap();
                if point >= 30 {
                    assert!(matches!(result, Err(RuntimeError::InvalidSchedule)));
                } else {
                    assert!(matches!(result, Err(RuntimeError::StartupFailure)));
                }
                assert!(matches!(
                    tokio::time::timeout(Duration::from_secs(5), pending)
                        .await
                        .unwrap()
                        .unwrap(),
                    Err(TransportError::Shutdown)
                ));
                tokio::time::timeout(Duration::from_secs(5), peer)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(runtime.state(), RuntimeState::Failed);
                assert!(runtime.control.task.lock().unwrap().is_none());
                assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 0);
                assert_eq!(runtime.rebalance_probe.live.load(Ordering::SeqCst), 0);
                assert_eq!(runtime.pull_probe.live.load(Ordering::SeqCst), 0);
                assert_eq!(dropped.load(Ordering::SeqCst), 1);
                assert!(!executed.load(Ordering::SeqCst));
                assert!(runtime.pull.is_empty());
                assert!(runtime.rebalance.next_due().is_none());
                assert_eq!(
                    runtime.remoting().with_processor_table(|table| table.len()),
                    0
                );
                assert!(Arc::ptr_eq(&snapshot, &runtime.route_snapshot().unwrap()));
                assert!(runtime.select_producer("producer").unwrap().is_some());
                assert!(runtime.select_consumer("consumer").unwrap().is_some());
                assert!(runtime.groups.has_active_users(INNER_PRODUCER).unwrap());
                assert!(
                    runtime
                        .unregister_producer(&producer_lease, Instant::now())
                        .await
                        .unwrap()
                );
                assert!(
                    runtime
                        .unregister_consumer(&consumer_lease, Instant::now())
                        .await
                        .unwrap()
                );
                assert!(runtime.unregister_admin(&admin_lease).await.unwrap());
                assert!(!runtime.groups.has_active_users(INNER_PRODUCER).unwrap());
                assert_eq!(runtime.remoting().start(), Err(TransportError::Shutdown));
                assert!(Arc::ptr_eq(
                    &runtime,
                    &manager.get_or_create(&config).await.unwrap()
                ));
                assert!(
                    manager
                        .remove_client_factory(runtime.client_id(), &runtime)
                        .unwrap()
                );
                let replacement = manager.get_or_create(&config).await.unwrap();
                assert!(!Arc::ptr_eq(&runtime, &replacement));
                // Old failure/removal identity cannot evict that replacement.
                assert!(
                    !manager
                        .remove_client_factory(runtime.client_id(), &runtime)
                        .unwrap()
                );
                assert!(Arc::ptr_eq(
                    &replacement,
                    &manager.get_or_create(&config).await.unwrap()
                ));
            }
        }
    }

    #[tokio::test]
    async fn abandoned_registration_delivery_rolls_back_through_the_owned_worker() {
        let manager = ClientRuntimeManager::new();
        let mut config = ClientConfig::new("delivery-cancellation");
        config.namesrv_addr = Some(String::new());
        let runtime = manager.get_or_create(&config).await.unwrap();
        let sender = runtime.control.sender(&runtime).unwrap();
        let producer: Arc<dyn ProducerRuntime> = Arc::new(InnerProducer);
        let (reply, response) = oneshot::channel();
        sender
            .send(Command::RegisterProducer {
                group: "producer".into(),
                handle: producer.clone(),
                gate: runtime
                    .groups
                    .table_mutation_gate(super::super::tables::TableKind::Producer, "producer")
                    .unwrap()
                    .lock_owned()
                    .await,
                reply,
            })
            .unwrap_or_else(|_| panic!("owned worker must be available"));
        let delivery = response.await.unwrap().unwrap();
        assert!(runtime.select_producer("producer").unwrap().is_some());
        drop(delivery); // Caller never accepts the delivered identity.
        let replacement = runtime
            .register_producer("producer", producer.clone())
            .await
            .unwrap();
        assert_eq!(replacement.group(), "producer");
        runtime
            .unregister_producer(&replacement, Instant::now())
            .await
            .unwrap();
        runtime.start().await.unwrap();
        assert!(runtime.shutdown().await.unwrap());
        assert!(runtime.control.task.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn cancelled_terminal_join_keeps_the_owned_handle_for_another_joiner() {
        let control = Arc::new(ControlPlane::default());
        control.set_state(RuntimeState::Stopped);
        let (release, ready) = oneshot::channel();
        *control.task.lock().unwrap() = Some(tokio::spawn(async move {
            let _ = ready.await;
        }));
        let joining = control.clone();
        let first = tokio::spawn(async move { joining.join_terminal().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while control.join_gate.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        first.abort();
        assert!(matches!(first.await, Err(error) if error.is_cancelled()));
        assert!(control.task.lock().unwrap().is_some());
        let joining = control.clone();
        let mut second = tokio::spawn(async move { joining.join_terminal().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut second)
                .await
                .is_err()
        );
        release.send(()).unwrap();
        second.await.unwrap().unwrap();
        assert!(control.task.lock().unwrap().is_none());
    }
}
