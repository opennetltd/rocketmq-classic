use super::*;

impl ClientInner {
    pub(super) async fn acquire_admission(
        &self,
        semaphore: Arc<tokio::sync::Semaphore>,
        deadline: Instant,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, TransportError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        if let Ok(permit) = semaphore.clone().try_acquire_owned() {
            return Ok(permit);
        }
        if Instant::now() >= deadline {
            return Err(TransportError::TooManyRequests);
        }
        let mut shutdown = self.shutdown.subscribe();
        tokio::select! {
            permit = time::timeout_at(deadline, semaphore.acquire_owned()) => permit
                .map_err(|_| TransportError::timeout(
                    crate::error::TimeoutPhase::Admission,
                    None,
                    Some(deadline.saturating_duration_since(Instant::now())),
                ))?
                .map_err(|_| TransportError::Shutdown),
            changed = shutdown.changed() => {
                let _ = changed;
                Err(TransportError::Shutdown)
            }
        }
    }

    pub(super) fn notify_channel(&self, event_type: NettyEventType, channel: ChannelHandle) {
        if self.channel_listener.is_some() {
            let _ = self.channel_event_sender.try_send(ChannelEvent {
                remote_addr: channel.remote_addr().to_owned(),
                event_type,
                channel,
            });
        }
    }

    pub(super) fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        let _ = self.shutdown.send(true);
    }

    pub(super) fn insert_pending(
        &self,
        generation: u64,
        deadline: Instant,
        address: String,
        request_code: i32,
    ) -> Result<
        (
            i32,
            oneshot::Receiver<Result<RemotingCommand, TransportError>>,
        ),
        TransportError,
    > {
        if deadline <= Instant::now() {
            return Err(TransportError::Timeout);
        }
        let (sender, receiver) = oneshot::channel();
        let mut pending = self.pending.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        for _ in 0..=pending.len() {
            let opaque = self.opaque.fetch_add(1, Ordering::Relaxed);
            if let std::collections::hash_map::Entry::Vacant(entry) = pending.entry(opaque) {
                entry.insert(PendingEntry {
                    generation,
                    deadline,
                    address,
                    request_code,
                    response: sender,
                    retained_admission: None,
                });
                return Ok((opaque, receiver));
            }
        }
        Err(TransportError::OpaqueExhausted)
    }

    pub(super) fn cancel_pending(&self, opaque: i32, generation: u64) {
        self.finish_pending(opaque, generation, Err(TransportError::Closed));
    }

    pub(super) fn interrupt_pull_requests(&self, address: &str) {
        let requests = self
            .pending
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(opaque, entry)| {
                (entry.address == address
                    && matches!(
                        entry.request_code,
                        rocketmq_classic_protocol::codes::request::PULL_MESSAGE
                            | rocketmq_classic_protocol::codes::request::LITE_PULL_MESSAGE
                    ))
                .then_some((*opaque, entry.generation))
            })
            .collect::<Vec<_>>();
        for (opaque, generation) in requests {
            self.finish_pending(opaque, generation, Err(TransportError::Interrupted));
        }
    }

    pub(super) fn expire_pending(&self, opaque: i32, generation: u64) {
        self.finish_pending(opaque, generation, Err(TransportError::Timeout));
    }

    pub(super) fn complete_response(&self, generation: u64, command: RemotingCommand) {
        self.finish_pending(command.opaque, generation, Ok(command));
    }

    pub(super) fn finish_pending(
        &self,
        opaque: i32,
        generation: u64,
        result: Result<RemotingCommand, TransportError>,
    ) -> bool {
        let mut pending = self.pending.lock().unwrap();
        if pending
            .get(&opaque)
            .is_some_and(|entry| entry.generation == generation)
        {
            if let Some(entry) = pending.remove(&opaque) {
                let result = if entry.deadline <= Instant::now() && result.is_ok() {
                    Err(TransportError::Timeout)
                } else {
                    result
                };
                let _ = entry.response.send(result);
                return true;
            }
        }
        false
    }

    pub(super) fn fail_generation(&self, generation: u64, error: TransportError) {
        let ids = {
            let pending = self.pending.lock().unwrap();
            pending
                .iter()
                .filter_map(|(id, item)| {
                    (item.generation == generation).then_some((*id, item.address.clone()))
                })
                .collect::<Vec<_>>()
        };
        for (id, address) in ids {
            let error = match &error {
                TransportError::Closed
                | TransportError::Send(_)
                | TransportError::Codec(_)
                | TransportError::MalformedFrame(_) => TransportError::SendRequest {
                    remote_addr: address,
                    cause: None,
                },
                _ => error.clone(),
            };
            self.finish_pending(id, generation, Err(error));
        }
    }

    pub(super) fn fail_all_pending(&self) {
        let pending = self
            .pending
            .lock()
            .unwrap()
            .iter()
            .map(|(opaque, entry)| (*opaque, entry.generation))
            .collect::<Vec<_>>();
        for (opaque, generation) in pending {
            self.finish_pending(opaque, generation, Err(TransportError::Shutdown));
        }
    }

    pub(super) fn dispatch_callback(&self, job: CallbackJob) {
        let task: CallbackTask = Box::new(move || run_callback(job));
        if self.disable_callback_executor.load(Ordering::Acquire) {
            task();
            return;
        }
        if let Some(executor) = self.callback_executor.lock().unwrap().clone() {
            let task = Arc::new(Mutex::new(Some(task)));
            let task_for_executor = task.clone();
            let dispatched: CallbackTask = Box::new(move || {
                if let Some(task) = task_for_executor.lock().unwrap().take() {
                    task();
                }
            });
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                executor.execute(dispatched)
            }));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => {
                    if let Some(task) = task.lock().unwrap().take() {
                        task();
                    }
                }
            }
            return;
        }
        if let Err(error) = self.callback_sender.send(task) {
            (error.0)();
        }
    }

    pub(super) fn spawn(&self, task: JoinHandle<()>) {
        let mut tasks = self.tasks.lock().unwrap();
        tasks.retain(|task| !task.is_finished());
        if self.closed.load(Ordering::Acquire) {
            task.abort();
        } else {
            tasks.push(task);
        }
    }
}
