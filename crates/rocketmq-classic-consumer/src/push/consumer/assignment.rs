use super::*;

impl PushConsumer {
    /// Activates an assignment without a Broker lock for private pipeline tests.
    #[cfg(test)]
    pub(crate) async fn activate_process_queue(
        &self,
        input_queue: &MessageQueue,
        initial_offset: i64,
    ) -> Result<Arc<ProcessQueue>, PushConsumerError> {
        self.activate_process_queue_with_lock(input_queue, initial_offset, None)
            .await
    }

    pub(super) async fn activate_process_queue_with_lock(
        &self,
        input_queue: &MessageQueue,
        initial_offset: i64,
        initialization_time: Option<(i64, Instant)>,
    ) -> Result<Arc<ProcessQueue>, PushConsumerError> {
        if initial_offset < 0 {
            return Err(PushConsumerError::InvalidOffset);
        }
        let mut client_config = self.config.client_config().clone();
        let queue = client_config.queue_with_namespace(input_queue)?;
        let topic = queue
            .topic()
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidQueue)?;
        if queue.broker_name().is_none_or(str::is_empty) {
            return Err(PushConsumerError::InvalidQueue);
        }
        if !read_lock(&self.subscriptions).contains_key(topic) {
            return Err(PushConsumerError::MissingSubscriptionForQueue(
                topic.to_owned(),
            ));
        }
        let consumer_group = self.consume_stats_group();
        if read_lock(&self.offset_corrections_pending).contains(&queue) {
            return Err(PushConsumerError::OffsetCorrectionPending(
                queue.to_string(),
            ));
        }
        let (scheduler, offset_store) = match &*self.state.lock().await {
            State::Created => return Err(PushConsumerError::NotStarted),
            State::Shutdown => return Err(PushConsumerError::Shutdown),
            State::Started {
                pull_scheduler,
                offset_store,
                ..
            } => (pull_scheduler.clone(), offset_store.clone()),
        };
        if let Some(existing) = read_lock(&self.process_queues).get(&queue).cloned() {
            if existing
                .snapshot(existing.generation())
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                return Ok(existing);
            }
            return Err(PushConsumerError::ProcessQueueRemovalPending(
                queue.to_string(),
            ));
        }
        let generation = self
            .next_generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| PushConsumerError::GenerationExhausted)?;
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), generation));
        process_queue.set_locked(true);
        if let Some((timestamp_millis, initialized_at)) = initialization_time {
            let _ = process_queue.set_lock_state_at(
                generation,
                true,
                timestamp_millis,
                Some(initialized_at),
            );
        }
        {
            let mut queues = write_lock(&self.process_queues);
            if let Some(existing) = queues.get(&queue) {
                if existing
                    .snapshot(existing.generation())
                    .is_ok_and(|snapshot| !snapshot.dropped)
                {
                    return Ok(existing.clone());
                }
                return Err(PushConsumerError::ProcessQueueRemovalPending(
                    queue.to_string(),
                ));
            }
            queues.insert(queue.clone(), process_queue.clone());
        }
        offset_store.update_offset(&queue, initial_offset, false);
        let request = Arc::new(PullRequest::new(
            consumer_group,
            process_queue.clone(),
            initial_offset,
        ));
        if self.paused.load(Ordering::Acquire) {
            scheduler.pause(request.queue(), request.generation());
        }
        if let Err(error) = scheduler.schedule(request, 0) {
            scheduler.cancel(&queue, generation);
            let mut queues = write_lock(&self.process_queues);
            if queues
                .get(&queue)
                .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
            {
                queues.remove(&queue);
                offset_store.remove_offset(&queue);
            }
            return Err(PushConsumerError::PullScheduler(error.to_string()));
        }
        Ok(process_queue)
    }

    pub(crate) async fn remove_process_queue_generation(
        &self,
        input_queue: &MessageQueue,
        generation: u64,
    ) -> bool {
        let mut client_config = self.config.client_config().clone();
        let Ok(queue) = client_config.queue_with_namespace(input_queue) else {
            return false;
        };
        let (scheduler, offset_store, runtime) = match &*self.state.lock().await {
            State::Started {
                runtime,
                offset_store,
                pull_scheduler,
                ..
            } => (
                pull_scheduler.clone(),
                offset_store.clone(),
                runtime.upgrade(),
            ),
            State::Created | State::Shutdown => return false,
        };
        let process_queue = {
            let mut queues = write_lock(&self.process_queues);
            if queues
                .get(&queue)
                .is_none_or(|process_queue| process_queue.generation() != generation)
            {
                return false;
            }
            queues.remove(&queue)
        };
        if let Some(process_queue) = process_queue {
            let _ = process_queue.drop_assignment(generation);
            scheduler.cancel(&queue, generation);
            offset_store.remove_offset(&queue);
            write_lock(&self.offset_corrections_pending).remove(&queue);
            if let Some(runtime) = runtime {
                runtime.rebalance_immediately();
            }
            true
        } else {
            false
        }
    }
}
