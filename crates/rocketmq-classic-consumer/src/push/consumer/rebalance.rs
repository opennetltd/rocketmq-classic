use super::*;

impl PushConsumer {
    pub(crate) async fn rebalance_once(
        self: &Arc<Self>,
        strategy: &dyn crate::rebalance::AllocateMessageQueueStrategy,
        is_order: Option<bool>,
    ) -> bool {
        let Some(context) = self.pull_context().await else {
            return true;
        };
        self.pop_retired_workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|worker| !worker.is_finished());
        let rebalance_runtime = self.get_client_runtime().await;
        let mut balanced = true;
        let mut subscriptions = read_lock(&self.subscriptions)
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        subscriptions.sort_by(|left, right| java_string_cmp(left, right));
        let message_model = self.rebalance_service.message_model();
        let is_order = is_order.unwrap_or_else(|| self.consume_orderly.load(Ordering::Acquire));
        let subscribed_topics = subscriptions.iter().cloned().collect::<HashSet<_>>();
        let topic_queues = read_lock(&self.topic_queues).clone();
        let consumer_group = self.consume_stats_group();

        let mut assignments = Vec::new();
        let mut authoritative_topics = HashSet::new();
        let mut desired_push_queues = HashSet::new();
        let mut desired_pop_queues = HashSet::new();
        let mut missing_route_notifications = Vec::new();
        for topic in &subscriptions {
            if !self.rebalance_is_started().await {
                return true;
            }
            if message_model == rocketmq_classic_protocol::heartbeat::MessageModel::LiteSelective
                && self.client_rebalance(topic)
            {
                // Java's LiteSelective rebalance uses the default no-local-allocation branch;
                // broker assignment is used when client-side rebalance is disabled.
                continue;
            }

            if message_model == rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting {
                let Some(queue_snapshot) = topic_queues.get(topic) else {
                    missing_route_notifications.push(topic.clone());
                    continue;
                };
                let mut all_queues = queue_snapshot.iter().cloned().collect::<Vec<_>>();
                all_queues.sort();
                let all_queue_set = all_queues.iter().cloned().collect::<HashSet<_>>();
                authoritative_topics.insert(topic.clone());
                desired_push_queues.extend(all_queues.iter().cloned());
                assignments.push(DesiredAssignment {
                    topic: topic.clone(),
                    all_queues: Some(all_queue_set.clone()),
                    assigned_queues: all_queue_set.clone(),
                    pull_queues: all_queue_set,
                    pop_queues: HashSet::new(),
                });
                continue;
            }

            if self.client_rebalance(topic) {
                // Java selects local assignment from clientRebalance(topic);
                // `is_order` controls the Broker lock request, not selection.
                // The pinned client handles missing and empty routes before
                // querying membership, notifying empty sets for non-retry topics.
                let Some(queue_snapshot) =
                    topic_queues.get(topic).filter(|queues| !queues.is_empty())
                else {
                    if !topic.starts_with("%RETRY%") {
                        missing_route_notifications.push(topic.clone());
                    }
                    continue;
                };
                let Some(rebalance_runtime) = rebalance_runtime.as_ref() else {
                    balanced = false;
                    continue;
                };
                let consumer_ids = self
                    .find_consumer_id_list_for_rebalance(rebalance_runtime, topic, &consumer_group)
                    .await;
                let mut all_queues = queue_snapshot.iter().cloned().collect::<Vec<_>>();
                all_queues.sort();
                let all_queue_set = all_queues.iter().cloned().collect::<HashSet<_>>();
                let consumer_ids = match consumer_ids {
                    Ok(Some(consumer_ids)) if !consumer_ids.is_empty() => consumer_ids,
                    Ok(Some(_)) | Ok(None) => {
                        // Java's rebalanceByTopic leaves its default `true`
                        // result unchanged when membership is null or empty.
                        // It also leaves the current assignments untouched.
                        continue;
                    }
                    Err(error) => {
                        tracing::warn!(topic, error = %error, "push rebalance consumer list lookup failed");
                        balanced = false;
                        continue;
                    }
                };
                let mut consumer_ids = match consumer_ids.into_iter().collect::<Option<Vec<_>>>() {
                    Some(consumer_ids) => consumer_ids,
                    None => {
                        balanced = false;
                        continue;
                    }
                };
                consumer_ids.sort_by(|left, right| java_string_cmp(left, right));
                let allocation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    strategy.allocate(
                        &consumer_group,
                        rebalance_runtime.client_id(),
                        &all_queues,
                        &consumer_ids,
                    )
                }));
                let allocated = match allocation {
                    Ok(Ok(allocated)) => allocated,
                    Ok(Err(error)) => {
                        tracing::warn!(
                            topic,
                            strategy = strategy.name(),
                            error = %error,
                            "push queue allocation failed"
                        );
                        balanced = false;
                        continue;
                    }
                    Err(_) => {
                        tracing::warn!(
                            topic,
                            strategy = strategy.name(),
                            "push queue allocation panicked"
                        );
                        balanced = false;
                        continue;
                    }
                };
                let assigned = allocated.into_iter().collect::<HashSet<_>>();
                desired_push_queues.extend(assigned.iter().cloned());
                assignments.push(DesiredAssignment {
                    topic: topic.clone(),
                    all_queues: Some(all_queue_set),
                    assigned_queues: assigned.clone(),
                    pull_queues: assigned,
                    pop_queues: HashSet::new(),
                });
                authoritative_topics.insert(topic.clone());
                continue;
            } else {
                // Broker assignment does not depend on the local route table.
                let Some(rebalance_runtime) = rebalance_runtime.as_ref() else {
                    balanced = false;
                    continue;
                };
                let result = rebalance_runtime
                    .query_assignment(
                        topic,
                        &consumer_group,
                        strategy.name(),
                        message_model,
                        3_000,
                    )
                    .await;
                #[allow(clippy::mutable_key_type)]
                let assignment_set = match result {
                    Ok(Some(assignment_set)) => assignment_set,
                    Ok(None) => {
                        balanced = false;
                        continue;
                    }
                    Err(error) => {
                        tracing::warn!(topic, error = %error, "push broker assignment lookup failed");
                        balanced = false;
                        continue;
                    }
                };
                let Some(BrokerAssignmentQueues {
                    assigned,
                    pull: pull_queues,
                    pop: pop_queues,
                }) = partition_broker_assignments(assignment_set)
                else {
                    balanced = false;
                    continue;
                };
                self.reconcile_pop_retry_topic_subscription(
                    topic,
                    &consumer_group,
                    &pull_queues,
                    &pop_queues,
                );
                desired_push_queues.extend(pull_queues.iter().cloned());
                desired_pop_queues.extend(pop_queues.iter().cloned());
                authoritative_topics.insert(topic.clone());
                assignments.push(DesiredAssignment {
                    topic: topic.clone(),
                    all_queues: None,
                    assigned_queues: assigned,
                    pull_queues,
                    pop_queues,
                });
            };
        }

        let current_queues = read_lock(&self.process_queues)
            .iter()
            .map(|(queue, process_queue)| (queue.clone(), process_queue.clone()))
            .collect::<Vec<_>>();
        let mut changed_queues = HashSet::new();
        for (queue, process_queue) in current_queues {
            if !self.rebalance_is_started().await {
                return true;
            }
            let Some(topic) = queue.topic() else {
                balanced = false;
                continue;
            };
            let generation = process_queue.generation();
            let snapshot = match process_queue.snapshot(generation) {
                Ok(snapshot) => snapshot,
                Err(_) => {
                    balanced = false;
                    continue;
                }
            };
            if !subscribed_topics.contains(topic) {
                // Java truncates queues for removed subscriptions separately
                // from assignment cleanup: it retains their stored offsets.
                let _ = process_queue.drop_assignment(generation);
                context.scheduler.cancel(&queue, generation);
                let mut queues = write_lock(&self.process_queues);
                if queues
                    .get(&queue)
                    .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
                {
                    queues.remove(&queue);
                }
                continue;
            }
            let should_remove = authoritative_topics.contains(topic)
                && (!desired_push_queues.contains(&queue)
                    || process_queue.is_pull_expired_with_timeout(
                        self.pull_max_idle_time_millis.load(Ordering::Acquire),
                    ));
            if !should_remove {
                continue;
            }
            if !snapshot.dropped {
                if process_queue.drop_assignment(generation).is_err() {
                    balanced = false;
                    continue;
                }
                context.scheduler.cancel(&queue, generation);
            }
            match self
                .remove_process_queue_safely(&context, &queue, &process_queue)
                .await
            {
                true => {
                    changed_queues.insert(queue);
                }
                false => balanced = false,
            }
        }

        let current_pop_queues = read_lock(&self.pop_process_queues)
            .iter()
            .map(|(queue, process_queue)| (queue.clone(), process_queue.clone()))
            .collect::<Vec<_>>();
        for (queue, process_queue) in current_pop_queues {
            if !self.rebalance_is_started().await {
                return true;
            }
            let Some(topic) = queue.topic() else {
                balanced = false;
                continue;
            };
            let should_remove = !subscribed_topics.contains(topic)
                || (authoritative_topics.contains(topic)
                    && (!desired_pop_queues.contains(&queue)
                        || process_queue.is_pull_expired_with_timeout(
                            self.pop_pull_max_idle_time_millis.load(Ordering::Acquire),
                        )));
            if !should_remove {
                continue;
            }
            process_queue.drop_assignment();
            let removed = {
                let mut queues = write_lock(&self.pop_process_queues);
                if queues
                    .get(&queue)
                    .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
                {
                    queues.remove(&queue);
                    true
                } else {
                    false
                }
            };
            if !removed {
                continue;
            }
            let worker = self
                .pop_workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&queue);
            if let Some(worker) = worker {
                if !worker.is_finished() {
                    self.pop_retired_workers
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(worker);
                }
            }
            changed_queues.insert(queue);
        }

        let mut additions = desired_push_queues.into_iter().collect::<Vec<_>>();
        additions.sort();
        let lock_new_queues = is_order
            && message_model != rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting;
        for queue in additions {
            if !self.rebalance_is_started().await {
                return true;
            }
            let Some(topic) = queue.topic() else {
                balanced = false;
                continue;
            };
            if !subscribed_topics.contains(topic) {
                continue;
            }
            if let Some(existing) = read_lock(&self.process_queues).get(&queue).cloned() {
                if existing
                    .snapshot(existing.generation())
                    .is_ok_and(|snapshot| !snapshot.dropped)
                {
                    continue;
                }
                // A dropped generation stays registered until its persisted
                // offset cleanup succeeds; a replacement waits for that retry.
                balanced = false;
                continue;
            }
            if read_lock(&self.offset_corrections_pending).contains(&queue) {
                balanced = false;
                continue;
            }
            let orderly_locked = if lock_new_queues {
                let Some(rebalance_runtime) = rebalance_runtime.as_ref() else {
                    balanced = false;
                    continue;
                };
                let lock_result = self
                    .lock_orderly_queues_raw(rebalance_runtime, std::slice::from_ref(&queue))
                    .await;
                if lock_result.uncertain.contains(&queue) {
                    self.unlock_orderly_queues_tracking(
                        rebalance_runtime,
                        std::slice::from_ref(&queue),
                    )
                    .await;
                }
                if !lock_result.locked.contains(&queue) {
                    balanced = false;
                    continue;
                }
                let Some(_) = lock_result.lock_times.get(&queue) else {
                    balanced = false;
                    continue;
                };
                if !self.rebalance_is_started().await {
                    self.unlock_orderly_queues_tracking(
                        rebalance_runtime,
                        std::slice::from_ref(&queue),
                    )
                    .await;
                    return true;
                }
                true
            } else {
                false
            };
            context.offset_store.remove_offset(&queue);
            let queue_initialization_time = (epoch_millis(), Instant::now());
            let Some(rebalance_runtime) = rebalance_runtime.as_ref() else {
                balanced = false;
                continue;
            };
            let initial_offset = match self
                .compute_initial_offset(rebalance_runtime, &context.offset_store, &queue)
                .await
            {
                Ok(offset) => offset,
                Err(error) => {
                    tracing::warn!(queue = %queue, error = %error, "push start offset lookup failed");
                    if orderly_locked {
                        self.unlock_orderly_queues_tracking(
                            rebalance_runtime,
                            std::slice::from_ref(&queue),
                        )
                        .await;
                    }
                    balanced = false;
                    continue;
                }
            };
            match self
                .activate_process_queue_with_lock(
                    &queue,
                    initial_offset,
                    Some(queue_initialization_time),
                )
                .await
            {
                Ok(_) => {
                    changed_queues.insert(queue);
                }
                Err(error) => {
                    tracing::warn!(queue = %queue, error = %error, "push queue activation failed");
                    if orderly_locked {
                        self.unlock_orderly_queues_tracking(
                            rebalance_runtime,
                            std::slice::from_ref(&queue),
                        )
                        .await;
                    }
                    balanced = false;
                }
            }
        }

        let mut pop_additions = desired_pop_queues.into_iter().collect::<Vec<_>>();
        pop_additions.sort();
        for queue in pop_additions {
            if !self.rebalance_is_started().await {
                return true;
            }
            let Some(topic) = queue.topic() else {
                balanced = false;
                continue;
            };
            if !subscribed_topics.contains(topic) {
                continue;
            }
            if read_lock(&self.process_queues)
                .get(&queue)
                .is_some_and(|process_queue| {
                    process_queue
                        .snapshot(process_queue.generation())
                        .is_ok_and(|snapshot| !snapshot.dropped)
                })
            {
                balanced = false;
                continue;
            }
            let subscription = match read_lock(&self.subscriptions).get(topic).cloned() {
                Some(subscription) => subscription,
                None => {
                    balanced = false;
                    continue;
                }
            };
            let previous_worker = {
                let mut workers = self
                    .pop_workers
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if workers
                    .get(&queue)
                    .is_some_and(|worker| !worker.is_finished())
                    && read_lock(&self.pop_process_queues).contains_key(&queue)
                {
                    continue;
                }
                workers.remove(&queue)
            };
            if let Some(worker) = previous_worker.filter(|worker| !worker.is_finished()) {
                self.pop_retired_workers
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(worker);
            }

            if read_lock(&self.pop_process_queues).contains_key(&queue) {
                // Java's putIfAbsent does not replace a registered POP queue
                // just because its dropped flag is set. Removal is authorized
                // only by an authoritative assignment update above.
                balanced = false;
                continue;
            }
            let generation = match self.next_generation.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |value| value.checked_add(1),
            ) {
                Ok(generation) => generation,
                Err(_) => {
                    balanced = false;
                    continue;
                }
            };
            let process_queue = Arc::new(PopProcessQueue::new_for_assignment(
                queue.clone(),
                generation,
            ));
            {
                let mut queues = write_lock(&self.pop_process_queues);
                if queues.contains_key(&queue) {
                    balanced = false;
                    continue;
                }
                queues.insert(queue.clone(), process_queue.clone());
            }
            let consumer = self.clone();
            let pop_queue = process_queue.clone();
            let group = consumer_group.clone();
            let worker = tokio::spawn(async move {
                consumer.pop_loop(pop_queue, group, subscription).await;
            });
            self.pop_workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(queue.clone(), worker);
            changed_queues.insert(queue);
        }

        for assignment in &assignments {
            let expected = assignment
                .pull_queues
                .iter()
                .filter(|queue| queue.topic() == Some(assignment.topic.as_str()))
                .cloned()
                .collect::<HashSet<_>>();
            let working = read_lock(&self.process_queues)
                .iter()
                .filter(|(queue, process_queue)| {
                    queue.topic() == Some(assignment.topic.as_str())
                        && process_queue
                            .snapshot(process_queue.generation())
                            .is_ok_and(|snapshot| !snapshot.dropped)
                })
                .map(|(queue, _)| queue.clone())
                .collect::<HashSet<_>>();
            if expected != working {
                balanced = false;
            }
            let expected_pop = assignment
                .pop_queues
                .iter()
                .filter(|queue| queue.topic() == Some(assignment.topic.as_str()))
                .cloned()
                .collect::<HashSet<_>>();
            let working_pop = read_lock(&self.pop_process_queues)
                .iter()
                .filter(|(queue, process_queue)| {
                    queue.topic() == Some(assignment.topic.as_str()) && !process_queue.is_dropped()
                })
                .map(|(queue, _)| queue.clone())
                .collect::<HashSet<_>>();
            if expected_pop != working_pop {
                balanced = false;
            }
            let queue_changed = changed_queues.iter().any(|queue| {
                queue.topic() == Some(assignment.topic.as_str())
                    || assignment.assigned_queues.contains(queue)
            });
            if queue_changed
                && !self
                    .notify_assignment_changed(rebalance_runtime.as_ref(), assignment)
                    .await
            {
                balanced = false;
            }
        }
        for topic in missing_route_notifications {
            if !self.rebalance_is_started().await {
                return true;
            }
            let empty = HashSet::new();
            let assignment = DesiredAssignment {
                topic,
                all_queues: Some(empty.clone()),
                assigned_queues: empty.clone(),
                pull_queues: empty,
                pop_queues: HashSet::new(),
            };
            if !self
                .notify_assignment_changed(rebalance_runtime.as_ref(), &assignment)
                .await
            {
                balanced = false;
            }
        }
        balanced
    }

    pub(super) async fn rebalance_is_started(&self) -> bool {
        matches!(&*self.state.lock().await, State::Started { .. })
    }

    async fn find_consumer_id_list_for_rebalance(
        &self,
        runtime: &Arc<ClientRuntime>,
        topic: &str,
        consumer_group: &str,
    ) -> Result<Option<Vec<Option<String>>>, rocketmq_classic_core::runtime::RuntimeError> {
        #[cfg(test)]
        {
            let scripted = self
                .rebalance_membership
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_mut(topic)
                .and_then(VecDeque::pop_front);
            if let Some(scripted) = scripted {
                return Ok(scripted.map(|ids| ids.into_iter().map(Some).collect()));
            }
        }
        runtime.find_consumer_id_list(topic, consumer_group).await
    }

    pub(super) async fn remove_process_queue_safely(
        &self,
        context: &PullContext,
        queue: &MessageQueue,
        process_queue: &Arc<ProcessQueue>,
    ) -> bool {
        let generation = process_queue.generation();
        if !self.rebalance_is_started().await {
            return false;
        }
        let orderly_clustering = self.current_orderly_lock_settings().await.filter(|_| {
            self.config.message_model()
                == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
        });
        let _consume_guard = if orderly_clustering.is_some() {
            let gate = process_queue.orderly_consume_gate();
            match tokio::time::timeout(Duration::from_millis(500), gate.write_owned()).await {
                Ok(guard) => Some(guard),
                Err(_) => {
                    let force_unlock = orderly_clustering.is_some_and(|settings| {
                        process_queue
                            .is_lock_older_than(generation, settings.unlock_delay_millis)
                            .unwrap_or(false)
                    });
                    if force_unlock {
                        None
                    } else {
                        let _ = process_queue.increment_try_unlock_times(generation);
                        return false;
                    }
                }
            }
        } else {
            None
        };
        if let Err(error) = context.offset_store.persist(queue).await {
            tracing::warn!(queue = %queue, generation, error = %error, "push queue offset cleanup deferred");
            return false;
        }
        if orderly_clustering.is_some()
            && !self
                .unlock_orderly_process_queues(
                    &context.runtime,
                    std::slice::from_ref(process_queue),
                )
                .await
        {
            return false;
        }
        let mut queues = write_lock(&self.process_queues);
        if !queues
            .get(queue)
            .is_some_and(|current| Arc::ptr_eq(current, process_queue))
        {
            return false;
        }
        context.offset_store.remove_offset(queue);
        queues.remove(queue);
        write_lock(&self.offset_corrections_pending).remove(queue);
        true
    }

    pub(super) async fn compute_initial_offset(
        &self,
        runtime: &Arc<ClientRuntime>,
        offset_store: &Arc<dyn OffsetStore>,
        queue: &MessageQueue,
    ) -> Result<i64, PushConsumerError> {
        let stored = offset_store
            .read_offset(queue, ReadOffsetType::ReadFromStore)
            .await?;
        let retry_topic = queue
            .topic()
            .is_some_and(|topic| topic.starts_with("%RETRY%"));
        let consume_timestamp = self.config.consume_timestamp();
        let plan = initial_offset_plan(
            self.config.consume_from_where(),
            retry_topic,
            stored,
            &consume_timestamp,
        )?;
        let offset = match plan {
            InitialOffsetPlan::Stored(offset) | InitialOffsetPlan::Fixed(offset) => offset,
            InitialOffsetPlan::MaxOffset => self.query_max_offset(runtime, queue).await?,
            InitialOffsetPlan::SearchTimestamp(timestamp) => {
                self.query_search_offset(runtime, queue, timestamp).await?
            }
        };
        if offset < 0 {
            return Err(PushConsumerError::InvalidOffset);
        }
        Ok(offset)
    }

    async fn query_max_offset(
        &self,
        runtime: &Arc<ClientRuntime>,
        queue: &MessageQueue,
    ) -> Result<i64, PushConsumerError> {
        let topic = queue.topic().ok_or(PushConsumerError::InvalidQueue)?;
        let broker_name = queue.broker_name().ok_or(PushConsumerError::InvalidQueue)?;
        let deadline = self.offset_rpc_deadline();
        let address = self
            .find_offset_broker(runtime, topic, broker_name, deadline)
            .await?;
        let mut header = GetMaxOffsetRequestHeader::default();
        header.set_topic(Some(topic.to_owned()));
        header.set_queue_id(Some(queue.queue_id()));
        header.set_committed(true);
        header.set_broker_name(Some(broker_name.to_owned()));
        Ok(runtime
            .api()
            .get_max_offset(&address, &header, deadline)
            .await?)
    }

    async fn query_search_offset(
        &self,
        runtime: &Arc<ClientRuntime>,
        queue: &MessageQueue,
        timestamp: i64,
    ) -> Result<i64, PushConsumerError> {
        let topic = queue.topic().ok_or(PushConsumerError::InvalidQueue)?;
        let broker_name = queue.broker_name().ok_or(PushConsumerError::InvalidQueue)?;
        let deadline = self.offset_rpc_deadline();
        let address = self
            .find_offset_broker(runtime, topic, broker_name, deadline)
            .await?;
        let mut header = SearchOffsetRequestHeader::default();
        header.set_topic(Some(topic.to_owned()));
        header.set_lite_topic(None);
        header.set_queue_id(Some(queue.queue_id()));
        header.set_timestamp(Some(timestamp));
        header.set_boundary_type(Some(BoundaryType::Lower));
        header.set_broker_name(Some(broker_name.to_owned()));
        Ok(runtime
            .api()
            .search_offset(&address, &header, deadline)
            .await?)
    }

    async fn find_offset_broker(
        &self,
        runtime: &Arc<ClientRuntime>,
        topic: &str,
        broker_name: &str,
        deadline: Instant,
    ) -> Result<String, PushConsumerError> {
        let mut route = runtime.route_snapshot()?;
        let mut address = route.publish_address(broker_name).map(str::to_owned);
        if address.is_none() {
            runtime.update_topic_route(topic, deadline).await?;
            route = runtime.route_snapshot()?;
            address = route.publish_address(broker_name).map(str::to_owned);
        }
        address.ok_or_else(|| OffsetStoreError::BrokerNotFound(broker_name.to_owned()).into())
    }

    pub(super) async fn prepare_admin_queue(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<(Arc<ClientRuntime>, MessageQueue, String, String, Instant), PushConsumerError>
    {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let queue = self
            .config
            .client_config()
            .clone()
            .queue_with_namespace(input_queue)?;
        let topic = queue
            .topic()
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidQueue)?;
        let broker_name = queue
            .broker_name()
            .filter(|broker| !broker.is_empty())
            .ok_or(PushConsumerError::InvalidQueue)?
            .to_owned();
        let deadline = self.offset_rpc_deadline();
        let address = self
            .find_offset_broker(&context.runtime, topic, &broker_name, deadline)
            .await?;
        Ok((context.runtime, queue, broker_name, address, deadline))
    }

    async fn request_orderly_lock_batch(
        &self,
        runtime: &Arc<ClientRuntime>,
        broker_name: &str,
        topic: &str,
        body: &LockBatchRequestBody,
    ) -> Result<Option<HashSet<Option<MessageQueue>>>, String> {
        #[cfg(test)]
        {
            let (waiter, result, requested) = {
                let mut scripted = self
                    .scripted_lock_transport
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                scripted.lock_bodies.push(body.clone());
                (
                    scripted.lock_waiters.pop_front(),
                    scripted.lock_results.pop_front(),
                    scripted.lock_requested.clone(),
                )
            };
            requested.notify_one();
            if let Some(waiter) = waiter {
                return waiter
                    .await
                    .map_err(|_| "scripted lock response sender dropped".to_owned())?;
            }
            if let Some(result) = result {
                return result;
            }
        }

        let deadline = Instant::now() + Duration::from_millis(ORDERLY_LOCK_RPC_TIMEOUT_MILLIS);
        let address = self
            .find_offset_broker(runtime, topic, broker_name, deadline)
            .await
            .map_err(|error| error.to_string())?;
        let address =
            broker_vip_channel(self.config.client_config().vip_channel_enabled(), &address)
                .map_err(|error| error.to_string())?;
        runtime
            .api()
            .lock_batch_mq(&address, body, deadline)
            .await
            .map_err(|error| error.to_string())
    }

    async fn request_orderly_unlock_batch(
        &self,
        runtime: &Arc<ClientRuntime>,
        broker_name: &str,
        topic: &str,
        body: &UnlockBatchRequestBody,
        oneway: bool,
    ) -> Result<(), String> {
        #[cfg(test)]
        {
            let mut scripted = self
                .scripted_lock_transport
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            scripted.unlock_bodies.push(body.clone());
            if let Some(result) = scripted.unlock_results.pop_front() {
                return result;
            }
        }

        let deadline = Instant::now() + Duration::from_millis(ORDERLY_LOCK_RPC_TIMEOUT_MILLIS);
        let address = self
            .find_offset_broker(runtime, topic, broker_name, deadline)
            .await
            .map_err(|error| error.to_string())?;
        let address =
            broker_vip_channel(self.config.client_config().vip_channel_enabled(), &address)
                .map_err(|error| error.to_string())?;
        runtime
            .api()
            .unlock_batch_mq(&address, body, oneway, deadline)
            .await
            .map_err(|error| error.to_string())
    }

    fn orderly_group_fields(
        &self,
        runtime: &ClientRuntime,
    ) -> Result<(String, String), PushConsumerError> {
        let group = self.consume_stats_group();
        if group.is_empty() {
            return Err(PushConsumerError::InvalidConsumerGroup);
        }
        Ok((group, runtime.client_id().to_owned()))
    }

    pub(super) async fn lock_orderly_queues_raw(
        &self,
        runtime: &Arc<ClientRuntime>,
        queues: &[MessageQueue],
    ) -> OrderlyLockBatchResult {
        let mut result = OrderlyLockBatchResult::default();
        let mut groups = BTreeMap::<String, (String, HashSet<Option<MessageQueue>>)>::new();
        for queue in queues {
            let (Some(broker), Some(topic)) = (queue.broker_name(), queue.topic()) else {
                continue;
            };
            let group = groups
                .entry(broker.to_owned())
                .or_insert_with(|| (topic.to_owned(), HashSet::new()));
            group.1.insert(Some(queue.clone()));
        }
        let (consumer_group, client_id) = match self.orderly_group_fields(runtime) {
            Ok(fields) => fields,
            Err(error) => {
                tracing::error!(error = %error, "cannot build orderly lock request identity");
                result.uncertain.extend(queues.iter().cloned());
                return result;
            }
        };

        for (broker_name, (topic, requested)) in groups {
            let requested_queues = requested
                .iter()
                .filter_map(|queue| queue.as_ref().cloned())
                .collect::<Vec<_>>();
            let mut body = LockBatchRequestBody::default();
            body.set_consumer_group(Some(consumer_group.clone()));
            body.set_client_id(Some(client_id.clone()));
            body.set_only_this_broker(false);
            body.set_mq_set(Some(SharedValue::new(
                requested
                    .iter()
                    .cloned()
                    .map(|queue| queue.map(SharedValue::new))
                    .collect(),
            )));
            match self
                .request_orderly_lock_batch(runtime, &broker_name, &topic, &body)
                .await
            {
                Ok(Some(locked_queues)) => {
                    result.responded.extend(requested_queues.iter().cloned());
                    let received_at = Instant::now();
                    let received_at_millis = epoch_millis();
                    for queue in requested_queues {
                        if locked_queues.contains(&Some(queue.clone())) {
                            result.locked.insert(queue.clone());
                            result
                                .lock_times
                                .insert(queue, (received_at_millis, received_at));
                        }
                    }
                }
                // Java's LockBatchResponseBody getter is nullable. Its caller
                // treats a null set as an exceptional response and preserves
                // the previous ProcessQueue lock state.
                Ok(None) => {}
                Err(error) => {
                    result.uncertain.extend(requested_queues);
                    tracing::warn!(
                        broker = %broker_name,
                        error,
                        "orderly batch lock request failed"
                    );
                }
            }
        }
        result
    }

    pub(super) async fn unlock_orderly_queues_raw(
        &self,
        runtime: &Arc<ClientRuntime>,
        queues: &[MessageQueue],
        oneway: bool,
    ) -> HashSet<MessageQueue> {
        let mut groups = BTreeMap::<String, (String, HashSet<Option<MessageQueue>>)>::new();
        for queue in queues {
            let (Some(broker), Some(topic)) = (queue.broker_name(), queue.topic()) else {
                continue;
            };
            let group = groups
                .entry(broker.to_owned())
                .or_insert_with(|| (topic.to_owned(), HashSet::new()));
            group.1.insert(Some(queue.clone()));
        }
        let (consumer_group, client_id) = match self.orderly_group_fields(runtime) {
            Ok(fields) => fields,
            Err(error) => {
                tracing::error!(error = %error, "cannot build orderly unlock request identity");
                return HashSet::new();
            }
        };

        let mut unlocked = HashSet::new();
        for (broker_name, (topic, requested)) in groups {
            let mut body = UnlockBatchRequestBody::default();
            body.set_consumer_group(Some(consumer_group.clone()));
            body.set_client_id(Some(client_id.clone()));
            body.set_only_this_broker(false);
            body.set_mq_set(Some(SharedValue::new(
                requested
                    .iter()
                    .cloned()
                    .map(|queue| queue.map(SharedValue::new))
                    .collect(),
            )));
            match self
                .request_orderly_unlock_batch(runtime, &broker_name, &topic, &body, oneway)
                .await
            {
                Ok(()) => {
                    unlocked.extend(requested.into_iter().flatten());
                }
                Err(error) => {
                    tracing::warn!(
                        broker = %broker_name,
                        error,
                        "orderly batch unlock request failed"
                    );
                }
            }
        }
        unlocked
    }

    pub(super) async fn lock_orderly_process_queues(
        &self,
        runtime: &Arc<ClientRuntime>,
        process_queues: &[Arc<ProcessQueue>],
    ) -> HashSet<MessageQueue> {
        let mut attempts = Vec::new();
        for process_queue in process_queues {
            let generation = process_queue.generation();
            if !process_queue
                .snapshot(generation)
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                continue;
            }
            if let Ok(attempt_id) = process_queue.begin_lock_attempt(generation) {
                attempts.push((
                    process_queue.queue().clone(),
                    process_queue.clone(),
                    generation,
                    attempt_id,
                ));
            }
        }
        let queues = attempts
            .iter()
            .map(|(queue, _, _, _)| queue.clone())
            .collect::<Vec<_>>();
        let lock_result = self.lock_orderly_queues_raw(runtime, &queues).await;
        let mut applied_locked = HashSet::new();
        let mut stale_locked = Vec::new();
        for (queue, process_queue, generation, attempt_id) in attempts {
            if !lock_result.responded.contains(&queue) {
                continue;
            }
            let returned_locked = lock_result.locked.contains(&queue);
            let applied = if returned_locked {
                let Some((timestamp_millis, lock_instant)) =
                    lock_result.lock_times.get(&queue).copied()
                else {
                    continue;
                };
                process_queue.complete_lock_attempt_at(
                    generation,
                    attempt_id,
                    true,
                    timestamp_millis,
                    lock_instant,
                )
            } else {
                process_queue.complete_lock_attempt(generation, attempt_id, false)
            };
            match applied {
                Ok(LockAttemptResult::Applied) if returned_locked => {
                    applied_locked.insert(queue);
                }
                Ok(LockAttemptResult::Dropped) if returned_locked => {
                    stale_locked.push((queue, process_queue, generation));
                }
                Ok(LockAttemptResult::Applied | LockAttemptResult::Superseded)
                | Ok(LockAttemptResult::Dropped)
                | Err(_) => {}
            }
        }
        if !stale_locked.is_empty() {
            let queues = stale_locked
                .iter()
                .map(|(queue, _, _)| queue.clone())
                .collect::<Vec<_>>();
            let unlocked = self
                .unlock_orderly_queues_raw(runtime, &queues, false)
                .await;
            for (queue, process_queue, generation) in stale_locked {
                if unlocked.contains(&queue) {
                    let _ = process_queue.record_unlocked(generation);
                    write_lock(&self.pending_orderly_unlocks).remove(&queue);
                } else {
                    write_lock(&self.pending_orderly_unlocks).insert(queue);
                }
            }
        }
        applied_locked
    }

    async fn unlock_orderly_queues_tracking(
        &self,
        runtime: &Arc<ClientRuntime>,
        queues: &[MessageQueue],
    ) -> bool {
        if queues.is_empty() {
            return true;
        }
        write_lock(&self.pending_orderly_unlocks).extend(queues.iter().cloned());
        let unlocked = self.unlock_orderly_queues_raw(runtime, queues, false).await;
        let mut pending = write_lock(&self.pending_orderly_unlocks);
        for queue in &unlocked {
            pending.remove(queue);
        }
        queues.iter().all(|queue| unlocked.contains(queue))
    }

    pub(super) async fn unlock_orderly_process_queues(
        &self,
        runtime: &Arc<ClientRuntime>,
        process_queues: &[Arc<ProcessQueue>],
    ) -> bool {
        let mut targets = process_queues
            .iter()
            .map(|process_queue| (process_queue.queue().clone(), Some(process_queue.clone())))
            .collect::<HashMap<_, _>>();
        {
            let pending = read_lock(&self.pending_orderly_unlocks);
            for queue in pending.iter() {
                targets.entry(queue.clone()).or_insert(None);
            }
        }
        let queues = targets.keys().cloned().collect::<Vec<_>>();
        if queues.is_empty() {
            return true;
        }
        write_lock(&self.pending_orderly_unlocks).extend(queues.iter().cloned());
        let unlocked = self
            .unlock_orderly_queues_raw(runtime, &queues, false)
            .await;
        for (queue, process_queue) in targets {
            if unlocked.contains(&queue) {
                if let Some(process_queue) = process_queue {
                    let _ = process_queue.record_unlocked(process_queue.generation());
                }
                write_lock(&self.pending_orderly_unlocks).remove(&queue);
            } else if let Some(process_queue) = process_queue {
                let _ = process_queue.increment_try_unlock_times(process_queue.generation());
            }
        }
        queues.iter().all(|queue| unlocked.contains(queue))
    }

    async fn retry_pending_orderly_unlocks(&self, runtime: &Arc<ClientRuntime>) {
        let queues = read_lock(&self.pending_orderly_unlocks)
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let unlocked = self
            .unlock_orderly_queues_raw(runtime, &queues, false)
            .await;
        let mut pending = write_lock(&self.pending_orderly_unlocks);
        for queue in unlocked {
            pending.remove(&queue);
        }
    }

    pub(crate) async fn renew_orderly_locks(self: &Arc<Self>) {
        let Some(context) = self.pull_context().await else {
            return;
        };
        if self.current_orderly_lock_settings().await.is_none()
            || self.config.message_model()
                != rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
        {
            return;
        }
        let process_queues = read_lock(&self.process_queues)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        self.lock_orderly_process_queues(&context.runtime, &process_queues)
            .await;
        self.retry_pending_orderly_unlocks(&context.runtime).await;
    }

    fn offset_rpc_deadline(&self) -> Instant {
        Instant::now()
            + Duration::from_millis(
                self.config.client_config().mq_client_api_timeout().max(0) as u64
            )
    }

    pub(super) async fn notify_assignment_changed(
        &self,
        runtime: Option<&Arc<ClientRuntime>>,
        assignment: &DesiredAssignment,
    ) -> bool {
        if let Some(subscription) = write_lock(&self.subscriptions).get_mut(&assignment.topic) {
            subscription.set_sub_version(epoch_millis());
        }
        self.update_pull_thresholds_after_rebalance();
        let Some(runtime) = runtime else {
            return false;
        };
        let mut successful = true;
        if let Err(error) = runtime.send_heartbeat_to_all_brokers().await {
            tracing::warn!(topic = %assignment.topic, error = %error, "push subscription heartbeat after rebalance failed");
            successful = false;
        }
        if let Some(listener) = self.config.message_queue_listener() {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                listener(
                    &assignment.topic,
                    assignment.all_queues.as_ref(),
                    &assignment.assigned_queues,
                );
            }));
            if outcome.is_err() {
                tracing::warn!(topic = %assignment.topic, "push message queue listener panicked");
                successful = false;
            }
        }
        successful
    }
}
