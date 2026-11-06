use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::{Arc, Weak},
    time::{Duration, Instant as StdInstant, SystemTime, UNIX_EPOCH},
};

use rocketmq_classic_common::{
    message::{Message, MessageQueue, constants::*, encode_nullable_property_entries},
    sysflag::message,
};
use rocketmq_classic_core::{
    api::producer::{SendMode, SendResult, SendStatus},
    error::RpcError,
    route::{TopicSnapshot, broker_vip_channel},
    runtime::ClientRuntime,
};
use rocketmq_classic_protocol::header::SendMessageRequestHeaderV1;
use tokio::time::Instant;

use crate::{
    AsyncSenderTask, Producer, ProducerError,
    accumulator::{BatchKey, BatchLimits, EnqueueResult, PendingBatchStatus, ReadyBatch},
    backpressure::AdjustablePermit,
    batch::encode_prepared_batch,
    hooks::{CheckForbiddenContext, SendMessageContext, SendMessageType},
    validation,
};

#[cfg(test)]
use crate::producer::ScriptedSendAttempt;

struct PreparedMessage {
    topic: String,
    body: Vec<u8>,
    properties: Option<String>,
    unique_id: String,
    born_timestamp: i64,
    flag: i32,
    sys_flag: i32,
    reconsume_times: Option<i32>,
    max_reconsume_times: Option<i32>,
    batch: bool,
    is_reply: bool,
}

struct SendAttempt<'a> {
    runtime: &'a ClientRuntime,
    queue: &'a MessageQueue,
    broker: &'a str,
    address: &'a str,
    prepared: &'a PreparedMessage,
    mode: SendMode,
    deadline: Instant,
}

type AsyncBackpressurePermit = (AdjustablePermit, AdjustablePermit);

struct DefaultSendRequest<'a> {
    runtime: &'a Arc<ClientRuntime>,
    message: &'a mut Message,
    wire_message: Message,
    batch: bool,
    mode: SendMode,
    deadline: Instant,
    backpressure: Option<AsyncBackpressurePermit>,
    backpressure_enabled: bool,
}

fn selector_panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&'static str>()
                .map(|message| (*message).to_owned())
        })
        .unwrap_or_else(|| "non-string selector panic payload".to_owned())
}

fn batch_initiation_failure(error: ProducerError) -> ProducerError {
    ProducerError::BatchInitiationFailed(Box::new(error))
}

fn client_retries_exhausted(
    error: ProducerError,
    attempts: usize,
    started: StdInstant,
    topic: &str,
    brokers_sent: &[Option<String>],
) -> ProducerError {
    let brokers = brokers_sent
        .iter()
        .map(|broker| broker.as_deref().unwrap_or("null"))
        .collect::<Vec<_>>()
        .join(", ");
    let message = format!(
        "Send [{attempts}] times, still failed, cost [{}]ms, Topic: {topic}, BrokersSent: [{brokers}]\nSee https://rocketmq.apache.org/docs/faq/ for further details.",
        started.elapsed().as_millis()
    );
    ProducerError::SendRetriesExhausted {
        message,
        response_code: 10003,
        source: Box::new(error),
    }
}

impl Producer {
    async fn acquire_async_backpressure(
        &self,
        message_size: usize,
        deadline: Instant,
    ) -> Result<(bool, Option<AsyncBackpressurePermit>), ProducerError> {
        let enabled = self.enable_backpressure_for_async_mode();
        if !enabled {
            return Ok((false, None));
        }
        let count = self.backpressure_send_num.acquire(1, deadline).await?;
        let bytes = self
            .backpressure_send_size
            .acquire(message_size.max(1), deadline)
            .await?;
        Ok((true, Some((count, bytes))))
    }

    /// Refreshes the topic route from NameServer and returns writable queues,
    /// matching Java's `fetchPublishMessageQueues` behavior.
    pub async fn fetch_publish_message_queues(
        &self,
        topic: &str,
    ) -> Result<Vec<MessageQueue>, ProducerError> {
        let runtime = self.started_runtime().await?;
        let topic = self.topic_with_namespace(topic)?;
        self.remember_publish_topic(&topic);
        let timeout = Duration::from_millis(
            self.config.client_config().mq_client_api_timeout().max(0) as u64,
        );
        let deadline = deadline_after(timeout);
        let updated = runtime.update_topic_route(&topic, deadline).await?;
        if !updated {
            return Err(ProducerError::NoRoute(topic.to_owned()));
        }
        let Some(info) = self.current_publish_info(&runtime, &topic)? else {
            return Err(ProducerError::NoRoute(topic.to_owned()));
        };
        if info.publish_queues.is_empty() {
            return Err(ProducerError::NoRoute(topic.to_owned()));
        }
        info.publish_queues
            .iter()
            .map(|queue| self.queue_without_namespace(queue))
            .collect()
    }

    /// Sync-mode send. The returned future waits for the Java sync-mode result
    /// and uses the configured synchronous retry budget.
    pub async fn send(&self, message: &mut Message) -> Result<SendResult, ProducerError> {
        self.send_auto_or_direct(message, None, SendMode::Sync)
            .await
    }

    pub async fn send_with_timeout(
        &self,
        message: &mut Message,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_default(message, false, SendMode::Sync, timeout)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    /// Sends one message directly, bypassing the producer auto-batch
    /// accumulator, like Java's `DefaultMQProducer.sendDirect` helper.
    pub async fn send_direct(&self, message: &mut Message) -> Result<SendResult, ProducerError> {
        self.send_direct_with_timeout(
            message,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_direct_with_timeout(
        &self,
        message: &mut Message,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_message_direct_with_mode(message, None, SendMode::Sync, timeout)
            .await
    }

    /// Future-mode counterpart to [`Self::send_direct`].
    pub async fn send_direct_future(
        &self,
        message: &mut Message,
    ) -> Result<SendResult, ProducerError> {
        self.send_direct_future_with_timeout(
            message,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_direct_future_with_timeout(
        &self,
        message: &mut Message,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_message_direct_with_mode(message, None, SendMode::Future, timeout)
            .await
    }

    /// Future-mode send. Like Java's callback send, it retries asynchronous
    /// failures up to the configured budget and resolves with the terminal result.
    pub async fn send_future(&self, message: &mut Message) -> Result<SendResult, ProducerError> {
        self.send_auto_or_direct(message, None, SendMode::Future)
            .await
    }

    pub async fn send_future_with_timeout(
        &self,
        message: &mut Message,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_default(message, false, SendMode::Future, timeout)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    pub async fn send_oneway(&self, message: &mut Message) -> Result<(), ProducerError> {
        self.send_oneway_with_timeout(
            message,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_oneway_with_timeout(
        &self,
        message: &mut Message,
        timeout: Duration,
    ) -> Result<(), ProducerError> {
        self.send_default(message, false, SendMode::Oneway, timeout)
            .await
            .map(|_| ())
    }

    /// Sends to a caller-selected queue using its Broker address from the
    /// current route. This overload makes one Broker attempt.
    pub async fn send_to_queue(
        &self,
        message: &mut Message,
        queue: &MessageQueue,
    ) -> Result<SendResult, ProducerError> {
        self.send_auto_or_direct(message, Some(queue.clone()), SendMode::Sync)
            .await
    }

    pub async fn send_to_queue_with_timeout(
        &self,
        message: &mut Message,
        queue: &MessageQueue,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_to_queue_mode(message, queue, false, SendMode::Sync, timeout)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    /// Sends directly to a selected queue, bypassing auto-batching.
    pub async fn send_to_queue_direct(
        &self,
        message: &mut Message,
        queue: &MessageQueue,
    ) -> Result<SendResult, ProducerError> {
        self.send_to_queue_direct_with_timeout(
            message,
            queue,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_to_queue_direct_with_timeout(
        &self,
        message: &mut Message,
        queue: &MessageQueue,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_message_direct_with_mode(message, Some(queue), SendMode::Sync, timeout)
            .await
    }

    /// Future-mode direct send to a selected queue.
    pub async fn send_to_queue_direct_future(
        &self,
        message: &mut Message,
        queue: &MessageQueue,
    ) -> Result<SendResult, ProducerError> {
        self.send_to_queue_direct_future_with_timeout(
            message,
            queue,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_to_queue_direct_future_with_timeout(
        &self,
        message: &mut Message,
        queue: &MessageQueue,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_message_direct_with_mode(message, Some(queue), SendMode::Future, timeout)
            .await
    }

    /// Sends one message through the accumulator even when `auto_batch` is
    /// disabled. Ineligible or over-capacity messages follow the direct path.
    pub async fn send_by_accumulator(
        &self,
        message: &mut Message,
    ) -> Result<SendResult, ProducerError> {
        self.send_accumulator_or_direct(message, None, SendMode::Sync)
            .await
    }

    /// Future-mode counterpart to [`Self::send_by_accumulator`].
    pub async fn send_by_accumulator_future(
        &self,
        message: &mut Message,
    ) -> Result<SendResult, ProducerError> {
        self.send_accumulator_or_direct(message, None, SendMode::Future)
            .await
    }

    /// Sends one message to a selected queue through the accumulator even when
    /// `auto_batch` is disabled.
    pub async fn send_to_queue_by_accumulator(
        &self,
        message: &mut Message,
        queue: &MessageQueue,
    ) -> Result<SendResult, ProducerError> {
        self.send_accumulator_or_direct(message, Some(queue.clone()), SendMode::Sync)
            .await
    }

    /// Future-mode counterpart to [`Self::send_to_queue_by_accumulator`].
    pub async fn send_to_queue_by_accumulator_future(
        &self,
        message: &mut Message,
        queue: &MessageQueue,
    ) -> Result<SendResult, ProducerError> {
        self.send_accumulator_or_direct(message, Some(queue.clone()), SendMode::Future)
            .await
    }

    /// Future-mode counterpart to Java's explicit-queue callback overload.
    /// Failed attempts retry the selected Broker up to the configured budget.
    pub async fn send_to_queue_future(
        &self,
        message: &mut Message,
        queue: &MessageQueue,
    ) -> Result<SendResult, ProducerError> {
        self.send_auto_or_direct(message, Some(queue.clone()), SendMode::Future)
            .await
    }

    pub async fn send_to_queue_future_with_timeout(
        &self,
        message: &mut Message,
        queue: &MessageQueue,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_to_queue_mode(message, queue, false, SendMode::Future, timeout)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    pub async fn send_to_queue_oneway(
        &self,
        message: &mut Message,
        queue: &MessageQueue,
    ) -> Result<(), ProducerError> {
        self.send_to_queue_mode(
            message,
            queue,
            false,
            SendMode::Oneway,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
        .map(|_| ())
    }

    /// Invokes a queue selector for the current writable route without sending.
    /// The selector receives unnamespaced queues and a message view with its
    /// namespace removed. The returned queue is namespaced for use with this
    /// producer. Values captured by the closure serve the role of Java's
    /// selector `arg`.
    pub async fn invoke_message_queue_selector<F>(
        &self,
        message: &Message,
        selector: F,
        timeout: Duration,
    ) -> Result<MessageQueue, ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send,
    {
        let started = Instant::now();
        let deadline = deadline_after(timeout);
        let runtime = self.started_runtime().await?;
        let wire_message = self.message_with_namespace(message)?;
        validation::validate_message(Some(&wire_message), &self.config)?;
        let info = self
            .acquire_publish_info(&runtime, wire_message.topic(), deadline)
            .await?;
        let selected = self.select_queue_with_selector(&info, message, selector)?;
        if Instant::now().duration_since(started) > timeout {
            return Err(ProducerError::Timeout);
        }
        selected
            .map(|(_, queue)| queue)
            .ok_or(ProducerError::NoQueueSelected)
    }

    /// Runs a custom queue selector against the current writable route and
    /// sends to the returned queue when its Broker address is known.
    pub async fn send_with_selector<F>(
        &self,
        message: &mut Message,
        selector: F,
    ) -> Result<SendResult, ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send,
    {
        self.send_with_selector_mode(
            message,
            selector,
            SendMode::Sync,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
            true,
        )
        .await?
        .ok_or(ProducerError::Timeout)
    }

    pub async fn send_with_selector_with_timeout<F>(
        &self,
        message: &mut Message,
        selector: F,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send,
    {
        self.send_with_selector_mode(message, selector, SendMode::Sync, timeout, false)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    pub async fn send_future_with_selector<F>(
        &self,
        message: &mut Message,
        selector: F,
    ) -> Result<SendResult, ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send,
    {
        self.send_with_selector_mode(
            message,
            selector,
            SendMode::Future,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
            true,
        )
        .await?
        .ok_or(ProducerError::Timeout)
    }

    pub async fn send_future_with_selector_with_timeout<F>(
        &self,
        message: &mut Message,
        selector: F,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send,
    {
        self.send_with_selector_mode(message, selector, SendMode::Future, timeout, false)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    pub async fn send_oneway_with_selector<F>(
        &self,
        message: &mut Message,
        selector: F,
    ) -> Result<(), ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send,
    {
        self.send_with_selector_mode(
            message,
            selector,
            SendMode::Oneway,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
            false,
        )
        .await
        .map(|_| ())
    }

    async fn send_with_selector_mode<F>(
        &self,
        message: &mut Message,
        selector: F,
        mode: SendMode,
        timeout: Duration,
        auto_batch_enabled: bool,
    ) -> Result<Option<SendResult>, ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send,
    {
        let deadline = deadline_after(timeout);
        let wire_message = self.message_with_namespace(message)?;
        let use_auto_batch = auto_batch_enabled && mode != SendMode::Oneway && self.auto_batch();
        let (backpressure_enabled, backpressure) = if mode == SendMode::Future && !use_auto_batch {
            let message_body_length = wire_message.body().len();
            self.acquire_async_backpressure(message_body_length, deadline)
                .await?
        } else {
            (false, None)
        };
        let runtime = self.started_runtime().await?;
        validation::validate_message(Some(&wire_message), &self.config)?;
        let info = self
            .acquire_publish_info(&runtime, wire_message.topic(), deadline)
            .await?;
        let (logical_queue, mut queue) = self
            .select_queue_with_selector(&info, message, selector)?
            .ok_or(ProducerError::NoQueueSelected)?;
        if use_auto_batch {
            return self
                .send_auto_or_direct(message, Some(logical_queue), mode)
                .await
                .map(Some);
        }
        let broker = queue
            .broker_name()
            .ok_or_else(|| ProducerError::NoRoute(wire_message.topic().to_owned()))?
            .to_owned();
        let address = self.broker_address_or_error(&runtime, &info, &queue)?;
        if mode == SendMode::Future {
            let mut hooked_message = wire_message;
            return self
                .send_future_attempts(
                    runtime.clone(),
                    &mut queue,
                    &broker,
                    &address,
                    &mut hooked_message,
                    false,
                    None,
                    deadline,
                    backpressure,
                    backpressure_enabled,
                    false,
                )
                .await;
        }
        let mut hooked_message = wire_message;
        let mut check_forbidden_rejected = false;
        self.send_one_attempt_with_hooks(
            &runtime,
            &mut queue,
            &broker,
            &address,
            &mut hooked_message,
            false,
            mode,
            deadline,
            &mut check_forbidden_rejected,
        )
        .await
    }

    async fn send_auto_or_direct(
        &self,
        message: &mut Message,
        queue: Option<MessageQueue>,
        mode: SendMode,
    ) -> Result<SendResult, ProducerError> {
        self.send_with_accumulator_policy(message, queue, mode, false)
            .await
    }

    async fn send_accumulator_or_direct(
        &self,
        message: &mut Message,
        queue: Option<MessageQueue>,
        mode: SendMode,
    ) -> Result<SendResult, ProducerError> {
        self.send_with_accumulator_policy(message, queue, mode, true)
            .await
    }

    async fn send_with_accumulator_policy(
        &self,
        message: &mut Message,
        queue: Option<MessageQueue>,
        mode: SendMode,
        force_accumulate: bool,
    ) -> Result<SendResult, ProducerError> {
        let wire_message = self.message_with_namespace(message)?;
        let timeout = Duration::from_millis(self.config.send_timeout_millis().max(0) as u64);
        if mode == SendMode::Oneway
            || (!force_accumulate && !self.auto_batch())
            || !can_accumulate(&wire_message)
        {
            return self
                .send_message_direct_with_mode(message, queue.as_ref(), mode, timeout)
                .await;
        }

        validation::validate_message(Some(&wire_message), &self.config)?;
        let mut accumulated_message = wire_message.clone();
        self.set_unique_id_on_input_and_copy_to_wire(message, &mut accumulated_message);

        // Java's sendByAccumulator validates and writes UNIQ_KEY on the
        // caller's Message before handing it to the accumulator. Preserve that
        // mutation even when dispatch subsequently fails because the producer
        // has not been started.
        self.started_runtime().await?;
        let accumulator_settings_changed = self.subscribe_accumulator_settings();
        let settings = self.effective_accumulator_settings();
        let producer_owner = self
            .self_weak
            .get()
            .and_then(Weak::upgrade)
            .ok_or(ProducerError::NotStarted)?;
        let queued = self
            .auto_batch_engine()
            .enqueue_owned(
                accumulated_message,
                queue.clone(),
                mode,
                BatchLimits {
                    max_delay: Duration::from_millis(settings.max_delay_millis as u64),
                    max_bytes: settings.max_bytes,
                    total_max_bytes: settings.total_max_bytes,
                },
                producer_owner,
            )
            .await?;

        match queued {
            EnqueueResult::Bypass => {
                self.send_message_direct_with_mode(message, queue.as_ref(), mode, timeout)
                    .await
            }
            EnqueueResult::Queued {
                receiver,
                key,
                generation,
                flush_at,
                schedule_timer,
            } => {
                if schedule_timer {
                    self.schedule_auto_batch_timer(
                        key,
                        generation,
                        flush_at,
                        accumulator_settings_changed,
                    )
                    .await?;
                }
                receive_auto_batch_result(receiver).await
            }
            EnqueueResult::Ready { receiver, batch } => {
                self.schedule_auto_batch_flush(batch).await?;
                receive_auto_batch_result(receiver).await
            }
        }
    }

    async fn send_message_direct_with_mode(
        &self,
        message: &mut Message,
        queue: Option<&MessageQueue>,
        mode: SendMode,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        let result = if let Some(queue) = queue {
            self.send_to_queue_mode(message, queue, false, mode, timeout)
                .await?
        } else {
            self.send_default(message, false, mode, timeout).await?
        };
        result.ok_or(ProducerError::Timeout)
    }

    pub(crate) async fn resume_auto_batch_tasks(&self) -> Result<(), ProducerError> {
        if !self.auto_batch_engine().start().await {
            return Ok(());
        }
        let settings = self.effective_accumulator_settings();
        let pending = self
            .auto_batch_engine()
            .pending_timers(Duration::from_millis(settings.max_delay_millis as u64))
            .await;
        let settings_changed = self.subscribe_accumulator_settings();
        for (key, generation, flush_at) in pending {
            self.schedule_auto_batch_timer(key, generation, flush_at, settings_changed.clone())
                .await?;
        }
        Ok(())
    }

    async fn schedule_auto_batch_timer(
        &self,
        key: BatchKey,
        generation: u64,
        flush_at: Instant,
        mut settings_changed: tokio::sync::watch::Receiver<u64>,
    ) -> Result<(), ProducerError> {
        let producer = self
            .self_weak
            .get()
            .cloned()
            .ok_or(ProducerError::NotStarted)?;
        self.track_auto_batch_task(async move {
            let mut next_flush = flush_at;
            loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(next_flush) => {}
                    changed = settings_changed.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
                let Some(producer) = producer.upgrade() else {
                    return;
                };
                if !producer.auto_batch_engine().is_running() {
                    return;
                }
                let settings = producer.effective_accumulator_settings();
                match producer
                    .auto_batch_engine()
                    .recheck(
                        &key,
                        generation,
                        Duration::from_millis(settings.max_delay_millis as u64),
                        settings.max_bytes,
                    )
                    .await
                {
                    PendingBatchStatus::Missing => return,
                    PendingBatchStatus::Ready(batch) => {
                        let _ = producer.schedule_auto_batch_flush(batch).await;
                        return;
                    }
                    PendingBatchStatus::WaitUntil(deadline) => next_flush = deadline,
                }
            }
        })
        .await;
        Ok(())
    }

    async fn schedule_auto_batch_flush(&self, batch: ReadyBatch) -> Result<(), ProducerError> {
        let producer = batch
            .owner
            .as_ref()
            .cloned()
            .or_else(|| self.self_weak.get().and_then(Weak::upgrade))
            .ok_or(ProducerError::NotStarted)?;
        self.track_auto_batch_task(async move {
            producer.flush_auto_batch(batch).await;
        })
        .await;
        Ok(())
    }

    async fn track_auto_batch_task(&self, task: impl Future<Output = ()> + Send + 'static) {
        let mut tasks = self.auto_batch_tasks.lock().await;
        if self.auto_batch_engine().is_closed() {
            return;
        }
        let handle = tokio::spawn(task);
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }

    async fn flush_auto_batch(&self, accumulated: ReadyBatch) {
        let count = accumulated.messages.len();
        let result = self
            .send_accumulated_batch(&accumulated)
            .await
            .and_then(|result| split_auto_batch_result(result, count));
        self.auto_batch_engine()
            .release(accumulated.body_bytes)
            .await;

        match result {
            Ok(results) if results.len() == accumulated.waiters.len() => {
                for (waiter, result) in accumulated.waiters.into_iter().zip(results) {
                    let _ = waiter.send(Ok(result));
                }
            }
            Ok(_) => {
                for waiter in accumulated.waiters {
                    let _ = waiter.send(Err(ProducerError::InvalidAutoBatchResult.to_string()));
                }
            }
            Err(error) => {
                let message = error.to_string();
                for waiter in accumulated.waiters {
                    let _ = waiter.send(Err(message.clone()));
                }
            }
        }
    }

    async fn send_accumulated_batch(
        &self,
        accumulated: &ReadyBatch,
    ) -> Result<SendResult, ProducerError> {
        let mut batch = encode_prepared_batch(&accumulated.messages, &self.config, &self.ids)?;
        let keys = accumulated
            .messages
            .iter()
            .filter_map(Message::keys)
            .flat_map(|keys| {
                keys.split(KEY_SEPARATOR)
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .filter(|key| !key.is_empty())
            .collect::<BTreeSet<_>>();
        if !keys.is_empty() {
            batch
                .set_keys(keys.into_iter().collect::<Vec<_>>().join(KEY_SEPARATOR))
                .map_err(|_| ProducerError::InvalidAutoBatchResult)?;
        }
        if let Some(tag) = &accumulated.key.tag {
            batch
                .set_tags(tag.clone())
                .map_err(|_| ProducerError::InvalidAutoBatchResult)?;
        }
        batch
            .set_wait_store_msg_ok(accumulated.key.wait_store_msg_ok)
            .map_err(|_| ProducerError::InvalidAutoBatchResult)?;

        let timeout = Duration::from_millis(self.config.send_timeout_millis().max(0) as u64);
        let result = if let Some(queue) = &accumulated.key.queue {
            self.send_to_queue_mode(&mut batch, queue, true, accumulated.key.mode, timeout)
                .await?
        } else {
            self.send_default(&mut batch, true, accumulated.key.mode, timeout)
                .await?
        };
        result.ok_or(ProducerError::Timeout)
    }

    /// Validates, Java-encodes, and sends a batch as one Broker message.
    ///
    /// Like Java collection sends, this assigns missing unique IDs and applies
    /// the configured namespace directly to each input message before dispatch.
    pub async fn send_batch(&self, messages: &mut [Message]) -> Result<SendResult, ProducerError> {
        self.send_batch_with_timeout(
            messages,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_batch_with_timeout(
        &self,
        messages: &mut [Message],
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        let deadline = deadline_after(timeout);
        self.prepare_batch_messages(messages)?;
        let mut batch = encode_prepared_batch(messages, &self.config, &self.ids)
            .map_err(batch_initiation_failure)?;
        self.send_default_until(&mut batch, true, SendMode::Sync, deadline)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    pub async fn send_batch_future(
        &self,
        messages: &mut [Message],
    ) -> Result<SendResult, ProducerError> {
        self.send_batch_future_with_timeout(
            messages,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_batch_future_with_timeout(
        &self,
        messages: &mut [Message],
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        let deadline = deadline_after(timeout);
        self.prepare_batch_messages(messages)?;
        let mut batch = encode_prepared_batch(messages, &self.config, &self.ids)
            .map_err(batch_initiation_failure)?;
        self.send_default_until(&mut batch, true, SendMode::Future, deadline)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    pub async fn send_batch_to_queue(
        &self,
        messages: &mut [Message],
        queue: &MessageQueue,
    ) -> Result<SendResult, ProducerError> {
        self.send_batch_to_queue_with_timeout(
            messages,
            queue,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_batch_to_queue_with_timeout(
        &self,
        messages: &mut [Message],
        queue: &MessageQueue,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_batch_to_queue_mode(messages, queue, SendMode::Sync, timeout)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    pub async fn send_batch_to_queue_future(
        &self,
        messages: &mut [Message],
        queue: &MessageQueue,
    ) -> Result<SendResult, ProducerError> {
        self.send_batch_to_queue_future_with_timeout(
            messages,
            queue,
            Duration::from_millis(self.config.send_timeout_millis().max(0) as u64),
        )
        .await
    }

    pub async fn send_batch_to_queue_future_with_timeout(
        &self,
        messages: &mut [Message],
        queue: &MessageQueue,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.send_batch_to_queue_mode(messages, queue, SendMode::Future, timeout)
            .await?
            .ok_or(ProducerError::Timeout)
    }

    async fn send_batch_to_queue_mode(
        &self,
        messages: &mut [Message],
        queue: &MessageQueue,
        mode: SendMode,
        timeout: Duration,
    ) -> Result<Option<SendResult>, ProducerError> {
        let deadline = deadline_after(timeout);
        self.prepare_batch_messages(messages)?;
        let mut batch = encode_prepared_batch(messages, &self.config, &self.ids)
            .map_err(batch_initiation_failure)?;
        self.send_to_queue_until(&mut batch, queue, true, mode, deadline)
            .await
    }

    async fn send_to_queue_mode(
        &self,
        message: &mut Message,
        queue: &MessageQueue,
        batch: bool,
        mode: SendMode,
        timeout: Duration,
    ) -> Result<Option<SendResult>, ProducerError> {
        let deadline = deadline_after(timeout);
        self.send_to_queue_until(message, queue, batch, mode, deadline)
            .await
    }

    async fn send_to_queue_until(
        &self,
        message: &mut Message,
        queue: &MessageQueue,
        batch: bool,
        mode: SendMode,
        deadline: Instant,
    ) -> Result<Option<SendResult>, ProducerError> {
        let (backpressure_enabled, backpressure) = if mode == SendMode::Future {
            let message_body_length = message.body().len();
            self.acquire_async_backpressure(message_body_length, deadline)
                .await?
        } else {
            (false, None)
        };
        let wire_message = self.message_with_namespace(message)?;
        let mut wire_queue = self.queue_with_namespace(queue)?;
        let runtime = self.started_runtime().await?;
        validation::validate_message(Some(&wire_message), &self.config)?;
        let info = self
            .acquire_publish_info(&runtime, wire_message.topic(), deadline)
            .await?;
        let broker = wire_queue
            .broker_name()
            .ok_or_else(|| ProducerError::NoRoute(wire_message.topic().to_owned()))?
            .to_owned();
        let address = self.broker_address_or_error(&runtime, &info, &wire_queue)?;
        let mut hooked_message = wire_message;
        self.set_unique_id_on_input_and_copy_to_wire(message, &mut hooked_message);
        if mode == SendMode::Future {
            return self
                .send_future_attempts(
                    runtime.clone(),
                    &mut wire_queue,
                    &broker,
                    &address,
                    &mut hooked_message,
                    batch,
                    None,
                    deadline,
                    backpressure,
                    backpressure_enabled,
                    false,
                )
                .await;
        }
        let mut check_forbidden_rejected = false;
        self.send_one_attempt_with_hooks(
            &runtime,
            &mut wire_queue,
            &broker,
            &address,
            &mut hooked_message,
            batch,
            mode,
            deadline,
            &mut check_forbidden_rejected,
        )
        .await
    }

    async fn send_default(
        &self,
        message: &mut Message,
        batch: bool,
        mode: SendMode,
        timeout: Duration,
    ) -> Result<Option<SendResult>, ProducerError> {
        let deadline = deadline_after(timeout);
        self.send_default_until(message, batch, mode, deadline)
            .await
    }

    async fn send_default_until(
        &self,
        message: &mut Message,
        batch: bool,
        mode: SendMode,
        deadline: Instant,
    ) -> Result<Option<SendResult>, ProducerError> {
        let wire_message = self.message_with_namespace(message)?;
        let (backpressure_enabled, backpressure) = if mode == SendMode::Future {
            let message_body_length = wire_message.body().len();
            self.acquire_async_backpressure(message_body_length, deadline)
                .await?
        } else {
            (false, None)
        };
        let runtime = self.started_runtime().await?;
        self.send_default_on_runtime_until(DefaultSendRequest {
            runtime: &runtime,
            message,
            wire_message,
            batch,
            mode,
            deadline,
            backpressure,
            backpressure_enabled,
        })
        .await
    }

    /// Sends with the internal producer's default sync behavior through an
    /// enclosing client runtime. This is used by Consumer retry-topic
    /// publication, whose Java counterpart is `MQClientInstance`'s
    /// `CLIENT_INNER_PRODUCER`.
    #[doc(hidden)]
    pub async fn send_internal_with_runtime(
        self: &Arc<Self>,
        runtime: &Arc<ClientRuntime>,
        message: &mut Message,
        timeout: Duration,
    ) -> Result<SendResult, ProducerError> {
        self.ensure_internal_detector_task(runtime).await;
        let wire_message = self.message_with_namespace(message)?;
        let deadline = deadline_after(timeout);
        self.send_default_on_runtime_until(DefaultSendRequest {
            runtime,
            message,
            wire_message,
            batch: false,
            mode: SendMode::Sync,
            deadline,
            backpressure: None,
            backpressure_enabled: false,
        })
        .await?
        .ok_or(ProducerError::Timeout)
    }

    async fn send_default_on_runtime_until(
        &self,
        request: DefaultSendRequest<'_>,
    ) -> Result<Option<SendResult>, ProducerError> {
        let DefaultSendRequest {
            runtime,
            message,
            wire_message,
            batch,
            mode,
            deadline,
            backpressure,
            backpressure_enabled,
        } = request;
        validation::validate_message(Some(&wire_message), &self.config)?;
        let send_started = StdInstant::now();
        let mut info = self
            .acquire_publish_info(runtime, wire_message.topic(), deadline)
            .await?;
        let mut hooked_message = wire_message;
        let mut caller_unique_id_set = false;
        let attempts = attempt_count(mode, &self.config);
        if attempts == 0 {
            return Err(ProducerError::NoSendAttempt);
        }

        let mut brokers_sent = Vec::with_capacity(attempts);
        let mut last_broker = None;
        let mut last_error = None;
        let mut last_send_result = None;
        for attempt in 0..attempts {
            if Instant::now() >= deadline {
                return last_send_result
                    .map(Some)
                    .ok_or_else(|| last_error.unwrap_or(ProducerError::Timeout));
            }
            let topic = hooked_message.topic().to_owned();
            let latency_fault_enabled = self.send_latency_fault_enable();
            if attempt > 0 && latency_fault_enabled {
                self.reset_publish_queue_index(&topic);
            }
            if let Some(current_info) = self.current_publish_info(runtime, &topic)? {
                info = current_info;
            }
            if !info
                .publish_queues
                .iter()
                .any(|queue| queue.topic() == Some(topic.as_str()))
            {
                info = self.acquire_publish_info(runtime, &topic, deadline).await?;
            }
            let selected = self.fault.select(
                &info.publish_queues,
                last_broker.as_deref(),
                latency_fault_enabled,
                |length| self.next_publish_queue_index(&topic, length),
                StdInstant::now(),
            );
            let Some(mut queue) = selected else {
                return last_send_result
                    .map(Some)
                    .ok_or_else(|| ProducerError::NoRoute(topic.clone()));
            };
            let Some(broker) = queue.broker_name().map(str::to_owned) else {
                return last_send_result
                    .map(Some)
                    .ok_or_else(|| ProducerError::NoRoute(topic.clone()));
            };
            last_broker = Some(broker.clone());
            brokers_sent.push(Some(broker.clone()));

            let attempt_started = StdInstant::now();
            let Some(address) = self.broker_address(runtime, &info, &queue)? else {
                // Java sendKernelImpl rechecks its cached TopicPublishInfo and
                // throws MQClientException when the selected Broker has no
                // publish address. The default send loop retries that client
                // error and wraps the final failure with the attempts/Broker list.
                self.update_fault(&broker, attempt_started.elapsed(), false, true)?;
                last_error = Some(ProducerError::BrokerAddressNotFound { broker });
                continue;
            };

            if !caller_unique_id_set {
                self.set_unique_id_on_input_and_copy_to_wire(message, &mut hooked_message);
                caller_unique_id_set = true;
            }

            if mode == SendMode::Future {
                return self
                    .send_future_attempts(
                        runtime.clone(),
                        &mut queue,
                        &broker,
                        &address,
                        &mut hooked_message,
                        batch,
                        Some(&info),
                        deadline,
                        backpressure,
                        backpressure_enabled,
                        true,
                    )
                    .await;
            }

            let request_deadline = self.request_deadline(mode, deadline, attempt, attempts);
            let started = StdInstant::now();
            let mut check_forbidden_rejected = false;
            let result = self
                .send_one_attempt_with_hooks(
                    runtime,
                    &mut queue,
                    &broker,
                    &address,
                    &mut hooked_message,
                    batch,
                    mode,
                    request_deadline,
                    &mut check_forbidden_rejected,
                )
                .await;
            let latency = started.elapsed();
            match result {
                Ok(Some(send_result)) => {
                    self.update_fault(&broker, latency, false, true)?;
                    if mode == SendMode::Sync
                        && send_result.status != SendStatus::SendOk
                        && self.config.retry_another_broker_when_not_store_ok()
                        && attempt + 1 < attempts
                    {
                        last_send_result = Some(send_result);
                        continue;
                    }
                    return Ok(Some(send_result));
                }
                Ok(None) => {
                    self.update_fault(&broker, latency, false, true)?;
                    return Ok(None);
                }
                Err(error) => {
                    // Java catches a request-scoped RemotingTooMuchRequestException and
                    // retries while the outer send deadline still has time. A capped
                    // deadline can expire during hooks/message preparation before the
                    // remoting layer runs, so preserve that retry path for local timeouts.
                    let now = Instant::now();
                    let sync_retry_remains =
                        mode == SendMode::Sync && attempt + 1 < attempts && now < deadline;
                    let request_timeout_can_retry = sync_retry_remains
                        && request_deadline < deadline
                        && now >= request_deadline
                        && matches!(&error, ProducerError::Timeout);
                    let (isolation, reachable, retry) = if check_forbidden_rejected {
                        // Java's CheckForbiddenHook declares MQClientException, so the
                        // failure is a reachable-client error even if a Rust hook uses
                        // another ProducerError variant to describe its cause.
                        (false, true, sync_retry_remains)
                    } else {
                        match &error {
                            ProducerError::Rpc(rpc) => {
                                let (isolation, reachable) =
                                    classify_fault(rpc, mode, self.start_detector_enable());
                                (
                                    isolation,
                                    reachable,
                                    attempt + 1 < attempts && should_retry(mode, rpc, &self.config),
                                )
                            }
                            _ => (false, true, request_timeout_can_retry),
                        }
                    };
                    self.update_fault(&broker, latency, isolation, reachable)?;
                    if retry {
                        last_error = Some(error);
                        continue;
                    }
                    if let Some(send_result) = last_send_result.take() {
                        return Ok(Some(send_result));
                    }
                    if check_forbidden_rejected {
                        if mode == SendMode::Sync && attempt + 1 < attempts && now >= deadline {
                            // Java advances to the next loop iteration and reports the
                            // exhausted overall deadline before invoking another hook.
                            return Err(ProducerError::Timeout);
                        }
                        return Err(client_retries_exhausted(
                            error,
                            attempt + 1,
                            send_started,
                            hooked_message.topic(),
                            &brokers_sent,
                        ));
                    }
                    return Err(error);
                }
            }
        }
        if let Some(send_result) = last_send_result {
            return Ok(Some(send_result));
        }
        let error =
            last_error.unwrap_or_else(|| ProducerError::NoRoute(hooked_message.topic().to_owned()));
        if matches!(error, ProducerError::BrokerAddressNotFound { .. }) {
            return Err(client_retries_exhausted(
                error,
                brokers_sent.len(),
                send_started,
                hooked_message.topic(),
                &brokers_sent,
            ));
        }
        Err(error)
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_one_attempt_with_hooks(
        &self,
        runtime: &ClientRuntime,
        queue: &mut MessageQueue,
        broker: &str,
        address: &str,
        message: &mut Message,
        batch: bool,
        mode: SendMode,
        deadline: Instant,
        check_forbidden_rejected: &mut bool,
    ) -> Result<Option<SendResult>, ProducerError> {
        self.ids.set_if_missing(message);
        let mut original_body = Some(message.body().to_vec());
        let compression_sys_flag = self.compress_message_before_hook(message, batch);
        if let Err(error) = self.execute_check_forbidden_hooks(message, queue, address, mode) {
            *check_forbidden_rejected = true;
            return Err(error);
        }
        let hooks = self.send_message_hook_snapshot();
        let mut context = self.send_message_context(message, queue, address, mode);
        Self::execute_send_message_hook_before_snapshot(&hooks, &mut context);
        // Java passes the selected MessageQueue object itself through the hook
        // context, so in-place queue edits affect the ensuing request.
        *queue = context.queue.clone();

        let prepared_result = self.prepare(context.message, batch, compression_sys_flag);
        if mode == SendMode::Future {
            context.message.set_body(
                original_body
                    .take()
                    .expect("original message body is retained"),
            );
        }
        let mut result = match prepared_result {
            Ok(prepared) => {
                self.send_one_attempt_at(SendAttempt {
                    runtime,
                    queue,
                    broker,
                    address,
                    prepared: &prepared,
                    mode,
                    deadline,
                })
                .await
            }
            Err(error) => Err(error),
        };

        if let Ok(Some(send_result)) = &mut result {
            send_result.topic = self.topic_without_namespace(&send_result.topic)?;
        }

        match &result {
            Ok(Some(send_result)) => context.send_result = Some(send_result.clone()),
            Err(error) => context.exception = Some(error),
            Ok(None) => {}
        }
        if !matches!(&result, Err(ProducerError::InvalidRetryMetadata)) {
            Self::execute_send_message_hook_after_snapshot(&hooks, &mut context);
        }
        if let Some(original_body) = original_body {
            context.message.set_body(original_body);
        }
        let hook_result = context.send_result.take();
        drop(context);
        if let (Ok(Some(send_result)), Some(hook_result)) = (&mut result, hook_result) {
            *send_result = hook_result.clone();
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_future_attempts(
        &self,
        runtime: Arc<ClientRuntime>,
        queue: &mut MessageQueue,
        broker: &str,
        address: &str,
        message: &mut Message,
        batch: bool,
        publish_info: Option<&TopicSnapshot>,
        deadline: Instant,
        backpressure: Option<AsyncBackpressurePermit>,
        backpressure_enabled: bool,
        wrap_check_forbidden_error: bool,
    ) -> Result<Option<SendResult>, ProducerError> {
        let executor = self.async_sender_executor();
        let producer = self
            .self_weak
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or(ProducerError::NotStarted)?;
        let mut queue = queue.clone();
        let broker = broker.to_owned();
        let address = address.to_owned();
        let mut message = message.clone();
        let publish_info = publish_info.cloned();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let task: AsyncSenderTask = Box::pin(async move {
            let backpressure = backpressure;
            let result = producer
                .send_future_attempts_inline(
                    &runtime,
                    &mut queue,
                    &broker,
                    &address,
                    &mut message,
                    batch,
                    publish_info.as_ref(),
                    deadline,
                    wrap_check_forbidden_error,
                )
                .await;
            drop(backpressure);
            let _ = result_tx.send(result);
        });

        match executor.execute(task) {
            Ok(()) => match tokio::time::timeout_at(deadline, result_rx).await {
                Ok(result) => result.map_err(|_| ProducerError::AsyncSenderExecutorDropped)?,
                Err(_) => Err(ProducerError::Timeout),
            },
            Err(task) if backpressure_enabled => {
                task.await;
                result_rx
                    .await
                    .map_err(|_| ProducerError::AsyncSenderExecutorDropped)?
            }
            Err(_task) => Err(ProducerError::AsyncSenderExecutorRejected),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_future_attempts_inline(
        &self,
        runtime: &ClientRuntime,
        queue: &mut MessageQueue,
        broker: &str,
        address: &str,
        message: &mut Message,
        batch: bool,
        publish_info: Option<&TopicSnapshot>,
        deadline: Instant,
        wrap_check_forbidden_error: bool,
    ) -> Result<Option<SendResult>, ProducerError> {
        if Instant::now() >= deadline {
            return Err(ProducerError::Timeout);
        }

        self.ids.set_if_missing(message);
        let original_body = message.body().to_vec();
        let compression_sys_flag = self.compress_message_before_hook(message, batch);
        let check_started = StdInstant::now();
        if let Err(error) =
            self.execute_check_forbidden_hooks(message, queue, address, SendMode::Future)
        {
            if wrap_check_forbidden_error {
                self.update_fault(broker, check_started.elapsed(), false, true)?;
                return Err(client_retries_exhausted(
                    error,
                    1,
                    check_started,
                    message.topic(),
                    &[Some(broker.to_owned())],
                ));
            }
            return Err(error);
        }
        let hooks = self.send_message_hook_snapshot();
        let mut context = self.send_message_context(message, queue, address, SendMode::Future);
        Self::execute_send_message_hook_before_snapshot(&hooks, &mut context);
        // Preserve Java's shared MessageQueue mutation semantics before the
        // asynchronous submission and any retry setup.
        *queue = context.queue.clone();

        let prepared = self.prepare(context.message, batch, compression_sys_flag);
        context.message.set_body(original_body);
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(ProducerError::InvalidRetryMetadata) => {
                return Err(ProducerError::InvalidRetryMetadata);
            }
            Err(error) => {
                context.exception = Some(&error);
                Self::execute_send_message_hook_after_snapshot(&hooks, &mut context);
                drop(context);
                return Err(error);
            }
        };
        if let Ok(topic) = self.topic_without_namespace(context.message.topic()) {
            context.message.set_topic(topic);
        }

        let attempts = attempt_count(SendMode::Future, &self.config);
        let request_queue = queue.clone();
        let mut current_broker = broker.to_owned();
        let mut current_address = address.to_owned();
        let mut outcome = Err(ProducerError::NoSendAttempt);
        let mut pending_after_called = false;
        for attempt in 0..attempts {
            if Instant::now() >= deadline {
                outcome = Err(ProducerError::Timeout);
                break;
            }
            if !pending_after_called {
                Self::execute_send_message_hook_after_snapshot(&hooks, &mut context);
                pending_after_called = true;
            }
            let started = StdInstant::now();
            let result = self
                .send_one_attempt_at(SendAttempt {
                    runtime,
                    queue: &request_queue,
                    broker: &current_broker,
                    address: &current_address,
                    prepared: &prepared,
                    mode: SendMode::Future,
                    deadline,
                })
                .await;
            let latency = started.elapsed();
            match result {
                Ok(Some(mut send_result)) => {
                    send_result.topic = match self.topic_without_namespace(&send_result.topic) {
                        Ok(topic) => topic,
                        Err(error) => {
                            outcome = Err(error);
                            break;
                        }
                    };
                    self.update_fault(&current_broker, latency, false, true)?;
                    outcome = Ok(Some(send_result));
                    break;
                }
                Ok(None) => {
                    self.update_fault(&current_broker, latency, false, true)?;
                    outcome = Ok(None);
                    break;
                }
                Err(error) => {
                    if let ProducerError::Rpc(rpc) = &error {
                        let (isolation, reachable) =
                            classify_fault(rpc, SendMode::Future, self.start_detector_enable());
                        self.update_fault(&current_broker, latency, isolation, reachable)?;
                    }
                    outcome = Err(error);
                    if attempt + 1 >= attempts
                        || !matches!(
                            &outcome,
                            Err(ProducerError::Rpc(rpc))
                                if should_retry(SendMode::Future, rpc, &self.config)
                        )
                    {
                        break;
                    }

                    let topic = prepared.topic.clone();
                    let current_info = publish_info.and_then(|original| {
                        self.cached_publish_info(&topic)
                            .or_else(|| Some(Arc::new(original.clone())))
                    });
                    if let Some(info) = current_info.as_deref() {
                        let selected = self.fault.select(
                            &info.publish_queues,
                            Some(&current_broker),
                            self.send_latency_fault_enable(),
                            |length| self.next_publish_queue_index(&topic, length),
                            StdInstant::now(),
                        );
                        let Some(selected) = selected else {
                            break;
                        };
                        let Some(next_broker) = selected.broker_name().map(str::to_owned) else {
                            break;
                        };
                        let next_address = match self.broker_address(runtime, info, &selected) {
                            Ok(Some(address)) => address,
                            Ok(None) => break,
                            Err(error) => {
                                outcome = Err(error);
                                break;
                            }
                        };
                        current_broker = next_broker;
                        current_address = next_address;
                    }
                }
            }
        }

        match &outcome {
            Ok(Some(send_result)) => context.send_result = Some(send_result.clone()),
            Err(error) => context.exception = Some(error),
            Ok(None) => {}
        }
        Self::execute_send_message_hook_after_snapshot(&hooks, &mut context);
        let hook_result = context.send_result.take();
        drop(context);
        if let (Ok(Some(send_result)), Some(hook_result)) = (&mut outcome, hook_result) {
            *send_result = hook_result.clone();
        }
        outcome
    }

    fn execute_check_forbidden_hooks(
        &self,
        message: &mut Message,
        queue: &mut MessageQueue,
        address: &str,
        communication_mode: SendMode,
    ) -> Result<(), ProducerError> {
        let hooks = self.check_forbidden_hook_snapshot();
        if hooks.is_empty() {
            return Ok(());
        }
        let mut context = CheckForbiddenContext {
            name_srv_addr: self.config.client_config().get_namesrv_addr(),
            group: self.producer_group(),
            message,
            queue,
            broker_addr: address.to_owned(),
            communication_mode: communication_mode.into(),
            send_result: None,
            exception: None,
            arg: None,
            unit_mode: self.config.client_config().unit_mode(),
        };
        Self::execute_check_forbidden_hook_snapshot(&hooks, &mut context)
    }

    fn send_message_context<'a>(
        &'a self,
        message: &'a mut Message,
        queue: &MessageQueue,
        address: &str,
        mode: SendMode,
    ) -> SendMessageContext<'a> {
        let mut client_config = self.config.client_config().clone();
        let namespace = client_config
            .get_namespace()
            .ok()
            .flatten()
            .map(str::to_owned);
        let (is_transaction_half, is_delay) = {
            let properties = message.properties();
            (
                properties
                    .get(PROPERTY_TRANSACTION_PREPARED)
                    .is_some_and(|value| value == "true"),
                properties.contains_key(PROPERTY_DELAY_TIME_LEVEL)
                    || properties.contains_key("__STARTDELIVERTIME")
                    || properties.contains_key(PROPERTY_TIMER_DELIVER_MS)
                    || properties.contains_key(PROPERTY_TIMER_DELAY_SEC)
                    || properties.contains_key(PROPERTY_TIMER_DELAY_MS),
            )
        };
        let message_type = if is_delay {
            SendMessageType::Delay
        } else if is_transaction_half {
            SendMessageType::TransactionHalf
        } else {
            SendMessageType::Normal
        };
        SendMessageContext {
            producer_group: self.producer_group(),
            producer: self,
            message,
            queue: queue.clone(),
            broker_addr: address.to_owned(),
            born_host: self.config.client_config().client_ip().map(str::to_owned),
            communication_mode: mode.into(),
            send_result: None,
            exception: None,
            message_type,
            namespace,
            trace_context: None,
            properties: None,
        }
    }

    async fn send_one_attempt_at(
        &self,
        attempt: SendAttempt<'_>,
    ) -> Result<Option<SendResult>, ProducerError> {
        if Instant::now() >= attempt.deadline {
            return Err(ProducerError::Timeout);
        }
        let header = {
            let mut __phase18_header_0 = SendMessageRequestHeaderV1::default();
            __phase18_header_0.set_producer_group(self.producer_group());
            __phase18_header_0.set_topic(attempt.prepared.topic.clone());
            __phase18_header_0.set_default_topic(self.config.create_topic_key());
            __phase18_header_0.set_default_topic_queue_nums(self.config.default_topic_queue_nums());
            __phase18_header_0.set_queue_id(attempt.queue.queue_id());
            __phase18_header_0.set_sys_flag(attempt.prepared.sys_flag);
            __phase18_header_0.set_born_timestamp(attempt.prepared.born_timestamp);
            __phase18_header_0.set_flag(attempt.prepared.flag);
            __phase18_header_0.set_properties(attempt.prepared.properties.clone());
            __phase18_header_0.set_reconsume_times(attempt.prepared.reconsume_times);
            __phase18_header_0.set_unit_mode(Some(self.config.client_config().unit_mode()));
            __phase18_header_0.set_max_reconsume_times(attempt.prepared.max_reconsume_times);
            __phase18_header_0.set_batch(Some(attempt.prepared.batch));
            __phase18_header_0
        };
        let send_kind = rocketmq_classic_core::api::MQClientApi::send_kind_for_message(
            attempt.prepared.batch,
            attempt.prepared.is_reply,
        );

        #[cfg(test)]
        {
            let mut scripted = self
                .scripted_transport
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            scripted.attempts.push(ScriptedSendAttempt {
                broker: attempt.broker.to_owned(),
                address: attempt.address.to_owned(),
                producer_group: header.get_producer_group().map(str::to_owned),
                queue: attempt.queue.clone(),
                default_topic: header.get_default_topic().to_owned(),
                default_topic_queue_nums: header.get_default_topic_queue_nums(),
                body: attempt.prepared.body.clone(),
                topic: attempt.prepared.topic.clone(),
                properties: attempt.prepared.properties.clone(),
                sys_flag: attempt.prepared.sys_flag,
                unique_id: attempt.prepared.unique_id.clone(),
                mode: attempt.mode,
                batch: attempt.prepared.batch,
                send_kind,
            });
            if let Some(result) = scripted.responses.pop_front() {
                return result;
            }
        }
        Ok(attempt
            .runtime
            .api()
            .send_message(
                attempt.address,
                attempt.broker,
                &header,
                attempt.prepared.body.clone(),
                &attempt.prepared.unique_id,
                send_kind,
                attempt.mode,
                attempt.deadline,
            )
            .await?)
    }

    async fn acquire_publish_info(
        &self,
        runtime: &ClientRuntime,
        topic: &str,
        deadline: Instant,
    ) -> Result<Arc<TopicSnapshot>, ProducerError> {
        self.remember_publish_topic(topic);
        if let Some(info) = self.current_publish_info(runtime, topic)? {
            if !info.publish_queues.is_empty() {
                return Ok(info);
            }
        }

        let _ = runtime.update_topic_route(topic, deadline).await?;
        if let Some(info) = self.current_publish_info(runtime, topic)? {
            if !info.publish_queues.is_empty() {
                return Ok(info);
            }
        }

        let _ = runtime
            .update_topic_route_from_default(
                topic,
                self.config.default_topic_queue_nums(),
                deadline,
            )
            .await?;
        if let Some(info) = self.current_publish_info(runtime, topic)? {
            if !info.publish_queues.is_empty() {
                return Ok(info);
            }
        }
        Err(ProducerError::NoRoute(topic.to_owned()))
    }

    fn current_publish_info(
        &self,
        runtime: &ClientRuntime,
        topic: &str,
    ) -> Result<Option<Arc<TopicSnapshot>>, ProducerError> {
        if let Some(info) = self.cached_publish_info(topic) {
            return Ok(Some(info));
        }
        let info = runtime.route_snapshot()?.topics.get(topic).cloned();
        if let Some(info) = &info {
            self.cache_publish_info(topic, info.clone());
        }
        Ok(info)
    }

    fn select_queue_with_selector<F>(
        &self,
        info: &TopicSnapshot,
        message: &Message,
        selector: F,
    ) -> Result<Option<(MessageQueue, MessageQueue)>, ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue>,
    {
        let logical_queues = info
            .publish_queues
            .iter()
            .map(|queue| self.queue_without_namespace(queue))
            .collect::<Result<Vec<_>, _>>()?;
        let mut selector_message = message.clone_message();
        if let Some(topic) = message.topic_nullable() {
            selector_message.set_topic(self.topic_without_namespace(topic)?);
        }
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            selector(&logical_queues, &selector_message)
        }))
        .map_err(|payload| ProducerError::SelectorPanicked(selector_panic_message(&*payload)))?
        .map(|logical_queue| {
            self.queue_with_namespace(&logical_queue)
                .map(|queue| (logical_queue, queue))
        })
        .transpose()
    }

    fn next_publish_queue_index(&self, topic: &str, queue_count: usize) -> usize {
        self.cached_publish_info_value(topic).map_or_else(
            || self.selector.next_index(topic, queue_count),
            |info| info.next_index(queue_count),
        )
    }

    fn reset_publish_queue_index(&self, topic: &str) {
        if let Some(info) = self.cached_publish_info_value(topic) {
            info.reset_index();
        } else {
            self.selector.reset(topic);
        }
    }

    fn broker_address(
        &self,
        runtime: &ClientRuntime,
        info: &TopicSnapshot,
        queue: &MessageQueue,
    ) -> Result<Option<String>, ProducerError> {
        // DefaultMQProducerImpl.sendKernelImpl reads the facade's current
        // isSendMessageWithVIPChannel value for every send. The runtime's
        // ClientConfig is a startup snapshot and must not control this path.
        let vip_enabled = self.send_message_with_vip_channel();
        if let Some(address) = info.endpoints.get(queue) {
            return Ok(Some(broker_vip_channel(vip_enabled, address)?));
        }
        let Some(broker) = queue.broker_name() else {
            return Ok(None);
        };
        let snapshot = runtime.route_snapshot()?;
        let Some(address) = snapshot.publish_address(broker) else {
            return Ok(None);
        };
        Ok(Some(broker_vip_channel(vip_enabled, address)?))
    }

    fn broker_address_or_error(
        &self,
        runtime: &ClientRuntime,
        info: &TopicSnapshot,
        queue: &MessageQueue,
    ) -> Result<String, ProducerError> {
        let broker = queue.broker_name().unwrap_or("null").to_owned();
        self.broker_address(runtime, info, queue)?
            .ok_or(ProducerError::BrokerAddressNotFound { broker })
    }

    fn request_deadline(
        &self,
        mode: SendMode,
        deadline: Instant,
        attempt: usize,
        attempts: usize,
    ) -> Instant {
        self.request_deadline_at(mode, deadline, Instant::now(), attempt, attempts)
    }

    fn request_deadline_at(
        &self,
        mode: SendMode,
        deadline: Instant,
        now: Instant,
        attempt: usize,
        attempts: usize,
    ) -> Instant {
        let max_per_request = self.config.send_msg_max_timeout_per_request();
        if mode != SendMode::Sync || max_per_request <= -1 || attempt + 1 >= attempts {
            return deadline;
        }
        let per_request = now + Duration::from_millis(max_per_request.max(0) as u64);
        per_request.min(deadline)
    }

    fn message_with_namespace(&self, message: &Message) -> Result<Message, ProducerError> {
        let mut namespaced = message.clone();
        let mut config = self.config.client_config().clone();
        if let Some(topic) = config.with_namespace(Some(message.topic()))? {
            namespaced.set_topic(topic);
        }
        Ok(namespaced)
    }

    fn set_unique_id_on_input_and_copy_to_wire(
        &self,
        message: &mut Message,
        wire_message: &mut Message,
    ) {
        let unique_id = self.ids.set_if_missing(message);
        let _ = wire_message.set_system_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, unique_id);
    }

    fn prepare_batch_messages(&self, messages: &mut [Message]) -> Result<(), ProducerError> {
        let result = (|| -> Result<(), ProducerError> {
            crate::batch::MessageBatch::generate_from_list(messages.to_vec())?;

            for message in messages {
                validation::validate_message(Some(message), &self.config)?;
                self.ids.set_if_missing(message);
                let topic = self.topic_with_namespace(message.topic())?;
                message.set_topic(topic);
            }

            Ok(())
        })();

        result.map_err(batch_initiation_failure)
    }

    fn topic_with_namespace(&self, topic: &str) -> Result<String, ProducerError> {
        let mut config = self.config.client_config().clone();
        Ok(config
            .with_namespace(Some(topic))?
            .unwrap_or_else(|| topic.to_owned()))
    }

    fn queue_with_namespace(&self, queue: &MessageQueue) -> Result<MessageQueue, ProducerError> {
        let mut config = self.config.client_config().clone();
        Ok(config.queue_with_namespace(queue)?)
    }

    fn queue_without_namespace(&self, queue: &MessageQueue) -> Result<MessageQueue, ProducerError> {
        let mut config = self.config.client_config().clone();
        let mut logical = queue.clone();
        if let Some(topic) = config.without_namespace(queue.topic())? {
            logical.set_topic(Some(topic));
        }
        Ok(logical)
    }

    fn topic_without_namespace(&self, topic: &str) -> Result<String, ProducerError> {
        let mut config = self.config.client_config().clone();
        Ok(config
            .without_namespace(Some(topic))?
            .unwrap_or_else(|| topic.to_owned()))
    }

    fn compress_message_before_hook(&self, message: &mut Message, batch: bool) -> i32 {
        if batch || (message.body().len() as i64) < self.config.compress_threshold() as i64 {
            return 0;
        }
        let original_body = message.body().to_vec();
        match rocketmq_classic_common::compression::compress(
            self.config.compress_type(),
            &original_body,
            self.config.compress_level(),
        ) {
            Ok(compressed) => {
                message.set_body(compressed);
                message::COMPRESSED_FLAG | self.config.compress_type().get_compression_flag()
            }
            Err(_) => 0,
        }
    }

    fn prepare(
        &self,
        message: &Message,
        batch: bool,
        compression_sys_flag: i32,
    ) -> Result<PreparedMessage, ProducerError> {
        let mut copy = message.clone();
        let unique_id = self.ids.set_if_missing(&mut copy);
        let mut properties: BTreeMap<String, String> = copy.properties().clone();
        let is_reply = properties
            .get(PROPERTY_MESSAGE_TYPE)
            .is_some_and(|value| value == REPLY_MESSAGE_FLAG);
        let retry_group = copy.topic().starts_with("%RETRY%");
        let reconsume_times = if retry_group {
            properties
                .remove(PROPERTY_RECONSUME_TIME)
                .map(|value| {
                    value
                        .parse()
                        .map_err(|_| ProducerError::InvalidRetryMetadata)
                })
                .transpose()?
        } else {
            None
        };
        let max_reconsume_times = if retry_group {
            properties
                .remove(PROPERTY_MAX_RECONSUME_TIMES)
                .map(|value| {
                    value
                        .parse()
                        .map_err(|_| ProducerError::InvalidRetryMetadata)
                })
                .transpose()?
        } else {
            None
        };

        let mut property_entries = properties
            .iter()
            .map(|(key, value)| (Some(key.clone()), Some(value.clone())))
            .collect::<Vec<_>>();
        property_entries.extend(
            copy.nullable_property_entries()
                .filter(|(key, _)| key.is_none()),
        );

        let body = copy.body().to_vec();
        let mut sys_flag = compression_sys_flag;
        if copy
            .properties()
            .get(PROPERTY_TRANSACTION_PREPARED)
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        {
            sys_flag |= message::TRANSACTION_PREPARED_TYPE;
        }
        Ok(PreparedMessage {
            topic: copy.topic().to_owned(),
            body,
            properties: Some(encode_nullable_property_entries(Some(&property_entries))),
            unique_id,
            born_timestamp: epoch_millis_now(),
            flag: copy.flag(),
            sys_flag,
            reconsume_times,
            max_reconsume_times,
            batch,
            is_reply,
        })
    }

    fn update_fault(
        &self,
        broker: &str,
        latency: Duration,
        isolation: bool,
        reachable: bool,
    ) -> Result<(), ProducerError> {
        if self.send_latency_fault_enable() {
            self.fault
                .update(broker, latency, isolation, reachable, StdInstant::now())
                .map_err(|error| {
                    ProducerError::FaultStrategy(error.java_exception_name().to_owned())
                })?;
        }
        Ok(())
    }
}

fn can_accumulate(message: &Message) -> bool {
    if message.topic().starts_with("%RETRY%")
        || message.properties().contains_key(PROPERTY_PRODUCER_GROUP)
    {
        return false;
    }
    [
        PROPERTY_DELAY_TIME_LEVEL,
        PROPERTY_TIMER_DELAY_MS,
        PROPERTY_TIMER_DELAY_SEC,
        PROPERTY_TIMER_DELIVER_MS,
    ]
    .into_iter()
    .all(|property| {
        message
            .properties()
            .get(property)
            .is_none_or(|value| value.parse::<i64>().is_ok_and(|delay| delay <= 0))
    })
}

async fn receive_auto_batch_result(
    receiver: crate::accumulator::BatchReceiver,
) -> Result<SendResult, ProducerError> {
    match receiver.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(error)) => Err(ProducerError::AutoBatchSend(error)),
        Err(_) => Err(ProducerError::Shutdown),
    }
}

fn split_auto_batch_result(
    result: SendResult,
    message_count: usize,
) -> Result<Vec<SendResult>, ProducerError> {
    if !result.msg_id.contains(',') {
        return Ok(vec![result; message_count]);
    }
    let message_ids = result.msg_id.split(',').collect::<Vec<_>>();
    let offset_message_ids = result
        .offset_msg_id
        .as_deref()
        .ok_or(ProducerError::InvalidAutoBatchResult)?
        .split(',')
        .collect::<Vec<_>>();
    if message_ids.len() != message_count || offset_message_ids.len() != message_count {
        return Err(ProducerError::InvalidAutoBatchResult);
    }
    Ok((0..message_count)
        .map(|index| {
            let mut per_message = result.clone();
            per_message.msg_id = message_ids[index].to_owned();
            per_message.offset_msg_id = Some(offset_message_ids[index].to_owned());
            per_message.queue_offset = result.queue_offset.wrapping_add(index as i64);
            per_message
        })
        .collect())
}

fn deadline_after(timeout: Duration) -> Instant {
    Instant::now() + timeout
}

fn epoch_millis_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

fn attempt_count(mode: SendMode, config: &crate::ProducerConfig) -> usize {
    match mode {
        SendMode::Sync => {
            usize::try_from(1_i64 + i64::from(config.retry_times_when_send_failed())).unwrap_or(0)
        }
        SendMode::Future => 1 + config.retry_times_when_send_async_failed().max(0) as usize,
        SendMode::Oneway => 1,
    }
}

fn should_retry(mode: SendMode, error: &RpcError, config: &crate::ProducerConfig) -> bool {
    match mode {
        SendMode::Sync => match error {
            RpcError::Response { code, .. } => config.retry_response_codes().contains(code),
            _ => true,
        },
        SendMode::Future => !matches!(
            error,
            RpcError::Transport(rocketmq_classic_remoting::TransportError::TooManyRequests)
        ),
        SendMode::Oneway => false,
    }
}

fn classify_fault(error: &RpcError, mode: SendMode, detector_enabled: bool) -> (bool, bool) {
    match mode {
        SendMode::Future => match error {
            RpcError::Response { .. }
            | RpcError::MalformedResponse { .. }
            | RpcError::Header(_)
            | RpcError::Transport(_) => (true, true),
            _ => (false, true),
        },
        SendMode::Sync | SendMode::Oneway => match error {
            RpcError::Response { .. } => (true, false),
            RpcError::Transport(_) => (true, !detector_enabled),
            _ => (false, true),
        },
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        CheckForbiddenContext, CheckForbiddenHook, CommunicationMode, ProducerQueueIndex,
        SendMessageHook,
    };
    use rocketmq_classic_common::message::decode_properties;
    use rocketmq_classic_core::api::producer::{SendKind, SendStatus};
    use rocketmq_classic_core::error::RpcError;
    use rocketmq_classic_protocol::codes::response;
    use rocketmq_classic_remoting::TransportError;

    use super::*;

    fn scripted_result(msg_id: &str, offset_msg_id: &str, queue_offset: i64) -> SendResult {
        SendResult {
            status: SendStatus::SendOk,
            msg_id: msg_id.into(),
            offset_msg_id: Some(offset_msg_id.into()),
            topic: "orders".into(),
            broker_name: "scripted-broker".into(),
            queue_id: 0,
            queue_offset,
            transaction_id: None,
            recall_handle: None,
            raw_resp_body: None,
            region_id: "DefaultRegion".into(),
            trace_on: true,
        }
    }

    async fn scripted_producer(
        config: crate::ProducerConfig,
    ) -> (Arc<Producer>, [MessageQueue; 2]) {
        scripted_producer_with_manager(
            config,
            rocketmq_classic_core::runtime::ClientRuntimeManager::new(),
        )
        .await
    }

    async fn scripted_producer_with_manager(
        config: crate::ProducerConfig,
        manager: rocketmq_classic_core::runtime::ClientRuntimeManager,
    ) -> (Arc<Producer>, [MessageQueue; 2]) {
        let mut client_config = config.client_config().clone();
        let topic = client_config
            .with_namespace(Some("orders"))
            .unwrap()
            .unwrap_or_else(|| "orders".into());
        let producer = Arc::new(Producer::with_runtime_manager(config, manager));
        producer.start().await.unwrap();
        let queues = [
            MessageQueue::new(&topic, "broker-a", 0),
            MessageQueue::new(&topic, "broker-b", 0),
        ];
        let snapshot = Arc::new(TopicSnapshot {
            route: Arc::new(
                serde_json::from_str::<rocketmq_classic_protocol::route::TopicRouteData>("{}")
                    .unwrap(),
            ),
            publish_queues: queues.to_vec(),
            subscribe_queues: Default::default(),
            endpoints: BTreeMap::from([
                (queues[0].clone(), "10.0.0.1:10911".into()),
                (queues[1].clone(), "10.0.0.2:10911".into()),
            ]),
            order_topic: false,
        });
        producer.cache_publish_info(&topic, snapshot);
        (producer, queues)
    }

    async fn wait_for_scheduled_auto_batch_timer(producer: &Producer) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !producer.auto_batch_tasks.lock().await.is_empty() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the pending batch should own a timer task");
    }

    #[tokio::test]
    async fn publish_info_queue_list_mutation_changes_the_next_default_send_queue() {
        let (producer, queues) =
            scripted_producer(crate::ProducerConfig::new("publish-info-live-send").unwrap()).await;
        let info = producer
            .get_topic_publish_info_table()
            .get("orders")
            .unwrap();
        let selected_queue = queues[1].clone();
        info.message_queue_list()
            .unwrap()
            .with_mut(|queues| *queues = vec![selected_queue]);
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("live-route", "offset", 1))));

        producer
            .send(&mut Message::new("orders", b"live route table".to_vec()))
            .await
            .unwrap();

        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts[0].queue,
            queues[1]
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn producer_group_setter_updates_later_send_headers_including_null() {
        let (producer, _) =
            scripted_producer(crate::ProducerConfig::new("live-producer-group-send").unwrap())
                .await;
        producer.set_producer_group(Some("changed-after-start".into()));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("group-one", "offset-one", 1))));
        producer
            .send(&mut Message::new("orders", b"new producer group".to_vec()))
            .await
            .unwrap();

        producer.set_producer_group(None);
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("group-null", "offset-null", 2))));
        producer
            .send(&mut Message::new("orders", b"null producer group".to_vec()))
            .await
            .unwrap();

        {
            let transport = producer.scripted_transport.lock().unwrap();
            assert_eq!(
                transport.attempts[0].producer_group.as_deref(),
                Some("changed-after-start")
            );
            assert_eq!(transport.attempts[1].producer_group, None);
        }
        assert!(matches!(
            producer.shutdown().await,
            Err(crate::ProducerError::Runtime(
                rocketmq_classic_core::runtime::RuntimeError::InvalidRoute("null producer group")
            ))
        ));
        assert_eq!(
            producer.service_state(),
            Some(crate::ProducerServiceState::Running)
        );
        producer.set_producer_group(Some("live-producer-group-send".into()));
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn vip_channel_setter_after_start_changes_later_send_addresses() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let expected = &fixture["runtimeSetterFlows"];
        let mut config = crate::ProducerConfig::new("live-vip-channel-send").unwrap();
        config.set_send_message_with_vip_channel(true);
        let (producer, _) = scripted_producer(config).await;
        producer.set_producer_group(Some(
            expected["producerGroupAfterSetter"]
                .as_str()
                .unwrap()
                .to_owned(),
        ));
        producer.set_send_message_with_vip_channel(
            expected["vipChannelAfterSetter"].as_bool().unwrap(),
        );
        assert_eq!(
            producer.send_message_with_vip_channel(),
            expected["vipChannelAfterSetter"]
        );
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("vip-send", "vip-offset", 1))));

        producer
            .send(&mut Message::new("orders", b"vip channel".to_vec()))
            .await
            .unwrap();

        {
            let transport = producer.scripted_transport.lock().unwrap();
            let attempt = &transport.attempts[0];
            let java_address = expected["brokerAddressAtSend"].as_str().unwrap();
            assert_eq!(
                attempt.address.rsplit(':').next(),
                java_address.rsplit(':').next()
            );
            assert_eq!(
                attempt.producer_group.as_deref(),
                expected["producerGroupAtSend"].as_str()
            );
        }
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn runtime_default_topic_settings_reach_later_send_headers() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let expected = &fixture["runtimeSettings"];
        let (producer, _) =
            scripted_producer(crate::ProducerConfig::new("runtime-route-settings").unwrap()).await;
        producer.set_create_topic_key(expected["createTopicKey"].as_str().unwrap());
        producer.set_default_topic_queue_nums(
            expected["defaultTopicQueueNums"].as_i64().unwrap() as i32
        );
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("runtime-route", "offset", 0))));

        producer
            .send(&mut Message::new("orders", b"runtime settings".to_vec()))
            .await
            .unwrap();

        {
            let transport = producer.scripted_transport.lock().unwrap();
            assert_eq!(transport.attempts.len(), 1);
            assert_eq!(
                transport.attempts[0].default_topic,
                expected["createTopicKey"]
            );
            assert_eq!(
                transport.attempts[0].default_topic_queue_nums,
                expected["defaultTopicQueueNums"].as_i64().unwrap() as i32
            );
        }
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn runtime_send_timeout_applies_to_later_default_send() {
        let (producer, _) =
            scripted_producer(crate::ProducerConfig::new("runtime-send-timeout").unwrap()).await;
        producer.set_send_timeout_millis(0);

        let result = producer
            .send_direct(&mut Message::new("orders", b"expired".to_vec()))
            .await;

        assert!(matches!(result, Err(ProducerError::Timeout)));
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );
        producer.shutdown().await.unwrap();
    }

    #[test]
    fn live_publish_info_mutations_feed_send_routes_and_queue_cursor() {
        let producer = Producer::new(crate::ProducerConfig::new("publish-info-send").unwrap());
        let topic = "orders";
        let first = MessageQueue::new(topic, "broker-a", 0);
        let second = MessageQueue::new(topic, "broker-b", 0);
        let snapshot = Arc::new(TopicSnapshot {
            route: Arc::new(rocketmq_classic_protocol::route::TopicRouteData::default()),
            publish_queues: vec![first, second.clone()],
            subscribe_queues: Default::default(),
            endpoints: BTreeMap::new(),
            order_topic: false,
        });
        producer.cache_publish_info(topic, snapshot);
        let table = producer.get_topic_publish_info_table();
        let info = table.get(topic).unwrap();
        let list = info.message_queue_list().unwrap();
        list.with_mut(|queues| *queues = vec![second.clone()]);
        assert_eq!(
            producer.cached_publish_info(topic).unwrap().publish_queues,
            [second]
        );

        let cursor = ProducerQueueIndex::default();
        info.set_send_which_queue(cursor.clone());
        assert!(cursor.current().is_none());
        let index = producer.next_publish_queue_index(topic, 2);
        assert_eq!(
            index,
            (cursor.current().unwrap() & i32::MAX) as usize % 2,
            "fault selection advances the live TopicPublishInfo ThreadLocalIndex"
        );
    }

    #[tokio::test]
    async fn namespaced_send_uses_wire_names_and_unwraps_the_result() {
        let mut config = crate::ProducerConfig::new("namespace-send").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".into()));
        let (producer, queues) = scripted_producer(config).await;
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        type HookEvents = Arc<std::sync::Mutex<Vec<(String, Option<String>, String)>>>;
        struct Hook(HookEvents);
        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "namespace-test"
            }

            fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push((
                    context.message.topic().to_owned(),
                    context.namespace.clone(),
                    context.queue.topic().unwrap_or_default().to_owned(),
                ));
            }
        }
        producer.register_send_message_hook(Arc::new(Hook(seen.clone())));
        let mut response = scripted_result("msg-ns", "offset-ns", 7);
        response.topic = "tenant%orders".into();
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(response)));

        assert_eq!(
            producer.producer_group().as_deref(),
            Some("tenant%namespace-send")
        );
        let mut message = Message::new("orders", b"namespaced body".to_vec());
        let result = producer
            .send_with_timeout(&mut message, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(message.topic(), "orders");
        assert_eq!(result.topic, "orders");
        {
            let transport = producer.scripted_transport.lock().unwrap();
            assert_eq!(transport.attempts[0].topic, "tenant%orders");
            assert_eq!(transport.attempts[0].queue.topic(), Some("tenant%orders"));
        }
        assert_eq!(queues[0].topic(), Some("tenant%orders"));
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            &[(
                "tenant%orders".to_owned(),
                Some("tenant".to_owned()),
                "tenant%orders".to_owned(),
            )]
        );

        let logical_queue = MessageQueue::new("orders", "broker-a", 0);
        let mut response = scripted_result("msg-ns-queue", "offset-ns-queue", 8);
        response.topic = "tenant%orders".into();
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(response)));
        let queue_result = producer
            .send_to_queue_with_timeout(&mut message, &logical_queue, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(logical_queue.topic(), Some("orders"));
        assert_eq!(queue_result.topic, "orders");

        let mut response = scripted_result("msg-ns-selector", "offset-ns-selector", 9);
        response.topic = "tenant%orders".into();
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(response)));
        let selector_result = producer
            .send_with_selector_with_timeout(
                &mut message,
                |logical_queues, selected_message| {
                    assert_eq!(selected_message.topic(), "orders");
                    assert_eq!(logical_queues[0].topic(), Some("orders"));
                    Some(logical_queues[0].clone())
                },
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(selector_result.topic, "orders");
        {
            let transport = producer.scripted_transport.lock().unwrap();
            assert_eq!(transport.attempts.len(), 3);
            assert!(
                transport
                    .attempts
                    .iter()
                    .all(|attempt| attempt.topic == "tenant%orders")
            );
            assert!(
                transport
                    .attempts
                    .iter()
                    .all(|attempt| attempt.queue.topic() == Some("tenant%orders"))
            );
        }
        assert_eq!(seen.lock().unwrap().len(), 3);
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn configured_async_sender_executor_runs_future_send_attempts() {
        struct TokioSender(Arc<std::sync::atomic::AtomicUsize>);

        impl crate::AsyncSenderExecutor for TokioSender {
            fn execute(&self, task: AsyncSenderTask) -> Result<(), AsyncSenderTask> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::spawn(task);
                Ok(())
            }
        }

        let (producer, _) =
            scripted_producer(crate::ProducerConfig::new("async-executor").unwrap()).await;
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("async-executor", "offset", 1))));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        producer.set_async_sender_executor(Some(Arc::new(TokioSender(calls.clone()))));

        let result = producer
            .send_future(&mut Message::new("orders", b"executor task".to_vec()))
            .await
            .unwrap();

        assert_eq!(result.msg_id, "async-executor");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts[0].mode,
            SendMode::Future
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_future_after_async_submission_does_not_cancel_the_send() {
        struct StartedHook(std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>);
        impl crate::SendMessageHook for StartedHook {
            fn hook_name(&self) -> &str {
                "started-hook"
            }

            fn send_message_before(&self, _context: &mut crate::SendMessageContext<'_>) {
                if let Some(started) = self.0.lock().unwrap().take() {
                    let _ = started.send(());
                }
            }
        }

        let (producer, _) =
            scripted_producer(crate::ProducerConfig::new("async-future-drop").unwrap()).await;
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("detached-send", "offset", 1))));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        producer.register_send_message_hook(Arc::new(StartedHook(std::sync::Mutex::new(Some(
            started_tx,
        )))));

        let sending_producer = producer.clone();
        let send_future = tokio::spawn(async move {
            sending_producer
                .send_future(&mut Message::new("orders", b"drop result future".to_vec()))
                .await
        });
        started_rx.await.unwrap();
        send_future.abort();
        let _ = send_future.await;

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if !producer
                    .scripted_transport
                    .lock()
                    .unwrap()
                    .attempts
                    .is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("submitted send continues after its result future is dropped");
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts.len(),
            1
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn rejected_async_sender_executor_follows_java_backpressure_fallback() {
        struct RejectSender;
        impl crate::AsyncSenderExecutor for RejectSender {
            fn execute(&self, task: AsyncSenderTask) -> Result<(), AsyncSenderTask> {
                Err(task)
            }
        }

        let (producer, _) =
            scripted_producer(crate::ProducerConfig::new("async-reject").unwrap()).await;
        producer.set_async_sender_executor(Some(Arc::new(RejectSender)));
        let error = producer
            .send_future(&mut Message::new("orders", b"rejected".to_vec()))
            .await
            .unwrap_err();
        assert!(matches!(error, ProducerError::AsyncSenderExecutorRejected));
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );
        producer.shutdown().await.unwrap();

        let mut config = crate::ProducerConfig::new("async-reject-backpressure").unwrap();
        config.set_enable_backpressure_for_async_mode(true);
        let (producer, _) = scripted_producer(config).await;
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("inline-fallback", "offset", 2))));
        producer.set_async_sender_executor(Some(Arc::new(RejectSender)));
        let result = producer
            .send_future(&mut Message::new("orders", b"inline fallback".to_vec()))
            .await
            .unwrap();
        assert_eq!(result.msg_id, "inline-fallback");
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts.len(),
            1
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn queued_async_sender_work_obeys_the_send_deadline() {
        struct HeldSender(Arc<std::sync::Mutex<Option<AsyncSenderTask>>>);
        impl crate::AsyncSenderExecutor for HeldSender {
            fn execute(&self, task: AsyncSenderTask) -> Result<(), AsyncSenderTask> {
                *self.0.lock().unwrap() = Some(task);
                Ok(())
            }
        }

        let mut config = crate::ProducerConfig::new("async-executor-deadline").unwrap();
        config.set_enable_backpressure_for_async_mode(true);
        let (producer, _) = scripted_producer(config).await;
        let held_task = Arc::new(std::sync::Mutex::new(None));
        producer.set_async_sender_executor(Some(Arc::new(HeldSender(held_task.clone()))));
        let initial_count_permits = producer.backpressure_send_num.available_permits();
        let initial_size_permits = producer.backpressure_send_size.available_permits();
        let mut message = Message::new("orders", b"deadline before executor runs".to_vec());
        let message_size = message.body().len().max(1);

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            producer.send_future_with_timeout(&mut message, std::time::Duration::from_millis(15)),
        )
        .await
        .expect("the send should observe its deadline without executor progress");
        let error = result.unwrap_err();

        assert!(matches!(error, ProducerError::Timeout));
        assert_eq!(
            producer.backpressure_send_num.available_permits(),
            initial_count_permits - 1
        );
        assert_eq!(
            producer.backpressure_send_size.available_permits(),
            initial_size_permits - i64::try_from(message_size).unwrap()
        );
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );

        let delayed_task = held_task.lock().unwrap().take().unwrap();
        delayed_task.await;
        assert_eq!(
            producer.backpressure_send_num.available_permits(),
            initial_count_permits
        );
        assert_eq!(
            producer.backpressure_send_size.available_permits(),
            initial_size_permits
        );
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn producer_callback_executor_forwards_to_shared_remoting_client() {
        struct InlineCallback;
        impl rocketmq_classic_remoting::client::CallbackExecutor for InlineCallback {
            fn execute(
                &self,
                task: Box<dyn FnOnce() + Send + 'static>,
            ) -> Result<(), Box<dyn FnOnce() + Send + 'static>> {
                task();
                Ok(())
            }
        }

        let (producer, _) =
            scripted_producer(crate::ProducerConfig::new("callback-executor").unwrap()).await;
        let runtime = producer.started_runtime().await.unwrap();
        let executor: Arc<dyn rocketmq_classic_remoting::client::CallbackExecutor> =
            Arc::new(InlineCallback);
        producer
            .set_callback_executor(Some(executor.clone()))
            .await
            .unwrap();
        let current = runtime.remoting().get_callback_executor().unwrap();
        assert!(Arc::ptr_eq(&executor, &current));

        producer.set_callback_executor(None).await.unwrap();
        assert!(runtime.remoting().get_callback_executor().is_some());
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn scripted_two_broker_retry_preserves_body_and_unique_id() {
        let mut config = crate::ProducerConfig::new("scripted-retry").unwrap();
        config.set_retry_times_when_send_failed(0);
        let (producer, _) = scripted_producer(config).await;
        producer.set_retry_times_when_send_failed(1);
        {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            scripted.responses.extend([
                Err(ProducerError::Rpc(RpcError::Response {
                    request_code: 10,
                    peer: "broker".into(),
                    code: response::SERVICE_NOT_AVAILABLE,
                    remark: Some("retry this request".into()),
                })),
                Ok(Some(scripted_result("msg-1", "offset-1", 7))),
            ]);
        }

        let mut message = Message::new("orders", b"same immutable body".to_vec());
        let result = producer
            .send_with_timeout(&mut message, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(result.msg_id, "msg-1");
        assert_eq!(message.body().as_ref(), b"same immutable body");
        let unique_id = message
            .get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .expect("Java send writes its prepared ID to the caller message");
        assert!(matches!(unique_id.len(), 32 | 56));

        let attempts = {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            std::mem::take(&mut scripted.attempts)
        };
        assert_eq!(attempts.len(), 2);
        assert_ne!(attempts[0].broker, attempts[1].broker);
        assert_ne!(attempts[0].address, attempts[1].address);
        assert_eq!(attempts[0].body, attempts[1].body);
        assert_eq!(attempts[0].unique_id, attempts[1].unique_id);
        assert_eq!(attempts[0].unique_id.as_str(), unique_id.as_str());
        assert_eq!(attempts[0].mode, SendMode::Sync);
        assert_eq!(attempts[1].mode, SendMode::Sync);
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn runtime_non_ok_store_retry_switches_brokers_on_later_sends() {
        let (producer, _) =
            scripted_producer(crate::ProducerConfig::new("runtime-non-ok-retry").unwrap()).await;
        producer.set_retry_another_broker_when_not_store_ok(true);
        let mut stored_not_ok = scripted_result("not-ok", "offset-1", 1);
        stored_not_ok.status = SendStatus::FlushDiskTimeout;
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .extend([
                Ok(Some(stored_not_ok)),
                Ok(Some(scripted_result("retry-ok", "offset-2", 2))),
            ]);

        let result = producer
            .send_with_timeout(
                &mut Message::new("orders", b"retry a stored response".to_vec()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(result.msg_id, "retry-ok");
        {
            let transport = producer.scripted_transport.lock().unwrap();
            assert_eq!(transport.attempts.len(), 2);
            assert_ne!(transport.attempts[0].broker, transport.attempts[1].broker);
        }
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn zero_per_request_timeout_retries_sync_attempts_within_total_budget() {
        struct Hook(std::sync::Arc<std::sync::atomic::AtomicUsize>);

        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "zero-per-request-timeout-retry"
            }

            fn send_message_before(&self, _context: &mut SendMessageContext<'_>) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }

            fn send_message_after(&self, _context: &mut SendMessageContext<'_>) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let mut config = crate::ProducerConfig::new("zero-per-request-timeout-retry").unwrap();
        config.set_send_msg_max_timeout_per_request(0);
        config.set_retry_times_when_send_failed(1);
        let (producer, _) = scripted_producer(config).await;
        let hook_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        producer.register_send_message_hook(Arc::new(Hook(hook_calls.clone())));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Err(ProducerError::Timeout));

        let result = producer
            .send_with_timeout(
                &mut Message::new("orders", b"zero per-request timeout".to_vec()),
                Duration::from_secs(3),
            )
            .await;
        assert!(matches!(result, Err(ProducerError::Timeout)), "{result:?}");

        let java: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        assert_eq!(
            java["perRequestTimeoutRetry"]["retryTimesWhenSendFailed"],
            1
        );
        assert_eq!(java["perRequestTimeoutRetry"]["sendMsgTimeout"], 3_000);
        assert_eq!(
            java["perRequestTimeoutRetry"]["sendMsgMaxTimeoutPerRequest"],
            0
        );
        assert_eq!(java["perRequestTimeoutRetry"]["beforeAttempts"], 2);
        assert_eq!(java["perRequestTimeoutRetry"]["afterAttempts"], 2);
        assert_eq!(
            hook_calls.load(std::sync::atomic::Ordering::SeqCst),
            4,
            "Java retries the local request-timeout exception while total budget remains"
        );
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts.len(),
            1
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn send_message_hooks_run_in_order_for_each_attempt_and_keep_context_state() {
        struct Hook(Arc<std::sync::Mutex<Vec<String>>>);

        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "send-hook-test"
            }

            fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
                context.message.set_tags("hooked-tag").unwrap();
                context
                    .properties
                    .get_or_insert_with(Default::default)
                    .insert("hook-state".into(), "retained".into());
                context.trace_context = Some(Box::new(17_u8));
                self.0.lock().unwrap().push("before".into());
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                assert_eq!(
                    context
                        .properties
                        .as_ref()
                        .and_then(|values| values.get("hook-state"))
                        .map(String::as_str),
                    Some("retained")
                );
                assert_eq!(
                    context
                        .trace_context
                        .as_deref()
                        .and_then(|value| value.downcast_ref::<u8>()),
                    Some(&17)
                );
                self.0
                    .lock()
                    .unwrap()
                    .push(if context.send_result.is_some() {
                        "after-success".into()
                    } else {
                        assert!(context.exception.is_some());
                        "after-error".into()
                    });
            }
        }

        let mut config = crate::ProducerConfig::new("send-hook-lifecycle").unwrap();
        config.set_retry_times_when_send_failed(1);
        let (producer, _) = scripted_producer(config).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_send_message_hook(Arc::new(Hook(events.clone())));
        {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            scripted.responses.extend([
                Err(ProducerError::Rpc(RpcError::Response {
                    request_code: 10,
                    peer: "broker".into(),
                    code: response::SERVICE_NOT_AVAILABLE,
                    remark: Some("retry this request".into()),
                })),
                Ok(Some(scripted_result("hooked-msg", "hooked-offset", 9))),
            ]);
        }

        let mut message = Message::new("orders", b"hook body".to_vec());
        producer
            .send_with_timeout(&mut message, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["before", "after-error", "before", "after-success"]
        );
        let attempts = {
            let scripted = producer.scripted_transport.lock().unwrap();
            assert_eq!(scripted.attempts.len(), 2);
            scripted.attempts.clone()
        };
        for attempt in &attempts {
            let properties = decode_properties(attempt.properties.as_deref().unwrap());
            assert_eq!(
                properties.get(PROPERTY_TAGS).map(String::as_str),
                Some("hooked-tag")
            );
        }
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn send_message_hook_panics_are_isolated_like_java() {
        struct PanickingHook;

        impl SendMessageHook for PanickingHook {
            fn hook_name(&self) -> &str {
                "panicking-send-hook"
            }

            fn send_message_before(&self, _context: &mut SendMessageContext<'_>) {
                panic!("before hook failure");
            }

            fn send_message_after(&self, _context: &mut SendMessageContext<'_>) {
                panic!("after hook failure");
            }
        }

        struct RecordingHook(Arc<std::sync::Mutex<Vec<String>>>);

        impl SendMessageHook for RecordingHook {
            fn hook_name(&self) -> &str {
                "recording-send-hook"
            }

            fn send_message_before(&self, _context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push("before".into());
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                assert!(context.send_result.is_some());
                self.0.lock().unwrap().push("after".into());
            }
        }

        let (producer, _) =
            scripted_producer(crate::ProducerConfig::new("send-hook-panic").unwrap()).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_send_message_hook(Arc::new(PanickingHook));
        producer.register_send_message_hook(Arc::new(RecordingHook(events.clone())));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("panic-isolated", "offset", 1))));

        let result = producer
            .send_with_timeout(
                &mut Message::new("orders", b"hook body".to_vec()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(result.msg_id, "panic-isolated");
        assert_eq!(events.lock().unwrap().as_slice(), ["before", "after"]);
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn send_message_hook_queue_mutation_changes_the_request_queue() {
        struct Hook;
        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "send-hook-queue-mutation"
            }

            fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
                context.queue.set_queue_id(7);
            }
        }

        let config = crate::ProducerConfig::new("send-hook-queue-mutation").unwrap();
        let (producer, queues) = scripted_producer(config).await;
        producer.register_send_message_hook(Arc::new(Hook));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("hook-queue", "hook-offset", 1))));

        producer
            .send_with_timeout(
                &mut Message::new("orders", b"hook queue".to_vec()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        {
            let scripted = producer.scripted_transport.lock().unwrap();
            assert_eq!(scripted.attempts.len(), 1);
            assert_eq!(scripted.attempts[0].queue.queue_id(), 7);
        }
        assert_eq!(queues[0].queue_id(), 0);
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn send_message_hook_result_mutation_reaches_the_returned_result() {
        struct Hook;
        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "send-hook-result-mutation"
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                if let Some(result) = &mut context.send_result {
                    result.msg_id = "hook-mutated-id".into();
                }
            }
        }

        let config = crate::ProducerConfig::new("send-hook-result-mutation").unwrap();
        let (producer, _) = scripted_producer(config).await;
        producer.register_send_message_hook(Arc::new(Hook));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("original-id", "offset", 1))));

        let result = producer
            .send_with_timeout(
                &mut Message::new("orders", b"hook result".to_vec()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(result.msg_id, "hook-mutated-id");
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn check_forbidden_hook_runs_before_send_hooks_and_can_adjust_message_and_queue() {
        struct Forbidden(Arc<std::sync::Mutex<Vec<&'static str>>>);

        impl CheckForbiddenHook for Forbidden {
            fn hook_name(&self) -> &str {
                "forbidden-context-test"
            }

            fn check_forbidden(
                &self,
                context: &mut CheckForbiddenContext<'_>,
            ) -> Result<(), ProducerError> {
                assert_eq!(context.name_srv_addr, None);
                assert_eq!(context.group.as_deref(), Some("check-forbidden-order"));
                assert_eq!(context.communication_mode, CommunicationMode::Sync);
                assert_eq!(context.broker_addr, "10.0.0.1:10911");
                assert_eq!(context.queue.broker_name(), Some("broker-a"));
                assert_eq!(context.queue.queue_id(), 0);
                assert_eq!(context.send_result, None);
                assert!(context.exception.is_none());
                assert!(context.arg.is_none());
                assert!(!context.unit_mode);
                context.message.set_tags("checked").unwrap();
                context.queue.set_queue_id(1);
                self.0.lock().unwrap().push("forbidden");
                Ok(())
            }
        }

        struct Send(Arc<std::sync::Mutex<Vec<&'static str>>>);

        impl SendMessageHook for Send {
            fn hook_name(&self) -> &str {
                "send-after-forbidden-test"
            }

            fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
                assert_eq!(context.message.tags().as_deref(), Some("checked"));
                self.0.lock().unwrap().push("send-before");
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                assert!(context.send_result.is_some());
                self.0.lock().unwrap().push("send-after");
            }
        }

        let (producer, queues) =
            scripted_producer(crate::ProducerConfig::new("check-forbidden-order").unwrap()).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_check_forbidden_hook(Arc::new(Forbidden(events.clone())));
        producer.register_send_message_hook(Arc::new(Send(events.clone())));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result(
                "checked-message",
                "checked-offset",
                3,
            ))));

        producer
            .send_to_queue_with_timeout(
                &mut Message::new("orders", b"checked body".to_vec()),
                &queues[0],
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["forbidden", "send-before", "send-after"]
        );
        let attempts = producer.scripted_transport.lock().unwrap().attempts.clone();
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].queue.queue_id(), 1);
        assert_eq!(
            decode_properties(attempts[0].properties.as_deref().unwrap())
                .get(PROPERTY_TAGS)
                .map(String::as_str),
            Some("checked")
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn rejecting_check_forbidden_hook_aborts_before_rpc_and_send_hooks() {
        struct Reject;

        impl CheckForbiddenHook for Reject {
            fn hook_name(&self) -> &str {
                "reject-send-test"
            }

            fn check_forbidden(
                &self,
                _context: &mut CheckForbiddenContext<'_>,
            ) -> Result<(), ProducerError> {
                Err(ProducerError::ForbiddenTopic)
            }
        }

        let (producer, queues) =
            scripted_producer(crate::ProducerConfig::new("reject-send").unwrap()).await;
        producer.register_check_forbidden_hook(Arc::new(Reject));
        let error = producer
            .send_to_queue_with_timeout(
                &mut Message::new("orders", b"blocked body".to_vec()),
                &queues[0],
                Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ProducerError::ForbiddenTopic));
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn default_future_check_forbidden_error_matches_java_async_wrapper() {
        struct Reject(std::sync::Arc<std::sync::atomic::AtomicUsize>);

        impl CheckForbiddenHook for Reject {
            fn hook_name(&self) -> &str {
                "default-future-reject"
            }

            fn check_forbidden(
                &self,
                _context: &mut CheckForbiddenContext<'_>,
            ) -> Result<(), ProducerError> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err(ProducerError::ForbiddenTopic)
            }
        }

        let mut config = crate::ProducerConfig::new("check-forbidden-async").unwrap();
        config.set_retry_times_when_send_async_failed(3);
        let (producer, _) = scripted_producer(config).await;
        producer.set_send_latency_fault_enable(true);
        let hook_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        producer.register_check_forbidden_hook(Arc::new(Reject(hook_calls.clone())));

        let error = producer
            .send_future_with_timeout(
                &mut Message::new("orders", b"blocked by async check hook".to_vec()),
                Duration::from_secs(3),
            )
            .await
            .unwrap_err();
        let ProducerError::SendRetriesExhausted {
            message,
            response_code,
            source,
        } = &error
        else {
            panic!("expected Java-compatible async wrapper, got {error:?}");
        };

        let java: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        assert_eq!(
            java["checkForbiddenDefaultAsync"]["retryTimesWhenSendAsyncFailed"],
            3
        );
        assert_eq!(java["checkForbiddenDefaultAsync"]["checkCalls"], 1);
        assert_eq!(java["checkForbiddenDefaultAsync"]["sendBeforeCalls"], 0);
        assert_eq!(java["checkForbiddenDefaultAsync"]["sendAfterCalls"], 0);
        assert_eq!(
            java["checkForbiddenDefaultAsync"]["outcome"],
            "MQClientException"
        );
        assert_eq!(java["checkForbiddenDefaultAsync"]["responseCode"], 10003);
        assert_eq!(
            java["checkForbiddenDefaultAsync"]["messageContainsAttemptCount"],
            true
        );
        assert_eq!(
            java["checkForbiddenDefaultAsync"]["causeClass"],
            "MQClientException"
        );
        assert_eq!(*response_code, 10003);
        assert!(message.contains("Send [1] times, still failed"));
        assert!(message.contains("Topic: orders"));
        assert!(message.contains("BrokersSent: ["));
        assert!(message.contains("broker-a") || message.contains("broker-b"));
        assert!(matches!(source.as_ref(), ProducerError::ForbiddenTopic));
        assert_eq!(hook_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            producer
                .fault
                .take_due_for_detection(StdInstant::now(), Duration::from_secs(2))
                .len(),
            1,
            "default async hook failures update the reachable Broker fault entry"
        );
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn explicit_queue_future_check_forbidden_keeps_java_kernel_error() {
        struct Reject(std::sync::Arc<std::sync::atomic::AtomicUsize>);

        impl CheckForbiddenHook for Reject {
            fn hook_name(&self) -> &str {
                "explicit-future-reject"
            }

            fn check_forbidden(
                &self,
                _context: &mut CheckForbiddenContext<'_>,
            ) -> Result<(), ProducerError> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err(ProducerError::ForbiddenTopic)
            }
        }

        let (producer, queues) =
            scripted_producer(crate::ProducerConfig::new("explicit-future-reject").unwrap()).await;
        let hook_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        producer.register_check_forbidden_hook(Arc::new(Reject(hook_calls.clone())));

        let error = producer
            .send_to_queue_direct_future_with_timeout(
                &mut Message::new("orders", b"blocked by explicit async hook".to_vec()),
                &queues[0],
                Duration::from_secs(3),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ProducerError::ForbiddenTopic));

        let java: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        assert_eq!(java["checkForbiddenExplicitAsync"]["checkCalls"], 1);
        assert_eq!(java["checkForbiddenExplicitAsync"]["sendBeforeCalls"], 0);
        assert_eq!(java["checkForbiddenExplicitAsync"]["sendAfterCalls"], 0);
        assert_eq!(
            java["checkForbiddenExplicitAsync"]["outcome"],
            "MQClientException"
        );
        assert_eq!(java["checkForbiddenExplicitAsync"]["responseCode"], -1);
        assert_eq!(
            java["checkForbiddenExplicitAsync"]["messageContainsAttemptCount"],
            false
        );
        assert_eq!(java["checkForbiddenExplicitAsync"]["causeClass"], "none");
        assert_eq!(hook_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn default_sync_send_retries_check_forbidden_hook_errors_like_java() {
        struct Reject(std::sync::Arc<std::sync::atomic::AtomicUsize>);

        impl CheckForbiddenHook for Reject {
            fn hook_name(&self) -> &str {
                "default-send-reject-retry"
            }

            fn check_forbidden(
                &self,
                _context: &mut CheckForbiddenContext<'_>,
            ) -> Result<(), ProducerError> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err(ProducerError::ForbiddenTopic)
            }
        }

        let mut config = crate::ProducerConfig::new("check-forbidden-retry").unwrap();
        config.set_retry_times_when_send_failed(1);
        let (producer, _) = scripted_producer(config).await;
        let hook_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        producer.register_check_forbidden_hook(Arc::new(Reject(hook_calls.clone())));

        let error = producer
            .send_with_timeout(
                &mut Message::new("orders", b"blocked by check hook".to_vec()),
                Duration::from_secs(3),
            )
            .await
            .unwrap_err();
        let ProducerError::SendRetriesExhausted {
            message,
            response_code,
            source,
        } = &error
        else {
            panic!("expected Java-compatible retry exhaustion, got {error:?}");
        };

        let java: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        assert_eq!(java["checkForbiddenRetry"]["retryTimesWhenSendFailed"], 1);
        assert_eq!(java["checkForbiddenRetry"]["checkCalls"], 2);
        assert_eq!(java["checkForbiddenRetry"]["sendBeforeCalls"], 0);
        assert_eq!(java["checkForbiddenRetry"]["sendAfterCalls"], 0);
        assert_eq!(java["checkForbiddenRetry"]["outcome"], "MQClientException");
        assert_eq!(java["checkForbiddenRetry"]["responseCode"], 10003);
        assert_eq!(*response_code, java["checkForbiddenRetry"]["responseCode"]);
        assert_eq!(
            java["checkForbiddenRetry"]["messageContainsAttemptCount"],
            true
        );
        assert!(message.contains("Topic: orders"));
        assert!(message.contains("BrokersSent: ["));
        assert!(message.contains("broker-a"));
        assert!(message.contains("broker-b"));
        assert!(matches!(source.as_ref(), ProducerError::ForbiddenTopic));
        assert!(
            error.to_string().contains("Send [2] times, still failed"),
            "the Java retry-exhausted MQClientException includes the attempt count"
        );
        assert_eq!(
            hook_calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "Java retries a synchronous MQClientException from CheckForbiddenHook"
        );
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn default_sync_check_forbidden_deadline_exhaustion_matches_java() {
        struct SlowReject(std::sync::Arc<std::sync::atomic::AtomicUsize>);

        impl CheckForbiddenHook for SlowReject {
            fn hook_name(&self) -> &str {
                "default-send-slow-reject-deadline"
            }

            fn check_forbidden(
                &self,
                _context: &mut CheckForbiddenContext<'_>,
            ) -> Result<(), ProducerError> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(80));
                Err(ProducerError::ForbiddenTopic)
            }
        }

        let mut config = crate::ProducerConfig::new("check-forbidden-deadline").unwrap();
        config.set_retry_times_when_send_failed(1);
        let (producer, _) = scripted_producer(config).await;
        let hook_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        producer.register_check_forbidden_hook(Arc::new(SlowReject(hook_calls.clone())));

        let error = producer
            .send_with_timeout(
                &mut Message::new("orders", b"deadline exhausted in hook".to_vec()),
                Duration::from_millis(20),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ProducerError::Timeout));

        let java: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        assert_eq!(java["checkForbiddenDeadline"]["sendMsgTimeout"], 20);
        assert_eq!(
            java["checkForbiddenDeadline"]["retryTimesWhenSendFailed"],
            1
        );
        assert_eq!(java["checkForbiddenDeadline"]["checkCalls"], 1);
        assert_eq!(java["checkForbiddenDeadline"]["sendBeforeCalls"], 0);
        assert_eq!(java["checkForbiddenDeadline"]["sendAfterCalls"], 0);
        assert_eq!(
            java["checkForbiddenDeadline"]["outcome"],
            "RemotingTooMuchRequestException"
        );
        assert_eq!(
            hook_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the overall deadline prevents Java's next retry attempt"
        );
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn send_hooks_observe_compressed_body_before_and_after_sync_attempt() {
        type BodyLengths = Arc<std::sync::Mutex<Vec<usize>>>;
        struct Hook(BodyLengths);
        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "compressed-body-hook"
            }

            fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push(context.message.body().len());
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push(context.message.body().len());
            }
        }

        let mut config = crate::ProducerConfig::new("compressed-body-hook").unwrap();
        config.set_compress_threshold(1);
        let (producer, _) = scripted_producer(config).await;
        let body_lengths = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_send_message_hook(Arc::new(Hook(body_lengths.clone())));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result(
                "compressed-hook-msg",
                "compressed-hook-offset",
                11,
            ))));

        let body = vec![b'x'; 2048];
        producer
            .send_with_timeout(
                &mut Message::new("orders", body.clone()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        let attempt = producer.scripted_transport.lock().unwrap().attempts[0].clone();
        {
            let body_lengths = body_lengths.lock().unwrap();
            assert_eq!(body_lengths.len(), 2);
            assert!(
                body_lengths[0] < body.len(),
                "before hook sees compressed body"
            );
            assert_eq!(body_lengths[0], body_lengths[1]);
            assert_eq!(body_lengths[1], attempt.body.len());
        }
        assert_ne!(attempt.sys_flag & message::COMPRESSED_FLAG, 0);
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn async_send_retries_share_one_hook_context_until_terminal_result() {
        struct Hook(Arc<std::sync::Mutex<Vec<&'static str>>>);

        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "async-retry-hook"
            }

            fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
                context
                    .properties
                    .get_or_insert_with(Default::default)
                    .insert("hook-state".into(), "retained".into());
                self.0.lock().unwrap().push("before");
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                assert_eq!(
                    context
                        .properties
                        .as_ref()
                        .and_then(|properties| properties.get("hook-state"))
                        .map(String::as_str),
                    Some("retained")
                );
                self.0
                    .lock()
                    .unwrap()
                    .push(if context.send_result.is_some() {
                        "after-success"
                    } else if context.exception.is_some() {
                        "after-error"
                    } else {
                        "after-pending"
                    });
            }
        }

        let mut config = crate::ProducerConfig::new("async-retry-hook").unwrap();
        config.set_retry_times_when_send_async_failed(1);
        let (producer, _) = scripted_producer(config).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_send_message_hook(Arc::new(Hook(events.clone())));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .extend([
                Err(ProducerError::Rpc(RpcError::Response {
                    request_code: 10,
                    peer: "broker".into(),
                    code: response::SERVICE_NOT_AVAILABLE,
                    remark: Some("retry this request".into()),
                })),
                Ok(Some(scripted_result(
                    "async-retry-msg",
                    "async-retry-offset",
                    9,
                ))),
            ]);

        producer
            .send_future_with_timeout(
                &mut Message::new("orders", b"async body".to_vec()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["before", "after-pending", "after-success"]
        );
        let attempts = producer.scripted_transport.lock().unwrap().attempts.clone();
        assert_eq!(attempts.len(), 2);
        assert_ne!(attempts[0].broker, attempts[1].broker);
        assert_eq!(attempts[0].body, attempts[1].body);
        assert_eq!(attempts[0].unique_id, attempts[1].unique_id);
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn explicit_queue_future_retries_on_the_same_broker_with_one_hook_lifecycle() {
        struct Hook(Arc<std::sync::Mutex<Vec<&'static str>>>);

        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "explicit-async-retry-hook"
            }

            fn send_message_before(&self, _context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push("before");
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                self.0
                    .lock()
                    .unwrap()
                    .push(if context.send_result.is_some() {
                        "after-success"
                    } else if context.exception.is_some() {
                        "after-error"
                    } else {
                        "after-pending"
                    });
            }
        }

        let mut config = crate::ProducerConfig::new("explicit-async-retry-hook").unwrap();
        config.set_retry_times_when_send_async_failed(1);
        let (producer, queues) = scripted_producer(config).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_send_message_hook(Arc::new(Hook(events.clone())));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .extend([
                Err(ProducerError::Rpc(RpcError::Response {
                    request_code: 10,
                    peer: "broker".into(),
                    code: response::SERVICE_NOT_AVAILABLE,
                    remark: Some("retry this request".into()),
                })),
                Ok(Some(scripted_result(
                    "explicit-async-msg",
                    "explicit-async-offset",
                    9,
                ))),
            ]);

        producer
            .send_to_queue_future_with_timeout(
                &mut Message::new("orders", b"async body".to_vec()),
                &queues[0],
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["before", "after-pending", "after-success"]
        );
        let attempts = producer.scripted_transport.lock().unwrap().attempts.clone();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].broker, attempts[1].broker);
        assert_eq!(attempts[0].address, attempts[1].address);
        assert_eq!(attempts[0].queue, queues[0]);
        assert_eq!(attempts[1].queue, queues[0]);
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn async_send_hooks_report_submission_and_terminal_failure_once_each() {
        struct Hook(Arc<std::sync::Mutex<Vec<&'static str>>>);

        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "async-terminal-failure-hook"
            }

            fn send_message_before(&self, _context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push("before");
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                self.0
                    .lock()
                    .unwrap()
                    .push(if context.send_result.is_some() {
                        "after-success"
                    } else if context.exception.is_some() {
                        "after-error"
                    } else {
                        "after-pending"
                    });
            }
        }

        let mut config = crate::ProducerConfig::new("async-terminal-failure-hook").unwrap();
        config.set_retry_times_when_send_async_failed(1);
        let (producer, _) = scripted_producer(config).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_send_message_hook(Arc::new(Hook(events.clone())));
        let unavailable = || {
            Err(ProducerError::Rpc(RpcError::Response {
                request_code: 10,
                peer: "broker".into(),
                code: response::SERVICE_NOT_AVAILABLE,
                remark: Some("still unavailable".into()),
            }))
        };
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .extend([unavailable(), unavailable()]);

        assert!(matches!(
            producer
                .send_future_with_timeout(
                    &mut Message::new("orders", b"async body".to_vec()),
                    Duration::from_secs(1)
                )
                .await,
            Err(ProducerError::Rpc(RpcError::Response { .. }))
        ));
        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["before", "after-pending", "after-error"]
        );
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts.len(),
            2
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn scripted_async_and_oneway_failures_keep_their_java_retry_policies() {
        let mut config = crate::ProducerConfig::new("scripted-mode-policy").unwrap();
        config.set_retry_times_when_send_async_failed(0);
        let (producer, queues) = scripted_producer(config).await;
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Err(ProducerError::Rpc(RpcError::Response {
                request_code: 10,
                peer: "broker".into(),
                code: response::SERVICE_NOT_AVAILABLE,
                remark: None,
            })));
        assert!(matches!(
            producer
                .send_future_with_timeout(
                    &mut Message::new("orders", b"async".to_vec()),
                    Duration::from_secs(1)
                )
                .await,
            Err(ProducerError::Rpc(RpcError::Response { .. }))
        ));
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts.len(),
            1
        );

        {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            scripted.responses.clear();
            scripted.responses.push_back(Ok(None));
        }
        producer
            .send_to_queue_oneway(&mut Message::new("orders", b"oneway".to_vec()), &queues[0])
            .await
            .unwrap();
        {
            let scripted = producer.scripted_transport.lock().unwrap();
            assert_eq!(scripted.attempts.len(), 2);
            assert_eq!(scripted.attempts[1].mode, SendMode::Oneway);
            assert_eq!(scripted.attempts[1].queue, queues[0]);
        }

        {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            scripted.responses.clear();
            scripted.responses.push_back(Ok(Some(scripted_result(
                "selected-msg",
                "selected-offset",
                9,
            ))));
        }
        producer
            .send_with_selector_with_timeout(
                &mut Message::new("orders", b"selected".to_vec()),
                |publish_queues, _| Some(publish_queues[1].clone()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts[2].queue,
            queues[1]
        );

        {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            scripted.responses.clear();
            scripted.responses.push_back(Ok(Some(scripted_result(
                "queue-msg",
                "queue-offset",
                10,
            ))));
        }
        producer
            .send_to_queue_with_timeout(
                &mut Message::new("orders", b"queue".to_vec()),
                &queues[0],
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        {
            let scripted = producer.scripted_transport.lock().unwrap();
            assert_eq!(scripted.attempts.len(), 4);
            assert_eq!(scripted.attempts[3].queue, queues[0]);
        }
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn oneway_send_hook_receives_submission_and_empty_result_context() {
        struct Hook(Arc<std::sync::Mutex<Vec<(String, bool, bool)>>>);

        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "oneway-send-hook"
            }

            fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push((
                    format!("before:{}", context.communication_mode.java_name()),
                    context.send_result.is_some(),
                    context.exception.is_some(),
                ));
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push((
                    format!("after:{}", context.communication_mode.java_name()),
                    context.send_result.is_some(),
                    context.exception.is_some(),
                ));
            }
        }

        let (producer, queues) =
            scripted_producer(crate::ProducerConfig::new("oneway-send-hook").unwrap()).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_send_message_hook(Arc::new(Hook(events.clone())));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(None));

        producer
            .send_to_queue_oneway(&mut Message::new("orders", b"oneway".to_vec()), &queues[0])
            .await
            .unwrap();
        assert_eq!(
            events.lock().unwrap().as_slice(),
            [
                ("before:ONEWAY".into(), false, false),
                ("after:ONEWAY".into(), false, false),
            ]
        );
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts.len(),
            1
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn explicit_batch_send_runs_hooks_for_one_batch_request() {
        struct Hook(Arc<std::sync::Mutex<Vec<(String, usize, SendMessageType)>>>);

        impl SendMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "batch-send-hook"
            }

            fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push((
                    "before".into(),
                    context.message.body().len(),
                    context.message_type,
                ));
            }

            fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
                self.0.lock().unwrap().push((
                    "after".into(),
                    context.message.body().len(),
                    context.message_type,
                ));
            }
        }

        let (producer, _) =
            scripted_producer(crate::ProducerConfig::new("batch-send-hook").unwrap()).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        producer.register_send_message_hook(Arc::new(Hook(events.clone())));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("batch-result", "batch-offset", 1))));

        let mut messages = [
            Message::new("orders", b"first".to_vec()),
            Message::new("orders", b"second".to_vec()),
        ];
        producer.send_batch(&mut messages).await.unwrap();
        {
            let events = events.lock().unwrap();
            assert_eq!(events.len(), 2);
            assert_eq!(events[0].0, "before");
            assert_eq!(events[1].0, "after");
            assert!(events[0].1 > 0);
            assert_eq!(events[0].1, events[1].1);
            assert_eq!(events[0].2, SendMessageType::Normal);
            assert_eq!(events[1].2, SendMessageType::Normal);
        }
        {
            let transport = producer.scripted_transport.lock().unwrap();
            assert_eq!(transport.attempts.len(), 1);
            assert!(transport.attempts[0].batch);
        }
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn producer_send_uses_java_send_smart_and_reply_request_code_selection() {
        const CHILD_PROPERTY: &str = "ROCKETMQ_TEST_SEND_SMART_PROPERTY_CHILD";
        const SEND_SMART_PROPERTY: &str = "org.apache.rocketmq.client.sendSmartMsg";

        match std::env::var(CHILD_PROPERTY).as_deref() {
            Ok("false") | Ok("TRUE") => {
                let smart = std::env::var(CHILD_PROPERTY).unwrap() == "TRUE";
                let (producer, queues) = scripted_producer(
                    crate::ProducerConfig::new("send-smart-request-code").unwrap(),
                )
                .await;
                producer
                    .scripted_transport
                    .lock()
                    .unwrap()
                    .responses
                    .extend([
                        Ok(Some(scripted_result("normal", "normal-offset", 1))),
                        Ok(Some(scripted_result("reply", "reply-offset", 2))),
                        Ok(Some(scripted_result(
                            "batch-a,batch-b",
                            "batch-offset-a,batch-offset-b",
                            3,
                        ))),
                    ]);

                let mut normal = Message::new("orders", b"normal".to_vec());
                producer
                    .send_to_queue(&mut normal, &queues[0])
                    .await
                    .unwrap();
                let mut reply = Message::new("orders", b"reply".to_vec());
                reply
                    .set_system_property(PROPERTY_MESSAGE_TYPE, REPLY_MESSAGE_FLAG)
                    .unwrap();
                producer
                    .send_to_queue(&mut reply, &queues[0])
                    .await
                    .unwrap();
                let mut batch = [
                    Message::new("orders", b"batch-a".to_vec()),
                    Message::new("orders", b"batch-b".to_vec()),
                ];
                producer.send_batch(&mut batch).await.unwrap();

                let attempts = producer.scripted_transport.lock().unwrap().attempts.clone();
                assert_eq!(attempts.len(), 3);
                assert_eq!(
                    attempts
                        .iter()
                        .map(|attempt| attempt.send_kind)
                        .collect::<Vec<_>>(),
                    if smart {
                        vec![SendKind::V2, SendKind::ReplyV2, SendKind::Batch]
                    } else {
                        vec![SendKind::V1, SendKind::ReplyV1, SendKind::Batch]
                    }
                );
                assert_eq!(
                    decode_properties(attempts[1].properties.as_deref().unwrap())
                        .get(PROPERTY_MESSAGE_TYPE)
                        .map(String::as_str),
                    Some(REPLY_MESSAGE_FLAG)
                );
                producer.shutdown().await.unwrap();
            }
            _ => {
                for value in ["false", "TRUE"] {
                    let output = std::process::Command::new(std::env::current_exe().unwrap())
                        .args([
                            "--exact",
                            "send::tests::producer_send_uses_java_send_smart_and_reply_request_code_selection",
                            "--nocapture",
                        ])
                        .env(CHILD_PROPERTY, value)
                        .env(SEND_SMART_PROPERTY, value)
                        .output()
                        .expect("spawn isolated producer send test process");
                    assert!(
                        output.status.success(),
                        "{value} child test failed:\n{}\n{}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn auto_batch_flushes_one_message_batch_and_fans_out_results() {
        let mut config = crate::ProducerConfig::new("scripted-auto-batch").unwrap();
        config.set_batch_max_delay_millis(15);
        let (producer, _) = scripted_producer(config).await;
        assert!(!producer.auto_batch());
        producer.set_auto_batch(true);
        assert!(producer.auto_batch());
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result(
                "msg-a,msg-b",
                "offset-a,offset-b",
                20,
            ))));
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("msg-c", "offset-c", 22))));

        let mut first = Message::new("orders", b"first".to_vec());
        first.set_tags("blue").unwrap();
        first.set_keys("key-a").unwrap();
        let mut second = Message::new("orders", b"second".to_vec());
        second.set_tags("blue").unwrap();
        second.set_keys("key-b").unwrap();
        let (first_result, second_result) =
            tokio::join!(producer.send(&mut first), producer.send(&mut second));
        let first_result = first_result.unwrap();
        let second_result = second_result.unwrap();
        assert_eq!(first_result.msg_id, "msg-a");
        assert_eq!(second_result.msg_id, "msg-b");
        assert_eq!(first_result.queue_offset, 20);
        assert_eq!(second_result.queue_offset, 21);
        let first_unique_id = first
            .get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .expect("auto-batched Java send mutates the first caller message");
        let second_unique_id = second
            .get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .expect("auto-batched Java send mutates the second caller message");
        assert_ne!(first_unique_id, second_unique_id);
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        assert_eq!(
            fixture["uniqueId"]["supportedIpBytes"].as_bool(),
            Some(true)
        );
        assert_eq!(
            fixture["uniqueId"]["lengthMatchesIpBytes"].as_bool(),
            Some(true)
        );
        assert!(matches!(first_unique_id.len(), 32 | 56));
        assert!(matches!(second_unique_id.len(), 32 | 56));

        producer.set_auto_batch(false);
        assert!(!producer.auto_batch());
        let mut third = Message::new("orders", b"third".to_vec());
        let third_result = producer.send(&mut third).await.unwrap();
        assert_eq!(third_result.msg_id, "msg-c");
        assert_eq!(third_result.queue_offset, 22);

        let attempts = {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            std::mem::take(&mut scripted.attempts)
        };
        assert_eq!(attempts.len(), 2);
        assert!(attempts[0].batch);
        assert_eq!(attempts[0].mode, SendMode::Sync);
        assert!(!attempts[1].batch);
        let properties = decode_properties(attempts[0].properties.as_deref().unwrap());
        assert_eq!(
            properties.get(PROPERTY_TAGS).map(String::as_str),
            Some("blue")
        );
        assert_eq!(
            properties.get(PROPERTY_KEYS).map(String::as_str),
            Some("key-a key-b")
        );
        assert_eq!(first.body().as_ref(), b"first");
        assert_eq!(second.body().as_ref(), b"second");
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn auto_batch_is_shared_by_client_id_and_keeps_first_producer_owner() {
        let mut first_config = crate::ProducerConfig::new("shared-accumulator-owner").unwrap();
        first_config
            .client_config_mut()
            .set_client_ip(Some("192.0.2.18".into()));
        first_config
            .client_config_mut()
            .set_instance_name(Some("phase18-accumulator-cache".into()));
        first_config.set_batch_max_delay_millis(75);
        let mut second_config = first_config.clone();
        second_config.set_producer_group(Some("shared-accumulator-caller".into()));
        let manager = rocketmq_classic_core::runtime::ClientRuntimeManager::new();
        let (first, _) = scripted_producer_with_manager(first_config, manager.clone()).await;
        let (second, _) = scripted_producer_with_manager(second_config, manager.clone()).await;

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let cache = &fixture["produceAccumulatorClientIdCache"];

        assert_eq!(
            first.config.client_config().build_client_id(),
            second.config.client_config().build_client_id()
        );
        assert_eq!(cache["sameClientIdReusesAccumulator"], true);
        assert!(Arc::ptr_eq(
            first.auto_batch_accumulator.get().unwrap(),
            second.auto_batch_accumulator.get().unwrap()
        ));
        assert_eq!(
            cache["clientId"],
            first.config.client_config().build_client_id()
        );
        assert_eq!(
            cache["sameClientId"],
            second.config.client_config().build_client_id()
        );
        let mut other_id_config = first.config.clone();
        other_id_config
            .client_config_mut()
            .set_instance_name(Some("phase18-accumulator-cache-other".into()));
        let other_id = Producer::with_runtime_manager(other_id_config, manager.clone());
        other_id.init_produce_accumulator().unwrap();
        assert_eq!(cache["differentClientIdUsesDifferentAccumulator"], true);
        assert_eq!(
            cache["otherClientId"],
            other_id.config.client_config().build_client_id()
        );
        assert!(!Arc::ptr_eq(
            first.auto_batch_accumulator.get().unwrap(),
            other_id.auto_batch_accumulator.get().unwrap()
        ));
        first.set_auto_batch(true);
        second.set_auto_batch(true);
        second.set_batch_max_bytes(8_192).unwrap();
        assert_eq!(first.batch_max_bytes(), 8_192);

        first
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result(
                "shared-a,shared-b",
                "shared-offset-a,shared-offset-b",
                30,
            ))));

        let mut first_message = Message::new("orders", b"first".to_vec());
        let first_send = {
            let producer = first.clone();
            tokio::spawn(async move { producer.send(&mut first_message).await })
        };
        wait_for_scheduled_auto_batch_timer(&first).await;
        let mut second_message = Message::new("orders", b"second".to_vec());
        let second_send = {
            let producer = second.clone();
            tokio::spawn(async move { producer.send(&mut second_message).await })
        };

        assert_eq!(first_send.await.unwrap().unwrap().msg_id, "shared-a");
        assert_eq!(second_send.await.unwrap().unwrap().msg_id, "shared-b");
        let first_attempts = std::mem::take(&mut first.scripted_transport.lock().unwrap().attempts);
        let second_attempts =
            std::mem::take(&mut second.scripted_transport.lock().unwrap().attempts);
        assert_eq!(first_attempts.len(), 1);
        assert!(first_attempts[0].batch);
        assert!(second_attempts.is_empty());

        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();

        let mut restart_config = crate::ProducerConfig::new("shared-accumulator-restart").unwrap();
        *restart_config.client_config_mut() = first.config.client_config().clone();
        let (restart, _) = scripted_producer_with_manager(restart_config, manager).await;
        assert_eq!(restart.batch_max_delay_millis(), 75);
        assert_eq!(restart.batch_max_bytes(), 8_192);
        restart.set_auto_batch(true);
        restart
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result(
                "restarted",
                "restarted-offset",
                32,
            ))));
        let mut restarted_message = Message::new("orders", b"restart".to_vec());
        assert_eq!(
            restart.send(&mut restarted_message).await.unwrap().msg_id,
            "restarted"
        );
        restart.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn auto_batch_aggregate_size_is_validated_after_batch_encoding() {
        let mut config = crate::ProducerConfig::new("auto-batch-aggregate-size").unwrap();
        config.set_max_message_size(64);
        let (producer, _) = scripted_producer(config).await;
        let messages = vec![
            Message::new("orders", vec![1]),
            Message::new("orders", vec![2]),
        ];
        let batch = ReadyBatch {
            key: BatchKey::new(&messages[0], None, SendMode::Sync),
            messages,
            waiters: Vec::new(),
            body_bytes: 2,
            owner: None,
        };

        assert!(matches!(
            producer.send_accumulated_batch(&batch).await,
            Err(ProducerError::BodyTooLarge {
                actual,
                maximum: 64
            }) if actual > 64
        ));
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn lowering_runtime_batch_byte_limit_flushes_pending_batch() {
        let mut config = crate::ProducerConfig::new("runtime-batch-byte-limit").unwrap();
        config.set_batch_max_delay_millis(5_000);
        let (producer, _) = scripted_producer(config).await;
        producer.set_auto_batch(true);
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result("dynamic-batch", "offset", 9))));

        let send = {
            let producer = producer.clone();
            tokio::spawn(async move {
                producer
                    .send(&mut Message::new("orders", b"pending".to_vec()))
                    .await
            })
        };
        wait_for_scheduled_auto_batch_timer(&producer).await;
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );

        producer.set_batch_max_bytes(1).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), send)
            .await
            .expect("a lower live threshold should flush the pending batch")
            .unwrap()
            .unwrap();
        assert_eq!(result.msg_id, "dynamic-batch");
        {
            let scripted = producer.scripted_transport.lock().unwrap();
            assert_eq!(scripted.attempts.len(), 1);
            assert!(scripted.attempts[0].batch);
        }
        producer.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_batch_delay_reschedules_and_aggregate_limit_gates_next_enqueue() {
        let mut config = crate::ProducerConfig::new("runtime-batch-delay-total-limit").unwrap();
        config.set_batch_max_delay_millis(10);
        let (producer, _) = scripted_producer(config).await;
        producer.set_auto_batch(true);
        {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            scripted.responses.push_back(Ok(Some(scripted_result(
                "direct-after-limit",
                "offset-direct",
                10,
            ))));
            scripted.responses.push_back(Ok(Some(scripted_result(
                "pending-batch",
                "offset-batch",
                11,
            ))));
        }

        let pending = {
            let producer = producer.clone();
            tokio::spawn(async move {
                producer
                    .send(&mut Message::new("orders", b"one".to_vec()))
                    .await
            })
        };
        wait_for_scheduled_auto_batch_timer(&producer).await;

        producer.set_batch_max_delay_millis(50).unwrap();
        producer.set_total_batch_max_bytes(3).unwrap();
        let direct_result = producer
            .send(&mut Message::new("orders", b"next".to_vec()))
            .await
            .unwrap();
        assert_eq!(direct_result.msg_id, "direct-after-limit");
        {
            let scripted = producer.scripted_transport.lock().unwrap();
            assert_eq!(scripted.attempts.len(), 1);
            assert!(!scripted.attempts[0].batch);
        }

        tokio::time::advance(Duration::from_millis(10)).await;
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts.len(),
            1
        );

        tokio::time::advance(Duration::from_millis(39)).await;
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            producer.scripted_transport.lock().unwrap().attempts.len(),
            1
        );

        tokio::time::advance(Duration::from_millis(1)).await;
        let pending_result = tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .expect("the rescheduled batch should flush at the updated delay")
            .unwrap()
            .unwrap();
        assert_eq!(pending_result.msg_id, "pending-batch");
        {
            let scripted = producer.scripted_transport.lock().unwrap();
            assert_eq!(scripted.attempts.len(), 2);
            assert!(!scripted.attempts[0].batch);
            assert!(scripted.attempts[1].batch);
        }
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn direct_and_forced_accumulator_helpers_keep_java_path_choices() {
        let mut config = crate::ProducerConfig::new("scripted-explicit-batch-paths").unwrap();
        config.set_batch_max_delay_millis(10);
        let (producer, default_queues) = scripted_producer(config).await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        {
            let mut scripted = producer.scripted_transport.lock().unwrap();
            for (msg_id, offset) in [
                ("direct-default-sync", 10),
                ("direct-default-future", 11),
                ("direct-default-sync-timeout", 12),
                ("direct-default-future-timeout", 13),
                ("direct-queue-sync", 14),
                ("direct-queue-future", 15),
                ("direct-queue-sync-timeout", 16),
                ("direct-queue-future-timeout", 17),
                ("accumulator-default-sync", 18),
                ("accumulator-default-future", 19),
                ("accumulator-queue-sync", 20),
                ("accumulator-queue-future", 21),
            ] {
                scripted.responses.push_back(Ok(Some(scripted_result(
                    msg_id,
                    &offset.to_string(),
                    offset,
                ))));
            }
        }

        producer.set_auto_batch(true);
        producer
            .send_direct(&mut Message::new("orders", b"direct".to_vec()))
            .await
            .unwrap();
        producer
            .send_direct_future(&mut Message::new("orders", b"direct future".to_vec()))
            .await
            .unwrap();
        producer
            .send_direct_with_timeout(
                &mut Message::new("orders", b"direct sync timeout".to_vec()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        producer
            .send_direct_future_with_timeout(
                &mut Message::new("orders", b"direct future timeout".to_vec()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        producer
            .send_to_queue_direct(
                &mut Message::new("orders", b"direct queue sync".to_vec()),
                &queue,
            )
            .await
            .unwrap();
        producer
            .send_to_queue_direct_future(
                &mut Message::new("orders", b"direct queue".to_vec()),
                &queue,
            )
            .await
            .unwrap();
        producer
            .send_to_queue_direct_with_timeout(
                &mut Message::new("orders", b"direct queue sync timeout".to_vec()),
                &queue,
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        producer
            .send_to_queue_direct_future_with_timeout(
                &mut Message::new("orders", b"direct queue future timeout".to_vec()),
                &queue,
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        producer.set_auto_batch(false);
        producer
            .send_by_accumulator(&mut Message::new("orders", b"accumulate".to_vec()))
            .await
            .unwrap();
        producer
            .send_by_accumulator_future(&mut Message::new("orders", b"accumulate future".to_vec()))
            .await
            .unwrap();
        producer
            .send_to_queue_by_accumulator(
                &mut Message::new("orders", b"accumulate queue sync".to_vec()),
                &queue,
            )
            .await
            .unwrap();
        producer
            .send_to_queue_by_accumulator_future(
                &mut Message::new("orders", b"accumulate queue".to_vec()),
                &queue,
            )
            .await
            .unwrap();

        let attempts = {
            let scripted = producer.scripted_transport.lock().unwrap();
            scripted.attempts.clone()
        };
        assert_eq!(attempts.len(), 12);
        for (index, mode) in [
            SendMode::Sync,
            SendMode::Future,
            SendMode::Sync,
            SendMode::Future,
            SendMode::Sync,
            SendMode::Future,
            SendMode::Sync,
            SendMode::Future,
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(attempts[index].mode, mode);
            assert!(!attempts[index].batch, "direct paths bypass auto-batching");
        }
        assert!(default_queues.contains(&attempts[0].queue));
        assert!(default_queues.contains(&attempts[1].queue));
        assert!(default_queues.contains(&attempts[2].queue));
        assert!(default_queues.contains(&attempts[3].queue));
        assert_eq!(attempts[4].queue, queue);
        assert_eq!(attempts[5].queue, queue);
        assert_eq!(attempts[6].queue, queue);
        assert_eq!(attempts[7].queue, queue);
        assert_eq!(attempts[8].mode, SendMode::Sync);
        assert!(
            attempts[8].batch,
            "forced accumulation must ignore auto_batch=false"
        );
        assert_eq!(attempts[9].mode, SendMode::Future);
        assert!(attempts[9].batch);
        assert_eq!(attempts[10].mode, SendMode::Sync);
        assert!(attempts[10].batch);
        assert_eq!(attempts[10].queue, queue);
        assert_eq!(attempts[11].mode, SendMode::Future);
        assert!(attempts[11].batch);
        assert_eq!(attempts[11].queue, queue);

        producer.shutdown().await.unwrap();
    }

    #[test]
    fn auto_batch_eligibility_matches_java_direct_send_fallbacks() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        for row in fixture["autoBatchEligibility"].as_array().unwrap() {
            let message = match row["input"].as_str().unwrap() {
                "regular" => Message::new("orders", b"body".to_vec()),
                "delayed" => {
                    let mut message = Message::new("orders", b"body".to_vec());
                    message
                        .set_system_property(PROPERTY_TIMER_DELAY_MS, "100")
                        .unwrap();
                    message
                }
                "retry-topic" => Message::new("%RETRY%workers", b"body".to_vec()),
                "producer-group" => {
                    let mut message = Message::new("orders", b"body".to_vec());
                    message
                        .set_system_property(PROPERTY_PRODUCER_GROUP, "transaction-producer")
                        .unwrap();
                    message
                }
                other => panic!("unknown Java auto-batch fixture case: {other}"),
            };
            assert_eq!(
                can_accumulate(&message),
                row["valid"].as_bool().unwrap(),
                "{row}"
            );
        }
    }

    #[tokio::test]
    async fn accumulator_send_mutates_caller_before_runtime_dispatch_like_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let expected = &fixture["sendInputMutation"];
        let producer =
            Producer::new(crate::ProducerConfig::new("accumulator-input-mutation").unwrap());
        let mut message = Message::new("orders", vec![1, 2, 3]);

        assert!(matches!(
            producer.send_by_accumulator(&mut message).await,
            Err(ProducerError::NotStarted)
        ));
        let unique_id = message
            .get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .expect("pinned Java assigns UNIQ_KEY before accumulator dispatch");
        assert_eq!(
            expected["uniqueIdPresent"].as_bool(),
            Some(true),
            "{expected}"
        );
        assert_eq!(
            expected["sendOutcome"].as_str(),
            Some("MQClientException"),
            "{expected}"
        );
        assert_eq!(
            expected["uniqueIdLengthMatchesAddressFamily"].as_bool(),
            Some(true),
            "{expected}"
        );
        assert!(matches!(unique_id.len(), 32 | 56));
        assert_eq!(
            expected["topic"].as_str(),
            Some(message.topic()),
            "{expected}"
        );

        let mut existing = Message::new("orders", vec![1, 2, 3]);
        existing
            .set_system_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "existing-id")
            .unwrap();
        assert!(matches!(
            producer.send_by_accumulator(&mut existing).await,
            Err(ProducerError::NotStarted)
        ));
        assert_eq!(
            existing
                .properties()
                .get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                .map(String::as_str),
            Some("existing-id")
        );

        let mut direct = Message::new("orders", vec![1, 2, 3]);
        assert!(matches!(
            producer.send_direct(&mut direct).await,
            Err(ProducerError::NotStarted)
        ));
        assert!(
            !direct
                .properties()
                .contains_key(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            "Java direct send fails service-state validation before assigning UNIQ_KEY"
        );
        assert_eq!(
            expected["directUniqueIdPresent"].as_bool(),
            Some(false),
            "{expected}"
        );
        assert_eq!(
            expected["directSendOutcome"].as_str(),
            Some("MQClientException"),
            "{expected}"
        );
    }

    #[tokio::test]
    async fn collection_send_mutates_batch_messages_before_dispatch_like_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let expected = &fixture["batchSendInputMutation"];
        let mut config = crate::ProducerConfig::new("batch-input-mutation").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("batch-tenant".into()));
        let producer = Producer::new(config);
        let mut messages = [
            Message::new("orders", vec![1, 2, 3]),
            Message::new("orders", vec![4, 5, 6]),
        ];

        assert!(matches!(
            producer.send_batch(&mut messages).await,
            Err(ProducerError::NotStarted)
        ));
        assert_eq!(expected["sendOutcome"].as_str(), Some("MQClientException"));
        for (message, topic, present) in [
            (&messages[0], "firstTopic", "firstUniqueIdPresent"),
            (&messages[1], "secondTopic", "secondUniqueIdPresent"),
        ] {
            assert_eq!(message.topic(), expected[topic].as_str().unwrap());
            let unique_id = message
                .get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                .expect("Java batch send assigns a unique ID before dispatch");
            assert_eq!(expected[present].as_bool(), Some(true));
            assert!(matches!(unique_id.len(), 32 | 56));
        }

        let mut mixed_topics = [
            Message::new("orders", vec![1]),
            Message::new("payments", vec![2]),
        ];
        let mixed_topic_error = producer
            .send_batch(&mut mixed_topics)
            .await
            .expect_err("mixed-topic batches are rejected during Java batch preparation");
        assert!(matches!(
            &mixed_topic_error,
            ProducerError::BatchInitiationFailed(source)
                if matches!(source.as_ref(), ProducerError::BatchTopicMismatch)
        ));
        assert_eq!(
            mixed_topic_error.to_string(),
            expected["mixedTopicErrorMessage"].as_str().unwrap()
        );
        assert_eq!(
            expected["mixedTopicOutcome"].as_str(),
            Some("MQClientException")
        );
        assert_eq!(
            mixed_topics[0].topic(),
            expected["mixedFirstTopic"].as_str().unwrap()
        );
        assert_eq!(
            mixed_topics[1].topic(),
            expected["mixedSecondTopic"].as_str().unwrap()
        );
        for (message, key) in [
            (&mixed_topics[0], "mixedFirstUniqueIdPresent"),
            (&mixed_topics[1], "mixedSecondUniqueIdPresent"),
        ] {
            assert_eq!(expected[key].as_bool(), Some(false));
            assert!(
                !message
                    .properties()
                    .contains_key(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            );
        }

        let mut limited_config = crate::ProducerConfig::new("batch-partial-validation").unwrap();
        limited_config.set_max_message_size(2);
        limited_config
            .client_config_mut()
            .set_namespace(Some("batch-tenant".into()));
        let limited_producer = Producer::new(limited_config);
        let mut aggregate_size = [Message::new("orders", vec![1])];
        let aggregate_error = limited_producer
            .send_batch(&mut aggregate_size)
            .await
            .expect_err("the producer state check follows successful batch preparation");
        assert!(matches!(aggregate_error, ProducerError::NotStarted));
        assert_eq!(
            expected["aggregateSizeOutcome"].as_str(),
            Some("MQClientException")
        );
        assert_eq!(
            aggregate_size[0].topic(),
            expected["aggregateSizeTopic"].as_str().unwrap()
        );
        assert!(
            aggregate_size[0]
                .properties()
                .contains_key(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        );
        assert_eq!(
            expected["aggregateSizeUniqueIdPresent"].as_bool(),
            Some(true)
        );

        let mut partial_validation = [
            Message::new("orders", vec![1]),
            Message::new("orders", vec![2, 3, 4]),
        ];
        let partial_validation_error = limited_producer
            .send_batch(&mut partial_validation)
            .await
            .expect_err("per-element validation failures are wrapped during batch preparation");
        assert!(matches!(
            &partial_validation_error,
            ProducerError::BatchInitiationFailed(source)
                if matches!(source.as_ref(), ProducerError::BodyTooLarge { actual: 3, maximum: 2 })
        ));
        assert_eq!(
            partial_validation_error.to_string(),
            expected["partialValidationErrorMessage"].as_str().unwrap()
        );
        assert_eq!(
            expected["partialValidationOutcome"].as_str(),
            Some("MQClientException")
        );
        assert_eq!(
            partial_validation[0].topic(),
            expected["partialFirstTopic"].as_str().unwrap()
        );
        assert_eq!(
            partial_validation[1].topic(),
            expected["partialSecondTopic"].as_str().unwrap()
        );
        assert!(
            partial_validation[0]
                .properties()
                .contains_key(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        );
        assert!(
            !partial_validation[1]
                .properties()
                .contains_key(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        );
        assert_eq!(
            expected["partialFirstUniqueIdPresent"].as_bool(),
            Some(true)
        );
        assert_eq!(
            expected["partialSecondUniqueIdPresent"].as_bool(),
            Some(false)
        );

        let mut active_config = crate::ProducerConfig::new("batch-aggregate-size").unwrap();
        active_config.set_max_message_size(64);
        let (active_producer, _) = scripted_producer(active_config).await;
        let mut aggregate_messages = [
            Message::new("orders", vec![1]),
            Message::new("orders", vec![2]),
        ];
        let aggregate_error = active_producer
            .send_batch(&mut aggregate_messages)
            .await
            .expect_err("the encoded batch is validated after producer start");
        assert!(matches!(
            aggregate_error,
            ProducerError::BodyTooLarge {
                actual,
                maximum: 64
            } if actual > 64
        ));
        assert!(
            active_producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );
        active_producer.shutdown().await.unwrap();
    }

    #[test]
    fn auto_batch_result_fanout_preserves_java_ids_and_offsets() {
        let mut result = SendResult {
            status: SendStatus::SendOk,
            msg_id: "msg-a,msg-b".into(),
            offset_msg_id: Some("offset-a,offset-b".into()),
            topic: "orders".into(),
            broker_name: "broker-a".into(),
            queue_id: 2,
            queue_offset: 40,
            transaction_id: None,
            recall_handle: None,
            raw_resp_body: None,
            region_id: "region-a".into(),
            trace_on: true,
        };
        let split = split_auto_batch_result(result.clone(), 2).unwrap();
        assert_eq!(split[0].msg_id, "msg-a");
        assert_eq!(split[1].msg_id, "msg-b");
        assert_eq!(split[0].offset_msg_id.as_deref(), Some("offset-a"));
        assert_eq!(split[1].offset_msg_id.as_deref(), Some("offset-b"));
        assert_eq!(split[0].queue_offset, 40);
        assert_eq!(split[1].queue_offset, 41);

        result.msg_id = "batch-msg-id".into();
        result.offset_msg_id = Some("batch-offset-id".into());
        let repeated = split_auto_batch_result(result, 2).unwrap();
        assert_eq!(repeated[0], repeated[1]);

        let mut inconsistent = split[0].clone();
        inconsistent.msg_id = "msg-a,msg-b".into();
        inconsistent.offset_msg_id = Some("offset-a".into());
        assert!(matches!(
            split_auto_batch_result(inconsistent, 2),
            Err(ProducerError::InvalidAutoBatchResult)
        ));
    }

    #[test]
    fn mode_specific_attempts_match_java_retry_budgets() {
        let config = crate::ProducerConfig::new("producer").unwrap();
        assert_eq!(attempt_count(SendMode::Sync, &config), 3);
        assert_eq!(attempt_count(SendMode::Future, &config), 3);
        assert_eq!(attempt_count(SendMode::Oneway, &config), 1);

        let producer = crate::Producer::new(config.clone());
        producer.set_retry_times_when_send_failed(0);
        producer.set_retry_times_when_send_async_failed(4);
        assert_eq!(attempt_count(SendMode::Sync, producer.config()), 1);
        assert_eq!(attempt_count(SendMode::Future, producer.config()), 5);

        let mut no_attempts = config.clone();
        no_attempts.set_retry_times_when_send_failed(-1);
        assert_eq!(attempt_count(SendMode::Sync, &no_attempts), 0);
    }

    #[test]
    fn sync_and_future_retry_broker_responses_using_java_policies() {
        let config = crate::ProducerConfig::new("producer").unwrap();
        let retryable = RpcError::Response {
            request_code: 10,
            peer: "broker".into(),
            code: response::SERVICE_NOT_AVAILABLE,
            remark: None,
        };
        assert!(should_retry(SendMode::Sync, &retryable, &config));
        assert!(should_retry(SendMode::Future, &retryable, &config));
        let transport = RpcError::Transport(TransportError::InvalidConfiguration("test".into()));
        assert!(should_retry(SendMode::Sync, &transport, &config));
        assert!(should_retry(SendMode::Future, &transport, &config));
        assert!(!should_retry(
            SendMode::Future,
            &RpcError::Transport(TransportError::TooManyRequests),
            &config
        ));
        assert!(!should_retry(SendMode::Oneway, &transport, &config));
    }

    #[test]
    fn default_sync_retry_response_codes_match_the_java_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let config = crate::ProducerConfig::new("producer").unwrap();
        for code in fixture["defaults"]["retryResponseCodes"]
            .as_array()
            .unwrap()
        {
            let code = code.as_i64().unwrap() as i32;
            let error = RpcError::Response {
                request_code: 10,
                peer: "broker".into(),
                code,
                remark: None,
            };
            assert!(should_retry(SendMode::Sync, &error, &config), "code {code}");
        }
        let non_retryable = RpcError::Response {
            request_code: 10,
            peer: "broker".into(),
            code: i32::MAX,
            remark: None,
        };
        assert!(!should_retry(SendMode::Sync, &non_retryable, &config));
    }

    #[test]
    fn send_hook_message_type_matches_java_transaction_and_timer_precedence() {
        let producer = Producer::new(crate::ProducerConfig::new("send-hook-type").unwrap());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let defaults = &fixture["sendMessageContext"]["defaults"];
        let mut normal = Message::new("orders", b"normal".to_vec());
        let normal_context =
            producer.send_message_context(&mut normal, &queue, "broker-a:10911", SendMode::Sync);
        assert_eq!(
            normal_context.message_type.java_name(),
            defaults["messageType"]
        );
        assert_eq!(
            normal_context.send_result.is_none(),
            defaults["sendResultIsNull"]
        );
        assert_eq!(
            normal_context.exception.is_none(),
            defaults["exceptionIsNull"]
        );
        assert_eq!(
            normal_context.trace_context.is_none(),
            defaults["traceContextIsNull"]
        );
        assert_eq!(normal_context.properties.is_none(), defaults["propsIsNull"]);
        assert_eq!(
            normal_context.namespace.is_none(),
            defaults["namespaceIsNull"]
        );

        let mut transaction = Message::new("orders", b"transaction".to_vec());
        transaction
            .set_system_property(PROPERTY_TRANSACTION_PREPARED, "true")
            .unwrap();
        assert_eq!(
            producer
                .send_message_context(&mut transaction, &queue, "broker-a:10911", SendMode::Sync)
                .message_type,
            SendMessageType::TransactionHalf
        );

        for property in [
            PROPERTY_DELAY_TIME_LEVEL,
            "__STARTDELIVERTIME",
            PROPERTY_TIMER_DELIVER_MS,
            PROPERTY_TIMER_DELAY_SEC,
            PROPERTY_TIMER_DELAY_MS,
        ] {
            let mut delayed = Message::new("orders", b"delayed".to_vec());
            delayed
                .set_system_property(PROPERTY_TRANSACTION_PREPARED, "true")
                .unwrap();
            delayed.set_system_property(property, "1").unwrap();
            assert_eq!(
                producer
                    .send_message_context(&mut delayed, &queue, "broker-a:10911", SendMode::Sync)
                    .message_type,
                SendMessageType::Delay,
                "{property} must override transaction message type like Java"
            );
        }
    }

    #[test]
    fn fault_classification_matches_detector_reachability_switch() {
        let transport = RpcError::Transport(TransportError::InvalidConfiguration("test".into()));
        assert_eq!(
            classify_fault(&transport, SendMode::Sync, false),
            (true, true)
        );
        assert_eq!(
            classify_fault(&transport, SendMode::Sync, true),
            (true, false)
        );
        let response = RpcError::Response {
            request_code: 10,
            peer: "broker".into(),
            code: response::TOPIC_NOT_EXIST,
            remark: None,
        };
        assert_eq!(
            classify_fault(&response, SendMode::Sync, false),
            (true, false)
        );
        assert_eq!(
            classify_fault(&transport, SendMode::Future, true),
            (true, true)
        );
        assert_eq!(
            classify_fault(&response, SendMode::Future, true),
            (true, true)
        );
    }

    #[test]
    fn per_request_timeout_only_limits_sync_attempts_that_have_a_retry_after_them() {
        let producer = crate::Producer::new(crate::ProducerConfig::new("producer").unwrap());
        let now = Instant::now();
        let deadline = now + Duration::from_secs(5);
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        for case in fixture["perRequestTimeoutRetry"]["configuredValues"]
            .as_array()
            .unwrap()
        {
            let value = case["input"].as_i64().unwrap() as i32;
            producer.set_send_msg_max_timeout_per_request(value);
            assert_eq!(
                producer.send_msg_max_timeout_per_request(),
                case["readback"].as_i64().unwrap() as i32
            );
            let expected = if value <= -1 {
                deadline
            } else {
                (now + Duration::from_millis(value as u64)).min(deadline)
            };
            assert_eq!(
                producer.request_deadline_at(SendMode::Sync, deadline, now, 0, 3),
                expected,
                "request timeout input {value}"
            );
        }
        assert_eq!(
            producer.request_deadline_at(SendMode::Sync, deadline, now, 2, 3),
            deadline
        );
        producer.set_send_msg_max_timeout_per_request(125);
        assert_eq!(
            producer.request_deadline_at(SendMode::Future, deadline, now, 0, 3),
            deadline
        );
        assert_eq!(
            producer.request_deadline_at(SendMode::Oneway, deadline, now, 0, 1),
            deadline
        );
    }

    #[test]
    fn compression_threshold_matches_java_and_batch_payloads_stay_uncompressed() {
        let config = crate::ProducerConfig::new("compression-test").unwrap();
        let producer = crate::Producer::new(config);
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let compression = &fixture["compression"];
        assert_eq!(
            compression["threshold"].as_i64().unwrap(),
            i64::from(producer.config.compress_threshold())
        );
        for case in compression["cases"].as_array().unwrap() {
            let length = case["bodyLength"].as_u64().unwrap() as usize;
            let expected = case["compressed"].as_bool().unwrap();
            let message = Message::new("orders", vec![b'x'; length]);
            let mut message_for_send = message.clone();
            let compression_sys_flag =
                producer.compress_message_before_hook(&mut message_for_send, false);
            let prepared = producer
                .prepare(&message_for_send, false, compression_sys_flag)
                .unwrap();
            assert_eq!(
                message::check(prepared.sys_flag, message::COMPRESSED_FLAG),
                expected,
                "Java threshold fixture case {case}"
            );
            if expected {
                assert_eq!(
                    rocketmq_classic_common::compression::decompress(
                        producer.config.compress_type(),
                        &prepared.body
                    )
                    .unwrap(),
                    message.body().to_vec()
                );
            } else {
                assert_eq!(prepared.body, message.body().to_vec());
            }
            assert_eq!(message.body().len(), length);

            let batch_prepared = producer.prepare(&message, true, 0).unwrap();
            assert_eq!(batch_prepared.sys_flag, 0);
            assert_eq!(batch_prepared.body, message.body().to_vec());
        }
    }

    #[test]
    fn prepared_send_keeps_java_null_key_property_encoding() {
        let producer = Producer::new(crate::ProducerConfig::new("null-key-property").unwrap());
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap();
        let expected = fixture["messagePublicApi"]["nullKeyPropertyEncoded"]
            .as_str()
            .unwrap();
        let mut message = Message::new("orders", b"body".to_vec());
        message.set_property_nullable(None, Some("value".to_owned()));

        let prepared = producer.prepare(&message, false, 0).unwrap();
        assert!(prepared.properties.as_deref().unwrap().contains(expected));
    }

    #[test]
    fn runtime_compression_settings_match_java_and_config_clones_are_snapshots() {
        let config = crate::ProducerConfig::new("runtime-compression-test").unwrap();
        let snapshot = config.clone();
        let producer = crate::Producer::new(config);
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let runtime = &fixture["runtimeCompressionSettings"];

        let threshold = runtime["threshold"].as_i64().unwrap() as i32;
        producer.set_compress_threshold(threshold);
        assert_eq!(producer.compress_threshold(), threshold);
        for case in runtime["thresholdCases"].as_array().unwrap() {
            let length = case["bodyLength"].as_u64().unwrap() as usize;
            let expected = case["compressed"].as_bool().unwrap();
            let original = Message::new("orders", vec![b'x'; length]);
            let mut message = original.clone();
            let flag = producer.compress_message_before_hook(&mut message, false);
            let prepared = producer.prepare(&message, false, flag).unwrap();
            assert_eq!(
                message::check(prepared.sys_flag, message::COMPRESSED_FLAG),
                expected,
                "Java runtime compression threshold case {case}"
            );
        }

        let updated_threshold = runtime["thresholdAfterUpdate"].as_i64().unwrap() as i32;
        producer.set_compress_threshold(updated_threshold);
        assert_eq!(producer.compress_threshold(), updated_threshold);

        let body = vec![b'x'; 16_384];
        let mut compressed_by_level = Vec::new();
        for level_name in ["level1", "level9"] {
            let level = runtime[level_name]["level"].as_i64().unwrap() as i32;
            producer.set_compress_level(level);
            assert_eq!(producer.compress_level(), level);
            let original = Message::new("orders", body.clone());
            let mut message = original.clone();
            let flag = producer.compress_message_before_hook(&mut message, false);
            let prepared = producer.prepare(&message, false, flag).unwrap();
            assert_eq!(
                message::check(prepared.sys_flag, message::COMPRESSED_FLAG),
                runtime[level_name]["compressed"].as_bool().unwrap()
            );
            assert_eq!(
                rocketmq_classic_common::compression::decompress(
                    rocketmq_classic_common::sysflag::message::CompressionType::Zlib,
                    &prepared.body
                )
                .unwrap(),
                body
            );
            compressed_by_level.push(prepared.body);
        }
        assert_ne!(
            compressed_by_level[0], compressed_by_level[1],
            "runtime compression level changes subsequent compressed output"
        );
        assert_ne!(
            runtime["level1"]["compressedBodyLength"], runtime["level9"]["compressedBodyLength"],
            "the invoked Java fixture confirms these levels produce distinct output"
        );

        assert_eq!(snapshot.compress_threshold(), 4_096);
        assert_eq!(snapshot.compress_level(), 5);
    }

    #[test]
    fn runtime_fault_strategy_settings_match_java_and_change_later_selection() {
        let producer =
            crate::Producer::new(crate::ProducerConfig::new("runtime-fault-settings").unwrap());
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let runtime = &fixture["runtimeFaultStrategy"];
        let defaults = &fixture["faultStrategy"];
        let values = |name: &str| {
            runtime[name]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_i64().unwrap())
                .collect::<Vec<_>>()
        };

        let latency_input = crate::ProducerLongArray::new(values("initialLatencyMaxMillis"));
        let duration_input =
            crate::ProducerLongArray::new(values("initialNotAvailableDurationMillis"));
        assert_eq!(
            producer.latency_max_millis().snapshot(),
            Some(
                defaults["latencyMaxMillis"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_i64().unwrap())
                    .collect::<Vec<_>>(),
            )
        );
        assert_eq!(
            producer.not_available_duration_millis().snapshot(),
            Some(
                defaults["notAvailableDurationMillis"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_i64().unwrap())
                    .collect::<Vec<_>>(),
            )
        );
        producer.set_latency_max_millis(latency_input.clone());
        producer.set_not_available_duration_millis(duration_input.clone());
        producer.set_send_latency_fault_enable(true);
        producer.set_start_detector_enable(true);
        let mutation = |name: &str| {
            let mutation = &runtime[name];
            (
                mutation["index"].as_u64().unwrap() as usize,
                mutation["value"].as_i64().unwrap(),
            )
        };
        let (index, value) = mutation("latencyInputMutation");
        let previous = latency_input.get(index);
        assert_eq!(latency_input.set(index, value), previous);
        let (index, value) = mutation("latencyGetterMutation");
        let latency_getter = producer.latency_max_millis();
        let previous = latency_getter.get(index);
        assert_eq!(latency_getter.set(index, value), previous);
        let (index, value) = mutation("durationInputMutation");
        let previous = duration_input.get(index);
        assert_eq!(duration_input.set(index, value), previous);
        let (index, value) = mutation("durationGetterMutation");
        let duration_getter = producer.not_available_duration_millis();
        let previous = duration_getter.get(index);
        assert_eq!(duration_getter.set(index, value), previous);
        assert_eq!(producer.latency_max_millis(), latency_input);
        assert_eq!(producer.not_available_duration_millis(), duration_input);
        assert_eq!(latency_getter.snapshot(), Some(values("latencyMaxMillis")));
        assert_eq!(
            duration_getter.snapshot(),
            Some(values("notAvailableDurationMillis"))
        );
        assert!(runtime["latencyArrayIdentityRetained"].as_bool().unwrap());
        assert!(runtime["durationArrayIdentityRetained"].as_bool().unwrap());
        assert!(runtime["latencyGetterIdentityRetained"].as_bool().unwrap());
        assert!(runtime["durationGetterIdentityRetained"].as_bool().unwrap());
        assert_eq!(
            producer.send_latency_fault_enable(),
            runtime["sendLatencyFaultEnableAfterToggle"]
        );
        assert_eq!(
            producer.start_detector_enable(),
            runtime["startDetectorEnableAfterToggle"]
        );

        producer
            .update_fault("enabled-broker", Duration::from_millis(18), false, true)
            .unwrap();
        let available_queues = [
            MessageQueue::new("orders", "enabled-broker", 0),
            MessageQueue::new("orders", "fallback-broker", 0),
        ];
        let mut index = 0;
        let selected = producer
            .fault
            .select(
                &available_queues,
                None,
                producer.send_latency_fault_enable(),
                |length| {
                    let current = index;
                    index += 1;
                    current % length
                },
                StdInstant::now(),
            )
            .unwrap();
        assert_eq!(
            selected.broker_name(),
            Some(if runtime["enabledBrokerAvailable"].as_bool().unwrap() {
                "enabled-broker"
            } else {
                "fallback-broker"
            })
        );

        producer.set_send_latency_fault_enable(false);
        producer
            .update_fault("disabled-broker", Duration::from_millis(15), false, true)
            .unwrap();
        assert_eq!(
            producer.send_latency_fault_enable(),
            runtime["sendLatencyFaultEnableAfterDisable"]
        );
        producer.set_send_latency_fault_enable(true);
        let disabled_queues = [
            MessageQueue::new("orders", "disabled-broker", 0),
            MessageQueue::new("orders", "fallback-broker", 0),
        ];
        assert_eq!(
            producer
                .fault
                .select(
                    &disabled_queues,
                    None,
                    producer.send_latency_fault_enable(),
                    |_| 0,
                    StdInstant::now(),
                )
                .unwrap()
                .broker_name(),
            Some(if runtime["disabledBrokerAvailable"].as_bool().unwrap() {
                "disabled-broker"
            } else {
                "fallback-broker"
            })
        );

        let prior_latency = producer.latency_max_millis();
        let replacement_latency = crate::ProducerLongArray::new(vec![20]);
        producer.set_latency_max_millis(replacement_latency.clone());
        assert!(prior_latency.set(0, 0).is_some());
        assert_eq!(
            replacement_latency.snapshot(),
            Some(values("latencyReplacementValues"))
        );
        assert!(
            runtime["latencyReplacementIdentityRetained"]
                .as_bool()
                .unwrap()
        );
        let prior_duration = producer.not_available_duration_millis();
        let replacement_duration = crate::ProducerLongArray::new(vec![0]);
        producer.set_not_available_duration_millis(replacement_duration.clone());
        assert!(prior_duration.set(0, 999).is_some());
        assert_eq!(
            replacement_duration.snapshot(),
            Some(values("durationReplacementValues"))
        );
        assert!(
            runtime["durationReplacementIdentityRetained"]
                .as_bool()
                .unwrap()
        );
        producer
            .update_fault(
                "updated-table-broker",
                Duration::from_millis(15),
                false,
                true,
            )
            .unwrap();
        let updated_table_queues = [
            MessageQueue::new("orders", "updated-table-broker", 0),
            MessageQueue::new("orders", "fallback-broker", 0),
        ];
        assert_eq!(
            producer
                .fault
                .select(
                    &updated_table_queues,
                    None,
                    producer.send_latency_fault_enable(),
                    |_| 0,
                    StdInstant::now(),
                )
                .unwrap()
                .broker_name(),
            Some(
                if runtime["updatedTableBrokerAvailable"].as_bool().unwrap() {
                    "updated-table-broker"
                } else {
                    "fallback-broker"
                }
            )
        );
        assert_eq!(
            producer.send_latency_fault_enable(),
            runtime["sendLatencyFaultEnableAfterReenable"]
        );
        producer.set_start_detector_enable(false);
        assert_eq!(
            producer.start_detector_enable(),
            runtime["startDetectorEnableAfterDisable"]
        );
    }

    #[tokio::test]
    async fn internal_runtime_send_uses_the_latency_fault_selector() {
        let mut config = crate::ProducerConfig::new("CLIENT_INNER_PRODUCER").unwrap();
        config.set_send_latency_fault_enable(true);
        config.set_start_detector_enable(true);
        let producer = Arc::new(Producer::new(config.clone()));
        let manager = rocketmq_classic_core::runtime::ClientRuntimeManager::new();
        let runtime = manager.get_or_create(config.client_config()).await.unwrap();
        let queues = [
            MessageQueue::new("orders", "broker-a", 0),
            MessageQueue::new("orders", "broker-b", 0),
        ];
        producer.cache_publish_info(
            "orders",
            Arc::new(TopicSnapshot {
                route: Arc::new(
                    serde_json::from_str::<rocketmq_classic_protocol::route::TopicRouteData>("{}")
                        .unwrap(),
                ),
                publish_queues: queues.to_vec(),
                subscribe_queues: Default::default(),
                endpoints: BTreeMap::from([
                    (queues[0].clone(), "10.0.0.1:10911".into()),
                    (queues[1].clone(), "10.0.0.2:10911".into()),
                ]),
                order_topic: false,
            }),
        );
        producer
            .update_fault("broker-a", Duration::ZERO, true, true)
            .unwrap();
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(Some(scripted_result(
                "internal-send",
                "internal-offset",
                12,
            ))));

        let result = producer
            .send_internal_with_runtime(
                &runtime,
                &mut Message::new("orders", b"retry body".to_vec()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(result.msg_id, "internal-send");
        assert!(producer.detector_task_running_for_test().await);
        let attempts = producer.scripted_transport.lock().unwrap().attempts.clone();
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].queue, queues[1]);
    }

    #[test]
    fn runtime_fault_strategy_null_and_mismatched_arrays_match_java() {
        let producer =
            crate::Producer::new(crate::ProducerConfig::new("fault-table-null-inputs").unwrap());
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let runtime = &fixture["runtimeFaultStrategyNullArrays"];
        producer.set_send_latency_fault_enable(true);

        producer.set_latency_max_millis(None);
        let null_latency_max = producer.latency_max_millis();
        assert!(null_latency_max.is_null());
        assert_eq!(null_latency_max.snapshot(), None);
        assert_eq!(null_latency_max.len(), 0);
        assert!(null_latency_max.is_empty());
        assert!(null_latency_max.get(0).is_none());
        assert!(null_latency_max.set(0, 10).is_none());
        assert_eq!(
            producer
                .update_fault("null-latency-max", Duration::from_millis(10), false, true)
                .unwrap_err()
                .to_string(),
            runtime["nullLatencyMaxUpdateFailure"].as_str().unwrap()
        );

        producer.set_latency_max_millis(vec![10]);
        producer.set_not_available_duration_millis(None);
        let null_duration = producer.not_available_duration_millis();
        assert!(null_duration.is_null());
        assert_eq!(null_duration.snapshot(), None);
        assert!(
            producer
                .update_fault(
                    "null-duration-unmatched",
                    Duration::from_millis(9),
                    false,
                    true
                )
                .is_ok()
        );
        assert_eq!(
            producer
                .update_fault(
                    "null-duration-matched",
                    Duration::from_millis(10),
                    false,
                    true
                )
                .unwrap_err()
                .to_string(),
            runtime["matchedLatencyWithNullDurationFailure"]
                .as_str()
                .unwrap()
        );

        producer.set_latency_max_millis(vec![10, 20]);
        producer.set_not_available_duration_millis(vec![100]);
        assert_eq!(
            producer
                .update_fault(
                    "short-duration-table",
                    Duration::from_millis(20),
                    false,
                    true
                )
                .unwrap_err()
                .to_string(),
            runtime["shortDurationTableFailure"].as_str().unwrap()
        );

        producer.set_latency_max_millis(vec![20]);
        producer.set_not_available_duration_millis(vec![100, 200]);
        assert!(
            producer
                .update_fault(
                    "extra-duration-rows",
                    Duration::from_millis(20),
                    false,
                    true
                )
                .is_ok()
        );
        assert_eq!(
            runtime["latencyMaxGetterIsNull"],
            serde_json::Value::Bool(true)
        );
        assert_eq!(
            runtime["notAvailableDurationGetterIsNull"],
            serde_json::Value::Bool(true)
        );
        assert_eq!(runtime["unmatchedLatencyWithNullDurationFailure"], "none");
        assert_eq!(runtime["extraDurationRowsFailure"], "none");
    }

    #[tokio::test]
    async fn async_backpressure_uses_java_minimums_and_releases_permits_on_drop() {
        let mut config = crate::ProducerConfig::new("backpressure-producer").unwrap();
        config.set_enable_backpressure_for_async_mode(true);
        config.set_backpressure_async_send_num(1);
        config.set_backpressure_async_send_size(1);
        let producer = Producer::new(config);
        let mut permits = Vec::new();
        for _ in 0..10 {
            let (enabled, permit) = producer
                .acquire_async_backpressure(1, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            assert!(enabled);
            permits.push(permit.unwrap());
        }
        assert!(matches!(
            producer
                .acquire_async_backpressure(1, Instant::now() + Duration::from_millis(5))
                .await,
            Err(ProducerError::Timeout)
        ));
        drop(permits);
        let (enabled, permit) = producer
            .acquire_async_backpressure(1, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(enabled);
        assert!(permit.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn producer_runtime_backpressure_settings_update_live_limits_and_mode() {
        let producer = Producer::new(crate::ProducerConfig::new("runtime-backpressure").unwrap());
        assert!(!producer.enable_backpressure_for_async_mode());
        let (enabled, permit) = producer
            .acquire_async_backpressure(1, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(!enabled);
        assert!(permit.is_none());

        producer.set_enable_backpressure_for_async_mode(true);
        producer.set_backpressure_async_send_num(1);
        producer.set_backpressure_async_send_size(1);
        assert!(producer.enable_backpressure_for_async_mode());
        assert!(producer.config().enable_backpressure_for_async_mode());
        assert_eq!(producer.backpressure_async_send_num(), 10);
        assert_eq!(producer.backpressure_async_send_size(), 1024 * 1024);
        drop(producer.acquire_backpressure_for_async_send_num_lock());
        drop(producer.acquire_backpressure_for_async_send_size_lock());

        let (enabled, permit) = producer
            .acquire_async_backpressure(1, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(enabled);
        assert!(permit.is_some());
        producer.set_backpressure_async_send_num(12);
        assert_eq!(producer.backpressure_send_num.available_permits(), 11);
        producer.set_backpressure_async_send_num_inside_adjust(13);
        producer.set_backpressure_async_send_size_inside_adjust(2 * 1024 * 1024 + 1);
        assert_eq!(producer.backpressure_async_send_num(), 13);
        assert_eq!(producer.backpressure_async_send_size(), 2 * 1024 * 1024 + 1);
        producer.set_backpressure_async_send_num(12);
        producer.set_backpressure_async_send_size(2 * 1024 * 1024);

        producer.set_enable_backpressure_for_async_mode(false);
        assert!(!producer.enable_backpressure_for_async_mode());
        assert!(!producer.config().enable_backpressure_for_async_mode());
        let (enabled, permit) = producer
            .acquire_async_backpressure(1, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(!enabled);
        assert!(permit.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_resize_preserves_in_flight_count_like_the_java_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let runtime = &fixture["runtimeSettings"];
        let mut config = crate::ProducerConfig::new("runtime-resize-fixture").unwrap();
        config.set_enable_backpressure_for_async_mode(true);
        config.set_backpressure_async_send_num(20);
        config.set_backpressure_async_send_size(2 * 1024 * 1024);
        let producer = Producer::new(config);
        let mut held_count_permits = Vec::new();

        for _ in 0..7 {
            let (enabled, permits) = producer
                .acquire_async_backpressure(1, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            assert!(enabled);
            let (count, size) = permits.unwrap();
            held_count_permits.push(count);
            drop(size);
        }

        producer.set_backpressure_async_send_num(12);
        assert_eq!(
            i64::from(producer.semaphore_async_send_num_available_permits()),
            runtime["sendNumPermitsAfterResize"].as_i64().unwrap()
        );
        assert_eq!(
            i64::from(producer.semaphore_async_send_size_available_permits()),
            runtime["sendSizePermitsAfterUpdate"].as_i64().unwrap()
        );
        drop(held_count_permits);
        assert_eq!(
            i64::from(producer.semaphore_async_send_num_available_permits()),
            runtime["sendNumPermitsAfterInFlightRelease"]
                .as_i64()
                .unwrap()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn raw_semaphore_replacement_preserves_java_in_flight_accounting() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let expected = &fixture["implementationSemaphoreSettings"];
        let producer = Producer::new(crate::ProducerConfig::new("raw-semaphore-replace").unwrap());
        let permit = producer
            .backpressure_send_num
            .clone()
            .acquire(2, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();

        producer.set_semaphore_async_send_num(7);
        assert_eq!(producer.semaphore_async_send_num_available_permits(), 7);
        drop(permit);
        assert_eq!(
            producer.semaphore_async_send_num_available_permits(),
            expected["countAfterRawReplaceAndInFlightRelease"]
                .as_i64()
                .unwrap() as i32
        );

        producer.set_backpressure_async_send_num(20);
        assert_eq!(
            producer.semaphore_async_send_num_available_permits(),
            expected["countAfterFacadeResize"].as_i64().unwrap() as i32
        );
        assert_eq!(producer.backpressure_async_send_num(), 20);
    }

    #[test]
    fn producer_retry_response_codes_are_live_for_sync_retry_decisions() {
        let producer = Producer::new(crate::ProducerConfig::new("runtime-retry-codes").unwrap());
        let code = 9_001;
        let error = RpcError::Response {
            request_code: 10,
            peer: "broker".into(),
            code,
            remark: None,
        };
        let live_codes = producer.retry_response_codes();
        assert!(!live_codes.contains(&code));
        assert!(!should_retry(SendMode::Sync, &error, producer.config()));

        producer.add_retry_response_code(code);
        assert!(live_codes.contains(&code));
        assert!(should_retry(SendMode::Sync, &error, producer.config()));

        live_codes.remove(&code);
        assert!(!should_retry(SendMode::Sync, &error, producer.config()));
    }
}
