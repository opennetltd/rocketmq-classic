use std::{
    collections::{BTreeMap, BTreeSet},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use rocketmq_classic_common::message::{Message, constants::KEY_SEPARATOR};
use rocketmq_classic_core::{
    config::{AccessChannel, ClientConfig},
    runtime::ClientRuntimeManager,
    trace::{MessageType, TraceBean, TraceContext, TraceType},
};
use rocketmq_classic_protocol::{
    NamespaceUtil,
    codes::{request, response},
    header::{CommandHeader, RecallMessageRequestHeader},
};
use rocketmq_classic_remoting::{RemotingCommand, client::RpcHook};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, interval_at},
};

const TRACE_QUEUE_CAPACITY: usize = 2_048;
const TRACE_MAX_BATCH_SIZE: usize = 20;
const TRACE_MAX_MESSAGE_SIZE: usize = 128_000;

#[cfg(test)]
use crate::SendMessageType;
use crate::{
    EndTransactionContext, EndTransactionHook, Producer, ProducerConfig, ProducerError,
    SendMessageContext, SendMessageHook,
};

const TRACE_TOPIC: &str = "RMQ_SYS_TRACE_TOPIC";
const TRACE_TOPIC_PREFIX: &str = "RMQ_SYS_TRACE_DATA_";
const TRACE_GROUP_PREFIX: &str = "_INNER_TRACE_PRODUCER";
const TRACE_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const TRACE_SEND_TIMEOUT_MILLIS: i32 = 5_000;
const TRACE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const TRACE_SHUTDOWN_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const RECALL_TRACE_ENABLE_KEY: &str = "com.rocketmq.recall.default.trace.enable";

static TRACE_PRODUCER_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) struct ProducerSendTraceHook {
    dispatcher: Arc<TraceDispatcher>,
}

pub(crate) struct ProducerEndTransactionTraceHook {
    dispatcher: Arc<TraceDispatcher>,
    client_config: ClientConfig,
}

impl ProducerEndTransactionTraceHook {
    pub(crate) fn new(dispatcher: Arc<TraceDispatcher>, client_config: ClientConfig) -> Self {
        Self {
            dispatcher,
            client_config,
        }
    }
}

impl ProducerSendTraceHook {
    pub(crate) fn new(dispatcher: Arc<TraceDispatcher>) -> Self {
        Self { dispatcher }
    }
}

pub(crate) struct RecallMessageTraceHook {
    dispatcher: Arc<TraceDispatcher>,
}

impl RecallMessageTraceHook {
    pub(crate) fn new(dispatcher: Arc<TraceDispatcher>) -> Self {
        Self { dispatcher }
    }
}

impl RpcHook for RecallMessageTraceHook {
    fn after_response(
        &self,
        _remote_addr: &str,
        request_command: &RemotingCommand,
        response_command: &RemotingCommand,
    ) {
        if let Some(trace) = recall_trace_context(request_command, response_command) {
            let _ = self.dispatcher.append(trace);
        }
    }
}

pub(crate) fn recall_trace_property_enabled() -> bool {
    recall_trace_property_value(std::env::var(RECALL_TRACE_ENABLE_KEY).ok().as_deref())
}

fn recall_trace_property_value(value: Option<&str>) -> bool {
    value.is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn recall_trace_context(
    request_command: &RemotingCommand,
    response_command: &RemotingCommand,
) -> Option<TraceContext> {
    if request_command.code != request::RECALL_MESSAGE {
        return None;
    }
    let region_id = response_command
        .ext_fields
        .as_ref()?
        .get(rocketmq_classic_common::message::constants::PROPERTY_MSG_REGION)?
        .clone();
    let request_fields = request_command.ext_fields.as_ref()?;
    let request_header = RecallMessageRequestHeader::decode_fields(request_fields).ok()?;
    let recall_handle =
        crate::RecallMessageHandle::decode_handle(request_header.get_recall_handle()).ok()?;
    let group_name = request_header.get_producer_group().map(without_namespace);
    Some(TraceContext {
        trace_type: Some(TraceType::Recall),
        timestamp_millis: epoch_millis(),
        region_id: Some(region_id),
        region_name: Some(String::new()),
        group_name,
        cost_time_millis: 0,
        success: response_command.code == response::SUCCESS,
        request_id: None,
        context_code: 0,
        access_channel: None,
        trace_beans: Some(vec![TraceBean {
            topic: Some(without_namespace(request_header.get_topic())),
            message_id: Some(recall_handle.message_id().to_owned()),
            offset_message_id: None,
            tags: None,
            keys: None,
            store_host: None,
            body_length: 0,
            retry_times: 0,
            message_type: None,
            transaction_id: None,
            transaction_state: None,
            from_transaction_check: false,
            ..TraceBean::default()
        }]),
    })
}

impl SendMessageHook for ProducerSendTraceHook {
    fn hook_name(&self) -> &str {
        "SendMessageTraceHook"
    }

    fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
        let trace_topic_name = self.dispatcher.trace_topic_name();
        if context.message.topic().starts_with(&trace_topic_name) {
            return;
        }
        let mut trace_context = TraceContext::new(TraceType::Pub);
        trace_context.group_name = Some(without_namespace(&self.producer_group));
        trace_context.trace_beans = Some(vec![TraceBean {
            topic: Some(without_namespace(context.message.topic())),
            message_id: None,
            offset_message_id: None,
            tags: context.message.tags(),
            keys: context.message.keys(),
            store_host: Some(context.broker_addr.clone()),
            body_length: i32::try_from(context.message.body().len()).unwrap_or(i32::MAX),
            retry_times: 0,
            message_type: Some(context.message_type),
            transaction_id: None,
            transaction_state: None,
            from_transaction_check: false,
            ..TraceBean::default()
        }]);
        context.trace_context = Some(Box::new(trace_context));
    }

    fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
        let Some(trace_context) = context
            .trace_context
            .as_mut()
            .and_then(|value| value.downcast_mut::<TraceContext>())
        else {
            return;
        };
        let Some(send_result) = &context.send_result else {
            return;
        };
        if !send_result.trace_on || send_result.region_id.is_empty() {
            return;
        }
        let cost = epoch_millis().saturating_sub(trace_context.timestamp_millis);
        trace_context.cost_time_millis = i32::try_from(cost).unwrap_or(i32::MAX);
        trace_context.success = send_result.status == crate::SendStatus::SendOk;
        trace_context.region_id = Some(send_result.region_id.clone());
        let store_time_millis =
            trace_context.timestamp_millis + i64::from(trace_context.cost_time_millis) / 2;
        if let Some(bean) = trace_context
            .trace_beans
            .as_mut()
            .and_then(|beans| beans.first_mut())
        {
            bean.message_id = Some(send_result.msg_id.clone());
            bean.offset_message_id = send_result.offset_msg_id.clone();
            bean.store_time_millis = store_time_millis;
        }
        let _ = self.dispatcher.append(trace_context.clone());
    }
}

impl EndTransactionHook for ProducerEndTransactionTraceHook {
    fn hook_name(&self) -> &str {
        "EndTransactionTraceHook"
    }

    fn end_transaction(&self, context: &mut EndTransactionContext<'_>) {
        if context
            .message
            .topic()
            .starts_with(&self.dispatcher.trace_topic_name())
        {
            return;
        }
        let trace = end_transaction_trace_context(
            &self.client_config,
            &self.producer_group,
            context.message,
            EndTransactionTraceData {
                message_id: &context.message_id,
                broker_addr: &context.broker_addr,
                transaction_id: context.transaction_id.as_deref(),
                state: context.transaction_state,
                from_transaction_check: context.from_transaction_check,
            },
        );
        let _ = self.dispatcher.append(trace);
    }
}

struct EndTransactionTraceData<'a> {
    message_id: &'a str,
    broker_addr: &'a str,
    transaction_id: Option<&'a str>,
    state: crate::LocalTransactionState,
    from_transaction_check: bool,
}

fn end_transaction_trace_context(
    client_config: &ClientConfig,
    producer_group: &str,
    message: &Message,
    data: EndTransactionTraceData<'_>,
) -> TraceContext {
    let region_id = message
        .properties()
        .get(rocketmq_classic_common::message::constants::PROPERTY_MSG_REGION)
        .filter(|region| !region.is_empty())
        .cloned()
        .unwrap_or_else(|| "DefaultRegion".to_owned());
    TraceContext {
        trace_type: Some(TraceType::EndTransaction),
        timestamp_millis: epoch_millis(),
        region_id: Some(region_id),
        region_name: Some(String::new()),
        group_name: Some(without_namespace(producer_group)),
        cost_time_millis: 0,
        success: true,
        request_id: None,
        context_code: 0,
        access_channel: None,
        trace_beans: Some(vec![TraceBean {
            topic: Some(without_namespace(message.topic())),
            message_id: Some(data.message_id.to_owned()),
            offset_message_id: None,
            tags: message.tags(),
            keys: message.keys(),
            store_host: Some(data.broker_addr.to_owned()),
            body_length: 0,
            retry_times: 0,
            message_type: Some(MessageType::TransactionCommit),
            transaction_id: data.transaction_id.map(str::to_owned),
            transaction_state: Some(data.state),
            from_transaction_check: data.from_transaction_check,
            // Java EndTransactionTraceHookImpl overrides the TraceBean
            // default local address with the host producer's client ID.
            client_host: Some(client_config.build_client_id()),
            ..TraceBean::default()
        }]),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceProducerKind {
    Produce,
    Consume,
}

impl TraceProducerKind {
    pub const fn values() -> [Self; 2] {
        [Self::Produce, Self::Consume]
    }

    pub const fn java_name(self) -> &'static str {
        match self {
            Self::Produce => "PRODUCE",
            Self::Consume => "CONSUME",
        }
    }

    pub const fn ordinal(self) -> i32 {
        match self {
            Self::Produce => 0,
            Self::Consume => 1,
        }
    }

    pub fn value_of_name(
        name: Option<&str>,
    ) -> Result<Self, rocketmq_classic_protocol::codes::JavaEnumLookupError> {
        Self::values()
            .into_iter()
            .find(|kind| Some(kind.java_name()) == name)
            .ok_or_else(|| match name {
                None => rocketmq_classic_protocol::codes::JavaEnumLookupError::NullName,
                Some(name) => rocketmq_classic_protocol::codes::JavaEnumLookupError::UnknownName {
                    enum_class: "org.apache.rocketmq.client.trace.TraceDispatcher$Type",
                    name: name.to_owned(),
                },
            })
    }
}

fn without_namespace(value: &str) -> String {
    NamespaceUtil::without_namespace(Some(value)).unwrap_or_default()
}

fn epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(i64::MAX as u128) as i64
        })
}

enum TraceCommand {
    Append(TraceContext),
    Flush(oneshot::Sender<()>),
    Shutdown(oneshot::Sender<()>),
}

#[derive(Debug, thiserror::Error)]
pub enum TraceDispatcherError {
    #[error(transparent)]
    Producer(#[from] ProducerError),
    #[error("trace dispatcher has already shut down")]
    Shutdown,
    #[error("trace dispatcher worker is unavailable")]
    WorkerUnavailable,
}

/// Bounded asynchronous publisher for Java-compatible trace records.
pub struct TraceDispatcher {
    producer_config: RwLock<ProducerConfig>,
    manager: ClientRuntimeManager,
    rpc_hook: Option<Arc<dyn RpcHook>>,
    trace_topic_name: Arc<RwLock<String>>,
    access_channel: Arc<RwLock<Option<AccessChannel>>>,
    batch_size: usize,
    trace_producer: Mutex<Option<Arc<Producer>>>,
    sender: mpsc::Sender<TraceCommand>,
    receiver: Mutex<Option<mpsc::Receiver<TraceCommand>>>,
    start_gate: tokio::sync::Mutex<()>,
    accept_gate: Mutex<()>,
    started: AtomicBool,
    shutdown: AtomicBool,
    worker: Mutex<Option<JoinHandle<()>>>,
    shutdown_done: watch::Sender<Option<bool>>,
}

impl TraceDispatcher {
    #[doc(hidden)]
    pub fn new(
        client_config: &ClientConfig,
        group: &str,
        kind: TraceProducerKind,
        customized_trace_topic: Option<&str>,
        manager: ClientRuntimeManager,
        rpc_hook: Option<Arc<dyn RpcHook>>,
    ) -> Result<Self, TraceDispatcherError> {
        let trace_number = TRACE_PRODUCER_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
        let mut trace_client_config = client_config.clone();
        let namespaced_group = trace_client_config
            .with_namespace(Some(group))
            .map_err(ProducerError::from)?
            .unwrap_or_else(|| group.to_owned());
        let mut producer_config = ProducerConfig::new(format!(
            "{TRACE_GROUP_PREFIX}-{namespaced_group}-{}-{trace_number}",
            kind.java_name()
        ))?;
        // Java gives the internal producer the already namespaced host group
        // and namespaceV2 RPC metadata, but does not copy the legacy namespace.
        // Clearing it also keeps a custom trace topic unchanged on the wire.
        trace_client_config.set_namespace(None);
        trace_client_config.set_vip_channel_enabled(false);
        trace_client_config.set_enable_trace(false);
        *producer_config.client_config_mut() = trace_client_config;
        producer_config.set_send_timeout_millis(TRACE_SEND_TIMEOUT_MILLIS);
        producer_config.set_max_message_size(TRACE_MAX_MESSAGE_SIZE as i32);

        let trace_topic_name = customized_trace_topic
            .filter(|topic| !topic.trim().is_empty())
            .unwrap_or(TRACE_TOPIC)
            .to_owned();
        let batch_size = usize::try_from(client_config.trace_msg_batch_num())
            .unwrap_or(1)
            .clamp(1, TRACE_MAX_BATCH_SIZE);
        let (sender, receiver) = mpsc::channel(TRACE_QUEUE_CAPACITY);
        let (shutdown_done, shutdown_waiter) = watch::channel(None);
        drop(shutdown_waiter);
        Ok(Self {
            producer_config: RwLock::new(producer_config),
            manager,
            rpc_hook,
            trace_topic_name: Arc::new(RwLock::new(trace_topic_name)),
            access_channel: Arc::new(RwLock::new(client_config.access_channel())),
            batch_size,
            trace_producer: Mutex::new(None),
            sender,
            receiver: Mutex::new(Some(receiver)),
            start_gate: tokio::sync::Mutex::new(()),
            accept_gate: Mutex::new(()),
            started: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            worker: Mutex::new(None),
            shutdown_done,
        })
    }

    pub fn access_channel(&self) -> Option<AccessChannel> {
        *self
            .access_channel
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn set_access_channel(&self, access_channel: Option<AccessChannel>) {
        *self
            .access_channel
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = access_channel;
        self.producer_config
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .client_config_mut()
            .set_access_channel(access_channel);
    }

    pub fn trace_topic_name(&self) -> String {
        self.trace_topic_name
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn set_trace_topic_name(&self, trace_topic_name: impl Into<String>) {
        *self
            .trace_topic_name
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = trace_topic_name.into();
    }

    pub fn trace_producer(&self) -> Option<Arc<Producer>> {
        self.trace_producer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn namespace_v2(&self) -> Option<String> {
        self.producer_config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .client_config()
            .namespace_v2()
            .map(str::to_owned)
    }

    pub fn set_namespace_v2(&self, namespace_v2: Option<String>) {
        self.producer_config
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .client_config_mut()
            .set_namespace_v2(namespace_v2);
    }

    pub async fn start(&self) -> Result<(), TraceDispatcherError> {
        let _start_gate = self.start_gate.lock().await;
        if self.shutdown.load(Ordering::Acquire) {
            return Err(TraceDispatcherError::Shutdown);
        }
        if self.started.load(Ordering::Acquire) {
            return Ok(());
        }

        let producer_config = self
            .producer_config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut producer_config = producer_config;
        producer_config
            .client_config_mut()
            .set_access_channel(self.access_channel());
        let producer = Arc::new(Producer::with_runtime_manager_and_hook(
            producer_config,
            self.manager.clone(),
            self.rpc_hook.clone(),
        ));
        Box::pin(producer.start()).await?;
        *self
            .trace_producer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(producer.clone());
        let receiver = self
            .receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(receiver) = receiver else {
            self.trace_producer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            let _ = producer
                .shutdown_owned_trace_producer(Instant::now() + TRACE_SHUTDOWN_CLEANUP_TIMEOUT)
                .await;
            return Err(TraceDispatcherError::WorkerUnavailable);
        };
        let trace_topic_name = self.trace_topic_name.clone();
        let access_channel = self.access_channel.clone();
        let batch_size = self.batch_size;
        let worker = tokio::spawn(async move {
            trace_worker(
                receiver,
                producer,
                trace_topic_name,
                access_channel,
                batch_size,
            )
            .await;
        });
        *self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(worker);
        self.started.store(true, Ordering::Release);
        Ok(())
    }

    /// Adds a context without blocking the business send or consume path.
    pub fn append(&self, context: TraceContext) -> bool {
        let _accept_gate = self
            .accept_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.shutdown.load(Ordering::Acquire) {
            return false;
        }
        self.sender.try_send(TraceCommand::Append(context)).is_ok()
    }

    pub async fn flush(&self) -> Result<(), TraceDispatcherError> {
        if !self.started.load(Ordering::Acquire) {
            return Ok(());
        }
        let (reply, response) = oneshot::channel();
        self.sender
            .send(TraceCommand::Flush(reply))
            .await
            .map_err(|_| TraceDispatcherError::WorkerUnavailable)?;
        response
            .await
            .map_err(|_| TraceDispatcherError::WorkerUnavailable)
    }

    pub async fn shutdown(&self) -> Result<(), TraceDispatcherError> {
        let mut completion = self.shutdown_done.subscribe();
        {
            let _start_gate = self.start_gate.lock().await;
            let _accept_gate = self
                .accept_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !self.shutdown.swap(true, Ordering::AcqRel) {
                let worker = self
                    .worker
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                let producer = self
                    .trace_producer
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                let sender = self.sender.clone();
                let shutdown_done = self.shutdown_done.clone();
                tokio::spawn(async move {
                    let result = finish_trace_shutdown(worker, sender, producer).await;
                    shutdown_done.send_replace(Some(result));
                });
            }
        }
        match wait_for_shutdown(&mut completion).await {
            Ok(()) => Ok(()),
            Err(()) => Err(TraceDispatcherError::WorkerUnavailable),
        }
    }
}

async fn finish_trace_shutdown(
    worker: Option<JoinHandle<()>>,
    sender: mpsc::Sender<TraceCommand>,
    producer: Option<Arc<Producer>>,
) -> bool {
    let started_at = Instant::now();
    let graceful_deadline = started_at + TRACE_SHUTDOWN_TIMEOUT;
    let shutdown_deadline = graceful_deadline + TRACE_SHUTDOWN_CLEANUP_TIMEOUT;
    let mut producer_stopped = false;
    if let Some(mut worker_handle) = worker {
        let (reply, response) = oneshot::channel();
        let admitted = matches!(
            timeout_at(
                graceful_deadline,
                sender.send(TraceCommand::Shutdown(reply))
            )
            .await,
            Ok(Ok(()))
        );
        if admitted {
            let _ = timeout_at(graceful_deadline, response).await;
        }
        match timeout_at(graceful_deadline, &mut worker_handle).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::debug!(error = %error, "trace dispatcher worker stopped unexpectedly");
                worker_handle.abort();
                let _ = worker_handle.await;
            }
            Err(_) => {
                worker_handle.abort();
                let _ = worker_handle.await;
            }
        }
    }

    if !producer_stopped {
        if let Some(producer) = producer {
            match timeout_at(
                shutdown_deadline,
                producer.shutdown_owned_trace_producer(shutdown_deadline),
            )
            .await
            {
                Ok(Ok(())) => producer_stopped = true,
                Ok(Err(error)) => {
                    tracing::debug!(error = %error, "trace producer cleanup failed");
                }
                Err(_) => {
                    tracing::warn!("trace producer cleanup exceeded the shared shutdown deadline");
                }
            }
        } else {
            producer_stopped = true;
        }
    }

    producer_stopped
}

async fn wait_for_shutdown(completion: &mut watch::Receiver<Option<bool>>) -> Result<(), ()> {
    loop {
        if let Some(stopped) = *completion.borrow() {
            return stopped.then_some(()).ok_or(());
        }
        completion.changed().await.map_err(|_| ())?;
    }
}

async fn trace_worker(
    mut receiver: mpsc::Receiver<TraceCommand>,
    producer: Arc<Producer>,
    default_trace_topic: Arc<RwLock<String>>,
    default_access_channel: Arc<RwLock<Option<AccessChannel>>>,
    batch_size: usize,
) {
    let mut pending = Vec::with_capacity(batch_size);
    let mut flush_clock = interval_at(Instant::now() + TRACE_FLUSH_INTERVAL, TRACE_FLUSH_INTERVAL);
    loop {
        tokio::select! {
            command = receiver.recv() => {
                match command {
                    Some(TraceCommand::Append(context)) => {
                        pending.push(context);
                        if pending.len() >= batch_size {
                            let topic = default_trace_topic
                                .read()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .clone();
                            send_contexts(
                                &producer,
                                &topic,
                                &default_access_channel,
                                std::mem::take(&mut pending),
                            )
                            .await;
                            pending = Vec::with_capacity(batch_size);
                        }
                    }
                    Some(TraceCommand::Flush(reply)) => {
                        let topic = default_trace_topic
                            .read()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clone();
                        send_contexts(
                            &producer,
                            &topic,
                            &default_access_channel,
                            std::mem::take(&mut pending),
                        )
                        .await;
                        pending = Vec::with_capacity(batch_size);
                        let _ = reply.send(());
                    }
                    Some(TraceCommand::Shutdown(reply)) => {
                        let topic = default_trace_topic
                            .read()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clone();
                        send_contexts(
                            &producer,
                            &topic,
                            &default_access_channel,
                            std::mem::take(&mut pending),
                        )
                        .await;
                        if let Err(error) = producer.shutdown().await {
                            tracing::debug!(error = %error, "trace producer shutdown failed");
                        }
                        let _ = reply.send(());
                        return;
                    }
                    None => {
                        let topic = default_trace_topic
                            .read()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clone();
                        send_contexts(
                            &producer,
                            &topic,
                            &default_access_channel,
                            std::mem::take(&mut pending),
                        )
                        .await;
                        if let Err(error) = producer.shutdown().await {
                            tracing::debug!(error = %error, "trace producer shutdown failed");
                        }
                        return;
                    }
                }
            }
            _ = flush_clock.tick(), if !pending.is_empty() => {
                let topic = default_trace_topic
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                send_contexts(
                    &producer,
                    &topic,
                    &default_access_channel,
                    std::mem::take(&mut pending),
                )
                .await;
                pending = Vec::with_capacity(batch_size);
            }
        }
    }
}

async fn send_contexts(
    producer: &Producer,
    default_trace_topic: &str,
    default_access_channel: &RwLock<Option<AccessChannel>>,
    contexts: Vec<TraceContext>,
) {
    let mut grouped = BTreeMap::<(String, String), Vec<TraceContext>>::new();
    for context in contexts {
        let Some(region_id) = context.region_id.as_deref() else {
            continue;
        };
        let Some(first_bean) = context.trace_beans.as_ref().and_then(|beans| beans.first()) else {
            continue;
        };
        let access_channel = context.access_channel.unwrap_or_else(|| {
            default_access_channel
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .unwrap_or_else(|| {
                    producer
                        .config()
                        .client_config()
                        .access_channel()
                        .unwrap_or_default()
                })
        });
        let trace_topic = if access_channel == AccessChannel::Cloud {
            format!("{TRACE_TOPIC_PREFIX}{region_id}")
        } else {
            default_trace_topic.to_owned()
        };
        grouped
            .entry((first_bean.topic.clone().unwrap_or_default(), trace_topic))
            .or_default()
            .push(context);
    }

    for ((_, trace_topic), contexts) in grouped {
        for (data, keys) in encode_trace_chunks(contexts) {
            send_trace_message(producer, &trace_topic, data, &keys).await;
        }
    }
}

fn encode_trace_chunks(contexts: Vec<TraceContext>) -> Vec<(String, BTreeSet<String>)> {
    let mut chunks = Vec::new();
    let mut data = String::with_capacity(TRACE_MAX_MESSAGE_SIZE);
    let mut keys = BTreeSet::new();
    for context in contexts {
        let encoded = catch_unwind(AssertUnwindSafe(|| context.encode()));
        let Ok(encoded) = encoded else {
            tracing::warn!("dropping malformed trace context after encoding failure");
            continue;
        };
        data.push_str(&encoded);
        keys.extend(context.transfer_keys());
        if data.encode_utf16().count() >= TRACE_MAX_MESSAGE_SIZE {
            chunks.push((std::mem::take(&mut data), std::mem::take(&mut keys)));
            data = String::with_capacity(TRACE_MAX_MESSAGE_SIZE);
            keys = BTreeSet::new();
        }
    }
    if !data.is_empty() {
        chunks.push((data, keys));
    }
    chunks
}

async fn send_trace_message(
    producer: &Producer,
    topic: &str,
    data: String,
    keys: &BTreeSet<String>,
) {
    let mut message = Message::new(topic, data.into_bytes());
    if let Err(error) =
        message.set_keys(keys.iter().cloned().collect::<Vec<_>>().join(KEY_SEPARATOR))
    {
        tracing::debug!(error = %error, "trace message key assignment failed");
        return;
    }
    if let Err(error) = producer.send_future(&mut message).await {
        tracing::debug!(error = %error, "trace message send failed");
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, atomic::AtomicUsize},
    };

    use crate::CommunicationMode;
    use rocketmq_classic_common::message::MessageQueue;

    use super::*;

    fn recall_trace(message_id: impl Into<String>) -> TraceContext {
        TraceContext {
            trace_type: Some(TraceType::Recall),
            timestamp_millis: 1,
            region_id: Some("region-a".into()),
            region_name: Some(String::new()),
            group_name: Some("producer-group".into()),
            cost_time_millis: 0,
            success: true,
            request_id: None,
            context_code: 0,
            access_channel: None,
            trace_beans: Some(vec![TraceBean {
                topic: Some("orders".into()),
                message_id: Some(message_id.into()),
                ..TraceBean::default()
            }]),
        }
    }

    #[test]
    fn end_transaction_trace_client_host_matches_java_host_producer_client_id() {
        let mut client_config = ClientConfig::default();
        client_config.set_client_ip(Some("192.0.2.17".into()));
        client_config.set_instance_name(Some("trace-host".into()));
        client_config.set_namespace(Some("tenant".into()));
        let expected_client_id = "192.0.2.17@trace-host";
        let message = Message::new("other%orders", b"transaction".to_vec());

        let context = end_transaction_trace_context(
            &client_config,
            "other%producer-group",
            &message,
            EndTransactionTraceData {
                message_id: "message-id",
                broker_addr: "broker:10911",
                transaction_id: Some("transaction-id"),
                state: crate::LocalTransactionState::Commit,
                from_transaction_check: false,
            },
        );

        assert_eq!(
            context.trace_beans.as_ref().unwrap()[0]
                .client_host
                .as_deref(),
            Some(expected_client_id)
        );
        assert_eq!(context.group_name.as_deref(), Some("producer-group"));
        assert_eq!(
            context.trace_beans.as_ref().unwrap()[0].topic.as_deref(),
            Some("orders")
        );
    }

    #[test]
    fn trace_queue_accepts_java_capacity_and_drops_overflow_without_blocking() {
        let dispatcher = TraceDispatcher::new(
            &ClientConfig::default(),
            "trace-capacity-test",
            TraceProducerKind::Produce,
            None,
            ClientRuntimeManager::new(),
            None,
        )
        .unwrap();
        for index in 0..TRACE_QUEUE_CAPACITY {
            assert!(dispatcher.append(recall_trace(index.to_string())));
        }
        assert!(!dispatcher.append(recall_trace("overflow")));
    }

    #[tokio::test]
    async fn shutdown_owns_worker_across_caller_cancellation_and_full_queue() {
        let dispatcher = Arc::new(
            TraceDispatcher::new(
                &ClientConfig::default(),
                "trace-shutdown-cancel-test",
                TraceProducerKind::Produce,
                None,
                ClientRuntimeManager::new(),
                None,
            )
            .unwrap(),
        );
        for index in 0..TRACE_QUEUE_CAPACITY {
            assert!(dispatcher.append(recall_trace(index.to_string())));
        }

        let mut receiver = dispatcher
            .receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap();
        let processed = Arc::new(AtomicUsize::new(0));
        let worker_finished = Arc::new(AtomicBool::new(false));
        let processed_worker = processed.clone();
        let finished_worker = worker_finished.clone();
        let worker = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            while let Some(command) = receiver.recv().await {
                match command {
                    TraceCommand::Append(_) => {
                        processed_worker.fetch_add(1, Ordering::Relaxed);
                    }
                    TraceCommand::Flush(reply) => {
                        let _ = reply.send(());
                    }
                    TraceCommand::Shutdown(reply) => {
                        let _ = reply.send(());
                        break;
                    }
                }
            }
            finished_worker.store(true, Ordering::Release);
        });
        *dispatcher
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(worker);
        dispatcher.started.store(true, Ordering::Release);

        let caller_dispatcher = dispatcher.clone();
        let caller = tokio::spawn(async move { caller_dispatcher.shutdown().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !dispatcher.shutdown.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown should stop accepting appends before awaiting drain");
        caller.abort();

        assert!(!dispatcher.append(recall_trace("after-shutdown")));
        tokio::time::timeout(Duration::from_secs(2), dispatcher.shutdown())
            .await
            .expect("shutdown supervisor should outlive its caller")
            .unwrap();
        assert_eq!(processed.load(Ordering::Acquire), TRACE_QUEUE_CAPACITY);
        assert!(worker_finished.load(Ordering::Acquire));
        dispatcher.shutdown().await.unwrap();
    }

    #[test]
    fn internal_trace_producer_uses_host_namespace_once_and_keeps_namespace_v2() {
        let mut client_config = ClientConfig::default();
        client_config.set_namespace(Some("tenant".into()));
        client_config.set_namespace_v2(Some("tenant-v2".into()));
        let dispatcher = TraceDispatcher::new(
            &client_config,
            "orders-producer",
            TraceProducerKind::Produce,
            Some("custom-trace"),
            ClientRuntimeManager::new(),
            None,
        )
        .unwrap();

        let producer_config = dispatcher
            .producer_config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            producer_config
                .producer_group()
                .expect("the trace producer group is always configured")
                .starts_with("_INNER_TRACE_PRODUCER-tenant%orders-producer-PRODUCE-")
        );
        let mut client_config = producer_config.client_config().clone();
        assert_eq!(client_config.get_namespace().unwrap(), None);
        assert_eq!(
            producer_config.client_config().namespace_v2(),
            Some("tenant-v2")
        );
        drop(producer_config);
        assert_eq!(dispatcher.trace_topic_name(), "custom-trace");
        assert_eq!(dispatcher.access_channel(), Some(AccessChannel::Local));
        dispatcher.set_trace_topic_name("trace-updated");
        assert_eq!(dispatcher.trace_topic_name(), "trace-updated");
        dispatcher.set_access_channel(Some(AccessChannel::Cloud));
        assert_eq!(dispatcher.access_channel(), Some(AccessChannel::Cloud));
        dispatcher.set_namespace_v2(Some("updated-v2".into()));
        assert_eq!(dispatcher.namespace_v2().as_deref(), Some("updated-v2"));
    }

    #[test]
    fn trace_message_chunks_flush_at_the_java_128k_threshold_and_keep_keys_per_chunk() {
        let ids = ["1", "2", "3"].map(|suffix| format!("{}-{suffix}", "x".repeat(65_000)));
        let chunks = encode_trace_chunks(ids.iter().cloned().map(recall_trace).collect());
        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].0.encode_utf16().count() >= TRACE_MAX_MESSAGE_SIZE);
        assert!(chunks[1].0.encode_utf16().count() < TRACE_MAX_MESSAGE_SIZE);
        assert_eq!(
            chunks[0].1,
            BTreeSet::from([ids[0].clone(), ids[1].clone()])
        );
        assert_eq!(chunks[1].1, BTreeSet::from([ids[2].clone()]));
        assert!(chunks.iter().all(|(data, _)| data.ends_with('\u{2}')));
    }

    #[test]
    fn malformed_contexts_are_skipped_without_discarding_valid_trace_rows() {
        let mut malformed_sub_after = TraceContext::new(TraceType::SubAfter);
        malformed_sub_after.region_id = Some("region-a".into());
        malformed_sub_after.trace_beans = Some(vec![TraceBean {
            topic: Some("orders".into()),
            ..TraceBean::default()
        }]);

        let mut malformed_pub = TraceContext::new(TraceType::Pub);
        malformed_pub.region_id = Some("region-a".into());
        malformed_pub.trace_beans = Some(vec![TraceBean {
            topic: Some("orders".into()),
            ..TraceBean::default()
        }]);

        let mut malformed_transaction = TraceContext::new(TraceType::EndTransaction);
        malformed_transaction.region_id = Some("region-a".into());
        malformed_transaction.trace_beans = Some(vec![TraceBean {
            topic: Some("orders".into()),
            ..TraceBean::default()
        }]);

        let chunks = encode_trace_chunks(vec![
            malformed_sub_after,
            malformed_pub,
            malformed_transaction,
            recall_trace("valid-after-malformed"),
        ]);

        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].0.starts_with("Recall\u{1}"));
        assert_eq!(
            chunks[0].1,
            BTreeSet::from(["valid-after-malformed".into()])
        );
    }

    #[test]
    fn producer_send_trace_hook_emits_java_pub_context_only_when_trace_is_enabled() {
        let producer_config = ProducerConfig::new("trace-hook-producer").unwrap();
        let producer = Producer::new(producer_config);
        let client_config = ClientConfig::default();
        let dispatcher = Arc::new(
            TraceDispatcher::new(
                &client_config,
                "trace-hook-producer",
                TraceProducerKind::Produce,
                None,
                ClientRuntimeManager::new(),
                None,
            )
            .unwrap(),
        );
        let hook = ProducerSendTraceHook::new(dispatcher.clone(), "other%workers");
        let mut message = Message::new("other%orders", b"body".to_vec());
        message.set_tags("paid").unwrap();
        message.set_keys("order-1").unwrap();
        let mut context = SendMessageContext {
            producer_group: Some("other%workers".into()),
            producer: &producer,
            message: &mut message,
            queue: MessageQueue::new("other%orders", "broker-a", 0),
            broker_addr: "127.0.0.1:10911".into(),
            born_host: None,
            communication_mode: CommunicationMode::Sync,
            send_result: None,
            exception: None,
            message_type: SendMessageType::Normal,
            namespace: None,
            trace_context: None,
            properties: Some(HashMap::new()),
        };
        hook.send_message_before(&mut context);
        assert!(context.trace_context.is_some());
        context.send_result = Some(crate::SendResult {
            status: crate::SendStatus::SendOk,
            msg_id: "message-1".into(),
            offset_msg_id: Some("offset-1".into()),
            topic: "other%orders".into(),
            broker_name: "broker-a".into(),
            queue_id: 0,
            queue_offset: 12,
            transaction_id: None,
            recall_handle: None,
            raw_resp_body: None,
            region_id: "region-a".into(),
            trace_on: true,
        });
        hook.send_message_after(&mut context);

        let mut receiver = dispatcher
            .receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(TraceCommand::Append(trace)) = receiver.as_mut().unwrap().try_recv().ok() else {
            panic!("producer hook did not append a trace context");
        };
        assert_eq!(trace.trace_type, Some(TraceType::Pub));
        assert_eq!(trace.region_id.as_deref(), Some("region-a"));
        assert!(
            trace
                .request_id
                .as_deref()
                .is_some_and(|request_id| !request_id.is_empty())
        );
        assert_eq!(trace.group_name.as_deref(), Some("workers"));
        assert_eq!(
            trace.trace_beans.as_ref().unwrap()[0].topic.as_deref(),
            Some("orders")
        );
        assert_eq!(
            trace.trace_beans.as_ref().unwrap()[0].message_id.as_deref(),
            Some("message-1")
        );
        assert_eq!(
            trace.trace_beans.as_ref().unwrap()[0]
                .offset_message_id
                .as_deref(),
            Some("offset-1")
        );
        assert_eq!(trace.trace_beans.as_ref().unwrap()[0].body_length, 4);
        let bean = &trace.trace_beans.as_ref().unwrap()[0];
        assert_eq!(
            bean.store_time_millis,
            trace.timestamp_millis + i64::from(trace.cost_time_millis) / 2
        );
        assert_eq!(bean.message_type, Some(SendMessageType::Normal));

        let mut not_traced = Message::new("other%orders", b"body".to_vec());
        context.message = &mut not_traced;
        context.send_result.as_mut().unwrap().trace_on = false;
        context.trace_context = None;
        hook.send_message_before(&mut context);
        hook.send_message_after(&mut context);
        assert!(receiver.as_mut().unwrap().try_recv().is_err());
    }

    #[test]
    fn recall_trace_context_matches_java_fields_and_unwraps_namespace() {
        let recall_handle =
            crate::RecallMessageHandle::build_handle("orders", "broker-a", "123", "message-1");
        let header = {
            let mut __phase18_header_0 = RecallMessageRequestHeader::default();
            __phase18_header_0.set_producer_group(Some("other%producer-group".into()));
            __phase18_header_0.set_topic("other%orders".into());
            __phase18_header_0.set_recall_handle(recall_handle);
            __phase18_header_0
        };
        let mut fields = rocketmq_classic_protocol::header::HeaderMap::new();
        header.encode_fields(&mut fields);
        let request_command = RemotingCommand {
            code: request::RECALL_MESSAGE,
            ext_fields: Some(fields.into()),
            ..RemotingCommand::default()
        };
        let response_command = RemotingCommand {
            code: response::SUCCESS,
            ext_fields: Some(
                rocketmq_classic_protocol::header::HeaderMap::from([(
                    rocketmq_classic_common::message::constants::PROPERTY_MSG_REGION.into(),
                    "region-a".into(),
                )])
                .into(),
            ),
            ..RemotingCommand::default()
        };

        let trace = recall_trace_context(&request_command, &response_command)
            .expect("valid recall response creates trace context");
        assert_eq!(trace.trace_type, Some(TraceType::Recall));
        assert_eq!(trace.group_name.as_deref(), Some("producer-group"));
        assert_eq!(trace.region_id.as_deref(), Some("region-a"));
        assert!(trace.success);
        assert_eq!(trace.trace_beans.as_ref().unwrap().len(), 1);
        assert_eq!(
            trace.trace_beans.as_ref().unwrap()[0].topic.as_deref(),
            Some("orders")
        );
        assert_eq!(
            trace.trace_beans.as_ref().unwrap()[0].message_id.as_deref(),
            Some("message-1")
        );

        let failed_response = RemotingCommand {
            code: response::SYSTEM_ERROR,
            ..response_command
        };
        assert!(
            !recall_trace_context(&request_command, &failed_response)
                .unwrap()
                .success
        );
        assert!(recall_trace_context(&request_command, &RemotingCommand::default()).is_none());
    }

    #[test]
    fn recall_trace_boolean_setting_matches_java_boolean_parse() {
        assert!(recall_trace_property_value(Some("TRUE")));
        assert!(!recall_trace_property_value(Some(" true ")));
        assert!(!recall_trace_property_value(Some("yes")));
        assert!(!recall_trace_property_value(None));
    }
}
