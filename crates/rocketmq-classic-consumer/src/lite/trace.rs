use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use rocketmq_classic_common::message::{Message, MessageExt, constants::*};
use rocketmq_classic_core::{
    RpcHook,
    config::{AccessChannel, ClientConfig},
};
use rocketmq_classic_producer::{Producer, ProducerConfig, ProducerError};
use rocketmq_classic_protocol::NamespaceUtil;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, interval_at},
};

use crate::consume::{ConsumeMessageContext, ConsumeMessageHook};

const TRACE_TOPIC: &str = "RMQ_SYS_TRACE_TOPIC";
const TRACE_TOPIC_PREFIX: &str = "RMQ_SYS_TRACE_DATA_";
const TRACE_GROUP_PREFIX: &str = "_INNER_TRACE_PRODUCER";
const TRACE_CONTEXT_CAPACITY: usize = 2_048;
const TRACE_MAX_BATCH_SIZE: usize = 20;
const TRACE_MAX_MESSAGE_SIZE: usize = 128_000;
const TRACE_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const FIELD_SEPARATOR: char = '\u{1}';
const RECORD_SEPARATOR: char = '\u{2}';

static TRACE_PRODUCER_COUNTER: AtomicU64 = AtomicU64::new(0);
static TRACE_REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LiteTraceKind {
    SubBefore,
    SubAfter,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiteTraceBean {
    pub topic: String,
    pub message_id: Option<String>,
    pub tags: Option<String>,
    pub keys: Option<String>,
    pub store_time_millis: i64,
    pub body_length: i32,
    pub retry_times: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiteTraceContext {
    pub trace_kind: LiteTraceKind,
    pub timestamp_millis: i64,
    pub region_id: Option<String>,
    pub group_name: String,
    pub request_id: String,
    pub cost_time_millis: i32,
    pub success: bool,
    pub context_code: i32,
    pub access_channel: Option<AccessChannel>,
    pub trace_beans: Vec<LiteTraceBean>,
}

impl LiteTraceContext {
    /// Encodes consume trace rows with Java 5.5.1's control-character format.
    pub fn encode(&self) -> String {
        let mut encoded = String::new();
        for bean in &self.trace_beans {
            match self.trace_kind {
                LiteTraceKind::SubBefore => {
                    append_fields(
                        &mut encoded,
                        [
                            "SubBefore".to_owned(),
                            self.timestamp_millis.to_string(),
                            java_nullable(self.region_id.as_deref()),
                            self.group_name.clone(),
                            self.request_id.clone(),
                            java_nullable(bean.message_id.as_deref()),
                            bean.retry_times.to_string(),
                            java_nullable(bean.keys.as_deref()),
                        ],
                    );
                }
                LiteTraceKind::SubAfter => {
                    let mut fields = vec![
                        "SubAfter".to_owned(),
                        self.request_id.clone(),
                        java_nullable(bean.message_id.as_deref()),
                        self.cost_time_millis.to_string(),
                        self.success.to_string(),
                        java_nullable(bean.keys.as_deref()),
                        self.context_code.to_string(),
                    ];
                    if self.access_channel != Some(AccessChannel::Cloud) {
                        fields.push(self.timestamp_millis.to_string());
                        fields.push(self.group_name.clone());
                    } else {
                        fields.push(String::new());
                    }
                    append_fields(&mut encoded, fields);
                }
            }
        }
        encoded
    }
}

/// Internal Java-compatible consume hook installed when Lite tracing is enabled.
pub(crate) struct LiteConsumeTraceHook {
    dispatcher: Arc<LiteTraceDispatcher>,
    client_config: ClientConfig,
}

impl LiteConsumeTraceHook {
    pub(crate) fn new(dispatcher: Arc<LiteTraceDispatcher>, client_config: ClientConfig) -> Self {
        Self {
            dispatcher,
            client_config,
        }
    }
}

struct LiteConsumeTraceBefore {
    timestamp_millis: i64,
    consumer_group: Option<String>,
}

impl ConsumeMessageHook for LiteConsumeTraceHook {
    fn hook_name(&self) -> &str {
        "ConsumeMessageTraceHook"
    }

    fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
        context.trace_context = Some(Box::new(LiteConsumeTraceBefore {
            timestamp_millis: epoch_millis(),
            consumer_group: context.consumer_group.as_deref().map(without_namespace),
        }));
    }

    fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
        let Some(messages) = context.messages.as_deref() else {
            return;
        };
        let Some(before) = context
            .trace_context
            .as_deref()
            .and_then(|value| value.downcast_ref::<LiteConsumeTraceBefore>())
        else {
            return;
        };
        for trace in consume_contexts(
            messages,
            before.consumer_group.as_deref(),
            &self.client_config,
            before.timestamp_millis,
            epoch_millis(),
        ) {
            let _ = self.dispatcher.append(trace);
        }
    }
}

fn append_fields(encoded: &mut String, fields: impl IntoIterator<Item = String>) {
    let mut first = true;
    for field in fields {
        if !first {
            encoded.push(FIELD_SEPARATOR);
        }
        encoded.push_str(&field);
        first = false;
    }
    encoded.push(RECORD_SEPARATOR);
}

fn java_nullable(value: Option<&str>) -> String {
    value.unwrap_or("null").to_owned()
}

pub(crate) fn consume_contexts(
    messages: &[MessageExt],
    before_consumer_group: Option<&str>,
    client_config: &ClientConfig,
    before_timestamp_millis: i64,
    after_timestamp_millis: i64,
) -> Vec<LiteTraceContext> {
    if messages.is_empty() {
        return Vec::new();
    }
    let before_group_name = before_consumer_group
        .map(str::to_owned)
        .unwrap_or_else(|| "null".to_owned());
    let after_group_name = before_consumer_group
        .map(without_namespace)
        .unwrap_or_else(|| "null".to_owned());
    let mut region_id = None;
    let mut trace_beans = Vec::with_capacity(messages.len());
    for message in messages {
        let properties = message.properties();
        if properties
            .get(PROPERTY_TRACE_SWITCH)
            .is_some_and(|trace_switch| trace_switch == "false")
        {
            continue;
        }
        region_id = properties.get(PROPERTY_MSG_REGION).cloned();
        trace_beans.push(LiteTraceBean {
            topic: without_namespace(message.topic()),
            message_id: message.message_id().map(str::to_owned),
            tags: message.tags(),
            keys: message.keys(),
            store_time_millis: message.store_timestamp(),
            body_length: message.store_size(),
            retry_times: message.reconsume_times(),
        });
    }
    if trace_beans.is_empty() {
        return Vec::new();
    }

    let request_number = TRACE_REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
    let request_id = format!("LITE-{}-{request_number}", before_timestamp_millis);
    let before = LiteTraceContext {
        trace_kind: LiteTraceKind::SubBefore,
        timestamp_millis: before_timestamp_millis,
        region_id: region_id.clone(),
        group_name: before_group_name,
        request_id: request_id.clone(),
        cost_time_millis: 0,
        success: true,
        context_code: 0,
        access_channel: None,
        trace_beans: trace_beans.clone(),
    };
    let per_message_cost = after_timestamp_millis
        .saturating_sub(before_timestamp_millis)
        .checked_div(i64::try_from(messages.len()).unwrap_or(i64::MAX).max(1))
        .unwrap_or_default();
    let after = LiteTraceContext {
        trace_kind: LiteTraceKind::SubAfter,
        timestamp_millis: after_timestamp_millis,
        region_id,
        group_name: after_group_name,
        request_id,
        cost_time_millis: i32::try_from(per_message_cost).unwrap_or_else(|_| {
            if per_message_cost.is_negative() {
                i32::MIN
            } else {
                i32::MAX
            }
        }),
        success: true,
        context_code: 0,
        access_channel: client_config.access_channel(),
        trace_beans,
    };
    vec![before, after]
}

fn without_namespace(value: &str) -> String {
    NamespaceUtil::without_namespace(Some(value)).unwrap_or_default()
}

enum TraceCommand {
    Append(LiteTraceContext),
    Flush(oneshot::Sender<()>),
    Shutdown(oneshot::Sender<()>),
}

#[derive(Debug, thiserror::Error)]
pub enum LiteTraceError {
    #[error(transparent)]
    Producer(#[from] ProducerError),
    #[error("Lite trace dispatcher has already shut down")]
    Shutdown,
    #[error("Lite trace dispatcher worker is unavailable")]
    WorkerUnavailable,
}

/// Asynchronous Lite consume trace publisher using the Java trace wire format.
pub struct LiteTraceDispatcher {
    producer_config: ProducerConfig,
    rpc_hook: Option<Arc<dyn RpcHook>>,
    trace_topic_name: String,
    batch_size: usize,
    trace_producer: Mutex<Option<Arc<Producer>>>,
    sender: mpsc::Sender<TraceCommand>,
    receiver: Mutex<Option<mpsc::Receiver<TraceCommand>>>,
    start_gate: tokio::sync::Mutex<()>,
    started: AtomicBool,
    shutdown: AtomicBool,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl LiteTraceDispatcher {
    pub(crate) fn new(
        client_config: &ClientConfig,
        consumer_group: &str,
        customized_trace_topic: Option<&str>,
        rpc_hook: Option<Arc<dyn RpcHook>>,
    ) -> Result<Self, LiteTraceError> {
        let trace_number = TRACE_PRODUCER_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
        let mut producer_client_config = client_config.clone();
        let namespaced_group = producer_client_config
            .with_namespace(Some(consumer_group))
            .map_err(ProducerError::from)?
            .unwrap_or_else(|| consumer_group.to_owned());
        let mut producer_config = ProducerConfig::new(format!(
            "{TRACE_GROUP_PREFIX}-{namespaced_group}-CONSUME-{trace_number}"
        ))?;
        // Match Java AsyncTraceDispatcher: retain namespaceV2, while the
        // generated trace group already contains the host namespace.
        producer_client_config.set_namespace(None);
        producer_client_config.set_vip_channel_enabled(false);
        producer_client_config.set_enable_trace(false);
        *producer_config.client_config_mut() = producer_client_config;
        producer_config.set_send_timeout_millis(5_000);
        producer_config.set_max_message_size(TRACE_MAX_MESSAGE_SIZE as i32);

        let trace_topic_name = customized_trace_topic
            .filter(|topic| !topic.trim().is_empty())
            .unwrap_or(TRACE_TOPIC)
            .to_owned();
        let batch_size = usize::try_from(client_config.trace_msg_batch_num())
            .unwrap_or(1)
            .clamp(1, TRACE_MAX_BATCH_SIZE);
        let (sender, receiver) = mpsc::channel(TRACE_CONTEXT_CAPACITY);
        Ok(Self {
            producer_config,
            rpc_hook,
            trace_topic_name,
            batch_size,
            trace_producer: Mutex::new(None),
            sender,
            receiver: Mutex::new(Some(receiver)),
            start_gate: tokio::sync::Mutex::new(()),
            started: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            worker: Mutex::new(None),
        })
    }

    pub fn trace_topic_name(&self) -> &str {
        &self.trace_topic_name
    }

    /// Returns the owned internal producer after the dispatcher has started.
    pub fn trace_producer(&self) -> Option<Arc<Producer>> {
        self.trace_producer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Starts the trace producer and its bounded batching worker.
    pub async fn start(
        &self,
        name_server_address: Option<String>,
        access_channel: AccessChannel,
    ) -> Result<(), LiteTraceError> {
        let _start_gate = self.start_gate.lock().await;
        if self.shutdown.load(Ordering::Acquire) {
            return Err(LiteTraceError::Shutdown);
        }
        if self.started.load(Ordering::Acquire) {
            return Ok(());
        }

        let mut producer_config = self.producer_config.clone();
        let client = producer_config.client_config_mut();
        if let Some(address) = name_server_address {
            client.set_namesrv_addr(Some(address));
        }
        client.set_access_channel(Some(access_channel));
        let producer = Arc::new(Producer::new_with_rpc_hook(
            producer_config,
            self.rpc_hook.clone(),
        ));
        producer.start().await?;
        *self
            .trace_producer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(producer.clone());
        let receiver = self
            .receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or(LiteTraceError::WorkerUnavailable)?;
        let trace_topic_name = self.trace_topic_name.clone();
        let batch_size = self.batch_size;
        let worker = tokio::spawn(async move {
            trace_worker(receiver, producer, trace_topic_name, batch_size).await;
        });
        *self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(worker);
        self.started.store(true, Ordering::Release);
        Ok(())
    }

    /// Adds a consume trace context without blocking the polling path.
    pub fn append(&self, context: LiteTraceContext) -> bool {
        if self.shutdown.load(Ordering::Acquire) {
            return false;
        }
        self.sender.try_send(TraceCommand::Append(context)).is_ok()
    }

    /// Sends all contexts queued before this call.
    pub async fn flush(&self) -> Result<(), LiteTraceError> {
        if !self.started.load(Ordering::Acquire) {
            return Ok(());
        }
        let (reply, response) = oneshot::channel();
        self.sender
            .send(TraceCommand::Flush(reply))
            .await
            .map_err(|_| LiteTraceError::WorkerUnavailable)?;
        response
            .await
            .map_err(|_| LiteTraceError::WorkerUnavailable)
    }

    /// Flushes queued contexts and stops the trace producer.
    pub async fn shutdown(&self) -> Result<(), LiteTraceError> {
        let _start_gate = self.start_gate.lock().await;
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(mut worker) = worker else {
            return Ok(());
        };
        let (reply, response) = oneshot::channel();
        self.sender
            .send(TraceCommand::Shutdown(reply))
            .await
            .map_err(|_| LiteTraceError::WorkerUnavailable)?;
        response
            .await
            .map_err(|_| LiteTraceError::WorkerUnavailable)?;
        if tokio::time::timeout(Duration::from_secs(6), &mut worker)
            .await
            .is_err()
        {
            worker.abort();
            let _ = worker.await;
        }
        Ok(())
    }
}

async fn trace_worker(
    mut receiver: mpsc::Receiver<TraceCommand>,
    producer: Arc<Producer>,
    default_trace_topic: String,
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
                            send_contexts(&producer, &default_trace_topic, std::mem::take(&mut pending)).await;
                            pending = Vec::with_capacity(batch_size);
                        }
                    }
                    Some(TraceCommand::Flush(reply)) => {
                        send_contexts(&producer, &default_trace_topic, std::mem::take(&mut pending)).await;
                        pending = Vec::with_capacity(batch_size);
                        let _ = reply.send(());
                    }
                    Some(TraceCommand::Shutdown(reply)) => {
                        send_contexts(&producer, &default_trace_topic, std::mem::take(&mut pending)).await;
                        if let Err(error) = producer.shutdown().await {
                            tracing::debug!(error = %error, "Lite trace producer shutdown failed");
                        }
                        let _ = reply.send(());
                        return;
                    }
                    None => {
                        send_contexts(&producer, &default_trace_topic, std::mem::take(&mut pending)).await;
                        if let Err(error) = producer.shutdown().await {
                            tracing::debug!(error = %error, "Lite trace producer shutdown failed");
                        }
                        return;
                    }
                }
            }
            _ = flush_clock.tick(), if !pending.is_empty() => {
                send_contexts(&producer, &default_trace_topic, std::mem::take(&mut pending)).await;
                pending = Vec::with_capacity(batch_size);
            }
        }
    }
}

async fn send_contexts(
    producer: &Producer,
    default_trace_topic: &str,
    contexts: Vec<LiteTraceContext>,
) {
    let mut grouped = BTreeMap::<(String, String), (String, BTreeSet<String>)>::new();
    for context in contexts {
        let Some(region_id) = context.region_id.as_deref() else {
            continue;
        };
        let Some(first_bean) = context.trace_beans.first() else {
            continue;
        };
        let access_channel = context.access_channel.unwrap_or_else(|| {
            producer
                .config()
                .client_config()
                .access_channel()
                .unwrap_or_default()
        });
        let trace_topic = if access_channel == AccessChannel::Cloud {
            format!("{TRACE_TOPIC_PREFIX}{region_id}")
        } else {
            default_trace_topic.to_owned()
        };
        let entry = grouped
            .entry((first_bean.topic.clone(), trace_topic.clone()))
            .or_default();
        entry.0.push_str(&context.encode());
        for bean in context.trace_beans {
            if let Some(message_id) = bean.message_id {
                entry.1.insert(message_id);
            }
            if let Some(keys) = bean.keys {
                entry.1.extend(keys.split(KEY_SEPARATOR).map(str::to_owned));
            }
        }
    }

    for ((_, trace_topic), (data, keys)) in grouped {
        let mut message = Message::new(trace_topic, data.into_bytes());
        if let Err(error) =
            message.set_keys(keys.into_iter().collect::<Vec<_>>().join(KEY_SEPARATOR))
        {
            tracing::debug!(error = %error, "Lite trace message key assignment failed");
            continue;
        }
        if let Err(error) = producer.send_future(&mut message).await {
            tracing::debug!(error = %error, "Lite trace message send failed");
        }
    }
}

pub(crate) fn epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(i64::MAX as u128) as i64
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_common::message::Message;

    struct NoopRpcHook;

    impl RpcHook for NoopRpcHook {}

    #[test]
    fn consume_context_wire_rows_match_java_trace_encoder_layout() {
        let bean = LiteTraceBean {
            topic: "orders".to_owned(),
            message_id: Some("msg-1".to_owned()),
            tags: Some("paid".to_owned()),
            keys: Some("order-1 order-2".to_owned()),
            store_time_millis: 1_700,
            body_length: 45,
            retry_times: 2,
        };
        let before = LiteTraceContext {
            trace_kind: LiteTraceKind::SubBefore,
            timestamp_millis: 1_000,
            region_id: Some("region-a".to_owned()),
            group_name: "workers".to_owned(),
            request_id: "req-1".to_owned(),
            cost_time_millis: 0,
            success: true,
            context_code: 0,
            access_channel: None,
            trace_beans: vec![bean.clone()],
        };
        let after = LiteTraceContext {
            trace_kind: LiteTraceKind::SubAfter,
            timestamp_millis: 1_005,
            region_id: Some("region-a".to_owned()),
            group_name: "workers".to_owned(),
            request_id: "req-1".to_owned(),
            cost_time_millis: 5,
            success: true,
            context_code: 0,
            access_channel: Some(AccessChannel::Local),
            trace_beans: vec![bean],
        };
        assert_eq!(
            before.encode(),
            "SubBefore\u{1}1000\u{1}region-a\u{1}workers\u{1}req-1\u{1}msg-1\u{1}2\u{1}order-1 order-2\u{2}"
        );
        assert_eq!(
            after.encode(),
            "SubAfter\u{1}req-1\u{1}msg-1\u{1}5\u{1}true\u{1}order-1 order-2\u{1}0\u{1}1005\u{1}workers\u{2}"
        );
        let mut cloud = after;
        cloud.access_channel = Some(AccessChannel::Cloud);
        assert_eq!(
            cloud.encode(),
            "SubAfter\u{1}req-1\u{1}msg-1\u{1}5\u{1}true\u{1}order-1 order-2\u{1}0\u{1}\u{2}"
        );
    }

    #[test]
    fn consume_context_builder_obeys_namespace_trace_switch_and_batch_cost() {
        let mut client = ClientConfig::new("lite-trace");
        client.set_namespace(Some("ns".to_owned()));
        let mut included =
            crate::test_support::message_ext_from(Message::new("ns%orders", b"body".to_vec()));
        included.set_message_id(Some("msg-1".to_owned()));
        included.set_store_timestamp(1_700);
        included.set_store_size(4);
        included.set_reconsume_times(2);
        included.set_tags("paid").unwrap();
        included.set_keys("order-1 order-2").unwrap();
        included
            .set_system_property(PROPERTY_MSG_REGION, "region-a")
            .unwrap();
        let mut excluded =
            crate::test_support::message_ext_from(Message::new("ns%orders", b"skip".to_vec()));
        excluded
            .set_system_property(PROPERTY_TRACE_SWITCH, "false")
            .unwrap();

        let contexts = consume_contexts(
            &[included, excluded],
            Some("workers"),
            &client,
            1_000,
            1_020,
        );
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[0].group_name, "workers");
        assert_eq!(contexts[0].trace_beans.len(), 1);
        assert_eq!(contexts[0].trace_beans[0].topic, "orders");
        assert_eq!(contexts[0].trace_beans[0].body_length, 4);
        assert_eq!(contexts[0].trace_beans[0].retry_times, 2);
        assert_eq!(contexts[1].cost_time_millis, 10);
        assert_eq!(contexts[0].request_id, contexts[1].request_id);
        assert!(consume_contexts(&[], Some("workers"), &client, 1, 2).is_empty());
    }

    #[test]
    fn lite_consume_trace_hook_appends_before_and_after_rows_from_shared_context() {
        let mut client = ClientConfig::new("lite-trace-hook");
        client.set_namespace(Some("ns".to_owned()));
        let dispatcher =
            Arc::new(LiteTraceDispatcher::new(&client, "ns%workers", None, None).unwrap());
        let hook = LiteConsumeTraceHook::new(dispatcher.clone(), client.clone());
        let mut message =
            crate::test_support::message_ext_from(Message::new("ns%orders", b"body".to_vec()));
        message.set_message_id(Some("msg-1".to_owned()));
        message
            .set_system_property(PROPERTY_MSG_REGION, "region-a")
            .unwrap();
        let mut context = ConsumeMessageContext::new(
            Some("ns%workers".to_owned()),
            Some(vec![message]),
            rocketmq_classic_common::message::MessageQueue::new("ns%orders", "broker-a", 0),
            Some("ns".to_owned()),
        );

        hook.consume_message_before(&mut context);
        context.success = true;
        context.status = Some("CONSUME_SUCCESS".to_owned());
        hook.consume_message_after(&mut context);

        let mut receiver = dispatcher
            .receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap();
        let Some(TraceCommand::Append(before)) = receiver.try_recv().ok() else {
            panic!("Lite trace hook did not append the before record");
        };
        let Some(TraceCommand::Append(after)) = receiver.try_recv().ok() else {
            panic!("Lite trace hook did not append the after record");
        };
        assert_eq!(before.trace_kind, LiteTraceKind::SubBefore);
        assert_eq!(after.trace_kind, LiteTraceKind::SubAfter);
        assert_eq!(before.group_name, "workers");
        assert_eq!(before.trace_beans[0].topic, "orders");
        assert_eq!(before.request_id, after.request_id);
        assert!(after.success);
    }

    #[test]
    fn consume_trace_rows_keep_the_before_hook_live_group_identity() {
        let cases = [
            (
                Some("tenant"),
                Some("tenant%initial-group"),
                "tenant%orders",
                "initial-group",
                "initial-group",
                "orders",
            ),
            (
                Some("tenant"),
                Some("other%later-group"),
                "%RETRY%other%orders",
                "later-group",
                "later-group",
                "%RETRY%orders",
            ),
            (
                None,
                Some("other%later-group"),
                "%DLQ%other%orders",
                "later-group",
                "later-group",
                "%DLQ%orders",
            ),
            (
                Some("tenant"),
                Some("tenant%tenant%later-group"),
                "tenant%tenant%orders",
                "tenant%later-group",
                "later-group",
                "tenant%orders",
            ),
            (
                Some("tenant"),
                Some("CID_RMQ_SYS_HEART_BEAT"),
                "RMQ_SYS_TRACE_TOPIC",
                "CID_RMQ_SYS_HEART_BEAT",
                "CID_RMQ_SYS_HEART_BEAT",
                "RMQ_SYS_TRACE_TOPIC",
            ),
            (Some("tenant"), Some(""), "", "", "", ""),
            (
                Some("tenant"),
                None,
                "tenant%orders",
                "null",
                "null",
                "orders",
            ),
        ];

        for (
            namespace,
            live_group,
            topic,
            expected_before_group,
            expected_after_group,
            expected_topic,
        ) in cases
        {
            let mut client = ClientConfig::new("lite-trace-live-group");
            client.set_namespace(namespace.map(str::to_owned));
            let dispatcher =
                Arc::new(LiteTraceDispatcher::new(&client, "initial-group", None, None).unwrap());
            let hook = LiteConsumeTraceHook::new(dispatcher.clone(), client.clone());
            let message =
                crate::test_support::message_ext_from(Message::new(topic, b"body".to_vec()));
            let mut context = ConsumeMessageContext::new(
                live_group.map(str::to_owned),
                Some(vec![message]),
                rocketmq_classic_common::message::MessageQueue::new(topic, "broker-a", 0),
                namespace.map(str::to_owned),
            );

            hook.consume_message_before(&mut context);
            context.consumer_group = Some("mutated-after-before".to_owned());
            hook.consume_message_after(&mut context);

            let mut receiver = dispatcher
                .receiver
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .unwrap();
            let Some(TraceCommand::Append(before)) = receiver.try_recv().ok() else {
                panic!("Lite trace hook did not append the before record");
            };
            let Some(TraceCommand::Append(after)) = receiver.try_recv().ok() else {
                panic!("Lite trace hook did not append the after record");
            };
            assert_eq!(
                before.group_name, expected_before_group,
                "namespace={namespace:?}"
            );
            assert_eq!(
                after.group_name, expected_after_group,
                "namespace={namespace:?}"
            );
            assert_eq!(before.trace_beans[0].topic, expected_topic);
            assert_eq!(after.trace_beans[0].topic, expected_topic);
        }
    }

    #[test]
    fn dispatcher_topic_defaults_and_customization_match_java_settings() {
        let client = ClientConfig::new("lite-trace-topic");
        let default = LiteTraceDispatcher::new(&client, "workers", None, None).unwrap();
        assert_eq!(default.trace_topic_name(), TRACE_TOPIC);
        let blank = LiteTraceDispatcher::new(&client, "workers", Some("  "), None).unwrap();
        assert_eq!(blank.trace_topic_name(), TRACE_TOPIC);
        let custom =
            LiteTraceDispatcher::new(&client, "workers", Some("trace-custom"), None).unwrap();
        assert_eq!(custom.trace_topic_name(), "trace-custom");
    }

    #[tokio::test]
    async fn lite_trace_dispatcher_exposes_its_owned_trace_producer() {
        let client = ClientConfig::new("lite-trace-producer-accessor");
        let dispatcher = LiteTraceDispatcher::new(&client, "workers", None, None).unwrap();
        assert!(dispatcher.trace_producer().is_none());

        dispatcher.start(None, AccessChannel::Local).await.unwrap();
        let trace_producer = dispatcher.trace_producer().unwrap();
        dispatcher.shutdown().await.unwrap();
        assert!(Arc::ptr_eq(
            &trace_producer,
            &dispatcher.trace_producer().unwrap()
        ));
    }

    #[test]
    fn lite_internal_trace_producer_uses_host_namespace_once_and_keeps_namespace_v2() {
        let mut client = ClientConfig::new("lite-trace-namespace");
        client.set_namespace(Some("tenant".into()));
        client.set_namespace_v2(Some("tenant-v2".into()));
        let dispatcher =
            LiteTraceDispatcher::new(&client, "workers", Some("custom-trace"), None).unwrap();

        assert!(
            dispatcher
                .producer_config
                .producer_group()
                .expect("the lite trace producer group is always configured")
                .starts_with("_INNER_TRACE_PRODUCER-tenant%workers-CONSUME-")
        );
        let mut client_config = dispatcher.producer_config.client_config().clone();
        assert_eq!(client_config.get_namespace().unwrap(), None);
        assert_eq!(
            dispatcher.producer_config.client_config().namespace_v2(),
            Some("tenant-v2")
        );
        assert_eq!(dispatcher.trace_topic_name(), "custom-trace");
    }

    #[test]
    fn lite_internal_trace_dispatcher_retains_host_rpc_hook() {
        let client = ClientConfig::new("lite-trace-rpc-hook");
        let rpc_hook: Arc<dyn RpcHook> = Arc::new(NoopRpcHook);
        let dispatcher =
            LiteTraceDispatcher::new(&client, "workers", None, Some(rpc_hook.clone())).unwrap();

        assert!(Arc::ptr_eq(
            dispatcher.rpc_hook.as_ref().unwrap(),
            &rpc_hook
        ));
    }
}
