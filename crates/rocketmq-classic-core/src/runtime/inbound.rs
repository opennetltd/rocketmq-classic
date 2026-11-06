use super::{ClientRuntime, ConsumerOperation, RuntimeState};
use rocketmq_classic_common::message::{
    MessageExt, MessageHostAddress,
    constants::{PROPERTY_CORRELATION_ID, PROPERTY_PRODUCER_GROUP},
    decode_properties,
};
use rocketmq_classic_protocol::{
    body::GetConsumerStatusBody,
    codes::{request, response},
    header::{CommandHeader, ReplyMessageRequestHeader},
};
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{RequestExecutor, RequestProcessor},
};
use std::{
    sync::{
        Arc, Mutex, OnceLock, Weak,
        mpsc::{SyncSender, sync_channel},
    },
    thread::{self, JoinHandle},
};

pub(super) const CODES: [i32; 8] = [
    request::CHECK_TRANSACTION_STATE,
    request::NOTIFY_CONSUMER_IDS_CHANGED,
    request::RESET_CONSUMER_CLIENT_OFFSET,
    request::GET_CONSUMER_STATUS_FROM_CLIENT,
    request::GET_CONSUMER_RUNNING_INFO,
    request::CONSUME_MESSAGE_DIRECTLY,
    request::PUSH_REPLY_MESSAGE_TO_CLIENT,
    request::NOTIFY_UNSUBSCRIBE_LITE,
];

#[derive(Default)]
pub(super) struct RuntimeProcessor {
    runtime: OnceLock<Weak<ClientRuntime>>,
    request_executor: OnceLock<Arc<InboundExecutor>>,
}

const INBOUND_EXECUTOR_WORKERS: usize = 4;
const INBOUND_EXECUTOR_QUEUE_CAPACITY: usize = 1_024;
type InboundTask = Box<dyn FnOnce() + Send + 'static>;

struct InboundExecutor {
    sender: Mutex<Option<SyncSender<InboundTask>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl InboundExecutor {
    fn new() -> Result<Self, super::RuntimeError> {
        let (sender, receiver) = sync_channel::<InboundTask>(INBOUND_EXECUTOR_QUEUE_CAPACITY);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(INBOUND_EXECUTOR_WORKERS);
        for index in 0..INBOUND_EXECUTOR_WORKERS {
            let receiver = receiver.clone();
            let worker = thread::Builder::new()
                .name(format!("rocketmq-client-inbound-{index}"))
                .spawn(move || {
                    loop {
                        let task = receiver
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .recv();
                        match task {
                            Ok(task) => task(),
                            Err(_) => return,
                        }
                    }
                })
                .map_err(|error| super::RuntimeError::InboundRequestExecutor(error.to_string()))?;
            workers.push(worker);
        }
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            workers: Mutex::new(workers),
        })
    }

    async fn shutdown(&self) -> Result<(), super::RuntimeError> {
        self.sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let workers = std::mem::take(
            &mut *self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        if workers.is_empty() {
            return Ok(());
        }
        let panicked_workers = tokio::task::spawn_blocking(move || {
            workers
                .into_iter()
                .map(|worker| worker.join().is_err())
                .filter(|panicked| *panicked)
                .count()
        })
        .await
        .map_err(|error| {
            super::RuntimeError::InboundRequestExecutor(format!(
                "failed to join inbound request workers: {error}"
            ))
        })?;
        if panicked_workers > 0 {
            return Err(super::RuntimeError::InboundRequestExecutor(format!(
                "{panicked_workers} inbound request workers panicked during shutdown"
            )));
        }
        Ok(())
    }
}

impl RequestExecutor for InboundExecutor {
    fn execute(&self, task: Box<dyn FnOnce() + Send + 'static>) -> Result<(), String> {
        let sender = self
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(sender) = sender.as_ref() else {
            return Err("inbound request executor is shut down".into());
        };
        sender.try_send(task).map_err(|error| match error {
            std::sync::mpsc::TrySendError::Full(_) => "inbound request executor is busy".into(),
            std::sync::mpsc::TrySendError::Disconnected(_) => {
                "inbound request executor is shut down".into()
            }
        })
    }
}

impl Drop for InboundExecutor {
    fn drop(&mut self) {
        self.sender
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        self.workers
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }
}

impl RuntimeProcessor {
    pub fn bind(&self, runtime: &Arc<ClientRuntime>) {
        let _ = self.runtime.set(Arc::downgrade(runtime));
    }

    pub fn request_executor(&self) -> Result<Arc<dyn RequestExecutor>, super::RuntimeError> {
        if self.request_executor.get().is_none() {
            let executor = Arc::new(InboundExecutor::new()?);
            let _ = self.request_executor.set(executor);
        }
        self.request_executor
            .get()
            .cloned()
            .map(|executor| executor as Arc<dyn RequestExecutor>)
            .ok_or(super::RuntimeError::ControlTaskFailed)
    }

    pub async fn shutdown_executor(&self) -> Result<(), super::RuntimeError> {
        if let Some(executor) = self.request_executor.get() {
            executor.shutdown().await
        } else {
            Ok(())
        }
    }

    fn dispatch(
        &self,
        remote: &str,
        command: RemotingCommand,
    ) -> Result<Option<RemotingCommand>, String> {
        let Some(runtime) = self.runtime.get().and_then(Weak::upgrade) else {
            return Ok(Some(error(
                response::SYSTEM_ERROR,
                "client runtime is unavailable",
            )));
        };
        if command.code == request::NOTIFY_CONSUMER_IDS_CHANGED {
            // Java decodes this header for logging only, then wakes the
            // runtime-wide rebalance service and returns null. The group does
            // not constrain which consumers are rebalanced. Java does this
            // before the service starts as well; the wakeup remains sticky.
            runtime.rebalance_immediately();
            return Ok(None);
        }
        if runtime.state() != RuntimeState::Running {
            return Ok(Some(error(
                response::SYSTEM_ERROR,
                "client runtime is not running",
            )));
        }
        if command.code == request::PUSH_REPLY_MESSAGE_TO_CLIENT {
            return Ok(Some(receive_reply_message(&runtime, &command)));
        }
        let groups = runtime
            .groups
            .snapshot()
            .map_err(|error| error.to_string())?;
        let fields = command.ext_fields.as_ref();
        let field = |key: &str| {
            fields
                .and_then(|fields| fields.get(key))
                .map(String::as_str)
        };
        let selected = match command.code {
            request::CHECK_TRANSACTION_STATE => {
                let producers = groups
                    .producers
                    .into_iter()
                    .filter_map(|(group, producer)| {
                        producer
                            .transaction_processor()
                            .map(|processor| (group, processor))
                    })
                    .collect::<Vec<_>>();
                let properties =
                    match stored_properties(command.body.as_deref().unwrap_or_default()) {
                        Ok(properties) => properties,
                        Err(_) => return Ok(None),
                    };
                producers
                    .into_iter()
                    .find(|(group, _)| properties.get(PROPERTY_PRODUCER_GROUP) == Some(group))
                    .map(|(_, processor)| processor)
            }
            request::NOTIFY_UNSUBSCRIBE_LITE => {
                // MQClientAPIImpl registers this code, but Java's
                // ClientRemotingProcessor switch has no branch and returns null.
                return Ok(None);
            }
            code => {
                let (operation, key, phase) = match code {
                    request::RESET_CONSUMER_CLIENT_OFFSET => {
                        (ConsumerOperation::ResetOffset, "group", "Phase 17")
                    }
                    request::GET_CONSUMER_STATUS_FROM_CLIENT => {
                        (ConsumerOperation::ConsumerStatus, "group", "Phase 17")
                    }
                    request::GET_CONSUMER_RUNNING_INFO => {
                        (ConsumerOperation::RunningInfo, "consumerGroup", "Phase 17")
                    }
                    request::CONSUME_MESSAGE_DIRECTLY => (
                        ConsumerOperation::ConsumeDirectly,
                        "consumerGroup",
                        "Phase 17",
                    ),
                    _ => return Ok(Some(unsupported(code, "no Java client owner"))),
                };
                let Some(group) = field(key) else {
                    return Ok(Some(unsupported(code, phase)));
                };
                let Some(consumer) = runtime
                    .select_consumer(group)
                    .map_err(|error| error.to_string())?
                else {
                    return missing_consumer_response(operation, group);
                };
                match consumer.inbound_processor(operation) {
                    Some(processor) => Some(processor),
                    None => return missing_consumer_response(operation, group),
                }
            }
        };
        match selected {
            Some(processor) => processor.process_with_context(remote, command),
            None if command.code == request::CHECK_TRANSACTION_STATE => Ok(None),
            None => Ok(Some(unsupported(
                command.code,
                match command.code {
                    request::CHECK_TRANSACTION_STATE => "Phase 14",
                    _ => "Phase 17",
                },
            ))),
        }
    }
}

fn receive_reply_message(runtime: &ClientRuntime, command: &RemotingCommand) -> RemotingCommand {
    let result = (|| {
        let fields = command
            .ext_fields
            .as_ref()
            .ok_or_else(|| "reply request has no ext fields".to_owned())?;
        let header =
            ReplyMessageRequestHeader::decode_fields(fields).map_err(|error| error.to_string())?;
        let born_host = MessageHostAddress::parse_java_socket_address(header.get_born_host())
            .map_err(|error| error.to_string())?;
        let store_host = MessageHostAddress::parse_java_socket_address(header.get_store_host())
            .map_err(|error| error.to_string())?;
        let mut body = command.body.clone().unwrap_or_default();
        if header.get_sys_flag() & rocketmq_classic_common::sysflag::message::COMPRESSED_FLAG != 0 {
            let compression_type =
                rocketmq_classic_common::sysflag::message::compression_type(header.get_sys_flag())
                    .map_err(|error| error.to_string())?;
            if let Ok(uncompressed) =
                rocketmq_classic_common::compression::decompress(compression_type, &body)
            {
                body = uncompressed;
            }
        }

        let properties = decode_properties(header.get_properties().unwrap_or_default());
        let correlation_id = properties.get(PROPERTY_CORRELATION_ID).cloned();
        let mut reply = MessageExt::new();
        reply.set_topic(header.get_topic());
        reply.set_body(body);
        reply.set_flag(header.get_flag());
        for (key, value) in properties {
            let _ = reply.set_system_property(key, value);
        }
        let _ = reply.set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_REPLY_MESSAGE_ARRIVE_TIME,
            epoch_millis_now().to_string(),
        );

        reply.set_queue_id(header.get_queue_id());
        reply.set_sys_flag(header.get_sys_flag());
        reply.set_born_timestamp(header.get_born_timestamp());
        reply.set_born_host(Some(born_host));
        reply.set_store_timestamp(header.get_store_timestamp());
        reply.set_store_host(Some(store_host));
        reply.set_reconsume_times(header.get_reconsume_times().unwrap_or_default());
        Ok::<_, String>((reply, correlation_id))
    })();

    match result {
        Ok((reply, Some(correlation_id))) => {
            match runtime.complete_reply_request(&correlation_id, reply) {
                Ok(_) => RemotingCommand {
                    code: response::SUCCESS,
                    ..crate::api::java_client_response_command(response::SUCCESS, None)
                },
                Err(_) => error(response::SYSTEM_ERROR, "process reply message fail"),
            }
        }
        Ok((_, None)) => RemotingCommand {
            code: response::SUCCESS,
            ..crate::api::java_client_response_command(response::SUCCESS, None)
        },
        Err(_) => error(response::SYSTEM_ERROR, "process reply message fail"),
    }
}

fn epoch_millis_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

impl RequestProcessor for RuntimeProcessor {
    fn process(&self, command: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
        self.dispatch("", command)
    }
    fn process_with_context(
        &self,
        remote: &str,
        command: RemotingCommand,
    ) -> Result<Option<RemotingCommand>, String> {
        self.dispatch(remote, command)
    }
}

fn error(code: i32, remark: &str) -> RemotingCommand {
    crate::api::java_client_response_command(code, Some(remark.into()))
}
fn unsupported(code: i32, phase: &str) -> RemotingCommand {
    error(
        response::REQUEST_CODE_NOT_SUPPORTED,
        &format!("client request {code} has no active handler ({phase})"),
    )
}

fn missing_consumer_response(
    operation: ConsumerOperation,
    group: &str,
) -> Result<Option<RemotingCommand>, String> {
    match operation {
        ConsumerOperation::ResetOffset => Ok(None),
        ConsumerOperation::ConsumerStatus => {
            let body = GetConsumerStatusBody::default()
                .encode_java_json()
                .map_err(|error| error.to_string())?;
            Ok(Some(RemotingCommand {
                code: response::SUCCESS,
                body: Some(body),
                ..crate::api::java_client_response_command(response::SUCCESS, None)
            }))
        }
        ConsumerOperation::RunningInfo | ConsumerOperation::ConsumeDirectly => Ok(Some(error(
            response::SYSTEM_ERROR,
            &format!("The Consumer Group <{group}> not exist in this consumer"),
        ))),
    }
}

/// Routing-only metadata extraction from MessageDecoder's V1/V2 layout. Full
/// message decoding/decompression belongs to Phase 8/14; no payload is copied.
fn stored_properties(
    body: &[u8],
) -> Result<rocketmq_classic_common::message::MessageProperties, String> {
    fn bytes(body: &[u8], offset: usize, length: usize) -> Result<&[u8], String> {
        offset
            .checked_add(length)
            .and_then(|end| body.get(offset..end))
            .ok_or_else(|| "malformed stored message metadata".into())
    }
    fn int(body: &[u8], offset: usize) -> Result<i32, String> {
        Ok(i32::from_be_bytes(
            bytes(body, offset, 4)?.try_into().unwrap(),
        ))
    }
    let magic = int(body, 4)?;
    let flag = int(body, 36)?;
    let body_position =
        84 + if flag & (1 << 4) != 0 { 12 } else { 0 } + if flag & (1 << 5) != 0 { 12 } else { 0 };
    let length =
        usize::try_from(int(body, body_position)?).map_err(|_| "negative stored body length")?;
    let topic_position = body_position
        .checked_add(4)
        .and_then(|offset| offset.checked_add(length))
        .ok_or("stored body length overflow")?;
    let (topic_length, width) = match magic {
        -626843481 => (
            usize::try_from(bytes(body, topic_position, 1)?[0] as i8)
                .map_err(|_| "negative V1 topic length")?,
            1,
        ),
        -626843477 => (
            usize::try_from(i16::from_be_bytes(
                bytes(body, topic_position, 2)?.try_into().unwrap(),
            ))
            .map_err(|_| "negative V2 topic length")?,
            2,
        ),
        _ => return Err("invalid stored message magic code".into()),
    };
    let properties_position = topic_position
        .checked_add(width)
        .and_then(|offset| offset.checked_add(topic_length))
        .ok_or("stored topic length overflow")?;
    let length = i16::from_be_bytes(bytes(body, properties_position, 2)?.try_into().unwrap()).max(0)
        as usize;
    Ok(decode_properties(&String::from_utf8_lossy(bytes(
        body,
        properties_position + 2,
        length,
    )?)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::ClientConfig,
        runtime::{
            ClientRuntimeManager, ConsumerRuntime, GroupLease, PendingReplyCompletion,
            ProducerRuntime, RequestFutureTable, RequestResponseFuture,
        },
    };
    use base64::Engine;
    use rocketmq_classic_common::message::Message;
    use rocketmq_classic_protocol::heartbeat::ConsumerData;
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    fn fixtures() -> serde_json::Value {
        serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/inbound-routing.json"
        ))
        .unwrap()
    }
    fn body(case: &serde_json::Value) -> Vec<u8> {
        base64::engine::general_purpose::STANDARD
            .decode(case["body"].as_str().unwrap())
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn inbound_executor_shutdown_joins_running_work_and_rejects_new_work() {
        let executor = Arc::new(InboundExecutor::new().unwrap());
        let (started_sender, started_receiver) = std::sync::mpsc::sync_channel(1);
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        let (completed_sender, completed_receiver) = std::sync::mpsc::sync_channel(1);
        executor
            .execute(Box::new(move || {
                started_sender.send(()).unwrap();
                release_receiver.recv().unwrap();
                completed_sender.send(()).unwrap();
            }))
            .unwrap();
        started_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        let shutdown_executor = executor.clone();
        let mut shutdown = tokio::spawn(async move { shutdown_executor.shutdown().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let is_open = executor
                    .sender
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_some();
                if !is_open {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown closes the executor before joining its workers");
        assert!(executor.execute(Box::new(|| {})).is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut shutdown)
                .await
                .is_err()
        );

        release_sender.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .expect("shutdown waits for accepted work")
            .unwrap()
            .unwrap();
        completed_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert!(
            executor
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        );
    }

    #[test]
    fn routing_properties_match_java_v1_v2_and_all_ipv4_ipv6_layouts() {
        for case in fixtures()["cases"].as_array().unwrap() {
            let body = body(case);
            assert_eq!(
                serde_json::to_value(stored_properties(&body).unwrap()).unwrap(),
                case["properties"],
                "{}",
                case["case"]
            );
            for length in 0..body.len() {
                assert!(
                    stored_properties(&body[..length]).is_err(),
                    "{} prefix {length}",
                    case["case"]
                );
            }
        }
    }

    #[test]
    fn hostile_metadata_lengths_and_magic_are_rejected_without_payload_allocation() {
        let mut body = body(&fixtures()["cases"][0]);
        body[84..88].copy_from_slice(&i32::MAX.to_be_bytes());
        assert!(stored_properties(&body).is_err());
        body[84..88].copy_from_slice(&(-1i32).to_be_bytes());
        assert!(stored_properties(&body).is_err());
        body[4..8].copy_from_slice(&0i32.to_be_bytes());
        assert!(stored_properties(&body).is_err());
    }

    struct Handler {
        owner: String,
        runtime: Weak<ClientRuntime>,
        calls: Mutex<Vec<(String, i32)>>,
    }
    impl RequestProcessor for Handler {
        fn process(&self, _: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
            panic!("channel context must be passed");
        }
        fn process_with_context(
            &self,
            remote: &str,
            command: RemotingCommand,
        ) -> Result<Option<RemotingCommand>, String> {
            let runtime = self.runtime.upgrade().unwrap();
            runtime.route_snapshot().unwrap(); // Callback may reenter cache and group registry.
            if self.owner.starts_with("producer") {
                assert!(runtime.select_producer(&self.owner).unwrap().is_some());
            } else {
                assert!(runtime.select_consumer(&self.owner).unwrap().is_some());
            }
            self.calls
                .lock()
                .unwrap()
                .push((remote.to_owned(), command.code));
            Ok(Some(RemotingCommand {
                code: 0,
                remark: Some(self.owner.clone()),
                ..Default::default()
            }))
        }
    }
    struct Producer {
        handler: Arc<Handler>,
    }
    impl ProducerRuntime for Producer {
        fn transaction_processor(&self) -> Option<Arc<dyn RequestProcessor>> {
            Some(self.handler.clone())
        }
    }
    struct NoInboundProducer;
    impl ProducerRuntime for NoInboundProducer {}
    struct Consumer {
        handler: Arc<Handler>,
    }
    impl ConsumerRuntime for Consumer {
        fn heartbeat_data(&self) -> ConsumerData {
            ConsumerData {
                group_name: Some(self.handler.owner.clone()),
                ..Default::default()
            }
        }
        fn inbound_processor(&self, _: ConsumerOperation) -> Option<Arc<dyn RequestProcessor>> {
            Some(self.handler.clone())
        }
    }
    struct NoInboundConsumer;
    impl ConsumerRuntime for NoInboundConsumer {
        fn heartbeat_data(&self) -> ConsumerData {
            ConsumerData {
                group_name: Some("consumer-no-inbound".into()),
                ..Default::default()
            }
        }
    }
    struct RebalancingConsumer(AtomicUsize);
    impl ConsumerRuntime for RebalancingConsumer {
        fn heartbeat_data(&self) -> ConsumerData {
            ConsumerData::default()
        }
        fn try_rebalance(&self) -> Option<Pin<Box<dyn Future<Output = bool> + Send + '_>>> {
            Some(Box::pin(async {
                self.0.fetch_add(1, Ordering::SeqCst);
                true
            }))
        }
    }
    fn handler(runtime: &Arc<ClientRuntime>, owner: &str) -> Arc<Handler> {
        Arc::new(Handler {
            owner: owner.into(),
            runtime: Arc::downgrade(runtime),
            calls: Mutex::default(),
        })
    }
    async fn runtime(id: &str) -> Arc<ClientRuntime> {
        let mut config = ClientConfig::new(id);
        config.namesrv_addr = Some(String::new());
        let runtime = ClientRuntimeManager::new()
            .get_or_create(&config)
            .await
            .unwrap();
        runtime.start().await.unwrap();
        runtime
    }
    fn notify_request() -> RemotingCommand {
        RemotingCommand {
            code: request::NOTIFY_CONSUMER_IDS_CHANGED,
            ..Default::default()
        }
    }
    fn inactive_request() -> RemotingCommand {
        RemotingCommand {
            code: request::RESET_CONSUMER_CLIENT_OFFSET,
            ..Default::default()
        }
    }
    fn dispatch(
        runtime: &Arc<ClientRuntime>,
        code: i32,
        fields: &[(&str, &str)],
        body: Option<Vec<u8>>,
    ) -> RemotingCommand {
        dispatch_maybe(runtime, code, fields, body).unwrap()
    }
    fn dispatch_maybe(
        runtime: &Arc<ClientRuntime>,
        code: i32,
        fields: &[(&str, &str)],
        body: Option<Vec<u8>>,
    ) -> Option<RemotingCommand> {
        runtime
            .inbound
            .process_with_context(
                "broker:10911",
                RemotingCommand {
                    code,
                    ext_fields: Some(
                        fields
                            .iter()
                            .map(|(key, value)| (key.to_string(), value.to_string()))
                            .collect(),
                    ),
                    body,
                    ..Default::default()
                },
            )
            .unwrap()
    }
    fn assert_missing_consumer_response(
        runtime: &Arc<ClientRuntime>,
        code: i32,
        header: &str,
        group: &str,
    ) {
        let result = dispatch_maybe(runtime, code, &[(header, group)], None);
        match code {
            request::RESET_CONSUMER_CLIENT_OFFSET => assert!(result.is_none()),
            request::GET_CONSUMER_STATUS_FROM_CLIENT => {
                let result = result.unwrap();
                assert_eq!(result.code, response::SUCCESS);
                let body = result.body.unwrap();
                let body =
                    GetConsumerStatusBody::decode_java_json(std::str::from_utf8(&body).unwrap())
                        .unwrap();
                body.get_message_queue_table()
                    .unwrap()
                    .with(|entries| assert!(entries.is_empty()));
            }
            request::GET_CONSUMER_RUNNING_INFO | request::CONSUME_MESSAGE_DIRECTLY => {
                let result = result.unwrap();
                assert_eq!(result.code, response::SYSTEM_ERROR);
                assert_eq!(
                    result.remark.as_deref(),
                    Some(
                        format!("The Consumer Group <{group}> not exist in this consumer").as_str()
                    )
                );
            }
            _ => unreachable!(),
        }
    }
    fn dispatch_optional(
        runtime: &Arc<ClientRuntime>,
        fields: &[(&str, &str)],
    ) -> Option<RemotingCommand> {
        runtime
            .inbound
            .process_with_context(
                "broker:10911",
                RemotingCommand {
                    code: request::NOTIFY_CONSUMER_IDS_CHANGED,
                    ext_fields: Some(
                        fields
                            .iter()
                            .map(|(key, value)| (key.to_string(), value.to_string()))
                            .collect(),
                    ),
                    ..Default::default()
                },
            )
            .unwrap()
    }
    async fn settle() {
        for _ in 0..30 {
            tokio::task::yield_now().await;
        }
    }
    async fn unregister(runtime: &Arc<ClientRuntime>, lease: &GroupLease, producer: bool) {
        if producer {
            runtime
                .unregister_producer(lease, tokio::time::Instant::now())
                .await
                .unwrap();
        } else {
            runtime
                .unregister_consumer(lease, tokio::time::Instant::now())
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn unbound_and_non_running_feature_slots_return_system_error_but_code_40_is_silent() {
        let unbound = RuntimeProcessor::default()
            .process(inactive_request())
            .unwrap()
            .unwrap();
        assert_eq!(unbound.code, response::SYSTEM_ERROR);
        assert_eq!(
            unbound.remark.as_deref(),
            Some("client runtime is unavailable")
        );

        let mut config = ClientConfig::new("inbound-not-running");
        config.namesrv_addr = Some(String::new());
        let runtime = ClientRuntimeManager::new()
            .get_or_create(&config)
            .await
            .unwrap();
        assert!(runtime.inbound.process(notify_request()).unwrap().is_none());
        let created = runtime
            .inbound
            .process(inactive_request())
            .unwrap()
            .unwrap();
        assert_eq!(created.code, response::SYSTEM_ERROR);
        assert_eq!(
            created.remark.as_deref(),
            Some("client runtime is not running")
        );

        runtime.start().await.unwrap();
        assert!(runtime.shutdown().await.unwrap());
        assert!(runtime.inbound.process(notify_request()).unwrap().is_none());
        let stopped = runtime
            .inbound
            .process(inactive_request())
            .unwrap()
            .unwrap();
        assert_eq!(stopped.code, response::SYSTEM_ERROR);
        assert_eq!(
            stopped.remark.as_deref(),
            Some("client runtime is not running")
        );

        let mut failed_config = ClientConfig::new("inbound-failed");
        failed_config.namesrv_addr = Some(String::new());
        failed_config.poll_name_server_interval = 0;
        let failed = ClientRuntimeManager::new()
            .get_or_create(&failed_config)
            .await
            .unwrap();
        assert!(failed.start().await.is_err());
        assert_eq!(failed.state(), RuntimeState::Failed);
        assert!(failed.inbound.process(notify_request()).unwrap().is_none());
        let failure = failed.inbound.process(inactive_request()).unwrap().unwrap();
        assert_eq!(failure.code, response::SYSTEM_ERROR);
        assert_eq!(
            failure.remark.as_deref(),
            Some("client runtime is not running")
        );
    }

    #[tokio::test]
    async fn replies_complete_by_correlation_and_ack_unknown_or_duplicate_ids() {
        let runtime = runtime("transaction-slots").await;
        let first = Arc::new(Producer {
            handler: handler(&runtime, "producer-a"),
        });
        let second = Arc::new(Producer {
            handler: handler(&runtime, "producer-b"),
        });
        let first_lease = runtime
            .register_producer("producer-a", first.clone())
            .await
            .unwrap();
        let second_lease = runtime
            .register_producer("producer-b", second.clone())
            .await
            .unwrap();
        for case in fixtures()["cases"].as_array().unwrap() {
            let result = dispatch(
                &runtime,
                request::CHECK_TRANSACTION_STATE,
                &[("producerGroup", "forged-header-group")],
                Some(body(case)),
            );
            assert_eq!(result.remark.as_deref(), case["group"].as_str());
        }

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut first_pending = runtime
            .register_reply_request(&first_lease, "inbound-correlation-first", deadline)
            .await
            .unwrap();
        let mut second_pending = runtime
            .register_reply_request(&second_lease, "inbound-correlation-second", deadline)
            .await
            .unwrap();
        let late_send_completion = second_pending.control();
        unregister(&runtime, &first_lease, true).await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(1), first_pending.receive())
                .await
                .is_err(),
            "producer unregister must leave the pending correlation available"
        );

        let late_reply = dispatch_reply(&runtime, "inbound-correlation-first", b"late reply");
        assert_eq!(late_reply.code, response::SUCCESS);
        let PendingReplyCompletion::Reply(first_reply) = first_pending.receive().await.unwrap()
        else {
            panic!("an outstanding correlation must accept a reply after unregister")
        };
        assert_eq!(first_reply.body().as_ref(), b"late reply");

        let reply = dispatch_reply(&runtime, "inbound-correlation-second", b"reply-body");
        assert_eq!(reply.code, response::SUCCESS);
        assert!(reply.remark.is_none());
        // A Broker reply can beat the async send completion callback. The
        // later send-success signal must not reclaim or recreate the entry.
        late_send_completion.mark_send_succeeded();
        let PendingReplyCompletion::Reply(reply_message) = second_pending.receive().await.unwrap()
        else {
            panic!("matching correlation must complete with the decoded reply")
        };
        assert_eq!(reply_message.topic(), "cluster-a_REPLY_TOPIC");
        assert_eq!(reply_message.body().as_ref(), b"reply-body");
        assert_eq!(
            reply_message.properties()[PROPERTY_CORRELATION_ID],
            "inbound-correlation-second"
        );
        assert!(
            reply_message.properties()["ARRIVE_TIME"]
                .parse::<i64>()
                .is_ok()
        );

        assert_eq!(
            dispatch_reply(&runtime, "inbound-correlation-second", b"duplicate").code,
            response::SUCCESS
        );
        assert_eq!(
            dispatch_reply(&runtime, "unknown", b"late").code,
            response::SUCCESS
        );
        let late_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut late_pending = runtime
            .register_reply_request(&second_lease, "inbound-correlation-expired", late_deadline)
            .await
            .unwrap();
        assert_eq!(
            runtime
                .groups
                .expire_pending_replies(late_deadline + Duration::from_millis(1)),
            1,
            "the injected clock expires the request after its deadline"
        );
        assert!(matches!(
            late_pending.receive().await.unwrap(),
            PendingReplyCompletion::Timeout {
                send_succeeded: false
            }
        ));
        assert_eq!(
            dispatch_reply(&runtime, "inbound-correlation-expired", b"late reply").code,
            response::SUCCESS,
            "late replies are acknowledged without resurrecting their correlation"
        );
        unregister(&runtime, &second_lease, true).await;
        assert!(runtime.shutdown().await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_reply_and_timeout_claim_exactly_one_terminal_owner() {
        let runtime = runtime("reply-timeout-race").await;
        let producer = Arc::new(Producer {
            handler: handler(&runtime, "race-producer"),
        });
        let lease = runtime
            .register_producer("race-producer", producer)
            .await
            .unwrap();

        for iteration in 0..100 {
            let correlation_id = format!("inbound-reply-timeout-race-{iteration}");
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            let mut pending = runtime
                .register_reply_request(&lease, &correlation_id, deadline)
                .await
                .unwrap();
            let gate = Arc::new(tokio::sync::Barrier::new(3));

            let reply_runtime = runtime.clone();
            let reply_gate = gate.clone();
            let reply_correlation = correlation_id.clone();
            let reply = tokio::spawn(async move {
                reply_gate.wait().await;
                let message = crate::test_support::message_ext_from(Message::new(
                    "orders_REPLY_TOPIC",
                    b"reply".to_vec(),
                ));
                reply_runtime
                    .complete_reply_request(&reply_correlation, message)
                    .unwrap()
            });

            let timeout_runtime = runtime.clone();
            let timeout_gate = gate.clone();
            let timeout = tokio::spawn(async move {
                timeout_gate.wait().await;
                timeout_runtime.groups.expire_pending_replies(deadline)
            });

            gate.wait().await;
            let reply_won = reply.await.unwrap();
            let expired = timeout.await.unwrap();
            assert_eq!(usize::from(reply_won) + expired, 1);
            match pending.receive().await.unwrap() {
                PendingReplyCompletion::Reply(reply) => {
                    assert!(reply_won);
                    assert_eq!(reply.body().as_ref(), b"reply");
                }
                PendingReplyCompletion::Timeout { .. } => assert_eq!(expired, 1),
                other => panic!("unexpected terminal request/reply result: {other:?}"),
            }
            assert!(
                !runtime
                    .complete_reply_request(
                        &correlation_id,
                        crate::test_support::message_ext_from(Message::new(
                            "orders_REPLY_TOPIC",
                            b"duplicate".to_vec(),
                        ))
                    )
                    .unwrap()
            );
            assert_eq!(runtime.groups.expire_pending_replies(deadline), 0);
        }

        unregister(&runtime, &lease, true).await;
        assert!(runtime.shutdown().await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reply_races_with_send_failure_or_cancellation_without_double_claim() {
        let runtime = runtime("reply-send-cancel-race").await;
        let producer = Arc::new(Producer {
            handler: handler(&runtime, "race-producer"),
        });
        let lease = runtime
            .register_producer("race-producer", producer)
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

        let mut send_race = runtime
            .register_reply_request(&lease, "inbound-send-race", deadline)
            .await
            .unwrap();
        let send_control = send_race.control();
        let gate = Arc::new(tokio::sync::Barrier::new(3));
        let reply_runtime = runtime.clone();
        let reply_gate = gate.clone();
        let reply_task = tokio::spawn(async move {
            reply_gate.wait().await;
            reply_runtime.complete_reply_request(
                "inbound-send-race",
                crate::test_support::message_ext_from(Message::new(
                    "orders_REPLY_TOPIC",
                    b"reply".to_vec(),
                )),
            )
        });
        let failure_gate = gate.clone();
        let failure_task = tokio::spawn(async move {
            failure_gate.wait().await;
            send_control.fail_send("scripted send failure".into())
        });
        gate.wait().await;
        let reply_won = reply_task.await.unwrap().unwrap();
        let failure_won = failure_task.await.unwrap();
        assert_eq!(usize::from(reply_won) + usize::from(failure_won), 1);
        match send_race.receive().await.unwrap() {
            PendingReplyCompletion::Reply(_) => assert!(reply_won),
            PendingReplyCompletion::SendFailed(cause) => {
                assert!(failure_won);
                assert_eq!(cause, "scripted send failure");
            }
            other => panic!("unexpected terminal request/reply result: {other:?}"),
        }

        let mut cancel_race = runtime
            .register_reply_request(&lease, "inbound-cancel-race", deadline)
            .await
            .unwrap();
        let cancel_control = cancel_race.control();
        let gate = Arc::new(tokio::sync::Barrier::new(3));
        let reply_runtime = runtime.clone();
        let reply_gate = gate.clone();
        let reply_task = tokio::spawn(async move {
            reply_gate.wait().await;
            reply_runtime.complete_reply_request(
                "inbound-cancel-race",
                crate::test_support::message_ext_from(Message::new(
                    "orders_REPLY_TOPIC",
                    b"reply".to_vec(),
                )),
            )
        });
        let cancel_gate = gate.clone();
        let cancel_task = tokio::spawn(async move {
            cancel_gate.wait().await;
            cancel_control.cancel()
        });
        gate.wait().await;
        let reply_won = reply_task.await.unwrap().unwrap();
        let cancelled = cancel_task.await.unwrap();
        assert_eq!(usize::from(reply_won) + usize::from(cancelled), 1);
        if reply_won {
            assert!(matches!(
                cancel_race.receive().await.unwrap(),
                PendingReplyCompletion::Reply(_)
            ));
        } else {
            assert!(cancel_race.receive().await.is_err());
        }

        unregister(&runtime, &lease, true).await;
        assert!(runtime.shutdown().await.unwrap());
    }

    fn dispatch_reply(
        runtime: &Arc<ClientRuntime>,
        correlation_id: &str,
        body: &[u8],
    ) -> RemotingCommand {
        let result = runtime
            .inbound
            .process_with_context("broker:10911", reply_command(correlation_id, body))
            .unwrap()
            .unwrap();
        assert_eq!(result.code, response::SUCCESS);
        result
    }

    fn reply_command(correlation_id: &str, body: &[u8]) -> RemotingCommand {
        use rocketmq_classic_common::message::{
            constants::{PROPERTY_CORRELATION_ID, PROPERTY_MESSAGE_TYPE},
            encode_properties,
        };
        let properties = std::collections::BTreeMap::from([
            (
                PROPERTY_CORRELATION_ID.to_owned(),
                correlation_id.to_owned(),
            ),
            (PROPERTY_MESSAGE_TYPE.to_owned(), "reply".to_owned()),
        ]);
        let header = {
            let mut __phase18_header_0 = ReplyMessageRequestHeader::default();
            __phase18_header_0.set_producer_group("response-producer".into());
            __phase18_header_0.set_topic("cluster-a_REPLY_TOPIC".into());
            __phase18_header_0.set_default_topic("TBW102".into());
            __phase18_header_0.set_default_topic_queue_nums(4);
            __phase18_header_0.set_queue_id(1);
            __phase18_header_0.set_born_timestamp(1_700_000_000_123);
            __phase18_header_0.set_flag(0);
            __phase18_header_0.set_properties(Some(encode_properties(&properties)));
            __phase18_header_0.set_born_host("127.0.0.1:10911".into());
            __phase18_header_0.set_store_host("127.0.0.1:10911".into());
            __phase18_header_0.set_store_timestamp(1_700_000_001_234);
            __phase18_header_0
        };
        let mut fields = rocketmq_classic_protocol::header::HeaderMap::new();
        header.encode_fields(&mut fields);
        RemotingCommand {
            code: request::PUSH_REPLY_MESSAGE_TO_CLIENT,
            ext_fields: Some(fields.into()),
            body: Some(body.to_vec()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn null_latch_reply_returns_system_error_and_keeps_pending_future() {
        let runtime = runtime("reply-null-latch").await;
        let correlation_id = "inbound-null-latch";
        let table = RequestFutureTable::shared();
        let _ = table.remove(correlation_id).unwrap();
        let future = Arc::new(RequestResponseFuture::new(correlation_id, 10_000, None));
        future.set_count_down_latch(None);
        table.insert(correlation_id, future.clone()).unwrap();

        let response = receive_reply_message(&runtime, &reply_command(correlation_id, b"reply"));
        assert_eq!(response.code, response::SYSTEM_ERROR);
        assert_eq!(
            response.remark.as_deref(),
            Some("process reply message fail")
        );
        assert!(table.contains_key(correlation_id).unwrap());
        assert_eq!(future.response_msg().unwrap().body().as_ref(), b"reply");

        table.remove(correlation_id).unwrap();
        assert!(runtime.shutdown().await.unwrap());
    }

    fn dispatch_reply_with_hosts(
        runtime: &Arc<ClientRuntime>,
        born_host: &str,
        store_host: &str,
    ) -> RemotingCommand {
        use rocketmq_classic_common::message::{
            constants::{PROPERTY_CORRELATION_ID, PROPERTY_MESSAGE_TYPE},
            encode_properties,
        };
        let properties = std::collections::BTreeMap::from([
            (
                PROPERTY_CORRELATION_ID.to_owned(),
                "unmatched-reply-fixture".to_owned(),
            ),
            (PROPERTY_MESSAGE_TYPE.to_owned(), "reply".to_owned()),
        ]);
        let header = {
            let mut __phase18_header_1 = ReplyMessageRequestHeader::default();
            __phase18_header_1.set_producer_group("response-producer".into());
            __phase18_header_1.set_topic("cluster-a_REPLY_TOPIC".into());
            __phase18_header_1.set_default_topic("TBW102".into());
            __phase18_header_1.set_default_topic_queue_nums(4);
            __phase18_header_1.set_queue_id(1);
            __phase18_header_1.set_born_timestamp(1_700_000_000_123);
            __phase18_header_1.set_flag(0);
            __phase18_header_1.set_properties(Some(encode_properties(&properties)));
            __phase18_header_1.set_born_host(born_host.to_owned());
            __phase18_header_1.set_store_host(store_host.to_owned());
            __phase18_header_1.set_store_timestamp(1_700_000_001_234);
            __phase18_header_1
        };
        let mut fields = rocketmq_classic_protocol::header::HeaderMap::new();
        header.encode_fields(&mut fields);
        runtime
            .inbound
            .process_with_context(
                "broker:10911",
                RemotingCommand {
                    code: request::PUSH_REPLY_MESSAGE_TO_CLIENT,
                    ext_fields: Some(fields.into()),
                    body: Some(Vec::new()),
                    ..Default::default()
                },
            )
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn consumer_slots_route_group_with_registry_reentry_and_do_not_cross_runtime_identity() {
        let runtime = runtime("consumer-slots").await;
        let other = self::runtime("other-client").await;
        let first = Arc::new(Consumer {
            handler: handler(&runtime, "consumer-a"),
        });
        let second = Arc::new(Consumer {
            handler: handler(&other, "consumer-a"),
        });
        let first_lease = runtime
            .register_consumer("consumer-a", first.clone())
            .await
            .unwrap();
        let second_lease = other
            .register_consumer("consumer-a", second.clone())
            .await
            .unwrap();
        for (code, key) in [
            (request::RESET_CONSUMER_CLIENT_OFFSET, "group"),
            (request::GET_CONSUMER_STATUS_FROM_CLIENT, "group"),
            (request::GET_CONSUMER_RUNNING_INFO, "consumerGroup"),
            (request::CONSUME_MESSAGE_DIRECTLY, "consumerGroup"),
        ] {
            assert_eq!(
                dispatch(&runtime, code, &[(key, "consumer-a")], None)
                    .remark
                    .as_deref(),
                Some("consumer-a")
            );
            assert_missing_consumer_response(&runtime, code, key, "missing");
        }
        assert_eq!(first.handler.calls.lock().unwrap().len(), 4);
        assert!(second.handler.calls.lock().unwrap().is_empty());
        unregister(&runtime, &first_lease, false).await;
        assert_missing_consumer_response(
            &runtime,
            request::CONSUME_MESSAGE_DIRECTLY,
            "consumerGroup",
            "consumer-a",
        );
        unregister(&other, &second_lease, false).await;
        assert!(runtime.shutdown().await.unwrap());
        assert!(other.shutdown().await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn consumer_ids_changed_wakes_runtime_wide_rebalance_and_returns_no_response() {
        let java_fixture = fixtures();
        let java_cases = java_fixture["consumerIdsChangedCases"]
            .as_array()
            .expect("invoked Java code 40 cases");
        assert_eq!(java_cases.len(), 2);
        for case in java_cases {
            assert_eq!(case["wakeupBefore"], false);
            assert_eq!(case["wakeupAfter"], true);
            assert_eq!(case["responsePresent"], false);
        }

        let mut config = ClientConfig::new("consumer-ids-changed-runtime-wide");
        config.namesrv_addr = Some(String::new());
        let runtime = ClientRuntimeManager::new()
            .get_or_create(&config)
            .await
            .unwrap();
        let first = Arc::new(RebalancingConsumer(AtomicUsize::new(0)));
        let second = Arc::new(RebalancingConsumer(AtomicUsize::new(0)));
        let first_lease = runtime
            .register_consumer("consumer-a", first.clone())
            .await
            .unwrap();
        let second_lease = runtime
            .register_consumer("consumer-b", second.clone())
            .await
            .unwrap();

        // Java leaves a sticky wakeup before the owned worker exists. Advance
        // the source minimum interval, then start that worker.
        let first_response = dispatch_optional(&runtime, &[("consumerGroup", "consumer-a")]);
        assert_eq!(first_response.is_some(), java_cases[0]["responsePresent"]);
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        settle().await;
        runtime.start().await.unwrap();
        settle().await;
        assert_eq!(first.0.load(Ordering::SeqCst), 1);
        assert_eq!(second.0.load(Ordering::SeqCst), 1);

        // The Java header's consumerGroup is optional and does not route the
        // wakeup to a single registered group.
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        settle().await;
        let second_response = dispatch_optional(&runtime, &[]);
        assert_eq!(second_response.is_some(), java_cases[1]["responsePresent"]);
        settle().await;
        assert_eq!(first.0.load(Ordering::SeqCst), 2);
        assert_eq!(second.0.load(Ordering::SeqCst), 2);

        unregister(&runtime, &first_lease, false).await;
        unregister(&runtime, &second_lease, false).await;
        assert!(runtime.shutdown().await.unwrap());
    }

    #[tokio::test]
    async fn absent_consumer_group_and_handler_capabilities_match_java_responses() {
        let runtime = runtime("consumer-inactive-slots").await;
        let consumer = Arc::new(NoInboundConsumer);
        let lease = runtime
            .register_consumer("consumer-no-inbound", consumer)
            .await
            .unwrap();
        for (code, key, phase) in [
            (request::RESET_CONSUMER_CLIENT_OFFSET, "group", "Phase 17"),
            (
                request::GET_CONSUMER_STATUS_FROM_CLIENT,
                "group",
                "Phase 17",
            ),
            (
                request::GET_CONSUMER_RUNNING_INFO,
                "consumerGroup",
                "Phase 17",
            ),
            (
                request::CONSUME_MESSAGE_DIRECTLY,
                "consumerGroup",
                "Phase 17",
            ),
        ] {
            let expected = format!("client request {code} has no active handler ({phase})");
            assert_missing_consumer_response(&runtime, code, key, "consumer-no-inbound");

            let no_group = dispatch(&runtime, code, &[], None);
            assert_eq!(no_group.code, response::REQUEST_CODE_NOT_SUPPORTED);
            assert_eq!(no_group.remark.as_deref(), Some(expected.as_str()));
        }
        unregister(&runtime, &lease, false).await;
        assert!(runtime.shutdown().await.unwrap());
    }

    #[tokio::test]
    async fn transaction_check_without_processor_is_silent_and_malformed_reply_fails() {
        let runtime = runtime("producer-inactive-slots").await;
        let producer = Arc::new(NoInboundProducer);
        let lease = runtime
            .register_producer("producer-no-inbound", producer)
            .await
            .unwrap();

        let transaction = runtime
            .inbound
            .process_with_context(
                "broker:10911",
                RemotingCommand {
                    code: request::CHECK_TRANSACTION_STATE,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(transaction.is_none());

        for properties in [None, Some("CORRELATION_ID\u{1}missing\u{2}")] {
            let reply = if let Some(value) = properties {
                dispatch(
                    &runtime,
                    request::PUSH_REPLY_MESSAGE_TO_CLIENT,
                    &[("properties", value)],
                    None,
                )
            } else {
                dispatch(&runtime, request::PUSH_REPLY_MESSAGE_TO_CLIENT, &[], None)
            };
            assert_eq!(reply.code, response::SYSTEM_ERROR);
            assert_eq!(reply.remark.as_deref(), Some("process reply message fail"));
        }

        unregister(&runtime, &lease, true).await;
        assert!(runtime.shutdown().await.unwrap());
    }

    #[tokio::test]
    async fn reply_host_addresses_match_invoked_java_validation() {
        let runtime = runtime("reply-host-addresses").await;
        for case in fixtures()["replyAddressCases"].as_array().unwrap() {
            let response = dispatch_reply_with_hosts(
                &runtime,
                case["bornHost"].as_str().unwrap(),
                case["storeHost"].as_str().unwrap(),
            );
            let expected = match case["outcome"].as_str().unwrap() {
                "SUCCESS" => response::SUCCESS,
                "SYSTEM_ERROR" => response::SYSTEM_ERROR,
                other => panic!("unexpected Java reply-address outcome: {other}"),
            };
            assert_eq!(response.code, expected, "case {}", case["case"]);
            if expected == response::SYSTEM_ERROR {
                assert_eq!(
                    response.remark.as_deref(),
                    Some("process reply message fail"),
                    "case {}",
                    case["case"]
                );
            }
        }
        assert!(runtime.shutdown().await.unwrap());
    }

    #[tokio::test]
    async fn registered_but_undispatched_lite_code_matches_java_null_response() {
        let case = fixtures()["undispatchedCases"][0].clone();
        let code = case["requestCode"].as_i64().unwrap() as i32;
        let runtime = runtime("undispatched-lite-request").await;
        let response = runtime
            .inbound
            .process(RemotingCommand {
                code,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            response.is_some(),
            case["responsePresent"].as_bool().unwrap(),
            "Java ClientRemotingProcessor.processRequest result for request code {code}"
        );
        if let (Some(response), Some(expected_code)) = (
            response,
            case.get("responseCode").and_then(serde_json::Value::as_i64),
        ) {
            assert_eq!(response.code, expected_code as i32);
        }
        assert!(runtime.shutdown().await.unwrap());
    }
}
