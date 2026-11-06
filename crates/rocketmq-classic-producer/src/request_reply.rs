use rocketmq_classic_common::message::{
    Message, MessageQueue,
    constants::{
        PROPERTY_CLUSTER, PROPERTY_CORRELATION_ID, PROPERTY_MESSAGE_REPLY_TO_CLIENT,
        PROPERTY_MESSAGE_TTL, PROPERTY_MESSAGE_TYPE, REPLY_MESSAGE_FLAG, REPLY_TOPIC_POSTFIX,
    },
};
use rocketmq_classic_common::utils::create_correlation_id;
use rocketmq_classic_core::runtime::{
    PendingReplyCompletion, PendingReplyControl, PendingReplyRequest, RequestFutureTable,
    RequestResponseCallback, RequestResponseFuture, RequestSendFailureCause, RequestTimeoutCause,
};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use thiserror::Error;
use tokio::time::Instant;

use crate::{Producer, ProducerError};

/// Java `RequestCallback` expressed as a single Rust result callback.
pub type RequestCallback = Box<dyn FnOnce(Result<Message, ProducerError>) + Send + 'static>;

struct ProducerRequestCallback {
    callback: Mutex<Option<RequestCallback>>,
    topic: String,
    timeout_millis: u128,
}

impl RequestResponseCallback for ProducerRequestCallback {
    fn on_success(&self, response: Option<Message>) {
        let callback = self
            .callback
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(callback) = callback {
            callback(response.ok_or(ProducerError::RequestReplyCancelled));
        }
    }

    fn on_exception(&self, cause: Option<Arc<dyn std::error::Error + Send + Sync>>) {
        let error = match cause {
            Some(cause) if cause.downcast_ref::<RequestTimeoutCause>().is_some() => {
                ProducerError::RequestReplyTimeout {
                    topic: self.topic.clone(),
                    timeout_millis: self.timeout_millis,
                }
            }
            Some(cause) => match cause.downcast_ref::<RequestSendFailureCause>() {
                Some(cause) => ProducerError::RequestSendFailed {
                    topic: self.topic.clone(),
                    cause: cause.message().to_owned(),
                },
                None => ProducerError::RequestSendFailed {
                    topic: self.topic.clone(),
                    cause: cause.to_string(),
                },
            },
            None => ProducerError::RequestReplyCancelled,
        };
        let callback = self
            .callback
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(callback) = callback {
            callback(Err(error));
        }
    }
}

/// Singleton access to the process-wide request/reply correlation table.
pub struct RequestFutureHolder;

#[derive(Default)]
struct RequestFutureRuntimeScheduler {
    producers: HashSet<usize>,
    task: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Default)]
struct RequestFutureScheduler {
    runtimes: HashMap<tokio::runtime::Id, RequestFutureRuntimeScheduler>,
}

fn request_future_scheduler() -> &'static tokio::sync::Mutex<RequestFutureScheduler> {
    static SCHEDULER: OnceLock<tokio::sync::Mutex<RequestFutureScheduler>> = OnceLock::new();
    SCHEDULER.get_or_init(Default::default)
}

impl RequestFutureHolder {
    pub fn get_instance() -> &'static Self {
        static HOLDER: RequestFutureHolder = RequestFutureHolder;
        &HOLDER
    }

    pub fn get_request_future_table(&self) -> RequestFutureTable {
        RequestFutureTable::shared()
    }

    /// Starts the shared three-second initial-delay, one-second timeout scan
    /// for this Producer on the current Tokio runtime. Producer startup and
    /// request preparation call this automatically.
    pub async fn start_scheduled_task(&self, producer: &Producer) {
        let runtime_id = tokio::runtime::Handle::current().id();
        let producer_id = Arc::as_ptr(&producer.request_future_scheduler_id) as usize;
        let mut scheduler = request_future_scheduler().lock().await;
        scheduler.runtimes.retain(|id, runtime| {
            *id == runtime_id
                || !runtime
                    .task
                    .as_ref()
                    .is_some_and(tokio::task::JoinHandle::is_finished)
        });
        let runtime = scheduler.runtimes.entry(runtime_id).or_default();
        runtime.producers.insert(producer_id);
        if runtime
            .task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            runtime.task.take();
        }
        if runtime.task.is_some() {
            return;
        }
        let table = self.get_request_future_table();
        runtime.task = Some(tokio::spawn(async move {
            let period = tokio::time::Duration::from_secs(1);
            let first_scan = tokio::time::Instant::now() + tokio::time::Duration::from_secs(3);
            let mut interval = tokio::time::interval_at(first_scan, period);
            loop {
                interval.tick().await;
                table.expire_due_for_current_runtime(tokio::time::Instant::now());
            }
        }));
    }

    /// Removes this Producer from the scheduler set. The shared scanner stops
    /// after the final Producer shuts down, while pending table entries remain.
    pub async fn shutdown(&self, producer: &Producer) {
        let producer_id = Arc::as_ptr(&producer.request_future_scheduler_id) as usize;
        let tasks = {
            let mut scheduler = request_future_scheduler().lock().await;
            let mut tasks = Vec::new();
            scheduler.runtimes.retain(|_, runtime| {
                runtime.producers.remove(&producer_id);
                if runtime.producers.is_empty() {
                    if let Some(task) = runtime.task.take() {
                        tasks.push(task);
                    }
                    false
                } else {
                    true
                }
            });
            tasks
        };
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
    }
}

type RequestSendFuture<'a> =
    Pin<Box<dyn Future<Output = Result<crate::SendResult, ProducerError>> + Send + 'a>>;
type QueueSelector<'a> = dyn FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send + 'a;

enum RequestRoute<'a> {
    Default,
    Queue(MessageQueue),
    Selector(Box<QueueSelector<'a>>),
}

/// Java-compatible failures from `MessageUtil.createReplyMessage`.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum ReplyMessageError {
    #[error("create reply message fail, requestMessage cannot be null.")]
    NullRequest,
    #[error("create reply message fail, requestMessage error, property[CLUSTER] is null.")]
    MissingCluster,
}

impl ReplyMessageError {
    /// Java `ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION` numeric code.
    pub const fn java_error_code(self) -> i32 {
        10_007
    }
}

/// Builds the reply message used by RocketMQ's request/reply flow.
///
/// As in Java 5.5.1, a request and its `CLUSTER` property are required. The
/// helper copies request correlation, reply-to and TTL metadata when present.
pub fn create_reply_message(
    request: Option<&Message>,
    body: impl Into<Vec<u8>>,
) -> Result<Message, ReplyMessageError> {
    let request = request.ok_or(ReplyMessageError::NullRequest)?;
    let cluster = request
        .get_property(PROPERTY_CLUSTER)
        .ok_or(ReplyMessageError::MissingCluster)?;

    let mut reply = Message::default();
    reply.set_topic(format!("{cluster}_{REPLY_TOPIC_POSTFIX}"));
    reply.set_body(body);
    let _ = reply.set_system_property(PROPERTY_MESSAGE_TYPE, REPLY_MESSAGE_FLAG);
    for property in [
        PROPERTY_CORRELATION_ID,
        PROPERTY_MESSAGE_REPLY_TO_CLIENT,
        PROPERTY_MESSAGE_TTL,
    ] {
        if let Some(value) = request.get_property(property) {
            let _ = reply.set_system_property(property, value);
        }
    }
    Ok(reply)
}

impl Producer {
    /// Sends a request and asynchronously resolves with its correlated reply.
    pub async fn request(
        &self,
        message: &mut Message,
        timeout: Duration,
    ) -> Result<Message, ProducerError> {
        self.request_with_route(message, timeout, RequestRoute::Default)
            .await
    }

    /// Sends a request through a queue selected from the current writable route.
    pub async fn request_with_selector<'a, F>(
        &'a self,
        message: &'a mut Message,
        selector: F,
        timeout: Duration,
    ) -> Result<Message, ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send + 'a,
    {
        self.request_with_route(message, timeout, RequestRoute::Selector(Box::new(selector)))
            .await
    }

    /// Sends a request to one explicit queue.
    pub async fn request_to_queue(
        &self,
        message: &mut Message,
        queue: &MessageQueue,
        timeout: Duration,
    ) -> Result<Message, ProducerError> {
        self.request_with_route(message, timeout, RequestRoute::Queue(queue.clone()))
            .await
    }

    /// Registers a callback for the reply, timeout or final send failure.
    /// Successful request-send completion alone does not invoke the callback.
    pub async fn request_with_callback(
        self: &Arc<Self>,
        message: &mut Message,
        callback: RequestCallback,
        timeout: Duration,
    ) -> Result<(), ProducerError> {
        self.request_callback_with_route(message, timeout, RequestRoute::Default, callback)
            .await
    }

    /// Callback counterpart to [`Producer::request_with_selector`].
    pub async fn request_with_selector_callback<F>(
        self: &Arc<Self>,
        message: &mut Message,
        selector: F,
        callback: RequestCallback,
        timeout: Duration,
    ) -> Result<(), ProducerError>
    where
        F: FnOnce(&[MessageQueue], &Message) -> Option<MessageQueue> + Send + 'static,
    {
        self.request_callback_with_route(
            message,
            timeout,
            RequestRoute::Selector(Box::new(selector)),
            callback,
        )
        .await
    }

    /// Callback counterpart to [`Producer::request_to_queue`].
    pub async fn request_to_queue_with_callback(
        self: &Arc<Self>,
        message: &mut Message,
        queue: &MessageQueue,
        callback: RequestCallback,
        timeout: Duration,
    ) -> Result<(), ProducerError> {
        self.request_callback_with_route(
            message,
            timeout,
            RequestRoute::Queue(queue.clone()),
            callback,
        )
        .await
    }

    async fn request_callback_with_route(
        self: &Arc<Self>,
        message: &mut Message,
        timeout: Duration,
        route: RequestRoute<'static>,
        callback: RequestCallback,
    ) -> Result<(), ProducerError> {
        let request_callback = Arc::new(ProducerRequestCallback {
            callback: Mutex::new(Some(callback)),
            topic: message.topic().to_owned(),
            timeout_millis: timeout.as_millis(),
        });
        let callback: Arc<dyn RequestResponseCallback> = request_callback;
        let (pending, deadline) = self
            .prepare_request(message, timeout, Some(callback))
            .await?;
        let mut request_message = message.clone();
        let producer = self.clone();
        let task = tokio::spawn(async move {
            let _ = producer
                .wait_for_reply(
                    &mut request_message,
                    pending,
                    deadline,
                    timeout,
                    false,
                    route,
                )
                .await;
            drop(producer);
        });
        self.track_request_callback_task(task).await;
        Ok(())
    }

    async fn request_with_route<'a>(
        &'a self,
        message: &'a mut Message,
        timeout: Duration,
        route: RequestRoute<'a>,
    ) -> Result<Message, ProducerError> {
        let (pending, deadline) = self.prepare_request(message, timeout, None).await?;
        self.wait_for_reply(message, pending, deadline, timeout, true, route)
            .await
    }

    async fn prepare_request(
        &self,
        message: &mut Message,
        timeout: Duration,
        callback: Option<Arc<dyn RequestResponseCallback>>,
    ) -> Result<(PendingReplyRequest, Instant), ProducerError> {
        RequestFutureHolder::get_instance()
            .start_scheduled_task(self)
            .await;
        let timeout_millis = timeout.as_millis();
        if timeout_millis > i64::MAX as u128 {
            return Err(ProducerError::InvalidRequestTimeout);
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(ProducerError::InvalidRequestTimeout)?;
        let (runtime, lease) = self.started_registration().await?;
        let reply_to = runtime.client_id().to_owned();
        let ttl = timeout_millis.to_string();
        for _ in 0..3 {
            let correlation_id = create_correlation_id();
            let future = Arc::new(RequestResponseFuture::new(
                correlation_id.clone(),
                timeout_millis as i64,
                callback.clone(),
            ));
            let _ = message.set_system_property(PROPERTY_CORRELATION_ID, correlation_id.clone());
            let _ = message.set_system_property(PROPERTY_MESSAGE_REPLY_TO_CLIENT, reply_to.clone());
            let _ = message.set_system_property(PROPERTY_MESSAGE_TTL, ttl.clone());
            match runtime
                .register_reply_request_with_future(&lease, future, deadline)
                .await
            {
                Ok(pending) => return Ok((pending, deadline)),
                Err(rocketmq_classic_core::runtime::RuntimeError::DuplicateCorrelation {
                    ..
                }) => {
                    continue;
                }
                Err(rocketmq_classic_core::runtime::RuntimeError::RequestReplyAdmissionTimeout) => {
                    return Err(ProducerError::RequestReplyTimeout {
                        topic: message.topic().to_owned(),
                        timeout_millis,
                    });
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(ProducerError::RequestReplyCancelled)
    }

    async fn wait_for_reply(
        &self,
        message: &mut Message,
        mut pending: PendingReplyRequest,
        deadline: Instant,
        timeout: Duration,
        expire_waiter_at_deadline: bool,
        route: RequestRoute<'_>,
    ) -> Result<Message, ProducerError> {
        let timeout_millis = timeout.as_millis();
        let topic = message.topic().to_owned();
        let remaining = deadline.saturating_duration_since(Instant::now());
        let send = match route {
            RequestRoute::Default => {
                Box::pin(self.send_future_with_timeout(message, remaining)) as RequestSendFuture<'_>
            }
            RequestRoute::Queue(queue) => Box::pin(async move {
                self.send_to_queue_future_with_timeout(message, &queue, remaining)
                    .await
            }) as RequestSendFuture<'_>,
            RequestRoute::Selector(selector) => {
                Box::pin(self.send_future_with_selector_with_timeout(message, selector, remaining))
                    as RequestSendFuture<'_>
            }
        };
        tokio::pin!(send);
        let control: PendingReplyControl = pending.control();
        let mut completion = Box::pin(pending.receive());
        let result = tokio::select! {
            result = &mut completion => result,
            _ = tokio::time::sleep_until(deadline), if expire_waiter_at_deadline => {
                control.expire_if_due(Instant::now());
                (&mut completion).await
            }
            send_result = &mut send => {
                match send_result {
                    Ok(_) => control.mark_send_succeeded(),
                    Err(error) => {
                        control.fail_send(error.to_string());
                    }
                }
                (&mut completion).await
            }
        };
        drop(completion);
        match result {
            Ok(PendingReplyCompletion::Reply(reply)) => Ok(Message::clone(&*reply)),
            Ok(PendingReplyCompletion::Timeout { send_succeeded: _ }) => {
                Err(ProducerError::RequestReplyTimeout {
                    topic,
                    timeout_millis,
                })
            }
            Ok(PendingReplyCompletion::SendFailed(cause)) => {
                Err(ProducerError::RequestSendFailed { topic, cause })
            }
            Err(_) => Err(ProducerError::RequestReplyCancelled),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Producer, ProducerConfig, SendResult, SendStatus};
    use rocketmq_classic_common::message::decode_properties;
    use rocketmq_classic_core::{
        api::producer::SendMode, route::TopicSnapshot, runtime::ClientRuntimeManager,
    };
    use rocketmq_classic_protocol::route::TopicRouteData;
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::oneshot;

    fn scripted_send_result() -> SendResult {
        SendResult {
            status: SendStatus::SendOk,
            msg_id: "send-ok".into(),
            offset_msg_id: Some("offset-ok".into()),
            topic: "orders".into(),
            broker_name: "broker-a".into(),
            queue_id: 0,
            queue_offset: 1,
            transaction_id: None,
            recall_handle: None,
            raw_resp_body: None,
            region_id: "DefaultRegion".into(),
            trace_on: true,
        }
    }

    async fn producer(name: &str) -> (Arc<Producer>, [MessageQueue; 2], String) {
        producer_with_config(ProducerConfig::new(name).unwrap()).await
    }

    async fn producer_with_config(
        config: ProducerConfig,
    ) -> (Arc<Producer>, [MessageQueue; 2], String) {
        let mut client_config = config.client_config().clone();
        let topic = client_config
            .with_namespace(Some("orders"))
            .unwrap()
            .unwrap_or_else(|| "orders".into());
        let producer = Arc::new(Producer::with_runtime_manager(
            config,
            ClientRuntimeManager::new(),
        ));
        producer.start().await.unwrap();
        let queues = [
            MessageQueue::new(&topic, "broker-a", 0),
            MessageQueue::new(&topic, "broker-b", 0),
        ];
        producer.cache_publish_info(
            &topic,
            Arc::new(TopicSnapshot {
                route: Arc::new(serde_json::from_str::<TopicRouteData>("{}").unwrap()),
                publish_queues: queues.to_vec(),
                subscribe_queues: BTreeSet::new(),
                endpoints: BTreeMap::from([
                    (queues[0].clone(), "10.0.0.1:10911".into()),
                    (queues[1].clone(), "10.0.0.2:10911".into()),
                ]),
                order_topic: false,
            }),
        );
        let client_id = producer
            .started_runtime()
            .await
            .unwrap()
            .client_id()
            .to_owned();
        (producer, queues, client_id)
    }

    fn queue_success(producer: &Producer, count: usize) {
        producer
            .scripted_transport
            .lock()
            .unwrap()
            .responses
            .extend((0..count).map(|_| Ok(Some(scripted_send_result()))));
    }

    fn take_attempts(producer: &Producer) -> Vec<crate::producer::ScriptedSendAttempt> {
        std::mem::take(&mut producer.scripted_transport.lock().unwrap().attempts)
    }

    async fn wait_for_attempts(producer: &Producer, expected: usize) {
        for _ in 0..1000 {
            if producer.scripted_transport.lock().unwrap().attempts.len() >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("scripted request send did not start");
    }

    #[tokio::test]
    async fn producer_shutdown_matches_java_pending_request_and_timeout_scanner_state() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/lifecycle.json"))
                .unwrap();
        let java = &fixture["requestFutureShutdown"];
        assert_eq!(java["pendingRemainsAfterProducerShutdown"], true);
        assert_eq!(java["callbackCountAfterProducerShutdown"], 0);
        assert_eq!(java["timeoutScannerStopsAfterLastProducer"], true);

        let mut config = ProducerConfig::new("request-future-shutdown").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        let producer = Arc::new(Producer::with_runtime_manager(
            config,
            ClientRuntimeManager::new(),
        ));
        producer.start().await.unwrap();
        let (runtime, lease) = producer.started_registration().await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut pending = runtime
            .register_reply_request(&lease, "request-shutdown-pending", deadline)
            .await
            .unwrap();

        producer.shutdown().await.unwrap();

        assert_eq!(
            runtime.state(),
            rocketmq_classic_core::runtime::RuntimeState::Stopped
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(10), pending.receive())
                .await
                .is_err()
        );
        assert!(pending.control().cancel());
    }

    fn assert_request_attempt(
        attempt: &crate::producer::ScriptedSendAttempt,
        client_id: &str,
        timeout_millis: u128,
        queue: &MessageQueue,
    ) {
        let properties = decode_properties(attempt.properties.as_deref().unwrap_or_default());
        let correlation_id = properties.get(PROPERTY_CORRELATION_ID).unwrap();
        assert_eq!(correlation_id.len(), 36);
        assert_eq!(correlation_id.as_bytes()[14], b'4');
        assert!(matches!(
            correlation_id.as_bytes()[19],
            b'8' | b'9' | b'a' | b'b'
        ));
        assert_eq!(
            properties
                .get(PROPERTY_MESSAGE_REPLY_TO_CLIENT)
                .map(String::as_str),
            Some(client_id)
        );
        assert_eq!(
            properties.get(PROPERTY_MESSAGE_TTL).map(String::as_str),
            Some(timeout_millis.to_string().as_str())
        );
        assert_eq!(attempt.mode, SendMode::Future);
        assert_eq!(&attempt.queue, queue);
    }

    fn timeout_callback(
        calls: Arc<AtomicUsize>,
        sender: oneshot::Sender<Result<Vec<u8>, ProducerError>>,
    ) -> RequestCallback {
        Box::new(move |result| {
            calls.fetch_add(1, Ordering::SeqCst);
            let _ = sender.send(result.map(|reply| reply.body().to_vec()));
        })
    }

    #[test]
    fn correlation_id_uses_java_uuid_v4_text_format() {
        let id = create_correlation_id();
        assert_eq!(id.len(), 36);
        assert_eq!(id.as_bytes()[14], b'4');
        assert!(matches!(id.as_bytes()[19], b'8' | b'9' | b'a' | b'b'));
        assert!(id.bytes().enumerate().all(|(index, byte)| {
            matches!(index, 8 | 13 | 18 | 23)
                .then_some(byte == b'-')
                .unwrap_or_else(|| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        }));
        assert!(id.bytes().all(|byte| !byte.is_ascii_uppercase()));
    }

    #[tokio::test(start_paused = true)]
    async fn all_six_request_overloads_send_with_java_metadata_and_wait_for_reply() {
        let (producer, queues, client_id) = producer("request-overloads").await;
        let timeout = Duration::from_millis(80);
        queue_success(&producer, 6);
        let mut attempts = Vec::new();

        let mut default_message = Message::new("orders", b"default".to_vec());
        assert!(matches!(
            producer.request(&mut default_message, timeout).await,
            Err(ProducerError::RequestReplyTimeout { .. })
        ));
        attempts.extend(take_attempts(&producer));

        let mut selected_message = Message::new("orders", b"selected".to_vec());
        assert!(matches!(
            producer
                .request_with_selector(
                    &mut selected_message,
                    |available, _| Some(available[1].clone()),
                    timeout,
                )
                .await,
            Err(ProducerError::RequestReplyTimeout { .. })
        ));
        attempts.extend(take_attempts(&producer));

        let mut queued_message = Message::new("orders", b"queued".to_vec());
        assert!(matches!(
            producer
                .request_to_queue(&mut queued_message, &queues[0], timeout)
                .await,
            Err(ProducerError::RequestReplyTimeout { .. })
        ));
        attempts.extend(take_attempts(&producer));

        let calls = Arc::new(AtomicUsize::new(0));
        let (sender, mut receiver) = oneshot::channel();
        let mut callback_message = Message::new("orders", b"callback".to_vec());
        producer
            .request_with_callback(
                &mut callback_message,
                timeout_callback(calls.clone(), sender),
                timeout,
            )
            .await
            .unwrap();
        wait_for_attempts(&producer, 1).await;
        assert!(matches!(
            receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), &mut receiver)
                .await
                .unwrap()
                .unwrap(),
            Err(ProducerError::RequestReplyTimeout { .. })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        attempts.extend(take_attempts(&producer));

        let calls = Arc::new(AtomicUsize::new(0));
        let (sender, mut receiver) = oneshot::channel();
        let mut selector_callback_message = Message::new("orders", b"selector-callback".to_vec());
        producer
            .request_with_selector_callback(
                &mut selector_callback_message,
                move |available, _| Some(available[1].clone()),
                timeout_callback(calls.clone(), sender),
                timeout,
            )
            .await
            .unwrap();
        wait_for_attempts(&producer, 1).await;
        assert!(matches!(
            receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), &mut receiver)
                .await
                .unwrap()
                .unwrap(),
            Err(ProducerError::RequestReplyTimeout { .. })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        attempts.extend(take_attempts(&producer));

        let calls = Arc::new(AtomicUsize::new(0));
        let (sender, mut receiver) = oneshot::channel();
        let mut queue_callback_message = Message::new("orders", b"queue-callback".to_vec());
        producer
            .request_to_queue_with_callback(
                &mut queue_callback_message,
                &queues[0],
                timeout_callback(calls.clone(), sender),
                timeout,
            )
            .await
            .unwrap();
        wait_for_attempts(&producer, 1).await;
        assert!(matches!(
            receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), &mut receiver)
                .await
                .unwrap()
                .unwrap(),
            Err(ProducerError::RequestReplyTimeout { .. })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        attempts.extend(take_attempts(&producer));

        assert_eq!(attempts.len(), 6);
        for (index, attempt) in attempts.iter().enumerate() {
            let expected_queue = match index {
                1 | 4 => &queues[1],
                2 | 5 => &queues[0],
                _ => &attempt.queue,
            };
            assert_request_attempt(attempt, &client_id, timeout.as_millis(), expected_queue);
        }
        producer.shutdown().await.unwrap();
        assert!(producer.request_callback_tasks.lock().await.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn namespaced_request_wraps_its_topic_once_before_send() {
        let mut config = ProducerConfig::new("namespaced-request").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".into()));
        let (producer, queues, _) = producer_with_config(config).await;
        queue_success(&producer, 1);

        let mut message = Message::new("orders", b"request".to_vec());
        assert!(matches!(
            producer
                .request(&mut message, Duration::from_millis(80))
                .await,
            Err(ProducerError::RequestReplyTimeout { .. })
        ));

        assert_eq!(message.topic(), "orders");
        let attempts = take_attempts(&producer);
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].topic, "tenant%orders");
        assert_eq!(attempts[0].queue.topic(), Some("tenant%orders"));
        assert_eq!(queues[0].topic(), Some("tenant%orders"));
        let properties = decode_properties(attempts[0].properties.as_deref().unwrap_or_default());
        assert_eq!(
            properties.get(PROPERTY_MESSAGE_TTL).map(String::as_str),
            Some("80")
        );
        producer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_request_timeout_fails_before_mutating_or_sending_message() {
        let producer = Producer::new(ProducerConfig::new("invalid-request-timeout").unwrap());
        let mut message = Message::new("orders", b"body".to_vec());
        let timeout = Duration::from_millis(i64::MAX as u64 + 1);
        assert!(matches!(
            producer.request(&mut message, timeout).await,
            Err(ProducerError::InvalidRequestTimeout)
        ));
        assert!(message.properties().get(PROPERTY_CORRELATION_ID).is_none());
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn send_failure_completes_request_and_callback_once() {
        let (producer, _, _) = producer("request-send-failure").await;
        let foreign_queue = MessageQueue::new("orders", "not-in-route", 8);
        let timeout = Duration::from_millis(100);

        let mut message = Message::new("orders", b"invalid-route".to_vec());
        assert!(matches!(
            producer
                .request_to_queue(&mut message, &foreign_queue, timeout)
                .await,
            Err(ProducerError::RequestSendFailed { .. })
        ));
        assert!(
            producer
                .scripted_transport
                .lock()
                .unwrap()
                .attempts
                .is_empty()
        );

        let calls = Arc::new(AtomicUsize::new(0));
        let (sender, receiver) = oneshot::channel();
        let mut callback_message = Message::new("orders", b"callback-invalid-route".to_vec());
        producer
            .request_to_queue_with_callback(
                &mut callback_message,
                &foreign_queue,
                timeout_callback(calls.clone(), sender),
                timeout,
            )
            .await
            .unwrap();
        let callback_result = tokio::time::timeout(Duration::from_secs(5), receiver)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            callback_result,
            Err(ProducerError::RequestSendFailed { .. })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        producer.shutdown().await.unwrap();
        assert!(producer.request_callback_tasks.lock().await.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn callback_timeout_uses_java_housekeeping_initial_delay_and_period() {
        let producer = Arc::new(Producer::with_runtime_manager(
            ProducerConfig::new("request-callback-schedule").unwrap(),
            ClientRuntimeManager::new(),
        ));
        let holder = RequestFutureHolder::get_instance();
        holder.start_scheduled_task(&producer).await;
        tokio::task::yield_now().await;

        let table = RequestFutureTable::shared();
        let first_calls = Arc::new(AtomicUsize::new(0));
        let second_calls = Arc::new(AtomicUsize::new(0));
        let (first_sender, mut first_receiver) = oneshot::channel();
        let (second_sender, mut second_receiver) = oneshot::channel();
        let add_future =
            |timeout_millis: i64,
             calls: Arc<AtomicUsize>,
             sender: oneshot::Sender<Result<Vec<u8>, ProducerError>>| {
                let correlation_id = create_correlation_id();
                let callback: Arc<dyn RequestResponseCallback> =
                    Arc::new(ProducerRequestCallback {
                        callback: Mutex::new(Some(timeout_callback(calls, sender))),
                        topic: "orders".to_owned(),
                        timeout_millis: timeout_millis as u128,
                    });
                let future = Arc::new(RequestResponseFuture::new(
                    correlation_id.clone(),
                    timeout_millis,
                    Some(callback),
                ));
                table.insert(correlation_id, future).unwrap();
            };
        add_future(50, first_calls.clone(), first_sender);
        add_future(3_500, second_calls.clone(), second_sender);

        assert!(matches!(
            first_receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            second_receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        tokio::time::advance(Duration::from_secs(2)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            first_receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            second_receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            first_receiver.try_recv().unwrap(),
            Err(ProducerError::RequestReplyTimeout { .. })
        ));
        assert!(matches!(
            second_receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 0);

        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            second_receiver.try_recv().unwrap(),
            Err(ProducerError::RequestReplyTimeout { .. })
        ));
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
        holder.shutdown(&producer).await;
    }
}
