//! Scripted-peer proof that one clustering Push consumer admits one Pull per assigned
//! queue at the same time, releases each scheduler slot when its owned task finishes,
//! and shuts down while polls are still outstanding.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::BytesMut;
use rocketmq_classic_common::message::MessageExt;
use rocketmq_classic_consumer::{
    ConsumeConcurrentlyStatus, PushConsumer, PushConsumerConfig, PushMessageListener,
};
use rocketmq_classic_protocol::{
    codes::{request, response},
    header::HeaderMap,
};
use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::{Instant, timeout},
};

const TOPIC: &str = "fanout-orders";
const QUEUE_COUNT: i32 = 16;
/// Java `PullMessageRequestHeader.suspendTimeoutMillis` for a simple/Pull consumer.
const PULL_SUSPEND_MILLIS: i64 = 15_000;

#[derive(Clone, Debug, PartialEq, Eq)]
struct PullObserved {
    connection: u64,
    opaque: i32,
    queue_id: i32,
    suspend_timeout_millis: Option<i64>,
}

#[derive(Debug)]
enum BrokerEvent {
    Pull(PullObserved),
    Unregistered,
}

/// One accepted connection task plus the label used when a cleanup join fails.
type RecordedTask = (&'static str, JoinHandle<()>);

/// Bound for joining one peer task during cleanup; a timeout is a test failure.
const CLEANUP_BOUND: Duration = Duration::from_secs(2);

/// Every accepted connection task is recorded here, so teardown can join each one instead of
/// leaving detached tasks behind.
#[derive(Clone, Default)]
struct PeerTasks {
    handles: Arc<Mutex<Vec<RecordedTask>>>,
    live_connection_tasks: Arc<AtomicUsize>,
}

/// Decrements the live-connection witness when its task ends, including on abort.
struct LiveWitness(Arc<AtomicUsize>);

impl Drop for LiveWitness {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Spawn one connection task and record it together with its witness.
fn spawn_connection_task(
    tasks: &PeerTasks,
    name: &'static str,
    future: impl Future<Output = ()> + Send + 'static,
) {
    let counter = Arc::clone(&tasks.live_connection_tasks);
    counter.fetch_add(1, Ordering::SeqCst);
    // The witness is built before the task exists and moved into the future, so an abort before
    // the first poll still drops it and releases the already-incremented counter.
    let witness = LiveWitness(counter);
    let handle = tokio::spawn(async move {
        let _live = witness;
        future.await;
    });
    tasks
        .handles
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push((name, handle));
}

/// Abort owned tasks if their surrounding cleanup future is cancelled or unwinds.
struct AbortFallback(Vec<tokio::task::AbortHandle>);

impl Drop for AbortFallback {
    fn drop(&mut self) {
        for handle in &self.0 {
            handle.abort();
        }
    }
}

/// Recover the original panic text from a failed scenario task, so cleanup can report what the
/// scenario actually asserted instead of only that some task died.
fn scenario_failure_message(error: tokio::task::JoinError) -> String {
    if !error.is_panic() {
        return error.to_string();
    }
    let payload = error.into_panic();
    // `assert!` and friends panic with an owned String; a bare `panic!("literal")` may be a &str.
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_owned();
    }
    "the scenario task panicked without a message".to_owned()
}

/// Join one peer task, returning why it did not finish cleanly instead of panicking, so cleanup
/// can observe every handle before any failure is reported. The handle stays owned while waiting,
/// and a timed-out task is aborted and then awaited.
async fn join_peer_task(label: &str, task: &mut JoinHandle<()>, bound: Duration) -> Option<String> {
    let _fallback = AbortFallback(vec![task.abort_handle()]);
    match timeout(bound, &mut *task).await {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(format!("peer task {label} ended abnormally: {error}")),
        Err(_) => {
            task.abort();
            // Await the cancellation result without dropping ownership at a second timeout.
            // These local peer futures yield cooperatively; the budget bounds graceful joining.
            let result = task.await;
            Some(format!(
                "peer task {label} survived the {bound:?} cleanup bound and was aborted: {result:?}"
            ))
        }
    }
}

/// Accumulates peer cleanup failures so every handle is drained before anything is reported.
struct Cleanup {
    bound: Duration,
    reasons: Vec<String>,
}

impl Cleanup {
    fn new(bound: Duration) -> Self {
        Self {
            bound,
            reasons: Vec::new(),
        }
    }

    async fn join(&mut self, label: &'static str, task: &mut JoinHandle<()>) {
        if let Some(reason) = join_peer_task(label, task, self.bound).await {
            self.reasons.push(reason);
        }
    }

    /// Drain every recorded connection task, then read the witness. Nothing panics here: a timed
    /// out or panicking child only adds another reason.
    async fn drain_peers(&mut self, tasks: &PeerTasks) -> usize {
        // The registry lock is released before any await so teardown cannot deadlock on it.
        let recorded = {
            let mut guard = tasks
                .handles
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *guard)
        };
        let _fallback = AbortFallback(
            recorded
                .iter()
                .map(|(_, handle)| handle.abort_handle())
                .collect(),
        );
        for (name, mut handle) in recorded {
            self.join(name, &mut handle).await;
        }
        tasks.live_connection_tasks.load(Ordering::SeqCst)
    }
}

/// Result of running a scenario under peer ownership: the scenario failure is kept separate from
/// cleanup failures, and both are reported only after every handle was drained.
#[derive(Debug)]
struct ScenarioOutcome {
    scenario_failure: Option<String>,
    peer_failures: Vec<String>,
    live_witness_after_drain: usize,
}

impl ScenarioOutcome {
    /// Report the scenario failure and any cleanup failure together, after cleanup has finished.
    fn finish(self) {
        let mut problems = self.peer_failures;
        if let Some(message) = self.scenario_failure {
            problems.push(format!("the scenario failed: {message}"));
        }
        if self.live_witness_after_drain != 0 {
            problems.push(format!(
                "the live peer witness ended at {}, not zero",
                self.live_witness_after_drain
            ));
        }
        assert!(problems.is_empty(), "{problems:#?}");
    }
}

/// Peer ownership that lives outside the fallible scenario body.
struct PeerHarness {
    shutdown_tx: broadcast::Sender<()>,
    broker_task: JoinHandle<()>,
    namesrv_task: Option<JoinHandle<()>>,
    peer_tasks: PeerTasks,
}

impl Drop for PeerHarness {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(());
        self.broker_task.abort();
        if let Some(task) = &self.namesrv_task {
            task.abort();
        }
        let handles = self
            .peer_tasks
            .handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (_, handle) in handles.iter() {
            handle.abort();
        }
    }
}

impl PeerHarness {
    /// Run `scenario` in its own task so neither a returned failure nor an injected panic can
    /// bypass cleanup: acceptance is closed, every recorded child task is drained, and only then is
    /// the scenario failure handed back to the caller.
    async fn run(
        mut self,
        cleanup_bound: Duration,
        scenario_bound: Duration,
        scenario: impl Future<Output = ()> + Send + 'static,
    ) -> ScenarioOutcome {
        let mut scenario_task = tokio::spawn(scenario);
        let _scenario = AbortFallback(vec![scenario_task.abort_handle()]);
        let joined = timeout(scenario_bound, &mut scenario_task).await;
        if joined.is_err() {
            scenario_task.abort();
            let _ = scenario_task.await;
        }

        // Ownership teardown happens unconditionally, before any failure is surfaced.
        let _ = self.shutdown_tx.send(());
        let mut cleanup = Cleanup::new(cleanup_bound);
        cleanup.join("broker accept", &mut self.broker_task).await;
        if let Some(task) = self.namesrv_task.as_mut() {
            cleanup.join("nameserver accept", task).await;
        }
        let live_witness_after_drain = cleanup.drain_peers(&self.peer_tasks).await;

        let scenario_failure = match joined {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(scenario_failure_message(error)),
            Err(_) => Some(format!(
                "the scenario did not finish within its {scenario_bound:?} bound"
            )),
        };
        ScenarioOutcome {
            scenario_failure,
            peer_failures: cleanup.reasons,
            live_witness_after_drain,
        }
    }
}

async fn read_command<S: AsyncReadExt + Unpin>(stream: &mut S) -> Option<RemotingCommand> {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.ok()?;
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.ok()?;
    frame::decode_next(&mut bytes).ok().flatten()
}

fn encode_reply(
    opaque: i32,
    code: i32,
    body: Option<Vec<u8>>,
    ext_fields: Option<HeaderMap>,
) -> Vec<u8> {
    let mut reply = RemotingCommand {
        code,
        opaque,
        body,
        ext_fields: ext_fields.map(Into::into),
        ..Default::default()
    };
    reply.mark_response();
    frame::encode(&reply).unwrap()
}

fn reply(
    out: &mpsc::UnboundedSender<Vec<u8>>,
    request: &RemotingCommand,
    code: i32,
    body: Option<Vec<u8>>,
    ext_fields: Option<HeaderMap>,
) {
    let _ = out.send(encode_reply(request.opaque, code, body, ext_fields));
}

/// A valid empty Pull reply. An empty `SUCCESS` frame is not a legal Pull result.
fn pull_not_found(opaque: i32) -> Vec<u8> {
    encode_reply(
        opaque,
        response::PULL_NOT_FOUND,
        None,
        Some(HeaderMap::from([
            ("suggestWhichBrokerId".into(), "0".into()),
            ("nextBeginOffset".into(), "0".into()),
            ("minOffset".into(), "0".into()),
            ("maxOffset".into(), "0".into()),
        ])),
    )
}

async fn nameserver_peer(
    broker_address: String,
    mut shutdown: broadcast::Receiver<()>,
) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        loop {
            // Acceptance closes cooperatively, so teardown never has to abort this task.
            let mut stream = tokio::select! {
                _ = shutdown.recv() => break,
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => stream,
                    Err(_) => break,
                },
            };
            loop {
                let command = tokio::select! {
                    _ = shutdown.recv() => None,
                    command = read_command(&mut stream) => command,
                };
                let Some(command) = command else {
                    break;
                };
                let requested_topic = command
                    .ext_fields
                    .as_ref()
                    .and_then(|fields| fields.get("topic"))
                    .map(String::as_str);
                let body = if command.code == request::GET_ROUTEINFO_BY_TOPIC
                    && requested_topic == Some(TOPIC)
                {
                    json!({
                        "queueDatas": [{
                            "brokerName": "broker-a",
                            "readQueueNums": QUEUE_COUNT,
                            "writeQueueNums": QUEUE_COUNT,
                            "perm": 6,
                            "topicSysFlag": 0
                        }],
                        "brokerDatas": [{
                            "cluster": "DefaultCluster",
                            "brokerName": "broker-a",
                            "brokerAddrs": {"0": broker_address}
                        }],
                        "filterServerTable": {}
                    })
                } else if command.code == request::GET_ROUTEINFO_BY_TOPIC {
                    // Retry and other topics stay unassigned so only the target topic
                    // can produce Pull traffic.
                    json!({"queueDatas": [], "brokerDatas": [], "filterServerTable": {}})
                } else {
                    json!(null)
                };
                let body = (body != json!(null)).then(|| serde_json::to_vec(&body).unwrap());
                stream
                    .write_all(&encode_reply(command.opaque, response::SUCCESS, body, None))
                    .await
                    .unwrap();
            }
        }
    });
    (address, task)
}

/// One broker peer that answers control traffic but holds every target-topic Pull
/// response until a release round is broadcast. Frame reads stay in their own task so a
/// held response can never block later requests on the same connection.
/// `cooperative` controls whether the per-connection reader and handler honour cancellation; a
/// forced-timeout test sets it to false to prove cleanup drains rather than tolerates them.
async fn broker_peer(
    shutdown: broadcast::Receiver<()>,
    cooperative: bool,
) -> (
    String,
    broadcast::Sender<u64>,
    mpsc::UnboundedReceiver<BrokerEvent>,
    JoinHandle<()>,
    PeerTasks,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (release_tx, _) = broadcast::channel(16);
    let client_id = Arc::new(Mutex::new(None::<String>));
    let tasks = PeerTasks::default();

    let connect_release = release_tx.clone();
    let accept_shutdown = shutdown.resubscribe();
    let accept_tasks = tasks.clone();
    let accept_task = tokio::spawn(async move {
        let mut shutdown = accept_shutdown;
        let mut connection = 0_u64;
        loop {
            // Acceptance closes cooperatively, so teardown never aborts it mid-connection.
            let stream = tokio::select! {
                _ = shutdown.recv() => break,
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => stream,
                    Err(_) => break,
                },
            };
            connection += 1;
            let (mut read_half, mut write_half) = stream.into_split();
            let (out_tx, out_rx) = mpsc::unbounded_channel::<Vec<u8>>();
            spawn_connection_task(&accept_tasks, "broker writer", async move {
                let mut out_rx = out_rx;
                while let Some(bytes) = out_rx.recv().await {
                    if write_half.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
            });

            let (command_tx, command_rx) = mpsc::unbounded_channel::<RemotingCommand>();
            let reader_shutdown = shutdown.resubscribe();
            spawn_connection_task(&accept_tasks, "broker reader", async move {
                let command_tx = command_tx;
                let mut shutdown = reader_shutdown;
                loop {
                    tokio::select! {
                        _ = shutdown.recv(), if cooperative => break,
                        command = read_command(&mut read_half) => match command {
                            Some(command) => {
                                if command_tx.send(command).is_err() {
                                    break;
                                }
                            }
                            None => break,
                        },
                    }
                }
            });

            let mut release_rx = connect_release.subscribe();
            let mut connection_shutdown = shutdown.resubscribe();
            let event_tx = event_tx.clone();
            let client_id = client_id.clone();
            spawn_connection_task(&accept_tasks, "broker handler", async move {
                let mut command_rx = command_rx;
                // (connection, opaque) -> queue id for every unanswered target Pull.
                let mut held: Vec<(u64, i32)> = Vec::new();
                loop {
                    tokio::select! {
                        command = command_rx.recv() => {
                            let Some(command) = command else { break };
                            match command.code {
                                request::HEART_BEAT => {
                                    if let Some(body) = command.body.as_ref() {
                                        if let Ok(value) =
                                            serde_json::from_slice::<serde_json::Value>(body)
                                        {
                                            if let Some(id) =
                                                value.get("clientID").and_then(|v| v.as_str())
                                            {
                                                *client_id
                                                    .lock()
                                                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                                    Some(id.to_owned());
                                            }
                                        }
                                    }
                                    reply(&out_tx, &command, response::SUCCESS, None, None);
                                }
                                request::GET_CONSUMER_LIST_BY_GROUP => {
                                    let consumer_ids = client_id
                                        .lock()
                                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                                        .clone()
                                        .map(|id| vec![id])
                                        .unwrap_or_default();
                                    reply(
                                        &out_tx,
                                        &command,
                                        response::SUCCESS,
                                        Some(
                                            json!({ "consumerIdList": consumer_ids })
                                                .to_string()
                                                .into_bytes(),
                                        ),
                                        None,
                                    );
                                }
                                request::PULL_MESSAGE => {
                                    let fields = command.ext_fields.clone().unwrap_or_default();
                                    let topic = fields.get("topic").map(String::as_str).unwrap_or_default();
                                    if topic != TOPIC {
                                        // Retry-topic and housekeeping pulls are answered
                                        // immediately so they cannot skew the barrier.
                                        let _ = out_tx.send(pull_not_found(command.opaque));
                                        continue;
                                    }
                                    let queue_id = fields
                                        .get("queueId")
                                        .and_then(|value| value.parse::<i32>().ok())
                                        .unwrap_or(-1);
                                    held.push((connection, command.opaque));
                                    let _ = event_tx.send(BrokerEvent::Pull(PullObserved {
                                        connection,
                                        opaque: command.opaque,
                                        queue_id,
                                        suspend_timeout_millis: fields
                                            .get("suspendTimeoutMillis")
                                            .and_then(|value| value.parse::<i64>().ok()),
                                    }));
                                }
                                request::UNREGISTER_CLIENT => {
                                    let _ = event_tx.send(BrokerEvent::Unregistered);
                                    reply(&out_tx, &command, response::SUCCESS, None, None);
                                }
                                // No committed offset: Java answers QUERY_NOT_FOUND and the
                                // client then resolves ConsumeFromWhere on the broker.
                                request::QUERY_CONSUMER_OFFSET => {
                                    reply(&out_tx, &command, response::QUERY_NOT_FOUND, None, None)
                                }
                                request::GET_MAX_OFFSET
                                | request::GET_MIN_OFFSET
                                | request::SEARCH_OFFSET_BY_TIMESTAMP => reply(
                                    &out_tx,
                                    &command,
                                    response::SUCCESS,
                                    None,
                                    Some(HeaderMap::from([("offset".into(), "0".into())])),
                                ),
                                _ => reply(&out_tx, &command, response::SUCCESS, None, None),
                            }
                        }
                        Ok(_round) = release_rx.recv() => {
                            for (held_connection, opaque) in held.drain(..) {
                                debug_assert_eq!(held_connection, connection);
                                let _ = out_tx.send(pull_not_found(opaque));
                            }
                        }
                        // Cooperative teardown: the handler exits first, which closes the writer
                        // channel and the command queue behind it.
                        _ = connection_shutdown.recv(), if cooperative => break,
                    }
                }
            });
        }
    });

    (address, release_tx, event_rx, accept_task, tasks)
}

/// Collects target-topic Pull requests until every queue id `0..QUEUE_COUNT` has been
/// seen at least once, then rejects any further admission while responses are held.
async fn collect_round(
    events: &mut mpsc::UnboundedReceiver<BrokerEvent>,
    budget: Duration,
) -> BTreeMap<i32, Vec<PullObserved>> {
    let mut rounds: BTreeMap<i32, Vec<PullObserved>> = BTreeMap::new();
    let started = Instant::now();
    let complete = timeout(budget, async {
        loop {
            match events.recv().await {
                Some(BrokerEvent::Pull(pull)) => {
                    rounds.entry(pull.queue_id).or_default().push(pull);
                    if rounds.len() == usize::try_from(QUEUE_COUNT).unwrap() {
                        break;
                    }
                }
                Some(BrokerEvent::Unregistered) => {}
                None => break,
            }
        }
    })
    .await;
    if complete.is_err() {
        panic!(
            "only {} of {QUEUE_COUNT} queues started a Pull within {budget:?}",
            rounds.len()
        );
    }

    // A second admission for any queue while every response is still held means more
    // than one active pull per queue-generation.
    let remaining = budget.saturating_sub(started.elapsed());
    let extra = timeout(remaining.min(Duration::from_millis(250)), async {
        loop {
            match events.recv().await {
                Some(BrokerEvent::Pull(pull)) => return Some(pull),
                Some(BrokerEvent::Unregistered) => continue,
                None => return None,
            }
        }
    })
    .await;
    if let Ok(Some(extra)) = extra {
        panic!(
            "queue {} started another Pull before any Pull response was sent",
            extra.queue_id
        );
    }
    rounds
}

fn assert_fanout(rounds: &BTreeMap<i32, Vec<PullObserved>>) -> BTreeSet<(u64, i32)> {
    let expected = (0..QUEUE_COUNT).collect::<Vec<_>>();
    assert_eq!(rounds.keys().copied().collect::<Vec<_>>(), expected);
    for (queue_id, pulls) in rounds {
        assert_eq!(
            pulls.len(),
            1,
            "queue {queue_id} admitted more than one Pull"
        );
        assert_eq!(
            pulls[0].suspend_timeout_millis,
            Some(PULL_SUSPEND_MILLIS),
            "queue {queue_id} used the wrong Pull suspend timeout"
        );
    }
    rounds
        .values()
        .flatten()
        .map(|pull| (pull.connection, pull.opaque))
        .collect::<BTreeSet<_>>()
}

#[tokio::test]
async fn push_pull_fanout_admits_every_assigned_queue_before_any_response() {
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let (broker_address, release_tx, mut events, broker_task, peer_tasks) =
        broker_peer(shutdown_rx, true).await;
    let (namesrv_addr, namesrv_task) =
        nameserver_peer(broker_address, shutdown_tx.subscribe()).await;

    let harness = PeerHarness {
        shutdown_tx,
        broker_task,
        namesrv_task: Some(namesrv_task),
        peer_tasks,
    };

    // The scenario body is fallible: an assertion failure inside it must still leave the peer with
    // every child task drained.
    let outcome = harness
        .run(CLEANUP_BOUND, Duration::from_secs(30), async move {
            let mut config = PushConsumerConfig::new("push-pull-fanout").unwrap();
            config
                .client_config_mut()
                .set_namesrv_addr(Some(namesrv_addr));
            let consumer = Arc::new(PushConsumer::new(config));
            consumer
                .register_message_listener(PushMessageListener::concurrently(
                    |_messages: Vec<MessageExt>, context| async move {
                        (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                    },
                ))
                .await
                .unwrap();
            consumer.subscribe(TOPIC, "*").await.unwrap();
            consumer.start().await.unwrap();

            // Round 1: initial fan-out. Every assigned queue must be in flight at once; a fixed
            // four-worker pool would leave twelve queues unrequested.
            let first = assert_fanout(&collect_round(&mut events, Duration::from_secs(5)).await);
            assert_eq!(first.len(), usize::try_from(QUEUE_COUNT).unwrap());

            // Release round 1; the second fan-out proves each owned task released its slot and
            // that the follow-up was rescheduled through the scheduler.
            release_tx.send(1).unwrap();
            let second = assert_fanout(&collect_round(&mut events, Duration::from_secs(5)).await);
            for key in &first {
                assert!(
                    !second.contains(key),
                    "round 2 reused the (connection, opaque) identity {key:?} of round 1"
                );
            }

            // Shut down while the second round's polls are still unanswered. The local wait
            // budget is deliberately tiny so outstanding tasks must be aborted, not awaited.
            consumer
                .shutdown_with_await_termination_millis(50)
                .await
                .unwrap();
            assert!(!consumer.is_running().await);

            let mut late_pulls = 0_usize;
            let mut unregistered = false;
            while let Ok(Some(event)) = timeout(Duration::from_millis(250), events.recv()).await {
                match event {
                    BrokerEvent::Pull(_) => late_pulls += 1,
                    BrokerEvent::Unregistered => unregistered = true,
                }
            }
            assert_eq!(late_pulls, 0, "a Pull was admitted after shutdown");
            assert!(
                unregistered,
                "the consumer did not unregister before its broker connection closed"
            );

            drop(events);
        })
        .await;

    outcome.finish();
}

/// A client that never closes its socket must not strand a peer task: the cleanup path is
/// cooperative, so it also works when a scenario failed before any exchange completed.
#[tokio::test]
async fn peer_teardown_finishes_every_task_after_an_abandoned_connection() {
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let (broker_address, _release_tx, events, broker_task, peer_tasks) =
        broker_peer(shutdown_rx, true).await;

    // The injected condition is a client that connects and then never speaks or closes.
    let abandoned = TcpStream::connect(&broker_address).await.unwrap();
    let witness = Arc::clone(&peer_tasks.live_connection_tasks);
    let harness = PeerHarness {
        shutdown_tx,
        broker_task,
        namesrv_task: None,
        peer_tasks,
    };

    let outcome = harness
        .run(CLEANUP_BOUND, Duration::from_secs(10), async move {
            let deadline = Instant::now() + Duration::from_secs(2);
            while witness.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_ne!(
                witness.load(Ordering::SeqCst),
                0,
                "the peer recorded no connection task for the accepted socket"
            );
            drop(events);
        })
        .await;

    outcome.finish();
    drop(abandoned);
}

/// An assertion failure in the scenario body must not bypass cleanup: every child task is drained
/// and the witness reaches zero before the injected failure is reported.
#[tokio::test]
async fn peer_cleanup_drains_every_task_after_an_injected_scenario_failure() {
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let (broker_address, _release_tx, events, broker_task, peer_tasks) =
        broker_peer(shutdown_rx, true).await;

    // A live connection is held open in the test body, so the peer's reader and handler are both
    // parked on it when the scenario fails.
    let held_open = TcpStream::connect(&broker_address).await.unwrap();
    let observed = Arc::clone(&peer_tasks.live_connection_tasks);

    let harness = PeerHarness {
        shutdown_tx,
        broker_task,
        namesrv_task: None,
        peer_tasks,
    };

    let outcome = harness
        .run(CLEANUP_BOUND, Duration::from_secs(10), async move {
            let deadline = Instant::now() + Duration::from_secs(2);
            while observed.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_ne!(
                observed.load(Ordering::SeqCst),
                0,
                "the peer recorded no connection task for the held socket"
            );
            drop(events);
            // The injected scenario failure: an assertion that fails while the connection is open.
            panic!("injected fan-out scenario failure while a connection was held open");
        })
        .await;

    let message = outcome
        .scenario_failure
        .expect("the injected scenario failure must be reported");
    assert_eq!(
        message, "injected fan-out scenario failure while a connection was held open",
        "cleanup must report the original scenario failure verbatim"
    );
    // Cleanup ran first and completely: no child was left unobserved, and none of them failed.
    assert_eq!(
        outcome.peer_failures,
        Vec::<String>::new(),
        "cleanup reported failures"
    );
    assert_eq!(
        outcome.live_witness_after_drain, 0,
        "a peer task survived cleanup"
    );
    drop(held_open);
}

/// Forced cleanup timeouts must still drain every recorded child, and the accumulated reasons are
/// reported only after the witness reached zero.
#[tokio::test]
async fn peer_cleanup_reports_forced_timeouts_after_draining_every_child() {
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    // Uncooperative children: cancellation cannot end them, so cleanup must abort and observe them.
    let (broker_address, _release_tx, events, broker_task, peer_tasks) =
        broker_peer(shutdown_rx, false).await;

    let client = TcpStream::connect(&broker_address).await.unwrap();
    let observed = Arc::clone(&peer_tasks.live_connection_tasks);

    let harness = PeerHarness {
        shutdown_tx,
        broker_task,
        namesrv_task: None,
        peer_tasks,
    };

    // A short cleanup bound turns each uncooperative child into a recorded timeout.
    let outcome = harness
        .run(
            Duration::from_millis(300),
            Duration::from_secs(10),
            async move {
                let deadline = Instant::now() + Duration::from_secs(2);
                // One accepted connection spawns a reader, a writer and a handler task.
                while observed.load(Ordering::SeqCst) < 3 && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                assert!(
                    observed.load(Ordering::SeqCst) >= 3,
                    "the peer spawned {} connection tasks, expected more than one",
                    observed.load(Ordering::SeqCst)
                );
                drop(events);
            },
        )
        .await;

    assert_eq!(outcome.scenario_failure, None, "the scenario itself failed");
    assert!(
        outcome.peer_failures.len() > 1,
        "cleanup must report every timed-out child, got {:?}",
        outcome.peer_failures
    );
    for reason in &outcome.peer_failures {
        assert!(
            reason.contains("aborted"),
            "a timed-out task was not aborted: {reason}"
        );
    }
    // Every handle was observed before the failures were reported.
    assert_eq!(
        outcome.live_witness_after_drain, 0,
        "a peer task survived cleanup"
    );
    drop(client);
}

/// The witness is created before its task exists, so an abort before the first poll still releases
/// the already-incremented counter.
#[tokio::test]
async fn an_aborted_peer_task_releases_its_witness_before_the_first_poll() {
    let tasks = PeerTasks::default();
    let polled = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&polled);

    spawn_connection_task(&tasks, "never polled", async move {
        flag.store(true, Ordering::SeqCst);
    });
    assert_eq!(tasks.live_connection_tasks.load(Ordering::SeqCst), 1);

    let (_, handle) = {
        let mut recorded = tasks
            .handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        recorded.pop().expect("one recorded task")
    };
    // Cancel before the runtime ever polls the task.
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());

    assert!(
        !polled.load(Ordering::SeqCst),
        "the aborted task was polled after cancellation"
    );
    assert_eq!(
        tasks.live_connection_tasks.load(Ordering::SeqCst),
        0,
        "an abort before the first poll leaked a live peer witness"
    );
}

/// A scenario timeout must observe cancellation before returning its failure.
#[tokio::test]
async fn scenario_timeout_aborts_and_joins_the_scenario() {
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let (_address, _release, _events, broker_task, peer_tasks) =
        broker_peer(shutdown_rx, true).await;
    let counter = Arc::new(AtomicUsize::new(1));
    let witness = LiveWitness(counter.clone());
    let outcome = PeerHarness {
        shutdown_tx,
        broker_task,
        namesrv_task: None,
        peer_tasks,
    }
    .run(CLEANUP_BOUND, Duration::from_millis(20), async move {
        let _witness = witness;
        std::future::pending::<()>().await;
    })
    .await;
    assert!(outcome.scenario_failure.unwrap().contains("did not finish"));
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert!(outcome.peer_failures.is_empty());
}

/// Cancelling the entire harness still aborts its scenario and all registered peers.
#[tokio::test]
async fn cancelling_harness_aborts_owned_tasks() {
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let (address, _release, _events, broker_task, peer_tasks) =
        broker_peer(shutdown_rx, false).await;
    let connection = TcpStream::connect(address).await.unwrap();
    let observed = peer_tasks.live_connection_tasks.clone();
    let scenario_live = Arc::new(AtomicUsize::new(1));
    let witness = LiveWitness(scenario_live.clone());
    let harness = tokio::spawn(
        PeerHarness {
            shutdown_tx,
            broker_task,
            namesrv_task: None,
            peer_tasks,
        }
        .run(CLEANUP_BOUND, Duration::from_secs(30), async move {
            let _witness = witness;
            std::future::pending::<()>().await;
        }),
    );
    timeout(CLEANUP_BOUND, async {
        while observed.load(Ordering::SeqCst) < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    harness.abort();
    assert!(harness.await.unwrap_err().is_cancelled());
    timeout(CLEANUP_BOUND, async {
        while observed.load(Ordering::SeqCst) != 0 || scenario_live.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(connection);
}

/// One panicked child must not prevent cancellation and joining of the next child.
#[tokio::test]
async fn child_panic_does_not_skip_remaining_peer_cleanup() {
    let tasks = PeerTasks::default();
    spawn_connection_task(&tasks, "panicked child", async {
        panic!("injected child panic")
    });
    spawn_connection_task(&tasks, "pending child", std::future::pending());
    let mut cleanup = Cleanup::new(Duration::from_millis(20));
    assert_eq!(cleanup.drain_peers(&tasks).await, 0);
    assert_eq!(cleanup.reasons.len(), 2);
    assert!(cleanup.reasons[0].contains("panicked child"));
    assert!(cleanup.reasons[1].contains("aborted"));
}
