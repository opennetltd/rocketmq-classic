//! Scripted-peer proof that one clustering Push consumer admits one Pull per assigned
//! queue at the same time, releases each scheduler slot when its owned task finishes,
//! and shuts down while polls are still outstanding.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
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
    let handle = tokio::spawn(async move {
        let _live = LiveWitness(counter);
        future.await;
    });
    tasks
        .handles
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push((name, handle));
}

/// Join one recorded peer task. A cleanup timeout is a test failure, never something tolerated.
async fn join_peer_task(label: &str, task: JoinHandle<()>) {
    match timeout(Duration::from_secs(2), task).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("peer task {label} ended abnormally: {error}"),
        Err(_) => panic!("peer task {label} survived the 2s cleanup bound"),
    }
}

/// Join every recorded connection task and require the witness to reach zero.
async fn join_recorded_tasks(tasks: &PeerTasks) {
    // The registry lock is released before any await so teardown cannot deadlock on it.
    let recorded = {
        let mut guard = tasks
            .handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::take(&mut *guard)
    };
    assert!(
        !recorded.is_empty(),
        "the peer recorded no connection task, so the witness proves nothing"
    );
    for (name, handle) in recorded {
        join_peer_task(name, handle).await;
    }
    assert_eq!(
        tasks.live_connection_tasks.load(Ordering::SeqCst),
        0,
        "a peer connection task survived teardown"
    );
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
async fn broker_peer(
    shutdown: broadcast::Receiver<()>,
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
                        _ = shutdown.recv() => break,
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
                        _ = connection_shutdown.recv() => break,
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
        broker_peer(shutdown_rx).await;
    let (namesrv_addr, namesrv_task) =
        nameserver_peer(broker_address, shutdown_tx.subscribe()).await;

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

    // Peer teardown is owned and bounded: acceptance closes, every connection is cancelled
    // cooperatively, then each recorded task is joined. A cleanup timeout fails the test.
    drop(events);
    let _ = release_tx;
    let _ = shutdown_tx.send(());
    join_peer_task("broker accept", broker_task).await;
    join_peer_task("nameserver accept", namesrv_task).await;
    join_recorded_tasks(&peer_tasks).await;
}

/// A client that never closes its socket must not strand a peer task: the cleanup path is
/// cooperative, so it also works when a scenario failed before any exchange completed.
#[tokio::test]
async fn peer_teardown_finishes_every_task_after_an_abandoned_connection() {
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let (broker_address, _release_tx, events, broker_task, peer_tasks) =
        broker_peer(shutdown_rx).await;

    // The injected failure is a client that connects and then never speaks or closes.
    let abandoned = TcpStream::connect(&broker_address).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while peer_tasks.live_connection_tasks.load(Ordering::SeqCst) == 0 && Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_ne!(
        peer_tasks.live_connection_tasks.load(Ordering::SeqCst),
        0,
        "the peer recorded no connection task for the accepted socket"
    );

    drop(events);
    let _ = shutdown_tx.send(());
    join_peer_task("broker accept", broker_task).await;
    join_recorded_tasks(&peer_tasks).await;
    drop(abandoned);
}
