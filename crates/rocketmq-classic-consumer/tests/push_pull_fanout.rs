//! Scripted-peer proof that one clustering Push consumer admits one Pull per assigned
//! queue at the same time, releases each scheduler slot when its owned task finishes,
//! and shuts down while polls are still outstanding.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
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
    net::TcpListener,
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

async fn nameserver_peer(broker_address: String) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            while let Some(command) = read_command(&mut stream).await {
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
async fn broker_peer() -> (
    String,
    broadcast::Sender<u64>,
    mpsc::UnboundedReceiver<BrokerEvent>,
    JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (release_tx, _) = broadcast::channel(16);
    let client_id = Arc::new(Mutex::new(None::<String>));

    let connect_release = release_tx.clone();
    let accept_task = tokio::spawn(async move {
        let mut connection = 0_u64;
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            connection += 1;
            let (mut read_half, mut write_half) = stream.into_split();
            let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Vec<u8>>();
            tokio::spawn(async move {
                while let Some(bytes) = out_rx.recv().await {
                    if write_half.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
            });

            let (command_tx, mut command_rx) = mpsc::unbounded_channel::<RemotingCommand>();
            tokio::spawn(async move {
                while let Some(command) = read_command(&mut read_half).await {
                    if command_tx.send(command).is_err() {
                        break;
                    }
                }
            });

            let mut release_rx = connect_release.subscribe();
            let event_tx = event_tx.clone();
            let client_id = client_id.clone();
            tokio::spawn(async move {
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
                    }
                }
            });
        }
    });

    (address, release_tx, event_rx, accept_task)
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
    let (broker_address, release_tx, mut events, broker_task) = broker_peer().await;
    let (namesrv_addr, namesrv_task) = nameserver_peer(broker_address).await;

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

    // Peer teardown is bounded: closing the client ends every per-connection task.
    drop(events);
    let _ = release_tx;
    broker_task.abort();
    namesrv_task.abort();
    for task in [broker_task, namesrv_task] {
        if let Ok(result) = timeout(Duration::from_secs(1), task).await {
            // Aborted tasks report a join error; anything else is unexpected.
            assert!(result.is_err(), "peer task ended without being aborted");
        }
    }
}
