use std::{
    any::Any,
    collections::HashSet,
    env,
    error::Error,
    fmt::Write as _,
    path::Path,
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant as StdInstant},
};

use rocketmq_classic::{
    AclClientRpcHook, ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, ConsumeOrderlyContext,
    ConsumeOrderlyStatus, EndTransactionContext, EndTransactionHook, LocalTransactionState,
    MessageSelector, Producer, ProducerConfig, ProducerError, PullConsumer, PullConsumerConfig,
    PushConsumer, PushConsumerConfig, PushMessageListener, RequestFutureHolder, SendMessageContext,
    SendMessageHook, SessionCredentials, TransactionCallbackError, TransactionListener,
    TransactionProducer, create_reply_message,
};
use rocketmq_classic_common::message::{
    DecodeOptions, Message, MessageExt, MessageQueue, decode_many_as_client_compatible,
    decode_message_id, encode_compact_messages, encode_properties,
};
use rocketmq_classic_common::sysflag::pull::{build_pull_sys_flag, build_pull_sys_flag_with_lite};
use rocketmq_classic_consumer::PopReceipt;
use rocketmq_classic_consumer::rebalance::{
    AllocateMessageQueueAveragely, AllocateMessageQueueStrategy, RebalanceError,
};
use rocketmq_classic_core::api::{
    MQClientApi,
    producer::{SendKind, SendMode},
};
use rocketmq_classic_core::error::RpcError;
use rocketmq_classic_core::runtime::{ClientRuntimeManager, RuntimeState};
use rocketmq_classic_core::unique_id::UniqueIdGenerator;
use rocketmq_classic_protocol::{
    SharedValue,
    body::{
        LiteSubscriptionAction, LiteSubscriptionCtlRequestBody, LiteSubscriptionDto,
        LiteSubscriptionDtoSet, LockBatchRequestBody, MessageRequestMode,
        QueryAssignmentRequestBody, SetMessageRequestModeRequestBody, UnlockBatchRequestBody,
    },
    codes::{request, response},
    header::{
        ConsumerSendMsgBackRequestHeader, EndTransactionRequestHeader, GetMaxOffsetRequestHeader,
        PopLiteMessageRequestHeader, PopMessageRequestHeader, PullMessageRequestHeader,
        QueryConsumerOffsetRequestHeader, SendMessageRequestHeaderV1,
        UnregisterClientRequestHeader, UpdateConsumerOffsetRequestHeader, ViewMessageRequestHeader,
    },
    heartbeat::{
        ConsumeFromWhere, ConsumeType, ConsumerData, HeartbeatData, MessageModel, ProducerData,
        SubscriptionData,
    },
};
use rocketmq_classic_remoting::client::{ClientConfig, RemotingClient, TlsConfig, TransportMode};
use tokio::time::Instant;

#[derive(Debug, Clone, Copy)]
struct LifecycleCounts {
    pending_requests: usize,
    connections: usize,
    owned_tasks: usize,
}

fn remoting_lifecycle_counts(client: &RemotingClient) -> Result<LifecycleCounts, Box<dyn Error>> {
    #[cfg(rocketmq_classic_test_support)]
    {
        let counts = client.lifecycle_counts();
        Ok(LifecycleCounts {
            pending_requests: counts.pending_requests,
            connections: counts.connections,
            owned_tasks: counts.owned_tasks,
        })
    }
    #[cfg(not(rocketmq_classic_test_support))]
    {
        let _ = client;
        Err("lifecycle diagnostics require tests/differential/run.sh".into())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    match arguments.first().map(String::as_str) {
        Some("route") if arguments.len() == 3 => route(&arguments[1], &arguments[2]).await,
        Some("p3-raw-route") if arguments.len() == 3 => {
            p3_raw_route(&arguments[1], &arguments[2]).await
        }
        Some("p3-tls") if arguments.len() == 5 => {
            p3_tls(&arguments[1], &arguments[2], &arguments[3], &arguments[4]).await
        }
        Some("p16-auth") if arguments.len() == 3 => {
            p16_auth(&arguments[1], &arguments[2]).await
        }
        Some("p16-tls") if arguments.len() == 6 => {
            p16_tls(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                &arguments[5],
            )
            .await
        }
        Some("p17-query") if arguments.len() == 6 => {
            p17_query(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                arguments[4].parse()?,
                arguments[5].parse()?,
            )
            .await
        }
        Some("rebalance") if arguments.len() == 4 => {
            rebalance(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("p7-producer") if arguments.len() == 3 => {
            high_level_producer_modes(&arguments[1], &arguments[2]).await
        }
        Some("p7-retry") if arguments.len() == 8 => {
            p7_retry_probe(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                arguments[5].parse()?,
                &arguments[6],
                &arguments[7],
            )
            .await
        }
        Some("p10-push") if arguments.len() == 6 => {
            p10_push_probe(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                &arguments[5],
            )
            .await
        }
        Some("p11-mixed-group") if arguments.len() == 7 => {
            p11_mixed_group_probe(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                &arguments[5],
                &arguments[6],
            )
            .await
        }
        Some("p12-orderly") if arguments.len() == 9 => {
            p12_orderly_probe(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                P12OrderlyProbeFiles {
                    ownership: &arguments[4],
                    ready: &arguments[5],
                    stop: &arguments[6],
                    exited: &arguments[7],
                    marker_dir: &arguments[8],
                },
            )
            .await
        }
        Some("p11-push") if arguments.len() == 6 => {
            p11_push_retry_probe(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                &arguments[5],
            )
            .await
        }
        Some("p11-producer") if arguments.len() == 5 => {
            p11_producer_probe(&arguments[1], &arguments[2], &arguments[3], &arguments[4]).await
        }
        Some("p7-lifecycle") if arguments.len() == 3 => {
            producer_lifecycle_probe(&arguments[1], &arguments[2]).await
        }
        Some("p7-stress") if arguments.len() == 7 => {
            let count = arguments[3].parse::<usize>()?;
            let max_in_flight = arguments[4].parse::<usize>()?;
            let seed = arguments[5].parse::<u32>()?;
            let start_offset = arguments[6].parse::<i64>()?;
            producer_stress_probe(
                &arguments[1],
                &arguments[2],
                count,
                max_in_flight,
                seed,
                start_offset,
            )
            .await
        }
        Some("p5-producer") if arguments.len() == 5 => {
            p5_producer(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                arguments[4].parse()?,
            )
            .await
        }
        Some("p5-consumer") if arguments.len() == 5 => {
            p5_consumer(&arguments[1], &arguments[2], &arguments[3], &arguments[4]).await
        }
        Some("p5-extension-prepare") if arguments.len() == 4 => {
            p5_extensions(&arguments[1], &arguments[2], &arguments[3], true).await
        }
        Some("p5-extension-consume") if arguments.len() == 4 => {
            p5_extensions(&arguments[1], &arguments[2], &arguments[3], false).await
        }
        Some("p5-extension-lite-pop") if arguments.len() == 4 => {
            p5_extension_lite_pop(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("p13-pop-consume") if arguments.len() == 4 => {
            p13_pop_consume(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("p13-pop-prepare") if arguments.len() == 4 => {
            p13_pop_prepare(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("p6-shared-runtime") if arguments.len() == 6 => {
            shared_runtime_probe(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                &arguments[5],
            )
            .await
        }
        Some("p9-pull") if arguments.len() == 4 => {
            pull_consumer_probe(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("p19-request") if arguments.len() == 5 => {
            request_reply_request(&arguments[1], &arguments[2], &arguments[3], &arguments[4]).await
        }
        Some("p19-timeout") if arguments.len() == 7 => {
            request_reply_timeout(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                arguments[5].parse()?,
                arguments[6].parse()?,
            )
            .await
        }
        Some("p19-late-timeout") if arguments.len() == 7 => {
            request_reply_late_timeout(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                arguments[5].parse()?,
                arguments[6].parse()?,
            )
            .await
        }
        Some("p19-respond") if arguments.len() == 6 || arguments.len() == 7 => {
            request_reply_respond(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                &arguments[5],
                arguments.get(6).map_or(Ok(0), |delay| delay.parse())?,
            )
            .await
        }
        Some("transaction") if arguments.len() == 7 => {
            transaction(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                &arguments[5],
                &arguments[6],
            )
            .await
        }
        Some("p14-transaction") if arguments.len() == 7 => {
            p14_transaction(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                &arguments[5],
                &arguments[6],
            )
            .await
        }
        Some("max-offset") if arguments.len() == 4 => {
            max_offset(&arguments[1], &arguments[2], arguments[3].parse()?).await
        }
        Some("heartbeat") if arguments.len() == 3 => {
            heartbeat(&arguments[1], &arguments[2]).await
        }
        Some("send") if arguments.len() == 4 => {
            send(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("timeout-send") if arguments.len() == 7 => {
            timeout_send(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                arguments[5].parse()?,
                &arguments[6],
            )
            .await
        }
        Some("async-send") if arguments.len() == 4 => {
            async_send(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("oneway-send") if arguments.len() == 6 => {
            oneway_send(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                arguments[5].parse()?,
            )
            .await
        }
        Some("batch-send") if arguments.len() == 5 => {
            batch_send(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                arguments[4].parse()?,
            )
            .await
        }
        Some("view") if arguments.len() == 4 => {
            view(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("view-batch") if arguments.len() == 4 => {
            view_batch(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("offset-query") if arguments.len() == 5 => {
            offset_query(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                arguments[4].parse()?,
            )
            .await
        }
        Some("offset-query-named") if arguments.len() == 6 => {
            offset_query_named(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                arguments[5].parse()?,
            )
            .await
        }
        Some("offset-update") if arguments.len() == 6 => {
            offset_update(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                arguments[4].parse()?,
                arguments[5].parse()?,
            )
            .await
        }
        Some("pull") if arguments.len() == 6 => {
            pull(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                arguments[4].parse()?,
                arguments[5].parse()?,
            )
            .await
        }
        Some("p14-pull") if arguments.len() == 7 => {
            pull_named(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                arguments[5].parse()?,
                arguments[6].parse()?,
            )
            .await
        }
        Some("lock") if arguments.len() == 7 => {
            lock_batch(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                &arguments[5],
                arguments[6].parse()?,
            )
            .await
        }
        Some("unlock") if arguments.len() == 7 => {
            unlock_batch(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                &arguments[5],
                arguments[6].parse()?,
            )
            .await
        }
        _ => Err("usage: live_client route <namesrv-address> <topic> | rebalance <namesrv-address> <topic> <comma-separated-consumer-ids> | p7-producer <namesrv-address> <topic> | p10-push <namesrv-address> <topic> <group> <ready-file> <failure-go-file> | p11-mixed-group <namesrv-address> <topic> <group> <ownership-file> <stop-file> <exited-file> | p12-orderly <namesrv-address> <topic> <group> <ownership-file> <ready-file> <stop-file> <exited-file> <marker-dir> | p11-push <namesrv-address> <topic> <group> <body> <ready-file> | p11-producer <namesrv-address> <topic> <producer-group> <body> | p5-producer <broker-address> <topic> <producer-group> <minimum-base-offset> | p5-consumer <broker-address> <group> <topic> <seed-body> | p5-extension-prepare|p5-extension-consume|p5-extension-lite-pop <broker-address> <topic> <group-prefix> | p6-shared-runtime <namesrv-address> <topic> <producer-a> <producer-b> <consumer-group> | p9-pull <namesrv-address> <topic> <consumer-group> | p19-request <namesrv-address> <topic> <producer-group> <body> | p19-timeout|p19-late-timeout <namesrv-address> <topic> <producer-group> <body> <timeout-ms> <hold-ms> | p19-respond <namesrv-address> <topic> <consumer-group> <producer-group> <reply-body> [delay-ms] | transaction <namesrv-address> <topic> <producer-group> <commit|rollback> <key> <body> | p14-transaction <namesrv-address> <topic> <producer-group> <commit|rollback|unknown> <key> <body> | p14-pull <broker-address> <broker-name> <group> <topic> <queue-id> <offset> | max-offset <broker-address> <topic> <queue-id> | timeout-send <proxy-address> <topic> <producer-group> <body> <timeout-ms> <go-file> | heartbeat <broker-address> <producer-group> | send|async-send <broker-address> <topic> <body> | oneway-send <broker-address> <topic> <body> <key> <minimum-max-offset> | batch-send <broker-address> <topic> <body-prefix> <message-count> | view <broker-address> <topic> <offset-message-id> | view-batch <broker-address> <topic> <comma-separated-offset-message-ids> | offset-query <broker-address> <group> <topic> <queue-id> | offset-query-named <broker-address> <broker-name> <group> <topic> <queue-id> | offset-update <broker-address> <group> <topic> <queue-id> <offset> | pull <broker-address> <group> <topic> <queue-id> <queue-offset> | lock|unlock <broker-address> <group> <client-id> <topic> <broker-name> <queue-id>".into()),
    }
}

struct P10FixedQueueAllocation {
    topic: String,
    retry_topic: String,
}

impl AllocateMessageQueueStrategy for P10FixedQueueAllocation {
    fn allocate(
        &self,
        _consumer_group: &str,
        _current_client_id: &str,
        queues: &[MessageQueue],
        _consumer_ids: &[String],
    ) -> Result<Vec<MessageQueue>, RebalanceError> {
        Ok(queues
            .iter()
            .filter(|queue| {
                matches!(queue.topic(), Some(topic) if topic == self.topic || topic == self.retry_topic)
                    && queue.broker_name() == Some("broker-a")
                    && queue.queue_id() == 0
            })
            .cloned()
            .collect())
    }

    fn name(&self) -> &str {
        "p10-fixed-live-test-assignment"
    }
}

async fn p10_push_probe(
    namesrv_address: &str,
    topic: &str,
    consumer_group: &str,
    ready_file: &str,
    failure_go_file: &str,
) -> Result<(), Box<dyn Error>> {
    let retry_topic = format!("%RETRY%{consumer_group}");
    let mut config = PushConsumerConfig::new(consumer_group)?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.client_config_mut().set_mq_client_api_timeout(5_000);
    config.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
    config.set_max_reconsume_times(1);
    config.set_pull_batch_size(32);
    config.set_consume_message_batch_max_size(4);
    config.set_allocate_message_queue_strategy(Arc::new(P10FixedQueueAllocation {
        topic: topic.to_owned(),
        retry_topic: retry_topic.clone(),
    }));
    let consumer = Arc::new(PushConsumer::new(config));
    let (observed_tx, mut observed_rx) = tokio::sync::mpsc::unbounded_channel::<(String, i32)>();
    let ready_path = Path::new(ready_file).to_owned();
    let failure_go_path = Path::new(failure_go_file).to_owned();
    consumer
        .set_message_listener(PushMessageListener::concurrently(
            move |messages: Vec<MessageExt>, mut context: ConsumeConcurrentlyContext| {
                let observed_tx = observed_tx.clone();
                let ready_path = ready_path.clone();
                let failure_go_path = failure_go_path.clone();
                Box::pin(async move {
                    let mut partial_reject_index = None;
                    let mut saw_failure = false;
                    let mut saw_recovered_partial = false;
                    let mut bodies = Vec::new();
                    let mut retries = Vec::new();
                    for (index, message) in messages.iter().enumerate() {
                        let body = String::from_utf8_lossy(message.body().as_ref()).into_owned();
                        let reconsume_times = message.reconsume_times();
                        println!(
                            "P10\tCALLBACK\t{body}\t{reconsume_times}\t{}",
                            message.queue_offset()
                        );
                        let _ = observed_tx.send((body.clone(), reconsume_times));
                        if body == "p10-partial-retry" && reconsume_times == 0 {
                            partial_reject_index = Some(index);
                        }
                        if body == "p10-partial-retry" && reconsume_times > 0 {
                            saw_recovered_partial = true;
                        }
                        if body == "p10-failure-dlq" {
                            saw_failure = true;
                        }
                        bodies.push(body);
                        retries.push(reconsume_times);
                    }
                    context.set_delay_level_when_next_consume(1);
                    if saw_failure {
                        println!(
                            "P10\tBATCH\tRECONSUME_LATER\t{}\t{}",
                            bodies.join(","),
                            retries
                                .iter()
                                .map(i32::to_string)
                                .collect::<Vec<_>>()
                                .join(",")
                        );
                        (Some(ConsumeConcurrentlyStatus::ReconsumeLater), context)
                    } else if let Some(index) = partial_reject_index {
                        context.set_ack_index(index as i32 - 1);
                        println!(
                            "P10\tBATCH\tPARTIAL\t{}\t{}\t{}",
                            bodies.join(","),
                            retries
                                .iter()
                                .map(i32::to_string)
                                .collect::<Vec<_>>()
                                .join(","),
                            context.ack_index()
                        );
                        (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                    } else {
                        println!(
                            "P10\tBATCH\tSUCCESS\t{}\t{}",
                            bodies.join(","),
                            retries
                                .iter()
                                .map(i32::to_string)
                                .collect::<Vec<_>>()
                                .join(",")
                        );
                        if saw_recovered_partial {
                            if let Err(error) = std::fs::write(&ready_path, "ready\n") {
                                eprintln!("failed to publish P10 readiness gate: {error}");
                                return (Some(ConsumeConcurrentlyStatus::ReconsumeLater), context);
                            }
                            let deadline = Instant::now() + Duration::from_secs(90);
                            while !failure_go_path.exists() {
                                if Instant::now() >= deadline {
                                    eprintln!("timed out waiting for P10 failure seed");
                                    return (
                                        Some(ConsumeConcurrentlyStatus::ReconsumeLater),
                                        context,
                                    );
                                }
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
                        }
                        (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                    }
                })
            },
        ))
        .await?;
    consumer.subscribe(topic, "*").await?;
    consumer.start().await?;
    let operation: Result<(), Box<dyn Error>> = async {
        wait_for_file(Path::new(ready_file), Duration::from_secs(90)).await?;
        println!("P10\tREADY\tFIXED\tbroker-a\t0");
        wait_for_file(Path::new(failure_go_file), Duration::from_secs(90)).await?;
        let deadline = Instant::now() + Duration::from_secs(90);
        let dlq_topic = format!("%DLQ%{consumer_group}");
        let mut events = Vec::new();
        let dlq_message = loop {
            while let Ok((body, reconsume_times)) = observed_rx.try_recv() {
                println!("P10\tOBSERVED\t{body}\t{reconsume_times}");
                events.push((body, reconsume_times));
            }
            match consumer
                .query_message(&dlq_topic, "p10-failure-dlq-key", 10, 0, i64::MAX)
                .await
            {
                Ok(query) => {
                    let matching = query.get_message_list().and_then(|messages| {
                        messages
                            .snapshot()
                            .into_iter()
                            .flatten()
                            .map(|message| {
                                message.with(|message| {
                                    (
                                        message.topic().to_owned(),
                                        String::from_utf8_lossy(message.body().as_ref())
                                            .into_owned(),
                                        message.reconsume_times(),
                                        message.keys().unwrap_or_default(),
                                    )
                                })
                            })
                            .find(|(_, body, _, keys)| {
                                body == "p10-failure-dlq"
                                    && keys
                                        .split_whitespace()
                                        .any(|key| key == "p10-failure-dlq-key")
                            })
                    });
                    if let Some(message) = matching {
                        break message;
                    }
                }
                Err(error) => println!(
                    "P10\tDLQ_QUERY_RETRY\t{}",
                    error.to_string().replace(['\t', '\n'], " ")
                ),
            }
            if Instant::now() >= deadline {
                return Err("timed out waiting for the configured P10 DLQ transition".into());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        if dlq_message.0 != dlq_topic {
            return Err(
                format!("DLQ query returned an unexpected topic: {:?}", dlq_message).into(),
            );
        }
        println!(
            "P10\tDLQ\t{}\t{}\t{}\t{}",
            dlq_message.0, dlq_message.1, dlq_message.2, dlq_message.3
        );
        while let Ok(event) = observed_rx.try_recv() {
            events.push(event);
        }
        for body in [
            "p10-success",
            "p10-partial-ack",
            "p10-partial-retry",
            "p10-failure-dlq",
        ] {
            if !events.iter().any(|(observed, _)| observed == body) {
                return Err(format!("listener did not observe {body}").into());
            }
        }
        let partial_retries = events
            .iter()
            .filter(|(body, _)| body == "p10-partial-retry")
            .map(|(_, count)| *count)
            .collect::<Vec<_>>();
        let failure_retries = events
            .iter()
            .filter(|(body, _)| body == "p10-failure-dlq")
            .map(|(_, count)| *count)
            .collect::<Vec<_>>();
        if partial_retries != [0, 1] {
            return Err(format!("partial batch retry counts differ: {partial_retries:?}").into());
        }
        if failure_retries != [0, 1] || dlq_message.2 != 2 {
            return Err(format!(
                "DLQ retry counts differ: listener={failure_retries:?}, dlq={}",
                dlq_message.2
            )
            .into());
        }
        println!("P10\tPASS\tLISTENER_SUCCESS_PARTIAL_FAILURE\tRETRIES=1\tDLQ=FOUND");
        Ok(())
    }
    .await;
    let shutdown = consumer.shutdown().await;
    operation?;
    shutdown?;
    Ok(())
}

async fn p11_mixed_group_probe(
    namesrv_address: &str,
    topic: &str,
    consumer_group: &str,
    ownership_file: &str,
    stop_file: &str,
    exited_file: &str,
) -> Result<(), Box<dyn Error>> {
    let mut config = PushConsumerConfig::new(consumer_group)?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.client_config_mut().set_mq_client_api_timeout(5_000);
    config.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
    config.set_pull_batch_size(32);
    config.set_consume_message_batch_max_size(4);

    let consumer = Arc::new(PushConsumer::new(config));
    consumer
        .set_message_listener(PushMessageListener::concurrently(
            |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                Box::pin(async move {
                    for message in messages {
                        println!(
                            "P11\tDELIVERY\trust\t{}\t{}",
                            String::from_utf8_lossy(message.body().as_ref()),
                            message.reconsume_times()
                        );
                    }
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                })
            },
        ))
        .await?;
    consumer.subscribe(topic, "*").await?;
    consumer.start().await?;

    let ownership_path = Path::new(ownership_file).to_owned();
    let stop_path = Path::new(stop_file).to_owned();
    let exited_path = Path::new(exited_file).to_owned();
    let operation: Result<(), Box<dyn Error>> = async {
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            let running_info = consumer.consumer_running_info().await?;
            let mut owned = running_info
                .get_mq_table()
                .map(|table| {
                    table.with(|rows| {
                        rows.iter()
                            .filter(|(queue, _)| queue.topic() == Some(topic))
                            .map(|(queue, _)| {
                                format!(
                                    "{}|{}|{}",
                                    queue.topic().unwrap_or_default(),
                                    queue.broker_name().unwrap_or_default(),
                                    queue.queue_id()
                                )
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .unwrap_or_default();
            owned.sort();
            write_p11_ownership(&ownership_path, &owned)?;
            println!(
                "P11\tSNAPSHOT\trust\t{}\t{}",
                owned.len(),
                if owned.is_empty() {
                    "-".to_owned()
                } else {
                    owned.join(",")
                }
            );
            if stop_path.exists() {
                break;
            }
            if Instant::now() >= deadline {
                return Err("timed out waiting for the P11 stop gate".into());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(())
    }
    .await;
    let shutdown = consumer.shutdown().await;
    write_p11_ownership(&ownership_path, &[])?;
    std::fs::write(&exited_path, "exited\n")?;
    operation?;
    shutdown?;
    Ok(())
}

struct P12OrderlyProbeFiles<'a> {
    ownership: &'a str,
    ready: &'a str,
    stop: &'a str,
    exited: &'a str,
    marker_dir: &'a str,
}

async fn p12_orderly_probe(
    namesrv_address: &str,
    topic: &str,
    consumer_group: &str,
    files: P12OrderlyProbeFiles<'_>,
) -> Result<(), Box<dyn Error>> {
    let mut config = PushConsumerConfig::new(consumer_group)?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config
        .client_config_mut()
        .set_instance_name(Some("a-p12-rust".to_owned()));
    config.client_config_mut().set_mq_client_api_timeout(5_000);
    config.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
    config.set_pull_batch_size(1);
    config.set_consume_message_batch_max_size(1);

    let consumer = Arc::new(PushConsumer::new(config));
    let marker_dir = Path::new(files.marker_dir).to_owned();
    consumer
        .set_message_listener(PushMessageListener::orderly(
            move |messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                let marker_dir = marker_dir.clone();
                Box::pin(async move {
                    for message in messages {
                        let body = String::from_utf8_lossy(message.body().as_ref()).into_owned();
                        let broker = message.broker_name().unwrap_or_default().to_owned();
                        let queue_id = message.queue_id();
                        let offset = message.queue_offset();
                        println!(
                            "P12\tCALLBACK\trust\t{body}\t{broker}\t{queue_id}\t{offset}\tBEGIN"
                        );
                        let marker = marker_dir
                            .join(format!("rust-{broker}-{queue_id}-{offset}-{body}.seen"));
                        if let Err(error) = std::fs::write(&marker, "seen\n") {
                            eprintln!("P12 marker write failed: {error}");
                            return (
                                Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment),
                                context,
                            );
                        }
                        println!(
                            "P12\tCALLBACK\trust\t{body}\t{broker}\t{queue_id}\t{offset}\tEND"
                        );
                    }
                    (Some(ConsumeOrderlyStatus::Success), context)
                })
            },
        ))
        .await?;
    consumer.subscribe(topic, "*").await?;
    consumer.start().await?;

    let ownership_path = Path::new(files.ownership).to_owned();
    let ready_path = Path::new(files.ready).to_owned();
    let stop_path = Path::new(files.stop).to_owned();
    let exited_path = Path::new(files.exited).to_owned();
    std::fs::write(&ready_path, "ready\n")?;
    let operation: Result<(), Box<dyn Error>> = async {
        let deadline = Instant::now() + Duration::from_secs(150);
        loop {
            let running_info = consumer.consumer_running_info().await?;
            let mut owned = running_info
                .get_mq_table()
                .map(|table| {
                    table.with(|rows| {
                        rows.iter()
                            .filter(|(queue, _)| queue.topic() == Some(topic))
                            .map(|(queue, info)| {
                                format!(
                                    "{}|{}|{}|{}",
                                    queue.topic().unwrap_or_default(),
                                    queue.broker_name().unwrap_or_default(),
                                    queue.queue_id(),
                                    info.is_locked()
                                )
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .unwrap_or_default();
            owned.sort();
            write_p11_ownership(&ownership_path, &owned)?;
            println!(
                "P12\tSNAPSHOT\trust\t{}\t{}",
                owned.len(),
                if owned.is_empty() {
                    "-".to_owned()
                } else {
                    owned.join(",")
                }
            );
            if stop_path.exists() {
                break;
            }
            if Instant::now() >= deadline {
                return Err("timed out waiting for the P12 stop gate".into());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Ok(())
    }
    .await;
    let shutdown = consumer.shutdown().await;
    write_p11_ownership(&ownership_path, &[])?;
    std::fs::write(&exited_path, "exited\n")?;
    operation?;
    shutdown?;
    Ok(())
}

async fn p11_push_retry_probe(
    namesrv_address: &str,
    topic: &str,
    consumer_group: &str,
    expected_body: &str,
    ready_file: &str,
) -> Result<(), Box<dyn Error>> {
    let manager = ClientRuntimeManager::shared_default();
    let ready_path = Path::new(ready_file);
    let mut observed = Vec::new();

    for cycle in 0..3 {
        let mut config = PushConsumerConfig::new(consumer_group)?;
        config
            .client_config_mut()
            .set_namesrv_addr(Some(namesrv_address.to_owned()));
        config.client_config_mut().set_mq_client_api_timeout(5_000);
        config.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
        config.set_max_reconsume_times(1);
        config.set_consume_message_batch_max_size(1);
        config.set_pull_batch_size(8);

        let consumer = Arc::new(PushConsumer::new(config));
        let (delivery_tx, mut delivery_rx) =
            tokio::sync::mpsc::unbounded_channel::<(String, i32, String, i32, i64)>();
        let expected_body = expected_body.to_owned();
        let listener_expected_body = expected_body.clone();
        consumer
            .set_message_listener(PushMessageListener::concurrently(
                move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                    let delivery_tx = delivery_tx.clone();
                    let expected_body = listener_expected_body.clone();
                    Box::pin(async move {
                        let mut retry = false;
                        for message in messages {
                            let body = String::from_utf8_lossy(message.body().as_ref()).into_owned();
                            let reconsume_times = message.reconsume_times();
                            let broker = message.broker_name().unwrap_or_default().to_owned();
                            println!(
                                "P11\tDELIVERY\trust\t{body}\t{reconsume_times}\t{}\t{broker}\t{}\t{}",
                                message.topic(),
                                message.queue_id(),
                                message.queue_offset()
                            );
                            let _ = delivery_tx.send((
                                body.clone(),
                                reconsume_times,
                                broker,
                                message.queue_id(),
                                message.queue_offset(),
                            ));
                            retry |= body == expected_body && reconsume_times == 0;
                        }
                        if retry {
                            (Some(ConsumeConcurrentlyStatus::ReconsumeLater), context)
                        } else {
                            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                        }
                    })
                },
            ))
            .await?;
        consumer.subscribe(topic, "*").await?;
        consumer.start().await?;
        let runtime = consumer
            .get_client_runtime()
            .await
            .ok_or("started Rust Push consumer did not retain its ClientRuntime")?;

        if cycle == 0 {
            std::fs::write(ready_path, "ready\n")?;
            let deadline = Duration::from_secs(90);
            loop {
                let delivery = tokio::time::timeout(deadline, delivery_rx.recv())
                    .await
                    .map_err(|_| "timed out waiting for Java-produced Rust Push retry")?
                    .ok_or("Rust Push delivery channel closed before the retry completed")?;
                observed.push(delivery.clone());
                if delivery.0 == expected_body && delivery.1 > 0 {
                    break;
                }
            }
            let retry_counts = observed
                .iter()
                .filter(|delivery| delivery.0 == expected_body)
                .map(|delivery| delivery.1)
                .collect::<Vec<_>>();
            if retry_counts != [0, 1] {
                return Err(format!("Rust Push retry counts differ: {retry_counts:?}").into());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        } else {
            tokio::time::sleep(Duration::from_millis(750)).await;
        }

        consumer.shutdown().await?;
        assert_p11_runtime_stopped(&runtime, &manager, &format!("Rust Push cycle {cycle}"))?;
        let counts = remoting_lifecycle_counts(runtime.remoting())?;
        println!(
            "P11\tCYCLE\trust\t{cycle}\t{}\t{}\t{}\t{}",
            counts.pending_requests,
            counts.connections,
            counts.owned_tasks,
            runtime.state() == RuntimeState::Stopped
        );
    }

    println!("P11\tPASS\tRUST_PUSH_RETRY\tCYCLES=3\tRETRIES=1");
    Ok(())
}

async fn p11_producer_probe(
    namesrv_address: &str,
    topic: &str,
    producer_group: &str,
    body: &str,
) -> Result<(), Box<dyn Error>> {
    let manager = ClientRuntimeManager::shared_default();
    let mut config = ProducerConfig::new(producer_group)?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.client_config_mut().set_mq_client_api_timeout(5_000);
    let producer = Arc::new(Producer::new(config));
    producer.start().await?;
    let runtime = manager
        .factory_table()
        .snapshot()?
        .into_values()
        .next()
        .ok_or("started Rust producer did not register a ClientRuntime")?;

    let mut message = Message::new(topic, body.as_bytes().to_vec());
    let result = producer
        .send_with_selector(&mut message, |queues, _| {
            queues
                .iter()
                .find(|queue| queue.broker_name() == Some("broker-b") && queue.queue_id() == 0)
                .cloned()
        })
        .await;
    let shutdown = producer.shutdown().await;
    let result = result?;
    shutdown?;
    assert_p11_runtime_stopped(&runtime, &manager, "Rust producer")?;
    println!(
        "P11\tPRODUCED\trust\t{body}\t{}\t{}\t{}\t{}",
        result.status.name(),
        result.broker_name,
        result.queue_id,
        result.queue_offset
    );
    Ok(())
}

fn assert_p11_runtime_stopped(
    runtime: &rocketmq_classic_core::runtime::ClientRuntime,
    manager: &ClientRuntimeManager,
    owner: &str,
) -> Result<(), Box<dyn Error>> {
    let counts = remoting_lifecycle_counts(runtime.remoting())?;
    let factories_empty = manager.factory_table().is_empty()?;
    if runtime.state() != RuntimeState::Stopped
        || !factories_empty
        || counts.pending_requests != 0
        || counts.connections != 0
        || counts.owned_tasks != 0
    {
        return Err(format!(
            "{owner} shutdown retained runtime resources: state={:?} factories_empty={factories_empty} counts={counts:?}",
            runtime.state()
        )
        .into());
    }
    Ok(())
}

fn write_p11_ownership(path: &Path, queues: &[String]) -> Result<(), Box<dyn Error>> {
    let temporary = path.with_extension("tmp");
    let contents = if queues.is_empty() {
        String::new()
    } else {
        format!("{}\n", queues.join("\n"))
    };
    std::fs::write(&temporary, contents)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

async fn wait_for_file(path: &Path, duration: Duration) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + duration;
    while !path.exists() {
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for gate file {}", path.display()).into());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Ok(())
}

async fn route(namesrv_address: &str, topic: &str) -> Result<(), Box<dyn Error>> {
    let mut config = ProducerConfig::new("p19RouteRust")?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.client_config_mut().set_mq_client_api_timeout(5_000);

    let producer = Arc::new(Producer::new(config));
    producer.start().await?;
    let result = producer.fetch_publish_message_queues(topic).await;
    let shutdown = producer.shutdown().await;
    shutdown?;
    match result {
        Ok(queues) if queues.is_empty() => println!("EMPTY_ROUTE\t{topic}"),
        Ok(mut queues) => {
            queues.sort_by_key(|queue| {
                (
                    queue.broker_name().unwrap_or_default().to_owned(),
                    queue.queue_id(),
                )
            });
            for queue in queues {
                println!(
                    "QUEUE\t{}\t{}\t{}",
                    queue.topic().unwrap_or_default(),
                    queue.broker_name().unwrap_or_default(),
                    queue.queue_id()
                );
            }
        }
        Err(ProducerError::NoRoute(_)) => println!("NO_ROUTE\t{topic}"),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

async fn p3_raw_route(namesrv_address: &str, topic: &str) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    remoting.update_name_server_address_list(vec![namesrv_address.to_owned()]);
    remoting.start()?;
    let api = MQClientApi::new(remoting.clone());
    let route = api
        .get_topic_route(topic, Instant::now() + Duration::from_secs(10))
        .await?;
    let brokers = route
        .as_ref()
        .map(|route| route.broker_datas_snapshot().unwrap_or_default().len())
        .unwrap_or_default();
    remoting.shutdown().await;
    let counts = remoting_lifecycle_counts(&remoting)?;
    if brokers == 0 {
        return Err(format!("raw route invoke returned no route for {topic}").into());
    }
    if counts.pending_requests != 0 || counts.connections != 0 || counts.owned_tasks != 0 {
        return Err(format!("raw route shutdown retained resources: {counts:?}").into());
    }
    println!(
        "P3\tROUTE\tFOUND\t{brokers}\t{}\t{}\t{}",
        counts.pending_requests, counts.connections, counts.owned_tasks
    );
    Ok(())
}

async fn p3_tls(
    address: &str,
    trust_cert_path: &str,
    mode: &str,
    expected_outcome: &str,
) -> Result<(), Box<dyn Error>> {
    let ca_pem = std::fs::read(trust_cert_path)?;
    let mut tls = TlsConfig::default();
    tls.ca_pem = Some(ca_pem);
    tls.test_mode = false;
    tls.auth_server = true;
    let remoting = RemotingClient::with_config(ClientConfig {
        transport: TransportMode::Tls(tls),
        connect_timeout: 5_000,
        ..ClientConfig::default()
    })?;
    remoting.start()?;
    let api = MQClientApi::new(remoting.clone());
    let mut heartbeat = HeartbeatData {
        client_id: Some(format!("compat-p3-rust-{mode}")),
        ..HeartbeatData::default()
    };
    heartbeat.set_producer_data_set_owned(Some(vec![Some(ProducerData {
        group_name: Some("compat_p3_tls".to_owned()),
    })]));
    let result = api
        .send_heartbeat(
            address,
            &heartbeat,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    let accepted = result.is_ok();
    remoting.shutdown().await;
    let counts = remoting_lifecycle_counts(&remoting)?;
    if counts.pending_requests != 0 || counts.connections != 0 || counts.owned_tasks != 0 {
        return Err(format!("TLS shutdown retained resources: {counts:?}").into());
    }
    let actual = if accepted { "accepted" } else { "rejected" };
    if actual != expected_outcome {
        return Err(format!("unexpected TLS outcome for {mode}: {actual}").into());
    }
    println!(
        "P3\tTLS\t{mode}\t{}\t{}\t{}\t{}",
        if accepted { "ACCEPTED" } else { "REJECTED" },
        counts.pending_requests,
        counts.connections,
        counts.owned_tasks,
    );
    Ok(())
}

async fn p16_auth(address: &str, mode: &str) -> Result<(), Box<dyn Error>> {
    let access_key = env::var("P16_ACCESS_KEY")?;
    let valid_secret = env::var("P16_SECRET_KEY")?;
    let secret = if mode == "wrong-secret" {
        "p16-deliberately-wrong-secret".to_owned()
    } else {
        valid_secret
    };
    let remoting = RemotingClient::with_config(ClientConfig {
        connect_timeout: 5_000,
        ..ClientConfig::default()
    })?;
    remoting.add_rpc_hook(Arc::new(AclClientRpcHook::new(SessionCredentials::new(
        access_key, secret,
    ))));
    remoting.start()?;
    let api = MQClientApi::new(remoting.clone());
    let mut heartbeat = HeartbeatData {
        client_id: Some(format!("compat-p16-rust-{mode}")),
        ..HeartbeatData::default()
    };
    heartbeat.set_producer_data_set_owned(Some(vec![Some(ProducerData {
        group_name: Some("compat_p16_security".to_owned()),
    })]));
    let result = api
        .send_heartbeat(
            address,
            &heartbeat,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    let accepted = result.is_ok();
    remoting.shutdown().await;
    let counts = remoting_lifecycle_counts(&remoting)?;
    if counts.pending_requests != 0 || counts.connections != 0 || counts.owned_tasks != 0 {
        return Err(format!("ACL shutdown retained resources: {counts:?}").into());
    }
    let expected = mode == "valid";
    if accepted != expected {
        return Err(format!("unexpected ACL outcome for {mode}: {accepted}").into());
    }
    println!(
        "P16\tACL\t{mode}\t{}\t{}\t{}\t{}",
        if accepted { "ACCEPTED" } else { "REJECTED" },
        counts.pending_requests,
        counts.connections,
        counts.owned_tasks,
    );
    Ok(())
}

async fn p16_tls(
    address: &str,
    mode: &str,
    trust_cert_path: &str,
    client_cert_path: &str,
    client_key_path: &str,
) -> Result<(), Box<dyn Error>> {
    let access_key = env::var("P16_ACCESS_KEY")?;
    let secret_key = env::var("P16_SECRET_KEY")?;
    let mut tls = TlsConfig::default();
    tls.ca_pem = Some(std::fs::read(trust_cert_path)?);
    tls.test_mode = false;
    tls.auth_server = true;
    if client_cert_path != "-" && client_key_path != "-" {
        tls.client_cert_pem = Some(std::fs::read(client_cert_path)?);
        tls.client_key_pem = Some(std::fs::read(client_key_path)?);
    }
    let remoting = RemotingClient::with_config(ClientConfig {
        transport: TransportMode::Tls(tls),
        connect_timeout: 5_000,
        ..ClientConfig::default()
    })?;
    remoting.add_rpc_hook(Arc::new(AclClientRpcHook::new(SessionCredentials::new(
        access_key, secret_key,
    ))));
    remoting.start()?;
    let api = MQClientApi::new(remoting.clone());
    let mut heartbeat = HeartbeatData {
        client_id: Some(format!("compat-p16-rust-tls-{mode}")),
        ..HeartbeatData::default()
    };
    heartbeat.set_producer_data_set_owned(Some(vec![Some(ProducerData {
        group_name: Some("compat_p16_security".to_owned()),
    })]));
    let result = api
        .send_heartbeat(
            address,
            &heartbeat,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    let accepted = result.is_ok();
    remoting.shutdown().await;
    let counts = remoting_lifecycle_counts(&remoting)?;
    if counts.pending_requests != 0 || counts.connections != 0 || counts.owned_tasks != 0 {
        return Err(format!("TLS shutdown retained resources: {counts:?}").into());
    }
    let expected = mode == "trusted" || mode == "wrong-host";
    if accepted != expected {
        return Err(format!("unexpected TLS outcome for {mode}: {accepted}").into());
    }
    println!(
        "P16\tTLS\t{mode}\t{}\t{}\t{}\t{}",
        if accepted { "ACCEPTED" } else { "REJECTED" },
        counts.pending_requests,
        counts.connections,
        counts.owned_tasks,
    );
    Ok(())
}

async fn p17_query(
    namesrv_address: &str,
    topic: &str,
    key: &str,
    begin_timestamp: i64,
    end_timestamp: i64,
) -> Result<(), Box<dyn Error>> {
    let mut config = ProducerConfig::new("compat_p17_admin_query_rust")?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    let producer = Arc::new(Producer::new(config));
    producer.start().await?;
    let result = producer
        .query_message(topic, key, 32, begin_timestamp, end_timestamp)
        .await;
    producer.shutdown().await?;
    let result = match result {
        Ok(result) => result,
        Err(ProducerError::AdminQuery(
            rocketmq_classic_core::api::admin::AdminQueryError::Rpc(RpcError::Response {
                request_code: request::QUERY_MESSAGE,
                code,
                ..
            }),
        )) => {
            println!("P17\tERROR\t{key}\t{code}");
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    let mut messages = result
        .get_message_list()
        .map(|messages| messages.snapshot())
        .unwrap_or_default()
        .into_iter()
        .flatten()
        .map(|message| message.snapshot())
        .collect::<Vec<_>>();
    let mut rows = messages
        .drain(..)
        .map(|message| {
            let body = String::from_utf8_lossy(message.body().as_ref()).into_owned();
            (
                message.store_timestamp(),
                message.broker_name().unwrap_or_default().to_owned(),
                message.queue_id(),
                body,
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    println!("P17\tRESULT\t{key}\t{}", rows.len());
    for (store_timestamp, broker_name, queue_id, body) in rows {
        println!("P17\tMESSAGE\t{key}\t{broker_name}\t{queue_id}\t{store_timestamp}\t{body}");
    }
    Ok(())
}

async fn rebalance(
    namesrv_address: &str,
    topic: &str,
    comma_separated_consumer_ids: &str,
) -> Result<(), Box<dyn Error>> {
    let mut config = ProducerConfig::new("p19RebalanceRust")?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.client_config_mut().set_mq_client_api_timeout(5_000);

    let producer = Arc::new(Producer::new(config));
    producer.start().await?;
    let result = producer.fetch_publish_message_queues(topic).await;
    let shutdown = producer.shutdown().await;
    shutdown?;
    let mut queues = result?;
    queues.sort_by_key(|queue| {
        (
            queue.topic().unwrap_or_default().to_owned(),
            queue.broker_name().unwrap_or_default().to_owned(),
            queue.queue_id(),
        )
    });

    let mut consumer_ids = comma_separated_consumer_ids
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if consumer_ids.iter().any(String::is_empty) {
        return Err("consumer ID list contains an empty value".into());
    }
    consumer_ids.sort();
    let strategy = AllocateMessageQueueAveragely;
    for consumer_id in &consumer_ids {
        let assigned = strategy.allocate(
            "compat_phase0_rebalance_group",
            consumer_id,
            &queues,
            &consumer_ids,
        )?;
        let queue_keys = assigned
            .iter()
            .map(|queue| {
                format!(
                    "{}|{}|{}",
                    queue.topic().unwrap_or_default(),
                    queue.broker_name().unwrap_or_default(),
                    queue.queue_id()
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        println!("ASSIGN\t{consumer_id}\t{queue_keys}");
    }
    Ok(())
}

struct FixedTransactionListener {
    state: LocalTransactionState,
    check_state: LocalTransactionState,
    check_count: Arc<AtomicUsize>,
}

struct P14EndTransactionProbe;

impl EndTransactionHook for P14EndTransactionProbe {
    fn hook_name(&self) -> &str {
        "p14-end-transaction-probe"
    }

    fn end_transaction(&self, context: &mut EndTransactionContext<'_>) {
        println!(
            "P14\tEND_HOOK\t{}\t{}\t{}\t{}\t{}",
            context.broker_addr,
            context.message_id,
            context.transaction_id.as_deref().unwrap_or("-"),
            context.transaction_state.java_name(),
            context.from_transaction_check
        );
    }
}

impl TransactionListener for FixedTransactionListener {
    fn execute_local_transaction(
        &self,
        _message: &mut Message,
        _argument: Option<&(dyn Any + Send + Sync)>,
    ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
        Ok(Some(self.state))
    }

    fn check_local_transaction(
        &self,
        _message: &mut MessageExt,
    ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
        self.check_count.fetch_add(1, Ordering::AcqRel);
        Ok(Some(self.check_state))
    }
}

async fn transaction(
    namesrv_address: &str,
    topic: &str,
    producer_group: &str,
    decision: &str,
    key: &str,
    body: &str,
) -> Result<(), Box<dyn Error>> {
    let state = match decision {
        "commit" => LocalTransactionState::Commit,
        "rollback" => LocalTransactionState::Rollback,
        _ => return Err("transaction decision must be commit or rollback".into()),
    };
    let mut config = ProducerConfig::new(producer_group)?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    let producer = TransactionProducer::new(config);
    producer.set_transaction_listener(Some(Arc::new(FixedTransactionListener {
        state,
        check_state: LocalTransactionState::Unknown,
        check_count: Arc::new(AtomicUsize::new(0)),
    })));
    producer.start().await?;

    let mut message = Message::new(topic, body.as_bytes().to_vec());
    message.set_keys(key.to_owned())?;
    let sent = producer
        .send_message_in_transaction(&mut message, None)
        .await;
    let shutdown = producer.shutdown().await;
    shutdown?;
    let result = sent?;
    println!(
        "TX\t{}\t{}\t{}\t{}",
        result.send_result.status.name(),
        result.local_transaction_state.java_name(),
        key,
        result.send_result.msg_id
    );
    Ok(())
}

async fn p14_transaction(
    namesrv_address: &str,
    topic: &str,
    producer_group: &str,
    decision: &str,
    key: &str,
    body: &str,
) -> Result<(), Box<dyn Error>> {
    let (state, check_state) = match decision {
        "commit" => (
            LocalTransactionState::Commit,
            LocalTransactionState::Unknown,
        ),
        "rollback" => (
            LocalTransactionState::Rollback,
            LocalTransactionState::Unknown,
        ),
        "unknown" => (
            LocalTransactionState::Unknown,
            LocalTransactionState::Commit,
        ),
        _ => return Err("P14 transaction decision must be commit, rollback, or unknown".into()),
    };
    let check_count = Arc::new(AtomicUsize::new(0));
    let mut config = ProducerConfig::new(producer_group)?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    let producer = TransactionProducer::new(config);
    producer.register_end_transaction_hook(Arc::new(P14EndTransactionProbe));
    producer.set_transaction_listener(Some(Arc::new(FixedTransactionListener {
        state,
        check_state,
        check_count: check_count.clone(),
    })));
    producer.start().await?;

    let mut message = Message::new(topic, body.as_bytes().to_vec());
    message.set_keys(key.to_owned())?;
    let sent_at = Instant::now();
    let sent = producer
        .send_message_in_transaction(&mut message, None)
        .await;
    let result = match sent {
        Ok(result) => result,
        Err(error) => {
            producer.shutdown().await?;
            return Err(error.into());
        }
    };

    let check_elapsed_ms = if decision == "unknown" {
        let checked = tokio::time::timeout(Duration::from_secs(25), async {
            while check_count.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .is_ok();
        let elapsed_ms = sent_at.elapsed().as_millis();
        if !checked {
            producer.shutdown().await?;
            return Err("Broker transaction check did not arrive within 25 seconds".into());
        }
        Some(elapsed_ms)
    } else {
        None
    };
    if decision == "unknown" {
        // The one-way end-transaction request is scheduled on the remoting client.
        // Keep that client alive briefly after the callback before shutdown.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    producer.shutdown().await?;
    println!(
        "P14\tTX\t{}\t{}\t{}\t{}\t{}\t{}",
        result.send_result.status.name(),
        result.local_transaction_state.java_name(),
        key,
        result.send_result.broker_name,
        result.send_result.queue_id,
        result.send_result.queue_offset
    );
    if let Some(elapsed_ms) = check_elapsed_ms {
        println!(
            "P14\tCHECK\tCOMMIT\t{}\t{elapsed_ms}",
            check_count.load(Ordering::Acquire)
        );
    }
    Ok(())
}

async fn heartbeat(broker_address: &str, producer_group: &str) -> Result<(), Box<dyn Error>> {
    let heartbeat = HeartbeatData {
        client_id: Some("p19-live-heartbeat".into()),
        ..HeartbeatData::default()
    };
    heartbeat
        .producer_data_set
        .as_ref()
        .expect("default heartbeat initializes producer set")
        .with_mut(|producers| {
            producers.insert(Some(SharedValue::from(ProducerData {
                group_name: Some(producer_group.to_owned()),
            })));
        });
    let body = heartbeat.java_encode()?;

    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let result = api
        .send_heartbeat(
            broker_address,
            &heartbeat,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    let version = result?;
    println!("HEARTBEAT\t{version}\t{}", encode_hex(&body));
    Ok(())
}

async fn send(broker_address: &str, topic: &str, body: &str) -> Result<(), Box<dyn Error>> {
    send_with_mode(broker_address, topic, body, SendMode::Sync).await
}

async fn async_send(broker_address: &str, topic: &str, body: &str) -> Result<(), Box<dyn Error>> {
    send_with_mode(broker_address, topic, body, SendMode::Future).await
}

async fn high_level_producer_modes(
    namesrv_address: &str,
    topic: &str,
) -> Result<(), Box<dyn Error>> {
    let mut config = ProducerConfig::new("compat_p7_rust")?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.set_send_timeout_millis(10_000);
    let producer = Arc::new(Producer::new(config));
    producer.start().await?;

    let result = async {
        let mut sync_message = Message::new(topic, b"p7-sync".to_vec());
        let sync = producer.send(&mut sync_message).await?;
        println!("P7\tSYNC\t{}\t{}", sync.status.name(), sync.queue_id);

        let mut async_message = Message::new(topic, b"p7-async".to_vec());
        let async_result = producer.send_future(&mut async_message).await?;
        println!(
            "P7\tASYNC\t1\t{}\t{}",
            async_result.status.name(),
            async_result.queue_id
        );

        let mut oneway_message = Message::new(topic, b"p7-oneway".to_vec());
        producer.send_oneway(&mut oneway_message).await?;
        println!("P7\tONEWAY\tNO_RESPONSE");

        let explicit_queue = MessageQueue::new(topic, "broker-a", 1);
        let mut explicit_message = Message::new(topic, b"p7-explicit".to_vec());
        let explicit = producer
            .send_to_queue(&mut explicit_message, &explicit_queue)
            .await?;
        println!(
            "P7\tEXPLICIT_QUEUE\t{}\t{}",
            explicit.status.name(),
            explicit.queue_id
        );

        let mut selector_message = Message::new(topic, b"p7-selector".to_vec());
        let selected = producer
            .send_with_selector(&mut selector_message, |queues, _| {
                queues.iter().find(|queue| queue.queue_id() == 2).cloned()
            })
            .await?;
        println!(
            "P7\tSELECTOR\t{}\t{}",
            selected.status.name(),
            selected.queue_id
        );

        let mut batch = vec![
            Message::new(topic, b"p7-batch-0".to_vec()),
            Message::new(topic, b"p7-batch-1".to_vec()),
        ];
        let valid_batch = producer.send_batch(&mut batch).await?;
        println!(
            "P7\tVALID_BATCH\t{}\t{}\t{}",
            valid_batch.status.name(),
            valid_batch.queue_id,
            batch.len()
        );

        let mut invalid_batch = vec![
            Message::new(topic, b"p7-invalid-batch-0".to_vec()),
            Message::new(format!("{topic}-different"), b"p7-invalid-batch-1".to_vec()),
        ];
        if producer.send_batch(&mut invalid_batch).await.is_ok() {
            return Err("batch with mixed topics was accepted".into());
        }
        println!("P7\tINVALID_BATCH\tREJECTED");
        Ok::<(), Box<dyn Error>>(())
    }
    .await;

    let shutdown = producer.shutdown().await;
    shutdown?;
    result
}

struct P7RetryAttemptHook {
    attempts: std::sync::Mutex<usize>,
}

impl SendMessageHook for P7RetryAttemptHook {
    fn hook_name(&self) -> &str {
        "p7-retry-attempt-recorder"
    }

    fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
        let attempt = {
            let mut attempts = self
                .attempts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *attempts += 1;
            *attempts
        };
        let broker = context.queue.broker_name().unwrap_or_default();
        let status = context
            .send_result
            .as_ref()
            .map_or("ERROR", |result| result.status.name());
        let body = String::from_utf8_lossy(context.message.body().as_ref()).replace('\t', " ");
        let unique_id = context
            .message
            .properties()
            .get("UNIQ_KEY")
            .cloned()
            .unwrap_or_default();
        println!("ATTEMPT\trust\t{attempt}\t{broker}\t{status}\t{body}\t{unique_id}");
        let error = context
            .exception
            .map(ToString::to_string)
            .unwrap_or_default()
            .replace(['\t', '\n'], " ");
        println!("ATTEMPT_ERROR\trust\t{attempt}\t{error}");
    }
}

async fn p7_retry_probe(
    namesrv_address: &str,
    topic: &str,
    mode: &str,
    body: &str,
    timeout_ms: i32,
    ready_file: &str,
    go_file: &str,
) -> Result<(), Box<dyn Error>> {
    let mut config = ProducerConfig::new("compat_p7_retry_rust")?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.set_send_timeout_millis(timeout_ms);
    config.set_retry_times_when_send_failed(2);
    let producer = Arc::new(Producer::new(config));
    producer.add_retry_response_code(rocketmq_classic_protocol::codes::response::NO_PERMISSION);
    producer.register_send_message_hook(Arc::new(P7RetryAttemptHook {
        attempts: std::sync::Mutex::new(0),
    }));
    producer.start().await?;

    let result = async {
        let queues = producer.fetch_publish_message_queues(topic).await?;
        let mut brokers = queues
            .iter()
            .filter_map(|queue| queue.broker_name().map(str::to_owned))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        brokers.sort();
        let has_both_brokers = brokers.iter().any(|broker| broker == "broker-a")
            && brokers.iter().any(|broker| broker == "broker-b");
        println!("ROUTE\trust\t{}", brokers.join(","));
        if !has_both_brokers {
            return Err("P7-V03 requires writable queues on both Brokers".into());
        }
        std::fs::write(ready_file, "ready\n")?;
        let gate_deadline = std::time::Instant::now() + Duration::from_secs(15);
        while !Path::new(go_file).exists() {
            if std::time::Instant::now() >= gate_deadline {
                return Err("timed out waiting for the P7-V03 send gate".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        let mut message = Message::new(topic, body.as_bytes().to_vec());
        message.set_keys(format!("compat_p7_{mode}_rust"))?;
        let started = Instant::now();
        let send = producer.send(&mut message).await;
        let elapsed = started.elapsed().as_millis();
        match send {
            Ok(result) => {
                println!(
                    "RESULT\trust\t{mode}\tSUCCESS\t{}\t{}\t{elapsed}",
                    result.status.name(),
                    result.broker_name
                );
                if mode == "retry" {
                    if result.status.name() == "SEND_OK" {
                        return Ok(());
                    }
                    return Err("retry probe did not recover with SEND_OK".into());
                }
                Err("deadline probe unexpectedly completed successfully".into())
            }
            Err(error) => {
                let description = error.to_string();
                let lower = description.to_lowercase();
                let outcome = if lower.contains("timeout") || lower.contains("deadline") {
                    "TIMEOUT"
                } else {
                    "BROKER_ERROR"
                };
                println!("RESULT\trust\t{mode}\t{outcome}\t{description}\t{elapsed}");
                match mode {
                    "retry"
                        if matches!(
                            &error,
                            ProducerError::SendRetriesExhausted {
                                response_code: 16,
                                ..
                            }
                        ) =>
                    {
                        Ok(())
                    }
                    "retry" => {
                        Err("retry probe did not classify final code as NO_PERMISSION".into())
                    }
                    "deadline" if outcome == "TIMEOUT" => Ok(()),
                    "deadline" => Err("deadline probe did not return a timeout".into()),
                    _ => Err(format!("unknown P7-V03 mode: {mode}").into()),
                }
            }
        }
    }
    .await;
    let shutdown = producer.shutdown().await;
    shutdown?;
    result
}

async fn producer_lifecycle_probe(
    namesrv_address: &str,
    topic: &str,
) -> Result<(), Box<dyn Error>> {
    const CYCLES: usize = 3;
    const SENDS_PER_CYCLE: usize = 20;
    let manager = ClientRuntimeManager::shared_default();

    for cycle in 0..CYCLES {
        let mut config = ProducerConfig::new(format!("compat_p7_lifecycle_rust_{cycle}"))?;
        config
            .client_config_mut()
            .set_namesrv_addr(Some(namesrv_address.to_owned()));
        let producer = Arc::new(Producer::new(config.clone()));
        producer.start().await?;
        let runtime = manager.get_or_create(config.client_config()).await?;

        for sequence in 0..SENDS_PER_CYCLE {
            let body = format!("p7-lifecycle-{cycle}-{sequence}");
            let mut message = Message::new(topic, body.into_bytes());
            let result = producer.send(&mut message).await?;
            if result.status.name() != "SEND_OK" {
                return Err(format!("lifecycle send returned {}", result.status.name()).into());
            }
        }

        producer.shutdown().await?;
        let deadline = Instant::now() + Duration::from_secs(5);
        let counts = loop {
            let counts = remoting_lifecycle_counts(runtime.remoting())?;
            let factories_empty = manager.factory_table().is_empty()?;
            if counts.pending_requests == 0
                && counts.connections == 0
                && counts.owned_tasks == 0
                && factories_empty
            {
                break counts;
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "producer lifecycle did not quiesce after shutdown: {counts:?}, factories_empty={factories_empty}"
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        println!(
            "P7L\tCYCLE\trust\t{cycle}\t{SENDS_PER_CYCLE}\t{}\t{}\t{}",
            counts.pending_requests, counts.connections, counts.owned_tasks
        );
    }

    Ok(())
}

async fn producer_stress_probe(
    namesrv_address: &str,
    topic: &str,
    count: usize,
    max_in_flight: usize,
    seed: u32,
    start_offset: i64,
) -> Result<(), Box<dyn Error>> {
    if count == 0 || max_in_flight == 0 || start_offset < 0 {
        return Err("stress inputs must be positive and bounded".into());
    }

    let mut payload_bytes = 0usize;
    let mut payload_hash = 0xcbf2_9ce4_8422_2325u64;
    let bodies = (0..count)
        .map(|sequence| p7_stress_body(seed, sequence))
        .inspect(|body| {
            payload_bytes += body.len();
            for byte in body {
                payload_hash ^= u64::from(*byte);
                payload_hash = payload_hash.wrapping_mul(0x100_0000_01b3);
            }
        })
        .collect::<Vec<_>>();

    let manager = ClientRuntimeManager::shared_default();
    let mut config = ProducerConfig::new("compat_p7_stress_rust")?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.set_send_timeout_millis(15_000);
    let producer = Arc::new(Producer::new(config.clone()));
    producer.start().await?;
    let runtime = manager.get_or_create(config.client_config()).await?;
    let queue = MessageQueue::new(topic, "broker-a", 0);
    let started = StdInstant::now();
    let mut offsets = Vec::with_capacity(count);

    let load_result: Result<(), Box<dyn Error>> = async {
        for first in (0..count).step_by(max_in_flight) {
            let end = (first + max_in_flight).min(count);
            let mut tasks = tokio::task::JoinSet::new();
            for (sequence, body) in bodies.iter().enumerate().take(end).skip(first) {
                let producer = Arc::clone(&producer);
                let queue = queue.clone();
                let topic = topic.to_owned();
                let body = body.clone();
                tasks.spawn(async move {
                    let mut message = Message::new(topic, body);
                    message
                        .set_keys(format!("compat_p7_stress_rust_{sequence}"))
                        .map_err(|error| error.to_string())?;
                    let result = producer
                        .send_to_queue(&mut message, &queue)
                        .await
                        .map_err(|error| error.to_string())?;
                    if result.status.name() != "SEND_OK"
                        || result.queue_id != 0
                        || result.broker_name != "broker-a"
                    {
                        return Err(format!(
                            "stress send returned unexpected Broker result: {:?}",
                            result
                        ));
                    }
                    Ok::<i64, String>(result.queue_offset)
                });
            }

            let mut failures = Vec::new();
            while let Some(result) = tasks.join_next().await {
                match result {
                    Ok(Ok(offset)) => offsets.push(offset),
                    Ok(Err(error)) => failures.push(error),
                    Err(error) => failures.push(error.to_string()),
                }
            }
            if !failures.is_empty() {
                return Err(failures.join("; ").into());
            }
        }
        Ok(())
    }
    .await;
    let elapsed_millis = started.elapsed().as_millis();
    let shutdown = producer.shutdown().await;
    load_result?;
    shutdown?;

    if elapsed_millis > 120_000 {
        return Err(format!(
            "bounded stress workload exceeded its 120-second deadline: {elapsed_millis}ms"
        )
        .into());
    }
    offsets.sort_unstable();
    let expected_offsets = (start_offset..start_offset + count as i64).collect::<Vec<_>>();
    if offsets != expected_offsets {
        return Err(format!(
            "stress queue offsets were not contiguous: expected {}..{}, got {}..{} across {} results",
            start_offset,
            start_offset + count as i64 - 1,
            offsets.first().copied().unwrap_or(-1),
            offsets.last().copied().unwrap_or(-1),
            offsets.len()
        )
        .into());
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let counts = loop {
        let counts = remoting_lifecycle_counts(runtime.remoting())?;
        let factories_empty = manager.factory_table().is_empty()?;
        if counts.pending_requests == 0
            && counts.connections == 0
            && counts.owned_tasks == 0
            && factories_empty
        {
            break counts;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "stress producer did not quiesce after shutdown: {counts:?}, factories_empty={factories_empty}"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    println!(
        "P7S\tLOAD\trust\t{count}\t{max_in_flight}\t{payload_bytes}\t{payload_hash:016x}\t{elapsed_millis}\t{}\t{}\t{}\t{}\t{}",
        offsets[0],
        offsets[count - 1],
        counts.pending_requests,
        counts.connections,
        counts.owned_tasks
    );
    Ok(())
}

fn p7_stress_body(seed: u32, sequence: usize) -> Vec<u8> {
    let mut state = seed ^ (sequence as u32 + 1).wrapping_mul(0x9e37_79b9);
    state = p7_xorshift(state);
    let length = 256 + (state & 0x3ff) as usize;
    (0..length)
        .map(|_| {
            state = p7_xorshift(state);
            state as u8
        })
        .collect()
}

fn p7_xorshift(mut state: u32) -> u32 {
    state ^= state << 13;
    state ^= state >> 17;
    state ^ (state << 5)
}

async fn shared_runtime_probe(
    namesrv_address: &str,
    topic: &str,
    producer_group_a: &str,
    producer_group_b: &str,
    consumer_group: &str,
) -> Result<(), Box<dyn Error>> {
    let mut producer_config_a = ProducerConfig::new(producer_group_a)?;
    producer_config_a
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    producer_config_a.set_send_timeout_millis(10_000);
    let producer_a = Arc::new(Producer::new(producer_config_a));

    let mut producer_config_b = ProducerConfig::new(producer_group_b)?;
    producer_config_b
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    producer_config_b.set_send_timeout_millis(10_000);
    let producer_b = Arc::new(Producer::new(producer_config_b));

    let mut consumer_config = PushConsumerConfig::new(consumer_group)?;
    consumer_config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    let consumer = Arc::new(PushConsumer::new(consumer_config));
    let (observed_tx, mut observed_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    consumer
        .set_message_listener(PushMessageListener::concurrently(
            move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let observed_tx = observed_tx.clone();
                async move {
                    for message in messages {
                        let body = String::from_utf8_lossy(message.body().as_ref()).into_owned();
                        let _ = observed_tx.send(body);
                    }
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        ))
        .await?;
    consumer.subscribe(topic, "*").await?;

    let mut producer_a_started = false;
    let mut producer_b_started = false;
    let mut consumer_started = false;
    let mut producer_a_stopped = false;
    let result: Result<(), Box<dyn Error>> = async {
        producer_a.start().await?;
        producer_a_started = true;
        producer_b.start().await?;
        producer_b_started = true;
        consumer.start().await?;
        consumer_started = true;

        send_tagged(&producer_a, topic, "DROP", "p6-before-change").await?;
        expect_consumed(&mut observed_rx, "p6-before-change").await?;

        consumer.subscribe(topic, "KEEP").await?;
        println!("P6\tSUBSCRIPTION\t*->KEEP");
        send_tagged(&producer_a, topic, "DROP", "p6-filtered-after-change").await?;
        send_tagged(&producer_a, topic, "KEEP", "p6-keep-after-change").await?;
        expect_consumed(&mut observed_rx, "p6-keep-after-change").await?;

        producer_a.shutdown().await?;
        producer_a_stopped = true;
        println!("P6\tUNREGISTER\t{producer_group_a}");
        send_tagged(&producer_b, topic, "KEEP", "p6-survivor-after-unregister").await?;
        println!("P6\tSURVIVOR\tSEND_OK");
        expect_consumed(&mut observed_rx, "p6-survivor-after-unregister").await?;

        match tokio::time::timeout(Duration::from_millis(1_500), observed_rx.recv()).await {
            Err(_) => {}
            Ok(Some(unexpected)) => {
                return Err(std::io::Error::other(format!(
                    "unexpected post-change delivery: {unexpected}"
                ))
                .into());
            }
            Ok(None) => return Err("push-consumer observation channel closed".into()),
        }
        Ok(())
    }
    .await;

    let mut cleanup_failures = Vec::new();
    if consumer_started {
        if let Err(error) = consumer.shutdown().await {
            cleanup_failures.push(format!("consumer shutdown: {error}"));
        }
    }
    if producer_b_started {
        if let Err(error) = producer_b.shutdown().await {
            cleanup_failures.push(format!("producer B shutdown: {error}"));
        }
    }
    if producer_a_started && !producer_a_stopped {
        if let Err(error) = producer_a.shutdown().await {
            cleanup_failures.push(format!("producer A shutdown: {error}"));
        }
    }

    result?;
    if !cleanup_failures.is_empty() {
        return Err(cleanup_failures.join("; ").into());
    }
    Ok(())
}

async fn pull_consumer_probe(
    namesrv_address: &str,
    topic: &str,
    consumer_group: &str,
) -> Result<(), Box<dyn Error>> {
    let consumer = new_pull_probe_consumer(namesrv_address, consumer_group).await?;
    let session: Result<MessageQueue, Box<dyn Error>> = async {
        let queues = consumer.fetch_message_queues(topic).await?;
        if queues.len() != 1 {
            return Err(std::io::Error::other(format!(
                "expected one pull queue, got {}",
                queues.len()
            ))
            .into());
        }
        let queue = queues.into_iter().next().expect("checked one queue exists");
        if queue.queue_id() != 0 || queue.broker_name() != Some("broker-a") {
            return Err(std::io::Error::other(format!("unexpected pull queue: {queue:?}")).into());
        }

        let found = consumer.pull(&queue, Some("MATCH"), 0, 1).await?;
        assert_pull_status(&found.result, "FOUND")?;
        print_pull_result(&found.result);
        let messages = found
            .messages
            .as_ref()
            .ok_or_else(|| std::io::Error::other("FOUND pull omitted decoded messages"))?;
        let body = messages
            .first()
            .map(|message| String::from_utf8_lossy(message.body().as_ref()).into_owned())
            .ok_or_else(|| std::io::Error::other("FOUND pull returned no decoded message"))?;
        if body != "p9-seed-match" {
            return Err(std::io::Error::other(format!(
                "manual pull returned unexpected body: {body}"
            ))
            .into());
        }
        println!("P9\tBODY\t{body}");

        let unmatched = consumer
            .pull_with_selector(&queue, &MessageSelector::by_tag("MISSING"), 0, 1)
            .await?;
        assert_pull_status(&unmatched.result, "NO_MATCHED_MSG")?;
        print_pull_result(&unmatched.result);
        let empty = consumer.pull(&queue, Some("*"), 2, 1).await?;
        assert_pull_status(&empty.result, "NO_NEW_MSG")?;
        print_pull_result(&empty.result);
        let invalid = consumer.pull(&queue, Some("*"), 3, 1).await?;
        assert_pull_status(&invalid.result, "OFFSET_ILLEGAL")?;
        print_pull_result(&invalid.result);

        let min_offset = consumer.get_min_offset(&queue).await?;
        let max_offset = consumer.get_max_offset(&queue).await?;
        let timestamp_search = consumer.search_offset(&queue, 0).await?;
        println!("P9\tBOUNDARY\t{min_offset}\t{max_offset}\t{timestamp_search}");
        if min_offset != 0 || max_offset != 2 || timestamp_search != 0 {
            return Err(std::io::Error::other(
                "pull offset boundary RPCs returned unexpected values",
            )
            .into());
        }

        consumer.update_consume_offset(&queue, 1).await?;
        consumer
            .update_consume_offset_to_broker(&queue, 1, false)
            .await?;
        Ok(queue)
    }
    .await;
    let shutdown = consumer.shutdown().await;
    let queue = session?;
    shutdown?;

    let restarted = new_pull_probe_consumer(namesrv_address, consumer_group).await?;
    let resume = restarted.fetch_consume_offset(&queue, true).await;
    let shutdown = restarted.shutdown().await;
    let resume = resume?;
    shutdown?;
    if resume != 1 {
        return Err(
            std::io::Error::other(format!("pull restart expected offset 1, got {resume}")).into(),
        );
    }
    println!("P9\tRESUME\t{resume}");
    Ok(())
}

async fn request_reply_request(
    namesrv_address: &str,
    topic: &str,
    producer_group: &str,
    body: &str,
) -> Result<(), Box<dyn Error>> {
    let mut config = ProducerConfig::new(producer_group)?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.set_topics(Some(vec![topic.to_owned()].into()));
    config.set_send_timeout_millis(10_000);
    let producer = Arc::new(Producer::new(config));
    producer.set_send_message_with_vip_channel(false);
    producer.start().await?;

    let result: Result<(), Box<dyn Error>> = async {
        let mut request = Message::new(topic, body.as_bytes().to_vec());
        let reply = producer
            .request(&mut request, Duration::from_secs(15))
            .await?;
        print_request_reply_message("REQUEST", &request);
        print_request_reply_message("REPLY", &reply);
        if request.get_property("CORRELATION_ID") != reply.get_property("CORRELATION_ID") {
            return Err("reply correlation ID differs from request".into());
        }
        Ok(())
    }
    .await;

    let shutdown = producer.shutdown().await;
    result?;
    shutdown?;
    Ok(())
}

async fn request_reply_timeout(
    namesrv_address: &str,
    topic: &str,
    producer_group: &str,
    body: &str,
    timeout_ms: u64,
    hold_ms: u64,
) -> Result<(), Box<dyn Error>> {
    let mut config = ProducerConfig::new(producer_group)?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.set_send_timeout_millis(10_000);
    let producer = Arc::new(Producer::new(config));
    producer.set_send_message_with_vip_channel(false);
    producer.start().await?;

    let result: Result<(), Box<dyn Error>> = async {
        let started = Instant::now();
        let mut request = Message::new(topic, body.as_bytes().to_vec());
        let result = producer
            .request(&mut request, Duration::from_millis(timeout_ms))
            .await;
        match result {
            Err(ProducerError::RequestReplyTimeout { .. }) => {}
            Err(error) => return Err(format!("expected request timeout, got {error}").into()),
            Ok(reply) => {
                return Err(format!(
                    "expected request timeout, received reply on {}",
                    reply.topic()
                )
                .into());
            }
        }
        let correlation_id = request
            .get_property("CORRELATION_ID")
            .ok_or("timed-out request has no correlation ID")?;
        tokio::time::sleep(Duration::from_millis(hold_ms)).await;
        let pending = RequestFutureHolder::get_instance()
            .get_request_future_table()
            .contains_key(&correlation_id)?;
        if pending {
            return Err("timed-out request remained in the correlation table".into());
        }
        println!(
            "REQ\tTIMEOUT\t{topic}\t{correlation_id}\t{}\tfalse\t{hold_ms}",
            started.elapsed().as_millis()
        );
        Ok(())
    }
    .await;
    let shutdown = producer.shutdown().await;
    result?;
    shutdown?;
    Ok(())
}

async fn request_reply_late_timeout(
    namesrv_address: &str,
    topic: &str,
    producer_group: &str,
    body: &str,
    timeout_ms: u64,
    late_wait_ms: u64,
) -> Result<(), Box<dyn Error>> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let mut config = ProducerConfig::new(producer_group)?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.set_send_timeout_millis(10_000);
    let producer = Arc::new(Producer::new(config));
    producer.set_send_message_with_vip_channel(false);
    producer.start().await?;

    let result: Result<(), Box<dyn Error>> = async {
        let mut request = Message::new(topic, body.as_bytes().to_vec());
        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_for_task = callback_count.clone();
        let (callback_tx, mut callback_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        producer
            .request_with_callback(
                &mut request,
                Box::new(move |result| {
                    callback_count_for_task.fetch_add(1, Ordering::AcqRel);
                    let outcome = match result {
                        Ok(_) => "SUCCESS".to_owned(),
                        Err(ProducerError::RequestReplyTimeout { .. }) => "TIMEOUT".to_owned(),
                        Err(error) => format!("ERROR:{error}"),
                    };
                    let _ = callback_tx.send(outcome);
                }),
                Duration::from_millis(timeout_ms),
            )
            .await?;
        let correlation_id = request
            .get_property("CORRELATION_ID")
            .ok_or("late-reply request has no correlation ID")?;
        let outcome = tokio::time::timeout(
            Duration::from_millis(timeout_ms.saturating_add(3_000)),
            callback_rx.recv(),
        )
        .await?
        .ok_or_else(|| std::io::Error::other("request timeout callback channel closed"))?;
        if outcome != "TIMEOUT" {
            return Err(format!("expected timeout callback, got {outcome}").into());
        }
        tokio::time::sleep(Duration::from_millis(late_wait_ms)).await;
        let callbacks = callback_count.load(Ordering::Acquire);
        let pending = RequestFutureHolder::get_instance()
            .get_request_future_table()
            .contains_key(&correlation_id)?;
        if callbacks != 1 || pending {
            return Err(format!(
                "late reply changed terminal state: callbacks={callbacks}, pending={pending}"
            )
            .into());
        }
        println!(
            "REQ\tLATE_TIMEOUT\t{topic}\t{correlation_id}\t{callbacks}\tfalse\t{late_wait_ms}\t{outcome}"
        );
        Ok(())
    }
    .await;
    let shutdown = producer.shutdown().await;
    result?;
    shutdown?;
    Ok(())
}

async fn request_reply_respond(
    namesrv_address: &str,
    topic: &str,
    consumer_group: &str,
    producer_group: &str,
    reply_body: &str,
    delay_ms: u64,
) -> Result<(), Box<dyn Error>> {
    use std::sync::atomic::{AtomicBool, Ordering};

    let mut producer_config = ProducerConfig::new(producer_group)?;
    producer_config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    producer_config.set_send_timeout_millis(10_000);
    let producer = Arc::new(Producer::new(producer_config));
    producer.set_send_message_with_vip_channel(false);

    let mut consumer_config = PushConsumerConfig::new(consumer_group)?;
    consumer_config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    let consumer = Arc::new(PushConsumer::new(consumer_config));
    let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let replied = Arc::new(AtomicBool::new(false));
    let listener_producer = producer.clone();
    let listener_replied = replied.clone();
    let listener_body = reply_body.as_bytes().to_vec();
    let listener_delay = Duration::from_millis(delay_ms);
    consumer
        .set_message_listener(PushMessageListener::concurrently(
            move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let producer = listener_producer.clone();
                let result_tx = result_tx.clone();
                let replied = listener_replied.clone();
                let reply_body = listener_body.clone();
                let delay = listener_delay;
                async move {
                    let mut status = ConsumeConcurrentlyStatus::ConsumeSuccess;
                    for request in messages {
                        if replied.swap(true, Ordering::AcqRel) {
                            continue;
                        }
                        let request_body = encode_hex(request.body().as_ref());
                        let request_correlation =
                            request.get_property("CORRELATION_ID").unwrap_or_default();
                        let request_reply_to =
                            request.get_property("REPLY_TO_CLIENT").unwrap_or_default();
                        println!(
                            "REQ\tRECEIVED\t{}\t{}\t{}\t{}",
                            request.topic(),
                            request_body,
                            request_correlation,
                            request_reply_to
                        );
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                        let outcome = match create_reply_message(Some(&request), reply_body.clone())
                        {
                            Ok(mut reply) => match producer.send(&mut reply).await {
                                Ok(sent) if sent.status.name() == "SEND_OK" => {
                                    format!(
                                        "SEND_OK\t{}\t{}\t{}\t{}",
                                        reply.topic(),
                                        request_body,
                                        request_correlation,
                                        request_reply_to
                                    )
                                }
                                Ok(sent) => {
                                    status = ConsumeConcurrentlyStatus::ReconsumeLater;
                                    format!("ERROR\tBroker send status {}", sent.status)
                                }
                                Err(error) => {
                                    status = ConsumeConcurrentlyStatus::ReconsumeLater;
                                    format!("ERROR\t{error}")
                                }
                            },
                            Err(error) => {
                                status = ConsumeConcurrentlyStatus::ReconsumeLater;
                                format!("ERROR\t{error}")
                            }
                        };
                        let _ = result_tx.send(outcome);
                    }
                    (Some(status), context)
                }
            },
        ))
        .await?;
    consumer.subscribe(topic, "*").await?;

    let mut producer_started = false;
    let mut consumer_started = false;
    let result: Result<(), Box<dyn Error>> = async {
        producer.start().await?;
        producer_started = true;
        consumer.start().await?;
        consumer_started = true;
        println!("REQ\tREADY");
        let outcome = tokio::time::timeout(Duration::from_secs(30), result_rx.recv())
            .await?
            .ok_or_else(|| std::io::Error::other("request observation channel closed"))?;
        if outcome.starts_with("ERROR\t") {
            return Err(outcome.into());
        }
        if !outcome.starts_with("SEND_OK\t") {
            return Err(format!("unexpected responder outcome: {outcome}").into());
        }
        println!("REQ\tRESPONDER\t{outcome}");
        Ok(())
    }
    .await;

    let mut cleanup_failures = Vec::new();
    if consumer_started {
        if let Err(error) = consumer.shutdown().await {
            cleanup_failures.push(format!("consumer shutdown: {error}"));
        }
    }
    if producer_started {
        if let Err(error) = producer.shutdown().await {
            cleanup_failures.push(format!("producer shutdown: {error}"));
        }
    }
    result?;
    if !cleanup_failures.is_empty() {
        return Err(cleanup_failures.join("; ").into());
    }
    Ok(())
}

fn print_request_reply_message(kind: &str, message: &Message) {
    let property = |name: &str| message.get_property(name).unwrap_or_else(|| "-".to_owned());
    println!(
        "REQ\t{kind}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        message.topic(),
        encode_hex(message.body().as_ref()),
        property("CORRELATION_ID"),
        property("REPLY_TO_CLIENT"),
        property("TTL"),
        property("CLUSTER"),
        property("MSG_TYPE"),
    );
}

async fn new_pull_probe_consumer(
    namesrv_address: &str,
    consumer_group: &str,
) -> Result<Arc<PullConsumer>, Box<dyn Error>> {
    let mut config = PullConsumerConfig::new(consumer_group)?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.set_enable_rebalance(false);
    let consumer = Arc::new(PullConsumer::new(config));
    consumer.start().await?;
    Ok(consumer)
}

fn assert_pull_status(
    result: &rocketmq_classic_core::api::consumer::PullResultExt,
    expected: &str,
) -> Result<(), Box<dyn Error>> {
    if result.status.name() != expected {
        return Err(std::io::Error::other(format!(
            "expected pull status {expected} but got {}",
            result.status.name()
        ))
        .into());
    }
    Ok(())
}

fn print_pull_result(result: &rocketmq_classic_core::api::consumer::PullResultExt) {
    println!(
        "P9\tPULL\t{}\t{}\t{}\t{}\t{}",
        result.status.name(),
        result.next_begin_offset,
        result.min_offset,
        result.max_offset,
        result.suggest_which_broker_id
    );
}

async fn send_tagged(
    producer: &Producer,
    topic: &str,
    tag: &str,
    body: &str,
) -> Result<(), Box<dyn Error>> {
    let mut message = Message::with_tags(
        Some(topic.to_owned()),
        Some(tag.to_owned()),
        Some(body.as_bytes().to_vec()),
    );
    let result = producer.send(&mut message).await?;
    if result.status.name() != "SEND_OK" {
        return Err(std::io::Error::other(format!(
            "send of {body} returned {}",
            result.status.name()
        ))
        .into());
    }
    Ok(())
}

async fn expect_consumed(
    received: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
    expected: &str,
) -> Result<(), Box<dyn Error>> {
    let actual = tokio::time::timeout(Duration::from_secs(15), received.recv())
        .await?
        .ok_or_else(|| std::io::Error::other("push-consumer observation channel closed"))?;
    if actual != expected {
        return Err(std::io::Error::other(format!(
            "expected consumed body {expected} but received {actual}"
        ))
        .into());
    }
    println!("P6\tRECEIVED\t{actual}");
    Ok(())
}

async fn oneway_send(
    broker_address: &str,
    topic: &str,
    body: &str,
    key: &str,
    minimum_max_offset: i64,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let result =
        oneway_send_and_query(&api, broker_address, topic, body, key, minimum_max_offset).await;
    remoting.shutdown().await;
    result
}

async fn oneway_send_and_query(
    api: &MQClientApi,
    broker_address: &str,
    topic: &str,
    body: &str,
    key: &str,
    minimum_max_offset: i64,
) -> Result<(), Box<dyn Error>> {
    let mut message = Message::new(topic, body.as_bytes().to_vec());
    message.set_keys(key.to_owned())?;
    message.set_system_property("UNIQ_KEY", format!("{key}_uniq"))?;
    let born_timestamp = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let header = {
        let mut __phase18_header_0 = SendMessageRequestHeaderV1::default();
        __phase18_header_0.set_producer_group(Some("compat_phase0".into()));
        __phase18_header_0.set_topic(topic.to_owned());
        __phase18_header_0.set_default_topic("TBW102".into());
        __phase18_header_0.set_default_topic_queue_nums(4);
        __phase18_header_0.set_queue_id(0);
        __phase18_header_0.set_sys_flag(0);
        __phase18_header_0.set_born_timestamp(born_timestamp);
        __phase18_header_0.set_flag(0);
        __phase18_header_0.set_properties(Some(encode_properties(&message.properties())));
        __phase18_header_0.set_reconsume_times(Some(0));
        __phase18_header_0.set_unit_mode(Some(false));
        __phase18_header_0.set_batch(Some(false));
        __phase18_header_0.set_broker_name(Some("broker-a".into()));
        __phase18_header_0
    };
    let result = api
        .send_message(
            broker_address,
            "broker-a",
            &header,
            body.as_bytes().to_vec(),
            "p19-live-rust-oneway-message",
            SendKind::V2,
            SendMode::Oneway,
            Instant::now() + Duration::from_secs(10),
        )
        .await?;
    if result.is_some() {
        return Err("oneway send unexpectedly returned a Broker result".into());
    }
    println!("ONEWAY_RETURN\tNO_RESPONSE\t{topic}\t{key}");

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let mut header = GetMaxOffsetRequestHeader::default();
        header.set_topic(Some(topic.to_owned()));
        header.set_queue_id(Some(0));
        header.set_broker_name(Some("broker-a".into()));
        let max_offset = api
            .get_max_offset(
                broker_address,
                &header,
                Instant::now() + Duration::from_secs(5),
            )
            .await?;
        if max_offset >= minimum_max_offset {
            println!("QUEUE_MAX_OFFSET\t{max_offset}");
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("oneway message did not reach the Broker queue before the deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn batch_send(
    broker_address: &str,
    topic: &str,
    body_prefix: &str,
    message_count: usize,
) -> Result<(), Box<dyn Error>> {
    if message_count == 0 {
        return Err("batch message count must be positive".into());
    }
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let ids = UniqueIdGenerator::new(None);
    let mut messages = Vec::with_capacity(message_count);
    for index in 0..message_count {
        let mut message = Message::new(topic, format!("{body_prefix}-{index}").into_bytes());
        message.set_keys(format!("compat_phase0_batch_rust_{index}"))?;
        ids.set_if_missing(&mut message);
        messages.push(message);
    }
    let mut batch_message = Message::new(topic, Vec::new());
    let batch_id = ids.set_if_missing(&mut batch_message);
    let body = encode_compact_messages(&messages)?;
    let header = {
        let mut __phase18_header_1 = SendMessageRequestHeaderV1::default();
        __phase18_header_1.set_producer_group(Some("compat_phase0".into()));
        __phase18_header_1.set_topic(topic.to_owned());
        __phase18_header_1.set_default_topic("TBW102".into());
        __phase18_header_1.set_default_topic_queue_nums(4);
        __phase18_header_1.set_queue_id(0);
        __phase18_header_1.set_sys_flag(0);
        __phase18_header_1.set_born_timestamp(i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis(),
        )?);
        __phase18_header_1.set_flag(0);
        __phase18_header_1.set_properties(Some(encode_properties(&batch_message.properties())));
        __phase18_header_1.set_reconsume_times(Some(0));
        __phase18_header_1.set_unit_mode(Some(false));
        __phase18_header_1.set_batch(Some(true));
        __phase18_header_1.set_broker_name(Some("broker-a".into()));
        __phase18_header_1
    };
    let result = api
        .send_message(
            broker_address,
            "broker-a",
            &header,
            body,
            &batch_id,
            SendKind::Batch,
            SendMode::Sync,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    let result = result?.ok_or("batch send returned no Broker result")?;
    let offset_msg_id = result
        .offset_msg_id
        .as_deref()
        .ok_or("batch send returned no offset message ID")?;
    let first_offset_message_id = offset_msg_id
        .split(',')
        .next()
        .filter(|message_id| !message_id.is_empty())
        .ok_or("batch send returned no offset message ID")?;
    let physical_offset = decode_message_id(first_offset_message_id)?.offset();
    println!(
        "BATCH_SEND\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        result.status,
        result.topic,
        result.queue_id,
        result.queue_offset,
        offset_msg_id,
        first_offset_message_id,
        physical_offset,
        message_count
    );
    Ok(())
}

async fn send_with_mode(
    broker_address: &str,
    topic: &str,
    body: &str,
    mode: SendMode,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let message = Message::new(topic, body.as_bytes().to_vec());
    let born_timestamp = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let header = {
        let mut __phase18_header_2 = SendMessageRequestHeaderV1::default();
        __phase18_header_2.set_producer_group(Some("compat_phase0".into()));
        __phase18_header_2.set_topic(topic.to_owned());
        __phase18_header_2.set_default_topic("TBW102".into());
        __phase18_header_2.set_default_topic_queue_nums(4);
        __phase18_header_2.set_queue_id(0);
        __phase18_header_2.set_sys_flag(0);
        __phase18_header_2.set_born_timestamp(born_timestamp);
        __phase18_header_2.set_flag(0);
        __phase18_header_2.set_properties(Some(encode_properties(&message.properties())));
        __phase18_header_2.set_reconsume_times(Some(0));
        __phase18_header_2.set_unit_mode(Some(false));
        __phase18_header_2.set_batch(Some(false));
        __phase18_header_2.set_broker_name(Some("broker-a".into()));
        __phase18_header_2
    };
    let result = api
        .send_message(
            broker_address,
            "broker-a",
            &header,
            body.as_bytes().to_vec(),
            "p19-live-rust-message",
            SendKind::V2,
            mode,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    let result = result?.ok_or("synchronous send returned no result")?;
    let offset_msg_id = result
        .offset_msg_id
        .as_deref()
        .ok_or("synchronous send returned no offset message ID")?;
    let physical_offset = decode_message_id(offset_msg_id)?.offset();
    if mode == SendMode::Future {
        println!(
            "ASYNC_FUTURE\t1\t{}\t{}\t{}\t{}\t{}\t{}",
            result.status,
            result.topic,
            result.queue_id,
            result.queue_offset,
            offset_msg_id,
            physical_offset
        );
    } else {
        println!(
            "SEND\t{}\t{}\t{}\t{}\t{}\t{}",
            result.status,
            result.topic,
            result.queue_id,
            result.queue_offset,
            offset_msg_id,
            physical_offset
        );
    }
    Ok(())
}

async fn p5_producer(
    broker_address: &str,
    topic: &str,
    producer_group: &str,
    minimum_base_offset: i64,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let operation = async {
        let mut max_header = GetMaxOffsetRequestHeader::default();
        max_header.set_topic(Some(topic.to_owned()));
        max_header.set_queue_id(Some(0));
        max_header.set_broker_name(Some("broker-a".into()));
        let base_deadline = Instant::now() + Duration::from_secs(15);
        let mut observed_base_offset = -1;
        let base_offset = loop {
            let now = Instant::now();
            if now >= base_deadline {
                return Err(format!(
                    "Broker queue max offset did not reach Java's committed offset: expected at least {minimum_base_offset}, observed {observed_base_offset}"
                )
                .into());
            }
            observed_base_offset = api
                .get_max_offset(
                    broker_address,
                    &max_header,
                    std::cmp::min(base_deadline, now + Duration::from_secs(5)),
                )
                .await?;
            if observed_base_offset >= minimum_base_offset {
                break observed_base_offset;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        println!("P5\tBASE\t{base_offset}");

        let ids = UniqueIdGenerator::new(None);
        for (method, kind, reply, batch) in [
            ("send-v1", SendKind::V1, false, false),
            ("send-v2", SendKind::V2, false, false),
            ("batch", SendKind::Batch, false, true),
        ] {
            let mut message = Message::new(topic, format!("p5-rust-{method}").into_bytes());
            message.set_keys(format!("compat_p5_rust_{method}"))?;
            if reply {
                message.set_system_property("MSG_TYPE", "reply")?;
                message.set_system_property(
                    "CORRELATION_ID",
                    format!("compat-p5-correlation-{method}"),
                )?;
                message.set_system_property("REPLY_TO_CLIENT", "compat-p5-client")?;
                message.set_system_property("TTL", "60000")?;
            }
            let message_id = ids.set_if_missing(&mut message);

            let (body, request_id, header_message) = if batch {
                let encoded = encode_compact_messages(std::slice::from_ref(&message))?;
                let mut envelope = Message::new(topic, Vec::new());
                let batch_id = ids.set_if_missing(&mut envelope);
                (encoded, batch_id, envelope)
            } else {
                let body = message.body().to_vec();
                (body, message_id, message)
            };
            let born_timestamp = i64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis(),
            )?;
            let mut header = SendMessageRequestHeaderV1::default();
            header.set_producer_group(Some(producer_group.to_owned()));
            header.set_topic(topic.to_owned());
            header.set_default_topic("TBW102".into());
            header.set_default_topic_queue_nums(4);
            header.set_queue_id(0);
            header.set_sys_flag(0);
            header.set_born_timestamp(born_timestamp);
            header.set_flag(header_message.flag());
            header.set_properties(Some(encode_properties(&header_message.properties())));
            header.set_reconsume_times(Some(0));
            header.set_unit_mode(Some(false));
            header.set_batch(Some(batch));
            header.set_broker_name(Some("broker-a".into()));
            let result = api
                .send_message(
                    broker_address,
                    "broker-a",
                    &header,
                    body,
                    &request_id,
                    kind,
                    SendMode::Sync,
                    Instant::now() + Duration::from_secs(10),
                )
                .await?
                .ok_or("synchronous producer RPC returned no result")?;
            if result.offset_msg_id.as_deref().is_none_or(str::is_empty) {
                return Err(format!("{method} returned an incomplete send result").into());
            }
            println!(
                "P5\tRESULT\t{method}\t{}\t{}\t{}\t{}",
                result.status.name(),
                result.topic,
                result.queue_id,
                result.queue_offset
            );
        }

        let heartbeat = HeartbeatData {
            client_id: Some("compat-p5-client".into()),
            ..HeartbeatData::default()
        };
        heartbeat
            .producer_data_set
            .as_ref()
            .expect("default heartbeat initializes producer set")
            .with_mut(|producers| {
                producers.insert(Some(SharedValue::from(ProducerData {
                    group_name: Some(producer_group.to_owned()),
                })));
            });
        let consumer = ConsumerData {
            group_name: Some("compat_p5_reply_group".into()),
            consume_type: Some(ConsumeType::ConsumePassively),
            message_model: Some(MessageModel::Clustering),
            consume_from_where: Some(ConsumeFromWhere::ConsumeFromFirstOffset),
            ..ConsumerData::default()
        };
        heartbeat
            .consumer_data_set
            .as_ref()
            .expect("default heartbeat initializes consumer set")
            .with_mut(|consumers| {
                consumers.insert(Some(SharedValue::from(consumer)));
            });
        api.send_heartbeat(
            broker_address,
            &heartbeat,
            Instant::now() + Duration::from_secs(10),
        )
        .await?;

        for (method, kind) in [
            ("reply-v1", SendKind::ReplyV1),
            ("reply-v2", SendKind::ReplyV2),
        ] {
            let mut message = Message::new(topic, format!("p5-rust-{method}").into_bytes());
            message.set_keys(format!("compat_p5_rust_{method}"))?;
            message.set_system_property("MSG_TYPE", "reply")?;
            message
                .set_system_property("CORRELATION_ID", format!("compat-p5-correlation-{method}"))?;
            message.set_system_property("REPLY_TO_CLIENT", "compat-p5-client")?;
            message.set_system_property("TTL", "60000")?;
            let message_id = ids.set_if_missing(&mut message);
            let born_timestamp = i64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis(),
            )?;
            let mut header = SendMessageRequestHeaderV1::default();
            header.set_producer_group(Some(producer_group.to_owned()));
            header.set_topic(topic.to_owned());
            header.set_default_topic("TBW102".into());
            header.set_default_topic_queue_nums(4);
            header.set_queue_id(0);
            header.set_sys_flag(0);
            header.set_born_timestamp(born_timestamp);
            header.set_flag(message.flag());
            header.set_properties(Some(encode_properties(&message.properties())));
            header.set_reconsume_times(Some(0));
            header.set_unit_mode(Some(false));
            header.set_batch(Some(false));
            header.set_broker_name(Some("broker-a".into()));
            let result = api
                .send_message(
                    broker_address,
                    "broker-a",
                    &header,
                    message.body().to_vec(),
                    &message_id,
                    kind,
                    SendMode::Oneway,
                    Instant::now() + Duration::from_secs(10),
                )
                .await?;
            if result.is_some() {
                return Err(
                    format!("{method} one-way reply unexpectedly returned a result").into(),
                );
            }
            println!("P5\tRESULT\t{method}\tONEWAY");
        }

        let mut end = EndTransactionRequestHeader::default();
        end.set_topic(Some(topic.to_owned()));
        end.set_producer_group(Some(producer_group.to_owned()));
        end.set_tran_state_table_offset(Some(-1));
        end.set_commit_log_offset(Some(-1));
        end.set_commit_or_rollback(Some(8));
        end.set_from_transaction_check(Some(false));
        end.set_msg_id(Some("compat-p5-end-transaction".into()));
        end.set_transaction_id(Some("compat-p5-transaction".into()));
        api.end_transaction_oneway(
            broker_address,
            &end,
            Some("compat-p5-end".into()),
            Instant::now() + Duration::from_secs(10),
        )
        .await?;
        println!("P5\tRESULT\tend-transaction\tONEWAY");

        let mut final_header = GetMaxOffsetRequestHeader::default();
        final_header.set_topic(Some(topic.to_owned()));
        final_header.set_queue_id(Some(0));
        final_header.set_broker_name(Some("broker-a".into()));
        let final_offset = api
            .get_max_offset(
                broker_address,
                &final_header,
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
        println!("P5\tMAX\t{final_offset}");
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    remoting.shutdown().await;
    operation
}

async fn p5_consumer(
    broker_address: &str,
    group: &str,
    topic: &str,
    seed_body: &str,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let operation = async {
        let client_id = "compat-p5-rust-consumer";
        let heartbeat = HeartbeatData {
            client_id: Some(client_id.to_owned()),
            ..HeartbeatData::default()
        };
        let mut subscription = SubscriptionData::new(Some(topic.to_owned()), Some("*".to_owned()));
        subscription.set_expression_type(Some("TAG".to_owned()));
        let consumer = ConsumerData {
            group_name: Some(group.to_owned()),
            consume_type: Some(ConsumeType::ConsumeActively),
            message_model: Some(MessageModel::Clustering),
            consume_from_where: Some(ConsumeFromWhere::ConsumeFromFirstOffset),
            subscription_data_set: Some(SharedValue::from(HashSet::from([Some(
                SharedValue::from(subscription),
            )]))),
            ..ConsumerData::default()
        };
        heartbeat
            .consumer_data_set
            .as_ref()
            .expect("default heartbeat initializes consumer set")
            .with_mut(|consumers| {
                consumers.insert(Some(SharedValue::from(consumer)));
            });
        api.send_heartbeat(
            broker_address,
            &heartbeat,
            Instant::now() + Duration::from_secs(10),
        )
        .await?;

        let ordinary = p5_pull(&api, broker_address, group, topic, false).await?;
        if ordinary.status.name() != "FOUND" {
            return Err(format!("ordinary pull returned {}", ordinary.status.name()).into());
        }
        let messages = decode_many_as_client_compatible(
            ordinary
                .raw_messages
                .as_deref()
                .ok_or("ordinary pull returned no message bytes")?,
            DecodeOptions::default(),
        );
        if !messages
            .iter()
            .any(|message| message.body().as_ref() == seed_body.as_bytes())
        {
            return Err("ordinary pull did not return this client's seed message".into());
        }
        println!(
            "P5C\tPULL\t{}\t{}\t{}\t{}",
            ordinary.status.name(),
            messages.len(),
            ordinary.min_offset,
            ordinary.max_offset
        );

        let lite = p5_pull(&api, broker_address, group, topic, true).await?;
        if lite.status.name() != "FOUND" {
            return Err(format!("lite pull returned {}", lite.status.name()).into());
        }
        let lite_messages = decode_many_as_client_compatible(
            lite.raw_messages
                .as_deref()
                .ok_or("lite pull returned no message bytes")?,
            DecodeOptions::default(),
        );
        if lite_messages.is_empty() {
            return Err("lite pull returned no decoded messages".into());
        }
        println!(
            "P5C\tLITE_PULL\t{}\t{}\t{}\t{}",
            lite.status.name(),
            lite_messages.len(),
            lite.min_offset,
            lite.max_offset
        );

        let mut update = UpdateConsumerOffsetRequestHeader::default();
        update.set_consumer_group(Some(group.to_owned()));
        update.set_topic(Some(topic.to_owned()));
        update.set_queue_id(Some(0));
        update.set_commit_offset(Some(1));
        update.set_broker_name(Some("broker-a".to_owned()));
        api.update_consumer_offset(
            broker_address,
            &update,
            false,
            Instant::now() + Duration::from_secs(10),
        )
        .await?;
        println!("P5C\tUPDATE_OFFSET\t1");

        let mut query = QueryConsumerOffsetRequestHeader::default();
        query.set_consumer_group(Some(group.to_owned()));
        query.set_topic(Some(topic.to_owned()));
        query.set_queue_id(Some(0));
        let queried = api
            .query_consumer_offset(
                broker_address,
                &query,
                Instant::now() + Duration::from_secs(10),
            )
            .await?
            .ok_or("query offset returned no stored offset")?;
        if queried != 1 {
            return Err(format!("query offset returned {queried} after update").into());
        }
        println!("P5C\tQUERY_OFFSET\t{queried}");

        let consumer_ids = api
            .get_consumer_list_by_group(
                broker_address,
                group,
                Instant::now() + Duration::from_secs(10),
            )
            .await?
            .ok_or("consumer-list returned no ID list")?;
        if !consumer_ids
            .iter()
            .any(|id| id.as_deref() == Some(client_id))
        {
            return Err("consumer-list omitted this client's registered identity".into());
        }
        println!("P5C\tCONSUMER_LIST\tPRESENT");

        let selected = messages
            .iter()
            .find(|message| message.body().as_ref() == seed_body.as_bytes())
            .ok_or("seed message disappeared from the decoded pull result")?;
        let mut send_back = ConsumerSendMsgBackRequestHeader::default();
        send_back.set_group(group.to_owned());
        send_back.set_broker_name(Some("broker-a".to_owned()));
        send_back.set_origin_topic(topic.to_owned());
        send_back.set_offset(selected.commit_log_offset());
        send_back.set_delay_level(3);
        send_back.set_origin_msg_id(selected.message_id().map(str::to_owned));
        send_back.set_max_reconsume_times(Some(16));
        send_back.set_unit_mode(false);
        api.consumer_send_message_back(
            broker_address,
            &send_back,
            Instant::now() + Duration::from_secs(10),
        )
        .await?;
        println!("P5C\tSEND_BACK\tOK");

        let queue = MessageQueue::new(topic, "broker-a", 0);
        let mut lock = LockBatchRequestBody::default();
        lock.set_consumer_group(Some(group.to_owned()));
        lock.set_client_id(Some(client_id.to_owned()));
        lock.set_only_this_broker(true);
        lock.set_mq_set(Some(SharedValue::from(HashSet::from([Some(
            SharedValue::from(queue.clone()),
        )]))));
        let locked = api
            .lock_batch_mq(
                broker_address,
                &lock,
                Instant::now() + Duration::from_secs(10),
            )
            .await?
            .is_some_and(|queues| queues.contains(&Some(queue.clone())));
        if !locked {
            return Err("lock response omitted the requested queue".into());
        }
        println!("P5C\tLOCK\ttrue");

        let mut unlock = UnlockBatchRequestBody::default();
        unlock.set_consumer_group(Some(group.to_owned()));
        unlock.set_client_id(Some(client_id.to_owned()));
        unlock.set_only_this_broker(true);
        unlock.set_mq_set(Some(SharedValue::from(HashSet::from([Some(
            SharedValue::from(queue),
        )]))));
        api.unlock_batch_mq(
            broker_address,
            &unlock,
            false,
            Instant::now() + Duration::from_secs(10),
        )
        .await?;
        println!("P5C\tUNLOCK\tOK");
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    remoting.shutdown().await;
    operation
}

async fn p13_pop_prepare(
    broker_address: &str,
    topic: &str,
    language: &str,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    remoting.start()?;
    let api = MQClientApi::new(remoting.clone());
    let operation = async {
        for suffix in ["expiry", "change", "mixed"] {
            let group = format!("compat_p13_pop_{language}_{suffix}");
            let mut mode = SetMessageRequestModeRequestBody::default();
            mode.set_topic(Some(topic.to_owned()));
            mode.set_consumer_group(Some(group.clone()));
            mode.set_mode(Some(MessageRequestMode::Pop));
            mode.set_pop_share_queue_num(1);
            api.set_message_request_mode(
                broker_address,
                &mode,
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
            println!("P13\tprepare\t{group}\tPOP");
        }
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    remoting.shutdown().await;
    operation
}

async fn p13_pop_consume(
    broker_address: &str,
    topic: &str,
    language: &str,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    remoting.start()?;
    let api = MQClientApi::new(remoting.clone());
    let operation = async {
        let prefix = format!("compat_p13_pop_{language}");
        let queue_zero_body = "p13-java-q0";
        let queue_one_body = "p13-java-q1";

        let (_first, first_receipt) = p13_pop_one(
            &api,
            broker_address,
            topic,
            &format!("{prefix}_expiry"),
            0,
            queue_zero_body,
            1_000,
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(6_000)).await;
        let (redelivered, redelivery_receipt) = p13_pop_one(
            &api,
            broker_address,
            topic,
            &format!("{prefix}_expiry"),
            0,
            queue_zero_body,
            10_000,
        )
        .await?;
        if first_receipt.raw() == redelivery_receipt.raw() {
            return Err("expiry redelivery reused the expired POP receipt".into());
        }
        let ack = api
            .ack_message(
                broker_address,
                &redelivery_receipt.ack_header(topic, &format!("{prefix}_expiry")),
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
        if ack.name() != "OK" {
            return Err(format!("expiry redelivery ACK returned {}", ack.name()).into());
        }
        println!("P13\texpiry-redelivery\tOK\t{}", redelivered.queue_id());

        let (_changed_source, old_receipt) = p13_pop_one(
            &api,
            broker_address,
            topic,
            &format!("{prefix}_change"),
            0,
            queue_zero_body,
            60_000,
        )
        .await?;
        let change_header =
            old_receipt.change_invisible_header(topic, &format!("{prefix}_change"), 120_000);
        let changed = api
            .change_invisible_time(
                broker_address,
                old_receipt.broker_name(),
                &change_header,
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
        if changed.status.name() != "OK" {
            return Err(format!("change-invisibility returned {}", changed.status.name()).into());
        }
        let replacement_raw = changed
            .extra_info
            .ok_or("change-invisibility omitted the replacement receipt")?;
        if replacement_raw == old_receipt.raw() {
            return Err("change-invisibility reused the old receipt".into());
        }
        let replacement = PopReceipt::parse(replacement_raw)?;
        let ack = api
            .ack_message(
                broker_address,
                &replacement.ack_header(topic, &format!("{prefix}_change")),
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
        if ack.name() != "OK" {
            return Err(format!("replacement receipt ACK returned {}", ack.name()).into());
        }
        println!("P13\tchange-invisibility\tOK");
        println!("P13\treplacement-receipt\tOK\tACKED");

        let (queue_zero, receipt_zero) = p13_pop_one(
            &api,
            broker_address,
            topic,
            &format!("{prefix}_mixed"),
            0,
            queue_zero_body,
            60_000,
        )
        .await?;
        let (queue_one, receipt_one) = p13_pop_one(
            &api,
            broker_address,
            topic,
            &format!("{prefix}_mixed"),
            1,
            queue_one_body,
            60_000,
        )
        .await?;
        if queue_zero.queue_id() != 0
            || queue_one.queue_id() != 1
            || receipt_zero.queue_id() != 0
            || receipt_one.queue_id() != 1
        {
            return Err("POP message and receipt queue mapping did not preserve queue 0/1".into());
        }
        for receipt in [&receipt_zero, &receipt_one] {
            let ack = api
                .ack_message(
                    broker_address,
                    &receipt.ack_header(topic, &format!("{prefix}_mixed")),
                    Instant::now() + Duration::from_secs(10),
                )
                .await?;
            if ack.name() != "OK" {
                return Err(format!("mixed queue ACK returned {}", ack.name()).into());
            }
        }
        println!("P13\tmixed-queue-mapping\tOK\t0,1");

        if PopReceipt::parse("17 123 456 2 1 broker-a 4 nope").is_ok() {
            return Err("Rust accepted malformed POP extra-info".into());
        }
        println!("P13\tmalformed-extra-info-rejected\tOK");
        println!("P13\tpop-delivery\tOK\t5");
        println!("P13\tack\tOK\t4");
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    remoting.shutdown().await;
    operation
}

async fn p13_pop_one(
    api: &MQClientApi,
    broker_address: &str,
    topic: &str,
    group: &str,
    queue_id: i32,
    expected_body: &str,
    invisible_time: i64,
) -> Result<(MessageExt, PopReceipt), Box<dyn Error>> {
    let born_time = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let mut header = PopMessageRequestHeader::default();
    header.set_consumer_group(Some(group.to_owned()));
    header.set_topic(Some(topic.to_owned()));
    header.set_queue_id(queue_id);
    header.set_init_mode(0);
    header.set_max_msg_nums(1);
    header.set_invisible_time(invisible_time);
    header.set_poll_time(0);
    header.set_born_time(born_time);
    header.set_exp_type(Some("TAG".to_owned()));
    header.set_exp(Some("*".to_owned()));
    let reply = api
        .pop_message(
            broker_address,
            &header,
            Instant::now() + Duration::from_secs(15),
        )
        .await?;
    if reply.status.name() != "FOUND" {
        return Err(format!("POP returned {} for {expected_body}", reply.status.name()).into());
    }
    let response_header = reply
        .header
        .ok_or("successful POP omitted response header")?;
    let raw = reply
        .raw_messages
        .ok_or("successful POP omitted message bytes")?;
    let mut messages = decode_many_as_client_compatible(&raw, DecodeOptions::default());
    if messages.len() != 1 || messages[0].body().as_ref() != expected_body.as_bytes() {
        return Err(format!("POP returned an unexpected message for {expected_body}").into());
    }
    let mut message = messages.remove(0);
    let raw_receipt = if let Some(existing) = message.properties().get("POP_CK") {
        existing.clone()
    } else {
        let start_offsets = p13_pop_offset_info(
            response_header.get_start_offset_info(),
            queue_id,
            "startOffsetInfo",
        )?;
        let message_offsets = p13_pop_offset_info(
            response_header.get_msg_offset_info(),
            queue_id,
            "msgOffsetInfo",
        )?;
        let checkpoint_offset = *start_offsets
            .first()
            .ok_or("POP startOffsetInfo omitted checkpoint offset")?;
        let message_offset = message_offsets
            .into_iter()
            .find(|offset| *offset == message.queue_offset())
            .ok_or("POP msgOffsetInfo omitted the returned queue offset")?;
        let raw = format!(
            "{checkpoint_offset} {} {} {} 0 broker-a {queue_id} {message_offset}",
            response_header.get_pop_time(),
            response_header.get_invisible_time(),
            response_header.get_revive_qid(),
        );
        message.set_system_property("POP_CK", raw.clone())?;
        raw
    };
    let receipt = PopReceipt::parse(raw_receipt)?;
    if message.queue_id() != queue_id || receipt.queue_id() != queue_id {
        return Err(format!(
            "POP queue {queue_id} mapped to message/receipt {}/{}",
            message.queue_id(),
            receipt.queue_id()
        )
        .into());
    }
    eprintln!(
        "P13\tpop-found\tgroup={group}\tqueue={queue_id}\tmessageOffset={}",
        message.queue_offset()
    );
    Ok((message, receipt))
}

fn p13_pop_offset_info(
    info: Option<&str>,
    queue_id: i32,
    field_name: &str,
) -> Result<Vec<i64>, Box<dyn Error>> {
    let info = info.ok_or_else(|| format!("POP response omitted {field_name}"))?;
    for entry in info.split(';') {
        let fields = entry.split_whitespace().collect::<Vec<_>>();
        if fields.len() != 3 {
            return Err(format!("malformed POP {field_name} entry {entry:?}").into());
        }
        let entry_queue_id = fields[1].parse::<i32>()?;
        if fields[0] != "0" || entry_queue_id != queue_id {
            continue;
        }
        return fields[2]
            .split(',')
            .map(|offset| offset.parse::<i64>().map_err(Into::into))
            .collect();
    }
    Err(format!("POP {field_name} omitted retry 0 queue {queue_id}").into())
}

async fn p5_extension_lite_pop(
    broker_address: &str,
    topic: &str,
    group_prefix: &str,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let client_id = format!("compat-p5-rust-{group_prefix}");
    let lite_group = format!("{group_prefix}_lite");
    let operation = async {
        let lite =
            p5_extension_pop(&api, broker_address, topic, &lite_group, &client_id, true).await?;
        require_pop_body(&lite.0, "p5-extension-lite", "POP-LITE")?;
        println!(
            "P5E\tpop-lite\tFOUND\t{}",
            encode_hex(lite.0[0].body().as_ref())
        );
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    remoting.shutdown().await;
    operation
}

async fn p5_extensions(
    broker_address: &str,
    topic: &str,
    group_prefix: &str,
    prepare_only: bool,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let client_id = format!("compat-p5-rust-{group_prefix}");
    let base_group = format!("{group_prefix}_base");
    let lite_group = format!("{group_prefix}_lite");
    let operation = async {
        if prepare_only {
            let heartbeat = HeartbeatData {
                client_id: Some(client_id.clone()),
                ..HeartbeatData::default()
            };
            let mut subscription =
                SubscriptionData::new(Some(topic.to_owned()), Some("*".to_owned()));
            subscription.set_expression_type(Some("TAG".to_owned()));
            let consumer = ConsumerData {
                group_name: Some(base_group.clone()),
                consume_type: Some(ConsumeType::ConsumeActively),
                message_model: Some(MessageModel::Clustering),
                consume_from_where: Some(ConsumeFromWhere::ConsumeFromFirstOffset),
                subscription_data_set: Some(SharedValue::from(HashSet::from([Some(
                    SharedValue::from(subscription),
                )]))),
                ..ConsumerData::default()
            };
            heartbeat
                .consumer_data_set
                .as_ref()
                .expect("default heartbeat initializes consumer set")
                .with_mut(|consumers| {
                    consumers.insert(Some(SharedValue::from(consumer)));
                });

            let heartbeat_version = api
                .send_heartbeat(
                    broker_address,
                    &heartbeat,
                    Instant::now() + Duration::from_secs(10),
                )
                .await?;
            println!("P5E\theartbeat\tOK\t{heartbeat_version}");
            let heartbeat_v2 = api
                .send_heartbeat_v2(
                    broker_address,
                    &heartbeat,
                    Instant::now() + Duration::from_secs(10),
                )
                .await?;
            println!(
                "P5E\theartbeat-v2\tOK\t{}\t{}\t{}",
                heartbeat_v2.version,
                heartbeat_v2.is_sub_change,
                heartbeat_v2.is_support_heartbeat_v2
            );

            let mut assignment_request = QueryAssignmentRequestBody::default();
            assignment_request.set_topic(Some(topic.to_owned()));
            assignment_request.set_consumer_group(Some(base_group.clone()));
            assignment_request.set_client_id(Some(client_id.clone()));
            assignment_request.set_strategy_name(Some("AVG".to_owned()));
            assignment_request.set_message_model(Some(MessageModel::Clustering));
            let assignments = api
                .query_assignment(
                    broker_address,
                    &assignment_request,
                    Instant::now() + Duration::from_secs(10),
                )
                .await?;
            let assignment_count = assignments
                .get_message_queue_assignments()
                .map_or(0, |items| items.snapshot().len());
            println!("P5E\tassignment\tOK\t{assignment_count}");

            let mut request_mode = SetMessageRequestModeRequestBody::default();
            request_mode.set_topic(Some(topic.to_owned()));
            request_mode.set_consumer_group(Some(base_group.clone()));
            request_mode.set_mode(Some(MessageRequestMode::Pop));
            request_mode.set_pop_share_queue_num(1);
            api.set_message_request_mode(
                broker_address,
                &request_mode,
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
            println!("P5E\trequest-mode\tOK");

            let lite_topic = "entries";
            let lite_subscription = LiteSubscriptionDto {
                action: Some(LiteSubscriptionAction::CompleteAdd),
                client_id: Some(client_id.clone()),
                group: Some(lite_group.clone()),
                topic: Some(topic.to_owned()),
                lite_topic_set: Some(SharedValue::from(std::collections::BTreeSet::from([Some(
                    lite_topic.to_owned(),
                )]))),
                version: 0,
                ..LiteSubscriptionDto::default()
            };
            let body = LiteSubscriptionCtlRequestBody {
                subscription_set: Some(SharedValue::from(LiteSubscriptionDtoSet::from([Some(
                    SharedValue::from(lite_subscription),
                )]))),
            };
            let subscription_result = api
                .lite_subscription_ctl(
                    broker_address,
                    &body,
                    Instant::now() + Duration::from_secs(10),
                )
                .await;
            match subscription_result {
                Ok(()) => println!("P5E\tlite-subscription\tOK"),
                Err(RpcError::Response {
                    request_code,
                    code,
                    remark,
                    ..
                }) if request_code == request::LITE_SUBSCRIPTION_CTL
                    && code == response::REQUEST_CODE_NOT_SUPPORTED
                    && remark.as_deref().is_some_and(|remark| {
                        remark.trim() == "request type 200071 not supported"
                    }) =>
                {
                    println!("P5E\tlite-subscription\tUNSUPPORTED\t200071\t3");
                }
                Err(error) => return Err(error.into()),
            }

            let mut unregister = UnregisterClientRequestHeader::default();
            unregister.set_client_id(client_id);
            unregister.set_producer_group(None);
            unregister.set_consumer_group(Some(base_group));
            api.unregister_client(
                broker_address,
                &unregister,
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
            println!("P5E\tunregister\tOK");
        } else {
            let lite_topic = format!("%LMQ%{topic}%entries");
            let pop_group = format!("{group_prefix}_pop");
            let pop = p5_extension_pop(&api, broker_address, topic, &pop_group, &client_id, false)
                .await?;
            require_pop_body(&pop.0, "p5-extension-seed", "POP")?;
            println!("P5E\tpop\tFOUND\t{}", encode_hex(pop.0[0].body().as_ref()));

            match p5_extension_pop(
                &api,
                broker_address,
                &lite_topic,
                &lite_group,
                &client_id,
                true,
            )
            .await
            {
                Ok(lite) => {
                    require_pop_body(&lite.0, "p5-extension-lite", "POP-LITE")?;
                    println!(
                        "P5E\tpop-lite\tFOUND\t{}",
                        encode_hex(lite.0[0].body().as_ref())
                    );
                }
                Err(error)
                    if is_unsupported_extension_request(
                        error.as_ref(),
                        request::POP_LITE_MESSAGE,
                    ) =>
                {
                    println!("P5E\tpop-lite\tUNSUPPORTED\t200070\t3");
                }
                Err(error) => return Err(error),
            }

            let ack_group = format!("{group_prefix}_ack");
            let acked =
                p5_extension_pop(&api, broker_address, topic, &ack_group, &client_id, false)
                    .await?;
            require_pop_body(&acked.0, "p5-extension-seed", "ACK POP")?;
            let ack_receipt = pop_receipt(&acked.0[0])?;
            let ack = api
                .ack_message(
                    broker_address,
                    &ack_receipt.ack_header(topic, &ack_group),
                    Instant::now() + Duration::from_secs(10),
                )
                .await?;
            println!("P5E\tack\t{}", ack.name());

            let batch_group = format!("{group_prefix}_batch_ack");
            let batch =
                p5_extension_pop(&api, broker_address, topic, &batch_group, &client_id, false)
                    .await?;
            require_pop_body(&batch.0, "p5-extension-seed", "BATCH ACK POP")?;
            let batch_receipt = pop_receipt(&batch.0[0])?.raw().to_owned();
            let batch_status = rocketmq_classic_consumer::pop::batch_ack_message_from_extra_info(
                &api,
                broker_address,
                Some(topic),
                Some(&batch_group),
                Some(&[Some(batch_receipt)]),
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
            println!("P5E\tbatch-ack\t{}", batch_status.name());

            let change_group = format!("{group_prefix}_change");
            let changed = p5_extension_pop(
                &api,
                broker_address,
                topic,
                &change_group,
                &client_id,
                false,
            )
            .await?;
            require_pop_body(&changed.0, "p5-extension-seed", "CHANGE INVISIBLE POP")?;
            let receipt = pop_receipt(&changed.0[0])?;
            let result = api
                .change_invisible_time(
                    broker_address,
                    receipt.broker_name(),
                    &receipt.change_invisible_header(topic, &change_group, 120_000),
                    Instant::now() + Duration::from_secs(10),
                )
                .await?;
            if result.pop_time.is_none()
                || result.invisible_time.is_none()
                || result.revive_qid.is_none()
            {
                return Err("change-invisible-time omitted successful response metadata".into());
            }
            println!(
                "P5E\tchange-invisible-time\t{}\tMETADATA",
                result.status.name()
            );
        }
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    remoting.shutdown().await;
    operation
}

fn is_unsupported_extension_request(error: &(dyn Error + 'static), request_code: i32) -> bool {
    matches!(
        error.downcast_ref::<RpcError>(),
        Some(RpcError::Response {
            request_code: response_request_code,
            code,
            remark,
            ..
        }) if *response_request_code == request_code
            && *code == response::REQUEST_CODE_NOT_SUPPORTED
            && remark.as_deref().is_some_and(|remark| {
                remark.trim() == format!("request type {request_code} not supported")
            })
    )
}

async fn p5_extension_pop(
    api: &MQClientApi,
    broker_address: &str,
    topic: &str,
    group: &str,
    client_id: &str,
    lite: bool,
) -> Result<(Vec<MessageExt>,), Box<dyn Error>> {
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut pop_header = None;
    let raw = if lite {
        let mut header = PopLiteMessageRequestHeader::default();
        header.set_client_id(Some(client_id.to_owned()));
        header.set_consumer_group(Some(group.to_owned()));
        header.set_topic(Some(topic.to_owned()));
        header.set_max_msg_num(1);
        header.set_invisible_time(60_000);
        header.set_poll_time(10_000);
        header.set_born_time(now);
        let reply = api
            .pop_lite_message(broker_address, &header, deadline)
            .await?;
        if reply.status.name() != "FOUND" {
            return Err(format!("lite POP returned {}", reply.status.name()).into());
        }
        reply.raw_messages
    } else {
        let mut header = PopMessageRequestHeader::default();
        header.set_consumer_group(Some(group.to_owned()));
        header.set_topic(Some(topic.to_owned()));
        header.set_queue_id(0);
        header.set_max_msg_nums(1);
        header.set_invisible_time(60_000);
        header.set_poll_time(0);
        header.set_born_time(now);
        header.set_exp_type(Some("TAG".to_owned()));
        header.set_exp(Some("*".to_owned()));
        let reply = api.pop_message(broker_address, &header, deadline).await?;
        if reply.status.name() != "FOUND" {
            return Err(format!("POP returned {}", reply.status.name()).into());
        }
        pop_header = reply.header;
        reply.raw_messages
    }
    .ok_or("successful POP omitted message bytes")?;
    let mut messages = decode_many_as_client_compatible(&raw, DecodeOptions::default());
    if messages.is_empty() {
        return Err("successful POP returned no decoded message".into());
    }
    if !lite {
        let response_header = pop_header.ok_or("successful POP omitted response header")?;
        let message = &mut messages[0];
        if message.properties().get("POP_CK").is_none() {
            let start_offsets = p13_pop_offset_info(
                response_header.get_start_offset_info(),
                0,
                "startOffsetInfo",
            )?;
            let message_offsets =
                p13_pop_offset_info(response_header.get_msg_offset_info(), 0, "msgOffsetInfo")?;
            let checkpoint_offset = *start_offsets
                .first()
                .ok_or("POP startOffsetInfo omitted checkpoint offset")?;
            let message_offset = message_offsets
                .into_iter()
                .find(|offset| *offset == message.queue_offset())
                .ok_or("POP msgOffsetInfo omitted the returned queue offset")?;
            let raw_receipt = format!(
                "{checkpoint_offset} {} {} {} 0 broker-a 0 {message_offset}",
                response_header.get_pop_time(),
                response_header.get_invisible_time(),
                response_header.get_revive_qid(),
            );
            message.set_system_property("POP_CK", raw_receipt)?;
        }
    }
    Ok((messages,))
}

fn require_pop_body(
    messages: &[MessageExt],
    expected: &str,
    operation: &str,
) -> Result<(), Box<dyn Error>> {
    if messages
        .iter()
        .any(|message| message.body().as_ref() == expected.as_bytes())
    {
        Ok(())
    } else {
        Err(format!("{operation} returned messages without expected body {expected:?}").into())
    }
}

fn pop_receipt(message: &MessageExt) -> Result<PopReceipt, Box<dyn Error>> {
    let properties = message.properties();
    let raw = properties
        .get("POP_CK")
        .ok_or("POP message omitted POP_CK receipt")?;
    Ok(PopReceipt::parse(raw.clone())?)
}

async fn p5_pull(
    api: &MQClientApi,
    broker_address: &str,
    group: &str,
    topic: &str,
    lite: bool,
) -> Result<rocketmq_classic_core::api::consumer::PullResultExt, Box<dyn Error>> {
    let mut header = PullMessageRequestHeader::default();
    header.set_consumer_group(Some(group.to_owned()));
    header.set_topic(Some(topic.to_owned()));
    header.set_queue_id(Some(0));
    header.set_queue_offset(Some(0));
    header.set_max_msg_nums(Some(32));
    header.set_sys_flag(Some(build_pull_sys_flag_with_lite(
        false, false, true, false, lite,
    )));
    header.set_commit_offset(Some(0));
    header.set_suspend_timeout_millis(Some(0));
    header.set_subscription(Some("*".to_owned()));
    header.set_sub_version(Some(0));
    header.set_expression_type(Some("TAG".to_owned()));
    header.set_broker_name(Some("broker-a".to_owned()));
    Ok(api
        .pull_message(
            broker_address,
            &header,
            Instant::now() + Duration::from_secs(10),
        )
        .await?)
}

async fn view(
    broker_address: &str,
    topic: &str,
    offset_message_id: &str,
) -> Result<(), Box<dyn Error>> {
    let physical_offset = decode_message_id(offset_message_id)?.offset();
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let result = api
        .view_message(
            broker_address,
            &{
                let mut __phase18_header_0 = ViewMessageRequestHeader::default();
                __phase18_header_0.set_topic(Some(topic.to_owned()));
                __phase18_header_0.set_offset(Some(physical_offset));
                __phase18_header_0
            },
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    let message = result?;
    println!(
        "MESSAGE\t{}\t{}\t{}\t{}",
        message.topic(),
        message.queue_id(),
        message.queue_offset(),
        encode_hex(&message.body())
    );
    Ok(())
}

async fn view_batch(
    broker_address: &str,
    topic: &str,
    offset_message_ids: &str,
) -> Result<(), Box<dyn Error>> {
    let physical_offsets = decode_batch_offsets(offset_message_ids)?;
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let result = async {
        for physical_offset in physical_offsets {
            let message = api
                .view_message(
                    broker_address,
                    &{
                        let mut __phase18_header_1 = ViewMessageRequestHeader::default();
                        __phase18_header_1.set_topic(Some(topic.to_owned()));
                        __phase18_header_1.set_offset(Some(physical_offset));
                        __phase18_header_1
                    },
                    Instant::now() + Duration::from_secs(10),
                )
                .await?;
            println!(
                "MESSAGE\t{}\t{}\t{}\t{}",
                message.topic(),
                message.queue_id(),
                message.queue_offset(),
                encode_hex(&message.body())
            );
        }
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    remoting.shutdown().await;
    result
}

async fn offset_query(
    broker_address: &str,
    group: &str,
    topic: &str,
    queue_id: i32,
) -> Result<(), Box<dyn Error>> {
    offset_query_named(broker_address, "broker-a", group, topic, queue_id).await
}

async fn offset_query_named(
    broker_address: &str,
    broker_name: &str,
    group: &str,
    topic: &str,
    queue_id: i32,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let mut header = QueryConsumerOffsetRequestHeader::default();
    header.set_consumer_group(Some(group.to_owned()));
    header.set_topic(Some(topic.to_owned()));
    header.set_queue_id(Some(queue_id));
    header.set_broker_name(Some(broker_name.to_owned()));
    let result = api
        .query_consumer_offset(
            broker_address,
            &header,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    let offset = result?.ok_or("Broker reported that the consumer offset was not found")?;
    println!("OFFSET\t{offset}");
    Ok(())
}

async fn max_offset(
    broker_address: &str,
    topic: &str,
    queue_id: i32,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let mut header = GetMaxOffsetRequestHeader::default();
    header.set_topic(Some(topic.to_owned()));
    header.set_queue_id(Some(queue_id));
    header.set_broker_name(Some("broker-a".to_owned()));
    let result = api
        .get_max_offset(
            broker_address,
            &header,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    println!("MAX_OFFSET\t{}", result?);
    Ok(())
}

async fn timeout_send(
    broker_address: &str,
    topic: &str,
    producer_group: &str,
    body: &str,
    timeout_ms: u64,
    go_file: &str,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let mut max_header = GetMaxOffsetRequestHeader::default();
    max_header.set_topic(Some(topic.to_owned()));
    max_header.set_queue_id(Some(0));
    max_header.set_broker_name(Some("broker-a".to_owned()));
    let initial_offset = api
        .get_max_offset(
            broker_address,
            &max_header,
            Instant::now() + Duration::from_secs(5),
        )
        .await?;
    println!("TIMEOUT\tREADY\trust\t{initial_offset}");

    let gate = Path::new(go_file);
    let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !gate.exists() {
        if tokio::time::Instant::now() >= ready_deadline {
            remoting.shutdown().await;
            return Err("timed out waiting for the delayed-response gate".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let message = Message::new(topic, body.as_bytes().to_vec());
    let born_timestamp = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let mut header = SendMessageRequestHeaderV1::default();
    header.set_producer_group(Some(producer_group.to_owned()));
    header.set_topic(topic.to_owned());
    header.set_default_topic("TBW102".to_owned());
    header.set_default_topic_queue_nums(4);
    header.set_queue_id(0);
    header.set_sys_flag(0);
    header.set_born_timestamp(born_timestamp);
    header.set_flag(0);
    header.set_properties(Some(encode_properties(&message.properties())));
    header.set_reconsume_times(Some(0));
    header.set_unit_mode(Some(false));
    header.set_batch(Some(false));
    header.set_broker_name(Some("broker-a".to_owned()));

    let started = std::time::Instant::now();
    let result = api
        .send_message(
            broker_address,
            "broker-a",
            &header,
            body.as_bytes().to_vec(),
            &format!("{body}-unique"),
            SendKind::V2,
            SendMode::Sync,
            Instant::now() + Duration::from_millis(timeout_ms),
        )
        .await;
    let elapsed_ms = started.elapsed().as_millis();
    remoting.shutdown().await;

    match result {
        Err(error)
            if error
                .to_string()
                .to_ascii_lowercase()
                .contains("deadline expired") =>
        {
            println!("TIMEOUT\tRESULT\trust\tTIMEOUT\tRPC\t{elapsed_ms}\t{initial_offset}");
            Ok(())
        }
        Err(error) => Err(error.into()),
        Ok(_) => Err("Broker response did not exceed the send deadline".into()),
    }
}

async fn offset_update(
    broker_address: &str,
    group: &str,
    topic: &str,
    queue_id: i32,
    offset: i64,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let mut header = UpdateConsumerOffsetRequestHeader::default();
    header.set_consumer_group(Some(group.to_owned()));
    header.set_topic(Some(topic.to_owned()));
    header.set_queue_id(Some(queue_id));
    header.set_commit_offset(Some(offset));
    header.set_broker_name(Some("broker-a".to_owned()));
    let result = api
        .update_consumer_offset(
            broker_address,
            &header,
            false,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    result?;
    println!("OFFSET_UPDATED\t{offset}");
    Ok(())
}

async fn pull(
    broker_address: &str,
    group: &str,
    topic: &str,
    queue_id: i32,
    queue_offset: i64,
) -> Result<(), Box<dyn Error>> {
    pull_named(
        broker_address,
        "broker-a",
        group,
        topic,
        queue_id,
        queue_offset,
    )
    .await
}

async fn pull_named(
    broker_address: &str,
    broker_name: &str,
    group: &str,
    topic: &str,
    queue_id: i32,
    queue_offset: i64,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let mut header = PullMessageRequestHeader::default();
    header.set_consumer_group(Some(group.to_owned()));
    header.set_topic(Some(topic.to_owned()));
    header.set_queue_id(Some(queue_id));
    header.set_queue_offset(Some(queue_offset));
    header.set_max_msg_nums(Some(32));
    header.set_sys_flag(Some(build_pull_sys_flag(false, false, true, false)));
    header.set_commit_offset(Some(0));
    header.set_suspend_timeout_millis(Some(0));
    header.set_subscription(Some("*".to_owned()));
    header.set_sub_version(Some(0));
    header.set_expression_type(Some("TAG".to_owned()));
    header.set_broker_name(Some(broker_name.to_owned()));
    let result = api
        .pull_message(
            broker_address,
            &header,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    let result = result?;
    let hex = result
        .raw_messages
        .as_deref()
        .map(encode_hex)
        .unwrap_or_default();
    println!(
        "PULL\t{}\t{}\t{}\t{}\t{}\t{}",
        result.status.name(),
        result.next_begin_offset,
        result.min_offset,
        result.max_offset,
        result.suggest_which_broker_id,
        hex
    );
    Ok(())
}

async fn lock_batch(
    broker_address: &str,
    group: &str,
    client_id: &str,
    topic: &str,
    broker_name: &str,
    queue_id: i32,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let queue = MessageQueue::new(topic, broker_name, queue_id);
    let mut body = LockBatchRequestBody::default();
    body.set_consumer_group(Some(group.to_owned()));
    body.set_client_id(Some(client_id.to_owned()));
    body.set_only_this_broker(true);
    body.set_mq_set(Some(SharedValue::new(HashSet::from([Some(
        SharedValue::new(queue.clone()),
    )]))));
    let result = api
        .lock_batch_mq(
            broker_address,
            &body,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    let acquired = result?.is_some_and(|queues| queues.contains(&Some(queue.clone())));
    println!(
        "LOCK\t{}\t{}\t{}\t{}",
        acquired, topic, broker_name, queue_id
    );
    Ok(())
}

async fn unlock_batch(
    broker_address: &str,
    group: &str,
    client_id: &str,
    topic: &str,
    broker_name: &str,
    queue_id: i32,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let mut body = UnlockBatchRequestBody::default();
    body.set_consumer_group(Some(group.to_owned()));
    body.set_client_id(Some(client_id.to_owned()));
    body.set_only_this_broker(true);
    body.set_mq_set(Some(SharedValue::new(HashSet::from([Some(
        SharedValue::new(MessageQueue::new(topic, broker_name, queue_id)),
    )]))));
    let result = api
        .unlock_batch_mq(
            broker_address,
            &body,
            false,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    result?;
    println!("UNLOCK\tOK");
    Ok(())
}

fn decode_batch_offsets(offset_message_ids: &str) -> Result<Vec<i64>, Box<dyn Error>> {
    let mut offsets = Vec::new();
    for message_id in offset_message_ids.split(',') {
        if message_id.is_empty() {
            return Err("batch readback requires every physical offset ID".into());
        }
        offsets.push(decode_message_id(message_id)?.offset());
    }
    Ok(offsets)
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::decode_batch_offsets;

    #[test]
    fn batch_readback_decodes_every_physical_offset_id() {
        let ids = "C0A86B0200002A9F0000000000000000,C0A86B0200002A9F00000000000000E1";

        assert_eq!(decode_batch_offsets(ids).unwrap(), [0, 225]);
    }

    #[test]
    fn batch_readback_rejects_missing_physical_offset_ids() {
        assert!(decode_batch_offsets("").is_err());
        assert!(decode_batch_offsets("C0A86B0200002A9F0000000000000000,").is_err());
    }
}
