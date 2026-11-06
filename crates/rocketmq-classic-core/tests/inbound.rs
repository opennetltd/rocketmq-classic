use bytes::BytesMut;
use rocketmq_classic_core::runtime::{ConsumerOperation, ConsumerRuntime};
use rocketmq_classic_core::{config::ClientConfig, runtime::ClientRuntimeManager};
use rocketmq_classic_protocol::codes::{request, response};
use rocketmq_classic_protocol::heartbeat::ConsumerData;
use rocketmq_classic_remoting::client::RequestProcessor;
use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Duration, Instant},
};

async fn read(stream: &mut TcpStream) -> RemotingCommand {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.unwrap();
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes).unwrap().unwrap()
}
async fn write(stream: &mut TcpStream, command: &RemotingCommand) {
    stream
        .write_all(&frame::encode(command).unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn inactive_runtime_slots_match_phase_errors_and_no_dispatch_silence() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let open = read(&mut stream).await;
        for (index, code) in [
            request::CHECK_TRANSACTION_STATE,
            request::NOTIFY_CONSUMER_IDS_CHANGED,
            request::RESET_CONSUMER_CLIENT_OFFSET,
            request::GET_CONSUMER_STATUS_FROM_CLIENT,
            request::GET_CONSUMER_RUNNING_INFO,
            request::CONSUME_MESSAGE_DIRECTLY,
            request::PUSH_REPLY_MESSAGE_TO_CLIENT,
            request::NOTIFY_UNSUBSCRIBE_LITE,
        ]
        .into_iter()
        .enumerate()
        {
            let command = RemotingCommand {
                code,
                opaque: 100 + index as i32,
                ..Default::default()
            };
            write(&mut stream, &command).await;
            if matches!(
                code,
                request::CHECK_TRANSACTION_STATE
                    | request::NOTIFY_CONSUMER_IDS_CHANGED
                    | request::NOTIFY_UNSUBSCRIBE_LITE
            ) {
                assert!(
                    tokio::time::timeout(Duration::from_millis(20), read(&mut stream))
                        .await
                        .is_err(),
                    "Java null-return paths must not reply"
                );
                continue;
            }
            let reply = read(&mut stream).await;
            assert_eq!(reply.opaque, command.opaque);
            assert!(reply.is_response());
            if code == request::PUSH_REPLY_MESSAGE_TO_CLIENT {
                assert_eq!(reply.code, response::SYSTEM_ERROR);
                assert_eq!(reply.remark.as_deref(), Some("process reply message fail"));
            } else {
                assert_eq!(reply.code, response::REQUEST_CODE_NOT_SUPPORTED);
                assert!(reply.remark.unwrap().contains("Phase"));
            }
        }
        let mut oneway = RemotingCommand {
            code: request::CHECK_TRANSACTION_STATE,
            opaque: 200,
            ..Default::default()
        };
        oneway.mark_oneway();
        write(&mut stream, &oneway).await;
        let mut done = RemotingCommand {
            opaque: open.opaque,
            ..Default::default()
        };
        done.mark_response();
        write(&mut stream, &done).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), read(&mut stream))
                .await
                .is_err()
        );
    });
    let mut config = ClientConfig::new("inbound");
    config.set_namesrv_addr(Some(String::new()));
    let manager = ClientRuntimeManager::new();
    let runtime = manager.get_or_create(&config).await.unwrap();
    runtime.start().await.unwrap();
    runtime
        .remoting()
        .invoke_sync_with_deadline(
            &address,
            RemotingCommand {
                code: 9999,
                ..Default::default()
            },
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    peer.await.unwrap();
    assert!(runtime.shutdown().await.unwrap());
    assert!(
        runtime
            .remoting()
            .with_processor_table(|table| table.is_empty())
    );
}

struct Handler {
    remote: String,
    calls: AtomicUsize,
}
impl RequestProcessor for Handler {
    fn process(&self, _: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
        panic!("channel context missing");
    }
    fn process_with_context(
        &self,
        remote: &str,
        command: RemotingCommand,
    ) -> Result<Option<RemotingCommand>, String> {
        assert_eq!(remote, self.remote);
        assert_eq!(command.code, request::CONSUME_MESSAGE_DIRECTLY);
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(RemotingCommand {
            code: 0,
            remark: Some("owner activated".into()),
            ..Default::default()
        }))
    }
}
struct Consumer(Arc<Handler>);
impl ConsumerRuntime for Consumer {
    fn heartbeat_data(&self) -> ConsumerData {
        ConsumerData {
            group_name: Some("consumer".into()),
            ..Default::default()
        }
    }
    fn inbound_processor(&self, operation: ConsumerOperation) -> Option<Arc<dyn RequestProcessor>> {
        (operation == ConsumerOperation::ConsumeDirectly)
            .then(|| self.0.clone() as Arc<dyn RequestProcessor>)
    }
}

#[tokio::test]
async fn active_owner_receives_channel_context_and_transport_returns_matching_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let open = read(&mut stream).await;
        let command = RemotingCommand {
            code: request::CONSUME_MESSAGE_DIRECTLY,
            opaque: 551,
            ext_fields: Some(
                std::collections::BTreeMap::from([("consumerGroup".into(), "consumer".into())])
                    .into(),
            ),
            ..Default::default()
        };
        write(&mut stream, &command).await;
        let reply = read(&mut stream).await;
        assert_eq!(reply.opaque, 551);
        assert!(reply.is_response());
        assert_eq!(reply.code, 0);
        assert_eq!(reply.remark.as_deref(), Some("owner activated"));
        let mut done = RemotingCommand {
            opaque: open.opaque,
            ..Default::default()
        };
        done.mark_response();
        write(&mut stream, &done).await;
        let mut byte = [0];
        assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
    });
    let mut config = ClientConfig::new("active-inbound");
    config.set_namesrv_addr(Some(String::new()));
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    let handler = Arc::new(Handler {
        remote: address.clone(),
        calls: AtomicUsize::new(0),
    });
    let consumer = Arc::new(Consumer(handler.clone()));
    let lease = runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    runtime.start().await.unwrap();
    runtime
        .remoting()
        .invoke_sync_with_deadline(
            &address,
            RemotingCommand {
                code: 9999,
                ..Default::default()
            },
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    runtime
        .unregister_consumer(&lease, Instant::now())
        .await
        .unwrap();
    assert!(runtime.shutdown().await.unwrap());
    peer.await.unwrap();
}
