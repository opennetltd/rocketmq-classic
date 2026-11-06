use std::time::Duration;

use bytes::BytesMut;
use rocketmq_classic_core::{
    api::{
        MQClientApi,
        producer::{SendKind, SendMode},
    },
    error::RpcError,
};
use rocketmq_classic_protocol::header::SendMessageRequestHeaderV1;
use rocketmq_classic_protocol::{
    codes::request,
    header::{AckMessageRequestHeader, PullMessageRequestHeader},
};
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{RemotingClient, TransportMode},
    codec::frame,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Instant, sleep},
};

#[tokio::test]
async fn sync_send_uses_wait_deadline_while_future_accepts_java_cleanup_grace() {
    for mode in [SendMode::Sync, SendMode::Future] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut len = [0; 4];
            stream.read_exact(&mut len).await.unwrap();
            let mut buffer = BytesMut::from(&len[..]);
            buffer.resize(u32::from_be_bytes(len) as usize + 4, 0);
            stream.read_exact(&mut buffer[4..]).await.unwrap();
            let request = frame::decode_next(&mut buffer).unwrap().unwrap();
            sleep(Duration::from_millis(150)).await;
            let mut reply = RemotingCommand {
                opaque: request.opaque,
                ext_fields: Some(
                    [
                        ("msgId".into(), "offset-id".into()),
                        ("queueId".into(), "2".into()),
                        ("queueOffset".into(), "17".into()),
                    ]
                    .into(),
                ),
                ..Default::default()
            };
            reply.mark_response();
            let _ = stream.write_all(&frame::encode(&reply).unwrap()).await;
        });
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let api = MQClientApi::new(client.clone());
        let result = api
            .send_message(
                &addr,
                "broker",
                &SendMessageRequestHeaderV1::default(),
                vec![42],
                "unique",
                SendKind::V2,
                mode,
                Instant::now() + Duration::from_millis(70),
            )
            .await;
        match mode {
            SendMode::Sync => assert!(
                matches!(result, Err(RpcError::Transport(ref error)) if error.is_timeout()),
                "{result:?}"
            ),
            SendMode::Future => assert_eq!(result.unwrap().unwrap().queue_offset, 17),
            _ => unreachable!(),
        }
        peer.await.unwrap();
        client.shutdown().await;
    }
}

#[tokio::test]
async fn pull_sync_future_ack_and_route_select_their_java_wait_modes() {
    for operation in ["pull-sync", "pull-future", "ack", "route"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut len = [0; 4];
            stream.read_exact(&mut len).await.unwrap();
            let mut buffer = BytesMut::from(&len[..]);
            buffer.resize(u32::from_be_bytes(len) as usize + 4, 0);
            stream.read_exact(&mut buffer[4..]).await.unwrap();
            let incoming = frame::decode_next(&mut buffer).unwrap().unwrap();
            assert_eq!(
                incoming.code,
                match operation {
                    "ack" => request::ACK_MESSAGE,
                    "route" => request::GET_ROUTEINFO_BY_TOPIC,
                    _ => request::PULL_MESSAGE,
                }
            );
            sleep(Duration::from_millis(150)).await;
            let mut reply = RemotingCommand {
                opaque: incoming.opaque,
                ext_fields: Some(
                    [
                        ("nextBeginOffset".into(), "17".into()),
                        ("minOffset".into(), "0".into()),
                        ("maxOffset".into(), "20".into()),
                        ("suggestWhichBrokerId".into(), "0".into()),
                    ]
                    .into(),
                ),
                body: Some(b"{}".to_vec()),
                ..Default::default()
            };
            reply.mark_response();
            let _ = stream.write_all(&frame::encode(&reply).unwrap()).await;
        });
        let client = RemotingClient::new(TransportMode::Plain);
        if operation == "route" {
            client.update_name_server_address_list(vec![addr.clone()]);
        }
        client.start().unwrap();
        let api = MQClientApi::new(client.clone());
        let deadline = Instant::now() + Duration::from_millis(70);
        let result = match operation {
            "pull-sync" => api
                .pull_message(
                    &addr,
                    &{
                        let mut __phase18_header_0 = PullMessageRequestHeader::default();
                        __phase18_header_0.set_sys_flag(Some(0));
                        __phase18_header_0
                    },
                    deadline,
                )
                .await
                .map(|_| ()),
            "pull-future" => api
                .pull_message_future(
                    &addr,
                    &{
                        let mut __phase18_header_1 = PullMessageRequestHeader::default();
                        __phase18_header_1.set_sys_flag(Some(0));
                        __phase18_header_1
                    },
                    deadline,
                )
                .await
                .map(|result| assert_eq!(result.next_begin_offset, 17)),
            "ack" => api
                .ack_message(&addr, &AckMessageRequestHeader::default(), deadline)
                .await
                .map(|status| {
                    assert_eq!(
                        status,
                        rocketmq_classic_core::api::extensions::AckStatus::Ok
                    )
                }),
            "route" => api.get_topic_route("topic", deadline).await.map(|_| ()),
            _ => unreachable!(),
        };
        if operation == "pull-sync" || operation == "route" {
            assert!(
                matches!(result, Err(RpcError::Transport(ref error)) if error.is_timeout()),
                "{operation}: {result:?}"
            );
        } else {
            result.unwrap();
        }
        peer.await.unwrap();
        client.shutdown().await;
    }
}
