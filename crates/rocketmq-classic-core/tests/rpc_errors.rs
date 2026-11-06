use std::{error::Error, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_core::{api::MQClientApi, error::RpcError};
use rocketmq_classic_protocol::header::QueryConsumerOffsetRequestHeader;
use rocketmq_classic_remoting::{
    client::{RemotingClient, TransportMode},
    codec::frame,
    error::TransportError,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::Instant,
};

#[tokio::test]
async fn scripted_transport_errors_retain_rpc_category_and_source() {
    for failure in ["connect", "close", "frame"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let peer = if failure == "connect" {
            drop(listener);
            None
        } else {
            Some(tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut len = [0; 4];
                stream.read_exact(&mut len).await.unwrap();
                let mut buffer = BytesMut::from(&len[..]);
                buffer.resize(u32::from_be_bytes(len) as usize + 4, 0);
                stream.read_exact(&mut buffer[4..]).await.unwrap();
                frame::decode_next(&mut buffer).unwrap().unwrap();
                if failure == "frame" {
                    stream.write_all(&0i32.to_be_bytes()).await.unwrap();
                }
            }))
        };
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let error = MQClientApi::new(client.clone())
            .query_consumer_offset(
                &addr,
                &QueryConsumerOffsetRequestHeader::default(),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap_err();
        let source = error.source().unwrap();
        assert!(source.downcast_ref::<TransportError>().is_some());
        match (&error, failure) {
            (
                RpcError::Transport(TransportError::ConnectRequest { remote_addr, .. }),
                "connect",
            ) => assert_eq!(remote_addr, &addr),
            (
                RpcError::Transport(TransportError::SendRequest { remote_addr, cause }),
                "close" | "frame",
            ) => {
                // Java invokeSync wraps established-channel exceptional completion as send failure.
                assert_eq!(remote_addr, &addr);
                assert!(cause.is_none());
            }
            other => panic!("unexpected {other:?}"),
        }
        if let Some(peer) = peer {
            peer.await.unwrap();
        }
        client.shutdown().await;
    }
}

#[tokio::test]
async fn unconfigured_nameserver_and_stopped_client_are_not_broker_responses() {
    let client = RemotingClient::new(TransportMode::Plain);
    let api = MQClientApi::new(client.clone());
    let deadline = Instant::now() + Duration::from_secs(2);
    assert!(matches!(
        api.query_consumer_offset("127.0.0.1:1", &Default::default(), deadline)
            .await,
        Err(RpcError::Transport(TransportError::NotStarted))
    ));
    client.start().unwrap();
    assert!(matches!(
        api.get_topic_route("topic", deadline).await,
        Err(RpcError::Transport(TransportError::InvalidConfiguration(_)))
    ));
    client.shutdown().await;
    assert!(matches!(
        api.query_consumer_offset("127.0.0.1:1", &Default::default(), deadline)
            .await,
        Err(RpcError::Transport(TransportError::Shutdown))
    ));
}
