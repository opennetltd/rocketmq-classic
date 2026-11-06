use std::{collections::BTreeMap, sync::Arc, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_consumer::offset::{
    OffsetRead, OffsetStore, ReadOffsetType, RemoteBrokerOffsetStore,
};
use rocketmq_classic_core::{config::ClientConfig, runtime::ClientRuntimeManager};
use rocketmq_classic_protocol::{
    codes::{request, response},
    header::{CommandHeader, HeaderMap, QueryConsumerOffsetRequestHeader},
};
use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

async fn read_command(stream: &mut TcpStream) -> RemotingCommand {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.unwrap();
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes).unwrap().unwrap()
}

async fn respond(
    stream: &mut TcpStream,
    command: &RemotingCommand,
    code: i32,
    fields: Option<HeaderMap>,
    body: Option<Vec<u8>>,
) {
    let mut reply = RemotingCommand {
        code,
        opaque: command.opaque,
        ext_fields: fields.map(Into::into),
        body,
        ..Default::default()
    };
    reply.mark_response();
    stream
        .write_all(&frame::encode(&reply).unwrap())
        .await
        .unwrap();
}

fn encoded<H: CommandHeader>(header: &H) -> HeaderMap {
    let mut fields = HeaderMap::new();
    header.encode_fields(&mut fields);
    fields
}

fn route(broker_id: i64, broker_address: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "queueDatas": [{
            "brokerName": "broker",
            "readQueueNums": 1,
            "writeQueueNums": 1,
            "perm": 6,
            "topicSysFlag": 0
        }],
        "brokerDatas": [{
            "cluster": "cluster",
            "brokerName": "broker",
            "brokerAddrs": {(broker_id.to_string()): broker_address}
        }],
        "filterServerTable": {}
    }))
    .unwrap()
}

async fn nameserver_with_route(route: Vec<u8>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let command = read_command(&mut stream).await;
        assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
        assert_eq!(
            command.ext_fields.as_ref().unwrap().get("topic"),
            Some(&"orders".to_owned())
        );
        respond(&mut stream, &command, response::SUCCESS, None, Some(route)).await;
    });
    (address, peer)
}

async fn make_runtime(
    nameserver_address: Option<String>,
) -> Arc<rocketmq_classic_core::runtime::ClientRuntime> {
    let mut config = ClientConfig::new("remote-offset-store-test");
    config.set_namesrv_addr(nameserver_address);
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    runtime.remoting().start().unwrap();
    runtime
}

fn query_header(queue: &MessageQueue) -> QueryConsumerOffsetRequestHeader {
    let mut header = QueryConsumerOffsetRequestHeader::default();
    header.set_consumer_group(Some("group".into()));
    header.set_topic(Some("orders".into()));
    header.set_queue_id(Some(queue.queue_id()));
    header.set_broker_name(Some("broker".into()));
    header
}

#[tokio::test]
async fn remote_read_refreshes_route_populates_cache_and_persists_oneway() {
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    // There is no master in the route. The first master-only lookup misses,
    // then the refreshed route allows the Java-compatible available-address fallback.
    let (nameserver_address, nameserver_peer) =
        nameserver_with_route(route(1, &broker_address)).await;
    let queue = MessageQueue::new("orders", "broker", 0);
    let broker_queue = queue.clone();
    let broker_peer = tokio::spawn(async move {
        let (mut stream, _) = broker_listener.accept().await.unwrap();
        let query = read_command(&mut stream).await;
        assert_eq!(query.code, request::QUERY_CONSUMER_OFFSET);
        assert!(!query.is_oneway());
        assert_eq!(
            query.ext_fields.as_ref().unwrap(),
            &encoded(&query_header(&broker_queue))
        );
        respond(
            &mut stream,
            &query,
            response::SUCCESS,
            Some(BTreeMap::from([("offset".into(), "41".into())])),
            None,
        )
        .await;

        let update = read_command(&mut stream).await;
        assert_eq!(update.code, request::UPDATE_CONSUMER_OFFSET);
        assert!(update.is_oneway());
        assert_eq!(
            update.ext_fields.as_ref().unwrap(),
            &BTreeMap::from([
                ("consumerGroup".into(), "group".into()),
                ("topic".into(), "orders".into()),
                ("queueId".into(), "0".into()),
                ("commitOffset".into(), "41".into()),
                ("bname".into(), "broker".into()),
            ])
        );
    });

    let runtime = make_runtime(Some(nameserver_address)).await;
    let store = RemoteBrokerOffsetStore::new(runtime.clone(), "group");
    assert_eq!(
        store
            .read_offset(&queue, ReadOffsetType::ReadFromStore)
            .await
            .unwrap(),
        OffsetRead::Found(41)
    );
    assert_eq!(
        store
            .read_offset(&queue, ReadOffsetType::ReadFromMemory)
            .await
            .unwrap(),
        OffsetRead::Found(41)
    );
    store.persist(&queue).await.unwrap();

    timeout(Duration::from_secs(3), nameserver_peer)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(3), broker_peer)
        .await
        .unwrap()
        .unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn remote_missing_offset_is_distinct_from_rpc_failure() {
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let (nameserver_address, nameserver_peer) =
        nameserver_with_route(route(0, &broker_address)).await;
    let queue = MessageQueue::new("orders", "broker", 0);
    let broker_peer = tokio::spawn(async move {
        let (mut stream, _) = broker_listener.accept().await.unwrap();
        let query = read_command(&mut stream).await;
        assert_eq!(query.code, request::QUERY_CONSUMER_OFFSET);
        respond(&mut stream, &query, response::QUERY_NOT_FOUND, None, None).await;
    });

    let runtime = make_runtime(Some(nameserver_address)).await;
    let store = RemoteBrokerOffsetStore::new(runtime.clone(), "group");
    assert_eq!(
        store
            .read_offset(&queue, ReadOffsetType::ReadFromStore)
            .await
            .unwrap(),
        OffsetRead::NotFound
    );
    assert_eq!(
        store
            .read_offset(&queue, ReadOffsetType::ReadFromMemory)
            .await
            .unwrap(),
        OffsetRead::NotFound
    );

    timeout(Duration::from_secs(3), nameserver_peer)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(3), broker_peer)
        .await
        .unwrap()
        .unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_persistence_keeps_the_in_memory_offset_for_retry() {
    let runtime = make_runtime(None).await;
    let store = RemoteBrokerOffsetStore::new(runtime.clone(), "group");
    let queue = MessageQueue::new("orders", "broker", 0);
    let stale_queue = MessageQueue::new("orders", "broker", 1);
    store.update_offset(&queue, 73, false);
    store.update_offset(&stale_queue, 58, false);

    assert!(
        store
            .persist_all(std::slice::from_ref(&queue))
            .await
            .is_err()
    );
    assert_eq!(
        store
            .read_offset(&queue, ReadOffsetType::ReadFromMemory)
            .await
            .unwrap(),
        OffsetRead::Found(73)
    );
    assert_eq!(
        store
            .read_offset(&stale_queue, ReadOffsetType::ReadFromMemory)
            .await
            .unwrap(),
        OffsetRead::NotFound
    );
    runtime.shutdown().await.unwrap();
}
