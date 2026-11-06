use std::collections::HashMap;

use base64::Engine;
use bytes::BytesMut;
use rocketmq_classic_core::{api::MQClientApi, error::RpcError};
use rocketmq_classic_protocol::{
    codes::{request, response},
    header::HeaderMap,
};
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{RemotingClient, TransportMode},
    codec::frame,
};
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

struct Reply {
    code: i32,
    fields: Option<HeaderMap>,
    body: Option<Vec<u8>>,
}

async fn read_command(stream: &mut TcpStream) -> RemotingCommand {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.unwrap();
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes).unwrap().unwrap()
}

async fn scripted_peer(
    replies: Vec<Reply>,
) -> (String, tokio::task::JoinHandle<Vec<RemotingCommand>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut requests = Vec::with_capacity(replies.len());
        for reply in replies {
            let request = read_command(&mut stream).await;
            let mut response = RemotingCommand {
                code: reply.code,
                opaque: request.opaque,
                ext_fields: reply.fields.map(Into::into),
                body: reply.body,
                ..RemotingCommand::default()
            };
            response.mark_response();
            stream
                .write_all(&frame::encode(&response).unwrap())
                .await
                .unwrap();
            requests.push(request);
        }
        requests
    });
    (address, task)
}

fn success(fields: Option<HeaderMap>, body: Option<&str>) -> Reply {
    Reply {
        code: response::SUCCESS,
        fields,
        body: body.map(|body| body.as_bytes().to_vec()),
    }
}

async fn api() -> (MQClientApi, RemotingClient) {
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    (MQClientApi::new(client.clone()), client)
}

fn assert_java_request(request: &RemotingCommand, code: i32, has_fields: bool) {
    assert_eq!(request.code, code);
    assert_eq!(
        request.language,
        Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
    );
    assert_eq!(request.ext_fields.is_some(), has_fields);
}

fn java_controller_fixture() -> Value {
    serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap()
}

fn assert_matches_java_request(request: &RemotingCommand, fixture_request: &Value) {
    assert_eq!(
        request.code,
        fixture_request["requestCode"].as_i64().unwrap() as i32
    );
    let actual_fields = request
        .ext_fields
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .unwrap();
    let mut expected_fields = fixture_request.get("extFields").cloned();
    let mut actual_fields = actual_fields;
    if let (Some(expected), Some(actual)) = (&mut expected_fields, &mut actual_fields) {
        // Java initializes these request-header fields at the call boundary.
        // Their exact millisecond value cannot be shared with a Rust process.
        expected.as_object_mut().unwrap().remove("invokeTime");
        actual.as_object_mut().unwrap().remove("invokeTime");
    }
    assert_eq!(actual_fields, expected_fields);

    let actual_body = request.body.as_deref();
    let expected_body = fixture_request["requestBodyBase64"].as_str().map(|body| {
        base64::engine::general_purpose::STANDARD
            .decode(body)
            .unwrap()
    });
    if request.code == request::UPDATE_CONTROLLER_CONFIG {
        let property_lines = |body: Option<&[u8]>| {
            body.map(|body| {
                body.split(|byte| *byte == b'\n')
                    .filter(|line| !line.is_empty())
                    .map(<[u8]>::to_vec)
                    .collect::<std::collections::HashSet<_>>()
            })
            .unwrap_or_default()
        };
        assert_eq!(
            property_lines(actual_body),
            property_lines(expected_body.as_deref())
        );
    } else {
        assert_eq!(actual_body, expected_body.as_deref());
    }
}

#[test]
fn controller_admin_surface_and_java_fixture_are_complete() {
    let fixture = java_controller_fixture();
    let methods = fixture["controllerAdminApi"].as_array().unwrap();
    assert_eq!(methods.len(), 7);
    for name in [
        "getControllerMetaData",
        "getInSyncStateData",
        "getBrokerEpochCache",
        "getControllerConfig",
        "updateControllerConfig",
        "electMaster",
        "cleanControllerBrokerData",
    ] {
        assert!(methods.iter().any(|method| {
            method
                .as_str()
                .is_some_and(|signature| signature.contains(&format!(".{name}(")))
        }));
    }
    let requests = fixture["controllerAdminOperations"]["requests"]
        .as_array()
        .unwrap();
    assert_eq!(requests.len(), 10);
    assert_eq!(requests[0]["timeoutMillis"], 3000);
    assert_eq!(requests[4]["timeoutMillis"], 2345);
    assert_eq!(requests[5]["timeoutMillis"], 2345);
    assert_eq!(requests[9]["timeoutMillis"], 3000);
    assert_eq!(
        fixture["controllerAdminOperations"]["metadataLeader"],
        "controller-a:9878"
    );
    assert_eq!(
        fixture["controllerAdminOperations"]["syncStateMasterEpoch"],
        9
    );
    assert_eq!(
        fixture["controllerAdminOperations"]["epochCacheMaxOffset"],
        20
    );
    assert_eq!(
        fixture["controllerAdminOperations"]["controllerConfigAlpha"],
        "one"
    );
    assert_eq!(fixture["controllerAdminOperations"]["electedBrokerId"], 7);
}

#[test]
fn controller_dto_java_fixture_matches_rust_dto_methods_and_strings() {
    use rocketmq_classic_core::api::controller::{
        BrokerMemberGroup, BrokerReplicasInfo, ControllerDtoError, ElectMasterResponseHeader,
        EpochEntry, EpochEntryCache, GetMetaDataResponseHeader, ReplicaIdentity, ReplicasInfo,
    };

    let fixture = java_controller_fixture();
    let api = &fixture["controllerDtoApi"];
    for class in [
        "getMetaDataResponseHeader",
        "electMasterResponseHeader",
        "brokerReplicasInfo",
        "replicasInfo",
        "replicaIdentity",
        "epochEntryCache",
        "brokerMemberGroup",
        "epochEntry",
    ] {
        assert!(api[class]["publicConstructors"].as_array().is_some());
        assert!(api[class]["publicMethods"].as_array().is_some());
    }
    assert!(
        api["brokerReplicasInfo"]["declaredPublicMethods"]
            .as_array()
            .unwrap()
            .iter()
            .any(|method| method.as_str().unwrap().contains(".addReplicaInfo("))
    );
    for name in ["isExistInSync", "isExistInNotSync", "isExistInAllReplicas"] {
        assert!(
            api["replicasInfo"]["declaredPublicMethods"]
                .as_array()
                .unwrap()
                .iter()
                .any(|method| method.as_str().unwrap().contains(&format!(".{name}(")))
        );
    }

    let operations = &fixture["controllerDtoOperations"];
    let alive = ReplicaIdentity::with_alive(
        Some("broker-a".into()),
        Some(0),
        Some("broker-a:10911".into()),
        Some(true),
    );
    let dead = ReplicaIdentity::with_alive(
        Some("broker-a".into()),
        Some(0),
        Some("broker-a:10911".into()),
        Some(false),
    );
    assert!(alive.java_equals(Some(&dead)).unwrap());
    assert_eq!(operations["replicaEqualsWrongType"], false);
    assert_eq!(
        alive.java_equals(Some(&"not-a-replica" as &dyn std::any::Any)),
        Ok(operations["replicaEqualsWrongType"].as_bool().unwrap())
    );
    assert_eq!(alive.java_hash_code(), dead.java_hash_code());
    assert_eq!(
        alive.java_hash_code(),
        operations["replicaHashCode"].as_i64().unwrap() as i32
    );
    assert_eq!(
        alive.to_string(),
        operations["replicaToString"].as_str().unwrap()
    );
    assert_eq!(operations["replicaEqualsIgnoresAlive"], true);
    assert_eq!(operations["replicaHashIgnoresAlive"], true);
    assert_eq!(operations["nullQueryAfterEmptyNameMismatch"], false);
    assert_eq!(operations["nullQueryException"], "NullPointerException");

    let replicas = ReplicasInfo::new(
        Some(0),
        Some("broker-a:10911".into()),
        7,
        8,
        Some(vec![Some(dead)]),
        Some(vec![]),
    );
    assert_eq!(
        replicas.is_exist_in_sync(Some("broker-a"), Some(0), Some("broker-a:10911")),
        Ok(operations["isExistInSyncIgnoresAlive"].as_bool().unwrap())
    );
    assert_eq!(operations["isExistInNotSync"], false);
    assert_eq!(operations["isExistInAllReplicas"], true);
    let null_sync = ReplicasInfo::new(Some(0), None, 7, 8, None, Some(vec![]));
    assert_eq!(operations["nullSyncListException"], "NullPointerException");
    assert_eq!(
        null_sync.is_exist_in_sync(Some("broker-a"), Some(0), None),
        Err(ControllerDtoError::NullDereference {
            operation: "ReplicasInfo.isExistIn*",
            field: "inSyncReplicas",
        })
    );
    let short_circuit = ReplicasInfo::new(
        Some(0),
        None,
        7,
        8,
        Some(vec![Some(ReplicaIdentity::new(
            Some("broker-a".into()),
            Some(0),
            Some("broker-a:10911".into()),
        ))]),
        None,
    );
    assert_eq!(operations["allReplicasShortCircuits"], true);
    assert_eq!(
        short_circuit.is_exist_in_all_replicas(Some("broker-a"), Some(0), Some("broker-a:10911")),
        Ok(true)
    );
    let null_second = ReplicasInfo::new(Some(0), None, 7, 8, Some(vec![]), None);
    assert_eq!(
        operations["allReplicasSecondListException"],
        "NullPointerException"
    );
    assert_eq!(
        null_second.is_exist_in_all_replicas(Some("broker-a"), Some(0), Some("broker-a:10911")),
        Err(ControllerDtoError::NullDereference {
            operation: "ReplicasInfo.isExistIn*",
            field: "notInSyncReplicas",
        })
    );

    let mut table = BrokerReplicasInfo::default();
    table.add_replica_info(None, None).unwrap();
    assert_eq!(operations["nullableReplicaMapEntry"], true);
    assert_eq!(
        table.add_replica_info(Some("broker-a".into()), None),
        Ok(())
    );
    let mut null_table = BrokerReplicasInfo {
        replicas_info_table: None,
    };
    assert_eq!(
        operations["nullReplicaTableException"],
        "NullPointerException"
    );
    assert!(matches!(
        null_table.add_replica_info(Some("broker-a".into()), None),
        Err(ControllerDtoError::NullDereference {
            operation: "BrokerReplicasInfo.addReplicaInfo",
            field: "replicasInfoTable",
        })
    ));

    let mut group = BrokerMemberGroup::new(Some("cluster-a".into()), Some("broker-a".into()));
    group
        .broker_addrs
        .as_mut()
        .unwrap()
        .insert(Some(8), Some("broker-8:10911".into()));
    group
        .broker_addrs
        .as_mut()
        .unwrap()
        .insert(Some(2), Some("broker-2:10911".into()));
    assert_eq!(
        group.minimum_broker_id(),
        Ok(operations["minimumBrokerId"].as_i64().unwrap())
    );
    assert_eq!(
        group.java_hash_code(),
        operations["brokerMemberGroupHashCode"].as_i64().unwrap() as i32
    );
    assert_eq!(
        group.to_string(),
        operations["brokerMemberGroupToString"].as_str().unwrap()
    );
    assert_eq!(
        BrokerMemberGroup::default().minimum_broker_id(),
        Ok(operations["emptyMinimumBrokerId"].as_i64().unwrap())
    );
    assert_eq!(
        operations["nullBrokerAddrsException"],
        "NullPointerException"
    );
    assert_eq!(operations["nullBrokerIdException"], "NullPointerException");
    assert_eq!(
        BrokerMemberGroup {
            broker_addrs: None,
            ..BrokerMemberGroup::default()
        }
        .minimum_broker_id(),
        Err(ControllerDtoError::NullDereference {
            operation: "BrokerMemberGroup.minimumBrokerId",
            field: "brokerAddrs",
        })
    );
    let null_key_group = BrokerMemberGroup {
        broker_addrs: Some(HashMap::from([(None, Some("null-address".into()))])),
        ..BrokerMemberGroup::default()
    };
    assert_eq!(
        null_key_group.minimum_broker_id(),
        Err(ControllerDtoError::NullDereference {
            operation: "BrokerMemberGroup.minimumBrokerId",
            field: "brokerAddrs key",
        })
    );

    let epoch = EpochEntry::new(4, 10);
    assert_eq!(
        epoch.end_offset,
        operations["epochEntryDefaultEnd"].as_i64().unwrap()
    );
    assert_eq!(
        epoch.java_hash_code(),
        operations["epochEntryHashCode"].as_i64().unwrap() as i32
    );
    assert_eq!(
        epoch.to_string(),
        operations["epochEntryToString"].as_str().unwrap()
    );
    assert_eq!(
        EpochEntry::from_entry(&epoch).to_string(),
        operations["epochEntryCopyConstructor"].as_str().unwrap()
    );
    let epoch_cache = EpochEntryCache::new(
        Some("cluster-a".into()),
        Some("broker-a".into()),
        2,
        Some(vec![Some(epoch)]),
        20,
    );
    assert_eq!(
        epoch_cache.to_string(),
        operations["epochEntryCacheToString"].as_str().unwrap()
    );
    assert_eq!(
        GetMetaDataResponseHeader::new(
            Some("cluster-a".into()),
            Some("controller-1".into()),
            Some("controller-a:9878".into()),
            true,
            Some("c-1,c-2".into())
        )
        .to_string(),
        operations["metadataHeaderToString"].as_str().unwrap()
    );
    assert_eq!(
        ElectMasterResponseHeader::new(Some(7), Some("broker-a:10911".into()), Some(11), Some(12))
            .to_string(),
        operations["electionHeaderToString"].as_str().unwrap()
    );
}

#[test]
fn controller_fastjson_body_decoders_match_invoked_java_eof_cases() {
    use rocketmq_classic_core::api::controller::{
        BrokerMemberGroup, BrokerReplicasInfo, EpochEntryCache,
    };

    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
    macro_rules! assert_decode_cases {
        ($dto:ty, $name:literal) => {{
            let dto_cases = &fixture["remotingSerializableBodyCases"][$name]["cases"];
            for case in dto_cases.as_array().unwrap() {
                let decoded = rocketmq_classic_protocol::decode_java_remoting_serializable::<$dto>(
                    case["rawInput"].as_str().unwrap().as_bytes(),
                )
                .unwrap();
                let actual = serde_json::to_value(decoded).unwrap();
                let expected =
                    serde_json::from_str::<Value>(case["decodedJson"].as_str().unwrap()).unwrap();
                for (field, expected_value) in expected.as_object().unwrap() {
                    assert_eq!(
                        actual.get(field),
                        Some(expected_value),
                        "{} {} field {field}",
                        $name,
                        case["id"]
                    );
                }
            }
        }};
    }

    assert_decode_cases!(BrokerReplicasInfo, "BrokerReplicasInfo");
    assert_decode_cases!(EpochEntryCache, "EpochEntryCache");
    assert_decode_cases!(BrokerMemberGroup, "BrokerMemberGroup");
}

#[tokio::test]
async fn controller_metadata_and_epoch_cache_decode_java_results() {
    let fixture = java_controller_fixture();
    let java_requests = fixture["controllerAdminOperations"]["requests"]
        .as_array()
        .unwrap();
    let (metadata_addr, metadata_peer) = scripted_peer(vec![success(
        Some(HeaderMap::from([
            ("group".into(), "cluster-a".into()),
            ("controllerLeaderId".into(), "c-2".into()),
            ("controllerLeaderAddress".into(), "controller-a:9878".into()),
            ("isLeader".into(), "TrUe".into()),
            ("peers".into(), "c-1,c-2".into()),
        ])),
        None,
    )])
    .await;
    let (epoch_addr, epoch_peer) = scripted_peer(vec![success(
        None,
        Some(concat!(
            r#"{"clusterName":"cluster-a","brokerName":"broker-a","brokerId":3,"epochList":[{"epoch":4,"startOffset":10}],"maxOffset":20}"#,
            "\u{001a}trailing"
        )),
    )])
    .await;
    let (client_api, client) = api().await;

    let metadata = client_api
        .get_controller_meta_data(&metadata_addr)
        .await
        .unwrap();
    assert_eq!(metadata.group.as_deref(), Some("cluster-a"));
    assert_eq!(
        metadata.controller_leader_address.as_deref(),
        fixture["controllerAdminOperations"]["metadataLeader"].as_str()
    );
    assert_eq!(metadata.controller_leader_id.as_deref(), Some("c-2"));
    assert_eq!(
        metadata.controller_leader_address.as_deref(),
        Some("controller-a:9878")
    );
    assert!(metadata.is_leader);
    assert_eq!(metadata.peers.as_deref(), Some("c-1,c-2"));

    let epoch = client_api
        .get_broker_epoch_cache(&epoch_addr)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(epoch.cluster_name.as_deref(), Some("cluster-a"));
    assert_eq!(
        epoch.max_offset,
        fixture["controllerAdminOperations"]["epochCacheMaxOffset"]
            .as_i64()
            .unwrap()
    );
    assert_eq!(
        epoch.epoch_list.unwrap()[0].as_ref().unwrap().end_offset,
        fixture["controllerAdminOperations"]["epochEntryEndOffsetWhenMissing"]
            .as_i64()
            .unwrap()
    );

    let requests = metadata_peer
        .await
        .unwrap()
        .into_iter()
        .chain(epoch_peer.await.unwrap())
        .collect::<Vec<_>>();
    assert_java_request(&requests[0], request::CONTROLLER_GET_METADATA_INFO, false);
    assert_java_request(&requests[1], request::GET_BROKER_EPOCH_CACHE, false);
    assert!(requests.iter().all(|request| request.body.is_none()));
    assert_matches_java_request(&requests[0], &java_requests[0]);
    assert_matches_java_request(&requests[1], &java_requests[3]);
    client.shutdown().await;
}

#[tokio::test]
async fn controller_sync_state_resolves_leader_and_encodes_nullable_broker_list() {
    let java_requests = java_controller_fixture()["controllerAdminOperations"]["requests"]
        .as_array()
        .unwrap()
        .clone();
    let (leader_addr, leader_peer) = scripted_peer(vec![success(
        None,
        Some(concat!(
            r#"{"replicasInfoTable":{"broker-a":{"masterBrokerId":0,"masterAddress":"broker-a:10911","masterEpoch":9,"syncStateSetEpoch":10,"inSyncReplicas":[{"brokerName":"broker-a","brokerId":0,"brokerAddress":"broker-a:10911","alive":true}],"notInSyncReplicas":[]}}}"#,
            "\u{001a}trailing"
        )),
    )])
    .await;
    let (controller_addr, controller_peer) = scripted_peer(vec![success(
        Some(HeaderMap::from([(
            "controllerLeaderAddress".into(),
            leader_addr.clone(),
        )])),
        None,
    )])
    .await;
    let (client_api, client) = api().await;

    let brokers = vec!["broker-a".to_owned()];
    let state = client_api
        .get_in_sync_state_data(&controller_addr, Some(&brokers))
        .await
        .unwrap()
        .unwrap();
    let replicas = state.replicas_info_table.unwrap();
    let replicas = replicas[&Some("broker-a".to_owned())].as_ref().unwrap();
    assert_eq!(replicas.master_epoch, Some(9));
    assert_eq!(
        replicas.in_sync_replicas.as_ref().unwrap()[0]
            .as_ref()
            .unwrap()
            .alive,
        Some(true)
    );

    let metadata_request = controller_peer.await.unwrap().pop().unwrap();
    assert_java_request(
        &metadata_request,
        request::CONTROLLER_GET_METADATA_INFO,
        false,
    );
    let sync_request = leader_peer.await.unwrap().pop().unwrap();
    assert_java_request(
        &sync_request,
        request::CONTROLLER_GET_SYNC_STATE_DATA,
        false,
    );
    assert_eq!(
        sync_request.body.as_deref(),
        Some(br#"["broker-a"]"#.as_slice())
    );
    assert_matches_java_request(&metadata_request, &java_requests[1]);
    assert_matches_java_request(&sync_request, &java_requests[2]);

    // Java encodes a null list as the literal JSON body `null`.
    let (leader_addr, leader_peer) = scripted_peer(vec![success(None, Some("null"))]).await;
    let (controller_addr, controller_peer) = scripted_peer(vec![success(
        Some(HeaderMap::from([(
            "controllerLeaderAddress".into(),
            leader_addr,
        )])),
        None,
    )])
    .await;
    assert!(
        client_api
            .get_in_sync_state_data(&controller_addr, None)
            .await
            .unwrap()
            .is_none()
    );
    let _ = controller_peer.await.unwrap();
    let request = leader_peer.await.unwrap().pop().unwrap();
    assert_eq!(request.body.as_deref(), Some(b"null".as_slice()));
    client.shutdown().await;
}

#[tokio::test]
async fn election_and_cleanup_use_controller_leader_and_encode_java_headers() {
    let java_requests = java_controller_fixture()["controllerAdminOperations"]["requests"]
        .as_array()
        .unwrap()
        .clone();
    let (leader_addr, leader_peer) = scripted_peer(vec![success(
        Some(HeaderMap::from([
            ("masterBrokerId".into(), "7".into()),
            ("masterAddress".into(), "broker-a:10911".into()),
            ("masterEpoch".into(), "11".into()),
            ("syncStateSetEpoch".into(), "12".into()),
        ])),
        Some(concat!(
            r#"{"cluster":"cluster-a","brokerName":"broker-a","brokerAddrs":{"7":"broker-a:10911"}}"#,
            "\u{001a}trailing"
        )),
    )])
    .await;
    let (controller_addr, controller_peer) = scripted_peer(vec![success(
        Some(HeaderMap::from([(
            "controllerLeaderAddress".into(),
            leader_addr.clone(),
        )])),
        None,
    )])
    .await;
    let (client_api, client) = api().await;

    let (header, members) = client_api
        .elect_master(&controller_addr, "cluster-a", "broker-a", Some(7))
        .await
        .unwrap();
    assert_eq!(header.master_broker_id, Some(7));
    assert_eq!(header.master_epoch, Some(11));
    assert_eq!(members.unwrap().minimum_broker_id(), Ok(7));
    let metadata_request = controller_peer.await.unwrap().pop().unwrap();
    assert_java_request(
        &metadata_request,
        request::CONTROLLER_GET_METADATA_INFO,
        false,
    );
    assert_matches_java_request(&metadata_request, &java_requests[6]);
    let election_request = leader_peer.await.unwrap().pop().unwrap();
    assert_java_request(&election_request, request::CONTROLLER_ELECT_MASTER, true);
    let fields = election_request.ext_fields.as_ref().unwrap();
    assert_eq!(
        fields.get("clusterName").map(String::as_str),
        Some("cluster-a")
    );
    assert_eq!(
        fields.get("brokerName").map(String::as_str),
        Some("broker-a")
    );
    assert_eq!(fields.get("brokerId").map(String::as_str), Some("7"));
    assert_eq!(
        fields.get("designateElect").map(String::as_str),
        Some("true")
    );
    assert!(fields.get("invokeTime").unwrap().parse::<i64>().unwrap() > 0);
    assert_matches_java_request(&election_request, &java_requests[7]);

    let (leader_addr, leader_peer) = scripted_peer(vec![success(None, None)]).await;
    let (controller_addr, controller_peer) = scripted_peer(vec![success(
        Some(HeaderMap::from([(
            "controllerLeaderAddress".into(),
            leader_addr,
        )])),
        None,
    )])
    .await;
    client_api
        .clean_controller_broker_data(
            &controller_addr,
            Some("cluster-a"),
            "broker-a",
            Some("1,2"),
            true,
        )
        .await
        .unwrap();
    let metadata_request = controller_peer.await.unwrap().pop().unwrap();
    assert_java_request(
        &metadata_request,
        request::CONTROLLER_GET_METADATA_INFO,
        false,
    );
    assert_matches_java_request(&metadata_request, &java_requests[8]);
    let cleanup_request = leader_peer.await.unwrap().pop().unwrap();
    assert_java_request(&cleanup_request, request::CLEAN_BROKER_DATA, true);
    let fields = cleanup_request.ext_fields.as_ref().unwrap();
    assert_eq!(
        fields.get("clusterName").map(String::as_str),
        Some("cluster-a")
    );
    assert_eq!(
        fields.get("brokerName").map(String::as_str),
        Some("broker-a")
    );
    assert_eq!(
        fields.get("brokerControllerIdsToClean").map(String::as_str),
        Some("1,2")
    );
    assert_eq!(
        fields.get("isCleanLivingBroker").map(String::as_str),
        Some("true")
    );
    assert!(fields.get("invokeTime").unwrap().parse::<i64>().unwrap() > 0);
    assert_matches_java_request(&cleanup_request, &java_requests[9]);
    client.shutdown().await;
}

#[tokio::test]
async fn controller_config_matches_fallback_and_fanout_rules() {
    let java_requests = java_controller_fixture()["controllerAdminOperations"]["requests"]
        .as_array()
        .unwrap()
        .clone();
    let (nameserver_addr, nameserver_peer) =
        scripted_peer(vec![success(None, Some("alpha=one\nbeta=two\n"))]).await;
    let (client_api, client) = api().await;
    client.update_name_server_address_list(vec![nameserver_addr.clone()]);
    let configs = client_api
        .get_controller_config(None, 2500)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(configs[&nameserver_addr].as_ref().unwrap()["alpha"], "one");
    let request = nameserver_peer.await.unwrap().pop().unwrap();
    assert_java_request(&request, request::GET_CONTROLLER_CONFIG, false);
    assert_matches_java_request(&request, &java_requests[4]);

    let properties = HashMap::from([
        ("alpha".to_owned(), "one".to_owned()),
        ("beta".to_owned(), "two".to_owned()),
    ]);
    let (first_addr, first_peer) = scripted_peer(vec![Reply {
        code: response::SYSTEM_ERROR,
        fields: None,
        body: None,
    }])
    .await;
    let (second_addr, second_peer) = scripted_peer(vec![success(None, None)]).await;
    let (third_addr, third_peer) = scripted_peer(vec![Reply {
        code: response::NO_PERMISSION,
        fields: None,
        body: None,
    }])
    .await;
    let controllers = vec![first_addr.clone(), second_addr.clone(), third_addr.clone()];
    let error = client_api
        .update_controller_config(Some(&properties), Some(&controllers), 2500)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::UPDATE_CONTROLLER_CONFIG,
            ref peer,
            code: response::NO_PERMISSION,
            ..
        } if peer == &third_addr
    ));
    for (address, peer) in [
        (&first_addr, first_peer),
        (&second_addr, second_peer),
        (&third_addr, third_peer),
    ] {
        let request = peer.await.unwrap().pop().unwrap();
        assert_java_request(&request, request::UPDATE_CONTROLLER_CONFIG, false);
        assert_eq!(
            request
                .body
                .as_ref()
                .unwrap()
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .count(),
            2,
            "{address}"
        );
        assert_matches_java_request(&request, &java_requests[5]);
    }

    let (no_fallback_api, no_fallback_client) = api().await;
    assert!(
        no_fallback_api
            .get_controller_config(None, 1)
            .await
            .unwrap()
            .is_none()
    );
    no_fallback_client.shutdown().await;
    client.shutdown().await;
}

#[tokio::test]
async fn controller_config_empty_properties_and_empty_controller_update_are_noops() {
    let (client_api, client) = api().await;
    let empty = HashMap::new();
    client_api
        .update_controller_config(Some(&empty), None, 100)
        .await
        .unwrap();
    let properties = HashMap::from([("a".to_owned(), "b".to_owned())]);
    client_api
        .update_controller_config(Some(&properties), Some(&[]), 100)
        .await
        .unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn config_get_preserves_malformed_properties_null_and_stops_on_error() {
    let (first_addr, first_peer) = scripted_peer(vec![success(None, Some("bad=\\u12G4\n"))]).await;
    let (client_api, client) = api().await;
    let malformed = client_api
        .get_controller_config(Some(std::slice::from_ref(&first_addr)), 2500)
        .await
        .unwrap()
        .unwrap();
    assert!(malformed[&first_addr].is_none());
    let _ = first_peer.await.unwrap();

    let (second_addr, second_peer) = scripted_peer(vec![Reply {
        code: response::SYSTEM_ERROR,
        fields: None,
        body: None,
    }])
    .await;
    let error = client_api
        .get_controller_config(Some(std::slice::from_ref(&second_addr)), 2500)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::GET_CONTROLLER_CONFIG,
            ref peer,
            code: response::SYSTEM_ERROR,
            ..
        } if peer == &second_addr
    ));
    let _ = second_peer.await.unwrap();
    client.shutdown().await;
}
