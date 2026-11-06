use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_core::route::{TopicRouteDataHandle, TopicSnapshot};
use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, RuntimeError},
};
use rocketmq_classic_protocol::route::{SharedBrokerDataHandle, TopicRouteData};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

mod support;
use support::{same_route_handle, same_route_option};

fn broker_handle(route: &TopicRouteData, index: usize) -> SharedBrokerDataHandle {
    route
        .broker_datas
        .as_ref()
        .expect("broker list")
        .with(|brokers| brokers[index].clone().expect("broker row"))
}

#[test]
fn public_topic_snapshot_factory_matches_invoked_java_route_conversions() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 36);

    for case in cases {
        let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
        if case["outcome"] == "OK" {
            let snapshot = TopicSnapshot::from_route_data("topic", &route).unwrap();
            assert_eq!(
                serde_json::to_value(&snapshot.publish_queues).unwrap(),
                case["publish"]
            );
            assert_eq!(
                serde_json::to_value(&snapshot.subscribe_queues).unwrap(),
                case["subscribe"]
            );
            assert_eq!(snapshot.order_topic, case["ordered"].as_bool().unwrap());
            assert_eq!(
                serde_json::to_value(
                    snapshot
                        .endpoints
                        .iter()
                        .map(|(queue, broker)| serde_json::json!({"queue":queue,"broker":broker}))
                        .collect::<Vec<_>>()
                )
                .unwrap(),
                case["endpoints"]
            );
            let expected_route =
                TopicRouteData::java_from_json(case["topicRouteData"].as_str().unwrap()).unwrap();
            assert_eq!(snapshot.route.as_ref(), &expected_route);
        } else {
            assert!(TopicSnapshot::from_route_data("topic", &route).is_err());
        }
    }

    let unmatched = cases
        .iter()
        .find(|case| case["id"] == "empty-mapping-unmatched-queue-broker")
        .expect("Java oracle includes an empty-mapping route with an unmatched queue broker");
    let route: TopicRouteData = serde_json::from_value(unmatched["input"].clone()).unwrap();
    let snapshot = TopicSnapshot::from_route_data("topic", &route).unwrap();
    assert!(snapshot.publish_queues.is_empty());
    assert_eq!(snapshot.subscribe_queues.len(), 2);
    assert!(snapshot.endpoints.is_empty());
}

#[test]
fn public_nullable_route_conversion_arguments_match_invoked_java() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    let cases = fixture["nullArgumentConversions"].as_array().unwrap();
    assert_eq!(cases.len(), 10);

    for case in cases {
        let route = if case["routeIsNull"].as_bool().unwrap() {
            None
        } else {
            Some(serde_json::from_value::<TopicRouteData>(case["routeInput"].clone()).unwrap())
        };
        let result =
            TopicSnapshot::from_nullable_route_data(case["topic"].as_str(), route.as_ref());
        if case["outcome"] == "OK" {
            let snapshot = result.unwrap();
            if let Some(route_data) = case.get("topicRouteData") {
                let expected_route =
                    TopicRouteData::java_from_json(route_data.as_str().unwrap()).unwrap();
                assert_eq!(snapshot.route.as_ref(), &expected_route, "{}", case["id"]);
            }
            if case["topic"].is_null() {
                assert!(
                    snapshot
                        .publish_queues
                        .iter()
                        .all(|queue| queue.topic().is_none()),
                    "{}",
                    case["id"]
                );
                assert!(
                    snapshot
                        .subscribe_queues
                        .iter()
                        .all(|queue| queue.topic().is_none()),
                    "{}",
                    case["id"]
                );
                if route.as_ref().is_some_and(|route| {
                    route
                        .topic_queue_mapping_by_broker
                        .as_ref()
                        .is_some_and(|mappings| mappings.with(|mappings| !mappings.is_empty()))
                }) {
                    assert!(!snapshot.endpoints.is_empty(), "{}", case["id"]);
                    assert!(
                        snapshot
                            .endpoints
                            .keys()
                            .all(|queue| queue.topic().is_none()),
                        "{}",
                        case["id"]
                    );
                }
            }
            match case["method"].as_str().unwrap() {
                "publish" => {
                    let expected: Vec<MessageQueue> =
                        serde_json::from_value(case["publish"].clone()).unwrap();
                    assert_eq!(snapshot.publish_queues, expected, "{}", case["id"]);
                    assert_eq!(snapshot.order_topic, case["ordered"].as_bool().unwrap());
                }
                "subscribe" => {
                    let expected: Vec<MessageQueue> =
                        serde_json::from_value(case["subscribe"].clone()).unwrap();
                    assert_eq!(
                        snapshot.subscribe_queues.into_iter().collect::<Vec<_>>(),
                        expected,
                        "{}",
                        case["id"]
                    );
                }
                other => panic!("unknown Java route converter {other}"),
            }
        } else {
            assert!(
                matches!(
                    result,
                    Err(RuntimeError::InvalidRoute("null topic route data"))
                ),
                "{}",
                case["id"]
            );
        }
    }
}

#[tokio::test]
async fn raw_clear_query_refetches_once_and_retained_table_observes_wire_refresh() {
    use bytes::BytesMut;
    use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::{Duration, Instant},
    };
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    let case = &fixture["runtimeTopicRouteTable"];
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new("raw-table-wire");
    config.set_namesrv_addr(Some(listener.local_addr().unwrap().to_string()));
    config.set_enable_heartbeat_channel_event_listener(false);
    let body = serde_json::to_vec(&case["initialInput"]).unwrap();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        for _ in 0..2 {
            let mut length = [0; 4];
            socket.read_exact(&mut length).await.unwrap();
            let mut bytes = BytesMut::from(&length[..]);
            bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
            socket.read_exact(&mut bytes[4..]).await.unwrap();
            let request = frame::decode_next(&mut bytes).unwrap().unwrap();
            assert_eq!(request.code, 105);
            assert_eq!(request.ext_fields.as_ref().unwrap()["topic"], "topic");
            let mut response = RemotingCommand {
                opaque: request.opaque,
                body: Some(body.clone()),
                ..Default::default()
            };
            response.mark_response();
            socket
                .write_all(&frame::encode(&response).unwrap())
                .await
                .unwrap();
        }
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0); // No third cached-query RPC.
    });
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    // Start only lower transport: no runtime periodic tasks/Broker RPCs exist.
    runtime.remoting().start().unwrap();
    let table = runtime.topic_route_table();
    assert!(
        runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap()
    );
    assert!(table.contains_key("topic").unwrap());
    let replacement = TopicRouteDataHandle::new(
        serde_json::from_value(case["replacementInput"].clone()).unwrap(),
    );
    table.put("topic", replacement.clone()).unwrap();
    assert!(same_route_handle(
        &runtime.query_topic_route("topic").await.unwrap().unwrap(),
        &replacement
    ));
    table.clear().unwrap();
    assert!(runtime.get_existing_topic_route("topic").unwrap().is_none());
    assert_eq!(
        runtime.route_snapshot().unwrap().publish_address("broker"),
        case["publishAddressAfterClear"].as_str()
    );
    let refreshed =
        tokio::time::timeout(Duration::from_secs(5), runtime.query_topic_route("topic"))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    assert!(same_route_option(table.get("topic").unwrap(), &refreshed));
    assert_eq!(
        refreshed
            .snapshot()
            .unwrap()
            .broker_datas_snapshot()
            .unwrap()[0]
            .as_ref()
            .unwrap()
            .broker_addrs
            .as_ref()
            .unwrap()
            .with(|addresses| addresses.get(&Some(0)).cloned().flatten()),
        Some("master:10911".to_owned())
    );
    assert!(same_route_handle(
        &runtime.query_topic_route("topic").await.unwrap().unwrap(),
        &refreshed
    ));
    runtime.remoting().shutdown().await;
    tokio::time::timeout(Duration::from_secs(5), peer)
        .await
        .unwrap()
        .unwrap();
    assert!(table.contains_key("topic").unwrap()); // Transport shutdown does not erase raw data.
}

#[tokio::test]
async fn raw_map_put_if_absent_is_atomic_and_values_are_owned_without_runtime_retention() {
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("raw-table-atomic"))
        .await
        .unwrap();
    let table = runtime.topic_route_table();
    let owner = TopicRouteDataHandle::new(TopicRouteData::default());
    let installed = std::thread::scope(|scope| {
        let mut calls = Vec::new();
        for _ in 0..64 {
            let table = table.clone();
            let owner = owner.clone();
            calls.push(scope.spawn(move || table.put_if_absent("topic", owner).unwrap().is_none()));
        }
        calls
            .into_iter()
            .map(|call| usize::from(call.join().unwrap()))
            .sum::<usize>()
    });
    assert_eq!(installed, 1);
    let before = runtime.route_snapshot().unwrap();
    assert!(table.replace("absent", owner.clone()).unwrap().is_none());
    assert!(
        table
            .put_if_absent("topic", owner.clone())
            .unwrap()
            .is_some()
    );
    assert!(Arc::ptr_eq(&before, &runtime.route_snapshot().unwrap()));
    let weak_owner = owner.downgrade();
    drop(owner);
    assert!(weak_owner.upgrade().is_some());
    let detached = table.snapshot().unwrap();
    table.clear().unwrap();
    assert!(table.is_empty().unwrap());
    assert!(weak_owner.upgrade().is_some());
    drop(detached);
    drop(before);
    assert!(weak_owner.upgrade().is_none());
}

#[tokio::test]
async fn retained_raw_topic_table_matches_invoked_java_primitives_and_cached_queries() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    let case = &fixture["runtimeTopicRouteTable"];
    let first =
        TopicRouteDataHandle::new(serde_json::from_value(case["initialInput"].clone()).unwrap());
    let second = TopicRouteDataHandle::new(
        serde_json::from_value(case["replacementInput"].clone()).unwrap(),
    );
    let equal_second = TopicRouteDataHandle::new(second.snapshot().unwrap());
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("raw-table"))
        .await
        .unwrap();
    let table = runtime.topic_route_table();
    let retained = table.clone();
    let same = same_route_option;
    let mut outcomes = BTreeMap::new();
    outcomes.insert("initialEmpty", table.is_empty().unwrap());
    outcomes.insert(
        "firstPutAbsent",
        table.put("topic", first.clone()).unwrap().is_none(),
    );
    outcomes.insert(
        "containsValueNullThrowsNullPointer",
        matches!(
            table.contains_value_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route value"))
        ),
    );
    outcomes.insert(
        "putNullKeyThrowsNullPointer",
        matches!(
            table.put_nullable(None, Some(second.clone())),
            Err(RuntimeError::InvalidRoute("null topic route key"))
        ),
    );
    outcomes.insert(
        "putNullValueThrowsNullPointer",
        matches!(
            table.put_nullable(Some("topic"), None),
            Err(RuntimeError::InvalidRoute("null topic route value"))
        ),
    );
    outcomes.insert(
        "putIfAbsentNullKeyThrowsNullPointer",
        matches!(
            table.put_if_absent_nullable(None, Some(second.clone())),
            Err(RuntimeError::InvalidRoute("null topic route key"))
        ),
    );
    outcomes.insert(
        "putIfAbsentNullValueThrowsNullPointer",
        matches!(
            table.put_if_absent_nullable(Some("topic"), None),
            Err(RuntimeError::InvalidRoute("null topic route value"))
        ),
    );
    outcomes.insert(
        "replaceNullKeyThrowsNullPointer",
        matches!(
            table.replace_nullable(None, Some(second.clone())),
            Err(RuntimeError::InvalidRoute("null topic route key"))
        ),
    );
    outcomes.insert(
        "replaceNullValueThrowsNullPointer",
        matches!(
            table.replace_nullable(Some("topic"), None),
            Err(RuntimeError::InvalidRoute("null topic route value"))
        ),
    );
    outcomes.insert(
        "conditionalReplaceNullKeyThrowsNullPointer",
        matches!(
            table.replace_if_equal_nullable(
                None,
                Some(&first.snapshot().unwrap()),
                Some(second.clone())
            ),
            Err(RuntimeError::InvalidRoute("null topic route key"))
        ),
    );
    outcomes.insert(
        "conditionalReplaceNullExpectedThrowsNullPointer",
        matches!(
            table.replace_if_equal_nullable(Some("topic"), None, Some(second.clone())),
            Err(RuntimeError::InvalidRoute("null topic route value"))
        ),
    );
    outcomes.insert(
        "conditionalReplaceNullValueThrowsNullPointer",
        matches!(
            table.replace_if_equal_nullable(Some("topic"), Some(&first.snapshot().unwrap()), None),
            Err(RuntimeError::InvalidRoute("null topic route value"))
        ),
    );
    outcomes.insert(
        "conditionalRemoveNullValueReturnsFalse",
        !table.remove_if_equal_nullable(Some("topic"), None).unwrap(),
    );
    outcomes.insert(
        "nullMapOperationArgumentsKeepMapping",
        table.len().unwrap() == 1 && same(table.get("topic").unwrap(), &first),
    );
    let key_view = table.key_set_view();
    let values_view = table.values_view();
    let entry_view = table.entry_set_view();
    outcomes.insert(
        "keyViewNullContainsThrowsNullPointer",
        matches!(
            key_view.contains_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route key"))
        ),
    );
    outcomes.insert(
        "keyViewNullRemoveThrowsNullPointer",
        matches!(
            key_view.remove_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route key"))
        ),
    );
    outcomes.insert(
        "valuesViewNullContainsThrowsNullPointer",
        matches!(
            values_view.contains_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route value"))
        ),
    );
    outcomes.insert(
        "valuesViewNullRemoveReturnsFalse",
        !values_view.remove_nullable(None).unwrap(),
    );
    outcomes.insert(
        "entryViewNullKeyContainsReturnsFalse",
        !entry_view
            .contains_nullable(None, Some(&first.snapshot().unwrap()))
            .unwrap(),
    );
    outcomes.insert(
        "entryViewNullKeyRemoveReturnsFalse",
        !entry_view
            .remove_nullable(None, Some(&first.snapshot().unwrap()))
            .unwrap(),
    );
    outcomes.insert(
        "entryViewNullValueContainsReturnsFalse",
        !entry_view.contains_nullable(Some("topic"), None).unwrap(),
    );
    outcomes.insert(
        "entryViewNullValueRemoveReturnsFalse",
        !entry_view.remove_nullable(Some("topic"), None).unwrap(),
    );
    outcomes.insert(
        "nullCollectionViewArgumentsKeepMapping",
        table.len().unwrap() == 1 && same(table.get("topic").unwrap(), &first),
    );
    outcomes.insert(
        "keyViewRemoveAllNullThrowsNullPointer",
        matches!(
            key_view.remove_all_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route collection"))
        ),
    );
    outcomes.insert(
        "keyViewRetainAllNullThrowsNullPointer",
        matches!(
            key_view.retain_all_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route collection"))
        ),
    );
    outcomes.insert(
        "valuesViewRemoveAllNullThrowsNullPointer",
        matches!(
            values_view.remove_all_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route collection"))
        ),
    );
    outcomes.insert(
        "valuesViewRetainAllNullThrowsNullPointer",
        matches!(
            values_view.retain_all_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route collection"))
        ),
    );
    outcomes.insert(
        "entryViewRemoveAllNullThrowsNullPointer",
        matches!(
            entry_view.remove_all_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route collection"))
        ),
    );
    outcomes.insert(
        "entryViewRetainAllNullThrowsNullPointer",
        matches!(
            entry_view.retain_all_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route collection"))
        ),
    );
    outcomes.insert(
        "nullViewCollectionArgumentsKeepMapping",
        table.len().unwrap() == 1 && same(table.get("topic").unwrap(), &first),
    );
    outcomes.insert(
        "keyViewRemoveIfNullThrowsNullPointer",
        matches!(
            key_view.remove_if_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route predicate"))
        ),
    );
    outcomes.insert(
        "valuesViewRemoveIfNullThrowsNullPointer",
        matches!(
            values_view.remove_if_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route predicate"))
        ),
    );
    outcomes.insert(
        "entryViewRemoveIfNullThrowsNullPointer",
        matches!(
            entry_view.remove_if_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route predicate"))
        ),
    );
    outcomes.insert(
        "nullViewPredicateArgumentsKeepMapping",
        table.len().unwrap() == 1 && same(table.get("topic").unwrap(), &first),
    );
    outcomes.insert(
        "forEachNullActionThrowsNullPointer",
        matches!(
            table.for_each_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route callback"))
        ),
    );
    outcomes.insert(
        "replaceAllNullFunctionThrowsNullPointer",
        matches!(
            table.replace_all_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route callback"))
        ),
    );
    outcomes.insert(
        "computeIfAbsentNullFunctionThrowsNullPointer",
        matches!(
            table.compute_if_absent_with_nullable_callback(Some("topic"), None),
            Err(RuntimeError::InvalidRoute("null topic route callback"))
        ),
    );
    outcomes.insert(
        "computeIfPresentNullFunctionThrowsNullPointer",
        matches!(
            table.compute_if_present_with_nullable_callback(Some("topic"), None),
            Err(RuntimeError::InvalidRoute("null topic route callback"))
        ),
    );
    outcomes.insert(
        "computeNullFunctionThrowsNullPointer",
        matches!(
            table.compute_with_nullable_callback(Some("topic"), None),
            Err(RuntimeError::InvalidRoute("null topic route callback"))
        ),
    );
    outcomes.insert(
        "mergeNullFunctionThrowsNullPointer",
        matches!(
            table.merge_with_nullable_callback(Some("topic"), Some(second.clone()), None),
            Err(RuntimeError::InvalidRoute("null topic route callback"))
        ),
    );
    outcomes.insert(
        "nullMapCallbackArgumentsKeepMapping",
        table.len().unwrap() == 1 && same(table.get("topic").unwrap(), &first),
    );
    let mut null_compute_callback_calls = 0;
    outcomes.insert(
        "computeIfAbsentNullKeyThrowsNullPointer",
        matches!(
            table.compute_if_absent_nullable(None, |_| {
                null_compute_callback_calls += 1;
                Some(second.clone())
            }),
            Err(RuntimeError::InvalidRoute("null topic route key"))
        ),
    );
    outcomes.insert(
        "computeIfPresentNullKeyThrowsNullPointer",
        matches!(
            table.compute_if_present_nullable(None, |_, _| {
                null_compute_callback_calls += 1;
                Some(second.clone())
            }),
            Err(RuntimeError::InvalidRoute("null topic route key"))
        ),
    );
    outcomes.insert(
        "computeNullKeyThrowsNullPointer",
        matches!(
            table.compute_nullable(None, |_, _| {
                null_compute_callback_calls += 1;
                Some(second.clone())
            }),
            Err(RuntimeError::InvalidRoute("null topic route key"))
        ),
    );
    outcomes.insert(
        "mergeNullKeyThrowsNullPointer",
        matches!(
            table.merge_nullable(None, Some(second.clone()), |_, new_value| {
                null_compute_callback_calls += 1;
                Some(new_value.clone())
            }),
            Err(RuntimeError::InvalidRoute("null topic route key"))
        ),
    );
    outcomes.insert(
        "mergeNullValueThrowsNullPointer",
        matches!(
            table.merge_nullable(Some("topic"), None, |_, new_value| {
                null_compute_callback_calls += 1;
                Some(new_value.clone())
            }),
            Err(RuntimeError::InvalidRoute("null topic route value"))
        ),
    );
    outcomes.insert(
        "nullComputeArgumentsSkipCallbacks",
        null_compute_callback_calls == 0,
    );
    outcomes.insert(
        "nullComputeArgumentsKeepMapping",
        table.len().unwrap() == 1 && same(table.get("topic").unwrap(), &first),
    );
    outcomes.insert(
        "getOrDefaultPresent",
        same_route_handle(
            &table.get_or_default("topic", second.clone()).unwrap(),
            &first,
        ),
    );
    outcomes.insert(
        "getOrDefaultAbsent",
        same_route_handle(
            &table.get_or_default("missing", second.clone()).unwrap(),
            &second,
        ),
    );
    outcomes.insert(
        "getOrDefaultNullDefaultPresent",
        same_route_option(
            table.get_or_default_nullable(Some("topic"), None).unwrap(),
            &first,
        ),
    );
    outcomes.insert(
        "getOrDefaultNullDefaultAbsent",
        table
            .get_or_default_nullable(Some("missing"), None)
            .unwrap()
            .is_none(),
    );
    outcomes.insert(
        "getOrDefaultNullKeyThrowsNullPointer",
        matches!(
            table.get_or_default_nullable(None, None),
            Err(RuntimeError::InvalidRoute("null topic route key"))
        ),
    );
    outcomes.insert(
        "getNullKeyThrowsNullPointer",
        matches!(
            table.get_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route key"))
        ),
    );
    outcomes.insert(
        "containsKeyNullKeyThrowsNullPointer",
        matches!(
            table.contains_key_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route key"))
        ),
    );
    outcomes.insert(
        "containsKeyNullKeyKeepsMapping",
        same(table.get("topic").unwrap(), &first),
    );
    outcomes.insert(
        "removeNullKeyThrowsNullPointer",
        matches!(
            table.remove_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route key"))
        ),
    );
    outcomes.insert(
        "removeNullKeyKeepsMapping",
        same(table.get("topic").unwrap(), &first),
    );
    outcomes.insert(
        "getterSeesFirst",
        same(runtime.get_existing_topic_route("topic").unwrap(), &first),
    );
    outcomes.insert(
        "putReturnsFirst",
        same(table.put("topic", second.clone()).unwrap(), &first),
    );
    outcomes.insert(
        "retainedGetterSeesReplacement",
        same(retained.get("topic").unwrap(), &second)
            && same(runtime.get_existing_topic_route("topic").unwrap(), &second),
    );
    outcomes.insert(
        "putIfAbsentKeepsSecond",
        same(
            table.put_if_absent("topic", first.clone()).unwrap(),
            &second,
        ),
    );
    outcomes.insert(
        "unequalReplaceRejected",
        !table
            .replace_if_equal("topic", &first.snapshot().unwrap(), second.clone())
            .unwrap(),
    );
    outcomes.insert(
        "equalCopyReplaceAccepted",
        table
            .replace_if_equal("topic", &equal_second.snapshot().unwrap(), first.clone())
            .unwrap(),
    );
    outcomes.insert(
        "replaceReturnsFirst",
        same(table.replace("topic", second.clone()).unwrap(), &first),
    );
    outcomes.insert(
        "unequalRemoveRejected",
        !table
            .remove_if_equal("topic", &first.snapshot().unwrap())
            .unwrap(),
    );
    outcomes.insert(
        "equalCopyRemoveAccepted",
        table
            .remove_if_equal("topic", &equal_second.snapshot().unwrap())
            .unwrap(),
    );
    outcomes.insert(
        "absentReplaceNull",
        table.replace("topic", first.clone()).unwrap().is_none(),
    );
    outcomes.insert("absentRemoveNull", table.remove("topic").unwrap().is_none());
    outcomes.insert(
        "putIfAbsentAbsentNull",
        table
            .put_if_absent("topic", first.clone())
            .unwrap()
            .is_none(),
    );
    outcomes.insert(
        "emptyKeyAccepted",
        table.put("", second.clone()).unwrap().is_none(),
    );
    outcomes.insert(
        "sizeTwo",
        table.len().unwrap() == 2 && table.contains_key("").unwrap(),
    );
    let detached = table.snapshot().unwrap();
    table.clear().unwrap();
    outcomes.insert("retainedViewSeesClear", retained.is_empty().unwrap());
    outcomes.insert(
        "getterSeesClear",
        runtime.get_existing_topic_route("topic").unwrap().is_none(),
    );
    outcomes.insert(
        "putAfterClearAccepted",
        table.put("topic", second.clone()).unwrap().is_none()
            && same(runtime.get_existing_topic_route("topic").unwrap(), &second),
    );
    let bulk_values = HashMap::from([
        ("bulk-a".to_owned(), first.clone()),
        ("bulk-b".to_owned(), second.clone()),
    ]);
    table.put_all(bulk_values).unwrap();
    outcomes.insert(
        "putAllAdds",
        same(table.get("bulk-a").unwrap(), &first)
            && same(table.get("bulk-b").unwrap(), &second)
            && table.len().unwrap() == 3,
    );
    let tree_map_oracle = &case["treeMapPutAllNullValue"];
    assert_eq!(tree_map_oracle["outcome"], "NullPointerException");
    let tree_map_order: Vec<String> =
        serde_json::from_value(tree_map_oracle["visitOrder"].clone()).unwrap();
    assert_eq!(
        tree_map_order,
        [
            "a-route-put-all-before-null-value",
            "m-route-put-all-null-value",
            "z-route-put-all-after-null-value"
        ]
    );
    let null_value_key = tree_map_oracle["nullValueKey"].as_str().unwrap();
    let tree_map_inputs: BTreeMap<_, _> = tree_map_order
        .into_iter()
        .map(|key| {
            let value = if key == null_value_key {
                None
            } else if key.starts_with('a') {
                Some(first.clone())
            } else {
                Some(second.clone())
            };
            (key, value)
        })
        .collect();
    let tree_map_rows = tree_map_inputs
        .into_iter()
        .map(|(key, value)| (Some(key), value));
    let tree_map_result = table.put_all_nullable_iter(tree_map_rows);
    outcomes.insert(
        "treeMapPutAllThrowsNullValue",
        matches!(
            tree_map_result,
            Err(RuntimeError::InvalidRoute("null topic route value"))
        ),
    );
    let before_null_value = "a-route-put-all-before-null-value";
    let after_null_value = "z-route-put-all-after-null-value";
    outcomes.insert(
        "treeMapPutAllKeepsBeforeRow",
        same(table.get(before_null_value).unwrap(), &first),
    );
    outcomes.insert(
        "treeMapPutAllSkipsAfterRow",
        table.get(after_null_value).unwrap().is_none(),
    );
    let expected_keys: Vec<String> =
        serde_json::from_value(tree_map_oracle["keysAfter"].clone()).unwrap();
    let mut actual_keys: Vec<_> = table.snapshot().unwrap().into_keys().collect();
    actual_keys.sort();
    assert_eq!(actual_keys, expected_keys);
    table.remove(before_null_value).unwrap();
    let tree_null_key_oracle = &case["treeMapPutAllNullKey"];
    assert_eq!(tree_null_key_oracle["outcome"], "NullPointerException");
    let tree_null_key_order: Vec<Option<String>> =
        serde_json::from_value(tree_null_key_oracle["visitOrder"].clone()).unwrap();
    assert_eq!(
        tree_null_key_order,
        [
            Some("a-route-put-all-before-null-key".to_owned()),
            None,
            Some("z-route-put-all-after-null-key".to_owned())
        ]
    );
    let tree_null_key_rows = tree_null_key_order.into_iter().map(|key| {
        let value = match key.as_deref() {
            Some(key) if key.starts_with('a') => Some(first.clone()),
            Some(key) if key.starts_with('z') => Some(second.clone()),
            None => Some(first.clone()),
            _ => unreachable!("fixture only contains the expected three keys"),
        };
        (key, value)
    });
    let tree_null_key_result = table.put_all_nullable_iter(tree_null_key_rows);
    outcomes.insert(
        "treeMapPutAllThrowsNullKey",
        matches!(
            tree_null_key_result,
            Err(RuntimeError::InvalidRoute("null topic route key"))
        ),
    );
    let before_null_key = "a-route-put-all-before-null-key";
    let after_null_key = "z-route-put-all-after-null-key";
    outcomes.insert(
        "treeMapPutAllNullKeyKeepsBeforeRow",
        same(table.get(before_null_key).unwrap(), &first),
    );
    outcomes.insert(
        "treeMapPutAllNullKeySkipsAfterRow",
        table.get(after_null_key).unwrap().is_none(),
    );
    let expected_keys: Vec<String> =
        serde_json::from_value(tree_null_key_oracle["keysAfter"].clone()).unwrap();
    let mut actual_keys: Vec<_> = table.snapshot().unwrap().into_keys().collect();
    actual_keys.sort();
    assert_eq!(actual_keys, expected_keys);
    table.remove(before_null_key).unwrap();
    let null_put_all_oracle = &case["nullPutAll"];
    assert_eq!(null_put_all_oracle["outcome"], "NullPointerException");
    let null_put_all_result = table.put_all_nullable_rows(None);
    outcomes.insert(
        "nullPutAllThrowsNullPointer",
        matches!(
            null_put_all_result,
            Err(RuntimeError::InvalidRoute("null topic route putAll map"))
        ),
    );
    let expected_keys: Vec<String> =
        serde_json::from_value(null_put_all_oracle["keysAfter"].clone()).unwrap();
    let mut actual_keys: Vec<_> = table.snapshot().unwrap().into_keys().collect();
    actual_keys.sort();
    assert_eq!(actual_keys, expected_keys);
    outcomes.insert("nullPutAllKeepsMappings", actual_keys.len() == 3);
    outcomes.insert(
        "containsValueEqualCopy",
        table
            .contains_value(&equal_second.snapshot().unwrap())
            .unwrap(),
    );
    let absent_route = second.snapshot().unwrap();
    broker_handle(&absent_route, 0)
        .with_mut(|broker| broker.broker_name = Some("absent-broker".into()));
    outcomes.insert(
        "containsValueAbsent",
        !table.contains_value(&absent_route).unwrap(),
    );
    let expected_routes = BTreeMap::from([
        ("topic".to_owned(), second.clone()),
        ("bulk-a".to_owned(), first.clone()),
        ("bulk-b".to_owned(), second.clone()),
    ]);
    let mut visited_routes = BTreeMap::new();
    table
        .for_each(|key, value| {
            let reentered_len = table.len().unwrap();
            visited_routes.insert(
                key.to_owned(),
                reentered_len == expected_routes.len()
                    && expected_routes
                        .get(key)
                        .is_some_and(|expected| same_route_handle(value, expected)),
            );
        })
        .unwrap();
    outcomes.insert(
        "forEachVisitsCurrentMappings",
        visited_routes.len() == expected_routes.len()
            && visited_routes.values().all(|matches| *matches),
    );
    outcomes.insert(
        "computeIfAbsentMaps",
        same(
            table
                .compute_if_absent("compute-a", |_| Some(first.clone()))
                .unwrap(),
            &first,
        ),
    );
    outcomes.insert(
        "computeIfAbsentKeepsCurrent",
        same(
            table
                .compute_if_absent("compute-a", |_| Some(second.clone()))
                .unwrap(),
            &first,
        ),
    );
    outcomes.insert(
        "computeIfAbsentNullLeavesAbsent",
        table
            .compute_if_absent("compute-null", |_| None)
            .unwrap()
            .is_none()
            && !table.contains_key("compute-null").unwrap(),
    );
    outcomes.insert(
        "computeIfPresentReplaces",
        same(
            table
                .compute_if_present("compute-a", |_, _| Some(second.clone()))
                .unwrap(),
            &second,
        ) && same(table.get("compute-a").unwrap(), &second),
    );
    outcomes.insert(
        "computeIfPresentNullRemoves",
        table
            .compute_if_present("compute-a", |_, _| None)
            .unwrap()
            .is_none()
            && !table.contains_key("compute-a").unwrap(),
    );
    outcomes.insert(
        "computeReplaces",
        same(
            table
                .compute("compute-a", |_, _| Some(first.clone()))
                .unwrap(),
            &first,
        ) && same(table.get("compute-a").unwrap(), &first),
    );
    outcomes.insert(
        "computeAbsentInserts",
        same(
            table
                .compute("compute-b", |_, _| Some(second.clone()))
                .unwrap(),
            &second,
        ),
    );
    outcomes.insert(
        "computeNullRemoves",
        table.compute("compute-b", |_, _| None).unwrap().is_none()
            && !table.contains_key("compute-b").unwrap(),
    );
    outcomes.insert(
        "computeAbsentNullLeavesAbsent",
        table
            .compute("compute-null-absent", |_, _| None)
            .unwrap()
            .is_none()
            && !table.contains_key("compute-null-absent").unwrap(),
    );
    outcomes.insert(
        "mergeCombines",
        same(
            table
                .merge("compute-a", second.clone(), |_, added| Some(added.clone()))
                .unwrap(),
            &second,
        ),
    );
    table.put("merge-remove", second.clone()).unwrap();
    outcomes.insert(
        "mergeNullRemoves",
        table
            .merge("merge-remove", first.clone(), |_, _| None)
            .unwrap()
            .is_none()
            && !table.contains_key("merge-remove").unwrap(),
    );
    outcomes.insert(
        "mergeAbsentInserts",
        same(
            table
                .merge("merge-new", first.clone(), |_, _| None)
                .unwrap(),
            &first,
        ),
    );
    table
        .replace_all(|key, _| {
            if key == "topic" {
                second.clone()
            } else {
                first.clone()
            }
        })
        .unwrap();
    outcomes.insert(
        "replaceAllUpdatesMappings",
        table.len().unwrap() == 5
            && same(table.get("topic").unwrap(), &second)
            && same(table.get("bulk-a").unwrap(), &first)
            && same(table.get("compute-a").unwrap(), &first)
            && same(table.get("merge-new").unwrap(), &first),
    );

    table.put("replaceAll-race", first.clone()).unwrap();
    let replace_all_calls = Arc::new(AtomicUsize::new(0));
    let (replace_all_entered_tx, replace_all_entered_rx) = mpsc::channel();
    let (release_replace_all_tx, release_replace_all_rx) = mpsc::channel();
    let replace_all_table = table.clone();
    let replace_all_second = second.clone();
    let replace_all_calls_for_task = replace_all_calls.clone();
    let replace_all_thread = std::thread::spawn(move || {
        replace_all_table.replace_all(move |key, old| {
            if key != "replaceAll-race" {
                return old.clone();
            }
            if replace_all_calls_for_task.fetch_add(1, Ordering::SeqCst) == 0 {
                replace_all_entered_tx.send(()).unwrap();
                release_replace_all_rx.recv().unwrap();
            }
            replace_all_second.clone()
        })
    });
    replace_all_entered_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    let replace_all_writer_table = table.clone();
    let replace_all_writer_value = second.clone();
    let (replace_all_writer_started_tx, replace_all_writer_started_rx) = mpsc::channel();
    let (replace_all_writer_finished_tx, replace_all_writer_finished_rx) = mpsc::channel();
    let replace_all_writer_thread = std::thread::spawn(move || {
        replace_all_writer_started_tx.send(()).unwrap();
        let previous = replace_all_writer_table.put("replaceAll-race", replace_all_writer_value);
        replace_all_writer_finished_tx.send(()).unwrap();
        previous
    });
    replace_all_writer_started_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    let replace_all_writer_runs = replace_all_writer_finished_rx
        .recv_timeout(Duration::from_millis(100))
        .is_ok();
    release_replace_all_tx.send(()).unwrap();
    replace_all_thread.join().unwrap().unwrap();
    let replace_all_writer_previous = replace_all_writer_thread.join().unwrap().unwrap();
    outcomes.insert(
        "replaceAllWriterRunsDuringCallback",
        replace_all_writer_runs,
    );
    outcomes.insert(
        "replaceAllCallbackRetriesAfterWriter",
        replace_all_calls.load(Ordering::SeqCst) == 2,
    );
    outcomes.insert(
        "replaceAllWriterObservedOldValue",
        same(Some(replace_all_writer_previous.unwrap()), &first),
    );
    outcomes.insert(
        "replaceAllFinalValueIsComputed",
        same(table.get("replaceAll-race").unwrap(), &second),
    );
    table.remove("replaceAll-race").unwrap();

    let before_nullable_replace_all = table.snapshot().unwrap();
    table.clear().unwrap();
    table
        .put("a-route-replace-all-before-null", first.clone())
        .unwrap();
    table
        .put("m-route-replace-all-null-result", first.clone())
        .unwrap();
    table
        .put("z-route-replace-all-after-null", first.clone())
        .unwrap();
    let null_replace_all_oracle = &case["replaceAllNullResult"];
    assert_eq!(null_replace_all_oracle["outcome"], "NullPointerException");
    let java_visit_order: Vec<String> =
        serde_json::from_value(null_replace_all_oracle["visitOrder"].clone()).unwrap();
    assert_eq!(java_visit_order.len(), 2);
    let native_order: Vec<String> = table.snapshot().unwrap().into_keys().collect();
    let failing_key = native_order[1].clone();
    let mut native_visit_order = Vec::new();
    let null_replace_all_result = table.replace_all_with_nullable_results(|key, _| {
        native_visit_order.push(key.to_owned());
        (key != failing_key).then(|| second.clone())
    });
    outcomes.insert(
        "replaceAllNullResultThrowsNullPointer",
        matches!(
            null_replace_all_result,
            Err(RuntimeError::InvalidRoute("null topic route value"))
        ),
    );
    outcomes.insert(
        "replaceAllNullResultVisitsPrefixOnly",
        native_visit_order == native_order[..2]
            && native_order
                .first()
                .is_some_and(|key| same_route_option(table.get(key).unwrap(), &second))
            && same_route_option(table.get(&failing_key).unwrap(), &first),
    );
    outcomes.insert(
        "replaceAllNullResultStopsBeforeSuffix",
        native_visit_order.len() == 2 && !native_visit_order.contains(&native_order[2]),
    );
    let expected_updated_keys: Vec<String> =
        serde_json::from_value(null_replace_all_oracle["updatedKeys"].clone()).unwrap();
    assert_eq!(expected_updated_keys, [java_visit_order[0].clone()]);
    assert_ne!(java_visit_order[0], java_visit_order[1]);
    let native_updated_keys: Vec<String> = native_order
        .iter()
        .filter(|key| same_route_option(table.get(key).unwrap(), &second))
        .cloned()
        .collect();
    assert_eq!(expected_updated_keys.len(), 1);
    assert_eq!(native_updated_keys.len(), 1);
    table.clear().unwrap();
    table.put_all(before_nullable_replace_all).unwrap();

    table.put("remapping-race", first.clone()).unwrap();
    let callback_calls = Arc::new(AtomicUsize::new(0));
    let callback_read_current = Arc::new(AtomicBool::new(false));
    let (callback_started_tx, callback_started_rx) = mpsc::channel();
    let (release_callback_tx, release_callback_rx) = mpsc::channel();
    let callback_table = table.clone();
    let callback_read_table = callback_table.clone();
    let callback_first = first.clone();
    let callback_second = second.clone();
    let callback_calls_for_task = callback_calls.clone();
    let callback_read_for_task = callback_read_current.clone();
    let remapping_thread = std::thread::spawn(move || {
        callback_table.compute("remapping-race", move |key, old| {
            callback_calls_for_task.fetch_add(1, Ordering::SeqCst);
            callback_read_for_task.store(
                old.zip(callback_read_table.get(key).ok().flatten().as_ref())
                    .is_some_and(|(old, observed)| same_route_handle(old, observed))
                    && old.is_some_and(|old| same_route_handle(old, &callback_first)),
                Ordering::SeqCst,
            );
            callback_started_tx.send(()).unwrap();
            release_callback_rx.recv().unwrap();
            Some(callback_second.clone())
        })
    });
    callback_started_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    let writer_table = table.clone();
    let writer_first = first.clone();
    let (writer_started_tx, writer_started_rx) = mpsc::channel();
    let (writer_finished_tx, writer_finished_rx) = mpsc::channel();
    let writer_thread = std::thread::spawn(move || {
        writer_started_tx.send(()).unwrap();
        let previous = writer_table.put("remapping-race", writer_first);
        writer_finished_tx.send(()).unwrap();
        previous
    });
    writer_started_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    let writer_waits = writer_finished_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err();
    release_callback_tx.send(()).unwrap();
    let remapping_result = remapping_thread.join().unwrap().unwrap();
    let writer_previous = writer_thread.join().unwrap().unwrap();
    outcomes.insert("remappingWriterWaitsForCallback", writer_waits);
    outcomes.insert(
        "remappingCallbackRunsOnce",
        callback_calls.load(Ordering::SeqCst) == 1,
    );
    outcomes.insert(
        "remappingCallbackReadsCurrent",
        callback_read_current.load(Ordering::SeqCst),
    );
    outcomes.insert(
        "remappingReturnsAndKeepsWriterValue",
        same(remapping_result, &second)
            && same(writer_previous, &second)
            && same(table.get("remapping-race").unwrap(), &first),
    );
    table.remove("remapping-race").unwrap();

    table.put("clear-race", first.clone()).unwrap();
    let clear_compute_calls = Arc::new(AtomicUsize::new(0));
    let (clear_compute_entered_tx, clear_compute_entered_rx) = mpsc::channel();
    let (release_clear_compute_tx, release_clear_compute_rx) = mpsc::channel();
    let clear_compute_table = table.clone();
    let clear_compute_calls_for_task = clear_compute_calls.clone();
    let clear_compute_second = second.clone();
    let clear_compute_thread = std::thread::spawn(move || {
        clear_compute_table.compute("clear-race", move |_, _| {
            clear_compute_calls_for_task.fetch_add(1, Ordering::SeqCst);
            clear_compute_entered_tx.send(()).unwrap();
            release_clear_compute_rx.recv().unwrap();
            Some(clear_compute_second.clone())
        })
    });
    clear_compute_entered_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    let clear_table = table.clone();
    let (clear_started_tx, clear_started_rx) = mpsc::channel();
    let (clear_finished_tx, clear_finished_rx) = mpsc::channel();
    let clear_thread = std::thread::spawn(move || {
        clear_started_tx.send(()).unwrap();
        clear_table.clear().unwrap();
        clear_finished_tx.send(()).unwrap();
    });
    clear_started_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    let clear_waits_for_compute = clear_finished_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err();
    release_clear_compute_tx.send(()).unwrap();
    let clear_compute_result = clear_compute_thread.join().unwrap().unwrap();
    clear_thread.join().unwrap();
    outcomes.insert("clearWaitsForCompute", clear_waits_for_compute);
    outcomes.insert(
        "clearComputeRunsOnce",
        clear_compute_calls.load(Ordering::SeqCst) == 1,
    );
    outcomes.insert(
        "clearComputeReturnsValue",
        same(clear_compute_result, &second),
    );
    outcomes.insert(
        "clearRemovesComputedValue",
        !table.contains_key("clear-race").unwrap(),
    );

    table.put("clear-reentrant", first.clone()).unwrap();
    let mut reentrant_clear_completes = false;
    let mut reentrant_clear_observes_absent = false;
    let reentrant_clear_result = table
        .compute("clear-reentrant", |key, _| {
            reentrant_clear_completes = table.clear().is_ok();
            reentrant_clear_observes_absent =
                reentrant_clear_completes && table.get(key).unwrap().is_none();
            Some(second.clone())
        })
        .unwrap();
    outcomes.insert("clearReentrantCompletes", reentrant_clear_completes);
    outcomes.insert(
        "clearReentrantRemovesBeforeCallbackReturns",
        reentrant_clear_observes_absent,
    );
    outcomes.insert(
        "clearReentrantComputeReturnsValue",
        same(reentrant_clear_result, &second),
    );
    outcomes.insert(
        "clearReentrantComputedValueRemains",
        table.contains_key("clear-reentrant").unwrap(),
    );

    table.put("recursive-write", first.clone()).unwrap();
    let mut recursive_write_rejected = false;
    table
        .compute("recursive-write", |_, _| {
            recursive_write_rejected = matches!(
                table.put("recursive-write", second.clone()),
                Err(RuntimeError::RecursiveRouteTableMutation)
            );
            Some(second.clone())
        })
        .unwrap();
    assert!(recursive_write_rejected);
    table.remove("recursive-write").unwrap();

    table.clear().unwrap();
    let key_view = table.key_set_view();
    table.put("key-remove", first.clone()).unwrap();
    table.put("key-keep", second.clone()).unwrap();
    table.put("key-retain", first.clone()).unwrap();
    outcomes.insert("keyViewContains", key_view.contains("key-keep").unwrap());
    outcomes.insert(
        "keyViewRemove",
        key_view.remove("key-remove").unwrap() && !table.contains_key("key-remove").unwrap(),
    );
    outcomes.insert(
        "keyViewRemoveIf",
        key_view.remove_if(|key| key == "key-keep").unwrap() > 0
            && !table.contains_key("key-keep").unwrap(),
    );
    table.put("key-all", second.clone()).unwrap();
    table.put("key-retain-all", first.clone()).unwrap();
    outcomes.insert(
        "keyViewRemoveAll",
        key_view
            .remove_all(&BTreeSet::from(["key-all".to_owned()]))
            .unwrap()
            > 0
            && !table.contains_key("key-all").unwrap(),
    );
    outcomes.insert(
        "keyViewRetainAll",
        key_view
            .retain_all(&BTreeSet::from(["key-retain-all".to_owned()]))
            .unwrap()
            > 0
            && table.len().unwrap() == 1
            && table.contains_key("key-retain-all").unwrap(),
    );

    table.clear().unwrap();
    let values_view = table.values_view();
    table.put("value-a", first.clone()).unwrap();
    table.put("value-b", second.clone()).unwrap();
    table.put("value-c", second.clone()).unwrap();
    outcomes.insert(
        "valuesViewContainsEqualCopy",
        values_view
            .contains(&equal_second.snapshot().unwrap())
            .unwrap(),
    );
    outcomes.insert(
        "valuesViewRemoveEqualCopy",
        values_view
            .remove(&equal_second.snapshot().unwrap())
            .unwrap()
            && table
                .snapshot()
                .unwrap()
                .values()
                .filter(|value| value.snapshot().unwrap() == equal_second.snapshot().unwrap())
                .count()
                == 1,
    );
    outcomes.insert(
        "valuesViewRemoveIf",
        values_view
            .remove_if(|value| value == &first.snapshot().unwrap())
            .unwrap()
            > 0
            && !table.contains_key("value-a").unwrap(),
    );
    table.clear().unwrap();
    table.put("value-all", second.clone()).unwrap();
    table.put("value-retain-drop", second.clone()).unwrap();
    outcomes.insert(
        "valuesViewRemoveAllEqual",
        values_view.remove_all(&[equal_second.clone()]).unwrap() > 0
            && !table.contains_key("value-all").unwrap()
            && !table.contains_key("value-retain-drop").unwrap(),
    );
    table.put("value-retain-drop", second.clone()).unwrap();
    table.put("value-retain-keep", first.clone()).unwrap();
    outcomes.insert(
        "valuesViewRetainAllEqual",
        values_view.retain_all(&[first.clone()]).unwrap() > 0
            && table.len().unwrap() == 1
            && table.contains_key("value-retain-keep").unwrap(),
    );

    table.clear().unwrap();
    let entry_view = table.entry_set_view();
    table.put("entry-remove", first.clone()).unwrap();
    table.put("entry-keep", second.clone()).unwrap();
    outcomes.insert(
        "entryViewContainsEqualCopy",
        entry_view
            .contains("entry-keep", &equal_second.snapshot().unwrap())
            .unwrap(),
    );
    outcomes.insert(
        "entryViewRemoveEqualCopy",
        entry_view
            .remove("entry-remove", &first.snapshot().unwrap())
            .unwrap()
            && !table.contains_key("entry-remove").unwrap(),
    );
    outcomes.insert(
        "entryViewRemoveIf",
        entry_view.remove_if(|key, _| key == "entry-keep").unwrap() > 0
            && !table.contains_key("entry-keep").unwrap(),
    );
    table.put("entry-all", second.clone()).unwrap();
    table.put("entry-retain-drop", second.clone()).unwrap();
    table.put("entry-retain-keep", first.clone()).unwrap();
    outcomes.insert(
        "entryViewRemoveAllEqual",
        entry_view
            .remove_all(&BTreeMap::from([(
                "entry-all".to_owned(),
                equal_second.clone(),
            )]))
            .unwrap()
            > 0
            && !table.contains_key("entry-all").unwrap(),
    );
    outcomes.insert(
        "entryViewRetainAllEqual",
        entry_view
            .retain_all(&BTreeMap::from([(
                "entry-retain-keep".to_owned(),
                first.clone(),
            )]))
            .unwrap()
            > 0
            && table.len().unwrap() == 1
            && table.contains_key("entry-retain-keep").unwrap(),
    );
    table.put("key-view-clear", first.clone()).unwrap();
    key_view.clear().unwrap();
    outcomes.insert("keyViewClear", table.is_empty().unwrap());
    table.put("values-view-clear", first.clone()).unwrap();
    values_view.clear().unwrap();
    outcomes.insert("valuesViewClear", table.is_empty().unwrap());
    table.put("entry-view-clear", first.clone()).unwrap();
    entry_view.clear().unwrap();
    outcomes.insert("entryViewClear", table.is_empty().unwrap());

    table.clear().unwrap();
    table.put("iterator-key", first.clone()).unwrap();
    outcomes.insert(
        "keyIteratorRemove",
        key_view.remove("iterator-key").unwrap() && table.is_empty().unwrap(),
    );
    table.put("iterator-value", first.clone()).unwrap();
    outcomes.insert(
        "valuesIteratorRemove",
        values_view.remove(&first.snapshot().unwrap()).unwrap() && table.is_empty().unwrap(),
    );
    table.put("iterator-entry", first.clone()).unwrap();
    outcomes.insert(
        "entryIteratorRemove",
        entry_view
            .remove("iterator-entry", &first.snapshot().unwrap())
            .unwrap()
            && table.is_empty().unwrap(),
    );
    table.put("entry-set-value", first.clone()).unwrap();
    let entry = entry_view.entry_snapshots().unwrap().pop().unwrap();
    outcomes.insert(
        "entrySetValueReturnsPrevious",
        same_route_handle(&entry.set_value(second.clone()).unwrap(), &first),
    );
    outcomes.insert(
        "entrySetValueUpdatesEntry",
        same_route_handle(&entry.value().unwrap(), &second),
    );
    outcomes.insert(
        "entrySetValueUpdatesTable",
        same_route_option(table.get("entry-set-value").unwrap(), &second),
    );
    table.put("entry-set-value", equal_second.clone()).unwrap();
    outcomes.insert(
        "entrySetValueEntryIsSnapshot",
        same_route_handle(&entry.value().unwrap(), &second),
    );
    outcomes.insert(
        "entrySetValueReturnsCapturedPrevious",
        same_route_handle(&entry.set_value(first.clone()).unwrap(), &second),
    );
    outcomes.insert(
        "entrySetValueReplacesCurrentMapping",
        same_route_option(table.get("entry-set-value").unwrap(), &first),
    );
    outcomes.insert(
        "entrySetValueNullThrowsNullPointer",
        matches!(
            entry.set_value_nullable(None),
            Err(RuntimeError::InvalidRoute("null topic route value"))
        ),
    );
    outcomes.insert(
        "entrySetValueNullLeavesEntry",
        same_route_handle(&entry.value().unwrap(), &first),
    );
    outcomes.insert(
        "entrySetValueNullLeavesMapping",
        same_route_option(table.get("entry-set-value").unwrap(), &first),
    );
    table.clear().unwrap();
    // Rust uses explicit live-view removals in place of Java Iterator.remove; snapshot traversal
    // stays detached and does not claim Java's weakly consistent iteration contract.
    table.put("topic", second.clone()).unwrap();
    table.remove("bulk-a").unwrap();
    table.remove("bulk-b").unwrap();
    table.remove("compute-a").unwrap();
    table.remove("merge-new").unwrap();
    let actual_outcomes = serde_json::to_value(outcomes).unwrap();
    for (name, outcome) in actual_outcomes.as_object().unwrap() {
        assert_eq!(
            outcome, &case["primitives"][name],
            "Java route table outcome {name}"
        );
    }
    assert_eq!(actual_outcomes, case["primitives"]);
    assert_eq!(detached.len(), 2);
    assert!(same(
        runtime.query_topic_route("topic").await.unwrap(),
        &second
    ));
    assert_eq!(
        runtime
            .find_broker_address_by_topic("topic")
            .unwrap()
            .as_deref(),
        case["topicAddressAfterDirectPut"].as_str()
    );
    assert!(runtime.route_snapshot().unwrap().topics.is_empty()); // No converted publication.
    let metadata = TopicRouteDataHandle::new(
        serde_json::from_value(case["metadataOnlyInput"].clone()).unwrap(),
    );
    table.put("metadata", first.clone()).unwrap();
    assert_eq!(
        table
            .remove_if_equal("metadata", &metadata.snapshot().unwrap())
            .unwrap(),
        case["metadataEqualConditionalRemoveAccepted"]
    );
    let invalid = first.snapshot().unwrap();
    broker_handle(&invalid, 0).with_mut(|broker| broker.broker_addrs = None);
    let invalid = TopicRouteDataHandle::new(invalid);
    table.put("invalid", invalid.clone()).unwrap();
    assert!(same(
        runtime.query_topic_route("invalid").await.unwrap(),
        &invalid
    ));
    assert!(runtime.find_broker_address_by_topic("invalid").is_err());
    let weak_runtime = Arc::downgrade(&runtime);
    drop(runtime);
    assert!(weak_runtime.upgrade().is_none());
    assert!(same(retained.get("invalid").unwrap(), &invalid));
    table.clear().unwrap();
    assert!(retained.is_empty().unwrap());
}

#[tokio::test]
async fn raw_topic_table_values_preserve_mutable_java_aliases() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    let expected = &fixture["runtimeTopicRouteTable"]["mutableAlias"];
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("raw-table-mutable-alias"))
        .await
        .unwrap();
    let table = runtime.topic_route_table();
    let source_alias = TopicRouteDataHandle::new(TopicRouteData::default());
    assert!(
        table
            .put("mutable-alias", source_alias.clone())
            .unwrap()
            .is_none()
    );

    source_alias
        .with_mut(|route| route.order_topic_conf = Some("broker:2".to_owned()))
        .unwrap();
    let table_value = table.get("mutable-alias").unwrap().unwrap();
    let original_mutation_visible =
        table_value.snapshot().unwrap().order_topic_conf.as_deref() == Some("broker:2");

    table_value
        .with_mut(|route| route.order_topic_conf = Some("broker:3".to_owned()))
        .unwrap();
    let table_value_mutation_visible =
        source_alias.snapshot().unwrap().order_topic_conf.as_deref() == Some("broker:3");
    assert_eq!(
        serde_json::json!({
            "putRetainsObjectIdentity": true,
            "originalMutationVisibleThroughTable": original_mutation_visible,
            "tableValueMutationVisibleThroughOriginal": table_value_mutation_visible
        }),
        *expected
    );
}
