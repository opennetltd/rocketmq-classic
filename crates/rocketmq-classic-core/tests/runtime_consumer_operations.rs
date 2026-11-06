use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, ConsumerRuntime, RuntimeError},
};
use rocketmq_classic_protocol::{
    SharedValue,
    body::ConsumerRunningInfoBody,
    heartbeat::{ConsumeType, ConsumerData},
};
use serde_json::Value;

struct ResetOwner {
    name: &'static str,
    started: tokio::sync::mpsc::UnboundedSender<&'static str>,
    release: Arc<tokio::sync::Notify>,
}

impl ConsumerRuntime for ResetOwner {
    fn heartbeat_data(&self) -> ConsumerData {
        ConsumerData::default()
    }
    fn reset_offset(
        &self,
        _topic: Option<String>,
        _offsets: Option<rocketmq_classic_core::runtime::ConsumerOffsetTable>,
    ) -> Option<rocketmq_classic_core::runtime::ConsumerOperationFuture<'_, ()>> {
        Some(Box::pin(async move {
            self.started.send(self.name).unwrap();
            self.release.notified().await;
            Ok(())
        }))
    }
}

#[tokio::test]
async fn reset_shares_java_monitor_with_registration_and_start_but_not_raw_maps() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    assert_eq!(fixture["resetOffsetSynchronized"], true);
    let mut config = ClientConfig::new("runtime-monitor");
    config.set_namesrv_addr(Some(String::new()));
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    let (started, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let first_release = Arc::new(tokio::sync::Notify::new());
    let second_release = Arc::new(tokio::sync::Notify::new());
    for (name, release) in [
        ("first", first_release.clone()),
        ("second", second_release.clone()),
    ] {
        runtime
            .consumer_table()
            .put(
                name,
                Arc::new(ResetOwner {
                    name,
                    started: started.clone(),
                    release,
                }),
            )
            .await
            .unwrap();
    }
    let first_runtime = runtime.clone();
    let first =
        tokio::spawn(async move { first_runtime.reset_offset(None, Some("first"), None).await });
    assert_eq!(receive.recv().await, Some("first"));
    let second_runtime = runtime.clone();
    let second = tokio::spawn(async move {
        second_runtime
            .reset_offset(None, Some("second"), None)
            .await
    });
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    let register_runtime = runtime.clone();
    let null_registration = tokio::spawn(async move {
        register_runtime
            .register_consumer_nullable(None, None)
            .await
    });
    let start_runtime = runtime.clone();
    let start = tokio::spawn(async move { start_runtime.start().await });
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(receive.try_recv().is_err());
    assert!(!null_registration.is_finished());
    assert!(!start.is_finished());
    // Raw map mutation and admin registration are not synchronized Java methods.
    runtime
        .consumer_table()
        .put(
            "raw",
            Arc::new(ResetOwner {
                name: "raw",
                started,
                release: Arc::new(tokio::sync::Notify::new()),
            }),
        )
        .await
        .unwrap();
    assert!(
        runtime
            .register_admin_ext(Some("admin"), Some(Arc::new(())))
            .unwrap()
    );
    assert!(!runtime.shutdown().await.unwrap());
    runtime.unregister_admin_ext(Some("admin")).unwrap();
    first_release.notify_one();
    first.await.unwrap().unwrap();
    assert_eq!(receive.recv().await, Some("second"));
    second_release.notify_one();
    second.await.unwrap().unwrap();
    assert!(null_registration.await.unwrap().unwrap().is_none());
    start.await.unwrap().unwrap();
    for group in ["first", "second", "raw"] {
        runtime.consumer_table().remove(group).await.unwrap();
    }
    runtime.shutdown().await.unwrap();
}

struct RunningInfoOwner {
    snapshot: ConsumerRunningInfoBody,
    name: String,
    consume_type: Option<ConsumeType>,
    calls: Mutex<Vec<&'static str>>,
}

impl ConsumerRuntime for RunningInfoOwner {
    fn heartbeat_data(&self) -> ConsumerData {
        panic!("Running Info must use consumeType directly")
    }

    fn consume_type(&self) -> Option<ConsumeType> {
        self.calls.lock().unwrap().push("type");
        self.consume_type
    }

    fn consumer_running_info(&self) -> Result<ConsumerRunningInfoBody, RuntimeError> {
        self.calls.lock().unwrap().push("info");
        match self.name.as_str() {
            "null-snapshot" | "snapshot-throws" => {
                Err(RuntimeError::ConsumerOperation(self.name.clone()))
            }
            _ => Ok(self.snapshot.clone()),
        }
    }
}

#[tokio::test]
async fn running_info_matches_java_enrichment_aliases_and_partial_failure_order() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let cases = fixture["runningInfo"].as_array().unwrap();
    assert_eq!(cases.len(), 9);
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let mut config = ClientConfig::new(name);
        config.set_namesrv_addr(None);
        let runtime = ClientRuntimeManager::new()
            .get_or_create(&config)
            .await
            .unwrap();
        if let Some(addresses) = case["addresses"].as_array() {
            runtime
                .remoting()
                .update_name_server_address_list(vec!["seed:9876".into()]);
            runtime.remoting().with_name_server_address_list(|list| {
                *list.unwrap() = addresses
                    .iter()
                    .map(|address| address.as_str().unwrap().into())
                    .collect();
            });
        }
        let mut snapshot = ConsumerRunningInfoBody::default();
        if name == "null-properties" {
            snapshot.set_properties(None);
        } else {
            snapshot.set_properties(Some(SharedValue::new(BTreeMap::from([
                ("owner".into(), "retained".into()),
                (
                    ConsumerRunningInfoBody::PROP_NAMESERVER_ADDR.into(),
                    "owner-ns".into(),
                ),
                (
                    ConsumerRunningInfoBody::PROP_CONSUME_TYPE.into(),
                    "owner-type".into(),
                ),
                (
                    ConsumerRunningInfoBody::PROP_CLIENT_VERSION.into(),
                    "owner-version".into(),
                ),
            ]))));
        }
        let properties = snapshot.get_properties();
        let owner = Arc::new(RunningInfoOwner {
            snapshot,
            name: name.into(),
            consume_type: match case["consumeType"].as_str() {
                Some("CONSUME_ACTIVELY") => Some(ConsumeType::ConsumeActively),
                Some("CONSUME_PASSIVELY") => Some(ConsumeType::ConsumePassively),
                None => None,
                Some(other) => panic!("unknown type: {other}"),
            },
            calls: Mutex::new(Vec::new()),
        });
        if name != "missing-group" {
            runtime
                .consumer_table()
                .put("selected", owner.clone())
                .await
                .unwrap();
        }
        let result = runtime.consumer_running_info(case["group"].as_str());
        if case["error"].is_null() {
            let result = result.unwrap();
            assert_eq!(
                result.is_some(),
                case["resultPresent"].as_bool().unwrap(),
                "{name}"
            );
            if let Some(result) = result {
                assert!(case["sameSnapshot"].as_bool().unwrap());
                assert!(case["sameProperties"].as_bool().unwrap());
                assert!(
                    result
                        .get_properties()
                        .unwrap()
                        .ptr_eq(properties.as_ref().unwrap())
                );
            }
        } else {
            assert!(result.is_err(), "{name}");
        }
        assert_eq!(
            serde_json::to_value(&*owner.calls.lock().unwrap()).unwrap(),
            case["calls"],
            "{name}"
        );
        let actual = if name == "null-snapshot" {
            Value::Null
        } else {
            serde_json::to_value(properties.as_ref().map(SharedValue::snapshot)).unwrap()
        };
        assert_eq!(actual, case["propertiesAfter"], "{name}");
        runtime.consumer_table().remove("selected").await.unwrap();
        runtime.shutdown().await.unwrap();
    }
}
