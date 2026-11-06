//! Exhaustive Java 5.5.1 fixture inventory for public basic CommandHeader models.
use std::collections::BTreeSet;

use rocketmq_classic_protocol::header::*;

mod support;
use support::{java_camel_to_snake, rust_header_method_exists};

fn java_fields(stem: &str) -> HeaderMap {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let path = format!("{root}/fixtures/remoting/{stem}.json");
    let metadata: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap())
            .unwrap_or_else(|error| panic!("{path}: {error}"));
    assert_eq!(metadata["java_client_version"], "5.5.1", "{stem}");
    serde_json::from_value(metadata["normalized_fields"]["ext_fields"].clone())
        .unwrap_or_else(|error| panic!("{stem}: {error}"))
}

// Each row names one public model, its Java->Rust
// oracle, and Rust->Java evidence when the Java verifier checks typed-*.bin.
macro_rules! inventory {
    ($visit:ident) => {
        $visit! {
            QueryConsumerOffsetRequestHeader => ("header-query-offset-request", "typed-query-offset.bin"),
            QueryConsumeTimeSpanRequestHeader => ("header-query-consume-time-span-request", "typed-query-consume-time-span-request.bin"),
            UpdateConsumerOffsetRequestHeader => ("header-update-offset-request", "typed-update-offset.bin"),
            QueryConsumerOffsetResponseHeader => ("header-query-offset-response", "typed-query-offset-response.bin"),
            SearchOffsetRequestHeader => ("header-search-offset-request", "typed-search-offset-request.bin"),
            GetMaxOffsetRequestHeader => ("header-max-offset-request", "typed-max-offset-request.bin"),
            GetMinOffsetRequestHeader => ("header-min-offset-request", "typed-min-offset-request.bin"),
            GetEarliestMsgStoretimeRequestHeader => ("header-earliest-msg-storetime-request", "typed-earliest-msg-storetime-request.bin"),
            SearchOffsetResponseHeader => ("header-search-offset-response", "typed-search-offset-response.bin"),
            GetMaxOffsetResponseHeader => ("header-max-offset-response", "typed-max-offset-response.bin"),
            GetMinOffsetResponseHeader => ("header-min-offset-response", "typed-min-offset-response.bin"),
            GetEarliestMsgStoretimeResponseHeader => ("header-earliest-msg-storetime-response", "typed-earliest-msg-storetime-response.bin"),
            PullMessageRequestHeader => ("header-pull-request", "typed-pull.bin"),
            PullMessageResponseHeader => ("header-pull-response", "typed-pull-response.bin"),
            UnregisterClientRequestHeader => ("header-unregister-request", "typed-unregister-request.bin"),
            GetRouteInfoRequestHeader => ("header-route-request", "typed-route.bin"),
            CreateTopicRequestHeader => ("header-create-topic-request", "typed-create-topic-request.bin"),
            CreateUserRequestHeader => ("header-create-user-request", "typed-create-user-request.bin"),
            CreateAclRequestHeader => ("header-create-acl-request", "typed-create-acl-request.bin"),
            DeleteAclRequestHeader => ("header-delete-acl-request", "typed-delete-acl-request.bin"),
            UpdateAclRequestHeader => ("header-update-acl-request", "typed-update-acl-request.bin"),
            UpdateUserRequestHeader => ("header-update-user-request", "typed-update-user-request.bin"),
            DeleteUserRequestHeader => ("header-delete-user-request", "typed-delete-user-request.bin"),
            GetUserRequestHeader => ("header-get-user-request", "typed-get-user-request.bin"),
            GetAclRequestHeader => ("header-get-acl-request", "typed-get-acl-request.bin"),
            ListAclsRequestHeader => ("header-list-acls-request", "typed-list-acls-request.bin"),
            ListUsersRequestHeader => ("header-list-users-request", "typed-list-users-request.bin"),
            QueryMessageRequestHeader => ("header-query-message-request", "typed-query-message-request.bin"),
            QueryMessageResponseHeader => ("header-query-message-response", "typed-query-message-response.bin"),
            QueryTopicsByConsumerRequestHeader => ("header-query-topics-by-consumer-request", "typed-query-topics-by-consumer-request.bin"),
            QuerySubscriptionByConsumerRequestHeader => ("header-query-subscription-by-consumer-request", "typed-query-subscription-by-consumer-request.bin"),
            QueryTopicConsumeByWhoRequestHeader => ("header-query-topic-consume-by-who-request", "typed-query-topic-consume-by-who-request.bin"),
            QueryCorrectionOffsetRequestHeader => ("header-query-correction-offset-request", "typed-query-correction-offset-request.bin"),
            QueryConsumeQueueRequestHeader => ("header-query-consume-queue-request", "typed-query-consume-queue-request.bin"),
            UpdateGroupForbiddenRequestHeader => ("header-update-group-forbidden-request", "typed-update-group-forbidden-request.bin"),
            GetTopicStatsInfoRequestHeader => ("header-topic-stats-info-request", "typed-topic-stats-info-request.bin"),
            GetTopicConfigRequestHeader => ("header-topic-config-request", "typed-topic-config-request.bin"),
            GetAllSubscriptionGroupRequestHeader => ("header-get-all-subscription-group-request", "typed-get-all-subscription-group-request.bin"),
            GetAllSubscriptionGroupResponseHeader => ("header-get-all-subscription-group-response", "typed-get-all-subscription-group-response.bin"),
            GetAllTopicConfigRequestHeader => ("header-get-all-topic-config-request", "typed-get-all-topic-config-request.bin"),
            GetAllTopicConfigResponseHeader => ("header-get-all-topic-config-response", "typed-get-all-topic-config-response.bin"),
            GetTopicsByClusterRequestHeader => ("header-topics-by-cluster-request", "typed-topics-by-cluster-request.bin"),
            GetSubscriptionGroupConfigRequestHeader => ("header-get-subscription-group-config-request", "typed-get-subscription-group-config-request.bin"),
            GetConsumeStatsRequestHeader => ("header-consume-stats-request", "typed-consume-stats-request.bin"),
            GetConsumeStatsInBrokerRequestHeader => ("header-consume-stats-in-broker-request", "typed-consume-stats-in-broker-request.bin"),
            ViewBrokerStatsDataRequestHeader => ("header-view-broker-stats-data-request", "typed-view-broker-stats-data-request.bin"),
            DeleteTopicRequestHeader => ("header-delete-topic-request", "typed-delete-topic-request.bin"),
            DeleteTopicFromNamesrvRequestHeader => ("header-delete-topic-from-namesrv-request", "typed-delete-topic-from-namesrv-request.bin"),
            DeleteKvConfigRequestHeader => ("header-delete-kv-config-request", "typed-delete-kv-config-request.bin"),
            GetKvConfigRequestHeader => ("header-get-kv-config-request", "typed-get-kv-config-request.bin"),
            GetKvConfigResponseHeader => ("header-get-kv-config-response", "typed-get-kv-config-response.bin"),
            WipeWritePermOfBrokerRequestHeader => ("header-wipe-write-perm-request", "typed-wipe-write-perm-request.bin"),
            AddWritePermOfBrokerRequestHeader => ("header-add-write-perm-request", "typed-add-write-perm-request.bin"),
            AddBrokerRequestHeader => ("header-add-broker-request", "typed-add-broker-request.bin"),
            RemoveBrokerRequestHeader => ("header-remove-broker-request", "typed-remove-broker-request.bin"),
            CloneGroupOffsetRequestHeader => ("header-clone-group-offset-request", "typed-clone-group-offset-request.bin"),
            WipeWritePermOfBrokerResponseHeader => ("header-wipe-write-perm-response", "typed-wipe-write-perm-response.bin"),
            AddWritePermOfBrokerResponseHeader => ("header-add-write-perm-response", "typed-add-write-perm-response.bin"),
            GetKvListByNamespaceRequestHeader => ("header-get-kv-list-by-namespace-request", "typed-get-kv-list-by-namespace-request.bin"),
            PutKvConfigRequestHeader => ("header-put-kv-config-request", "typed-put-kv-config-request.bin"),
            DeleteSubscriptionGroupRequestHeader => ("header-delete-subscription-group-request", "typed-delete-subscription-group-request.bin"),
            ViewMessageRequestHeader => ("header-view-message-request", "typed-view-message-request.bin"),
            LockBatchMqRequestHeader => ("header-lock-batch-request", "typed-lock-batch-request.bin"),
            UnlockBatchMqRequestHeader => ("header-unlock-batch-request", "typed-unlock-batch-request.bin"),
            GetConsumerListByGroupRequestHeader => ("header-consumer-list-request", "typed-consumer-list-request.bin"),
            GetConsumerRunningInfoRequestHeader => ("header-consumer-running-info-request", "typed-consumer-running-info-request.bin"),
            ConsumerSendMsgBackRequestHeader => ("header-send-back-request", "typed-send-back-request.bin"),
            UnregisterClientResponseHeader => ("header-unregister-response", "typed-unregister-response.bin"),
            EndTransactionRequestHeader => ("header-end-transaction-request", "typed-end-transaction.bin"),
            EndTransactionResponseHeader => ("header-end-transaction-response", "typed-end-transaction-response.bin"),
            CheckTransactionStateRequestHeader => ("header-check-transaction-request", "typed-check-transaction.bin"),
            CheckTransactionStateResponseHeader => ("header-check-transaction-response", "typed-check-transaction-response.bin"),
            NotificationRequestHeader => ("header-notification-request", "typed-notification.bin"),
            NotificationResponseHeader => ("header-notification-response", "typed-notification-response.bin"),
            GetLiteTopicInfoRequestHeader => ("header-lite-topic-info-request", "typed-lite-topic-info-request.bin"),
            GetLiteClientInfoRequestHeader => ("header-lite-client-info-request", "typed-lite-client-info-request.bin"),
            GetLiteGroupInfoRequestHeader => ("header-lite-group-info-request", "typed-lite-group-info-request.bin"),
            GetParentTopicInfoRequestHeader => ("header-parent-topic-info-request", "typed-parent-topic-info-request.bin"),
            ResetOffsetRequestHeader => ("header-reset-offset-request", "typed-reset-offset.bin"),
            ResetMasterFlushOffsetHeader => ("header-reset-master-flush-offset", "typed-reset-master-flush-offset.bin"),
            ExportRocksDBConfigToJsonRequestHeader => ("header-export-rocksdb-config-request", "typed-export-rocksdb-config-request.bin"),
            CheckRocksdbCqWriteProgressRequestHeader => ("header-check-rocksdb-cq-write-progress-request", "typed-check-rocksdb-cq-write-progress-request.bin"),
            GetConsumerStatusRequestHeader => ("header-consumer-status-request", "typed-consumer-status-request.bin"),
            GetConsumerConnectionListRequestHeader => ("header-consumer-connection-list-request", "typed-consumer-connection-list-request.bin"),
            PopMessageRequestHeader => ("header-pop-request", "typed-pop.bin"),
            PopMessageResponseHeader => ("header-pop-response", "typed-pop-response.bin"),
            PopLiteMessageResponseHeader => ("header-pop-lite-response", "typed-pop-lite-response.bin"),
            AckMessageRequestHeader => ("header-ack-request", "typed-ack.bin"),
            PollingInfoRequestHeader => ("header-polling-request", "typed-polling-request.bin"),
            PollingInfoResponseHeader => ("header-polling-response", "typed-polling-response.bin"),
            PopLiteMessageRequestHeader => ("header-pop-lite-request", "typed-pop-lite-request.bin"),
            LiteSubscriptionCtlRequestHeader => ("header-lite-subscription-request", "typed-lite-subscription-request.bin"),
            NotifyUnsubscribeLiteRequestHeader => ("header-notify-unsubscribe-lite-request", "typed-notify-unsubscribe-lite-request.bin"),
            ChangeInvisibleTimeRequestHeader => ("header-change-invisible-request", "typed-change-invisible.bin"),
            ChangeInvisibleTimeResponseHeader => ("header-change-invisible-response", "typed-change-invisible-response.bin"),
            HeartbeatRequestHeader => ("header-heartbeat-request", "typed-heartbeat-request.bin"),
            ConsumeMessageDirectlyResultRequestHeader => ("header-consume-directly-request", "typed-consume-directly-request.bin"),
            ReplyMessageRequestHeader => ("header-reply-request", "typed-reply.bin"),
            SendMessageResponseHeader => ("header-send-response", "typed-send-response.bin"),
            RecallMessageRequestHeader => ("header-recall-message-request", "typed-recall-request.bin"),
            RecallMessageResponseHeader => ("header-recall-message-response", "typed-recall-response.bin"),
            SendMessageRequestHeaderV2 => ("header-send-v2", "typed-send-v2.bin"),
            SendMessageRequestHeaderV1 => ("header-send-v1", "typed-send-v1.bin"),
        }
    };
}

macro_rules! cases {
    ($($model:ident => ($stem:literal, $reverse:literal),)*) => {
        #[test]
        fn every_java_header_decodes_all_fields() {
            $(
                let raw = java_fields($stem);
                let decoded = $model::decode_fields(&raw)
                    .unwrap_or_else(|error| panic!("{} / {}: {error}", stringify!($model), $stem));
                let mut encoded = HeaderMap::new();
                decoded.encode_fields(&mut encoded);
                assert_eq!(encoded, raw, "{} / {}: decode must preserve every modeled field", stringify!($model), $stem);
            )*
        }

        #[test]
        fn inventory_matches_public_source_and_reverse_evidence() {
            let source = include_str!("../src/header/basic.rs");
            let exports = include_str!("../src/header/mod.rs");
            let generator = include_str!("../../rocketmq-classic-remoting/examples/generate_fixtures.rs");
            let java = include_str!("../../../compat/java-reference/src/main/java/compat/Main.java");
            let implementations: BTreeSet<_> = source.lines().filter_map(|line| {
                line.trim().strip_prefix("impl CommandHeader for ")
                    .and_then(|tail| tail.strip_suffix(" {") )
            }).collect();
            let inventory: BTreeSet<_> = [$(stringify!($model)),*].into_iter().collect();
            assert_eq!(inventory, implementations, "update fixtures when basic.rs adds a CommandHeader");
            assert!(source.contains("pub struct UnlockBatchMqRequestHeader"));
            $(
                assert!(exports.contains(stringify!($model)), "{} must be public", stringify!($model));
                let _ = java_fields($stem);
                assert!(!$reverse.is_empty(), "missing reverse inventory entry: {}", stringify!($model));
                assert!(generator.contains($reverse), "missing Rust frame: {}", $reverse);
                assert!(java.contains(&format!("\"{}\"", $reverse)), "missing Java decoder assertion: {}", $reverse);
            )*
        }
    };
}

inventory!(cases);

#[test]
#[allow(deprecated)]
fn heartbeat_request_header_inherits_java_rpc_surface() {
    let identity: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/request-header-identity.json"
    ))
    .unwrap();
    let java = &identity["identityCases"]["HeartbeatRequestHeader"];

    let mut header = HeartbeatRequestHeader::default();
    assert_eq!(
        header.java_hash_code(),
        java["defaultHashCode"].as_i64().unwrap() as i32
    );
    assert_eq!(
        header.java_to_string(),
        java["defaultToString"].as_str().unwrap()
    );

    header.set_namespace(Some("namespace".into()));
    header.set_namespaced(Some(true));
    header.set_bname(Some("broker".into()));
    header.set_oneway(Some(false));
    assert_eq!(header.get_namespace(), Some("namespace"));
    assert_eq!(header.get_namespaced(), Some(true));
    assert_eq!(header.get_broker_name(), Some("broker"));
    assert_eq!(header.get_oneway(), Some(false));
    assert_eq!(header.get_bname(), Some("broker"));
    assert_eq!(
        header.java_hash_code(),
        java["baseHashCode"].as_i64().unwrap() as i32
    );
    assert_eq!(
        header.java_to_string(),
        java["baseToString"].as_str().unwrap()
    );
}

#[test]
#[allow(deprecated)]
fn lite_subscription_ctl_request_header_inherits_java_rpc_surface() {
    let identity: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/request-header-identity.json"
    ))
    .unwrap();
    let java = &identity["identityCases"]["LiteSubscriptionCtlRequestHeader"];

    let mut header = LiteSubscriptionCtlRequestHeader::default();
    assert_eq!(
        header.java_hash_code(),
        java["defaultHashCode"].as_i64().unwrap() as i32
    );
    assert_eq!(
        header.java_to_string(),
        java["defaultToString"].as_str().unwrap()
    );

    header.set_namespace(Some("namespace".into()));
    header.set_namespaced(Some(true));
    header.set_bname(Some("broker".into()));
    header.set_oneway(Some(false));
    assert_eq!(header.get_namespace(), Some("namespace"));
    assert_eq!(header.get_namespaced(), Some(true));
    assert_eq!(header.get_broker_name(), Some("broker"));
    assert_eq!(header.get_oneway(), Some(false));
    assert_eq!(header.get_bname(), Some("broker"));
    assert_eq!(
        header.java_hash_code(),
        java["baseHashCode"].as_i64().unwrap() as i32
    );
    assert_eq!(
        header.java_to_string(),
        java["baseToString"].as_str().unwrap()
    );

    let mut fields = HeaderMap::new();
    header.encode_fields(&mut fields);
    assert_eq!(fields.get("ns").map(String::as_str), Some("namespace"));
    assert_eq!(fields.get("nsd").map(String::as_str), Some("true"));
    assert_eq!(fields.get("bname").map(String::as_str), Some("broker"));
    assert_eq!(fields.get("oway").map(String::as_str), Some("false"));
    assert_eq!(
        LiteSubscriptionCtlRequestHeader::decode_fields(&fields).unwrap(),
        header
    );
}

#[test]
fn request_header_api_inventories_match_java_fields_and_accessors() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/request-header-identity.json"
    ))
    .unwrap();
    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");
    let inventories = fixture["apiInventories"].as_object().unwrap();
    let identities = fixture["identityCases"].as_object().unwrap();
    assert_eq!(inventories.len(), identities.len());

    for (rust_type, inventory) in inventories {
        assert_header_api_inventory_matches_rust(rust_type, inventory, source, trait_source);
    }
}

#[test]
fn remaining_request_header_api_inventories_match_java_fields_constructors_and_accessors() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/remaining-header-api.json"
    ))
    .unwrap();
    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");
    let inventories = fixture["apiInventories"].as_object().unwrap();
    assert_eq!(inventories.len(), 25);

    for (rust_type, inventory) in inventories {
        assert_header_api_inventory_matches_rust(rust_type, inventory, source, trait_source);
    }
}

#[test]
fn all_public_command_headers_have_java_api_inventory_coverage() {
    let source = include_str!("../src/header/basic.rs");
    let rust_types: BTreeSet<_> = source
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("impl CommandHeader for ")
                .and_then(|tail| tail.strip_suffix(" {"))
        })
        .collect();
    let fixtures: [serde_json::Value; 5] = [
        serde_json::from_str(include_str!(
            "../../../fixtures/protocol/request-header-identity.json"
        ))
        .unwrap(),
        serde_json::from_str(include_str!(
            "../../../fixtures/protocol/header-response-identity.json"
        ))
        .unwrap(),
        serde_json::from_str(include_str!(
            "../../../fixtures/protocol/remaining-header-api.json"
        ))
        .unwrap(),
        serde_json::from_str(include_str!(
            "../../../fixtures/remoting/offset-boundary-header-decode-modes.json"
        ))
        .unwrap(),
        serde_json::from_str(include_str!(
            "../../../fixtures/remoting/query-admin-header-decode-modes.json"
        ))
        .unwrap(),
    ];
    let mut java_types = BTreeSet::new();
    for fixture in &fixtures {
        for java_type in fixture["apiInventories"].as_object().unwrap().keys() {
            if java_type == "ExportRocksDBConfigType" {
                continue;
            }
            let rust_type = match java_type.as_str() {
                "DeleteKVConfigRequestHeader" => "DeleteKvConfigRequestHeader",
                other => other,
            };
            java_types.insert(rust_type);
        }
    }
    assert_eq!(rust_types.len(), 103);
    assert_eq!(
        java_types, rust_types,
        "Java API inventories must cover every public Rust CommandHeader"
    );
}

fn assert_header_api_inventory_matches_rust(
    rust_type: &str,
    inventory: &serde_json::Value,
    source: &str,
    trait_source: &str,
) {
    assert_eq!(
        inventory["publicFields"],
        serde_json::json!([]),
        "Java {rust_type} must not expose public fields"
    );
    let declaration = format!("pub struct {rust_type} {{");
    let (before_struct, struct_tail) = source
        .split_once(&declaration)
        .unwrap_or_else(|| panic!("missing Rust header {rust_type}"));
    let struct_body = struct_tail.split_once("\n}\n").unwrap().0;
    assert!(
        struct_body
            .lines()
            .all(|line| !line.trim_start().starts_with("pub ")),
        "Rust {rust_type} must not expose public fields"
    );

    let constructors = inventory["publicConstructors"].as_array().unwrap();
    assert!(
        constructors
            .iter()
            .filter_map(serde_json::Value::as_str)
            .any(|signature| signature.ends_with("()")),
        "Java {rust_type} must have a public no-argument constructor"
    );
    let derives_default = before_struct
        .rsplit_once("#[derive")
        .is_some_and(|(_, derive)| derive.contains("Default"));
    let implements_default = source.contains(&format!("impl Default for {rust_type} {{"));
    assert!(
        derives_default || implements_default,
        "Rust {rust_type} must provide Default for Java's no-argument construction"
    );

    let methods = inventory["callablePublicMethods"].as_array().unwrap();
    for signature in methods.iter().filter_map(serde_json::Value::as_str) {
        let (prefix, parameters) = signature.split_once('(').unwrap();
        let name = prefix
            .split_whitespace()
            .last()
            .unwrap()
            .rsplit('.')
            .next()
            .unwrap();
        if matches!(name, "encode" | "decode" | "checkFields") {
            let rust_method = match name {
                "encode" => "encode_fields",
                "decode" => "decode_fields",
                _ => "check_fields",
            };
            assert!(
                trait_source.contains(&format!("fn {rust_method}(")),
                "Java {rust_type}::{name} has no Rust CommandHeader::{rust_method}"
            );
            continue;
        }
        if matches!(
            name,
            "getAndCheckNotNull" | "writeIfNotNull" | "writeInt" | "writeLong"
        ) {
            let rust_method = java_camel_to_snake(name);
            assert!(
                rust_header_method_exists(source, trait_source, rust_type, &rust_method),
                "Java {rust_type}::{name} has no Rust FastCodesHeader::{rust_method}"
            );
            continue;
        }
        if !(name.starts_with("get") || name.starts_with("set") || name.starts_with("is")) {
            continue;
        }
        let parameter_count = if parameters.starts_with(')') {
            0
        } else {
            parameters.matches(',').count() + 1
        };
        if (name.starts_with("get") || name.starts_with("is")) && parameter_count != 0 {
            continue;
        }
        if name.starts_with("set") && parameter_count != 1 {
            continue;
        }
        let rust_method = java_camel_to_snake(name);
        assert!(
            rust_header_method_exists(source, trait_source, rust_type, &rust_method),
            "Java {rust_type}::{name} has no Rust {rust_type}::{rust_method}"
        );
    }
}

#[test]
fn get_topics_by_cluster_header_api_matches_java_inventory() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    let inventory = &fixture["apiInventories"]["GetTopicsByClusterRequestHeader"];
    assert_eq!(inventory["publicFields"], serde_json::json!([]));
    assert_eq!(
        inventory["publicConstructors"],
        serde_json::json!([
            "org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader()"
        ])
    );
    assert_eq!(
        inventory["declaredPublicMethods"],
        serde_json::json!([
            "java.lang.String org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader.getCluster()",
            "void org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader.checkFields()",
            "void org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader.setCluster(java.lang.String)"
        ])
    );

    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");
    assert!(rust_header_method_exists(
        source,
        trait_source,
        "GetTopicsByClusterRequestHeader",
        "get_cluster"
    ));
    assert!(rust_header_method_exists(
        source,
        trait_source,
        "GetTopicsByClusterRequestHeader",
        "set_cluster"
    ));
    assert!(rust_header_method_exists(
        source,
        trait_source,
        "GetTopicsByClusterRequestHeader",
        "check_fields"
    ));
}

#[test]
fn get_subscription_group_config_header_api_matches_java_inventory() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    let inventory = &fixture["apiInventories"]["GetSubscriptionGroupConfigRequestHeader"];
    assert_eq!(inventory["publicFields"], serde_json::json!([]));
    assert_eq!(
        inventory["publicConstructors"],
        serde_json::json!([
            "org.apache.rocketmq.remoting.protocol.header.GetSubscriptionGroupConfigRequestHeader()"
        ])
    );
    assert_eq!(
        inventory["declaredPublicMethods"],
        serde_json::json!([
            "java.lang.String org.apache.rocketmq.remoting.protocol.header.GetSubscriptionGroupConfigRequestHeader.getGroup()",
            "void org.apache.rocketmq.remoting.protocol.header.GetSubscriptionGroupConfigRequestHeader.checkFields()",
            "void org.apache.rocketmq.remoting.protocol.header.GetSubscriptionGroupConfigRequestHeader.setGroup(java.lang.String)"
        ])
    );
    let callable_methods = inventory["callablePublicMethods"].as_array().unwrap();
    for java_method in [
        "getNamespace",
        "setNamespace",
        "getNamespaced",
        "setNamespaced",
        "getBname",
        "setBname",
        "getBrokerName",
        "setBrokerName",
        "getOneway",
        "setOneway",
    ] {
        assert!(callable_methods.iter().any(|method| {
            method
                .as_str()
                .is_some_and(|signature| signature.contains(&format!(".{java_method}(")))
        }));
    }

    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");
    for method in [
        "get_group",
        "set_group",
        "check_fields",
        "get_namespace",
        "set_namespace",
        "get_namespaced",
        "set_namespaced",
        "get_bname",
        "set_bname",
        "get_broker_name",
        "set_broker_name",
        "get_oneway",
        "set_oneway",
    ] {
        assert!(rust_header_method_exists(
            source,
            trait_source,
            "GetSubscriptionGroupConfigRequestHeader",
            method
        ));
    }
}

#[test]
fn get_all_subscription_group_headers_match_java_public_api_inventory() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");
    for (rust_type, methods) in [
        (
            "GetAllSubscriptionGroupRequestHeader",
            [
                "get_group_seq",
                "set_group_seq",
                "get_data_version",
                "set_data_version",
                "get_max_group_num",
                "set_max_group_num",
            ]
            .as_slice(),
        ),
        (
            "GetAllSubscriptionGroupResponseHeader",
            ["get_total_group_num", "set_total_group_num"].as_slice(),
        ),
    ] {
        let inventory = &fixture["apiInventories"][rust_type];
        assert_eq!(inventory["publicFields"], serde_json::json!([]));
        let declared = inventory["declaredPublicMethods"].as_array().unwrap();
        assert!(
            declared
                .iter()
                .filter_map(serde_json::Value::as_str)
                .any(|signature| signature.ends_with(&format!(".{rust_type}.checkFields()"))),
            "{rust_type} lacks Java CommandCustomHeader.checkFields"
        );
        for method in methods {
            assert!(rust_header_method_exists(
                source,
                trait_source,
                rust_type,
                method
            ));
            let mut parts = method.split('_');
            let mut java_method = parts.next().unwrap().to_owned();
            for part in parts {
                let mut characters = part.chars();
                if let Some(first) = characters.next() {
                    java_method.extend(first.to_uppercase());
                    java_method.extend(characters);
                }
            }
            assert!(
                declared.iter().any(|signature| {
                    signature
                        .as_str()
                        .is_some_and(|signature| signature.contains(&format!(".{java_method}(")))
                }),
                "Java {rust_type} lacks {java_method}"
            );
        }
    }
}

#[test]
fn query_consume_queue_header_matches_java_public_api_inventory() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    let inventory = &fixture["apiInventories"]["QueryConsumeQueueRequestHeader"];
    assert_eq!(inventory["publicFields"], serde_json::json!([]));
    let declared = inventory["declaredPublicMethods"].as_array().unwrap();
    for java_method in [
        "getTopic",
        "setTopic",
        "getQueueId",
        "setQueueId",
        "getIndex",
        "setIndex",
        "getCount",
        "setCount",
        "getConsumerGroup",
        "setConsumerGroup",
        "checkFields",
    ] {
        assert!(
            declared.iter().any(|signature| {
                signature
                    .as_str()
                    .is_some_and(|signature| signature.contains(&format!(".{java_method}(")))
            }),
            "Java QueryConsumeQueueRequestHeader lacks {java_method}"
        );
    }
    let callable = inventory["callablePublicMethods"].as_array().unwrap();
    for java_method in [
        "getNamespace",
        "setNamespace",
        "getNamespaced",
        "setNamespaced",
        "getBrokerName",
        "setBrokerName",
        "getOneway",
        "setOneway",
        "getLo",
        "setLo",
    ] {
        assert!(
            callable.iter().any(|signature| {
                signature
                    .as_str()
                    .is_some_and(|signature| signature.contains(&format!(".{java_method}(")))
            }),
            "Java QueryConsumeQueueRequestHeader lacks inherited {java_method}"
        );
    }

    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");
    for method in [
        "get_topic",
        "set_topic",
        "get_queue_id",
        "set_queue_id",
        "get_index",
        "set_index",
        "get_count",
        "set_count",
        "get_consumer_group",
        "set_consumer_group",
        "check_fields",
        "get_namespace",
        "set_namespace",
        "get_namespaced",
        "set_namespaced",
        "get_broker_name",
        "set_broker_name",
        "get_oneway",
        "set_oneway",
        "get_lo",
        "set_lo",
    ] {
        assert!(rust_header_method_exists(
            source,
            trait_source,
            "QueryConsumeQueueRequestHeader",
            method
        ));
    }
}

#[test]
fn reset_master_flush_offset_header_api_matches_java_inventory() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    let inventory = &fixture["apiInventories"]["ResetMasterFlushOffsetHeader"];
    assert_eq!(inventory["publicFields"], serde_json::json!([]));
    assert_eq!(
        inventory["publicConstructors"],
        serde_json::json!([
            "org.apache.rocketmq.remoting.protocol.header.ResetMasterFlushOffsetHeader()"
        ])
    );
    assert_eq!(
        inventory["declaredPublicMethods"],
        serde_json::json!([
            "java.lang.Long org.apache.rocketmq.remoting.protocol.header.ResetMasterFlushOffsetHeader.getMasterFlushOffset()",
            "void org.apache.rocketmq.remoting.protocol.header.ResetMasterFlushOffsetHeader.checkFields()",
            "void org.apache.rocketmq.remoting.protocol.header.ResetMasterFlushOffsetHeader.setMasterFlushOffset(java.lang.Long)"
        ])
    );

    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");
    for method in [
        "get_master_flush_offset",
        "set_master_flush_offset",
        "check_fields",
    ] {
        assert!(rust_header_method_exists(
            source,
            trait_source,
            "ResetMasterFlushOffsetHeader",
            method
        ));
    }
}

#[test]
fn export_rocksdb_config_header_and_enum_api_match_java_inventory() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    let header = &fixture["apiInventories"]["ExportRocksDBConfigToJsonRequestHeader"];
    assert_eq!(header["publicFields"], serde_json::json!([]));
    let header_methods = header["declaredPublicMethods"].as_array().unwrap();
    for java_method in [
        "fetchConfigType",
        "checkFields",
        "getConfigType",
        "updateConfigType",
        "setConfigType",
    ] {
        assert!(header_methods.iter().any(|method| {
            method
                .as_str()
                .is_some_and(|signature| signature.contains(&format!(".{java_method}(")))
        }));
    }

    let enum_api = &fixture["apiInventories"]["ExportRocksDBConfigType"];
    let enum_methods = enum_api["declaredPublicMethods"].as_array().unwrap();
    for java_method in [
        "getConfigTypeByName",
        "fromString",
        "toString",
        "getTypeName",
    ] {
        assert!(enum_methods.iter().any(|method| {
            method
                .as_str()
                .is_some_and(|signature| signature.contains(&format!(".{java_method}(")))
        }));
    }

    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");
    for method in [
        "fetch_config_type",
        "update_config_type",
        "get_config_type",
        "set_config_type",
        "check_fields",
    ] {
        assert!(rust_header_method_exists(
            source,
            trait_source,
            "ExportRocksDBConfigToJsonRequestHeader",
            method
        ));
    }
    for method in [
        "get_config_type_by_name",
        "from_string",
        "to_string",
        "get_type_name",
        "value_of",
        "name",
        "java_to_string",
    ] {
        assert!(
            rust_header_method_exists(source, trait_source, "ExportRocksDBConfigType", method),
            "missing Rust enum API method {method}"
        );
    }
    assert!(source.contains("pub const fn values()"));
    assert!(source.contains("pub const fn ordinal(self)"));
}

#[test]
fn check_rocksdb_cq_write_progress_header_matches_java_public_api_inventory() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    let api = &fixture["apiInventories"]["CheckRocksdbCqWriteProgressRequestHeader"];
    assert_eq!(api["publicFields"], serde_json::json!([]));
    let declared = api["declaredPublicMethods"].as_array().unwrap();
    for method in [
        "getTopic",
        "setTopic",
        "getCheckStoreTime",
        "setCheckStoreTime",
        "checkFields",
    ] {
        assert!(
            declared.iter().any(|signature| {
                signature
                    .as_str()
                    .is_some_and(|signature| signature.contains(&format!(".{method}(")))
            }),
            "missing Java method {method}"
        );
    }

    let source = include_str!("../src/header/basic.rs");
    let exports = include_str!("../src/header/mod.rs");
    for method in [
        "get_topic",
        "set_topic",
        "get_check_store_time",
        "set_check_store_time",
    ] {
        assert!(rust_header_method_exists(
            source,
            exports,
            "CheckRocksdbCqWriteProgressRequestHeader",
            method
        ));
    }
    assert!(source.contains("impl CommandHeader for CheckRocksdbCqWriteProgressRequestHeader"));
}

#[test]
fn write_permission_headers_match_java_public_api_inventory() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");
    for (java_type, rust_type, methods) in [
        (
            "WipeWritePermOfBrokerRequestHeader",
            "WipeWritePermOfBrokerRequestHeader",
            ["get_broker_name", "set_broker_name", "check_fields"],
        ),
        (
            "AddWritePermOfBrokerRequestHeader",
            "AddWritePermOfBrokerRequestHeader",
            ["get_broker_name", "set_broker_name", "check_fields"],
        ),
        (
            "WipeWritePermOfBrokerResponseHeader",
            "WipeWritePermOfBrokerResponseHeader",
            [
                "get_wipe_topic_count",
                "set_wipe_topic_count",
                "check_fields",
            ],
        ),
        (
            "AddWritePermOfBrokerResponseHeader",
            "AddWritePermOfBrokerResponseHeader",
            ["get_add_topic_count", "set_add_topic_count", "check_fields"],
        ),
    ] {
        let api = &fixture["apiInventories"][java_type];
        assert_eq!(api["publicFields"], serde_json::json!([]), "{java_type}");
        let declared = api["declaredPublicMethods"].as_array().unwrap();
        let java_method_names = if java_type.ends_with("RequestHeader") {
            ["getBrokerName", "setBrokerName", "checkFields"]
        } else if java_type.starts_with("Wipe") {
            ["getWipeTopicCount", "setWipeTopicCount", "checkFields"]
        } else {
            ["getAddTopicCount", "setAddTopicCount", "checkFields"]
        };
        for java_method in java_method_names {
            assert!(
                declared.iter().any(|method| {
                    method
                        .as_str()
                        .is_some_and(|signature| signature.contains(&format!(".{java_method}(")))
                }),
                "missing Java API {java_type}.{java_method}"
            );
        }
        for method in methods {
            assert!(rust_header_method_exists(
                source,
                trait_source,
                rust_type,
                method
            ));
        }
    }
}

#[test]
fn broker_management_headers_match_java_public_api_inventory() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");
    struct Case {
        fixture_key: &'static str,
        java_type: &'static str,
        rust_type: &'static str,
        methods: &'static [&'static str],
        java_methods: &'static [&'static str],
    }
    let cases = [
        Case {
            fixture_key: "addBrokerRequestHeaderApi",
            java_type: "AddBrokerRequestHeader",
            rust_type: "AddBrokerRequestHeader",
            methods: &["get_config_path", "set_config_path", "check_fields"],
            java_methods: &["getConfigPath", "setConfigPath", "checkFields"],
        },
        Case {
            fixture_key: "removeBrokerRequestHeaderApi",
            java_type: "RemoveBrokerRequestHeader",
            rust_type: "RemoveBrokerRequestHeader",
            methods: &[
                "get_broker_cluster_name",
                "set_broker_cluster_name",
                "get_broker_name",
                "set_broker_name",
                "get_broker_id",
                "set_broker_id",
                "check_fields",
            ],
            java_methods: &[
                "getBrokerClusterName",
                "setBrokerClusterName",
                "getBrokerName",
                "setBrokerName",
                "getBrokerId",
                "setBrokerId",
                "checkFields",
            ],
        },
        Case {
            fixture_key: "cloneGroupOffsetRequestHeaderApi",
            java_type: "CloneGroupOffsetRequestHeader",
            rust_type: "CloneGroupOffsetRequestHeader",
            methods: &[
                "get_src_group",
                "set_src_group",
                "get_dest_group",
                "set_dest_group",
                "get_topic",
                "set_topic",
                "is_offline",
                "set_offline",
                "check_fields",
            ],
            java_methods: &[
                "getSrcGroup",
                "setSrcGroup",
                "getDestGroup",
                "setDestGroup",
                "getTopic",
                "setTopic",
                "isOffline",
                "setOffline",
                "checkFields",
            ],
        },
        Case {
            fixture_key: "viewBrokerStatsDataRequestHeaderApi",
            java_type: "ViewBrokerStatsDataRequestHeader",
            rust_type: "ViewBrokerStatsDataRequestHeader",
            methods: &[
                "get_stats_name",
                "set_stats_name",
                "get_stats_key",
                "set_stats_key",
            ],
            java_methods: &[
                "getStatsName",
                "setStatsName",
                "getStatsKey",
                "setStatsKey",
                "checkFields",
            ],
        },
    ];
    for case in &cases {
        let declared = fixture[case.fixture_key]["declaredPublicMethods"]
            .as_array()
            .unwrap();
        for java_method in case.java_methods {
            assert!(
                declared.iter().any(|method| {
                    method
                        .as_str()
                        .is_some_and(|signature| signature.contains(&format!(".{java_method}(")))
                }),
                "missing Java API {}.{java_method}",
                case.java_type
            );
        }
        for method in case.methods {
            assert!(rust_header_method_exists(
                source,
                trait_source,
                case.rust_type,
                method
            ));
        }
        assert!(source.contains(&format!("impl CommandHeader for {}", case.rust_type)));
    }
}

#[test]
fn name_server_kv_headers_keep_java_nullable_fields_and_names() {
    let mut get = GetKvConfigRequestHeader::default();
    assert_eq!(get.get_namespace(), None);
    assert_eq!(get.get_key(), None);
    get.set_namespace(Some("tenant".into()));
    get.set_key(Some("key".into()));
    let mut get_fields = HeaderMap::new();
    get.encode_fields(&mut get_fields);
    assert_eq!(
        get_fields,
        HeaderMap::from([
            ("key".into(), "key".into()),
            ("namespace".into(), "tenant".into()),
        ])
    );
    let decoded = GetKvConfigRequestHeader::decode_fields(&get_fields).unwrap();
    assert_eq!(decoded.get_namespace(), Some("tenant"));
    assert_eq!(decoded.get_key(), Some("key"));

    let mut put = PutKvConfigRequestHeader::default();
    assert_eq!(put.get_namespace(), None);
    assert_eq!(put.get_key(), None);
    assert_eq!(put.get_value(), None);
    put.set_namespace(Some("tenant".into()));
    put.set_key(Some("key".into()));
    put.set_value(Some("value".into()));
    let mut put_fields = HeaderMap::new();
    put.encode_fields(&mut put_fields);
    assert_eq!(put_fields.get("value").map(String::as_str), Some("value"));

    let mut list = GetKvListByNamespaceRequestHeader::default();
    assert_eq!(list.get_namespace(), None);
    list.set_namespace(Some("tenant".into()));
    let mut list_fields = HeaderMap::new();
    list.encode_fields(&mut list_fields);
    assert_eq!(
        list_fields,
        HeaderMap::from([("namespace".into(), "tenant".into())])
    );

    let mut response = GetKvConfigResponseHeader::default();
    assert_eq!(response.get_value(), None);
    response.set_value(Some("value".into()));
    let mut response_fields = HeaderMap::new();
    response.encode_fields(&mut response_fields);
    assert_eq!(
        response_fields,
        HeaderMap::from([("value".into(), "value".into())])
    );
    assert_eq!(
        GetKvConfigResponseHeader::decode_fields(&response_fields)
            .unwrap()
            .get_value(),
        Some("value")
    );
}

#[test]
fn consume_stats_topic_list_helpers_match_java_header_behavior() {
    let mut header =
        GetConsumeStatsRequestHeader::decode_fields(&java_fields("header-consume-stats-request"))
            .unwrap();
    assert_eq!(header.fetch_topic_list(), ["orders", "payments"]);

    let current = header.get_topic_list().map(str::to_owned);
    header.update_topic_list(None);
    header.update_topic_list(Some(&[]));
    assert_eq!(header.get_topic_list(), current.as_deref());

    let topics = [Some("orders".to_owned()), None, Some("payments".to_owned())];
    header.update_topic_list(Some(&topics));
    assert_eq!(header.get_topic_list(), Some("orders;null;payments;"));
    assert_eq!(header.fetch_topic_list(), ["orders", "null", "payments"]);
}
