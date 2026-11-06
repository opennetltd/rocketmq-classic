//! Java-compatible request and response code descriptions.
//!
//! Java's `RemotingHelper` lowercases the corresponding `RequestCode` or
//! `ResponseCode` field name with the process default locale and falls back to
//! the decimal code for unknown values.

use rocketmq_classic_protocol::codes;
use std::sync::OnceLock;

/// Returns the Java default-locale lowercase `RequestCode` field name, or the
/// decimal code.
pub fn get_request_code_desc(code: i32) -> String {
    let description = match code {
        codes::request::SEND_MESSAGE => "send_message",
        codes::request::PULL_MESSAGE => "pull_message",
        codes::request::QUERY_MESSAGE => "query_message",
        codes::request::QUERY_BROKER_OFFSET => "query_broker_offset",
        codes::request::QUERY_CONSUMER_OFFSET => "query_consumer_offset",
        codes::request::UPDATE_CONSUMER_OFFSET => "update_consumer_offset",
        codes::request::UPDATE_AND_CREATE_TOPIC => "update_and_create_topic",
        codes::request::UPDATE_AND_CREATE_TOPIC_LIST => "update_and_create_topic_list",
        codes::request::GET_ALL_TOPIC_CONFIG => "get_all_topic_config",
        codes::request::GET_TOPIC_CONFIG_LIST => "get_topic_config_list",
        codes::request::GET_TOPIC_NAME_LIST => "get_topic_name_list",
        codes::request::UPDATE_BROKER_CONFIG => "update_broker_config",
        codes::request::GET_BROKER_CONFIG => "get_broker_config",
        codes::request::TRIGGER_DELETE_FILES => "trigger_delete_files",
        codes::request::GET_BROKER_RUNTIME_INFO => "get_broker_runtime_info",
        codes::request::SEARCH_OFFSET_BY_TIMESTAMP => "search_offset_by_timestamp",
        codes::request::GET_MAX_OFFSET => "get_max_offset",
        codes::request::GET_MIN_OFFSET => "get_min_offset",
        codes::request::GET_EARLIEST_MSG_STORETIME => "get_earliest_msg_storetime",
        codes::request::VIEW_MESSAGE_BY_ID => "view_message_by_id",
        codes::request::HEART_BEAT => "heart_beat",
        codes::request::UNREGISTER_CLIENT => "unregister_client",
        codes::request::CONSUMER_SEND_MSG_BACK => "consumer_send_msg_back",
        codes::request::END_TRANSACTION => "end_transaction",
        codes::request::GET_CONSUMER_LIST_BY_GROUP => "get_consumer_list_by_group",
        codes::request::CHECK_TRANSACTION_STATE => "check_transaction_state",
        codes::request::NOTIFY_CONSUMER_IDS_CHANGED => "notify_consumer_ids_changed",
        codes::request::LOCK_BATCH_MQ => "lock_batch_mq",
        codes::request::UNLOCK_BATCH_MQ => "unlock_batch_mq",
        codes::request::GET_ALL_CONSUMER_OFFSET => "get_all_consumer_offset",
        codes::request::GET_ALL_DELAY_OFFSET => "get_all_delay_offset",
        codes::request::CHECK_CLIENT_CONFIG => "check_client_config",
        codes::request::GET_CLIENT_CONFIG => "get_client_config",
        codes::request::GET_TIMER_CHECK_POINT => "get_timer_check_point",
        codes::request::GET_TIMER_METRICS => "get_timer_metrics",
        codes::request::POP_MESSAGE => "pop_message",
        codes::request::ACK_MESSAGE => "ack_message",
        codes::request::BATCH_ACK_MESSAGE => "batch_ack_message",
        codes::request::PEEK_MESSAGE => "peek_message",
        codes::request::CHANGE_MESSAGE_INVISIBLETIME => "change_message_invisibletime",
        codes::request::NOTIFICATION => "notification",
        codes::request::POLLING_INFO => "polling_info",
        codes::request::POP_ROLLBACK => "pop_rollback",
        codes::request::POP_LITE_MESSAGE => "pop_lite_message",
        codes::request::LITE_SUBSCRIPTION_CTL => "lite_subscription_ctl",
        codes::request::ACK_LITE_MESSAGE => "ack_lite_message",
        codes::request::NOTIFY_UNSUBSCRIBE_LITE => "notify_unsubscribe_lite",
        codes::request::GET_BROKER_LITE_INFO => "get_broker_lite_info",
        codes::request::GET_PARENT_TOPIC_INFO => "get_parent_topic_info",
        codes::request::GET_LITE_TOPIC_INFO => "get_lite_topic_info",
        codes::request::GET_LITE_CLIENT_INFO => "get_lite_client_info",
        codes::request::GET_LITE_GROUP_INFO => "get_lite_group_info",
        codes::request::TRIGGER_LITE_DISPATCH => "trigger_lite_dispatch",
        codes::request::PUT_KV_CONFIG => "put_kv_config",
        codes::request::GET_KV_CONFIG => "get_kv_config",
        codes::request::DELETE_KV_CONFIG => "delete_kv_config",
        codes::request::REGISTER_BROKER => "register_broker",
        codes::request::UNREGISTER_BROKER => "unregister_broker",
        codes::request::GET_ROUTEINFO_BY_TOPIC => "get_routeinfo_by_topic",
        codes::request::GET_BROKER_CLUSTER_INFO => "get_broker_cluster_info",
        codes::request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP => {
            "update_and_create_subscriptiongroup"
        }
        codes::request::GET_ALL_SUBSCRIPTIONGROUP_CONFIG => "get_all_subscriptiongroup_config",
        codes::request::GET_TOPIC_STATS_INFO => "get_topic_stats_info",
        codes::request::GET_CONSUMER_CONNECTION_LIST => "get_consumer_connection_list",
        codes::request::GET_PRODUCER_CONNECTION_LIST => "get_producer_connection_list",
        codes::request::WIPE_WRITE_PERM_OF_BROKER => "wipe_write_perm_of_broker",
        codes::request::GET_ALL_TOPIC_LIST_FROM_NAMESERVER => "get_all_topic_list_from_nameserver",
        codes::request::DELETE_SUBSCRIPTIONGROUP => "delete_subscriptiongroup",
        codes::request::GET_CONSUME_STATS => "get_consume_stats",
        codes::request::SUSPEND_CONSUMER => "suspend_consumer",
        codes::request::RESUME_CONSUMER => "resume_consumer",
        codes::request::RESET_CONSUMER_OFFSET_IN_CONSUMER => "reset_consumer_offset_in_consumer",
        codes::request::RESET_CONSUMER_OFFSET_IN_BROKER => "reset_consumer_offset_in_broker",
        codes::request::ADJUST_CONSUMER_THREAD_POOL => "adjust_consumer_thread_pool",
        codes::request::WHO_CONSUME_THE_MESSAGE => "who_consume_the_message",
        codes::request::DELETE_TOPIC_IN_BROKER => "delete_topic_in_broker",
        codes::request::DELETE_TOPIC_IN_NAMESRV => "delete_topic_in_namesrv",
        codes::request::REGISTER_TOPIC_IN_NAMESRV => "register_topic_in_namesrv",
        codes::request::GET_KVLIST_BY_NAMESPACE => "get_kvlist_by_namespace",
        codes::request::RESET_CONSUMER_CLIENT_OFFSET => "reset_consumer_client_offset",
        codes::request::GET_CONSUMER_STATUS_FROM_CLIENT => "get_consumer_status_from_client",
        codes::request::INVOKE_BROKER_TO_RESET_OFFSET => "invoke_broker_to_reset_offset",
        codes::request::INVOKE_BROKER_TO_GET_CONSUMER_STATUS => {
            "invoke_broker_to_get_consumer_status"
        }
        codes::request::QUERY_TOPIC_CONSUME_BY_WHO => "query_topic_consume_by_who",
        codes::request::GET_TOPICS_BY_CLUSTER => "get_topics_by_cluster",
        codes::request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST => {
            "update_and_create_subscriptiongroup_list"
        }
        codes::request::QUERY_TOPICS_BY_CONSUMER => "query_topics_by_consumer",
        codes::request::QUERY_SUBSCRIPTION_BY_CONSUMER => "query_subscription_by_consumer",
        codes::request::REGISTER_FILTER_SERVER => "register_filter_server",
        codes::request::REGISTER_MESSAGE_FILTER_CLASS => "register_message_filter_class",
        codes::request::QUERY_CONSUME_TIME_SPAN => "query_consume_time_span",
        codes::request::GET_SYSTEM_TOPIC_LIST_FROM_NS => "get_system_topic_list_from_ns",
        codes::request::GET_SYSTEM_TOPIC_LIST_FROM_BROKER => "get_system_topic_list_from_broker",
        codes::request::CLEAN_EXPIRED_CONSUMEQUEUE => "clean_expired_consumequeue",
        codes::request::GET_CONSUMER_RUNNING_INFO => "get_consumer_running_info",
        codes::request::QUERY_CORRECTION_OFFSET => "query_correction_offset",
        codes::request::CONSUME_MESSAGE_DIRECTLY => "consume_message_directly",
        codes::request::SEND_MESSAGE_V2 => "send_message_v2",
        codes::request::GET_UNIT_TOPIC_LIST => "get_unit_topic_list",
        codes::request::GET_HAS_UNIT_SUB_TOPIC_LIST => "get_has_unit_sub_topic_list",
        codes::request::GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST => "get_has_unit_sub_ununit_topic_list",
        codes::request::CLONE_GROUP_OFFSET => "clone_group_offset",
        codes::request::VIEW_BROKER_STATS_DATA => "view_broker_stats_data",
        codes::request::CLEAN_UNUSED_TOPIC => "clean_unused_topic",
        codes::request::GET_BROKER_CONSUME_STATS => "get_broker_consume_stats",
        codes::request::UPDATE_NAMESRV_CONFIG => "update_namesrv_config",
        codes::request::GET_NAMESRV_CONFIG => "get_namesrv_config",
        codes::request::SEND_BATCH_MESSAGE => "send_batch_message",
        codes::request::QUERY_CONSUME_QUEUE => "query_consume_queue",
        codes::request::QUERY_DATA_VERSION => "query_data_version",
        codes::request::RESUME_CHECK_HALF_MESSAGE => "resume_check_half_message",
        codes::request::SEND_REPLY_MESSAGE => "send_reply_message",
        codes::request::SEND_REPLY_MESSAGE_V2 => "send_reply_message_v2",
        codes::request::PUSH_REPLY_MESSAGE_TO_CLIENT => "push_reply_message_to_client",
        codes::request::ADD_WRITE_PERM_OF_BROKER => "add_write_perm_of_broker",
        codes::request::GET_ALL_PRODUCER_INFO => "get_all_producer_info",
        codes::request::DELETE_EXPIRED_COMMITLOG => "delete_expired_commitlog",
        codes::request::GET_TOPIC_CONFIG => "get_topic_config",
        codes::request::GET_SUBSCRIPTIONGROUP_CONFIG => "get_subscriptiongroup_config",
        codes::request::UPDATE_AND_GET_GROUP_FORBIDDEN => "update_and_get_group_forbidden",
        codes::request::CHECK_ROCKSDB_CQ_WRITE_PROGRESS => "check_rocksdb_cq_write_progress",
        codes::request::EXPORT_ROCKSDB_CONFIG_TO_JSON => "export_rocksdb_config_to_json",
        codes::request::LITE_PULL_MESSAGE => "lite_pull_message",
        codes::request::RECALL_MESSAGE => "recall_message",
        codes::request::QUERY_ASSIGNMENT => "query_assignment",
        codes::request::SET_MESSAGE_REQUEST_MODE => "set_message_request_mode",
        codes::request::GET_ALL_MESSAGE_REQUEST_MODE => "get_all_message_request_mode",
        codes::request::UPDATE_AND_CREATE_STATIC_TOPIC => "update_and_create_static_topic",
        codes::request::GET_BROKER_MEMBER_GROUP => "get_broker_member_group",
        codes::request::ADD_BROKER => "add_broker",
        codes::request::REMOVE_BROKER => "remove_broker",
        codes::request::BROKER_HEARTBEAT => "broker_heartbeat",
        codes::request::NOTIFY_MIN_BROKER_ID_CHANGE => "notify_min_broker_id_change",
        codes::request::EXCHANGE_BROKER_HA_INFO => "exchange_broker_ha_info",
        codes::request::GET_BROKER_HA_STATUS => "get_broker_ha_status",
        codes::request::RESET_MASTER_FLUSH_OFFSET => "reset_master_flush_offset",
        codes::request::CONTROLLER_ALTER_SYNC_STATE_SET => "controller_alter_sync_state_set",
        codes::request::CONTROLLER_ELECT_MASTER => "controller_elect_master",
        codes::request::CONTROLLER_REGISTER_BROKER => "controller_register_broker",
        codes::request::CONTROLLER_GET_REPLICA_INFO => "controller_get_replica_info",
        codes::request::CONTROLLER_GET_METADATA_INFO => "controller_get_metadata_info",
        codes::request::CONTROLLER_GET_SYNC_STATE_DATA => "controller_get_sync_state_data",
        codes::request::GET_BROKER_EPOCH_CACHE => "get_broker_epoch_cache",
        codes::request::NOTIFY_BROKER_ROLE_CHANGED => "notify_broker_role_changed",
        codes::request::UPDATE_CONTROLLER_CONFIG => "update_controller_config",
        codes::request::GET_CONTROLLER_CONFIG => "get_controller_config",
        codes::request::CLEAN_BROKER_DATA => "clean_broker_data",
        codes::request::CONTROLLER_GET_NEXT_BROKER_ID => "controller_get_next_broker_id",
        codes::request::CONTROLLER_APPLY_BROKER_ID => "controller_apply_broker_id",
        codes::request::UPDATE_COLD_DATA_FLOW_CTR_CONFIG => "update_cold_data_flow_ctr_config",
        codes::request::REMOVE_COLD_DATA_FLOW_CTR_CONFIG => "remove_cold_data_flow_ctr_config",
        codes::request::GET_COLD_DATA_FLOW_CTR_INFO => "get_cold_data_flow_ctr_info",
        codes::request::SET_COMMITLOG_READ_MODE => "set_commitlog_read_mode",
        codes::request::AUTH_CREATE_USER => "auth_create_user",
        codes::request::AUTH_UPDATE_USER => "auth_update_user",
        codes::request::AUTH_DELETE_USER => "auth_delete_user",
        codes::request::AUTH_GET_USER => "auth_get_user",
        codes::request::AUTH_LIST_USER => "auth_list_user",
        codes::request::AUTH_CREATE_ACL => "auth_create_acl",
        codes::request::AUTH_UPDATE_ACL => "auth_update_acl",
        codes::request::AUTH_DELETE_ACL => "auth_delete_acl",
        codes::request::AUTH_GET_ACL => "auth_get_acl",
        codes::request::AUTH_LIST_ACL => "auth_list_acl",
        codes::request::SWITCH_TIMER_ENGINE => "switch_timer_engine",
        codes::request::DELETE_TOPIC_IN_BROKER_LIST => "delete_topic_in_broker_list",
        codes::request::DELETE_SUBSCRIPTION_GROUP_LIST => "delete_subscription_group_list",
        _ => return code.to_string(),
    };
    java_default_locale_lowercase(description)
}

/// Returns the Java default-locale lowercase `ResponseCode` field name, or the
/// decimal code.
pub fn get_response_code_desc(code: i32) -> String {
    let description = match code {
        codes::response::SUCCESS => "success",
        codes::response::SYSTEM_ERROR => "system_error",
        codes::response::SYSTEM_BUSY => "system_busy",
        codes::response::REQUEST_CODE_NOT_SUPPORTED => "request_code_not_supported",
        codes::response::TRANSACTION_FAILED => "transaction_failed",
        codes::response::FLUSH_DISK_TIMEOUT => "flush_disk_timeout",
        codes::response::SLAVE_NOT_AVAILABLE => "slave_not_available",
        codes::response::FLUSH_SLAVE_TIMEOUT => "flush_slave_timeout",
        codes::response::MESSAGE_ILLEGAL => "message_illegal",
        codes::response::SERVICE_NOT_AVAILABLE => "service_not_available",
        codes::response::VERSION_NOT_SUPPORTED => "version_not_supported",
        codes::response::NO_PERMISSION => "no_permission",
        codes::response::TOPIC_NOT_EXIST => "topic_not_exist",
        codes::response::TOPIC_EXIST_ALREADY => "topic_exist_already",
        codes::response::PULL_NOT_FOUND => "pull_not_found",
        codes::response::PULL_RETRY_IMMEDIATELY => "pull_retry_immediately",
        codes::response::PULL_OFFSET_MOVED => "pull_offset_moved",
        codes::response::QUERY_NOT_FOUND => "query_not_found",
        codes::response::SUBSCRIPTION_PARSE_FAILED => "subscription_parse_failed",
        codes::response::SUBSCRIPTION_NOT_EXIST => "subscription_not_exist",
        codes::response::SUBSCRIPTION_NOT_LATEST => "subscription_not_latest",
        codes::response::SUBSCRIPTION_GROUP_NOT_EXIST => "subscription_group_not_exist",
        codes::response::FILTER_DATA_NOT_EXIST => "filter_data_not_exist",
        codes::response::FILTER_DATA_NOT_LATEST => "filter_data_not_latest",
        codes::response::INVALID_PARAMETER => "invalid_parameter",
        codes::response::TRANSACTION_SHOULD_COMMIT => "transaction_should_commit",
        codes::response::TRANSACTION_SHOULD_ROLLBACK => "transaction_should_rollback",
        codes::response::TRANSACTION_STATE_UNKNOW => "transaction_state_unknow",
        codes::response::TRANSACTION_STATE_GROUP_WRONG => "transaction_state_group_wrong",
        codes::response::NO_BUYER_ID => "no_buyer_id",
        codes::response::NOT_IN_CURRENT_UNIT => "not_in_current_unit",
        codes::response::CONSUMER_NOT_ONLINE => "consumer_not_online",
        codes::response::CONSUME_MSG_TIMEOUT => "consume_msg_timeout",
        codes::response::NO_MESSAGE => "no_message",
        codes::response::POLLING_FULL => "polling_full",
        codes::response::POLLING_TIMEOUT => "polling_timeout",
        codes::response::BROKER_NOT_EXIST => "broker_not_exist",
        codes::response::BROKER_DISPATCH_NOT_COMPLETE => "broker_dispatch_not_complete",
        codes::response::BROADCAST_CONSUMPTION => "broadcast_consumption",
        codes::response::FLOW_CONTROL => "flow_control",
        codes::response::NOT_LEADER_FOR_QUEUE => "not_leader_for_queue",
        codes::response::ILLEGAL_OPERATION => "illegal_operation",
        codes::response::RPC_UNKNOWN => "rpc_unknown",
        codes::response::RPC_ADDR_IS_NULL => "rpc_addr_is_null",
        codes::response::RPC_SEND_TO_CHANNEL_FAILED => "rpc_send_to_channel_failed",
        codes::response::RPC_TIME_OUT => "rpc_time_out",
        codes::response::GO_AWAY => "go_away",
        codes::response::CONTROLLER_FENCED_MASTER_EPOCH => "controller_fenced_master_epoch",
        codes::response::CONTROLLER_FENCED_SYNC_STATE_SET_EPOCH => {
            "controller_fenced_sync_state_set_epoch"
        }
        codes::response::CONTROLLER_INVALID_MASTER => "controller_invalid_master",
        codes::response::CONTROLLER_INVALID_REPLICAS => "controller_invalid_replicas",
        codes::response::CONTROLLER_MASTER_NOT_AVAILABLE => "controller_master_not_available",
        codes::response::CONTROLLER_INVALID_REQUEST => "controller_invalid_request",
        codes::response::CONTROLLER_BROKER_NOT_ALIVE => "controller_broker_not_alive",
        codes::response::CONTROLLER_NOT_LEADER => "controller_not_leader",
        codes::response::CONTROLLER_BROKER_METADATA_NOT_EXIST => {
            "controller_broker_metadata_not_exist"
        }
        codes::response::CONTROLLER_INVALID_CLEAN_BROKER_METADATA => {
            "controller_invalid_clean_broker_metadata"
        }
        codes::response::CONTROLLER_BROKER_NEED_TO_BE_REGISTERED => {
            "controller_broker_need_to_be_registered"
        }
        codes::response::CONTROLLER_MASTER_STILL_EXIST => "controller_master_still_exist",
        codes::response::CONTROLLER_ELECT_MASTER_FAILED => "controller_elect_master_failed",
        codes::response::CONTROLLER_ALTER_SYNC_STATE_SET_FAILED => {
            "controller_alter_sync_state_set_failed"
        }
        codes::response::CONTROLLER_BROKER_ID_INVALID => "controller_broker_id_invalid",
        codes::response::CONTROLLER_JRAFT_INTERNAL_ERROR => "controller_jraft_internal_error",
        codes::response::CONTROLLER_BROKER_LIVE_INFO_NOT_EXISTS => {
            "controller_broker_live_info_not_exists"
        }
        codes::response::LMQ_QUOTA_EXCEEDED => "lmq_quota_exceeded",
        codes::response::LITE_SUBSCRIPTION_QUOTA_EXCEEDED => "lite_subscription_quota_exceeded",
        codes::response::USER_NOT_EXIST => "user_not_exist",
        codes::response::POLICY_NOT_EXIST => "policy_not_exist",
        _ => return code.to_string(),
    };
    java_default_locale_lowercase(description)
}

fn java_default_locale_lowercase(value: &str) -> String {
    static TURKIC_DEFAULT_LOCALE: OnceLock<bool> = OnceLock::new();
    let is_turkic = *TURKIC_DEFAULT_LOCALE.get_or_init(|| {
        ["LC_ALL", "LC_CTYPE", "LANG"]
            .into_iter()
            .filter_map(std::env::var_os)
            .map(|locale| locale.to_string_lossy().into_owned())
            .find(|locale| !locale.is_empty())
            .map(|locale| {
                matches!(
                    locale
                        .split(['_', '-', '.', '@'])
                        .next()
                        .unwrap_or_default()
                        .to_ascii_lowercase()
                        .as_str(),
                    "tr" | "az"
                )
            })
            .unwrap_or(false)
    });

    if is_turkic {
        value.replace('i', "ı")
    } else {
        value.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::{get_request_code_desc, get_response_code_desc};

    #[test]
    fn code_descriptions_match_the_pinned_java_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/response-future-api.json"
        ))
        .unwrap();

        for (cases, describe) in [
            (
                &fixture["requestCodeDescriptions"],
                get_request_code_desc as fn(i32) -> String,
            ),
            (
                &fixture["responseCodeDescriptions"],
                get_response_code_desc as fn(i32) -> String,
            ),
        ] {
            let cases = cases.as_array().expect("Java code-description cases");
            assert!(!cases.is_empty());
            for case in cases {
                let code = case["code"].as_i64().unwrap() as i32;
                assert_eq!(
                    describe(code),
                    case["description"].as_str().unwrap(),
                    "code description for {code}"
                );
            }
        }

        assert_eq!(
            get_request_code_desc(i32::MAX),
            fixture["unknownRequestCodeDescription"]
        );
        assert_eq!(
            get_response_code_desc(i32::MIN),
            fixture["unknownResponseCodeDescription"]
        );
    }

    #[test]
    fn code_descriptions_match_java_turkish_default_locale() {
        const CHILD_MARKER: &str = "ROCKETMQ_CLASSIC_TURKISH_LOCALE_TEST";

        if std::env::var_os(CHILD_MARKER).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "remoting_helper::tests::code_descriptions_match_java_turkish_default_locale",
                ])
                .env("LC_ALL", "tr_TR.UTF-8")
                .env(CHILD_MARKER, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "Turkish-locale child test failed:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/code-descriptions-turkish.json"
        ))
        .unwrap();

        for (cases, describe) in [
            (
                &fixture["requestCodeDescriptions"],
                get_request_code_desc as fn(i32) -> String,
            ),
            (
                &fixture["responseCodeDescriptions"],
                get_response_code_desc as fn(i32) -> String,
            ),
        ] {
            let cases = cases.as_array().expect("Java code-description cases");
            assert!(!cases.is_empty());
            for case in cases {
                let code = case["code"].as_i64().unwrap() as i32;
                assert_eq!(
                    describe(code),
                    case["description"].as_str().unwrap(),
                    "Turkish code description for {code}"
                );
            }
        }

        assert_eq!(
            get_request_code_desc(i32::MAX),
            fixture["unknownRequestCodeDescription"]
        );
        assert_eq!(
            get_response_code_desc(i32::MIN),
            fixture["unknownResponseCodeDescription"]
        );
    }
}
