use std::{env, fs, path::PathBuf};

use rocketmq_classic_protocol::{
    SharedValue,
    body::{
        BatchAck, BatchAckMessageRequestBody, ConsumeMessageDirectlyResult, ConsumeMessageResult,
        ConsumeQueueData, ConsumeStats, ConsumerListByGroupResponseBody, DataVersion,
        GetConsumerStatusBody, GetLiteGroupInfoResponseBody, LiteLagInfo, LiteSubscriptionAction,
        LiteSubscriptionCtlRequestBody, LiteSubscriptionDto, LockBatchRequestBody,
        LockBatchResponseBody, MessageRequestMode, OffsetOption, OffsetOptionType, OffsetWrapper,
        QueryAssignmentRequestBody, QueryConsumeQueueResponseBody, ResetOffsetBody,
        SetMessageRequestModeRequestBody, SubscriptionGroupWrapper, TopicConfig,
        TopicConfigSerializeWrapper, UnlockBatchRequestBody,
    },
    codes::{LanguageCode, SerializeType},
    header::{
        AckMessageRequestHeader, AddBrokerRequestHeader, AddWritePermOfBrokerRequestHeader,
        AddWritePermOfBrokerResponseHeader, ChangeInvisibleTimeRequestHeader,
        ChangeInvisibleTimeResponseHeader, CheckRocksdbCqWriteProgressRequestHeader,
        CheckTransactionStateRequestHeader, CheckTransactionStateResponseHeader,
        CloneGroupOffsetRequestHeader, CommandHeader, ConsumeMessageDirectlyResultRequestHeader,
        ConsumerSendMsgBackRequestHeader, CreateAclRequestHeader, CreateTopicRequestHeader,
        CreateUserRequestHeader, DeleteAclRequestHeader, DeleteKvConfigRequestHeader,
        DeleteSubscriptionGroupRequestHeader, DeleteTopicFromNamesrvRequestHeader,
        DeleteTopicRequestHeader, DeleteUserRequestHeader, EndTransactionRequestHeader,
        EndTransactionResponseHeader, ExportRocksDBConfigToJsonRequestHeader,
        ExportRocksDBConfigType, GetAclRequestHeader, GetAllSubscriptionGroupRequestHeader,
        GetAllSubscriptionGroupResponseHeader, GetAllTopicConfigRequestHeader,
        GetAllTopicConfigResponseHeader, GetConsumeStatsInBrokerRequestHeader,
        GetConsumeStatsRequestHeader, GetConsumerConnectionListRequestHeader,
        GetConsumerListByGroupRequestHeader, GetConsumerRunningInfoRequestHeader,
        GetConsumerStatusRequestHeader, GetEarliestMsgStoretimeRequestHeader,
        GetEarliestMsgStoretimeResponseHeader, GetKvConfigRequestHeader, GetKvConfigResponseHeader,
        GetKvListByNamespaceRequestHeader, GetLiteClientInfoRequestHeader,
        GetLiteGroupInfoRequestHeader, GetLiteTopicInfoRequestHeader, GetMaxOffsetRequestHeader,
        GetMaxOffsetResponseHeader, GetMinOffsetRequestHeader, GetMinOffsetResponseHeader,
        GetParentTopicInfoRequestHeader, GetRouteInfoRequestHeader,
        GetSubscriptionGroupConfigRequestHeader, GetTopicConfigRequestHeader,
        GetTopicStatsInfoRequestHeader, GetTopicsByClusterRequestHeader, GetUserRequestHeader,
        HeartbeatRequestHeader, ListAclsRequestHeader, ListUsersRequestHeader,
        LiteSubscriptionCtlRequestHeader, LockBatchMqRequestHeader, NotificationRequestHeader,
        NotificationResponseHeader, NotifyUnsubscribeLiteRequestHeader, PollingInfoRequestHeader,
        PollingInfoResponseHeader, PopLiteMessageRequestHeader, PopLiteMessageResponseHeader,
        PopMessageRequestHeader, PopMessageResponseHeader, PullMessageRequestHeader,
        PullMessageResponseHeader, PutKvConfigRequestHeader, QueryConsumeQueueRequestHeader,
        QueryConsumeTimeSpanRequestHeader, QueryConsumerOffsetRequestHeader,
        QueryConsumerOffsetResponseHeader, QueryCorrectionOffsetRequestHeader,
        QueryMessageRequestHeader, QueryMessageResponseHeader,
        QuerySubscriptionByConsumerRequestHeader, QueryTopicConsumeByWhoRequestHeader,
        QueryTopicsByConsumerRequestHeader, RecallMessageRequestHeader,
        RecallMessageResponseHeader, RemoveBrokerRequestHeader, ReplyMessageRequestHeader,
        ResetMasterFlushOffsetHeader, ResetOffsetRequestHeader, SearchOffsetRequestHeader,
        SearchOffsetResponseHeader, SendMessageRequestHeaderV1, SendMessageRequestHeaderV2,
        SendMessageResponseHeader, UnlockBatchMqRequestHeader, UnregisterClientRequestHeader,
        UnregisterClientResponseHeader, UpdateAclRequestHeader, UpdateConsumerOffsetRequestHeader,
        UpdateGroupForbiddenRequestHeader, UpdateUserRequestHeader,
        ViewBrokerStatsDataRequestHeader, ViewMessageRequestHeader,
        WipeWritePermOfBrokerRequestHeader, WipeWritePermOfBrokerResponseHeader,
    },
    subscription::SubscriptionGroupConfig,
};
use rocketmq_classic_remoting::{codec::frame, command::RemotingCommand};

fn main() {
    let mut args = env::args_os().skip(1);
    let mode = args.next().unwrap_or_default();
    let output = args.next().map(PathBuf::from).expect(
        "usage: cargo run -p rocketmq-classic-remoting --example generate_fixtures -- <directory>",
    );
    fs::create_dir_all(&output).expect("create output directory");
    if mode == "p4-headers" {
        generate_p4_headers(&output);
        return;
    }
    if mode == "p4-bodies" {
        generate_p4_bodies(&output);
        return;
    }
    for (name, code, serialization) in [
        ("json-route", 105, SerializeType::Json),
        ("rocketmq-transaction", 37, SerializeType::RocketMq),
    ] {
        let command = RemotingCommand {
            code,
            language: Some(LanguageCode::Rust),
            version: 0,
            opaque: 7,
            remark: Some(name.into()),
            ext_fields: Some(
                std::collections::BTreeMap::from([("oracle".into(), "rust-phase-2".into())]).into(),
            ),
            body: Some(vec![0, 1, 2, 255]),
            serialization,
            ..RemotingCommand::default()
        };
        fs::write(
            output.join(format!("{name}.bin")),
            frame::encode(&command).expect("encode frame"),
        )
        .expect("write frame");
    }
}

fn generate_p4_headers(output: &std::path::Path) {
    // Decode the checked-in Java 5.5.1 oracle through each public Rust model,
    // then emit a Rust frame for the same request or response role.
    write_oracle_header::<CreateTopicRequestHeader>(
        output,
        "typed-create-topic-request.bin",
        "header-create-topic-request",
        17,
    );
    write_oracle_header::<CreateUserRequestHeader>(
        output,
        "typed-create-user-request.bin",
        "header-create-user-request",
        3001,
    );
    write_oracle_header::<CreateAclRequestHeader>(
        output,
        "typed-create-acl-request.bin",
        "header-create-acl-request",
        3006,
    );
    write_oracle_header::<DeleteAclRequestHeader>(
        output,
        "typed-delete-acl-request.bin",
        "header-delete-acl-request",
        3008,
    );
    write_oracle_header::<UpdateAclRequestHeader>(
        output,
        "typed-update-acl-request.bin",
        "header-update-acl-request",
        3007,
    );
    write_oracle_header::<UpdateUserRequestHeader>(
        output,
        "typed-update-user-request.bin",
        "header-update-user-request",
        3002,
    );
    write_oracle_header::<DeleteUserRequestHeader>(
        output,
        "typed-delete-user-request.bin",
        "header-delete-user-request",
        3003,
    );
    write_oracle_header::<GetUserRequestHeader>(
        output,
        "typed-get-user-request.bin",
        "header-get-user-request",
        3004,
    );
    write_oracle_header::<GetAclRequestHeader>(
        output,
        "typed-get-acl-request.bin",
        "header-get-acl-request",
        3009,
    );
    write_oracle_header::<ListAclsRequestHeader>(
        output,
        "typed-list-acls-request.bin",
        "header-list-acls-request",
        3010,
    );
    write_oracle_header::<ListUsersRequestHeader>(
        output,
        "typed-list-users-request.bin",
        "header-list-users-request",
        3005,
    );
    write_oracle_header::<QueryMessageRequestHeader>(
        output,
        "typed-query-message-request.bin",
        "header-query-message-request",
        12,
    );
    write_oracle_header::<QueryConsumeTimeSpanRequestHeader>(
        output,
        "typed-query-consume-time-span-request.bin",
        "header-query-consume-time-span-request",
        303,
    );
    write_oracle_header::<QueryTopicsByConsumerRequestHeader>(
        output,
        "typed-query-topics-by-consumer-request.bin",
        "header-query-topics-by-consumer-request",
        343,
    );
    write_oracle_header::<QuerySubscriptionByConsumerRequestHeader>(
        output,
        "typed-query-subscription-by-consumer-request.bin",
        "header-query-subscription-by-consumer-request",
        345,
    );
    write_oracle_header::<QueryTopicConsumeByWhoRequestHeader>(
        output,
        "typed-query-topic-consume-by-who-request.bin",
        "header-query-topic-consume-by-who-request",
        300,
    );
    write_oracle_header::<QueryCorrectionOffsetRequestHeader>(
        output,
        "typed-query-correction-offset-request.bin",
        "header-query-correction-offset-request",
        308,
    );
    write_oracle_header::<UpdateGroupForbiddenRequestHeader>(
        output,
        "typed-update-group-forbidden-request.bin",
        "header-update-group-forbidden-request",
        353,
    );
    write_oracle_header::<GetTopicStatsInfoRequestHeader>(
        output,
        "typed-topic-stats-info-request.bin",
        "header-topic-stats-info-request",
        202,
    );
    write_oracle_header::<GetTopicConfigRequestHeader>(
        output,
        "typed-topic-config-request.bin",
        "header-topic-config-request",
        351,
    );
    write_oracle_header::<GetAllTopicConfigRequestHeader>(
        output,
        "typed-get-all-topic-config-request.bin",
        "header-get-all-topic-config-request",
        21,
    );
    write_oracle_header::<GetAllTopicConfigResponseHeader>(
        output,
        "typed-get-all-topic-config-response.bin",
        "header-get-all-topic-config-response",
        0,
    );
    write_oracle_header::<GetAllSubscriptionGroupRequestHeader>(
        output,
        "typed-get-all-subscription-group-request.bin",
        "header-get-all-subscription-group-request",
        201,
    );
    write_oracle_header::<GetAllSubscriptionGroupResponseHeader>(
        output,
        "typed-get-all-subscription-group-response.bin",
        "header-get-all-subscription-group-response",
        0,
    );
    write_oracle_header::<QueryConsumeQueueRequestHeader>(
        output,
        "typed-query-consume-queue-request.bin",
        "header-query-consume-queue-request",
        321,
    );
    write_oracle_header::<GetTopicsByClusterRequestHeader>(
        output,
        "typed-topics-by-cluster-request.bin",
        "header-topics-by-cluster-request",
        224,
    );
    write_oracle_header::<GetSubscriptionGroupConfigRequestHeader>(
        output,
        "typed-get-subscription-group-config-request.bin",
        "header-get-subscription-group-config-request",
        352,
    );
    write_oracle_header::<GetLiteTopicInfoRequestHeader>(
        output,
        "typed-lite-topic-info-request.bin",
        "header-lite-topic-info-request",
        200076,
    );
    write_oracle_header::<GetLiteClientInfoRequestHeader>(
        output,
        "typed-lite-client-info-request.bin",
        "header-lite-client-info-request",
        200077,
    );
    write_oracle_header::<GetLiteGroupInfoRequestHeader>(
        output,
        "typed-lite-group-info-request.bin",
        "header-lite-group-info-request",
        200078,
    );
    write_oracle_header::<GetParentTopicInfoRequestHeader>(
        output,
        "typed-parent-topic-info-request.bin",
        "header-parent-topic-info-request",
        200075,
    );
    write_oracle_header::<DeleteTopicRequestHeader>(
        output,
        "typed-delete-topic-request.bin",
        "header-delete-topic-request",
        215,
    );
    write_oracle_header::<DeleteTopicFromNamesrvRequestHeader>(
        output,
        "typed-delete-topic-from-namesrv-request.bin",
        "header-delete-topic-from-namesrv-request",
        216,
    );
    write_oracle_header::<DeleteKvConfigRequestHeader>(
        output,
        "typed-delete-kv-config-request.bin",
        "header-delete-kv-config-request",
        102,
    );
    write_oracle_header::<GetKvConfigRequestHeader>(
        output,
        "typed-get-kv-config-request.bin",
        "header-get-kv-config-request",
        101,
    );
    write_oracle_header::<PutKvConfigRequestHeader>(
        output,
        "typed-put-kv-config-request.bin",
        "header-put-kv-config-request",
        100,
    );
    write_oracle_header::<GetKvListByNamespaceRequestHeader>(
        output,
        "typed-get-kv-list-by-namespace-request.bin",
        "header-get-kv-list-by-namespace-request",
        219,
    );
    write_oracle_header::<GetKvConfigResponseHeader>(
        output,
        "typed-get-kv-config-response.bin",
        "header-get-kv-config-response",
        0,
    );
    write_oracle_header::<WipeWritePermOfBrokerRequestHeader>(
        output,
        "typed-wipe-write-perm-request.bin",
        "header-wipe-write-perm-request",
        205,
    );
    write_oracle_header::<AddWritePermOfBrokerRequestHeader>(
        output,
        "typed-add-write-perm-request.bin",
        "header-add-write-perm-request",
        327,
    );
    write_oracle_header::<AddBrokerRequestHeader>(
        output,
        "typed-add-broker-request.bin",
        "header-add-broker-request",
        902,
    );
    write_oracle_header::<RemoveBrokerRequestHeader>(
        output,
        "typed-remove-broker-request.bin",
        "header-remove-broker-request",
        903,
    );
    write_oracle_header::<CloneGroupOffsetRequestHeader>(
        output,
        "typed-clone-group-offset-request.bin",
        "header-clone-group-offset-request",
        314,
    );
    write_oracle_header::<WipeWritePermOfBrokerResponseHeader>(
        output,
        "typed-wipe-write-perm-response.bin",
        "header-wipe-write-perm-response",
        0,
    );
    write_oracle_header::<AddWritePermOfBrokerResponseHeader>(
        output,
        "typed-add-write-perm-response.bin",
        "header-add-write-perm-response",
        0,
    );
    write_oracle_header::<DeleteSubscriptionGroupRequestHeader>(
        output,
        "typed-delete-subscription-group-request.bin",
        "header-delete-subscription-group-request",
        207,
    );
    write_oracle_header::<QueryMessageResponseHeader>(
        output,
        "typed-query-message-response.bin",
        "header-query-message-response",
        0,
    );
    write_oracle_header::<ViewMessageRequestHeader>(
        output,
        "typed-view-message-request.bin",
        "header-view-message-request",
        33,
    );
    write_oracle_header::<QueryConsumerOffsetResponseHeader>(
        output,
        "typed-query-offset-response.bin",
        "header-query-offset-response",
        0,
    );
    write_oracle_header::<SearchOffsetRequestHeader>(
        output,
        "typed-search-offset-request.bin",
        "header-search-offset-request",
        29,
    );
    write_oracle_header::<GetMaxOffsetRequestHeader>(
        output,
        "typed-max-offset-request.bin",
        "header-max-offset-request",
        30,
    );
    write_oracle_header::<GetMinOffsetRequestHeader>(
        output,
        "typed-min-offset-request.bin",
        "header-min-offset-request",
        31,
    );
    write_oracle_header::<GetEarliestMsgStoretimeRequestHeader>(
        output,
        "typed-earliest-msg-storetime-request.bin",
        "header-earliest-msg-storetime-request",
        32,
    );
    write_oracle_header::<SearchOffsetResponseHeader>(
        output,
        "typed-search-offset-response.bin",
        "header-search-offset-response",
        0,
    );
    write_oracle_header::<GetMaxOffsetResponseHeader>(
        output,
        "typed-max-offset-response.bin",
        "header-max-offset-response",
        0,
    );
    write_oracle_header::<GetMinOffsetResponseHeader>(
        output,
        "typed-min-offset-response.bin",
        "header-min-offset-response",
        0,
    );
    write_oracle_header::<GetEarliestMsgStoretimeResponseHeader>(
        output,
        "typed-earliest-msg-storetime-response.bin",
        "header-earliest-msg-storetime-response",
        0,
    );
    write_oracle_header::<ChangeInvisibleTimeResponseHeader>(
        output,
        "typed-change-invisible-response.bin",
        "header-change-invisible-response",
        0,
    );
    write_oracle_header::<UnregisterClientRequestHeader>(
        output,
        "typed-unregister-request.bin",
        "header-unregister-request",
        35,
    );
    write_oracle_header::<RecallMessageRequestHeader>(
        output,
        "typed-recall-request.bin",
        "header-recall-message-request",
        370,
    );
    write_oracle_header::<RecallMessageResponseHeader>(
        output,
        "typed-recall-response.bin",
        "header-recall-message-response",
        0,
    );
    write_oracle_header::<LockBatchMqRequestHeader>(
        output,
        "typed-lock-batch-request.bin",
        "header-lock-batch-request",
        41,
    );
    write_oracle_header::<UnlockBatchMqRequestHeader>(
        output,
        "typed-unlock-batch-request.bin",
        "header-unlock-batch-request",
        42,
    );
    write_oracle_header::<GetConsumerListByGroupRequestHeader>(
        output,
        "typed-consumer-list-request.bin",
        "header-consumer-list-request",
        38,
    );
    write_oracle_header::<ConsumerSendMsgBackRequestHeader>(
        output,
        "typed-send-back-request.bin",
        "header-send-back-request",
        36,
    );
    write_oracle_header::<UnregisterClientResponseHeader>(
        output,
        "typed-unregister-response.bin",
        "header-unregister-response",
        0,
    );
    write_oracle_header::<EndTransactionResponseHeader>(
        output,
        "typed-end-transaction-response.bin",
        "header-end-transaction-response",
        0,
    );
    write_oracle_header::<GetConsumerStatusRequestHeader>(
        output,
        "typed-consumer-status-request.bin",
        "header-consumer-status-request",
        221,
    );
    write_oracle_header::<GetConsumerConnectionListRequestHeader>(
        output,
        "typed-consumer-connection-list-request.bin",
        "header-consumer-connection-list-request",
        203,
    );
    write_oracle_header::<GetConsumeStatsRequestHeader>(
        output,
        "typed-consume-stats-request.bin",
        "header-consume-stats-request",
        208,
    );
    write_oracle_header::<GetConsumeStatsInBrokerRequestHeader>(
        output,
        "typed-consume-stats-in-broker-request.bin",
        "header-consume-stats-in-broker-request",
        317,
    );
    write_oracle_header::<ViewBrokerStatsDataRequestHeader>(
        output,
        "typed-view-broker-stats-data-request.bin",
        "header-view-broker-stats-data-request",
        315,
    );
    write_oracle_header::<GetConsumerRunningInfoRequestHeader>(
        output,
        "typed-consumer-running-info-request.bin",
        "header-consumer-running-info-request",
        307,
    );
    write_oracle_header::<HeartbeatRequestHeader>(
        output,
        "typed-heartbeat-request.bin",
        "header-heartbeat-request",
        34,
    );
    write_oracle_header::<PollingInfoRequestHeader>(
        output,
        "typed-polling-request.bin",
        "header-polling-request",
        200055,
    );
    write_oracle_header::<PollingInfoResponseHeader>(
        output,
        "typed-polling-response.bin",
        "header-polling-response",
        0,
    );
    write_oracle_header::<PopLiteMessageRequestHeader>(
        output,
        "typed-pop-lite-request.bin",
        "header-pop-lite-request",
        200070,
    );
    write_oracle_header::<LiteSubscriptionCtlRequestHeader>(
        output,
        "typed-lite-subscription-request.bin",
        "header-lite-subscription-request",
        200071,
    );
    write_oracle_header::<NotifyUnsubscribeLiteRequestHeader>(
        output,
        "typed-notify-unsubscribe-lite-request.bin",
        "header-notify-unsubscribe-lite-request",
        200073,
    );
    write_oracle_header::<ConsumeMessageDirectlyResultRequestHeader>(
        output,
        "typed-consume-directly-request.bin",
        "header-consume-directly-request",
        309,
    );

    write_typed(output, "typed-route.bin", 105, &{
        let mut __phase18_header_0 = GetRouteInfoRequestHeader::default();
        __phase18_header_0.set_topic("orders".into());
        __phase18_header_0.set_accept_standard_json_only(None);
        __phase18_header_0
    });
    write_typed(output, "typed-send-v1.bin", 10, &{
        let mut __phase18_header_1 = SendMessageRequestHeaderV1::default();
        __phase18_header_1.set_producer_group(Some("producers".into()));
        __phase18_header_1.set_topic("orders".into());
        __phase18_header_1.set_default_topic("TBW102".into());
        __phase18_header_1.set_default_topic_queue_nums(4);
        __phase18_header_1.set_queue_id(2);
        __phase18_header_1.set_sys_flag(1);
        __phase18_header_1.set_born_timestamp(1_712_345_678_901);
        __phase18_header_1.set_flag(7);
        __phase18_header_1.set_properties(Some("TAGS\\u0001blue".into()));
        __phase18_header_1.set_reconsume_times(Some(0));
        __phase18_header_1.set_unit_mode(Some(false));
        __phase18_header_1.set_max_reconsume_times(Some(16));
        __phase18_header_1.set_batch(Some(false));
        __phase18_header_1
    });
    write_typed(output, "typed-send-v2.bin", 310, &{
        let mut __phase18_header_2 = SendMessageRequestHeaderV2::default();
        __phase18_header_2.set_a(Some("producers".into()));
        __phase18_header_2.set_b(Some("orders".into()));
        __phase18_header_2.set_c(Some("TBW102".into()));
        __phase18_header_2.set_d(Some(4));
        __phase18_header_2.set_e(Some(2));
        __phase18_header_2.set_f(Some(1));
        __phase18_header_2.set_g(Some(1_712_345_678_901));
        __phase18_header_2.set_h(Some(7));
        __phase18_header_2.set_i(Some("TAGS\\u0001blue".into()));
        __phase18_header_2.set_j(Some(0));
        __phase18_header_2.set_k(Some(false));
        __phase18_header_2.set_l(Some(16));
        __phase18_header_2.set_m(Some(false));
        __phase18_header_2
    });
    write_typed(output, "typed-send-batch-v2.bin", 320, &{
        let mut __phase18_header_3 = SendMessageRequestHeaderV2::default();
        __phase18_header_3.set_a(Some("producers".into()));
        __phase18_header_3.set_b(Some("orders".into()));
        __phase18_header_3.set_c(Some("TBW102".into()));
        __phase18_header_3.set_d(Some(4));
        __phase18_header_3.set_e(Some(2));
        __phase18_header_3.set_f(Some(1));
        __phase18_header_3.set_g(Some(1_712_345_678_901));
        __phase18_header_3.set_h(Some(7));
        __phase18_header_3.set_i(Some("TAGS\\u0001blue".into()));
        __phase18_header_3.set_j(Some(0));
        __phase18_header_3.set_k(Some(false));
        __phase18_header_3.set_l(Some(16));
        __phase18_header_3.set_m(Some(false));
        __phase18_header_3
    });
    write_typed(output, "typed-pull.bin", 11, &{
        let mut __phase18_header_4 = PullMessageRequestHeader::default();
        __phase18_header_4.set_consumer_group(Some("workers".into()));
        __phase18_header_4.set_topic(Some("orders".into()));
        __phase18_header_4.set_lite_topic(Some("lite-orders".into()));
        __phase18_header_4.set_queue_id(Some(1));
        __phase18_header_4.set_queue_offset(Some(9));
        __phase18_header_4.set_max_msg_nums(Some(32));
        __phase18_header_4.set_sys_flag(Some(0));
        __phase18_header_4.set_commit_offset(Some(8));
        __phase18_header_4.set_suspend_timeout_millis(Some(15_000));
        __phase18_header_4.set_sub_version(Some(1_712_345_678_901));
        __phase18_header_4
    });
    write_typed(output, "typed-query-offset.bin", 14, &{
        let mut header = QueryConsumerOffsetRequestHeader::default();
        header.set_consumer_group(Some("workers".into()));
        header.set_topic(Some("orders".into()));
        header.set_queue_id(Some(1));
        header
    });
    write_typed(output, "typed-update-offset.bin", 15, &{
        let mut header = UpdateConsumerOffsetRequestHeader::default();
        header.set_consumer_group(Some("workers".into()));
        header.set_topic(Some("orders".into()));
        header.set_queue_id(Some(1));
        header.set_commit_offset(Some(9));
        header
    });
    write_typed(output, "typed-end-transaction.bin", 37, &{
        let mut __phase18_header_5 = EndTransactionRequestHeader::default();
        __phase18_header_5.set_topic(Some("orders".into()));
        __phase18_header_5.set_producer_group(Some("producers".into()));
        __phase18_header_5.set_tran_state_table_offset(Some(3));
        __phase18_header_5.set_commit_log_offset(Some(9));
        __phase18_header_5.set_commit_or_rollback(Some(8));
        __phase18_header_5.set_from_transaction_check(Some(false));
        __phase18_header_5.set_msg_id(Some("msg-1".into()));
        __phase18_header_5.set_transaction_id(Some("tx-1".into()));
        __phase18_header_5.set_namespace(None);
        __phase18_header_5.set_namespaced(None);
        __phase18_header_5.set_broker_name(None);
        __phase18_header_5.set_oneway(None);
        __phase18_header_5
    });
    write_typed(output, "typed-check-transaction.bin", 39, &{
        let mut __phase18_header_6 = CheckTransactionStateRequestHeader::default();
        __phase18_header_6.set_topic(Some("orders".into()));
        __phase18_header_6.set_tran_state_table_offset(3);
        __phase18_header_6.set_commit_log_offset(9);
        __phase18_header_6.set_msg_id(Some("msg-1".into()));
        __phase18_header_6.set_transaction_id(Some("tx-1".into()));
        __phase18_header_6.set_offset_msg_id(Some("offset-1".into()));
        __phase18_header_6.set_namespace(None);
        __phase18_header_6.set_namespaced(None);
        __phase18_header_6.set_broker_name(None);
        __phase18_header_6.set_oneway(None);
        __phase18_header_6
    });
    let mut check_transaction_response = CheckTransactionStateResponseHeader::default();
    check_transaction_response.set_producer_group(Some("producers".into()));
    check_transaction_response.set_tran_state_table_offset(Some(3));
    check_transaction_response.set_commit_log_offset(Some(9));
    check_transaction_response.set_commit_or_rollback(Some(8));
    write_typed(
        output,
        "typed-check-transaction-response.bin",
        0,
        &check_transaction_response,
    );
    write_typed(output, "typed-reply.bin", 324, &{
        let mut __phase18_header_7 = ReplyMessageRequestHeader::default();
        __phase18_header_7.set_producer_group("producers".into());
        __phase18_header_7.set_topic("orders".into());
        __phase18_header_7.set_default_topic("TBW102".into());
        __phase18_header_7.set_default_topic_queue_nums(4);
        __phase18_header_7.set_queue_id(2);
        __phase18_header_7.set_sys_flag(1);
        __phase18_header_7.set_born_timestamp(5);
        __phase18_header_7.set_flag(7);
        __phase18_header_7.set_properties(Some("CORRELATION_ID=correlation-1".into()));
        __phase18_header_7.set_reconsume_times(None);
        __phase18_header_7.set_unit_mode(false);
        __phase18_header_7.set_born_host("127.0.0.1:1001".into());
        __phase18_header_7.set_store_host("127.0.0.1:10911".into());
        __phase18_header_7.set_store_timestamp(6);
        __phase18_header_7
    });
    let binary_reply = vec![0, 255, 128, 42, 10];
    write_typed_body(
        output,
        "typed-reply-v2.bin",
        325,
        &{
            let mut __phase18_header_8 = SendMessageRequestHeaderV2::default();
            __phase18_header_8.set_a(Some("producers".into()));
            __phase18_header_8.set_b(Some("orders".into()));
            __phase18_header_8.set_c(Some("TBW102".into()));
            __phase18_header_8.set_d(Some(4));
            __phase18_header_8.set_e(Some(2));
            __phase18_header_8.set_f(Some(1));
            __phase18_header_8.set_g(Some(5));
            __phase18_header_8.set_h(Some(7));
            __phase18_header_8.set_i(Some("CORRELATION_ID=correlation-1".into()));
            __phase18_header_8.set_j(Some(0));
            __phase18_header_8.set_k(Some(false));
            __phase18_header_8.set_l(None);
            __phase18_header_8.set_m(Some(false));
            __phase18_header_8
        },
        binary_reply.clone(),
    );
    write_typed_body(
        output,
        "typed-reply-push.bin",
        326,
        &{
            let mut __phase18_header_9 = ReplyMessageRequestHeader::default();
            __phase18_header_9.set_producer_group("producers".into());
            __phase18_header_9.set_topic("orders".into());
            __phase18_header_9.set_default_topic("TBW102".into());
            __phase18_header_9.set_default_topic_queue_nums(4);
            __phase18_header_9.set_queue_id(2);
            __phase18_header_9.set_sys_flag(1);
            __phase18_header_9.set_born_timestamp(5);
            __phase18_header_9.set_flag(7);
            __phase18_header_9.set_properties(Some("CORRELATION_ID=correlation-1".into()));
            __phase18_header_9.set_reconsume_times(None);
            __phase18_header_9.set_unit_mode(false);
            __phase18_header_9.set_born_host("127.0.0.1:1001".into());
            __phase18_header_9.set_store_host("127.0.0.1:10911".into());
            __phase18_header_9.set_store_timestamp(6);
            __phase18_header_9
        },
        binary_reply,
    );
    let mut pull_response = PullMessageResponseHeader::default();
    pull_response.set_suggest_which_broker_id(Some(0));
    pull_response.set_next_begin_offset(Some(10));
    pull_response.set_min_offset(Some(0));
    pull_response.set_max_offset(Some(100));
    write_typed(output, "typed-pull-response.bin", 0, &pull_response);
    let mut send_response = SendMessageResponseHeader::default();
    send_response.set_msg_id(Some("msg-1".into()));
    send_response.set_queue_id(Some(2));
    send_response.set_queue_offset(Some(99));
    write_typed(output, "typed-send-response.bin", 0, &send_response);
    write_typed(output, "typed-notification.bin", 200054, &{
        let mut __phase18_header_10 = NotificationRequestHeader::default();
        __phase18_header_10.set_consumer_group("workers".into());
        __phase18_header_10.set_topic("orders".into());
        __phase18_header_10.set_queue_id(1);
        __phase18_header_10.set_poll_time(1000);
        __phase18_header_10.set_born_time(1_712_345_678_901);
        __phase18_header_10.set_order(Some(false));
        __phase18_header_10
    });
    let mut notification_response = NotificationResponseHeader::default();
    notification_response.set_has_msg(true);
    write_typed(
        output,
        "typed-notification-response.bin",
        0,
        &notification_response,
    );
    write_typed(output, "typed-reset-offset.bin", 211, &{
        let mut __phase18_header_11 = ResetOffsetRequestHeader::default();
        __phase18_header_11.set_group("workers".into());
        __phase18_header_11.set_topic("orders".into());
        __phase18_header_11.set_queue_id(1);
        __phase18_header_11.set_offset(Some(9));
        __phase18_header_11.set_timestamp(1_712_345_678_901);
        __phase18_header_11.set_force(true);
        __phase18_header_11
    });
    write_typed(output, "typed-reset-master-flush-offset.bin", 908, &{
        let mut header = ResetMasterFlushOffsetHeader::default();
        header.set_master_flush_offset(Some(9_876_543_210));
        header
    });
    write_typed(output, "typed-export-rocksdb-config-request.bin", 355, &{
        let mut header = ExportRocksDBConfigToJsonRequestHeader::default();
        header.update_config_type(&[
            ExportRocksDBConfigType::Topics,
            ExportRocksDBConfigType::SubscriptionGroups,
            ExportRocksDBConfigType::ConsumerOffsets,
        ]);
        header
    });
    write_typed(
        output,
        "typed-check-rocksdb-cq-write-progress-request.bin",
        354,
        &{
            let mut header = CheckRocksdbCqWriteProgressRequestHeader::default();
            header.set_topic(Some("orders".into()));
            header.set_check_store_time(1_712_345_678_901);
            header
        },
    );
    write_typed(output, "typed-pop.bin", 200050, &{
        let mut __phase18_header_12 = PopMessageRequestHeader::default();
        __phase18_header_12.set_consumer_group(Some("workers".into()));
        __phase18_header_12.set_topic(Some("orders".into()));
        __phase18_header_12.set_queue_id(1);
        __phase18_header_12.set_max_msg_nums(16);
        __phase18_header_12.set_invisible_time(30_000);
        __phase18_header_12.set_poll_time(1_000);
        __phase18_header_12.set_born_time(1_712_345_678_901);
        __phase18_header_12.set_init_mode(0);
        __phase18_header_12.set_order(Some(false));
        __phase18_header_12
    });
    let mut pop_lite_response = PopLiteMessageResponseHeader::default();
    pop_lite_response.set_pop_time(10);
    pop_lite_response.set_invisible_time(20);
    pop_lite_response.set_revive_qid(1);
    pop_lite_response.set_start_offset_info(Some("orders@0:100".into()));
    pop_lite_response.set_msg_offset_info(Some("orders@0:101".into()));
    pop_lite_response.set_order_count_info(Some("0".into()));
    write_typed(output, "typed-pop-lite-response.bin", 0, &pop_lite_response);
    let mut pop_response = PopMessageResponseHeader::default();
    pop_response.set_pop_time(10);
    pop_response.set_invisible_time(20);
    pop_response.set_revive_qid(1);
    pop_response.set_rest_num(3);
    pop_response.set_start_offset_info(Some("orders@0:100".into()));
    pop_response.set_msg_offset_info(Some("orders@0:101".into()));
    pop_response.set_order_count_info(Some("0".into()));
    write_typed(output, "typed-pop-response.bin", 0, &pop_response);
    write_typed(output, "typed-ack.bin", 200051, &{
        let mut __phase18_header_13 = AckMessageRequestHeader::default();
        __phase18_header_13.set_consumer_group("workers".into());
        __phase18_header_13.set_topic("orders".into());
        __phase18_header_13.set_queue_id(1);
        __phase18_header_13.set_extra_info("startOffset popTime invisibleTime queueId".into());
        __phase18_header_13.set_offset(9);
        __phase18_header_13.set_lite_topic(None);
        __phase18_header_13
    });
    write_typed(output, "typed-change-invisible.bin", 200053, &{
        let mut __phase18_header_14 = ChangeInvisibleTimeRequestHeader::default();
        __phase18_header_14.set_consumer_group(Some("workers".into()));
        __phase18_header_14.set_topic(Some("orders".into()));
        __phase18_header_14.set_queue_id(Some(1));
        __phase18_header_14
            .set_extra_info(Some("startOffset popTime invisibleTime queueId".into()));
        __phase18_header_14.set_offset(Some(9));
        __phase18_header_14.set_invisible_time(Some(60_000));
        __phase18_header_14.set_lite_topic(None);
        __phase18_header_14.set_suspend(false);
        __phase18_header_14
    });
}

fn write_oracle_header<H: CommandHeader>(
    output: &std::path::Path,
    name: &str,
    stem: &str,
    code: i32,
) {
    let path = format!(
        "{}/../../fixtures/remoting/{stem}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let metadata: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).expect("read Java header oracle"))
            .expect("parse Java header oracle");
    assert_eq!(metadata["java_client_version"], "5.5.1", "{stem}");
    assert_eq!(metadata["request_code"], code, "{stem}");
    let fields = serde_json::from_value(metadata["normalized_fields"]["ext_fields"].clone())
        .expect("parse Java ext fields");
    let header = H::decode_fields(&fields).expect("decode Java header oracle");
    write_typed(output, name, code, &header);
}

fn write_typed(output: &std::path::Path, name: &str, code: i32, header: &impl CommandHeader) {
    write_typed_body(output, name, code, header, Vec::new());
}

fn write_typed_body(
    output: &std::path::Path,
    name: &str,
    code: i32,
    header: &impl CommandHeader,
    body: Vec<u8>,
) {
    let mut ext_fields = std::collections::BTreeMap::new();
    header.encode_fields(&mut ext_fields);
    let command = RemotingCommand {
        code,
        language: Some(LanguageCode::Rust),
        opaque: 101,
        ext_fields: Some(ext_fields.into()),
        body: if body.is_empty() { None } else { Some(body) },
        ..RemotingCommand::default()
    };
    fs::write(
        output.join(name),
        frame::encode(&command).expect("encode typed header"),
    )
    .expect("write typed header fixture");
}

fn write_body(output: &std::path::Path, name: &str, code: i32, body: &impl serde::Serialize) {
    write_body_bytes(
        output,
        name,
        code,
        serde_json::to_vec(body).expect("serialize typed body"),
    );
}

fn write_body_bytes(output: &std::path::Path, name: &str, code: i32, body: Vec<u8>) {
    let command = RemotingCommand {
        code,
        language: Some(LanguageCode::Rust),
        opaque: 102,
        body: Some(body),
        ..RemotingCommand::default()
    };
    fs::write(
        output.join(name),
        frame::encode(&command).expect("encode typed body frame"),
    )
    .expect("write typed body fixture");
}

#[allow(clippy::mutable_key_type)] // Java DTO Set keys hash by stable object identity.
fn generate_p4_bodies(output: &std::path::Path) {
    let mut query_assignment = QueryAssignmentRequestBody::default();
    query_assignment.set_topic(Some("orders".into()));
    query_assignment.set_consumer_group(Some("workers".into()));
    query_assignment.set_client_id(Some("client".into()));
    query_assignment.set_strategy_name(Some("AllocateMessageQueueAveragely".into()));
    query_assignment.set_message_model(Some(
        rocketmq_classic_protocol::heartbeat::MessageModel::Clustering,
    ));
    write_body(output, "body-query-assignment.bin", 400, &query_assignment);

    let topic_config = TopicConfig::new("orders");
    let topic_configs = std::collections::BTreeMap::from([("orders".to_owned(), topic_config)]);
    let topic_config_wrapper = TopicConfigSerializeWrapper::new(
        Some(DataVersion::new(7, 1_712_345_678_901, Some(4))),
        Some(topic_configs),
    );
    write_body(
        output,
        "body-topic-config-wrapper.bin",
        21,
        &topic_config_wrapper,
    );

    let mut workers = SubscriptionGroupConfig::default();
    workers.set_group_name(Some("workers".into()));
    workers.set_retry_queue_nums(5);
    workers.set_consume_broadcast_enable(false);
    let mut analytics = SubscriptionGroupConfig::default();
    analytics.set_group_name(Some("analytics".into()));
    analytics.set_consume_message_orderly(true);
    let group_wrapper = SubscriptionGroupWrapper::new(
        Some(DataVersion::new(6, 1_712_345_678_902, Some(9))),
        Some(std::collections::BTreeMap::from([
            ("workers".into(), workers),
            ("analytics".into(), analytics),
        ])),
        Some(std::collections::BTreeMap::from([
            (
                "workers".into(),
                std::collections::BTreeMap::from([("orders".into(), 1), ("payments".into(), 0)]),
            ),
            (
                "analytics".into(),
                std::collections::BTreeMap::from([("alerts".into(), 1)]),
            ),
        ])),
    );
    write_body(
        output,
        "body-subscription-group-wrapper.bin",
        201,
        &group_wrapper,
    );

    let mut subscription_data = rocketmq_classic_protocol::heartbeat::SubscriptionData::new(
        Some("orders".into()),
        Some("*".into()),
    );
    subscription_data.set_sub_version(1234);
    let mut consume_queue_row = ConsumeQueueData::default();
    consume_queue_row.set_physic_offset(987_654_321);
    consume_queue_row.set_physic_size(128);
    consume_queue_row.set_tags_code(171_234);
    consume_queue_row.set_extend_data_json(Some("{\"filterBitMap\":\"AQID\"}".into()));
    consume_queue_row.set_bit_map(Some("1010".into()));
    consume_queue_row.set_eval(true);
    consume_queue_row.set_msg(Some("message-body".into()));
    let mut consume_queue_body = QueryConsumeQueueResponseBody::default();
    consume_queue_body.set_subscription_data(Some(SharedValue::new(subscription_data)));
    consume_queue_body.set_filter_data(Some("filter-data".into()));
    consume_queue_body.set_queue_data(Some(SharedValue::new(vec![Some(SharedValue::new(
        consume_queue_row,
    ))])));
    consume_queue_body.set_max_queue_index(20);
    consume_queue_body.set_min_queue_index(4);
    write_body(
        output,
        "body-query-consume-queue.bin",
        321,
        &consume_queue_body,
    );

    let mut request_mode = SetMessageRequestModeRequestBody::default();
    request_mode.set_topic(Some("orders".into()));
    request_mode.set_consumer_group(Some("workers".into()));
    request_mode.set_mode(Some(MessageRequestMode::Pop));
    request_mode.set_pop_share_queue_num(2);
    write_body(output, "body-set-request-mode.bin", 401, &request_mode);
    write_body(
        output,
        "body-lite-subscription.bin",
        200071,
        &LiteSubscriptionCtlRequestBody {
            subscription_set: Some(SharedValue::new(std::collections::HashSet::from([Some(
                SharedValue::new(LiteSubscriptionDto {
                    action: Some(LiteSubscriptionAction::CompleteAdd),
                    client_id: Some("client".into()),
                    group: Some("workers".into()),
                    topic: Some("orders".into()),
                    lite_topic_set: Some(SharedValue::new(std::collections::BTreeSet::from([
                        Some("lite-orders".into()),
                    ]))),
                    offset_option: Some(SharedValue::new(OffsetOption {
                        option_type: Some(OffsetOptionType::Policy),
                        value: 1,
                    })),
                    version: 2,
                }),
            )]))),
        },
    );
    let mut batch_ack = BatchAck::default();
    batch_ack.set_consumer_group(Some("workers".into()));
    batch_ack.set_topic(Some("orders".into()));
    batch_ack.set_retry(Some("0".into()));
    batch_ack.set_start_offset(100);
    batch_ack.set_queue_id(1);
    batch_ack.set_revive_queue_id(0);
    batch_ack.set_pop_time(10);
    batch_ack.set_invisible_time(20);
    batch_ack.set_bit_set(Some(SharedValue::new(
        rocketmq_classic_protocol::body::BitSet::from_byte_array(&[0b0000_0101]),
    )));
    let mut batch_ack_body = BatchAckMessageRequestBody::default();
    batch_ack_body.set_broker_name(Some("broker-a".into()));
    batch_ack_body.set_acks(Some(SharedValue::new(vec![Some(SharedValue::new(
        batch_ack,
    ))])));
    write_body(output, "body-batch-ack.bin", 200151, &batch_ack_body);
    let mut reset_offset = ResetOffsetBody::default();
    reset_offset.set_offset_table(Some(SharedValue::new(vec![(
        rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 1),
        Some(9),
    )])));
    write_body_bytes(
        output,
        "body-reset-offset.bin",
        212,
        reset_offset
            .encode_java_json()
            .expect("encode Java-compatible reset body"),
    );

    let mut consumer_status = GetConsumerStatusBody::default();
    consumer_status.set_message_queue_table(Some(SharedValue::new(vec![(
        rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 1),
        Some(9),
    )])));
    consumer_status.set_consumer_table(Some(SharedValue::new(std::collections::BTreeMap::new())));
    write_body_bytes(
        output,
        "body-consumer-status.bin",
        223,
        consumer_status
            .encode_java_json()
            .expect("encode Java-compatible consumer status body"),
    );
    let mut orders_offset = OffsetWrapper::default();
    orders_offset.set_broker_offset(29);
    orders_offset.set_consumer_offset(20);
    orders_offset.set_pull_offset(25);
    orders_offset.set_last_timestamp(1000);
    let mut payments_offset = OffsetWrapper::default();
    payments_offset.set_broker_offset(42);
    payments_offset.set_consumer_offset(38);
    payments_offset.set_pull_offset(41);
    payments_offset.set_last_timestamp(2000);
    let mut consume_stats = ConsumeStats::default();
    consume_stats.set_offset_table(Some(SharedValue::new(std::collections::HashMap::from([
        (
            rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 1),
            Some(SharedValue::new(orders_offset)),
        ),
        (
            rocketmq_classic_common::message::MessageQueue::new("payments", "broker-b", 2),
            Some(SharedValue::new(payments_offset)),
        ),
    ]))));
    consume_stats.set_consume_tps(1.25);
    write_body_bytes(
        output,
        "body-consume-stats.bin",
        208,
        consume_stats
            .java_encode()
            .expect("encode Java-compatible consume stats body"),
    );
    let mut lite_group_info = GetLiteGroupInfoResponseBody::default();
    lite_group_info.set_group(Some("workers".into()));
    lite_group_info.set_parent_topic(Some("lite-parent".into()));
    lite_group_info.set_lite_topic(Some("%LMQ%lite-parent%orders".into()));
    lite_group_info.set_earliest_unconsumed_timestamp(1400);
    lite_group_info.set_total_lag_count(40);
    let mut lite_offset = OffsetWrapper::default();
    lite_offset.set_broker_offset(100);
    lite_offset.set_consumer_offset(80);
    lite_offset.set_pull_offset(85);
    lite_offset.set_last_timestamp(1700);
    lite_group_info.set_lite_topic_offset_wrapper(Some(SharedValue::new(lite_offset)));
    let mut orders_lag = LiteLagInfo::default();
    orders_lag.set_lite_topic(Some("%LMQ%lite-parent%orders".into()));
    orders_lag.set_lag_count(30);
    orders_lag.set_earliest_unconsumed_timestamp(1600);
    orders_lag.set_consumer_offset(70);
    let mut payments_lag = LiteLagInfo::default();
    payments_lag.set_lite_topic(Some("%LMQ%lite-parent%payments".into()));
    payments_lag.set_lag_count(10);
    payments_lag.set_earliest_unconsumed_timestamp(1500);
    payments_lag.set_consumer_offset(90);
    lite_group_info.set_lag_count_top_k(Some(SharedValue::new(vec![
        Some(SharedValue::new(orders_lag.clone())),
        None,
        Some(SharedValue::new(payments_lag.clone())),
    ])));
    lite_group_info.set_lag_timestamp_top_k(Some(SharedValue::new(vec![
        Some(SharedValue::new(payments_lag)),
        Some(SharedValue::new(orders_lag)),
    ])));
    write_body_bytes(
        output,
        "body-lite-group-info.bin",
        200078,
        lite_group_info
            .java_encode()
            .expect("encode Java-compatible Lite group info body"),
    );
    let mut consume_directly = ConsumeMessageDirectlyResult::default();
    consume_directly.set_order(false);
    consume_directly.set_auto_commit(true);
    consume_directly.set_consume_result(Some(ConsumeMessageResult::CrSuccess));
    consume_directly.set_remark(None);
    consume_directly.set_spent_time_mills(2);
    write_body(output, "body-consume-directly.bin", 309, &consume_directly);
    let queue = rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 1);
    let absent_consumer_list = ConsumerListByGroupResponseBody::default();
    let mut empty_consumer_list = ConsumerListByGroupResponseBody::default();
    empty_consumer_list.set_consumer_id_list(Some(SharedValue::new(vec![])));
    let mut populated_consumer_list = ConsumerListByGroupResponseBody::default();
    populated_consumer_list.set_consumer_id_list(Some(SharedValue::new(vec![
        Some("client-a".into()),
        Some("client-b".into()),
    ])));
    for (name, body) in [
        ("body-consumer-list-absent.bin", absent_consumer_list),
        ("body-consumer-list-empty.bin", empty_consumer_list),
        ("body-consumer-list-populated.bin", populated_consumer_list),
    ] {
        write_body(output, name, 38, &body);
    }
    let queues = || {
        SharedValue::new(std::collections::HashSet::from([Some(SharedValue::new(
            queue.clone(),
        ))]))
    };
    let mut lock_request = LockBatchRequestBody::default();
    lock_request.set_consumer_group(Some("workers".into()));
    lock_request.set_client_id(Some("client".into()));
    lock_request.set_only_this_broker(false);
    lock_request.set_mq_set(Some(queues()));
    write_body(output, "body-lock-request.bin", 41, &lock_request);

    let mut unlock_request = UnlockBatchRequestBody::default();
    unlock_request.set_consumer_group(Some("workers".into()));
    unlock_request.set_client_id(Some("client".into()));
    unlock_request.set_only_this_broker(true);
    unlock_request.set_mq_set(Some(queues()));
    write_body(output, "body-unlock-request.bin", 42, &unlock_request);
    write_body(output, "body-lock-response.bin", 41, &{
        let mut body = LockBatchResponseBody::default();
        body.set_lock_okmq_set(Some(SharedValue::new(std::collections::HashSet::from([
            Some(SharedValue::new(queue.clone())),
        ]))));
        body
    });
    write_body(output, "body-unlock-response.bin", 42, &{
        let mut body = LockBatchResponseBody::default();
        body.set_lock_okmq_set(Some(SharedValue::new(std::collections::HashSet::from([
            Some(SharedValue::new(queue)),
        ]))));
        body
    });
}
