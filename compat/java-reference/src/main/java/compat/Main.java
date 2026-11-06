package compat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.FastCodesHeader;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.SerializeType;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetLiteTopicInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetLiteClientInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetLiteGroupInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetParentTopicInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.NotificationRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PollingInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PollingInfoResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.ReplyMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.NotificationResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.NotifyUnsubscribeLiteRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.HeartbeatRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.LiteSubscriptionCtlRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.LockBatchMqRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UnlockBatchMqRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicStatsInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVConfigResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVListByNamespaceRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.PutKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QuerySubscriptionByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicsByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientResponseHeader;
import org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody;
import org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody;
import org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody;
import org.apache.rocketmq.remoting.protocol.body.BatchAck;
import org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody;
import org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.remoting.protocol.body.GetLiteGroupInfoResponseBody;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.remoting.protocol.body.CMResult;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.lite.LiteSubscriptionDTO;
import org.apache.rocketmq.common.lite.LiteLagInfo;
import org.apache.rocketmq.common.lite.LiteSubscriptionAction;
import org.apache.rocketmq.common.lite.OffsetOption;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;

public final class Main {
    private static final int MAX_FRAME_SIZE = 16 * 1024 * 1024;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && "extension-more-fixture".equals(args[0])) {
            ExtensionMoreInvokedFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "change-invisible-time-header-decode-mode-fixture".equals(args[0])) {
            ChangeInvisibleTimeHeaderDecodeModeFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "change-invisible-time-response-header-fixture".equals(args[0])) {
            ChangeInvisibleTimeResponseHeaderFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "query-admin-header-decode-mode-fixture".equals(args[0])) {
            QueryAdminHeaderDecodeModeFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "code-registry-fixtures".equals(args[0])) {
            CodeRegistryFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "orderly-consumer-fixture".equals(args[0])) {
            OrderlyConsumerFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "lite-pop-fixture".equals(args[0])) {
            LitePopFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "pull-implementation-accessor-fixture".equals(args[0])) {
            PullImplementationAccessorFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "push-implementation-accessor-fixture".equals(args[0])) {
            PushImplementationAccessorFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "consumer-facade-api-fixture".equals(args[0])) {
            ConsumerFacadeApiFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "consumer-expired-message-fixture".equals(args[0])) {
            ConsumerExpiredMessageFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "transaction-fixture".equals(args[0])) {
            TransactionFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "request-reply-fixture".equals(args[0])) {
            RequestReplyFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "remoting-api-surface-fixture".equals(args[0])) {
            RemotingApiSurfaceFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "consumer-running-info-fixture".equals(args[0])) {
            ConsumerRunningInfoFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "protocol-dto-identity-fixture".equals(args[0])) {
            ProtocolDtoIdentityFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "header-response-identity-fixture".equals(args[0])) {
            HeaderResponseIdentityFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "request-header-identity-fixture".equals(args[0])) {
            RequestHeaderIdentityFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "acl-fixture".equals(args[0])) {
            AclFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "trace-fixture".equals(args[0])) {
            TraceFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "namespace-fixture".equals(args[0])) {
            NamespaceFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "topic-validator-fixture".equals(args[0])) {
            TopicValidatorFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "tls-settings-fixture".equals(args[0])) {
            TlsSettingsFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "client-config-tls-mode-probe".equals(args[0])) {
            ClientConfigTlsModeProbe.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "client-config-properties-probe".equals(args[0])) {
            ClientConfigPropertiesProbe.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "runtime-hook-fixtures".equals(args[0])) {
            RuntimeHookFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "runtime-discovery-fixtures".equals(args[0])) {
            RuntimeDiscoveryFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "runtime-inbound-routing-fixtures".equals(args[0])) {
            RuntimeInboundRoutingFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "runtime-remoting-shutdown-fixtures".equals(args[0])) {
            RuntimeRemotingShutdownFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "runtime-scheduled-executor-shutdown-fixtures".equals(args[0])) {
            RuntimeScheduledExecutorShutdownFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "runtime-heartbeat-fixtures".equals(args[0])) {
            RuntimeHeartbeatFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "runtime-route-fixtures".equals(args[0])) {
            RuntimeRouteFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "runtime-config-fixtures".equals(args[0])) {
            RuntimeConfigFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "pull-header-decode-mode-fixtures".equals(args[0])) {
            PullHeaderDecodeModeFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "pull-response-header-decode-mode-fixtures".equals(args[0])) {
            PullMessageResponseHeaderDecodeModeFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "send-message-response-header-decode-mode-fixtures".equals(args[0])) {
            SendMessageResponseHeaderDecodeModeFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "send-message-request-v2-decode-mode-fixtures".equals(args[0])) {
            SendMessageRequestHeaderV2DecodeModeFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "offset-header-decode-mode-fixtures".equals(args[0])) {
            OffsetHeaderDecodeModeFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "create-topic-header-decode-mode-fixtures".equals(args[0])) {
            CreateTopicHeaderDecodeModeFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "end-transaction-header-decode-mode-fixtures".equals(args[0])) {
            EndTransactionHeaderDecodeModeFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "query-admin-header-decode-mode-fixtures".equals(args[0])) {
            QueryAdminHeaderDecodeModeFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "topic-list-fixture".equals(args[0])) {
            TopicListFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "query-subscription-body-fixture".equals(args[0])) {
            QuerySubscriptionBodyFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "group-list-fixture".equals(args[0])) {
            GroupListFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "topic-stats-table-fixture".equals(args[0])) {
            TopicStatsTableFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "mq-client-admin-fixtures".equals(args[0])) {
            MqClientAdminFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "offset-boundary-header-decode-mode-fixtures".equals(args[0])) {
            OffsetBoundaryHeaderDecodeModeFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "client-config-properties-fixture".equals(args[0])) {
            ClientConfigPropertiesProbe.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "runtime-address-fixtures".equals(args[0])) {
            RuntimeAddressFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "runtime-lookup-fixtures".equals(args[0])) {
            RuntimeLookupFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "runtime-client-check-fixtures".equals(args[0])) {
            RuntimeClientCheckFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "runtime-offset-fixtures".equals(args[0])) {
            RuntimeOffsetFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "runtime-api-factory-fixtures".equals(args[0])) {
            RuntimeApiFactoryFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "runtime-consumer-operations-fixtures".equals(args[0])) {
            RuntimeConsumerOperationsFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "producer-core-fixtures".equals(args[0])) {
            ProducerCoreFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "query-admin-header-decode-mode-fixture".equals(args[0])) {
            QueryAdminHeaderDecodeModeFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "producer-lifecycle-fixtures".equals(args[0])) {
            ProducerLifecycleFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "consumer-lifecycle-fixtures".equals(args[0])) {
            ConsumerLifecycleFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "message-decode-fixtures".equals(args[0])) {
            MessageDecodeFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "message-utils-fixture".equals(args[0])) {
            MessageUtilsFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "rebalance-fixtures".equals(args[0])) {
            RebalanceFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "rebalance-initial-offset-fixtures".equals(args[0])) {
            RebalanceInitialOffsetFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "rpc-invoked-ext-fixtures".equals(args[0])) {
            InvokedExtFixtures.run(option(args, "--output"), java.util.Arrays.asList(args).contains("--verify"));
            return;
        }
        if (args.length > 0 && "rpc-invoked-ext-more-fixtures".equals(args[0])) {
            ExtensionMoreInvokedFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "rpc-invoked-fixtures".equals(args[0])) {
            InvokedRpcFixtures.run(option(args, "--output"), java.util.Arrays.asList(args).contains("--verify"));
            return;
        }
        if (args.length > 0 && "rpc-invoked-pop-ack-fixtures".equals(args[0])) {
            PopAckInvokedFixtures.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "runtime-hook-fixtures".equals(args[0])) {
            RuntimeHookFixtures.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && "invoked-rpc-oracle".equals(args[0])) {
            InvokedRpcOracle.run();
            return;
        }
        if (args.length == 0 || "self-test".equals(args[0])) {
            selfTest();
            InvokedRpcOracle.run();
            return;
        }
        if ("fixtures".equals(args[0])) {
            Path output = option(args, "--output");
            generateFixtures(output);
            return;
        }
        if ("header-fixtures".equals(args[0])) {
            Path output = option(args, "--output");
            generateHeaderFixtures(output);
            return;
        }
        if ("body-fixtures".equals(args[0])) {
            generateBodyFixtures(option(args, "--output"));
            return;
        }
        if ("rpc-fixtures".equals(args[0])) {
            RpcFixtures.generate(option(args, "--output"));
            return;
        }
        if ("verify-rpc-frames".equals(args[0])) {
            RpcFixtures.verify(option(args, "--dir"), option(args, "--oracle"));
            return;
        }
        if ("decode".equals(args[0])) {
            Path input = option(args, "--input");
            System.out.println(toJson(decode(Files.readAllBytes(input))));
            return;
        }
        if ("verify-rust-fixtures".equals(args[0])) {
            verifyRustFixtures(option(args, "--dir"));
            return;
        }
        if ("verify-rust-bodies".equals(args[0])) {
            verifyRustBodies(option(args, "--dir"));
            return;
        }
        if ("scenario".equals(args[0])) {
            String id = option(args, "--id").toString();
            Path output = option(args, "--output");
            writeScenario(id, output);
            return;
        }
        throw new IllegalArgumentException("usage: self-test | fixtures --output DIR | header-fixtures --output DIR | code-registry-fixtures <file> [--verify] | client-config-properties-fixture <file> [--verify] | change-invisible-time-header-decode-mode-fixture <file> [--verify] | change-invisible-time-response-header-fixture <file> [--verify] | lite-pop-fixture <file> [--verify] | transaction-fixture <file> [--verify] | request-reply-fixture <file> [--verify] | remoting-api-surface-fixture <file> [--verify] | orderly-consumer-fixture <file> [--verify] | rebalance-fixtures <file> [--verify] | message-utils-fixture <file> [--verify] | rpc-invoked-ext-more-fixtures <DIR> [--verify] | decode --input FILE | scenario --id ID --output FILE");
    }

    private static Path option(String[] args, String name) {
        for (int i = 0; i + 1 < args.length; i++) {
            if (name.equals(args[i])) {
                return Path.of(args[i + 1]);
            }
        }
        throw new IllegalArgumentException("missing option " + name);
    }

    private static void selfTest() throws Exception {
        for (String id : new String[] {"ROUTE-001-get-existing-topic", "HEARTBEAT-001", "PROD-001-sync-send",
            "CONS-002-pull-found", "CONS-003-pull-no-new-msg", "CONS-001-query-offset",
            "CONS-004-update-offset", "ORDER-001-lock-unlock"}) {
            int code = id.startsWith("ROUTE") ? 105 : id.startsWith("HEARTBEAT") ? 34
                : id.startsWith("PROD") ? 10 : id.startsWith("CONS-001") ? 14
                : id.startsWith("CONS-004") ? 15 : id.startsWith("ORDER") ? 41 : 11;
            SerializeType type = id.startsWith("HEARTBEAT") || id.startsWith("CONS-002")
                || id.startsWith("CONS-003") || id.startsWith("CONS-004") ? SerializeType.ROCKETMQ : SerializeType.JSON;
            byte[] frame = bytes(command(code, type, id).encode());
            Map<String, Object> decoded = decode(frame);
            require(((Number) decoded.get("code")).intValue() == code, "fixture code round-trip");
            require(id.equals(decoded.get("remark")), "fixture semantic round-trip");
            require(Arrays.equals(frame, bytes(RemotingCommand.decode(Arrays.copyOfRange(frame, 4, frame.length)).encode())),
                "fixture deterministic re-encode");
        }
        for (SerializeType type : SerializeType.values()) {
            RemotingCommand command = command(34, type, "oracle-self-test");
            byte[] frame = bytes(command.encode());
            Map<String, Object> decoded = decode(frame);
            require(((Number) decoded.get("code")).intValue() == 34, "code round-trip");
            require(type.name().equals(decoded.get("serialization")), "serialization round-trip");
            require("oracle-self-test".equals(decoded.get("remark")), "remark round-trip");
            require(Arrays.equals(frame, bytes(RemotingCommand.decode(Arrays.copyOfRange(frame, 4, frame.length)).encode())), "deterministic re-encode");
        }
        boolean malformedRejected = false;
        try {
            decode(new byte[] {0, 0, 0, 99, 0, 0, 0, 0});
        } catch (IOException expected) {
            malformedRejected = true;
        }
        require(malformedRejected, "malformed frame rejected");
        System.out.println("Java reference self-test passed");
    }

    private static void generateFixtures(Path output) throws Exception {
        Files.createDirectories(output);
        String[] ids = {"ROUTE-001-get-existing-topic", "HEARTBEAT-001", "PROD-001-sync-send",
            "CONS-002-pull-found", "CONS-003-pull-no-new-msg", "CONS-001-query-offset",
            "CONS-004-update-offset", "ORDER-001-lock-unlock", "TX-001-end-transaction",
            "REQREP-001-send-reply", "PRIVATE-LARGE-CODE"};
        int[] codes = {105, 34, 10, 11, 11, 14, 15, 41, 37, 324, 987654};
        for (int i = 0; i < ids.length; i++) {
            writeFixture(output, ids[i], codes[i], i % 2 == 0 ? SerializeType.JSON : SerializeType.ROCKETMQ);
            for (SerializeType type : SerializeType.values()) {
                writeFixture(output, ids[i] + "-" + type.name(), codes[i], type);
            }
        }
        writeFixture(output, "HEADER-PROOF-001", 10, SerializeType.JSON);
        writeFixture(output, "PRIVATE-LARGE-CODE", 987654, SerializeType.ROCKETMQ);
        RemotingCommand largeCodeAndVersion = command(987654, SerializeType.ROCKETMQ,
            "PRIVATE-LARGE-CODE-VERSION");
        largeCodeAndVersion.setVersion(987654);
        writeFixture(output, "PRIVATE-LARGE-CODE-VERSION", 987654, SerializeType.ROCKETMQ,
            largeCodeAndVersion, bytes(largeCodeAndVersion.encode()));
        writeOversizedKeyFixture(output);
        writeFastHeaderFixtures(output);
        selfTest();
    }

    private static void generateHeaderFixtures(Path output) throws Exception {
        Files.createDirectories(output);
        GetRouteInfoRequestHeader route = new GetRouteInfoRequestHeader();
        route.setTopic("orders");
        writeTypedHeaderFixture(output, "route-request", 105, route);

        CreateTopicRequestHeader createTopic = new CreateTopicRequestHeader();
        createTopic.setTopic("orders");
        createTopic.setDefaultTopic("TBW102");
        createTopic.setReadQueueNums(4);
        createTopic.setWriteQueueNums(4);
        createTopic.setPerm(6);
        createTopic.setTopicFilterType("SINGLE_TAG");
        createTopic.setTopicSysFlag(3);
        createTopic.setAttributes("message.type=normal");
        writeTypedHeaderFixture(output, "create-topic-request", 17, createTopic);

        org.apache.rocketmq.remoting.protocol.header.CreateUserRequestHeader createUser =
            new org.apache.rocketmq.remoting.protocol.header.CreateUserRequestHeader("alice");
        writeTypedHeaderFixture(output, "create-user-request", 3001, createUser);

        org.apache.rocketmq.remoting.protocol.header.CreateAclRequestHeader createAcl =
            new org.apache.rocketmq.remoting.protocol.header.CreateAclRequestHeader("subject");
        writeTypedHeaderFixture(output, "create-acl-request", 3006, createAcl);

        org.apache.rocketmq.remoting.protocol.header.DeleteAclRequestHeader deleteAcl =
            new org.apache.rocketmq.remoting.protocol.header.DeleteAclRequestHeader("subject", "topic-a");
        deleteAcl.setPolicyType("POLICY");
        writeTypedHeaderFixture(output, "delete-acl-request", 3008, deleteAcl);

        org.apache.rocketmq.remoting.protocol.header.UpdateAclRequestHeader updateAcl =
            new org.apache.rocketmq.remoting.protocol.header.UpdateAclRequestHeader("subject");
        writeTypedHeaderFixture(output, "update-acl-request", 3007, updateAcl);

        org.apache.rocketmq.remoting.protocol.header.UpdateUserRequestHeader updateUser =
            new org.apache.rocketmq.remoting.protocol.header.UpdateUserRequestHeader("alice");
        writeTypedHeaderFixture(output, "update-user-request", 3002, updateUser);

        org.apache.rocketmq.remoting.protocol.header.DeleteUserRequestHeader deleteUser =
            new org.apache.rocketmq.remoting.protocol.header.DeleteUserRequestHeader("alice");
        writeTypedHeaderFixture(output, "delete-user-request", 3003, deleteUser);

        org.apache.rocketmq.remoting.protocol.header.GetUserRequestHeader getUser =
            new org.apache.rocketmq.remoting.protocol.header.GetUserRequestHeader("alice");
        writeTypedHeaderFixture(output, "get-user-request", 3004, getUser);

        org.apache.rocketmq.remoting.protocol.header.GetAclRequestHeader getAcl =
            new org.apache.rocketmq.remoting.protocol.header.GetAclRequestHeader("subject");
        writeTypedHeaderFixture(output, "get-acl-request", 3009, getAcl);

        org.apache.rocketmq.remoting.protocol.header.ListAclsRequestHeader listAcls =
            new org.apache.rocketmq.remoting.protocol.header.ListAclsRequestHeader("alice", "topic-a");
        writeTypedHeaderFixture(output, "list-acls-request", 3010, listAcls);

        org.apache.rocketmq.remoting.protocol.header.ListUsersRequestHeader listUsers =
            new org.apache.rocketmq.remoting.protocol.header.ListUsersRequestHeader("ali");
        writeTypedHeaderFixture(output, "list-users-request", 3005, listUsers);

        QueryMessageRequestHeader queryMessage = new QueryMessageRequestHeader();
        queryMessage.setTopic("orders");
        queryMessage.setKey("order-42");
        queryMessage.setMaxNum(32);
        queryMessage.setBeginTimestamp(1_712_345_678_000L);
        queryMessage.setEndTimestamp(1_712_345_679_000L);
        queryMessage.setIndexType("UNIQUE_KEY");
        queryMessage.setLastKey("last-index-key");
        writeTypedHeaderFixture(output, "query-message-request", 12, queryMessage);
        QueryMessageResponseHeader queryMessageResponse = new QueryMessageResponseHeader();
        queryMessageResponse.setIndexLastUpdateTimestamp(1_712_345_679_000L);
        queryMessageResponse.setIndexLastUpdatePhyoffset(9876L);
        writeTypedHeaderFixture(output, "query-message-response",
            RemotingCommand.createResponseCommandWithHeader(0, queryMessageResponse));

        ViewMessageRequestHeader viewMessage = new ViewMessageRequestHeader();
        viewMessage.setTopic("orders");
        viewMessage.setOffset(9876L);
        writeTypedHeaderFixture(output, "view-message-request", 33, viewMessage);

        SendMessageRequestHeader send = new SendMessageRequestHeader();
        send.setProducerGroup("producers");
        send.setTopic("orders");
        send.setDefaultTopic("TBW102");
        send.setDefaultTopicQueueNums(4);
        send.setQueueId(2);
        send.setSysFlag(1);
        send.setBornTimestamp(1712345678901L);
        send.setFlag(7);
        send.setProperties("TAGS\\u0001blue");
        send.setReconsumeTimes(0);
        send.setUnitMode(false);
        send.setMaxReconsumeTimes(16);
        send.setBatch(false);
        writeTypedHeaderFixture(output, "send-v1", 10, send);
        writeTypedHeaderFixture(output, "send-v2", 310,
            SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(send));
        writeTypedHeaderFixture(output, "send-batch-v2", 320,
            SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(send));

        PullMessageRequestHeader pull = new PullMessageRequestHeader();
        pull.setConsumerGroup("workers");
        pull.setTopic("orders");
        pull.setLiteTopic("lite-orders");
        pull.setQueueId(1);
        pull.setQueueOffset(9L);
        pull.setMaxMsgNums(32);
        pull.setSysFlag(0);
        pull.setCommitOffset(8L);
        pull.setSuspendTimeoutMillis(15000L);
        pull.setSubVersion(1712345678901L);
        writeTypedHeaderFixture(output, "pull-request", 11, pull);

        PullMessageResponseHeader pullResponse = new PullMessageResponseHeader();
        pullResponse.setSuggestWhichBrokerId(0L);
        pullResponse.setNextBeginOffset(10L);
        pullResponse.setMinOffset(0L);
        pullResponse.setMaxOffset(100L);
        RemotingCommand response = RemotingCommand.createResponseCommandWithHeader(0, pullResponse);
        writeTypedHeaderFixture(output, "pull-response", response);

        QueryConsumerOffsetRequestHeader queryOffset = new QueryConsumerOffsetRequestHeader();
        queryOffset.setConsumerGroup("workers");
        queryOffset.setTopic("orders");
        queryOffset.setQueueId(1);
        writeTypedHeaderFixture(output, "query-offset-request", 14, queryOffset);
        QueryConsumeTimeSpanRequestHeader queryConsumeTimeSpan = new QueryConsumeTimeSpanRequestHeader();
        queryConsumeTimeSpan.setTopic("orders");
        queryConsumeTimeSpan.setGroup("workers");
        queryConsumeTimeSpan.setLo(true);
        writeTypedHeaderFixture(output, "query-consume-time-span-request", 303, queryConsumeTimeSpan);
        QueryTopicsByConsumerRequestHeader queryTopics = new QueryTopicsByConsumerRequestHeader();
        queryTopics.setGroup("workers");
        queryTopics.setNamespace("tenant");
        queryTopics.setNamespaced(true);
        queryTopics.setBrokerName("broker-a");
        queryTopics.setOneway(false);
        writeTypedHeaderFixture(output, "query-topics-by-consumer-request", 343, queryTopics);
        QuerySubscriptionByConsumerRequestHeader querySubscription = new QuerySubscriptionByConsumerRequestHeader();
        querySubscription.setGroup("workers");
        querySubscription.setTopic("orders");
        querySubscription.setNamespace("tenant");
        querySubscription.setNamespaced(true);
        querySubscription.setBrokerName("broker-a");
        querySubscription.setOneway(false);
        querySubscription.setLo(true);
        writeTypedHeaderFixture(output, "query-subscription-by-consumer-request", 345, querySubscription);
        QueryTopicConsumeByWhoRequestHeader queryTopicConsumeByWho = new QueryTopicConsumeByWhoRequestHeader();
        queryTopicConsumeByWho.setTopic("orders");
        queryTopicConsumeByWho.setNamespace("tenant");
        queryTopicConsumeByWho.setNamespaced(true);
        queryTopicConsumeByWho.setBrokerName("broker-a");
        queryTopicConsumeByWho.setOneway(false);
        queryTopicConsumeByWho.setLo(true);
        writeTypedHeaderFixture(output, "query-topic-consume-by-who-request", 300, queryTopicConsumeByWho);
        org.apache.rocketmq.remoting.protocol.header.QueryCorrectionOffsetHeader queryCorrectionOffset =
            new org.apache.rocketmq.remoting.protocol.header.QueryCorrectionOffsetHeader();
        queryCorrectionOffset.setTopic("orders");
        queryCorrectionOffset.setCompareGroup("workers");
        queryCorrectionOffset.setFilterGroups("ignored-group");
        queryCorrectionOffset.setNamespace("tenant");
        queryCorrectionOffset.setNamespaced(true);
        queryCorrectionOffset.setBrokerName("broker-a");
        queryCorrectionOffset.setOneway(false);
        queryCorrectionOffset.setLo(true);
        writeTypedHeaderFixture(output, "query-correction-offset-request", 308, queryCorrectionOffset);
        org.apache.rocketmq.remoting.protocol.header.UpdateGroupForbiddenRequestHeader groupForbidden =
            new org.apache.rocketmq.remoting.protocol.header.UpdateGroupForbiddenRequestHeader();
        groupForbidden.setTopic("orders");
        groupForbidden.setGroup("workers");
        groupForbidden.setReadable(true);
        groupForbidden.setNamespace("tenant");
        groupForbidden.setNamespaced(true);
        groupForbidden.setBrokerName("broker-a");
        groupForbidden.setOneway(false);
        groupForbidden.setLo(true);
        writeTypedHeaderFixture(output, "update-group-forbidden-request", 353, groupForbidden);
        GetTopicStatsInfoRequestHeader topicStatsInfo = new GetTopicStatsInfoRequestHeader();
        topicStatsInfo.setTopic("orders");
        topicStatsInfo.setNamespace("tenant");
        topicStatsInfo.setNamespaced(true);
        topicStatsInfo.setBrokerName("broker-a");
        topicStatsInfo.setOneway(false);
        topicStatsInfo.setLo(true);
        writeTypedHeaderFixture(output, "topic-stats-info-request", 202, topicStatsInfo);
        org.apache.rocketmq.remoting.protocol.header.GetTopicConfigRequestHeader topicConfig =
            new org.apache.rocketmq.remoting.protocol.header.GetTopicConfigRequestHeader();
        topicConfig.setTopic("orders");
        topicConfig.setLo(true);
        topicConfig.setNamespace("tenant");
        topicConfig.setNamespaced(true);
        topicConfig.setBrokerName("broker-a");
        topicConfig.setOneway(false);
        writeTypedHeaderFixture(output, "topic-config-request", 351, topicConfig);
        org.apache.rocketmq.remoting.protocol.header.GetAllSubscriptionGroupRequestHeader allSubscriptionGroups =
            new org.apache.rocketmq.remoting.protocol.header.GetAllSubscriptionGroupRequestHeader();
        allSubscriptionGroups.setGroupSeq(2);
        allSubscriptionGroups.setDataVersion("{\"counter\":4,\"stateVersion\":7,\"timestamp\":1712345678901}");
        allSubscriptionGroups.setMaxGroupNum(2000);
        writeTypedHeaderFixture(output, "get-all-subscription-group-request", 201, allSubscriptionGroups);
        org.apache.rocketmq.remoting.protocol.header.GetAllSubscriptionGroupResponseHeader allSubscriptionGroupsResponse =
            new org.apache.rocketmq.remoting.protocol.header.GetAllSubscriptionGroupResponseHeader();
        allSubscriptionGroupsResponse.setTotalGroupNum(5);
        writeTypedHeaderFixture(output, "get-all-subscription-group-response",
            RemotingCommand.createResponseCommandWithHeader(0, allSubscriptionGroupsResponse));
        org.apache.rocketmq.remoting.protocol.header.QueryConsumeQueueRequestHeader consumeQueueHeader =
            new org.apache.rocketmq.remoting.protocol.header.QueryConsumeQueueRequestHeader();
        consumeQueueHeader.setTopic("orders");
        consumeQueueHeader.setQueueId(2);
        consumeQueueHeader.setIndex(9_876_543_210L);
        consumeQueueHeader.setCount(16);
        consumeQueueHeader.setConsumerGroup("workers");
        consumeQueueHeader.setNamespace("tenant");
        consumeQueueHeader.setNamespaced(true);
        consumeQueueHeader.setBrokerName("broker-a");
        consumeQueueHeader.setOneway(false);
        consumeQueueHeader.setLo(true);
        writeTypedHeaderFixture(output, "query-consume-queue-request", 321, consumeQueueHeader);
        org.apache.rocketmq.remoting.protocol.header.GetAllTopicConfigRequestHeader allTopicConfig =
            new org.apache.rocketmq.remoting.protocol.header.GetAllTopicConfigRequestHeader();
        allTopicConfig.setTopicSeq(2);
        allTopicConfig.setDataVersion("{\"counter\":4,\"stateVersion\":7,\"timestamp\":1712345678901}");
        allTopicConfig.setMaxTopicNum(2000);
        writeTypedHeaderFixture(output, "get-all-topic-config-request", 21, allTopicConfig);
        org.apache.rocketmq.remoting.protocol.header.GetAllTopicConfigResponseHeader allTopicConfigResponse =
            new org.apache.rocketmq.remoting.protocol.header.GetAllTopicConfigResponseHeader();
        allTopicConfigResponse.setTotalTopicNum(5);
        writeTypedHeaderFixture(output, "get-all-topic-config-response",
            RemotingCommand.createResponseCommandWithHeader(0, allTopicConfigResponse));
        org.apache.rocketmq.remoting.protocol.header.GetSubscriptionGroupConfigRequestHeader
            subscriptionGroupConfig =
                new org.apache.rocketmq.remoting.protocol.header.GetSubscriptionGroupConfigRequestHeader();
        subscriptionGroupConfig.setGroup("workers");
        subscriptionGroupConfig.setNamespace("tenant");
        subscriptionGroupConfig.setNamespaced(true);
        subscriptionGroupConfig.setBrokerName("broker-a");
        subscriptionGroupConfig.setOneway(false);
        writeTypedHeaderFixture(output, "get-subscription-group-config-request", 352,
            subscriptionGroupConfig);
        GetLiteTopicInfoRequestHeader liteTopicInfo = new GetLiteTopicInfoRequestHeader();
        liteTopicInfo.setParentTopic("lite-parent");
        liteTopicInfo.setLiteTopic("%LMQ%lite-parent%orders");
        writeTypedHeaderFixture(output, "lite-topic-info-request", 200076, liteTopicInfo);
        GetLiteClientInfoRequestHeader liteClientInfo = new GetLiteClientInfoRequestHeader();
        liteClientInfo.setParentTopic("lite-parent");
        liteClientInfo.setGroup("workers");
        liteClientInfo.setClientId("client-a");
        writeTypedHeaderFixture(output, "lite-client-info-request", 200077, liteClientInfo);
        GetLiteGroupInfoRequestHeader liteGroupInfo = new GetLiteGroupInfoRequestHeader();
        liteGroupInfo.setGroup("workers");
        liteGroupInfo.setLiteTopic("%LMQ%lite-parent%orders");
        liteGroupInfo.setTopK(5);
        writeTypedHeaderFixture(output, "lite-group-info-request", 200078, liteGroupInfo);
        GetParentTopicInfoRequestHeader parentTopicInfo = new GetParentTopicInfoRequestHeader();
        parentTopicInfo.setTopic("lite-parent");
        writeTypedHeaderFixture(output, "parent-topic-info-request", 200075, parentTopicInfo);
        DeleteTopicRequestHeader deleteTopic = new DeleteTopicRequestHeader();
        deleteTopic.setTopic("orders");
        deleteTopic.setNamespace("tenant");
        deleteTopic.setNamespaced(true);
        deleteTopic.setBrokerName("broker-a");
        deleteTopic.setOneway(false);
        deleteTopic.setLo(true);
        writeTypedHeaderFixture(output, "delete-topic-request", 215, deleteTopic);
        DeleteTopicFromNamesrvRequestHeader deleteTopicFromNamesrv =
            new DeleteTopicFromNamesrvRequestHeader();
        deleteTopicFromNamesrv.setTopic("orders");
        deleteTopicFromNamesrv.setClusterName("cluster-a");
        deleteTopicFromNamesrv.setNamespace("tenant");
        deleteTopicFromNamesrv.setNamespaced(true);
        deleteTopicFromNamesrv.setBrokerName("broker-a");
        deleteTopicFromNamesrv.setOneway(false);
        deleteTopicFromNamesrv.setLo(true);
        writeTypedHeaderFixture(output, "delete-topic-from-namesrv-request", 216,
            deleteTopicFromNamesrv);
        DeleteKVConfigRequestHeader deleteKvConfig = new DeleteKVConfigRequestHeader();
        deleteKvConfig.setNamespace("config-namespace");
        deleteKvConfig.setKey("config-key");
        writeTypedHeaderFixture(output, "delete-kv-config-request", 102, deleteKvConfig);
        GetKVConfigRequestHeader getKvConfig = new GetKVConfigRequestHeader();
        getKvConfig.setNamespace("config-namespace");
        getKvConfig.setKey("config-key");
        writeTypedHeaderFixture(output, "get-kv-config-request", 101, getKvConfig);
        PutKVConfigRequestHeader putKvConfig = new PutKVConfigRequestHeader();
        putKvConfig.setNamespace("config-namespace");
        putKvConfig.setKey("config-key");
        putKvConfig.setValue("config-value");
        writeTypedHeaderFixture(output, "put-kv-config-request", 100, putKvConfig);
        GetKVListByNamespaceRequestHeader getKvList = new GetKVListByNamespaceRequestHeader();
        getKvList.setNamespace("config-namespace");
        writeTypedHeaderFixture(output, "get-kv-list-by-namespace-request", 219, getKvList);
        GetKVConfigResponseHeader getKvConfigResponse = new GetKVConfigResponseHeader();
        getKvConfigResponse.setValue("config-value");
        writeTypedHeaderFixture(output, "get-kv-config-response",
            RemotingCommand.createResponseCommandWithHeader(0, getKvConfigResponse));
        org.apache.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerRequestHeader wipeWritePerm =
            new org.apache.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerRequestHeader();
        wipeWritePerm.setBrokerName("broker-a");
        writeTypedHeaderFixture(output, "wipe-write-perm-request", 205, wipeWritePerm);
        org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerRequestHeader addWritePerm =
            new org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerRequestHeader();
        addWritePerm.setBrokerName("broker-a");
        writeTypedHeaderFixture(output, "add-write-perm-request", 327, addWritePerm);
        org.apache.rocketmq.remoting.protocol.header.AddBrokerRequestHeader addBroker =
            new org.apache.rocketmq.remoting.protocol.header.AddBrokerRequestHeader();
        addBroker.setConfigPath("/etc/rocketmq/broker.conf");
        writeTypedHeaderFixture(output, "add-broker-request", 902, addBroker);
        org.apache.rocketmq.remoting.protocol.header.RemoveBrokerRequestHeader removeBroker =
            new org.apache.rocketmq.remoting.protocol.header.RemoveBrokerRequestHeader();
        removeBroker.setBrokerClusterName("cluster-a");
        removeBroker.setBrokerName("broker-a");
        removeBroker.setBrokerId(Long.MIN_VALUE);
        writeTypedHeaderFixture(output, "remove-broker-request", 903, removeBroker);
        org.apache.rocketmq.remoting.protocol.header.CloneGroupOffsetRequestHeader cloneGroupOffset =
            new org.apache.rocketmq.remoting.protocol.header.CloneGroupOffsetRequestHeader();
        cloneGroupOffset.setSrcGroup("source");
        cloneGroupOffset.setDestGroup("destination");
        cloneGroupOffset.setTopic("orders");
        cloneGroupOffset.setOffline(true);
        cloneGroupOffset.setNamespace("tenant");
        cloneGroupOffset.setNamespaced(true);
        cloneGroupOffset.setBrokerName("broker-a");
        cloneGroupOffset.setOneway(false);
        writeTypedHeaderFixture(output, "clone-group-offset-request", 314, cloneGroupOffset);
        org.apache.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader wipeWritePermResponse =
            new org.apache.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader();
        wipeWritePermResponse.setWipeTopicCount(17);
        writeTypedHeaderFixture(output, "wipe-write-perm-response",
            RemotingCommand.createResponseCommandWithHeader(0, wipeWritePermResponse));
        org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerResponseHeader addWritePermResponse =
            new org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerResponseHeader();
        addWritePermResponse.setAddTopicCount(-3);
        writeTypedHeaderFixture(output, "add-write-perm-response",
            RemotingCommand.createResponseCommandWithHeader(0, addWritePermResponse));
        DeleteSubscriptionGroupRequestHeader deleteSubscriptionGroup =
            new DeleteSubscriptionGroupRequestHeader();
        deleteSubscriptionGroup.setGroupName("workers");
        deleteSubscriptionGroup.setCleanOffset(true);
        deleteSubscriptionGroup.setNamespace("tenant");
        deleteSubscriptionGroup.setNamespaced(true);
        deleteSubscriptionGroup.setBrokerName("broker-a");
        deleteSubscriptionGroup.setOneway(false);
        writeTypedHeaderFixture(output, "delete-subscription-group-request", 207,
            deleteSubscriptionGroup);
        QueryConsumerOffsetResponseHeader queryResponse = new QueryConsumerOffsetResponseHeader();
        queryResponse.setOffset(9L);
        writeTypedHeaderFixture(output, "query-offset-response",
            RemotingCommand.createResponseCommandWithHeader(0, queryResponse));

        UpdateConsumerOffsetRequestHeader update = new UpdateConsumerOffsetRequestHeader();
        update.setConsumerGroup("workers");
        update.setTopic("orders");
        update.setQueueId(1);
        update.setCommitOffset(9L);
        writeTypedHeaderFixture(output, "update-offset-request", 15, update);

        SearchOffsetRequestHeader searchOffset = new SearchOffsetRequestHeader();
        searchOffset.setTopic("orders"); searchOffset.setLiteTopic("lite-orders");
        searchOffset.setQueueId(1); searchOffset.setTimestamp(1_712_345_678_901L);
        searchOffset.setBoundaryType(BoundaryType.UPPER); searchOffset.setBrokerName("broker-a");
        writeTypedHeaderFixture(output, "search-offset-request", 29, searchOffset);
        GetMaxOffsetRequestHeader maxOffset = new GetMaxOffsetRequestHeader();
        maxOffset.setTopic("orders"); maxOffset.setQueueId(1); maxOffset.setCommitted(false);
        maxOffset.setBrokerName("broker-a");
        writeTypedHeaderFixture(output, "max-offset-request", 30, maxOffset);
        GetMinOffsetRequestHeader minOffset = new GetMinOffsetRequestHeader();
        minOffset.setTopic("orders"); minOffset.setQueueId(1); minOffset.setBrokerName("broker-a");
        writeTypedHeaderFixture(output, "min-offset-request", 31, minOffset);
        GetEarliestMsgStoretimeRequestHeader earliestStoretime = new GetEarliestMsgStoretimeRequestHeader();
        earliestStoretime.setTopic("orders"); earliestStoretime.setQueueId(1);
        earliestStoretime.setBrokerName("broker-a");
        writeTypedHeaderFixture(output, "earliest-msg-storetime-request", 32, earliestStoretime);
        SearchOffsetResponseHeader searchResponse = new SearchOffsetResponseHeader();
        searchResponse.setOffset(9L);
        writeTypedHeaderFixture(output, "search-offset-response",
            RemotingCommand.createResponseCommandWithHeader(0, searchResponse));
        GetMaxOffsetResponseHeader maxResponse = new GetMaxOffsetResponseHeader();
        maxResponse.setOffset(100L);
        writeTypedHeaderFixture(output, "max-offset-response",
            RemotingCommand.createResponseCommandWithHeader(0, maxResponse));
        GetMinOffsetResponseHeader minResponse = new GetMinOffsetResponseHeader();
        minResponse.setOffset(0L);
        writeTypedHeaderFixture(output, "min-offset-response",
            RemotingCommand.createResponseCommandWithHeader(0, minResponse));
        GetEarliestMsgStoretimeResponseHeader earliestResponse = new GetEarliestMsgStoretimeResponseHeader();
        earliestResponse.setTimestamp(-1L);
        writeTypedHeaderFixture(output, "earliest-msg-storetime-response",
            RemotingCommand.createResponseCommandWithHeader(0, earliestResponse));

        SendMessageResponseHeader sendResponse = new SendMessageResponseHeader();
        sendResponse.setMsgId("msg-1");
        sendResponse.setQueueId(2);
        sendResponse.setQueueOffset(99L);
        writeTypedHeaderFixture(output, "send-response",
            RemotingCommand.createResponseCommandWithHeader(0, sendResponse));

        RecallMessageRequestHeader recall = new RecallMessageRequestHeader();
        recall.setProducerGroup("producers");
        recall.setTopic("orders");
        recall.setRecallHandle("message-id%orders%1712345678901");
        recall.setNamespace("tenant");
        recall.setNamespaced(true);
        recall.setBrokerName("broker-a");
        recall.setOneway(false);
        recall.setLo(true);
        writeTypedHeaderFixture(output, "recall-message-request", 370, recall);
        RecallMessageResponseHeader recallResponse = new RecallMessageResponseHeader();
        recallResponse.setMsgId("broker-message-id");
        writeTypedHeaderFixture(output, "recall-message-response",
            RemotingCommand.createResponseCommandWithHeader(0, recallResponse));

        GetConsumerListByGroupRequestHeader consumerList = new GetConsumerListByGroupRequestHeader();
        consumerList.setConsumerGroup("workers");
        writeTypedHeaderFixture(output, "consumer-list-request", 38, consumerList);

        UnregisterClientRequestHeader unregister = new UnregisterClientRequestHeader();
        unregister.setClientID("client-1");
        unregister.setProducerGroup("producers");
        writeTypedHeaderFixture(output, "unregister-request", 35, unregister);

        ConsumerSendMsgBackRequestHeader sendBack = new ConsumerSendMsgBackRequestHeader();
        sendBack.setOffset(9L);
        sendBack.setGroup("workers");
        sendBack.setDelayLevel(3);
        sendBack.setOriginMsgId("msg-1");
        sendBack.setOriginTopic("orders");
        sendBack.setUnitMode(false);
        sendBack.setMaxReconsumeTimes(16);
        writeTypedHeaderFixture(output, "send-back-request", 36, sendBack);

        PopMessageRequestHeader pop = new PopMessageRequestHeader();
        pop.setConsumerGroup("workers");
        pop.setTopic("orders");
        pop.setQueueId(1);
        pop.setMaxMsgNums(16);
        pop.setInvisibleTime(30_000L);
        pop.setPollTime(1_000L);
        pop.setBornTime(1_712_345_678_901L);
        pop.setInitMode(0);
        writeTypedHeaderFixture(output, "pop-request", 200050, pop);

        AckMessageRequestHeader ack = new AckMessageRequestHeader();
        ack.setConsumerGroup("workers");
        ack.setTopic("orders");
        ack.setQueueId(1);
        ack.setExtraInfo("startOffset popTime invisibleTime queueId");
        ack.setOffset(9L);
        writeTypedHeaderFixture(output, "ack-request", 200051, ack);

        ChangeInvisibleTimeRequestHeader change = new ChangeInvisibleTimeRequestHeader();
        change.setConsumerGroup("workers");
        change.setTopic("orders");
        change.setQueueId(1);
        change.setExtraInfo("startOffset popTime invisibleTime queueId");
        change.setOffset(9L);
        change.setInvisibleTime(60_000L);
        writeTypedHeaderFixture(output, "change-invisible-request", 200053, change);

        ChangeInvisibleTimeResponseHeader changeResponse = new ChangeInvisibleTimeResponseHeader();
        changeResponse.setPopTime(123L);
        changeResponse.setInvisibleTime(456L);
        changeResponse.setReviveQid(2);
        writeTypedHeaderFixture(output, "change-invisible-response",
            RemotingCommand.createResponseCommandWithHeader(0, changeResponse));

        NotificationRequestHeader notification = new NotificationRequestHeader();
        notification.setConsumerGroup("workers");
        notification.setTopic("orders");
        notification.setQueueId(1);
        notification.setPollTime(1_000L);
        notification.setBornTime(1_712_345_678_901L);
        writeTypedHeaderFixture(output, "notification-request", 200054, notification);

        PollingInfoRequestHeader polling = new PollingInfoRequestHeader();
        polling.setConsumerGroup("workers");
        polling.setTopic("orders");
        polling.setQueueId(1);
        writeTypedHeaderFixture(output, "polling-request", 200055, polling);
        PollingInfoResponseHeader pollingResponse = new PollingInfoResponseHeader();
        pollingResponse.setPollingNum(2);
        writeTypedHeaderFixture(output, "polling-response", RemotingCommand.createResponseCommandWithHeader(0, pollingResponse));

        EndTransactionRequestHeader endTransaction = new EndTransactionRequestHeader();
        endTransaction.setTopic("orders"); endTransaction.setProducerGroup("producers");
        endTransaction.setTranStateTableOffset(3L); endTransaction.setCommitLogOffset(9L);
        endTransaction.setCommitOrRollback(8); endTransaction.setFromTransactionCheck(false);
        endTransaction.setMsgId("msg-1"); endTransaction.setTransactionId("tx-1");
        writeTypedHeaderFixture(output, "end-transaction-request", 37, endTransaction);
        writeTypedHeaderFixture(output, "end-transaction-response",
            RemotingCommand.createResponseCommandWithHeader(0, new EndTransactionResponseHeader()));

        CheckTransactionStateRequestHeader checkTransaction = new CheckTransactionStateRequestHeader();
        checkTransaction.setTopic("orders"); checkTransaction.setTranStateTableOffset(3L);
        checkTransaction.setCommitLogOffset(9L); checkTransaction.setMsgId("msg-1");
        checkTransaction.setTransactionId("tx-1"); checkTransaction.setOffsetMsgId("offset-1");
        writeTypedHeaderFixture(output, "check-transaction-request", 39, checkTransaction);

        CheckTransactionStateResponseHeader checkResponse = new CheckTransactionStateResponseHeader();
        checkResponse.setProducerGroup("producers"); checkResponse.setTranStateTableOffset(3L);
        checkResponse.setCommitLogOffset(9L); checkResponse.setCommitOrRollback(8);
        writeTypedHeaderFixture(output, "check-transaction-response", RemotingCommand.createResponseCommandWithHeader(0, checkResponse));

        ReplyMessageRequestHeader reply = new ReplyMessageRequestHeader();
        reply.setProducerGroup("producers"); reply.setTopic("orders"); reply.setDefaultTopic("TBW102");
        reply.setDefaultTopicQueueNums(4); reply.setQueueId(2); reply.setSysFlag(1);
        reply.setBornTimestamp(5L); reply.setFlag(7); reply.setProperties("CORRELATION_ID=correlation-1");
        reply.setUnitMode(false); reply.setBornHost("127.0.0.1:1001");
        reply.setStoreHost("127.0.0.1:10911"); reply.setStoreTimestamp(6L);
        writeTypedHeaderFixture(output, "reply-request", 324, reply);
        SendMessageRequestHeader replySend = new SendMessageRequestHeader();
        replySend.setProducerGroup("producers"); replySend.setTopic("orders");
        replySend.setDefaultTopic("TBW102"); replySend.setDefaultTopicQueueNums(4);
        replySend.setQueueId(2); replySend.setSysFlag(1); replySend.setBornTimestamp(5L);
        replySend.setFlag(7); replySend.setProperties("CORRELATION_ID=correlation-1");
        replySend.setUnitMode(false);
        byte[] binaryReply = new byte[] {0, (byte) 255, (byte) 128, 42, 10};
        RemotingCommand replyV2 = RemotingCommand.createRequestCommand(325,
            SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(replySend));
        replyV2.setBody(binaryReply);
        writeTypedHeaderFixture(output, "reply-v2-request", replyV2);
        RemotingCommand replyPush = RemotingCommand.createRequestCommand(326, reply);
        replyPush.setBody(binaryReply);
        writeTypedHeaderFixture(output, "reply-push-request", replyPush);

        ResetOffsetRequestHeader reset = new ResetOffsetRequestHeader();
        reset.setGroup("workers"); reset.setTopic("orders"); reset.setQueueId(1);
        reset.setOffset(9L); reset.setTimestamp(1_712_345_678_901L); reset.setForce(true);
        writeTypedHeaderFixture(output, "reset-offset-request", 211, reset);

        org.apache.rocketmq.remoting.protocol.header.ResetMasterFlushOffsetHeader resetMasterFlushOffset =
            new org.apache.rocketmq.remoting.protocol.header.ResetMasterFlushOffsetHeader();
        resetMasterFlushOffset.setMasterFlushOffset(9_876_543_210L);
        writeTypedHeaderFixture(output, "reset-master-flush-offset", 908, resetMasterFlushOffset);

        org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader exportRocksDB =
            new org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader();
        exportRocksDB.updateConfigType(java.util.Arrays.asList(
            org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader.ConfigType.TOPICS,
            org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader.ConfigType.SUBSCRIPTION_GROUPS,
            org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader.ConfigType.CONSUMER_OFFSETS));
        writeTypedHeaderFixture(output, "export-rocksdb-config-request", 355, exportRocksDB);

        org.apache.rocketmq.remoting.protocol.header.CheckRocksdbCqWriteProgressRequestHeader checkRocksdbCq =
            new org.apache.rocketmq.remoting.protocol.header.CheckRocksdbCqWriteProgressRequestHeader();
        checkRocksdbCq.setTopic("orders");
        checkRocksdbCq.setCheckStoreTime(1_712_345_678_901L);
        writeTypedHeaderFixture(output, "check-rocksdb-cq-write-progress-request", 354, checkRocksdbCq);

        NotificationResponseHeader notificationResponse = new NotificationResponseHeader();
        notificationResponse.setHasMsg(true); notificationResponse.setPollingFull(false);
        writeTypedHeaderFixture(output, "notification-response", RemotingCommand.createResponseCommandWithHeader(0, notificationResponse));

        PopLiteMessageRequestHeader popLite = new PopLiteMessageRequestHeader();
        popLite.setClientId("client"); popLite.setConsumerGroup("workers"); popLite.setTopic("orders");
        popLite.setMaxMsgNum(1); popLite.setInvisibleTime(1000L); popLite.setPollTime(1L); popLite.setBornTime(1L);
        writeTypedHeaderFixture(output, "pop-lite-request", 200070, popLite);
        PopLiteMessageResponseHeader popLiteResponse = new PopLiteMessageResponseHeader();
        popLiteResponse.setPopTime(10L); popLiteResponse.setInvisibleTime(20L); popLiteResponse.setReviveQid(1);
        popLiteResponse.setStartOffsetInfo("orders@0:100"); popLiteResponse.setMsgOffsetInfo("orders@0:101");
        popLiteResponse.setOrderCountInfo("0");
        writeTypedHeaderFixture(output, "pop-lite-response",
            RemotingCommand.createResponseCommandWithHeader(0, popLiteResponse));
        PopMessageResponseHeader popResponse = new PopMessageResponseHeader();
        popResponse.setPopTime(10L); popResponse.setInvisibleTime(20L);
        popResponse.setReviveQid(1); popResponse.setRestNum(3L);
        popResponse.setStartOffsetInfo("orders@0:100"); popResponse.setMsgOffsetInfo("orders@0:101");
        popResponse.setOrderCountInfo("0");
        writeTypedHeaderFixture(output, "pop-response",
            RemotingCommand.createResponseCommandWithHeader(0, popResponse));
        GetConsumerStatusRequestHeader status = new GetConsumerStatusRequestHeader();
        status.setTopic("orders"); status.setGroup("workers");
        writeTypedHeaderFixture(output, "consumer-status-request", 221, status);
        GetConsumerConnectionListRequestHeader connectionList = new GetConsumerConnectionListRequestHeader();
        connectionList.setConsumerGroup("workers"); connectionList.setNamespace("tenant");
        connectionList.setNamespaced(true); connectionList.setBrokerName("broker-a");
        connectionList.setOneway(false);
        writeTypedHeaderFixture(output, "consumer-connection-list-request", 203, connectionList);
        GetConsumeStatsRequestHeader consumeStats = new GetConsumeStatsRequestHeader();
        consumeStats.setConsumerGroup("workers"); consumeStats.setTopic("orders");
        consumeStats.setTopicList("orders;payments;"); consumeStats.setNamespace("tenant");
        consumeStats.setNamespaced(true); consumeStats.setBrokerName("broker-a");
        consumeStats.setOneway(false);
        writeTypedHeaderFixture(output, "consume-stats-request", 208, consumeStats);
        org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsInBrokerHeader consumeStatsInBroker =
            new org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsInBrokerHeader();
        consumeStatsInBroker.setIsOrder(true);
        writeTypedHeaderFixture(output, "consume-stats-in-broker-request", 317, consumeStatsInBroker);
        org.apache.rocketmq.remoting.protocol.header.ViewBrokerStatsDataRequestHeader brokerStats =
            new org.apache.rocketmq.remoting.protocol.header.ViewBrokerStatsDataRequestHeader();
        brokerStats.setStatsName("SNDBYTES"); brokerStats.setStatsKey("orders@0");
        writeTypedHeaderFixture(output, "view-broker-stats-data-request", 315, brokerStats);
        GetConsumerRunningInfoRequestHeader runningInfo = new GetConsumerRunningInfoRequestHeader();
        runningInfo.setConsumerGroup("workers"); runningInfo.setClientId("client-running-info");
        runningInfo.setJstackEnable(true); runningInfo.setNamespace("tenant");
        runningInfo.setNamespaced(true); runningInfo.setBrokerName("broker-a");
        runningInfo.setOneway(false);
        writeTypedHeaderFixture(output, "consumer-running-info-request", 307, runningInfo);
        HeartbeatRequestHeader heartbeat = new HeartbeatRequestHeader();
        heartbeat.setNamespace("tenant"); heartbeat.setNamespaced(true);
        heartbeat.setBrokerName("broker-a"); heartbeat.setOneway(false);
        writeTypedHeaderFixture(output, "heartbeat-request", 34, heartbeat);
        ConsumeMessageDirectlyResultRequestHeader direct = new ConsumeMessageDirectlyResultRequestHeader();
        direct.setConsumerGroup("workers");
        direct.setLo(true);
        writeTypedHeaderFixture(output, "consume-directly-request", 309, direct);
        writeTypedHeaderFixture(output, "lite-subscription-request", 200071, new LiteSubscriptionCtlRequestHeader());
        NotifyUnsubscribeLiteRequestHeader unsubscribe = new NotifyUnsubscribeLiteRequestHeader();
        unsubscribe.setLiteTopic("lite-orders"); unsubscribe.setConsumerGroup("workers"); unsubscribe.setClientId("client");
        writeTypedHeaderFixture(output, "notify-unsubscribe-lite-request", 200073, unsubscribe);
        LockBatchMqRequestHeader lockHeader = new LockBatchMqRequestHeader();
        lockHeader.setNamespace("tenant"); lockHeader.setBrokerName("broker-a");
        writeTypedHeaderFixture(output, "lock-batch-request", 41, lockHeader);
        UnlockBatchMqRequestHeader unlockHeader = new UnlockBatchMqRequestHeader();
        unlockHeader.setNamespace("tenant"); unlockHeader.setBrokerName("broker-a");
        writeTypedHeaderFixture(output, "unlock-batch-request", 42, unlockHeader);
        writeTypedHeaderFixture(output, "unregister-response",
            RemotingCommand.createResponseCommandWithHeader(0, new UnregisterClientResponseHeader()));

        QueryAssignmentRequestBody assignmentBody = new QueryAssignmentRequestBody();
        assignmentBody.setTopic("orders"); assignmentBody.setConsumerGroup("workers");
        assignmentBody.setClientId("client"); assignmentBody.setStrategyName("AllocateMessageQueueAveragely");
        assignmentBody.setMessageModel(org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.CLUSTERING);
        writeJsonBodyFixture(output, "body-query-assignment", 400, assignmentBody.encode());

        ResetOffsetBody resetBody = new ResetOffsetBody();
        resetBody.setOffsetTable(java.util.Map.of(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker-a", 1), 9L));
        writeJsonBodyFixture(output, "body-reset-offset", 212, resetBody.encode());
        GetConsumerStatusBody statusBody = new GetConsumerStatusBody();
        statusBody.setMessageQueueTable(java.util.Map.of(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker-a", 1), 9L));
        statusBody.setConsumerTable(java.util.Map.of());
        writeJsonBodyFixture(output, "body-consumer-status", 223, statusBody.encode());

        org.apache.rocketmq.remoting.protocol.admin.ConsumeStats consumeStatsBody =
            new org.apache.rocketmq.remoting.protocol.admin.ConsumeStats();
        org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper ordersOffset =
            new org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper();
        ordersOffset.setBrokerOffset(29L); ordersOffset.setConsumerOffset(20L);
        ordersOffset.setPullOffset(25L); ordersOffset.setLastTimestamp(1000L);
        org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper paymentsOffset =
            new org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper();
        paymentsOffset.setBrokerOffset(42L); paymentsOffset.setConsumerOffset(38L);
        paymentsOffset.setPullOffset(41L); paymentsOffset.setLastTimestamp(2000L);
        Map<org.apache.rocketmq.common.message.MessageQueue,
            org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper> consumeStatsOffsets = new LinkedHashMap<>();
        consumeStatsOffsets.put(
            new org.apache.rocketmq.common.message.MessageQueue("orders", "broker-a", 1), ordersOffset);
        consumeStatsOffsets.put(
            new org.apache.rocketmq.common.message.MessageQueue("payments", "broker-b", 2), paymentsOffset);
        consumeStatsBody.setOffsetTable(consumeStatsOffsets);
        consumeStatsBody.setConsumeTps(1.25);
        writeJsonBodyFixture(output, "body-consume-stats", 208, consumeStatsBody.encode());

        SetMessageRequestModeRequestBody modeBody = new SetMessageRequestModeRequestBody();
        modeBody.setTopic("orders"); modeBody.setConsumerGroup("workers");
        modeBody.setMode(MessageRequestMode.POP); modeBody.setPopShareQueueNum(2);
        writeJsonBodyFixture(output, "body-set-request-mode", 401, modeBody.encode());

        LiteSubscriptionDTO dto = new LiteSubscriptionDTO().setAction(LiteSubscriptionAction.COMPLETE_ADD)
            .setClientId("client").setGroup("workers").setTopic("orders")
            .setLiteTopicSet(java.util.Set.of("lite-orders"));
        dto.setOffsetOption(new OffsetOption(OffsetOption.Type.POLICY, OffsetOption.POLICY_MIN_VALUE));
        dto.setVersion(2L);
        LiteSubscriptionCtlRequestBody liteBody = new LiteSubscriptionCtlRequestBody();
        liteBody.setSubscriptionSet(java.util.Set.of(dto));
        writeJsonBodyFixture(output, "body-lite-subscription", 200071, liteBody.encode());

        BatchAck batchAck = new BatchAck();
        batchAck.setConsumerGroup("workers"); batchAck.setTopic("orders"); batchAck.setRetry("0");
        batchAck.setStartOffset(100L); batchAck.setQueueId(1); batchAck.setReviveQueueId(0);
        batchAck.setPopTime(10L); batchAck.setInvisibleTime(20L);
        java.util.BitSet bitSet = new java.util.BitSet(); bitSet.set(0); bitSet.set(2); batchAck.setBitSet(bitSet);
        BatchAckMessageRequestBody batchBody = new BatchAckMessageRequestBody();
        batchBody.setBrokerName("broker-a"); batchBody.setAcks(java.util.List.of(batchAck));
        writeJsonBodyFixture(output, "body-batch-ack", 200151, batchBody.encode());

        ConsumeMessageDirectlyResult directBody = new ConsumeMessageDirectlyResult();
        directBody.setConsumeResult(CMResult.CR_SUCCESS); directBody.setSpentTimeMills(2L);
        writeJsonBodyFixture(output, "body-consume-directly", 309, directBody.encode());

        System.out.println("Generated typed Java header fixtures in " + output);
    }

    private static void generateBodyFixtures(Path output) throws Exception {
        Files.createDirectories(output);
        GetConsumerListByGroupResponseBody absent = new GetConsumerListByGroupResponseBody();
        writeJsonBodyFixture(output, "body-consumer-list-absent", 38, absent.encode());
        GetConsumerListByGroupResponseBody empty = new GetConsumerListByGroupResponseBody();
        empty.setConsumerIdList(java.util.List.of());
        writeJsonBodyFixture(output, "body-consumer-list-empty", 38, empty.encode());
        GetConsumerListByGroupResponseBody populated = new GetConsumerListByGroupResponseBody();
        populated.setConsumerIdList(java.util.List.of("client-a", "client-b"));
        writeJsonBodyFixture(output, "body-consumer-list-populated", 38, populated.encode());

        org.apache.rocketmq.remoting.protocol.DataVersion configVersion =
            new org.apache.rocketmq.remoting.protocol.DataVersion();
        configVersion.setStateVersion(7);
        configVersion.setTimestamp(1_712_345_678_901L);
        configVersion.setCounter(new java.util.concurrent.atomic.AtomicLong(4));
        org.apache.rocketmq.common.TopicConfig topicConfig =
            new org.apache.rocketmq.common.TopicConfig("orders");
        org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper configWrapper =
            new org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper();
        configWrapper.setDataVersion(configVersion);
        configWrapper.setTopicConfigTable(new java.util.concurrent.ConcurrentHashMap<>(
            java.util.Map.of("orders", topicConfig)));
        writeJsonBodyFixture(output, "body-topic-config-wrapper", 21, configWrapper.encode());

        org.apache.rocketmq.remoting.protocol.DataVersion groupVersion =
            new org.apache.rocketmq.remoting.protocol.DataVersion();
        groupVersion.setStateVersion(6);
        groupVersion.setTimestamp(1_712_345_678_902L);
        groupVersion.setCounter(new java.util.concurrent.atomic.AtomicLong(9));
        org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig workers =
            new org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig();
        workers.setGroupName("workers");
        workers.setRetryQueueNums(5);
        workers.setConsumeBroadcastEnable(false);
        org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig analytics =
            new org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig();
        analytics.setGroupName("analytics");
        analytics.setConsumeMessageOrderly(true);
        java.util.concurrent.ConcurrentMap<String,
            org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig> groupTable =
                new java.util.concurrent.ConcurrentHashMap<>(java.util.Map.of(
                    "workers", workers, "analytics", analytics));
        java.util.concurrent.ConcurrentMap<String, java.util.concurrent.ConcurrentMap<String, Integer>> forbiddenTable =
            new java.util.concurrent.ConcurrentHashMap<>();
        forbiddenTable.put("workers", new java.util.concurrent.ConcurrentHashMap<>(
            java.util.Map.of("orders", 1, "payments", 0)));
        forbiddenTable.put("analytics", new java.util.concurrent.ConcurrentHashMap<>(
            java.util.Map.of("alerts", 1)));
        org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper groupWrapper =
            new org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper();
        groupWrapper.setDataVersion(groupVersion);
        groupWrapper.setSubscriptionGroupTable(groupTable);
        groupWrapper.setForbiddenTable(forbiddenTable);
        writeJsonBodyFixture(output, "body-subscription-group-wrapper", 201, groupWrapper.encode());

        org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData subscriptionData =
            new org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData("orders", "*");
        subscriptionData.setSubVersion(1234L);
        org.apache.rocketmq.remoting.protocol.body.ConsumeQueueData consumeQueueRow =
            new org.apache.rocketmq.remoting.protocol.body.ConsumeQueueData();
        consumeQueueRow.setPhysicOffset(987654321L);
        consumeQueueRow.setPhysicSize(128);
        consumeQueueRow.setTagsCode(171234L);
        consumeQueueRow.setExtendDataJson("{\"filterBitMap\":\"AQID\"}");
        consumeQueueRow.setBitMap("1010");
        consumeQueueRow.setEval(true);
        consumeQueueRow.setMsg("message-body");
        org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody consumeQueueBody =
            new org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody();
        consumeQueueBody.setSubscriptionData(subscriptionData);
        consumeQueueBody.setFilterData("filter-data");
        consumeQueueBody.setQueueData(java.util.List.of(consumeQueueRow));
        consumeQueueBody.setMaxQueueIndex(20L);
        consumeQueueBody.setMinQueueIndex(4L);
        writeJsonBodyFixture(output, "body-query-consume-queue", 321, consumeQueueBody.encode());

        GetLiteGroupInfoResponseBody liteGroupInfo = new GetLiteGroupInfoResponseBody();
        liteGroupInfo.setGroup("workers");
        liteGroupInfo.setParentTopic("lite-parent");
        liteGroupInfo.setLiteTopic("%LMQ%lite-parent%orders");
        liteGroupInfo.setEarliestUnconsumedTimestamp(1400);
        liteGroupInfo.setTotalLagCount(40);
        org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper liteOffset =
            new org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper();
        liteOffset.setBrokerOffset(100); liteOffset.setConsumerOffset(80);
        liteOffset.setPullOffset(85); liteOffset.setLastTimestamp(1700);
        liteGroupInfo.setLiteTopicOffsetWrapper(liteOffset);
        LiteLagInfo ordersLag = new LiteLagInfo();
        ordersLag.setLiteTopic("%LMQ%lite-parent%orders");
        ordersLag.setLagCount(30); ordersLag.setEarliestUnconsumedTimestamp(1600);
        ordersLag.setConsumerOffset(70);
        LiteLagInfo paymentsLag = new LiteLagInfo();
        paymentsLag.setLiteTopic("%LMQ%lite-parent%payments");
        paymentsLag.setLagCount(10); paymentsLag.setEarliestUnconsumedTimestamp(1500);
        paymentsLag.setConsumerOffset(90);
        liteGroupInfo.setLagCountTopK(new ArrayList<>(Arrays.asList(ordersLag, null, paymentsLag)));
        liteGroupInfo.setLagTimestampTopK(new ArrayList<>(Arrays.asList(paymentsLag, ordersLag)));
        writeJsonBodyFixture(output, "body-lite-group-info", 200078, liteGroupInfo.encode());

        org.apache.rocketmq.common.message.MessageQueue mq =
            new org.apache.rocketmq.common.message.MessageQueue("orders", "broker-a", 1);
        LockBatchRequestBody lock = new LockBatchRequestBody();
        lock.setConsumerGroup("workers"); lock.setClientId("client"); lock.setMqSet(java.util.Set.of(mq));
        writeJsonBodyFixture(output, "body-lock-request", 41, lock.encode());
        UnlockBatchRequestBody unlock = new UnlockBatchRequestBody();
        unlock.setConsumerGroup("workers"); unlock.setClientId("client"); unlock.setOnlyThisBroker(true);
        unlock.setMqSet(java.util.Set.of(mq));
        writeJsonBodyFixture(output, "body-unlock-request", 42, unlock.encode());
        LockBatchResponseBody response = new LockBatchResponseBody();
        response.setLockOKMQSet(java.util.Set.of(mq));
        writeJsonBodyFixture(output, "body-lock-response", 41, response.encode());
        writeJsonBodyFixture(output, "body-unlock-response", 42, response.encode());
    }

    private static void writeTypedHeaderFixture(Path output, String id, int code,
            CommandCustomHeader header) throws Exception {
        RemotingCommand command = RemotingCommand.createRequestCommand(code, header);
        command.setLanguage(LanguageCode.JAVA);
        command.setVersion(0);
        command.setOpaque(101);
        writeTypedHeaderFixture(output, id, command);
    }

    private static void writeTypedHeaderFixture(Path output, String id, RemotingCommand command)
            throws Exception {
        command.setLanguage(LanguageCode.JAVA);
        command.setVersion(0);
        command.setOpaque(101);
        command.setSerializeTypeCurrentRPC(SerializeType.JSON);
        byte[] frame = bytes(command.encode());
        Map<String, Object> decoded = decode(frame);
        String stem = "header-" + id;
        Files.write(output.resolve(stem + ".bin"), frame);
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("java_client_version", "5.5.1");
        metadata.put("scenario_id", "P4-HEADER-" + id.toUpperCase());
        metadata.put("request_code", command.getCode());
        metadata.put("normalized_fields", decoded);
        Files.writeString(output.resolve(stem + ".json"), toJson(metadata) + "\n", StandardCharsets.UTF_8);
    }

    private static void writeJsonBodyFixture(Path output, String id, int code, byte[] body) throws Exception {
        RemotingCommand command = command(code, SerializeType.JSON, id);
        command.setBody(body);
        writeFixture(output, id, code, SerializeType.JSON, command, bytes(command.encode()));
    }

    private static void verifyRustFixtures(Path directory) throws Exception {
        if (!Files.isDirectory(directory)) throw new IOException("missing Rust fixture directory: " + directory);
        int count = 0;
        Map<String, Map<String, Object>> decodedByName = new LinkedHashMap<>();
        try (var files = Files.list(directory)) {
            for (Path file : (Iterable<Path>) files.filter(path -> path.toString().endsWith(".bin"))::iterator) {
                decodedByName.put(file.getFileName().toString(), decode(Files.readAllBytes(file)));
                count++;
            }
        }
        if (count == 2) {
            Map<String, Object> json = decodedByName.get("json-route.bin");
            Map<String, Object> rocketmq = decodedByName.get("rocketmq-transaction.bin");
            require(json != null && ((Number) json.get("code")).intValue() == 105, "Rust JSON route fixture decoded");
            require(rocketmq != null && ((Number) rocketmq.get("code")).intValue() == 37, "Rust RocketMQ transaction fixture decoded");
            require(json.get("body_length").equals(4) && rocketmq.get("body_length").equals(4), "Rust fixture bodies decoded");
            require("RUST".equals(json.get("language")) && "RUST".equals(rocketmq.get("language")), "Rust language decoded");
            require("rust-phase-2".equals(((Map<?, ?>) json.get("ext_fields")).get("oracle")), "Rust JSON ext field decoded");
            require("rust-phase-2".equals(((Map<?, ?>) rocketmq.get("ext_fields")).get("oracle")), "Rust private ext field decoded");
        } else {
            require(count == 106, "all Rust typed header fixtures were supplied");
            Map<String, Map<String, Object>> byName = new LinkedHashMap<>();
            try (var files = Files.list(directory)) {
                for (Path file : (Iterable<Path>) files.filter(path -> path.toString().endsWith(".bin"))::iterator) {
                    String name = file.getFileName().toString();
                    byName.put(name, decode(Files.readAllBytes(file)));
                }
            }
            assertAllExtFields(byName, "typed-route.bin", 105);
            assertAllExtFields(byName, "typed-create-topic-request.bin", 17);
            assertAllExtFields(byName, "typed-create-user-request.bin", 3001);
            assertAllExtFields(byName, "typed-create-acl-request.bin", 3006);
            assertAllExtFields(byName, "typed-delete-acl-request.bin", 3008);
            assertAllExtFields(byName, "typed-update-acl-request.bin", 3007);
            assertAllExtFields(byName, "typed-update-user-request.bin", 3002);
            assertAllExtFields(byName, "typed-delete-user-request.bin", 3003);
            assertAllExtFields(byName, "typed-get-user-request.bin", 3004);
            assertAllExtFields(byName, "typed-get-acl-request.bin", 3009);
            assertAllExtFields(byName, "typed-list-acls-request.bin", 3010);
            assertAllExtFields(byName, "typed-list-users-request.bin", 3005);
            assertAllExtFields(byName, "typed-query-message-request.bin", 12);
            assertAllExtFields(byName, "typed-query-message-response.bin", 0);
            assertAllExtFields(byName, "typed-view-message-request.bin", 33);
            assertAllExtFields(byName, "typed-send-v1.bin", 10);
            assertAllExtFields(byName, "typed-send-v2.bin", 310);
            assertAllExtFields(byName, "typed-send-batch-v2.bin", 320);
            assertAllExtFields(byName, "typed-pull.bin", 11);
            assertAllExtFields(byName, "typed-query-offset.bin", 14);
            assertAllExtFields(byName, "typed-query-consume-time-span-request.bin", 303);
            assertAllExtFields(byName, "typed-query-topics-by-consumer-request.bin", 343);
            assertAllExtFields(byName, "typed-query-subscription-by-consumer-request.bin", 345);
            assertAllExtFields(byName, "typed-query-topic-consume-by-who-request.bin", 300);
            assertAllExtFields(byName, "typed-query-correction-offset-request.bin", 308);
            assertAllExtFields(byName, "typed-update-group-forbidden-request.bin", 353);
            assertAllExtFields(byName, "typed-topic-stats-info-request.bin", 202);
            assertAllExtFields(byName, "typed-get-all-subscription-group-request.bin", 201);
            assertAllExtFields(byName, "typed-get-all-subscription-group-response.bin", 0);
            assertAllExtFields(byName, "typed-query-consume-queue-request.bin", 321);
            assertAllExtFields(byName, "typed-get-all-topic-config-request.bin", 21);
            assertAllExtFields(byName, "typed-get-all-topic-config-response.bin", 0);
            assertAllExtFields(byName, "typed-topics-by-cluster-request.bin", 224);
            assertAllExtFields(byName, "typed-lite-topic-info-request.bin", 200076);
            assertAllExtFields(byName, "typed-lite-client-info-request.bin", 200077);
            assertAllExtFields(byName, "typed-lite-group-info-request.bin", 200078);
            assertAllExtFields(byName, "typed-parent-topic-info-request.bin", 200075);
            assertAllExtFields(byName, "typed-delete-topic-request.bin", 215);
            assertAllExtFields(byName, "typed-delete-topic-from-namesrv-request.bin", 216);
            assertAllExtFields(byName, "typed-delete-kv-config-request.bin", 102);
            assertAllExtFields(byName, "typed-get-kv-config-request.bin", 101);
            assertAllExtFields(byName, "typed-put-kv-config-request.bin", 100);
            assertAllExtFields(byName, "typed-get-kv-list-by-namespace-request.bin", 219);
            assertAllExtFields(byName, "typed-get-kv-config-response.bin", 0);
            assertAllExtFields(byName, "typed-wipe-write-perm-request.bin", 205);
            assertAllExtFields(byName, "typed-add-write-perm-request.bin", 327);
            assertAllExtFields(byName, "typed-add-broker-request.bin", 902);
            assertAllExtFields(byName, "typed-remove-broker-request.bin", 903);
            assertAllExtFields(byName, "typed-clone-group-offset-request.bin", 314);
            assertAllExtFields(byName, "typed-wipe-write-perm-response.bin", 0);
            assertAllExtFields(byName, "typed-add-write-perm-response.bin", 0);
            assertAllExtFields(byName, "typed-delete-subscription-group-request.bin", 207);
            assertAllExtFields(byName, "typed-update-offset.bin", 15);
            assertAllExtFields(byName, "typed-end-transaction.bin", 37);
            assertAllExtFields(byName, "typed-check-transaction.bin", 39);
            assertAllExtFields(byName, "typed-check-transaction-response.bin", 0);
            assertAllExtFields(byName, "typed-reply.bin", 324);
            assertAllExtFields(byName, "typed-reply-v2.bin", 325);
            assertAllExtFields(byName, "typed-reply-push.bin", 326);
            assertAllExtFields(byName, "typed-send-response.bin", 0);
            assertAllExtFields(byName, "typed-recall-request.bin", 370);
            assertAllExtFields(byName, "typed-recall-response.bin", 0);
            assertAllExtFields(byName, "typed-pull-response.bin", 0);
            assertAllExtFields(byName, "typed-notification.bin", 200054);
            assertAllExtFields(byName, "typed-notification-response.bin", 0);
            assertAllExtFields(byName, "typed-reset-offset.bin", 211);
            assertAllExtFields(byName, "typed-reset-master-flush-offset.bin", 908);
            assertAllExtFields(byName, "typed-export-rocksdb-config-request.bin", 355);
            assertAllExtFields(byName, "typed-check-rocksdb-cq-write-progress-request.bin", 354);
            assertAllExtFields(byName, "typed-pop.bin", 200050);
            assertAllExtFields(byName, "typed-pop-response.bin", 0);
            assertAllExtFields(byName, "typed-pop-lite-response.bin", 0);
            assertAllExtFields(byName, "typed-ack.bin", 200051);
            assertAllExtFields(byName, "typed-change-invisible.bin", 200053);
            assertAllExtFields(byName, "typed-change-invisible-response.bin", 0);
            assertAllExtFields(byName, "typed-query-offset-response.bin", 0);
            assertAllExtFields(byName, "typed-search-offset-request.bin", 29);
            assertAllExtFields(byName, "typed-max-offset-request.bin", 30);
            assertAllExtFields(byName, "typed-min-offset-request.bin", 31);
            assertAllExtFields(byName, "typed-earliest-msg-storetime-request.bin", 32);
            assertAllExtFields(byName, "typed-search-offset-response.bin", 0);
            assertAllExtFields(byName, "typed-max-offset-response.bin", 0);
            assertAllExtFields(byName, "typed-min-offset-response.bin", 0);
            assertAllExtFields(byName, "typed-earliest-msg-storetime-response.bin", 0);
            assertAllExtFields(byName, "typed-unregister-request.bin", 35);
            assertAllExtFields(byName, "typed-lock-batch-request.bin", 41);
            assertAllExtFields(byName, "typed-unlock-batch-request.bin", 42);
            assertAllExtFields(byName, "typed-consumer-list-request.bin", 38);
            assertAllExtFields(byName, "typed-send-back-request.bin", 36);
            assertAllExtFields(byName, "typed-unregister-response.bin", 0);
            assertAllExtFields(byName, "typed-end-transaction-response.bin", 0);
            assertAllExtFields(byName, "typed-consumer-status-request.bin", 221);
            assertAllExtFields(byName, "typed-consumer-connection-list-request.bin", 203);
            assertAllExtFields(byName, "typed-consume-stats-request.bin", 208);
            assertAllExtFields(byName, "typed-consume-stats-in-broker-request.bin", 317);
            assertAllExtFields(byName, "typed-view-broker-stats-data-request.bin", 315);
            assertAllExtFields(byName, "typed-topic-config-request.bin", 351);
            assertAllExtFields(byName, "typed-consumer-running-info-request.bin", 307);
            assertAllExtFields(byName, "typed-heartbeat-request.bin", 34);
            assertAllExtFields(byName, "typed-polling-request.bin", 200055);
            assertAllExtFields(byName, "typed-polling-response.bin", 0);
            assertAllExtFields(byName, "typed-pop-lite-request.bin", 200070);
            assertAllExtFields(byName, "typed-lite-subscription-request.bin", 200071);
            assertAllExtFields(byName, "typed-notify-unsubscribe-lite-request.bin", 200073);
            assertAllExtFields(byName, "typed-consume-directly-request.bin", 309);
            assertExtField(byName, "typed-route.bin", 105, "topic", "orders");
            assertExtField(byName, "typed-create-topic-request.bin", 17, "topic", "orders");
            assertExtField(byName, "typed-create-topic-request.bin", 17, "defaultTopic", "TBW102");
            assertExtField(byName, "typed-create-user-request.bin", 3001, "username", "alice");
            assertExtField(byName, "typed-create-acl-request.bin", 3006, "subject", "subject");
            assertExtField(byName, "typed-delete-acl-request.bin", 3008, "subject", "subject");
            assertExtField(byName, "typed-delete-acl-request.bin", 3008, "policyType", "POLICY");
            assertExtField(byName, "typed-delete-acl-request.bin", 3008, "resource", "topic-a");
            assertExtField(byName, "typed-update-acl-request.bin", 3007, "subject", "subject");
            assertExtField(byName, "typed-update-user-request.bin", 3002, "username", "alice");
            assertExtField(byName, "typed-delete-user-request.bin", 3003, "username", "alice");
            assertExtField(byName, "typed-get-user-request.bin", 3004, "username", "alice");
            assertExtField(byName, "typed-get-acl-request.bin", 3009, "subject", "subject");
            assertExtField(byName, "typed-list-acls-request.bin", 3010, "subjectFilter", "alice");
            assertExtField(byName, "typed-list-acls-request.bin", 3010, "resourceFilter", "topic-a");
            assertExtField(byName, "typed-list-users-request.bin", 3005, "filter", "ali");
            assertExtField(byName, "typed-query-message-request.bin", 12, "key", "order-42");
            assertExtField(byName, "typed-query-message-request.bin", 12, "indexType", "UNIQUE_KEY");
            assertExtField(byName, "typed-query-message-response.bin", 0, "indexLastUpdateTimestamp", "1712345679000");
            assertExtField(byName, "typed-view-message-request.bin", 33, "offset", "9876");
            assertExtField(byName, "typed-send-v1.bin", 10, "producerGroup", "producers");
            assertExtField(byName, "typed-send-v2.bin", 310, "a", "producers");
        assertExtField(byName, "typed-send-batch-v2.bin", 320, "a", "producers");
            assertExtField(byName, "typed-pull.bin", 11, "consumerGroup", "workers");
            assertExtField(byName, "typed-query-offset.bin", 14, "consumerGroup", "workers");
            assertExtField(byName, "typed-query-consume-time-span-request.bin", 303, "group", "workers");
            assertExtField(byName, "typed-query-topics-by-consumer-request.bin", 343, "group", "workers");
            assertExtField(byName, "typed-query-subscription-by-consumer-request.bin", 345, "group", "workers");
            assertExtField(byName, "typed-query-topic-consume-by-who-request.bin", 300, "topic", "orders");
            assertExtField(byName, "typed-topic-stats-info-request.bin", 202, "topic", "orders");
            assertExtField(byName, "typed-topics-by-cluster-request.bin", 224, "cluster", "cluster-a");
            assertExtField(byName, "typed-get-subscription-group-config-request.bin", 352,
                "group", "workers");
            assertExtField(byName, "typed-lite-topic-info-request.bin", 200076, "parentTopic", "lite-parent");
            assertExtField(byName, "typed-lite-topic-info-request.bin", 200076, "liteTopic", "%LMQ%lite-parent%orders");
            assertExtField(byName, "typed-lite-client-info-request.bin", 200077, "parentTopic", "lite-parent");
            assertExtField(byName, "typed-lite-client-info-request.bin", 200077, "group", "workers");
            assertExtField(byName, "typed-lite-client-info-request.bin", 200077, "clientId", "client-a");
            assertExtField(byName, "typed-lite-client-info-request.bin", 200077, "maxCount", "1000");
            assertExtField(byName, "typed-lite-group-info-request.bin", 200078, "group", "workers");
            assertExtField(byName, "typed-lite-group-info-request.bin", 200078, "liteTopic", "%LMQ%lite-parent%orders");
            assertExtField(byName, "typed-lite-group-info-request.bin", 200078, "topK", "5");
            assertExtField(byName, "typed-parent-topic-info-request.bin", 200075, "topic", "lite-parent");
            assertExtField(byName, "typed-delete-topic-request.bin", 215, "topic", "orders");
            assertExtField(byName, "typed-delete-topic-request.bin", 215, "lo", "true");
            assertExtField(byName, "typed-delete-topic-from-namesrv-request.bin", 216,
                "clusterName", "cluster-a");
            assertExtField(byName, "typed-delete-kv-config-request.bin", 102,
                "namespace", "config-namespace");
            assertExtField(byName, "typed-delete-kv-config-request.bin", 102,
                "key", "config-key");
            assertExtField(byName, "typed-get-kv-config-request.bin", 101,
                "namespace", "config-namespace");
            assertExtField(byName, "typed-get-kv-config-request.bin", 101,
                "key", "config-key");
            assertExtField(byName, "typed-put-kv-config-request.bin", 100,
                "namespace", "config-namespace");
            assertExtField(byName, "typed-put-kv-config-request.bin", 100,
                "key", "config-key");
            assertExtField(byName, "typed-put-kv-config-request.bin", 100,
                "value", "config-value");
            assertExtField(byName, "typed-get-kv-list-by-namespace-request.bin", 219,
                "namespace", "config-namespace");
            assertExtField(byName, "typed-get-kv-config-response.bin", 0,
                "value", "config-value");
            assertExtField(byName, "typed-wipe-write-perm-request.bin", 205,
                "brokerName", "broker-a");
            assertExtField(byName, "typed-add-write-perm-request.bin", 327,
                "brokerName", "broker-a");
            assertExtField(byName, "typed-add-broker-request.bin", 902,
                "configPath", "/etc/rocketmq/broker.conf");
            assertExtField(byName, "typed-remove-broker-request.bin", 903,
                "brokerClusterName", "cluster-a");
            assertExtField(byName, "typed-remove-broker-request.bin", 903,
                "brokerName", "broker-a");
            assertExtField(byName, "typed-remove-broker-request.bin", 903,
                "brokerId", Long.toString(Long.MIN_VALUE));
            assertExtField(byName, "typed-wipe-write-perm-response.bin", 0,
                "wipeTopicCount", "17");
            assertExtField(byName, "typed-add-write-perm-response.bin", 0,
                "addTopicCount", "-3");
            assertExtField(byName, "typed-delete-subscription-group-request.bin", 207,
                "groupName", "workers");
            assertExtField(byName, "typed-delete-subscription-group-request.bin", 207,
                "cleanOffset", "true");
            assertExtField(byName, "typed-update-offset.bin", 15, "commitOffset", "9");
            assertExtField(byName, "typed-search-offset-request.bin", 29, "boundaryType", "UPPER");
            assertExtField(byName, "typed-search-offset-request.bin", 29, "timestamp", "1712345678901");
            assertExtField(byName, "typed-max-offset-request.bin", 30, "committed", "false");
            assertExtField(byName, "typed-earliest-msg-storetime-response.bin", 0, "timestamp", "-1");
            assertExtField(byName, "typed-end-transaction.bin", 37, "producerGroup", "producers");
            assertExtField(byName, "typed-check-transaction.bin", 39, "commitLogOffset", "9");
            assertExtField(byName, "typed-check-transaction-response.bin", 0, "commitOrRollback", "8");
            assertExtField(byName, "typed-reply.bin", 324, "bornHost", "127.0.0.1:1001");
            assertExtField(byName, "typed-send-response.bin", 0, "msgId", "msg-1");
            assertExtField(byName, "typed-recall-request.bin", 370, "recallHandle", "message-id%orders%1712345678901");
            assertExtField(byName, "typed-recall-response.bin", 0, "msgId", "broker-message-id");
            assertExtField(byName, "typed-pull-response.bin", 0, "nextBeginOffset", "10");
            assertExtField(byName, "typed-notification.bin", 200054, "consumerGroup", "workers");
            assertExtField(byName, "typed-notification-response.bin", 0, "hasMsg", "true");
            assertExtField(byName, "typed-reset-offset.bin", 211, "timestamp", "1712345678901");
            assertExtField(byName, "typed-reset-master-flush-offset.bin", 908,
                "masterFlushOffset", "9876543210");
            assertExtField(byName, "typed-export-rocksdb-config-request.bin", 355,
                "configType", "topics;subscriptionGroups;consumerOffsets;");
            assertExtField(byName, "typed-check-rocksdb-cq-write-progress-request.bin", 354,
                "topic", "orders");
            assertExtField(byName, "typed-check-rocksdb-cq-write-progress-request.bin", 354,
                "checkStoreTime", "1712345678901");
            assertExtField(byName, "typed-pop.bin", 200050, "maxMsgNums", "16");
            assertExtField(byName, "typed-ack.bin", 200051, "extraInfo", "startOffset popTime invisibleTime queueId");
            assertExtField(byName, "typed-change-invisible.bin", 200053, "invisibleTime", "60000");
            assertExtField(byName, "typed-change-invisible-response.bin", 0, "popTime", "123");
            assertExtField(byName, "typed-change-invisible-response.bin", 0, "invisibleTime", "456");
            assertExtField(byName, "typed-change-invisible-response.bin", 0, "reviveQid", "2");
            for (String id : new String[] {"reply-v2", "reply-push"}) {
                byte[] rustBody = frameBody(Files.readAllBytes(directory.resolve("typed-" + id + ".bin")));
                Path javaPath = fixturePath("remoting", "header-" +
                    (id.equals("reply-v2") ? "reply-v2-request" : "reply-push-request") + ".bin");
                byte[] javaBody = frameBody(Files.readAllBytes(javaPath));
                require(Arrays.equals(javaBody, rustBody) && Arrays.equals(javaBody,
                    new byte[] {0, (byte) 255, (byte) 128, 42, 10}), "binary reply body: " + id);
            }
        }
        System.out.println("Java decoded " + count + " Rust-generated fixture(s)");
    }

    private static Path fixturePath(String... segments) {
        Path fromRepositoryRoot = Path.of("fixtures", segments);
        if (Files.exists(fromRepositoryRoot)) return fromRepositoryRoot;
        Path fromJavaReference = Path.of("../../fixtures", segments);
        if (Files.exists(fromJavaReference)) return fromJavaReference;
        return fromRepositoryRoot;
    }

    private static void assertExtField(Map<String, Map<String, Object>> decoded, String name,
            int code, String field, String expected) {
        Map<String, Object> command = decoded.get(name);
        require(command != null, "Rust typed fixture exists: " + name);
        require(((Number) command.get("code")).intValue() == code, "Rust typed fixture code: " + name);
        require(expected.equals(((Map<?, ?>) command.get("ext_fields")).get(field)),
            "Rust typed fixture field " + field + ": " + name);
    }

    @SuppressWarnings("unchecked")
    private static void assertAllExtFields(Map<String, Map<String, Object>> decoded, String name,
            int code) throws Exception {
        Map<String, Object> rustCommand = decoded.get(name);
        require(rustCommand != null, "Rust typed fixture exists: " + name);
        require(((Number) rustCommand.get("code")).intValue() == code, "Rust typed fixture code: " + name);
        String fixtureStem;
        switch (name) {
            case "typed-route.bin": fixtureStem = "header-route-request"; break;
            case "typed-create-topic-request.bin": fixtureStem = "header-create-topic-request"; break;
            case "typed-create-user-request.bin": fixtureStem = "header-create-user-request"; break;
            case "typed-create-acl-request.bin": fixtureStem = "header-create-acl-request"; break;
            case "typed-delete-acl-request.bin": fixtureStem = "header-delete-acl-request"; break;
            case "typed-update-acl-request.bin": fixtureStem = "header-update-acl-request"; break;
            case "typed-update-user-request.bin": fixtureStem = "header-update-user-request"; break;
            case "typed-delete-user-request.bin": fixtureStem = "header-delete-user-request"; break;
            case "typed-get-user-request.bin": fixtureStem = "header-get-user-request"; break;
            case "typed-get-acl-request.bin": fixtureStem = "header-get-acl-request"; break;
            case "typed-list-acls-request.bin": fixtureStem = "header-list-acls-request"; break;
            case "typed-list-users-request.bin": fixtureStem = "header-list-users-request"; break;
            case "typed-query-message-request.bin": fixtureStem = "header-query-message-request"; break;
            case "typed-query-message-response.bin": fixtureStem = "header-query-message-response"; break;
            case "typed-view-message-request.bin": fixtureStem = "header-view-message-request"; break;
            case "typed-send-v1.bin": fixtureStem = "header-send-v1"; break;
            case "typed-send-v2.bin": fixtureStem = "header-send-v2"; break;
            case "typed-send-batch-v2.bin": fixtureStem = "header-send-batch-v2"; break;
            case "typed-pull.bin": fixtureStem = "header-pull-request"; break;
            case "typed-pull-response.bin": fixtureStem = "header-pull-response"; break;
            case "typed-query-offset.bin": fixtureStem = "header-query-offset-request"; break;
            case "typed-query-consume-time-span-request.bin": fixtureStem = "header-query-consume-time-span-request"; break;
            case "typed-query-topics-by-consumer-request.bin": fixtureStem = "header-query-topics-by-consumer-request"; break;
            case "typed-query-subscription-by-consumer-request.bin": fixtureStem = "header-query-subscription-by-consumer-request"; break;
            case "typed-query-topic-consume-by-who-request.bin": fixtureStem = "header-query-topic-consume-by-who-request"; break;
            case "typed-query-correction-offset-request.bin": fixtureStem = "header-query-correction-offset-request"; break;
            case "typed-update-group-forbidden-request.bin": fixtureStem = "header-update-group-forbidden-request"; break;
            case "typed-topic-stats-info-request.bin": fixtureStem = "header-topic-stats-info-request"; break;
            case "typed-get-all-subscription-group-request.bin": fixtureStem = "header-get-all-subscription-group-request"; break;
            case "typed-get-all-subscription-group-response.bin": fixtureStem = "header-get-all-subscription-group-response"; break;
            case "typed-query-consume-queue-request.bin": fixtureStem = "header-query-consume-queue-request"; break;
            case "typed-get-all-topic-config-request.bin": fixtureStem = "header-get-all-topic-config-request"; break;
            case "typed-get-all-topic-config-response.bin": fixtureStem = "header-get-all-topic-config-response"; break;
            case "typed-topics-by-cluster-request.bin": fixtureStem = "header-topics-by-cluster-request"; break;
            case "typed-get-subscription-group-config-request.bin": fixtureStem = "header-get-subscription-group-config-request"; break;
            case "typed-lite-topic-info-request.bin": fixtureStem = "header-lite-topic-info-request"; break;
            case "typed-lite-client-info-request.bin": fixtureStem = "header-lite-client-info-request"; break;
            case "typed-lite-group-info-request.bin": fixtureStem = "header-lite-group-info-request"; break;
            case "typed-parent-topic-info-request.bin": fixtureStem = "header-parent-topic-info-request"; break;
            case "typed-delete-topic-request.bin": fixtureStem = "header-delete-topic-request"; break;
            case "typed-delete-topic-from-namesrv-request.bin": fixtureStem = "header-delete-topic-from-namesrv-request"; break;
            case "typed-delete-kv-config-request.bin": fixtureStem = "header-delete-kv-config-request"; break;
            case "typed-get-kv-config-request.bin": fixtureStem = "header-get-kv-config-request"; break;
            case "typed-put-kv-config-request.bin": fixtureStem = "header-put-kv-config-request"; break;
            case "typed-get-kv-list-by-namespace-request.bin": fixtureStem = "header-get-kv-list-by-namespace-request"; break;
            case "typed-get-kv-config-response.bin": fixtureStem = "header-get-kv-config-response"; break;
            case "typed-wipe-write-perm-request.bin": fixtureStem = "header-wipe-write-perm-request"; break;
            case "typed-add-write-perm-request.bin": fixtureStem = "header-add-write-perm-request"; break;
            case "typed-add-broker-request.bin": fixtureStem = "header-add-broker-request"; break;
            case "typed-remove-broker-request.bin": fixtureStem = "header-remove-broker-request"; break;
            case "typed-clone-group-offset-request.bin": fixtureStem = "header-clone-group-offset-request"; break;
            case "typed-wipe-write-perm-response.bin": fixtureStem = "header-wipe-write-perm-response"; break;
            case "typed-add-write-perm-response.bin": fixtureStem = "header-add-write-perm-response"; break;
            case "typed-delete-subscription-group-request.bin": fixtureStem = "header-delete-subscription-group-request"; break;
            case "typed-update-offset.bin": fixtureStem = "header-update-offset-request"; break;
            case "typed-end-transaction.bin": fixtureStem = "header-end-transaction-request"; break;
            case "typed-check-transaction.bin": fixtureStem = "header-check-transaction-request"; break;
            case "typed-check-transaction-response.bin": fixtureStem = "header-check-transaction-response"; break;
            case "typed-reply.bin": fixtureStem = "header-reply-request"; break;
            case "typed-reply-v2.bin": fixtureStem = "header-reply-v2-request"; break;
            case "typed-reply-push.bin": fixtureStem = "header-reply-push-request"; break;
            case "typed-pop-response.bin": fixtureStem = "header-pop-response"; break;
            case "typed-send-response.bin": fixtureStem = "header-send-response"; break;
            case "typed-recall-request.bin": fixtureStem = "header-recall-message-request"; break;
            case "typed-recall-response.bin": fixtureStem = "header-recall-message-response"; break;
            case "typed-notification.bin": fixtureStem = "header-notification-request"; break;
            case "typed-notification-response.bin": fixtureStem = "header-notification-response"; break;
            case "typed-reset-offset.bin": fixtureStem = "header-reset-offset-request"; break;
            case "typed-reset-master-flush-offset.bin": fixtureStem = "header-reset-master-flush-offset"; break;
            case "typed-export-rocksdb-config-request.bin": fixtureStem = "header-export-rocksdb-config-request"; break;
            case "typed-check-rocksdb-cq-write-progress-request.bin": fixtureStem = "header-check-rocksdb-cq-write-progress-request"; break;
            case "typed-pop.bin": fixtureStem = "header-pop-request"; break;
            case "typed-pop-lite-response.bin": fixtureStem = "header-pop-lite-response"; break;
            case "typed-ack.bin": fixtureStem = "header-ack-request"; break;
            case "typed-change-invisible.bin": fixtureStem = "header-change-invisible-request"; break;
            case "typed-change-invisible-response.bin": fixtureStem = "header-change-invisible-response"; break;
            case "typed-query-offset-response.bin": fixtureStem = "header-query-offset-response"; break;
            case "typed-search-offset-request.bin": fixtureStem = "header-search-offset-request"; break;
            case "typed-max-offset-request.bin": fixtureStem = "header-max-offset-request"; break;
            case "typed-min-offset-request.bin": fixtureStem = "header-min-offset-request"; break;
            case "typed-earliest-msg-storetime-request.bin": fixtureStem = "header-earliest-msg-storetime-request"; break;
            case "typed-search-offset-response.bin": fixtureStem = "header-search-offset-response"; break;
            case "typed-max-offset-response.bin": fixtureStem = "header-max-offset-response"; break;
            case "typed-min-offset-response.bin": fixtureStem = "header-min-offset-response"; break;
            case "typed-earliest-msg-storetime-response.bin": fixtureStem = "header-earliest-msg-storetime-response"; break;
            case "typed-unregister-request.bin": fixtureStem = "header-unregister-request"; break;
            case "typed-lock-batch-request.bin": fixtureStem = "header-lock-batch-request"; break;
            case "typed-unlock-batch-request.bin": fixtureStem = "header-unlock-batch-request"; break;
            case "typed-consumer-list-request.bin": fixtureStem = "header-consumer-list-request"; break;
            case "typed-send-back-request.bin": fixtureStem = "header-send-back-request"; break;
            case "typed-unregister-response.bin": fixtureStem = "header-unregister-response"; break;
            case "typed-end-transaction-response.bin": fixtureStem = "header-end-transaction-response"; break;
            case "typed-consumer-status-request.bin": fixtureStem = "header-consumer-status-request"; break;
            case "typed-consumer-connection-list-request.bin": fixtureStem = "header-consumer-connection-list-request"; break;
            case "typed-consume-stats-request.bin": fixtureStem = "header-consume-stats-request"; break;
            case "typed-consume-stats-in-broker-request.bin": fixtureStem = "header-consume-stats-in-broker-request"; break;
            case "typed-view-broker-stats-data-request.bin": fixtureStem = "header-view-broker-stats-data-request"; break;
            case "typed-topic-config-request.bin": fixtureStem = "header-topic-config-request"; break;
            case "typed-consumer-running-info-request.bin": fixtureStem = "header-consumer-running-info-request"; break;
            case "typed-heartbeat-request.bin": fixtureStem = "header-heartbeat-request"; break;
            case "typed-polling-request.bin": fixtureStem = "header-polling-request"; break;
            case "typed-polling-response.bin": fixtureStem = "header-polling-response"; break;
            case "typed-pop-lite-request.bin": fixtureStem = "header-pop-lite-request"; break;
            case "typed-lite-subscription-request.bin": fixtureStem = "header-lite-subscription-request"; break;
            case "typed-notify-unsubscribe-lite-request.bin": fixtureStem = "header-notify-unsubscribe-lite-request"; break;
            case "typed-consume-directly-request.bin": fixtureStem = "header-consume-directly-request"; break;
            default: throw new IllegalArgumentException("no Java oracle fixture for " + name);
        }
        Path metadataPath = fixturePath("remoting", fixtureStem + ".json");
        Map<String, Object> metadata = com.alibaba.fastjson2.JSON.parseObject(Files.readString(metadataPath));
        Map<String, Object> canonical = (Map<String, Object>) ((Map<String, Object>) metadata.get("normalized_fields")).get("ext_fields");
        Map<String, Object> actual = (Map<String, Object>) rustCommand.get("ext_fields");
        require(canonical.equals(actual), "Rust typed ext fields match Java 5.5.1 fixture: " + name
            + " fixture=" + metadataPath.toAbsolutePath() + " user.dir=" + System.getProperty("user.dir")
            + " expected=" + canonical + " actual=" + actual);
    }

    private static void verifyRustBodies(Path directory) throws Exception {
        Map<String, Map<String, Object>> decoded = new LinkedHashMap<>();
        try (var files = Files.list(directory)) {
            for (Path file : (Iterable<Path>) files.filter(path -> path.toString().endsWith(".bin"))::iterator) {
                decoded.put(file.getFileName().toString(), decode(Files.readAllBytes(file)));
            }
        }
            require(decoded.size() == 19, "all Rust typed JSON body frames decoded");
        for (String name : decoded.keySet()) {
            String stem = name.replace(".bin", "");
            byte[] rustFrame = Files.readAllBytes(directory.resolve(name));
            Path javaPath = fixturePath("remoting", stem + ".bin");
            byte[] javaFrame = Files.readAllBytes(javaPath);
            Object rustBody = com.alibaba.fastjson2.JSON.parse(frameBody(rustFrame));
            Object javaBody = com.alibaba.fastjson2.JSON.parse(frameBody(javaFrame));
            if (rustBody instanceof Map && javaBody instanceof Map) {
                Map<?, ?> rustMap = (Map<?, ?>) rustBody;
                Map<?, ?> javaMap = (Map<?, ?>) javaBody;
                if (rustMap.containsKey("offsetTable") && javaMap.containsKey("offsetTable")) {
                    require(normalizeQueueTable(rustMap.get("offsetTable"))
                        .equals(normalizeQueueTable(javaMap.get("offsetTable"))),
                        "Rust typed offset table matches Java: " + stem);
                    continue;
                }
            }
            require(javaBody.equals(rustBody), "Rust typed body matches Java 5.5.1 body: " + stem
                + " expected=" + javaBody + " actual=" + rustBody);
        }
        verifyJavaBodyAtPath("body-consumer-list-absent");
        verifyJavaBodyAtPath("body-consumer-list-empty");
        verifyJavaBodyAtPath("body-consumer-list-populated");
        verifyJavaBodyAtPath("body-topic-config-wrapper");
        verifyJavaBodyAtPath("body-subscription-group-wrapper");
        verifyJavaBodyAtPath("body-query-consume-queue");
        verifyJavaBodyAtPath("body-lite-group-info");
        verifyJavaBodyAtPath("body-lock-request");
        verifyJavaBodyAtPath("body-unlock-request");
        verifyJavaBodyAtPath("body-lock-response");
        verifyJavaBodyAtPath("body-unlock-response");
        verifyRustBodyAgainstJava(directory, "body-consumer-list-absent.bin");
        verifyRustBodyAgainstJava(directory, "body-consumer-list-empty.bin");
        verifyRustBodyAgainstJava(directory, "body-consumer-list-populated.bin");
        verifyRustBodyAgainstJava(directory, "body-topic-config-wrapper.bin");
        verifyRustBodyAgainstJava(directory, "body-subscription-group-wrapper.bin");
        verifyRustBodyAgainstJava(directory, "body-query-consume-queue.bin");
        verifyRustBodyAgainstJava(directory, "body-lite-group-info.bin");
        verifyRustBodyAgainstJava(directory, "body-lock-request.bin");
        verifyRustBodyAgainstJava(directory, "body-unlock-request.bin");
        verifyRustBodyAgainstJava(directory, "body-lock-response.bin");
        verifyRustBodyAgainstJava(directory, "body-unlock-response.bin");
        assertBodyField(directory, decoded, "body-query-assignment.bin", 400, "topic", "orders");
        assertBodyField(directory, decoded, "body-set-request-mode.bin", 401, "mode", "POP");
        assertBodyField(directory, decoded, "body-lite-subscription.bin", 200071, "subscriptionSet", null);
        assertBodyField(directory, decoded, "body-batch-ack.bin", 200151, "brokerName", "broker-a");
        assertBodyField(directory, decoded, "body-reset-offset.bin", 212, "offsetTable", null);
        assertBodyField(directory, decoded, "body-consumer-status.bin", 223, "messageQueueTable", null);
        assertBodyField(directory, decoded, "body-consume-stats.bin", 208, "offsetTable", null);
        assertBodyField(directory, decoded, "body-lite-group-info.bin", 200078, "group", "workers");
        assertBodyField(directory, decoded, "body-lite-group-info.bin", 200078, "totalLagCount", "40");
        assertBodyField(directory, decoded, "body-consume-directly.bin", 309, "consumeResult", "CR_SUCCESS");
        assertBodyField(directory, decoded, "body-topic-config-wrapper.bin", 21, "topicConfigTable", null);
        assertBodyField(directory, decoded, "body-subscription-group-wrapper.bin", 201, "subscriptionGroupTable", null);
        assertBodyField(directory, decoded, "body-query-consume-queue.bin", 321, "queueData", null);
        assertBodyField(directory, decoded, "body-consumer-list-empty.bin", 38, "consumerIdList", null);
        assertBodyField(directory, decoded, "body-consumer-list-populated.bin", 38, "consumerIdList", null);
        assertBodyField(directory, decoded, "body-lock-request.bin", 41, "mqSet", null);
        assertBodyField(directory, decoded, "body-unlock-request.bin", 42, "mqSet", null);
        assertBodyField(directory, decoded, "body-lock-response.bin", 41, "lockOKMQSet", null);
        require(decoded.containsKey("body-consume-directly.bin"), "direct consume body decoded");
        for (String name : decoded.keySet()) {
            require(!decoded.get(name).get("body_length").equals(0), "typed body is non-empty: " + name);
        }
        System.out.println("Java decoded " + decoded.size() + " Rust typed body fixture(s)");
    }

    private static void verifyRustBodyAgainstJava(Path rustDirectory, String name) throws Exception {
        byte[] rustFrame = Files.readAllBytes(rustDirectory.resolve(name));
        Path javaPath = fixturePath("remoting", name);
        Object rustBody = com.alibaba.fastjson2.JSON.parse(frameBody(rustFrame));
        Object javaBody = com.alibaba.fastjson2.JSON.parse(frameBody(Files.readAllBytes(javaPath)));
        require(javaBody.equals(rustBody), "Rust typed body matches Java 5.5.1 body: " + name
            + " expected=" + javaBody + " actual=" + rustBody);
    }

    private static void verifyJavaBodyAtPath(String stem) throws Exception {
        Path path = fixturePath("remoting", stem + ".bin");
        byte[] frame = Files.readAllBytes(path);
        Object body = com.alibaba.fastjson2.JSON.parse(frameBody(frame));
        if (stem.startsWith("body-consumer-list-")) {
            org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody decoded = com.alibaba.fastjson2.JSON.parseObject(
                frameBody(frame), org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody.class);
            if (stem.endsWith("absent")) require(decoded.getConsumerIdList() == null, "absent consumer list preserved");
            if (stem.endsWith("empty")) require(decoded.getConsumerIdList() != null && decoded.getConsumerIdList().isEmpty(), "empty consumer list preserved");
            if (stem.endsWith("populated")) require(decoded.getConsumerIdList().size() == 2, "populated consumer list preserved");
        } else if (stem.equals("body-topic-config-wrapper")) {
            org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper decoded =
                com.alibaba.fastjson2.JSON.parseObject(frameBody(frame),
                    org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper.class);
            require(decoded.getDataVersion().getStateVersion() == 7, "topic config DataVersion decoded");
            require(decoded.getDataVersion().getCounter().get() == 4, "topic config DataVersion counter decoded");
            require(decoded.getTopicConfigTable().get("orders").getReadQueueNums() == 16,
                "topic config table decodes Java defaults");
        } else if (stem.equals("body-subscription-group-wrapper")) {
            org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper decoded =
                com.alibaba.fastjson2.JSON.parseObject(frameBody(frame),
                    org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper.class);
            require(decoded.getDataVersion().getStateVersion() == 6,
                "subscription group DataVersion decoded");
            require(decoded.getDataVersion().getCounter().get() == 9,
                "subscription group DataVersion counter decoded");
            require(decoded.getSubscriptionGroupTable().get("workers").getRetryQueueNums() == 5,
                "subscription group config table decoded");
            require(decoded.getForbiddenTable().get("workers").get("orders") == 1,
                "subscription group forbidden table decoded");
        } else if (stem.equals("body-query-consume-queue")) {
            org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody decoded =
                com.alibaba.fastjson2.JSON.parseObject(frameBody(frame),
                    org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody.class);
            require(decoded.getSubscriptionData().getSubVersion() == 1234L,
                "consume queue subscription data decoded");
            require(decoded.getQueueData().size() == 1
                    && decoded.getQueueData().get(0).getPhysicOffset() == 987654321L,
                "consume queue row decoded");
            require(decoded.getMaxQueueIndex() == 20L && decoded.getMinQueueIndex() == 4L,
                "consume queue index bounds decoded");
        } else if (stem.equals("body-lite-group-info")) {
            GetLiteGroupInfoResponseBody decoded = com.alibaba.fastjson2.JSON.parseObject(
                frameBody(frame), GetLiteGroupInfoResponseBody.class);
            require(decoded.getEarliestUnconsumedTimestamp() == 1400, "Lite group earliest timestamp decoded");
            require(decoded.getLagCountTopK().size() == 3 && decoded.getLagCountTopK().get(1) == null,
                "Lite lag list null element preserved");
            require(decoded.getLagCountTopK().get(0).getConsumerOffset() == -1,
                "serialized-false Lite consumer offset uses Java default");
            require(!((Map<?, ?>) body).containsKey("consumerOffset"),
                "Lite consumerOffset excluded from body JSON");
        } else if (stem.endsWith("response")) {
            LockBatchResponseBody decoded = com.alibaba.fastjson2.JSON.parseObject(frameBody(frame), LockBatchResponseBody.class);
            require(!decoded.getLockOKMQSet().isEmpty(), "lock/unlock response queue set decoded");
        } else if (stem.endsWith("request")) {
            LockBatchRequestBody decoded = com.alibaba.fastjson2.JSON.parseObject(frameBody(frame), LockBatchRequestBody.class);
            require(decoded.getMqSet() != null && !decoded.getMqSet().isEmpty(), "lock/unlock request queue set decoded");
        }
        require(body != null, "Java body parsed: " + stem);
    }

    private static void assertBodyField(Path directory, Map<String, Map<String, Object>> decoded,
            String name, int code, String field, String expected) throws Exception {
        Map<String, Object> command = decoded.get(name);
        require(command != null, "Rust typed body exists: " + name);
        require(((Number) command.get("code")).intValue() == code, "Rust typed body code: " + name);
        byte[] frame = Files.readAllBytes(directory.resolve(name));
        int headerLength = RemotingCommand.getHeaderLength(ByteBuffer.wrap(frame, 4, 4).getInt());
        byte[] body = Arrays.copyOfRange(frame, 8 + headerLength, frame.length);
        Map<?, ?> json = (Map<?, ?>) com.alibaba.fastjson2.JSON.parse(body);
        Object actual = json.get(field);
        if (expected == null) require(actual != null, "Rust typed body field: " + name + "/" + field);
        else {
            String actualText = actual == null ? "null" : String.valueOf(actual);
            require(expected.equals(actualText), "Rust typed body value: " + name + "/" + field + " got " + actualText);
        }
    }

    private static byte[] frameBody(byte[] frame) {
        int headerLength = RemotingCommand.getHeaderLength(ByteBuffer.wrap(frame, 4, 4).getInt());
        return Arrays.copyOfRange(frame, 8 + headerLength, frame.length);
    }

    private static Map<String, Object> normalizeQueueTable(Object table) {
        Map<String, Object> normalized = new LinkedHashMap<>();
        ((Map<?, ?>) table).forEach((key, value) -> {
            Object queue = key instanceof String
                ? com.alibaba.fastjson2.JSON.parse((String) key) : key;
            normalized.put(com.alibaba.fastjson2.JSON.toJSONString(queue), value);
        });
        return normalized;
    }

    private static void writeFixture(Path output, String id, int code, SerializeType type) throws Exception {
        RemotingCommand command = command(code, type, id);
        writeFixture(output, id, code, type, command, bytes(command.encode()));
    }

    private static void writeFastHeaderFixtures(Path output) throws Exception {
        RemotingCommand fast = command(10, SerializeType.ROCKETMQ, "HEADER-FAST-001");
        fast.setRemark("header-equivalent");
        fast.setExtFields(new java.util.HashMap<>(Map.of("oracle", "5.5.1")));
        fast.writeCustomHeader(new FixtureFastHeader("fixture-topic"));
        ByteBuf buffer = Unpooled.buffer();
        fast.fastEncodeHeader(buffer);
        buffer.writeBytes(fast.getBody() == null ? new byte[0] : fast.getBody());
        byte[] fastFrame = new byte[buffer.readableBytes()];
        buffer.readBytes(fastFrame);
        buffer.release();
        writeFixture(output, "HEADER-FAST-001", 10, SerializeType.ROCKETMQ, fast, fastFrame);

        RemotingCommand ordinary = command(10, SerializeType.ROCKETMQ, "header-equivalent");
        ordinary.addExtField("topic", "fixture-topic");
        writeFixture(output, "HEADER-ORDINARY-001", 10, SerializeType.ROCKETMQ, ordinary, bytes(ordinary.encode()));
    }

    private static void writeFixture(Path output, String id, int code, SerializeType type,
            RemotingCommand command, byte[] frame) throws Exception {
        Map<String, Object> semantic = decode(frame);
        String stem = id.toLowerCase().replaceAll("[^a-z0-9-]", "-");
        Path binary = output.resolve(stem + ".bin");
        Files.write(binary, frame);
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("java_client_version", "5.5.1");
        metadata.put("server_version", "5.3.4");
        metadata.put("scenario_id", id);
        metadata.put("serialization", type.name());
        metadata.put("request_code", code);
        metadata.put("normalized_fields", semantic);
        metadata.put("body_length", command.getBody() == null ? 0 : command.getBody().length);
        metadata.put("payload_sha256", sha256(frame));
        metadata.put("creation_command", "mvn -f compat/java-reference/pom.xml exec:java -Dexec.args=\"fixtures --output <DIR>\"");
        Files.writeString(output.resolve(stem + ".json"), toJson(metadata) + "\n", StandardCharsets.UTF_8);
    }

    private static void writeOversizedKeyFixture(Path output) throws Exception {
        String key = "k".repeat(32768);
        RemotingCommand command = command(34, SerializeType.ROCKETMQ, "PRIVATE-LARGE-KEY");
        command.setExtFields(new java.util.HashMap<>());
        command.addExtField(key, "v");
        byte[] frame = bytes(command.encode());
        Path edgeCases = output.resolve("edge-cases");
        Files.createDirectories(edgeCases);
        Files.write(edgeCases.resolve("private-large-key.bin"), frame);
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("java_client_version", "5.5.1");
        metadata.put("server_version", "5.3.4");
        metadata.put("scenario_id", "PRIVATE-LARGE-KEY");
        metadata.put("serialization", SerializeType.ROCKETMQ.name());
        metadata.put("request_code", 34);
        metadata.put("utf8_key_length", key.getBytes(StandardCharsets.UTF_8).length);
        metadata.put("signed_wire_key_length", (short) key.getBytes(StandardCharsets.UTF_8).length);
        metadata.put("payload_sha256", sha256(frame));
        metadata.put("creation_command", "mvn -f compat/java-reference/pom.xml exec:java -Dexec.args=\"fixtures --output <DIR>\"");
        Files.writeString(edgeCases.resolve("private-large-key.json"), toJson(metadata) + "\n",
            StandardCharsets.UTF_8);
    }

    private static void writeScenario(String id, Path output) throws Exception {
        int code;
        if (id.startsWith("ROUTE")) code = 105;
        else if (id.startsWith("HEARTBEAT")) code = 34;
        else if (id.startsWith("PROD")) code = 10;
        else if (id.startsWith("CONS")) code = 11;
        else if (id.startsWith("ORDER")) code = 41;
        else throw new IllegalArgumentException("unknown offline scenario: " + id);
        Files.createDirectories(output.toAbsolutePath().getParent());
        byte[] frame = bytes(command(code, SerializeType.JSON, id).encode());
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("scenario_id", id);
        result.put("java_client_version", "5.5.1");
        result.put("server_version", "5.3.4");
        result.put("execution_phase", 19);
        result.put("live_status", "PENDING_PHASE_19");
        result.put("normalized_fields", decode(frame));
        Files.writeString(output, toJson(result) + "\n", StandardCharsets.UTF_8);
        System.out.println(toJson(result));
    }

    private static final class FixtureFastHeader implements CommandCustomHeader, FastCodesHeader {
        private final String topic;

        private FixtureFastHeader(String topic) {
            this.topic = topic;
        }

        @Override
        public void checkFields() {
        }

        @Override
        public void encode(ByteBuf out) {
            writeIfNotNull(out, "topic", topic);
        }

        @Override
        public void decode(java.util.HashMap<String, String> fields) {
            // This fixture exercises the Java encoder path only.
        }
    }

    private static RemotingCommand command(int code, SerializeType type, String remark) {
        RemotingCommand command = RemotingCommand.createRequestCommand(code, null);
        command.setLanguage(LanguageCode.RUST);
        command.setVersion(0);
        command.setOpaque(7);
        command.setRemark(remark);
        command.addExtField("oracle", "5.5.1");
        if (remark.startsWith("HEADER-PROOF")) command.addExtField("topic", "fixture-topic");
        command.setSerializeTypeCurrentRPC(type);
        return command;
    }

    private static Map<String, Object> decode(byte[] frame) throws Exception {
        if (frame.length < 8 || frame.length > MAX_FRAME_SIZE) throw new IOException("invalid frame size");
        ByteBuffer input = ByteBuffer.wrap(frame);
        int declared = input.getInt();
        if (declared != frame.length - 4) throw new IOException("frame length mismatch");
        int markedLength = input.getInt();
        int headerLength = RemotingCommand.getHeaderLength(markedLength);
        if (headerLength > input.remaining()) throw new IOException("header length exceeds frame");
        RemotingCommand command = RemotingCommand.decode(Arrays.copyOfRange(frame, 4, frame.length));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("code", command.getCode());
        result.put("language", command.getLanguage() == null ? null : command.getLanguage().name());
        result.put("version", command.getVersion());
        result.put("opaque", command.getOpaque());
        result.put("flag", command.getFlag());
        result.put("response", command.isResponseType());
        result.put("remark", command.getRemark());
        result.put("ext_fields", command.getExtFields());
        result.put("body_length", command.getBody() == null ? 0 : command.getBody().length);
        result.put("body_sha256", command.getBody() == null ? sha256(new byte[0]) : sha256(command.getBody()));
        result.put("serialization", command.getSerializeTypeCurrentRPC().name());
        result.put("frame_sha256", sha256(frame));
        return result;
    }

    private static byte[] bytes(ByteBuffer buffer) {
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private static String sha256(byte[] bytes) throws IOException {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder result = new StringBuilder();
            for (byte value : digest) result.append(String.format("%02x", value));
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 unavailable", e);
        }
    }

    private static String toJson(Object value) {
        if (value == null) return "null";
        if (value instanceof String) return "\"" + escape((String) value) + "\"";
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof Map<?, ?>) {
            StringBuilder result = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!first) result.append(',');
                first = false;
                result.append(toJson(entry.getKey().toString())).append(':').append(toJson(entry.getValue()));
            }
            return result.append('}').toString();
        }
        if (value instanceof Iterable<?>) {
            StringBuilder result = new StringBuilder("[");
            boolean first = true;
            for (Object item : (Iterable<?>) value) {
                if (!first) result.append(',');
                first = false;
                result.append(toJson(item));
            }
            return result.append(']').toString();
        }
        return toJson(value.toString());
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }

    private static void require(boolean condition, String label) {
        if (!condition) throw new IllegalStateException("self-test failed: " + label);
    }
}
