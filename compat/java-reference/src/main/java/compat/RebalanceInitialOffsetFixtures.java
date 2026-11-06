package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.RebalancePushImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

/** Golden cases execute Java 5.5.1 RebalancePushImpl against controlled offset RPC results. */
public final class RebalanceInitialOffsetFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";
    private static final long MAX_OFFSET = 9876L;
    private static final long SEARCH_OFFSET = 5432L;

    private RebalanceInitialOffsetFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("usage: RebalanceInitialOffsetFixtures <fixture.json> [--verify]");
        }
        Path output = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(generate()) + "\n";
        if (verify) {
            if (!Files.exists(output) || !json.equals(Files.readString(output))) {
                throw new IllegalStateException("Java initial-offset fixture drift: " + output);
            }
            System.out.println("verified Java 5.5.1 initial-offset fixtures at " + output);
        } else {
            Files.createDirectories(output.getParent());
            Files.writeString(output, json);
            System.out.println("wrote Java 5.5.1 initial-offset fixtures to " + output);
        }
    }

    private static Map<String, Object> generate() {
        Map<String, Object> root = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("sourceCommit", SOURCE_COMMIT);
        provenance.put("sourceClasses", List.of(
            "DefaultMQPushConsumer.setConsumeFromWhere/setConsumeTimestamp",
            "DefaultMQPushConsumerImpl.setOffsetStore",
            "RebalancePushImpl.computePullFromWhereWithException",
            "OffsetStore.readOffset(READ_FROM_STORE)",
            "MQAdminImpl.maxOffset/searchOffset",
            "UtilAll.parseDate(YYYYMMDDHHMMSS)"));
        provenance.put("serverBaselineContacted", false);
        root.put("provenance", provenance);
        root.put("mockMaxOffset", MAX_OFFSET);
        root.put("mockSearchOffset", SEARCH_OFFSET);
        root.put("timestampParsing", timestampParsingCases());

        List<Map<String, Object>> cases = new ArrayList<>();
        FixtureClientInstance client = new FixtureClientInstance();
        for (ConsumeFromWhere policy : ConsumeFromWhere.values()) {
            addCase(cases, client, policy, "orders", 71L, "stored_offset");
            addCase(cases, client, policy, "orders", -1L, "missing_regular_offset");
            addCase(cases, client, policy, "%RETRY%fixture-group", -1L, "missing_retry_offset");
            addCase(cases, client, policy, "orders", -2L, "invalid_negative_offset");
        }
        client.getPullMessageService().shutdown(true);
        client.getScheduledExecutorService().shutdownNow();
        client.getMQClientAPIImpl().shutdown();
        root.put("cases", cases);
        return root;
    }

    private static List<Map<String, Object>> timestampParsingCases() {
        List<Map<String, Object>> cases = new ArrayList<>();
        for (String input : List.of(
            "20240229010203",
            "20230229010203",
            "20241301010203",
            "20240229010203xyz",
            "2024022901020x",
            "2026010112000",
            "2024022901",
            " 20240229010203",
            "+20240229010203",
            "٢٠٢٤٠٢٢٩٠١٠٢٠٣",
            "２０２４０２２９０１０２０３",
            "not-a-date")) {
            Map<String, Object> result = new TreeMap<>();
            result.put("input", input);
            result.put("accepted", UtilAll.parseDate(input, UtilAll.YYYYMMDDHHMMSS) != null);
            cases.add(result);
        }
        return cases;
    }

    private static void addCase(List<Map<String, Object>> cases, FixtureClientInstance client, ConsumeFromWhere policy,
        String topic, long storedOffset, String scenario) {
        Map<String, Object> result = new TreeMap<>();
        result.put("id", policy.name() + "_" + scenario);
        result.put("consumeFromWhere", policy.name());
        result.put("topic", topic);
        result.put("storedOffset", storedOffset);
        result.put("consumeTimestamp", "20240229010203");
        try {
            result.put("result", compute(client, policy, topic, storedOffset));
            result.put("query", query(policy, topic, storedOffset));
        } catch (MQClientException error) {
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        }
        cases.add(result);
    }

    private static long compute(FixtureClientInstance client, ConsumeFromWhere policy, String topic, long storedOffset)
        throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("fixture-group");
        consumer.setConsumeFromWhere(policy);
        consumer.setConsumeTimestamp("20240229010203");
        DefaultMQPushConsumerImpl consumerImpl = new DefaultMQPushConsumerImpl(consumer, null);
        consumerImpl.setOffsetStore(offsetStore(storedOffset));
        AllocateMessageQueueStrategy strategy = new AllocateMessageQueueAveragely();
        RebalancePushImpl rebalance = new RebalancePushImpl(
            consumer.getConsumerGroup(), MessageModel.CLUSTERING, strategy, client, consumerImpl);
        return rebalance.computePullFromWhereWithException(new MessageQueue(topic, "broker-a", 0));
    }

    private static String query(ConsumeFromWhere policy, String topic, long storedOffset) {
        if (storedOffset >= 0 || storedOffset < -1) {
            return "NONE";
        }
        if (policy == ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET) {
            return "NONE";
        }
        if (policy == ConsumeFromWhere.CONSUME_FROM_TIMESTAMP
            && !topic.startsWith("%RETRY%")) {
            return "SEARCH_OFFSET";
        }
        if (policy == ConsumeFromWhere.CONSUME_FROM_TIMESTAMP
            || !topic.startsWith("%RETRY%")) {
            return "MAX_OFFSET";
        }
        return "NONE";
    }

    private static OffsetStore offsetStore(long offset) {
        return (OffsetStore) Proxy.newProxyInstance(
            OffsetStore.class.getClassLoader(), new Class<?>[] {OffsetStore.class},
            (proxy, method, args) -> {
                if ("readOffset".equals(method.getName())) {
                    if (args[1] != ReadOffsetType.READ_FROM_STORE) {
                        throw new AssertionError("unexpected Java offset read mode: " + args[1]);
                    }
                    return offset;
                }
                throw new UnsupportedOperationException("unexpected OffsetStore call: " + method.getName());
            });
    }

    private static final class FixtureClientInstance extends MQClientInstance {
        private final MQAdminImpl admin;

        FixtureClientInstance() {
            super(new ClientConfig(), 1, "rebalance-initial-offset-fixture");
            admin = new MQAdminImpl(this) {
                @Override
                public long maxOffset(MessageQueue queue) {
                    return MAX_OFFSET;
                }

                @Override
                public long searchOffset(MessageQueue queue, long timestamp) {
                    return SEARCH_OFFSET;
                }
            };
        }

        @Override
        public MQAdminImpl getMQAdminImpl() {
            return admin;
        }
    }
}
