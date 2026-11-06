package compat;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;

/** Invoked Java 5.5.1 Lite consumer, POP settings, receipt and result oracle. */
public final class LitePopFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private LitePopFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("usage: <fixture.json> [--verify]");
        }
        Path fixture = Path.of(args[0]);
        String expected = render();
        if (args.length > 1 && "--verify".equals(args[1])) {
            String actual = Files.readString(fixture);
            if (!expected.equals(actual)) {
                throw new IllegalStateException("Java Lite/POP fixture drift: " + fixture);
            }
        } else {
            Files.createDirectories(fixture.getParent());
            Files.writeString(fixture, expected);
        }
    }

    private static String render() throws Exception {
        long liteCreatedBefore = System.currentTimeMillis();
        DefaultLitePullConsumer lite = new DefaultLitePullConsumer("lite-fixture");
        DefaultLitePullConsumer noArgumentLite = new DefaultLitePullConsumer();
        Map<String, Object> defaults = new TreeMap<>();
        defaults.put("consumerGroup", lite.getConsumerGroup());
        defaults.put("noArgumentConsumerGroup", noArgumentLite.getConsumerGroup());
        defaults.put("enableStreamRequestType", lite.isEnableStreamRequestType());
        defaults.put("brokerSuspendMaxTimeMillis", lite.getBrokerSuspendMaxTimeMillis());
        defaults.put("consumerTimeoutMillisWhenSuspend", lite.getConsumerTimeoutMillisWhenSuspend());
        defaults.put("consumerPullTimeoutMillis", lite.getConsumerPullTimeoutMillis());
        defaults.put("messageModel", lite.getMessageModel().name());
        defaults.put("autoCommit", lite.isAutoCommit());
        defaults.put("pullThreadNums", lite.getPullThreadNums());
        defaults.put("autoCommitIntervalMillis", lite.getAutoCommitIntervalMillis());
        defaults.put("pullBatchSize", lite.getPullBatchSize());
        defaults.put("pullThresholdForAll", lite.getPullThresholdForAll());
        defaults.put("consumeMaxSpan", lite.getConsumeMaxSpan());
        defaults.put("pullThresholdForQueue", lite.getPullThresholdForQueue());
        defaults.put("pullThresholdSizeForQueue", lite.getPullThresholdSizeForQueue());
        defaults.put("pollTimeoutMillis", lite.getPollTimeoutMillis());
        defaults.put("topicMetadataCheckIntervalMillis", lite.getTopicMetadataCheckIntervalMillis());
        defaults.put("consumeFromWhere", lite.getConsumeFromWhere().name());
        long consumeTimestamp = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
            .parse(lite.getConsumeTimestamp()).getTime();
        defaults.put("consumeTimestampThirtyMinutesAgo", Math.abs(consumeTimestamp - (liteCreatedBefore - 30 * 60 * 1000L)) < 1_000);
        defaults.put("enableMessageTrace", lite.isEnableMsgTrace());
        defaults.put("customizedTraceTopic", lite.getCustomizedTraceTopic());
        defaults.put("offsetStoreAbsent", lite.getOffsetStore() == null);
        defaults.put("messageQueueListenerAbsent", lite.getMessageQueueListener() == null);
        defaults.put("allocateStrategy", lite.getAllocateMessageQueueStrategy().getName());

        lite.setAutoCommit(false);
        lite.setAutoCommitIntervalMillis(999);
        boolean belowMinimumWasIgnored = lite.getAutoCommitIntervalMillis() == 5_000;
        lite.setAutoCommitIntervalMillis(2_000);
        lite.setPullThreadNums(7);
        lite.setPullBatchSize(13);
        lite.setPullThresholdForAll(23);
        lite.setConsumeMaxSpan(29);
        lite.setPullThresholdForQueue(31);
        lite.setPullThresholdSizeForQueue(37);
        lite.setPollTimeoutMillis(41);
        lite.setTopicMetadataCheckIntervalMillis(43);
        lite.setMessageModel(org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.BROADCASTING);
        lite.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        lite.setConsumeTimestamp("20000101000000");
        lite.setConsumerPullTimeoutMillis(47);
        lite.setConsumerTimeoutMillisWhenSuspend(53);
        lite.setUnitMode(true);
        lite.setConsumerGroup("lite-configured-group");
        lite.setAllocateMessageQueueStrategy(new AllocateMessageQueueAveragely());
        lite.setEnableMsgTrace(true);
        lite.setCustomizedTraceTopic("trace-configured-topic");
        OffsetStore configuredOffsetStore = (OffsetStore) java.lang.reflect.Proxy.newProxyInstance(
            OffsetStore.class.getClassLoader(), new Class<?>[] {OffsetStore.class}, (proxy, method, args) -> null);
        lite.setOffsetStore(configuredOffsetStore);
        lite.setMessageQueueListener((topic, queues, assigned) -> { });
        lite.setSubExpressionForAssign("lite-config-topic", "tag-a || tag-b");
        Map<String, Object> configured = new TreeMap<>();
        configured.put("belowMinimumAutoCommitIntervalIgnored", belowMinimumWasIgnored);
        configured.put("autoCommit", lite.isAutoCommit());
        configured.put("autoCommitIntervalMillis", lite.getAutoCommitIntervalMillis());
        configured.put("pullThreadNums", lite.getPullThreadNums());
        configured.put("pullBatchSize", lite.getPullBatchSize());
        configured.put("pullThresholdForAll", lite.getPullThresholdForAll());
        configured.put("consumeMaxSpan", lite.getConsumeMaxSpan());
        configured.put("pullThresholdForQueue", lite.getPullThresholdForQueue());
        configured.put("pullThresholdSizeForQueue", lite.getPullThresholdSizeForQueue());
        configured.put("pollTimeoutMillis", lite.getPollTimeoutMillis());
        configured.put("topicMetadataCheckIntervalMillis", lite.getTopicMetadataCheckIntervalMillis());
        configured.put("messageModel", lite.getMessageModel().name());
        configured.put("consumeFromWhere", lite.getConsumeFromWhere().name());
        configured.put("consumerPullTimeoutMillis", lite.getConsumerPullTimeoutMillis());
        configured.put("consumerTimeoutMillisWhenSuspend", lite.getConsumerTimeoutMillisWhenSuspend());
        configured.put("unitMode", lite.isUnitMode());
        configured.put("consumerGroup", lite.getConsumerGroup());
        configured.put("allocationStrategy", lite.getAllocateMessageQueueStrategy().getName());
        configured.put("enableMessageTrace", lite.isEnableMsgTrace());
        configured.put("customizedTraceTopic", lite.getCustomizedTraceTopic());
        configured.put("offsetStoreRetained", lite.getOffsetStore() == configuredOffsetStore);
        configured.put("messageQueueListenerPresent", lite.getMessageQueueListener() != null);
        configured.put("consumeTimestamp", lite.getConsumeTimestamp());

        Map<String, MessageSelector> heartbeatSelectors = new TreeMap<>();
        heartbeatSelectors.put("heartbeat-tag", MessageSelector.byTag("tag-a || tag-b"));
        heartbeatSelectors.put("heartbeat-sql", MessageSelector.bySql("price > 100"));
        lite.buildSubscriptionsForHeartbeat(heartbeatSelectors);
        Map<String, Object> heartbeatSubscriptions = new TreeMap<>();
        for (org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData subscription
            : lite.getSubscriptionsForHeartbeat()) {
            Map<String, Object> value = new TreeMap<>();
            value.put("expressionType", subscription.getExpressionType());
            value.put("expression", subscription.getSubString());
            value.put("tagCount", subscription.getTagsSet() == null ? null : subscription.getTagsSet().size());
            heartbeatSubscriptions.put(subscription.getTopic(), value);
        }

        List<String> liteApi = Arrays.stream(org.apache.rocketmq.client.consumer.LitePullConsumer.class.getMethods())
            .map(Method::toGenericString)
            .sorted()
            .collect(Collectors.toList());
        List<String> liteConfigApi = Arrays.stream(DefaultLitePullConsumer.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(Method::toGenericString)
            .sorted()
            .collect(Collectors.toList());
        List<String> liteConstructors = Arrays.stream(DefaultLitePullConsumer.class.getDeclaredConstructors())
            .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
            .map(Constructor::toGenericString)
            .sorted()
            .collect(Collectors.toList());

        Map<String, Object> invalidInputs = new TreeMap<>();
        DefaultLitePullConsumer emptyAssignment = new DefaultLitePullConsumer("empty-assignment");
        invalidInputs.put("emptyAssignment", exceptionName(() -> emptyAssignment.assign(Collections.emptyList())));
        DefaultLitePullConsumer blankAssignSelector = new DefaultLitePullConsumer("blank-assign-selector");
        invalidInputs.put("blankAssignSelector", exceptionName(() -> blankAssignSelector.setSubExpressionForAssign("topic", "  ")));
        DefaultLitePullConsumer subscribeThenAssign = new DefaultLitePullConsumer("subscribe-then-assign");
        subscribeThenAssign.subscribe("topic", "*");
        invalidInputs.put("subscribeThenAssign", exceptionName(() -> subscribeThenAssign.assign(
            Collections.singleton(new MessageQueue("topic", "broker-a", 0)))));
        DefaultLitePullConsumer assignThenSubscribe = new DefaultLitePullConsumer("assign-then-subscribe");
        assignThenSubscribe.assign(Collections.singleton(new MessageQueue("topic", "broker-a", 0)));
        invalidInputs.put("assignThenSubscribe", exceptionName(() -> assignThenSubscribe.subscribe("topic", "*")));
        DefaultLitePullConsumer invalidSuspendTimeout = new DefaultLitePullConsumer("invalid-suspend-timeout");
        invalidSuspendTimeout.setConsumerTimeoutMillisWhenSuspend(1);
        invalidInputs.put("suspendTimeout", exceptionName(invalidSuspendTimeout::start));
        DefaultLitePullConsumer invalidDefaultGroup = new DefaultLitePullConsumer();
        invalidInputs.put("defaultGroup", exceptionName(invalidDefaultGroup::start));
        DefaultLitePullConsumer invalidConsumeFromWhere = new DefaultLitePullConsumer("invalid-consume-from-where");
        invalidInputs.put("consumeFromWhere", exceptionName(
            () -> invalidConsumeFromWhere.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_MIN_OFFSET)));

        DefaultMQPushConsumer push = new DefaultMQPushConsumer("pop-fixture");
        Map<String, Object> popDefaults = new TreeMap<>();
        popDefaults.put("popThresholdForQueue", push.getPopThresholdForQueue());
        popDefaults.put("popInvisibleTime", push.getPopInvisibleTime());
        popDefaults.put("popBatchNums", push.getPopBatchNums());
        push.setPopThresholdForQueue(17);
        push.setPopInvisibleTime(23_000);
        push.setPopBatchNums(19);
        Map<String, Object> popConfigured = new TreeMap<>();
        popConfigured.put("popThresholdForQueue", push.getPopThresholdForQueue());
        popConfigured.put("popInvisibleTime", push.getPopInvisibleTime());
        popConfigured.put("popBatchNums", push.getPopBatchNums());

        List<MessageExt> emptyMessages = new ArrayList<>();
        PopResult result = new PopResult(PopStatus.FOUND, emptyMessages);
        Map<String, Object> popResultDefaults = new TreeMap<>();
        popResultDefaults.put("status", result.getPopStatus().name());
        popResultDefaults.put("popTime", result.getPopTime());
        popResultDefaults.put("invisibleTime", result.getInvisibleTime());
        popResultDefaults.put("restNum", result.getRestNum());
        popResultDefaults.put("messageCount", result.getMsgFoundList().size());
        result.setPopTime(123L);
        result.setInvisibleTime(456L);
        result.setRestNum(7L);
        Map<String, Object> popResult = new TreeMap<>();
        popResult.put("status", result.getPopStatus().name());
        popResult.put("popTime", result.getPopTime());
        popResult.put("invisibleTime", result.getInvisibleTime());
        popResult.put("restNum", result.getRestNum());
        popResult.put("messageCount", result.getMsgFoundList().size());
        popResult.put("statusNames", Arrays.stream(PopStatus.values()).map(PopStatus::name).collect(Collectors.toList()));

        String[] extraInfo = ExtraInfoUtil.split("17 123 456 2 1 broker-a 4 19");
        Map<String, Object> receipt = new TreeMap<>();
        receipt.put("raw", "17 123 456 2 1 broker-a 4 19");
        receipt.put("checkpointQueueOffset", ExtraInfoUtil.getCkQueueOffset(extraInfo));
        receipt.put("popTime", ExtraInfoUtil.getPopTime(extraInfo));
        receipt.put("invisibleTime", ExtraInfoUtil.getInvisibleTime(extraInfo));
        receipt.put("reviveQueueId", ExtraInfoUtil.getReviveQid(extraInfo));
        receipt.put("retry", ExtraInfoUtil.getRetry(extraInfo));
        receipt.put("brokerName", ExtraInfoUtil.getBrokerName(extraInfo));
        receipt.put("queueId", ExtraInfoUtil.getQueueId(extraInfo));
        receipt.put("queueOffset", ExtraInfoUtil.getQueueOffset(extraInfo));
        Map<String, Object> invalidReceipts = new TreeMap<>();
        invalidReceipts.put("missingFields", exceptionName(() -> ExtraInfoUtil.getQueueOffset(new String[] {"17"})));
        invalidReceipts.put("invalidQueueOffset", exceptionName(() -> ExtraInfoUtil.getQueueOffset(
            new String[] {"17", "123", "456", "2", "1", "broker-a", "4", "bad"})));

        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("sourceCommit", SOURCE_COMMIT);
        fixture.put("litePullDefaults", defaults);
        fixture.put("litePullConfigured", configured);
        fixture.put("litePullApi", liteApi);
        fixture.put("liteConfigApi", liteConfigApi);
        fixture.put("liteConstructors", liteConstructors);
        fixture.put("invalidInputs", invalidInputs);
        fixture.put("heartbeatSubscriptions", heartbeatSubscriptions);
        fixture.put("pushPopDefaults", popDefaults);
        fixture.put("pushPopConfigured", popConfigured);
        fixture.put("popResultDefaults", popResultDefaults);
        fixture.put("popResult", popResult);
        fixture.put("receipt", receipt);
        fixture.put("invalidReceipts", invalidReceipts);
        return JSON.toJSONString(fixture, JSONWriter.Feature.PrettyFormat) + "\n";
    }

    private static String exceptionName(ThrowingAction action) {
        try {
            action.run();
            return "none";
        } catch (Throwable error) {
            String result = error.getClass().getSimpleName();
            if (error.getCause() != null) {
                result += "/" + error.getCause().getClass().getSimpleName();
            }
            return result;
        }
    }

    @FunctionalInterface
    private interface ThrowingAction {
        void run() throws Exception;
    }
}
