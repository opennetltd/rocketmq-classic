package compat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.RebalancePushImpl;
import org.apache.rocketmq.common.message.MessageQueue;

/** Invoked Java 5.5.1 public orderly-consumer defaults and status names. */
public final class OrderlyConsumerFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private OrderlyConsumerFixtures() {
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
                throw new IllegalStateException("Java orderly fixture drift: " + fixture);
            }
        } else {
            Files.createDirectories(fixture.getParent());
            Files.writeString(fixture, expected);
        }
    }

    private static String render() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("orderly-fixture");
        MessageQueue queue = new MessageQueue("orders", "broker-a", 3);
        ConsumeOrderlyContext context = new ConsumeOrderlyContext(queue);
        int defaultMaxReconsumeTimes = consumer.getMaxReconsumeTimes();
        long defaultSuspendTime = consumer.getSuspendCurrentQueueTimeMillis();
        int defaultPullBatchSize = consumer.getPullBatchSize();
        int defaultPullBatchSizeInBytes = consumer.getPullBatchSizeInBytes();
        boolean defaultAutoCommit = context.isAutoCommit();
        long defaultContextSuspendTime = context.getSuspendCurrentQueueTimeMillis();
        String statuses = Arrays.stream(ConsumeOrderlyStatus.values())
            .map(ConsumeOrderlyStatus::name)
            .map(value -> "\"" + value + "\"")
            .collect(Collectors.joining(", "));
        consumer.setMaxReconsumeTimes(7);
        consumer.setSuspendCurrentQueueTimeMillis(4321);
        consumer.setPullBatchSize(7);
        consumer.setPullBatchSizeInBytes(12345);
        context.setAutoCommit(false);
        context.setSuspendCurrentQueueTimeMillis(7654);
        return "{\n"
            + "  \"sourceCommit\": \"" + SOURCE_COMMIT + "\",\n"
            + "  \"pushConsumerDefaults\": {\n"
            + "    \"maxReconsumeTimes\": " + defaultMaxReconsumeTimes + ",\n"
            + "    \"suspendCurrentQueueTimeMillis\": " + defaultSuspendTime + ",\n"
            + "    \"pullBatchSize\": " + defaultPullBatchSize + ",\n"
            + "    \"pullBatchSizeInBytes\": " + defaultPullBatchSizeInBytes + ",\n"
            + "    \"consumeTimeoutMinutes\": " + consumer.getConsumeTimeout() + ",\n"
            + "    \"consumeMessageBatchMaxSize\": " + consumer.getConsumeMessageBatchMaxSize() + "\n"
            + "  },\n"
            + "  \"configuredPublicSettings\": {\n"
            + "    \"maxReconsumeTimes\": " + consumer.getMaxReconsumeTimes() + ",\n"
            + "    \"suspendCurrentQueueTimeMillis\": " + consumer.getSuspendCurrentQueueTimeMillis() + ",\n"
            + "    \"pullBatchSize\": " + consumer.getPullBatchSize() + ",\n"
            + "    \"pullBatchSizeInBytes\": " + consumer.getPullBatchSizeInBytes() + "\n"
            + "  },\n"
            + "  \"orderlyContextDefaults\": {\n"
            + "    \"autoCommit\": " + defaultAutoCommit + ",\n"
            + "    \"suspendCurrentQueueTimeMillis\": " + defaultContextSuspendTime + ",\n"
            + "    \"messageQueue\": {\n"
            + "      \"topic\": \"" + context.getMessageQueue().getTopic() + "\",\n"
            + "      \"brokerName\": \"" + context.getMessageQueue().getBrokerName() + "\",\n"
            + "      \"queueId\": " + context.getMessageQueue().getQueueId() + "\n"
            + "    }\n"
            + "  },\n"
            + "  \"mutatedContext\": {\n"
            + "    \"autoCommit\": " + context.isAutoCommit() + ",\n"
            + "    \"suspendCurrentQueueTimeMillis\": " + context.getSuspendCurrentQueueTimeMillis() + "\n"
            + "  },\n"
            + "  \"orderlyStatuses\": [" + statuses + "],\n"
            + "  \"lockConstants\": {\n"
            + "    \"maxLiveTimeMillis\": " + ProcessQueue.REBALANCE_LOCK_MAX_LIVE_TIME + ",\n"
            + "    \"intervalMillis\": " + ProcessQueue.REBALANCE_LOCK_INTERVAL + "\n"
            + "  },\n"
            + "  \"orderlyRuntimeProperties\": {\n"
            + "    \"unlockDelayTimeMillis\": "
            + staticLong(RebalancePushImpl.class, "UNLOCK_DELAY_TIME_MILLS") + ",\n"
            + "    \"maxContinuousConsumeMillis\": "
            + staticLong(ConsumeMessageOrderlyService.class, "MAX_TIME_CONSUME_CONTINUOUSLY") + "\n"
            + "  }\n"
            + "}\n";
    }

    private static long staticLong(Class<?> type, String name) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.getLong(null);
    }
}
