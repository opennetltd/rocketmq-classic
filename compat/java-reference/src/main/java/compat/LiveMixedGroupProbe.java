package compat;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;

/** Holds a pinned Java Push consumer and publishes its live queue ownership. */
public final class LiveMixedGroupProbe {
    private static final long TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(180);

    private LiveMixedGroupProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException(
                "usage: LiveMixedGroupProbe <nameserver-address> <topic> <group> "
                    + "<ownership-file> <stop-file>");
        }
        run(args[0], args[1], args[2], Path.of(args[3]), Path.of(args[4]));
    }

    private static void run(
        String nameserverAddress, String topic, String group, Path ownershipFile, Path stopFile
    ) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr(nameserverAddress);
        consumer.setVipChannelEnabled(false);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (var message : messages) {
                System.out.printf("P11\tDELIVERY\tjava\t%s\t%d%n",
                    new String(message.getBody(), StandardCharsets.UTF_8),
                    message.getReconsumeTimes());
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.subscribe(topic, "*");
        consumer.start();

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        try {
            while (!Files.exists(stopFile)) {
                List<String> queues = ownedQueues(consumer, topic);
                writeOwnership(ownershipFile, queues);
                System.out.printf("P11\tSNAPSHOT\tjava\t%d\t%s%n", queues.size(),
                    queues.isEmpty() ? "-" : String.join(",", queues));
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("timed out waiting for the P11 stop gate");
                }
                Thread.sleep(500);
            }
        } finally {
            consumer.shutdown();
            writeOwnership(ownershipFile, Collections.emptyList());
            System.out.println("P11\tSTOPPED\tjava");
        }
    }

    private static List<String> ownedQueues(DefaultMQPushConsumer consumer, String topic)
        throws Exception {
        Object implementation = consumer.getDefaultMQPushConsumerImpl();
        Object rebalance = invokeNoArgument(implementation, "getRebalanceImpl");
        Object rawTable = invokeNoArgument(rebalance, "getProcessQueueTable");
        if (!(rawTable instanceof Map<?, ?>)) {
            throw new IllegalStateException("Java Push rebalance table is not a Map");
        }
        List<String> queues = new ArrayList<>();
        for (Object rawQueue : ((Map<?, ?>) rawTable).keySet()) {
            if (!(rawQueue instanceof MessageQueue)) {
                throw new IllegalStateException("Java Push rebalance table contains a non-queue key");
            }
            MessageQueue queue = (MessageQueue) rawQueue;
            if (topic.equals(queue.getTopic())) {
                queues.add(queueKey(queue));
            }
        }
        Collections.sort(queues);
        return queues;
    }

    private static Object invokeNoArgument(Object target, String name) throws Exception {
        Class<?> current = target.getClass();
        while (current != null) {
            try {
                Method method = current.getDeclaredMethod(name);
                method.setAccessible(true);
                return method.invoke(target);
            } catch (NoSuchMethodException ignored) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchMethodException(target.getClass().getName() + "." + name);
    }

    private static String queueKey(MessageQueue queue) {
        return queue.getTopic() + "|" + queue.getBrokerName() + "|" + queue.getQueueId();
    }

    private static void writeOwnership(Path path, List<String> queues) throws Exception {
        Path temporary = path.resolveSibling(path.getFileName() + ".tmp");
        String contents = queues.isEmpty() ? "" : String.join("\n", queues) + "\n";
        Files.writeString(temporary, contents, StandardCharsets.UTF_8);
        try {
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
