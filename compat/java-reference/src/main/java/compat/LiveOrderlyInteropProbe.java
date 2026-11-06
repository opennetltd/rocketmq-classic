package compat;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

/** Holds a Java orderly consumer behind a callback gate during mixed-client rebalance. */
public final class LiveOrderlyInteropProbe {
    private static final long CALLBACK_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(120);

    private LiveOrderlyInteropProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 6 && "seed".equals(args[0])) {
            seed(args[1], args[2], args[3], Integer.parseInt(args[4]), args[5]);
            return;
        }
        if (args.length == 9 && "consumer".equals(args[0])) {
            consume(args[1], args[2], args[3], Path.of(args[4]), Path.of(args[5]),
                Path.of(args[6]), Path.of(args[7]), Path.of(args[8]));
            // exec:java shares Maven's JVM; terminate it after a successful orderly shutdown
            // so RocketMQ's idle scheduled worker cannot keep the isolated probe alive.
            System.exit(0);
            return;
        }
        throw new IllegalArgumentException(
            "usage: seed <nameserver> <topic> <broker-name> <queue-id> <body> | "
                + "consumer <nameserver> <topic> <group> <ownership-file> <ready-file> "
                + "<stop-file> <marker-dir> <release-file>");
    }

    private static void seed(
        String nameserver, String topic, String brokerName, int queueId, String body
    ) throws Exception {
        var producer = new org.apache.rocketmq.client.producer.DefaultMQProducer(
            "compat_p12_orderly_seed");
        producer.setNamesrvAddr(nameserver);
        producer.setVipChannelEnabled(false);
        producer.start();
        try {
            var message = new Message(topic, body.getBytes(StandardCharsets.UTF_8));
            var result = producer.send(message, new MessageQueue(topic, brokerName, queueId));
            if (result == null || !"SEND_OK".equals(result.getSendStatus().name())) {
                throw new IllegalStateException("Java orderly seed did not return SEND_OK: " + result);
            }
            System.out.printf("P12\tPRODUCED\t%s\t%s\t%d\t%d%n", body,
                result.getMessageQueue().getBrokerName(), result.getMessageQueue().getQueueId(),
                result.getQueueOffset());
        } finally {
            producer.shutdown();
        }
    }

    private static void consume(
        String nameserver, String topic, String group, Path ownershipFile, Path readyFile,
        Path stopFile, Path markerDirectory, Path releaseFile
    ) throws Exception {
        Files.createDirectories(markerDirectory);
        var consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr(nameserver);
        consumer.setVipChannelEnabled(false);
        consumer.setInstanceName("z-p12-java");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setConsumeThreadMin(2);
        consumer.setConsumeThreadMax(4);
        consumer.registerMessageListener((MessageListenerOrderly) (messages, context) -> {
            for (var message : messages) {
                String body = new String(message.getBody(), StandardCharsets.UTF_8);
                String broker = message.getBrokerName();
                int queueId = message.getQueueId();
                long offset = message.getQueueOffset();
                System.out.printf("P12\tCALLBACK\tjava\t%s\t%s\t%d\t%d\tBEGIN%n",
                    body, broker, queueId, offset);
                Path marker = markerDirectory.resolve(
                    "java-" + broker + "-" + queueId + "-" + offset + "-" + body + ".seen");
                try {
                    Files.writeString(marker, "begin\n", StandardCharsets.UTF_8);
                } catch (java.io.IOException error) {
                    throw new IllegalStateException("failed to write the P12 callback marker", error);
                }
                if (body.startsWith("p12-slow")) {
                    long deadline = System.nanoTime() + CALLBACK_TIMEOUT_NANOS;
                    while (!Files.exists(releaseFile)) {
                        if (System.nanoTime() >= deadline) {
                            throw new IllegalStateException("timed out waiting for the P12 callback release gate");
                        }
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                    }
                }
                System.out.printf("P12\tCALLBACK\tjava\t%s\t%s\t%d\t%d\tEND%n",
                    body, broker, queueId, offset);
                try {
                    Files.writeString(marker, "end\n", StandardCharsets.UTF_8);
                } catch (java.io.IOException error) {
                    throw new IllegalStateException("failed to close the P12 callback marker", error);
                }
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });
        consumer.subscribe(topic, "*");
        consumer.start();
        Files.writeString(readyFile, "ready\n", StandardCharsets.UTF_8);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(150);
        try {
            while (!Files.exists(stopFile)) {
                List<String> queues = ownedQueues(consumer, topic);
                writeOwnership(ownershipFile, queues);
                System.out.printf("P12\tSNAPSHOT\tjava\t%d\t%s%n", queues.size(),
                    queues.isEmpty() ? "-" : String.join(",", queues));
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("timed out waiting for the P12 stop gate");
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(250));
            }
        } finally {
            consumer.shutdown();
            writeOwnership(ownershipFile, Collections.emptyList());
            Files.writeString(readyFile.resolveSibling("java-exited"), "exited\n",
                StandardCharsets.UTF_8);
            System.out.println("P12\tSTOPPED\tjava");
        }
    }

    private static List<String> ownedQueues(DefaultMQPushConsumer consumer, String topic)
        throws Exception {
        Object implementation = consumer.getDefaultMQPushConsumerImpl();
        Object rebalance = invokeNoArgument(implementation, "getRebalanceImpl");
        Object rawTable = invokeNoArgument(rebalance, "getProcessQueueTable");
        if (!(rawTable instanceof Map<?, ?>)) {
            throw new IllegalStateException("Java orderly rebalance table is not a Map");
        }
        List<String> queues = new ArrayList<>();
        for (Object rawQueue : ((Map<?, ?>) rawTable).keySet()) {
            if (!(rawQueue instanceof MessageQueue)) {
                throw new IllegalStateException("Java orderly rebalance table has a non-queue key");
            }
            MessageQueue queue = (MessageQueue) rawQueue;
            if (topic.equals(queue.getTopic())) {
                Object processQueue = ((Map<?, ?>) rawTable).get(rawQueue);
                Object locked = invokeNoArgument(processQueue, "isLocked");
                queues.add(queue.getTopic() + "|" + queue.getBrokerName() + "|"
                    + queue.getQueueId() + "|" + locked);
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

    private static void writeOwnership(Path path, List<String> queues) throws Exception {
        Path temporary = path.resolveSibling(path.getFileName() + ".tmp");
        String contents = queues.isEmpty() ? "" : String.join("\n", queues) + "\n";
        Files.writeString(temporary, contents, StandardCharsets.UTF_8);
        try {
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        } catch (java.nio.file.AtomicMoveNotSupportedException ignored) {
            Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
