package compat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;

/**
 * P10-V05: shows that Java 5.5.1 starts one outstanding Pull for every assigned queue before any
 * target Pull response, then delivers a seeded message on each queue inside the declared window.
 *
 * <p>Rows are tab separated with the {@code P10F} tag and validated by
 * {@code tests/differential/check_p10_fanout.py}. Every timestamp is a process-local monotonic
 * nanosecond value; clocks are never compared across processes.</p>
 */
public final class LivePullFanoutProbe {
    private static final int QUEUES = 16;
    private static final long ADMISSION_WINDOW_NANOS = 5_000_000_000L;
    private static final long DELIVERY_WINDOW_MILLIS = 5_000;

    private LivePullFanoutProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5 || !"probe".equals(args[0])) {
            throw new IllegalArgumentException(
                "usage: LivePullFanoutProbe probe <nameserver> <topic> <consumer-group> <ready-file>");
        }
        probe(args[1], args[2], args[3], java.nio.file.Paths.get(args[4]));
    }

    private static void probe(String namesrvAddress, String topic, String group, Path readyFile)
        throws Exception {
        Recorder recorder = new Recorder(topic, group);

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group, new FanoutRpcHook(recorder));
        consumer.setNamesrvAddr(namesrvAddress);
        consumer.setInstanceName("p10f-consumer");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");
        final Set<String> delivered = ConcurrentHashMap.newKeySet();
        final Map<String, Long> deliveryNanos = new ConcurrentHashMap<>();
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (MessageExt message : messages) {
                String key = new String(message.getBody(), StandardCharsets.UTF_8);
                long now = System.nanoTime();
                if (deliveryNanos.putIfAbsent(key, now) == null) {
                    delivered.add(key);
                    recorder.row("DELIVERY", now, message.getQueueId(), key,
                        message.getReconsumeTimes());
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        DefaultMQProducer producer = new DefaultMQProducer(group + "_producer");
        producer.setNamesrvAddr(namesrvAddress);
        producer.setInstanceName("p10f-producer");
        producer.setSendMessageWithVIPChannel(false);
        producer.setSendMsgTimeout(10_000);

        // The consumer registers the RPCHook on its client instance, so it starts first.
        consumer.start();
        try {
            List<MessageQueue> queues = routeQueues(producer, topic, recorder);

            long admissionDeadline = System.nanoTime() + ADMISSION_WINDOW_NANOS;
            while (true) {
                recorder.checkViolations();
                if (recorder.distinctStartedQueues() >= QUEUES) {
                    long spanNanos = recorder.admissionSpanNanos();
                    if (spanNanos > ADMISSION_WINDOW_NANOS) {
                        throw new IllegalStateException("first Pull starts span "
                            + (spanNanos / 1_000_000L) + " ms, exceeding the admission window");
                    }
                    recorder.row("GATE", System.nanoTime(), spanNanos / 1_000_000L,
                        recorder.distinctStartedQueues());
                    Path parent = readyFile.getParent();
                    if (parent != null) {
                        Files.createDirectories(parent);
                    }
                    Files.write(readyFile, "java-ready\n".getBytes(StandardCharsets.UTF_8));
                    break;
                }
                if (System.nanoTime() >= admissionDeadline) {
                    throw new IllegalStateException("only " + recorder.distinctStartedQueues()
                        + " of " + QUEUES + " queues started a Pull within the admission window");
                }
                Thread.sleep(10);
            }

            producer.start();
            try {
                for (MessageQueue queue : queues) {
                    String key = "p10f-q" + queue.getQueueId();
                    long sendStart = System.nanoTime();
                    recorder.row("SEND_START", sendStart, queue.getQueueId(), key);
                    SendResult result = producer.send(
                        new Message(topic, "P10F", key, key.getBytes(StandardCharsets.UTF_8)), queue);
                    recorder.row("SEND_END", System.nanoTime(), queue.getQueueId(), key,
                        result.getSendStatus(), result.getQueueOffset());
                }

                long deliveryDeadline = System.currentTimeMillis() + DELIVERY_WINDOW_MILLIS;
                while (delivered.size() < QUEUES) {
                    if (System.currentTimeMillis() >= deliveryDeadline) {
                        throw new IllegalStateException("only " + delivered.size() + " of " + QUEUES
                            + " seeded messages were delivered within the delivery window");
                    }
                    Thread.sleep(10);
                }
            } finally {
                producer.shutdown();
            }
        } finally {
            consumer.shutdown();
            recorder.row("SHUTDOWN", System.nanoTime());
        }
    }

    private static List<MessageQueue> routeQueues(DefaultMQProducer producer, String topic,
        Recorder recorder) throws Exception {
        // The publish queue list is the same route inventory the Push consumer subscribes to.
        List<MessageQueue> fetched = producer.fetchPublishMessageQueues(topic);
        Map<Integer, MessageQueue> byId = new TreeMap<>();
        for (MessageQueue queue : fetched) {
            if (byId.put(queue.getQueueId(), queue) != null) {
                throw new IllegalStateException("duplicate route queue " + queue);
            }
            recorder.row("ROUTE", topic, queue.getBrokerName(), queue.getQueueId());
        }
        if (byId.size() != QUEUES) {
            throw new IllegalStateException("expected " + QUEUES + " route queues, got " + byId.keySet());
        }
        return new ArrayList<>(byId.values());
    }

    /** Records Pull request issue and response completion boundaries for the target topic. */
    private static final class FanoutRpcHook implements RPCHook {
        private final Recorder recorder;

        private FanoutRpcHook(Recorder recorder) {
            this.recorder = recorder;
        }

        @Override
        public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
            recorder.pullStart(remoteAddr, request);
        }

        @Override
        public void doAfterResponse(String remoteAddr, RemotingCommand request,
            RemotingCommand response) {
            recorder.pullEnd(remoteAddr, request, response);
        }
    }

    /** Shared evidence state; every timestamp is a process-local monotonic nanosecond value. */
    private static final class Recorder {
        private final String topic;
        private final String group;
        private final Object lock = new Object();
        private final Map<Integer, Long> firstStart = new TreeMap<>();
        private final Map<Integer, Long> lastStart = new TreeMap<>();
        private long earliestEndNanos = Long.MAX_VALUE;
        private String violation;

        private Recorder(String topic, String group) {
            this.topic = topic;
            this.group = group;
        }

        void pullStart(String remoteAddr, RemotingCommand request) {
            PullMessageRequestHeader header = targetPull(request);
            if (header == null) {
                return;
            }
            long now = System.nanoTime();
            int queueId = header.getQueueId();
            synchronized (lock) {
                if (earliestEndNanos != Long.MAX_VALUE && violation == null) {
                    violation = "a target Pull response completed before every assigned queue had an "
                        + "outstanding Pull";
                }
                if (!firstStart.containsKey(queueId)) {
                    firstStart.put(queueId, now);
                }
                lastStart.put(queueId, now);
            }
            row(now, "PULL_START", queueId, request.getOpaque(), header.getSuspendTimeoutMillis(),
                remoteAddr);
        }

        void pullEnd(String remoteAddr, RemotingCommand request, RemotingCommand response) {
            PullMessageRequestHeader header = targetPull(request);
            if (header == null) {
                return;
            }
            long now = System.nanoTime();
            int code = response == null ? -1 : response.getCode();
            synchronized (lock) {
                earliestEndNanos = Math.min(earliestEndNanos, now);
            }
            row(now, "PULL_END", header.getQueueId(), request.getOpaque(), code);
        }

        /** Return the target-topic Pull header, or null when the command is not one of ours. */
        private PullMessageRequestHeader targetPull(RemotingCommand request) {
            if (request.getCode() != RequestCode.PULL_MESSAGE) {
                return null;
            }
            Object attached = request.readCustomHeader();
            if (attached instanceof PullMessageRequestHeader) {
                PullMessageRequestHeader header = (PullMessageRequestHeader) attached;
                if (topic.equals(header.getTopic()) && group.equals(header.getConsumerGroup())) {
                    return header;
                }
                return null;
            }
            Map<String, String> fields = request.getExtFields();
            if (fields == null || !topic.equals(fields.get("topic"))
                || !group.equals(fields.get("consumerGroup"))) {
                return null;
            }
            PullMessageRequestHeader header = new PullMessageRequestHeader();
            header.setTopic(topic);
            header.setConsumerGroup(group);
            header.setQueueId(Integer.parseInt(fields.getOrDefault("queueId", "-1")));
            header.setSuspendTimeoutMillis(
                Long.parseLong(fields.getOrDefault("suspendTimeoutMillis", "0")));
            return header;
        }

        int distinctStartedQueues() {
            synchronized (lock) {
                return firstStart.size();
            }
        }

        long admissionSpanNanos() {
            synchronized (lock) {
                long earliest = Long.MAX_VALUE;
                long latest = Long.MIN_VALUE;
                for (Long value : firstStart.values()) {
                    earliest = Math.min(earliest, value);
                }
                for (Long value : lastStart.values()) {
                    latest = Math.max(latest, value);
                }
                return firstStart.isEmpty() ? 0 : latest - earliest;
            }
        }

        void checkViolations() {
            String message;
            synchronized (lock) {
                message = violation;
            }
            if (message != null) {
                throw new IllegalStateException(message);
            }
        }

        /** Emit a row whose first value after the language tag is not a timestamp. */
        void row(String kind, Object... values) {
            StringBuilder line = new StringBuilder("P10F\t").append(kind).append("\tjava");
            for (Object value : values) {
                line.append('\t').append(value);
            }
            System.out.println(line);
        }

        /** Emit a row whose first value after the language tag is a monotonic timestamp. */
        void row(long timestamp, String kind, Object... values) {
            StringBuilder line = new StringBuilder("P10F\t").append(kind).append("\tjava\t")
                .append(timestamp);
            for (Object value : values) {
                line.append('\t').append(value);
            }
            System.out.println(line);
        }
    }
}
