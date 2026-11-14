package compat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

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

    /** Admission is measured from the first target Pull, never from client setup. */
    private static final long ADMISSION_WINDOW_NANOS = 5_000_000_000L;

    /** Route refresh, rebalance and the first Pull get their own bounded wait. */
    private static final long STARTUP_WINDOW_NANOS = 30_000_000_000L;

    /** Slack past the admission window so a hopeless barrier reports instead of hanging. */
    private static final long ADMISSION_POLL_SLACK_NANOS = 2_000_000_000L;

    private static final long DELIVERY_WINDOW_MILLIS = 5_000;

    private LivePullFanoutProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5 || !"probe".equals(args[0])) {
            throw new IllegalArgumentException(
                "usage: LivePullFanoutProbe probe <nameserver> <topic> <consumer-group> <ready-file>");
        }
        probe(args[1], args[2], args[3], java.nio.file.Paths.get(args[4]));
    }

    static void probe(String namesrvAddress, String topic, String group, Path readyFile)
        throws Exception {
        Recorder recorder = new Recorder(topic, group, QUEUES, ADMISSION_WINDOW_NANOS,
            line -> System.out.println(line));

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group, new FanoutRpcHook(recorder));
        consumer.setNamesrvAddr(namesrvAddress);
        consumer.setInstanceName("p10f-consumer");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");
        // Every listener callback is evidence; de-duplication only decides when to stop waiting.
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (MessageExt message : messages) {
                String key = new String(message.getBody(), StandardCharsets.UTF_8);
                recorder.recordDelivery(message.getQueueId(), key,
                    message.getReconsumeTimes());
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        DefaultMQProducer producer = new DefaultMQProducer(group + "_producer");
        producer.setNamesrvAddr(namesrvAddress);
        producer.setInstanceName("p10f-producer");
        producer.setSendMessageWithVIPChannel(false);
        producer.setSendMsgTimeout(10_000);

        Lifecycle lifecycle = new Lifecycle();
        try {
            // The consumer carries the RPCHook, and {@code fetchPublishMessageQueues} requires a
            // RUNNING producer, so both clients are acquired before any route lookup or send.
            acquire(lifecycle,
                client(consumer::start, consumer::shutdown),
                client(producer::start, producer::shutdown),
                () -> measure(topic, readyFile, recorder, producer));
        } finally {
            recorder.row("SHUTDOWN", System.nanoTime());
        }
    }

    private static void measure(String topic, Path readyFile, Recorder recorder,
        DefaultMQProducer producer) throws Exception {
        List<MessageQueue> queues = routeQueues(producer, topic, recorder);

        // Startup gets its own bounded wait; the admission window is measured by the recorder from
        // the first target Pull start.
        long startupDeadline = System.nanoTime() + STARTUP_WINDOW_NANOS;
        while (recorder.firstPullStartNanos() == null) {
            recorder.checkViolations();
            if (System.nanoTime() >= startupDeadline) {
                throw new IllegalStateException("no target-topic Pull was issued within the "
                    + (STARTUP_WINDOW_NANOS / 1_000_000L) + " ms startup window");
            }
            Thread.sleep(10);
        }

        long firstPull = recorder.firstPullStartNanos();
        long waitDeadline = firstPull + ADMISSION_WINDOW_NANOS + ADMISSION_POLL_SLACK_NANOS;
        while (true) {
            recorder.checkViolations();
            if (recorder.tryLatchGate() != null) {
                break;
            }
            recorder.checkViolations();
            if (System.nanoTime() >= waitDeadline) {
                throw new IllegalStateException("the admission gate was never reached: "
                    + recorder.describeState());
            }
            Thread.sleep(10);
        }

        Path parent = readyFile.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(readyFile, "java-ready\n".getBytes(StandardCharsets.UTF_8));

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
        while (recorder.deliveredCount() < QUEUES) {
            recorder.checkViolations();
            if (System.currentTimeMillis() >= deliveryDeadline) {
                throw new IllegalStateException("only " + recorder.deliveredCount() + " of " + QUEUES
                    + " seeded messages were delivered within the delivery window");
            }
            Thread.sleep(10);
        }
    }

    private static List<MessageQueue> routeQueues(DefaultMQProducer producer, String topic,
        Recorder recorder) throws Exception {
        // The publish queue list is the same route inventory the Push consumer subscribes to. The
        // pinned client rejects this call unless the producer is RUNNING, hence the acquisition
        // order in {@link #probe}.
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

    /** A client acquisition that the probe must close once it started. */
    interface Client {
        void start() throws Exception;

        void shutdown();
    }

    interface ThrowingRunnable {
        void run() throws Exception;
    }

    interface Body {
        void run() throws Exception;
    }

    static Client client(ThrowingRunnable start, Runnable shutdown) {
        return new Client() {
            @Override
            public void start() throws Exception {
                start.run();
            }

            @Override
            public void shutdown() {
                shutdown.run();
            }
        };
    }

    /** Records every client it started and closes them in reverse order, even on partial failure. */
    static final class Lifecycle implements AutoCloseable {
        private final List<Client> started = new ArrayList<>();

        void start(Client client) throws Exception {
            client.start();
            started.add(client);
        }

        @Override
        public void close() {
            for (int index = started.size() - 1; index >= 0; index--) {
                started.get(index).shutdown();
            }
            started.clear();
        }
    }

    /**
     * Acquires both clients and guarantees that every client which started is closed, whatever the
     * measured body does. The consumer starts first because its instance carries the RPCHook; the
     * producer must be RUNNING before the route lookup or any send.
     */
    static void acquire(Lifecycle lifecycle, Client consumer, Client producer, Body body)
        throws Exception {
        try {
            lifecycle.start(consumer);
            lifecycle.start(producer);
            body.run();
        } finally {
            lifecycle.close();
        }
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

    /** Where evidence rows go; the fixture captures them instead of printing. */
    interface RowSink {
        void emit(String line);
    }

    /** Immutable admission-barrier snapshot, so callers cannot observe a half-updated gate. */
    static final class GateSnapshot {
        private final long atNanos;
        private final long spanMillis;
        private final int distinctQueues;

        private GateSnapshot(long atNanos, long spanMillis, int distinctQueues) {
            this.atNanos = atNanos;
            this.spanMillis = spanMillis;
            this.distinctQueues = distinctQueues;
        }

        long atNanos() {
            return atNanos;
        }

        long spanMillis() {
            return spanMillis;
        }

        int distinctQueues() {
            return distinctQueues;
        }
    }

    /** Shared evidence state; every timestamp is a process-local monotonic nanosecond value. */
    /** Monotonic probe clock. Fixtures inject a deterministic one to drive event order. */
    interface ProbeClock {
        long nanoTime();
    }

    static final class Recorder {
        private final String topic;
        private final String group;
        private final int expectedQueues;
        private final long admissionWindowNanos;
        private final RowSink sink;
        private final ProbeClock clock;

        /** Guard for recorder state; package-private so a fixture can hold it deterministically. */
        final Object lock = new Object();

        /** queueId to the opaque of that queue's currently outstanding Pull, if any. */
        private final Map<Integer, Integer> activePulls = new TreeMap<>();

        /** queueId to the first time a target Pull was issued for it. */
        private final Map<Integer, Long> firstStart = new TreeMap<>();

        /** Distinct seeded keys seen so far; only used to decide when delivery waiting ends. */
        private final Set<String> deliveredKeys = new HashSet<>();

        private int completedResponses;
        private boolean gateLatched;
        private GateSnapshot gate;
        private String violation;

        Recorder(String topic, String group, int expectedQueues, long admissionWindowNanos,
            RowSink sink) {
            this(topic, group, expectedQueues, admissionWindowNanos, sink, System::nanoTime);
        }

        Recorder(String topic, String group, int expectedQueues, long admissionWindowNanos,
            RowSink sink, ProbeClock clock) {
            this.topic = topic;
            this.group = group;
            this.expectedQueues = expectedQueues;
            this.admissionWindowNanos = admissionWindowNanos;
            this.sink = sink;
            this.clock = clock;
        }

        void pullStart(String remoteAddr, RemotingCommand request) {
            PullMessageRequestHeader header = targetPull(request);
            if (header == null) {
                return;
            }
            int queueId = header.getQueueId();
            // The clock is sampled inside the state lock, so two concurrent admissions cannot
            // record their transitions in one order and their timestamps in another.
            long at;
            GateSnapshot latched;
            synchronized (lock) {
                at = clock.nanoTime();
                firstStart.putIfAbsent(queueId, at);
                Integer outstanding = activePulls.putIfAbsent(queueId, request.getOpaque());
                if (outstanding != null && violation == null) {
                    violation = "queue " + queueId + " admitted a second Pull while opaque "
                        + outstanding + " was still outstanding";
                }
                // Latching inside the same critical section as the start means a response can never
                // slip between "the last queue became active" and "the barrier is recorded".
                latched = attemptLatchLocked(at);
            }
            // Rows are emitted after the lock is released but always carry the locked sample.
            row(at, "PULL_START", queueId, request.getOpaque(), header.getSuspendTimeoutMillis(),
                remoteAddr);
            if (latched != null) {
                row(latched.atNanos, "GATE", latched.spanMillis, latched.distinctQueues);
            }
        }

        void pullEnd(String remoteAddr, RemotingCommand request, RemotingCommand response) {
            PullMessageRequestHeader header = targetPull(request);
            if (header == null) {
                return;
            }
            int code = response == null ? -1 : response.getCode();
            long at;
            synchronized (lock) {
                at = clock.nanoTime();
                Integer outstanding = activePulls.get(header.getQueueId());
                if (outstanding == null || outstanding != request.getOpaque()) {
                    if (violation == null) {
                        violation = "a target Pull response for queue " + header.getQueueId()
                            + " / opaque " + request.getOpaque()
                            + " did not match an outstanding Pull";
                    }
                } else {
                    activePulls.remove(header.getQueueId());
                }
                completedResponses++;
                // A premature response is recorded when it happens, not on some later start.
                if (!gateLatched && violation == null) {
                    violation = "a target Pull response completed before every assigned queue had "
                        + "an outstanding Pull";
                }
            }
            row(at, "PULL_END", header.getQueueId(), request.getOpaque(), code);
        }

        /**
         * Latch the admission barrier atomically. The gate requires exactly the expected number of
         * distinct queues to be active, no target response completed yet, and an elapsed span from
         * the first target Pull within the window. Returns the snapshot once latched, otherwise
         * null; the GATE row is emitted exactly once.
         */
        GateSnapshot tryLatchGate() {
            GateSnapshot latched;
            synchronized (lock) {
                if (gateLatched) {
                    return gate;
                }
                // The clock sample belongs to the same critical section as the latch itself.
                latched = attemptLatchLocked(clock.nanoTime());
            }
            if (latched != null) {
                row(latched.atNanos, "GATE", latched.spanMillis, latched.distinctQueues);
            }
            return latched;
        }

        /** Latch the barrier; call only while holding {@code lock}. Returns null when unchanged. */
        private GateSnapshot attemptLatchLocked(long now) {
            if (gateLatched || violation != null || completedResponses > 0
                || firstStart.isEmpty()) {
                return null;
            }
            // The admission-window origin is the earliest recorded start, not whichever call
            // happened to take the lock first.
            long elapsed = now - earliestStartLocked();
            if (elapsed > admissionWindowNanos) {
                violation = "only " + activePulls.size() + " of " + expectedQueues
                    + " assigned queues had an outstanding Pull within the "
                    + (admissionWindowNanos / 1_000_000L) + " ms admission window";
                return null;
            }
            if (activePulls.size() != expectedQueues) {
                return null;
            }
            long earliest = Long.MAX_VALUE;
            long latest = Long.MIN_VALUE;
            for (Long value : firstStart.values()) {
                earliest = Math.min(earliest, value);
                latest = Math.max(latest, value);
            }
            // Every timestamp comes from the same lock-held clock sample as this latch, so a
            // covered start can never carry a later stamp than the gate that claims it.
            if (now < latest) {
                throw new IllegalStateException("probe clock moved backwards inside the recorder");
            }
            gate = new GateSnapshot(now, (latest - earliest) / 1_000_000L, firstStart.size());
            gateLatched = true;
            return gate;
        }

        /** Emit every delivery row first; de-duplication only tracks progress. */
        boolean recordDelivery(int queueId, String key, int reconsumeTimes) {
            long at;
            boolean fresh;
            synchronized (lock) {
                at = clock.nanoTime();
                fresh = deliveredKeys.add(key);
            }
            row(at, "DELIVERY", queueId, key, reconsumeTimes);
            return fresh;
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

        Long firstPullStartNanos() {
            synchronized (lock) {
                return earliestStartLocked();
            }
        }

        /** Earliest recorded target Pull start; call only while holding {@code lock}. */
        private Long earliestStartLocked() {
            Long earliest = null;
            for (Long value : firstStart.values()) {
                if (earliest == null || value < earliest) {
                    earliest = value;
                }
            }
            return earliest;
        }

        int activePullCount() {
            synchronized (lock) {
                return activePulls.size();
            }
        }

        boolean gateLatched() {
            synchronized (lock) {
                return gateLatched;
            }
        }

        GateSnapshot gateSnapshot() {
            synchronized (lock) {
                return gate;
            }
        }

        int deliveredCount() {
            synchronized (lock) {
                return deliveredKeys.size();
            }
        }

        String describeState() {
            synchronized (lock) {
                Long first = earliestStartLocked();
                long elapsed = first == null ? 0 : clock.nanoTime() - first;
                return "active=" + activePulls.size() + "/" + expectedQueues
                    + " distinctStarted=" + firstStart.size()
                    + " responses=" + completedResponses
                    + " sinceFirstPullMs=" + (elapsed / 1_000_000L);
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
            sink.emit(line.toString());
        }

        /** Emit a row whose first value after the language tag is a monotonic timestamp. */
        void row(long timestamp, String kind, Object... values) {
            StringBuilder line = new StringBuilder("P10F\t").append(kind).append("\tjava\t")
                .append(timestamp);
            for (Object value : values) {
                line.append('\t').append(value);
            }
            sink.emit(line.toString());
        }
    }
}
