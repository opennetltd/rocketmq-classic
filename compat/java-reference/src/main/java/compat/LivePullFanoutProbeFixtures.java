package compat;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

/**
 * Offline checks for {@link LivePullFanoutProbe}: the pinned 5.5.1 producer API requires a RUNNING
 * producer before a route lookup, the probe's acquisition seam closes every client it started, and
 * the shared evidence recorder keeps one active Pull per queue and latches the admission barrier
 * only while no target response has completed.
 *
 * <p>No Broker or Name Server is contacted: the pinned API rejects the administrative call on state
 * before any network I/O, and the lifecycle seam uses stub clients.</p>
 */
public final class LivePullFanoutProbeFixtures {
    private static final String TOPIC = "p10f-fixture-topic";
    private static final String GROUP = "p10f-fixture-group";

    private LivePullFanoutProbeFixtures() {}

    public static void main(String[] args) {
        List<String> failures = new ArrayList<>();
        int checks = 0;

        checks += pinnedApiRejectsRouteLookupBeforeProducerStart(failures);
        checks += acquireClosesEveryStartedClient(failures);
        checks += recorderFlagsResponseBeforeBarrier(failures);
        checks += recorderFlagsDuplicateActiveStart(failures);
        checks += recorderAllowsFollowUpAfterLatchedGate(failures);
        checks += recorderLogsEveryDeliveryCallback(failures);

        if (!failures.isEmpty()) {
            for (String failure : failures) {
                System.err.println("FAIL " + failure);
            }
            System.err.println("LivePullFanoutProbe fixtures FAIL (" + failures.size()
                + " of " + checks + " check(s))");
            System.exit(1);
        }
        System.out.println("LivePullFanoutProbe fixtures PASS (" + checks + " check(s))");
    }

    /**
     * P1: {@code fetchPublishMessageQueues} is the probe's route inventory, and the pinned client
     * rejects it unless the producer is RUNNING. That is why both clients are acquired before any
     * lookup or send.
     */
    private static int pinnedApiRejectsRouteLookupBeforeProducerStart(List<String> failures) {
        DefaultMQProducer producer = new DefaultMQProducer(GROUP + "_producer");
        producer.setInstanceName("p10f-fixture-producer");
        try {
            producer.fetchPublishMessageQueues(TOPIC);
            failures.add("pinned API: fetchPublishMessageQueues succeeded on a non-RUNNING producer");
            return 1;
        } catch (MQClientException error) {
            String message = error.getMessage() == null ? "" : error.getMessage();
            if (!message.contains("service state not OK")) {
                failures.add("pinned API: unexpected rejection for a non-RUNNING producer: " + message);
            }
            return 1;
        } finally {
            producer.shutdown();
        }
    }

    /** The acquisition seam must close exactly the clients that started, in reverse order. */
    private static int acquireClosesEveryStartedClient(List<String> failures) {
        List<String> events = new ArrayList<>();
        LivePullFanoutProbe.Lifecycle lifecycle = new LivePullFanoutProbe.Lifecycle();
        try {
            LivePullFanoutProbe.acquire(lifecycle,
                stubClient(events, "consumer", true),
                stubClient(events, "producer", true),
                () -> events.add("body"));
        } catch (Exception error) {
            failures.add("acquisition: healthy run threw " + error);
        }
        int checks = 0;
        checks += expect(events, List.of("consumer.start", "producer.start", "body",
            "producer.shutdown", "consumer.shutdown"), "acquisition: order/cleanup", failures);

        events.clear();
        lifecycle = new LivePullFanoutProbe.Lifecycle();
        try {
            LivePullFanoutProbe.acquire(lifecycle,
                stubClient(events, "consumer", true),
                stubClient(events, "producer", false),
                () -> events.add("body"));
            failures.add("acquisition: a failing producer start was not reported");
        } catch (Exception expected) {
            // The consumer still started, so it must be the only client closed.
        }
        checks += expect(events, List.of("consumer.start", "producer.start-failed",
            "consumer.shutdown"), "partial acquisition cleanup", failures);

        events.clear();
        lifecycle = new LivePullFanoutProbe.Lifecycle();
        try {
            LivePullFanoutProbe.acquire(lifecycle,
                stubClient(events, "consumer", true),
                stubClient(events, "producer", true),
                () -> {
                    events.add("body");
                    throw new IllegalStateException("measured body failed");
                });
            failures.add("acquisition: a failing measured body was not reported");
        } catch (Exception expected) {
            if (!(expected instanceof IllegalStateException)) {
                failures.add("acquisition: unexpected acquisition failure " + expected);
            }
            // Both clients started, so both must be closed.
        }
        checks += expect(events, List.of("consumer.start", "producer.start", "body",
            "producer.shutdown", "consumer.shutdown"), "acquisition cleanup after body failure",
            failures);
        return checks;
    }

    private static LivePullFanoutProbe.Client stubClient(List<String> events, String name,
        boolean starts) {
        return new LivePullFanoutProbe.Client() {
            @Override
            public void start() throws Exception {
                if (!starts) {
                    events.add(name + ".start-failed");
                    throw new IllegalStateException(name + " failed to start");
                }
                events.add(name + ".start");
            }

            @Override
            public void shutdown() {
                events.add(name + ".shutdown");
            }
        };
    }

    /** A response that lands while fewer than all assigned queues are active fails at the response. */
    private static int recorderFlagsResponseBeforeBarrier(List<String> failures) {
        List<String> rows = new ArrayList<>();
        LivePullFanoutProbe.Recorder recorder = recorder(3, rows);
        recorder.pullStart("127.0.0.1:1", pullRequest(101, 0));
        recorder.pullStart("127.0.0.1:1", pullRequest(102, 1));
        // Queue 2 never became active, and no later start is needed to notice this.
        recorder.pullEnd("127.0.0.1:1", pullRequest(101, 0), successResponse());

        int checks = 0;
        checks += expect(recorder.gateLatched(), false, "recorder: gate after a premature response",
            failures);
        checks += expect(violationMessage(recorder),
            "a target Pull response completed before every assigned queue had an outstanding Pull",
            "recorder: premature response detection", failures);
        checks += expect(count(rows, "PULL_END"), 1, "recorder: premature response row", failures);
        return checks;
    }

    /** A second admission for a queue whose Pull is still outstanding is a violation. */
    private static int recorderFlagsDuplicateActiveStart(List<String> failures) {
        List<String> rows = new ArrayList<>();
        LivePullFanoutProbe.Recorder recorder = recorder(2, rows);
        recorder.pullStart("127.0.0.1:1", pullRequest(201, 0));
        recorder.pullStart("127.0.0.1:1", pullRequest(202, 0));

        int checks = 0;
        checks += expect(violationMessage(recorder),
            "queue 0 admitted a second Pull while opaque 201 was still outstanding",
            "recorder: duplicate active start", failures);
        checks += expect(recorder.activePullCount(), 1, "recorder: active map size", failures);
        return checks;
    }

    /** The barrier latches with the last admission; later follow-ups stay legal. */
    private static int recorderAllowsFollowUpAfterLatchedGate(List<String> failures) {
        List<String> rows = new ArrayList<>();
        LivePullFanoutProbe.Recorder recorder = recorder(2, rows);
        recorder.pullStart("127.0.0.1:1", pullRequest(301, 0));
        recorder.pullStart("127.0.0.1:1", pullRequest(302, 1));

        int checks = 0;
        checks += expect(recorder.gateLatched(), true, "recorder: gate latched by last admission",
            failures);
        LivePullFanoutProbe.GateSnapshot gate = recorder.gateSnapshot();
        if (gate == null) {
            failures.add("recorder: missing gate snapshot");
            return checks + 1;
        }
        checks += expect(gate.distinctQueues(), 2, "recorder: gate distinct queue count", failures);
        checks += expect(count(rows, "GATE"), 1, "recorder: exactly one GATE row", failures);

        // A response after the barrier is normal, and so is the follow-up Pull it schedules.
        recorder.pullEnd("127.0.0.1:1", pullRequest(301, 0), successResponse());
        recorder.pullStart("127.0.0.1:1", pullRequest(303, 0));
        checks += expect(violationMessage(recorder), null, "recorder: follow-up after the gate",
            failures);
        checks += expect(count(rows, "GATE"), 1, "recorder: GATE row is not repeated", failures);

        // A response that does not match an outstanding request is still detected.
        recorder.pullEnd("127.0.0.1:1", pullRequest(999, 1), successResponse());
        checks += expect(violationMessage(recorder),
            "a target Pull response for queue 1 / opaque 999 did not match an outstanding Pull",
            "recorder: unmatched response", failures);
        return checks;
    }

    /** Every listener callback is evidence; de-duplication only tracks progress. */
    private static int recorderLogsEveryDeliveryCallback(List<String> failures) {
        List<String> rows = new ArrayList<>();
        LivePullFanoutProbe.Recorder recorder = recorder(2, rows);
        boolean first = recorder.recordDelivery(1_000L, 3, "p10f-q3", 0);
        boolean second = recorder.recordDelivery(2_000L, 3, "p10f-q3", 1);

        int checks = 0;
        checks += expect(first, true, "recorder: first delivery counts", failures);
        checks += expect(second, false, "recorder: repeated delivery does not count twice", failures);
        checks += expect(count(rows, "DELIVERY"), 2, "recorder: every callback emits a row", failures);
        checks += expect(recorder.deliveredCount(), 1, "recorder: distinct delivered keys", failures);
        return checks;
    }

    private static LivePullFanoutProbe.Recorder recorder(int queues, List<String> rows) {
        return new LivePullFanoutProbe.Recorder(TOPIC, GROUP, queues, 5_000_000_000L, rows::add);
    }

    private static RemotingCommand pullRequest(int opaque, int queueId) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        request.setOpaque(opaque);
        request.addExtField("topic", TOPIC);
        request.addExtField("consumerGroup", GROUP);
        request.addExtField("queueId", String.valueOf(queueId));
        request.addExtField("suspendTimeoutMillis", "15000");
        return request;
    }

    private static RemotingCommand successResponse() {
        return RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
    }

    private static String violationMessage(LivePullFanoutProbe.Recorder recorder) {
        try {
            recorder.checkViolations();
            return null;
        } catch (IllegalStateException error) {
            return error.getMessage();
        }
    }

    private static int count(List<String> rows, String kind) {
        int total = 0;
        for (String row : rows) {
            if (row.startsWith("P10F\t" + kind + "\t")) {
                total++;
            }
        }
        return total;
    }

    private static <T> int expect(List<T> actual, List<T> expected, String label,
        List<String> failures) {
        return expect(actual.equals(expected), actual, expected, label, failures);
    }

    private static <T> int expect(T actual, T expected, String label, List<String> failures) {
        return expect(actual == null ? expected == null : actual.equals(expected), actual, expected,
            label, failures);
    }

    private static int expect(boolean ok, Object actual, Object expected, String label,
        List<String> failures) {
        if (!ok) {
            failures.add(label + ": expected " + expected + ", got " + actual);
        }
        return 1;
    }
}
