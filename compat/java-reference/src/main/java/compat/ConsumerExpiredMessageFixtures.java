package compat;

import com.alibaba.fastjson2.JSON;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.LongStream;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;

/** Invokes Java 5.5.1 ProcessQueue.cleanExpiredMsg with scripted send-back outcomes. */
public final class ConsumerExpiredMessageFixtures {
    private ConsumerExpiredMessageFixtures() { }

    private static final class RecordingConsumer extends DefaultMQPushConsumer {
        private final List<String> attempts = new ArrayList<>();
        private boolean failSendBack;
        private boolean orderly;

        @Override
        public boolean isConsumeOrderly() {
            return orderly;
        }

        @Override
        public void sendMessageBack(MessageExt message, int delayLevel) {
            attempts.add(message.getQueueOffset() + ":" + delayLevel);
            if (failSendBack) {
                throw new IllegalStateException("scripted send-back failure");
            }
        }
    }

    private static MessageExt message(long offset, int bodySize, long consumeStartedAt) {
        MessageExt message = new MessageExt();
        message.setTopic("orders");
        message.setQueueId(0);
        message.setQueueOffset(offset);
        message.setBody(new byte[bodySize]);
        MessageAccessor.setConsumeStartTimeStamp(message, Long.toString(consumeStartedAt));
        return message;
    }

    private static Map<String, Object> runCase(
        boolean orderly, boolean failSendBack, long... offsets
    ) {
        long now = System.currentTimeMillis();
        ProcessQueue processQueue = new ProcessQueue();
        List<MessageExt> messages = new ArrayList<>();
        for (int index = 0; index < offsets.length; index++) {
            long timestamp = orderly || failSendBack || index == 0 ? now - 61_000 : now + 60_000;
            messages.add(message(offsets[index], index + 2, timestamp));
        }
        processQueue.putMessage(messages);

        RecordingConsumer consumer = new RecordingConsumer();
        consumer.setConsumeTimeout(1);
        consumer.orderly = orderly;
        consumer.failSendBack = failSendBack;
        processQueue.cleanExpiredMsg(consumer);

        Map<String, Object> result = new TreeMap<>();
        result.put("attempts", consumer.attempts);
        result.put("remainingOffsets", new ArrayList<>(processQueue.getMsgTreeMap().keySet()));
        result.put("cachedCount", processQueue.getMsgCount().get());
        result.put("cachedBytes", processQueue.getMsgSize().get());
        return result;
    }

    private static Map<String, Object> timestampCase(
        String timestamp, boolean timestampPresent, boolean orderly
    ) {
        ProcessQueue processQueue = new ProcessQueue();
        MessageExt candidate = new MessageExt();
        candidate.setTopic("orders");
        candidate.setQueueId(0);
        candidate.setQueueOffset(40);
        candidate.setBody(new byte[4]);
        if (timestampPresent) {
            MessageAccessor.setConsumeStartTimeStamp(candidate, timestamp);
        }
        processQueue.putMessage(List.of(candidate));

        RecordingConsumer consumer = new RecordingConsumer();
        consumer.setConsumeTimeout(1);
        consumer.orderly = orderly;
        String errorClass = null;
        try {
            processQueue.cleanExpiredMsg(consumer);
        } catch (Throwable error) {
            errorClass = error.getClass().getName();
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("errorClass", errorClass);
        result.put("attempts", consumer.attempts);
        result.put("remainingOffsets", new ArrayList<>(processQueue.getMsgTreeMap().keySet()));
        result.put("cachedCount", processQueue.getMsgCount().get());
        result.put("cachedBytes", processQueue.getMsgSize().get());
        return result;
    }

    private static Map<String, Object> generate() {
        Map<String, Object> fixture = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        provenance.put("entrypoint", "ProcessQueue.cleanExpiredMsg(DefaultMQPushConsumer)");
        provenance.put("serverBaselineContacted", false);
        fixture.put("provenance", provenance);
        String nullConsumerErrorClass = null;
        try {
            new ProcessQueue().cleanExpiredMsg(null);
        } catch (Throwable error) {
            nullConsumerErrorClass = error.getClass().getName();
        }
        fixture.put("nullConsumerErrorClass", nullConsumerErrorClass);
        fixture.put("expiredHead", runCase(false, false, 10, 11));
        fixture.put("sendBackFailure", runCase(false, true, 20, 21));
        fixture.put("attemptLimit", runCase(false, true, LongStream.range(60, 78).toArray()));
        fixture.put("orderlySkipped", runCase(true, false, 30));
        fixture.put("malformedTimestamp", timestampCase("not-a-long", true, false));
        fixture.put("emptyTimestamp", timestampCase("", true, false));
        fixture.put("missingTimestamp", timestampCase(null, false, false));
        fixture.put("orderlyMalformedTimestamp", timestampCase("not-a-long", true, true));
        fixture.put("unicodeTimestamp", timestampCase("٤٦٠٠٠", true, false));
        return fixture;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) throw new IllegalArgumentException("expected fixture path");
        Path output = Path.of(args[0]);
        boolean verify = List.of(args).contains("--verify");
        String json = JSON.toJSONString(generate()) + "\n";
        if (verify) {
            if (!Files.exists(output) || !json.equals(Files.readString(output))) {
                throw new IllegalStateException("fixture drift: " + output);
            }
        } else {
            Files.createDirectories(output.getParent());
            Files.writeString(output, json);
        }
    }
}
