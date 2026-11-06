package compat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.TreeSet;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

/** Exercises the high-level Java producer send overloads against a live Broker. */
public final class LiveHighLevelProducerProbe {
    private LiveHighLevelProducerProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 4 && "seed-p10".equals(args[0])) {
            seedP10(args[1], args[2], args[3]);
            return;
        }
        if (args.length == 8 && "retry-probe".equals(args[0])) {
            retryProbe(args[1], args[2], args[3], args[4], Integer.parseInt(args[5]), args[6], args[7]);
            return;
        }
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: <nameserver-address> <topic> | retry-probe <nameserver-address> <topic> <mode> <body> <timeout-ms> <ready-file> <go-file>");
        }
        run(args[0], args[1]);
    }

    private static void seedP10(String namesrvAddress, String topic, String stage) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("compat_p10_seed_java");
        producer.setNamesrvAddr(namesrvAddress);
        producer.setSendMessageWithVIPChannel(false);
        producer.setSendMsgTimeout(10000);
        String[][] messages;
        if ("initial".equals(stage)) {
            messages = new String[][] {
                {"p10-success", "p10-success-key"},
                {"p10-partial-ack", "p10-partial-ack-key"},
                {"p10-partial-retry", "p10-partial-retry-key"}
            };
        } else if ("failure".equals(stage)) {
            messages = new String[][] {{"p10-failure-dlq", "p10-failure-dlq-key"}};
        } else {
            throw new IllegalArgumentException("unknown P10 seed stage: " + stage);
        }
        MessageQueue queue = new MessageQueue(topic, "broker-a", 0);
        producer.start();
        try {
            for (String[] item : messages) {
                Message message = new Message(topic, item[0].getBytes(StandardCharsets.UTF_8));
                message.setKeys(item[1]);
                SendResult result = producer.send(message, queue);
                if (!"SEND_OK".equals(result.getSendStatus().name())) {
                    throw new IllegalStateException("P10 seed did not return SEND_OK: " + result);
                }
                System.out.printf("P10\tPRODUCED\t%s\t%s\t%s\t%s\t%d%n",
                    stage, item[0], item[1], result.getMessageQueue().getBrokerName(),
                    result.getMessageQueue().getQueueId());
            }
        } finally {
            producer.shutdown();
        }
    }

    private static void retryProbe(
        String namesrvAddress, String topic, String mode, String body, int timeoutMillis,
        String readyFile, String goFile
    ) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("compat_p7_retry_java");
        producer.setNamesrvAddr(namesrvAddress);
        producer.setSendMessageWithVIPChannel(false);
        producer.setSendMsgTimeout(timeoutMillis);
        producer.setRetryTimesWhenSendFailed(2);
        producer.getRetryResponseCodes().add(ResponseCode.NO_PERMISSION);
        AtomicInteger attempt = new AtomicInteger();
        producer.getDefaultMQProducerImpl().registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "p7-retry-attempt-recorder";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {}

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                int number = attempt.incrementAndGet();
                Message message = context.getMessage();
                String uniqueId = message.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                String status = context.getSendResult() == null ? "ERROR" :
                    context.getSendResult().getSendStatus().name();
                String error = context.getException() == null ? "" : context.getException().toString();
                System.out.printf("ATTEMPT\tjava\t%d\t%s\t%s\t%s\t%s%n",
                    number, context.getMq().getBrokerName(), status,
                    toHex(message.getBody()),
                    uniqueId == null ? "" : uniqueId);
                System.out.printf("ATTEMPT_ERROR\tjava\t%d\t%s%n", number,
                    error.replace('\t', ' ').replace('\n', ' '));
            }
        });
        producer.start();
        Throwable failure = null;
        String outcome = "SUCCESS";
        long started;
        try {
            TreeSet<String> brokers = new TreeSet<>();
            for (MessageQueue queue : producer.fetchPublishMessageQueues(topic)) {
                brokers.add(queue.getBrokerName());
            }
            System.out.printf("ROUTE\tjava\t%s%n", String.join(",", brokers));
            if (!brokers.contains("broker-a") || !brokers.contains("broker-b")) {
                throw new IllegalStateException("topic route has no writable queues");
            }
            Files.writeString(Path.of(readyFile), "ready\n");
            awaitGoFile(goFile);
            Message message = new Message(topic, body.getBytes(StandardCharsets.UTF_8));
            message.setKeys("compat_p7_" + mode + "_java");
            started = System.nanoTime();
            try {
                SendResult result = producer.send(message);
                System.out.printf("RESULT\tjava\t%s\tSUCCESS\t%s\t%s\t%d%n", mode,
                    result.getSendStatus().name(), result.getMessageQueue().getBrokerName(),
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
                if ("retry".equals(mode) && !"SEND_OK".equals(result.getSendStatus().name())) {
                    throw new IllegalStateException("retry probe did not recover with SEND_OK");
                }
            } catch (Throwable error) {
                failure = error;
                outcome = isTimeout(error) ? "TIMEOUT" : "BROKER_ERROR";
                System.out.printf("RESULT\tjava\t%s\t%s\t%s\t%d%n", mode, outcome,
                    error.toString().replace('\t', ' ').replace('\n', ' '),
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
            }
            if ("retry".equals(mode) && outcome != "SUCCESS") {
                throw new IllegalStateException("retry probe did not recover after injected Broker errors", failure);
            }
            if ("deadline".equals(mode) && outcome != "TIMEOUT") {
                throw new IllegalStateException("deadline probe expected timeout", failure);
            }
            if (!"retry".equals(mode) && !"deadline".equals(mode)) {
                throw new IllegalArgumentException("unknown retry probe mode: " + mode);
            }
        } finally {
            producer.shutdown();
        }
    }

    private static void awaitGoFile(String goFile) throws Exception {
        Path path = Path.of(goFile);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            if (Files.exists(path)) {
                return;
            }
            Thread.sleep(10);
        }
        throw new IllegalStateException("timed out waiting for the send gate");
    }

    private static boolean isTimeout(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            String description = (cause.getClass().getSimpleName() + " " + cause.getMessage())
                .toLowerCase(java.util.Locale.ROOT);
            if (description.contains("timeout") || description.contains("deadline")) {
                return true;
            }
        }
        return false;
    }

    private static String toHex(byte[] value) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] encoded = new char[value.length * 2];
        for (int index = 0; index < value.length; index++) {
            int octet = value[index] & 0xff;
            encoded[index * 2] = digits[octet >>> 4];
            encoded[index * 2 + 1] = digits[octet & 0x0f];
        }
        return new String(encoded);
    }

    private static void run(String namesrvAddress, String topic) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("compat_p7_java");
        producer.setNamesrvAddr(namesrvAddress);
        producer.setSendMessageWithVIPChannel(false);
        producer.setSendMsgTimeout(10_000);
        producer.setDefaultTopicQueueNums(4);
        producer.start();
        try {
            SendResult sync = producer.send(message(topic, "p7-sync"));
            print("SYNC", sync);

            CountDownLatch completed = new CountDownLatch(1);
            AtomicInteger callbackCount = new AtomicInteger();
            AtomicReference<SendResult> asyncResult = new AtomicReference<>();
            AtomicReference<Throwable> asyncFailure = new AtomicReference<>();
            producer.send(message(topic, "p7-async"), new SendCallback() {
                @Override
                public void onSuccess(SendResult result) {
                    callbackCount.incrementAndGet();
                    asyncResult.set(result);
                    completed.countDown();
                }

                @Override
                public void onException(Throwable error) {
                    callbackCount.incrementAndGet();
                    asyncFailure.set(error);
                    completed.countDown();
                }
            });
            if (!completed.await(15, TimeUnit.SECONDS)) {
                throw new IllegalStateException("asynchronous send callback timed out");
            }
            if (callbackCount.get() != 1 || asyncFailure.get() != null || asyncResult.get() == null) {
                throw new IllegalStateException("asynchronous send callback did not complete once",
                    asyncFailure.get());
            }
            System.out.printf("P7\tASYNC\t%d\t%s\t%d%n", callbackCount.get(),
                asyncResult.get().getSendStatus().name(), asyncResult.get().getMessageQueue().getQueueId());

            producer.sendOneway(message(topic, "p7-oneway"));
            System.out.println("P7\tONEWAY\tNO_RESPONSE");

            SendResult explicit = producer.send(message(topic, "p7-explicit"),
                new MessageQueue(topic, "broker-a", 1));
            print("EXPLICIT_QUEUE", explicit);

            SendResult selected = producer.send(message(topic, "p7-selector"), (queues, msg, arg) ->
                queues.stream().filter(queue -> queue.getQueueId() == 2).findFirst()
                    .orElseThrow(() -> new IllegalStateException("queue 2 is missing from the route")), null);
            print("SELECTOR", selected);

            java.util.List<Message> batchMessages = Arrays.asList(
                message(topic, "p7-batch-0"), message(topic, "p7-batch-1"));
            SendResult batch = producer.send(batchMessages);
            System.out.printf("P7\tVALID_BATCH\t%s\t%d\t%d%n", batch.getSendStatus().name(),
                batch.getMessageQueue().getQueueId(), batchMessages.size());

            try {
                producer.send(Arrays.asList(
                    message(topic, "p7-invalid-batch-0"),
                    message(topic + "-different", "p7-invalid-batch-1")));
                throw new IllegalStateException("batch with mixed topics was accepted");
            } catch (MQClientException expected) {
                System.out.println("P7\tINVALID_BATCH\tREJECTED");
            }
        } finally {
            producer.shutdown();
        }
    }

    private static Message message(String topic, String body) {
        return new Message(topic, body.getBytes(StandardCharsets.UTF_8));
    }

    private static void print(String method, SendResult result) {
        System.out.printf("P7\t%s\t%s\t%d%n", method, result.getSendStatus().name(),
            result.getMessageQueue().getQueueId());
    }
}
