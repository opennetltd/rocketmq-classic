package compat;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.RequestCallback;
import org.apache.rocketmq.client.producer.RequestFutureHolder;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.utils.MessageUtil;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/** Exercises a Broker-correlated request and reply against a local Broker. */
public final class LiveRequestReplyProbe {
    private LiveRequestReplyProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 5 && "request".equals(args[0])) {
            request(args[1], args[2], args[3], args[4]);
            return;
        }
        if (args.length == 7 && "timeout".equals(args[0])) {
            timeout(args[1], args[2], args[3], args[4], Long.parseLong(args[5]),
                Long.parseLong(args[6]));
            return;
        }
        if (args.length == 7 && "late-timeout".equals(args[0])) {
            lateTimeout(args[1], args[2], args[3], args[4], Long.parseLong(args[5]),
                Long.parseLong(args[6]));
            return;
        }
        if ((args.length == 6 || args.length == 7) && "respond".equals(args[0])) {
            respond(args[1], args[2], args[3], args[4], args[5],
                args.length == 7 ? Long.parseLong(args[6]) : 0);
            return;
        }
        throw new IllegalArgumentException(
            "usage: request <nameserver> <topic> <producer-group> <body> | "
                + "timeout|late-timeout <nameserver> <topic> <producer-group> <body> "
                + "<timeout-ms> <hold-ms> | respond <nameserver> <topic> <consumer-group> "
                + "<producer-group> <reply-body> [delay-ms]");
    }

    private static void request(String namesrvAddress, String topic, String group, String body)
        throws Exception {
        DefaultMQProducer producer = producer(namesrvAddress, group);
        producer.start();
        try {
            Message request = new Message(topic, body.getBytes(StandardCharsets.UTF_8));
            Message reply = producer.request(request, 15_000);
            print("REQUEST", request, "");
            print("REPLY", reply, request.getProperty("CORRELATION_ID"));
        } finally {
            producer.shutdown();
        }
    }

    private static void respond(
        String namesrvAddress, String topic, String consumerGroup, String producerGroup,
        String replyBody, long delayMillis
    ) throws Exception {
        DefaultMQProducer producer = producer(namesrvAddress, producerGroup);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrvAddress);
        consumer.setVipChannelEnabled(false);
        consumer.subscribe(topic, "*");
        ArrayBlockingQueue<String> result = new ArrayBlockingQueue<>(1);
        AtomicBoolean replied = new AtomicBoolean();
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (MessageExt request : messages) {
                if (!replied.compareAndSet(false, true)) {
                    continue;
                }
                System.out.println("REQ\tRECEIVED\t" + request.getTopic() + "\t"
                    + toHex(request.getBody()) + "\t"
                    + request.getProperty("CORRELATION_ID") + "\t"
                    + request.getProperty("REPLY_TO_CLIENT"));
                try {
                    if (delayMillis > 0) {
                        Thread.sleep(delayMillis);
                    }
                    Message reply = MessageUtil.createReplyMessage(
                        request, replyBody.getBytes(StandardCharsets.UTF_8));
                    SendResult sent = producer.send(reply);
                    result.offer("SEND_OK\t" + reply.getTopic() + "\t"
                        + toHex(request.getBody()) + "\t"
                        + request.getProperty("CORRELATION_ID") + "\t"
                        + request.getProperty("REPLY_TO_CLIENT"));
                } catch (Exception error) {
                    error.printStackTrace(System.err);
                    result.offer("ERROR\t" + error.getClass().getName() + "\t" + error.getMessage());
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        boolean producerStarted = false;
        boolean consumerStarted = false;
        try {
            producer.start();
            producerStarted = true;
            consumer.start();
            consumerStarted = true;
            System.out.println("REQ\tREADY");
            String outcome = result.poll(30, TimeUnit.SECONDS);
            if (outcome == null) {
                throw new IllegalStateException("timed out waiting for a request");
            }
            if (!outcome.startsWith("SEND_OK\t")) {
                throw new IllegalStateException("reply send failed: " + outcome);
            }
            System.out.println("REQ\tRESPONDER\t" + outcome);
        } finally {
            if (consumerStarted) {
                consumer.shutdown();
            }
            if (producerStarted) {
                producer.shutdown();
            }
        }
    }

    private static void timeout(
        String namesrvAddress, String topic, String group, String body,
        long timeoutMillis, long holdMillis
    ) throws Exception {
        DefaultMQProducer producer = producer(namesrvAddress, group);
        producer.start();
        try {
            long started = System.nanoTime();
            Message request = new Message(topic, body.getBytes(StandardCharsets.UTF_8));
            try {
                Message reply = producer.request(request, timeoutMillis);
                throw new IllegalStateException("expected request timeout, received " + reply.getTopic());
            } catch (RequestTimeoutException expected) {
                // The Java requester waits for the configured timeout while its shared
                // timeout scanner removes the correlation entry on its next pass.
            }
            String correlationId = request.getProperty("CORRELATION_ID");
            if (correlationId == null || correlationId.isEmpty()) {
                throw new IllegalStateException("timed-out request has no correlation ID");
            }
            Thread.sleep(holdMillis);
            boolean pending = RequestFutureHolder.getInstance().getRequestFutureTable()
                .containsKey(correlationId);
            if (pending) {
                throw new IllegalStateException("timed-out request remained in the correlation table");
            }
            System.out.printf("REQ\tTIMEOUT\t%s\t%s\t%d\tfalse\t%d%n", topic, correlationId,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started), holdMillis);
        } finally {
            producer.shutdown();
        }
    }

    private static void lateTimeout(
        String namesrvAddress, String topic, String group, String body,
        long timeoutMillis, long lateWaitMillis
    ) throws Exception {
        DefaultMQProducer producer = producer(namesrvAddress, group);
        producer.start();
        try {
            Message request = new Message(topic, body.getBytes(StandardCharsets.UTF_8));
            AtomicInteger callbackCount = new AtomicInteger();
            AtomicReference<Throwable> callbackError = new AtomicReference<>();
            AtomicReference<String> callbackOutcome = new AtomicReference<>("NONE");
            CountDownLatch callbackDone = new CountDownLatch(1);
            RequestCallback callback = new RequestCallback() {
                @Override
                public void onSuccess(Message message) {
                    callbackCount.incrementAndGet();
                    callbackOutcome.set("SUCCESS");
                    callbackDone.countDown();
                }

                @Override
                public void onException(Throwable error) {
                    callbackCount.incrementAndGet();
                    callbackError.set(error);
                    callbackOutcome.set(error.getClass().getName());
                    callbackDone.countDown();
                }
            };
            producer.request(request, callback, timeoutMillis);
            String correlationId = request.getProperty("CORRELATION_ID");
            if (correlationId == null || correlationId.isEmpty()) {
                throw new IllegalStateException("late-reply request has no correlation ID");
            }
            if (!callbackDone.await(timeoutMillis + 2_000, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("request timeout callback did not run");
            }
            Throwable error = callbackError.get();
            if (!(error instanceof RequestTimeoutException)) {
                throw new IllegalStateException("expected RequestTimeoutException callback, got "
                    + callbackOutcome.get(), error);
            }
            Thread.sleep(lateWaitMillis);
            int callbacks = callbackCount.get();
            boolean pending = RequestFutureHolder.getInstance().getRequestFutureTable()
                .containsKey(correlationId);
            if (callbacks != 1 || pending) {
                throw new IllegalStateException("late reply changed terminal callback/table state: callbacks="
                    + callbacks + ", pending=" + pending);
            }
            System.out.printf("REQ\tLATE_TIMEOUT\t%s\t%s\t%d\tfalse\t%d\t%s%n", topic,
                correlationId, callbacks, lateWaitMillis, callbackOutcome.get());
        } finally {
            producer.shutdown();
        }
    }

    private static DefaultMQProducer producer(String namesrvAddress, String group) {
        DefaultMQProducer producer = new DefaultMQProducer(group);
        producer.setNamesrvAddr(namesrvAddress);
        producer.setSendMessageWithVIPChannel(false);
        producer.setSendMsgTimeout(10_000);
        return producer;
    }

    private static void print(String kind, Message message, String expectedCorrelation) {
        String correlation = message.getProperty("CORRELATION_ID");
        String replyTo = message.getProperty("REPLY_TO_CLIENT");
        String ttl = message.getProperty("TTL");
        String cluster = message.getProperty("CLUSTER");
        String type = message.getProperty("MSG_TYPE");
        if ("REPLY".equals(kind) && !expectedCorrelation.equals(correlation)) {
            throw new IllegalStateException("reply correlation ID differs from request");
        }
        System.out.printf("REQ\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s%n",
            kind, message.getTopic(), toHex(message.getBody()),
            correlation == null ? "-" : correlation,
            replyTo == null ? "-" : replyTo,
            ttl == null ? "-" : ttl,
            cluster == null ? "-" : cluster,
            type == null ? "-" : type);
    }

    private static String toHex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }
}
