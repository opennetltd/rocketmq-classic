package compat;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/** Exercises subscription changes and shared producer ownership against a live Broker. */
public final class LiveSharedRuntimeProbe {
    private LiveSharedRuntimeProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException(
                "usage: LiveSharedRuntimeProbe <nameserver> <topic> <producer-a> <producer-b> <consumer-group>");
        }

        String namesrvAddress = args[0];
        String topic = args[1];
        LinkedBlockingQueue<String> received = new LinkedBlockingQueue<>();
        DefaultMQProducer producerA = producer(args[2], namesrvAddress);
        DefaultMQProducer producerB = producer(args[3], namesrvAddress);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(args[4]);
        consumer.setNamesrvAddr(namesrvAddress);
        consumer.setVipChannelEnabled(false);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (Message message : messages) {
                received.add(new String(message.getBody(), StandardCharsets.UTF_8));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        boolean producerAStarted = false;
        boolean producerBStarted = false;
        boolean consumerStarted = false;
        boolean producerAStopped = false;
        try {
            producerA.start();
            producerAStarted = true;
            producerB.start();
            producerBStarted = true;
            consumer.start();
            consumerStarted = true;

            send(producerA, topic, "DROP", "p6-before-change");
            expect(received, "p6-before-change");

            consumer.subscribe(topic, "KEEP");
            System.out.println("P6\tSUBSCRIPTION\t*->KEEP");
            send(producerA, topic, "DROP", "p6-filtered-after-change");
            send(producerA, topic, "KEEP", "p6-keep-after-change");
            expect(received, "p6-keep-after-change");

            producerA.shutdown();
            producerAStopped = true;
            System.out.printf("P6\tUNREGISTER\t%s%n", args[2]);
            SendResult survivor = send(producerB, topic, "KEEP", "p6-survivor-after-unregister");
            if (!"SEND_OK".equals(survivor.getSendStatus().name())) {
                throw new IllegalStateException("remaining producer did not send successfully");
            }
            System.out.printf("P6\tSURVIVOR\t%s%n", survivor.getSendStatus().name());
            expect(received, "p6-survivor-after-unregister");

            String unexpected = received.poll(1500, TimeUnit.MILLISECONDS);
            if (unexpected != null) {
                throw new IllegalStateException("unexpected post-change delivery: " + unexpected);
            }
        } finally {
            if (consumerStarted) {
                consumer.shutdown();
            }
            if (producerBStarted) {
                producerB.shutdown();
            }
            if (producerAStarted && !producerAStopped) {
                producerA.shutdown();
            }
        }
    }

    private static DefaultMQProducer producer(String group, String namesrvAddress) {
        DefaultMQProducer producer = new DefaultMQProducer(group);
        producer.setNamesrvAddr(namesrvAddress);
        producer.setSendMessageWithVIPChannel(false);
        producer.setSendMsgTimeout(10_000);
        return producer;
    }

    private static SendResult send(
        DefaultMQProducer producer, String topic, String tag, String body
    ) throws Exception {
        SendResult result = producer.send(
            new Message(topic, tag, body.getBytes(StandardCharsets.UTF_8)));
        if (!"SEND_OK".equals(result.getSendStatus().name())) {
            throw new IllegalStateException("producer send did not return SEND_OK: " + body);
        }
        return result;
    }

    private static void expect(LinkedBlockingQueue<String> received, String expected)
        throws Exception {
        String actual = received.poll(15, TimeUnit.SECONDS);
        if (!expected.equals(actual)) {
            throw new IllegalStateException(
                "expected consumed body " + expected + " but received " + actual);
        }
        System.out.printf("P6\tRECEIVED\t%s%n", actual);
    }
}
