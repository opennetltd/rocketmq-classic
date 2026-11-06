package compat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

/** Exercises public Java producer and Push consumer paths against the live Broker. */
public final class LivePushInteropProbe {
    private static final int CYCLES = 3;

    private LivePushInteropProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 6 && "send".equals(args[0])) {
            send(args[1], args[2], args[3], Integer.parseInt(args[4]), args[5]);
            return;
        }
        if (args.length == 6 && "consume".equals(args[0])) {
            consume(args[1], args[2], args[3], args[4], Path.of(args[5]));
            return;
        }
        throw new IllegalArgumentException(
            "usage: send <nameserver-address> <topic> <broker-name> <queue-id> <body> | "
                + "consume <nameserver-address> <topic> <group> <body> <ready-file>");
    }

    private static void send(
        String nameserverAddress, String topic, String brokerName, int queueId, String body
    ) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("compat_p11_java_producer");
        producer.setNamesrvAddr(nameserverAddress);
        producer.setSendMessageWithVIPChannel(false);
        producer.setSendMsgTimeout(10_000);
        producer.start();
        try {
            Message message = new Message(topic, body.getBytes(StandardCharsets.UTF_8));
            SendResult result = producer.send(message, new MessageQueue(topic, brokerName, queueId));
            if (result == null || !"SEND_OK".equals(result.getSendStatus().name())) {
                throw new IllegalStateException("Java P11 producer did not return SEND_OK: " + result);
            }
            System.out.printf("P11\tPRODUCED\tjava\t%s\t%s\t%d\t%d%n", body,
                result.getMessageQueue().getBrokerName(), result.getMessageQueue().getQueueId(),
                result.getQueueOffset());
        } finally {
            producer.shutdown();
        }
    }

    private static void consume(
        String nameserverAddress, String topic, String group, String body, Path readyFile
    ) throws Exception {
        for (int cycle = 0; cycle < CYCLES; cycle++) {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
            consumer.setNamesrvAddr(nameserverAddress);
            consumer.setVipChannelEnabled(false);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.setConsumeMessageBatchMaxSize(1);
            consumer.setMaxReconsumeTimes(1);

            final int activeCycle = cycle;
            AtomicInteger attempts = new AtomicInteger();
            CountDownLatch completed = new CountDownLatch(1);
            consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
                ConsumeConcurrentlyStatus outcome = ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                for (var message : messages) {
                    String observed = new String(message.getBody(), StandardCharsets.UTF_8);
                    int reconsumeTimes = message.getReconsumeTimes();
                    System.out.printf("P11\tDELIVERY\tjava\t%s\t%d\t%s\t%d\t%d%n",
                        observed, reconsumeTimes, message.getTopic(), message.getQueueId(),
                        message.getQueueOffset());
                    if (activeCycle == 0 && body.equals(observed)) {
                        int attempt = attempts.incrementAndGet();
                        if (attempt == 1) {
                            outcome = ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        } else {
                            completed.countDown();
                        }
                    }
                }
                return outcome;
            });
            consumer.subscribe(topic, "*");
            consumer.start();

            MQClientInstance factory = consumer.getDefaultMQPushConsumerImpl().getmQClientFactory();
            ExecutorService runtimeScheduler = factory.getScheduledExecutorService();
            ExecutorService pullScheduler = factory.getPullMessageService().getScheduledExecutorService();
            if (cycle == 0) {
                Files.writeString(readyFile, "ready\n", StandardCharsets.UTF_8);
                if (!completed.await(90, TimeUnit.SECONDS)) {
                    consumer.shutdown();
                    throw new IllegalStateException("timed out waiting for Rust-produced Java Push retry");
                }
                if (attempts.get() != 2) {
                    consumer.shutdown();
                    throw new IllegalStateException("Java Push did not observe exactly one retry: " + attempts.get());
                }
                Thread.sleep(250);
            } else {
                Thread.sleep(750);
            }

            consumer.shutdown();
            boolean runtimeStopped = awaitTermination(runtimeScheduler);
            boolean pullStopped = awaitTermination(pullScheduler);
            if (!runtimeStopped || !pullStopped) {
                throw new IllegalStateException("Java Push shutdown retained scheduler tasks: runtimeStopped="
                    + runtimeStopped + " pullStopped=" + pullStopped);
            }
            System.out.printf("P11\tCYCLE\tjava\t%d\ttrue\ttrue%n", cycle);
        }
        System.out.println("P11\tPASS\tJAVA_PUSH_RETRY\tCYCLES=3\tRETRIES=1");
    }

    private static boolean awaitTermination(ExecutorService executor) throws InterruptedException {
        return executor == null || executor.awaitTermination(10, TimeUnit.SECONDS)
            && executor.isTerminated();
    }
}
