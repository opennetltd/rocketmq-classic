package compat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.producer.SendResult;

/** Exercises Java 5.5.1 multi-Broker query fan-out against Phase 19 Brokers. */
public final class LiveP17AdminQueryProbe {
    private static final String DUPLICATE_KEY = "p17-duplicate-key";
    private static final String BOUNDED_KEY = "p17-bounded-time-range";

    private LiveP17AdminQueryProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 3 && args[0].equals("seed")) {
            seed(args[1], args[2]);
            return;
        }
        if (args.length == 6 && args[0].equals("query")) {
            query(args[1], args[2], args[3], Long.parseLong(args[4]), Long.parseLong(args[5]));
            return;
        }
        throw new IllegalArgumentException(
            "usage: seed <nameserver-address> <topic> | "
                + "query <nameserver-address> <topic> <key> <begin-ms> <end-ms>");
    }

    private static DefaultMQProducer producer(String nameserverAddress, String suffix) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("compat_p17_admin_query_" + suffix);
        producer.setNamesrvAddr(nameserverAddress);
        producer.setInstanceName("compat-p17-java-" + suffix + "-" + System.nanoTime());
        producer.start();
        return producer;
    }

    private static void seed(String nameserverAddress, String topic) throws Exception {
        DefaultMQProducer producer = producer(nameserverAddress, "seed");
        try {
            send(producer, topic, "broker-a", DUPLICATE_KEY, "p17-duplicate-broker-a");
            send(producer, topic, "broker-b", DUPLICATE_KEY, "p17-duplicate-broker-b");
            send(producer, topic, "broker-a", BOUNDED_KEY, "p17-bounded-in-range");
            Thread.sleep(2_500);
            send(producer, topic, "broker-b", BOUNDED_KEY, "p17-bounded-out-of-range");
            System.out.println("P17\tSEED\t4\t2\t2");
        } finally {
            producer.shutdown();
        }
    }

    private static void send(DefaultMQProducer producer, String topic, String brokerName,
                             String key, String body) throws Exception {
        Message message = new Message(topic, body.getBytes(StandardCharsets.UTF_8));
        message.setKeys(key);
        SendResult result = producer.send(message, new MessageQueue(topic, brokerName, 0));
        if (result == null || result.getMessageQueue() == null
                || !brokerName.equals(result.getMessageQueue().getBrokerName())) {
            throw new IllegalStateException("seed did not reach requested Broker " + brokerName);
        }
        System.out.printf("P17\tSEND\t%s\t%s\t%d\t%d%n", key,
            result.getMessageQueue().getBrokerName(), result.getQueueOffset(), result.getSendStatus().ordinal());
    }

    private static void query(String nameserverAddress, String topic, String key,
                              long beginTimestamp, long endTimestamp) throws Exception {
        DefaultMQProducer producer = producer(nameserverAddress, "query");
        try {
            QueryResult result;
            try {
                result = producer.queryMessage(topic, key, 32, beginTimestamp, endTimestamp);
            } catch (MQClientException error) {
                System.out.printf("P17\tERROR\t%s\t%d\n", key, error.getResponseCode());
                return;
            }
            List<MessageExt> messages = new ArrayList<>(result.getMessageList());
            messages.sort(Comparator.comparingLong(MessageExt::getStoreTimestamp)
                .thenComparing(MessageExt::getBrokerName)
                .thenComparingInt(MessageExt::getQueueId)
                .thenComparing(message -> new String(message.getBody(), StandardCharsets.UTF_8)));
            System.out.printf("P17\tRESULT\t%s\t%d\t%d%n", key, messages.size(),
                result.getIndexLastUpdateTimestamp());
            for (MessageExt message : messages) {
                System.out.printf("P17\tMESSAGE\t%s\t%s\t%d\t%d\t%s%n", key,
                    message.getBrokerName(), message.getQueueId(), message.getStoreTimestamp(),
                    new String(message.getBody(), StandardCharsets.UTF_8));
            }
        } finally {
            producer.shutdown();
        }
    }
}
