package compat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.MessageQueue;

/** Compares the pinned Java average strategy on live Broker routes. */
public final class LiveRebalanceProbe {
    private LiveRebalanceProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                "usage: LiveRebalanceProbe <namesrv-address> <topic> <comma-separated-consumer-ids>");
        }
        List<String> consumerIds = new ArrayList<>(List.of(args[2].split(",")));
        Collections.sort(consumerIds);

        DefaultMQProducer producer = new DefaultMQProducer("p19RebalanceJava");
        producer.setNamesrvAddr(args[0]);
        try {
            producer.start();
            List<MessageQueue> queues = producer.fetchPublishMessageQueues(args[1]);
            Collections.sort(queues);
            AllocateMessageQueueStrategy strategy = new AllocateMessageQueueAveragely();
            for (String consumerId : consumerIds) {
                List<MessageQueue> assigned = strategy.allocate(
                    "compat_phase0_rebalance_group", consumerId, queues, consumerIds);
                String keys = assigned.stream().map(LiveRebalanceProbe::queueKey)
                    .collect(Collectors.joining(","));
                System.out.printf("ASSIGN\t%s\t%s%n", consumerId, keys);
            }
        } finally {
            producer.shutdown();
        }
    }

    private static String queueKey(MessageQueue queue) {
        return queue.getTopic() + "|" + queue.getBrokerName() + "|" + queue.getQueueId();
    }
}
