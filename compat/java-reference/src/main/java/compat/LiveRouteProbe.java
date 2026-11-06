package compat;

import java.util.Comparator;
import java.util.List;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

/** Minimal live route probe using the pinned RocketMQ Java client dependency. */
public final class LiveRouteProbe {
    private LiveRouteProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: LiveRouteProbe <namesrv-address> <topic>");
        }

        DefaultMQProducer producer = new DefaultMQProducer("p19RouteJava");
        producer.setNamesrvAddr(args[0]);
        try {
            producer.start();
            try {
                List<MessageQueue> queues = producer.fetchPublishMessageQueues(args[1]);
                queues.sort(Comparator.comparing(MessageQueue::getBrokerName)
                    .thenComparingInt(MessageQueue::getQueueId));
                if (queues.isEmpty()) {
                    System.out.printf("EMPTY_ROUTE\t%s%n", args[1]);
                } else {
                    for (MessageQueue queue : queues) {
                        System.out.printf("QUEUE\t%s\t%s\t%d%n", queue.getTopic(),
                            queue.getBrokerName(), queue.getQueueId());
                    }
                }
            } catch (MQClientException error) {
                if (!isTopicNotExist(error)) {
                    throw error;
                }
                System.out.printf("NO_ROUTE\t%s%n", args[1]);
            }
        } finally {
            producer.shutdown();
        }
    }

    private static boolean isTopicNotExist(Throwable error) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current instanceof MQClientException
                && ((MQClientException) current).getResponseCode() == ResponseCode.TOPIC_NOT_EXIST) {
                return true;
            }
        }
        return false;
    }
}
