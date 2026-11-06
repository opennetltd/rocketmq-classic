package compat;

import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

/** Exercises Java 5.5.1 explicit pulls, status mapping, and remote offset resume. */
public final class LivePullConsumerProbe {
    private LivePullConsumerProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 3 && "seed".equals(args[0])) {
            seed(args[1], args[2]);
            return;
        }
        if (args.length == 4 && "probe".equals(args[0])) {
            probe(args[1], args[2], args[3]);
            return;
        }
        throw new IllegalArgumentException(
            "usage: LivePullConsumerProbe seed <nameserver> <topic> | "
                + "probe <nameserver> <topic> <consumer-group>");
    }

    private static void seed(String namesrvAddress, String topic) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("compat_p9_seed");
        producer.setNamesrvAddr(namesrvAddress);
        producer.setSendMessageWithVIPChannel(false);
        producer.setSendMsgTimeout(10_000);
        producer.start();
        try {
            MessageQueue queue = new MessageQueue(topic, "broker-a", 0);
            producer.send(new Message(topic, "MATCH", "p9-seed-match".getBytes(StandardCharsets.UTF_8)), queue);
            producer.send(new Message(topic, "OTHER", "p9-seed-other".getBytes(StandardCharsets.UTF_8)), queue);
            System.out.println("P9\tSEEDED\t2");
        } finally {
            producer.shutdown();
        }
    }

    private static void probe(String namesrvAddress, String topic, String group) throws Exception {
        DefaultMQPullConsumer consumer = newConsumer(namesrvAddress, group);
        consumer.start();
        MessageQueue queue;
        try {
            Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(topic);
            if (queues.size() != 1) {
                throw new IllegalStateException("expected one queue, got " + queues.size());
            }
            queue = queues.iterator().next();
            if (queue.getQueueId() != 0 || !"broker-a".equals(queue.getBrokerName())) {
                throw new IllegalStateException("unexpected pull queue: " + queue);
            }

            PullResult found = consumer.pull(queue, "MATCH", 0, 1);
            print("FOUND", found);
            if (found.getMsgFoundList() == null || found.getMsgFoundList().size() != 1
                || !"p9-seed-match".equals(new String(
                    found.getMsgFoundList().get(0).getBody(), StandardCharsets.UTF_8))) {
                throw new IllegalStateException("manual pull did not return the seeded MATCH body");
            }
            System.out.println("P9\tBODY\tp9-seed-match");

            print("NO_MATCHED_MSG", consumer.pull(
                queue, MessageSelector.byTag("MISSING"), 0, 1));
            print("NO_NEW_MSG", consumer.pull(queue, "*", 2, 1));
            print("OFFSET_ILLEGAL", consumer.pull(queue, "*", 3, 1));

            long minOffset = consumer.minOffset(queue);
            long maxOffset = consumer.maxOffset(queue);
            long timestampSearch = consumer.searchOffset(queue, 0L);
            System.out.printf("P9\tBOUNDARY\t%d\t%d\t%d%n",
                minOffset, maxOffset, timestampSearch);
            if (minOffset != 0 || maxOffset != 2 || timestampSearch != 0) {
                throw new IllegalStateException("offset boundary RPCs returned unexpected values");
            }

            consumer.updateConsumeOffset(queue, 1);
            consumer.getOffsetStore().updateConsumeOffsetToBroker(queue, 1, false);
        } finally {
            consumer.shutdown();
        }

        DefaultMQPullConsumer restarted = newConsumer(namesrvAddress, group);
        restarted.start();
        try {
            long resumeOffset = restarted.fetchConsumeOffset(queue, true);
            if (resumeOffset != 1) {
                throw new IllegalStateException("pull restart expected offset 1, got " + resumeOffset);
            }
            System.out.printf("P9\tRESUME\t%d%n", resumeOffset);
        } finally {
            restarted.shutdown();
        }
    }

    private static DefaultMQPullConsumer newConsumer(String namesrvAddress, String group) {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(group);
        consumer.setNamesrvAddr(namesrvAddress);
        consumer.setEnableRebalance(false);
        return consumer;
    }

    private static void print(String expectedStatus, PullResult result) {
        String status = result.getPullStatus().name();
        if (!expectedStatus.equals(status)) {
            throw new IllegalStateException(
                "expected pull status " + expectedStatus + " but got " + status);
        }
        if (!(result instanceof PullResultExt)) {
            throw new IllegalStateException("pull result omitted the suggested Broker ID");
        }
        PullResultExt extended = (PullResultExt) result;
        System.out.printf("P9\tPULL\t%s\t%d\t%d\t%d\t%d%n", status,
            result.getNextBeginOffset(), result.getMinOffset(), result.getMaxOffset(),
            extended.getSuggestWhichBrokerId());
    }
}
