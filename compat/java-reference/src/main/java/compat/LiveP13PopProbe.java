package compat;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.exception.OffsetNotFoundException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;

/** Exercises live POP receipt lifecycle behavior against the pinned Broker. */
public final class LiveP13PopProbe {
    private static final String BROKER_NAME = "broker-a";

    private LiveP13PopProbe() {}

    public static void main(String[] args) throws Exception {
        boolean seeding = args.length > 0 && "seed".equals(args[0]);
        if ((!seeding && args.length != 4) || (seeding && args.length != 5)
            || !("prepare".equals(args[0]) || seeding || "consume".equals(args[0]))) {
            throw new IllegalArgumentException("usage: LiveP13PopProbe prepare|consume <broker-address> <topic> <language>; seed <broker-address> <nameserver-address> <topic> <language>");
        }
        if ("prepare".equals(args[0])) {
            prepare(args[1], args[2], args[3]);
        } else if (seeding) {
            seed(args[1], args[2], args[3], args[4]);
        } else {
            consume(args[1], args[2], args[3]);
        }
    }

    private static void prepare(String brokerAddress, String topic, String language) throws Exception {
        ClientConfig config = new ClientConfig();
        config.setVipChannelEnabled(false);
        MQClientAPIExt api = new MQClientAPIExt(config, new NettyClientConfig(), null, null);
        api.start();
        try {
            for (String suffix : Arrays.asList("expiry", "change", "mixed")) {
                String group = "compat_p13_pop_" + language + "_" + suffix;
                api.setMessageRequestMode(brokerAddress, topic, group, MessageRequestMode.POP, 1, 10_000);
                System.out.printf("P13\tprepare\t%s\tPOP%n", group);
                System.err.printf("P13\tpop-offset-state\tphase=after-prepare\tgroup=%s\tqueue=0\toffset=%s%n",
                    group, consumerOffset(api, brokerAddress, topic, group, 0));
            }
        } finally {
            api.shutdown();
        }
    }

    private static void seed(String brokerAddress, String nameserverAddress, String topic, String language) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("compat_p13_" + language);
        producer.setNamesrvAddr(nameserverAddress);
        producer.setVipChannelEnabled(false);
        MQClientAPIImpl api = producerApi();
        api.start();
        try {
            producer.start();
            useHostBrokerAddress(producer, topic, brokerAddress);
            for (int queueId = 0; queueId < 2; queueId++) {
                String body = "p13-" + language + "-q" + queueId;
                Message message = new Message(topic, body.getBytes(StandardCharsets.UTF_8));
                int requestedQueue = queueId;
                SendResult sent = producer.send(message, (available, ignored, arg) -> {
                    int requested = (Integer) arg;
                    return available.stream()
                        .filter(candidate -> BROKER_NAME.equals(candidate.getBrokerName()))
                        .filter(candidate -> candidate.getQueueId() == requested)
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("route omitted " + BROKER_NAME + ":" + requested));
                }, requestedQueue);
                if (sent == null || !"SEND_OK".equals(sent.getSendStatus().name())
                    || sent.getMessageQueue().getQueueId() != queueId) {
                    throw new IllegalStateException("seed send did not use the requested queue: " + sent);
                }
                long maxOffset = awaitMaxOffset(api, brokerAddress,
                    new MessageQueue(topic, BROKER_NAME, queueId), sent.getQueueOffset());
                if (maxOffset <= sent.getQueueOffset()) {
                    throw new IllegalStateException("seed send offset was not visible on the requested queue: " + sent
                        + " maxOffset=" + maxOffset);
                }
                System.out.printf("P13\tseed\t%s\t%d\t%d\t%d%n",
                    body, sent.getMessageQueue().getQueueId(), sent.getQueueOffset(), maxOffset);
                for (String consumerLanguage : Arrays.asList("java", "rust")) {
                    for (String suffix : Arrays.asList("expiry", "change", "mixed")) {
                        String group = "compat_p13_pop_" + consumerLanguage + "_" + suffix;
                        System.err.printf("P13\tpop-offset-state\tphase=after-seed-q%d\tgroup=%s\tqueue=0\toffset=%s%n",
                            queueId, group, consumerOffset(api, brokerAddress, topic, group, 0));
                    }
                }
            }
            initializePopGroupOffsets(api, brokerAddress, topic);
        } finally {
            producer.shutdown();
            api.shutdown();
        }
    }

    private static void useHostBrokerAddress(DefaultMQProducer producer, String topic, String brokerAddress)
        throws Exception {
        MQClientInstance client = producer.getDefaultMQProducerImpl().getMqClientFactory();
        if (!client.updateTopicRouteInfoFromNameServer(topic)) {
            throw new IllegalStateException("NameServer did not provide a route for " + topic);
        }
        Field addressTableField = MQClientInstance.class.getDeclaredField("brokerAddrTable");
        addressTableField.setAccessible(true);
        Object rawTable = addressTableField.get(client);
        if (!(rawTable instanceof ConcurrentMap<?, ?>)) {
            throw new IllegalStateException("unexpected Broker address table type: " + rawTable);
        }
        Object rawAddresses = ((ConcurrentMap<?, ?>) rawTable).get(BROKER_NAME);
        if (!(rawAddresses instanceof Map<?, ?>)) {
            throw new IllegalStateException("NameServer route omitted Broker " + BROKER_NAME);
        }
        @SuppressWarnings("unchecked")
        Map<Long, String> addresses = (Map<Long, String>) rawAddresses;
        if (!addresses.containsKey(0L)) {
            throw new IllegalStateException("NameServer route omitted master address for " + BROKER_NAME);
        }
        // The two-Broker test profile advertises Docker-only service names to host-side clients.
        addresses.put(0L, brokerAddress);
        if (!brokerAddress.equals(client.findBrokerAddressInPublish(BROKER_NAME))) {
            throw new IllegalStateException("producer did not adopt host Broker address " + brokerAddress);
        }
    }

    private static long awaitMaxOffset(MQClientAPIImpl api, String brokerAddress, MessageQueue queue,
        long sentOffset) throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        long maxOffset;
        do {
            maxOffset = api.getMaxOffset(brokerAddress, queue, 10_000);
            if (maxOffset > sentOffset) {
                return maxOffset;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        } while (System.nanoTime() < deadlineNanos);
        return maxOffset;
    }

    private static void initializePopGroupOffsets(MQClientAPIImpl api, String brokerAddress, String topic)
        throws Exception {
        for (String language : Arrays.asList("java", "rust")) {
            for (String suffix : Arrays.asList("expiry", "change", "mixed")) {
                String group = "compat_p13_pop_" + language + "_" + suffix;
                for (int queueId = 0; queueId < 2; queueId++) {
                    UpdateConsumerOffsetRequestHeader header = new UpdateConsumerOffsetRequestHeader();
                    header.setConsumerGroup(group);
                    header.setTopic(topic);
                    header.setQueueId(queueId);
                    header.setCommitOffset(0L);
                    header.setBrokerName(BROKER_NAME);
                    api.updateConsumerOffset(brokerAddress, header, 10_000);
                    String actualOffset = consumerOffset(api, brokerAddress, topic, group, queueId);
                    if (!"0".equals(actualOffset)) {
                        throw new IllegalStateException("failed to initialize POP group " + group
                            + " queue " + queueId + " at offset 0: " + actualOffset);
                    }
                    System.out.printf("P13\tpop-offset-reset\t%s\t%d\t0%n", group, queueId);
                }
            }
        }
    }

    private static void consume(String brokerAddress, String topic, String language) throws Exception {
        ClientConfig config = new ClientConfig();
        config.setVipChannelEnabled(false);
        MQClientAPIExt api = new MQClientAPIExt(config, new NettyClientConfig(), null, null);
        api.start();
        try {
            String prefix = "compat_p13_pop_" + language;
            String queueZeroBody = "p13-" + language + "-q0";
            String queueOneBody = "p13-" + language + "-q1";
            printPopOffsetState(api, brokerAddress, topic, prefix + "_expiry", 0, "after-api-start");

            MessageExt first = pop(api, brokerAddress, topic, prefix + "_expiry", 0, queueZeroBody, 1_000);
            String firstReceipt = receipt(first);
            TimeUnit.MILLISECONDS.sleep(6_000);
            MessageExt redelivered = pop(api, brokerAddress, topic, prefix + "_expiry", 0, queueZeroBody, 10_000);
            String redeliveryReceipt = receipt(redelivered);
            if (firstReceipt.equals(redeliveryReceipt)) {
                throw new IllegalStateException("expiry redelivery reused the expired POP receipt");
            }
            requireAck(api.ackMessageAsync(brokerAddress, ackHeader(redelivered, topic,
                prefix + "_expiry", redeliveryReceipt), 10_000).get(15, TimeUnit.SECONDS), "expiry-redelivery");
            System.out.printf("P13\texpiry-redelivery\tOK\t%d\n", redelivered.getQueueId());

            MessageExt changedSource = pop(api, brokerAddress, topic, prefix + "_change", 0, queueZeroBody, 60_000);
            String oldReceipt = receipt(changedSource);
            AckResult changed = api.changeInvisibleTimeAsync(brokerAddress, BROKER_NAME,
                changeHeader(changedSource, topic, prefix + "_change", oldReceipt, 120_000), 10_000)
                .get(15, TimeUnit.SECONDS);
            if (changed.getStatus() != AckStatus.OK || changed.getExtraInfo() == null
                || oldReceipt.equals(changed.getExtraInfo())) {
                throw new IllegalStateException("change-invisibility did not return a replacement receipt: " + changed);
            }
            requireAck(api.ackMessageAsync(brokerAddress, ackHeader(changedSource, topic,
                prefix + "_change", changed.getExtraInfo()), 10_000).get(15, TimeUnit.SECONDS), "replacement-receipt");
            System.out.println("P13\tchange-invisibility\tOK");
            System.out.println("P13\treplacement-receipt\tOK\tACKED");

            MessageExt queueZero = pop(api, brokerAddress, topic, prefix + "_mixed", 0, queueZeroBody, 60_000);
            MessageExt queueOne = pop(api, brokerAddress, topic, prefix + "_mixed", 1, queueOneBody, 60_000);
            int receiptZeroQueue = receiptQueueId(receipt(queueZero));
            int receiptOneQueue = receiptQueueId(receipt(queueOne));
            if (queueZero.getQueueId() != 0 || queueOne.getQueueId() != 1
                || receiptZeroQueue != 0 || receiptOneQueue != 1) {
                throw new IllegalStateException("POP receipt queue mapping did not preserve queue 0/1");
            }
            requireAck(api.ackMessageAsync(brokerAddress, ackHeader(queueZero, topic,
                prefix + "_mixed", receipt(queueZero)), 10_000).get(15, TimeUnit.SECONDS), "mixed-queue-0");
            requireAck(api.ackMessageAsync(brokerAddress, ackHeader(queueOne, topic,
                prefix + "_mixed", receipt(queueOne)), 10_000).get(15, TimeUnit.SECONDS), "mixed-queue-1");
            System.out.println("P13\tmixed-queue-mapping\tOK\t0,1");

            boolean malformedRejected = false;
            try {
                ExtraInfoUtil.getQueueOffset(ExtraInfoUtil.split("17 123 456 2 1 broker-a 4 nope"));
            } catch (RuntimeException expected) {
                malformedRejected = true;
            }
            if (!malformedRejected) {
                throw new IllegalStateException("Java accepted malformed POP extra-info");
            }
            System.out.println("P13\tmalformed-extra-info-rejected\tOK");
            System.out.println("P13\tpop-delivery\tOK\t5");
            System.out.println("P13\tack\tOK\t4");
        } finally {
            api.shutdown();
        }
    }

    private static MQClientAPIImpl producerApi() {
        ClientConfig config = new ClientConfig();
        config.setVipChannelEnabled(false);
        return new MQClientAPIImpl(new NettyClientConfig(), null, null, config);
    }

    private static MessageExt pop(MQClientAPIExt api, String brokerAddress, String topic,
        String group, int queueId, String expectedBody, long invisibleTime) throws Exception {
        PopMessageRequestHeader header = new PopMessageRequestHeader();
        header.setConsumerGroup(group);
        header.setTopic(topic);
        header.setQueueId(queueId);
        header.setInitMode(0);
        header.setMaxMsgNums(1);
        header.setInvisibleTime(invisibleTime);
        header.setPollTime(0);
        header.setBornTime(System.currentTimeMillis());
        header.setExpType("TAG");
        header.setExp("*");
        MessageQueue queue = new MessageQueue(topic, BROKER_NAME, queueId);
        printPopOffsetState(api, brokerAddress, topic, group, queueId, "before-min-query");
        long minOffset = api.getMinOffset(brokerAddress, queue, 10_000);
        printPopOffsetState(api, brokerAddress, topic, group, queueId, "after-min-query");
        long maxOffset = api.getMaxOffset(brokerAddress, queue, 10_000);
        printPopOffsetState(api, brokerAddress, topic, group, queueId, "after-max-query");
        String committedOffsetBefore = consumerOffset(api, brokerAddress, topic, group, queueId);
        System.err.printf("P13\tpop-offset-state\tphase=before-pop-request\tgroup=%s\tqueue=%d\toffset=%s%n",
            group, queueId, committedOffsetBefore);
        PopResult result = api.popMessageAsync(brokerAddress, BROKER_NAME, header, 10_000)
            .get(15, TimeUnit.SECONDS);
        if (result.getPopStatus() == null || !"FOUND".equals(result.getPopStatus().name())
            || result.getMsgFoundList() == null || result.getMsgFoundList().isEmpty()) {
            String committedOffsetAfter = consumerOffset(api, brokerAddress, topic, group, queueId);
            System.err.printf("P13\tpop-diagnostic\tgroup=%s\tqueue=%d\tinitMode=%d\tmin=%d\tmax=%d\tgroupOffsetBefore=%s\tgroupOffsetAfter=%s\tresult=%s%n",
                group, queueId, header.getInitMode(), minOffset, maxOffset,
                committedOffsetBefore, committedOffsetAfter, result);
            throw new IllegalStateException("POP did not find " + expectedBody + ": " + result);
        }
        MessageExt message = result.getMsgFoundList().get(0);
        if (!expectedBody.equals(new String(message.getBody(), StandardCharsets.UTF_8))) {
            throw new IllegalStateException("POP returned an unexpected body: " + Arrays.toString(message.getBody()));
        }
        System.err.printf("P13\tpop-found\tgroup=%s\tqueue=%d\tmessageOffset=%d%n",
            group, queueId, message.getQueueOffset());
        return message;
    }

    private static void printPopOffsetState(MQClientAPIImpl api, String brokerAddress, String topic,
        String group, int queueId, String phase) throws Exception {
        System.err.printf("P13\tpop-offset-state\tphase=%s\tgroup=%s\tqueue=%d\toffset=%s%n",
            phase, group, queueId, consumerOffset(api, brokerAddress, topic, group, queueId));
    }

    private static String consumerOffset(MQClientAPIImpl api, String brokerAddress, String topic,
        String group, int queueId) throws Exception {
        QueryConsumerOffsetRequestHeader header = new QueryConsumerOffsetRequestHeader();
        header.setConsumerGroup(group);
        header.setTopic(topic);
        header.setQueueId(queueId);
        header.setBrokerName(BROKER_NAME);
        header.setSetZeroIfNotFound(false);
        try {
            return Long.toString(api.queryConsumerOffset(brokerAddress, header, 10_000));
        } catch (OffsetNotFoundException notFound) {
            return "NOT_FOUND";
        }
    }

    private static String receipt(MessageExt message) {
        String value = message.getProperty(MessageConst.PROPERTY_POP_CK);
        if (value == null || value.isEmpty()) throw new IllegalStateException("POP result omitted its receipt");
        return value;
    }

    private static int receiptQueueId(String raw) {
        return ExtraInfoUtil.getQueueId(ExtraInfoUtil.split(raw));
    }

    private static AckMessageRequestHeader ackHeader(MessageExt message, String topic, String group, String token) {
        AckMessageRequestHeader header = new AckMessageRequestHeader();
        header.setConsumerGroup(group);
        header.setTopic(topic);
        header.setQueueId(message.getQueueId());
        header.setOffset(message.getQueueOffset());
        header.setExtraInfo(token);
        return header;
    }

    private static ChangeInvisibleTimeRequestHeader changeHeader(MessageExt message, String topic,
        String group, String token, long invisibleTime) {
        ChangeInvisibleTimeRequestHeader header = new ChangeInvisibleTimeRequestHeader();
        header.setConsumerGroup(group);
        header.setTopic(topic);
        header.setQueueId(message.getQueueId());
        header.setOffset(message.getQueueOffset());
        header.setExtraInfo(token);
        header.setInvisibleTime(invisibleTime);
        return header;
    }

    private static void requireAck(AckResult result, String operation) {
        if (result.getStatus() != AckStatus.OK) {
            throw new IllegalStateException(operation + " ACK returned " + result.getStatus());
        }
    }
}
