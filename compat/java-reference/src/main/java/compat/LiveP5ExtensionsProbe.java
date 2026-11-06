package compat;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.lite.LiteSubscriptionAction;
import org.apache.rocketmq.common.lite.LiteSubscriptionDTO;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.remoting.common.HeartbeatV2Result;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/** Exercises the pinned Java extension RPCs against an isolated live Broker. */
public final class LiveP5ExtensionsProbe {
    private static final String BROKER_NAME = "broker-a";

    private LiveP5ExtensionsProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                "usage: LiveP5ExtensionsProbe prepare|consume|lite-pop <broker-address> <topic> <group-prefix>");
        }
        String mode = args[0];
        String brokerAddress = args[1];
        String topic = args[2];
        String groupPrefix = args[3];
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setVipChannelEnabled(false);
        MQClientAPIExt api = new MQClientAPIExt(clientConfig, new NettyClientConfig(), null, null);
        api.start();
        try {
            if ("prepare".equals(mode)) {
                prepare(api, brokerAddress, topic, groupPrefix);
            } else if ("consume".equals(mode)) {
                consume(api, brokerAddress, topic, groupPrefix);
            } else if ("lite-pop".equals(mode)) {
                litePop(api, brokerAddress, topic, groupPrefix);
            } else {
                throw new IllegalArgumentException("unknown mode: " + mode);
            }
        } finally {
            api.shutdown();
        }
    }

    private static void prepare(MQClientAPIExt api, String brokerAddress, String topic,
        String groupPrefix) throws Exception {
        String clientId = "compat-p5-java-" + groupPrefix;
        String baseGroup = groupPrefix + "_base";
        String liteGroup = groupPrefix + "_lite";
        HeartbeatData heartbeat = new HeartbeatData();
        heartbeat.setClientID(clientId);
        ConsumerData consumer = new ConsumerData();
        consumer.setGroupName(baseGroup);
        consumer.setConsumeType(ConsumeType.CONSUME_ACTIVELY);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setSubscriptionDataSet(Collections.singleton(new SubscriptionData(topic, "*")));
        heartbeat.setConsumerDataSet(Collections.singleton(consumer));

        int version = api.sendHeartbeat(brokerAddress, heartbeat, 10_000);
        System.out.printf("P5E\theartbeat\tOK\t%d%n", version);
        HeartbeatV2Result v2 = api.sendHeartbeatV2(brokerAddress, heartbeat, 10_000);
        System.out.printf("P5E\theartbeat-v2\tOK\t%d\t%b\t%b%n",
            v2.getVersion(), v2.isSubChange(), v2.isSupportV2());

        Set<MessageQueueAssignment> assignments = api.queryAssignment(brokerAddress, topic,
            baseGroup, clientId, "AVG", MessageModel.CLUSTERING, 10_000);
        System.out.printf("P5E\tassignment\tOK\t%d%n", assignments == null ? 0 : assignments.size());

        api.setMessageRequestMode(brokerAddress, topic, baseGroup, MessageRequestMode.POP, 1, 10_000);
        System.out.println("P5E\trequest-mode\tOK");

        LiteSubscriptionDTO lite = new LiteSubscriptionDTO()
            .setAction(LiteSubscriptionAction.COMPLETE_ADD)
            .setClientId(clientId)
            .setGroup(liteGroup)
            .setTopic(topic)
            .setLiteTopicSet(Collections.singleton("entries"))
            .setVersion(0);
        try {
            api.syncLiteSubscriptionAsync(brokerAddress, lite, 10_000).get(15, TimeUnit.SECONDS);
            System.out.println("P5E\tlite-subscription\tOK");
        } catch (ExecutionException error) {
            if (!isUnsupportedRequest(error, 200071)) {
                throw error;
            }
            System.out.println("P5E\tlite-subscription\tUNSUPPORTED\t200071\t3");
        }

        api.unregisterClient(brokerAddress, clientId, null, baseGroup, 10_000);
        System.out.println("P5E\tunregister\tOK");
    }

    private static void litePop(MQClientAPIExt api, String brokerAddress, String topic,
        String groupPrefix) throws Exception {
        String clientId = "compat-p5-java-" + groupPrefix;
        MessageExt lite = pop(api, brokerAddress, topic, groupPrefix + "_lite", clientId, true,
            "p5-extension-lite");
        System.out.printf("P5E\tpop-lite\tFOUND\t%s%n", hex(lite.getBody()));
    }

    private static void consume(MQClientAPIExt api, String brokerAddress, String topic,
        String groupPrefix) throws Exception {
        String clientId = "compat-p5-java-" + groupPrefix;
        MessageExt message = pop(api, brokerAddress, topic, groupPrefix + "_pop", clientId, false,
            "p5-extension-seed");
        System.out.printf("P5E\tpop\tFOUND\t%s%n", hex(message.getBody()));

        String liteTopic = "%LMQ%" + topic + "%entries";
        try {
            MessageExt lite = pop(api, brokerAddress, liteTopic, groupPrefix + "_lite", clientId, true,
                "p5-extension-lite");
            System.out.printf("P5E\tpop-lite\tFOUND\t%s%n", hex(lite.getBody()));
        } catch (ExecutionException error) {
            if (!isUnsupportedRequest(error, 200070)) {
                throw error;
            }
            System.out.println("P5E\tpop-lite\tUNSUPPORTED\t200070\t3");
        }

        String ackGroup = groupPrefix + "_ack";
        MessageExt ackMessage = pop(api, brokerAddress, topic, ackGroup, clientId, false,
            "p5-extension-seed");
        AckResult ack = api.ackMessageAsync(brokerAddress, ackHeader(ackMessage, topic, ackGroup),
            10_000).get(15, TimeUnit.SECONDS);
        System.out.printf("P5E\tack\t%s%n", ack.getStatus().name());

        String batchGroup = groupPrefix + "_batch_ack";
        MessageExt batchMessage = pop(api, brokerAddress, topic, batchGroup, clientId, false,
            "p5-extension-seed");
        String batchReceipt = batchMessage.getProperty(MessageConst.PROPERTY_POP_CK);
        AckResult batch = api.batchAckMessageAsync(brokerAddress, topic, batchGroup,
            Collections.singletonList(batchReceipt), 10_000).get(15, TimeUnit.SECONDS);
        System.out.printf("P5E\tbatch-ack\t%s%n", batch.getStatus().name());

        String changeGroup = groupPrefix + "_change";
        MessageExt changeMessage = pop(api, brokerAddress, topic, changeGroup, clientId, false,
            "p5-extension-seed");
        ChangeInvisibleTimeRequestHeader change = changeHeader(changeMessage, topic, changeGroup);
        AckResult changed = api.changeInvisibleTimeAsync(brokerAddress, BROKER_NAME, change, 10_000)
            .get(15, TimeUnit.SECONDS);
        System.out.printf("P5E\tchange-invisible-time\t%s\tMETADATA%n",
            changed.getStatus().name());
    }

    private static MessageExt pop(MQClientAPIExt api, String brokerAddress, String topic,
        String group, String clientId, boolean lite, String expectedBody) throws Exception {
        PopResult result;
        long bornTime = System.currentTimeMillis();
        if (lite) {
            PopLiteMessageRequestHeader header = new PopLiteMessageRequestHeader();
            header.setClientId(clientId);
            header.setConsumerGroup(group);
            header.setTopic(topic);
            header.setMaxMsgNum(1);
            header.setInvisibleTime(60_000);
            header.setPollTime(10_000);
            header.setBornTime(bornTime);
            result = api.popLiteMessageAsync(brokerAddress, BROKER_NAME, header, 15_000)
                .get(20, TimeUnit.SECONDS);
        } else {
            PopMessageRequestHeader header = new PopMessageRequestHeader();
            header.setConsumerGroup(group);
            header.setTopic(topic);
            header.setQueueId(0);
            header.setMaxMsgNums(1);
            header.setInvisibleTime(60_000);
            header.setPollTime(0);
            header.setBornTime(bornTime);
            header.setExpType("TAG");
            header.setExp("*");
            result = api.popMessageAsync(brokerAddress, BROKER_NAME, header, 15_000)
                .get(20, TimeUnit.SECONDS);
        }
        if (result.getPopStatus() == null || !"FOUND".equals(result.getPopStatus().name())
            || result.getMsgFoundList() == null || result.getMsgFoundList().isEmpty()) {
            throw new IllegalStateException("POP did not find the seeded message: " + result);
        }
        MessageExt selected = result.getMsgFoundList().get(0);
        if (!expectedBody.equals(new String(selected.getBody(), java.nio.charset.StandardCharsets.UTF_8))) {
            throw new IllegalStateException("POP returned an unexpected body for " + topic);
        }
        return selected;
    }

    private static AckMessageRequestHeader ackHeader(MessageExt message, String topic, String group) {
        AckMessageRequestHeader header = new AckMessageRequestHeader();
        header.setConsumerGroup(group);
        header.setTopic(topic);
        header.setQueueId(message.getQueueId());
        header.setOffset(message.getQueueOffset());
        header.setExtraInfo(message.getProperty(MessageConst.PROPERTY_POP_CK));
        return header;
    }

    private static ChangeInvisibleTimeRequestHeader changeHeader(
        MessageExt message, String topic, String group
    ) {
        ChangeInvisibleTimeRequestHeader header = new ChangeInvisibleTimeRequestHeader();
        header.setConsumerGroup(group);
        header.setTopic(topic);
        header.setQueueId(message.getQueueId());
        header.setOffset(message.getQueueOffset());
        header.setExtraInfo(message.getProperty(MessageConst.PROPERTY_POP_CK));
        header.setInvisibleTime(120_000L);
        return header;
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte item : value) result.append(String.format("%02x", item));
        return result.toString();
    }

    private static boolean isUnsupportedRequest(ExecutionException error, int requestCode) {
        Throwable cause = error.getCause();
        if (!(cause instanceof MQBrokerException)) {
            return false;
        }
        MQBrokerException brokerError = (MQBrokerException) cause;
        return brokerError.getResponseCode() == 3
            && ("request type " + requestCode + " not supported").equals(
                brokerError.getErrorMessage().trim());
    }
}
