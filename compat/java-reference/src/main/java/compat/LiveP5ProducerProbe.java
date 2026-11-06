package compat;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

/** Exercises the Phase 5 producer RPC variants against a live Broker. */
public final class LiveP5ProducerProbe {
    private static final String PRODUCER_GROUP = "compat_p5_producer";
    private static final String BROKER_NAME = "broker-a";

    private LiveP5ProducerProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: LiveP5ProducerProbe <broker-address> <topic>");
        }
        String brokerAddress = args[0];
        String topic = args[1];
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(new NettyClientConfig(), null, null, clientConfig);
        api.start();
        Field smartSend = MQClientAPIImpl.class.getDeclaredField("sendSmartMsg");
        smartSend.setAccessible(true);
        boolean originalSmartSend = smartSend.getBoolean(null);
        try {
            MessageQueue queue = new MessageQueue(topic, BROKER_NAME, 0);
            long baseOffset = api.getMaxOffset(brokerAddress, queue, 10_000);
            System.out.printf("P5\tBASE\t%d%n", baseOffset);

            emit(api, smartSend, false, false, false, brokerAddress, topic, "send-v1");
            emit(api, smartSend, true, false, false, brokerAddress, topic, "send-v2");
            emit(api, smartSend, true, true, false, brokerAddress, topic, "batch");
            HeartbeatData heartbeat = new HeartbeatData();
            heartbeat.setClientID("compat-p5-client");
            ProducerData producer = new ProducerData();
            producer.setGroupName(PRODUCER_GROUP);
            heartbeat.getProducerDataSet().add(producer);
            ConsumerData consumer = new ConsumerData();
            consumer.setGroupName("compat_p5_reply_group");
            consumer.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
            consumer.setMessageModel(MessageModel.CLUSTERING);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            heartbeat.getConsumerDataSet().add(consumer);
            api.sendHeartbeat(brokerAddress, heartbeat, 10_000);
            emit(api, smartSend, false, false, true, brokerAddress, topic, "reply-v1");
            emit(api, smartSend, true, false, true, brokerAddress, topic, "reply-v2");

            EndTransactionRequestHeader end = new EndTransactionRequestHeader();
            end.setTopic(topic);
            end.setProducerGroup(PRODUCER_GROUP);
            end.setTranStateTableOffset(-1L);
            end.setCommitLogOffset(-1L);
            end.setCommitOrRollback(8);
            end.setFromTransactionCheck(false);
            end.setMsgId("compat-p5-end-transaction");
            end.setTransactionId("compat-p5-transaction");
            api.endTransactionOneway(brokerAddress, end, "compat-p5-end", 10_000);
            System.out.println("P5\tRESULT\tend-transaction\tONEWAY");

            System.out.printf("P5\tMAX\t%d%n", api.getMaxOffset(brokerAddress, queue, 10_000));
        } finally {
            smartSend.setBoolean(null, originalSmartSend);
            api.shutdown();
        }
    }

    private static void emit(
        MQClientAPIImpl api, Field smartSend, boolean smart, boolean batch, boolean reply,
        String brokerAddress, String topic, String method
    ) throws Exception {
        smartSend.setBoolean(null, smart);
        Message message = new Message(topic, ("p5-java-" + method).getBytes(StandardCharsets.UTF_8));
        message.setKeys("compat_p5_java_" + method);
        MessageClientIDSetter.setUniqID(message);
        if (reply) {
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_MESSAGE_TYPE, MixAll.REPLY_MESSAGE_FLAG);
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_CORRELATION_ID, "compat-p5-correlation-" + method);
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, "compat-p5-client");
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_MESSAGE_TTL, "60000");
        }

        Message payload = message;
        if (batch) {
            MessageBatch messageBatch = MessageBatch.generateFromList(Collections.singletonList(message));
            MessageClientIDSetter.setUniqID(messageBatch);
            messageBatch.setBody(messageBatch.encode());
            payload = messageBatch;
        }
        SendMessageRequestHeader header = new SendMessageRequestHeader();
        header.setProducerGroup(PRODUCER_GROUP);
        header.setTopic(topic);
        header.setDefaultTopic("TBW102");
        header.setDefaultTopicQueueNums(4);
        header.setQueueId(0);
        header.setSysFlag(0);
        header.setBornTimestamp(System.currentTimeMillis());
        header.setFlag(payload.getFlag());
        header.setProperties(MessageDecoder.messageProperties2String(payload.getProperties()));
        header.setReconsumeTimes(0);
        header.setUnitMode(false);
        header.setBatch(batch);
        header.setBrokerName(BROKER_NAME);
        CommunicationMode mode = reply ? CommunicationMode.ONEWAY : CommunicationMode.SYNC;
        SendResult result = api.sendMessage(
            brokerAddress, BROKER_NAME, payload, header, 10_000, mode, null, null);
        if (reply) {
            if (result != null) {
                throw new IllegalStateException(method + " one-way reply unexpectedly returned a result");
            }
            System.out.printf("P5\tRESULT\t%s\tONEWAY%n", method);
            return;
        }
        if (result == null || result.getOffsetMsgId() == null || result.getOffsetMsgId().isEmpty()) {
            throw new IllegalStateException(method + " returned an incomplete send result");
        }
        System.out.printf("P5\tRESULT\t%s\t%s\t%s\t%d\t%d%n",
            method, result.getSendStatus().name(), result.getMessageQueue().getTopic(),
            result.getMessageQueue().getQueueId(), result.getQueueOffset());
    }
}
