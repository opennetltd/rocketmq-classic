package compat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;

/** Exercises synchronous send and physical-offset readback through Java 5.5.1. */
public final class LiveProducerProbe {
    private LiveProducerProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 4 && "send".equals(args[0])) {
            send(args[1], args[2], args[3]);
            return;
        }
        if (args.length == 5 && "send-lite".equals(args[0])) {
            sendLite(args[1], args[2], args[3], args[4]);
            return;
        }
        if (args.length == 7 && "timeout-send".equals(args[0])) {
            timeoutSend(args[1], args[2], args[3], args[4], Integer.parseInt(args[5]), args[6]);
            return;
        }
        if (args.length == 4 && "async-send".equals(args[0])) {
            asyncSend(args[1], args[2], args[3]);
            return;
        }
        if (args.length == 6 && "oneway-send".equals(args[0])) {
            onewaySend(args[1], args[2], args[3], args[4], Long.parseLong(args[5]));
            return;
        }
        if (args.length == 5 && "batch-send".equals(args[0])) {
            batchSend(args[1], args[2], args[3], Integer.parseInt(args[4]));
            return;
        }
        if (args.length == 4 && "view".equals(args[0])) {
            view(args[1], args[2], args[3]);
            return;
        }
        if (args.length == 4 && "view-batch".equals(args[0])) {
            viewBatch(args[1], args[2], args[3]);
            return;
        }
        throw new IllegalArgumentException(
            "usage: LiveProducerProbe send|send-lite|async-send <broker-address> <topic> <body> | timeout-send <proxy-address> <topic> <producer-group> <body> <timeout-ms> <go-file> | oneway-send <broker-address> <topic> <body> <key> <minimum-max-offset> | batch-send <broker-address> <topic> <body-prefix> <message-count> | view <broker-address> <topic> <offset-message-id> | view-batch <broker-address> <topic> <comma-separated-offset-message-ids>");
    }

    private static MQClientAPIImpl newApi() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setVipChannelEnabled(false);
        return new MQClientAPIImpl(new NettyClientConfig(), null, null, clientConfig);
    }

    private static void send(String brokerAddress, String topic, String body) throws Exception {
        send(brokerAddress, topic, null, body);
    }

    private static void sendLite(String brokerAddress, String topic, String liteTopic, String body) throws Exception {
        send(brokerAddress, topic, liteTopic, body);
    }

    private static void send(String brokerAddress, String topic, String liteTopic, String body) throws Exception {
        MQClientAPIImpl api = newApi();
        api.start();
        try {
            Message message = new Message(topic, body.getBytes(StandardCharsets.UTF_8));
            if (liteTopic != null) {
                MessageAccessor.putProperty(message, MessageConst.PROPERTY_LITE_TOPIC, liteTopic);
            }
            SendMessageRequestHeader header = new SendMessageRequestHeader();
            header.setProducerGroup("compat_phase0");
            header.setTopic(topic);
            header.setDefaultTopic("TBW102");
            header.setDefaultTopicQueueNums(4);
            header.setQueueId(0);
            header.setSysFlag(0);
            header.setBornTimestamp(System.currentTimeMillis());
            header.setFlag(message.getFlag());
            header.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));
            header.setReconsumeTimes(0);
            header.setUnitMode(false);
            header.setBatch(false);
            header.setBrokerName("broker-a");
            SendResult result = api.sendMessage(brokerAddress, "broker-a", message, header,
                10_000, CommunicationMode.SYNC, null, null);
            long physicalOffset = MessageDecoder.decodeMessageId(result.getOffsetMsgId()).getOffset();
            System.out.printf("SEND\t%s\t%s\t%d\t%d\t%s\t%d%n",
                result.getSendStatus().name(), result.getMessageQueue().getTopic(),
                result.getMessageQueue().getQueueId(), result.getQueueOffset(),
                result.getOffsetMsgId(), physicalOffset);
        } finally {
            api.shutdown();
        }
    }

    private static void timeoutSend(
        String brokerAddress, String topic, String producerGroup, String body,
        int timeoutMillis, String goFile
    ) throws Exception {
        MQClientAPIImpl api = newApi();
        api.start();
        try {
            MessageQueue queue = new MessageQueue(topic, "broker-a", 0);
            long initialOffset = api.getMaxOffset(brokerAddress, queue, 5_000);
            System.out.printf("TIMEOUT\tREADY\tjava\t%d%n", initialOffset);
            awaitGoFile(goFile);

            Message message = new Message(topic, body.getBytes(StandardCharsets.UTF_8));
            message.setKeys("compat_timeout_java");
            MessageClientIDSetter.setUniqID(message);
            SendMessageRequestHeader header = new SendMessageRequestHeader();
            header.setProducerGroup(producerGroup);
            header.setTopic(topic);
            header.setDefaultTopic("TBW102");
            header.setDefaultTopicQueueNums(4);
            header.setQueueId(0);
            header.setSysFlag(0);
            header.setBornTimestamp(System.currentTimeMillis());
            header.setFlag(message.getFlag());
            header.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));
            header.setReconsumeTimes(0);
            header.setUnitMode(false);
            header.setBatch(false);
            header.setBrokerName("broker-a");

            long started = System.nanoTime();
            Exception sendError = null;
            try {
                api.sendMessage(brokerAddress, "broker-a", message, header, timeoutMillis,
                    CommunicationMode.SYNC, null, null);
            } catch (Exception error) {
                sendError = error;
            }
            if (sendError == null) {
                throw new IllegalStateException("Broker response did not exceed the send deadline");
            }
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            if (!isTimeout(sendError)) {
                throw sendError;
            }
            System.out.printf("TIMEOUT\tRESULT\tjava\tTIMEOUT\t%s\t%d\t%d%n",
                sendError.getClass().getSimpleName(), elapsedMillis, initialOffset);
        } finally {
            api.shutdown();
        }
    }

    private static void awaitGoFile(String goFile) throws Exception {
        Path path = Path.of(goFile);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            if (Files.exists(path)) {
                return;
            }
            Thread.sleep(10);
        }
        throw new IllegalStateException("timed out waiting for the delayed-response gate");
    }

    private static boolean isTimeout(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            String description = cause.getClass().getSimpleName() + " " + cause.getMessage();
            String lower = description.toLowerCase(java.util.Locale.ROOT);
            if (lower.contains("timeout") || lower.contains("deadline expired")) {
                return true;
            }
        }
        return false;
    }

    private static void asyncSend(String brokerAddress, String topic, String body) throws Exception {
        MQClientAPIImpl api = newApi();
        api.start();
        try {
            Message message = new Message(topic, body.getBytes(StandardCharsets.UTF_8));
            SendMessageRequestHeader header = new SendMessageRequestHeader();
            header.setProducerGroup("compat_phase0");
            header.setTopic(topic);
            header.setDefaultTopic("TBW102");
            header.setDefaultTopicQueueNums(4);
            header.setQueueId(0);
            header.setSysFlag(0);
            header.setBornTimestamp(System.currentTimeMillis());
            header.setFlag(message.getFlag());
            header.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));
            header.setReconsumeTimes(0);
            header.setUnitMode(false);
            header.setBatch(false);
            header.setBrokerName("broker-a");

            CountDownLatch completed = new CountDownLatch(1);
            AtomicInteger callbackCount = new AtomicInteger();
            AtomicReference<SendResult> sendResult = new AtomicReference<>();
            AtomicReference<Throwable> sendFailure = new AtomicReference<>();
            SendCallback callback = new SendCallback() {
                @Override
                public void onSuccess(SendResult result) {
                    callbackCount.incrementAndGet();
                    sendResult.set(result);
                    completed.countDown();
                }

                @Override
                public void onException(Throwable error) {
                    callbackCount.incrementAndGet();
                    sendFailure.set(error);
                    completed.countDown();
                }
            };
            // The direct API callback updates producer fault state. Construct its
            // implementation without starting NameServer discovery or routing.
            DefaultMQProducerImpl producer = new DefaultMQProducerImpl(
                new DefaultMQProducer("compat_phase0"));
            api.sendMessage(brokerAddress, "broker-a", message, header, 10_000,
                CommunicationMode.ASYNC, callback, null, null, 0, null, producer);

            if (!completed.await(15, TimeUnit.SECONDS)) {
                throw new IllegalStateException("asynchronous send callback timed out");
            }
            if (callbackCount.get() != 1) {
                throw new IllegalStateException("asynchronous send callback count was " + callbackCount.get());
            }
            if (sendFailure.get() != null) {
                throw new Exception("asynchronous send failed", sendFailure.get());
            }
            SendResult result = sendResult.get();
            if (result == null) {
                throw new IllegalStateException("asynchronous send callback returned no result");
            }
            long physicalOffset = MessageDecoder.decodeMessageId(result.getOffsetMsgId()).getOffset();
            System.out.printf("ASYNC_CALLBACK\t%d\t%s\t%s\t%d\t%d\t%s\t%d%n",
                callbackCount.get(), result.getSendStatus().name(),
                result.getMessageQueue().getTopic(), result.getMessageQueue().getQueueId(),
                result.getQueueOffset(), result.getOffsetMsgId(), physicalOffset);
        } finally {
            api.shutdown();
        }
    }

    private static void onewaySend(
        String brokerAddress, String topic, String body, String key, long minimumMaxOffset)
        throws Exception {
        MQClientAPIImpl api = newApi();
        api.start();
        try {
            Message message = new Message(topic, body.getBytes(StandardCharsets.UTF_8));
            message.setKeys(key);
            MessageAccessor.putProperty(
                message, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, key + "_uniq");
            SendMessageRequestHeader header = new SendMessageRequestHeader();
            header.setProducerGroup("compat_phase0");
            header.setTopic(topic);
            header.setDefaultTopic("TBW102");
            header.setDefaultTopicQueueNums(4);
            header.setQueueId(0);
            header.setSysFlag(0);
            header.setBornTimestamp(System.currentTimeMillis());
            header.setFlag(message.getFlag());
            header.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));
            header.setReconsumeTimes(0);
            header.setUnitMode(false);
            header.setBatch(false);
            header.setBrokerName("broker-a");

            SendResult result = api.sendMessage(brokerAddress, "broker-a", message, header,
                10_000, CommunicationMode.ONEWAY, null, null);
            if (result != null) {
                throw new IllegalStateException("oneway send unexpectedly returned a Broker result");
            }
            System.out.printf("ONEWAY_RETURN\tNO_RESPONSE\t%s\t%s%n", topic, key);
            long maxOffset = waitForQueueMaxOffset(api, brokerAddress, topic, minimumMaxOffset);
            System.out.printf("QUEUE_MAX_OFFSET\t%d%n", maxOffset);
        } finally {
            api.shutdown();
        }
    }

    private static long waitForQueueMaxOffset(
        MQClientAPIImpl api, String brokerAddress, String topic, long minimumMaxOffset)
        throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            long maxOffset = api.getMaxOffset(
                brokerAddress, new MessageQueue(topic, "broker-a", 0), 5_000);
            if (maxOffset >= minimumMaxOffset) {
                return maxOffset;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException(
            "oneway message did not reach the Broker queue before the deadline");
    }

    private static void batchSend(
        String brokerAddress, String topic, String bodyPrefix, int messageCount) throws Exception {
        if (messageCount < 1) {
            throw new IllegalArgumentException("batch message count must be positive");
        }
        MQClientAPIImpl api = newApi();
        api.start();
        try {
            List<Message> messages = new ArrayList<>(messageCount);
            for (int index = 0; index < messageCount; index++) {
                Message message = new Message(
                    topic, (bodyPrefix + "-" + index).getBytes(StandardCharsets.UTF_8));
                message.setKeys("compat_phase0_batch_java_" + index);
                MessageClientIDSetter.setUniqID(message);
                messages.add(message);
            }
            MessageBatch batch = MessageBatch.generateFromList(messages);
            MessageClientIDSetter.setUniqID(batch);
            batch.setBody(batch.encode());

            SendMessageRequestHeader header = new SendMessageRequestHeader();
            header.setProducerGroup("compat_phase0");
            header.setTopic(topic);
            header.setDefaultTopic("TBW102");
            header.setDefaultTopicQueueNums(4);
            header.setQueueId(0);
            header.setSysFlag(0);
            header.setBornTimestamp(System.currentTimeMillis());
            header.setFlag(batch.getFlag());
            header.setProperties(MessageDecoder.messageProperties2String(batch.getProperties()));
            header.setReconsumeTimes(0);
            header.setUnitMode(false);
            header.setBatch(true);
            header.setBrokerName("broker-a");

            SendResult result = api.sendMessage(brokerAddress, "broker-a", batch, header,
                10_000, CommunicationMode.SYNC, null, null);
            String firstOffsetMessageId = result.getOffsetMsgId().split(",", 2)[0];
            long physicalOffset = MessageDecoder.decodeMessageId(firstOffsetMessageId).getOffset();
            System.out.printf("BATCH_SEND\t%s\t%s\t%d\t%d\t%s\t%s\t%d\t%d%n",
                result.getSendStatus().name(), result.getMessageQueue().getTopic(),
                result.getMessageQueue().getQueueId(), result.getQueueOffset(),
                result.getOffsetMsgId(), firstOffsetMessageId, physicalOffset, messageCount);
        } finally {
            api.shutdown();
        }
    }

    private static void view(String brokerAddress, String topic, String offsetMessageId)
        throws Exception {
        long physicalOffset = MessageDecoder.decodeMessageId(offsetMessageId).getOffset();
        MQClientAPIImpl api = newApi();
        api.start();
        try {
            MessageExt message = api.viewMessage(brokerAddress, topic, physicalOffset, 10_000);
            System.out.printf("MESSAGE\t%s\t%d\t%d\t%s%n", message.getTopic(),
                message.getQueueId(), message.getQueueOffset(), toHex(message.getBody()));
        } finally {
            api.shutdown();
        }
    }

    private static void viewBatch(String brokerAddress, String topic, String offsetMessageIds)
        throws Exception {
        String[] messageIds = offsetMessageIds.split(",", -1);
        if (messageIds.length == 0) {
            throw new IllegalArgumentException("batch readback requires physical offset IDs");
        }
        for (String messageId : messageIds) {
            if (messageId.isEmpty()) {
                throw new IllegalArgumentException("batch readback requires every physical offset ID");
            }
        }
        MQClientAPIImpl api = newApi();
        api.start();
        try {
            for (String messageId : messageIds) {
                long physicalOffset = MessageDecoder.decodeMessageId(messageId).getOffset();
                MessageExt message = api.viewMessage(brokerAddress, topic, physicalOffset, 10_000);
                System.out.printf("MESSAGE\t%s\t%d\t%d\t%s%n", message.getTopic(),
                    message.getQueueId(), message.getQueueOffset(), toHex(message.getBody()));
            }
        } finally {
            api.shutdown();
        }
    }

    private static String toHex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] encoded = new char[bytes.length * 2];
        for (int index = 0; index < bytes.length; index++) {
            int value = bytes[index] & 0xff;
            encoded[index * 2] = digits[value >>> 4];
            encoded[index * 2 + 1] = digits[value & 0x0f];
        }
        return new String(encoded);
    }
}
