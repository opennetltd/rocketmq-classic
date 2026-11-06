package compat;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;

/** Exercises raw consumer RPCs through the pinned Java 5.5.1 client API. */
public final class LiveP5ConsumerProbe {
    private LiveP5ConsumerProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                "usage: LiveP5ConsumerProbe <broker-address> <group> <topic> <seed-body>");
        }
        run(args[0], args[1], args[2], args[3]);
    }

    private static void run(String brokerAddress, String group, String topic, String seedBody)
        throws Exception {
        String clientId = "compat-p5-java-consumer";
        MQClientAPIImpl api = new MQClientAPIImpl(
            new NettyClientConfig(), null, null, new ClientConfig());
        api.start();
        try {
            HeartbeatData heartbeat = new HeartbeatData();
            heartbeat.setClientID(clientId);
            ConsumerData consumer = new ConsumerData();
            consumer.setGroupName(group);
            consumer.setConsumeType(ConsumeType.CONSUME_ACTIVELY);
            consumer.setMessageModel(MessageModel.CLUSTERING);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.setSubscriptionDataSet(Collections.singleton(new SubscriptionData(topic, "*")));
            heartbeat.setConsumerDataSet(Collections.singleton(consumer));
            api.sendHeartbeat(brokerAddress, heartbeat, 10_000);

            MessageExt selected = null;
            PullResult ordinary = pull(api, brokerAddress, group, topic, false);
            if (!(ordinary instanceof PullResultExt)) {
                throw new IllegalStateException("ordinary pull omitted message bytes");
            }
            List<MessageExt> ordinaryMessages = decodeMessages(ordinary);
            if (ordinaryMessages != null) {
                for (MessageExt message : ordinaryMessages) {
                    if (seedBody.equals(new String(message.getBody(), java.nio.charset.StandardCharsets.UTF_8))) {
                        selected = message;
                    }
                }
            }
            if (!"FOUND".equals(ordinary.getPullStatus().name()) || selected == null) {
                throw new IllegalStateException("ordinary pull did not return this client's seed message");
            }
            System.out.printf("P5C\tPULL\t%s\t%d\t%d\t%d%n", ordinary.getPullStatus().name(),
                ordinaryMessages.size(), ordinary.getMinOffset(), ordinary.getMaxOffset());

            PullResult lite = pull(api, brokerAddress, group, topic, true);
            List<MessageExt> liteMessages = decodeMessages(lite);
            if (!"FOUND".equals(lite.getPullStatus().name()) || liteMessages.isEmpty()) {
                throw new IllegalStateException("lite pull did not return the seeded queue messages");
            }
            System.out.printf("P5C\tLITE_PULL\t%s\t%d\t%d\t%d%n", lite.getPullStatus().name(),
                liteMessages.size(), lite.getMinOffset(), lite.getMaxOffset());

            UpdateConsumerOffsetRequestHeader update = new UpdateConsumerOffsetRequestHeader();
            update.setConsumerGroup(group);
            update.setTopic(topic);
            update.setQueueId(0);
            update.setCommitOffset(1L);
            api.updateConsumerOffset(brokerAddress, update, 10_000);
            System.out.println("P5C\tUPDATE_OFFSET\t1");

            QueryConsumerOffsetRequestHeader query = new QueryConsumerOffsetRequestHeader();
            query.setConsumerGroup(group);
            query.setTopic(topic);
            query.setQueueId(0);
            long queried = api.queryConsumerOffset(brokerAddress, query, 10_000);
            if (queried != 1L) {
                throw new IllegalStateException("query offset returned " + queried + " after update");
            }
            System.out.printf("P5C\tQUERY_OFFSET\t%d%n", queried);

            List<String> consumerIds = api.getConsumerIdListByGroup(brokerAddress, group, 10_000);
            if (consumerIds == null || !consumerIds.contains(clientId)) {
                throw new IllegalStateException("consumer-list omitted this client's registered identity");
            }
            System.out.println("P5C\tCONSUMER_LIST\tPRESENT");

            api.consumerSendMessageBack(brokerAddress, "broker-a", selected, group, 3, 10_000, 16);
            System.out.println("P5C\tSEND_BACK\tOK");

            MessageQueue queue = new MessageQueue(topic, "broker-a", 0);
            LockBatchRequestBody lock = new LockBatchRequestBody();
            lock.setConsumerGroup(group);
            lock.setClientId(clientId);
            lock.setOnlyThisBroker(true);
            lock.setMqSet(Collections.singleton(queue));
            if (!api.lockBatchMQ(brokerAddress, lock, 10_000).contains(queue)) {
                throw new IllegalStateException("lock response omitted the requested queue");
            }
            System.out.println("P5C\tLOCK\ttrue");

            UnlockBatchRequestBody unlock = new UnlockBatchRequestBody();
            unlock.setConsumerGroup(group);
            unlock.setClientId(clientId);
            unlock.setOnlyThisBroker(true);
            unlock.setMqSet(Collections.singleton(queue));
            api.unlockBatchMQ(brokerAddress, unlock, 10_000, false);
            System.out.println("P5C\tUNLOCK\tOK");
        } finally {
            api.shutdown();
        }
    }

    private static PullResult pull(
        MQClientAPIImpl api, String brokerAddress, String group, String topic, boolean lite
    ) throws Exception {
        PullMessageRequestHeader header = new PullMessageRequestHeader();
        header.setConsumerGroup(group);
        header.setTopic(topic);
        header.setQueueId(0);
        header.setQueueOffset(0L);
        header.setMaxMsgNums(32);
        header.setSysFlag(PullSysFlag.buildSysFlag(false, false, true, false, lite));
        header.setCommitOffset(0L);
        header.setSuspendTimeoutMillis(0L);
        header.setSubscription("*");
        header.setSubVersion(0L);
        header.setExpressionType("TAG");
        header.setBrokerName("broker-a");
        return api.pullMessage(brokerAddress, header, 10_000, CommunicationMode.SYNC, null);
    }

    private static List<MessageExt> decodeMessages(PullResult result) {
        if (!(result instanceof PullResultExt)) {
            throw new IllegalStateException("pull result did not include the Broker message body");
        }
        byte[] binary = ((PullResultExt) result).getMessageBinary();
        return binary == null ? Collections.emptyList()
            : MessageDecoder.decodesBatch(ByteBuffer.wrap(binary), true, true, true);
    }
}
