package compat;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;

/** Exercises consumer RPCs through the pinned Java 5.5.1 client API. */
public final class LiveConsumerProbe {
    private LiveConsumerProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 5 && "query".equals(args[0])) {
            query(args[1], args[2], args[3], Integer.parseInt(args[4]));
            return;
        }
        if (args.length == 6 && "update".equals(args[0])) {
            update(args[1], args[2], args[3], Integer.parseInt(args[4]), Long.parseLong(args[5]));
            return;
        }
        if (args.length == 6 && "pull".equals(args[0])) {
            pull(args[1], args[2], args[3], Integer.parseInt(args[4]), Long.parseLong(args[5]));
            return;
        }
        if (args.length == 7 && "lock".equals(args[0])) {
            lock(args[1], args[2], args[3], args[4], args[5], Integer.parseInt(args[6]));
            return;
        }
        if (args.length == 7 && "unlock".equals(args[0])) {
            unlock(args[1], args[2], args[3], args[4], args[5], Integer.parseInt(args[6]));
            return;
        }
        throw new IllegalArgumentException(
            "usage: LiveConsumerProbe query <broker-address> <group> <topic> <queue-id> | "
                + "update <broker-address> <group> <topic> <queue-id> <offset> | "
                + "pull <broker-address> <group> <topic> <queue-id> <queue-offset> | "
                + "lock|unlock <broker-address> <group> <client-id> <topic> <broker-name> <queue-id>");
    }

    private static MQClientAPIImpl newApi() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setVipChannelEnabled(false);
        return new MQClientAPIImpl(new NettyClientConfig(), null, null, clientConfig);
    }

    private static void query(String brokerAddress, String group, String topic, int queueId)
        throws Exception {
        MQClientAPIImpl api = newApi();
        api.start();
        try {
            QueryConsumerOffsetRequestHeader header = new QueryConsumerOffsetRequestHeader();
            header.setConsumerGroup(group);
            header.setTopic(topic);
            header.setQueueId(queueId);
            System.out.printf("OFFSET\t%d%n", api.queryConsumerOffset(brokerAddress, header, 10_000));
        } finally {
            api.shutdown();
        }
    }

    private static void update(
        String brokerAddress, String group, String topic, int queueId, long offset
    ) throws Exception {
        MQClientAPIImpl api = newApi();
        api.start();
        try {
            UpdateConsumerOffsetRequestHeader header = new UpdateConsumerOffsetRequestHeader();
            header.setConsumerGroup(group);
            header.setTopic(topic);
            header.setQueueId(queueId);
            header.setCommitOffset(offset);
            api.updateConsumerOffset(brokerAddress, header, 10_000);
            System.out.printf("OFFSET_UPDATED\t%d%n", offset);
        } finally {
            api.shutdown();
        }
    }

    private static void pull(
        String brokerAddress, String group, String topic, int queueId, long queueOffset
    ) throws Exception {
        MQClientAPIImpl api = newApi();
        api.start();
        try {
            PullMessageRequestHeader header = new PullMessageRequestHeader();
            header.setConsumerGroup(group);
            header.setTopic(topic);
            header.setQueueId(queueId);
            header.setQueueOffset(queueOffset);
            header.setMaxMsgNums(32);
            header.setSysFlag(PullSysFlag.buildSysFlag(false, false, true, false));
            header.setCommitOffset(0L);
            header.setSuspendTimeoutMillis(0L);
            header.setSubscription("*");
            header.setSubVersion(0L);
            header.setExpressionType("TAG");
            PullResult result = api.pullMessage(
                brokerAddress, header, 10_000, CommunicationMode.SYNC, null);
            if (!(result instanceof PullResultExt)) {
                throw new IllegalStateException("pull result did not include the Broker message body");
            }
            PullResultExt extended = (PullResultExt) result;
            byte[] binary = extended.getMessageBinary();
            String hex = binary == null ? "" : toHex(binary);
            System.out.printf("PULL\t%s\t%d\t%d\t%d\t%d\t%s%n",
                result.getPullStatus().name(), result.getNextBeginOffset(), result.getMinOffset(),
                result.getMaxOffset(), extended.getSuggestWhichBrokerId(), hex);
        } finally {
            api.shutdown();
        }
    }

    private static MessageQueue queue(String topic, String brokerName, int queueId) {
        return new MessageQueue(topic, brokerName, queueId);
    }

    private static void lock(
        String brokerAddress, String group, String clientId, String topic, String brokerName, int queueId
    ) throws Exception {
        MQClientAPIImpl api = newApi();
        api.start();
        try {
            MessageQueue messageQueue = queue(topic, brokerName, queueId);
            LockBatchRequestBody body = new LockBatchRequestBody();
            body.setConsumerGroup(group);
            body.setClientId(clientId);
            body.setOnlyThisBroker(true);
            body.setMqSet(java.util.Collections.singleton(messageQueue));
            boolean acquired = api.lockBatchMQ(brokerAddress, body, 10_000).contains(messageQueue);
            System.out.printf("LOCK\t%s\t%s\t%s\t%d%n", acquired, topic, brokerName, queueId);
        } finally {
            api.shutdown();
        }
    }

    private static void unlock(
        String brokerAddress, String group, String clientId, String topic, String brokerName, int queueId
    ) throws Exception {
        MQClientAPIImpl api = newApi();
        api.start();
        try {
            UnlockBatchRequestBody body = new UnlockBatchRequestBody();
            body.setConsumerGroup(group);
            body.setClientId(clientId);
            body.setOnlyThisBroker(true);
            body.setMqSet(java.util.Collections.singleton(queue(topic, brokerName, queueId)));
            api.unlockBatchMQ(brokerAddress, body, 10_000, false);
            System.out.println("UNLOCK\tOK");
        } finally {
            api.shutdown();
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }
}
