package compat;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData;

/** Sends one Java 5.5.1 heartbeat to the pinned local Broker. */
public final class LiveHeartbeatProbe {
    private LiveHeartbeatProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                "usage: LiveHeartbeatProbe <broker-address> <producer-group>");
        }

        HeartbeatData heartbeat = new HeartbeatData();
        heartbeat.setClientID("p19-live-heartbeat");
        ProducerData producer = new ProducerData();
        producer.setGroupName(args[1]);
        heartbeat.getProducerDataSet().add(producer);

        MQClientAPIImpl api = new MQClientAPIImpl(
            new NettyClientConfig(), null, null, new ClientConfig());
        api.start();
        try {
            int version = api.sendHeartbeat(args[0], heartbeat, 10_000);
            System.out.printf("HEARTBEAT\t%d\t%s%n", version,
                toHex(heartbeat.encode()));
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
