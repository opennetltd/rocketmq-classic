package compat;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

/** Exercises raw NameServer and verified TLS traffic against Phase 19 local services. */
public final class LiveP3RemotingProbe {
    private LiveP3RemotingProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 3 && args[0].equals("route")) {
            rawRoute(args[1], args[2]);
            return;
        }
        if (args.length == 4 && args[0].equals("tls")) {
            tls(args[1], args[2], args[3]);
            return;
        }
        throw new IllegalArgumentException(
            "usage: route <nameserver-address> <topic> | tls <broker-address> <ca-file> <trusted|wrong-host|untrusted>");
    }

    private static MQClientAPIExt api(ClientConfig config) {
        MQClientAPIExt api = new MQClientAPIExt(config, new NettyClientConfig(), null, null);
        api.start();
        return api;
    }

    private static void rawRoute(String nameserverAddress, String topic) throws Exception {
        System.setProperty("tls.enable", "false");
        ClientConfig config = new ClientConfig();
        config.setNamesrvAddr(nameserverAddress);
        MQClientAPIExt api = api(config);
        api.updateNameServerAddressList(nameserverAddress);
        TopicRouteData route;
        try {
            route = api.getTopicRouteInfoFromNameServer(topic, 10_000);
        } finally {
            api.shutdown();
        }
        assertCleanShutdown(api);
        if (route == null || route.getBrokerDatas() == null || route.getBrokerDatas().isEmpty()) {
            throw new IllegalStateException("raw route invoke returned no Broker route for " + topic);
        }
        System.out.printf("P3\tROUTE\tFOUND\t%d\t0\t0\t0\n", route.getBrokerDatas().size());
    }

    private static void tls(String address, String caFile, String mode) throws Exception {
        System.setProperty("tls.enable", "true");
        System.setProperty("tls.test.mode.enable", "false");
        System.setProperty("tls.client.authServer", "true");
        System.setProperty("tls.client.trustCertPath", caFile);
        ClientConfig config = new ClientConfig();
        MQClientAPIExt api = api(config);
        boolean accepted = false;
        try {
            HeartbeatData heartbeat = new HeartbeatData();
            heartbeat.setClientID("compat-p3-java-" + mode);
            ProducerData producer = new ProducerData();
            producer.setGroupName("compat_p3_tls");
            heartbeat.setProducerDataSet(Collections.singleton(producer));
            api.sendHeartbeat(address, heartbeat, 10_000);
            accepted = true;
        } catch (Throwable expectedForNegativeCases) {
            if (mode.equals("trusted")) {
                throw expectedForNegativeCases;
            }
        } finally {
            api.shutdown();
        }

        assertCleanShutdown(api);
        // RocketMQ Java 5.5.1 creates the outbound SslHandler without a peer host,
        // so its verified mode checks the CA chain but does not check the hostname.
        boolean expectedAccepted = mode.equals("trusted") || mode.equals("wrong-host");
        if (accepted != expectedAccepted) {
            throw new IllegalStateException("unexpected TLS outcome for " + mode + ": " + accepted);
        }
        System.out.printf("P3\tTLS\t%s\t%s\t0\t0\t0\n", mode,
            accepted ? "ACCEPTED" : "REJECTED");
    }

    private static void assertCleanShutdown(MQClientAPIImpl api) throws Exception {
        NettyRemotingClient remoting = (NettyRemotingClient) api.getRemotingClient();
        int channels = mapField(NettyRemotingClient.class, remoting, "channelTables").size();
        int pending = mapField(NettyRemotingAbstract.class, remoting, "responseTable").size();
        if (channels != 0 || pending != 0 || !executorsStopped(remoting)) {
            throw new IllegalStateException("shutdown retained resources: channels=" + channels
                + " pending=" + pending + " executorsStopped=" + executorsStopped(remoting));
        }
    }

    private static boolean executorsStopped(NettyRemotingClient client) throws Exception {
        ExecutorService publicExecutor = (ExecutorService) field(NettyRemotingClient.class,
            client, "publicExecutor");
        ExecutorService scanExecutor = (ExecutorService) field(NettyRemotingClient.class,
            client, "scanExecutor");
        ExecutorService callbackExecutor = (ExecutorService) field(NettyRemotingClient.class,
            client, "callbackExecutor");
        Object worker = field(NettyRemotingClient.class, client, "eventLoopGroupWorker");
        boolean workerStopped = (boolean) worker.getClass().getMethod("isTerminated").invoke(worker);
        return await(publicExecutor) && await(scanExecutor) && await(callbackExecutor) && workerStopped;
    }

    private static boolean await(ExecutorService executor) throws InterruptedException {
        return executor == null || executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private static Map<?, ?> mapField(Class<?> owner, Object target, String name) throws Exception {
        return (Map<?, ?>) field(owner, target, name);
    }

    private static Object field(Class<?> owner, Object target, String name) throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
