package compat;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData;

/** Exercises ACL and verified mutual-TLS traffic against Phase 19 local services. */
public final class LiveP16SecurityProbe {
    private LiveP16SecurityProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].startsWith("auth-")) {
            auth(args[1], args[0].substring("auth-".length()));
            return;
        }
        if (args.length == 5 && args[0].startsWith("tls-")) {
            tls(args[1], args[0].substring("tls-".length()), args[2], args[3], args[4]);
            return;
        }
        throw new IllegalArgumentException(
            "usage: auth-<valid|wrong-secret|tampered> <address> | "
                + "tls-<trusted|wrong-host|untrusted|missing-client> <address> <ca> <cert|-> <key|->");
    }

    private static MQClientAPIExt api(ClientConfig config, String accessKey, String secretKey) {
        MQClientAPIExt api = new MQClientAPIExt(config, new NettyClientConfig(), null,
            new AclClientRPCHook(new SessionCredentials(accessKey, secretKey)));
        api.start();
        return api;
    }

    private static void auth(String address, String mode) throws Exception {
        String accessKey = requiredEnv("P16_ACCESS_KEY");
        String secretKey = mode.equals("wrong-secret")
            ? "p16-deliberately-wrong-secret" : requiredEnv("P16_SECRET_KEY");
        System.setProperty("tls.enable", "false");
        MQClientAPIExt api = api(new ClientConfig(), accessKey, secretKey);
        boolean accepted = false;
        try {
            api.sendHeartbeat(address, heartbeat("compat-p16-java-" + mode), 10_000);
            accepted = true;
        } catch (Throwable expectedForNegativeCases) {
            if (mode.equals("valid")) {
                throw expectedForNegativeCases;
            }
        } finally {
            api.shutdown();
        }
        assertCleanShutdown(api);
        if (accepted != mode.equals("valid")) {
            throw new IllegalStateException("unexpected ACL outcome for " + mode + ": " + accepted);
        }
        System.out.printf("P16\tACL\t%s\t%s\t0\t0\t0\n", mode,
            accepted ? "ACCEPTED" : "REJECTED");
    }

    private static void tls(String address, String mode, String caFile, String certFile,
                            String keyFile) throws Exception {
        System.setProperty("tls.enable", "true");
        System.setProperty("tls.test.mode.enable", "false");
        System.setProperty("tls.client.authServer", "true");
        System.setProperty("tls.client.trustCertPath", caFile);
        if (!certFile.equals("-") && !keyFile.equals("-")) {
            System.setProperty("tls.client.certPath", certFile);
            System.setProperty("tls.client.keyPath", keyFile);
        }
        MQClientAPIExt api = api(new ClientConfig(), requiredEnv("P16_ACCESS_KEY"),
            requiredEnv("P16_SECRET_KEY"));
        boolean accepted = false;
        try {
            api.sendHeartbeat(address, heartbeat("compat-p16-java-tls-" + mode), 10_000);
            accepted = true;
        } catch (Throwable expectedForNegativeCases) {
            if (mode.equals("trusted") || mode.equals("wrong-host")) {
                throw expectedForNegativeCases;
            }
        } finally {
            api.shutdown();
        }
        assertCleanShutdown(api);
        // Pinned Java 5.5.1 checks the certificate chain but does not pass a
        // peer host to its outbound SslHandler, so trusted wrong-host is accepted.
        boolean expectedAccepted = mode.equals("trusted") || mode.equals("wrong-host");
        if (accepted != expectedAccepted) {
            throw new IllegalStateException("unexpected TLS outcome for " + mode + ": " + accepted);
        }
        System.out.printf("P16\tTLS\t%s\t%s\t0\t0\t0\n", mode,
            accepted ? "ACCEPTED" : "REJECTED");
    }

    private static HeartbeatData heartbeat(String clientId) {
        HeartbeatData heartbeat = new HeartbeatData();
        heartbeat.setClientID(clientId);
        ProducerData producer = new ProducerData();
        producer.setGroupName("compat_p16_security");
        heartbeat.setProducerDataSet(Collections.singleton(producer));
        return heartbeat;
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("missing required synthetic credential environment variable " + name);
        }
        return value;
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

    private static void assertCleanShutdown(MQClientAPIImpl api) throws Exception {
        NettyRemotingClient remoting = (NettyRemotingClient) api.getRemotingClient();
        int channels = mapField(NettyRemotingClient.class, remoting, "channelTables").size();
        int pending = mapField(NettyRemotingAbstract.class, remoting, "responseTable").size();
        if (channels != 0 || pending != 0 || !executorsStopped(remoting)) {
            throw new IllegalStateException("shutdown retained resources: channels=" + channels
                + " pending=" + pending + " executorsStopped=" + executorsStopped(remoting));
        }
    }
}
