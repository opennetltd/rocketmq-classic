package compat;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;

/** Exercises sustained producer sends and repeated live start/shutdown cycles. */
public final class LiveProducerLifecycleProbe {
    private static final int CYCLES = 3;
    private static final int SENDS_PER_CYCLE = 20;

    private LiveProducerLifecycleProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 3 && "delete-topic".equals(args[0])) {
            deleteTopic(args[1], args[2]);
            return;
        }
        if (args.length != 2) {
            throw new IllegalArgumentException(
                "usage: <nameserver-address> <topic> | delete-topic <broker-address> <topic>");
        }
        for (int cycle = 0; cycle < CYCLES; cycle++) {
            runCycle(args[0], args[1], cycle);
        }
    }

    private static void deleteTopic(String brokerAddress, String topic) throws Exception {
        MQClientAPIImpl api = new MQClientAPIImpl(new NettyClientConfig(), null, null, new ClientConfig());
        api.start();
        try {
            api.deleteTopicInBroker(brokerAddress, topic, 10_000);
        } finally {
            api.shutdown();
        }
        System.out.printf("P7L\tTOPIC_DELETED\t%s\t%s%n", brokerAddress, topic);
    }

    private static void runCycle(String namesrvAddress, String topic, int cycle) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("compat_p7_lifecycle_java_" + cycle);
        producer.setNamesrvAddr(namesrvAddress);
        producer.setSendMessageWithVIPChannel(false);
        producer.setSendMsgTimeout(10_000);
        producer.start();

        MQClientInstance factory = producer.getDefaultMQProducerImpl().getmQClientFactory();
        MQClientAPIImpl api = factory.getMQClientAPIImpl();
        NettyRemotingClient remoting = (NettyRemotingClient) api.getRemotingClient();
        ExecutorService producerExecutor = producer.getDefaultMQProducerImpl().getAsyncSenderExecutor();
        ExecutorService runtimeScheduler = factory.getScheduledExecutorService();

        try {
            for (int sequence = 0; sequence < SENDS_PER_CYCLE; sequence++) {
                Message message = new Message(topic,
                    ("p7-lifecycle-" + cycle + "-" + sequence).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                SendResult result = producer.send(message);
                if (result == null || !"SEND_OK".equals(result.getSendStatus().name())) {
                    throw new IllegalStateException("lifecycle send did not return SEND_OK");
                }
            }
        } finally {
            producer.shutdown();
        }

        int channels = mapField(NettyRemotingClient.class, remoting, "channelTables").size();
        int pending = mapField(NettyRemotingAbstract.class, remoting, "responseTable").size();
        boolean producerStopped = await(producerExecutor);
        boolean schedulerStopped = await(runtimeScheduler);
        boolean remotingStopped = executorsStopped(remoting);
        int tasks = (producerStopped ? 0 : 1) + (schedulerStopped ? 0 : 1)
            + (remotingStopped ? 0 : 1);
        if (channels != 0 || pending != 0 || tasks != 0) {
            throw new IllegalStateException("producer shutdown retained resources: channels=" + channels
                + " pending=" + pending + " ownedTaskGroups=" + tasks
                + " producerExecutorStopped=" + producerStopped
                + " runtimeSchedulerStopped=" + schedulerStopped
                + " remotingExecutorsStopped=" + remotingStopped);
        }
        System.out.printf("P7L\tCYCLE\tjava\t%d\t%d\t%d\t%d\t%d%n",
            cycle, SENDS_PER_CYCLE, pending, channels, tasks);
    }

    private static boolean executorsStopped(NettyRemotingClient client) throws Exception {
        ExecutorService publicExecutor = (ExecutorService) field(NettyRemotingClient.class,
            client, "publicExecutor");
        ExecutorService scanExecutor = (ExecutorService) field(NettyRemotingClient.class,
            client, "scanExecutor");
        ExecutorService callbackExecutor = (ExecutorService) field(NettyRemotingClient.class,
            client, "callbackExecutor");
        ExecutorService worker = (ExecutorService) field(NettyRemotingClient.class,
            client, "eventLoopGroupWorker");
        return await(publicExecutor) && await(scanExecutor) && await(callbackExecutor) && await(worker);
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
