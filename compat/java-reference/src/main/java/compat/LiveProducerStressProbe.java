package compat;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;

/** Runs a fixed-seed, bounded concurrent send workload against the disposable Broker. */
public final class LiveProducerStressProbe {
    private static final long MAX_RUN_MILLIS = 120_000;

    private LiveProducerStressProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException(
                "usage: <nameserver-address> <topic> <messages> <max-inflight> <seed> <start-offset>");
        }
        run(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]),
            Integer.parseInt(args[4]), Long.parseLong(args[5]));
    }

    private static void run(
        String namesrvAddress,
        String topic,
        int count,
        int maxInflight,
        int seed,
        long startOffset
    ) throws Exception {
        if (count <= 0 || maxInflight <= 0 || startOffset < 0) {
            throw new IllegalArgumentException("stress inputs must be positive and bounded");
        }

        List<byte[]> bodies = new ArrayList<>(count);
        long payloadBytes = 0;
        long payloadHash = 0xcbf29ce484222325L;
        for (int sequence = 0; sequence < count; sequence++) {
            byte[] body = body(seed, sequence);
            bodies.add(body);
            payloadBytes += body.length;
            for (byte value : body) {
                payloadHash ^= value & 0xffL;
                payloadHash *= 0x100000001b3L;
            }
        }

        DefaultMQProducer producer = new DefaultMQProducer("compat_p7_stress_java");
        producer.setNamesrvAddr(namesrvAddress);
        producer.setSendMessageWithVIPChannel(false);
        producer.setSendMsgTimeout(15_000);
        producer.setDefaultTopicQueueNums(4);
        producer.start();

        DefaultMQProducerImpl producerImpl = producer.getDefaultMQProducerImpl();
        MQClientInstance factory = producerImpl.getmQClientFactory();
        MQClientAPIImpl api = factory.getMQClientAPIImpl();
        NettyRemotingClient remoting = (NettyRemotingClient) api.getRemotingClient();
        ExecutorService workers = Executors.newFixedThreadPool(maxInflight);
        ExecutorCompletionService<Long> completed = new ExecutorCompletionService<>(workers);
        MessageQueue queue = new MessageQueue(topic, "broker-a", 0);
        Set<Long> offsets = new HashSet<>(count);
        long started = System.nanoTime();
        try {
            for (int sequence = 0; sequence < count; sequence++) {
                final int index = sequence;
                completed.submit(() -> {
                    Message message = new Message(topic, bodies.get(index));
                    message.setKeys("compat_p7_stress_java_" + index);
                    SendResult result = producer.send(message, queue);
                    if (result == null || !"SEND_OK".equals(result.getSendStatus().name())
                        || result.getMessageQueue().getQueueId() != 0
                        || !"broker-a".equals(result.getMessageQueue().getBrokerName())) {
                        throw new IllegalStateException("stress send returned an unexpected Broker result");
                    }
                    return result.getQueueOffset();
                });
            }

            long deadline = started + TimeUnit.MILLISECONDS.toNanos(MAX_RUN_MILLIS);
            for (int sequence = 0; sequence < count; sequence++) {
                long remaining = deadline - System.nanoTime();
                Future<Long> future = completed.poll(remaining, TimeUnit.NANOSECONDS);
                if (future == null) {
                    throw new IllegalStateException("bounded stress workload exceeded its 120-second deadline");
                }
                long offset = future.get();
                if (!offsets.add(offset)) {
                    throw new IllegalStateException("stress workload returned a duplicate queue offset: " + offset);
                }
            }
        } finally {
            workers.shutdownNow();
            producer.shutdown();
        }

        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        for (int sequence = 0; sequence < count; sequence++) {
            if (!offsets.contains(startOffset + sequence)) {
                throw new IllegalStateException("stress workload omitted queue offset " + (startOffset + sequence));
            }
        }

        int pending = mapField(NettyRemotingAbstract.class, remoting, "responseTable").size();
        int connections = mapField(NettyRemotingClient.class, remoting, "channelTables").size();
        boolean producerStopped = await(producerImpl.getAsyncSenderExecutor());
        boolean schedulerStopped = await(factory.getScheduledExecutorService());
        boolean remotingStopped = executorsStopped(remoting);
        int tasks = (producerStopped ? 0 : 1) + (schedulerStopped ? 0 : 1)
            + (remotingStopped ? 0 : 1);
        if (pending != 0 || connections != 0 || tasks != 0) {
            throw new IllegalStateException("stress shutdown retained resources: pending=" + pending
                + " connections=" + connections + " ownedTaskGroups=" + tasks);
        }
        System.out.printf("P7S\tLOAD\tjava\t%d\t%d\t%d\t%016x\t%d\t%d\t%d\t%d\t%d\t%d%n",
            count, maxInflight, payloadBytes, payloadHash, elapsedMillis,
            startOffset, startOffset + count - 1L, pending, connections, tasks);
    }

    private static byte[] body(int seed, int sequence) {
        int state = seed ^ ((sequence + 1) * 0x9e3779b9);
        state = next(state);
        int length = 256 + (state & 0x3ff);
        byte[] body = new byte[length];
        for (int index = 0; index < body.length; index++) {
            state = next(state);
            body[index] = (byte) state;
        }
        return body;
    }

    private static int next(int state) {
        state ^= state << 13;
        state ^= state >>> 17;
        return state ^ (state << 5);
    }

    private static boolean executorsStopped(NettyRemotingClient client) throws Exception {
        return await((ExecutorService) field(NettyRemotingClient.class, client, "publicExecutor"))
            && await((ExecutorService) field(NettyRemotingClient.class, client, "scanExecutor"))
            && await((ExecutorService) field(NettyRemotingClient.class, client, "callbackExecutor"))
            && await((ExecutorService) field(NettyRemotingClient.class, client, "eventLoopGroupWorker"));
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
