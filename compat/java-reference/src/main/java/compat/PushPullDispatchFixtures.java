package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.PullAPIWrapper;
import org.apache.rocketmq.client.impl.consumer.PullMessageService;
import org.apache.rocketmq.client.impl.consumer.PullRequest;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;

/** Invokes pinned Push/Pull methods without a Broker, sockets or running client services. */
public final class PushPullDispatchFixtures {
    private PushPullDispatchFixtures() {}

    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("pull-parity");
        DefaultMQPushConsumerImpl impl = consumer.getDefaultMQPushConsumerImpl();
        Factory factory = allocate(Factory.class);
        factory.api = allocate(Api.class);
        factory.service = new Service();
        factory.slave = true;
        factory.commit = 42L;
        try {
            impl.setServiceState(ServiceState.RUNNING);
            impl.setmQClientFactory(factory);
            Field wrapper = DefaultMQPushConsumerImpl.class.getDeclaredField("pullAPIWrapper");
            wrapper.setAccessible(true);
            wrapper.set(impl, new PullAPIWrapper(factory, "pull-parity", false));
            impl.setOffsetStore((OffsetStore) Proxy.newProxyInstance(OffsetStore.class.getClassLoader(),
                new Class<?>[] {OffsetStore.class}, (proxy, method, values) -> {
                    if ("readOffset".equals(method.getName())) { return factory.commit; }
                    return null;
                }));
            impl.getRebalanceImpl().getSubscriptionInner().put("orders",
                FilterAPI.buildSubscriptionData("orders", "*"));
            for (int id = 0; id < 80; id++) {
                impl.pullMessage(request(id));
            }
            require(factory.api.calls == 80, "all queues dispatch before any response");
            require(factory.api.mode == CommunicationMode.ASYNC, "Push selects ASYNC");
            require(factory.api.timeout == 30000L, "Pull RPC gets 30 s after route refresh");
            require(factory.refreshes == 1, "missing route refreshed before RPC");
            require(factory.api.header.getSuspendTimeoutMillis() == 15000L, "15 s suspend");
            require(factory.api.header.getCommitOffset() == 42L, "slave retains commitOffset");
            require(!PullSysFlag.hasCommitOffsetFlag(factory.api.header.getSysFlag()),
                "slave clears only commit flag");

            factory.commit = -1L;
            impl.pullMessage(request(81));
            long missingCommit = factory.api.header.getCommitOffset();
            require(missingCommit == -1L && !PullSysFlag.hasCommitOffsetFlag(factory.api.header.getSysFlag()),
                "missing memory offset stays -1 with commit disabled");
            factory.commit = 42L;

            consumer.setConsumeConcurrentlyMaxSpan(1);
            PullRequest spaced = request(80);
            MessageExt first = message(0);
            MessageExt last = message(100);
            spaced.getProcessQueue().putMessage(java.util.List.of(first, last));
            int before = factory.api.calls;
            impl.pullMessage(spaced);
            require(factory.api.calls == before && factory.service.delay == 50L,
                "concurrent span flow control");
            impl.setConsumeOrderly(true);
            spaced.getProcessQueue().setLocked(true);
            spaced.setPreviouslyLocked(true);
            impl.pullMessage(spaced);
            require(factory.api.calls == before + 1, "orderly ignores concurrent max span");
            consumer.setPullThresholdForQueue(1);
            impl.pullMessage(spaced);
            require(factory.api.calls == before + 1 && factory.service.delay == 50L,
                "orderly retains cache-count limit");

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("javaClientVersion", "5.5.1");
            result.put("outstandingQueuesBeforeAnyResponse", 80);
            result.put("communicationMode", factory.api.mode.name());
            result.put("brokerSuspendMillis", factory.api.header.getSuspendTimeoutMillis());
            result.put("rpcTimeoutMillisAfterRouteRefresh", factory.api.timeout);
            result.put("missingMemoryCommitOffset", missingCommit);
            result.put("slaveCommitOffset", factory.api.header.getCommitOffset());
            result.put("slaveCommitFlag", PullSysFlag.hasCommitOffsetFlag(factory.api.header.getSysFlag()));
            result.put("concurrentMaxSpanDelayMillis", factory.service.delay);
            result.put("orderlyIgnoresConcurrentMaxSpan", true);
            result.put("orderlyRetainsCacheCountLimit", true);
            String json = JSON.toJSONString(result) + "\n";
            Path path = Path.of(PushPullDispatchFixtures.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                .resolve("../../../../fixtures/consumer/push-pull-dispatch.json").normalize();
            if (args.length == 1 && "--write".equals(args[0])) {
                Files.writeString(path, json);
            } else {
                require(JSON.parseObject(Files.readString(path)).equals(JSON.parseObject(json)),
                    "pinned Push Pull fixture drift");
            }
            System.out.println("Pinned Java Push Pull dispatch fixtures PASS (80 outstanding queues)");
        } finally {
            factory.service.getScheduledExecutorService().shutdownNow();
        }
    }

    private static PullRequest request(int id) {
        PullRequest request = new PullRequest();
        request.setConsumerGroup("pull-parity");
        request.setMessageQueue(new MessageQueue("orders", "broker", id));
        request.setProcessQueue(new ProcessQueue());
        request.setNextOffset(0);
        return request;
    }

    private static MessageExt message(long offset) {
        MessageExt message = new MessageExt();
        message.setQueueOffset(offset);
        message.setBody(new byte[] {1});
        return message;
    }

    private static void require(boolean condition, String message) {
        if (!condition) { throw new AssertionError(message); }
    }

    // Match the existing offline fixtures: skip networking constructors of recording subclasses.
    private static <T> T allocate(Class<T> type) throws Exception {
        Class<?> unsafe = Class.forName("sun.misc.Unsafe");
        Field field = unsafe.getDeclaredField("theUnsafe");
        field.setAccessible(true);
        return type.cast(unsafe.getMethod("allocateInstance", Class.class).invoke(field.get(null), type));
    }

    private static final class Factory extends MQClientInstance {
        Api api;
        Service service;
        boolean slave;
        long commit;
        int refreshes;
        private Factory() { super(new ClientConfig(), 1, "offline-pull-parity"); }
        @Override public String getBrokerNameFromMessageQueue(MessageQueue queue) { return queue.getBrokerName(); }
        @Override public FindBrokerResult findBrokerAddressInSubscribe(String broker, long id, boolean only) {
            return refreshes == 0 ? null : new FindBrokerResult("127.0.0.1:10911", slave, 999);
        }
        @Override public boolean updateTopicRouteInfoFromNameServer(String topic) { refreshes++; return true; }
        @Override public MQClientAPIImpl getMQClientAPIImpl() { return api; }
        @Override public PullMessageService getPullMessageService() { return service; }
    }

    private static final class Api extends MQClientAPIImpl {
        int calls;
        long timeout;
        CommunicationMode mode;
        PullMessageRequestHeader header;
        private Api() { super(null, null, null, null, null, null); }
        @Override public PullResult pullMessage(String address, PullMessageRequestHeader request,
            long millis, CommunicationMode communication, PullCallback callback) {
            calls++;
            header = request;
            timeout = millis;
            mode = communication;
            return null; // Hold every callback; later queues must still dispatch.
        }
    }

    private static final class Service extends PullMessageService {
        long delay;
        private Service() { super(null); }
        @Override public void executePullRequestLater(PullRequest request, long millis) { delay = millis; }
    }
}
