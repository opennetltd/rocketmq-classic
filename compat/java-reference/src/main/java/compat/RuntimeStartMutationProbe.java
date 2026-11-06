package compat;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;

/**
 * Fork-only probe for post-construction NameServer startup branches. The child
 * halts after writing its observation because the pinned runtime starts
 * process-lifetime worker threads that can outlive shutdown.
 */
public final class RuntimeStartMutationProbe {
    private RuntimeStartMutationProbe() { }

    private static Map<String, Object> runCase(boolean nullAtConstruction) throws Exception {
        return runCase(nullAtConstruction, false);
    }

    private static Map<String, Object> runCase(boolean nullAtConstruction, boolean emptyBeforeStart)
        throws Exception {
        Map<String, Object> item = new TreeMap<>();
        ClientConfig config = new ClientConfig();
        config.setClientIP(nullAtConstruction ? "192.0.2.6" : "192.0.2.7");
        if (!nullAtConstruction) config.setNamesrvAddr("configured-at-construction:9876");
        MQClientInstance runtime = new MQClientInstance(config, nullAtConstruction ? 73 : 74,
            config.buildMQClientId());
        Field apiField = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        apiField.setAccessible(true);
        MQClientAPIImpl originalApi = (MQClientAPIImpl) apiField.get(runtime);
        List<String> initialAddresses = originalApi.getNameServerAddressList();
        StartProbeApi api = new StartProbeApi(config);
        if (initialAddresses != null) api.updateNameServerAddressList(String.join(";", initialAddresses));
        apiField.set(runtime, api);
        item.put("activeAddressesBeforeMutation", api.getNameServerAddressList());
        if (nullAtConstruction) {
            config.setNamesrvAddr("late-after-construction:9876");
        } else if (emptyBeforeStart) {
            config.setNamesrvAddr("");
        } else {
            config.setNamesrvAddr(null);
        }
        item.put("namesrvAddrAtStart", config.getNamesrvAddr());
        item.put("activeAddressesBeforeStart", api.getNameServerAddressList());

        Field executorField = MQClientInstance.class.getDeclaredField("scheduledExecutorService");
        executorField.setAccessible(true);
        ScheduledExecutorService original = (ScheduledExecutorService) executorField.get(runtime);
        original.shutdownNow();
        AtomicInteger scheduledCalls = new AtomicInteger();
        ScheduledExecutorService recorder = (ScheduledExecutorService) java.lang.reflect.Proxy
            .newProxyInstance(RuntimeStartMutationProbe.class.getClassLoader(),
                new Class<?>[] {ScheduledExecutorService.class}, (proxy, method, values) -> {
                    if ("scheduleAtFixedRate".equals(method.getName())) {
                        scheduledCalls.incrementAndGet();
                        return null;
                    }
                    if ("shutdownNow".equals(method.getName())) return new ArrayList<>();
                    if ("isShutdown".equals(method.getName()) || "isTerminated".equals(method.getName())) return false;
                    if ("awaitTermination".equals(method.getName())) return true;
                    if ("equals".equals(method.getName())) return proxy == values[0];
                    if ("hashCode".equals(method.getName())) return System.identityHashCode(proxy);
                    if ("toString".equals(method.getName())) return "start-probe scheduler";
                    return null;
                });
        executorField.set(runtime, recorder);

        try {
            runtime.start();
            item.put("startOutcome", "RUNNING");
        } catch (Throwable error) {
            item.put("startOutcome", error.getClass().getSimpleName());
        }
        item.put("discoveryCalls", api.discoveryCalls.get());
        item.put("scheduledTaskCount", scheduledCalls.get());
        item.put("activeAddressesAfterStart", api.getNameServerAddressList());
        item.put("serverBaselineContacted", false);
        try {
            runtime.shutdown();
            item.put("shutdownOutcome", "OK");
        } catch (Throwable error) {
            item.put("shutdownOutcome", error.getClass().getSimpleName());
        }
        return item;
    }

    private static final class StartProbeApi extends MQClientAPIImpl {
        final AtomicInteger discoveryCalls = new AtomicInteger();

        StartProbeApi(ClientConfig config) {
            super(new NettyClientConfig(), null, null, config);
        }

        @Override
        public String fetchNameServerAddr() {
            discoveryCalls.incrementAndGet();
            return null;
        }

        @Override
        public void start() { }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("expected fixture output path");
        Map<String, Object> item = new TreeMap<>();
        item.put("nullToConfigured", runCase(true));
        item.put("configuredToNull", runCase(false));
        item.put("configuredToEmpty", runCase(false, true));
        for (Object value : item.values()) {
            @SuppressWarnings("unchecked") Map<String, Object> result = (Map<String, Object>) value;
            if (!"RUNNING".equals(result.get("startOutcome")) || !"OK".equals(result.get("shutdownOutcome"))) {
                System.err.println("runtime start/shutdown probe failed: " + item);
                System.err.flush();
                Runtime.getRuntime().halt(1);
            }
        }

        String json = com.alibaba.fastjson2.JSON.toJSONString(item,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path file = Path.of(args[0]);
        boolean verify = Boolean.parseBoolean(System.getProperty("start.fixture.verify", "true"));
        if (verify) {
            if (!json.equals(Files.readString(file))) {
                System.err.println("fixture drift: " + file + "\n" + json);
                System.err.flush();
                Runtime.getRuntime().halt(1);
            }
        } else {
            Files.createDirectories(file.getParent());
            Files.writeString(file, json);
        }
        System.out.println("Runtime start mutation probe verified");
        System.out.flush();
        Runtime.getRuntime().halt(0);
    }
}
