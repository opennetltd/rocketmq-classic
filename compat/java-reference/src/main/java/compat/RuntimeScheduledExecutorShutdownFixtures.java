package compat;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;

/** Invoked Java 5.5.1 ScheduledExecutorService shutdownNow() behavior. */
public final class RuntimeScheduledExecutorShutdownFixtures {
    private RuntimeScheduledExecutorShutdownFixtures() { }

    private static Map<String, Object> shutdownCase(
        String name, ScheduledExecutorService executor) {
        AtomicInteger calls = new AtomicInteger();
        ScheduledFuture<?> first = executor.schedule(calls::incrementAndGet, 1, TimeUnit.DAYS);
        ScheduledFuture<?> second = executor.schedule(calls::incrementAndGet, 1, TimeUnit.DAYS);
        List<Runnable> returned = executor.shutdownNow();

        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("queuedTaskCount", returned.size());
        item.put("returnsFirstFuture", returned.contains(first));
        item.put("returnsSecondFuture", returned.contains(second));
        item.put("firstCancelled", first.isCancelled());
        item.put("secondCancelled", second.isCancelled());
        item.put("firstDone", first.isDone());
        item.put("secondDone", second.isDone());
        item.put("callbacksRun", calls.get());
        return item;
    }

    public static void main(String[] args) throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("scheduled-shutdown-fixture");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        try {
            List<Object> cases = new ArrayList<>();
            cases.add(shutdownCase("client-runtime-scheduler", runtime.getScheduledExecutorService()));
            cases.add(shutdownCase("pull-message-service-scheduler",
                runtime.getPullMessageService().getScheduledExecutorService()));

            Map<String, Object> fixture = new TreeMap<>();
            fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
            fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
            fixture.put("entrypoint", "MQClientInstance.getScheduledExecutorService/shutdownNow; PullMessageService.getScheduledExecutorService/shutdownNow");
            fixture.put("serverBaselineContacted", false);
            fixture.put("cases", cases);

            String json = com.alibaba.fastjson2.JSON.toJSONString(fixture) + "\n";
            Path file = Path.of(args[0]);
            if (args.length > 1 && "--verify".equals(args[1])) {
                if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
            } else {
                Files.createDirectories(file.getParent());
                Files.writeString(file, json);
            }
            System.out.println("Runtime scheduled-executor shutdown cases verified: " + cases.size());
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod(
                "cleanupAfterConstructionFailure", MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, original, new IllegalStateException("scheduler fixture teardown"));
        }
    }
}
