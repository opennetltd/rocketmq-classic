package compat;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;

/** Direct public runtime operations, without starting transport or workers. */
public final class RuntimeConsumerOperationsFixtures {
    private RuntimeConsumerOperationsFixtures() { }

    public static void main(String[] args) throws Exception {
        List<Object> runningInfo = new ArrayList<>();
        for (String name : new String[] {"active-no-addresses", "passive-empty-addresses",
            "active-address-list", "missing-group", "null-group", "null-snapshot",
            "null-properties", "null-consume-type", "snapshot-throws"}) {
            runningInfo.add(runningInfo(name));
        }
        List<Object> direct = new ArrayList<>();
        for (String name : new String[] {"concurrent-plain-unique", "orderly-plain-unique", "concurrent-plain-id",
            "concurrent-null-topic", "orderly-null-topic", "concurrent-empty-topic",
            "concurrent-client-unique", "orderly-client-unique", "concurrent-client-no-unique", "orderly-client-no-unique",
            "concurrent-success", "concurrent-later", "concurrent-null-status",
            "concurrent-throws", "concurrent-null-broker", "orderly-success", "orderly-commit",
            "orderly-rollback", "orderly-later", "orderly-null-status", "push-null-message",
            "push-null-service", "missing-group", "pull-group", "other-group", "null-group",
            "concurrent-custom-subtype", "concurrent-client-custom-subtype",
            "concurrent-client-custom-subtype-unique"}) {
            direct.add(direct(name));
        }
        List<Object> reset = new ArrayList<>();
        for (String name : new String[] {"missing-group", "pull-group", "null-group", "empty-push-null-inputs",
            "null-topic-active", "null-map-active", "null-map-other-topic", "null-value", "null-key-only",
            "unassigned-offset", "success", "per-queue-failure", "live-map-update", "live-new-queue", "orderly-success"}) {
            reset.add(reset(name));
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("serverBaselineContacted", false);
        fixture.put("runningInfoEntrypoint", "MQClientInstance.consumerRunningInfo(String)");
        fixture.put("runningInfo", runningInfo);
        fixture.put("directConsumeEntrypoint", "MQClientInstance.consumeMessageDirectly(MessageExt,String,String)");
        fixture.put("directConsume", direct);
        fixture.put("customSubtypeProcessQueue", customSubtypeProcessQueue());
        fixture.put("derivedGetterSubtype", derivedGetterSubtype());
        fixture.put("derivedGetterNullSubtype", derivedGetterNullSubtype());
        fixture.put("priorityPropertySubtype", priorityPropertySubtype());
        fixture.put("resetOffsetEntrypoint", "MQClientInstance.resetOffset(String,String,Map<MessageQueue,Long>)");
        fixture.put("resetOffset", reset);
        fixture.put("resetStoreHandoff", reset("store-handoff"));
        fixture.put("unsubscribedTruncation", reset("rebalance-unsubscribed"));
        fixture.put("resume", resume());
        fixture.put("pullRebalance", pullRebalance());
        fixture.put("popRebalance", popRebalance());
        fixture.put("popUnsubscribedTruncation", popUnsubscribedTruncation());
        fixture.put("popToPullTransition", popToPullTransition());
        fixture.put("popRetryTransitions", popRetryTransitions());
        fixture.put("pullMaxIdleTimeMillis", Long.parseLong(
            System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000")));
        fixture.put("rebalanceRetention", List.of(reset("subscribed-null-value-missing-route"),
            reset("subscribed-null-value-assigned"), reset("subscribed-pull-expired")));
        fixture.put("resetOffsetSynchronized", java.lang.reflect.Modifier.isSynchronized(
            MQClientInstance.class.getMethod("resetOffset", String.class, String.class, Map.class).getModifiers()));
        String json = JSON.toJSONString(fixture, JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path path = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(path))) throw new IllegalStateException("fixture drift: " + path);
        } else {
            Files.createDirectories(path.getParent()); Files.writeString(path, json);
        }
        System.out.println("Direct runtime consumer operations verified: " + runningInfo.size()
            + " Running Info, " + direct.size() + " direct-consume, " + reset.size() + " reset cases");
    }

    private static List<Object> resume() {
        var selected = new org.apache.rocketmq.client.consumer.DefaultMQPushConsumer("selected").getDefaultMQPushConsumerImpl();
        var sibling = new org.apache.rocketmq.client.consumer.DefaultMQPushConsumer("sibling").getDefaultMQPushConsumerImpl();
        var queue = new org.apache.rocketmq.common.message.MessageQueue("orders", "broker-a", 0);
        sibling.getRebalanceImpl().getProcessQueueTable().put(queue, new org.apache.rocketmq.client.impl.consumer.ProcessQueue());
        List<Object> rows = new ArrayList<>();
        for (int round = 0; round < 2; round++) {
            var processQueue = new org.apache.rocketmq.client.impl.consumer.ProcessQueue();
            selected.getRebalanceImpl().getProcessQueueTable().put(queue, processQueue);
            if (round == 0) selected.suspend();
            selected.resume();
            Map<String, Object> row = new TreeMap<>();
            row.put("present", selected.getRebalanceImpl().getProcessQueueTable().containsKey(queue));
            row.put("dropped", processQueue.isDropped());
            row.put("paused", selected.isPause());
            row.put("siblingPresent", sibling.getRebalanceImpl().getProcessQueueTable().containsKey(queue));
            rows.add(row);
        }
        return rows;
    }

    private static List<Object> popRebalance() throws Exception {
        ClientConfig config = new ClientConfig(); config.setClientIP("pop-rebalance-runtime"); config.setNamesrvAddr(null);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        try {
            var facade = new org.apache.rocketmq.client.consumer.DefaultMQPushConsumer("pop-rebalance");
            facade.setMessageModel(org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.BROADCASTING);
            var consumer = new org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl(facade, null);
            var rebalance = consumer.getRebalanceImpl();
            consumer.setmQClientFactory(runtime);
            rebalance.setmQClientFactory(runtime);
            var queue = new org.apache.rocketmq.common.message.MessageQueue("orders", "broker-a", 0);
            var table = rebalance.getPopProcessQueueTable();
            var expired = new org.apache.rocketmq.client.impl.consumer.PopProcessQueue();
            expired.setLastPopTimestamp(System.currentTimeMillis() - 120_001L);
            table.put(queue, expired);

            var assignment = new org.apache.rocketmq.common.message.MessageQueueAssignment();
            assignment.setMessageQueue(queue);
            assignment.setMode(org.apache.rocketmq.common.message.MessageRequestMode.POP);
            var update = org.apache.rocketmq.client.impl.consumer.RebalanceImpl.class
                .getDeclaredMethod("updateMessageQueueAssignment", String.class, java.util.Set.class, boolean.class);
            update.setAccessible(true);
            update.invoke(rebalance, "orders", java.util.Set.of(assignment), false);
            var replacement = table.get(queue);
            Map<String, Object> expiredRow = new TreeMap<>();
            expiredRow.put("case", "expired-assigned"); expiredRow.put("oldDropped", expired.isDropped());
            expiredRow.put("present", replacement != null); expiredRow.put("replaced", replacement != expired);

            replacement.setLastPopTimestamp(System.currentTimeMillis());
            update.invoke(rebalance, "orders", java.util.Set.of(assignment), false);
            Map<String, Object> retainedRow = new TreeMap<>();
            retainedRow.put("case", "active-assigned");
            retainedRow.put("dropped", replacement.isDropped());
            retainedRow.put("present", table.containsKey(queue));
            retainedRow.put("same", table.get(queue) == replacement);

            replacement.setDropped(true);
            update.invoke(rebalance, "orders", java.util.Set.of(assignment), false);
            Map<String, Object> droppedRow = new TreeMap<>();
            droppedRow.put("case", "dropped-assigned"); droppedRow.put("dropped", replacement.isDropped());
            droppedRow.put("present", table.containsKey(queue)); droppedRow.put("same", table.get(queue) == replacement);

            update.invoke(rebalance, "orders", java.util.Set.of(), false);
            Map<String, Object> emptyRow = new TreeMap<>();
            emptyRow.put("case", "empty-assignment"); emptyRow.put("dropped", replacement.isDropped());
            emptyRow.put("present", table.containsKey(queue));
            return List.of(expiredRow, retainedRow, droppedRow, emptyRow);
        } finally {
            runtime.getPullMessageService().shutdown(true);
            runtime.getScheduledExecutorService().shutdownNow();
            runtime.getMQClientAPIImpl().shutdown();
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure", MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true); cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("POP rebalance fixture teardown"));
        }
    }

    private static List<Object> pullRebalance() throws Exception {
        ClientConfig config = new ClientConfig(); config.setClientIP("pull-rebalance-runtime"); config.setNamesrvAddr(null);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        try {
            var facade = new org.apache.rocketmq.client.consumer.DefaultMQPushConsumer("pull-rebalance");
            facade.setMessageModel(org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.BROADCASTING);
            facade.setConsumeFromWhere(org.apache.rocketmq.common.consumer.ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            var consumer = new org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl(facade, null);
            var rebalance = consumer.getRebalanceImpl();
            consumer.setmQClientFactory(runtime);
            rebalance.setmQClientFactory(runtime);
            consumer.setOffsetStore((org.apache.rocketmq.client.consumer.store.OffsetStore) Proxy.newProxyInstance(
                RuntimeConsumerOperationsFixtures.class.getClassLoader(),
                new Class<?>[] {org.apache.rocketmq.client.consumer.store.OffsetStore.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("readOffset")) return -1L;
                    if (method.getName().equals("cloneOffsetTable")) return Map.of();
                    return null;
                }));

            var queue = new org.apache.rocketmq.common.message.MessageQueue("orders", "broker-a", 0);
            var table = rebalance.getProcessQueueTable();
            var expired = new org.apache.rocketmq.client.impl.consumer.ProcessQueue();
            expired.setLastPullTimestamp(System.currentTimeMillis() - 120_001L);
            table.put(queue, expired);

            var assignment = new org.apache.rocketmq.common.message.MessageQueueAssignment();
            assignment.setMessageQueue(queue);
            assignment.setMode(org.apache.rocketmq.common.message.MessageRequestMode.PULL);
            var update = org.apache.rocketmq.client.impl.consumer.RebalanceImpl.class
                .getDeclaredMethod("updateMessageQueueAssignment", String.class, java.util.Set.class, boolean.class);
            update.setAccessible(true);
            update.invoke(rebalance, "orders", java.util.Set.of(assignment), false);
            var replacement = table.get(queue);
            Map<String, Object> expiredRow = new TreeMap<>();
            expiredRow.put("case", "expired-assigned"); expiredRow.put("oldDropped", expired.isDropped());
            expiredRow.put("present", replacement != null); expiredRow.put("replaced", replacement != expired);

            replacement.setLastPullTimestamp(System.currentTimeMillis());
            update.invoke(rebalance, "orders", java.util.Set.of(assignment), false);
            Map<String, Object> retainedRow = new TreeMap<>();
            retainedRow.put("case", "active-assigned");
            retainedRow.put("dropped", replacement.isDropped());
            retainedRow.put("present", table.containsKey(queue));
            retainedRow.put("same", table.get(queue) == replacement);

            replacement.setDropped(true);
            update.invoke(rebalance, "orders", java.util.Set.of(assignment), false);
            Map<String, Object> droppedRow = new TreeMap<>();
            droppedRow.put("case", "dropped-assigned"); droppedRow.put("dropped", replacement.isDropped());
            droppedRow.put("present", table.containsKey(queue)); droppedRow.put("same", table.get(queue) == replacement);

            update.invoke(rebalance, "orders", java.util.Set.of(), false);
            Map<String, Object> emptyRow = new TreeMap<>();
            emptyRow.put("case", "empty-assignment"); emptyRow.put("dropped", replacement.isDropped());
            emptyRow.put("present", table.containsKey(queue));
            return List.of(expiredRow, retainedRow, droppedRow, emptyRow);
        } finally {
            runtime.getPullMessageService().shutdown(true);
            runtime.getScheduledExecutorService().shutdownNow();
            runtime.getMQClientAPIImpl().shutdown();
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure", MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true); cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("PULL rebalance fixture teardown"));
        }
    }

    private static Map<String, Object> popUnsubscribedTruncation() throws Exception {
        var facade = new org.apache.rocketmq.client.consumer.DefaultMQPushConsumer("pop-unsubscribed");
        var consumer = new org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl(facade, null);
        var rebalance = consumer.getRebalanceImpl();
        var queue = new org.apache.rocketmq.common.message.MessageQueue("orders", "broker-a", 0);
        var processQueue = new org.apache.rocketmq.client.impl.consumer.PopProcessQueue();
        var table = rebalance.getPopProcessQueueTable();
        table.put(queue, processQueue);

        var truncate = org.apache.rocketmq.client.impl.consumer.RebalanceImpl.class
            .getDeclaredMethod("truncateMessageQueueNotMyTopic");
        truncate.setAccessible(true);
        truncate.invoke(rebalance);

        Map<String, Object> row = new TreeMap<>();
        row.put("case", "unsubscribed-pop");
        row.put("dropped", processQueue.isDropped());
        row.put("present", table.containsKey(queue));
        return row;
    }

    private static Map<String, Object> popToPullTransition() throws Exception {
        var facade = new org.apache.rocketmq.client.consumer.DefaultMQPushConsumer("pop-to-pull");
        var consumer = new org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl(facade, null);
        var rebalance = consumer.getRebalanceImpl();
        rebalance.setConsumerGroup("pop-to-pull");
        var queue = new org.apache.rocketmq.common.message.MessageQueue("orders", "broker-a", 0);
        var popProcessQueue = new org.apache.rocketmq.client.impl.consumer.PopProcessQueue();
        var pullProcessQueue = new org.apache.rocketmq.client.impl.consumer.ProcessQueue();
        rebalance.getPopProcessQueueTable().put(queue, popProcessQueue);
        rebalance.getProcessQueueTable().put(queue, pullProcessQueue);

        var assignment = new org.apache.rocketmq.common.message.MessageQueueAssignment();
        assignment.setMessageQueue(queue);
        assignment.setMode(org.apache.rocketmq.common.message.MessageRequestMode.PULL);
        var update = org.apache.rocketmq.client.impl.consumer.RebalanceImpl.class
            .getDeclaredMethod("updateMessageQueueAssignment", String.class, java.util.Set.class, boolean.class);
        update.setAccessible(true);
        update.invoke(rebalance, "orders", java.util.Set.of(assignment), false);

        Map<String, Object> row = new TreeMap<>();
        row.put("case", "pop-to-pull");
        row.put("popDropped", popProcessQueue.isDropped());
        row.put("popPresent", rebalance.getPopProcessQueueTable().containsKey(queue));
        row.put("pullPresent", rebalance.getProcessQueueTable().containsKey(queue));
        row.put("pullSame", rebalance.getProcessQueueTable().get(queue) == pullProcessQueue);
        return row;
    }

    private static List<Object> popRetryTransitions() throws Exception {
        String group = "pop-transition";
        return List.of(
            popRetryTransition("pop-to-push", "orders", group, List.of(0), List.of(), false),
            popRetryTransition("push-to-pop", "orders", group, List.of(), List.of(0), true),
            popRetryTransition("mixed-modes", "orders", group, List.of(0), List.of(1), true),
            popRetryTransition("retry-topic", "%RETRY%workers_orders", group, List.of(0), List.of(), false),
            popRetryTransition("empty-assignment", "orders", group, List.of(), List.of(), true));
    }

    private static Map<String, Object> popRetryTransition(String name, String topic, String group,
        List<Integer> pullQueueIds, List<Integer> popQueueIds, boolean initialSubscription) throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("pop-retry-transition-runtime");
        config.setNamesrvAddr(null);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        try {
            var facade = new org.apache.rocketmq.client.consumer.DefaultMQPushConsumer(group);
            var consumer = new org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl(facade, null);
            var rebalance = consumer.getRebalanceImpl();
            rebalance.setConsumerGroup(group);
            consumer.setmQClientFactory(runtime);
            rebalance.setmQClientFactory(runtime);

            String retryTopic = org.apache.rocketmq.common.KeyBuilder.buildPopRetryTopic(topic, group);
            if (initialSubscription) {
                rebalance.getSubscriptionInner().put(retryTopic,
                    new org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData(retryTopic, "*"));
            }
            var retryRoute = new java.util.HashSet<org.apache.rocketmq.common.message.MessageQueue>();
            retryRoute.add(new org.apache.rocketmq.common.message.MessageQueue(retryTopic, "broker-a", 7));
            rebalance.getTopicSubscribeInfoTable().put(retryTopic, retryRoute);

            var assignments = new java.util.HashSet<org.apache.rocketmq.common.message.MessageQueueAssignment>();
            for (Integer queueId : pullQueueIds) {
                var queue = new org.apache.rocketmq.common.message.MessageQueue(topic, "broker-a", queueId);
                rebalance.getProcessQueueTable().put(queue,
                    new org.apache.rocketmq.client.impl.consumer.ProcessQueue());
                var assignment = new org.apache.rocketmq.common.message.MessageQueueAssignment();
                assignment.setMessageQueue(queue);
                assignment.setMode(org.apache.rocketmq.common.message.MessageRequestMode.PULL);
                assignments.add(assignment);
            }
            for (Integer queueId : popQueueIds) {
                var queue = new org.apache.rocketmq.common.message.MessageQueue(topic, "broker-a", queueId);
                var processQueue = new org.apache.rocketmq.client.impl.consumer.PopProcessQueue();
                processQueue.setLastPopTimestamp(System.currentTimeMillis());
                rebalance.getPopProcessQueueTable().put(queue, processQueue);
                var assignment = new org.apache.rocketmq.common.message.MessageQueueAssignment();
                assignment.setMessageQueue(queue);
                assignment.setMode(org.apache.rocketmq.common.message.MessageRequestMode.POP);
                assignments.add(assignment);
            }

            var update = org.apache.rocketmq.client.impl.consumer.RebalanceImpl.class
                .getDeclaredMethod("updateMessageQueueAssignment", String.class, java.util.Set.class, boolean.class);
            update.setAccessible(true);
            update.invoke(rebalance, topic, assignments, false);

            Map<String, Object> result = new TreeMap<>();
            result.put("case", name);
            result.put("topic", topic);
            result.put("group", group);
            result.put("retryTopic", retryTopic);
            result.put("initialSubscription", initialSubscription);
            result.put("pullQueueIds", pullQueueIds);
            result.put("popQueueIds", popQueueIds);
            result.put("subscriptionPresent", rebalance.getSubscriptionInner().containsKey(retryTopic));
            result.put("routePresent", rebalance.getTopicSubscribeInfoTable().containsKey(retryTopic));
            return result;
        } finally {
            runtime.getPullMessageService().shutdown(true);
            runtime.getScheduledExecutorService().shutdownNow();
            runtime.getMQClientAPIImpl().shutdown();
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure", MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("POP transition fixture teardown"));
        }
    }

    private static Map<String, Object> reset(String name) throws Exception {
        ClientConfig config = new ClientConfig(); config.setClientIP("reset-runtime"); config.setNamesrvAddr(null);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        var facade = new org.apache.rocketmq.client.consumer.DefaultMQPushConsumer("reset");
        facade.setMessageModel(org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.BROADCASTING);
        List<String> lifecycle = new ArrayList<>();
        var consumer = new org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl(facade, null) {
            @Override public void suspend() { super.suspend(); lifecycle.add("suspend"); }
            @Override public void resume() {
                // Keep the separate truncation probe's entry state isolated;
                // all reset cases exercise the real finally rebalance.
                if (name.equals("rebalance-unsubscribed")) setPause(false);
                else super.resume();
                lifecycle.add("resume");
            }
        };
        var values = new TreeMap<Integer, Long>(); var persisted = new TreeMap<Integer, Long>();
        Map<Integer, List<String>> events = new TreeMap<>();
        Long[] handoffValues = new Long[] {60L, 70L};
        var finalStore = (org.apache.rocketmq.client.consumer.store.OffsetStore) Proxy.newProxyInstance(
            RuntimeConsumerOperationsFixtures.class.getClassLoader(),
            new Class<?>[] {org.apache.rocketmq.client.consumer.store.OffsetStore.class}, (proxy, method, arguments) -> {
                if (!method.getName().equals("removeOffset")) throw new IllegalStateException("unexpected final-store call");
                int id = ((org.apache.rocketmq.common.message.MessageQueue) arguments[0]).getQueueId();
                events.computeIfAbsent(id, ignored -> new ArrayList<>()).add("remove:third"); handoffValues[1] = null; return null;
            });
        var nextStore = (org.apache.rocketmq.client.consumer.store.OffsetStore) Proxy.newProxyInstance(
            RuntimeConsumerOperationsFixtures.class.getClassLoader(),
            new Class<?>[] {org.apache.rocketmq.client.consumer.store.OffsetStore.class}, (proxy, method, arguments) -> {
                if (!method.getName().equals("persist")) throw new IllegalStateException("unexpected second-store call");
                int id = ((org.apache.rocketmq.common.message.MessageQueue) arguments[0]).getQueueId();
                events.computeIfAbsent(id, ignored -> new ArrayList<>()).add("persist:second");
                persisted.put(id, handoffValues[0]); consumer.setOffsetStore(finalStore); return null;
            });
        var store = (org.apache.rocketmq.client.consumer.store.OffsetStore) Proxy.newProxyInstance(
            RuntimeConsumerOperationsFixtures.class.getClassLoader(),
            new Class<?>[] {org.apache.rocketmq.client.consumer.store.OffsetStore.class}, (proxy, method, arguments) -> {
                var queue = (org.apache.rocketmq.common.message.MessageQueue) arguments[0]; int id = queue.getQueueId();
                var calls = events.computeIfAbsent(id, ignored -> new ArrayList<>());
                switch (method.getName()) {
                    case "updateOffset":
                        calls.add("update:" + arguments[1]); values.put(id, (Long) arguments[1]);
                        if (name.equals("store-handoff")) consumer.setOffsetStore(nextStore);
                        return null;
                    case "persist":
                        calls.add("persist");
                        if (name.equals("per-queue-failure") && id == 0) throw new IllegalStateException("persist failed");
                        persisted.put(id, values.get(id)); return null;
                    case "removeOffset": calls.add("remove"); values.remove(id); return null;
                    default: throw new IllegalStateException("unexpected store method: " + method.getName());
                }
            });
        consumer.setOffsetStore(store);
        boolean orderly = name.equals("orderly-success") || name.startsWith("subscribed-");
        var orderlyField = org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl.class.getDeclaredField("consumeOrderly");
        orderlyField.setAccessible(true); orderlyField.set(consumer, orderly);
        var table = consumer.getRebalanceImpl().getProcessQueueTable();
        Map<Integer, org.apache.rocketmq.client.impl.consumer.ProcessQueue> queues = new TreeMap<>();
        var cleared = new java.util.concurrent.CountDownLatch(1);
        var queue0 = new org.apache.rocketmq.common.message.MessageQueue("orders", "broker-a", 0);
        var queue1 = new org.apache.rocketmq.common.message.MessageQueue("orders", "broker-a", 1);
        if (name.startsWith("subscribed-null-value-") || name.equals("subscribed-pull-expired")) {
            consumer.getRebalanceImpl().setMessageModel(facade.getMessageModel());
            consumer.getRebalanceImpl().getSubscriptionInner().put("orders",
                new org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData("orders", "*"));
            if (name.endsWith("-assigned")) consumer.getRebalanceImpl().getTopicSubscribeInfoTable().put("orders", java.util.Set.of(queue0));
            if (name.equals("subscribed-pull-expired")) {
                consumer.getRebalanceImpl().getTopicSubscribeInfoTable().put("orders", java.util.Set.of(queue0));
            }
        }
        if (!name.equals("empty-push-null-inputs")) {
            var processQueue = new org.apache.rocketmq.client.impl.consumer.ProcessQueue() {
                @Override public void clear() { super.clear(); cleared.countDown(); }
            };
            if (name.equals("subscribed-pull-expired")) {
                processQueue.setLastPullTimestamp(System.currentTimeMillis() - 120_001L);
            }
            table.put(queue0, processQueue); queues.put(0, processQueue); values.put(0, 50L);
        }
        if (name.equals("per-queue-failure")) {
            var processQueue = new org.apache.rocketmq.client.impl.consumer.ProcessQueue();
            table.put(queue1, processQueue); queues.put(1, processQueue); values.put(1, 60L);
        }
        Map<org.apache.rocketmq.common.message.MessageQueue, Long> offsets = new java.util.LinkedHashMap<>();
        offsets.put(queue0, name.equals("null-value") || name.equals("rebalance-unsubscribed")
            || name.startsWith("subscribed-null-value-") ? null : 7L);
        if (name.equals("per-queue-failure") || name.equals("live-new-queue")) offsets.put(queue1, 9L);
        if (name.equals("null-key-only")) { offsets.clear(); offsets.put(null, 8L); }
        if (name.equals("unassigned-offset")) { offsets.clear(); offsets.put(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker-a", 9), 99L); }
        String topic = name.equals("subscribed-pull-expired") ? "other"
            : name.equals("empty-push-null-inputs") || name.equals("null-topic-active") ? null
            : name.equals("null-map-other-topic") ? "other" : "orders";
        if (name.startsWith("null-map") || name.equals("empty-push-null-inputs")) offsets = null;
        final var input = offsets;
        Map<String, Object> item = new TreeMap<>();
        try {
            String group = name.equals("null-group") ? null : name.equals("missing-group") ? "missing" : "selected";
            runtime.getConsumerTable().put("selected", name.equals("pull-group")
                ? new org.apache.rocketmq.client.consumer.DefaultMQPullConsumer("pull").getDefaultMQPullConsumerImpl() : consumer);
            item.put("name", name); item.put("topic", topic); item.put("group", group); item.put("orderly", orderly);
            item.put("offsets", resetInputRows(input));
            if (name.startsWith("live-")) {
                var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
                Thread worker = new Thread(() -> {
                    try { runtime.resetOffset(topic, group, input); } catch (Throwable error) { failure.set(error); }
                }, "reset-fixture");
                worker.start();
                if (!cleared.await(5, java.util.concurrent.TimeUnit.SECONDS)) throw new IllegalStateException("reset did not clear queue");
                long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
                while (java.util.Arrays.stream(worker.getStackTrace()).noneMatch(frame ->
                    frame.getClassName().equals("java.util.concurrent.TimeUnit") && frame.getMethodName().equals("sleep"))) {
                    if (System.nanoTime() > deadline) { worker.interrupt(); throw new IllegalStateException("reset did not reach drain sleep"); }
                    Thread.yield();
                }
                if (name.equals("live-map-update")) { input.put(queue0, 77L); }
                else {
                    var processQueue = new org.apache.rocketmq.client.impl.consumer.ProcessQueue();
                    table.put(queue1, processQueue); queues.put(1, processQueue); values.put(1, 60L);
                }
                worker.interrupt(); worker.join(5000);
                if (worker.isAlive()) throw new IllegalStateException("reset worker did not finish");
                if (failure.get() != null) throw new IllegalStateException("live reset failed", failure.get());
                item.put("error", null);
            } else {
                if (!orderly) {
                    // Interrupt only after the worker reaches Java's drain
                    // sleep. suspend() may consume an interrupt delivered
                    // before resetOffset starts.
                    var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
                    Thread worker = new Thread(() -> {
                        try { runtime.resetOffset(topic, group, input); }
                        catch (Throwable error) { failure.set(error); }
                    }, "reset-fixture-" + name);
                    worker.start();
                    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
                    while (worker.isAlive() && java.util.Arrays.stream(worker.getStackTrace()).noneMatch(frame ->
                        frame.getClassName().equals("java.util.concurrent.TimeUnit") && frame.getMethodName().equals("sleep"))) {
                        if (System.nanoTime() > deadline) { worker.interrupt(); throw new IllegalStateException("reset did not reach drain sleep: " + name); }
                        Thread.yield();
                    }
                    worker.interrupt(); worker.join(5000);
                    if (worker.isAlive()) throw new IllegalStateException("reset worker did not finish: " + name);
                    item.put("error", failure.get() == null ? null : failure.get().getClass().getSimpleName());
                } else {
                    try { runtime.resetOffset(topic, group, input); item.put("error", null); }
                    catch (Exception error) { item.put("error", error.getClass().getSimpleName()); }
                    finally { Thread.interrupted(); }
                }
            }
            if (name.equals("rebalance-unsubscribed")) { lifecycle.add("rebalance"); consumer.doRebalance(); }
            List<Object> rows = new ArrayList<>();
            for (var entry : queues.entrySet()) {
                int id = entry.getKey(); var queue = id == 0 ? queue0 : queue1;
                Map<String, Object> row = new TreeMap<>(); row.put("id", id);
                row.put("present", table.containsKey(queue)); row.put("dropped", entry.getValue().isDropped());
                row.put("value", values.get(id)); row.put("persisted", persisted.get(id)); rows.add(row);
            }
            Map<String, Object> normalizedEvents = new TreeMap<>();
            events.forEach((id, calls) -> normalizedEvents.put(id.toString(), calls));
            item.put("queues", rows); item.put("events", normalizedEvents); item.put("lifecycle", lifecycle); item.put("pausedAfter", consumer.isPause());
            if (name.equals("store-handoff")) item.put("storeValues", java.util.Arrays.asList(values.get(0), handoffValues[0], handoffValues[1]));
            return item;
        } finally {
            Thread.interrupted(); runtime.getConsumerTable().clear();
            runtime.getScheduledExecutorService().shutdownNow();
            runtime.getMQClientAPIImpl().shutdown();
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure", MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true); cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("fixture teardown"));
        }
    }

    private static Object resetInputRows(Map<org.apache.rocketmq.common.message.MessageQueue, Long> offsets) {
        if (offsets == null) return null;
        List<Object> rows = new ArrayList<>();
        for (var entry : offsets.entrySet()) {
            Map<String, Object> row = new TreeMap<>();
            row.put("queueId", entry.getKey() == null ? null : entry.getKey().getQueueId());
            row.put("offset", entry.getValue()); rows.add(row);
        }
        return rows;
    }

    private static Map<String, Object> direct(String name) throws Exception {
        ClientConfig config = new ClientConfig(); config.setClientIP("direct-runtime"); config.setNamesrvAddr(null);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        var push = new org.apache.rocketmq.client.consumer.DefaultMQPushConsumer("tenant%direct");
        push.setNamespace("tenant");
        var implementation = push.getDefaultMQPushConsumerImpl();
        boolean customSubtype = name.equals("concurrent-custom-subtype");
        boolean customClientSubtype = name.startsWith("concurrent-client-custom-subtype");
        boolean clientMessage = name.contains("-client-");
        org.apache.rocketmq.common.message.MessageExt message = customSubtype
            ? new CustomDirectMessageExt() : customClientSubtype
                ? new CustomDirectMessageClientExt() : clientMessage
                ? new org.apache.rocketmq.common.message.MessageClientExt()
                : new org.apache.rocketmq.common.message.MessageExt();
        String topic = name.endsWith("null-topic") ? null : name.endsWith("empty-topic") ? "" : "tenant%orders";
        String physicalId = clientMessage || name.contains("-plain-") ? "physical-id" : null;
        String uniqueId = name.endsWith("-unique") && !name.endsWith("-no-unique") ? "client-unique" : null;
        byte[] callerBody = new byte[] {1};
        message.setTopic(topic); message.setQueueId(3); message.setBody(callerBody);
        if (customSubtype) {
            ((CustomDirectMessageExt) message).callerBody = callerBody;
            callerBody[0] = 2;
        }
        if (customSubtype) message.setFlag(5);
        if (customSubtype) message.setStoreHost(new java.net.InetSocketAddress("127.0.0.1", 10911));
        if (customSubtype) {
            message.setBornTimestamp(1L);
            message.setStoreTimestamp(2L);
            message.setSysFlag(3);
            message.setBornHostV6Flag();
            message.setStoreHostAddressV6Flag();
            message.setBodyCRC(4);
            message.setCommitLogOffset(5L);
            message.setStoreSize(6);
            message.setPreparedTransactionOffset(7L);
            message.setBornHost(new java.net.InetSocketAddress("127.0.0.1", 10912));
            message.setTopicSysFlag(5);
            message.setGroupSysFlag(6);
            message.setPriority(-7);
            message.setWaitStoreMsgOK(true);
            message.setBuyerId("base-buyer");
            message.setInstanceId("base-instance");
            message.setDelayTimeLevel(4);
            message.setDelayTimeSec(5L);
            message.setDelayTimeMs(6L);
            message.setDeliverTimeMs(7L);
            message.putUserProperty("base-user", "base-value");
            message.putUserProperty("", "");
            message.putUserProperty(null, null);
            message.setTags("base-tags");
            message.setKeys("base-key");
            message.setKeys(List.of("first", "second"));
            message.setTransactionId("base-transaction");
        }
        if (clientMessage) ((org.apache.rocketmq.common.message.MessageClientExt) message).setOffsetMsgId(physicalId);
        else message.setMsgId(physicalId);
        if (customSubtype) {
            org.apache.rocketmq.common.message.MessageAccessor.putProperty(message, "RETRY_TOPIC", "subtype-retry-target");
            message.setReconsumeTimes(2);
            message.setQueueOffset(17L);
        }
        if (uniqueId != null) org.apache.rocketmq.common.message.MessageAccessor.putProperty(message, "UNIQ_KEY", uniqueId);
        List<Object> callbacks = new ArrayList<>();
        java.util.function.BiConsumer<List<org.apache.rocketmq.common.message.MessageExt>,
            org.apache.rocketmq.common.message.MessageQueue> record = (messages, queue) -> {
                Map<String, Object> item = new TreeMap<>();
                item.put("messageClass", messages.get(0).getClass().getSimpleName());
                item.put("topic", messages.get(0).getTopic()); item.put("broker", messages.get(0).getBrokerName());
                item.put("queueTopic", queue.getTopic()); item.put("queueBroker", queue.getBrokerName());
                item.put("queueId", queue.getQueueId()); callbacks.add(item);
                item.put("messageId", messages.get(0).getMsgId());
                item.put("flag", messages.get(0).getFlag());
                item.put("storeHostPresent", messages.get(0).getStoreHost() != null);
                item.put("reconsumeTimes", messages.get(0).getReconsumeTimes());
                item.put("queueOffset", messages.get(0).getQueueOffset());
                item.put("bodyLength", messages.get(0).getBody() == null ? null : messages.get(0).getBody().length);
                item.put("offsetMessageId", messages.get(0) instanceof org.apache.rocketmq.common.message.MessageClientExt
                    ? ((org.apache.rocketmq.common.message.MessageClientExt) messages.get(0)).getOffsetMsgId() : null);
                item.put("propertiesPresent", messages.get(0).getProperties() != null);
                if (messages.get(0) instanceof CustomDirectMessageExt) {
                    CustomDirectMessageExt custom = (CustomDirectMessageExt) messages.get(0);
                    item.put("messageQueueId", messages.get(0).getQueueId());
                    item.put("tags", messages.get(0).getTags());
                    item.put("keys", messages.get(0).getKeys());
                    Map<String, String> properties = messages.get(0).getProperties();
                    item.put("subtypeProperty", properties == null ? null : properties.get("subtype-only"));
                    item.put("propertiesSharesSubtypeMap", properties == custom.customProperties);
                    if (properties != null) {
                        properties.put("through-view", "visible");
                        item.put("mapMutationVisibleToSubtype",
                            "visible".equals(custom.customProperties.get("through-view")));
                        custom.customProperties.put("through-subtype", "visible");
                        item.put("subtypeMutationVisibleToMap",
                            "visible".equals(properties.get("through-subtype")));
                        item.put("mutationVisibleToLaterGetter",
                            "visible".equals(messages.get(0).getProperties().get("through-view")));
                    }
                    byte[] body = messages.get(0).getBody();
                    item.put("bodySetterRetainsCallerArray",
                        body == custom.callerBody && body == custom.customBody);
                    item.put("bodySetterMutationVisibleToGetter",
                        body != null && body.length > 0 && body[0] == 2);
                    item.put("bodySharesSubtypeArray", body == custom.customBody);
                    if (body != null && body.length > 0 && custom.customBody != null) {
                        body[0] = 9;
                        item.put("bodyMutationVisibleToSubtype", custom.customBody[0] == 9);
                        custom.customBody[0] = 10;
                        item.put("subtypeMutationVisibleToGetter", messages.get(0).getBody()[0] == 10);
                    }
                    item.put("bornTimestamp", messages.get(0).getBornTimestamp());
                    item.put("storeTimestamp", messages.get(0).getStoreTimestamp());
                    item.put("sysFlag", messages.get(0).getSysFlag());
                    item.put("bodyCRC", messages.get(0).getBodyCRC());
                    item.put("commitLogOffset", messages.get(0).getCommitLogOffset());
                    item.put("storeSize", messages.get(0).getStoreSize());
                    item.put("preparedTransactionOffset", messages.get(0).getPreparedTransactionOffset());
                    item.put("bornHostString", messages.get(0).getBornHostString());
                    item.put("bornHostNameString", messages.get(0).getBornHostNameString());
                    item.put("bornHostBytes", unsignedBytes(messages.get(0).getBornHostBytes().array()));
                    item.put("storeHostBytes", unsignedBytes(messages.get(0).getStoreHostBytes().array()));
                    item.put("bornHostBytesBuffer",
                        unsignedBytes(messages.get(0).getBornHostBytes(ByteBuffer.allocate(8))));
                    item.put("storeHostBytesBuffer",
                        unsignedBytes(messages.get(0).getStoreHostBytes(ByteBuffer.allocate(8))));
                    item.put("bornHost", messages.get(0).getBornHost().toString());
                    item.put("storeHost", messages.get(0).getStoreHost().toString());
                    item.put("topicSysFlag", messages.get(0).getTopicSysFlag());
                    item.put("groupSysFlag", messages.get(0).getGroupSysFlag());
                    item.put("priority", messages.get(0).getPriority());
                    item.put("priorityProperty", messages.get(0).getProperty(
                        org.apache.rocketmq.common.message.MessageConst.PROPERTY_PRIORITY));
                    item.put("waitStore", messages.get(0).isWaitStoreMsgOK());
                    item.put("buyerId", messages.get(0).getBuyerId());
                    item.put("delayTimeLevel", messages.get(0).getDelayTimeLevel());
                    item.put("delayTimeSec", messages.get(0).getDelayTimeSec());
                    item.put("delayTimeMs", messages.get(0).getDelayTimeMs());
                    item.put("deliverTimeMs", messages.get(0).getDeliverTimeMs());
                    item.put("userProperty", messages.get(0).getUserProperty("user-probe"));
                    item.put("hasProperty", messages.get(0).hasProperty("subtype-only"));
                    item.put("transactionId", messages.get(0).getTransactionId());
                    item.put("customField", custom.getCustomField());
                    item.put("messageDisplay", messages.get(0).toString());
                }
            };
        org.apache.rocketmq.client.impl.consumer.ConsumeMessageService service;
        if (name.startsWith("orderly")) {
            org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly listener = (messages, context) -> {
                record.accept(messages, context.getMessageQueue()); context.setAutoCommit(false);
                if (name.endsWith("null-status")) return null;
                if (name.endsWith("commit")) return org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus.COMMIT;
                if (name.endsWith("rollback")) return org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus.ROLLBACK;
                if (name.endsWith("later")) return org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                return org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus.SUCCESS;
            };
            service = new org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService(implementation, listener);
        } else {
            org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently listener = (messages, context) -> {
                record.accept(messages, context.getMessageQueue());
                if (name.endsWith("throws")) throw new IllegalStateException("listener failed");
                if (name.endsWith("null-status")) return null;
                return name.endsWith("later") ? org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus.RECONSUME_LATER
                    : org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            };
            service = new org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService(implementation, listener);
        }
        var serviceField = implementation.getClass().getDeclaredField("consumeMessageService");
        serviceField.setAccessible(true);
        serviceField.set(implementation, name.equals("push-null-service") ? null : service);
        var item = new TreeMap<String, Object>();
        try {
            String group = name.equals("null-group") ? null : name.equals("missing-group") ? "missing" : "selected";
            if (name.equals("pull-group")) {
                runtime.getConsumerTable().put("selected", new org.apache.rocketmq.client.consumer.DefaultMQPullConsumer("pull")
                    .getDefaultMQPullConsumerImpl());
            } else if (name.equals("other-group")) {
                runtime.getConsumerTable().put("selected", (MQConsumerInner) Proxy.newProxyInstance(
                    RuntimeConsumerOperationsFixtures.class.getClassLoader(), new Class<?>[] {MQConsumerInner.class},
                    (proxy, method, arguments) -> { throw new IllegalStateException("unexpected consumer call"); }));
            } else { runtime.getConsumerTable().put("selected", implementation); }
            String broker = name.endsWith("null-broker") ? null : "broker-a";
            boolean nullMessage = name.equals("push-null-message") || name.endsWith("group");
            item.put("name", name); item.put("group", group); item.put("broker", broker); item.put("nullMessage", nullMessage);
            item.put("inputClient", clientMessage); item.put("inputTopic", topic);
            item.put("inputMessageId", physicalId); item.put("inputUniq", uniqueId);
            try {
                var result = runtime.consumeMessageDirectly(nullMessage ? null : message, group, broker);
                item.put("error", null); item.put("resultPresent", result != null);
                if (result != null) {
                    // Only wall-clock duration and JVM exception rendering are
                    // nondeterministic/runtime-specific; keep result/order/counts.
                    item.put("result", result.getConsumeResult().name()); item.put("order", result.isOrder());
                    item.put("autoCommit", result.isAutoCommit()); item.put("remarkPresent", result.getRemark() != null);
                    if (result.getSpentTimeMills() < 0) throw new IllegalStateException("negative elapsed time");
                }
            } catch (Exception error) { item.put("error", error.getClass().getSimpleName()); }
            item.put("callbacks", callbacks);
            return item;
        } finally {
            service.shutdown(100);
            runtime.getConsumerTable().clear();
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure", MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true); cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("fixture teardown"));
        }
    }

    private static final class CustomDirectMessageExt extends org.apache.rocketmq.common.message.MessageExt {
        private String customBrokerName;
        private int topicWrites;
        private int reconsumeWrites;
        private int queueOffsetWrites;
        private int bodyWrites;
        private int flagWrites;
        private int maxOffsetReads;
        private int tagsReads;
        private int keysReads;
        private int propertiesReads;
        private int storeHostWrites;
        private int metadataWrites;
        private int hostFlagWrites;
        private int topicSysFlagWrites;
        private int groupSysFlagWrites;
        private int propertySetterWrites;
        private int userPropertyReads;
        private int hasPropertyReads;
        private int transactionIdReads;
        private int userApiWrites;
        private int priorityReads;
        private int priorityWrites;
        private int customPriority = 37;
        private final Map<String, String> customProperties = new HashMap<>(Map.of("subtype-only", "visible"));
        private byte[] customBody;
        private byte[] callerBody;

        @Override
        public String getTopic() {
            return "%RETRY%tenant%direct";
        }

        @Override
        public void setTopic(String topic) {
            topicWrites++;
        }

        @Override
        public int getQueueId() {
            return 37;
        }

        @Override
        public void setQueueId(int queueId) {
            // This fixture keeps a subtype-defined queue ID independent of the base field.
        }

        @Override
        public String getBrokerName() {
            return customBrokerName;
        }

        @Override
        public void setBrokerName(String brokerName) {
            customBrokerName = brokerName;
        }

        @Override
        public String getMsgId() {
            return "subtype-message-id";
        }

        @Override
        public void setMsgId(String msgId) {
            // This fixture keeps a subtype-defined ID independent of the base field.
        }

        @Override
        public int getReconsumeTimes() {
            return 13;
        }

        @Override
        public void setReconsumeTimes(int reconsumeTimes) {
            this.reconsumeWrites++;
        }

        @Override
        public long getQueueOffset() {
            return 987654321L;
        }

        @Override
        public void setQueueOffset(long queueOffset) {
            this.queueOffsetWrites++;
        }

        @Override
        public byte[] getBody() {
            return customBody;
        }

        @Override
        public void setBody(byte[] body) {
            this.bodyWrites++;
            this.customBody = body;
        }

        @Override
        public int getFlag() {
            return 71;
        }

        @Override
        public void setFlag(int flag) {
            this.flagWrites++;
        }

        @Override
        public int getPriority() {
            priorityReads++;
            return customPriority;
        }

        @Override
        public void setPriority(int priority) {
            priorityWrites++;
            customPriority = priority;
        }

        @Override
        public String getProperty(String name) {
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_MAX_OFFSET.equals(name)) {
                this.maxOffsetReads++;
                return "987654333";
            }
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_TAGS.equals(name)) {
                this.tagsReads++;
                return "subtype-tag";
            }
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_KEYS.equals(name)) {
                this.keysReads++;
                return "subtype-key";
            }
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG.equals(name)) {
                return "73";
            }
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_GROUP_CONFIG.equals(name)) {
                return "-19";
            }
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_WAIT_STORE_MSG_OK.equals(name)) {
                return "false";
            }
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_BUYER_ID.equals(name)) {
                return "subtype-buyer";
            }
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_DELAY_TIME_LEVEL.equals(name)) {
                return "9";
            }
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_TIMER_DELAY_SEC.equals(name)) {
                return "123";
            }
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_TIMER_DELAY_MS.equals(name)) {
                return "456";
            }
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_TIMER_DELIVER_MS.equals(name)) {
                return "789";
            }
            return super.getProperty(name);
        }

        @Override
        public Map<String, String> getProperties() {
            this.propertiesReads++;
            return customProperties;
        }

        @Override
        public java.net.SocketAddress getStoreHost() {
            return new java.net.InetSocketAddress("127.0.0.1", 10913);
        }

        @Override
        public void setStoreHost(java.net.SocketAddress storeHost) {
            this.storeHostWrites++;
            super.setStoreHost(storeHost);
        }

        @Override
        public long getBornTimestamp() {
            return 1700000000123L;
        }

        @Override
        public void setBornTimestamp(long bornTimestamp) {
            metadataWrites++;
        }

        @Override
        public java.net.SocketAddress getBornHost() {
            return new java.net.InetSocketAddress("127.0.0.1", 10913);
        }

        @Override
        public String getBornHostString() {
            return "custom-born-host-address";
        }

        @Override
        public String getBornHostNameString() {
            return "custom-born-host-name";
        }

        @Override
        public ByteBuffer getBornHostBytes() {
            return ByteBuffer.wrap(new byte[] {10, 11, 12});
        }

        @Override
        public ByteBuffer getBornHostBytes(ByteBuffer byteBuffer) {
            byteBuffer.put(new byte[] {1, 2, 3, 4});
            byteBuffer.flip();
            return byteBuffer;
        }

        @Override
        public ByteBuffer getStoreHostBytes() {
            return ByteBuffer.wrap(new byte[] {13, 14, 15});
        }

        @Override
        public ByteBuffer getStoreHostBytes(ByteBuffer byteBuffer) {
            byteBuffer.put(new byte[] {5, 6, 7, 8});
            byteBuffer.flip();
            return byteBuffer;
        }

        @Override
        public void setBornHost(java.net.SocketAddress bornHost) {
            metadataWrites++;
            super.setBornHost(bornHost);
        }

        @Override
        public long getStoreTimestamp() {
            return 1700000000456L;
        }

        @Override
        public void setStoreTimestamp(long storeTimestamp) {
            metadataWrites++;
        }

        @Override
        public int getSysFlag() {
            return 0x1234;
        }

        @Override
        public void setSysFlag(int sysFlag) {
            metadataWrites++;
        }

        @Override
        public void setBornHostV6Flag() {
            hostFlagWrites++;
        }

        @Override
        public void setStoreHostAddressV6Flag() {
            hostFlagWrites++;
        }

        @Override
        public int getBodyCRC() {
            return 0x4567;
        }

        @Override
        public void setBodyCRC(int bodyCRC) {
            metadataWrites++;
        }

        @Override
        public long getCommitLogOffset() {
            return 9876543L;
        }

        @Override
        public void setCommitLogOffset(long commitLogOffset) {
            metadataWrites++;
        }

        @Override
        public int getStoreSize() {
            return 765;
        }

        @Override
        public void setStoreSize(int storeSize) {
            metadataWrites++;
        }

        @Override
        public long getPreparedTransactionOffset() {
            return 1234567L;
        }

        @Override
        public void setPreparedTransactionOffset(long preparedTransactionOffset) {
            metadataWrites++;
        }

        @Override
        public void setTopicSysFlag(Integer topicSysFlag) {
            topicSysFlagWrites++;
        }

        @Override
        public void setGroupSysFlag(Integer groupSysFlag) {
            groupSysFlagWrites++;
        }

        @Override
        public void setWaitStoreMsgOK(boolean waitStoreMsgOK) {
            propertySetterWrites++;
        }

        @Override
        public void setBuyerId(String buyerId) {
            propertySetterWrites++;
        }

        @Override
        public void setInstanceId(String instanceId) {
            propertySetterWrites++;
        }

        @Override
        public void setDelayTimeLevel(int level) {
            propertySetterWrites++;
        }

        @Override
        public void setDelayTimeSec(long sec) {
            propertySetterWrites++;
        }

        @Override
        public void setDelayTimeMs(long timeMs) {
            propertySetterWrites++;
        }

        @Override
        public void setDeliverTimeMs(long timeMs) {
            propertySetterWrites++;
        }

        @Override
        public String getUserProperty(String name) {
            userPropertyReads++;
            return "subtype-user-value";
        }

        @Override
        public boolean hasProperty(String name) {
            hasPropertyReads++;
            return false;
        }

        @Override
        public String getTransactionId() {
            transactionIdReads++;
            return "subtype-transaction";
        }

        @Override
        public void putUserProperty(String name, String value) {
            userApiWrites++;
        }

        @Override
        public void setTags(String tags) {
            userApiWrites++;
        }

        @Override
        public void setKeys(String keys) {
            userApiWrites++;
        }

        @Override
        public void setTransactionId(String transactionId) {
            userApiWrites++;
        }

        public String getCustomField() {
            return "topicWrites=" + topicWrites + ";reconsumeWrites=" + reconsumeWrites
                + ";queueOffsetWrites=" + queueOffsetWrites + ";bodyWrites=" + bodyWrites
                + ";flagWrites=" + flagWrites + ";maxOffsetReads=" + maxOffsetReads
                + ";tagsReads=" + tagsReads + ";keysReads=" + keysReads
                + ";propertiesReads=" + propertiesReads + ";storeHostWrites=" + storeHostWrites
                + ";metadataWrites=" + metadataWrites + ";hostFlagWrites=" + hostFlagWrites
                + ";topicSysFlagWrites=" + topicSysFlagWrites
                + ";groupSysFlagWrites=" + groupSysFlagWrites
                + ";propertySetterWrites=" + propertySetterWrites
                + ";userPropertyReads=" + userPropertyReads
                + ";hasPropertyReads=" + hasPropertyReads
                + ";transactionIdReads=" + transactionIdReads
                + ";userApiWrites=" + userApiWrites
                + ";priorityWrites=" + priorityWrites
                + ";priorityReads=" + priorityReads;
        }

        @Override
        public String toString() {
            return "CustomDirectMessageExt[topicWrites=" + topicWrites + "]";
        }
    }

    private static final class CustomDirectMessageClientExt
        extends org.apache.rocketmq.common.message.MessageClientExt {
        private String offsetMessageId;

        @Override
        public String getOffsetMsgId() {
            return offsetMessageId;
        }

        @Override
        public void setOffsetMsgId(String offsetMessageId) {
            this.offsetMessageId = offsetMessageId == null ? null : "subtype-" + offsetMessageId;
        }
    }

    private static List<Integer> unsignedBytes(byte[] bytes) {
        List<Integer> result = new ArrayList<>();
        for (byte value : bytes) result.add(value & 0xff);
        return result;
    }

    private static List<Integer> unsignedBytes(ByteBuffer bytes) {
        ByteBuffer remaining = bytes.asReadOnlyBuffer();
        List<Integer> result = new ArrayList<>();
        while (remaining.hasRemaining()) result.add(remaining.get() & 0xff);
        return result;
    }

    private static Map<String, Object> customSubtypeProcessQueue() {
        CustomDirectMessageExt message = new CustomDirectMessageExt();
        message.setQueueOffset(17L);
        message.setBody(new byte[] {1, 2, 3});
        org.apache.rocketmq.client.impl.consumer.ProcessQueue processQueue =
            new org.apache.rocketmq.client.impl.consumer.ProcessQueue();
        boolean dispatch = processQueue.putMessage(List.of(message));
        Map<String, Object> result = new TreeMap<>();
        result.put("dispatch", dispatch);
        result.put("count", processQueue.getMsgCount().get());
        result.put("size", processQueue.getMsgSize().get());
        result.put("msgAccCnt", processQueue.getMsgAccCnt());
        result.put("offsets", new ArrayList<>(processQueue.getMsgTreeMap().keySet()));
        result.put("customField", message.getCustomField());
        return result;
    }

    private static Map<String, Object> priorityPropertySubtype() {
        var message = new PriorityPropertyMessageExt();
        message.setPriority(17);
        Map<String, Object> result = new TreeMap<>();
        result.put("priority", message.getPriority());
        result.put("priorityProperty", message.getProperty(
            org.apache.rocketmq.common.message.MessageConst.PROPERTY_PRIORITY));
        result.put("storedPriorityProperty", message.getProperties().get(
            org.apache.rocketmq.common.message.MessageConst.PROPERTY_PRIORITY));
        boolean negativeRejected;
        try {
            message.setPriority(-1);
            negativeRejected = false;
        } catch (IllegalArgumentException expected) {
            negativeRejected = true;
        }
        result.put("negativePriorityRejected", negativeRejected);
        return result;
    }

    private static Map<String, Object> derivedGetterSubtype() {
        var message = new DerivedGetterMessageExt();
        Map<String, Object> result = new TreeMap<>();
        result.put("tags", message.getTags());
        result.put("keys", message.getKeys());
        result.put("waitStore", message.isWaitStoreMsgOK());
        result.put("buyerId", message.getBuyerId());
        result.put("delayTimeLevel", message.getDelayTimeLevel());
        result.put("delayTimeSec", message.getDelayTimeSec());
        result.put("delayTimeMs", message.getDelayTimeMs());
        result.put("deliverTimeMs", message.getDeliverTimeMs());
        result.put("topicSysFlag", message.getTopicSysFlag());
        result.put("groupSysFlag", message.getGroupSysFlag());
        result.put("propertyTags", message.getProperty(
            org.apache.rocketmq.common.message.MessageConst.PROPERTY_TAGS));
        result.put("propertyKeys", message.getProperty(
            org.apache.rocketmq.common.message.MessageConst.PROPERTY_KEYS));
        result.put("getterCalls", message.getGetterCalls());
        return result;
    }

    private static final class DerivedGetterMessageExt extends org.apache.rocketmq.common.message.MessageExt {
        private int getterCalls;

        @Override public String getTags() { getterCalls++; return "override-tags"; }
        @Override public String getKeys() { getterCalls++; return "override-keys"; }
        @Override public boolean isWaitStoreMsgOK() { getterCalls++; return true; }
        @Override public String getBuyerId() { getterCalls++; return "override-buyer"; }
        @Override public int getDelayTimeLevel() { getterCalls++; return 31; }
        @Override public long getDelayTimeSec() { getterCalls++; return 32L; }
        @Override public long getDelayTimeMs() { getterCalls++; return 33L; }
        @Override public long getDeliverTimeMs() { getterCalls++; return 34L; }
        @Override public Integer getTopicSysFlag() { getterCalls++; return 35; }
        @Override public Integer getGroupSysFlag() { getterCalls++; return 36; }

        @Override
        public String getProperty(String name) {
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_TAGS.equals(name)) return "property-tags";
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_KEYS.equals(name)) return "property-keys";
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_WAIT_STORE_MSG_OK.equals(name)) return "false";
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_BUYER_ID.equals(name)) return "property-buyer";
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_DELAY_TIME_LEVEL.equals(name)) return "9";
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_TIMER_DELAY_SEC.equals(name)) return "123";
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_TIMER_DELAY_MS.equals(name)) return "456";
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_TIMER_DELIVER_MS.equals(name)) return "789";
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG.equals(name)) return "73";
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_GROUP_CONFIG.equals(name)) return "-19";
            return super.getProperty(name);
        }

        private int getGetterCalls() { return getterCalls; }
    }

    private static Map<String, Object> derivedGetterNullSubtype() {
        var message = new DerivedGetterNullMessageExt();
        Map<String, Object> result = new TreeMap<>();
        result.put("tags", message.getTags());
        result.put("keys", message.getKeys());
        result.put("buyerId", message.getBuyerId());
        result.put("topicSysFlag", message.getTopicSysFlag());
        result.put("groupSysFlag", message.getGroupSysFlag());
        result.put("propertyTags", message.getProperty(
            org.apache.rocketmq.common.message.MessageConst.PROPERTY_TAGS));
        result.put("propertyKeys", message.getProperty(
            org.apache.rocketmq.common.message.MessageConst.PROPERTY_KEYS));
        result.put("propertyBuyerId", message.getProperty(
            org.apache.rocketmq.common.message.MessageConst.PROPERTY_BUYER_ID));
        result.put("propertyTopicSysFlag", message.getProperty(
            org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG));
        result.put("propertyGroupSysFlag", message.getProperty(
            org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_GROUP_CONFIG));
        result.put("getterCalls", message.getGetterCalls());
        return result;
    }

    private static final class DerivedGetterNullMessageExt extends org.apache.rocketmq.common.message.MessageExt {
        private int getterCalls;

        @Override public String getTags() { getterCalls++; return null; }
        @Override public String getKeys() { getterCalls++; return null; }
        @Override public String getBuyerId() { getterCalls++; return null; }
        @Override public Integer getTopicSysFlag() { getterCalls++; return null; }
        @Override public Integer getGroupSysFlag() { getterCalls++; return null; }

        @Override
        public String getProperty(String name) {
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_TAGS.equals(name)) return "property-tags";
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_KEYS.equals(name)) return "property-keys";
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_BUYER_ID.equals(name)) return "property-buyer";
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG.equals(name)) return "malformed-topic";
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_GROUP_CONFIG.equals(name)) return "malformed-group";
            return super.getProperty(name);
        }

        private int getGetterCalls() { return getterCalls; }
    }

    private static final class PriorityPropertyMessageExt extends org.apache.rocketmq.common.message.MessageExt {
        @Override
        public String getProperty(String name) {
            if (org.apache.rocketmq.common.message.MessageConst.PROPERTY_PRIORITY.equals(name)) {
                return "23";
            }
            return super.getProperty(name);
        }
    }

    private static Map<String, Object> runningInfo(String name) throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("runtime-consumer-operations"); config.setNamesrvAddr(null);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        List<String> calls = new ArrayList<>();
        ConsumerRunningInfo snapshot = "null-snapshot".equals(name) ? null : new ConsumerRunningInfo();
        Properties properties = null;
        if (snapshot != null) {
            if ("null-properties".equals(name)) {
                snapshot.setProperties(null);
            } else {
                properties = snapshot.getProperties();
                properties.setProperty("owner", "retained");
                properties.setProperty(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, "owner-ns");
                properties.setProperty(ConsumerRunningInfo.PROP_CONSUME_TYPE, "owner-type");
                properties.setProperty(ConsumerRunningInfo.PROP_CLIENT_VERSION, "owner-version");
            }
        }
        ConsumeType consumeType = "null-consume-type".equals(name) ? null
            : name.startsWith("passive") ? ConsumeType.CONSUME_PASSIVELY : ConsumeType.CONSUME_ACTIVELY;
        MQConsumerInner consumer = (MQConsumerInner) Proxy.newProxyInstance(
            RuntimeConsumerOperationsFixtures.class.getClassLoader(), new Class<?>[] {MQConsumerInner.class},
            (proxy, method, arguments) -> {
                if ("consumerRunningInfo".equals(method.getName())) {
                    calls.add("info");
                    if ("snapshot-throws".equals(name)) throw new IllegalStateException("snapshot failed");
                    return snapshot;
                }
                if ("consumeType".equals(method.getName())) { calls.add("type"); return consumeType; }
                throw new IllegalStateException("unexpected consumer method: " + method.getName());
            });
        List<String> addresses = "active-address-list".equals(name)
            ? List.of("ns-b:9876", "ns-a:9876", "ns-b:9876")
            : "passive-empty-addresses".equals(name) ? List.of() : null;
        Map<String, Object> item = new TreeMap<>();
        try {
            if (addresses != null) {
                var remoting = runtime.getMQClientAPIImpl().getRemotingClient();
                remoting.updateNameServerAddressList(new ArrayList<>(List.of("seed:9876")));
                // The public getter is a live list. Pin its order after the
                // updater's documented shuffle; no ordering is normalized away.
                remoting.getNameServerAddressList().clear();
                remoting.getNameServerAddressList().addAll(addresses);
            }
            if (!"missing-group".equals(name)) runtime.getConsumerTable().put("selected", consumer);
            String group = "null-group".equals(name) ? null : "selected";
            item.put("name", name); item.put("addresses", addresses); item.put("group", group);
            item.put("consumeType", consumeType == null ? null : consumeType.name());
            try {
                ConsumerRunningInfo result = runtime.consumerRunningInfo(group);
                item.put("resultPresent", result != null);
                item.put("sameSnapshot", result != null && result == snapshot);
                item.put("sameProperties", result != null && result.getProperties() == properties);
                item.put("error", null);
            } catch (Exception error) {
                item.put("error", error.getClass().getSimpleName());
            }
            item.put("calls", calls);
            item.put("propertiesAfter", properties == null ? null : new TreeMap<>(properties));
            return item;
        } finally {
            runtime.getConsumerTable().clear();
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("fixture teardown"));
        }
    }
}
