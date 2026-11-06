package compat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueForC;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

/** Actual Java route converters/VIP transform; no client or server is started. */
public final class RuntimeRouteFixtures {
    private RuntimeRouteFixtures() { }

    private static final class RouteApi extends org.apache.rocketmq.client.impl.MQClientAPIImpl {
        TopicRouteData route;
        int requests;
        final List<String> requestedTopics = new ArrayList<>();
        RouteApi(org.apache.rocketmq.client.ClientConfig config) {
            super(new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient) java.lang.reflect.Proxy.newProxyInstance(
                    org.apache.rocketmq.remoting.RemotingClient.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> method.getReturnType() == boolean.class ? false : null));
        }
        @Override
        public TopicRouteData getTopicRouteInfoFromNameServer(String topic, long timeout) {
            requests++;
            requestedTopics.add(topic);
            return route;
        }
    }

    private static Map<String, Object> nullSubscriptionTopicRefresh() throws Exception {
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("route-null-subscription-topic");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        RouteApi api = new RouteApi(config);
        Map<String, Object> item = new TreeMap<>();
        try {
            java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            field.setAccessible(true);
            field.set(runtime, api);
            org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData nullTopic =
                new org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData();
            org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData named =
                new org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData();
            named.setTopic("topic");
            java.util.Set<org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData> subscriptions =
                new java.util.HashSet<>(java.util.Arrays.asList(nullTopic, named));
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumer =
                (org.apache.rocketmq.client.impl.consumer.MQConsumerInner) java.lang.reflect.Proxy.newProxyInstance(
                    org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class},
                    (proxy, method, values) -> "subscriptions".equals(method.getName())
                        ? subscriptions : method.getReturnType() == boolean.class ? false : null);
            if (!runtime.registerConsumer("consumer", consumer))
                throw new IllegalStateException("consumer registration");
            runtime.updateTopicRouteInfoFromNameServer();
            item.put("requests", api.requests);
            item.put("requestedNull", api.requestedTopics.contains(null));
            item.put("requestedNamed", api.requestedTopics.contains("topic"));
            api.route = new TopicRouteData();
            try {
                runtime.updateTopicRouteInfoFromNameServer((String) null);
                item.put("successRouteOutcome", "OK");
            } catch (RuntimeException error) {
                item.put("successRouteOutcome", error.getClass().getSimpleName());
            }
        } finally {
            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            try { cleanup.invoke(runtime, original, new IllegalStateException("null subscription topic fixture teardown")); }
            finally { api.shutdown(); }
        }
        return item;
    }

    private static List<Object> runtimeNullBrokerValues() throws Exception {
        List<Object> cases = new ArrayList<>();
        for (boolean slavePresent : new boolean[] {false, true}) {
            org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
            config.setClientIP("route-null-values-" + slavePresent);
            MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
            org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
            RouteApi api = new RouteApi(config);
            Map<String, Object> item = new TreeMap<>();
            try {
                java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
                field.setAccessible(true); field.set(runtime, api);
                String input = "{\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":2,\"writeQueueNums\":2,\"perm\":6,\"topicSysFlag\":0}],"
                    + "\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":null"
                    + (slavePresent ? ",\"1\":\"slave:10911\"" : "") + "}}],\"filterServerTable\":{}}";
                item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
                api.route = com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class);
                item.put("decodedMasterKeyPresent", api.route.getBrokerDatas().get(0).getBrokerAddrs().containsKey(0L));
                item.put("decodedMasterNull", api.route.getBrokerDatas().get(0).getBrokerAddrs().get(0L) == null);
                item.put("changed", runtime.updateTopicRouteInfoFromNameServer("topic"));
                TopicRouteData cached = runtime.getAnExistTopicRouteData("topic");
                item.put("cachedMasterKeyPresent", cached.getBrokerDatas().get(0).getBrokerAddrs().containsKey(0L));
                item.put("cachedMasterNull", cached.getBrokerDatas().get(0).getBrokerAddrs().get(0L) == null);
                item.put("publishQueueCount", MQClientInstance.topicRouteData2TopicPublishInfo("topic", cached).getMessageQueueList().size());
                item.put("subscribeQueueCount", MQClientInstance.topicRouteData2TopicSubscribeInfo("topic", cached).size());
                item.put("publishAddress", runtime.findBrokerAddressInPublish("broker"));
                org.apache.rocketmq.client.impl.FindBrokerResult admin = runtime.findBrokerAddressInAdmin("broker");
                item.put("adminAddress", admin == null ? null : admin.getBrokerAddr());
                org.apache.rocketmq.client.impl.FindBrokerResult exact = runtime.findBrokerAddressInSubscribe("broker", 0L, true);
                item.put("subscribeExactMasterAddress", exact == null ? null : exact.getBrokerAddr());
                boolean onlyAllowedChoices = true;
                for (int sample = 0; sample < 128; sample++) {
                    String address = runtime.findBrokerAddrByTopic("topic");
                    onlyAllowedChoices &= address == null || (slavePresent && "slave:10911".equals(address));
                }
                item.put("topicSelectionOnlyAllowedChoices", onlyAllowedChoices);
                item.put("outcome", "OK");
            } finally {
                java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                    org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
                cleanup.setAccessible(true);
                try { cleanup.invoke(runtime, original, new IllegalStateException("null address-value fixture teardown")); }
                finally { api.shutdown(); }
            }
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> runtimeTopicPublishInfoPublication() throws Exception {
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("route-publish-info-state");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        RouteApi api = new RouteApi(config);
        Map<String, Object> result = new TreeMap<>();
        Map<String, org.apache.rocketmq.client.impl.producer.TopicPublishInfo> published = new HashMap<>();
        String input = "{\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":0,\"writeQueueNums\":0,\"perm\":6,\"topicSysFlag\":0}],"
            + "\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"127.0.0.1:10911\"}}],"
            + "\"filterServerTable\":{\"broker\":[\"filter:10911\"]}}";
        result.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
        try {
            java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            field.setAccessible(true);
            field.set(runtime, api);
            api.route = com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class);
            org.apache.rocketmq.client.impl.producer.MQProducerInner producer =
                (org.apache.rocketmq.client.impl.producer.MQProducerInner) java.lang.reflect.Proxy.newProxyInstance(
                    org.apache.rocketmq.client.impl.producer.MQProducerInner.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.client.impl.producer.MQProducerInner.class},
                    (proxy, method, values) -> {
                        if ("updateTopicPublishInfo".equals(method.getName())) {
                            published.put((String) values[0],
                                (org.apache.rocketmq.client.impl.producer.TopicPublishInfo) values[1]);
                            return null;
                        }
                        if ("getPublishTopicList".equals(method.getName())) return java.util.Collections.emptySet();
                        if (method.getReturnType() == boolean.class) return false;
                        if (method.getReturnType() == int.class) return 0;
                        return null;
                    });
            runtime.getProducerTable().put("producer", producer);
            result.put("producerInserted", runtime.getProducerTable().containsKey("producer"));
            result.put("acceptedRouteChanged", runtime.updateTopicRouteInfoFromNameServer("topic"));
            org.apache.rocketmq.client.impl.producer.TopicPublishInfo info = published.get("topic");
            result.put("callbackDelivered", info != null);
            result.put("haveTopicRouterInfo", info != null && info.isHaveTopicRouterInfo());
            result.put("ok", info != null && info.ok());
            result.put("publishQueueCount", info == null ? null : info.getMessageQueueList().size());
            result.put("topicRouteDataPresent", info != null && info.getTopicRouteData() != null);
            TopicRouteData rawRoute = runtime.getTopicRouteTable().get("topic");
            TopicRouteData publishedRoute = info == null ? null : info.getTopicRouteData();
            result.put("rawRouteQueueListSameAsApiRoute", rawRoute != null && api.route != null
                && rawRoute.getQueueDatas() == api.route.getQueueDatas());
            result.put("rawRouteQueueElementSameAsApiRoute", rawRoute != null && api.route != null
                && rawRoute.getQueueDatas() != null && api.route.getQueueDatas() != null
                && !rawRoute.getQueueDatas().isEmpty() && !api.route.getQueueDatas().isEmpty()
                && rawRoute.getQueueDatas().get(0) == api.route.getQueueDatas().get(0));
            result.put("publishedQueueListSameAsRaw", rawRoute != null && publishedRoute != null
                && rawRoute.getQueueDatas() == publishedRoute.getQueueDatas());
            result.put("publishedQueueElementSameAsRaw", rawRoute != null && publishedRoute != null
                && rawRoute.getQueueDatas() != null && publishedRoute.getQueueDatas() != null
                && !rawRoute.getQueueDatas().isEmpty() && !publishedRoute.getQueueDatas().isEmpty()
                && rawRoute.getQueueDatas().get(0) == publishedRoute.getQueueDatas().get(0));
            result.put("publishedBrokerListSameAsRaw", rawRoute != null && publishedRoute != null
                && rawRoute.getBrokerDatas() == publishedRoute.getBrokerDatas());
            result.put("rawRouteBrokerElementSameAsApiRoute", rawRoute != null && api.route != null
                && rawRoute.getBrokerDatas() != null && api.route.getBrokerDatas() != null
                && !rawRoute.getBrokerDatas().isEmpty() && !api.route.getBrokerDatas().isEmpty()
                && rawRoute.getBrokerDatas().get(0) == api.route.getBrokerDatas().get(0));
            result.put("publishedBrokerElementSameAsRaw", rawRoute != null && publishedRoute != null
                && rawRoute.getBrokerDatas() != null && publishedRoute.getBrokerDatas() != null
                && !rawRoute.getBrokerDatas().isEmpty() && !publishedRoute.getBrokerDatas().isEmpty()
                && rawRoute.getBrokerDatas().get(0) == publishedRoute.getBrokerDatas().get(0));
            result.put("publishedFilterTableSameAsRaw", rawRoute != null && publishedRoute != null
                && rawRoute.getFilterServerTable() == publishedRoute.getFilterServerTable());
            result.put("rawRouteFilterTableSameAsApiRoute", rawRoute != null && api.route != null
                && rawRoute.getFilterServerTable() == api.route.getFilterServerTable());
            result.put("publishedFilterListSameAsRaw", rawRoute != null && publishedRoute != null
                && rawRoute.getFilterServerTable() != null && publishedRoute.getFilterServerTable() != null
                && rawRoute.getFilterServerTable().get("broker") != null
                && rawRoute.getFilterServerTable().get("broker")
                    == publishedRoute.getFilterServerTable().get("broker"));
            result.put("rawRouteFilterListSameAsApiRoute", rawRoute != null && api.route != null
                && rawRoute.getFilterServerTable() != null && api.route.getFilterServerTable() != null
                && rawRoute.getFilterServerTable().get("broker") != null
                && rawRoute.getFilterServerTable().get("broker")
                    == api.route.getFilterServerTable().get("broker"));
            api.route = null;
            result.put("missingRouteChanged", runtime.updateTopicRouteInfoFromNameServer("missing"));
            result.put("missingRouteCallbackDelivered", published.containsKey("missing"));
        } finally {
            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            try { cleanup.invoke(runtime, original, new IllegalStateException("publish info state fixture teardown")); }
            finally { api.shutdown(); }
        }
        return result;
    }

    private static List<Object> runtimeNullNestedRouteElements() throws Exception {
        String[][] inputs = new String[][] {
            {"null-queue", "{\"queueDatas\":[null],\"brokerDatas\":[],\"filterServerTable\":{}}"},
            {"null-broker", "{\"queueDatas\":[],\"brokerDatas\":[null],\"filterServerTable\":{}}"},
            {"null-static-mapping", "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":{},"
                + "\"topicQueueMappingByBroker\":{\"broker\":null}}"},
            {"null-filter-list", "{\"queueDatas\":[],\"brokerDatas\":[],"
                + "\"filterServerTable\":{\"broker\":null}}"}
        };
        List<Object> cases = new ArrayList<>();
        for (String[] entry : inputs) {
            org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
            config.setClientIP("route-null-nested-" + entry[0]);
            MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
            org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
            RouteApi api = new RouteApi(config);
            Map<String, Object> item = new TreeMap<>();
            try {
                java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
                field.setAccessible(true);
                field.set(runtime, api);
                item.put("id", entry[0]);
                item.put("input", com.alibaba.fastjson2.JSON.parseObject(entry[1]));
                api.route = com.alibaba.fastjson2.JSON.parseObject(entry[1], TopicRouteData.class);
                try {
                    item.put("changed", runtime.updateTopicRouteInfoFromNameServer("topic"));
                    item.put("outcome", "OK");
                } catch (RuntimeException error) {
                    item.put("outcome", error.getClass().getSimpleName());
                }
                item.put("cachedRouteExists", runtime.getAnExistTopicRouteData("topic") != null);
                item.put("requests", api.requests);
            } finally {
                java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                    org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
                cleanup.setAccessible(true);
                try { cleanup.invoke(runtime, original, new IllegalStateException("null nested route fixture teardown")); }
                finally { api.shutdown(); }
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> nullableRouteCollections() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{\"queueDatas\":null,\"brokerDatas\":[],\"filterServerTable\":{}}",
            "{\"queueDatas\":[],\"brokerDatas\":null,\"filterServerTable\":{}}",
            "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":null}",
            "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":{}}"
        }) {
            TopicRouteData route = com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class);
            TopicRouteData copy = new TopicRouteData(route);
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
            item.put("decodedQueueNull", route.getQueueDatas() == null);
            item.put("decodedBrokerNull", route.getBrokerDatas() == null);
            item.put("decodedFilterNull", route.getFilterServerTable() == null);
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(route.toJson());
            item.put("encodedHasQueue", encoded.containsKey("queueDatas"));
            item.put("encodedHasBroker", encoded.containsKey("brokerDatas"));
            item.put("encodedHasFilter", encoded.containsKey("filterServerTable"));
            item.put("encodedHasMapping", encoded.containsKey("topicQueueMappingByBroker"));
            item.put("copyQueueEmpty", copy.getQueueDatas().isEmpty());
            item.put("copyBrokerEmpty", copy.getBrokerDatas().isEmpty());
            item.put("copyFilterEmpty", copy.getFilterServerTable().isEmpty());
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> nullableNestedRouteFields() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{\"queueDatas\":[{}],\"brokerDatas\":[{}]}",
            "{\"queueDatas\":[{\"brokerName\":null}],\"brokerDatas\":[{\"cluster\":null,\"brokerName\":null}]}",
            "{\"queueDatas\":[{\"brokerName\":\"broker\",\"perm\":6}],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\"}]}",
            "{\"queueDatas\":[{\"brokerName\":null,\"readQueueNums\":1,\"perm\":4}],\"brokerDatas\":[{\"brokerName\":\"other\"}]}"
        }) {
            TopicRouteData route = com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class);
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(route.toJson());
            com.alibaba.fastjson2.JSONObject queue = encoded.getJSONArray("queueDatas").getJSONObject(0);
            com.alibaba.fastjson2.JSONObject broker = encoded.getJSONArray("brokerDatas").getJSONObject(0);
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
            item.put("queueNameNull", route.getQueueDatas().get(0).getBrokerName() == null);
            item.put("brokerNameNull", route.getBrokerDatas().get(0).getBrokerName() == null);
            item.put("clusterNull", route.getBrokerDatas().get(0).getCluster() == null);
            item.put("queueRead", route.getQueueDatas().get(0).getReadQueueNums());
            item.put("queueWrite", route.getQueueDatas().get(0).getWriteQueueNums());
            item.put("queuePerm", route.getQueueDatas().get(0).getPerm());
            item.put("queueFlag", route.getQueueDatas().get(0).getTopicSysFlag());
            item.put("encodedHasQueueName", queue.containsKey("brokerName"));
            item.put("encodedHasBrokerName", broker.containsKey("brokerName"));
            item.put("encodedHasCluster", broker.containsKey("cluster"));
            java.util.Set<MessageQueue> subscriptions = MQClientInstance.topicRouteData2TopicSubscribeInfo("topic", route);
            item.put("subscribeQueueCount", subscriptions.size());
            item.put("subscribeQueueBrokerNull", subscriptions.stream().anyMatch(queueValue -> queueValue.getBrokerName() == null));
            item.put("subscribeJsonBrokerOmitted", subscriptions.stream().anyMatch(queueValue ->
                !com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(queueValue)).containsKey("brokerName")));
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> nullableMessageQueueFields() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{}",
            "{\"topic\":null,\"brokerName\":null,\"queueId\":0}",
            "{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":-2}"
        }) {
            MessageQueue queue = com.alibaba.fastjson2.JSON.parseObject(input, MessageQueue.class);
            com.alibaba.fastjson2.JSONObject encoded =
                com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(queue));
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
            item.put("topicNull", queue.getTopic() == null);
            item.put("brokerNull", queue.getBrokerName() == null);
            item.put("queueId", queue.getQueueId());
            item.put("encodedHasTopic", encoded.containsKey("topic"));
            item.put("encodedHasBroker", encoded.containsKey("brokerName"));
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> messageQueueCompareToCases() {
        List<Object> cases = new ArrayList<>();
        MessageQueue[][] pairs = new MessageQueue[][] {
            {new MessageQueue(null, "b", 0), new MessageQueue("t", "b", 0)},
            {new MessageQueue("t", "b", 0), new MessageQueue(null, "b", 0)},
            {new MessageQueue("t", null, 0), new MessageQueue("t", "b", 0)},
            {new MessageQueue("t", "b", 0), new MessageQueue("t", null, 0)},
            {new MessageQueue("a", "b", 0), new MessageQueue("c", "b", 0)},
            {new MessageQueue("abc", "b", 0), new MessageQueue("a", "b", 0)},
            {new MessageQueue("\uD800\uDC00", "b", 0), new MessageQueue("\uE000", "b", 0)},
            {new MessageQueue("t", "a", 0), new MessageQueue("t", "c", 0)},
            {new MessageQueue("t", "b", Integer.MAX_VALUE), new MessageQueue("t", "b", -1)},
            {new MessageQueue("t", "b", Integer.MIN_VALUE), new MessageQueue("t", "b", 1)},
            {new MessageQueue("t", "b", 5), new MessageQueue("t", "b", 5)}
        };
        for (MessageQueue[] pair : pairs) {
            Map<String, Object> item = new TreeMap<>();
            item.put("left", pair[0]);
            item.put("right", pair[1]);
            try {
                item.put("result", pair[0].compareTo(pair[1]));
                item.put("outcome", "OK");
            } catch (NullPointerException error) {
                item.put("outcome", "NullPointerException");
            }
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> compareToNullOtherCases() {
        Map<String, Object> cases = new TreeMap<>();
        cases.put("messageQueue", compareToNullOutcome(
            () -> new MessageQueue("topic", "broker", 0).compareTo(null)));
        cases.put("messageQueueForC", compareToNullOutcome(
            () -> new MessageQueueForC("topic", "broker", 0, 0).compareTo(null)));
        cases.put("queueData", compareToNullOutcome(
            () -> queueData("broker", 1, 1, 1, 1).compareTo(null)));
        cases.put("brokerData", compareToNullOutcome(
            () -> brokerData("cluster", "broker", null, false, "zone").compareTo(null)));
        return cases;
    }

    private static String compareToNullOutcome(java.util.function.IntSupplier comparison) {
        try {
            comparison.getAsInt();
            return "NONE";
        } catch (Throwable error) {
            return error.getClass().getSimpleName();
        }
    }

    private static List<Object> messageQueueMethodCases() {
        MessageQueue[] queues = new MessageQueue[] {
            new MessageQueue(),
            new MessageQueue("topic", "broker", -17),
            new MessageQueue("\uD834\uDD1E", "Br\u00e9", Integer.MIN_VALUE)
        };
        List<Object> cases = new ArrayList<>();
        for (int index = 0; index < queues.length; index++) {
            MessageQueue queue = queues[index];
            Map<String, Object> item = new TreeMap<>();
            item.put("case", new String[] {"defaults", "populated", "unicode-boundary"}[index]);
            item.put("queue", messageQueueRow(queue));
            item.put("hashCode", queue.hashCode());
            item.put("toString", queue.toString());
            item.put("equalsSelf", queue.equals(queue));
            item.put("equalsCopy", queue.equals(new MessageQueue(queue)));
            item.put("equalsNull", queue.equals(null));
            item.put("equalsWrongType", queue.equals("not a MessageQueue"));
            item.put("equalsTopicChanged", queue.equals(new MessageQueue(
                queue.getTopic() == null ? "topic" : queue.getTopic() + "x",
                queue.getBrokerName(), queue.getQueueId())));
            item.put("equalsBrokerChanged", queue.equals(new MessageQueue(
                queue.getTopic(),
                queue.getBrokerName() == null ? "broker" : queue.getBrokerName() + "x",
                queue.getQueueId())));
            item.put("equalsQueueIdChanged", queue.equals(new MessageQueue(
                queue.getTopic(), queue.getBrokerName(), queue.getQueueId() + 1)));
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> messageQueueRow(MessageQueue queue) {
        Map<String, Object> result = new TreeMap<>();
        result.put("topic", queue.getTopic());
        result.put("brokerName", queue.getBrokerName());
        result.put("queueId", queue.getQueueId());
        return result;
    }

    private static Map<String, Object> messageQueueForCApiCases() {
        MessageQueueForC queue = new MessageQueueForC("topic", "broker", -7, 42);
        Map<String, Object> result = new TreeMap<>();
        result.put("values", messageQueueForCRow(queue));
        result.put("hashCode", queue.hashCode());
        result.put("toString", queue.toString());
        result.put("equalsSelf", queue.equals(queue));
        result.put("equalsCopy", queue.equals(new MessageQueueForC("topic", "broker", -7, 42)));
        result.put("equalsNull", queue.equals(null));
        result.put("equalsWrongType", queue.equals("not a MessageQueueForC"));
        result.put("equalsOffsetChanged", queue.equals(new MessageQueueForC("topic", "broker", -7, 43)));
        result.put("hashIgnoresOffset", queue.hashCode() == new MessageQueueForC("topic", "broker", -7, 43).hashCode());

        MessageQueueForC mutated = new MessageQueueForC("old-topic", "old-broker", 0, 0);
        mutated.setTopic("new-topic");
        mutated.setBrokerName(null);
        mutated.setQueueId(Integer.MIN_VALUE);
        mutated.setOffset(Long.MIN_VALUE);
        result.put("setterValues", messageQueueForCRow(mutated));
        return result;
    }

    private static Map<String, Object> messageQueueForCRow(MessageQueueForC queue) {
        Map<String, Object> result = new TreeMap<>();
        result.put("topic", queue.getTopic());
        result.put("brokerName", queue.getBrokerName());
        result.put("queueId", queue.getQueueId());
        result.put("offset", queue.getOffset());
        return result;
    }

    private static List<Object> messageQueueForCCompareToCases() {
        MessageQueueForC[][] pairs = new MessageQueueForC[][] {
            {new MessageQueueForC("a", "b", 0, 0), new MessageQueueForC("c", "b", 0, 0)},
            {new MessageQueueForC("abc", "b", 0, 0), new MessageQueueForC("a", "b", 0, 0)},
            {new MessageQueueForC("\uD800\uDC00", "b", 0, 0), new MessageQueueForC("\uE000", "b", 0, 0)},
            {new MessageQueueForC("t", "b", Integer.MAX_VALUE, 0), new MessageQueueForC("t", "b", Integer.MIN_VALUE, 0)},
            {new MessageQueueForC("t", "b", 0, Long.MIN_VALUE), new MessageQueueForC("t", "b", 0, Long.MAX_VALUE)},
            {new MessageQueueForC("t", "b", 0, 5), new MessageQueueForC("t", "b", 0, 3)},
            {new MessageQueueForC(null, "b", 0, 0), new MessageQueueForC("t", "b", 0, 0)},
            {new MessageQueueForC("t", "b", 0, 0), new MessageQueueForC(null, "b", 0, 0)},
            {new MessageQueueForC("t", null, 0, 0), new MessageQueueForC("t", "b", 0, 0)},
            {new MessageQueueForC("t", "b", 0, 0), new MessageQueueForC("t", null, 0, 0)},
            {new MessageQueueForC("t", "b", 0, 5), new MessageQueueForC("t", "b", 0, 5)},
            {new MessageQueueForC("t", "b", 0, 3), new MessageQueueForC("t", "b", 0, 5)}
        };
        List<Object> cases = new ArrayList<>();
        for (MessageQueueForC[] pair : pairs) {
            Map<String, Object> item = new TreeMap<>();
            item.put("left", messageQueueForCRow(pair[0]));
            item.put("right", messageQueueForCRow(pair[1]));
            try {
                item.put("result", pair[0].compareTo(pair[1]));
                item.put("outcome", "OK");
            } catch (Throwable error) {
                item.put("outcome", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> nullableStaticMappingFields() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{}",
            "{\"topic\":null,\"bname\":null,\"scope\":null,\"currIdMap\":null}",
            "{\"topic\":\"T\",\"bname\":\"B\",\"scope\":\"S\",\"currIdMap\":{\"0\":1}}"
        }) {
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo mapping =
                com.alibaba.fastjson2.JSON.parseObject(input,
                    org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo.class);
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(mapping.toJson());
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
            item.put("topicNull", mapping.getTopic() == null);
            item.put("bnameNull", mapping.getBname() == null);
            item.put("scopeNull", mapping.getScope() == null);
            item.put("currIdMapNull", mapping.getCurrIdMap() == null);
            item.put("currIdMapEmpty", mapping.getCurrIdMap() != null && mapping.getCurrIdMap().isEmpty());
            item.put("encodedHasTopic", encoded.containsKey("topic"));
            item.put("encodedHasBname", encoded.containsKey("bname"));
            item.put("encodedHasScope", encoded.containsKey("scope"));
            item.put("encodedHasCurrIdMap", encoded.containsKey("currIdMap"));
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> staticMappingMapEntryCases() {
        List<Object> cases = new ArrayList<>();
        for (String[] testCase : new String[][] {
            {"null-value", "{\"currIdMap\":{\"0\":null}}"},
            {"null-key", "{\"currIdMap\":{\"null\":1}}"},
            {"invalid-key", "{\"currIdMap\":{\"not-a-number\":1}}"},
            {"overflow-key", "{\"currIdMap\":{\"2147483648\":1}}"},
            {"invalid-value", "{\"currIdMap\":{\"0\":\"not-a-number\"}}"},
            {"integer-limits", "{\"currIdMap\":{\"-2147483648\":-2147483648,\"2147483647\":2147483647}}"}
        }) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", testCase[0]);
            item.put("rawInput", testCase[1]);
            try {
                org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo mapping =
                    com.alibaba.fastjson2.JSON.parseObject(testCase[1],
                        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo.class);
                item.put("outcome", "accepted");
                item.put("mapSize", mapping.getCurrIdMap() == null ? null : mapping.getCurrIdMap().size());
                item.put("hasKeyZero", mapping.getCurrIdMap() != null && mapping.getCurrIdMap().containsKey(0));
                item.put("keyZeroValue", mapping.getCurrIdMap() == null ? null : mapping.getCurrIdMap().get(0));
            } catch (Throwable error) {
                item.put("outcome", "rejected");
                item.put("error", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> routeDtoInputCase(String id, String type, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("id", id);
        item.put("type", type);
        item.put("rawInput", rawInput);
        try {
            Map<String, Object> fields = new TreeMap<>();
            switch (type) {
                case "queueData": {
                    org.apache.rocketmq.remoting.protocol.route.QueueData value =
                        com.alibaba.fastjson2.JSON.parseObject(rawInput,
                            org.apache.rocketmq.remoting.protocol.route.QueueData.class);
                    fields.put("brokerName", value.getBrokerName());
                    fields.put("readQueueNums", value.getReadQueueNums());
                    fields.put("writeQueueNums", value.getWriteQueueNums());
                    fields.put("perm", value.getPerm());
                    fields.put("topicSysFlag", value.getTopicSysFlag());
                    break;
                }
                case "messageQueue": {
                    MessageQueue value = com.alibaba.fastjson2.JSON.parseObject(rawInput, MessageQueue.class);
                    fields.put("topic", value.getTopic());
                    fields.put("brokerName", value.getBrokerName());
                    fields.put("queueId", value.getQueueId());
                    break;
                }
                case "brokerData": {
                    org.apache.rocketmq.remoting.protocol.route.BrokerData value =
                        com.alibaba.fastjson2.JSON.parseObject(rawInput,
                            org.apache.rocketmq.remoting.protocol.route.BrokerData.class);
                    fields.put("cluster", value.getCluster());
                    fields.put("brokerName", value.getBrokerName());
                    fields.put("zoneName", value.getZoneName());
                    fields.put("enableActingMaster", value.isEnableActingMaster());
                    Map<String, Object> addresses = new TreeMap<>();
                    if (value.getBrokerAddrs() != null) {
                        value.getBrokerAddrs().forEach((key, address) -> addresses.put(String.valueOf(key), address));
                    }
                    fields.put("brokerAddrs", value.getBrokerAddrs() == null ? null : addresses);
                    if (id.startsWith("broker-address-") && id.contains("null-key")) {
                        item.put("brokerAddrsNullKeyPresent", value.getBrokerAddrs().containsKey(null));
                        item.put("brokerAddrsStringNullKeyPresent", value.getBrokerAddrs().containsKey("null"));
                    }
                    break;
                }
                case "mapping": {
                    org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo value =
                        com.alibaba.fastjson2.JSON.parseObject(rawInput,
                            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo.class);
                    fields.put("topic", value.getTopic());
                    fields.put("bname", value.getBname());
                    fields.put("scope", value.getScope());
                    fields.put("totalQueues", value.getTotalQueues());
                    fields.put("epoch", value.getEpoch());
                    fields.put("dirty", value.isDirty());
                    Map<String, Object> ids = new TreeMap<>();
                    if (value.getCurrIdMap() != null) {
                        value.getCurrIdMap().forEach((key, number) -> ids.put(String.valueOf(key), number));
                    }
                    fields.put("currIdMap", value.getCurrIdMap() == null ? null : ids);
                    break;
                }
                case "topicRouteData": {
                    TopicRouteData value = com.alibaba.fastjson2.JSON.parseObject(rawInput, TopicRouteData.class);
                    fields.putAll(routeJson(value));
                    break;
                }
                default: throw new IllegalArgumentException("unknown DTO: " + type);
            }
            item.put("outcome", "ACCEPTED");
            item.put("fields", fields);
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> routeDtoInputCases() {
        List<Object> cases = new ArrayList<>();
        String[] rootDtoTypes = {
            "queueData", "messageQueue", "brokerData", "mapping", "topicRouteData"
        };
        String[][] rootShapes = {
            {"empty-object", "{}"}, {"null", "null"}, {"empty-array", "[]"},
            {"singleton-array", "[{}]"}, {"number", "0"}, {"boolean", "true"},
            {"string", "\"text\""}
        };
        for (String dto : rootDtoTypes) {
            for (String[] shape : rootShapes) {
                cases.add(routeDtoInputCase("root-" + dto + "-" + shape[0], dto, shape[1]));
            }
        }
        cases.add(routeDtoInputCase("queue-scalars", "queueData",
            "{\"brokerName\":123,\"readQueueNums\":\"2\",\"writeQueueNums\":1.9,\"perm\":true,\"topicSysFlag\":null}"));
        cases.add(routeDtoInputCase("queue-duplicate-broker-name-last-wins", "queueData",
            "{\"brokerName\":\"first\",\"brokerName\":123}"));
        cases.add(routeDtoInputCase("queue-duplicate-read-queue-nums-last-wins", "queueData",
            "{\"readQueueNums\":1,\"readQueueNums\":3.9}"));
        cases.add(routeDtoInputCase("queue-duplicate-write-queue-nums-last-wins", "queueData",
            "{\"writeQueueNums\":1,\"writeQueueNums\":2.9}"));
        cases.add(routeDtoInputCase("queue-duplicate-perm-last-wins", "queueData",
            "{\"perm\":3,\"perm\":0}"));
        cases.add(routeDtoInputCase("queue-duplicate-topic-sys-flag-last-wins", "queueData",
            "{\"topicSysFlag\":7,\"topicSysFlag\":null}"));
        cases.add(routeDtoInputCase("queue-uppercase-property", "queueData",
            "{\"READQUEUENUMS\":7,\"BROKERNAME\":\"upper\"}"));
        cases.add(routeDtoInputCase("queue-titlecase-property", "queueData",
            "{\"ReadQueueNums\":7,\"BrokerName\":\"title\"}"));
        cases.add(routeDtoInputCase("queue-snakecase-property", "queueData",
            "{\"read_queue_nums\":7,\"broker_name\":\"snake\"}"));
        cases.add(routeDtoInputCase("queue-case-collision", "queueData",
            "{\"readQueueNums\":2,\"READQUEUENUMS\":7}"));
        cases.add(routeDtoInputCase("broker-uppercase-property", "brokerData",
            "{\"BROKERNAME\":\"upper\",\"CLUSTER\":\"cluster\",\"BROKERADDRS\":{\"0\":\"master:10911\"}}"));
        cases.add(routeDtoInputCase("broker-duplicate-cluster-last-wins", "brokerData",
            "{\"cluster\":\"first\",\"cluster\":\"last\"}"));
        cases.add(routeDtoInputCase("broker-duplicate-name-last-wins", "brokerData",
            "{\"brokerName\":\"first\",\"brokerName\":\"last\"}"));
        cases.add(routeDtoInputCase("broker-duplicate-addresses-last-wins", "brokerData",
            "{\"brokerAddrs\":{\"0\":\"master\"},\"brokerAddrs\":{\"2\":\"slave\"}}"));
        cases.add(routeDtoInputCase("broker-duplicate-zone-last-wins", "brokerData",
            "{\"zoneName\":\"first\",\"zoneName\":\"last\"}"));
        cases.add(routeDtoInputCase("broker-duplicate-acting-master-last-wins", "brokerData",
            "{\"enableActingMaster\":false,\"enableActingMaster\":true}"));
        cases.add(routeDtoInputCase("mapping-duplicate-topic-last-wins", "mapping",
            "{\"topic\":\"first\",\"topic\":\"last\"}"));
        cases.add(routeDtoInputCase("mapping-duplicate-total-queues-last-wins", "mapping",
            "{\"totalQueues\":1,\"totalQueues\":2.9}"));
        cases.add(routeDtoInputCase("mapping-duplicate-bname-last-wins", "mapping",
            "{\"bname\":\"first\",\"bname\":\"last\"}"));
        cases.add(routeDtoInputCase("mapping-duplicate-epoch-last-wins", "mapping",
            "{\"epoch\":1,\"epoch\":2}"));
        cases.add(routeDtoInputCase("mapping-duplicate-dirty-last-wins", "mapping",
            "{\"dirty\":false,\"dirty\":true}"));
        cases.add(routeDtoInputCase("mapping-duplicate-scope-last-wins", "mapping",
            "{\"scope\":\"first\",\"scope\":\"last\"}"));
        cases.add(routeDtoInputCase("mapping-duplicate-curr-id-map-last-wins", "mapping",
            "{\"currIdMap\":{\"0\":0},\"currIdMap\":{\"2\":2}}"));
        cases.add(routeDtoInputCase("topic-route-duplicate-order-topic-last-wins", "topicRouteData",
            "{\"orderTopicConf\":\"first\",\"orderTopicConf\":\"last\"}"));
        cases.add(routeDtoInputCase("topic-route-duplicate-queue-datas-last-wins", "topicRouteData",
            "{\"queueDatas\":[{\"brokerName\":\"first\"}],\"queueDatas\":[{\"brokerName\":\"last\"}]}"));
        cases.add(routeDtoInputCase("topic-route-duplicate-broker-datas-last-wins", "topicRouteData",
            "{\"brokerDatas\":[{\"cluster\":\"old\",\"brokerName\":\"first\",\"brokerAddrs\":{\"0\":\"old\"},\"zoneName\":\"old\",\"enableActingMaster\":false}],\"brokerDatas\":[{\"cluster\":\"last\",\"brokerName\":\"last\",\"brokerAddrs\":{\"0\":\"last\"},\"zoneName\":\"last\",\"enableActingMaster\":true}]}"));
        cases.add(routeDtoInputCase("topic-route-duplicate-filter-server-table-last-wins", "topicRouteData",
            "{\"filterServerTable\":{\"broker\":[\"first\"]},\"filterServerTable\":{\"broker\":[\"last\"]}}"));
        cases.add(routeDtoInputCase("topic-route-duplicate-mapping-table-last-wins", "topicRouteData",
            "{\"topicQueueMappingByBroker\":{\"first\":{\"topic\":\"old\",\"bname\":\"first\"}},\"topicQueueMappingByBroker\":{\"last\":{\"topic\":\"topic\",\"bname\":\"last\"}}}"));
        cases.add(routeDtoInputCase("mapping-uppercase-property", "mapping",
            "{\"TOPIC\":\"topic\",\"TOTALQUEUES\":7,\"CURRIDMAP\":{\"0\":1}}"));
        cases.add(routeDtoInputCase("topic-route-uppercase-property", "topicRouteData",
            "{\"ORDERTOPICCONF\":\"broker:1\",\"QUEUEDATAS\":[{\"BROKERNAME\":\"broker\"}]}"));
        cases.add(routeDtoInputCase("queue-empty-object-number", "queueData",
            "{\"readQueueNums\":{}}"));
        cases.add(routeDtoInputCase("queue-unquoted-fraction-property-key", "queueData",
            "{1.0:\"unknown\"}"));
        cases.add(routeDtoInputCase("queue-unquoted-integer-property-key", "queueData",
            "{1:\"unknown\"}"));
        cases.add(routeDtoInputCase("queue-unquoted-plus-property-key", "queueData",
            "{+1:\"unknown\"}"));
        cases.add(routeDtoInputCase("queue-unknown-nested-unquoted-numeric-key", "queueData",
            "{\"unknown\":{1:\"value\"}}"));
        cases.add(routeDtoInputCase("queue-decimal-overflow-number", "queueData",
            "{\"readQueueNums\":2147483648.0}"));
        cases.add(routeDtoInputCase("message-queue-scalars", "messageQueue",
            "{\"topic\":123,\"brokerName\":false,\"queueId\":\"2\"}"));
        cases.add(routeDtoInputCase("message-queue-structured-strings", "messageQueue",
            "{\"topic\":[],\"brokerName\":{}}"));
        cases.add(routeDtoInputCase("message-queue-id-null", "messageQueue",
            "{\"queueId\":null}"));
        cases.add(routeDtoInputCase("message-queue-id-minimum", "messageQueue",
            "{\"queueId\":-2147483648}"));
        cases.add(routeDtoInputCase("message-queue-id-maximum", "messageQueue",
            "{\"queueId\":2147483647}"));
        cases.add(routeDtoInputCase("message-queue-id-fraction", "messageQueue",
            "{\"queueId\":-1.9}"));
        cases.add(routeDtoInputCase("message-queue-id-plus-string", "messageQueue",
            "{\"queueId\":\"+2\"}"));
        cases.add(routeDtoInputCase("message-queue-id-leading-zero-string", "messageQueue",
            "{\"queueId\":\"002\"}"));
        cases.add(routeDtoInputCase("message-queue-id-fraction-string", "messageQueue",
            "{\"queueId\":\"-1.9\"}"));
        cases.add(routeDtoInputCase("message-queue-id-exponent-string", "messageQueue",
            "{\"queueId\":\"1e2\"}"));
        cases.add(routeDtoInputCase("message-queue-id-whitespace-string", "messageQueue",
            "{\"queueId\":\" 2 \"}"));
        cases.add(routeDtoInputCase("message-queue-id-string-overflow", "messageQueue",
            "{\"queueId\":\"2147483648\"}"));
        cases.add(routeDtoInputCase("message-queue-id-string-underflow", "messageQueue",
            "{\"queueId\":\"-2147483649\"}"));
        cases.add(routeDtoInputCase("message-queue-unicode-decimal-id", "messageQueue",
            "{\"queueId\":\"１２\"}"));
        cases.add(routeDtoInputCase("message-queue-id-overflow", "messageQueue",
            "{\"queueId\":2147483648}"));
        cases.add(routeDtoInputCase("broker-scalars", "brokerData",
            "{\"cluster\":true,\"brokerName\":7,\"zoneName\":1.5,\"brokerAddrs\":{\"+1\":true,\"2\":3,\"3\":null},\"enableActingMaster\":\"TRUE\"}"));
        cases.add(routeDtoInputCase("broker-address-unquoted-integer-keys", "brokerData",
            "{\"brokerAddrs\":{0:\"master\",-1:\"slave\",9223372036854775807:\"maximum\"}}"));
        cases.add(routeDtoInputCase("broker-address-unquoted-leading-zero-key", "brokerData",
            "{\"brokerAddrs\":{01:\"leading-zero\"}}"));
        cases.add(routeDtoInputCase("broker-address-unquoted-plus-key", "brokerData",
            "{\"brokerAddrs\":{+1:\"plus\"}}"));
        cases.add(routeDtoInputCase("broker-address-unquoted-fraction-key", "brokerData",
            "{\"brokerAddrs\":{1.0:\"fraction\"}}"));
        cases.add(routeDtoInputCase("broker-address-unquoted-negative-fraction-key", "brokerData",
            "{\"brokerAddrs\":{-1.5:\"negative-fraction\"}}"));
        cases.add(routeDtoInputCase("broker-address-unquoted-exponent-key", "brokerData",
            "{\"brokerAddrs\":{1e2:\"exponent\"}}"));
        cases.add(routeDtoInputCase("broker-address-decimal-key", "brokerData",
            "{\"brokerAddrs\":{\"1.0\":0}}"));
        cases.add(routeDtoInputCase("broker-unicode-decimal-address-key", "brokerData",
            "{\"brokerAddrs\":{\"١٢\":\"master\"}}"));
        cases.add(routeDtoInputCase("broker-boolean-minus-one", "brokerData",
            "{\"enableActingMaster\":-1}"));
        cases.add(routeDtoInputCase("broker-boolean-other-number", "brokerData",
            "{\"enableActingMaster\":2}"));
        cases.add(routeDtoInputCase("broker-boolean-decimal-number", "brokerData",
            "{\"enableActingMaster\":1.0}"));
        cases.add(routeDtoInputCase("broker-boolean-high-precision-decimal-one", "brokerData",
            "{\"enableActingMaster\":1.0000000000000000001}"));
        cases.add(routeDtoInputCase("broker-boolean-numeric-string", "brokerData",
            "{\"enableActingMaster\":\"1\"}"));
        cases.add(routeDtoInputCase("broker-boolean-null", "brokerData",
            "{\"enableActingMaster\":null}"));
        cases.add(routeDtoInputCase("mapping-scalars", "mapping",
            "{\"topic\":42,\"bname\":true,\"scope\":3,\"totalQueues\":\"2\",\"epoch\":1.9,\"dirty\":\"TRUE\",\"currIdMap\":{\"01\":\"2\",\"2\":1.9,\"3\":true}}"));
        cases.add(routeDtoInputCase("mapping-curr-id-map-unquoted-integer-keys", "mapping",
            "{\"currIdMap\":{0:1,-1:2,2147483647:3}}"));
        cases.add(routeDtoInputCase("mapping-curr-id-map-unquoted-leading-zero-key", "mapping",
            "{\"currIdMap\":{01:1}}"));
        cases.add(routeDtoInputCase("mapping-curr-id-map-unquoted-plus-key", "mapping",
            "{\"currIdMap\":{+1:1}}"));
        cases.add(routeDtoInputCase("mapping-curr-id-map-unquoted-fraction-key", "mapping",
            "{\"currIdMap\":{1.0:1}}"));
        cases.add(routeDtoInputCase("mapping-curr-id-map-unquoted-negative-fraction-key", "mapping",
            "{\"currIdMap\":{-1.5:1}}"));
        cases.add(routeDtoInputCase("mapping-curr-id-map-unquoted-exponent-key", "mapping",
            "{\"currIdMap\":{1e2:1}}"));
        cases.add(routeDtoInputCase("mapping-exponent-string", "mapping",
            "{\"epoch\":\"1e2\"}"));
        cases.add(routeDtoInputCase("mapping-long-minimum", "mapping",
            "{\"epoch\":\"-9223372036854775808\"}"));
        cases.add(routeDtoInputCase("mapping-long-plus-maximum-string", "mapping",
            "{\"epoch\":\"+9223372036854775807\"}"));
        cases.add(routeDtoInputCase("mapping-long-leading-zero-string", "mapping",
            "{\"epoch\":\"0002\"}"));
        cases.add(routeDtoInputCase("mapping-long-fraction-string", "mapping",
            "{\"epoch\":\"1.9\"}"));
        cases.add(routeDtoInputCase("mapping-long-maximum-decimal-string", "mapping",
            "{\"epoch\":\"9223372036854775807.0\"}"));
        cases.add(routeDtoInputCase("mapping-long-minimum-decimal-string", "mapping",
            "{\"epoch\":\"-9223372036854775808.0\"}"));
        cases.add(routeDtoInputCase("mapping-long-overflow-decimal-string", "mapping",
            "{\"epoch\":\"9223372036854775808.0\"}"));
        cases.add(routeDtoInputCase("mapping-long-underflow-decimal-string", "mapping",
            "{\"epoch\":\"-9223372036854775809.0\"}"));
        cases.add(routeDtoInputCase("mapping-long-maximum-decimal-number", "mapping",
            "{\"epoch\":9223372036854775807.0}"));
        cases.add(routeDtoInputCase("mapping-long-overflow-decimal-number", "mapping",
            "{\"epoch\":9223372036854775808.0}"));
        cases.add(routeDtoInputCase("mapping-long-underflow-decimal-number", "mapping",
            "{\"epoch\":-9223372036854775809.0}"));
        cases.add(routeDtoInputCase("mapping-unicode-decimal-id-key", "mapping",
            "{\"currIdMap\":{\"１２\":1}}"));
        cases.add(routeDtoInputCase("mapping-curr-id-map-high-precision-decimals", "mapping",
            "{\"currIdMap\":{\"0\":9223372036854775807.0,\"1\":2147483648.0,\"-1\":-2147483649.0}}"));
        cases.add(routeDtoInputCase("broker-address-high-precision-number-strings", "brokerData",
            "{\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":9223372036854775807.123456789012345678901,\"1\":1e2}}"));
        cases.add(routeDtoInputCase("filter-server-high-precision-number-strings", "topicRouteData",
            "{\"filterServerTable\":{\"broker\":[9223372036854775807.123456789012345678901,1e2]}}"));
        String[][] nestedMemberShapes = {
            {"null", "null"}, {"string", "\"value\""}, {"integer", "7"},
            {"fraction", "1.5"}, {"boolean", "true"}, {"array", "[]"}, {"object", "{}"}
        };
        for (String[] shape : nestedMemberShapes) {
            cases.add(routeDtoInputCase("broker-address-map-value-" + shape[0], "brokerData",
                "{\"brokerAddrs\":{\"0\":" + shape[1] + "}}"));
            cases.add(routeDtoInputCase("filter-server-list-member-" + shape[0], "topicRouteData",
                "{\"filterServerTable\":{\"broker\":[" + shape[1] + "]}}"));
            cases.add(routeDtoInputCase("mapping-curr-id-map-value-" + shape[0], "mapping",
                "{\"currIdMap\":{\"0\":" + shape[1] + "}}"));
            cases.add(routeDtoInputCase("route-queue-data-element-" + shape[0], "topicRouteData",
                "{\"queueDatas\":[" + shape[1] + "]}"));
            cases.add(routeDtoInputCase("route-broker-data-element-" + shape[0], "topicRouteData",
                "{\"brokerDatas\":[" + shape[1] + "]}"));
            cases.add(routeDtoInputCase("route-filter-server-map-value-" + shape[0], "topicRouteData",
                "{\"filterServerTable\":{\"broker\":" + shape[1] + "}}"));
            cases.add(routeDtoInputCase("route-mapping-table-map-value-" + shape[0], "topicRouteData",
                "{\"topicQueueMappingByBroker\":{\"broker\":" + shape[1] + "}}"));
        }
        cases.add(routeDtoInputCase("broker-address-object-value-unquoted-numeric-key", "brokerData",
            "{\"brokerAddrs\":{\"0\":{1:\"value\"}}}"));
        cases.add(routeDtoInputCase("route-filter-server-map-value-empty-string", "topicRouteData",
            "{\"filterServerTable\":{\"broker\":\"\"}}"));
        cases.add(routeDtoInputCase("route-filter-server-unquoted-fraction-key", "topicRouteData",
            "{\"filterServerTable\":{1.0:[\"filter\"]}}"));
        cases.add(routeDtoInputCase("route-filter-server-unquoted-plus-key", "topicRouteData",
            "{\"filterServerTable\":{+1:[\"filter\"]}}"));
        cases.add(routeDtoInputCase("route-filter-server-unquoted-exponent-key", "topicRouteData",
            "{\"filterServerTable\":{1e2:[\"filter\"]}}"));
        cases.add(routeDtoInputCase("route-mapping-table-unquoted-leading-zero-key", "topicRouteData",
            "{\"topicQueueMappingByBroker\":{01:{\"topic\":\"topic\",\"bname\":\"broker\"}}}"));
        cases.add(routeDtoInputCase("route-mapping-table-unquoted-plus-key", "topicRouteData",
            "{\"topicQueueMappingByBroker\":{+1:{\"topic\":\"topic\",\"bname\":\"broker\"}}}"));
        cases.add(routeDtoInputCase("route-mapping-table-unquoted-exponent-key", "topicRouteData",
            "{\"topicQueueMappingByBroker\":{1e2:{\"topic\":\"topic\",\"bname\":\"broker\"}}}"));
        cases.add(routeDtoInputCase("queue-unicode-decimal-string", "queueData",
            "{\"readQueueNums\":\"１２\",\"writeQueueNums\":\"-١٢\"}"));
        cases.add(routeDtoInputCase("mapping-unicode-decimal-strings", "mapping",
            "{\"totalQueues\":\"१२\",\"epoch\":\"-１２\"}"));
        cases.add(routeDtoInputCase("mapping-long-overflow", "mapping",
            "{\"epoch\":9223372036854775808}"));
        cases.add(routeDtoInputCase("topic-route-nested-scalars", "topicRouteData",
            "{\"orderTopicConf\":7,\"queueDatas\":[{\"brokerName\":123,\"readQueueNums\":\"2\",\"writeQueueNums\":1.9,\"perm\":true,\"topicSysFlag\":null}],"
                + "\"brokerDatas\":[{\"cluster\":true,\"brokerName\":7,\"zoneName\":1.5,\"brokerAddrs\":{\"1.0\":true},\"enableActingMaster\":1}],"
                + "\"filterServerTable\":{\"broker\":[1,true,null],\"empty\":[],\"missing\":null},"
                + "\"topicQueueMappingByBroker\":{\"broker\":{\"topic\":42,\"totalQueues\":\"2\",\"bname\":true,\"epoch\":\"1e2\",\"dirty\":\"TRUE\",\"currIdMap\":{\"01\":\"2\"}}}}"));
        cases.add(routeDtoInputCase("queue-int-overflow", "queueData",
            "{\"readQueueNums\":2147483648}"));
        cases.add(routeDtoInputCase("mapping-null-id", "mapping",
            "{\"currIdMap\":{\"1\":null}}"));
        cases.add(routeDtoInputCase("broker-invalid-boolean", "brokerData",
            "{\"enableActingMaster\":\"yes\"}"));
        cases.add(routeDtoInputCase("queue-fastjson-single-quote-unquoted-key-trailing-comma", "queueData",
            "{brokerName:'broker',readQueueNums:'2',}"));
        cases.add(routeDtoInputCase("broker-fastjson-comments-unquoted-map-key", "brokerData",
            "/* route */ {cluster:'cluster',brokerName:'broker',brokerAddrs:{0:'master:10911',},} // tail\n"));
        cases.add(routeDtoInputCase("mapping-fastjson-line-comment", "mapping",
            "{topic:'topic', // mapping fields\n bname:'broker',totalQueues:2,currIdMap:{'0':0,},}"));
        cases.add(routeDtoInputCase("queue-fastjson-long-suffix", "queueData",
            "{readQueueNums:2L,writeQueueNums:3L,perm:4L,topicSysFlag:5L}"));
        cases.add(routeDtoInputCase("queue-fastjson-decimal-suffix", "queueData",
            "{readQueueNums:2.9D,writeQueueNums:3.9F}"));
        cases.add(routeDtoInputCase("queue-fastjson-leading-plus-number", "queueData",
            "{\"readQueueNums\":+2}"));
        cases.add(routeDtoInputCase("queue-fastjson-leading-zero-number", "queueData",
            "{\"readQueueNums\":02}"));
        cases.add(routeDtoInputCase("queue-fastjson-leading-plus-fraction", "queueData",
            "{\"writeQueueNums\":+2.9}"));
        cases.add(routeDtoInputCase("queue-fastjson-leading-zero-fraction", "queueData",
            "{\"writeQueueNums\":02.9}"));
        cases.add(routeDtoInputCase("mapping-fastjson-leading-plus-exponent", "mapping",
            "{\"epoch\":+2e1}"));
        cases.add(routeDtoInputCase("mapping-fastjson-leading-zero-exponent", "mapping",
            "{\"epoch\":02e1}"));
        cases.add(routeDtoInputCase("queue-fastjson-nan-number", "queueData",
            "{\"readQueueNums\":NaN}"));
        cases.add(routeDtoInputCase("queue-fastjson-infinity-number", "queueData",
            "{\"readQueueNums\":Infinity}"));
        cases.add(routeDtoInputCase("queue-fastjson-hex-number", "queueData",
            "{\"readQueueNums\":0x10}"));
        cases.add(routeDtoInputCase("queue-fastjson-vertical-tab-prefix", "queueData",
            String.valueOf((char) 0x0b) + "{\"readQueueNums\":2}"));
        cases.add(routeDtoInputCase("queue-fastjson-form-feed-prefix", "queueData",
            String.valueOf((char) 0x0c) + "{\"readQueueNums\":2}"));
        cases.add(routeDtoInputCase("queue-fastjson-form-feed-before-number", "queueData",
            "{\"readQueueNums\":" + String.valueOf((char) 0x0c) + "2}"));
        cases.add(routeDtoInputCase("queue-fastjson-form-feed-around-punctuation", "queueData",
            "{" + String.valueOf((char) 0x0c) + "\"readQueueNums\""
                + String.valueOf((char) 0x0c) + ":" + String.valueOf((char) 0x0c) + "2"
                + String.valueOf((char) 0x0c) + "}"));
        cases.add(routeDtoInputCase("queue-fastjson-vertical-tab-before-number", "queueData",
            "{\"readQueueNums\":" + String.valueOf((char) 0x0b) + "2}"));
        cases.add(routeDtoInputCase("queue-fastjson-nbsp-prefix", "queueData",
            String.valueOf((char) 0x00a0) + "{\"readQueueNums\":2}"));
        cases.add(routeDtoInputCase("queue-fastjson-bom-prefix", "queueData",
            String.valueOf((char) 0xfeff) + "{\"readQueueNums\":2}"));
        cases.add(routeDtoInputCase("queue-fastjson-bom-before-number", "queueData",
            "{\"readQueueNums\":" + String.valueOf((char) 0xfeff) + "2}"));
        cases.add(routeDtoInputCase("mapping-fastjson-long-suffix", "mapping",
            "{epoch:9223372036854775807L,totalQueues:2L,currIdMap:{0:1L}}"));
        cases.add(routeDtoInputCase("route-filter-string-as-list", "topicRouteData",
            "{filterServerTable:{broker:'server:10911'}}"));
        cases.add(routeDtoInputCase("route-filter-number-as-list", "topicRouteData",
            "{filterServerTable:{broker:1}}"));
        cases.add(routeDtoInputCase("route-null-and-scalar-queue-rows", "topicRouteData",
            "{queueDatas:[null,'broker'],brokerDatas:[]}"));
        cases.add(routeDtoInputCase("route-null-mapping-value", "topicRouteData",
            "{topicQueueMappingByBroker:{broker:null}}"));
        cases.add(routeDtoInputCase("broker-address-array-shape", "brokerData",
            "{brokerAddrs:[1,2]}"));
        cases.add(routeDtoInputCase("mapping-id-array-shape", "mapping",
            "{currIdMap:[1,2]}"));
        cases.add(routeDtoInputCase("broker-address-key-plus-leading-zero", "brokerData",
            "{\"brokerAddrs\":{\"+002\":\"master\"}}"));
        cases.add(routeDtoInputCase("broker-address-key-fraction-string", "brokerData",
            "{\"brokerAddrs\":{\"-1.9\":\"master\"}}"));
        cases.add(routeDtoInputCase("broker-address-key-exponent-string", "brokerData",
            "{\"brokerAddrs\":{\"1e2\":\"master\"}}"));
        cases.add(routeDtoInputCase("broker-address-key-whitespace-string", "brokerData",
            "{\"brokerAddrs\":{\" 2 \":\"master\"}}"));
        cases.add(routeDtoInputCase("broker-address-key-long-maximum", "brokerData",
            "{\"brokerAddrs\":{\"9223372036854775807\":\"master\"}}"));
        cases.add(routeDtoInputCase("broker-address-key-long-overflow", "brokerData",
            "{\"brokerAddrs\":{\"9223372036854775808\":\"master\"}}"));
        cases.add(routeDtoInputCase("mapping-id-key-plus-leading-zero", "mapping",
            "{\"currIdMap\":{\"+002\":1}}"));
        cases.add(routeDtoInputCase("mapping-id-key-fraction-string", "mapping",
            "{\"currIdMap\":{\"-1.9\":1}}"));
        cases.add(routeDtoInputCase("mapping-id-key-exponent-string", "mapping",
            "{\"currIdMap\":{\"1e2\":1}}"));
        cases.add(routeDtoInputCase("mapping-id-key-whitespace-string", "mapping",
            "{\"currIdMap\":{\" 2 \":1}}"));
        cases.add(routeDtoInputCase("mapping-id-key-int-maximum", "mapping",
            "{\"currIdMap\":{\"2147483647\":1}}"));
        cases.add(routeDtoInputCase("mapping-id-key-int-overflow", "mapping",
            "{\"currIdMap\":{\"2147483648\":1}}"));
        cases.add(routeDtoInputCase("route-queue-datas-single-object", "topicRouteData",
            "{\"queueDatas\":{\"brokerName\":\"broker\",\"readQueueNums\":2,\"writeQueueNums\":3}}"));
        cases.add(routeDtoInputCase("route-broker-datas-single-object", "topicRouteData",
            "{\"brokerDatas\":{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"127.0.0.1:10911\"},\"zoneName\":\"zone\"}}"));
        cases.add(routeDtoInputCase("route-queue-datas-null-element", "topicRouteData",
            "{\"queueDatas\":[null]}"));
        cases.add(routeDtoInputCase("route-broker-datas-null-element", "topicRouteData",
            "{\"brokerDatas\":[null]}"));
        cases.add(routeDtoInputCase("broker-address-key-normalization-collision-forward", "brokerData",
            "{\"brokerAddrs\":{\"5\":\"first\",\"05\":\"last\"}}"));
        cases.add(routeDtoInputCase("broker-address-key-normalization-collision-reverse", "brokerData",
            "{\"brokerAddrs\":{\"05\":\"first\",\"5\":\"last\"}}"));
        cases.add(routeDtoInputCase("mapping-id-key-normalization-collision-forward", "mapping",
            "{\"currIdMap\":{\"1\":10,\"01\":20}}"));
        cases.add(routeDtoInputCase("mapping-id-key-normalization-collision-reverse", "mapping",
            "{\"currIdMap\":{\"01\":10,\"1\":20}}"));
        cases.add(routeDtoInputCase("broker-address-key-normalization-collision-repeat-first-key-last-wins", "brokerData",
            "{\"brokerAddrs\":{\"5\":\"first\",\"05\":\"middle\",\"5\":\"last\"}}"));
        cases.add(routeDtoInputCase("broker-address-key-normalization-collision-repeat-alias-last-wins", "brokerData",
            "{\"brokerAddrs\":{\"05\":\"first\",\"5\":\"middle\",\"05\":\"last\"}}"));
        cases.add(routeDtoInputCase("mapping-id-key-normalization-collision-repeat-first-key-last-wins", "mapping",
            "{\"currIdMap\":{\"1\":10,\"01\":15,\"1\":20}}"));
        cases.add(routeDtoInputCase("mapping-id-key-normalization-collision-repeat-alias-last-wins", "mapping",
            "{\"currIdMap\":{\"01\":10,\"1\":15,\"01\":20}}"));
        cases.add(routeDtoInputCase("broker-address-unquoted-null-key", "brokerData",
            "{\"brokerAddrs\":{null:\"master\"}}"));
        cases.add(routeDtoInputCase("broker-address-quoted-null-key", "brokerData",
            "{\"brokerAddrs\":{\"null\":\"master\"}}"));
        cases.add(routeDtoInputCase("mapping-id-unquoted-null-key", "mapping",
            "{\"currIdMap\":{null:1}}"));
        cases.add(routeDtoInputCase("mapping-id-quoted-null-key", "mapping",
            "{\"currIdMap\":{\"null\":1}}"));
        String[][] fields = {
            {"queueData", "brokerName"}, {"queueData", "readQueueNums"},
            {"queueData", "writeQueueNums"}, {"queueData", "perm"},
            {"queueData", "topicSysFlag"},
            {"brokerData", "cluster"}, {"brokerData", "brokerName"},
            {"brokerData", "zoneName"}, {"brokerData", "enableActingMaster"},
            {"brokerData", "brokerAddrs"},
            {"mapping", "topic"}, {"mapping", "bname"}, {"mapping", "scope"},
            {"mapping", "totalQueues"}, {"mapping", "epoch"}, {"mapping", "dirty"},
            {"mapping", "currIdMap"},
            {"topicRouteData", "orderTopicConf"}, {"topicRouteData", "queueDatas"},
            {"topicRouteData", "brokerDatas"}, {"topicRouteData", "filterServerTable"},
            {"topicRouteData", "topicQueueMappingByBroker"}
        };
        String[][] shapes = {
            {"null", "null"}, {"numeric-string", "\"7\""}, {"integer", "0"},
            {"fraction", "1.5"}, {"boolean", "true"}, {"array", "[]"}, {"object", "{}"}
        };
        for (String[] field : fields) {
            for (String[] shape : shapes) {
                String rawInput = "{" + com.alibaba.fastjson2.JSON.toJSONString(field[1])
                    + ":" + shape[1] + "}";
                cases.add(routeDtoInputCase("shape-" + field[0] + "-" + field[1] + "-" + shape[0],
                    field[0], rawInput));
            }
        }
        // Fastjson accepts literal C0 controls inside string keys and values.
        // Preserve that source behavior at route DTO entrypoints as well as
        // the heartbeat DTO path; the Rust normalizer must not reject them.
        for (int codeUnit = 0; codeUnit <= 0x1f; codeUnit++) {
            String hex = String.format(java.util.Locale.ROOT, "%04x", codeUnit);
            String control = String.valueOf((char) codeUnit);
            cases.add(routeDtoInputCase("route-raw-c0-" + hex + "-order-topic-value",
                "topicRouteData", "{\"orderTopicConf\":\"left" + control + "right\"}"));
            cases.add(routeDtoInputCase("route-raw-c0-" + hex + "-field-name",
                "topicRouteData", "{\"order" + control + "TopicConf\":\"ignored\"}"));
            cases.add(routeDtoInputCase("route-raw-c0-" + hex + "-filter-key",
                "topicRouteData", "{\"filterServerTable\":{\"left" + control
                    + "right\":[\"filter\"]}}"));
            cases.add(routeDtoInputCase("route-raw-c0-" + hex + "-filter-value",
                "topicRouteData", "{\"filterServerTable\":{\"broker\":[\"left"
                    + control + "right\"]}}"));
        }
        return cases;
    }

    private static List<Object> nullStaticMappingConversion() {
        List<Object> cases = new ArrayList<>();
        for (String mapping : new String[] {
            "{\"scope\":\"__global__\",\"totalQueues\":1,\"bname\":\"broker\",\"currIdMap\":null}",
            "{\"scope\":\"__global__\",\"totalQueues\":1,\"bname\":null,\"currIdMap\":{\"0\":0}}",
            "{\"scope\":\"__global__\",\"totalQueues\":1,\"bname\":null,\"currIdMap\":{}}",
            "{\"scope\":\"__global__\",\"totalQueues\":1,\"bname\":\"broker\"}"
        }) {
            String input = "{\"queueDatas\":[],\"brokerDatas\":[],\"topicQueueMappingByBroker\":{\"broker\":" + mapping + "}}";
            TopicRouteData route = com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
            try {
                Map<MessageQueue, String> endpoints =
                    org.apache.rocketmq.remoting.rpc.ClientMetadata.topicRouteData2EndpointsForStaticTopic("topic", route);
                item.put("outcome", "OK");
                item.put("endpoints", endpoints.toString());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> overlappingStaticMappingOwners() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{\"topicQueueMappingByBroker\":{\"alpha\":{\"scope\":\"scope\",\"bname\":\"A\",\"epoch\":1,\"totalQueues\":2,\"currIdMap\":{\"0\":0}},\"beta\":{\"scope\":\"scope\",\"bname\":\"B\",\"epoch\":2,\"totalQueues\":1,\"currIdMap\":{\"0\":0}}}}",
            "{\"topicQueueMappingByBroker\":{\"alpha\":{\"scope\":\"scope\",\"bname\":\"A\",\"epoch\":2,\"totalQueues\":2,\"currIdMap\":{\"0\":0}},\"beta\":{\"scope\":\"scope\",\"bname\":\"B\",\"epoch\":1,\"totalQueues\":1,\"currIdMap\":{\"0\":0}}}}",
            "{\"topicQueueMappingByBroker\":{\"alpha\":{\"scope\":\"scope\",\"bname\":\"A\",\"epoch\":1,\"totalQueues\":2,\"currIdMap\":{\"0\":0}},\"beta\":{\"scope\":\"scope\",\"bname\":\"B\",\"epoch\":1,\"totalQueues\":1,\"currIdMap\":{\"0\":0}}}}",
            "{\"topicQueueMappingByBroker\":{\"alpha\":{\"scope\":\"scope\",\"bname\":\"A\",\"epoch\":1,\"totalQueues\":3,\"currIdMap\":{\"0\":0,\"1\":1,\"2\":2}},\"beta\":{\"scope\":\"scope\",\"bname\":\"B\",\"epoch\":2,\"totalQueues\":2,\"currIdMap\":{\"1\":1}}}}",
            "{\"topicQueueMappingByBroker\":{\"alpha\":{\"scope\":\"scope\",\"bname\":\"A\",\"epoch\":1,\"totalQueues\":2,\"currIdMap\":{\"0\":0}},\"beta\":{\"scope\":\"scope\",\"bname\":\"B\",\"epoch\":2,\"totalQueues\":1,\"currIdMap\":{\"5\":5}}}}",
            "{\"topicQueueMappingByBroker\":{\"alpha\":{\"scope\":\"__global__\",\"bname\":\"A\",\"epoch\":1,\"totalQueues\":2,\"currIdMap\":{\"0\":0}},\"beta\":{\"scope\":\"global__\",\"bname\":\"B\",\"epoch\":2,\"totalQueues\":3,\"currIdMap\":{\"2\":2}}}}",
            "{\"topicQueueMappingByBroker\":{\"alpha\":{\"scope\":\"scope\",\"bname\":null,\"epoch\":1,\"totalQueues\":2,\"currIdMap\":{}},\"beta\":{\"scope\":\"other\",\"bname\":\"\",\"epoch\":1,\"totalQueues\":1,\"currIdMap\":{\"0\":0}}}}",
            "{\"topicQueueMappingByBroker\":{\"alpha\":{\"scope\":\"scope\",\"bname\":\"A\",\"epoch\":-2,\"totalQueues\":2,\"currIdMap\":{\"0\":0}},\"beta\":{\"scope\":\"other\",\"bname\":\"B\",\"epoch\":-1,\"totalQueues\":1,\"currIdMap\":{\"0\":0}}}}",
            "{\"topicQueueMappingByBroker\":{\"alpha\":{\"scope\":\"scope\",\"bname\":\"A\",\"epoch\":1,\"totalQueues\":-2,\"currIdMap\":{\"0\":0}},\"beta\":{\"scope\":\"other\",\"bname\":\"B\",\"epoch\":1,\"totalQueues\":0,\"currIdMap\":{\"0\":0}}}}",
            "{\"topicQueueMappingByBroker\":{\"alpha\":{\"scope\":null,\"bname\":\"A\",\"epoch\":1,\"totalQueues\":2,\"currIdMap\":{\"0\":0}},\"beta\":{\"scope\":\"scope\",\"bname\":\"B\",\"epoch\":1,\"totalQueues\":2,\"currIdMap\":{\"0\":0}}}}"
        }) {
            TopicRouteData route = com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class);
            Map<MessageQueue, String> endpoints =
                org.apache.rocketmq.remoting.rpc.ClientMetadata.topicRouteData2EndpointsForStaticTopic("topic", route);
            List<Map<String, Object>> rows = new ArrayList<>();
            for (Map.Entry<MessageQueue, String> entry : endpoints.entrySet()) {
                Map<String, Object> row = new TreeMap<>();
                row.put("queueId", entry.getKey().getQueueId());
                row.put("brokerName", entry.getKey().getBrokerName());
                row.put("owner", entry.getValue());
                rows.add(row);
            }
            rows.sort(Comparator.comparingInt((Map<String, Object> row) -> (Integer) row.get("queueId"))
                .thenComparing(row -> (String) row.get("brokerName")));
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
            item.put("endpoints", rows);
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> runtimeNullableCollections() throws Exception {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{\"queueDatas\":null,\"brokerDatas\":[],\"filterServerTable\":{}}",
            "{\"queueDatas\":null,\"brokerDatas\":[],\"filterServerTable\":{},\"orderTopicConf\":\"broker:1\"}",
            "{\"queueDatas\":null,\"brokerDatas\":[],\"filterServerTable\":{},\"topicQueueMappingByBroker\":{\"broker\":{\"scope\":\"__global__\",\"totalQueues\":1,\"bname\":\"broker\"}}}",
            "{\"queueDatas\":[],\"brokerDatas\":null,\"filterServerTable\":{}}",
            "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":null}",
            "{\"queueDatas\":null,\"brokerDatas\":[],\"filterServerTable\":{},\"orderTopicConf\":\"broker:1\"}",
            "{\"queueDatas\":[],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":null,\"brokerAddrs\":{}}]}",
            "{\"queueDatas\":[],\"brokerDatas\":[{\"cluster\":null,\"brokerName\":\"broker\",\"brokerAddrs\":{}}]}",
            "{\"queueDatas\":[{\"brokerName\":null,\"readQueueNums\":1,\"perm\":4}],\"brokerDatas\":[]}",
            "{\"queueDatas\":[{\"brokerName\":null},{\"brokerName\":\"broker\"}],\"brokerDatas\":[]}",
            "{\"queueDatas\":null,\"brokerDatas\":[],\"filterServerTable\":{},\"topicQueueMappingByBroker\":{\"broker\":{\"scope\":\"__global__\",\"totalQueues\":1,\"bname\":\"broker\"}}}"
        }) {
            org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
            config.setClientIP("route-null-collections-" + cases.size());
            MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
            org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
            RouteApi api = new RouteApi(config);
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
            try {
                java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
                field.setAccessible(true); field.set(runtime, api);
                api.route = com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class);
                boolean registeredConsumer = cases.size() == 5 || cases.size() == 10;
                item.put("registeredConsumer", registeredConsumer);
                if (registeredConsumer) {
                    org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumer =
                        (org.apache.rocketmq.client.impl.consumer.MQConsumerInner) java.lang.reflect.Proxy.newProxyInstance(
                            org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class.getClassLoader(),
                            new Class<?>[] {org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class},
                            (proxy, method, values) -> method.getReturnType() == boolean.class ? false : null);
                    runtime.getConsumerTable().put("consumer", consumer);
                }
                try {
                    item.put("changed", runtime.updateTopicRouteInfoFromNameServer("topic"));
                    item.put("outcome", "OK");
                } catch (RuntimeException error) {
                    item.put("outcome", error.getClass().getSimpleName());
                }
                TopicRouteData cached = runtime.getAnExistTopicRouteData("topic");
                item.put("cached", cached != null);
                if (cached != null) {
                    item.put("cachedQueueNull", cached.getQueueDatas() == null);
                    item.put("cachedBrokerNull", cached.getBrokerDatas() == null);
                    item.put("cachedFilterNull", cached.getFilterServerTable() == null);
                }
            } finally {
                java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                    org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
                cleanup.setAccessible(true);
                cleanup.invoke(runtime, original, new IllegalStateException("route fixture teardown"));
                api.shutdown();
            }
            cases.add(item);
        }
        return cases;
    }

    private static Object routeCollectionValue(String field, String state) {
        if ("null".equals(state)) return null;
        if ("nullNested".equals(state)) {
            if ("queueDatas".equals(field) || "brokerDatas".equals(field)) {
                List<Object> rows = new ArrayList<>();
                rows.add(null);
                return rows;
            }
            Map<String, Object> values = new TreeMap<>();
            values.put("broker", null);
            return values;
        }
        if ("empty".equals(state)) {
            return "queueDatas".equals(field) || "brokerDatas".equals(field)
                ? new ArrayList<>() : new TreeMap<>();
        }
        if ("populated".equals(state)) {
            if ("queueDatas".equals(field)) {
                Map<String, Object> queue = new TreeMap<>();
                queue.put("brokerName", "broker");
                queue.put("readQueueNums", 1);
                queue.put("writeQueueNums", 1);
                queue.put("perm", 6);
                queue.put("topicSysFlag", 0);
                return List.of(queue);
            }
            if ("brokerDatas".equals(field)) {
                Map<String, Object> broker = new TreeMap<>();
                broker.put("cluster", "cluster");
                broker.put("brokerName", "broker");
                broker.put("brokerAddrs", Map.of(0L, "127.0.0.1:10911"));
                return List.of(broker);
            }
            if ("filterServerTable".equals(field)) {
                return Map.of("broker", List.of("filter:10911"));
            }
            Map<String, Object> mapping = new TreeMap<>();
            mapping.put("topic", "topic");
            mapping.put("bname", "broker");
            mapping.put("scope", "__global__");
            mapping.put("totalQueues", 1);
            mapping.put("currIdMap", Map.of("0", 0));
            return Map.of("broker", mapping);
        }
        return null;
    }

    private static List<Object> runtimeRouteNestedCollectionMatrix() throws Exception {
        List<Object> cases = new ArrayList<>(625);
        String[] fields = {"queueDatas", "brokerDatas", "filterServerTable", "topicQueueMappingByBroker"};
        String[] states = {"absent", "null", "empty", "populated", "nullNested"};
        for (int indexValue = 0; indexValue < 625; indexValue++) {
            Map<String, Object> input = new TreeMap<>();
            Map<String, Object> selectedStates = new TreeMap<>();
            int remaining = indexValue;
            for (int index = 0; index < fields.length; index++) {
                String state = states[remaining % states.length];
                remaining /= states.length;
                selectedStates.put(fields[index], state);
                if (!"absent".equals(state)) {
                    input.put(fields[index], routeCollectionValue(fields[index], state));
                }
            }
            String json = com.alibaba.fastjson2.JSON.toJSONString(input,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString);
            org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
            config.setClientIP("route-nested-matrix-" + cases.size());
            MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
            org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
            RouteApi api = new RouteApi(config);
            Map<String, Object> item = new TreeMap<>();
            try {
                java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
                field.setAccessible(true);
                field.set(runtime, api);
                item.put("input", com.alibaba.fastjson2.JSON.parseObject(json));
                item.put("states", selectedStates);
                api.route = com.alibaba.fastjson2.JSON.parseObject(json, TopicRouteData.class);
                try {
                    item.put("changed", runtime.updateTopicRouteInfoFromNameServer("topic"));
                    item.put("outcome", "OK");
                } catch (RuntimeException error) {
                    item.put("changed", null);
                    item.put("outcome", error.getClass().getSimpleName());
                }
                TopicRouteData cached = runtime.getAnExistTopicRouteData("topic");
                item.put("cached", cached != null);
                if (cached != null) {
                    item.put("cachedQueueCount", cached.getQueueDatas() == null ? -1 : cached.getQueueDatas().size());
                    item.put("cachedBrokerCount", cached.getBrokerDatas() == null ? -1 : cached.getBrokerDatas().size());
                    item.put("cachedFilterCount", cached.getFilterServerTable() == null ? -1 : cached.getFilterServerTable().size());
                    item.put("cachedMappingCount", cached.getTopicQueueMappingByBroker() == null ? -1 : cached.getTopicQueueMappingByBroker().size());
                }
            } finally {
                java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                    org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
                cleanup.setAccessible(true);
                cleanup.invoke(runtime, original, new IllegalStateException("route nested collection fixture teardown"));
                api.shutdown();
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> runtimeRouteNestedCollectionConsumers() throws Exception {
        List<Object> cases = new ArrayList<>();
        String[] ids = {"populated-control", "null-queue-row-static", "null-broker-row",
            "null-filter-list", "null-static-mapping", "null-queue-list-static",
            "null-broker-address-map"};
        String[] fields = {null, "queueDatas", "brokerDatas", "filterServerTable",
            "topicQueueMappingByBroker", null, null};
        Map<String, Object> base = new TreeMap<>();
        for (String field : new String[] {"queueDatas", "brokerDatas", "filterServerTable", "topicQueueMappingByBroker"}) {
            base.put(field, routeCollectionValue(field, "populated"));
        }
        for (int index = 0; index < ids.length; index++) {
            Map<String, Object> input = new TreeMap<>(base);
            if ("null-queue-list-static".equals(ids[index])) {
                input.put("queueDatas", null);
            } else if ("null-broker-address-map".equals(ids[index])) {
                @SuppressWarnings("unchecked") List<Map<String, Object>> brokers =
                    (List<Map<String, Object>>) input.get("brokerDatas");
                brokers.get(0).put("brokerAddrs", null);
            } else if (fields[index] != null) {
                input.put(fields[index], routeCollectionValue(fields[index], "nullNested"));
            }
            String json = com.alibaba.fastjson2.JSON.toJSONString(input,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString);
            org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
            config.setClientIP("route-nested-consumer-" + ids[index]);
            MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
            org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
            RouteApi api = new RouteApi(config);
            AtomicInteger needChecks = new AtomicInteger();
            AtomicInteger updates = new AtomicInteger();
            Map<String, Object> item = new TreeMap<>();
            try {
                java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
                field.setAccessible(true);
                field.set(runtime, api);
                org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumer =
                    (org.apache.rocketmq.client.impl.consumer.MQConsumerInner) java.lang.reflect.Proxy.newProxyInstance(
                        org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class.getClassLoader(),
                        new Class<?>[] {org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class},
                        (proxy, method, values) -> {
                            if ("isSubscribeTopicNeedUpdate".equals(method.getName())) needChecks.incrementAndGet();
                            if ("updateTopicSubscribeInfo".equals(method.getName())) updates.incrementAndGet();
                            return method.getReturnType() == boolean.class ? false : null;
                        });
                runtime.getConsumerTable().put("consumer", consumer);
                item.put("id", ids[index]);
                item.put("input", com.alibaba.fastjson2.JSON.parseObject(json));
                api.route = com.alibaba.fastjson2.JSON.parseObject(json, TopicRouteData.class);
                try {
                    item.put("changed", runtime.updateTopicRouteInfoFromNameServer("topic"));
                    item.put("outcome", "OK");
                } catch (RuntimeException error) {
                    item.put("changed", null);
                    item.put("outcome", error.getClass().getSimpleName());
                }
                TopicRouteData cached = runtime.getAnExistTopicRouteData("topic");
                item.put("cached", cached != null);
                item.put("consumerNeedChecks", needChecks.get());
                item.put("consumerUpdates", updates.get());
                if (cached != null) {
                    item.put("cachedQueueCount", cached.getQueueDatas() == null ? -1 : cached.getQueueDatas().size());
                    item.put("cachedBrokerCount", cached.getBrokerDatas() == null ? -1 : cached.getBrokerDatas().size());
                    item.put("cachedFilterCount", cached.getFilterServerTable() == null ? -1 : cached.getFilterServerTable().size());
                    item.put("cachedMappingCount", cached.getTopicQueueMappingByBroker() == null ? -1 : cached.getTopicQueueMappingByBroker().size());
                }
            } finally {
                java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                    org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
                cleanup.setAccessible(true);
                cleanup.invoke(runtime, original, new IllegalStateException("route nested consumer fixture teardown"));
                api.shutdown();
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> runtimeRouteCollectionMatrix() throws Exception {
        List<Object> cases = new ArrayList<>(256);
        String[] fields = {"queueDatas", "brokerDatas", "filterServerTable", "topicQueueMappingByBroker"};
        String[] states = {"absent", "null", "empty", "populated"};
        for (String queueState : states) {
            for (String brokerState : states) {
                for (String filterState : states) {
                    for (String mappingState : states) {
                        String[] selected = {queueState, brokerState, filterState, mappingState};
                        Map<String, Object> input = new TreeMap<>();
                        Map<String, Object> state = new TreeMap<>();
                        for (int index = 0; index < fields.length; index++) {
                            state.put(fields[index], selected[index]);
                            if (!"absent".equals(selected[index])) {
                                input.put(fields[index], routeCollectionValue(fields[index], selected[index]));
                            }
                        }
                        String json = com.alibaba.fastjson2.JSON.toJSONString(input,
                            com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString);
                        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
                        config.setClientIP("route-collection-matrix-" + cases.size());
                        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
                        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
                        RouteApi api = new RouteApi(config);
                        Map<String, Object> item = new TreeMap<>();
                        try {
                            java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
                            field.setAccessible(true);
                            field.set(runtime, api);
                            item.put("input", com.alibaba.fastjson2.JSON.parseObject(json));
                            item.put("states", state);
                            api.route = com.alibaba.fastjson2.JSON.parseObject(json, TopicRouteData.class);
                            try {
                                item.put("changed", runtime.updateTopicRouteInfoFromNameServer("topic"));
                                item.put("outcome", "OK");
                            } catch (RuntimeException error) {
                                item.put("changed", null);
                                item.put("outcome", error.getClass().getSimpleName());
                            }
                            TopicRouteData cached = runtime.getAnExistTopicRouteData("topic");
                            item.put("cached", cached != null);
                            if (cached != null) {
                                item.put("cachedQueueCount", cached.getQueueDatas() == null ? -1 : cached.getQueueDatas().size());
                                item.put("cachedBrokerCount", cached.getBrokerDatas() == null ? -1 : cached.getBrokerDatas().size());
                                item.put("cachedFilterCount", cached.getFilterServerTable() == null ? -1 : cached.getFilterServerTable().size());
                                item.put("cachedMappingCount", cached.getTopicQueueMappingByBroker() == null ? -1 : cached.getTopicQueueMappingByBroker().size());
                            }
                        } finally {
                            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
                            cleanup.setAccessible(true);
                            try { cleanup.invoke(runtime, original, new IllegalStateException("route collection matrix teardown")); }
                            finally { api.shutdown(); }
                        }
                        cases.add(item);
                    }
                }
            }
        }
        return cases;
    }

    private static Map<String, Object> runtimeNullBrokerMap() throws Exception {
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("route-null-map");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        RouteApi api = new RouteApi(config);
        Map<String, Object> item = new TreeMap<>();
        AtomicInteger consumerNeedChecks = new AtomicInteger();
        AtomicInteger consumerUpdates = new AtomicInteger();
        AtomicInteger producerUpdates = new AtomicInteger();
        try {
            java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            field.setAccessible(true); field.set(runtime, api);
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumer =
                (org.apache.rocketmq.client.impl.consumer.MQConsumerInner) java.lang.reflect.Proxy.newProxyInstance(
                    org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class},
                    (proxy, method, values) -> {
                        if ("isSubscribeTopicNeedUpdate".equals(method.getName())) consumerNeedChecks.incrementAndGet();
                        if ("updateTopicSubscribeInfo".equals(method.getName())) consumerUpdates.incrementAndGet();
                        return method.getReturnType() == boolean.class ? false : null;
                    });
            runtime.getConsumerTable().put("consumer", consumer);
            org.apache.rocketmq.client.impl.producer.MQProducerInner producer =
                (org.apache.rocketmq.client.impl.producer.MQProducerInner) java.lang.reflect.Proxy.newProxyInstance(
                    org.apache.rocketmq.client.impl.producer.MQProducerInner.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.client.impl.producer.MQProducerInner.class},
                    (proxy, method, values) -> {
                        if ("updateTopicPublishInfo".equals(method.getName())) producerUpdates.incrementAndGet();
                        if ("getPublishTopicList".equals(method.getName())) return java.util.Collections.emptySet();
                        return method.getReturnType() == boolean.class ? false : null;
                    });
            runtime.getProducerTable().put("producer", producer);
            String initial = "{\"queueDatas\":[],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"old:10911\"}}],\"filterServerTable\":{}}";
            String invalid = "{\"queueDatas\":[],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":null}],\"filterServerTable\":{}}";
            String recovery = "{\"queueDatas\":[],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"recovery-broker\",\"brokerAddrs\":{\"0\":\"recovered:10911\"}}],\"filterServerTable\":{}}";
            item.put("initialInput", com.alibaba.fastjson2.JSON.parseObject(initial));
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(invalid));
            item.put("recoveryInput", com.alibaba.fastjson2.JSON.parseObject(recovery));
            api.route = com.alibaba.fastjson2.JSON.parseObject(initial, TopicRouteData.class);
            item.put("initialChanged", runtime.updateTopicRouteInfoFromNameServer("topic"));
            item.put("consumerNeedChecksAfterInitial", consumerNeedChecks.get());
            item.put("consumerUpdatesAfterInitial", consumerUpdates.get());
            item.put("producerUpdatesAfterInitial", producerUpdates.get());
            TopicRouteData before = runtime.getAnExistTopicRouteData("topic");
            api.route = com.alibaba.fastjson2.JSON.parseObject(invalid, TopicRouteData.class);
            try {
                item.put("changed", runtime.updateTopicRouteInfoFromNameServer("topic"));
                item.put("outcome", "OK");
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
            }
            item.put("cachedRouteUnchanged", before.equals(runtime.getAnExistTopicRouteData("topic")));
            item.put("priorBrokerPublishAddressAfterFailure", runtime.findBrokerAddressInPublish("broker"));
            item.put("consumerNeedChecksAfterFailure", consumerNeedChecks.get());
            item.put("consumerUpdatesAfterFailure", consumerUpdates.get());
            item.put("producerUpdatesAfterFailure", producerUpdates.get());
            api.route = com.alibaba.fastjson2.JSON.parseObject(recovery, TopicRouteData.class);
            item.put("recoveryChanged", runtime.updateTopicRouteInfoFromNameServer("topic"));
            item.put("recoveryOutcome", "OK");
            item.put("recoveredRouteChanged", !before.equals(runtime.getAnExistTopicRouteData("topic")));
            item.put("consumerNeedChecksAfterRecovery", consumerNeedChecks.get());
            item.put("consumerUpdatesAfterRecovery", consumerUpdates.get());
            item.put("producerUpdatesAfterRecovery", producerUpdates.get());
            item.put("recoveredPublishAddress", runtime.findBrokerAddressInPublish("recovery-broker"));
            item.put("priorBrokerPublishAddressAfterRecovery", runtime.findBrokerAddressInPublish("broker"));
        } finally {
            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, original, new IllegalStateException("route fixture teardown"));
            api.shutdown();
        }
        return item;
    }

    private static Map<String, Object> runtimeEmptyEndpointTransition(
        Map<String, Object> initial, Map<String, Object> ordinary) throws Exception {
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("route-endpoint-transition");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        RouteApi api = new RouteApi(config);
        Map<String, Object> item = new TreeMap<>();
        try {
            java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            field.setAccessible(true); field.set(runtime, api);
            item.put("initialInput", initial); item.put("input", ordinary);
            api.route = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(initial), TopicRouteData.class);
            item.put("initialChanged", runtime.updateTopicRouteInfoFromNameServer("topic"));
            List<MessageQueue> queues = new ArrayList<>(org.apache.rocketmq.remoting.rpc.ClientMetadata
                .topicRouteData2EndpointsForStaticTopic("topic", api.route).keySet());
            queues.sort(Comparator.naturalOrder());
            if (queues.isEmpty()) throw new IllegalStateException("transition requires static endpoints");
            MessageQueue unmappedQueue = new MessageQueue("topic", "fallback-broker", 99);
            item.put("unmappedQueue", unmappedQueue);
            MessageQueue nullTopicQueue = new MessageQueue(null, "fallback-broker", 99);
            item.put("nullTopicQueue", nullTopicQueue);
            List<Object> before = new ArrayList<>();
            for (MessageQueue queue : queues) before.add(new TreeMap<>(Map.of(
                "queue", queue, "broker", runtime.getBrokerNameFromMessageQueue(queue))));
            item.put("before", before);
            item.put("unmappedBrokerBefore", runtime.getBrokerNameFromMessageQueue(unmappedQueue));
            item.put("nullTopicBefore", brokerNameOutcome(runtime, nullTopicQueue));
            api.route = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(ordinary), TopicRouteData.class);
            item.put("changed", runtime.updateTopicRouteInfoFromNameServer("topic"));
            List<Object> after = new ArrayList<>();
            for (MessageQueue queue : queues) after.add(new TreeMap<>(Map.of(
                "queue", queue, "broker", runtime.getBrokerNameFromMessageQueue(queue))));
            item.put("after", after);
            item.put("unmappedBrokerAfter", runtime.getBrokerNameFromMessageQueue(unmappedQueue));
            item.put("nullTopicAfter", brokerNameOutcome(runtime, nullTopicQueue));
            item.put("retained", before.equals(after));
            item.put("rawRouteHasStaticMappings", runtime.getAnExistTopicRouteData("topic").getTopicQueueMappingByBroker() != null
                && !runtime.getAnExistTopicRouteData("topic").getTopicQueueMappingByBroker().isEmpty());
        } finally {
            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, original, new IllegalStateException("endpoint fixture teardown"));
            api.shutdown();
        }
        return item;
    }

    private static Map<String, Object> runtimeConsumerNeedUpdate() throws Exception {
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("route-consumer-need-update");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        RouteApi api = new RouteApi(config);
        Map<String, Object> item = new TreeMap<>();
        AtomicInteger consumerNeedChecks = new AtomicInteger();
        AtomicInteger consumerUpdates = new AtomicInteger();
        AtomicInteger producerUpdates = new AtomicInteger();
        java.util.concurrent.atomic.AtomicBoolean needed = new java.util.concurrent.atomic.AtomicBoolean(false);
        String input = "{\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":1,\"writeQueueNums\":1,\"perm\":6,\"topicSysFlag\":0}],"
            + "\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"127.0.0.1:10911\"}}],"
            + "\"filterServerTable\":{}}";
        item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
        try {
            java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            field.setAccessible(true);
            field.set(runtime, api);
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumer =
                (org.apache.rocketmq.client.impl.consumer.MQConsumerInner) java.lang.reflect.Proxy.newProxyInstance(
                    org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class},
                    (proxy, method, values) -> {
                        if ("isSubscribeTopicNeedUpdate".equals(method.getName())) {
                            consumerNeedChecks.incrementAndGet();
                            return needed.get();
                        }
                        if ("updateTopicSubscribeInfo".equals(method.getName())) consumerUpdates.incrementAndGet();
                        return method.getReturnType() == boolean.class ? false : null;
                    });
            org.apache.rocketmq.client.impl.producer.MQProducerInner producer =
                (org.apache.rocketmq.client.impl.producer.MQProducerInner) java.lang.reflect.Proxy.newProxyInstance(
                    org.apache.rocketmq.client.impl.producer.MQProducerInner.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.client.impl.producer.MQProducerInner.class},
                    (proxy, method, values) -> {
                        if ("updateTopicPublishInfo".equals(method.getName())) producerUpdates.incrementAndGet();
                        if ("getPublishTopicList".equals(method.getName())) return java.util.Collections.emptySet();
                        return method.getReturnType() == boolean.class ? false : null;
                    });
            runtime.getConsumerTable().put("consumer", consumer);
            runtime.getProducerTable().put("producer", producer);
            api.route = com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class);
            item.put("initialChanged", runtime.updateTopicRouteInfoFromNameServer("topic"));
            item.put("consumerNeedChecksAfterInitial", consumerNeedChecks.get());
            item.put("consumerUpdatesAfterInitial", consumerUpdates.get());
            item.put("producerUpdatesAfterInitial", producerUpdates.get());
            TopicRouteData before = runtime.getAnExistTopicRouteData("topic");
            needed.set(true);
            api.route = com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class);
            item.put("secondChanged", runtime.updateTopicRouteInfoFromNameServer("topic"));
            item.put("secondOutcome", "OK");
            item.put("cachedRouteUnchanged", before.equals(runtime.getAnExistTopicRouteData("topic")));
            item.put("consumerNeedChecksAfterSecond", consumerNeedChecks.get());
            item.put("consumerUpdatesAfterSecond", consumerUpdates.get());
            item.put("producerUpdatesAfterSecond", producerUpdates.get());
        } finally {
            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, original, new IllegalStateException("consumer need-update fixture teardown"));
            api.shutdown();
        }
        return item;
    }

    private static Map<String, Object> brokerNameOutcome(MQClientInstance runtime, MessageQueue queue) {
        Map<String, Object> item = new TreeMap<>();
        try {
            item.put("broker", runtime.getBrokerNameFromMessageQueue(queue));
            item.put("outcome", "OK");
        } catch (RuntimeException error) {
            item.put("broker", null);
            item.put("outcome", error.getClass().getSimpleName());
        }
        return item;
    }

    private static Map<String, Object> runtimeNullBrokerQueue() throws Exception {
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("null-broker-name-queue");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        try {
            return brokerNameOutcome(runtime, null);
        } finally {
            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod(
                "cleanupAfterConstructionFailure", org.apache.rocketmq.client.impl.MQClientAPIImpl.class,
                Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, runtime.getMQClientAPIImpl(),
                new IllegalStateException("null broker-name queue fixture teardown"));
        }
    }

    private static Map<String, Object> runtimeNullTopicBrokerSelection() throws Exception {
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("null-broker-topic");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        Map<String, Object> result = new TreeMap<>();
        try {
            try {
                result.put("address", runtime.findBrokerAddrByTopic(null));
                result.put("outcome", "OK");
            } catch (RuntimeException error) {
                result.put("address", null);
                result.put("outcome", error.getClass().getSimpleName());
            }
        } finally {
            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod(
                "cleanupAfterConstructionFailure", org.apache.rocketmq.client.impl.MQClientAPIImpl.class,
                Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, runtime.getMQClientAPIImpl(),
                new IllegalStateException("null broker topic fixture teardown"));
        }
        return result;
    }

    private static Map<String, Object> brokerAddressLookupOutcome(
        java.util.function.Supplier<?> lookup) {
        Map<String, Object> result = new TreeMap<>();
        try {
            result.put("isNull", lookup.get() == null);
            result.put("outcome", "OK");
        } catch (RuntimeException error) {
            result.put("isNull", false);
            result.put("outcome", error.getClass().getSimpleName());
        }
        return result;
    }

    private static Map<String, Object> runtimeNullBrokerAddressLookups() throws Exception {
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("null-broker-address-lookups");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        Map<String, Object> result = new TreeMap<>();
        try {
            result.put("admin", brokerAddressLookupOutcome(() -> runtime.findBrokerAddressInAdmin(null)));
            result.put("publish", brokerAddressLookupOutcome(() -> runtime.findBrokerAddressInPublish(null)));
            result.put("subscribe", brokerAddressLookupOutcome(
                () -> runtime.findBrokerAddressInSubscribe(null, 0L, true)));
        } finally {
            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod(
                "cleanupAfterConstructionFailure", org.apache.rocketmq.client.impl.MQClientAPIImpl.class,
                Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, runtime.getMQClientAPIImpl(),
                new IllegalStateException("null broker address lookup fixture teardown"));
        }
        return result;
    }

    private static Map<String, Object> brokerAddressRow(Long id, String address) {
        Map<String, Object> row = new TreeMap<>();
        row.put("id", id);
        row.put("address", address);
        return row;
    }

    private static Map<String, Object> brokerAddressQuery(String id, long brokerId, boolean onlyThisBroker) {
        Map<String, Object> query = new TreeMap<>();
        query.put("id", id);
        query.put("brokerId", brokerId);
        query.put("onlyThisBroker", onlyThisBroker);
        return query;
    }

    private static Map<String, Object> brokerAddressScenario(
        String id,
        boolean tableEntry,
        List<Map<String, Object>> addresses,
        Map<String, Integer> versions,
        List<Map<String, Object>> queries
    ) {
        Map<String, Object> input = new TreeMap<>();
        input.put("tableEntry", tableEntry);
        input.put("addresses", addresses);
        input.put("versions", new TreeMap<>(versions));
        input.put("queries", queries);
        Map<String, Object> scenario = new TreeMap<>();
        scenario.put("id", id);
        scenario.put("input", input);
        return scenario;
    }

    private static Map<String, Object> findBrokerResultOutcome(Callable<org.apache.rocketmq.client.impl.FindBrokerResult> lookup) {
        Map<String, Object> outcome = new TreeMap<>();
        try {
            org.apache.rocketmq.client.impl.FindBrokerResult result = lookup.call();
            outcome.put("outcome", "OK");
            outcome.put("isNull", result == null);
            if (result != null) {
                outcome.put("address", result.getBrokerAddr());
                outcome.put("slave", result.isSlave());
                outcome.put("version", result.getBrokerVersion());
            }
        } catch (Exception error) {
            outcome.put("outcome", error.getClass().getSimpleName());
        }
        return outcome;
    }

    private static List<Object> runtimeBrokerAddressResolution() throws Exception {
        List<Map<String, Object>> scenarios = new ArrayList<>();
        scenarios.add(brokerAddressScenario("master-exact-and-arbitrary-fallback", true,
            List.of(brokerAddressRow(0L, "master:10911")), Map.of("master:10911", 551), List.of(
                brokerAddressQuery("exact-master", 0L, true),
                brokerAddressQuery("only-this-broker-miss", 7L, true),
                brokerAddressQuery("arbitrary-master-fallback", 7L, false))));
        scenarios.add(brokerAddressScenario("slave-exact-and-adjacent", true,
            List.of(brokerAddressRow(0L, "master:10911"), brokerAddressRow(2L, "slave:10911")),
            Map.of("master:10911", 551, "slave:10911", 552), List.of(
                brokerAddressQuery("exact-slave", 2L, true),
                brokerAddressQuery("adjacent-slave", 1L, true))));
        scenarios.add(brokerAddressScenario("arbitrary-slave-fallback", true,
            List.of(brokerAddressRow(2L, "slave:10911")), Map.of("slave:10911", 552), List.of(
                brokerAddressQuery("arbitrary-slave-fallback", 8L, false))));
        scenarios.add(brokerAddressScenario("adjacent-id-wraps-at-long-max", true,
            List.of(brokerAddressRow(Long.MIN_VALUE, "wrapped:10911")), Map.of("wrapped:10911", 553), List.of(
                brokerAddressQuery("adjacent-long-overflow", Long.MAX_VALUE, true))));
        scenarios.add(brokerAddressScenario("empty-inner-address-map", true,
            List.of(), Map.of(), List.of(brokerAddressQuery("empty-map", 0L, false))));
        scenarios.add(brokerAddressScenario("missing-broker-table-entry", false,
            List.of(), Map.of(), List.of(brokerAddressQuery("absent-map", 0L, false))));
        scenarios.add(brokerAddressScenario("null-master-address", true,
            List.of(brokerAddressRow(0L, null)), Map.of(), List.of(
                brokerAddressQuery("null-exact-master", 0L, true),
                brokerAddressQuery("null-arbitrary-fallback", 1L, false))));
        scenarios.add(brokerAddressScenario("null-id-live-address", true,
            List.of(brokerAddressRow(null, "null-id:10911")), Map.of("null-id:10911", 554), List.of(
                brokerAddressQuery("null-id-fallback", 7L, false))));
        scenarios.add(brokerAddressScenario("broker-version-address-missing", true,
            List.of(brokerAddressRow(0L, "master:10911")), Map.of("other:10911", 555), List.of(
                brokerAddressQuery("broker-version-default", 0L, true))));

        List<Object> cases = new ArrayList<>();
        java.lang.reflect.Field addressField = MQClientInstance.class.getDeclaredField("brokerAddrTable");
        addressField.setAccessible(true);
        java.lang.reflect.Field versionField = MQClientInstance.class.getDeclaredField("brokerVersionTable");
        versionField.setAccessible(true);
        for (Map<String, Object> scenario : scenarios) {
            org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
            config.setClientIP("broker-lookup-" + scenario.get("id"));
            MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
            Map<String, Object> input = (Map<String, Object>) scenario.get("input");
            Map<String, Object> item = new TreeMap<>();
            item.put("id", scenario.get("id"));
            item.put("input", input);
            try {
                java.util.concurrent.ConcurrentMap<String, HashMap<Long, String>> addressTable =
                    (java.util.concurrent.ConcurrentMap<String, HashMap<Long, String>>) addressField.get(runtime);
                HashMap<Long, String> brokerAddresses = new HashMap<>();
                for (Map<String, Object> row : (List<Map<String, Object>>) input.get("addresses")) {
                    Number key = (Number) row.get("id");
                    brokerAddresses.put(key == null ? null : key.longValue(), (String) row.get("address"));
                }
                if (Boolean.TRUE.equals(input.get("tableEntry"))) {
                    addressTable.put("broker", brokerAddresses);
                }
                java.util.concurrent.ConcurrentMap<String, java.util.concurrent.ConcurrentHashMap<String, Integer>>
                    versionTable = (java.util.concurrent.ConcurrentMap<String,
                        java.util.concurrent.ConcurrentHashMap<String, Integer>>) versionField.get(runtime);
                Map<String, Integer> versions = (Map<String, Integer>) input.get("versions");
                if (!versions.isEmpty()) {
                    versionTable.put("broker", new java.util.concurrent.ConcurrentHashMap<>(versions));
                }

                item.put("publishAddress", runtime.findBrokerAddressInPublish("broker"));
                item.put("admin", findBrokerResultOutcome(() -> runtime.findBrokerAddressInAdmin("broker")));
                List<Object> queryResults = new ArrayList<>();
                for (Map<String, Object> query : (List<Map<String, Object>>) input.get("queries")) {
                    Map<String, Object> result = new TreeMap<>();
                    result.put("id", query.get("id"));
                    result.put("brokerId", query.get("brokerId"));
                    result.put("onlyThisBroker", query.get("onlyThisBroker"));
                    long brokerId = ((Number) query.get("brokerId")).longValue();
                    boolean onlyThisBroker = (Boolean) query.get("onlyThisBroker");
                    result.put("result", findBrokerResultOutcome(
                        () -> runtime.findBrokerAddressInSubscribe("broker", brokerId, onlyThisBroker)));
                    queryResults.add(result);
                }
                item.put("subscribe", queryResults);
            } finally {
                java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod(
                    "cleanupAfterConstructionFailure", org.apache.rocketmq.client.impl.MQClientAPIImpl.class,
                    Throwable.class);
                cleanup.setAccessible(true);
                cleanup.invoke(runtime, runtime.getMQClientAPIImpl(),
                    new IllegalStateException("broker address lookup fixture teardown"));
            }
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> routeLookupOutcome(java.util.concurrent.Callable<?> lookup) {
        Map<String, Object> result = new TreeMap<>();
        try {
            Object value = lookup.call();
            result.put("outcome", "OK");
            if (value instanceof Boolean) result.put("changed", value);
            else result.put("isNull", value == null);
        } catch (Exception error) {
            result.put("outcome", error.getClass().getSimpleName());
        }
        return result;
    }

    private static Map<String, Object> runtimeNullTopicRouteLookups() throws Exception {
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("null-topic-route-lookups");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        RouteApi api = new RouteApi(config);
        Map<String, Object> result = new TreeMap<>();
        try {
            java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            field.setAccessible(true);
            field.set(runtime, api);
            result.put("existing", routeLookupOutcome(() -> runtime.getAnExistTopicRouteData(null)));
            result.put("query", routeLookupOutcome(() -> runtime.queryTopicRouteData(null)));
            api.route = null;
            result.put("updateMissing", routeLookupOutcome(
                () -> runtime.updateTopicRouteInfoFromNameServer((String) null)));
            result.put("requestsAfterMissing", api.requests);
            api.route = new TopicRouteData();
            result.put("updatePresent", routeLookupOutcome(
                () -> runtime.updateTopicRouteInfoFromNameServer((String) null)));
            result.put("requestsAfterPresent", api.requests);
            result.put("requestedNullTopic", api.requestedTopics.stream().allMatch(value -> value == null));
        } finally {
            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod(
                "cleanupAfterConstructionFailure", org.apache.rocketmq.client.impl.MQClientAPIImpl.class,
                Throwable.class);
            cleanup.setAccessible(true);
            try { cleanup.invoke(runtime, original, new IllegalStateException("null topic route fixture teardown")); }
            finally { api.shutdown(); }
        }
        return result;
    }

    private static List<Object> runtimeRawOfflineCleanup(Map<String, Object> initial) throws Exception {
        List<Object> cases = new ArrayList<>();
        for (boolean priorBroker : new boolean[] {false, true}) {
            org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
            config.setClientIP("raw-offline-cleanup-" + priorBroker);
            MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
            org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
            RouteApi api = new RouteApi(config);
            Map<String, Object> item = new TreeMap<>();
            try {
                java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
                field.setAccessible(true); field.set(runtime, api);
                api.route = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(initial), TopicRouteData.class);
                item.put("priorBroker", priorBroker);
                item.put("initialInput", initial);
                if (priorBroker) runtime.updateTopicRouteInfoFromNameServer("topic");
                item.put("publishAddressBefore", runtime.findBrokerAddressInPublish("broker"));
                TopicRouteData raw = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(initial), TopicRouteData.class);
                raw.getBrokerDatas().get(0).setBrokerAddrs(null);
                runtime.getTopicRouteTable().clear();
                runtime.getTopicRouteTable().put("raw", raw);
                item.put("rawInput", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(raw,
                    com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue, com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
                java.lang.reflect.Method clean = MQClientInstance.class.getDeclaredMethod("cleanOfflineBroker");
                clean.setAccessible(true);
                try {
                    clean.invoke(runtime);
                    item.put("outcome", "OK");
                } catch (java.lang.reflect.InvocationTargetException error) {
                    item.put("outcome", error.getCause().getClass().getSimpleName());
                }
                item.put("publishAddressAfter", runtime.findBrokerAddressInPublish("broker"));
                item.put("rawObjectRetained", runtime.getAnExistTopicRouteData("raw") == raw);
                item.put("rawNullMapRetained", runtime.getAnExistTopicRouteData("raw").getBrokerDatas().get(0).getBrokerAddrs() == null);
                item.put("fetchCount", api.requests);
            } finally {
                java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                    org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
                cleanup.setAccessible(true);
                try { cleanup.invoke(runtime, original, new IllegalStateException("raw offline cleanup fixture teardown")); }
                finally { api.shutdown(); }
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> runtimeRawCleanupShapes(Map<String, Object> initial) throws Exception {
        List<String[]> shapes = List.of(
            new String[] {"null-route-collections", "{\"queueDatas\":null,\"brokerDatas\":null,\"filterServerTable\":null}"},
            new String[] {"null-broker-row", "{\"brokerDatas\":[null]}"},
            new String[] {"null-address-map", "{\"brokerDatas\":[{\"brokerName\":\"broker\",\"brokerAddrs\":null}]}"},
            new String[] {"empty-address-map", "{\"brokerDatas\":[{\"brokerName\":\"broker\",\"brokerAddrs\":{}}]}"},
            new String[] {"null-address-only", "{\"brokerDatas\":[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":null}}]}"},
            new String[] {"null-and-live-addresses", "{\"brokerDatas\":[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":null,\"2\":\"slave:10911\"}}]}"},
            new String[] {"null-row-and-live-address", "{\"brokerDatas\":[null,{\"brokerName\":\"broker\",\"brokerAddrs\":{\"2\":\"slave:10911\"}}]}"},
            new String[] {"partial-candidate-before-null-row", "{\"brokerDatas\":[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"master:10911\"}},null]}"},
            new String[] {"missing-route-fields", "{}"},
            new String[] {"null-broker-list", "{\"brokerDatas\":null}"},
            new String[] {"empty-broker-list", "{\"brokerDatas\":[]}"},
            new String[] {"missing-address-map", "{\"brokerDatas\":[{\"brokerName\":\"broker\"}]}"},
            new String[] {"null-broker-name-live-address", "{\"brokerDatas\":[{\"brokerName\":null,\"brokerAddrs\":{\"2\":\"slave:10911\"}}]}"},
            new String[] {"ignored-null-collections-live-address", "{\"queueDatas\":null,\"filterServerTable\":null,\"brokerDatas\":[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"2\":\"slave:10911\"}}]}"},
            new String[] {"same-address-under-renamed-broker", "{\"brokerDatas\":[{\"brokerName\":\"renamed\",\"brokerAddrs\":{\"9\":\"master:10911\"}}]}"},
            new String[] {"address-retained-by-sibling-topic", "{\"brokerDatas\":[]}"},
            new String[] {"candidate-before-null-address-map", "{\"brokerDatas\":[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"master:10911\"}},{\"brokerName\":\"broker\",\"brokerAddrs\":null}]}"},
            new String[] {"null-address-map-before-candidate", "{\"brokerDatas\":[{\"brokerName\":\"broken\",\"brokerAddrs\":null},{\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"master:10911\"}}]}"},
            new String[] {"null-broker-id-live-address", "{\"brokerDatas\":[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"null\":\"master:10911\"}}]}"},
            new String[] {"empty-address-row-removed-before-later-failure", "{\"brokerDatas\":[null]}"}
        );
        List<Object> cases = new ArrayList<>();
        for (String[] shape : shapes) {
            cases.add(runtimeRawCleanupShape(initial, shape[0], shape[1], false));
        }
        cases.add(runtimeRawCleanupShape(initial, "live-null-address-retained",
            "{\"brokerDatas\":[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":null}}]}", true));
        return cases;
    }

    private static Map<String, Object> runtimeRawCleanupShape(Map<String, Object> initial, String id,
        String rawInput, boolean seedNullMaster) throws Exception {
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("raw-cleanup-shape-" + id);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        RouteApi api = new RouteApi(config);
        Map<String, Object> item = new TreeMap<>();
        try {
            java.lang.reflect.Field apiField = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            apiField.setAccessible(true);
            apiField.set(runtime, api);

            Map<String, Object> seed = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(initial), Map.class);
            if (id.equals("empty-address-row-removed-before-later-failure")) {
                seed = com.alibaba.fastjson2.JSON.parseObject(
                    "{\"queueDatas\":[],\"brokerDatas\":["
                        + "{\"cluster\":\"cluster\",\"brokerName\":\"a\",\"brokerAddrs\":{}},"
                        + "{\"cluster\":\"cluster\",\"brokerName\":\"b\",\"brokerAddrs\":{\"0\":\"stale:10911\"}}],"
                        + "\"filterServerTable\":{}}",
                    Map.class);
            }
            if (seedNullMaster) {
                List<Map<String, Object>> brokers = (List<Map<String, Object>>) seed.get("brokerDatas");
                Map<String, Object> addresses = (Map<String, Object>) brokers.get(0).get("brokerAddrs");
                addresses.put("0", null);
            }
            TopicRouteData initialRoute = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(seed, com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue),
                TopicRouteData.class);
            api.route = initialRoute;
            item.put("id", id);
            item.put("initialInput", seed);
            item.put("rawInput", com.alibaba.fastjson2.JSON.parseObject(rawInput));
            runtime.updateTopicRouteInfoFromNameServer("topic");

            TopicRouteData raw = com.alibaba.fastjson2.JSON.parseObject(rawInput, TopicRouteData.class);
            runtime.getTopicRouteTable().clear();
            runtime.getTopicRouteTable().put("raw", raw);
            if (id.equals("empty-address-row-removed-before-later-failure")) {
                java.lang.reflect.Field addressesField = MQClientInstance.class.getDeclaredField("brokerAddrTable");
                addressesField.setAccessible(true);
                item.put("brokerAddressTraversal",
                    new ArrayList<>(((Map<?, ?>) addressesField.get(runtime)).keySet()));
            }
            if (id.equals("address-retained-by-sibling-topic")) {
                String siblingInput = "{\"brokerDatas\":[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"master:10911\"}}]}";
                TopicRouteData sibling = com.alibaba.fastjson2.JSON.parseObject(siblingInput, TopicRouteData.class);
                runtime.getTopicRouteTable().put("sibling", sibling);
                item.put("siblingInput", com.alibaba.fastjson2.JSON.parseObject(siblingInput));
                java.lang.reflect.Field versionsField = MQClientInstance.class.getDeclaredField("brokerVersionTable");
                versionsField.setAccessible(true);
                java.util.concurrent.ConcurrentMap<String, java.util.concurrent.ConcurrentHashMap<String, Integer>> versions =
                    (java.util.concurrent.ConcurrentMap<String, java.util.concurrent.ConcurrentHashMap<String, Integer>>) versionsField.get(runtime);
                versions.computeIfAbsent("broker", ignored -> new java.util.concurrent.ConcurrentHashMap<>())
                    .put("master:10911", 551);
                item.put("brokerVersionBeforeCleanup", versions.get("broker").get("master:10911"));
            }
            if (id.equals("partial-candidate-before-null-row")) {
                java.lang.reflect.Field addressesField = MQClientInstance.class.getDeclaredField("brokerAddrTable");
                addressesField.setAccessible(true);
                Map<?, ?> addressTable = (Map<?, ?>) addressesField.get(runtime);
                item.put("cleanupAddressTraversal",
                    new ArrayList<>(((Map<?, ?>) addressTable.get("broker")).values()));
                java.lang.reflect.Method exists = MQClientInstance.class.getDeclaredMethod(
                    "isBrokerAddrExistInTopicRouteTable", String.class);
                exists.setAccessible(true);
                item.put("masterAddressVisibleBeforeFailure", exists.invoke(runtime, "master:10911"));
                try {
                    exists.invoke(runtime, "slave:10911");
                    item.put("laterStaleAddressOutcome", "OK");
                } catch (java.lang.reflect.InvocationTargetException error) {
                    item.put("laterStaleAddressOutcome", error.getCause().getClass().getSimpleName());
                }
            }
            java.lang.reflect.Method clean = MQClientInstance.class.getDeclaredMethod("cleanOfflineBroker");
            clean.setAccessible(true);
            try {
                clean.invoke(runtime);
                item.put("outcome", "OK");
            } catch (java.lang.reflect.InvocationTargetException error) {
                item.put("outcome", error.getCause().getClass().getSimpleName());
            }

            java.lang.reflect.Field addressesField = MQClientInstance.class.getDeclaredField("brokerAddrTable");
            addressesField.setAccessible(true);
            Map<?, ?> brokerAddressTable = (Map<?, ?>) addressesField.get(runtime);
            Map<String, Object> normalized = new TreeMap<>();
            for (Map.Entry<?, ?> broker : brokerAddressTable.entrySet()) {
                Map<String, Object> byId = new TreeMap<>();
                for (Map.Entry<?, ?> address : ((Map<?, ?>) broker.getValue()).entrySet()) {
                    byId.put(String.valueOf(address.getKey()), address.getValue());
                }
                normalized.put(String.valueOf(broker.getKey()), byId);
            }
            item.put("brokerAddressTable", normalized);
            if (id.equals("address-retained-by-sibling-topic")) {
                java.lang.reflect.Field versionsField = MQClientInstance.class.getDeclaredField("brokerVersionTable");
                versionsField.setAccessible(true);
                Map<?, ?> versions = (Map<?, ?>) versionsField.get(runtime);
                item.put("brokerVersionAfterCleanup",
                    ((Map<?, ?>) versions.get("broker")).get("master:10911"));
            }
            item.put("fetchCount", api.requests);
            item.put("rawObjectRetained", runtime.getAnExistTopicRouteData("raw") == raw);
        } finally {
            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            try { cleanup.invoke(runtime, original, new IllegalStateException("raw cleanup shape fixture teardown")); }
            finally { api.shutdown(); }
        }
        return item;
    }

    private static void awaitUnchecked(CountDownLatch latch, String description) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out waiting for " + description);
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for " + description, error);
        }
    }

    private static boolean throwsNullPointer(Runnable operation) {
        try {
            operation.run();
            return false;
        } catch (NullPointerException error) {
            return true;
        }
    }

    private static Map<String, Object> runtimeTopicRouteTable(Map<String, Object> initial) throws Exception {
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("route-live-table");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        RouteApi api = new RouteApi(config);
        Map<String, Object> result = new TreeMap<>();
        try {
            java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            field.setAccessible(true); field.set(runtime, api);
            TopicRouteData first = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(initial), TopicRouteData.class);
            TopicRouteData second = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(initial), TopicRouteData.class);
            second.getBrokerDatas().get(0).getBrokerAddrs().put(0L, "replacement:10911");
            TopicRouteData equalSecond = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(second), TopicRouteData.class);
            result.put("initialInput", initial);
            result.put("replacementInput", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(second,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
            java.util.concurrent.ConcurrentMap<String, TopicRouteData> table = runtime.getTopicRouteTable();
            Map<String, Object> primitives = new TreeMap<>();
            primitives.put("initialEmpty", table.isEmpty());
            primitives.put("firstPutAbsent", table.put("topic", first) == null);
            primitives.put("containsValueNullThrowsNullPointer",
                throwsNullPointer(() -> table.containsValue(null)));
            primitives.put("putNullKeyThrowsNullPointer",
                throwsNullPointer(() -> table.put(null, second)));
            primitives.put("putNullValueThrowsNullPointer",
                throwsNullPointer(() -> table.put("topic", null)));
            primitives.put("putIfAbsentNullKeyThrowsNullPointer",
                throwsNullPointer(() -> table.putIfAbsent(null, second)));
            primitives.put("putIfAbsentNullValueThrowsNullPointer",
                throwsNullPointer(() -> table.putIfAbsent("topic", null)));
            primitives.put("replaceNullKeyThrowsNullPointer",
                throwsNullPointer(() -> table.replace(null, second)));
            primitives.put("replaceNullValueThrowsNullPointer",
                throwsNullPointer(() -> table.replace("topic", null)));
            primitives.put("conditionalReplaceNullKeyThrowsNullPointer",
                throwsNullPointer(() -> table.replace(null, first, second)));
            primitives.put("conditionalReplaceNullExpectedThrowsNullPointer",
                throwsNullPointer(() -> table.replace("topic", null, second)));
            primitives.put("conditionalReplaceNullValueThrowsNullPointer",
                throwsNullPointer(() -> table.replace("topic", first, null)));
            primitives.put("conditionalRemoveNullValueReturnsFalse",
                !table.remove("topic", null));
            primitives.put("nullMapOperationArgumentsKeepMapping",
                table.size() == 1 && table.get("topic") == first);
            java.util.Set<String> keyView = table.keySet();
            java.util.Collection<TopicRouteData> valuesView = table.values();
            java.util.Set<Map.Entry<String, TopicRouteData>> entryView = table.entrySet();
            primitives.put("keyViewNullContainsThrowsNullPointer",
                throwsNullPointer(() -> keyView.contains(null)));
            primitives.put("keyViewNullRemoveThrowsNullPointer",
                throwsNullPointer(() -> keyView.remove(null)));
            primitives.put("valuesViewNullContainsThrowsNullPointer",
                throwsNullPointer(() -> valuesView.contains(null)));
            primitives.put("valuesViewNullRemoveReturnsFalse", !valuesView.remove(null));
            Map.Entry<String, TopicRouteData> nullKeyEntry =
                new java.util.AbstractMap.SimpleImmutableEntry<>(null, first);
            primitives.put("entryViewNullKeyContainsReturnsFalse",
                !entryView.contains(nullKeyEntry));
            primitives.put("entryViewNullKeyRemoveReturnsFalse",
                !entryView.remove(nullKeyEntry));
            Map.Entry<String, TopicRouteData> nullValueEntry =
                new java.util.AbstractMap.SimpleImmutableEntry<>("topic", null);
            primitives.put("entryViewNullValueContainsReturnsFalse",
                !entryView.contains(nullValueEntry));
            primitives.put("entryViewNullValueRemoveReturnsFalse",
                !entryView.remove(nullValueEntry));
            primitives.put("nullCollectionViewArgumentsKeepMapping",
                table.size() == 1 && table.get("topic") == first);
            primitives.put("keyViewRemoveAllNullThrowsNullPointer",
                throwsNullPointer(() -> keyView.removeAll(null)));
            primitives.put("keyViewRetainAllNullThrowsNullPointer",
                throwsNullPointer(() -> keyView.retainAll(null)));
            primitives.put("valuesViewRemoveAllNullThrowsNullPointer",
                throwsNullPointer(() -> valuesView.removeAll(null)));
            primitives.put("valuesViewRetainAllNullThrowsNullPointer",
                throwsNullPointer(() -> valuesView.retainAll(null)));
            primitives.put("entryViewRemoveAllNullThrowsNullPointer",
                throwsNullPointer(() -> entryView.removeAll(null)));
            primitives.put("entryViewRetainAllNullThrowsNullPointer",
                throwsNullPointer(() -> entryView.retainAll(null)));
            primitives.put("nullViewCollectionArgumentsKeepMapping",
                table.size() == 1 && table.get("topic") == first);
            primitives.put("keyViewRemoveIfNullThrowsNullPointer",
                throwsNullPointer(() -> keyView.removeIf(null)));
            primitives.put("valuesViewRemoveIfNullThrowsNullPointer",
                throwsNullPointer(() -> valuesView.removeIf(null)));
            primitives.put("entryViewRemoveIfNullThrowsNullPointer",
                throwsNullPointer(() -> entryView.removeIf(null)));
            primitives.put("nullViewPredicateArgumentsKeepMapping",
                table.size() == 1 && table.get("topic") == first);
            primitives.put("forEachNullActionThrowsNullPointer",
                throwsNullPointer(() -> table.forEach(null)));
            primitives.put("replaceAllNullFunctionThrowsNullPointer",
                throwsNullPointer(() -> table.replaceAll(null)));
            primitives.put("computeIfAbsentNullFunctionThrowsNullPointer",
                throwsNullPointer(() -> table.computeIfAbsent("topic", null)));
            primitives.put("computeIfPresentNullFunctionThrowsNullPointer",
                throwsNullPointer(() -> table.computeIfPresent("topic", null)));
            primitives.put("computeNullFunctionThrowsNullPointer",
                throwsNullPointer(() -> table.compute("topic", null)));
            primitives.put("mergeNullFunctionThrowsNullPointer",
                throwsNullPointer(() -> table.merge("topic", second, null)));
            primitives.put("nullMapCallbackArgumentsKeepMapping",
                table.size() == 1 && table.get("topic") == first);
            AtomicInteger nullComputeCallbackCalls = new AtomicInteger();
            primitives.put("computeIfAbsentNullKeyThrowsNullPointer", throwsNullPointer(() ->
                table.computeIfAbsent(null, key -> {
                    nullComputeCallbackCalls.incrementAndGet();
                    return second;
                })));
            primitives.put("computeIfPresentNullKeyThrowsNullPointer", throwsNullPointer(() ->
                table.computeIfPresent(null, (key, oldValue) -> {
                    nullComputeCallbackCalls.incrementAndGet();
                    return second;
                })));
            primitives.put("computeNullKeyThrowsNullPointer", throwsNullPointer(() ->
                table.compute(null, (key, oldValue) -> {
                    nullComputeCallbackCalls.incrementAndGet();
                    return second;
                })));
            primitives.put("mergeNullKeyThrowsNullPointer", throwsNullPointer(() ->
                table.merge(null, second, (oldValue, newValue) -> {
                    nullComputeCallbackCalls.incrementAndGet();
                    return newValue;
                })));
            primitives.put("mergeNullValueThrowsNullPointer", throwsNullPointer(() ->
                table.merge("topic", null, (oldValue, newValue) -> {
                    nullComputeCallbackCalls.incrementAndGet();
                    return newValue;
                })));
            primitives.put("nullComputeArgumentsSkipCallbacks", nullComputeCallbackCalls.get() == 0);
            primitives.put("nullComputeArgumentsKeepMapping",
                table.size() == 1 && table.get("topic") == first);
            primitives.put("getOrDefaultPresent", table.getOrDefault("topic", second) == first);
            primitives.put("getOrDefaultAbsent", table.getOrDefault("missing", second) == second);
            primitives.put("getOrDefaultNullDefaultPresent", table.getOrDefault("topic", null) == first);
            primitives.put("getOrDefaultNullDefaultAbsent", table.getOrDefault("missing", null) == null);
            String nullGetOrDefaultKeyError = "OK";
            try {
                table.getOrDefault(null, null);
            } catch (Throwable error) {
                nullGetOrDefaultKeyError = error.getClass().getSimpleName();
            }
            primitives.put("getOrDefaultNullKeyThrowsNullPointer",
                "NullPointerException".equals(nullGetOrDefaultKeyError));
            String nullGetKeyError = "OK";
            try {
                table.get(null);
            } catch (Throwable error) {
                nullGetKeyError = error.getClass().getSimpleName();
            }
            primitives.put("getNullKeyThrowsNullPointer",
                "NullPointerException".equals(nullGetKeyError));
            String nullContainsKeyError = "OK";
            try {
                table.containsKey(null);
            } catch (Throwable error) {
                nullContainsKeyError = error.getClass().getSimpleName();
            }
            primitives.put("containsKeyNullKeyThrowsNullPointer",
                "NullPointerException".equals(nullContainsKeyError));
            primitives.put("containsKeyNullKeyKeepsMapping", table.get("topic") == first);
            String nullRemoveKeyError = "OK";
            try {
                table.remove(null);
            } catch (Throwable error) {
                nullRemoveKeyError = error.getClass().getSimpleName();
            }
            primitives.put("removeNullKeyThrowsNullPointer",
                "NullPointerException".equals(nullRemoveKeyError));
            primitives.put("removeNullKeyKeepsMapping", table.get("topic") == first);
            primitives.put("getterSeesFirst", runtime.getAnExistTopicRouteData("topic") == first);
            primitives.put("putReturnsFirst", table.put("topic", second) == first);
            primitives.put("retainedGetterSeesReplacement", runtime.getTopicRouteTable() == table && runtime.getAnExistTopicRouteData("topic") == second);
            primitives.put("putIfAbsentKeepsSecond", table.putIfAbsent("topic", first) == second);
            primitives.put("unequalReplaceRejected", !table.replace("topic", first, second));
            primitives.put("equalCopyReplaceAccepted", equalSecond != second && table.replace("topic", equalSecond, first));
            primitives.put("replaceReturnsFirst", table.replace("topic", second) == first);
            primitives.put("unequalRemoveRejected", !table.remove("topic", first));
            primitives.put("equalCopyRemoveAccepted", table.remove("topic", equalSecond));
            primitives.put("absentReplaceNull", table.replace("topic", first) == null);
            primitives.put("absentRemoveNull", table.remove("topic") == null);
            primitives.put("putIfAbsentAbsentNull", table.putIfAbsent("topic", first) == null);
            primitives.put("emptyKeyAccepted", table.put("", second) == null);
            primitives.put("sizeTwo", table.size() == 2 && table.containsKey(""));
            table.clear();
            primitives.put("retainedViewSeesClear", table.isEmpty());
            primitives.put("getterSeesClear", runtime.getAnExistTopicRouteData("topic") == null);
            primitives.put("putAfterClearAccepted", table.put("topic", second) == null && runtime.getAnExistTopicRouteData("topic") == second);
            Map<String, TopicRouteData> bulkValues = new TreeMap<>();
            bulkValues.put("bulk-a", first);
            bulkValues.put("bulk-b", second);
            table.putAll(bulkValues);
            primitives.put("putAllAdds", table.get("bulk-a") == first && table.get("bulk-b") == second
                && table.size() == 3);
            String beforeNullRoute = "a-route-put-all-before-null-value";
            String nullRouteValue = "m-route-put-all-null-value";
            String afterNullRoute = "z-route-put-all-after-null-value";
            Map<String, TopicRouteData> treeRouteInputs = new TreeMap<>();
            treeRouteInputs.put(afterNullRoute, second);
            treeRouteInputs.put(nullRouteValue, null);
            treeRouteInputs.put(beforeNullRoute, first);
            Map<String, Object> treeRouteOutcome = new TreeMap<>();
            treeRouteOutcome.put("visitOrder", new ArrayList<>(treeRouteInputs.keySet()));
            treeRouteOutcome.put("nullValueKey", nullRouteValue);
            String treeRouteError = "OK";
            try {
                table.putAll(treeRouteInputs);
            } catch (Throwable error) {
                treeRouteError = error.getClass().getSimpleName();
            }
            treeRouteOutcome.put("outcome", treeRouteError);
            treeRouteOutcome.put("keysAfter", new ArrayList<>(new java.util.TreeSet<>(table.keySet())));
            result.put("treeMapPutAllNullValue", treeRouteOutcome);
            primitives.put("treeMapPutAllThrowsNullValue", "NullPointerException".equals(treeRouteError));
            primitives.put("treeMapPutAllKeepsBeforeRow", table.get(beforeNullRoute) == first);
            primitives.put("treeMapPutAllSkipsAfterRow", !table.containsKey(afterNullRoute));
            table.remove(beforeNullRoute);
            String beforeNullKeyRoute = "a-route-put-all-before-null-key";
            String nullKeyBoundary = "m-route-put-all-null-key";
            String afterNullKeyRoute = "z-route-put-all-after-null-key";
            Map<String, TopicRouteData> treeNullKeyInputs = new TreeMap<>((left, right) -> {
                if (left == right) return 0;
                if (left == null) return right.compareTo(nullKeyBoundary) >= 0 ? -1 : 1;
                if (right == null) return left.compareTo(nullKeyBoundary) >= 0 ? 1 : -1;
                return left.compareTo(right);
            });
            treeNullKeyInputs.put(afterNullKeyRoute, second);
            treeNullKeyInputs.put(null, first);
            treeNullKeyInputs.put(beforeNullKeyRoute, first);
            Map<String, Object> treeNullKeyOutcome = new TreeMap<>();
            treeNullKeyOutcome.put("visitOrder", new ArrayList<>(treeNullKeyInputs.keySet()));
            treeNullKeyOutcome.put("nullKey", null);
            String treeNullKeyError = "OK";
            try {
                table.putAll(treeNullKeyInputs);
            } catch (Throwable error) {
                treeNullKeyError = error.getClass().getSimpleName();
            }
            treeNullKeyOutcome.put("outcome", treeNullKeyError);
            treeNullKeyOutcome.put("keysAfter", new ArrayList<>(new java.util.TreeSet<>(table.keySet())));
            result.put("treeMapPutAllNullKey", treeNullKeyOutcome);
            primitives.put("treeMapPutAllThrowsNullKey", "NullPointerException".equals(treeNullKeyError));
            primitives.put("treeMapPutAllNullKeyKeepsBeforeRow", table.get(beforeNullKeyRoute) == first);
            primitives.put("treeMapPutAllNullKeySkipsAfterRow", !table.containsKey(afterNullKeyRoute));
            table.remove(beforeNullKeyRoute);
            Map<String, Object> nullPutAllOutcome = new TreeMap<>();
            String nullPutAllError = "OK";
            try {
                table.putAll(null);
            } catch (Throwable error) {
                nullPutAllError = error.getClass().getSimpleName();
            }
            nullPutAllOutcome.put("outcome", nullPutAllError);
            nullPutAllOutcome.put("keysAfter", new ArrayList<>(new java.util.TreeSet<>(table.keySet())));
            result.put("nullPutAll", nullPutAllOutcome);
            primitives.put("nullPutAllThrowsNullPointer", "NullPointerException".equals(nullPutAllError));
            primitives.put("nullPutAllKeepsMappings", table.size() == 3
                && table.get("topic") == second && table.get("bulk-a") == first && table.get("bulk-b") == second);
            TopicRouteData absentValue = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(second), TopicRouteData.class);
            absentValue.getBrokerDatas().get(0).setBrokerName("absent-broker");
            primitives.put("containsValueEqualCopy", table.containsValue(equalSecond));
            primitives.put("containsValueAbsent", !table.containsValue(absentValue));
            Map<String, TopicRouteData> expectedRoutes = new TreeMap<>();
            expectedRoutes.put("topic", second);
            expectedRoutes.put("bulk-a", first);
            expectedRoutes.put("bulk-b", second);
            Map<String, Boolean> visitedRoutes = new TreeMap<>();
            table.forEach((key, value) -> visitedRoutes.put(key, expectedRoutes.get(key) == value));
            primitives.put("forEachVisitsCurrentMappings", visitedRoutes.size() == expectedRoutes.size()
                && visitedRoutes.values().stream().allMatch(Boolean::booleanValue));
            primitives.put("computeIfAbsentMaps", table.computeIfAbsent("compute-a", key -> first) == first);
            primitives.put("computeIfAbsentKeepsCurrent", table.computeIfAbsent("compute-a", key -> second) == first);
            primitives.put("computeIfAbsentNullLeavesAbsent", table.computeIfAbsent("compute-null", key -> null) == null
                && !table.containsKey("compute-null"));
            primitives.put("computeIfPresentReplaces", table.computeIfPresent("compute-a", (key, value) -> second) == second
                && table.get("compute-a") == second);
            primitives.put("computeIfPresentNullRemoves", table.computeIfPresent("compute-a", (key, value) -> null) == null
                && !table.containsKey("compute-a"));
            primitives.put("computeReplaces", table.compute("compute-a", (key, value) -> first) == first
                && table.get("compute-a") == first);
            primitives.put("computeAbsentInserts", table.compute("compute-b", (key, value) -> second) == second);
            primitives.put("computeNullRemoves", table.compute("compute-b", (key, value) -> null) == null
                && !table.containsKey("compute-b"));
            primitives.put("computeAbsentNullLeavesAbsent", table.compute("compute-null-absent", (key, value) -> null) == null
                && !table.containsKey("compute-null-absent"));
            primitives.put("mergeCombines", table.merge("compute-a", second, (oldValue, newValue) -> newValue) == second);
            primitives.put("mergeAbsentInserts", table.merge("merge-new", first, (oldValue, newValue) -> null) == first);
            table.put("merge-remove", second);
            primitives.put("mergeNullRemoves", table.merge("merge-remove", first, (oldValue, newValue) -> null) == null
                && !table.containsKey("merge-remove"));
            table.replaceAll((key, value) -> key.equals("topic") ? second : first);
            primitives.put("replaceAllUpdatesMappings", table.size() == 5 && table.get("topic") == second
                && table.get("bulk-a") == first && table.get("compute-a") == first && table.get("merge-new") == first);
            table.put("replaceAll-race", first);
            AtomicInteger replaceAllCalls = new AtomicInteger();
            CountDownLatch replaceAllEntered = new CountDownLatch(1);
            CountDownLatch releaseReplaceAll = new CountDownLatch(1);
            CountDownLatch replaceAllWriterStarted = new CountDownLatch(1);
            CountDownLatch replaceAllWriterFinished = new CountDownLatch(1);
            AtomicReference<TopicRouteData> replaceAllWriterPrevious = new AtomicReference<>();
            AtomicReference<Throwable> replaceAllFailure = new AtomicReference<>();
            AtomicReference<Throwable> replaceAllWriterFailure = new AtomicReference<>();
            Thread replaceAllThread = new Thread(() -> {
                try {
                    table.replaceAll((key, oldValue) -> {
                        if (!key.equals("replaceAll-race")) {
                            return oldValue;
                        }
                        if (replaceAllCalls.incrementAndGet() == 1) {
                            replaceAllEntered.countDown();
                            awaitUnchecked(releaseReplaceAll, "raw route replaceAll release");
                        }
                        return second;
                    });
                } catch (Throwable error) {
                    replaceAllFailure.set(error);
                }
            }, "java-raw-route-replace-all-oracle");
            Thread replaceAllWriterThread = new Thread(() -> {
                replaceAllWriterStarted.countDown();
                try {
                    replaceAllWriterPrevious.set(table.put("replaceAll-race", second));
                } catch (Throwable error) {
                    replaceAllWriterFailure.set(error);
                } finally {
                    replaceAllWriterFinished.countDown();
                }
            }, "java-raw-route-replace-all-writer");
            replaceAllThread.setDaemon(true);
            replaceAllWriterThread.setDaemon(true);
            replaceAllThread.start();
            try {
                awaitUnchecked(replaceAllEntered, "raw route replaceAll callback");
                replaceAllWriterThread.start();
                awaitUnchecked(replaceAllWriterStarted, "raw route replaceAll same-key writer");
                primitives.put("replaceAllWriterRunsDuringCallback",
                    replaceAllWriterFinished.await(100, TimeUnit.MILLISECONDS));
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while checking raw route replaceAll", error);
            } finally {
                releaseReplaceAll.countDown();
            }
            try {
                replaceAllThread.join(5000);
                replaceAllWriterThread.join(5000);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while joining raw route replaceAll threads", error);
            }
            if (replaceAllThread.isAlive() || replaceAllWriterThread.isAlive()) {
                throw new IllegalStateException("ConcurrentHashMap raw route replaceAll threads did not finish");
            }
            if (replaceAllFailure.get() != null) {
                throw new IllegalStateException("ConcurrentHashMap raw route replaceAll failed", replaceAllFailure.get());
            }
            if (replaceAllWriterFailure.get() != null) {
                throw new IllegalStateException("ConcurrentHashMap raw route replaceAll writer failed", replaceAllWriterFailure.get());
            }
            primitives.put("replaceAllCallbackRetriesAfterWriter", replaceAllCalls.get() == 2);
            primitives.put("replaceAllWriterObservedOldValue", replaceAllWriterPrevious.get() == first);
            primitives.put("replaceAllFinalValueIsComputed", table.get("replaceAll-race") == second);
            table.remove("replaceAll-race");
            Map<String, TopicRouteData> beforeNullReplaceAll = new HashMap<>(table);
            table.clear();
            String nullReplaceAllBefore = "a-route-replace-all-before-null";
            String nullReplaceAllFailure = "m-route-replace-all-null-result";
            String nullReplaceAllAfter = "z-route-replace-all-after-null";
            table.put(nullReplaceAllAfter, first);
            table.put(nullReplaceAllFailure, first);
            table.put(nullReplaceAllBefore, first);
            List<String> nullReplaceAllVisitOrder = new ArrayList<>();
            String nullReplaceAllError = "OK";
            try {
                table.replaceAll((key, oldValue) -> {
                    nullReplaceAllVisitOrder.add(key);
                    return nullReplaceAllVisitOrder.size() == 2 ? null : second;
                });
            } catch (Throwable error) {
                nullReplaceAllError = error.getClass().getSimpleName();
            }
            Map<String, Object> nullReplaceAllOutcome = new TreeMap<>();
            nullReplaceAllOutcome.put("outcome", nullReplaceAllError);
            nullReplaceAllOutcome.put("visitOrder", new ArrayList<>(nullReplaceAllVisitOrder));
            List<String> nullReplaceAllUpdatedKeys = new ArrayList<>();
            for (String key : table.keySet()) {
                if (table.get(key) == second) nullReplaceAllUpdatedKeys.add(key);
            }
            nullReplaceAllUpdatedKeys.sort(String::compareTo);
            nullReplaceAllOutcome.put("updatedKeys", nullReplaceAllUpdatedKeys);
            result.put("replaceAllNullResult", nullReplaceAllOutcome);
            primitives.put("replaceAllNullResultThrowsNullPointer",
                "NullPointerException".equals(nullReplaceAllError));
            primitives.put("replaceAllNullResultVisitsPrefixOnly", nullReplaceAllVisitOrder.size() == 2
                && table.get(nullReplaceAllVisitOrder.get(0)) == second
                && table.get(nullReplaceAllVisitOrder.get(1)) == first);
            primitives.put("replaceAllNullResultStopsBeforeSuffix", nullReplaceAllVisitOrder.size() == 2);
            table.clear();
            table.putAll(beforeNullReplaceAll);
            table.put("remapping-race", first);
            AtomicInteger remappingCalls = new AtomicInteger();
            CountDownLatch remappingEntered = new CountDownLatch(1);
            CountDownLatch releaseRemapping = new CountDownLatch(1);
            CountDownLatch writerStarted = new CountDownLatch(1);
            CountDownLatch writerFinished = new CountDownLatch(1);
            AtomicReference<TopicRouteData> remappingRead = new AtomicReference<>();
            AtomicReference<TopicRouteData> remappingResult = new AtomicReference<>();
            AtomicReference<TopicRouteData> writerPrevious = new AtomicReference<>();
            AtomicReference<Throwable> writerFailure = new AtomicReference<>();
            Thread remappingThread = new Thread(() -> remappingResult.set(table.compute("remapping-race", (key, old) -> {
                remappingCalls.incrementAndGet();
                remappingRead.set(table.get(key));
                remappingEntered.countDown();
                awaitUnchecked(releaseRemapping, "raw route remapping release");
                return second;
            })), "java-raw-route-remapping-oracle");
            Thread writerThread = new Thread(() -> {
                writerStarted.countDown();
                try {
                    writerPrevious.set(table.put("remapping-race", first));
                } catch (Throwable error) {
                    writerFailure.set(error);
                } finally {
                    writerFinished.countDown();
                }
            }, "java-raw-route-remapping-writer");
            remappingThread.setDaemon(true);
            writerThread.setDaemon(true);
            remappingThread.start();
            try {
                awaitUnchecked(remappingEntered, "raw route remapping callback");
                writerThread.start();
                awaitUnchecked(writerStarted, "raw route same-key writer");
                primitives.put("remappingWriterWaitsForCallback",
                    !writerFinished.await(100, TimeUnit.MILLISECONDS));
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while checking raw route remapping", error);
            } finally {
                releaseRemapping.countDown();
            }
            try {
                remappingThread.join(5000);
                writerThread.join(5000);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while joining raw route remapping threads", error);
            }
            if (remappingThread.isAlive() || writerThread.isAlive()) {
                throw new IllegalStateException("ConcurrentHashMap raw route remapping threads did not finish");
            }
            if (writerFailure.get() != null) {
                throw new IllegalStateException("ConcurrentHashMap raw route writer failed", writerFailure.get());
            }
            primitives.put("remappingCallbackRunsOnce", remappingCalls.get() == 1);
            primitives.put("remappingCallbackReadsCurrent", remappingRead.get() == first);
            primitives.put("remappingReturnsAndKeepsWriterValue", remappingResult.get() == second
                && writerPrevious.get() == second && table.get("remapping-race") == first);
            table.remove("remapping-race");

            table.put("clear-race", first);
            AtomicInteger clearComputeCalls = new AtomicInteger();
            CountDownLatch clearComputeEntered = new CountDownLatch(1);
            CountDownLatch releaseClearCompute = new CountDownLatch(1);
            CountDownLatch clearStarted = new CountDownLatch(1);
            CountDownLatch clearFinished = new CountDownLatch(1);
            AtomicReference<TopicRouteData> clearComputeResult = new AtomicReference<>();
            AtomicReference<Throwable> clearFailure = new AtomicReference<>();
            Thread clearComputeThread = new Thread(() -> clearComputeResult.set(table.compute("clear-race", (key, old) -> {
                clearComputeCalls.incrementAndGet();
                clearComputeEntered.countDown();
                awaitUnchecked(releaseClearCompute, "raw route clear-race compute release");
                return second;
            })), "java-raw-route-clear-compute");
            Thread clearThread = new Thread(() -> {
                clearStarted.countDown();
                try {
                    table.clear();
                } catch (Throwable error) {
                    clearFailure.set(error);
                } finally {
                    clearFinished.countDown();
                }
            }, "java-raw-route-clear");
            clearComputeThread.setDaemon(true);
            clearThread.setDaemon(true);
            clearComputeThread.start();
            try {
                awaitUnchecked(clearComputeEntered, "raw route clear-race compute callback");
                clearThread.start();
                awaitUnchecked(clearStarted, "raw route clear thread");
                primitives.put("clearWaitsForCompute",
                    !clearFinished.await(100, TimeUnit.MILLISECONDS));
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while checking raw route clear", error);
            } finally {
                releaseClearCompute.countDown();
            }
            try {
                clearComputeThread.join(5000);
                clearThread.join(5000);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while joining raw route clear threads", error);
            }
            if (clearComputeThread.isAlive() || clearThread.isAlive()) {
                throw new IllegalStateException("ConcurrentHashMap raw route clear threads did not finish");
            }
            if (clearFailure.get() != null) {
                throw new IllegalStateException("ConcurrentHashMap raw route clear failed", clearFailure.get());
            }
            primitives.put("clearComputeRunsOnce", clearComputeCalls.get() == 1);
            primitives.put("clearComputeReturnsValue", clearComputeResult.get() == second);
            primitives.put("clearRemovesComputedValue", !table.containsKey("clear-race"));

            table.put("clear-reentrant", first);
            AtomicReference<Throwable> reentrantClearFailure = new AtomicReference<>();
            AtomicReference<Boolean> reentrantClearObservedAbsent = new AtomicReference<>(false);
            TopicRouteData reentrantClearResult = table.compute("clear-reentrant", (key, oldValue) -> {
                try {
                    table.clear();
                    reentrantClearObservedAbsent.set(!table.containsKey(key));
                } catch (Throwable error) {
                    reentrantClearFailure.set(error);
                }
                return second;
            });
            primitives.put("clearReentrantCompletes", reentrantClearFailure.get() == null);
            primitives.put("clearReentrantRemovesBeforeCallbackReturns",
                Boolean.TRUE.equals(reentrantClearObservedAbsent.get()));
            primitives.put("clearReentrantComputeReturnsValue", reentrantClearResult == second);
            primitives.put("clearReentrantComputedValueRemains", table.get("clear-reentrant") == second);
            table.clear();

            table.remove("compute-a");
            table.remove("merge-new");
            table.clear();

            table.put("key-remove", first);
            table.put("key-keep", second);
            table.put("key-retain", first);
            primitives.put("keyViewContains", table.keySet().contains("key-keep"));
            primitives.put("keyViewRemove", table.keySet().remove("key-remove") && !table.containsKey("key-remove"));
            primitives.put("keyViewRemoveIf", table.keySet().removeIf(key -> key.equals("key-keep"))
                && !table.containsKey("key-keep"));
            table.put("key-all", second);
            table.put("key-retain-all", first);
            primitives.put("keyViewRemoveAll", table.keySet().removeAll(java.util.Set.of("key-all"))
                && !table.containsKey("key-all"));
            primitives.put("keyViewRetainAll", table.keySet().retainAll(java.util.Set.of("key-retain-all"))
                && table.size() == 1 && table.containsKey("key-retain-all"));

            table.clear();
            table.put("value-a", first);
            table.put("value-b", second);
            table.put("value-c", second);
            primitives.put("valuesViewContainsEqualCopy", table.values().contains(equalSecond));
            primitives.put("valuesViewRemoveEqualCopy", table.values().remove(equalSecond)
                && table.values().stream().filter(equalSecond::equals).count() == 1);
            primitives.put("valuesViewRemoveIf", table.values().removeIf(value -> value.equals(first))
                && !table.containsKey("value-a"));
            table.clear();
            table.put("value-all", second);
            table.put("value-retain-drop", second);
            table.put("value-retain-keep", first);
            primitives.put("valuesViewRemoveAllEqual", table.values().removeAll(java.util.List.of(equalSecond))
                && !table.containsKey("value-all") && !table.containsKey("value-retain-drop"));
            table.clear();
            table.put("value-retain-drop", second);
            table.put("value-retain-keep", first);
            primitives.put("valuesViewRetainAllEqual", table.values().retainAll(java.util.List.of(first))
                && table.size() == 1 && table.containsKey("value-retain-keep"));

            java.util.Map.Entry<String, TopicRouteData> entryKeep =
                new java.util.AbstractMap.SimpleImmutableEntry<>("entry-keep", equalSecond);
            table.clear();
            table.put("entry-remove", first);
            table.put("entry-keep", second);
            primitives.put("entryViewContainsEqualCopy", table.entrySet().contains(entryKeep));
            primitives.put("entryViewRemoveEqualCopy", table.entrySet().remove(
                new java.util.AbstractMap.SimpleImmutableEntry<>("entry-remove", first))
                && !table.containsKey("entry-remove"));
            primitives.put("entryViewRemoveIf", table.entrySet().removeIf(entry -> entry.getKey().equals("entry-keep"))
                && !table.containsKey("entry-keep"));
            table.put("entry-all", second);
            table.put("entry-retain-drop", second);
            table.put("entry-retain-keep", first);
            primitives.put("entryViewRemoveAllEqual", table.entrySet().removeAll(java.util.Set.of(
                new java.util.AbstractMap.SimpleImmutableEntry<>("entry-all", equalSecond)))
                && !table.containsKey("entry-all"));
            primitives.put("entryViewRetainAllEqual", table.entrySet().retainAll(java.util.Set.of(
                new java.util.AbstractMap.SimpleImmutableEntry<>("entry-retain-keep", first)))
                && table.size() == 1 && table.containsKey("entry-retain-keep"));

            table.put("key-view-clear", first);
            table.keySet().clear();
            primitives.put("keyViewClear", table.isEmpty());
            table.put("values-view-clear", first);
            table.values().clear();
            primitives.put("valuesViewClear", table.isEmpty());
            table.put("entry-view-clear", first);
            table.entrySet().clear();
            primitives.put("entryViewClear", table.isEmpty());

            table.clear();
            table.put("iterator-key", first);
            java.util.Iterator<String> keyIterator = table.keySet().iterator();
            keyIterator.next();
            keyIterator.remove();
            primitives.put("keyIteratorRemove", table.isEmpty());
            table.put("iterator-value", first);
            java.util.Iterator<TopicRouteData> valueIterator = table.values().iterator();
            valueIterator.next();
            valueIterator.remove();
            primitives.put("valuesIteratorRemove", table.isEmpty());
            table.put("iterator-entry", first);
            java.util.Iterator<java.util.Map.Entry<String, TopicRouteData>> entryIterator = table.entrySet().iterator();
            entryIterator.next();
            entryIterator.remove();
            primitives.put("entryIteratorRemove", table.isEmpty());
            table.put("entry-set-value", first);
            java.util.Iterator<java.util.Map.Entry<String, TopicRouteData>> setValueIterator =
                table.entrySet().iterator();
            java.util.Map.Entry<String, TopicRouteData> setValueEntry = setValueIterator.next();
            primitives.put("entrySetValueReturnsPrevious", setValueEntry.setValue(second) == first);
            primitives.put("entrySetValueUpdatesEntry", setValueEntry.getValue() == second);
            primitives.put("entrySetValueUpdatesTable", table.get("entry-set-value") == second);
            table.put("entry-set-value", equalSecond);
            primitives.put("entrySetValueEntryIsSnapshot", setValueEntry.getValue() == second);
            primitives.put("entrySetValueReturnsCapturedPrevious", setValueEntry.setValue(first) == second);
            primitives.put("entrySetValueReplacesCurrentMapping", table.get("entry-set-value") == first);
            String nullSetValueError = "OK";
            try {
                setValueEntry.setValue(null);
            } catch (Throwable error) {
                nullSetValueError = error.getClass().getSimpleName();
            }
            primitives.put("entrySetValueNullThrowsNullPointer", "NullPointerException".equals(nullSetValueError));
            primitives.put("entrySetValueNullLeavesEntry", setValueEntry.getValue() == first);
            primitives.put("entrySetValueNullLeavesMapping", table.get("entry-set-value") == first);
            table.clear();
            result.put("primitives", primitives);

            Map<String, Object> mutableAlias = new TreeMap<>();
            TopicRouteData aliasValue = new TopicRouteData();
            aliasValue.setOrderTopicConf("broker:1");
            mutableAlias.put("putRetainsObjectIdentity",
                table.put("mutable-alias", aliasValue) == null
                    && table.get("mutable-alias") == aliasValue
                    && runtime.getAnExistTopicRouteData("mutable-alias") == aliasValue);
            TopicRouteData aliasFromTable = table.get("mutable-alias");
            aliasValue.setOrderTopicConf("broker:2");
            mutableAlias.put("originalMutationVisibleThroughTable",
                table.get("mutable-alias").getOrderTopicConf().equals("broker:2"));
            aliasFromTable.setOrderTopicConf("broker:3");
            mutableAlias.put("tableValueMutationVisibleThroughOriginal",
                aliasValue.getOrderTopicConf().equals("broker:3"));
            result.put("mutableAlias", mutableAlias);
            table.clear();

            api.route = first;
            result.put("refreshChanged", runtime.updateTopicRouteInfoFromNameServer("topic"));
            result.put("retainedViewSeesRefresh", table.get("topic") == runtime.getAnExistTopicRouteData("topic"));
            result.put("initialPublishAddress", runtime.findBrokerAddressInPublish("broker"));
            TopicRouteData cached = table.get("topic");
            result.put("directPutReturnsCached", table.put("topic", second) == cached);
            result.put("topicAddressAfterDirectPut", runtime.findBrokerAddrByTopic("topic"));
            result.put("publishAddressAfterDirectPut", runtime.findBrokerAddressInPublish("broker"));
            int beforeQuery = api.requests;
            result.put("queryReturnsDirectValueWithoutFetch", runtime.queryTopicRouteData("topic") == second && api.requests == beforeQuery);
            table.clear();
            result.put("getterAbsentAfterClear", runtime.getAnExistTopicRouteData("topic") == null);
            result.put("topicAddressAfterClear", runtime.findBrokerAddrByTopic("topic"));
            result.put("publishAddressAfterClear", runtime.findBrokerAddressInPublish("broker"));
            java.lang.reflect.Method clean = MQClientInstance.class.getDeclaredMethod("cleanOfflineBroker");
            clean.setAccessible(true); clean.invoke(runtime);
            result.put("publishAddressAfterOfflineClean", runtime.findBrokerAddressInPublish("broker"));
            result.put("queryAfterClearRefetches", runtime.queryTopicRouteData("topic") != null && api.requests == beforeQuery + 1);
            result.put("retainedViewSeesQueryRefresh", table.containsKey("topic"));

            TopicRouteData metadataOnly = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(first), TopicRouteData.class);
            metadataOnly.getBrokerDatas().get(0).setCluster("other-cluster");
            metadataOnly.getBrokerDatas().get(0).setZoneName("other-zone");
            metadataOnly.getBrokerDatas().get(0).setEnableActingMaster(true);
            result.put("metadataOnlyInput", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(metadataOnly,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
            result.put("brokerMetadataIgnoredByRouteEquals", first.equals(metadataOnly));
            table.put("metadata", first);
            result.put("metadataEqualConditionalRemoveAccepted", table.remove("metadata", metadataOnly));
            TopicRouteData beforeMetadataRefresh = table.get("topic");
            api.route = metadataOnly;
            result.put("metadataOnlyRefreshChanged", runtime.updateTopicRouteInfoFromNameServer("topic"));
            result.put("metadataOnlyRefreshKeepsCachedObject", table.get("topic") == beforeMetadataRefresh);

            TopicRouteData invalid = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(initial), TopicRouteData.class);
            invalid.getBrokerDatas().get(0).setBrokerAddrs(null);
            result.put("rawPutAcceptsNullBrokerMap", table.put("invalid", invalid) == null && runtime.getAnExistTopicRouteData("invalid") == invalid);
            int beforeInvalidQuery = api.requests;
            result.put("queryReturnsInvalidRawValueWithoutValidation", runtime.queryTopicRouteData("invalid") == invalid && api.requests == beforeInvalidQuery);
            api.route = invalid;
            result.put("unchangedInvalidRawRefreshChanged", runtime.updateTopicRouteInfoFromNameServer("invalid"));
            result.put("unchangedInvalidRawRefreshKeepsCachedObject", table.get("invalid") == invalid);
            table.clear();
        } finally {
            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            try { cleanup.invoke(runtime, original, new IllegalStateException("live route-table fixture teardown")); }
            finally { api.shutdown(); }
        }
        return result;
    }

    private static org.apache.rocketmq.remoting.protocol.route.BrokerData brokerData(
        String cluster, String name, HashMap<Long, String> addresses, boolean actingMaster, String zone) {
        org.apache.rocketmq.remoting.protocol.route.BrokerData data =
            new org.apache.rocketmq.remoting.protocol.route.BrokerData();
        data.setCluster(cluster);
        data.setBrokerName(name);
        data.setBrokerAddrs(addresses);
        data.setEnableActingMaster(actingMaster);
        data.setZoneName(zone);
        return data;
    }

    private static List<Object> brokerDataSelectAddrCases() {
        String[] inputs = new String[] {
            "{\"brokerAddrs\":{\"0\":\"master\",\"1\":\"slave\"}}",
            "{\"brokerAddrs\":{\"1\":\"one\",\"2\":\"two\"}}",
            "{\"brokerAddrs\":{\"0\":null,\"2\":\"slave\"}}",
            "{\"brokerAddrs\":{\"1\":null}}",
            "{\"brokerAddrs\":{}}",
            "{\"brokerAddrs\":null}",
            "{}",
            "{\"brokerAddrs\":{\"null\":\"only-null-id\"}}",
            "{\"brokerAddrs\":{\"null\":\"null-id\",\"2\":\"slave\"}}",
            "{\"brokerAddrs\":{\"null\":\"null-id\",\"0\":\"master\",\"2\":\"slave\"}}"
        };
        List<Object> cases = new ArrayList<>();
        for (int index = 0; index < inputs.length; index++) {
            org.apache.rocketmq.remoting.protocol.route.BrokerData data =
                com.alibaba.fastjson2.JSON.parseObject(inputs[index],
                    org.apache.rocketmq.remoting.protocol.route.BrokerData.class);
            Map<String, Object> item = new TreeMap<>();
            TreeSet<String> addresses = new TreeSet<>();
            boolean nullAddress = false;
            String exception = null;
            for (int sample = 0; sample < 256; sample++) {
                try {
                    String address = data.selectBrokerAddr();
                    if (address == null) nullAddress = true;
                    else addresses.add(address);
                } catch (RuntimeException error) {
                    exception = error.getClass().getSimpleName();
                    break;
                }
            }
            item.put("id", "select-" + index);
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(inputs[index]));
            item.put("addresses", new ArrayList<>(addresses));
            item.put("nullAddress", nullAddress);
            item.put("exception", exception);
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> brokerDataEqualityCases() {
        HashMap<Long, String> master = new HashMap<>();
        master.put(0L, "master:10911");
        HashMap<Long, String> otherMaster = new HashMap<>();
        otherMaster.put(0L, "other:10911");
        HashMap<Long, String> withNull = new HashMap<>();
        withNull.put(0L, null);
        withNull.put(2L, "slave:10911");
        List<Object> cases = new ArrayList<>();
        addBrokerDataEqualityCase(cases, "metadata-only", brokerData("cluster-a", "broker", master, false, "zone-a"),
            brokerData("cluster-b", "broker", master, true, "zone-b"));
        addBrokerDataEqualityCase(cases, "broker-name", brokerData("cluster", "broker-a", master, false, null),
            brokerData("cluster", "broker-b", master, false, null));
        addBrokerDataEqualityCase(cases, "addresses", brokerData("cluster", "broker", master, false, null),
            brokerData("cluster", "broker", otherMaster, false, null));
        addBrokerDataEqualityCase(cases, "null-map-vs-empty", brokerData(null, "broker", null, false, null),
            brokerData(null, "broker", new HashMap<>(), false, null));
        addBrokerDataEqualityCase(cases, "both-null-map", brokerData("a", null, null, false, "a"),
            brokerData("b", null, null, true, "b"));
        addBrokerDataEqualityCase(cases, "null-address-value", brokerData(null, "broker", withNull, false, null),
            brokerData("other", "broker", withNull, true, "other"));
        return cases;
    }

    private static void addBrokerDataEqualityCase(List<Object> cases, String id,
        org.apache.rocketmq.remoting.protocol.route.BrokerData left,
        org.apache.rocketmq.remoting.protocol.route.BrokerData right) {
        Map<String, Object> item = new TreeMap<>();
        item.put("id", id);
        item.put("left", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(left,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        item.put("right", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(right,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        item.put("equals", left.equals(right));
        item.put("hashEqual", left.hashCode() == right.hashCode());
        cases.add(item);
    }

    private static List<Object> brokerDataHashCodeCases() {
        HashMap<Long, String> addresses = new HashMap<>();
        addresses.put(Long.MIN_VALUE, "\uD800\uDC00");
        addresses.put(Long.MAX_VALUE, null);
        addresses.put(0L, "\uD83D\uDE00");
        List<Object> cases = new ArrayList<>();
        addBrokerDataHashCodeCase(cases, "null-fields", brokerData(null, null, null, false, null));
        addBrokerDataHashCodeCase(cases, "unicode-name", brokerData(null, "\uD800\uDC00-name",
            new HashMap<>(), true, "ignored"));
        addBrokerDataHashCodeCase(cases, "long-keys-unicode-values",
            brokerData("ignored-cluster", "broker", addresses, false, "ignored-zone"));
        return cases;
    }

    private static List<Object> brokerDataToStringCases() {
        HashMap<Long, String> addresses = new HashMap<>();
        addresses.put(2L, "slave:10911");
        addresses.put(0L, "master:10911");
        addresses.put(1L, null);
        List<Object> cases = new ArrayList<>();
        addBrokerDataToStringCase(cases, brokerData(null, null, null, false, null));
        addBrokerDataToStringCase(cases, brokerData("cluster", "broker", addresses, true, "zone"));
        return cases;
    }

    private static void addBrokerDataToStringCase(List<Object> cases,
        org.apache.rocketmq.remoting.protocol.route.BrokerData data) {
        Map<String, Object> item = new TreeMap<>();
        item.put("input", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(data,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        item.put("toString", data.toString());
        cases.add(item);
    }

    private static void addBrokerDataHashCodeCase(List<Object> cases, String id,
        org.apache.rocketmq.remoting.protocol.route.BrokerData data) {
        Map<String, Object> item = new TreeMap<>();
        item.put("id", id);
        item.put("input", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(data,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        item.put("hashCode", data.hashCode());
        cases.add(item);
    }

    private static org.apache.rocketmq.remoting.protocol.route.QueueData queueData(
        String brokerName, int read, int write, int perm, int flag) {
        org.apache.rocketmq.remoting.protocol.route.QueueData data =
            new org.apache.rocketmq.remoting.protocol.route.QueueData();
        data.setBrokerName(brokerName);
        data.setReadQueueNums(read);
        data.setWriteQueueNums(write);
        data.setPerm(perm);
        data.setTopicSysFlag(flag);
        return data;
    }

    private static List<Object> queueDataMethodCases() {
        org.apache.rocketmq.remoting.protocol.route.QueueData[][] pairs = new org.apache.rocketmq.remoting.protocol.route.QueueData[][] {
            {queueData(null, 0, 0, 0, 0), queueData(null, 0, 0, 0, 0)},
            {queueData("a", 1, 2, 4, 8), queueData("c", 1, 2, 4, 8)},
            {queueData("a", 1, 2, 4, 8), queueData("abcd", 1, 2, 4, 8)},
            {queueData("\uD800\uDC00", 1, 2, 4, 8), queueData("\uE000", 1, 2, 4, 8)},
            {queueData("same", 1, 2, 4, 8), queueData("same", 9, 8, 7, 6)}
        };
        List<Object> cases = new ArrayList<>();
        for (org.apache.rocketmq.remoting.protocol.route.QueueData[] pair : pairs) {
            Map<String, Object> item = new TreeMap<>();
            item.put("left", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(pair[0],
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue)));
            item.put("right", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(pair[1],
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue)));
            item.put("equals", pair[0].equals(pair[1]));
            item.put("leftHashCode", pair[0].hashCode());
            item.put("rightHashCode", pair[1].hashCode());
            item.put("leftToString", pair[0].toString());
            item.put("rightToString", pair[1].toString());
            try {
                item.put("result", pair[0].compareTo(pair[1]));
                item.put("outcome", "OK");
            } catch (NullPointerException error) {
                item.put("outcome", "NullPointerException");
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> topicQueueMappingMethodsCases() {
        String populated = "{\"topic\":\"topic\",\"scope\":\"__global__\",\"totalQueues\":3,"
            + "\"bname\":\"broker\",\"epoch\":4,\"dirty\":false,\"currIdMap\":{\"0\":0,\"1\":2,\"2\":4}}";
        String[][] pairs = new String[][] {
            {"{}", "{}"},
            {"{\"topic\":null,\"scope\":null,\"totalQueues\":0,\"bname\":null,\"epoch\":0,\"dirty\":false,\"currIdMap\":null}",
                "{\"topic\":null,\"scope\":null,\"totalQueues\":0,\"bname\":null,\"epoch\":0,\"dirty\":false,\"currIdMap\":{}}"},
            {populated, populated},
            {populated, populated.replace("\"dirty\":false", "\"dirty\":true")}
        };
        List<Object> cases = new ArrayList<>();
        for (String[] pair : pairs) {
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo left =
                com.alibaba.fastjson2.JSON.parseObject(pair[0],
                    org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo.class);
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo right =
                com.alibaba.fastjson2.JSON.parseObject(pair[1],
                    org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("left", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(left,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
            item.put("right", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(right,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
            item.put("equals", left.equals(right));
            item.put("leftHashCode", left.hashCode());
            item.put("rightHashCode", right.hashCode());
            item.put("leftToString", left.toString());
            item.put("rightToString", right.toString());
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> topicQueueMappingConstants() {
        Map<String, Object> constants = new TreeMap<>();
        constants.put("infoLevel0",
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo.LEVEL_0);
        constants.put("detailLevel0",
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.LEVEL_0);
        return constants;
    }

    private static Map<String, Object> logicQueueMappingItemCase(
            String name,
            org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem item,
            long[] physicalOffsets,
            long[] staticOffsets) {
        Map<String, Object> result = new TreeMap<>();
        result.put("case", name);
        result.put("value", com.alibaba.fastjson2.JSON.parseObject(
            com.alibaba.fastjson2.JSON.toJSONString(item,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        Map<String, Object> getters = new TreeMap<>();
        getters.put("gen", item.getGen());
        getters.put("queueId", item.getQueueId());
        getters.put("bname", item.getBname());
        getters.put("logicOffset", item.getLogicOffset());
        getters.put("startOffset", item.getStartOffset());
        getters.put("endOffset", item.getEndOffset());
        getters.put("timeOfStart", item.getTimeOfStart());
        getters.put("timeOfEnd", item.getTimeOfEnd());
        result.put("getters", getters);
        List<Object> conversions = new ArrayList<>();
        for (long physicalOffset : physicalOffsets) {
            Map<String, Object> conversion = new TreeMap<>();
            conversion.put("physicalOffset", physicalOffset);
            conversion.put("looseStaticOffset",
                item.computeStaticQueueOffsetLoosely(physicalOffset));
            conversion.put("strictStaticOffset",
                item.computeStaticQueueOffsetStrictly(physicalOffset));
            conversions.add(conversion);
        }
        for (long staticOffset : staticOffsets) {
            Map<String, Object> conversion = new TreeMap<>();
            conversion.put("staticOffset", staticOffset);
            conversion.put("physicalOffset", item.computePhysicalQueueOffset(staticOffset));
            conversions.add(conversion);
        }
        result.put("conversions", conversions);
        result.put("maxStaticQueueOffset", item.computeMaxStaticQueueOffset());
        result.put("endOffsetDecided", item.checkIfEndOffsetDecided());
        result.put("logicOffsetDecided", item.checkIfLogicoffsetDecided());
        result.put("offsetDelta", item.computeOffsetDelta());
        result.put("hashCode", item.hashCode());
        result.put("toString", item.toString());
        return result;
    }

    private static Map<String, Object> logicQueueMappingItemSetterCase() {
        org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem item =
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem();
        item.setGen(7);
        item.setQueueId(9);
        item.setBname("setter-broker");
        item.setLogicOffset(-11);
        item.setStartOffset(13);
        item.setEndOffset(17);
        item.setTimeOfStart(19);
        item.setTimeOfEnd(23);
        Map<String, Object> result = new TreeMap<>();
        result.put("value", com.alibaba.fastjson2.JSON.parseObject(
            com.alibaba.fastjson2.JSON.toJSONString(item,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        result.put("generation", item.getGen());
        result.put("queueId", item.getQueueId());
        result.put("bname", item.getBname());
        result.put("logicOffset", item.getLogicOffset());
        result.put("startOffset", item.getStartOffset());
        result.put("endOffset", item.getEndOffset());
        result.put("timeOfStart", item.getTimeOfStart());
        result.put("timeOfEnd", item.getTimeOfEnd());
        return result;
    }

    private static Map<String, Object> logicQueueMappingItemEqualityPair(
            String name,
            org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem left,
            org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem right) {
        Map<String, Object> result = new TreeMap<>();
        result.put("case", name);
        result.put("left", com.alibaba.fastjson2.JSON.parseObject(
            com.alibaba.fastjson2.JSON.toJSONString(left,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        result.put("right", com.alibaba.fastjson2.JSON.parseObject(
            com.alibaba.fastjson2.JSON.toJSONString(right,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        result.put("equals", left.equals(right));
        return result;
    }

    private static List<Object> logicQueueMappingItemEqualityCases() {
        org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem base =
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                1, 3, "broker", 10, 2, 8, 100, 200);
        List<Object> cases = new ArrayList<>();
        cases.add(logicQueueMappingItemEqualityPair("default",
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(),
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem()));
        cases.add(logicQueueMappingItemEqualityPair("same-populated", base,
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                1, 3, "broker", 10, 2, 8, 100, 200)));
        cases.add(logicQueueMappingItemEqualityPair("generation", base,
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                2, 3, "broker", 10, 2, 8, 100, 200)));
        cases.add(logicQueueMappingItemEqualityPair("queueId", base,
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                1, 4, "broker", 10, 2, 8, 100, 200)));
        cases.add(logicQueueMappingItemEqualityPair("bname", base,
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                1, 3, "other", 10, 2, 8, 100, 200)));
        cases.add(logicQueueMappingItemEqualityPair("logicOffset", base,
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                1, 3, "broker", 11, 2, 8, 100, 200)));
        cases.add(logicQueueMappingItemEqualityPair("startOffset", base,
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                1, 3, "broker", 10, 3, 8, 100, 200)));
        cases.add(logicQueueMappingItemEqualityPair("endOffset", base,
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                1, 3, "broker", 10, 2, 9, 100, 200)));
        cases.add(logicQueueMappingItemEqualityPair("timeOfStart", base,
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                1, 3, "broker", 10, 2, 8, 101, 200)));
        cases.add(logicQueueMappingItemEqualityPair("timeOfEnd", base,
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                1, 3, "broker", 10, 2, 8, 100, 201)));
        cases.add(logicQueueMappingItemEqualityPair("null-vs-empty-bname",
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                1, 3, null, 10, 2, 8, 100, 200),
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                1, 3, "", 10, 2, 8, 100, 200)));
        return cases;
    }

    private static List<Object> logicQueueMappingItemMethodCases() {
        long[] physicalOffsets = new long[] {-1, 0, 1, 2, 3, 8, 9, Long.MAX_VALUE};
        long[] staticOffsets = new long[] {-1, 0, 10, Long.MAX_VALUE};
        List<Object> cases = new ArrayList<>();
        cases.add(logicQueueMappingItemCase("default",
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(),
            physicalOffsets, staticOffsets));
        cases.add(logicQueueMappingItemCase("populated",
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                1, 3, "broker", 10, 2, 8, 100, 200),
            physicalOffsets, staticOffsets));
        cases.add(logicQueueMappingItemCase("undecided",
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                2, 4, null, -5, 3, -1, -1, -1),
            physicalOffsets, staticOffsets));
        return cases;
    }

    private static List<Object> topicQueueMappingDetailMethodCases() {
        String mappingItem = "{\"gen\":1,\"queueId\":3,\"bname\":\"broker\","
            + "\"logicOffset\":10,\"startOffset\":0,\"endOffset\":-1,"
            + "\"timeOfStart\":-1,\"timeOfEnd\":-1}";
        String changedMappingItem = mappingItem.replace("\"logicOffset\":10", "\"logicOffset\":11");
        String[][] pairs = new String[][] {
            {"defaults", "{}", "{}"},
            {"metadata-ignored", "{\"topic\":\"left\",\"totalQueues\":1,\"bname\":\"left\",\"hostedQueues\":{}}",
                "{\"topic\":\"right\",\"totalQueues\":9,\"bname\":\"right\",\"hostedQueues\":{}}"},
            {"hosted-equal", "{\"topic\":\"left\",\"hostedQueues\":{\"0\":[" + mappingItem + "]}}",
                "{\"topic\":\"right\",\"hostedQueues\":{\"0\":[" + mappingItem + "]}}"},
            {"hosted-different", "{\"hostedQueues\":{\"0\":[" + mappingItem + "]}}",
                "{\"hostedQueues\":{\"0\":[" + changedMappingItem + "]}}"},
            {"null-hosted-equal", "{\"topic\":\"left\",\"hostedQueues\":null}",
                "{\"topic\":\"right\",\"hostedQueues\":null}"},
            {"null-versus-empty", "{\"hostedQueues\":null}", "{\"hostedQueues\":{}}"}
        };
        List<Object> cases = new ArrayList<>();
        for (String[] pair : pairs) {
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail left =
                com.alibaba.fastjson2.JSON.parseObject(pair[1],
                    org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.class);
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail right =
                com.alibaba.fastjson2.JSON.parseObject(pair[2],
                    org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("case", pair[0]);
            item.put("left", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(left,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
            item.put("right", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(right,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
            item.put("equals", left.equals(right));
            item.put("leftHashCode", left.hashCode());
            item.put("rightHashCode", right.hashCode());
            item.put("leftToString", left.toString());
            item.put("rightToString", right.toString());
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> topicQueueMappingDetailOperationCase() {
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail detail =
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail(
                "topic", 10, "broker", 4);
        List<org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem> items =
            new ArrayList<>();
        items.add(new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
            1, 4, "broker", 0, 0, 6, -1, -1));
        boolean inserted = org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail
            .putMappingInfo(detail, 7, items);
        boolean emptyPut = org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail
            .putMappingInfo(null, null, java.util.Collections.emptyList());
        List<org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem> returned =
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail
                .getMappingInfo(detail, 7);
        items.add(new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
            2, 5, "broker", 6, 0, -1, -1, -1));
        List<org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem> single =
            new ArrayList<>();
        single.add(new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
            1, 2, "broker", 0, 0, 5, -1, -1));
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.putMappingInfo(
            detail, 8, single);
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo clone =
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail
                .cloneAsMappingInfo(detail);
        Map<String, Object> result = new TreeMap<>();
        result.put("inserted", inserted);
        result.put("emptyPutWithNullDetail", emptyPut);
        result.put("returnedSameList", returned == items);
        result.put("returnedSizeAfterExternalAdd", returned.size());
        result.put("missingMapping", org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail
            .getMappingInfo(detail, 99) == null);
        Map<String, Integer> currentIdMap = new TreeMap<>();
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail
            .buildIdMap(detail, 0).forEach((key, value) -> currentIdMap.put(key.toString(), value));
        result.put("currentIdMap", currentIdMap);
        result.put("otherLevelIdMap", org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail
            .buildIdMap(detail, 1));
        result.put("maxOffset", org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail
            .computeMaxOffsetFromMapping(detail, 7));
        result.put("emptyMaxOffset", org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail
            .computeMaxOffsetFromMapping(detail, 99));
        result.put("physicalSingle", org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail
            .checkIfAsPhysical(detail, 8));
        result.put("physicalMultiple", org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail
            .checkIfAsPhysical(detail, 7));
        result.put("physicalMissing", org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail
            .checkIfAsPhysical(detail, 99));
        result.put("clone", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(clone,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        return result;
    }

    private static List<Object> topicQueueMappingDetailNullOperationCases() {
        String nullItemInput = "{\"hostedQueues\":{\"0\":[null]}}";
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail nullItem =
            com.alibaba.fastjson2.JSON.parseObject(nullItemInput,
                org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.class);
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail nullQueues =
            com.alibaba.fastjson2.JSON.parseObject("{\"hostedQueues\":null}",
                org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.class);
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail valid =
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail(
                "topic", 1, "broker", 1);
        List<org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem> itemList =
            new ArrayList<>();
        itemList.add(new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
            1, 0, "broker", 0, 0, 1, -1, -1));
        List<Object[]> operations = new ArrayList<>();
        operations.add(new Object[] {"put-null-list", (Runnable) () ->
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.putMappingInfo(
                valid, 0, null)});
        operations.add(new Object[] {"put-null-detail", (Runnable) () ->
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.putMappingInfo(
                null, 0, itemList)});
        operations.add(new Object[] {"put-null-id", (Runnable) () ->
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.putMappingInfo(
                valid, null, itemList)});
        operations.add(new Object[] {"get-null-detail", (Runnable) () ->
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.getMappingInfo(
                null, 0)});
        operations.add(new Object[] {"get-null-hosted-queues", (Runnable) () ->
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.getMappingInfo(
                nullQueues, 0)});
        operations.add(new Object[] {"get-null-id", (Runnable) () ->
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.getMappingInfo(
                valid, null)});
        operations.add(new Object[] {"build-null-detail", (Runnable) () ->
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.buildIdMap(
                null, 0)});
        operations.add(new Object[] {"build-null-item", (Runnable) () ->
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.buildIdMap(
                nullItem, 0)});
        operations.add(new Object[] {"compute-null-hosted-queues", (Runnable) () ->
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.computeMaxOffsetFromMapping(
                nullQueues, 0)});
        operations.add(new Object[] {"compute-null-item", (Runnable) () ->
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.computeMaxOffsetFromMapping(
                nullItem, 0)});
        operations.add(new Object[] {"check-null-item", (Runnable) () ->
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.checkIfAsPhysical(
                nullItem, 0)});
        operations.add(new Object[] {"clone-null-detail", (Runnable) () ->
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.cloneAsMappingInfo(
                null)});
        List<Object> cases = new ArrayList<>();
        for (Object[] operation : operations) {
            Map<String, Object> item = new TreeMap<>();
            item.put("operation", operation[0]);
            try {
                ((Runnable) operation[1]).run();
                item.put("outcome", "OK");
            } catch (Throwable error) {
                item.put("outcome", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> topicRouteDataMethodCases() {
        String populated = "{\"orderTopicConf\":\"broker:2\",\"queueDatas\":[{\"brokerName\":\"broker\","
            + "\"readQueueNums\":2,\"writeQueueNums\":2,\"perm\":6,\"topicSysFlag\":1}],"
            + "\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\","
            + "\"brokerAddrs\":{\"0\":\"master:10911\"},\"zoneName\":\"zone\",\"enableActingMaster\":false}],"
            + "\"filterServerTable\":{\"broker\":[\"filter-a\",\"filter-b\"]},"
            + "\"topicQueueMappingByBroker\":{\"broker\":{\"topic\":\"topic\",\"scope\":\"__global__\","
            + "\"totalQueues\":2,\"bname\":\"broker\",\"epoch\":1,\"dirty\":false,\"currIdMap\":{\"0\":0,\"1\":1}}}}";
        String metadataOnly = populated.replace("\"cluster\":\"cluster\"", "\"cluster\":\"other\"")
            .replace("\"zoneName\":\"zone\"", "\"zoneName\":\"other\"")
            .replace("\"enableActingMaster\":false", "\"enableActingMaster\":true");
        String differentQueue = populated.replace("\"perm\":6", "\"perm\":4");
        String nullCollections = "{\"queueDatas\":null,\"brokerDatas\":null,\"filterServerTable\":null,"
            + "\"topicQueueMappingByBroker\":null}";
        String emptyCollections = "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":{},"
            + "\"topicQueueMappingByBroker\":{}}";
        String nullElements = "{\"queueDatas\":[null],\"brokerDatas\":[null],"
            + "\"filterServerTable\":{\"broker\":[\"filter-a\",null]},"
            + "\"topicQueueMappingByBroker\":{\"broker\":null}}";
        String nullFilterList = "{\"filterServerTable\":{\"broker\":null}}";
        String[][] pairs = new String[][] {
            {"{}", "{}"},
            {nullCollections, emptyCollections},
            {populated, metadataOnly},
            {populated, differentQueue},
            {nullElements, nullElements},
            {nullElements, emptyCollections},
            {nullFilterList, nullFilterList}
        };
        List<Object> cases = new ArrayList<>();
        for (String[] pair : pairs) {
            org.apache.rocketmq.remoting.protocol.route.TopicRouteData left =
                com.alibaba.fastjson2.JSON.parseObject(pair[0],
                    org.apache.rocketmq.remoting.protocol.route.TopicRouteData.class);
            org.apache.rocketmq.remoting.protocol.route.TopicRouteData right =
                com.alibaba.fastjson2.JSON.parseObject(pair[1],
                    org.apache.rocketmq.remoting.protocol.route.TopicRouteData.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("left", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(left,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
            item.put("right", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(right,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
            item.put("equals", left.equals(right));
            item.put("leftHashCode", left.hashCode());
            item.put("rightHashCode", right.hashCode());
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> routeDtoAccessorCases() {
        Map<String, Object> result = new TreeMap<>();

        org.apache.rocketmq.remoting.protocol.route.BrokerData broker =
            new org.apache.rocketmq.remoting.protocol.route.BrokerData();
        HashMap<Long, String> addresses = new HashMap<>();
        addresses.put(0L, "master:10911");
        addresses.put(2L, null);
        broker.setCluster("cluster");
        broker.setBrokerName("broker");
        broker.setBrokerAddrs(addresses);
        broker.setZoneName("zone");
        broker.setEnableActingMaster(true);
        Map<String, Object> brokerValues = new TreeMap<>();
        brokerValues.put("cluster", broker.getCluster());
        brokerValues.put("brokerName", broker.getBrokerName());
        Map<String, Object> brokerAddressValues = new TreeMap<>();
        if (broker.getBrokerAddrs() != null) {
            broker.getBrokerAddrs().forEach((key, value) -> brokerAddressValues.put(String.valueOf(key), value));
        }
        brokerValues.put("brokerAddrs", broker.getBrokerAddrs() == null ? null : brokerAddressValues);
        brokerValues.put("zoneName", broker.getZoneName());
        brokerValues.put("enableActingMaster", broker.isEnableActingMaster());
        result.put("brokerData", brokerValues);
        broker.setCluster(null);
        broker.setBrokerName(null);
        broker.setBrokerAddrs(null);
        broker.setZoneName(null);
        broker.setEnableActingMaster(false);
        Map<String, Object> nullBrokerValues = new TreeMap<>();
        nullBrokerValues.put("cluster", broker.getCluster());
        nullBrokerValues.put("brokerName", broker.getBrokerName());
        nullBrokerValues.put("brokerAddrs", broker.getBrokerAddrs());
        nullBrokerValues.put("zoneName", broker.getZoneName());
        nullBrokerValues.put("enableActingMaster", broker.isEnableActingMaster());
        result.put("brokerDataNulls", nullBrokerValues);

        org.apache.rocketmq.remoting.protocol.route.QueueData queue =
            new org.apache.rocketmq.remoting.protocol.route.QueueData();
        queue.setBrokerName("queue-broker");
        queue.setReadQueueNums(3);
        queue.setWriteQueueNums(5);
        queue.setPerm(6);
        queue.setTopicSysFlag(9);
        Map<String, Object> queueValues = new TreeMap<>();
        queueValues.put("brokerName", queue.getBrokerName());
        queueValues.put("readQueueNums", queue.getReadQueueNums());
        queueValues.put("writeQueueNums", queue.getWriteQueueNums());
        queueValues.put("perm", queue.getPerm());
        queueValues.put("topicSysFlag", queue.getTopicSysFlag());
        result.put("queueData", queueValues);
        queue.setBrokerName(null);
        queue.setReadQueueNums(0);
        queue.setWriteQueueNums(0);
        queue.setPerm(0);
        queue.setTopicSysFlag(0);
        Map<String, Object> nullQueueValues = new TreeMap<>();
        nullQueueValues.put("brokerName", queue.getBrokerName());
        nullQueueValues.put("readQueueNums", queue.getReadQueueNums());
        nullQueueValues.put("writeQueueNums", queue.getWriteQueueNums());
        nullQueueValues.put("perm", queue.getPerm());
        nullQueueValues.put("topicSysFlag", queue.getTopicSysFlag());
        result.put("queueDataNulls", nullQueueValues);

        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo mapping =
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo();
        java.util.concurrent.ConcurrentMap<Integer, Integer> ids = new java.util.concurrent.ConcurrentHashMap<>();
        ids.put(0, 1);
        ids.put(2, 4);
        mapping.setTopic("topic");
        mapping.setScope("__global__");
        mapping.setTotalQueues(5);
        mapping.setBname("mapping-broker");
        mapping.setEpoch(7L);
        mapping.setDirty(true);
        mapping.setCurrIdMap(ids);
        Map<String, Object> mappingValues = new TreeMap<>();
        mappingValues.put("topic", mapping.getTopic());
        mappingValues.put("scope", mapping.getScope());
        mappingValues.put("totalQueues", mapping.getTotalQueues());
        mappingValues.put("bname", mapping.getBname());
        mappingValues.put("epoch", mapping.getEpoch());
        mappingValues.put("dirty", mapping.isDirty());
        Map<String, Object> mappingIdValues = new TreeMap<>();
        if (mapping.getCurrIdMap() != null) {
            mapping.getCurrIdMap().forEach((key, value) -> mappingIdValues.put(String.valueOf(key), value));
        }
        mappingValues.put("currIdMap", mapping.getCurrIdMap() == null ? null : mappingIdValues);
        result.put("topicQueueMappingInfo", mappingValues);
        mapping.setTopic(null);
        mapping.setScope(null);
        mapping.setTotalQueues(0);
        mapping.setBname(null);
        mapping.setEpoch(0L);
        mapping.setDirty(false);
        mapping.setCurrIdMap(null);
        Map<String, Object> nullMappingValues = new TreeMap<>();
        nullMappingValues.put("topic", mapping.getTopic());
        nullMappingValues.put("scope", mapping.getScope());
        nullMappingValues.put("totalQueues", mapping.getTotalQueues());
        nullMappingValues.put("bname", mapping.getBname());
        nullMappingValues.put("epoch", mapping.getEpoch());
        nullMappingValues.put("dirty", mapping.isDirty());
        nullMappingValues.put("currIdMap", mapping.getCurrIdMap());
        result.put("topicQueueMappingInfoNulls", nullMappingValues);

        org.apache.rocketmq.remoting.protocol.route.TopicRouteData route =
            new org.apache.rocketmq.remoting.protocol.route.TopicRouteData();
        org.apache.rocketmq.remoting.protocol.route.BrokerData routeBroker =
            new org.apache.rocketmq.remoting.protocol.route.BrokerData();
        routeBroker.setBrokerName("route-broker");
        Map<String, List<String>> filters = new HashMap<>();
        filters.put("broker", List.of("filter-a", "filter-b"));
        Map<String, org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo> mappings = new HashMap<>();
        mappings.put("mapping-broker", new org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo(
            "topic", 5, "mapping-broker", 7L));
        route.setOrderTopicConf("mapping-broker:5");
        route.setQueueDatas(List.of(queueData("queue-broker", 3, 5, 6, 9)));
        route.setBrokerDatas(List.of(routeBroker));
        route.setFilterServerTable(new HashMap<>(filters));
        route.setTopicQueueMappingByBroker(mappings);
        Map<String, Object> routeValues = new TreeMap<>();
        routeValues.put("orderTopicConf", route.getOrderTopicConf());
        routeValues.put("queueDatas", route.getQueueDatas() == null ? null : route.getQueueDatas().stream()
            .map(value -> value == null ? null : value.getBrokerName()).collect(java.util.stream.Collectors.toList()));
        routeValues.put("brokerDatas", route.getBrokerDatas() == null ? null : route.getBrokerDatas().stream()
            .map(value -> value == null ? null : value.getBrokerName()).collect(java.util.stream.Collectors.toList()));
        routeValues.put("filterServerTable", route.getFilterServerTable());
        routeValues.put("topicQueueMappingByBroker", route.getTopicQueueMappingByBroker() == null ? null
            : new ArrayList<>(new TreeSet<>(route.getTopicQueueMappingByBroker().keySet())));
        result.put("topicRouteData", routeValues);
        route.setOrderTopicConf(null);
        route.setQueueDatas(null);
        route.setBrokerDatas(null);
        route.setFilterServerTable(null);
        route.setTopicQueueMappingByBroker(null);
        Map<String, Object> nullRouteValues = new TreeMap<>();
        nullRouteValues.put("orderTopicConf", route.getOrderTopicConf());
        nullRouteValues.put("queueDatas", route.getQueueDatas());
        nullRouteValues.put("brokerDatas", route.getBrokerDatas());
        nullRouteValues.put("filterServerTable", route.getFilterServerTable());
        nullRouteValues.put("topicQueueMappingByBroker", route.getTopicQueueMappingByBroker());
        result.put("topicRouteDataNulls", nullRouteValues);
        return result;
    }

    private static Map<String, Object> routeDtoReferenceAliasCases() {
        Map<String, Object> result = new TreeMap<>();

        org.apache.rocketmq.remoting.protocol.route.BrokerData broker =
            new org.apache.rocketmq.remoting.protocol.route.BrokerData();
        HashMap<Long, String> addresses = new HashMap<>();
        addresses.put(0L, "master:10911");
        broker.setBrokerAddrs(addresses);
        result.put("brokerSetterRetainsMap", broker.getBrokerAddrs() == addresses);
        result.put("brokerGetterIsStable", broker.getBrokerAddrs() == broker.getBrokerAddrs());
        addresses.put(2L, "caller-slave:10911");
        result.put("brokerCallerMutationVisible", "caller-slave:10911".equals(
            broker.getBrokerAddrs().get(2L)));
        broker.getBrokerAddrs().put(3L, "getter-slave:10911");
        result.put("brokerGetterMutationVisible", "getter-slave:10911".equals(addresses.get(3L)));

        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo mapping =
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo();
        java.util.concurrent.ConcurrentMap<Integer, Integer> ids =
            new java.util.concurrent.ConcurrentHashMap<>();
        ids.put(0, 4);
        mapping.setCurrIdMap(ids);
        result.put("mappingSetterRetainsMap", mapping.getCurrIdMap() == ids);
        result.put("mappingGetterIsStable", mapping.getCurrIdMap() == mapping.getCurrIdMap());
        ids.put(1, 7);
        result.put("mappingCallerMutationVisible", Integer.valueOf(7).equals(
            mapping.getCurrIdMap().get(1)));
        mapping.getCurrIdMap().put(2, 9);
        result.put("mappingGetterMutationVisible", Integer.valueOf(9).equals(ids.get(2)));

        org.apache.rocketmq.remoting.protocol.route.TopicRouteData route =
            new org.apache.rocketmq.remoting.protocol.route.TopicRouteData();
        org.apache.rocketmq.remoting.protocol.route.QueueData queue =
            queueData("queue-broker", 1, 2, 6, 0);
        List<org.apache.rocketmq.remoting.protocol.route.QueueData> queues = new ArrayList<>();
        queues.add(queue);
        route.setQueueDatas(queues);
        result.put("routeQueueSetterRetainsList", route.getQueueDatas() == queues);
        result.put("routeQueueGetterIsStable", route.getQueueDatas() == route.getQueueDatas());
        result.put("routeQueueElementIdentityRetained", route.getQueueDatas().get(0) == queue);
        queues.add(queueData("second-queue-broker", 1, 1, 6, 0));
        result.put("routeQueueCallerMutationVisible", route.getQueueDatas().size() == 2);
        org.apache.rocketmq.remoting.protocol.route.QueueData getterQueue = route.getQueueDatas().get(0);
        getterQueue.setPerm(5);
        result.put("routeQueueGetterElementMutationVisible", queue.getPerm() == 5);
        route.getQueueDatas().add(queueData("getter-queue-broker", 1, 1, 6, 0));
        result.put("routeQueueGetterMutationVisible", queues.size() == 3);

        org.apache.rocketmq.remoting.protocol.route.BrokerData routeBroker =
            new org.apache.rocketmq.remoting.protocol.route.BrokerData();
        routeBroker.setBrokerName("route-broker");
        List<org.apache.rocketmq.remoting.protocol.route.BrokerData> brokers = new ArrayList<>();
        brokers.add(routeBroker);
        route.setBrokerDatas(brokers);
        result.put("routeBrokerSetterRetainsList", route.getBrokerDatas() == brokers);
        result.put("routeBrokerGetterIsStable", route.getBrokerDatas() == route.getBrokerDatas());
        result.put("routeBrokerElementIdentityRetained", route.getBrokerDatas().get(0) == routeBroker);
        brokers.add(new org.apache.rocketmq.remoting.protocol.route.BrokerData());
        result.put("routeBrokerCallerMutationVisible", route.getBrokerDatas().size() == 2);
        route.getBrokerDatas().get(0).setBrokerName("getter-broker");
        result.put("routeBrokerGetterElementMutationVisible", "getter-broker".equals(routeBroker.getBrokerName()));
        route.getBrokerDatas().add(new org.apache.rocketmq.remoting.protocol.route.BrokerData());
        result.put("routeBrokerGetterMutationVisible", brokers.size() == 3);

        List<String> brokerFilters = new ArrayList<>();
        brokerFilters.add("filter-a");
        HashMap<String, List<String>> filters = new HashMap<>();
        filters.put("filter-broker", brokerFilters);
        route.setFilterServerTable(filters);
        result.put("routeFilterSetterRetainsMap", route.getFilterServerTable() == filters);
        result.put("routeFilterGetterIsStable", route.getFilterServerTable() == route.getFilterServerTable());
        result.put("routeFilterNestedListIdentityRetained",
            route.getFilterServerTable().get("filter-broker") == brokerFilters);
        brokerFilters.add("caller-filter");
        result.put("routeFilterCallerMutationVisible",
            route.getFilterServerTable().get("filter-broker").size() == 2);
        route.getFilterServerTable().put("getter-broker", new ArrayList<>(List.of("getter-filter")));
        result.put("routeFilterGetterMutationVisible", filters.containsKey("getter-broker"));
        route.getFilterServerTable().get("filter-broker").add("getter-filter");
        result.put("routeFilterGetterNestedMutationVisible", brokerFilters.size() == 3);

        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo routeMapping =
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo(
                "route-topic", 2, "mapping-broker", 4L);
        Map<String, org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo> routeMappings =
            new HashMap<>();
        routeMappings.put("mapping-broker", routeMapping);
        route.setTopicQueueMappingByBroker(routeMappings);
        result.put("routeMappingSetterRetainsMap", route.getTopicQueueMappingByBroker() == routeMappings);
        result.put("routeMappingGetterIsStable",
            route.getTopicQueueMappingByBroker() == route.getTopicQueueMappingByBroker());
        result.put("routeMappingElementIdentityRetained",
            route.getTopicQueueMappingByBroker().get("mapping-broker") == routeMapping);
        routeMappings.put("caller-broker",
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo());
        result.put("routeMappingCallerMutationVisible", route.getTopicQueueMappingByBroker().size() == 2);
        route.getTopicQueueMappingByBroker().get("mapping-broker").setDirty(true);
        result.put("routeMappingGetterElementMutationVisible", routeMapping.isDirty());
        route.getTopicQueueMappingByBroker().put("getter-broker",
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo());
        result.put("routeMappingGetterMutationVisible", routeMappings.size() == 3);
        return result;
    }

    private static Map<String, Object> clusterInfoAccessorCases() {
        Map<String, Object> result = new TreeMap<>();
        List<String> publicMethods = new ArrayList<>();
        for (java.lang.reflect.Method method : org.apache.rocketmq.remoting.protocol.body.ClusterInfo.class
            .getDeclaredMethods()) {
            if (java.lang.reflect.Modifier.isPublic(method.getModifiers())) {
                publicMethods.add(method.toGenericString());
            }
        }
        publicMethods.sort(String::compareTo);
        result.put("declaredPublicMethods", publicMethods);

        org.apache.rocketmq.remoting.protocol.body.ClusterInfo info =
            new org.apache.rocketmq.remoting.protocol.body.ClusterInfo();
        result.put("defaultBrokerAddrTableIsNull", info.getBrokerAddrTable() == null);
        result.put("defaultClusterAddrTableIsNull", info.getClusterAddrTable() == null);

        HashMap<String, org.apache.rocketmq.remoting.protocol.route.BrokerData> brokerTable = new HashMap<>();
        org.apache.rocketmq.remoting.protocol.route.BrokerData broker =
            new org.apache.rocketmq.remoting.protocol.route.BrokerData();
        HashMap<Long, String> brokerAddresses = new HashMap<>();
        brokerAddresses.put(0L, "master:10911");
        broker.setBrokerName("broker-a");
        broker.setBrokerAddrs(brokerAddresses);
        brokerTable.put("broker-a", broker);
        HashMap<String, java.util.Set<String>> clusterTable = new HashMap<>();
        clusterTable.put("cluster-a", new java.util.HashSet<>(java.util.Set.of("broker-a")));
        info.setBrokerAddrTable(brokerTable);
        info.setClusterAddrTable(clusterTable);
        result.put("brokerSetterRetainsMap", info.getBrokerAddrTable() == brokerTable);
        result.put("clusterSetterRetainsMap", info.getClusterAddrTable() == clusterTable);
        result.put("brokerGetterReturnsValue", info.getBrokerAddrTable().get("broker-a") == broker);
        result.put("clusterGetterReturnsValue",
            info.getClusterAddrTable().get("cluster-a").contains("broker-a"));
        info.setBrokerAddrTable(null);
        info.setClusterAddrTable(null);
        result.put("brokerSetterPreservesNull", info.getBrokerAddrTable() == null);
        result.put("clusterSetterPreservesNull", info.getClusterAddrTable() == null);
        return result;
    }

    private static Map<String, Object> clusterInfoReferenceAliasCases() {
        Map<String, Object> result = new TreeMap<>();
        org.apache.rocketmq.remoting.protocol.body.ClusterInfo info =
            new org.apache.rocketmq.remoting.protocol.body.ClusterInfo();

        HashMap<String, org.apache.rocketmq.remoting.protocol.route.BrokerData> callerBrokerTable = new HashMap<>();
        org.apache.rocketmq.remoting.protocol.route.BrokerData broker =
            new org.apache.rocketmq.remoting.protocol.route.BrokerData();
        HashMap<Long, String> addresses = new HashMap<>();
        addresses.put(0L, "master:10911");
        broker.setBrokerAddrs(addresses);
        callerBrokerTable.put("broker-a", broker);
        info.setBrokerAddrTable(callerBrokerTable);
        result.put("brokerSetterRetainsMap", info.getBrokerAddrTable() == callerBrokerTable);
        result.put("brokerGetterIsStable", info.getBrokerAddrTable() == info.getBrokerAddrTable());
        result.put("brokerElementIdentityRetained", info.getBrokerAddrTable().get("broker-a") == broker);
        callerBrokerTable.put("caller-broker", new org.apache.rocketmq.remoting.protocol.route.BrokerData());
        result.put("brokerCallerMutationVisible", info.getBrokerAddrTable().size() == 2);
        info.getBrokerAddrTable().put("getter-broker", new org.apache.rocketmq.remoting.protocol.route.BrokerData());
        result.put("brokerGetterMutationVisible", callerBrokerTable.containsKey("getter-broker"));
        broker.setZoneName("caller-zone");
        result.put("brokerElementCallerMutationVisible",
            "caller-zone".equals(info.getBrokerAddrTable().get("broker-a").getZoneName()));
        info.getBrokerAddrTable().get("broker-a").setZoneName("getter-zone");
        result.put("brokerElementGetterMutationVisible", "getter-zone".equals(broker.getZoneName()));

        HashMap<String, java.util.Set<String>> callerClusterTable = new HashMap<>();
        java.util.Set<String> callerBrokerNames = new java.util.HashSet<>();
        callerBrokerNames.add("broker-a");
        callerClusterTable.put("cluster-a", callerBrokerNames);
        info.setClusterAddrTable(callerClusterTable);
        result.put("clusterSetterRetainsMap", info.getClusterAddrTable() == callerClusterTable);
        result.put("clusterGetterIsStable", info.getClusterAddrTable() == info.getClusterAddrTable());
        result.put("clusterSetIdentityRetained", info.getClusterAddrTable().get("cluster-a") == callerBrokerNames);
        callerClusterTable.put("caller-cluster", new java.util.HashSet<>(java.util.Set.of("broker-b")));
        result.put("clusterCallerMutationVisible", info.getClusterAddrTable().size() == 2);
        info.getClusterAddrTable().put("getter-cluster", new java.util.HashSet<>(java.util.Set.of("broker-c")));
        result.put("clusterGetterMutationVisible", callerClusterTable.containsKey("getter-cluster"));
        callerBrokerNames.add("caller-broker");
        result.put("clusterSetCallerMutationVisible",
            info.getClusterAddrTable().get("cluster-a").contains("caller-broker"));
        info.getClusterAddrTable().get("cluster-a").add("getter-broker");
        result.put("clusterSetGetterMutationVisible", callerBrokerNames.contains("getter-broker"));
        return result;
    }

    private static Map<String, Object> clusterInfoRetrievalCases() {
        Map<String, Object> result = new TreeMap<>();
        org.apache.rocketmq.remoting.protocol.body.ClusterInfo empty =
            new org.apache.rocketmq.remoting.protocol.body.ClusterInfo();
        empty.setBrokerAddrTable(new HashMap<>());
        empty.setClusterAddrTable(new HashMap<>());
        result.put("emptyClusterNames", java.util.Arrays.asList(empty.retrieveAllClusterNames()));
        result.put("missingClusterAddresses", java.util.Arrays.asList(empty.retrieveAllAddrByCluster("missing")));

        org.apache.rocketmq.remoting.protocol.body.ClusterInfo populated =
            new org.apache.rocketmq.remoting.protocol.body.ClusterInfo();
        HashMap<String, org.apache.rocketmq.remoting.protocol.route.BrokerData> brokers = new HashMap<>();
        org.apache.rocketmq.remoting.protocol.route.BrokerData broker =
            new org.apache.rocketmq.remoting.protocol.route.BrokerData();
        HashMap<Long, String> addresses = new HashMap<>();
        addresses.put(0L, "master:10911");
        broker.setBrokerAddrs(addresses);
        brokers.put("broker-a", broker);
        populated.setBrokerAddrTable(brokers);
        HashMap<String, java.util.Set<String>> clusters = new HashMap<>();
        clusters.put("cluster-a", new java.util.HashSet<>(java.util.Set.of("broker-a")));
        populated.setClusterAddrTable(clusters);
        result.put("clusterNames", java.util.Arrays.asList(populated.retrieveAllClusterNames()));
        result.put("clusterAddresses", java.util.Arrays.asList(populated.retrieveAllAddrByCluster("cluster-a")));

        org.apache.rocketmq.remoting.protocol.body.ClusterInfo missingBroker =
            new org.apache.rocketmq.remoting.protocol.body.ClusterInfo();
        missingBroker.setBrokerAddrTable(new HashMap<>());
        HashMap<String, java.util.Set<String>> missingBrokerClusters = new HashMap<>();
        missingBrokerClusters.put("cluster-a", new java.util.HashSet<>(java.util.Set.of("missing")));
        missingBroker.setClusterAddrTable(missingBrokerClusters);
        result.put("missingBrokerAddresses", java.util.Arrays.asList(missingBroker.retrieveAllAddrByCluster("cluster-a")));

        org.apache.rocketmq.remoting.protocol.body.ClusterInfo nullBroker =
            new org.apache.rocketmq.remoting.protocol.body.ClusterInfo();
        HashMap<String, org.apache.rocketmq.remoting.protocol.route.BrokerData> nullBrokerTable = new HashMap<>();
        nullBrokerTable.put("broker-a", null);
        nullBroker.setBrokerAddrTable(nullBrokerTable);
        HashMap<String, java.util.Set<String>> nullBrokerClusters = new HashMap<>();
        nullBrokerClusters.put("cluster-a", new java.util.HashSet<>(java.util.Set.of("broker-a")));
        nullBroker.setClusterAddrTable(nullBrokerClusters);
        result.put("nullBrokerDataAddresses", java.util.Arrays.asList(nullBroker.retrieveAllAddrByCluster("cluster-a")));

        org.apache.rocketmq.remoting.protocol.body.ClusterInfo nullAddress =
            new org.apache.rocketmq.remoting.protocol.body.ClusterInfo();
        org.apache.rocketmq.remoting.protocol.route.BrokerData nullAddressBroker =
            new org.apache.rocketmq.remoting.protocol.route.BrokerData();
        HashMap<Long, String> nullAddressMap = new HashMap<>();
        nullAddressMap.put(0L, null);
        nullAddressBroker.setBrokerAddrs(nullAddressMap);
        HashMap<String, org.apache.rocketmq.remoting.protocol.route.BrokerData> nullAddressBrokers = new HashMap<>();
        nullAddressBrokers.put("broker-a", nullAddressBroker);
        nullAddress.setBrokerAddrTable(nullAddressBrokers);
        HashMap<String, java.util.Set<String>> nullAddressClusters = new HashMap<>();
        nullAddressClusters.put("cluster-a", new java.util.HashSet<>(java.util.Set.of("broker-a")));
        nullAddress.setClusterAddrTable(nullAddressClusters);
        result.put("nullBrokerAddressValue", java.util.Arrays.asList(nullAddress.retrieveAllAddrByCluster("cluster-a")));

        org.apache.rocketmq.remoting.protocol.body.ClusterInfo nullKeys =
            new org.apache.rocketmq.remoting.protocol.body.ClusterInfo();
        org.apache.rocketmq.remoting.protocol.route.BrokerData nullKeyBroker =
            new org.apache.rocketmq.remoting.protocol.route.BrokerData();
        HashMap<Long, String> nullKeyAddresses = new HashMap<>();
        nullKeyAddresses.put(0L, "null-key:10911");
        nullKeyBroker.setBrokerAddrs(nullKeyAddresses);
        HashMap<String, org.apache.rocketmq.remoting.protocol.route.BrokerData> nullKeyBrokers = new HashMap<>();
        nullKeyBrokers.put(null, nullKeyBroker);
        nullKeys.setBrokerAddrTable(nullKeyBrokers);
        HashMap<String, java.util.Set<String>> nullKeyClusters = new HashMap<>();
        java.util.Set<String> nullBrokerNames = new java.util.HashSet<>();
        nullBrokerNames.add(null);
        nullKeyClusters.put(null, nullBrokerNames);
        nullKeys.setClusterAddrTable(nullKeyClusters);
        result.put("nullClusterName", java.util.Arrays.asList(nullKeys.retrieveAllClusterNames()));
        result.put("nullClusterBrokerAddress", java.util.Arrays.asList(nullKeys.retrieveAllAddrByCluster(null)));

        org.apache.rocketmq.remoting.protocol.body.ClusterInfo nullTables =
            new org.apache.rocketmq.remoting.protocol.body.ClusterInfo();
        result.put("nullClusterTableNamesOutcome", nullPointerOutcome(nullTables::retrieveAllClusterNames));
        result.put("nullClusterTableAddressesOutcome",
            nullPointerOutcome(() -> nullTables.retrieveAllAddrByCluster("cluster-a")));
        org.apache.rocketmq.remoting.protocol.body.ClusterInfo nullBrokerNamesInfo =
            new org.apache.rocketmq.remoting.protocol.body.ClusterInfo();
        HashMap<String, java.util.Set<String>> nullSetTable = new HashMap<>();
        nullSetTable.put("cluster-a", null);
        nullBrokerNamesInfo.setClusterAddrTable(nullSetTable);
        result.put("nullBrokerNameSetOutcome",
            nullPointerOutcome(() -> nullBrokerNamesInfo.retrieveAllAddrByCluster("cluster-a")));
        org.apache.rocketmq.remoting.protocol.body.ClusterInfo nullBrokerTableInfo =
            new org.apache.rocketmq.remoting.protocol.body.ClusterInfo();
        HashMap<String, java.util.Set<String>> presentSetTable = new HashMap<>();
        presentSetTable.put("cluster-a", new java.util.HashSet<>(java.util.Set.of("broker-a")));
        nullBrokerTableInfo.setClusterAddrTable(presentSetTable);
        result.put("nullBrokerTableOutcome",
            nullPointerOutcome(() -> nullBrokerTableInfo.retrieveAllAddrByCluster("cluster-a")));
        org.apache.rocketmq.remoting.protocol.body.ClusterInfo nullBrokerAddressTableInfo =
            new org.apache.rocketmq.remoting.protocol.body.ClusterInfo();
        org.apache.rocketmq.remoting.protocol.route.BrokerData noAddressTableBroker =
            new org.apache.rocketmq.remoting.protocol.route.BrokerData();
        HashMap<String, org.apache.rocketmq.remoting.protocol.route.BrokerData> noAddressTableBrokers = new HashMap<>();
        noAddressTableBrokers.put("broker-a", noAddressTableBroker);
        nullBrokerAddressTableInfo.setBrokerAddrTable(noAddressTableBrokers);
        nullBrokerAddressTableInfo.setClusterAddrTable(presentSetTable);
        result.put("nullBrokerAddressTableOutcome",
            nullPointerOutcome(() -> nullBrokerAddressTableInfo.retrieveAllAddrByCluster("cluster-a")));
        return result;
    }

    private static String nullPointerOutcome(Runnable action) {
        try {
            action.run();
            return "OK";
        } catch (NullPointerException error) {
            return "NullPointerException";
        }
    }

    private static Map<String, Object> clusterInfoValueMethodCases() {
        Map<String, Object> result = new TreeMap<>();
        org.apache.rocketmq.remoting.protocol.body.ClusterInfo defaults =
            new org.apache.rocketmq.remoting.protocol.body.ClusterInfo();
        org.apache.rocketmq.remoting.protocol.body.ClusterInfo sameDefaults =
            new org.apache.rocketmq.remoting.protocol.body.ClusterInfo();
        result.put("defaultEquals", defaults.equals(sameDefaults));
        result.put("defaultHashCode", defaults.hashCode());

        org.apache.rocketmq.remoting.protocol.body.ClusterInfo empty =
            new org.apache.rocketmq.remoting.protocol.body.ClusterInfo();
        empty.setBrokerAddrTable(new HashMap<>());
        result.put("nullVersusEmptyEquals", defaults.equals(empty));
        result.put("emptyHashCode", empty.hashCode());

        org.apache.rocketmq.remoting.protocol.body.ClusterInfo left = clusterInfoForValueMethod("cluster-a", "master");
        org.apache.rocketmq.remoting.protocol.body.ClusterInfo right = clusterInfoForValueMethod("cluster-b", "master");
        result.put("metadataOnlyBrokerChangeEquals", left.equals(right));
        result.put("metadataOnlyBrokerLeftHashCode", left.hashCode());
        result.put("metadataOnlyBrokerRightHashCode", right.hashCode());

        org.apache.rocketmq.remoting.protocol.body.ClusterInfo changed = clusterInfoForValueMethod("cluster-a", "slave");
        result.put("changedBrokerAddressEquals", left.equals(changed));
        result.put("changedBrokerAddressHashCode", changed.hashCode());
        return result;
    }

    private static org.apache.rocketmq.remoting.protocol.body.ClusterInfo clusterInfoForValueMethod(
        String brokerCluster, String address) {
        org.apache.rocketmq.remoting.protocol.body.ClusterInfo info =
            new org.apache.rocketmq.remoting.protocol.body.ClusterInfo();
        org.apache.rocketmq.remoting.protocol.route.BrokerData broker =
            new org.apache.rocketmq.remoting.protocol.route.BrokerData();
        broker.setCluster(brokerCluster);
        broker.setBrokerName("broker-a");
        broker.setEnableActingMaster("cluster-a".equals(brokerCluster));
        HashMap<Long, String> addresses = new HashMap<>();
        addresses.put(0L, address);
        broker.setBrokerAddrs(addresses);
        HashMap<String, org.apache.rocketmq.remoting.protocol.route.BrokerData> brokers = new HashMap<>();
        brokers.put("broker-a", broker);
        HashMap<String, java.util.Set<String>> clusters = new HashMap<>();
        clusters.put("cluster-a", new java.util.HashSet<>(java.util.Set.of("broker-a")));
        info.setBrokerAddrTable(brokers);
        info.setClusterAddrTable(clusters);
        return info;
    }

    private static List<Object> topicRouteDataToStringCases() {
        String populated = "{\"orderTopicConf\":\"broker:1\",\"queueDatas\":[{\"brokerName\":\"broker\","
            + "\"readQueueNums\":1,\"writeQueueNums\":2,\"perm\":6,\"topicSysFlag\":0}],"
            + "\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\","
            + "\"brokerAddrs\":{\"0\":\"master:10911\"}}],"
            + "\"filterServerTable\":{\"broker\":[\"filter-a\",\"filter-b\"]},"
            + "\"topicQueueMappingByBroker\":{\"broker\":{\"topic\":\"topic\",\"scope\":\"__global__\","
            + "\"totalQueues\":2,\"bname\":\"broker\",\"epoch\":1,\"dirty\":false,\"currIdMap\":{\"0\":0}}}}";
        String nullCollections = "{\"orderTopicConf\":null,\"queueDatas\":null,\"brokerDatas\":null,"
            + "\"filterServerTable\":null,\"topicQueueMappingByBroker\":null}";
        String nullElements = "{\"queueDatas\":[null],\"brokerDatas\":[null],"
            + "\"filterServerTable\":{\"broker\":[\"filter-a\",null]},"
            + "\"topicQueueMappingByBroker\":{\"broker\":null}}";
        String nullFilterList = "{\"filterServerTable\":{\"broker\":null}}";
        String[] inputs = new String[] {"{}", nullCollections, populated, nullElements, nullFilterList};
        List<Object> cases = new ArrayList<>();
        for (String input : inputs) {
            org.apache.rocketmq.remoting.protocol.route.TopicRouteData route =
                com.alibaba.fastjson2.JSON.parseObject(input,
                    org.apache.rocketmq.remoting.protocol.route.TopicRouteData.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(route,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
            item.put("toString", route.toString());
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> brokerDataCompareToCases() {
        List<Object> cases = new ArrayList<>();
        String[][] pairs = new String[][] {
            {"alpha", "beta"},
            {"a", "abcd"},
            {"\uD800\uDC00", "\uE000"},
            {"same", "same"},
            {null, "broker"},
            {"broker", null}
        };
        for (String[] pair : pairs) {
            org.apache.rocketmq.remoting.protocol.route.BrokerData left = brokerData(
                "left-cluster", pair[0], null, false, "left-zone");
            org.apache.rocketmq.remoting.protocol.route.BrokerData right = brokerData(
                "right-cluster", pair[1], null, true, "right-zone");
            Map<String, Object> item = new TreeMap<>();
            item.put("left", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(left,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue)));
            item.put("right", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(right,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue)));
            try {
                item.put("result", left.compareTo(right));
                item.put("outcome", "OK");
            } catch (NullPointerException error) {
                item.put("outcome", "NullPointerException");
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> topicRouteDataChangedCases() {
        String emptyPrefix = "{\"queueDatas\":[],\"filterServerTable\":{},\"brokerDatas\":";
        String singleNull = emptyPrefix + "[{\"brokerName\":null,\"brokerAddrs\":{}}]}";
        String multipleWithNull = emptyPrefix
            + "[{\"brokerName\":null,\"brokerAddrs\":{}},{\"brokerName\":\"broker\",\"brokerAddrs\":{}}]}";
        String ordered = emptyPrefix
            + "[{\"brokerName\":\"a\",\"brokerAddrs\":{}},{\"brokerName\":\"b\",\"brokerAddrs\":{}}]}";
        String reversed = emptyPrefix
            + "[{\"brokerName\":\"b\",\"brokerAddrs\":{}},{\"brokerName\":\"a\",\"brokerAddrs\":{}}]}";
        String nullQueue = "{\"queueDatas\":[{\"brokerName\":null}],\"filterServerTable\":{},\"brokerDatas\":[]}";
        String multipleQueuesWithNull = "{\"queueDatas\":[{\"brokerName\":null},{\"brokerName\":\"broker\"}],\"filterServerTable\":{},\"brokerDatas\":[]}";
        String orderedQueues = "{\"queueDatas\":[{\"brokerName\":\"a\"},{\"brokerName\":\"b\"}],\"filterServerTable\":{},\"brokerDatas\":[]}";
        String reversedQueues = "{\"queueDatas\":[{\"brokerName\":\"b\"},{\"brokerName\":\"a\"}],\"filterServerTable\":{},\"brokerDatas\":[]}";
        String oneNullQueue = "{\"queueDatas\":[null],\"filterServerTable\":{},\"brokerDatas\":[]}";
        String multipleWithNullQueue = "{\"queueDatas\":[null,{\"brokerName\":\"broker\"}],\"filterServerTable\":{},\"brokerDatas\":[]}";
        String oneNullBroker = "{\"queueDatas\":[],\"filterServerTable\":{},\"brokerDatas\":[null]}";
        String multipleWithNullBroker = "{\"queueDatas\":[],\"filterServerTable\":{},\"brokerDatas\":[null,{\"brokerName\":\"broker\",\"brokerAddrs\":{}}]}";
        String[][] pairs = new String[][] {
            {singleNull, singleNull},
            {multipleWithNull, multipleWithNull},
            {reversed, ordered},
            {nullQueue, nullQueue},
            {multipleQueuesWithNull, multipleQueuesWithNull},
            {reversedQueues, orderedQueues},
            {oneNullQueue, oneNullQueue},
            {multipleWithNullQueue, multipleWithNullQueue},
            {oneNullBroker, oneNullBroker},
            {multipleWithNullBroker, multipleWithNullBroker}
        };
        List<Object> cases = new ArrayList<>();
        for (String[] pair : pairs) {
            org.apache.rocketmq.remoting.protocol.route.TopicRouteData current =
                com.alibaba.fastjson2.JSON.parseObject(pair[0],
                    org.apache.rocketmq.remoting.protocol.route.TopicRouteData.class);
            org.apache.rocketmq.remoting.protocol.route.TopicRouteData old =
                com.alibaba.fastjson2.JSON.parseObject(pair[1],
                    org.apache.rocketmq.remoting.protocol.route.TopicRouteData.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("current", com.alibaba.fastjson2.JSON.parseObject(pair[0]));
            item.put("old", com.alibaba.fastjson2.JSON.parseObject(pair[1]));
            try {
                item.put("changed", current.topicRouteDataChanged(old));
                item.put("outcome", "OK");
            } catch (NullPointerException error) {
                item.put("outcome", "NullPointerException");
            }
            cases.add(item);
        }
        org.apache.rocketmq.remoting.protocol.route.TopicRouteData current =
            com.alibaba.fastjson2.JSON.parseObject("{}",
                org.apache.rocketmq.remoting.protocol.route.TopicRouteData.class);
        Map<String, Object> nullOld = new TreeMap<>();
        nullOld.put("current", com.alibaba.fastjson2.JSON.parseObject(
            com.alibaba.fastjson2.JSON.toJSONString(current,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue)));
        nullOld.put("old", null);
        nullOld.put("changed", current.topicRouteDataChanged(null));
        nullOld.put("outcome", "OK");
        cases.add(nullOld);
        return cases;
    }

    private static List<Object> topicRouteDataCloneCases() {
        String[] inputs = new String[] {
            "{}",
            "{\"queueDatas\":null,\"brokerDatas\":null,\"filterServerTable\":null}",
            "{\"queueDatas\":[null],\"brokerDatas\":[null],\"filterServerTable\":{\"broker\":null}}",
            "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":{},\"topicQueueMappingByBroker\":{\"broker\":null}}",
            "{\"queueDatas\":[{\"brokerName\":\"broker\"}],\"brokerDatas\":[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"master\"}}],"
                + "\"filterServerTable\":{\"broker\":[\"filter\"]},\"topicQueueMappingByBroker\":{\"broker\":{\"topic\":\"topic\","
                + "\"scope\":\"__global__\",\"totalQueues\":1,\"bname\":\"broker\",\"epoch\":1,\"dirty\":false,\"currIdMap\":{\"0\":0}}}}",
            "{\"queueDatas\":[],\"brokerDatas\":[null],\"filterServerTable\":{}}",
            "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":{\"broker\":null}}",
            "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":{},\"topicQueueMappingByBroker\":{\"broker\":{\"topic\":\"topic\","
                + "\"totalQueues\":1,\"bname\":\"broker\",\"epoch\":1,\"currIdMap\":null}}}"
        };
        List<Object> cases = new ArrayList<>();
        for (int index = 0; index < inputs.length; index++) {
            org.apache.rocketmq.remoting.protocol.route.TopicRouteData source =
                com.alibaba.fastjson2.JSON.parseObject(inputs[index],
                    org.apache.rocketmq.remoting.protocol.route.TopicRouteData.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("id", "clone-" + index);
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(inputs[index]));
            org.apache.rocketmq.remoting.protocol.route.TopicRouteData copy =
                new org.apache.rocketmq.remoting.protocol.route.TopicRouteData(source);
            item.put("copyConstructor", routeJson(copy));
            item.put("copyCollectionAliases", topicRouteDataCollectionAliases(source, copy));
            item.put("copyQueueElementShared", source.getQueueDatas() != null && !source.getQueueDatas().isEmpty()
                && source.getQueueDatas().get(0) == copy.getQueueDatas().get(0));
            try {
                org.apache.rocketmq.remoting.protocol.route.TopicRouteData clone =
                    source.cloneTopicRouteData();
                item.put("cloneOutcome", "OK");
                item.put("clone", routeJson(clone));
                item.put("cloneCollectionAliases", topicRouteDataCollectionAliases(source, clone));
                item.put("cloneQueueElementShared", source.getQueueDatas() != null && !source.getQueueDatas().isEmpty()
                    && source.getQueueDatas().get(0) == clone.getQueueDatas().get(0));
            } catch (NullPointerException error) {
                item.put("cloneOutcome", "NullPointerException");
                item.put("clone", null);
                item.put("cloneCollectionAliases", topicRouteDataCollectionAliases(source, null));
                item.put("cloneQueueElementShared", false);
            }
            try {
                org.apache.rocketmq.remoting.protocol.route.TopicRouteData clone =
                    source.deepCloneTopicRouteData();
                item.put("deepCloneOutcome", "OK");
                item.put("deepClone", routeJson(clone));
                item.put("deepCloneCollectionAliases", topicRouteDataCollectionAliases(source, clone));
                item.put("deepCloneQueueElementShared", source.getQueueDatas() != null && !source.getQueueDatas().isEmpty()
                    && source.getQueueDatas().get(0) == clone.getQueueDatas().get(0));
            } catch (NullPointerException error) {
                item.put("deepCloneOutcome", "NullPointerException");
                item.put("deepClone", null);
                item.put("deepCloneCollectionAliases", topicRouteDataCollectionAliases(source, null));
                item.put("deepCloneQueueElementShared", false);
            }
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> topicRouteDataCollectionAliases(
        org.apache.rocketmq.remoting.protocol.route.TopicRouteData source,
        org.apache.rocketmq.remoting.protocol.route.TopicRouteData copy) {
        Map<String, Object> aliases = new TreeMap<>();
        aliases.put("queueList", copy != null && source.getQueueDatas() != null
            && source.getQueueDatas() == copy.getQueueDatas());
        aliases.put("queueElement", copy != null && source.getQueueDatas() != null
            && !source.getQueueDatas().isEmpty() && copy.getQueueDatas() != null
            && !copy.getQueueDatas().isEmpty()
            && source.getQueueDatas().get(0) == copy.getQueueDatas().get(0));
        aliases.put("brokerList", copy != null && source.getBrokerDatas() != null
            && source.getBrokerDatas() == copy.getBrokerDatas());
        aliases.put("brokerElement", copy != null && source.getBrokerDatas() != null
            && !source.getBrokerDatas().isEmpty() && copy.getBrokerDatas() != null
            && !copy.getBrokerDatas().isEmpty()
            && source.getBrokerDatas().get(0) == copy.getBrokerDatas().get(0));
        aliases.put("filterTable", copy != null && source.getFilterServerTable() != null
            && source.getFilterServerTable() == copy.getFilterServerTable());
        aliases.put("filterList", copy != null && source.getFilterServerTable() != null
            && source.getFilterServerTable().containsKey("broker")
            && source.getFilterServerTable().get("broker") != null
            && copy.getFilterServerTable() != null
            && copy.getFilterServerTable().containsKey("broker")
            && source.getFilterServerTable().get("broker")
                == copy.getFilterServerTable().get("broker"));
        aliases.put("mappingTable", copy != null && source.getTopicQueueMappingByBroker() != null
            && source.getTopicQueueMappingByBroker() == copy.getTopicQueueMappingByBroker());
        aliases.put("mappingValue", copy != null && source.getTopicQueueMappingByBroker() != null
            && source.getTopicQueueMappingByBroker().containsKey("broker")
            && source.getTopicQueueMappingByBroker().get("broker") != null
            && copy.getTopicQueueMappingByBroker() != null
            && copy.getTopicQueueMappingByBroker().containsKey("broker")
            && source.getTopicQueueMappingByBroker().get("broker")
                == copy.getTopicQueueMappingByBroker().get("broker"));
        return aliases;
    }

    private static Map<String, Object> routeJson(org.apache.rocketmq.remoting.protocol.route.TopicRouteData route) {
        return new TreeMap<>(com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(route,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
    }

    private static <T extends org.apache.rocketmq.remoting.protocol.RemotingSerializable>
        Map<String, Object> inheritedSerializationCase(String id, String rawInput, Class<T> type) {
        T value = com.alibaba.fastjson2.JSON.parseObject(rawInput, type);
        String instanceJson = value.toJson();
        String prettyJson = value.toJson(true);
        byte[] instanceBytes = value.encode();
        byte[] staticBytes = org.apache.rocketmq.remoting.protocol.RemotingSerializable.encode(value);
        byte[] featureBytes = value.encode(new com.alibaba.fastjson2.JSONWriter.Feature[0]);
        T decoded = org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(instanceBytes, type);
        T fromJson = org.apache.rocketmq.remoting.protocol.RemotingSerializable.fromJson(instanceJson, type);
        List<T> decodedList = org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(
            com.alibaba.fastjson2.JSON.toJSONBytes(java.util.Collections.singletonList(value)), type);

        Map<String, Object> item = new TreeMap<>();
        item.put("id", id);
        item.put("rawInput", rawInput);
        item.put("input", com.alibaba.fastjson2.JSON.parseObject(rawInput));
        item.put("instanceJson", instanceJson);
        item.put("instancePrettyJson", prettyJson);
        item.put("staticJson", org.apache.rocketmq.remoting.protocol.RemotingSerializable.toJson(value, false));
        item.put("staticPrettyJson", org.apache.rocketmq.remoting.protocol.RemotingSerializable.toJson(value, true));
        item.put("instanceEncodedJson", new String(instanceBytes, java.nio.charset.StandardCharsets.UTF_8));
        item.put("staticEncodedJson", new String(staticBytes, java.nio.charset.StandardCharsets.UTF_8));
        item.put("featureEncodedJson", new String(featureBytes, java.nio.charset.StandardCharsets.UTF_8));
        item.put("writerFeatureOverrides", writerFeatureOverrides(value));
        item.put("decodedJson", decoded.toJson());
        item.put("fromJsonJson", fromJson.toJson());
        item.put("decodedListJson", decodedList.get(0).toJson());
        item.put("decodedListCount", decodedList.size());
        return item;
    }

    private static Map<String, Object> writerFeatureOverrides(
            org.apache.rocketmq.remoting.protocol.RemotingSerializable value) {
        String baseline = new String(value.encode(), java.nio.charset.StandardCharsets.UTF_8);
        Map<String, Object> overrides = new TreeMap<>();
        for (com.alibaba.fastjson2.JSONWriter.Feature feature
                : com.alibaba.fastjson2.JSONWriter.Feature.values()) {
            try {
                String encoded = new String(value.encode(
                    new com.alibaba.fastjson2.JSONWriter.Feature[] {feature}),
                    java.nio.charset.StandardCharsets.UTF_8);
                if (!baseline.equals(encoded)) {
                    overrides.put(feature.name(), encoded);
                }
            } catch (RuntimeException error) {
                overrides.put(feature.name(), Map.of("error", error.getClass().getSimpleName()));
            }
        }
        overrides.put("WriteBooleanAsNumber+WriteNonStringValueAsString", new String(value.encode(
            new com.alibaba.fastjson2.JSONWriter.Feature[] {
                com.alibaba.fastjson2.JSONWriter.Feature.WriteBooleanAsNumber,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringValueAsString}),
            java.nio.charset.StandardCharsets.UTF_8));
        overrides.put("BrowserSecure+UnquoteFieldName", new String(value.encode(
            new com.alibaba.fastjson2.JSONWriter.Feature[] {
                com.alibaba.fastjson2.JSONWriter.Feature.BrowserSecure,
                com.alibaba.fastjson2.JSONWriter.Feature.UnquoteFieldName}),
            java.nio.charset.StandardCharsets.UTF_8));
        overrides.put("IgnoreErrorGetter", new String(value.encode(
            new com.alibaba.fastjson2.JSONWriter.Feature[] {
                com.alibaba.fastjson2.JSONWriter.Feature.IgnoreErrorGetter}),
            java.nio.charset.StandardCharsets.UTF_8));
        return overrides;
    }

    private static List<Object> inheritedSerializationCases() {
        List<Object> cases = new ArrayList<>();
        String[][] routeInputs = new String[][] {
            {"route-default", "{}"},
            {"route-null", "{\"orderTopicConf\":null,\"queueDatas\":null,\"brokerDatas\":null,\"filterServerTable\":null,\"topicQueueMappingByBroker\":null}"},
            {"route-populated", "{\"orderTopicConf\":\"broker:2\",\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":2,\"writeQueueNums\":2,\"perm\":6,\"topicSysFlag\":1}],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"master:10911\",\"2\":\"slave:10911\"}}],\"filterServerTable\":{\"broker\":[\"filter-a\",\"filter-b\"]},\"topicQueueMappingByBroker\":{\"broker\":{\"topic\":\"topic\",\"totalQueues\":2,\"bname\":\"broker\",\"epoch\":1,\"dirty\":false,\"scope\":\"__global__\",\"currIdMap\":{\"0\":0,\"1\":1}}}}"},
            {"route-browser-secure", "{\"topicQueueMappingByBroker\":{\"<broker>(a)\":{\"topic\":\"<orders>\",\"totalQueues\":1,\"bname\":\"broker\",\"epoch\":1,\"dirty\":false,\"scope\":\"tenant\",\"currIdMap\":{\"0\":0}}}}"}
        };
        for (String[] entry : routeInputs) {
            cases.add(inheritedSerializationCase(entry[0], entry[1], TopicRouteData.class));
        }
        String[][] mappingInputs = new String[][] {
            {"mapping-default", "{}"},
            {"mapping-null", "{\"topic\":null,\"totalQueues\":0,\"bname\":null,\"epoch\":0,\"dirty\":false,\"scope\":null,\"currIdMap\":null}"},
            {"mapping-populated", "{\"topic\":\"topic\",\"totalQueues\":2,\"bname\":\"broker\",\"epoch\":1,\"dirty\":false,\"scope\":\"__global__\",\"currIdMap\":{\"0\":0,\"1\":1}}"}
        };
        for (String[] entry : mappingInputs) {
            cases.add(inheritedSerializationCase(entry[0], entry[1],
                org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo.class));
        }
        String[][] detailInputs = new String[][] {
            {"detail-default", "{}"},
            {"detail-null", "{\"topic\":null,\"bname\":null,\"scope\":null,\"currIdMap\":null,\"hostedQueues\":null}"},
            {"detail-populated", "{\"topic\":\"topic\",\"scope\":\"__global__\",\"totalQueues\":2,\"bname\":\"broker\",\"epoch\":1,\"dirty\":true,\"currIdMap\":{\"0\":0},\"hostedQueues\":{\"0\":[{\"gen\":1,\"queueId\":3,\"bname\":\"broker\",\"logicOffset\":10,\"startOffset\":0,\"endOffset\":-1,\"timeOfStart\":-1,\"timeOfEnd\":-1},null]}}"}
        };
        for (String[] entry : detailInputs) {
            cases.add(inheritedSerializationCase(entry[0], entry[1],
                org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.class));
        }
        String[][] logicItemInputs = new String[][] {
            {"logic-item-default", "{}"},
            {"logic-item-populated", "{\"gen\":1,\"queueId\":3,\"bname\":\"broker\",\"logicOffset\":10,\"startOffset\":0,\"endOffset\":-1,\"timeOfStart\":-1,\"timeOfEnd\":-1}"}
        };
        for (String[] entry : logicItemInputs) {
            cases.add(inheritedSerializationCase(entry[0], entry[1],
                org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem.class));
        }
        return cases;
    }

    private static Map<String, Object> referenceDetectionSharedRouteCase() {
        TopicRouteData route = new TopicRouteData();
        org.apache.rocketmq.remoting.protocol.route.BrokerData sharedBroker =
            new org.apache.rocketmq.remoting.protocol.route.BrokerData();
        sharedBroker.setBrokerName("shared-broker");
        List<org.apache.rocketmq.remoting.protocol.route.BrokerData> brokers = new ArrayList<>();
        brokers.add(sharedBroker);
        brokers.add(sharedBroker);
        route.setBrokerDatas(brokers);

        Map<String, Object> item = new TreeMap<>();
        item.put("default", new String(route.encode(), java.nio.charset.StandardCharsets.UTF_8));
        item.put("referenceDetection", new String(route.encode(
            new com.alibaba.fastjson2.JSONWriter.Feature[] {
                com.alibaba.fastjson2.JSONWriter.Feature.ReferenceDetection}),
            java.nio.charset.StandardCharsets.UTF_8));
        return item;
    }

    private static Map<String, Object> inheritedSerializationNullUtilities() {
        Map<String, Object> item = new TreeMap<>();
        byte[] jsonNull = "null".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        item.put("decodeNullBytesReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(null, TopicRouteData.class) == null);
        item.put("decodeJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(jsonNull, TopicRouteData.class) == null);
        item.put("fromJsonNullInputReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.fromJson(null, TopicRouteData.class) == null);
        item.put("fromJsonJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.fromJson("null", TopicRouteData.class) == null);
        item.put("decodeListNullBytesReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(null, TopicRouteData.class) == null);
        item.put("decodeListJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(jsonNull, TopicRouteData.class) == null);
        List<TopicRouteData> nullMemberList = org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(
            "[null]".getBytes(java.nio.charset.StandardCharsets.UTF_8), TopicRouteData.class);
        item.put("decodeListNullMemberCount", nullMemberList.size());
        item.put("decodeListHasNullMember", nullMemberList.get(0) == null);

        Class<org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo> mappingType =
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo.class;
        item.put("mappingDecodeNullBytesReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(null, mappingType) == null);
        item.put("mappingDecodeJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(jsonNull, mappingType) == null);
        item.put("mappingFromJsonNullInputReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.fromJson(null, mappingType) == null);
        item.put("mappingFromJsonJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.fromJson("null", mappingType) == null);
        item.put("mappingDecodeListNullBytesReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(null, mappingType) == null);
        item.put("mappingDecodeListJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(jsonNull, mappingType) == null);
        List<org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo> nullMappingMemberList =
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(
                "[null]".getBytes(java.nio.charset.StandardCharsets.UTF_8), mappingType);
        item.put("mappingDecodeListNullMemberCount", nullMappingMemberList.size());
        item.put("mappingDecodeListHasNullMember", nullMappingMemberList.get(0) == null);
        return item;
    }

    private static List<Object> nullArgumentConversions(Map<String, Object> input,
        Map<String, Object> staticInput, Map<String, Object> orderedInput) {
        List<Object> cases = new ArrayList<>();
        for (String method : new String[] {"publish", "subscribe"}) {
            for (String shape : new String[] {"null-topic", "null-route", "null-topic-and-route",
                "null-topic-ordered"}) {
                boolean[] staticVariants = shape.equals("null-topic")
                    ? new boolean[] {false, true}
                    : new boolean[] {false};
                for (boolean staticRoute : staticVariants) {
                    String topic = shape.equals("null-topic") || shape.equals("null-topic-and-route")
                        || shape.equals("null-topic-ordered")
                        ? null
                        : "topic";
                    boolean routeIsNull = shape.equals("null-route") || shape.equals("null-topic-and-route");
                    Map<String, Object> routeInput = shape.equals("null-topic-ordered")
                        ? orderedInput
                        : staticRoute ? staticInput : input;
                    TopicRouteData route = routeIsNull
                        ? null
                        : com.alibaba.fastjson2.JSON.parseObject(
                            com.alibaba.fastjson2.JSON.toJSONString(routeInput,
                                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue), TopicRouteData.class);
                    Map<String, Object> item = new TreeMap<>();
                    item.put("id", method + "-" + shape + (staticRoute ? "-static" : ""));
                    item.put("method", method);
                    item.put("topic", topic);
                    item.put("routeIsNull", route == null);
                    if (route != null) {
                        item.put("routeInput", routeInput);
                    }
                    try {
                        if (method.equals("publish")) {
                            org.apache.rocketmq.client.impl.producer.TopicPublishInfo publish =
                                MQClientInstance.topicRouteData2TopicPublishInfo(topic, route);
                            item.put("publish", publish.getMessageQueueList());
                            item.put("ordered", publish.isOrderTopic());
                            item.put("topicRouteData", com.alibaba.fastjson2.JSON.toJSONString(
                                publish.getTopicRouteData(),
                                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue));
                        } else {
                            List<MessageQueue> subscribe = new ArrayList<>(
                                MQClientInstance.topicRouteData2TopicSubscribeInfo(topic, route));
                            subscribe.sort(Comparator.comparing(MessageQueue::getTopic,
                                    Comparator.nullsFirst(Comparator.naturalOrder()))
                                .thenComparing(MessageQueue::getBrokerName,
                                    Comparator.nullsFirst(Comparator.naturalOrder()))
                                .thenComparingInt(MessageQueue::getQueueId));
                            item.put("subscribe", subscribe);
                        }
                        item.put("outcome", "OK");
                    } catch (RuntimeException error) {
                        item.put("outcome", error.getClass().getSimpleName());
                    }
                    cases.add(item);
                }
            }
        }
        return cases;
    }

    public static void main(String[] args) throws Exception {
        String ordinary = "{\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":3,\"writeQueueNums\":2,\"perm\":6,\"topicSysFlag\":0}],"
            + "\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"master:10911\",\"2\":\"slave:10911\"}}],\"filterServerTable\":{}}";
        List<Object> cases = new ArrayList<>();
        List<Map<String, Object>> inputs = new ArrayList<>();
        for (String order : new String[] {null, "", "broker:2", "broker:2:ignored;", ";;", "broker:invalid",
            "broker:٢", "broker:+２", "broker:1٢", "broker:२", "broker:߂", "broker:²",
            "broker:𝟚", "broker: 2", "broker:", "broker:-２１４７４８３６４８", "broker:２１４７４８３６４８",
            ";broker:2", "broker:2;;broker:1", "broker:2;;;broker:1", "broker:2;;;", ";",
            ":1", "broker:2;:1", "broker:2;broker", "broker:2;broker:1"}) {
            Map<String, Object> input = new TreeMap<>(com.alibaba.fastjson2.JSON.parseObject(ordinary));
            input.put("orderTopicConf", order);
            inputs.add(input);
        }
        for (String extra : new String[] {
            "{\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":2147483647,\"writeQueueNums\":2147483647,\"perm\":0,\"topicSysFlag\":0}]}",
            "{\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":0,\"writeQueueNums\":2147483647,\"perm\":2,\"topicSysFlag\":0}],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"2\":\"slave:10911\"}}]}",
            "{\"orderTopicConf\":\"broker:2\",\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":1,\"writeQueueNums\":2147483647,\"perm\":6,\"topicSysFlag\":0}]}",
            "{\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":2,\"writeQueueNums\":3,\"perm\":6,\"topicSysFlag\":0},{\"brokerName\":\"broker\",\"readQueueNums\":1,\"writeQueueNums\":1,\"perm\":6,\"topicSysFlag\":0}]}"
        }) {
            Map<String, Object> input = new TreeMap<>(com.alibaba.fastjson2.JSON.parseObject(ordinary));
            input.putAll(com.alibaba.fastjson2.JSON.parseObject(extra));
            inputs.add(input);
        }
        Map<String, Object> unmatchedBroker = new TreeMap<>(com.alibaba.fastjson2.JSON.parseObject(ordinary));
        unmatchedBroker.put("queueDatas", com.alibaba.fastjson2.JSON.parseArray(
            "[{\"brokerName\":\"unlisted\",\"readQueueNums\":2,\"writeQueueNums\":3,\"perm\":6,\"topicSysFlag\":0}]"));
        unmatchedBroker.put("brokerDatas", com.alibaba.fastjson2.JSON.parseArray(
            "[{\"cluster\":\"cluster\",\"brokerName\":\"known\",\"brokerAddrs\":{\"0\":\"master:10911\"}}]"));
        unmatchedBroker.put("topicQueueMappingByBroker", new TreeMap<>());
        inputs.add(unmatchedBroker);
        Map<String, Object> emptyMappingWithOrdinaryRoute =
            new TreeMap<>(com.alibaba.fastjson2.JSON.parseObject(ordinary));
        emptyMappingWithOrdinaryRoute.put("topicQueueMappingByBroker", new TreeMap<>());
        inputs.add(emptyMappingWithOrdinaryRoute);
        List<Map<String, Object>> staticInputs = new ArrayList<>();
        for (String scope : new String[] {"__global__", "__custom", "__", null}) {
            Map<String, Object> input = new TreeMap<>(com.alibaba.fastjson2.JSON.parseObject(ordinary));
            input.put("queueDatas", com.alibaba.fastjson2.JSON.parseArray("[{\"brokerName\":\"broker\",\"readQueueNums\":2147483647,\"writeQueueNums\":2147483647,\"perm\":6,\"topicSysFlag\":0}]"));
            Map<String, Object> oldMapping = new TreeMap<>();
            oldMapping.put("topic", "topic"); oldMapping.put("bname", "old-broker"); oldMapping.put("scope", scope);
            oldMapping.put("epoch", 1); oldMapping.put("totalQueues", scope == null ? Integer.MAX_VALUE : 5); oldMapping.put("currIdMap", Map.of("0",0));
            Map<String, Object> newMapping = new TreeMap<>(oldMapping);
            newMapping.put("bname", "new-broker"); newMapping.put("epoch", 2); newMapping.put("totalQueues", scope == null ? Integer.MAX_VALUE : 2);
            input.put("topicQueueMappingByBroker", new TreeMap<>(Map.of("old",oldMapping,"new",newMapping)));
            inputs.add(input);
            staticInputs.add(input);
        }
        for (Map<String, Object> input : inputs) {
            TopicRouteData route = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(input, com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue), TopicRouteData.class);
            Map<String, Object> item = new TreeMap<>();
            if (input == unmatchedBroker) {
                item.put("id", "empty-mapping-unmatched-queue-broker");
            }
            item.put("input", input);
            try {
                org.apache.rocketmq.client.impl.producer.TopicPublishInfo publish = MQClientInstance.topicRouteData2TopicPublishInfo("topic", route);
                List<MessageQueue> publishQueues = new ArrayList<>(publish.getMessageQueueList());
                if (route.getOrderTopicConf() == null && route.getTopicQueueMappingByBroker() != null && !route.getTopicQueueMappingByBroker().isEmpty()) {
                    // Canonicalize equal-ID HashMap order, not Java's queue-ID ordering.
                    publishQueues.sort(Comparator.comparingInt(MessageQueue::getQueueId).thenComparing(MessageQueue::getBrokerName));
                }
                item.put("publish", publishQueues);
                item.put("ordered", publish.isOrderTopic());
                item.put("topicRouteData", com.alibaba.fastjson2.JSON.toJSONString(
                    publish.getTopicRouteData(),
                    com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue));
                List<MessageQueue> subscribe = new ArrayList<>(MQClientInstance.topicRouteData2TopicSubscribeInfo("topic", route));
                subscribe.sort(Comparator.naturalOrder());
                item.put("subscribe", subscribe);
                List<Map<String, Object>> endpoints = new ArrayList<>();
                org.apache.rocketmq.remoting.rpc.ClientMetadata.topicRouteData2EndpointsForStaticTopic("topic", route).entrySet().stream()
                    .sorted(Map.Entry.comparingByKey()).forEach(entry -> endpoints.add(new TreeMap<>(Map.of("queue",entry.getKey(),"broker",entry.getValue()))));
                item.put("endpoints", endpoints);
                item.put("outcome", "OK");
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        Map<String, String> vip = new TreeMap<>();
        for (String address : new String[] {"broker:10911", "[2001:db8::1]:10911", "2001:db8::1:10911"}) {
            vip.put(address, MixAll.brokerVIPChannel(true, address));
        }
        List<Object> vipCases = new ArrayList<>();
        for (String address : new String[] {"missing", "", ":10911", "broker:2", "broker:0", "broker:-1",
            "broker:65536", "broker:2147483647", "broker:-2147483648", "broker:+2", "broker: 2",
            "broker:١٠٩١١", "not:ipv6:10911", "broker:", "broker:999999999999"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("address", address);
            item.put("disabled", MixAll.brokerVIPChannel(false, address));
            try {
                item.put("enabledOutcome", "OK");
                item.put("enabledValue", MixAll.brokerVIPChannel(true, address));
            } catch (RuntimeException error) {
                item.put("enabledOutcome", error.getClass().getSimpleName());
                item.put("enabledValue", null);
            }
            vipCases.add(item);
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint", "MessageQueue.equals/hashCode/toString/compareTo; BrokerData.equals/hashCode; MQClientInstance.topicRouteData2TopicPublishInfo/topicRouteData2TopicSubscribeInfo/updateTopicRouteInfoFromNameServer/getTopicRouteTable/getAnExistTopicRouteData/queryTopicRouteData/cleanOfflineBroker/findBrokerAddressInPublish/findBrokerAddressInAdmin/findBrokerAddressInSubscribe/findBrokerAddrByTopic; TopicPublishInfo.haveTopicRouterInfo/ok/getMessageQueueList/getTopicRouteData; ConcurrentMap put/putIfAbsent/replace/remove/clear; TopicRouteData inherited RemotingSerializable encode/toJson/decode/decodeList/fromJson; ClientMetadata.topicRouteData2EndpointsForStaticTopic; MixAll.brokerVIPChannel; Integer.parseInt/Character.digit");
        fixture.put("serverBaselineContacted", false);
        fixture.put("cases", cases);
        fixture.put("nullArgumentConversions", nullArgumentConversions(inputs.get(0), staticInputs.get(0), inputs.get(2)));
        fixture.put("topicPublishInfoPublication", runtimeTopicPublishInfoPublication());
        fixture.put("vip", vip);
        fixture.put("vipCases", vipCases);
        List<Integer> digitZeros = new ArrayList<>();
        for (int codeUnit = 0; codeUnit <= Character.MAX_VALUE; codeUnit++) {
            if (Character.digit((char) codeUnit, 10) == 0) digitZeros.add(codeUnit);
        }
        for (int zero : digitZeros) {
            for (int digit = 0; digit < 10; digit++) {
                if (Character.digit((char) (zero + digit), 10) != digit)
                    throw new IllegalStateException("noncontiguous decimal digits");
            }
        }
        for (int codeUnit = 0; codeUnit <= Character.MAX_VALUE; codeUnit++) {
            int digit = Character.digit((char) codeUnit, 10);
            if (digit >= 0 && !digitZeros.contains(codeUnit - digit))
                throw new IllegalStateException("unrecorded decimal digit");
        }
        fixture.put("integerDigitZeroCodeUnits", digitZeros);
        List<Object> integerCases = new ArrayList<>();
        for (String text : new String[] {"+2147483647", "2147483648", "-2147483648", "-2147483649",
            "٠٠", "--2", "", "+", "-２１４７４８３６４８", "2\n", "𝟚"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("input", text);
            try {
                item.put("value", Integer.parseInt(text));
                item.put("outcome", "OK");
            } catch (NumberFormatException error) {
                item.put("outcome", "NumberFormatException");
            }
            integerCases.add(item);
        }
        fixture.put("integers", integerCases);
        fixture.put("runtimeNullBrokerMap", runtimeNullBrokerMap());
        fixture.put("runtimeConsumerNeedUpdate", runtimeConsumerNeedUpdate());
        fixture.put("runtimeNullBrokerValues", runtimeNullBrokerValues());
        fixture.put("runtimeNullNestedRouteElements", runtimeNullNestedRouteElements());
        fixture.put("nullSubscriptionTopicRefresh", nullSubscriptionTopicRefresh());
        fixture.put("nullableRouteCollections", nullableRouteCollections());
        fixture.put("nullableNestedRouteFields", nullableNestedRouteFields());
        fixture.put("nullableMessageQueueFields", nullableMessageQueueFields());
        fixture.put("messageQueueCompareTo", messageQueueCompareToCases());
        fixture.put("compareToNullOther", compareToNullOtherCases());
        fixture.put("messageQueueMethods", messageQueueMethodCases());
        fixture.put("messageQueueForCApi", messageQueueForCApiCases());
        fixture.put("messageQueueForCCompareTo", messageQueueForCCompareToCases());
        fixture.put("nullableStaticMappingFields", nullableStaticMappingFields());
        fixture.put("staticMappingMapEntryCases", staticMappingMapEntryCases());
        fixture.put("routeDtoInputCases", routeDtoInputCases());
        fixture.put("nullStaticMappingConversion", nullStaticMappingConversion());
        fixture.put("overlappingStaticMappingOwners", overlappingStaticMappingOwners());
        fixture.put("runtimeNullableCollections", runtimeNullableCollections());
        fixture.put("runtimeRouteCollectionMatrix", runtimeRouteCollectionMatrix());
        fixture.put("runtimeRouteNestedCollectionMatrix", runtimeRouteNestedCollectionMatrix());
        fixture.put("runtimeRouteNestedCollectionConsumers", runtimeRouteNestedCollectionConsumers());
        fixture.put("runtimeEmptyEndpointTransition", runtimeEmptyEndpointTransition(staticInputs.get(0), inputs.get(0)));
        fixture.put("runtimeNullBrokerQueue", runtimeNullBrokerQueue());
        fixture.put("runtimeNullTopicBrokerSelection", runtimeNullTopicBrokerSelection());
        fixture.put("runtimeNullBrokerAddressLookups", runtimeNullBrokerAddressLookups());
        fixture.put("runtimeBrokerAddressResolution", runtimeBrokerAddressResolution());
        fixture.put("runtimeNullTopicRouteLookups", runtimeNullTopicRouteLookups());
        fixture.put("runtimeTopicRouteTable", runtimeTopicRouteTable(inputs.get(0)));
        fixture.put("runtimeRawOfflineCleanup", runtimeRawOfflineCleanup(inputs.get(0)));
        fixture.put("runtimeRawCleanupShapes", runtimeRawCleanupShapes(inputs.get(0)));
        fixture.put("brokerDataEquality", brokerDataEqualityCases());
        fixture.put("brokerDataSelectAddr", brokerDataSelectAddrCases());
        fixture.put("brokerDataHashCode", brokerDataHashCodeCases());
        fixture.put("brokerDataToString", brokerDataToStringCases());
        fixture.put("queueDataMethods", queueDataMethodCases());
        fixture.put("topicQueueMappingMethods", topicQueueMappingMethodsCases());
        fixture.put("topicQueueMappingConstants", topicQueueMappingConstants());
        fixture.put("logicQueueMappingItemMethods", logicQueueMappingItemMethodCases());
        fixture.put("logicQueueMappingItemEquality", logicQueueMappingItemEqualityCases());
        fixture.put("logicQueueMappingItemSetters", logicQueueMappingItemSetterCase());
        fixture.put("topicQueueMappingDetailMethods", topicQueueMappingDetailMethodCases());
        fixture.put("topicQueueMappingDetailOperations", topicQueueMappingDetailOperationCase());
        fixture.put("topicQueueMappingDetailNullOperations", topicQueueMappingDetailNullOperationCases());
        fixture.put("topicRouteDataMethods", topicRouteDataMethodCases());
        fixture.put("routeDtoAccessors", routeDtoAccessorCases());
        fixture.put("routeDtoReferenceAliases", routeDtoReferenceAliasCases());
        fixture.put("clusterInfoAccessors", clusterInfoAccessorCases());
        fixture.put("clusterInfoReferenceAliases", clusterInfoReferenceAliasCases());
        fixture.put("clusterInfoRetrieval", clusterInfoRetrievalCases());
        fixture.put("clusterInfoValueMethods", clusterInfoValueMethodCases());
        fixture.put("topicRouteDataToString", topicRouteDataToStringCases());
        fixture.put("inheritedSerializationCases", inheritedSerializationCases());
        fixture.put("referenceDetectionSharedRouteCase", referenceDetectionSharedRouteCase());
        fixture.put("writerFeatureEscapingCase", inheritedSerializationCase(
            "writer-unicode",
            "{\"orderTopicConf\":\"café-漢字-😀-𝄞\"}",
            TopicRouteData.class));
        fixture.put("inheritedSerializationNullUtilities", inheritedSerializationNullUtilities());
        fixture.put("brokerDataCompareTo", brokerDataCompareToCases());
        fixture.put("topicRouteDataChanged", topicRouteDataChangedCases());
        fixture.put("topicRouteDataClone", topicRouteDataCloneCases());
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else {
            Files.createDirectories(file.getParent());
            Files.writeString(file, json);
        }
        System.out.println("Runtime route/VIP invoked cases verified: " + cases.size());
    }
}
