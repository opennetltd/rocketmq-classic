package compat;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

/** Real runtime/API check-client flow; only RemotingClient is mocked. */
public final class RuntimeClientCheckFixtures {
    private RuntimeClientCheckFixtures() { }

    public static void main(String[] args) throws Exception {
        List<Object> cases = new ArrayList<>();
        for (String expression : new String[] {null, "", "TAG", "tag", "SQL92", " "}) {
            record(cases, expression, false, true, 0, false);
            record(cases, expression, true, true, 0, false);
        }
        record(cases, "SQL92", true, false, 0, false);
        record(cases, "SQL92", false, null, 0, false);
        record(cases, "SQL92", true, true, 44, false);
        record(cases, "SQL92", true, true, 0, true);
        record(cases, "SQL92", true, true, 0, false, null);
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint", "MQClientInstance.checkClientInBroker/MQClientAPIImpl.checkClientInBroker/ExpressionType.isTagType");
        fixture.put("transport", "mock RemotingClient; constructed runtime never started");
        fixture.put("serverBaselineContacted", false); fixture.put("cases", cases);
        fixture.put("vipMutation", runtimeVipMutation());
        fixture.put("checkClientBodyInputCases", checkClientBodyInputCases());
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else { Files.createDirectories(file.getParent()); Files.writeString(file, json); }
        System.out.println("Runtime client check verified: " + cases.size());
    }

    private static List<Object> checkClientBodyInputCases() {
        List<Object> cases = new ArrayList<>();
        String[][] inputs = {
            {"client-id-duplicate-last-wins", "{\"clientId\":\"first\",\"clientId\":\"last\"}"},
            {"group-duplicate-last-wins", "{\"group\":\"first\",\"group\":\"last\"}"},
            {"namespace-duplicate-last-wins", "{\"namespace\":\"first\",\"namespace\":\"last\"}"},
            {"subscription-duplicate-last-wins", "{\"subscriptionData\":{\"topic\":\"first\",\"subVersion\":1},\"subscriptionData\":{\"topic\":\"last\",\"subVersion\":2}}"},
            {"scalar-string-coercions", "{\"clientId\":123,\"group\":true,\"namespace\":{\"z\":1,\"a\":2}}"},
            {"explicit-null-fields", "{\"clientId\":null,\"group\":null,\"namespace\":null,\"subscriptionData\":null}"},
            {"subscription-data-scalar-rejected", "{\"subscriptionData\":\"text\"}"},
            {"unknown-property-ignored", "{\"clientId\":\"client\",\"unknown\":42}"}
        };
        for (String[] input : inputs) {
            Map<String, Object> item = new TreeMap<>();
            item.put("id", input[0]);
            item.put("rawInput", input[1]);
            try {
                CheckClientRequestBody body = com.alibaba.fastjson2.JSON.parseObject(
                    input[1], CheckClientRequestBody.class);
                item.put("outcome", "ACCEPTED");
                item.put("fields", com.alibaba.fastjson2.JSON.parseObject(
                    com.alibaba.fastjson2.JSON.toJSONString(body)));
            } catch (RuntimeException error) {
                item.put("outcome", "REJECTED");
                item.put("error", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        String[][] fields = {
            {"clientId", "string"}, {"group", "string"},
            {"subscriptionData", "dto"}, {"namespace", "string"}
        };
        String[][] shapes = {
            {"null", "null"}, {"numeric-string", "\"7\""}, {"integer", "0"},
            {"fraction", "1.5"}, {"boolean", "true"}, {"array", "[]"}, {"object", "{}"}
        };
        for (String[] field : fields) {
            for (String[] shape : shapes) {
                String value = shape[1];
                if (field[1].equals("dto") && shape[0].equals("object")) {
                    value = "{\"subVersion\":0}";
                }
                String input = "{\"" + field[0] + "\":" + value + "}";
                Map<String, Object> item = new TreeMap<>();
                item.put("id", "shape-" + field[0] + "-" + shape[0]);
                item.put("rawInput", input);
                try {
                    CheckClientRequestBody body = com.alibaba.fastjson2.JSON.parseObject(
                        input, CheckClientRequestBody.class);
                    item.put("outcome", "ACCEPTED");
                    item.put("fields", com.alibaba.fastjson2.JSON.parseObject(
                        com.alibaba.fastjson2.JSON.toJSONString(body)));
                } catch (RuntimeException error) {
                    item.put("outcome", "REJECTED");
                    item.put("error", error.getClass().getSimpleName());
                }
                cases.add(item);
            }
        }
        String[][] roots = {
            {"null", "null"}, {"empty-object", "{}"}, {"empty-array", "[]"},
            {"string", "\"root\""}, {"integer", "7"}, {"fraction", "1.5"},
            {"boolean", "true"}
        };
        for (String[] root : roots) {
            String input = root[1];
            Map<String, Object> item = new TreeMap<>();
            item.put("id", "root-" + root[0]);
            item.put("rawInput", input);
            try {
                CheckClientRequestBody body = com.alibaba.fastjson2.JSON.parseObject(
                    input, CheckClientRequestBody.class);
                body.getClientId(); // Exercise the handler's first DTO access for null roots.
                item.put("outcome", "ACCEPTED");
                item.put("fields", com.alibaba.fastjson2.JSON.parseObject(
                    com.alibaba.fastjson2.JSON.toJSONString(body)));
            } catch (RuntimeException error) {
                item.put("outcome", "REJECTED");
                item.put("error", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static void record(List<Object> cases, String expression, boolean route, Boolean subscriptions, int code, boolean transportFailure) throws Exception {
        record(cases, expression, route, subscriptions, code, transportFailure, "topic");
    }

    private static void record(List<Object> cases, String expression, boolean route, Boolean subscriptions,
        int code, boolean transportFailure, String topic) throws Exception {
        ClientConfig config = new ClientConfig(); config.setClientIP("check-client");
        config.setVipChannelEnabled(true); config.setMqClientApiTimeout(1234);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        Map<String, Object> item = new TreeMap<>();
        item.put("expression", expression); item.put("isTag", ExpressionType.isTagType(expression));
        item.put("route", route); item.put("subscriptions", subscriptions);
        item.put("responseCode", code); item.put("transportFailure", transportFailure);
        if (topic == null) item.put("topicNull", true);
        item.put("invocations", 0);
        RemotingClient transport = (RemotingClient) Proxy.newProxyInstance(RemotingClient.class.getClassLoader(),
            new Class<?>[] {RemotingClient.class}, (proxy, method, values) -> {
                if (method.getName().equals("invokeSync")) {
                    item.put("invocations", ((Integer) item.get("invocations")) + 1);
                    RemotingCommand request = (RemotingCommand) values[1];
                    item.put("address", values[0]); item.put("timeout", values[2]);
                    item.put("requestCode", request.getCode()); item.put("language", request.getLanguage().name());
                    item.put("body", com.alibaba.fastjson2.JSON.parseObject(request.getBody()));
                    if (transportFailure) throw new RemotingConnectException("peer");
                    return RemotingCommand.createResponseCommand(code, "rejected");
                }
                return method.getReturnType() == boolean.class ? false : null;
            });
        MQClientAPIImpl api = new MQClientAPIImpl(new NettyClientConfig(), null, null, config, null, ignored -> transport);
        Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true); field.set(runtime, api);
        try {
            SubscriptionData subscription = new SubscriptionData(); subscription.setTopic(topic);
            subscription.setSubString("a > 1"); subscription.setSubVersion(42); subscription.setExpressionType(expression);
            MQConsumerInner consumer = (MQConsumerInner) Proxy.newProxyInstance(MQConsumerInner.class.getClassLoader(),
                new Class<?>[] {MQConsumerInner.class}, (proxy, method, values) -> {
                    if (method.getName().equals("subscriptions")) return subscriptions == null ? null : subscriptions ? Collections.singleton(subscription) : Collections.emptySet();
                    if (method.getName().equals("groupName")) return "different-callback-group";
                    return method.getReturnType() == boolean.class ? false : null;
                });
            runtime.registerConsumer("registered-group", consumer);
            if (route && topic != null) runtime.getTopicRouteTable().put(topic, com.alibaba.fastjson2.JSON.parseObject(
                "{\"queueDatas\":[],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"192.0.2.6:10911\"}}],\"filterServerTable\":{}}", TopicRouteData.class));
            try { runtime.checkClientInBroker(); item.put("outcome", "OK"); }
            catch (NullPointerException error) { item.put("outcome", "NullPointerException"); }
            catch (MQClientException error) {
                item.put("outcome", "MQClientException"); item.put("errorCode", error.getResponseCode());
                item.put("cause", error.getCause() == null ? null : error.getCause().getClass().getSimpleName());
            }
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure", MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true); cleanup.invoke(runtime, original, new IllegalStateException("fixture teardown"));
            api.shutdown();
        }
        cases.add(item);
    }

    private static Map<String, Object> runtimeVipMutation() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("check-client");
        config.setVipChannelEnabled(false);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        List<String> addresses = new ArrayList<>();
        RemotingClient transport = (RemotingClient) Proxy.newProxyInstance(
            RemotingClient.class.getClassLoader(), new Class<?>[] {RemotingClient.class},
            (proxy, method, values) -> {
                if (method.getName().equals("invokeSync")) {
                    addresses.add((String) values[0]);
                    return RemotingCommand.createResponseCommand(0, null);
                }
                return method.getReturnType() == boolean.class ? false : null;
            });
        MQClientAPIImpl api = new MQClientAPIImpl(
            new NettyClientConfig(), null, null, config, null, ignored -> transport);
        Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(runtime, api);
        Map<String, Object> result = new TreeMap<>();
        try {
            SubscriptionData subscription = new SubscriptionData();
            subscription.setTopic("topic");
            subscription.setSubString("a > 1");
            subscription.setSubVersion(42);
            subscription.setExpressionType("SQL92");
            MQConsumerInner consumer = (MQConsumerInner) Proxy.newProxyInstance(
                MQConsumerInner.class.getClassLoader(), new Class<?>[] {MQConsumerInner.class},
                (proxy, method, values) -> {
                    if (method.getName().equals("subscriptions")) {
                        return Collections.singleton(subscription);
                    }
                    return method.getReturnType() == boolean.class ? false : null;
                });
            runtime.registerConsumer("registered-group", consumer);
            runtime.getTopicRouteTable().put("topic", com.alibaba.fastjson2.JSON.parseObject(
                "{\"queueDatas\":[],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"127.0.0.1:10911\"}}],\"filterServerTable\":{}}",
                TopicRouteData.class));

            runtime.checkClientInBroker();
            result.put("vipDisabled", new ArrayList<>(addresses));
            addresses.clear();
            config.setVipChannelEnabled(true);
            runtime.checkClientInBroker();
            result.put("vipEnabled", new ArrayList<>(addresses));
            result.put("invocations", 2);
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod(
                "cleanupAfterConstructionFailure", MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, original, new IllegalStateException("VIP mutation fixture teardown"));
            api.shutdown();
        }
        return result;
    }
}
