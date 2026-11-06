package compat;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.OffsetNotFoundException;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

/** Persisted semantic evidence from real client invocations with only the transport mocked. */
public final class InvokedRpcFixtures {
    private InvokedRpcFixtures() { }

    private static final class Capture {
        String mode;
        RemotingCommand request;
        RemotingCommand response;

        RemotingClient proxy() {
            return (RemotingClient) Proxy.newProxyInstance(RemotingClient.class.getClassLoader(),
                new Class<?>[] {RemotingClient.class}, (proxy, method, args) -> {
                    if (method.getName().startsWith("invoke")) {
                        mode = method.getName();
                        request = (RemotingCommand) args[1];
                        return method.getName().equals("invokeSync") ? response : null;
                    }
                    return method.getReturnType() == boolean.class ? false : null;
                });
        }
    }

    private static void check(boolean ok, String label) {
        if (!ok) throw new IllegalStateException(label);
    }

    private static String quote(String value) {
        if (value == null) return "null";
        StringBuilder out = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            if (c == '"' || c == '\\') out.append('\\');
            if (c < 32) {
                out.append(String.format("\\u%04x", (int) c));
            } else {
                out.append(c);
            }
        }
        return out.append('"').toString();
    }

    private static String fields(Map<String, String> values) {
        StringBuilder out = new StringBuilder("{");
        if (values != null) {
            for (Map.Entry<String, String> entry : new TreeMap<>(values).entrySet()) {
                if (out.length() > 1) out.append(',');
                out.append(quote(entry.getKey())).append(':').append(quote(entry.getValue()));
            }
        }
        return out.append('}').toString();
    }

    private static String command(RemotingCommand cmd) {
        cmd.makeCustomHeaderToNet();
        return "{\"code\":" + cmd.getCode() + ",\"ext\":" + fields(cmd.getExtFields())
            + ",\"bodyBase64\":" + quote(cmd.getBody() == null ? null
                : Base64.getEncoder().encodeToString(cmd.getBody())) + "}";
    }

    private static String fixture(String name, Capture capture, int code, String outcome) {
        check(capture.request != null && capture.request.getCode() == code
            && ("invokeSync".equals(capture.mode) || "invokeOneway".equals(capture.mode)), name + " dispatch");
        return "{\"provenance\":{\"client\":\"org.apache.rocketmq:rocketmq-client:5.5.1\","
            + "\"entrypoint\":\"MQClientAPIImpl\",\"transport\":\"mock RemotingClient\","
            + "\"serverBaselineContacted\":false},\"case\":" + quote(name)
            + ",\"mode\":" + quote("invokeOneway".equals(capture.mode) ? "ONEWAY" : "SYNC")
            + ",\"request\":" + command(capture.request)
            + ",\"response\":" + (capture.response == null ? "null" : command(capture.response))
            + ",\"outcome\":" + quote(outcome) + "}\n";
    }

    private static String fixtureWithRequestLanguage(String name, Capture capture, int code,
        String outcome) {
        String fixture = fixture(name, capture, code, outcome);
        int outcomeIndex = fixture.lastIndexOf(",\"outcome\":");
        check(outcomeIndex >= 0, name + " outcome field");
        return fixture.substring(0, outcomeIndex) + ",\"requestLanguage\":"
            + quote(capture.request.getLanguage().name()) + fixture.substring(outcomeIndex);
    }

    public static void run(Path output, boolean verify) throws Exception {
        Capture capture = new Capture();
        ClientConfig config = new ClientConfig();
        config.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(new NettyClientConfig(), null, null, config,
            null, nettyConfig -> capture.proxy());
        List<String[]> cases = new ArrayList<>();

        TopicRouteData route = new TopicRouteData();
        route.setOrderTopicConf("oracle-order");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody(route.encode());
        check("oracle-order".equals(api.getTopicRouteInfoFromNameServer("oracle-topic", 10000)
            .getOrderTopicConf()), "route outcome");
        cases.add(new String[] {"route-success.json", fixture("route-success", capture,
            RequestCode.GET_ROUTEINFO_BY_TOPIC, "orderTopicConf=oracle-order")});

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.TOPIC_NOT_EXIST, "offline");
        try {
            api.getTopicRouteInfoFromNameServer("oracle-topic", 10000);
            throw new IllegalStateException("missing route accepted");
        } catch (MQClientException expected) {
            check(expected.getResponseCode() == ResponseCode.TOPIC_NOT_EXIST, "missing route code");
        }
        cases.add(new String[] {"route-not-found.json", fixture("route-not-found", capture,
            RequestCode.GET_ROUTEINFO_BY_TOPIC, "MQClientException;code=17")});

        try {
            api.getTopicRouteInfoFromNameServer("oracle-topic", 10000, false);
            throw new IllegalStateException("missing route accepted when disallowed");
        } catch (MQClientException expected) {
            check(expected.getResponseCode() == ResponseCode.TOPIC_NOT_EXIST,
                "explicit route-not-found code");
        }
        cases.add(new String[] {"route-not-found-explicit-false.json", fixture(
            "route-not-found-explicit-false", capture, RequestCode.GET_ROUTEINFO_BY_TOPIC,
            "MQClientException;code=17")});

        capture.response = RemotingCommand.createResponseCommand(987654, "unknown");
        try {
            api.getTopicRouteInfoFromNameServer("oracle-topic", 10000);
            throw new IllegalStateException("unknown route accepted");
        } catch (MQClientException expected) {
            check(expected.getResponseCode() == 987654, "unknown route code");
        }
        cases.add(new String[] {"route-unknown.json", fixture("route-unknown", capture,
            RequestCode.GET_ROUTEINFO_BY_TOPIC, "MQClientException;code=987654")});

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "no body");
        try {
            api.getTopicRouteInfoFromNameServer("oracle-topic", 10000);
            throw new IllegalStateException("bodyless route accepted");
        } catch (MQClientException expected) {
            check(expected.getResponseCode() == ResponseCode.SUCCESS, "bodyless route code");
        }
        cases.add(new String[] {"route-no-body.json", fixture("route-no-body", capture,
            RequestCode.GET_ROUTEINFO_BY_TOPIC, "MQClientException;code=0;malformed")});

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "invalid JSON");
        capture.response.setBody("{".getBytes(StandardCharsets.UTF_8));
        try {
            api.getTopicRouteInfoFromNameServer("oracle-topic", 10000);
            throw new IllegalStateException("invalid route accepted");
        } catch (RuntimeException expected) {
            check(expected.getClass().getName().startsWith("com.alibaba.fastjson2."),
                "invalid route parse failure");
        }
        cases.add(new String[] {"route-invalid-json.json", fixture("route-invalid-json", capture,
            RequestCode.GET_ROUTEINFO_BY_TOPIC, "JsonParseException;malformed")});

        SendMessageRequestHeader send = new SendMessageRequestHeader();
        send.setProducerGroup("producer");
        send.setTopic("oracle-topic");
        send.setDefaultTopic("TBW102");
        send.setDefaultTopicQueueNums(4);
        send.setQueueId(2);
        send.setSysFlag(0);
        send.setBornTimestamp(1L);
        send.setFlag(0);
        send.setProperties("");
        send.setReconsumeTimes(0);
        SendMessageResponseHeader sent = new SendMessageResponseHeader();
        sent.setMsgId("broker-message");
        sent.setQueueId(2);
        sent.setQueueOffset(17L);
        capture.response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, sent);
        capture.response.makeCustomHeaderToNet();
        SendResult result = api.sendMessage("broker:10911", "broker",
            new Message("oracle-topic", "payload".getBytes(StandardCharsets.UTF_8)), send, 10000,
            CommunicationMode.SYNC, null, null);
        check(result.getQueueOffset() == 17L && "broker-message".equals(result.getOffsetMsgId()),
            "send outcome");
        cases.add(new String[] {"send-success.json", fixture("send-success", capture,
            RequestCode.SEND_MESSAGE_V2, "SEND_OK;queueOffset=17;offsetMsgId=broker-message")});

        result = api.sendMessage("broker:10911", "broker",
            new Message("oracle-topic", "payload".getBytes(StandardCharsets.UTF_8)), send, 10000,
            CommunicationMode.SYNC, null, null, null, 0, null, null);
        check(result.getQueueOffset() == 17L && "broker-message".equals(result.getOffsetMsgId()),
            "full send overload outcome");
        cases.add(new String[] {"send-full-overload.json", fixture("send-full-overload", capture,
            RequestCode.SEND_MESSAGE_V2, "SEND_OK;queueOffset=17;offsetMsgId=broker-message")});

        capture.response = RemotingCommand.createResponseCommand(987654, "unknown");
        try {
            api.sendMessage("broker:10911", "broker",
                new Message("oracle-topic", "payload".getBytes(StandardCharsets.UTF_8)), send, 10000,
                CommunicationMode.SYNC, null, null);
            throw new IllegalStateException("unknown send accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == 987654, "unknown send code");
        }
        cases.add(new String[] {"send-unknown.json", fixture("send-unknown", capture,
            RequestCode.SEND_MESSAGE_V2, "MQBrokerException;code=987654")});

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "no header");
        try {
            api.sendMessage("broker:10911", "broker",
                new Message("oracle-topic", "payload".getBytes(StandardCharsets.UTF_8)), send, 10000,
                CommunicationMode.SYNC, null, null);
            throw new IllegalStateException("headerless send accepted");
        } catch (Exception expected) {
            check(expected instanceof NullPointerException,
                "headerless send exception: " + expected);
        }
        cases.add(new String[] {"send-no-header.json", fixture("send-no-header", capture,
            RequestCode.SEND_MESSAGE_V2, "NullPointerException;malformed")});

        QueryConsumerOffsetRequestHeader offset = new QueryConsumerOffsetRequestHeader();
        offset.setConsumerGroup("consumer");
        offset.setTopic("oracle-topic");
        offset.setQueueId(2);
        QueryConsumerOffsetResponseHeader offsetReply = new QueryConsumerOffsetResponseHeader();
        offsetReply.setOffset(42L);
        capture.response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, offsetReply);
        capture.response.makeCustomHeaderToNet();
        check(api.queryConsumerOffset("broker:10911", offset, 10000) == 42L, "offset outcome");
        cases.add(new String[] {"offset-success.json", fixture("offset-success", capture,
            RequestCode.QUERY_CONSUMER_OFFSET, "offset=42")});

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.QUERY_NOT_FOUND, "offline");
        try {
            api.queryConsumerOffset("broker:10911", offset, 10000);
            throw new IllegalStateException("missing offset accepted");
        } catch (OffsetNotFoundException expected) {
            check(expected.getResponseCode() == ResponseCode.QUERY_NOT_FOUND, "missing offset outcome");
        }
        cases.add(new String[] {"offset-not-found.json", fixture("offset-not-found", capture,
            RequestCode.QUERY_CONSUMER_OFFSET, "OffsetNotFoundException;code=22")});

        capture.response = RemotingCommand.createResponseCommand(987654, "unknown");
        try {
            api.queryConsumerOffset("broker:10911", offset, 10000);
            throw new IllegalStateException("unknown offset accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == 987654, "unknown offset code");
        }
        cases.add(new String[] {"offset-unknown.json", fixture("offset-unknown", capture,
            RequestCode.QUERY_CONSUMER_OFFSET, "MQBrokerException;code=987654")});

        ResetOffsetBody resetBody = new ResetOffsetBody();
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody(RemotingSerializable.encode(resetBody));
        check(api.invokeBrokerToResetOffset("broker:10911", "oracle-topic", "workers",
            1712345678901L, true, 10000, true).isEmpty(), "C++ reset offset outcome");
        cases.add(new String[] {"reset-offset-cpp.json", fixtureWithRequestLanguage(
            "reset-offset-cpp", capture, RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, "ok")});

        ResetOffsetBody explicitResetBody = new ResetOffsetBody();
        explicitResetBody.getOffsetTable().put(new org.apache.rocketmq.common.message.MessageQueue(
            "oracle-topic", "broker-a", 3), 27L);
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody(RemotingSerializable.encode(explicitResetBody));
        java.util.Map<org.apache.rocketmq.common.message.MessageQueue, Long> explicitOffsets =
            api.invokeBrokerToResetOffset("broker:10911", "oracle-topic", "workers",
                1712345678901L, 3, 27L, 10000);
        check(explicitOffsets.size() == 1 && explicitOffsets.values().iterator().next() == 27L,
            "explicit reset offset outcome");
        cases.add(new String[] {"reset-offset-explicit.json", fixture("reset-offset-explicit",
            capture, RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, "offset=27")});

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody(RemotingSerializable.encode(new ResetOffsetBody()));
        check(api.invokeBrokerToResetOffset("broker:10911", "oracle-topic", "workers",
            1712345678901L, true, 10000).isEmpty(), "default reset offset overload outcome");
        cases.add(new String[] {"reset-offset-default.json", fixture("reset-offset-default",
            capture, RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, "ok")});

        ConsumeStats emptyConsumeStats = new ConsumeStats();
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody(RemotingSerializable.encode(emptyConsumeStats));
        ConsumeStats stats = api.getConsumeStats("broker:10911", "workers", "orders",
            java.util.Arrays.asList("orders", null), 10000);
        check(stats.getOffsetTable().isEmpty() && stats.getConsumeTps() == 0.0,
            "synchronous consume stats outcome");
        cases.add(new String[] {"consume-stats-topic-list.json", fixture("consume-stats-topic-list",
            capture, RequestCode.GET_CONSUME_STATS, "ok")});

        stats = api.getConsumeStats("broker:10911", "workers", 10000);
        check(stats.getOffsetTable().isEmpty(), "default consume stats overload outcome");
        cases.add(new String[] {"consume-stats-default.json", fixture("consume-stats-default",
            capture, RequestCode.GET_CONSUME_STATS, "ok")});

        stats = api.getConsumeStats("broker:10911", "workers",
            java.util.Arrays.asList("orders", "payments"), 10000);
        check(stats.getOffsetTable().isEmpty(), "topic-list consume stats overload outcome");
        cases.add(new String[] {"consume-stats-list.json", fixture("consume-stats-list",
            capture, RequestCode.GET_CONSUME_STATS, "ok")});

        stats = api.getConsumeStats("broker:10911", "workers", "orders", 10000);
        check(stats.getOffsetTable().isEmpty(), "topic consume stats overload outcome");
        cases.add(new String[] {"consume-stats-topic.json", fixture("consume-stats-topic",
            capture, RequestCode.GET_CONSUME_STATS, "ok")});

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        api.deleteTopicInNameServer("namesrv:9876", "oracle-topic", 10000);
        cases.add(new String[] {"delete-topic-nameserver.json", fixture("delete-topic-nameserver",
            capture, RequestCode.DELETE_TOPIC_IN_NAMESRV, "ok")});

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        api.deleteTopicInNameServer("namesrv:9876", "cluster-a", "oracle-topic", 10000);
        cases.add(new String[] {"delete-topic-nameserver-cluster.json", fixture(
            "delete-topic-nameserver-cluster", capture, RequestCode.DELETE_TOPIC_IN_NAMESRV,
            "ok")});

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "no header");
        try {
            api.queryConsumerOffset("broker:10911", offset, 10000);
            throw new IllegalStateException("headerless offset accepted");
        } catch (NullPointerException expected) {
            // The pinned Java implementation dereferences the absent decoded header.
        }
        cases.add(new String[] {"offset-no-header.json", fixture("offset-no-header", capture,
            RequestCode.QUERY_CONSUMER_OFFSET, "NullPointerException;malformed")});

        SearchOffsetResponseHeader searchReply = new SearchOffsetResponseHeader();
        searchReply.setOffset(42L);
        capture.response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, searchReply);
        capture.response.makeCustomHeaderToNet();
        check(api.searchOffset("broker:10911", "oracle-topic", 2, -7L, 10000) == 42L,
            "deprecated search offset overload outcome");
        cases.add(new String[] {"search-offset-deprecated.json", fixture("search-offset-deprecated",
            capture, RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, "offset=42")});

        MessageQueue searchQueue = new MessageQueue("oracle-topic", "broker", 2);
        check(api.searchOffset("broker:10911", searchQueue, -7L, 10000) == 42L,
            "default-boundary search offset overload outcome");
        cases.add(new String[] {"search-offset-default-boundary.json", fixture(
            "search-offset-default-boundary", capture, RequestCode.SEARCH_OFFSET_BY_TIMESTAMP,
            "offset=42")});

        UpdateConsumerOffsetRequestHeader update = new UpdateConsumerOffsetRequestHeader();
        update.setConsumerGroup("consumer");
        update.setTopic("oracle-topic");
        update.setQueueId(2);
        update.setCommitOffset(42L);
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        api.updateConsumerOffset("broker:10911", update, 10000);
        cases.add(new String[] {"update-success.json", fixture("update-success", capture,
            RequestCode.UPDATE_CONSUMER_OFFSET, "ok")});

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.updateConsumerOffset("broker:10911", update, 10000);
            throw new IllegalStateException("update error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "update error code");
        }
        cases.add(new String[] {"update-error.json", fixture("update-error", capture,
            RequestCode.UPDATE_CONSUMER_OFFSET, "MQBrokerException;code=1")});

        capture.response = RemotingCommand.createResponseCommand(987654, "unknown");
        try {
            api.updateConsumerOffset("broker:10911", update, 10000);
            throw new IllegalStateException("unknown update accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == 987654, "unknown update code");
        }
        cases.add(new String[] {"update-unknown.json", fixture("update-unknown", capture,
            RequestCode.UPDATE_CONSUMER_OFFSET, "MQBrokerException;code=987654")});

        capture.response = null;
        api.updateConsumerOffsetOneway("broker:10911", update, 10000);
        cases.add(new String[] {"update-oneway.json", fixture("update-oneway", capture,
            RequestCode.UPDATE_CONSUMER_OFFSET, "ok")});

        for (String[] item : cases) {
            Path file = output.resolve(item[0]);
            if (verify) {
                check(Files.exists(file) && item[1].equals(Files.readString(file)), "fixture drift: " + file);
            } else {
                Files.createDirectories(output);
                Files.writeString(file, item[1], StandardCharsets.UTF_8);
            }
        }
    }
}
