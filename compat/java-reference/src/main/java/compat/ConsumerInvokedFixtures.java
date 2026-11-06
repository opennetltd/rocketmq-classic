package compat;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody;

/** Consumer RPC evidence collected at MQClientAPIImpl with only RemotingClient mocked. */
public final class ConsumerInvokedFixtures {
    private ConsumerInvokedFixtures() { }

    private static final class Capture {
        RemotingCommand request;
        RemotingCommand response;
        String mode;

        RemotingClient proxy() {
            return (RemotingClient) Proxy.newProxyInstance(RemotingClient.class.getClassLoader(),
                new Class<?>[] {RemotingClient.class}, (proxy, method, args) -> {
                    if (method.getName().startsWith("invoke")) {
                        mode = method.getName();
                        request = (RemotingCommand) args[1];
                        return "invokeSync".equals(mode) ? response : null;
                    }
                    return method.getReturnType() == boolean.class ? false : null;
                });
        }
    }

    private static void check(boolean condition, String label) {
        if (!condition) throw new IllegalStateException(label);
    }

    private static Map<String, Object> command(RemotingCommand command) {
        command.makeCustomHeaderToNet();
        Map<String, Object> result = new TreeMap<>();
        result.put("code", command.getCode());
        result.put("ext", command.getExtFields() == null ? Collections.emptyMap()
            : new TreeMap<>(command.getExtFields()));
        result.put("bodyBase64", command.getBody() == null ? null
            : Base64.getEncoder().encodeToString(command.getBody()));
        return result;
    }

    private static void record(Path output, boolean verify, Capture capture, String name,
        int code, String outcome) throws Exception {
        check("invokeSync".equals(capture.mode) && capture.request != null
            && capture.request.getCode() == code, name + " dispatch");
        Map<String, Object> fixture = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("entrypoint", "MQClientAPIImpl");
        provenance.put("transport", "mock RemotingClient");
        provenance.put("serverBaselineContacted", false);
        fixture.put("provenance", provenance);
        fixture.put("case", name);
        fixture.put("mode", "SYNC");
        fixture.put("request", command(capture.request));
        fixture.put("response", command(capture.response));
        fixture.put("outcome", outcome);
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture) + "\n";
        Path file = output.resolve(name + ".json");
        if (verify) {
            check(Files.exists(file) && json.equals(Files.readString(file)), "fixture drift: " + file);
        } else {
            Files.createDirectories(output);
            Files.writeString(file, json);
        }
    }

    public static void main(String[] args) throws Exception {
        check(args.length >= 1, "expected output directory");
        Path output = Path.of(args[0]);
        boolean verify = Arrays.asList(args).contains("--verify");
        Capture capture = new Capture();
        ClientConfig config = new ClientConfig();
        config.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(new NettyClientConfig(), null, null, config,
            null, nettyConfig -> capture.proxy());

        GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
        body.setConsumerIdList(Arrays.asList("client-a", "client-b"));
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody(body.encode());
        check(api.getConsumerIdListByGroup("broker:10911", "consumer", 10000)
            .equals(Arrays.asList("client-a", "client-b")), "consumer list populated");
        record(output, verify, capture, "consumer-list-populated", RequestCode.GET_CONSUMER_LIST_BY_GROUP,
            "clients=client-a,client-b");

        body.setConsumerIdList(new ArrayList<>());
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody(body.encode());
        check(api.getConsumerIdListByGroup("broker:10911", "consumer", 10000).isEmpty(),
            "consumer list empty");
        record(output, verify, capture, "consumer-list-empty", RequestCode.GET_CONSUMER_LIST_BY_GROUP,
            "clients=");

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "no body");
        try {
            api.getConsumerIdListByGroup("broker:10911", "consumer", 10000);
            throw new IllegalStateException("bodyless consumer list accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SUCCESS, "bodyless consumer list code");
        }
        record(output, verify, capture, "consumer-list-no-body", RequestCode.GET_CONSUMER_LIST_BY_GROUP,
            "MQBrokerException;code=0;malformed");

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.getConsumerIdListByGroup("broker:10911", "consumer", 10000);
            throw new IllegalStateException("consumer list error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "consumer list error code");
        }
        record(output, verify, capture, "consumer-list-error", RequestCode.GET_CONSUMER_LIST_BY_GROUP,
            "MQBrokerException;code=1");

        capture.response = RemotingCommand.createResponseCommand(987654, "offline");
        try {
            api.getConsumerIdListByGroup("broker:10911", "consumer", 10000);
            throw new IllegalStateException("unknown consumer list code accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == 987654, "unknown consumer list code");
        }
        record(output, verify, capture, "consumer-list-unknown", RequestCode.GET_CONSUMER_LIST_BY_GROUP,
            "MQBrokerException;code=987654");

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody("{".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        try {
            api.getConsumerIdListByGroup("broker:10911", "consumer", 10000);
            throw new IllegalStateException("malformed consumer list accepted");
        } catch (com.alibaba.fastjson2.JSONException expected) {
            // The pinned client propagates the JSON decoder's failure.
        }
        record(output, verify, capture, "consumer-list-invalid-json", RequestCode.GET_CONSUMER_LIST_BY_GROUP,
            "JSONException;malformed");

        MessageExt retry = new MessageExt();
        retry.setTopic("oracle-topic");
        retry.setCommitLogOffset(19L);
        retry.setMsgId("retry-id");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        api.consumerSendMessageBack("broker:10911", "broker", retry, "consumer", 3, 10000, 5);
        record(output, verify, capture, "send-back-success", RequestCode.CONSUMER_SEND_MSG_BACK, "ok");

        for (int code : new int[] {ResponseCode.SYSTEM_ERROR, 987654}) {
            capture.response = RemotingCommand.createResponseCommand(code, "offline");
            try {
                api.consumerSendMessageBack("broker:10911", "broker", retry, "consumer", 3, 10000, 5);
                throw new IllegalStateException("send back error accepted");
            } catch (MQBrokerException expected) {
                check(expected.getResponseCode() == code, "send back error code");
            }
            record(output, verify, capture, code == 987654 ? "send-back-unknown" : "send-back-error",
                RequestCode.CONSUMER_SEND_MSG_BACK, "MQBrokerException;code=" + code);
        }
    }
}