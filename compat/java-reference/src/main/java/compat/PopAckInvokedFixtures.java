package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.remoting.*;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.*;
import org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody;
import org.apache.rocketmq.remoting.protocol.header.*;

/** Actual pinned domain callbacks, with only RemotingClient replaced. */
public final class PopAckInvokedFixtures {
    private static void check(boolean value, String label) {
        if (!value) throw new IllegalStateException(label);
    }
    private static Map<String, Object> command(RemotingCommand cmd) {
        cmd.makeCustomHeaderToNet();
        Map<String, Object> result = new TreeMap<>();
        result.put("code", cmd.getCode());
        result.put("ext", cmd.getExtFields() == null ? Collections.emptyMap() : new TreeMap<>(cmd.getExtFields()));
        result.put("bodyBase64", cmd.getBody() == null ? null : Base64.getEncoder().encodeToString(cmd.getBody()));
        return result;
    }
    public static void main(String[] args) throws Exception {
        Path dir = Path.of(args[0]);
        boolean verify = Arrays.asList(args).contains("--verify");
        RemotingCommand[] request = new RemotingCommand[1];
        InvokeCallback[] completion = new InvokeCallback[1];
        RemotingClient transport = (RemotingClient) Proxy.newProxyInstance(RemotingClient.class.getClassLoader(),
            new Class<?>[] {RemotingClient.class}, (proxy, method, arguments) -> {
                if (method.getName().startsWith("invoke")) {
                    check(method.getName().equals("invokeAsync"), "async mode");
                    request[0] = (RemotingCommand) arguments[1];
                    completion[0] = (InvokeCallback) arguments[3];
                }
                return method.getReturnType() == boolean.class ? false : null;
            });
        ClientConfig config = new ClientConfig();
        config.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(new NettyClientConfig(), null, null, config,
            null, ignored -> transport);
        PopMessageRequestHeader pop = new PopMessageRequestHeader();
        pop.setConsumerGroup("consumer"); pop.setTopic("oracle-topic"); pop.setQueueId(2);
        pop.setMaxMsgNums(1); pop.setInvisibleTime(456L); pop.setBornTime(123L);
        PopLiteMessageRequestHeader lite = new PopLiteMessageRequestHeader();
        lite.setClientId("client"); lite.setConsumerGroup("consumer"); lite.setTopic("oracle-topic");
        lite.setMaxMsgNum(1); lite.setInvisibleTime(456L); lite.setBornTime(123L);
        AckMessageRequestHeader ack = new AckMessageRequestHeader();
        ack.setConsumerGroup("consumer"); ack.setTopic("oracle-topic"); ack.setQueueId(2);
        ack.setOffset(17L); ack.setExtraInfo("checkpoint");
        BatchAckMessageRequestBody batch = new BatchAckMessageRequestBody();
        batch.setBrokerName("broker"); batch.setAcks(Collections.emptyList());
        ChangeInvisibleTimeRequestHeader change = new ChangeInvisibleTimeRequestHeader();
        change.setConsumerGroup("consumer"); change.setTopic("oracle-topic"); change.setQueueId(2);
        change.setOffset(17L); change.setExtraInfo("checkpoint"); change.setInvisibleTime(456L);
        MessageExt message = new MessageExt();
        message.setTopic("oracle-topic"); message.setBody(new byte[] {42});
        message.setQueueId(2); message.setQueueOffset(17L);
        message.setBornHost(new java.net.InetSocketAddress("127.0.0.1", 1001));
        message.setStoreHost(new java.net.InetSocketAddress("127.0.0.1", 10911));
        byte[] rawMessage = MessageDecoder.encode(message, false);
        String[] families = {"pop", "pop-lite", "ack", "ack-lite", "batch-ack", "change"};
        for (int kind = 0; kind < families.length; kind++) {
            int[] statuses = kind < 2 ? new int[] {0, ResponseCode.POLLING_FULL,
                ResponseCode.POLLING_TIMEOUT, ResponseCode.PULL_NOT_FOUND, 1, 987654}
                : new int[] {0, 1, 987654};
            for (int code : statuses) {
                // Missing body and invalid numeric header exercise decoded success paths.
                String[] shapes = kind < 2 ? new String[] {"normal", "malformed", "header-default"}
                    : kind == 5 ? new String[] {"normal", "malformed"}
                    : new String[] {"normal"};
                for (String shape : shapes) {
                    if (!shape.equals("normal") && code != 0) continue;
                    String[] outcome = new String[1];
                    Throwable[] error = new Throwable[1];
                    final boolean fullPop = kind == 0;
                    PopCallback pc = new PopCallback() {
                        public void onSuccess(PopResult result) {
                            check(result.getRestNum() == (fullPop && !shape.equals("header-default") ? 7 : 0), "rest count");
                            if (code == 0) {
                                check(result.getMsgFoundList().size() == 1, "decoded message count");
                                check(result.getMsgFoundList().get(0).getQueueOffset() == 17L, "message offset");
                                if (fullPop) check(result.getPopTime() == (shape.equals("header-default") ? 0 : 123)
                                    && result.getInvisibleTime() == (shape.equals("header-default") ? 0 : 456), "pop metadata");
                            }
                            outcome[0] = "status=" + result.getPopStatus();
                        }
                        public void onException(Throwable value) { error[0] = value; }
                    };
                    AckCallback ac = new AckCallback() {
                        public void onSuccess(AckResult result) {
                            outcome[0] = "status=" + result.getStatus();
                            if (result.getExtraInfo() != null) outcome[0] += ";extra=" + result.getExtraInfo();
                        }
                        public void onException(Throwable value) { error[0] = value; }
                    };
                    switch (kind) {
                        case 0: api.popMessageAsync("broker", "broker:10911", pop, 10000, pc); break;
                        case 1: api.popLiteMessageAsync("broker", "broker:10911", lite, 10000, pc); break;
                        case 2: api.ackMessageAsync("broker:10911", 10000, ac, ack); break;
                        case 3: api.ackLiteMessageAsync("broker:10911", 10000, ac, ack); break;
                        case 4: api.batchAckMessageAsync("broker:10911", 10000, ac, batch); break;
                        default: api.changeInvisibleTimeAsync("broker", "broker:10911", change, 10000, ac);
                    }
                    RemotingCommand response = RemotingCommand.createResponseCommand(code, "offline");
                    if (kind < 2 || kind == 5) {
                        response.addExtField("popTime", "123"); response.addExtField("invisibleTime", "456");
                        response.addExtField("reviveQid", "2");
                        if (kind == 0) response.addExtField("restNum", "7");
                        if (shape.equals("malformed") && kind == 5) response.addExtField("popTime", "bad");
                        if (kind < 2 && code == 0 && !shape.equals("malformed")) response.setBody(rawMessage);
                        if (shape.equals("header-default")) {
                            response.getExtFields().remove("popTime");
                            response.addExtField("invisibleTime", "NaN");
                            response.addExtField("reviveQid", "2147483648");
                            if (kind == 0) response.addExtField("restNum", "99999999999999999999999999");
                        }
                    }
                    completion[0].operationSucceed(response);
                    if (error[0] != null) outcome[0] = error[0] instanceof MQBrokerException
                        ? "error=" + ((MQBrokerException) error[0]).getResponseCode() : "malformed";
                    check(outcome[0] != null, "callback completion");
                    if (shape.equals("malformed") && kind == 5)
                        check(outcome[0].equals("status=OK;extra=17 0 456 2 0 broker 2 17"), "invalid primitive defaults");
                    else if (shape.equals("malformed")) check(outcome[0].equals("malformed"), "malformed rejected");
                    else if (kind < 2) check(outcome[0].equals(code == 0 ? "status=FOUND"
                        : code == ResponseCode.POLLING_FULL ? "status=POLLING_FULL"
                        : code == ResponseCode.POLLING_TIMEOUT || code == ResponseCode.PULL_NOT_FOUND
                            ? "status=POLLING_NOT_FOUND" : "error=" + code), "pop outcome");
                    else check(outcome[0].startsWith(code == 0 ? "status=OK" : "status=NO_EXIST"), "ack outcome");
                    String name = families[kind] + "-" + code + "-" + shape;
                    Map<String, Object> fixture = new TreeMap<>();
                    fixture.put("case", name); fixture.put("family", families[kind]); fixture.put("mode", "ASYNC");
                    fixture.put("provenance", new TreeMap<>(Map.of("client", "org.apache.rocketmq:rocketmq-client:5.5.1",
                        "entrypoint", "MQClientAPIImpl", "transport", "mock RemotingClient", "serverBaselineContacted", false)));
                    fixture.put("request", command(request[0])); fixture.put("response", command(response));
                    fixture.put("outcome", outcome[0]);
                    String json = JSON.toJSONString(fixture) + "\n";
                    Path file = dir.resolve(name + ".json");
                    if (verify) check(Files.exists(file) && Files.readString(file).equals(json), "fixture drift " + name);
                    else { Files.createDirectories(dir); Files.writeString(file, json); }
                }
            }
        }

        List<String> extraInfoList = Arrays.asList(
            "10 123 456 2 0 broker-a 1 10",
            "10 123 456 2 0 broker-a 1 12",
            "20 200 300 3 2 broker-a 2 21",
            "10 1 456 2 0 broker-a 1 10",
            "9223372036854775807 123 456 2 1 broker-a 4 -9223372036854775808");
        String[] batchOutcome = new String[1];
        Throwable[] batchError = new Throwable[1];
        api.batchAckMessageAsync("broker:10911", 10000, new AckCallback() {
            public void onSuccess(AckResult result) {
                batchOutcome[0] = "status=" + result.getStatus();
            }
            public void onException(Throwable value) { batchError[0] = value; }
        }, "orders", "workers", extraInfoList);
        RemotingCommand batchResponse = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        completion[0].operationSucceed(batchResponse);
        if (batchError[0] != null || !"status=OK".equals(batchOutcome[0])) {
            throw new IllegalStateException("batch ACK extra-info outcome " + batchError[0]);
        }
        Map<String, Object> batchFixture = new TreeMap<>();
        batchFixture.put("case", "batch-ack-extra-info");
        batchFixture.put("family", "batch-ack-extra-info");
        batchFixture.put("mode", "ASYNC");
        batchFixture.put("provenance", new TreeMap<>(Map.of("client", "org.apache.rocketmq:rocketmq-client:5.5.1",
            "entrypoint", "MQClientAPIImpl", "transport", "mock RemotingClient", "serverBaselineContacted", false)));
        batchFixture.put("input", new TreeMap<>(Map.of("topic", "orders", "consumerGroup", "workers",
            "extraInfoList", extraInfoList)));
        batchFixture.put("request", command(request[0]));
        batchFixture.put("response", command(batchResponse));
        batchFixture.put("outcome", batchOutcome[0]);
        String batchJson = JSON.toJSONString(batchFixture) + "\n";
        Path batchFile = dir.resolve("batch-ack-extra-info.json");
        if (verify) check(Files.exists(batchFile) && Files.readString(batchFile).equals(batchJson),
            "fixture drift batch-ack-extra-info");
        else { Files.createDirectories(dir); Files.writeString(batchFile, batchJson); }

        for (String topic : new String[] {"oracle-topic", "%RETRY%consumer", "%RETRY%consumer+P1"}) {
            change.setTopic(topic);
            AckResult[] ackResult = new AckResult[1];
            Throwable[] error = new Throwable[1];
            api.changeInvisibleTimeAsync("broker", "broker:10911", change, 10000, new AckCallback() {
                public void onSuccess(AckResult value) { ackResult[0] = value; }
                public void onException(Throwable value) { error[0] = value; }
            });
            ChangeInvisibleTimeResponseHeader header = new ChangeInvisibleTimeResponseHeader();
            header.setPopTime(123L);
            header.setInvisibleTime(456L);
            header.setReviveQid(2);
            RemotingCommand response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, header);
            response.makeCustomHeaderToNet();
            completion[0].operationSucceed(response);
            String retry = topic.contains("+") ? "2" : topic.startsWith("%RETRY%") ? "1" : "0";
            check(error[0] == null && ackResult[0] != null
                && ("17 123 456 2 " + retry + " broker 2 17").equals(ackResult[0].getExtraInfo()),
                "change invisible retry extra info " + topic);
        }
    }
}
