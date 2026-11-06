package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;

/** Producer RPC evidence from actual MQClientAPIImpl dispatch with only transport mocked. */
public final class ProducerMoreInvokedFixtures {
    private ProducerMoreInvokedFixtures() { }

    private static final class Capture {
        RemotingCommand request;
        RemotingCommand response;
        String mode;
        InvokeCallback asyncCallback;

        RemotingClient proxy() {
            return (RemotingClient) Proxy.newProxyInstance(RemotingClient.class.getClassLoader(),
                new Class<?>[] {RemotingClient.class}, (proxy, method, args) -> {
                    if (method.getName().startsWith("invoke")) {
                        mode = method.getName();
                        request = (RemotingCommand) args[1];
                        if ("invokeAsync".equals(mode)) asyncCallback = (InvokeCallback) args[3];
                        return "invokeSync".equals(mode) ? response : null;
                    }
                    return method.getReturnType() == boolean.class ? false : null;
                });
        }
    }

    private static void check(boolean ok, String label) {
        if (!ok) throw new IllegalStateException(label);
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
        check(capture.request != null && capture.request.getCode() == code, name + " request code");
        check("invokeSync".equals(capture.mode) || "invokeOneway".equals(capture.mode)
            || "invokeAsync".equals(capture.mode), name + " mode");
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("entrypoint", "MQClientAPIImpl");
        provenance.put("transport", "mock RemotingClient");
        provenance.put("serverBaselineContacted", false);
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("provenance", provenance);
        fixture.put("case", name);
        fixture.put("mode", "invokeSync".equals(capture.mode) ? "SYNC"
            : "invokeAsync".equals(capture.mode) ? "ASYNC" : "ONEWAY");
        fixture.put("request", command(capture.request));
        fixture.put("response", "invokeOneway".equals(capture.mode) ? null : command(capture.response));
        fixture.put("outcome", outcome);
        String json = JSON.toJSONString(fixture) + "\n";
        Path path = output.resolve(name + ".json");
        if (verify) {
            check(Files.exists(path) && json.equals(Files.readString(path)), "fixture drift: " + path);
        } else {
            Files.createDirectories(output);
            Files.writeString(path, json);
        }
    }

    public static void main(String[] args) throws Exception {
        run(Path.of(args[0]), Arrays.asList(args).contains("--verify"));
    }

    private static SendResult send(Capture capture, MQClientAPIImpl api, Message message,
        SendMessageRequestHeader header, CommunicationMode mode) throws Exception {
        if (mode != CommunicationMode.ASYNC)
            return api.sendMessage("broker:10911", "broker", message, header, 10000, mode, null, null);
        SendResult[] result = new SendResult[1];
        Throwable[] error = new Throwable[1];
        SendCallback callback = new SendCallback() {
            public void onSuccess(SendResult value) { result[0] = value; }
            public void onException(Throwable value) { error[0] = value; }
        };
        DefaultMQProducerImpl producer = new DefaultMQProducerImpl(new DefaultMQProducer("oracle-producer"));
        check(api.sendMessage("broker:10911", "broker", message, header, 10000, mode,
            callback, null, null, 0, null, producer) == null, "async dispatch result");
        capture.asyncCallback.operationSucceed(capture.response);
        if (error[0] instanceof Exception) throw (Exception) error[0];
        check(error[0] == null && result[0] != null, "async terminal result");
        return result[0];
    }

    public static void run(Path output, boolean verify) throws Exception {
        Capture capture = new Capture();
        ClientConfig config = new ClientConfig();
        config.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(new NettyClientConfig(), null, null, config,
            null, nettyConfig -> capture.proxy());
        SendMessageRequestHeader header = new SendMessageRequestHeader();
        header.setProducerGroup("producer");
        header.setTopic("oracle-topic");
        header.setDefaultTopic("TBW102");
        header.setDefaultTopicQueueNums(4);
        header.setQueueId(2);
        header.setSysFlag(0);
        header.setBornTimestamp(1L);
        header.setFlag(0);
        header.setProperties("");
        header.setReconsumeTimes(0);
        Message message = new Message("oracle-topic", "payload".getBytes(StandardCharsets.UTF_8));
        SendMessageResponseHeader sent = new SendMessageResponseHeader();
        sent.setMsgId("broker-message");
        sent.setQueueId(2);
        sent.setQueueOffset(17L);
        java.lang.reflect.Field smart = MQClientAPIImpl.class.getDeclaredField("sendSmartMsg");
        smart.setAccessible(true);
        boolean original = smart.getBoolean(null);
        try {
            for (boolean enabled : new boolean[] {false, true}) {
                smart.setBoolean(null, enabled);
                String variant = enabled ? "v2" : "v1";
                for (boolean reply : new boolean[] {false, true}) {
                    Message current = reply
                        ? new Message("oracle-topic", "reply".getBytes(StandardCharsets.UTF_8)) : message;
                    if (reply) MessageAccessor.putProperty(current, MessageConst.PROPERTY_MESSAGE_TYPE,
                        MixAll.REPLY_MESSAGE_FLAG);
                    for (CommunicationMode mode : new CommunicationMode[] {
                        CommunicationMode.SYNC, CommunicationMode.ASYNC, CommunicationMode.ONEWAY}) {
                        capture.response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, sent);
                        capture.response.makeCustomHeaderToNet();
                        SendResult result = send(capture, api, current, header, mode);
                        check(mode != CommunicationMode.ONEWAY || result == null, "oneway result");
                        check(mode == CommunicationMode.ONEWAY || result.getRawRespBody() == null,
                            "raw response body default");
                        String outcome = mode == CommunicationMode.ONEWAY ? "ok"
                            : result.getSendStatus().name() + ";queueOffset=" + result.getQueueOffset()
                                + ";offsetMsgId=" + result.getOffsetMsgId();
                        record(output, verify, capture, "send-" + (reply ? "reply-" : "") + variant
                            + "-" + mode.name().toLowerCase(), reply
                            ? (enabled ? RequestCode.SEND_REPLY_MESSAGE_V2 : RequestCode.SEND_REPLY_MESSAGE)
                            : (enabled ? RequestCode.SEND_MESSAGE_V2 : RequestCode.SEND_MESSAGE), outcome);
                    }
                }
                header.setBatch(true);
                MessageBatch batch = MessageBatch.generateFromList(Collections.singletonList(message));
                batch.setBody(batch.encode());
                for (CommunicationMode mode : new CommunicationMode[] {
                    CommunicationMode.SYNC, CommunicationMode.ASYNC, CommunicationMode.ONEWAY}) {
                    capture.response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, sent);
                    capture.response.makeCustomHeaderToNet();
                    SendResult result = send(capture, api, batch, header, mode);
                    check(mode != CommunicationMode.ONEWAY || result == null, "batch oneway result");
                    check(mode == CommunicationMode.ONEWAY || result.getRawRespBody() == null,
                        "batch raw response body default");
                    record(output, verify, capture, "send-batch-" + variant + "-" + mode.name().toLowerCase(),
                        RequestCode.SEND_BATCH_MESSAGE, mode == CommunicationMode.ONEWAY ? "ok"
                            : result.getSendStatus().name() + ";queueOffset=" + result.getQueueOffset()
                                + ";offsetMsgId=" + result.getOffsetMsgId());
                }
                header.setBatch(false);
            }
            smart.setBoolean(null, true);
            capture.response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS,
                new SendMessageResponseHeader());
            for (CommunicationMode mode : new CommunicationMode[] {CommunicationMode.SYNC, CommunicationMode.ASYNC}) {
            for (int code : new int[] {ResponseCode.SUCCESS, 999}) {
                capture.response.setCode(code);
                String outcome;
                try {
                    send(capture, api, message, header, mode);
                    throw new IllegalStateException("malformed response accepted");
                } catch (MQBrokerException expected) {
                    outcome = "MQBrokerException;code=" + expected.getResponseCode();
                } catch (org.apache.rocketmq.remoting.exception.RemotingCommandException expected) {
                    outcome = "RemotingCommandException";
                } catch (NullPointerException expected) {
                    outcome = "NullPointerException";
                }
                record(output, verify, capture, (code == 999 ? "send-v2-unknown" : "send-v2-malformed")
                    + (mode == CommunicationMode.ASYNC ? "-async" : ""),
                    RequestCode.SEND_MESSAGE_V2, outcome);
            }
            }
        } finally {
            smart.setBoolean(null, original);
        }
    }
}
