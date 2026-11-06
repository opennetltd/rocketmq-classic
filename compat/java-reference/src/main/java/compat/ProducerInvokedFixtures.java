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
public final class ProducerInvokedFixtures {
    private ProducerInvokedFixtures() { }

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
        check("invokeSync".equals(capture.mode) || "invokeOneway".equals(capture.mode), name + " mode");
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("entrypoint", "MQClientAPIImpl");
        provenance.put("transport", "mock RemotingClient");
        provenance.put("serverBaselineContacted", false);
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("provenance", provenance);
        fixture.put("case", name);
        fixture.put("mode", "invokeSync".equals(capture.mode) ? "SYNC" : "ONEWAY");
        fixture.put("request", command(capture.request));
        fixture.put("response", "invokeSync".equals(capture.mode) ? command(capture.response) : null);
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
        for (int code : new int[] {ResponseCode.SUCCESS, ResponseCode.FLUSH_DISK_TIMEOUT,
            ResponseCode.FLUSH_SLAVE_TIMEOUT, ResponseCode.SLAVE_NOT_AVAILABLE}) {
            capture.response = RemotingCommand.createResponseCommandWithHeader(code, sent);
            capture.response.makeCustomHeaderToNet();
            SendResult result = api.sendMessage("broker:10911", "broker", message, header, 10000,
                CommunicationMode.SYNC, null, null);
            check(result.getQueueOffset() == 17 && "broker-message".equals(result.getOffsetMsgId()),
                "send status result " + code);
            SendStatus expected = code == ResponseCode.SUCCESS ? SendStatus.SEND_OK
                : code == ResponseCode.FLUSH_DISK_TIMEOUT ? SendStatus.FLUSH_DISK_TIMEOUT
                : code == ResponseCode.FLUSH_SLAVE_TIMEOUT ? SendStatus.FLUSH_SLAVE_TIMEOUT
                : SendStatus.SLAVE_NOT_AVAILABLE;
            check(result.getSendStatus() == expected, "send status " + code);
            String status = result.getSendStatus().name();
            record(output, verify, capture, "send-v2-" + status.toLowerCase().replace('_', '-'),
                RequestCode.SEND_MESSAGE_V2, status + ";queueOffset=17;offsetMsgId=broker-message");
        }
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.sendMessage("broker:10911", "broker", message, header, 10000, CommunicationMode.SYNC, null, null);
            throw new IllegalStateException("broker failure accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "broker failure code");
        }
        record(output, verify, capture, "send-v2-error", RequestCode.SEND_MESSAGE_V2,
            "MQBrokerException;code=1");

        check(api.sendMessage("broker:10911", "broker", message, header, 10000,
            CommunicationMode.ONEWAY, null, null) == null, "v2 oneway result");
        record(output, verify, capture, "send-v2-oneway", RequestCode.SEND_MESSAGE_V2, "ok");
        header.setBatch(true);
        MessageBatch batch = MessageBatch.generateFromList(Collections.singletonList(message));
        batch.setBody(batch.encode());
        check(api.sendMessage("broker:10911", "broker", batch, header, 10000,
            CommunicationMode.ONEWAY, null, null) == null, "batch oneway result");
        record(output, verify, capture, "send-batch-oneway", RequestCode.SEND_BATCH_MESSAGE, "ok");
        header.setBatch(false);
        Message reply = new Message("oracle-topic", "reply".getBytes(StandardCharsets.UTF_8));
        MessageAccessor.putProperty(reply, MessageConst.PROPERTY_MESSAGE_TYPE, MixAll.REPLY_MESSAGE_FLAG);
        check(api.sendMessage("broker:10911", "broker", reply, header, 10000,
            CommunicationMode.ONEWAY, null, null) == null, "reply oneway result");
        record(output, verify, capture, "send-reply-v2-oneway", RequestCode.SEND_REPLY_MESSAGE_V2, "ok");

        EndTransactionRequestHeader end = new EndTransactionRequestHeader();
        end.setProducerGroup("producer");
        end.setTranStateTableOffset(3L);
        end.setCommitLogOffset(9L);
        end.setCommitOrRollback(8);
        end.setMsgId("transaction-id");
        api.endTransactionOneway("broker:10911", end, "offline end", 10000);
        record(output, verify, capture, "end-transaction-oneway", RequestCode.END_TRANSACTION, "ok");
    }
}