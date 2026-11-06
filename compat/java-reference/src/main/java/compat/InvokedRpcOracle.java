package compat;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PopCallback;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.AckCallback;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.OffsetNotFoundException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.remoting.common.HeartbeatV2Result;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody;
import org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageResponseHeader;
import java.util.Collections;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;

/** Offline invocation checks: only the transport is substituted, not MQClientAPIImpl. */
public final class InvokedRpcOracle {
    private InvokedRpcOracle() { }

    private static final class Capture {
        String mode;
        String address;
        RemotingCommand request;
        RemotingCommand response;
        InvokeCallback asyncCallback;

        RemotingClient proxy() {
            return (RemotingClient) Proxy.newProxyInstance(RemotingClient.class.getClassLoader(),
                new Class<?>[] {RemotingClient.class}, (proxy, method, args) -> {
                    if (method.getName().startsWith("invoke")) {
                        mode = method.getName();
                        address = (String) args[0];
                        request = (RemotingCommand) args[1];
                        asyncCallback = method.getName().equals("invokeAsync") ? (InvokeCallback) args[3] : null;
                        return method.getName().equals("invokeSync") ? response : null;
                    }
                    if (method.getReturnType() == boolean.class) return false;
                    return null;
                });
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }

    private static void dispatched(Capture capture, String mode, int code, String address) {
        check(mode.equals(capture.mode) && code == capture.request.getCode()
            && java.util.Objects.equals(address, capture.address), "dispatch " + code + " " + mode);
        check(capture.request.getVersion() == MQVersion.CURRENT_VERSION,
            "client request version " + code + " " + capture.request.getVersion());
    }

    private static void checkSendModes(Capture capture, MQClientAPIImpl api, Message message,
        SendMessageRequestHeader header, DefaultMQProducerImpl producer, int code) throws Exception {
        byte[] body = message.getBody();
        check(api.sendMessage("broker:10911", "broker", message, header, 10000,
            CommunicationMode.ONEWAY, null, null) == null, "oneway send result " + code);
        dispatched(capture, "invokeOneway", code, "broker:10911");
        check(java.util.Arrays.equals(body, capture.request.getBody()), "oneway send body " + code);
        capture.request.makeCustomHeaderToNet();
        check("oracle-topic".equals(capture.request.getExtFields().get(
            code == RequestCode.SEND_MESSAGE || code == RequestCode.SEND_REPLY_MESSAGE ? "topic" : "b")),
            "send header topic " + code);

        SendMessageResponseHeader responseHeader = new SendMessageResponseHeader();
        responseHeader.setMsgId("mode-message");
        responseHeader.setQueueId(2);
        responseHeader.setQueueOffset(23L);
        final SendResult[] result = new SendResult[1];
        final Throwable[] error = new Throwable[1];
        SendCallback callback = new SendCallback() {
            @Override public void onSuccess(SendResult value) { result[0] = value; }
            @Override public void onException(Throwable value) { error[0] = value; }
        };
        for (int status : new int[] {ResponseCode.SUCCESS, ResponseCode.FLUSH_DISK_TIMEOUT,
            ResponseCode.FLUSH_SLAVE_TIMEOUT, ResponseCode.SLAVE_NOT_AVAILABLE, ResponseCode.SYSTEM_ERROR}) {
            RemotingCommand reply = status == ResponseCode.SYSTEM_ERROR
                ? RemotingCommand.createResponseCommand(status, "offline")
                : RemotingCommand.createResponseCommandWithHeader(status, responseHeader);
            reply.makeCustomHeaderToNet();
            capture.response = reply;
            if (status == ResponseCode.SYSTEM_ERROR) {
                try {
                    api.sendMessage("broker:10911", "broker", message, header, 10000,
                        CommunicationMode.SYNC, null, null);
                    throw new IllegalStateException("sync send error accepted " + code);
                } catch (MQBrokerException expected) {
                    check(expected.getResponseCode() == status, "sync send broker error " + code);
                }
            } else {
                SendResult sync = api.sendMessage("broker:10911", "broker", message, header, 10000,
                    CommunicationMode.SYNC, null, null);
                checkSendOutcome(sync, status, "sync " + code);
            }
            dispatched(capture, "invokeSync", code, "broker:10911");
            check(java.util.Arrays.equals(body, capture.request.getBody()), "sync send body " + code);

            result[0] = null;
            error[0] = null;
            check(api.sendMessage("broker:10911", "broker", message, header, 10000,
                CommunicationMode.ASYNC, callback, null, null, 0, null, producer) == null,
                "async send result " + code);
            dispatched(capture, "invokeAsync", code, "broker:10911");
            check(java.util.Arrays.equals(body, capture.request.getBody()), "async send body " + code);
            check(capture.asyncCallback != null, "async send callback " + code);
            capture.asyncCallback.operationSucceed(reply);
            if (status == ResponseCode.SYSTEM_ERROR) {
                check(result[0] == null && error[0] instanceof MQBrokerException
                    && ((MQBrokerException) error[0]).getResponseCode() == status,
                    "async send broker error " + code);
            } else {
                check(error[0] == null, "async send exception " + code);
                checkSendOutcome(result[0], status, "async " + code);
            }
        }
        result[0] = null;
        error[0] = null;
        api.sendMessage("broker:10911", "broker", message, header, 10000,
            CommunicationMode.ASYNC, callback, null, null, 0, null, producer);
        dispatched(capture, "invokeAsync", code, "broker:10911");
        IllegalStateException transportError = new IllegalStateException("transport");
        capture.asyncCallback.operationFail(transportError);
        check(result[0] == null && error[0] instanceof org.apache.rocketmq.client.exception.MQClientException
            && error[0].getCause() == transportError, "async send transport error " + code);
    }

    private static void checkSendOutcome(SendResult result, int code, String label) {
        check(result != null && result.getSendStatus() == (code == ResponseCode.SUCCESS ? SendStatus.SEND_OK
            : code == ResponseCode.FLUSH_DISK_TIMEOUT ? SendStatus.FLUSH_DISK_TIMEOUT
            : code == ResponseCode.FLUSH_SLAVE_TIMEOUT ? SendStatus.FLUSH_SLAVE_TIMEOUT
            : SendStatus.SLAVE_NOT_AVAILABLE)
            && "mode-message".equals(result.getOffsetMsgId()) && result.getQueueOffset() == 23L,
            "send outcome " + label + " " + code);
    }

    private static void checkPopCallbacks(Capture capture, MQClientAPIImpl api,
        PopMessageRequestHeader pop, PopLiteMessageRequestHeader lite) throws Exception {
        final PopResult[] result = new PopResult[1];
        final Throwable[] error = new Throwable[1];
        PopCallback callback = new PopCallback() {
            @Override public void onSuccess(PopResult value) { result[0] = value; }
            @Override public void onException(Throwable value) { error[0] = value; }
        };
        for (boolean isLite : new boolean[] {false, true}) {
            int requestCode = isLite ? RequestCode.POP_LITE_MESSAGE : RequestCode.POP_MESSAGE;
            for (int code : new int[] {ResponseCode.SUCCESS, ResponseCode.POLLING_FULL,
                ResponseCode.POLLING_TIMEOUT, ResponseCode.PULL_NOT_FOUND, ResponseCode.SYSTEM_ERROR}) {
                result[0] = null;
                error[0] = null;
                if (isLite) api.popLiteMessageAsync("broker", "broker:10911", lite, 10000, callback);
                else api.popMessageAsync("broker", "broker:10911", pop, 10000, callback);
                dispatched(capture, "invokeAsync", requestCode, "broker:10911");
                RemotingCommand reply;
                if (isLite) {
                    PopLiteMessageResponseHeader header = new PopLiteMessageResponseHeader();
                    header.setPopTime(123L);
                    header.setInvisibleTime(456L);
                    header.setReviveQid(2);
                    reply = RemotingCommand.createResponseCommandWithHeader(code, header);
                } else {
                    PopMessageResponseHeader header = new PopMessageResponseHeader();
                    header.setPopTime(123L);
                    header.setInvisibleTime(456L);
                    header.setReviveQid(2);
                    header.setRestNum(7L);
                    reply = RemotingCommand.createResponseCommandWithHeader(code, header);
                }
                reply.makeCustomHeaderToNet();
                if (code == ResponseCode.SUCCESS) reply.setBody(new byte[0]);
                capture.asyncCallback.operationSucceed(reply);
                if (code == ResponseCode.SYSTEM_ERROR) {
                    check(result[0] == null && error[0] instanceof MQBrokerException
                        && ((MQBrokerException) error[0]).getResponseCode() == code,
                        "pop broker error " + requestCode);
                } else {
                    PopStatus status = code == ResponseCode.SUCCESS ? PopStatus.FOUND
                        : code == ResponseCode.POLLING_FULL ? PopStatus.POLLING_FULL : PopStatus.POLLING_NOT_FOUND;
                    check(error[0] == null && result[0] != null && result[0].getPopStatus() == status
                        && (isLite ? result[0].getRestNum() == 0L
                            : result[0].getRestNum() == 7L)
                        && (code != ResponseCode.SUCCESS || (result[0].getMsgFoundList() != null
                            && result[0].getMsgFoundList().isEmpty()
                            && (isLite || (result[0].getPopTime() == 123L
                                && result[0].getInvisibleTime() == 456L)))),
                        "pop response " + requestCode + " " + code);
                }
            }
            result[0] = null;
            error[0] = null;
            if (isLite) api.popLiteMessageAsync("broker", "broker:10911", lite, 10000, callback);
            else api.popMessageAsync("broker", "broker:10911", pop, 10000, callback);
            IllegalStateException transportError = new IllegalStateException("transport");
            capture.asyncCallback.operationFail(transportError);
            check(result[0] == null && error[0] == transportError, "pop transport error " + requestCode);
        }
    }

    private static void checkAckCallbacks(Capture capture, MQClientAPIImpl api,
        AckMessageRequestHeader ack, BatchAckMessageRequestBody batch,
        ChangeInvisibleTimeRequestHeader change) throws Exception {
        final AckResult[] result = new AckResult[1];
        final Throwable[] error = new Throwable[1];
        AckCallback callback = new AckCallback() {
            @Override public void onSuccess(AckResult value) { result[0] = value; }
            @Override public void onException(Throwable value) { error[0] = value; }
        };
        for (int kind = 0; kind < 4; kind++) {
            int requestCode = kind == 1 ? RequestCode.BATCH_ACK_MESSAGE
                : kind == 3 ? RequestCode.CHANGE_MESSAGE_INVISIBLETIME : RequestCode.ACK_MESSAGE;
            for (int code : new int[] {ResponseCode.SUCCESS, ResponseCode.SYSTEM_ERROR}) {
                result[0] = null;
                error[0] = null;
                dispatchAck(api, kind, ack, batch, change, callback);
                dispatched(capture, "invokeAsync", requestCode, "broker:10911");
                RemotingCommand reply;
                if (kind == 3) {
                    ChangeInvisibleTimeResponseHeader header = new ChangeInvisibleTimeResponseHeader();
                    header.setPopTime(123L);
                    header.setInvisibleTime(456L);
                    header.setReviveQid(2);
                    reply = RemotingCommand.createResponseCommandWithHeader(code, header);
                    reply.makeCustomHeaderToNet();
                } else reply = RemotingCommand.createResponseCommand(code, "offline");
                capture.asyncCallback.operationSucceed(reply);
                check(error[0] == null && result[0] != null
                    && result[0].getStatus() == (code == ResponseCode.SUCCESS ? AckStatus.OK : AckStatus.NO_EXIST)
                    && (kind == 3 && code == ResponseCode.SUCCESS
                        ? result[0].getPopTime() == 123L
                            && "17 123 456 2 0 broker 2 17".equals(result[0].getExtraInfo())
                        : result[0].getPopTime() == 0L && result[0].getExtraInfo() == null),
                    "ack response " + kind + " " + code);
            }
            if (kind == 3) {
                for (int code : new int[] {ResponseCode.SUCCESS, ResponseCode.SYSTEM_ERROR}) {
                    result[0] = null;
                    error[0] = null;
                    dispatchAck(api, kind, ack, batch, change, callback);
                    capture.asyncCallback.operationSucceed(RemotingCommand.createResponseCommand(code, "no header"));
                    check(error[0] == null && result[0] != null
                        && result[0].getStatus() == (code == ResponseCode.SUCCESS ? AckStatus.OK : AckStatus.NO_EXIST)
                        && (code != ResponseCode.SUCCESS || (result[0].getPopTime() == 0L
                            && "17 0 0 0 0 broker 2 17".equals(result[0].getExtraInfo()))),
                        "change missing response header " + code);
                }
            }
            result[0] = null;
            error[0] = null;
            dispatchAck(api, kind, ack, batch, change, callback);
            IllegalStateException transportError = new IllegalStateException("transport");
            capture.asyncCallback.operationFail(transportError);
            check(result[0] == null && error[0] == transportError, "ack transport error " + kind);
        }
    }

    private static void dispatchAck(MQClientAPIImpl api, int kind, AckMessageRequestHeader ack,
        BatchAckMessageRequestBody batch, ChangeInvisibleTimeRequestHeader change, AckCallback callback)
        throws Exception {
        switch (kind) {
            case 0: api.ackMessageAsync("broker:10911", 10000, callback, ack); break;
            case 1: api.batchAckMessageAsync("broker:10911", 10000, callback, batch); break;
            case 2: api.ackLiteMessageAsync("broker:10911", 10000, callback, ack); break;
            default: api.changeInvisibleTimeAsync("broker", "broker:10911", change, 10000, callback);
        }
    }

    public static void run() throws Exception {
        Capture capture = new Capture();
        ClientConfig config = new ClientConfig();
        config.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(new NettyClientConfig(), null, null, config,
            null, nettyConfig -> capture.proxy());

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
        api.sendMessage("broker:10911", "broker", new Message("oracle-topic", "payload".getBytes(StandardCharsets.UTF_8)),
            send, 10000, CommunicationMode.ONEWAY, null, null);
        capture.request.makeCustomHeaderToNet();
        check("invokeOneway".equals(capture.mode), "send mode");
        check("broker:10911".equals(capture.address), "send address");
        check(capture.request.getCode() == RequestCode.SEND_MESSAGE_V2, "send code");
        check("oracle-topic".equals(capture.request.getExtFields().get("b")), "send V2 topic");
        check(new String(capture.request.getBody(), StandardCharsets.UTF_8).equals("payload"), "send body");

        Message message = new Message("oracle-topic", "payload".getBytes(StandardCharsets.UTF_8));
        SendMessageResponseHeader sendReply = new SendMessageResponseHeader();
        sendReply.setMsgId("broker-message");
        sendReply.setQueueId(2);
        sendReply.setQueueOffset(17L);
        for (int code : new int[] {ResponseCode.SUCCESS, ResponseCode.FLUSH_DISK_TIMEOUT,
            ResponseCode.FLUSH_SLAVE_TIMEOUT, ResponseCode.SLAVE_NOT_AVAILABLE}) {
            capture.response = RemotingCommand.createResponseCommandWithHeader(code, sendReply);
            capture.response.makeCustomHeaderToNet();
            SendResult result = api.sendMessage("broker:10911", "broker", message, send, 10000,
                CommunicationMode.SYNC, null, null);
            check("invokeSync".equals(capture.mode) && "broker:10911".equals(capture.address), "sync send transport");
            check(capture.request.getCode() == RequestCode.SEND_MESSAGE_V2, "sync send code");
            check(result.getSendStatus() == (code == ResponseCode.SUCCESS ? SendStatus.SEND_OK
                : code == ResponseCode.FLUSH_DISK_TIMEOUT ? SendStatus.FLUSH_DISK_TIMEOUT
                : code == ResponseCode.FLUSH_SLAVE_TIMEOUT ? SendStatus.FLUSH_SLAVE_TIMEOUT
                : SendStatus.SLAVE_NOT_AVAILABLE), "sync send status " + code);
            check("broker-message".equals(result.getOffsetMsgId()) && result.getQueueOffset() == 17L,
                "sync send result");
        }
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.sendMessage("broker:10911", "broker", message, send, 10000, CommunicationMode.SYNC, null, null);
            throw new IllegalStateException("send error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "send error code");
        }
        check("invokeSync".equals(capture.mode) && capture.request.getCode() == RequestCode.SEND_MESSAGE_V2,
            "send error transport");

        check(api.sendMessage("broker:10911", "broker", message, send, 10000,
            CommunicationMode.ASYNC, null, null) == null, "async send dispatch result");
        check("invokeAsync".equals(capture.mode) && "broker:10911".equals(capture.address),
            "async send transport");
        check(capture.request.getCode() == RequestCode.SEND_MESSAGE_V2, "async send code");

        DefaultMQProducerImpl producer = new DefaultMQProducerImpl(new DefaultMQProducer("oracle-producer"));
        final SendResult[] asyncResult = new SendResult[1];
        final Throwable[] asyncError = new Throwable[1];
        SendCallback sendCallback = new SendCallback() {
            @Override
            public void onSuccess(SendResult result) {
                asyncResult[0] = result;
            }

            @Override
            public void onException(Throwable error) {
                asyncError[0] = error;
            }
        };
        check(api.sendMessage("broker:10911", "broker", message, send, 10000,
            CommunicationMode.ASYNC, sendCallback, null, null, 0, null, producer) == null,
            "async send callback dispatch result");
        dispatched(capture, "invokeAsync", RequestCode.SEND_MESSAGE_V2, "broker:10911");
        check(capture.asyncCallback != null, "async send transport callback captured");
        RemotingCommand asyncReply = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, sendReply);
        asyncReply.makeCustomHeaderToNet();
        capture.asyncCallback.operationSucceed(asyncReply);
        check(asyncResult[0] != null && asyncResult[0].getSendStatus() == SendStatus.SEND_OK
            && "broker-message".equals(asyncResult[0].getOffsetMsgId())
            && asyncResult[0].getQueueOffset() == 17L && asyncError[0] == null,
            "async send callback success result");

        asyncResult[0] = null;
        check(api.sendMessage("broker:10911", "broker", message, send, 10000,
            CommunicationMode.ASYNC, sendCallback, null, null, 0, null, producer) == null,
            "async send error dispatch result");
        dispatched(capture, "invokeAsync", RequestCode.SEND_MESSAGE_V2, "broker:10911");
        capture.asyncCallback.operationSucceed(RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline"));
        check(asyncResult[0] == null && asyncError[0] instanceof MQBrokerException
            && ((MQBrokerException) asyncError[0]).getResponseCode() == ResponseCode.SYSTEM_ERROR,
            "async send broker error callback");

        Message replyMessage = new Message("oracle-topic", "reply".getBytes(StandardCharsets.UTF_8));
        MessageAccessor.putProperty(replyMessage, MessageConst.PROPERTY_MESSAGE_TYPE, MixAll.REPLY_MESSAGE_FLAG);
        api.sendMessage("broker:10911", "broker", replyMessage, send, 10000,
            CommunicationMode.ONEWAY, null, null);
        dispatched(capture, "invokeOneway", RequestCode.SEND_REPLY_MESSAGE_V2, "broker:10911");
        check("reply".equals(new String(capture.request.getBody(), StandardCharsets.UTF_8)),
            "reply send body");
        MessageBatch sendBatch = MessageBatch.generateFromList(Collections.singletonList(message));
        sendBatch.setBody(sendBatch.encode());
        api.sendMessage("broker:10911", "broker", sendBatch, send, 10000,
            CommunicationMode.ONEWAY, null, null);
        dispatched(capture, "invokeOneway", RequestCode.SEND_BATCH_MESSAGE, "broker:10911");
        check(java.util.Arrays.equals(sendBatch.getBody(), capture.request.getBody()), "batch send body");

        // The pinned client's static switch is sampled at class initialization; restore it for other callers.
        Field smartMsg = MQClientAPIImpl.class.getDeclaredField("sendSmartMsg");
        smartMsg.setAccessible(true);
        boolean originalSmartMsg = smartMsg.getBoolean(null);
        try {
            smartMsg.setBoolean(null, false);
            checkSendModes(capture, api, message, send, producer, RequestCode.SEND_MESSAGE);
            checkSendModes(capture, api, replyMessage, send, producer, RequestCode.SEND_REPLY_MESSAGE);
            checkSendModes(capture, api, sendBatch, send, producer, RequestCode.SEND_BATCH_MESSAGE);
            smartMsg.setBoolean(null, true);
            checkSendModes(capture, api, replyMessage, send, producer, RequestCode.SEND_REPLY_MESSAGE_V2);
            checkSendModes(capture, api, sendBatch, send, producer, RequestCode.SEND_BATCH_MESSAGE);
        } finally {
            smartMsg.setBoolean(null, originalSmartMsg);
        }

        PullMessageRequestHeader pull = new PullMessageRequestHeader();
        pull.setConsumerGroup("consumer");
        pull.setTopic("oracle-topic");
        pull.setQueueId(2);
        pull.setQueueOffset(10L);
        pull.setMaxMsgNums(1);
        pull.setSysFlag(0);
        pull.setCommitOffset(0L);
        pull.setSuspendTimeoutMillis(0L);
        pull.setSubVersion(1L);
        PullMessageResponseHeader pullReply = new PullMessageResponseHeader();
        pullReply.setNextBeginOffset(11L);
        pullReply.setMinOffset(0L);
        pullReply.setMaxOffset(20L);
        pullReply.setSuggestWhichBrokerId(0L);
        for (int code : new int[] {ResponseCode.SUCCESS, ResponseCode.PULL_NOT_FOUND,
            ResponseCode.PULL_RETRY_IMMEDIATELY, ResponseCode.PULL_OFFSET_MOVED}) {
            capture.response = RemotingCommand.createResponseCommandWithHeader(code, pullReply);
            capture.response.makeCustomHeaderToNet();
            if (code == ResponseCode.SUCCESS) capture.response.setBody("raw".getBytes(StandardCharsets.UTF_8));
            PullResult result = api.pullMessage("broker:10911", pull, 10000, CommunicationMode.SYNC, null);
            check("invokeSync".equals(capture.mode) && "broker:10911".equals(capture.address), "pull transport");
            check(capture.request.getCode() == RequestCode.PULL_MESSAGE, "pull code");
            check(result.getPullStatus() == (code == ResponseCode.SUCCESS ? PullStatus.FOUND
                : code == ResponseCode.PULL_NOT_FOUND ? PullStatus.NO_NEW_MSG
                : code == ResponseCode.PULL_RETRY_IMMEDIATELY ? PullStatus.NO_MATCHED_MSG
                : PullStatus.OFFSET_ILLEGAL), "pull status " + code);
            check(result.getNextBeginOffset() == 11L, "pull next offset");
        }
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.pullMessage("broker:10911", pull, 10000, CommunicationMode.SYNC, null);
            throw new IllegalStateException("pull error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "pull error code");
        }
        check("invokeSync".equals(capture.mode) && capture.request.getCode() == RequestCode.PULL_MESSAGE,
            "pull error transport");

        final PullResult[] asyncPullResult = new PullResult[1];
        final Throwable[] asyncPullError = new Throwable[1];
        PullCallback pullCallback = new PullCallback() {
            @Override public void onSuccess(PullResult value) { asyncPullResult[0] = value; }
            @Override public void onException(Throwable value) { asyncPullError[0] = value; }
        };
        for (int code : new int[] {ResponseCode.SUCCESS, ResponseCode.PULL_NOT_FOUND,
            ResponseCode.PULL_RETRY_IMMEDIATELY, ResponseCode.PULL_OFFSET_MOVED, ResponseCode.SYSTEM_ERROR}) {
            asyncPullResult[0] = null;
            asyncPullError[0] = null;
            check(api.pullMessage("broker:10911", pull, 10000, CommunicationMode.ASYNC, pullCallback) == null,
                "async pull dispatch result");
            dispatched(capture, "invokeAsync", RequestCode.PULL_MESSAGE, "broker:10911");
            RemotingCommand reply = code == ResponseCode.SYSTEM_ERROR
                ? RemotingCommand.createResponseCommand(code, "offline")
                : RemotingCommand.createResponseCommandWithHeader(code, pullReply);
            reply.makeCustomHeaderToNet();
            if (code == ResponseCode.SUCCESS) reply.setBody("raw".getBytes(StandardCharsets.UTF_8));
            capture.asyncCallback.operationSucceed(reply);
            if (code == ResponseCode.SYSTEM_ERROR) {
                check(asyncPullResult[0] == null && asyncPullError[0] instanceof MQBrokerException
                    && ((MQBrokerException) asyncPullError[0]).getResponseCode() == code,
                    "async pull broker error");
            } else {
                check(asyncPullError[0] == null && asyncPullResult[0] != null
                    && asyncPullResult[0].getPullStatus() == (code == ResponseCode.SUCCESS ? PullStatus.FOUND
                        : code == ResponseCode.PULL_NOT_FOUND ? PullStatus.NO_NEW_MSG
                        : code == ResponseCode.PULL_RETRY_IMMEDIATELY ? PullStatus.NO_MATCHED_MSG
                        : PullStatus.OFFSET_ILLEGAL)
                    && asyncPullResult[0].getNextBeginOffset() == 11L
                    && asyncPullResult[0].getMinOffset() == 0L
                    && asyncPullResult[0].getMaxOffset() == 20L,
                    "async pull completion " + code);
            }
        }
        asyncPullResult[0] = null;
        asyncPullError[0] = null;
        api.pullMessage("broker:10911", pull, 10000, CommunicationMode.ASYNC, pullCallback);
        dispatched(capture, "invokeAsync", RequestCode.PULL_MESSAGE, "broker:10911");
        IllegalStateException pullTransportError = new IllegalStateException("transport");
        capture.asyncCallback.operationFail(pullTransportError);
        check(asyncPullResult[0] == null && asyncPullError[0] == pullTransportError,
            "async pull transport failure");

        UpdateConsumerOffsetRequestHeader update = new UpdateConsumerOffsetRequestHeader();
        update.setConsumerGroup("consumer");
        update.setTopic("oracle-topic");
        update.setQueueId(2);
        update.setCommitOffset(42L);
        api.updateConsumerOffsetOneway("broker:10911", update, 10000);
        capture.request.makeCustomHeaderToNet();
        check("invokeOneway".equals(capture.mode) && "broker:10911".equals(capture.address), "update transport");
        check(capture.request.getCode() == RequestCode.UPDATE_CONSUMER_OFFSET, "update code");
        check("42".equals(capture.request.getExtFields().get("commitOffset")), "update offset");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        api.updateConsumerOffset("broker:10911", update, 10000);
        dispatched(capture, "invokeSync", RequestCode.UPDATE_CONSUMER_OFFSET, "broker:10911");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.updateConsumerOffset("broker:10911", update, 10000);
            throw new IllegalStateException("update offset error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "update offset error code");
        }

        HeartbeatData heartbeat = new HeartbeatData();
        heartbeat.setClientID("client");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setVersion(551);
        check(api.sendHeartbeat("broker:10911", heartbeat, 10000) == 551, "heartbeat version");
        check("invokeSync".equals(capture.mode) && capture.request.getCode() == RequestCode.HEART_BEAT,
            "heartbeat transport");
        check("client".equals(HeartbeatData.decode(capture.request.getBody(), HeartbeatData.class).getClientID()),
            "heartbeat body");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.sendHeartbeat("broker:10911", heartbeat, 10000);
            throw new IllegalStateException("heartbeat error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "heartbeat error code");
        }
        check("invokeSync".equals(capture.mode) && capture.request.getCode() == RequestCode.HEART_BEAT,
            "heartbeat error transport");

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        api.unregisterClient("broker:10911", "client", "producer", "consumer", 10000);
        capture.request.makeCustomHeaderToNet();
        check("invokeSync".equals(capture.mode) && capture.request.getCode() == RequestCode.UNREGISTER_CLIENT,
            "unregister transport");
        check("client".equals(capture.request.getExtFields().get("clientID")), "unregister client");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.unregisterClient("broker:10911", "client", "producer", "consumer", 10000);
            throw new IllegalStateException("unregister error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "unregister error code");
        }
        check("invokeSync".equals(capture.mode) && capture.request.getCode() == RequestCode.UNREGISTER_CLIENT,
            "unregister error transport");

        QueryConsumerOffsetRequestHeader offset = new QueryConsumerOffsetRequestHeader();
        offset.setConsumerGroup("consumer");
        offset.setTopic("oracle-topic");
        offset.setQueueId(2);
        QueryConsumerOffsetResponseHeader offsetReply = new QueryConsumerOffsetResponseHeader();
        offsetReply.setOffset(42L);
        capture.response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, offsetReply);
        capture.response.makeCustomHeaderToNet();
        check(api.queryConsumerOffset("broker:10911", offset, 10000) == 42L, "offset result");
        capture.request.makeCustomHeaderToNet();
        check("invokeSync".equals(capture.mode), "offset mode");
        check(capture.request.getCode() == RequestCode.QUERY_CONSUMER_OFFSET, "offset code");
        check("consumer".equals(capture.request.getExtFields().get("consumerGroup")), "offset group");

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.QUERY_NOT_FOUND, "offline");
        try {
            api.queryConsumerOffset("broker:10911", offset, 10000);
            throw new IllegalStateException("missing offset accepted");
        } catch (OffsetNotFoundException expected) {
            check(expected.getResponseCode() == ResponseCode.QUERY_NOT_FOUND, "missing offset code");
        }
        dispatched(capture, "invokeSync", RequestCode.QUERY_CONSUMER_OFFSET, "broker:10911");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.queryConsumerOffset("broker:10911", offset, 10000);
            throw new IllegalStateException("offset error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "offset error code");
        }

        TopicRouteData route = new TopicRouteData();
        route.setOrderTopicConf("oracle-order");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody(route.encode());
        check("oracle-order".equals(api.getTopicRouteInfoFromNameServer("oracle-topic", 10000)
            .getOrderTopicConf()), "route success body");
        dispatched(capture, "invokeSync", RequestCode.GET_ROUTEINFO_BY_TOPIC, null);

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.TOPIC_NOT_EXIST, "offline");
        try {
            api.getTopicRouteInfoFromNameServer("oracle-topic", 10000);
            throw new IllegalStateException("route error response accepted");
        } catch (org.apache.rocketmq.client.exception.MQClientException expected) {
            check(expected.getResponseCode() == ResponseCode.TOPIC_NOT_EXIST, "route error code");
        }
        capture.request.makeCustomHeaderToNet();
        check("invokeSync".equals(capture.mode) && capture.address == null, "route transport");
        check(capture.request.getCode() == RequestCode.GET_ROUTEINFO_BY_TOPIC, "route code");
        check("oracle-topic".equals(capture.request.getExtFields().get("topic")), "route topic");

        GetConsumerListByGroupResponseBody consumers = new GetConsumerListByGroupResponseBody();
        consumers.setConsumerIdList(java.util.Arrays.asList("client-a", "client-b"));
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody(consumers.encode());
        check(api.getConsumerIdListByGroup("broker:10911", "consumer", 10000)
            .equals(java.util.Arrays.asList("client-a", "client-b")), "consumer list body");
        dispatched(capture, "invokeSync", RequestCode.GET_CONSUMER_LIST_BY_GROUP, "broker:10911");
        capture.request.makeCustomHeaderToNet();
        check("consumer".equals(capture.request.getExtFields().get("consumerGroup")), "consumer list group");
        consumers.setConsumerIdList(Collections.emptyList());
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody(consumers.encode());
        check(api.getConsumerIdListByGroup("broker:10911", "consumer", 10000).isEmpty(),
            "empty consumer list body");
        dispatched(capture, "invokeSync", RequestCode.GET_CONSUMER_LIST_BY_GROUP, "broker:10911");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        try {
            api.getConsumerIdListByGroup("broker:10911", "consumer", 10000);
            throw new IllegalStateException("absent consumer list accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SUCCESS, "absent consumer list code");
        }
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.getConsumerIdListByGroup("broker:10911", "consumer", 10000);
            throw new IllegalStateException("consumer list error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "consumer list error code");
        }

        QueryAssignmentResponseBody assignments = new QueryAssignmentResponseBody();
        assignments.setMessageQueueAssignments(Collections.emptySet());
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody(assignments.encode());
        check(api.queryAssignment("broker:10911", "oracle-topic", "consumer", "client", "AVG",
            MessageModel.CLUSTERING, 10000).isEmpty(), "empty assignments");
        dispatched(capture, "invokeSync", RequestCode.QUERY_ASSIGNMENT, "broker:10911");
        QueryAssignmentRequestBody assignmentRequest = QueryAssignmentRequestBody.decode(capture.request.getBody(),
            QueryAssignmentRequestBody.class);
        check("oracle-topic".equals(assignmentRequest.getTopic())
            && "consumer".equals(assignmentRequest.getConsumerGroup()), "assignment request body");
        MessageQueueAssignment populated = new MessageQueueAssignment();
        populated.setMessageQueue(new MessageQueue("oracle-topic", "broker", 2));
        populated.setMode(MessageRequestMode.POP);
        populated.setAttachments(Collections.singletonMap("source", "oracle"));
        assignments.setMessageQueueAssignments(Collections.singleton(populated));
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody(assignments.encode());
        java.util.Set<MessageQueueAssignment> decodedAssignments = api.queryAssignment(
            "broker:10911", "oracle-topic", "consumer", "client", "AVG", MessageModel.CLUSTERING, 10000);
        dispatched(capture, "invokeSync", RequestCode.QUERY_ASSIGNMENT, "broker:10911");
        check(decodedAssignments != null && decodedAssignments.size() == 1, "populated assignment size");
        MessageQueueAssignment decodedAssignment = decodedAssignments.iterator().next();
        check(new MessageQueue("oracle-topic", "broker", 2).equals(decodedAssignment.getMessageQueue())
            && decodedAssignment.getMode() == MessageRequestMode.POP
            && "oracle".equals(decodedAssignment.getAttachments().get("source")),
            "populated assignment fields");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.queryAssignment("broker:10911", "oracle-topic", "consumer", "client", "AVG",
                MessageModel.CLUSTERING, 10000);
            throw new IllegalStateException("assignment error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "assignment error code");
        }

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setVersion(551);
        capture.response.addExtField(MixAll.IS_SUB_CHANGE, "true");
        capture.response.addExtField(MixAll.IS_SUPPORT_HEART_BEAT_V2, "true");
        HeartbeatV2Result v2 = api.sendHeartbeatV2("broker:10911", heartbeat, 10000);
        check(v2.getVersion() == 551 && v2.isSubChange() && v2.isSupportV2(), "heartbeat v2 fields");
        dispatched(capture, "invokeSync", RequestCode.HEART_BEAT, "broker:10911");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        v2 = api.sendHeartbeatV2("broker:10911", heartbeat, 10000);
        check(!v2.isSubChange() && !v2.isSupportV2(), "heartbeat v2 absent fields");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.sendHeartbeatV2("broker:10911", heartbeat, 10000);
            throw new IllegalStateException("heartbeat v2 error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "heartbeat v2 error code");
        }

        EndTransactionRequestHeader end = new EndTransactionRequestHeader();
        end.setProducerGroup("producer");
        end.setTranStateTableOffset(7L);
        end.setCommitLogOffset(19L);
        end.setCommitOrRollback(8);
        end.setMsgId("transaction-id");
        api.endTransactionOneway("broker:10911", end, "offline end", 10000);
        dispatched(capture, "invokeOneway", RequestCode.END_TRANSACTION, "broker:10911");
        capture.request.makeCustomHeaderToNet();
        check("offline end".equals(capture.request.getRemark())
            && "19".equals(capture.request.getExtFields().get("commitLogOffset")), "end transaction request");

        MessageQueue queue = new MessageQueue("oracle-topic", "broker", 2);
        LockBatchRequestBody lock = new LockBatchRequestBody();
        lock.setConsumerGroup("consumer");
        lock.setClientId("client");
        lock.setMqSet(Collections.singleton(queue));
        LockBatchResponseBody locked = new LockBatchResponseBody();
        locked.setLockOKMQSet(Collections.singleton(queue));
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody(locked.encode());
        check(api.lockBatchMQ("broker:10911", lock, 10000).contains(queue), "lock response");
        dispatched(capture, "invokeSync", RequestCode.LOCK_BATCH_MQ, "broker:10911");
        check(LockBatchRequestBody.decode(capture.request.getBody(), LockBatchRequestBody.class)
            .getMqSet().contains(queue), "lock request body");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.lockBatchMQ("broker:10911", lock, 10000);
            throw new IllegalStateException("lock error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "lock error code");
        }

        UnlockBatchRequestBody unlock = new UnlockBatchRequestBody();
        unlock.setConsumerGroup("consumer");
        unlock.setClientId("client");
        unlock.setMqSet(Collections.singleton(queue));
        api.unlockBatchMQ("broker:10911", unlock, 10000, true);
        dispatched(capture, "invokeOneway", RequestCode.UNLOCK_BATCH_MQ, "broker:10911");
        check(UnlockBatchRequestBody.decode(capture.request.getBody(), UnlockBatchRequestBody.class)
            .getMqSet().contains(queue), "unlock request body");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        api.unlockBatchMQ("broker:10911", unlock, 10000, false);
        dispatched(capture, "invokeSync", RequestCode.UNLOCK_BATCH_MQ, "broker:10911");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.unlockBatchMQ("broker:10911", unlock, 10000, false);
            throw new IllegalStateException("unlock error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "unlock error code");
        }

        MessageExt retry = new MessageExt();
        retry.setTopic("oracle-topic");
        retry.setCommitLogOffset(19L);
        retry.setMsgId("retry-id");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        api.consumerSendMessageBack("broker:10911", "broker", retry, "consumer", 3, 10000, 5);
        dispatched(capture, "invokeSync", RequestCode.CONSUMER_SEND_MSG_BACK, "broker:10911");
        capture.request.makeCustomHeaderToNet();
        check("19".equals(capture.request.getExtFields().get("offset")), "send back offset");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.consumerSendMessageBack("broker:10911", "broker", retry, "consumer", 3, 10000, 5);
            throw new IllegalStateException("send back error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "send back error code");
        }

        PopMessageRequestHeader pop = new PopMessageRequestHeader();
        pop.setConsumerGroup("consumer");
        pop.setTopic("oracle-topic");
        api.popMessageAsync("broker", "broker:10911", pop, 10000, null);
        dispatched(capture, "invokeAsync", RequestCode.POP_MESSAGE, "broker:10911");
        capture.request.makeCustomHeaderToNet();
        check("consumer".equals(capture.request.getExtFields().get("consumerGroup")), "pop group");
        PopLiteMessageRequestHeader lite = new PopLiteMessageRequestHeader();
        lite.setTopic("oracle-topic");
        api.popLiteMessageAsync("broker", "broker:10911", lite, 10000, null);
        dispatched(capture, "invokeAsync", RequestCode.POP_LITE_MESSAGE, "broker:10911");
        AckMessageRequestHeader ack = new AckMessageRequestHeader();
        ack.setConsumerGroup("consumer");
        ack.setTopic("oracle-topic");
        api.ackMessageAsync("broker:10911", 10000, null, ack);
        dispatched(capture, "invokeAsync", RequestCode.ACK_MESSAGE, "broker:10911");
        BatchAckMessageRequestBody batch = new BatchAckMessageRequestBody();
        batch.setBrokerName("broker");
        batch.setAcks(Collections.emptyList());
        api.batchAckMessageAsync("broker:10911", 10000, null, batch);
        dispatched(capture, "invokeAsync", RequestCode.BATCH_ACK_MESSAGE, "broker:10911");
        check("broker".equals(BatchAckMessageRequestBody.decode(capture.request.getBody(),
            BatchAckMessageRequestBody.class).getBrokerName()), "batch ack body");
        api.ackLiteMessageAsync("broker:10911", 10000, null, ack);
        dispatched(capture, "invokeAsync", RequestCode.ACK_MESSAGE, "broker:10911");
        ChangeInvisibleTimeRequestHeader change = new ChangeInvisibleTimeRequestHeader();
        change.setTopic("oracle-topic");
        api.changeInvisibleTimeAsync("broker", "broker:10911", change, 10000, null);
        dispatched(capture, "invokeAsync", RequestCode.CHANGE_MESSAGE_INVISIBLETIME, "broker:10911");
        change.setOffset(17L);
        change.setQueueId(2);
        checkPopCallbacks(capture, api, pop, lite);
        checkAckCallbacks(capture, api, ack, batch, change);
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        api.setMessageRequestMode("broker:10911", "oracle-topic", "consumer", MessageRequestMode.POP, 2, 10000);
        dispatched(capture, "invokeSync", RequestCode.SET_MESSAGE_REQUEST_MODE, "broker:10911");
        SetMessageRequestModeRequestBody modeBody = SetMessageRequestModeRequestBody.decode(
            capture.request.getBody(), SetMessageRequestModeRequestBody.class);
        check(modeBody.getMode() == MessageRequestMode.POP && modeBody.getPopShareQueueNum() == 2,
            "request mode body");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.setMessageRequestMode("broker:10911", "oracle-topic", "consumer", MessageRequestMode.POP, 2, 10000);
            throw new IllegalStateException("request mode error accepted");
        } catch (org.apache.rocketmq.client.exception.MQClientException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "request mode error code");
        }
        checkDecodedFailures(capture, api, message, send, pull, offset, producer);
        System.out.println("Invoked Java 5.5.1 MQClientAPIImpl: send/pull including async callbacks, offset, route, consumer list, assignment, lock/unlock, send-back, request mode, POP/Lite/ACK/change dispatch and callbacks (mock transport)");
    }

    private static void checkDecodedFailures(Capture capture, MQClientAPIImpl api, Message message,
        SendMessageRequestHeader send, PullMessageRequestHeader pull, QueryConsumerOffsetRequestHeader offset,
        DefaultMQProducerImpl producer) throws Exception {
        final int unknown = 987654;
        capture.response = RemotingCommand.createResponseCommand(unknown, "unknown");
        try {
            api.getTopicRouteInfoFromNameServer("oracle-topic", 10000);
            throw new IllegalStateException("unknown route accepted");
        } catch (org.apache.rocketmq.client.exception.MQClientException expected) {
            check(expected.getResponseCode() == unknown, "unknown route code");
        }
        dispatched(capture, "invokeSync", RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "no body");
        try {
            api.getTopicRouteInfoFromNameServer("oracle-topic", 10000);
            throw new IllegalStateException("bodyless route accepted");
        } catch (org.apache.rocketmq.client.exception.MQClientException expected) {
            check(expected.getResponseCode() == ResponseCode.SUCCESS, "bodyless route code");
        }
        dispatched(capture, "invokeSync", RequestCode.GET_ROUTEINFO_BY_TOPIC, null);

        capture.response = RemotingCommand.createResponseCommand(unknown, "unknown");
        try {
            api.sendMessage("broker:10911", "broker", message, send, 10000, CommunicationMode.SYNC, null, null);
            throw new IllegalStateException("unknown send accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == unknown, "unknown send code");
        }
        dispatched(capture, "invokeSync", RequestCode.SEND_MESSAGE_V2, "broker:10911");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "no header");
        try {
            api.sendMessage("broker:10911", "broker", message, send, 10000, CommunicationMode.SYNC, null, null);
            throw new IllegalStateException("headerless send accepted");
        } catch (NullPointerException expected) {
            dispatched(capture, "invokeSync", RequestCode.SEND_MESSAGE_V2, "broker:10911");
        }

        final SendResult[] sendResult = new SendResult[1];
        final Throwable[] sendError = new Throwable[1];
        SendCallback callback = new SendCallback() {
            @Override public void onSuccess(SendResult value) { sendResult[0] = value; }
            @Override public void onException(Throwable value) { sendError[0] = value; }
        };
        for (int code : new int[] {unknown, ResponseCode.SUCCESS}) {
            sendResult[0] = null;
            sendError[0] = null;
            api.sendMessage("broker:10911", "broker", message, send, 10000,
                CommunicationMode.ASYNC, callback, null, null, 0, null, producer);
            dispatched(capture, "invokeAsync", RequestCode.SEND_MESSAGE_V2, "broker:10911");
            capture.asyncCallback.operationSucceed(RemotingCommand.createResponseCommand(code, "no header"));
            check(sendResult[0] == null && sendError[0] != null,
                "async send unknown/malformed callback " + code);
            if (code == unknown) {
                check(sendError[0] instanceof MQBrokerException
                    && ((MQBrokerException) sendError[0]).getResponseCode() == unknown,
                    "async unknown send code");
            } else {
                check(sendError[0] instanceof NullPointerException, "async headerless send outcome");
            }
        }

        capture.response = RemotingCommand.createResponseCommand(unknown, "unknown");
        try {
            api.pullMessage("broker:10911", pull, 10000, CommunicationMode.SYNC, null);
            throw new IllegalStateException("unknown pull accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == unknown, "unknown pull code");
        }
        dispatched(capture, "invokeSync", RequestCode.PULL_MESSAGE, "broker:10911");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "no header");
        try {
            api.pullMessage("broker:10911", pull, 10000, CommunicationMode.SYNC, null);
            throw new IllegalStateException("headerless pull accepted");
        } catch (NullPointerException expected) {
            dispatched(capture, "invokeSync", RequestCode.PULL_MESSAGE, "broker:10911");
        }
        final PullResult[] pullResult = new PullResult[1];
        final Throwable[] pullError = new Throwable[1];
        PullCallback pullCallback = new PullCallback() {
            @Override public void onSuccess(PullResult value) { pullResult[0] = value; }
            @Override public void onException(Throwable value) { pullError[0] = value; }
        };
        for (int code : new int[] {unknown, ResponseCode.SUCCESS}) {
            pullResult[0] = null;
            pullError[0] = null;
            api.pullMessage("broker:10911", pull, 10000, CommunicationMode.ASYNC, pullCallback);
            dispatched(capture, "invokeAsync", RequestCode.PULL_MESSAGE, "broker:10911");
            capture.asyncCallback.operationSucceed(RemotingCommand.createResponseCommand(code, "no header"));
            check(pullResult[0] == null && pullError[0] != null,
                "async pull unknown/malformed callback " + code);
            if (code == unknown) {
                check(pullError[0] instanceof MQBrokerException
                    && ((MQBrokerException) pullError[0]).getResponseCode() == unknown,
                    "async unknown pull code");
            } else {
                check(pullError[0] instanceof NullPointerException, "async headerless pull outcome");
            }
        }

        capture.response = RemotingCommand.createResponseCommand(unknown, "unknown");
        try {
            api.queryConsumerOffset("broker:10911", offset, 10000);
            throw new IllegalStateException("unknown offset accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == unknown, "unknown offset code");
        }
        dispatched(capture, "invokeSync", RequestCode.QUERY_CONSUMER_OFFSET, "broker:10911");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "no header");
        try {
            api.queryConsumerOffset("broker:10911", offset, 10000);
            throw new IllegalStateException("headerless offset accepted");
        } catch (NullPointerException expected) {
            dispatched(capture, "invokeSync", RequestCode.QUERY_CONSUMER_OFFSET, "broker:10911");
        }

        capture.response = RemotingCommand.createResponseCommand(unknown, "unknown");
        try {
            api.queryAssignment("broker:10911", "oracle-topic", "consumer", "client", "AVG",
                MessageModel.CLUSTERING, 10000);
            throw new IllegalStateException("unknown assignment accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == unknown, "unknown assignment code");
        }
        dispatched(capture, "invokeSync", RequestCode.QUERY_ASSIGNMENT, "broker:10911");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "no body");
        try {
            api.queryAssignment("broker:10911", "oracle-topic", "consumer", "client", "AVG",
                MessageModel.CLUSTERING, 10000);
            throw new IllegalStateException("bodyless assignment accepted");
        } catch (NullPointerException expected) {
            dispatched(capture, "invokeSync", RequestCode.QUERY_ASSIGNMENT, "broker:10911");
        }
    }
}
