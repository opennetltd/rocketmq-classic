/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.RequestCallback;
import org.apache.rocketmq.client.producer.RequestFutureHolder;
import org.apache.rocketmq.client.producer.RequestResponseFuture;
import org.apache.rocketmq.client.utils.MessageUtil;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.utils.CorrelationIdUtil;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.ReplyMessageRequestHeader;

/** Invoked Java 5.5.1 request/reply API and message construction oracle. */
public final class RequestReplyFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private RequestReplyFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(render()) + "\n";
        if (verify) {
            if (!Files.exists(path) || !json.equals(Files.readString(path))) {
                throw new IllegalStateException("Java request/reply fixture drift: " + path);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
    }

    private static Map<String, Object> render() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("sourceCommit", SOURCE_COMMIT);
        provenance.put("entrypoints", List.of(
            "DefaultMQProducer.request",
            "DefaultMQProducerImpl.prepareSendRequest",
            "CorrelationIdUtil.createCorrelationId",
            "RequestFutureHolder.scanExpiredRequest",
            "RequestFutureHolder.publicMethodInventory",
            "RequestFutureHolder.singletonTableBehavior",
            "RequestResponseFuture.publicMethodInventory",
            "RequestResponseFuture.mutableStateAndCallbackBehavior",
            "RequestResponseFuture.nullableLatchBehavior",
            "RequestCallback.publicMethodInventory",
            "RequestResponseFuture.executeRequestCallback",
            "MessageUtil.createReplyMessage",
            "ClientRemotingProcessor.receiveReplyMessage"));
        provenance.put("asyncAdmissionSource", "DefaultMQProducerImpl.asyncSenderThreadPoolQueue");
        provenance.put("serverContacted", false);
        result.put("provenance", provenance);

        DefaultMQProducer admissionProducer = new DefaultMQProducer("request-reply-fixture");
        Field asyncQueueField = admissionProducer.getDefaultMQProducerImpl().getClass()
            .getDeclaredField("asyncSenderThreadPoolQueue");
        asyncQueueField.setAccessible(true);
        BlockingQueue<?> asyncQueue = (BlockingQueue<?>) asyncQueueField.get(
            admissionProducer.getDefaultMQProducerImpl());
        result.put("asyncSenderQueueCapacity", asyncQueue.remainingCapacity());

        List<String> overloads = Arrays.stream(DefaultMQProducer.class.getMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()) && method.getName().equals("request"))
            .map(RequestReplyFixtures::methodSignature)
            .sorted()
            .collect(Collectors.toList());
        result.put("publicRequestOverloads", overloads);
        result.put("publicCorrelationIdUtilMethods", publicMethodSignatures(CorrelationIdUtil.class));
        result.put("publicRequestCallbackMethods", publicMethodSignatures(RequestCallback.class));
        result.put("publicRequestFutureHolderMethods", publicMethodSignatures(RequestFutureHolder.class));
        result.put("publicRequestResponseFutureMethods", publicMethodSignatures(RequestResponseFuture.class));
        result.put("requestResponseFutureBehavior", requestResponseFutureBehavior());
        result.put("requestFutureHolderBehavior", requestFutureHolderBehavior());
        result.put("requestProperties", List.of(
            MessageConst.PROPERTY_CORRELATION_ID,
            MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT,
            MessageConst.PROPERTY_MESSAGE_TTL));
        String correlationId = CorrelationIdUtil.createCorrelationId();
        UUID correlationUuid = UUID.fromString(correlationId);
        result.put("correlationId", sortedMap(
            "format", "8-4-4-4-12 lowercase hexadecimal UUID",
            "uuidV4", correlationUuid.version() == 4,
            "variant", correlationUuid.variant(),
            "length", correlationId.length()));
        result.put("replyConstants", sortedMap(
            "topicSuffix", MixAll.REPLY_TOPIC_POSTFIX,
            "messageType", MixAll.REPLY_MESSAGE_FLAG,
            "arrivalTimeProperty", MessageConst.PROPERTY_REPLY_MESSAGE_ARRIVE_TIME));

        Message request = new Message("orders", "request-body".getBytes(StandardCharsets.UTF_8));
        MessageAccessor.putProperty(request, MessageConst.PROPERTY_CLUSTER, "cluster-east");
        MessageAccessor.putProperty(request, MessageConst.PROPERTY_CORRELATION_ID, "correlation-fixture-01");
        MessageAccessor.putProperty(request, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, "client-fixture-02");
        MessageAccessor.putProperty(request, MessageConst.PROPERTY_MESSAGE_TTL, "3200");
        Message reply = MessageUtil.createReplyMessage(request, "reply-內容".getBytes(StandardCharsets.UTF_8));
        Map<String, Object> replyMessage = new TreeMap<>();
        replyMessage.put("topic", reply.getTopic());
        replyMessage.put("bodyBase64", Base64.getEncoder().encodeToString(reply.getBody()));
        replyMessage.put("properties", new TreeMap<>(reply.getProperties()));
        result.put("replyMessage", replyMessage);

        Map<String, Object> failures = new TreeMap<>();
        failures.put("nullRequest", failure(null));
        Message noCluster = new Message("orders", new byte[] {1});
        failures.put("missingCluster", failure(noCluster));
        result.put("replyHelperFailures", failures);
        result.put("createReplyErrorCode", ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION);

        ReplyMessageRequestHeader header = new ReplyMessageRequestHeader();
        header.setProducerGroup("reply-producer");
        header.setTopic("cluster-east_REPLY_TOPIC");
        header.setDefaultTopic("TBW102");
        header.setDefaultTopicQueueNums(4);
        header.setQueueId(2);
        header.setSysFlag(0);
        header.setBornTimestamp(1_700_000_000_123L);
        header.setFlag(7);
        header.setProperties("CORRELATION_ID\u0001correlation-fixture-01\u0002MSG_TYPE\u0001reply\u0002");
        header.setReconsumeTimes(3);
        header.setUnitMode(false);
        header.setBornHost("10.0.0.1:10911");
        header.setStoreHost("10.0.0.2:10911");
        header.setStoreTimestamp(1_700_000_001_234L);
        RemotingCommand replyHeaderCommand = RemotingCommand
            .createRequestCommand(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, header);
        replyHeaderCommand.makeCustomHeaderToNet();
        result.put("replyHeader", new TreeMap<>(replyHeaderCommand.getExtFields()));
        result.put("replyRequestCode", RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT);
        result.put("unknownReplyAcknowledgement", "SUCCESS");
        result.put("malformedReplyAcknowledgement", sortedMap(
            "code", "SYSTEM_ERROR", "remark", "process reply message fail"));
        return result;
    }

    private static Map<String, Object> requestResponseFutureBehavior() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<String> callbackEvents = new ArrayList<>();
        RequestCallback callback = new RequestCallback() {
            @Override
            public void onSuccess(Message response) {
                callbackEvents.add("success:" + response.getTopic());
            }

            @Override
            public void onException(Throwable cause) {
                callbackEvents.add(cause == null ? "exception:null" :
                    "exception:" + cause.getClass().getName() + ":" + cause.getMessage());
            }
        };
        long beforeConstruction = System.currentTimeMillis();
        RequestResponseFuture future = new RequestResponseFuture("future-fixture-01", 30_000, callback);
        long afterConstruction = System.currentTimeMillis();
        result.put("correlationId", future.getCorrelationId());
        result.put("timeoutMillis", future.getTimeoutMillis());
        result.put("beginTimestampInConstructionWindow",
            future.getBeginTimestamp() >= beforeConstruction && future.getBeginTimestamp() <= afterConstruction);
        result.put("requestMessageNull", future.getRequestMsg() == null);
        result.put("callbackSameInstance", future.getRequestCallback() == callback);
        result.put("initialSendRequestOk", future.isSendRequestOk());
        result.put("initialResponseNull", future.getResponseMsg() == null);
        result.put("initialCauseNull", future.getCause() == null);
        result.put("initialLatchCount", future.getCountDownLatch().getCount());

        CountDownLatch replacementLatch = new CountDownLatch(2);
        future.setCountDownLatch(replacementLatch);
        result.put("replacementLatchSameInstance", future.getCountDownLatch() == replacementLatch);
        result.put("replacementLatchInitialCount", future.getCountDownLatch().getCount());
        Message response = new Message("future-response", "response-body".getBytes(StandardCharsets.UTF_8));
        future.putResponseMessage(response);
        result.put("putResponseMessageCount", replacementLatch.getCount());
        result.put("putResponseMessage", messageSnapshot(future.getResponseMsg()));
        replacementLatch.countDown();
        result.put("directLatchCountDownCount", replacementLatch.getCount());
        Message waitedResponse = future.waitResponseMessage(1);
        result.put("waitResponseReturnsStoredInstance", waitedResponse == response);

        future.setResponseMsg(null);
        result.put("responseSetterCanClear", future.getResponseMsg() == null);
        future.setResponseMsg(response);
        future.setTimeoutMillis(-1);
        result.put("timeoutSetter", future.getTimeoutMillis());
        result.put("negativeTimeoutIsExpired", future.isTimeout());
        future.setTimeoutMillis(30_000);
        future.setSendRequestOk(false);
        Throwable cause = new IllegalStateException("fixture-cause");
        future.setCause(cause);
        result.put("sendRequestOkSetter", future.isSendRequestOk());
        result.put("causeSameInstance", future.getCause() == cause);
        future.executeRequestCallback();
        future.executeRequestCallback();
        RequestResponseFuture successFuture = new RequestResponseFuture("future-success", 30_000, callback);
        successFuture.setResponseMsg(response);
        successFuture.executeRequestCallback();
        successFuture.executeRequestCallback();
        result.put("callbackEvents", callbackEvents);

        RequestResponseFuture withoutCallback = new RequestResponseFuture("future-no-callback", 1, null);
        withoutCallback.executeRequestCallback();
        result.put("nullCallbackGetter", withoutCallback.getRequestCallback() == null);
        result.put("nullCallbackExecution", "completed");

        RequestResponseFuture nullLatchFuture = new RequestResponseFuture("future-null-latch", 30_000, null);
        nullLatchFuture.setCountDownLatch(null);
        result.put("nullLatchGetter", nullLatchFuture.getCountDownLatch() == null);
        Message nullLatchResponse = new Message("null-latch-response", new byte[] {1});
        try {
            nullLatchFuture.putResponseMessage(nullLatchResponse);
            result.put("nullLatchPutResponseException", "none");
        } catch (NullPointerException exception) {
            result.put("nullLatchPutResponseException", exception.getClass().getName());
        }
        result.put("nullLatchPutResponseStored", nullLatchFuture.getResponseMsg() == nullLatchResponse);
        try {
            nullLatchFuture.waitResponseMessage(1);
            result.put("nullLatchWaitResponseException", "none");
        } catch (NullPointerException exception) {
            result.put("nullLatchWaitResponseException", exception.getClass().getName());
        }
        return result;
    }

    private static Map<String, Object> requestFutureHolderBehavior() {
        Map<String, Object> result = new TreeMap<>();
        RequestFutureHolder holder = RequestFutureHolder.getInstance();
        ConcurrentHashMap<String, RequestResponseFuture> first = holder.getRequestFutureTable();
        ConcurrentHashMap<String, RequestResponseFuture> second = holder.getRequestFutureTable();
        result.put("singletonInstance", holder == RequestFutureHolder.getInstance());
        result.put("tableSameInstance", first == second);
        first.clear();
        RequestResponseFuture future = new RequestResponseFuture("holder-fixture-01", 1000, null);
        first.put(future.getCorrelationId(), future);
        result.put("insertVisibleThroughSecondView", second.get(future.getCorrelationId()) == future);
        result.put("sizeAfterInsert", second.size());
        result.put("removedSameInstance", first.remove(future.getCorrelationId()) == future);
        result.put("sizeAfterRemove", second.size());
        result.put("waiterLatchCountAfterTableRemove", future.getCountDownLatch().getCount());
        boolean waiterResponseNullAfterTableRemove;
        try {
            waiterResponseNullAfterTableRemove = future.waitResponseMessage(1) == null;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(exception);
        }
        result.put("waiterResponseNullAfterTableRemove", waiterResponseNullAfterTableRemove);
        result.put("waiterLatchStillUnsignaledAfterTableRemove",
            future.getCountDownLatch().getCount() == 1);
        return result;
    }

    private static Map<String, Object> messageSnapshot(Message message) {
        Map<String, Object> result = new TreeMap<>();
        result.put("topic", message.getTopic());
        result.put("bodyBase64", Base64.getEncoder().encodeToString(message.getBody()));
        return result;
    }

    private static Map<String, Object> failure(Message request) {
        try {
            MessageUtil.createReplyMessage(request, new byte[] {1});
            return sortedMap("exception", "none", "message", "");
        } catch (Exception exception) {
            return sortedMap("exception", exception.getClass().getName(), "message", exception.getMessage());
        }
    }

    private static Map<String, Object> sortedMap(Object... fields) {
        Map<String, Object> result = new TreeMap<>();
        for (int index = 0; index < fields.length; index += 2) {
            result.put((String) fields[index], fields[index + 1]);
        }
        return result;
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getTypeName() + " " + method.getName() + "(" +
            Arrays.stream(method.getParameterTypes()).map(Class::getTypeName).collect(Collectors.joining(",")) + ")";
    }

    private static List<String> publicMethodSignatures(Class<?> type) {
        return Arrays.stream(type.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(RequestReplyFixtures::methodSignature)
            .sorted()
            .collect(Collectors.toList());
    }
}
