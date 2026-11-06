/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.trace.TraceBean;
import org.apache.rocketmq.client.trace.TraceContext;
import org.apache.rocketmq.client.trace.TraceDataEncoder;
import org.apache.rocketmq.client.trace.TraceTransferBean;
import org.apache.rocketmq.client.trace.TraceType;
import org.apache.rocketmq.common.message.MessageType;

/** Invoked Java 5.5.1 trace record encoder oracle. */
public final class TraceFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";
    private static final String FIXTURE_CLIENT_HOST = "192.0.2.18";

    private TraceFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1
            && (Boolean.parseBoolean(args[1]) || "--verify".equals(args[1]));
        String json = JSON.toJSONString(render()) + "\n";
        if (verify) {
            if (!Files.exists(path) || !json.equals(Files.readString(path))) {
                throw new IllegalStateException("Java trace fixture drift: " + path);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
    }

    private static Map<String, Object> render() throws Exception {
        List<Map<String, Object>> cases = new ArrayList<>();
        TraceContext pub = context(TraceType.Pub, "producer", "region-a", 1000, true,
            message("orders", "msg-1", "off-1", "paid", "k-1 k-2", "broker:10911", 12, 0,
                MessageType.Normal_Msg, null, null, false));
        pub.setCostTime(7);
        cases.add(render("pub", pub));
        cases.add(render("recall", context(TraceType.Recall, "producer", "region-b", 2000, false,
            message("orders", "msg-2", null, null, null, null, 0, 0, null, null, null, false))));
        TraceContext subBefore = context(TraceType.SubBefore, "workers", "region-a", 3000, true,
            message(null, "msg-3", null, null, "k-3", null, 0, 4, null, null, null, false),
            message(null, "msg-4", null, null, "k-4", null, 0, 1, null, null, null, false));
        subBefore.setRequestId("request-1");
        cases.add(render("sub_before", subBefore));
        TraceContext subAfterLocal = context(TraceType.SubAfter, "workers", "region-a", 3010, true,
            message(null, "msg-3", null, null, "k-3", null, 0, 0, null, null, null, false));
        subAfterLocal.setRequestId("request-1");
        subAfterLocal.setAccessChannel(AccessChannel.LOCAL);
        subAfterLocal.setCostTime(10);
        subAfterLocal.setContextCode(2);
        cases.add(render("sub_after_local", subAfterLocal));
        TraceContext subAfterCloud = context(TraceType.SubAfter, "workers", "region-a", 3010, false,
            message(null, "msg-4", null, null, "k-4", null, 0, 0, null, null, null, false));
        subAfterCloud.setRequestId("request-2");
        subAfterCloud.setAccessChannel(AccessChannel.CLOUD);
        subAfterCloud.setCostTime(11);
        subAfterCloud.setContextCode(3);
        cases.add(render("sub_after_cloud", subAfterCloud));
        cases.add(render("end_transaction", context(TraceType.EndTransaction, "producer", "region-a", 4000, true,
            message("orders", "msg-5", null, "paid", "k-5", "broker:10911", 0, 0,
                MessageType.Trans_msg_Commit, "txn-5", LocalTransactionState.COMMIT_MESSAGE, true))));

        Map<String, Object> result = new TreeMap<>();
        result.put("sourceCommit", SOURCE_COMMIT);
        result.put("clientArtifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        result.put("entrypoint", "TraceDataEncoder.encoderFromContextBean");
        result.put("defaults", defaults());
        result.put("identityEquality", identityEquality());
        result.put("invalidInputThrows", invalidInputThrows());
        result.put("cases", cases);
        result.put("selectionCases", selectionCases());
        return result;
    }

    private static List<Map<String, Object>> selectionCases() {
        List<Map<String, Object>> cases = new ArrayList<>();
        for (TraceType type : TraceType.values()) {
            for (int beanCount = 0; beanCount <= 2; beanCount++) {
                cases.add(renderSelection(
                    "selection_" + type.name().toLowerCase() + "_" + beanCount,
                    selectionContext(type, beanCount, false)));
            }
        }
        cases.add(renderSelection("pub_invalid_tail", selectionContext(TraceType.Pub, 2, true)));
        cases.add(renderSelection("end_transaction_invalid_tail",
            selectionContext(TraceType.EndTransaction, 2, true)));

        TraceContext pubMissingType = selectionContext(TraceType.Pub, 1, false);
        pubMissingType.getTraceBeans().get(0).setMsgType(null);
        cases.add(renderSelection("pub_selected_missing_message_type", pubMissingType));

        TraceContext transactionMissingType = selectionContext(TraceType.EndTransaction, 1, false);
        transactionMissingType.getTraceBeans().get(0).setMsgType(null);
        cases.add(renderSelection("end_transaction_selected_missing_message_type",
            transactionMissingType));

        TraceContext transactionMissingState = selectionContext(TraceType.EndTransaction, 1, false);
        transactionMissingState.getTraceBeans().get(0).setTransactionState(null);
        cases.add(renderSelection("end_transaction_selected_missing_state",
            transactionMissingState));
        return cases;
    }

    private static TraceContext selectionContext(TraceType type, int beanCount, boolean invalidTail) {
        List<TraceBean> beans = new ArrayList<>();
        for (int index = 0; index < beanCount; index++) {
            String messageId = index == 0 ? "first" : "second";
            boolean invalid = invalidTail && index == 1;
            MessageType messageType = null;
            if (!invalid && type == TraceType.Pub) {
                messageType = MessageType.Normal_Msg;
            } else if (!invalid && type == TraceType.EndTransaction) {
                messageType = MessageType.Trans_msg_Commit;
            }
            LocalTransactionState transactionState = type == TraceType.EndTransaction && !invalid
                ? LocalTransactionState.COMMIT_MESSAGE
                : null;
            beans.add(message("orders", messageId, null, "paid", messageId + "-key",
                "broker:10911", 12, 1, messageType,
                type == TraceType.EndTransaction ? "transaction-" + messageId : null,
                transactionState, false));
        }
        TraceContext context = context(type, "selection-group", "region-selection", 5000,
            true, beans.toArray(TraceBean[]::new));
        context.setCostTime(6);
        context.setAccessChannel(AccessChannel.LOCAL);
        return context;
    }

    private static Map<String, Object> renderSelection(String name, TraceContext context) {
        Map<String, Object> result = new TreeMap<>();
        result.put("name", name);
        result.put("context", selectionContextJson(context));
        try {
            TraceTransferBean transfer = TraceDataEncoder.encoderFromContextBean(context);
            String encoded = transfer.getTransData();
            result.put("encoded", encoded);
            result.put("rowCount", encoded.chars().filter(ch -> ch == '\u0002').count());
            result.put("error", null);
            result.put("transferKeys", new TreeSet<>(transfer.getTransKey()));
        } catch (RuntimeException error) {
            result.put("encoded", null);
            result.put("rowCount", null);
            result.put("error", error.getClass().getSimpleName());
            result.put("transferKeys", null);
        }
        return result;
    }

    private static Map<String, Object> selectionContextJson(TraceContext context) {
        Map<String, Object> value = new TreeMap<>();
        value.put("traceType", context.getTraceType().name());
        value.put("timestampMillis", context.getTimeStamp());
        value.put("regionId", context.getRegionId());
        value.put("regionName", context.getRegionName());
        value.put("groupName", context.getGroupName());
        value.put("costTimeMillis", context.getCostTime());
        value.put("success", context.isSuccess());
        value.put("requestId", context.getRequestId());
        value.put("contextCode", context.getContextCode());
        value.put("accessChannel", context.getAccessChannel() == null
            ? null : context.getAccessChannel().name());
        value.put("text", context.toString());
        List<Map<String, Object>> beans = new ArrayList<>();
        for (TraceBean bean : context.getTraceBeans()) {
            Map<String, Object> beanJson = new TreeMap<>();
            beanJson.put("topic", bean.getTopic());
            beanJson.put("messageId", bean.getMsgId());
            beanJson.put("offsetMessageId", bean.getOffsetMsgId());
            beanJson.put("tags", bean.getTags());
            beanJson.put("keys", bean.getKeys());
            beanJson.put("storeHost", bean.getStoreHost());
            beanJson.put("clientHost", bean.getClientHost());
            beanJson.put("storeTime", bean.getStoreTime());
            beanJson.put("bodyLength", bean.getBodyLength());
            beanJson.put("retryTimes", bean.getRetryTimes());
            beanJson.put("messageTypeOrdinal", bean.getMsgType() == null
                ? null : bean.getMsgType().ordinal());
            beanJson.put("transactionId", bean.getTransactionId());
            beanJson.put("transactionState", bean.getTransactionState() == null
                ? null : bean.getTransactionState().name());
            beanJson.put("fromTransactionCheck", bean.isFromTransactionCheck());
            beans.add(beanJson);
        }
        value.put("traceBeans", beans);
        return value;
    }

    private static Map<String, Object> identityEquality() {
        TraceBean firstBean = new TraceBean();
        TraceBean secondBean = new TraceBean();

        TraceContext firstContext = new TraceContext();
        firstContext.setTraceType(TraceType.Pub);
        firstContext.setTimeStamp(42);
        firstContext.setRegionId("region");
        firstContext.setGroupName("group");
        firstContext.setRequestId("request");
        TraceContext secondContext = new TraceContext();
        secondContext.setTraceType(TraceType.Pub);
        secondContext.setTimeStamp(42);
        secondContext.setRegionId("region");
        secondContext.setGroupName("group");
        secondContext.setRequestId("request");

        Map<String, Object> result = new TreeMap<>();
        result.put("equalValueBeansCompareEqual", firstBean.equals(secondBean));
        result.put("sameBeanReferenceComparesEqual", firstBean.equals(firstBean));
        result.put("equalValueContextsCompareEqual", firstContext.equals(secondContext));
        result.put("sameContextReferenceComparesEqual", firstContext.equals(firstContext));
        result.put("equalTimestampContextsCompareAsZero", firstContext.compareTo(secondContext) == 0);
        try {
            firstContext.compareTo(null);
            result.put("compareNullOtherException", "NONE");
        } catch (Throwable error) {
            result.put("compareNullOtherException", error.getClass().getSimpleName());
        }
        return result;
    }

    private static Map<String, Object> invalidInputThrows() {
        Map<String, Object> result = new TreeMap<>();
        result.put("defaultContext", encodeThrows(new TraceContext()));

        TraceContext pubWithoutBeans = new TraceContext();
        pubWithoutBeans.setTraceType(TraceType.Pub);
        result.put("pubWithoutBeans", encodeThrows(pubWithoutBeans));

        TraceContext pubWithoutMessageType = context(TraceType.Pub, "", "", 1, true,
            new TraceBean());
        result.put("pubWithoutMessageType", encodeThrows(pubWithoutMessageType));

        TraceContext subBeforeWithoutBeans = new TraceContext();
        subBeforeWithoutBeans.setTraceType(TraceType.SubBefore);
        result.put("subBeforeWithoutBeans", encodeThrows(subBeforeWithoutBeans));
        TraceContext subBeforeWithEmptyBeans = context(TraceType.SubBefore, "", "", 1, true);
        result.put("subBeforeWithEmptyBeans", encodeThrows(subBeforeWithEmptyBeans));

        TraceContext subAfterWithoutAccessChannel = context(TraceType.SubAfter, "", "", 1, true,
            new TraceBean());
        result.put("subAfterWithoutAccessChannel", encodeThrows(subAfterWithoutAccessChannel));

        TraceBean transactionWithoutState = new TraceBean();
        transactionWithoutState.setMsgType(MessageType.Normal_Msg);
        TraceContext transactionWithoutStateContext = context(TraceType.EndTransaction, "", "", 1, true,
            transactionWithoutState);
        result.put("transactionWithoutState", encodeThrows(transactionWithoutStateContext));
        return result;
    }

    private static boolean encodeThrows(TraceContext context) {
        try {
            TraceDataEncoder.encoderFromContextBean(context);
            return false;
        } catch (RuntimeException exception) {
            return true;
        }
    }

    private static Map<String, Object> defaults() throws Exception {
        TraceContext context = new TraceContext();
        TraceBean bean = new TraceBean();
        Map<String, Object> result = new TreeMap<>();
        result.put("traceTypeNull", context.getTraceType() == null);
        result.put("traceBeansNull", context.getTraceBeans() == null);
        result.put("timestampPositive", context.getTimeStamp() > 0);
        result.put("regionId", context.getRegionId());
        result.put("regionName", context.getRegionName());
        result.put("groupName", context.getGroupName());
        result.put("costTime", context.getCostTime());
        result.put("success", context.isSuccess());
        result.put("requestIdPresent", context.getRequestId() != null && !context.getRequestId().isEmpty());
        result.put("contextCode", context.getContextCode());
        result.put("accessChannelNull", context.getAccessChannel() == null);
        result.put("text", context.toString());
        result.put("beanTopic", bean.getTopic());
        result.put("beanMessageId", bean.getMsgId());
        result.put("beanOffsetMessageId", bean.getOffsetMsgId());
        result.put("beanTags", bean.getTags());
        result.put("beanKeys", bean.getKeys());
        result.put("beanStoreHostEqualsClientHost", bean.getStoreHost().equals(bean.getClientHost()));
        result.put("beanStoreHostIsAddress", InetAddress.getByName(bean.getStoreHost()) != null);
        result.put("beanStoreTime", bean.getStoreTime());
        result.put("beanRetryTimes", bean.getRetryTimes());
        result.put("beanBodyLength", bean.getBodyLength());
        result.put("beanMessageTypeNull", bean.getMsgType() == null);
        result.put("beanTransactionStateNull", bean.getTransactionState() == null);
        result.put("beanTransactionIdNull", bean.getTransactionId() == null);
        result.put("beanFromTransactionCheck", bean.isFromTransactionCheck());
        return result;
    }

    private static TraceContext context(TraceType type, String group, String region, long timestamp,
        boolean success, TraceBean... beans) {
        TraceContext context = new TraceContext();
        context.setTraceType(type);
        context.setGroupName(group);
        context.setRegionId(region);
        context.setTimeStamp(timestamp);
        context.setSuccess(success);
        context.setRequestId("trace-request-" + timestamp);
        context.setTraceBeans(List.of(beans));
        return context;
    }

    private static TraceBean message(String topic, String messageId, String offsetMessageId, String tags,
        String keys, String storeHost, int bodyLength, int retryTimes, MessageType messageType,
        String transactionId, LocalTransactionState transactionState, boolean fromTransactionCheck) {
        TraceBean bean = new TraceBean();
        bean.setTopic(topic);
        bean.setMsgId(messageId);
        bean.setOffsetMsgId(offsetMessageId);
        bean.setTags(tags);
        bean.setKeys(keys);
        bean.setStoreHost(storeHost);
        bean.setBodyLength(bodyLength);
        bean.setRetryTimes(retryTimes);
        bean.setMsgType(messageType);
        bean.setTransactionId(transactionId);
        bean.setTransactionState(transactionState);
        bean.setFromTransactionCheck(fromTransactionCheck);
        bean.setClientHost(FIXTURE_CLIENT_HOST);
        return bean;
    }

    private static Map<String, Object> render(String name, TraceContext context) {
        TraceTransferBean transfer = TraceDataEncoder.encoderFromContextBean(context);
        Map<String, Object> contextJson = new TreeMap<>();
        contextJson.put("traceType", context.getTraceType().name());
        contextJson.put("timestampMillis", context.getTimeStamp());
        contextJson.put("regionId", context.getRegionId());
        contextJson.put("regionName", context.getRegionName());
        contextJson.put("groupName", context.getGroupName());
        contextJson.put("costTimeMillis", context.getCostTime());
        contextJson.put("success", context.isSuccess());
        contextJson.put("requestId", context.getRequestId());
        contextJson.put("contextCode", context.getContextCode());
        contextJson.put("accessChannel", context.getAccessChannel() == null ? null : context.getAccessChannel().name());
        contextJson.put("text", context.toString());
        List<Map<String, Object>> beans = new ArrayList<>();
        for (TraceBean bean : context.getTraceBeans()) {
            Map<String, Object> value = new TreeMap<>();
            value.put("topic", bean.getTopic());
            value.put("messageId", bean.getMsgId());
            value.put("offsetMessageId", bean.getOffsetMsgId());
            value.put("tags", bean.getTags());
            value.put("keys", bean.getKeys());
            value.put("storeHost", bean.getStoreHost());
            value.put("clientHost", bean.getClientHost());
            value.put("storeTime", bean.getStoreTime());
            value.put("bodyLength", bean.getBodyLength());
            value.put("retryTimes", bean.getRetryTimes());
            value.put("messageTypeOrdinal", bean.getMsgType() == null ? null : bean.getMsgType().ordinal());
            value.put("transactionId", bean.getTransactionId());
            value.put("transactionState", bean.getTransactionState() == null ? null : bean.getTransactionState().name());
            value.put("fromTransactionCheck", bean.isFromTransactionCheck());
            beans.add(value);
        }
        contextJson.put("traceBeans", beans);

        Map<String, Object> result = new TreeMap<>();
        result.put("name", name);
        result.put("context", contextJson);
        result.put("encoded", transfer.getTransData());
        result.put("transferKeys", new TreeSet<>(transfer.getTransKey()));
        return result;
    }
}
