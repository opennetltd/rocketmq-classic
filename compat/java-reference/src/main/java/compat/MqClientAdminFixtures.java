/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.MqClientAdmin;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.admin.MqClientAdminImpl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientExt;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.remoting.protocol.body.CMResult;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.subscription.CustomizedRetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.ExponentialRetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicyType;
import org.apache.rocketmq.remoting.protocol.subscription.RetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.SimpleSubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;

/** Invokes the pinned Java MqClientAdmin asynchronous administrative RPCs. */
public final class MqClientAdminFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";
    private static final String BROKER_ADDRESS = "127.0.0.1:10911";
    private static final long TIMEOUT_MILLIS = 1234L;

    private MqClientAdminFixtures() { }

    private static final class Capture {
        String address;
        long timeoutMillis;
        RemotingCommand request;
        RemotingCommand response;

        RemotingClient proxy() {
            return (RemotingClient) java.lang.reflect.Proxy.newProxyInstance(
                RemotingClient.class.getClassLoader(), new Class<?>[] {RemotingClient.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("invoke")) {
                        address = (String) args[0];
                        request = (RemotingCommand) args[1];
                        timeoutMillis = ((Number) args[2]).longValue();
                        request.makeCustomHeaderToNet();
                        return CompletableFuture.completedFuture(response);
                    }
                    return method.getReturnType() == boolean.class ? false : null;
                });
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException(
                "usage: MqClientAdminFixtures <fixture.json> [--verify]");
        }
        Path output = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(generate()) + "\n";
        if (verify) {
            if (!Files.exists(output) || !json.equals(Files.readString(output))) {
                throw new IllegalStateException("Java MqClientAdmin fixture drift: " + output);
            }
            System.out.println("verified Java 5.5.1 MqClientAdmin fixture at " + output);
        } else {
            Files.createDirectories(output.getParent());
            Files.writeString(output, json);
            System.out.println("wrote Java 5.5.1 MqClientAdmin fixture to " + output);
        }
    }

    private static Map<String, Object> generate() throws Exception {
        Method method = MqClientAdmin.class.getMethod("queryConsumeTimeSpan",
            String.class, QueryConsumeTimeSpanRequestHeader.class, long.class);
        QueryConsumeTimeSpanRequestHeader header = new QueryConsumeTimeSpanRequestHeader();
        header.setTopic("orders");
        header.setGroup("workers");
        header.setNamespace("tenant");
        header.setNamespaced(true);
        header.setBrokerName("broker-a");
        header.setOneway(false);

        QueryConsumeTimeSpanBody body = new QueryConsumeTimeSpanBody();
        QueueTimeSpan span = new QueueTimeSpan();
        span.setMessageQueue(new MessageQueue("orders", "broker-a", 1));
        span.setMinTimeStamp(1_700_000_000_000L);
        span.setMaxTimeStamp(1_700_000_100_000L);
        span.setConsumeTimeStamp(1_700_000_050_000L);
        span.setDelayTime(50_000L);
        body.setConsumeTimeSpanSet(List.of(span));
        byte[] bodyBytes = RemotingSerializable.encode(body);

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        successCapture.response.setBody(bodyBytes);
        MqClientAdminImpl successAdmin = new MqClientAdminImpl(successCapture.proxy());
        List<QueueTimeSpan> spans = successAdmin
            .queryConsumeTimeSpan(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.TOPIC_NOT_EXIST, "topic not exist");
        MqClientAdminImpl errorAdmin = new MqClientAdminImpl(errorCapture.proxy());
        Map<String, Object> errorOutcome = new LinkedHashMap<>();
        try {
            errorAdmin.queryConsumeTimeSpan(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin Broker error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            errorOutcome.put("exceptionClass", clientException.getClass().getName());
            errorOutcome.put("responseCode", clientException.getResponseCode());
            errorOutcome.put("message", clientException.getMessage());
        }

        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", new TreeMap<>(successCapture.request.getExtFields()));
        success.put("responseCode", successCapture.response.getCode());
        success.put("responseBody", new String(bodyBytes, StandardCharsets.UTF_8));
        List<Map<String, Object>> rows = new ArrayList<>();
        for (QueueTimeSpan value : spans) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("topic", value.getMessageQueue().getTopic());
            row.put("brokerName", value.getMessageQueue().getBrokerName());
            row.put("queueId", value.getMessageQueue().getQueueId());
            row.put("minTimeStamp", value.getMinTimeStamp());
            row.put("maxTimeStamp", value.getMaxTimeStamp());
            row.put("consumeTimeStamp", value.getConsumeTimeStamp());
            row.put("delayTime", value.getDelayTime());
            rows.add(row);
        }
        success.put("result", rows);

        Map<String, Object> error = new LinkedHashMap<>();
        error.put("address", errorCapture.address);
        error.put("timeoutMillis", errorCapture.timeoutMillis);
        error.put("requestCode", errorCapture.request.getCode());
        error.put("requestExtFields", new TreeMap<>(errorCapture.request.getExtFields()));
        error.put("responseCode", errorCapture.response.getCode());
        error.put("responseRemark", errorCapture.response.getRemark());
        error.put("outcome", errorOutcome);

        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("artifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", SOURCE_COMMIT);
        fixture.put("interfacePublicMethods", publicMethods(MqClientAdmin.class));
        fixture.put("method", method.toGenericString());
        fixture.put("success", success);
        fixture.put("brokerError", error);
        fixture.put("queryMessage", queryMessage());
        fixture.put("viewMessage", viewMessage());
        fixture.put("updateOrCreateTopic", updateOrCreateTopic());
        fixture.put("deleteTopicInBroker", deleteTopicInBroker());
        fixture.put("deleteTopicInNameserver", deleteTopicInNameserver());
        fixture.put("deleteKvConfig", deleteKvConfig());
        fixture.put("deleteSubscriptionGroup", deleteSubscriptionGroup());
        fixture.put("invokeBrokerToResetOffset", invokeBrokerToResetOffset());
        fixture.put("consumeMessageDirectly", consumeMessageDirectly());
        fixture.put("getBrokerClusterInfo", getBrokerClusterInfo());
        fixture.put("getConsumerConnectionList", getConsumerConnectionList());
        fixture.put("getConsumeStats", getConsumeStats());
        fixture.put("getConsumerRunningInfo", getConsumerRunningInfo());
        fixture.put("updateOrCreateSubscriptionGroup", updateOrCreateSubscriptionGroup());
        return fixture;
    }

    private static Map<String, Object> getConsumerConnectionList() throws Exception {
        Method method = MqClientAdmin.class.getMethod("getConsumerConnectionList", String.class,
            GetConsumerConnectionListRequestHeader.class, long.class);
        GetConsumerConnectionListRequestHeader header = new GetConsumerConnectionListRequestHeader();
        header.setConsumerGroup("workers");
        header.setNamespace("tenant");
        header.setNamespaced(true);
        header.setBrokerName("broker-a");
        header.setOneway(false);

        ConsumerConnection expected = new ConsumerConnection();
        Connection first = new Connection();
        first.setClientId("client-a");
        first.setClientAddr("192.0.2.10:10911");
        first.setLanguage(LanguageCode.RUST);
        first.setVersion(55);
        expected.getConnectionSet().add(first);

        SubscriptionData subscription = new SubscriptionData("orders", "tag-a || tag-b");
        subscription.setClassFilterMode(true);
        subscription.setTagsSet(new TreeSet<>(Set.of("tag-a", "tag-b")));
        subscription.setCodeSet(new TreeSet<>(Set.of(1, 2)));
        subscription.setSubVersion(123L);
        subscription.setExpressionType("SQL92");
        expected.getSubscriptionTable().put("orders", subscription);
        expected.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
        expected.setMessageModel(MessageModel.CLUSTERING);
        expected.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        byte[] bodyBytes = RemotingSerializable.encode(expected);

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        successCapture.response.setBody(bodyBytes);
        ConsumerConnection value = new MqClientAdminImpl(successCapture.proxy())
            .getConsumerConnectionList(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", new TreeMap<>(successCapture.request.getExtFields()));
        success.put("responseCode", successCapture.response.getCode());
        success.put("responseBody", new String(bodyBytes, StandardCharsets.UTF_8));
        success.put("result", consumerConnectionSnapshot(value));
        success.put("consumerConnectionApi", consumerConnectionApi());

        Capture noBodyCapture = new Capture();
        noBodyCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        ConsumerConnection noBody = new MqClientAdminImpl(noBodyCapture.proxy())
            .getConsumerConnectionList(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, Object> successWithoutBody = new LinkedHashMap<>();
        successWithoutBody.put("address", noBodyCapture.address);
        successWithoutBody.put("timeoutMillis", noBodyCapture.timeoutMillis);
        successWithoutBody.put("requestCode", noBodyCapture.request.getCode());
        successWithoutBody.put("requestExtFields", new TreeMap<>(noBodyCapture.request.getExtFields()));
        successWithoutBody.put("responseCode", noBodyCapture.response.getCode());
        successWithoutBody.put("result", noBody == null ? null : consumerConnectionSnapshot(noBody));

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.SYSTEM_ERROR, "consumer connections unavailable");
        Map<String, Object> errorOutcome = new LinkedHashMap<>();
        try {
            new MqClientAdminImpl(errorCapture.proxy())
                .getConsumerConnectionList(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin consumer connection error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            errorOutcome.put("exceptionClass", clientException.getClass().getName());
            errorOutcome.put("responseCode", clientException.getResponseCode());
            errorOutcome.put("message", clientException.getMessage());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>();
        brokerError.put("address", errorCapture.address);
        brokerError.put("timeoutMillis", errorCapture.timeoutMillis);
        brokerError.put("requestCode", errorCapture.request.getCode());
        brokerError.put("requestExtFields", new TreeMap<>(errorCapture.request.getExtFields()));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", errorOutcome);

        Map<String, Object> result = new TreeMap<>();
        result.put("method", method.toGenericString());
        result.put("success", success);
        result.put("successWithoutBody", successWithoutBody);
        result.put("brokerError", brokerError);
        return result;
    }

    private static Map<String, Object> getConsumeStats() throws Exception {
        Method method = MqClientAdmin.class.getMethod("getConsumeStats", String.class,
            GetConsumeStatsRequestHeader.class, long.class);
        GetConsumeStatsRequestHeader header = new GetConsumeStatsRequestHeader();
        header.setConsumerGroup("workers");
        header.setTopic("ignored-topic");
        header.setTopicList("orders;payments;");
        header.setNamespace("tenant");
        header.setNamespaced(true);
        header.setBrokerName("broker-a");
        header.setOneway(false);

        ConsumeStats expected = new ConsumeStats();
        Map<MessageQueue, OffsetWrapper> offsets = new LinkedHashMap<>();
        MessageQueue orders = new MessageQueue("orders", "broker-a", 1);
        OffsetWrapper ordersOffset = new OffsetWrapper();
        ordersOffset.setBrokerOffset(29L);
        ordersOffset.setConsumerOffset(20L);
        ordersOffset.setPullOffset(25L);
        ordersOffset.setLastTimestamp(1000L);
        offsets.put(orders, ordersOffset);
        MessageQueue payments = new MessageQueue("payments", "broker-b", 2);
        OffsetWrapper paymentsOffset = new OffsetWrapper();
        paymentsOffset.setBrokerOffset(42L);
        paymentsOffset.setConsumerOffset(38L);
        paymentsOffset.setPullOffset(41L);
        paymentsOffset.setLastTimestamp(2000L);
        offsets.put(payments, paymentsOffset);
        expected.setOffsetTable(offsets);
        expected.setConsumeTps(1.25);
        byte[] bodyBytes = RemotingSerializable.encode(expected);

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        successCapture.response.setBody(bodyBytes);
        ConsumeStats value = new MqClientAdminImpl(successCapture.proxy())
            .getConsumeStats(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", new TreeMap<>(successCapture.request.getExtFields()));
        success.put("responseCode", successCapture.response.getCode());
        success.put("responseBody", new String(bodyBytes, StandardCharsets.UTF_8));
        success.put("result", consumeStatsSnapshot(value));

        Capture noBodyCapture = new Capture();
        noBodyCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        ConsumeStats noBody = new MqClientAdminImpl(noBodyCapture.proxy())
            .getConsumeStats(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, Object> successWithoutBody = new LinkedHashMap<>();
        successWithoutBody.put("address", noBodyCapture.address);
        successWithoutBody.put("timeoutMillis", noBodyCapture.timeoutMillis);
        successWithoutBody.put("requestCode", noBodyCapture.request.getCode());
        successWithoutBody.put("requestExtFields", new TreeMap<>(noBodyCapture.request.getExtFields()));
        successWithoutBody.put("responseCode", noBodyCapture.response.getCode());
        successWithoutBody.put("result", noBody == null ? null : consumeStatsSnapshot(noBody));

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.SYSTEM_ERROR, "consume stats unavailable");
        Map<String, Object> errorOutcome = new LinkedHashMap<>();
        try {
            new MqClientAdminImpl(errorCapture.proxy())
                .getConsumeStats(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin consume stats error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            errorOutcome.put("exceptionClass", clientException.getClass().getName());
            errorOutcome.put("responseCode", clientException.getResponseCode());
            errorOutcome.put("message", clientException.getMessage());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>();
        brokerError.put("address", errorCapture.address);
        brokerError.put("timeoutMillis", errorCapture.timeoutMillis);
        brokerError.put("requestCode", errorCapture.request.getCode());
        brokerError.put("requestExtFields", new TreeMap<>(errorCapture.request.getExtFields()));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", errorOutcome);

        Map<String, Object> result = new TreeMap<>();
        result.put("method", method.toGenericString());
        result.put("consumeStatsPublicMethods", publicMethodSignatures(ConsumeStats.class));
        result.put("offsetWrapperPublicMethods", publicMethodSignatures(OffsetWrapper.class));
        result.put("dtoSemantics", consumeStatsDtoSemantics());
        result.put("success", success);
        result.put("successWithoutBody", successWithoutBody);
        result.put("brokerError", brokerError);
        return result;
    }

    private static Map<String, Object> getConsumerRunningInfo() throws Exception {
        Method method = MqClientAdmin.class.getMethod("getConsumerRunningInfo", String.class,
            GetConsumerRunningInfoRequestHeader.class, long.class);
        GetConsumerRunningInfoRequestHeader header = new GetConsumerRunningInfoRequestHeader();
        header.setConsumerGroup("workers");
        header.setClientId("client-a");
        header.setJstackEnable(true);
        header.setNamespace("tenant");
        header.setNamespaced(true);
        header.setBrokerName("broker-a");
        header.setOneway(false);

        Path runningInfoFixture = Path.of("../../fixtures/consumer/running-info.json");
        if (!Files.exists(runningInfoFixture)) {
            runningInfoFixture = Path.of("fixtures/consumer/running-info.json");
        }
        String body = JSON.parseObject(Files.readString(runningInfoFixture)).getString("body");
        byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        successCapture.response.setBody(bodyBytes);
        ConsumerRunningInfo info = new MqClientAdminImpl(successCapture.proxy())
            .getConsumerRunningInfo(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, String> properties = new TreeMap<>();
        for (String name : info.getProperties().stringPropertyNames()) {
            properties.put(name, info.getProperties().getProperty(name));
        }
        Map<String, Object> resultSnapshot = new TreeMap<>();
        resultSnapshot.put("jstack", info.getJstack());
        resultSnapshot.put("mqPopTableSize", info.getMqPopTable().size());
        resultSnapshot.put("mqTableSize", info.getMqTable().size());
        resultSnapshot.put("properties", properties);
        resultSnapshot.put("statusTableSize", info.getStatusTable().size());
        resultSnapshot.put("subscriptionSetSize", info.getSubscriptionSet().size());
        resultSnapshot.put("userConsumerInfo", new TreeMap<>(info.getUserConsumerInfo()));

        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", new TreeMap<>(successCapture.request.getExtFields()));
        success.put("responseCode", successCapture.response.getCode());
        success.put("responseBody", body);
        success.put("result", resultSnapshot);

        Capture noBodyCapture = new Capture();
        noBodyCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        ConsumerRunningInfo noBody = new MqClientAdminImpl(noBodyCapture.proxy())
            .getConsumerRunningInfo(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, Object> successWithoutBody = new LinkedHashMap<>();
        successWithoutBody.put("address", noBodyCapture.address);
        successWithoutBody.put("timeoutMillis", noBodyCapture.timeoutMillis);
        successWithoutBody.put("requestCode", noBodyCapture.request.getCode());
        successWithoutBody.put("requestExtFields", new TreeMap<>(noBodyCapture.request.getExtFields()));
        successWithoutBody.put("responseCode", noBodyCapture.response.getCode());
        successWithoutBody.put("resultIsNull", noBody == null);

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.SYSTEM_ERROR, "consumer running info unavailable");
        Map<String, Object> errorOutcome = new LinkedHashMap<>();
        try {
            new MqClientAdminImpl(errorCapture.proxy())
                .getConsumerRunningInfo(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin consumer running info error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            errorOutcome.put("exceptionClass", clientException.getClass().getName());
            errorOutcome.put("responseCode", clientException.getResponseCode());
            errorOutcome.put("message", clientException.getMessage());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>();
        brokerError.put("address", errorCapture.address);
        brokerError.put("timeoutMillis", errorCapture.timeoutMillis);
        brokerError.put("requestCode", errorCapture.request.getCode());
        brokerError.put("requestExtFields", new TreeMap<>(errorCapture.request.getExtFields()));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", errorOutcome);

        Map<String, Object> result = new TreeMap<>();
        result.put("method", method.toGenericString());
        result.put("success", success);
        result.put("successWithoutBody", successWithoutBody);
        result.put("brokerError", brokerError);
        return result;
    }

    private static List<String> publicMethodSignatures(Class<?> type) {
        List<String> signatures = new ArrayList<>();
        for (Method method : type.getDeclaredMethods()) {
            if (java.lang.reflect.Modifier.isPublic(method.getModifiers())) {
                signatures.add(method.toGenericString());
            }
        }
        signatures.sort(String::compareTo);
        return signatures;
    }

    private static Map<String, Object> consumeStatsDtoSemantics() {
        ConsumeStats defaults = new ConsumeStats();
        OffsetWrapper defaultOffset = new OffsetWrapper();
        Map<String, Object> semantics = new TreeMap<>();
        semantics.put("defaultJson", new String(RemotingSerializable.encode(defaults), StandardCharsets.UTF_8));
        semantics.put("defaultConsumeTps", defaults.getConsumeTps());
        semantics.put("defaultOffsetTableNonNull", defaults.getOffsetTable() != null);
        semantics.put("defaultOffsetTableEmpty", defaults.getOffsetTable().isEmpty());
        semantics.put("defaultOffsetValues", List.of(
            defaultOffset.getBrokerOffset(), defaultOffset.getConsumerOffset(),
            defaultOffset.getPullOffset(), defaultOffset.getLastTimestamp()));

        MessageQueue queue = new MessageQueue("orders", "broker-a", 1);
        OffsetWrapper assignedOffset = new OffsetWrapper();
        Map<MessageQueue, OffsetWrapper> assignedMap = new LinkedHashMap<>();
        assignedMap.put(queue, assignedOffset);
        ConsumeStats assigned = new ConsumeStats();
        assigned.setOffsetTable(assignedMap);
        semantics.put("setterRetainsMapIdentity", assigned.getOffsetTable() == assignedMap);
        semantics.put("getterReturnsStoredOffsetIdentity", assigned.getOffsetTable().get(queue) == assignedOffset);
        assigned.getOffsetTable().get(queue).setBrokerOffset(9L);
        semantics.put("getterMutationVisibleToAssignedOffset", assignedOffset.getBrokerOffset() == 9L);
        semantics.put("getterMutationVisibleToAssignedMap", assignedMap.get(queue).getBrokerOffset() == 9L);

        assigned.setOffsetTable(null);
        semantics.put("setterPreservesNull", assigned.getOffsetTable() == null);
        semantics.put("explicitNullJson", new String(RemotingSerializable.encode(assigned), StandardCharsets.UTF_8));
        return semantics;
    }

    private static Map<String, Object> consumeStatsSnapshot(ConsumeStats value) {
        List<Map<String, Object>> offsets = new ArrayList<>();
        for (Map.Entry<MessageQueue, OffsetWrapper> entry : value.getOffsetTable().entrySet()) {
            MessageQueue queue = entry.getKey();
            OffsetWrapper offset = entry.getValue();
            Map<String, Object> row = new TreeMap<>();
            row.put("topic", queue.getTopic());
            row.put("brokerName", queue.getBrokerName());
            row.put("queueId", queue.getQueueId());
            row.put("brokerOffset", offset.getBrokerOffset());
            row.put("consumerOffset", offset.getConsumerOffset());
            row.put("pullOffset", offset.getPullOffset());
            row.put("lastTimestamp", offset.getLastTimestamp());
            offsets.add(row);
        }
        offsets.sort(Comparator.comparing((Map<String, Object> row) -> (String) row.get("topic"))
            .thenComparing(row -> (String) row.get("brokerName"))
            .thenComparingInt(row -> (Integer) row.get("queueId")));
        Map<String, Object> result = new TreeMap<>();
        result.put("consumeTps", value.getConsumeTps());
        result.put("offsetTable", offsets);
        result.put("computeTotalDiff", value.computeTotalDiff());
        result.put("computeInflightTotalDiff", value.computeInflightTotalDiff());
        return result;
    }

    private static Map<String, Object> updateOrCreateSubscriptionGroup() throws Exception {
        Method method = MqClientAdmin.class.getMethod("updateOrCreateSubscriptionGroup", String.class,
            SubscriptionGroupConfig.class, long.class);
        SubscriptionGroupConfig config = subscriptionGroupConfig();
        byte[] bodyBytes = RemotingSerializable.encode(config);

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        new MqClientAdminImpl(successCapture.proxy())
            .updateOrCreateSubscriptionGroup(BROKER_ADDRESS, config, TIMEOUT_MILLIS).get();
        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", successCapture.request.getExtFields() == null
            ? Map.of() : new TreeMap<>(successCapture.request.getExtFields()));
        success.put("responseCode", successCapture.response.getCode());
        success.put("requestBody", new String(successCapture.request.getBody(), StandardCharsets.UTF_8));
        success.put("completed", true);
        success.put("configToString", config.toString());
        success.put("result", subscriptionGroupConfigSnapshot(config));

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.SYSTEM_ERROR, "subscription group update failed");
        Map<String, Object> errorOutcome = new LinkedHashMap<>();
        try {
            new MqClientAdminImpl(errorCapture.proxy())
                .updateOrCreateSubscriptionGroup(BROKER_ADDRESS, config, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin subscription-group error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            errorOutcome.put("exceptionClass", clientException.getClass().getName());
            errorOutcome.put("responseCode", clientException.getResponseCode());
            errorOutcome.put("message", clientException.getMessage());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>();
        brokerError.put("address", errorCapture.address);
        brokerError.put("timeoutMillis", errorCapture.timeoutMillis);
        brokerError.put("requestCode", errorCapture.request.getCode());
        brokerError.put("requestExtFields", errorCapture.request.getExtFields() == null
            ? Map.of() : new TreeMap<>(errorCapture.request.getExtFields()));
        brokerError.put("requestBody", new String(errorCapture.request.getBody(), StandardCharsets.UTF_8));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", errorOutcome);

        Map<String, Object> result = new TreeMap<>();
        result.put("method", method.toGenericString());
        result.put("configPublicMethods", publicMethodSignatures(SubscriptionGroupConfig.class));
        result.put("groupRetryPolicyPublicMethods", publicMethodSignatures(GroupRetryPolicy.class));
        result.put("simpleSubscriptionDataPublicMethods", publicMethodSignatures(SimpleSubscriptionData.class));
        result.put("exponentialRetryPolicyPublicMethods", publicMethodSignatures(ExponentialRetryPolicy.class));
        result.put("customizedRetryPolicyPublicMethods", publicMethodSignatures(CustomizedRetryPolicy.class));
        result.put("retryPolicyPublicMethods", publicMethodSignatures(RetryPolicy.class));
        result.put("groupRetryPolicyTypeNames", List.of(
            GroupRetryPolicyType.EXPONENTIAL.name(), GroupRetryPolicyType.CUSTOMIZED.name()));
        result.put("dtoSemantics", subscriptionGroupConfigSemantics());
        result.put("success", success);
        result.put("brokerError", brokerError);
        return result;
    }

    private static SubscriptionGroupConfig subscriptionGroupConfig() {
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName("workers");
        config.setConsumeEnable(false);
        config.setConsumeFromMinEnable(false);
        config.setConsumeBroadcastEnable(false);
        config.setConsumeMessageOrderly(true);
        config.setRetryQueueNums(4);
        config.setRetryMaxTimes(8);
        GroupRetryPolicy retryPolicy = new GroupRetryPolicy();
        retryPolicy.setType(GroupRetryPolicyType.EXPONENTIAL);
        retryPolicy.setExponentialRetryPolicy(new ExponentialRetryPolicy(1000L, 60_000L, 3L));
        config.setGroupRetryPolicy(retryPolicy);
        config.setBrokerId(7L);
        config.setWhichBrokerWhenConsumeSlowly(9L);
        config.setNotifyConsumerIdsChangedEnable(false);
        config.setGroupSysFlag(3);
        config.setConsumeTimeoutMinute(12);
        java.util.Set<SimpleSubscriptionData> subscriptions = new java.util.LinkedHashSet<>();
        subscriptions.add(new SimpleSubscriptionData("orders", "TAG", "tag-a || tag-b", 10L));
        subscriptions.add(new SimpleSubscriptionData("payments", "SQL92", "amount > 0", 20L));
        config.setSubscriptionDataSet(subscriptions);
        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("stage", "blue");
        attributes.put("tenant", "north");
        config.setAttributes(attributes);
        return config;
    }

    private static Map<String, Object> subscriptionGroupConfigSemantics() {
        SubscriptionGroupConfig defaults = new SubscriptionGroupConfig();
        Map<String, Object> semantics = new TreeMap<>();
        semantics.put("defaultJson", new String(RemotingSerializable.encode(defaults), StandardCharsets.UTF_8));
        semantics.put("defaultConsumeValues", List.of(
            defaults.isConsumeEnable(), defaults.isConsumeFromMinEnable(), defaults.isConsumeBroadcastEnable(),
            defaults.isConsumeMessageOrderly(), defaults.getRetryQueueNums(), defaults.getRetryMaxTimes(),
            defaults.getBrokerId(), defaults.getWhichBrokerWhenConsumeSlowly(),
            defaults.isNotifyConsumerIdsChangedEnable(), defaults.getGroupSysFlag(),
            defaults.getConsumeTimeoutMinute()));
        semantics.put("defaultGroupNameIsNull", defaults.getGroupName() == null);
        semantics.put("defaultGroupRetryPolicyType", defaults.getGroupRetryPolicy().getType().name());
        semantics.put("defaultSubscriptionDataSetIsNull", defaults.getSubscriptionDataSet() == null);
        semantics.put("defaultAttributesEmpty", defaults.getAttributes().isEmpty());
        semantics.put("defaultConfigHashCode", defaults.hashCode());
        semantics.put("defaultRetryDelayAtZero", defaults.getGroupRetryPolicy().getRetryPolicy().nextDelayDuration(0));

        SimpleSubscriptionData left = new SimpleSubscriptionData("orders", "TAG", "a", 1L);
        SimpleSubscriptionData sameIgnoringVersion = new SimpleSubscriptionData("orders", "TAG", "a", 2L);
        semantics.put("simpleSubscriptionEqualityIgnoresVersion", left.equals(sameIgnoringVersion));
        semantics.put("simpleSubscriptionHashCodesMatchIgnoringVersion",
            left.hashCode() == sameIgnoringVersion.hashCode());
        Set<SimpleSubscriptionData> versionEqualitySet = new java.util.LinkedHashSet<>();
        versionEqualitySet.add(left);
        versionEqualitySet.add(sameIgnoringVersion);
        semantics.put("setDeduplicatesVersionOnlyDifference", versionEqualitySet.size() == 1);
        Set<SimpleSubscriptionData> nullableSubscriptionSet = new java.util.LinkedHashSet<>();
        nullableSubscriptionSet.add(null);
        nullableSubscriptionSet.add(null);
        semantics.put("setPreservesOneNullElement",
            nullableSubscriptionSet.size() == 1 && nullableSubscriptionSet.contains(null));
        semantics.put("simpleSubscriptionJavaHashCode", left.hashCode());
        semantics.put("simpleSubscriptionToString", left.toString());
        semantics.put("simpleSubscriptionVersionValue", sameIgnoringVersion.getVersion());

        SubscriptionGroupConfig brokerIdLeft = new SubscriptionGroupConfig();
        SubscriptionGroupConfig brokerIdRight = new SubscriptionGroupConfig();
        brokerIdRight.setBrokerId(19L);
        semantics.put("configEqualityIgnoresBrokerId", brokerIdLeft.equals(brokerIdRight));
        semantics.put("configHashCodesDifferByBrokerId",
            brokerIdLeft.hashCode() != brokerIdRight.hashCode());

        SubscriptionGroupConfig liteDefaults = new SubscriptionGroupConfig();
        semantics.put("defaultPriorityFactor", liteDefaults.getPriorityFactor());
        semantics.put("defaultLiteSubClientQuota", liteDefaults.getLiteSubClientQuota());
        semantics.put("defaultMaxClientEventCount", liteDefaults.getMaxClientEventCount());
        Map<String, String> liteAttributes = new LinkedHashMap<>();
        SubscriptionGroupConfig lite = new SubscriptionGroupConfig();
        lite.setAttributes(liteAttributes);
        lite.setLiteBindTopic("lite-orders");
        lite.setLiteSubExclusive(true);
        lite.setWildcardLiteGroup(true);
        liteAttributes.put("priority.factor", "17");
        liteAttributes.put("lite.sub.client.quota", "123");
        liteAttributes.put("lite.sub.client.max.event.cnt", "42");
        liteAttributes.put("lite.sub.reset.offset.exclusive", "TRUE");
        liteAttributes.put("lite.sub.reset.offset.unsubscribe", "false");
        semantics.put("liteBindTopic", lite.getLiteBindTopic());
        semantics.put("liteSubExclusive", lite.isLiteSubExclusive());
        semantics.put("wildcardLiteGroup", lite.isWildcardLiteGroup());
        semantics.put("priorityFactor", lite.getPriorityFactor());
        semantics.put("liteSubClientQuota", lite.getLiteSubClientQuota());
        semantics.put("maxClientEventCount", lite.getMaxClientEventCount());
        semantics.put("resetOffsetInExclusiveMode", lite.isResetOffsetInExclusiveMode());
        semantics.put("resetOffsetOnUnsubscribe", lite.isResetOffsetOnUnsubscribe());
        lite.setLiteBindTopic(null);
        lite.setLiteSubExclusive(false);
        lite.setWildcardLiteGroup(false);
        semantics.put("falseLiteSettersPreserveAttributes", "lite-orders".equals(lite.getLiteBindTopic())
            && "Exclusive".equals(liteAttributes.get("lite.sub.model"))
            && "true".equals(liteAttributes.get("lite.sub.wildcard")));
        liteAttributes.put("priority.factor", "invalid");
        liteAttributes.put("lite.sub.client.max.event.cnt", "invalid");
        semantics.put("invalidPriorityFactorUsesDefault", lite.getPriorityFactor() == 100);
        semantics.put("invalidMaxClientEventCountUsesDefault", lite.getMaxClientEventCount() == -1);

        GroupRetryPolicy exponential = new GroupRetryPolicy();
        exponential.setType(GroupRetryPolicyType.EXPONENTIAL);
        exponential.setExponentialRetryPolicy(new ExponentialRetryPolicy(1000L, 60_000L, 3L));
        semantics.put("exponentialRetryDelays", List.of(
            exponential.getRetryPolicy().nextDelayDuration(-1),
            exponential.getRetryPolicy().nextDelayDuration(0),
            exponential.getRetryPolicy().nextDelayDuration(1),
            exponential.getRetryPolicy().nextDelayDuration(5),
            exponential.getRetryPolicy().nextDelayDuration(33)));

        Map<String, String> attributes = new LinkedHashMap<>();
        java.util.Set<SimpleSubscriptionData> subscriptions = new java.util.LinkedHashSet<>();
        GroupRetryPolicy policy = new GroupRetryPolicy();
        SubscriptionGroupConfig assigned = new SubscriptionGroupConfig();
        assigned.setAttributes(attributes);
        assigned.setSubscriptionDataSet(subscriptions);
        assigned.setGroupRetryPolicy(policy);
        semantics.put("setterRetainsAttributesIdentity", assigned.getAttributes() == attributes);
        semantics.put("setterRetainsSubscriptionSetIdentity", assigned.getSubscriptionDataSet() == subscriptions);
        semantics.put("setterRetainsRetryPolicyIdentity", assigned.getGroupRetryPolicy() == policy);
        semantics.put("getterReturnsAssignedRetryPolicy", assigned.getGroupRetryPolicy() == policy);
        assigned.setAttributes(null);
        assigned.setSubscriptionDataSet(null);
        assigned.setGroupRetryPolicy(null);
        semantics.put("nullableReferencesPreserved", assigned.getAttributes() == null
            && assigned.getSubscriptionDataSet() == null && assigned.getGroupRetryPolicy() == null);
        return semantics;
    }

    private static Map<String, Object> subscriptionGroupConfigSnapshot(SubscriptionGroupConfig config) {
        Map<String, Object> result = new TreeMap<>();
        result.put("groupName", config.getGroupName());
        result.put("consumeEnable", config.isConsumeEnable());
        result.put("consumeFromMinEnable", config.isConsumeFromMinEnable());
        result.put("consumeBroadcastEnable", config.isConsumeBroadcastEnable());
        result.put("consumeMessageOrderly", config.isConsumeMessageOrderly());
        result.put("retryQueueNums", config.getRetryQueueNums());
        result.put("retryMaxTimes", config.getRetryMaxTimes());
        result.put("brokerId", config.getBrokerId());
        result.put("whichBrokerWhenConsumeSlowly", config.getWhichBrokerWhenConsumeSlowly());
        result.put("notifyConsumerIdsChangedEnable", config.isNotifyConsumerIdsChangedEnable());
        result.put("groupSysFlag", config.getGroupSysFlag());
        result.put("consumeTimeoutMinute", config.getConsumeTimeoutMinute());
        result.put("groupRetryPolicyType", config.getGroupRetryPolicy().getType().name());
        result.put("retryDelays", List.of(
            config.getGroupRetryPolicy().getRetryPolicy().nextDelayDuration(0),
            config.getGroupRetryPolicy().getRetryPolicy().nextDelayDuration(1),
            config.getGroupRetryPolicy().getRetryPolicy().nextDelayDuration(5)));
        List<Map<String, Object>> subscriptions = new ArrayList<>();
        for (SimpleSubscriptionData subscription : config.getSubscriptionDataSet()) {
            Map<String, Object> row = new TreeMap<>();
            row.put("topic", subscription.getTopic());
            row.put("expressionType", subscription.getExpressionType());
            row.put("expression", subscription.getExpression());
            row.put("version", subscription.getVersion());
            subscriptions.add(row);
        }
        subscriptions.sort(Comparator.comparing(row -> (String) row.get("topic")));
        result.put("subscriptionDataSet", subscriptions);
        result.put("attributes", new TreeMap<>(config.getAttributes()));
        return result;
    }

    private static Map<String, Object> consumerConnectionSnapshot(ConsumerConnection value) {
        List<Map<String, Object>> connections = new ArrayList<>();
        for (Connection connection : value.getConnectionSet()) {
            Map<String, Object> row = new TreeMap<>();
            row.put("clientId", connection.getClientId());
            row.put("clientAddr", connection.getClientAddr());
            row.put("language", connection.getLanguage());
            row.put("version", connection.getVersion());
            connections.add(row);
        }
        connections.sort(Comparator.comparing(row -> (String) row.get("clientId")));

        Map<String, Object> subscriptions = new TreeMap<>();
        for (Map.Entry<String, SubscriptionData> entry : value.getSubscriptionTable().entrySet()) {
            SubscriptionData subscription = entry.getValue();
            Map<String, Object> row = new TreeMap<>();
            row.put("classFilterMode", subscription.isClassFilterMode());
            row.put("topic", subscription.getTopic());
            row.put("subString", subscription.getSubString());
            row.put("tagsSet", new TreeSet<>(subscription.getTagsSet()));
            row.put("codeSet", new TreeSet<>(subscription.getCodeSet()));
            row.put("subVersion", subscription.getSubVersion());
            row.put("expressionType", subscription.getExpressionType());
            subscriptions.put(entry.getKey(), row);
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("connectionSet", connections);
        result.put("subscriptionTable", subscriptions);
        result.put("consumeType", value.getConsumeType());
        result.put("messageModel", value.getMessageModel());
        result.put("consumeFromWhere", value.getConsumeFromWhere());
        return result;
    }

    private static Map<String, Object> consumerConnectionApi() throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("connectionPublicConstructors", publicConstructors(Connection.class));
        result.put("connectionPublicMethods", publicMethods(Connection.class));
        result.put("consumerConnectionPublicConstructors", publicConstructors(ConsumerConnection.class));
        result.put("consumerConnectionPublicMethods", publicMethods(ConsumerConnection.class));

        Connection defaultValue = new Connection();
        Map<String, Object> connectionDefaults = new LinkedHashMap<>();
        connectionDefaults.put("clientIdNull", defaultValue.getClientId() == null);
        connectionDefaults.put("clientAddrNull", defaultValue.getClientAddr() == null);
        connectionDefaults.put("languageNull", defaultValue.getLanguage() == null);
        connectionDefaults.put("version", defaultValue.getVersion());
        result.put("connectionDefaults", connectionDefaults);

        Connection assignedConnection = new Connection();
        assignedConnection.setClientId("client-api");
        assignedConnection.setClientAddr("192.0.2.20:10911");
        assignedConnection.setLanguage(LanguageCode.RUST);
        assignedConnection.setVersion(55);
        Map<String, Object> connectionAccessors = new LinkedHashMap<>();
        connectionAccessors.put("clientId", assignedConnection.getClientId());
        connectionAccessors.put("clientAddr", assignedConnection.getClientAddr());
        connectionAccessors.put("language", assignedConnection.getLanguage().name());
        connectionAccessors.put("version", assignedConnection.getVersion());
        result.put("connectionAccessors", connectionAccessors);

        ConsumerConnection defaultConsumerConnection = new ConsumerConnection();
        Map<String, Object> consumerConnectionDefaults = new LinkedHashMap<>();
        consumerConnectionDefaults.put("connectionSetNonNull",
            defaultConsumerConnection.getConnectionSet() != null);
        consumerConnectionDefaults.put("connectionSetEmpty",
            defaultConsumerConnection.getConnectionSet() != null
                && defaultConsumerConnection.getConnectionSet().isEmpty());
        consumerConnectionDefaults.put("subscriptionTableNonNull",
            defaultConsumerConnection.getSubscriptionTable() != null);
        consumerConnectionDefaults.put("subscriptionTableEmpty",
            defaultConsumerConnection.getSubscriptionTable() != null
                && defaultConsumerConnection.getSubscriptionTable().isEmpty());
        consumerConnectionDefaults.put("consumeTypeNull", defaultConsumerConnection.getConsumeType() == null);
        consumerConnectionDefaults.put("messageModelNull", defaultConsumerConnection.getMessageModel() == null);
        consumerConnectionDefaults.put("consumeFromWhereNull",
            defaultConsumerConnection.getConsumeFromWhere() == null);
        result.put("consumerConnectionDefaults", consumerConnectionDefaults);

        ConsumerConnection assignedConsumerConnection = new ConsumerConnection();
        HashSet<Connection> assignedConnectionSet = new HashSet<>();
        Connection firstAssignedConnection = new Connection();
        assignedConnectionSet.add(firstAssignedConnection);
        assignedConsumerConnection.setConnectionSet(assignedConnectionSet);
        Set<Connection> returnedConnectionSet = assignedConsumerConnection.getConnectionSet();
        result.put("connectionSetGetterReturnsAssignedSet",
            returnedConnectionSet == assignedConnectionSet);
        result.put("connectionSetGetterReturnsAssignedElement",
            returnedConnectionSet.iterator().next() == firstAssignedConnection);
        Connection secondAssignedConnection = new Connection();
        returnedConnectionSet.add(secondAssignedConnection);
        result.put("connectionSetGetterMutationVisibleToAssignedSet",
            assignedConnectionSet.contains(secondAssignedConnection));

        Map<String, Object> connectionSetSemantics = new LinkedHashMap<>();
        Connection sameReference = new Connection();
        HashSet<Connection> identitySet = new HashSet<>();
        connectionSetSemantics.put("firstSameReferenceAdd", identitySet.add(sameReference));
        sameReference.setVersion(55);
        connectionSetSemantics.put("sameReferencePresentAfterMutation", identitySet.contains(sameReference));
        connectionSetSemantics.put("secondSameReferenceAdd", identitySet.add(sameReference));
        Connection equalFieldsDifferentReference = new Connection();
        connectionSetSemantics.put("differentReferenceAdd", identitySet.add(equalFieldsDifferentReference));
        connectionSetSemantics.put("differentReferencesCompareEqual", sameReference.equals(equalFieldsDifferentReference));
        connectionSetSemantics.put("sizeBeforeNull", identitySet.size());
        connectionSetSemantics.put("firstNullAdd", identitySet.add(null));
        connectionSetSemantics.put("secondNullAdd", identitySet.add(null));
        connectionSetSemantics.put("sizeAfterNull", identitySet.size());
        connectionSetSemantics.put("equalsDeclaringClass",
            Connection.class.getMethod("equals", Object.class).getDeclaringClass().getName());
        ConsumerConnection decodedSet = JSON.parseObject(
            "{\"connectionSet\":[null,null,{\"version\":7},{\"version\":7}]}",
            ConsumerConnection.class);
        connectionSetSemantics.put("decodedNullAndDistinctObjectsSize",
            decodedSet.getConnectionSet().size());
        result.put("connectionSetSemantics", connectionSetSemantics);

        ConcurrentHashMap<String, SubscriptionData> assignedSubscriptionTable = new ConcurrentHashMap<>();
        SubscriptionData firstSubscription = new SubscriptionData("orders", "tag-a");
        assignedSubscriptionTable.put("orders", firstSubscription);
        assignedConsumerConnection.setSubscriptionTable(assignedSubscriptionTable);
        Map<String, SubscriptionData> returnedSubscriptionTable =
            assignedConsumerConnection.getSubscriptionTable();
        result.put("subscriptionTableGetterReturnsAssignedMap",
            returnedSubscriptionTable == assignedSubscriptionTable);
        result.put("subscriptionTableGetterReturnsAssignedValue",
            returnedSubscriptionTable.get("orders") == firstSubscription);
        returnedSubscriptionTable.put("billing", new SubscriptionData("billing", "tag-b"));
        result.put("subscriptionTableGetterMutationVisibleToAssignedMap",
            assignedSubscriptionTable.containsKey("billing"));

        assignedConsumerConnection.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
        assignedConsumerConnection.setMessageModel(MessageModel.BROADCASTING);
        assignedConsumerConnection.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        Map<String, Object> consumerConnectionAccessors = new LinkedHashMap<>();
        consumerConnectionAccessors.put("consumeType", assignedConsumerConnection.getConsumeType().name());
        consumerConnectionAccessors.put("messageModel", assignedConsumerConnection.getMessageModel().name());
        consumerConnectionAccessors.put("consumeFromWhere",
            assignedConsumerConnection.getConsumeFromWhere().name());
        result.put("consumerConnectionAccessors", consumerConnectionAccessors);

        assignedConsumerConnection.setSubscriptionTable(null);
        result.put("subscriptionTableSetterPreservesNull",
            assignedConsumerConnection.getSubscriptionTable() == null);

        Method method = ConsumerConnection.class.getMethod("computeMinVersion");
        result.put("computeMinVersionMethod", method.toGenericString());
        result.put("defaultEmpty", computeMinVersionOutcome(new ConsumerConnection()));

        ConsumerConnection explicitEmpty = new ConsumerConnection();
        explicitEmpty.setConnectionSet(new HashSet<>());
        result.put("explicitEmpty", computeMinVersionOutcome(explicitEmpty));

        Connection defaultVersion = new Connection();
        ConsumerConnection defaultConnection = new ConsumerConnection();
        defaultConnection.getConnectionSet().add(defaultVersion);
        result.put("defaultConnection", computeMinVersionOutcome(defaultConnection));

        Connection high = new Connection();
        high.setVersion(9);
        Connection low = new Connection();
        low.setVersion(-3);
        Connection middle = new Connection();
        middle.setVersion(4);
        ConsumerConnection mixedVersions = new ConsumerConnection();
        mixedVersions.setConnectionSet(new HashSet<>(List.of(high, low, middle)));
        result.put("mixedVersions", computeMinVersionOutcome(mixedVersions));

        Connection maximum = new Connection();
        maximum.setVersion(Integer.MAX_VALUE);
        Connection minimum = new Connection();
        minimum.setVersion(Integer.MIN_VALUE);
        ConsumerConnection integerBounds = new ConsumerConnection();
        integerBounds.setConnectionSet(new HashSet<>(List.of(maximum, minimum)));
        result.put("integerBounds", computeMinVersionOutcome(integerBounds));

        ConsumerConnection nullSet = new ConsumerConnection();
        nullSet.setConnectionSet(null);
        result.put("nullSet", computeMinVersionOutcome(nullSet));

        ConsumerConnection nullElement = new ConsumerConnection();
        nullElement.getConnectionSet().add(null);
        result.put("nullElement", computeMinVersionOutcome(nullElement));
        return result;
    }

    private static List<String> publicConstructors(Class<?> type) {
        return Arrays.stream(type.getDeclaredConstructors())
            .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
            .map(Constructor::toGenericString)
            .sorted()
            .collect(Collectors.toList());
    }

    private static List<String> publicMethods(Class<?> type) {
        return Arrays.stream(type.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(Method::toGenericString)
            .sorted()
            .collect(Collectors.toList());
    }

    private static Map<String, Object> computeMinVersionOutcome(ConsumerConnection value) {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            result.put("value", value.computeMinVersion());
        } catch (NullPointerException exception) {
            result.put("exceptionClass", exception.getClass().getName());
        }
        return result;
    }

    private static Map<String, Object> getBrokerClusterInfo() throws Exception {
        Method method = MqClientAdmin.class.getMethod("getBrokerClusterInfo", String.class, long.class);
        BrokerData broker = new BrokerData();
        broker.setCluster("cluster-a");
        broker.setBrokerName("broker-a");
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(0L, BROKER_ADDRESS);
        broker.setBrokerAddrs(brokerAddrs);
        broker.setZoneName("zone-a");
        broker.setEnableActingMaster(false);

        ClusterInfo expected = new ClusterInfo();
        Map<String, BrokerData> brokerAddrTable = new TreeMap<>();
        brokerAddrTable.put("broker-a", broker);
        expected.setBrokerAddrTable(brokerAddrTable);
        Map<String, java.util.Set<String>> clusterAddrTable = new TreeMap<>();
        clusterAddrTable.put("cluster-a", new java.util.TreeSet<>(List.of("broker-a")));
        expected.setClusterAddrTable(clusterAddrTable);
        byte[] bodyBytes = RemotingSerializable.encode(expected);

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        successCapture.response.setBody(bodyBytes);
        ClusterInfo value = new MqClientAdminImpl(successCapture.proxy())
            .getBrokerClusterInfo(BROKER_ADDRESS, TIMEOUT_MILLIS).get();
        Map<String, Object> brokerSnapshot = new LinkedHashMap<>();
        BrokerData returnedBroker = value.getBrokerAddrTable().get("broker-a");
        brokerSnapshot.put("cluster", returnedBroker.getCluster());
        brokerSnapshot.put("brokerName", returnedBroker.getBrokerName());
        Map<String, String> addressSnapshot = new TreeMap<>();
        returnedBroker.getBrokerAddrs().forEach((id, address) ->
            addressSnapshot.put(Long.toString(id), address));
        brokerSnapshot.put("brokerAddrs", addressSnapshot);
        brokerSnapshot.put("zoneName", returnedBroker.getZoneName());
        brokerSnapshot.put("enableActingMaster", returnedBroker.isEnableActingMaster());
        Map<String, Object> brokerTableSnapshot = new TreeMap<>();
        brokerTableSnapshot.put("broker-a", brokerSnapshot);
        Map<String, Object> resultSnapshot = new LinkedHashMap<>();
        resultSnapshot.put("brokerAddrTable", brokerTableSnapshot);
        resultSnapshot.put("clusterAddrTable", new TreeMap<>(value.getClusterAddrTable()));
        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", successCapture.request.getExtFields());
        success.put("responseCode", successCapture.response.getCode());
        success.put("responseBody", new String(bodyBytes, StandardCharsets.UTF_8));
        success.put("result", resultSnapshot);

        Capture nullBodyCapture = new Capture();
        nullBodyCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        ClusterInfo nullBodyResult = new MqClientAdminImpl(nullBodyCapture.proxy())
            .getBrokerClusterInfo(BROKER_ADDRESS, TIMEOUT_MILLIS).get();
        Map<String, Object> successWithoutBody = new LinkedHashMap<>();
        successWithoutBody.put("address", nullBodyCapture.address);
        successWithoutBody.put("timeoutMillis", nullBodyCapture.timeoutMillis);
        successWithoutBody.put("requestCode", nullBodyCapture.request.getCode());
        successWithoutBody.put("responseCode", nullBodyCapture.response.getCode());
        successWithoutBody.put("result", nullBodyResult);

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.SYSTEM_ERROR, "cluster info unavailable");
        Map<String, Object> errorOutcome = new LinkedHashMap<>();
        try {
            new MqClientAdminImpl(errorCapture.proxy())
                .getBrokerClusterInfo(BROKER_ADDRESS, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin cluster info error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            errorOutcome.put("exceptionClass", clientException.getClass().getName());
            errorOutcome.put("responseCode", clientException.getResponseCode());
            errorOutcome.put("message", clientException.getMessage());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>();
        brokerError.put("address", errorCapture.address);
        brokerError.put("timeoutMillis", errorCapture.timeoutMillis);
        brokerError.put("requestCode", errorCapture.request.getCode());
        brokerError.put("requestExtFields", errorCapture.request.getExtFields());
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", errorOutcome);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("method", method.toGenericString());
        result.put("success", success);
        result.put("successWithoutBody", successWithoutBody);
        result.put("brokerError", brokerError);
        return result;
    }

    private static Map<String, Object> consumeMessageDirectly() throws Exception {
        Method method = MqClientAdmin.class.getMethod("consumeMessageDirectly", String.class,
            ConsumeMessageDirectlyResultRequestHeader.class, long.class);
        ConsumeMessageDirectlyResultRequestHeader header = new ConsumeMessageDirectlyResultRequestHeader();
        header.setConsumerGroup("workers");
        header.setClientId("client-a");
        header.setMsgId("message-1");
        header.setBrokerName("broker-a");
        header.setTopic("orders");
        header.setTopicSysFlag(3);
        header.setGroupSysFlag(5);
        header.setNamespace("tenant");
        header.setNamespaced(true);
        header.setBname("rpc-broker-a");
        header.setOneway(false);

        ConsumeMessageDirectlyResult expected = new ConsumeMessageDirectlyResult();
        expected.setOrder(true);
        expected.setAutoCommit(false);
        expected.setConsumeResult(CMResult.CR_SUCCESS);
        expected.setRemark("consumed directly");
        expected.setSpentTimeMills(37L);
        byte[] bodyBytes = RemotingSerializable.encode(expected);

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        successCapture.response.setBody(bodyBytes);
        ConsumeMessageDirectlyResult value = new MqClientAdminImpl(successCapture.proxy())
            .consumeMessageDirectly(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, Object> resultSnapshot = new LinkedHashMap<>();
        resultSnapshot.put("order", value.isOrder());
        resultSnapshot.put("autoCommit", value.isAutoCommit());
        resultSnapshot.put("consumeResult", value.getConsumeResult());
        resultSnapshot.put("remark", value.getRemark());
        resultSnapshot.put("spentTimeMills", value.getSpentTimeMills());
        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", new TreeMap<>(successCapture.request.getExtFields()));
        success.put("responseCode", successCapture.response.getCode());
        success.put("responseBody", new String(bodyBytes, StandardCharsets.UTF_8));
        success.put("result", resultSnapshot);

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.SYSTEM_ERROR, "direct consume failed");
        Map<String, Object> errorOutcome = new LinkedHashMap<>();
        try {
            new MqClientAdminImpl(errorCapture.proxy())
                .consumeMessageDirectly(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin direct consume error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            errorOutcome.put("exceptionClass", clientException.getClass().getName());
            errorOutcome.put("responseCode", clientException.getResponseCode());
            errorOutcome.put("message", clientException.getMessage());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>();
        brokerError.put("address", errorCapture.address);
        brokerError.put("timeoutMillis", errorCapture.timeoutMillis);
        brokerError.put("requestCode", errorCapture.request.getCode());
        brokerError.put("requestExtFields", new TreeMap<>(errorCapture.request.getExtFields()));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", errorOutcome);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("method", method.toGenericString());
        result.put("success", success);
        result.put("brokerError", brokerError);
        return result;
    }

    private static Map<String, Object> queryMessage() throws Exception {
        Method method = MqClientAdmin.class.getMethod("queryMessage", String.class, boolean.class,
            boolean.class, QueryMessageRequestHeader.class, long.class);
        QueryMessageRequestHeader keyHeader = queryHeader("green");
        QueryMessageRequestHeader uniqueHeader = queryHeader("unique-1");
        QueryMessageRequestHeader emptyKeyHeader = queryHeader("");
        QueryMessageRequestHeader nonBreakingSpaceHeader = queryHeader("\u00a0");
        QueryMessageRequestHeader javaWhitespaceHeader = queryHeader("\u2003");
        QueryMessageRequestHeader nextLineHeader = queryHeader("\u0085");
        QueryMessageRequestHeader fileSeparatorHeader = queryHeader("\u001c");

        MessageExt compressedMatch = queryMessageRecord("orders", "compressed-body", 1,
            MessageSysFlag.COMPRESSED_FLAG | MessageSysFlag.COMPRESSION_ZLIB_TYPE,
            "red green", "unique-1");
        MessageExt wrongKey = queryMessageRecord("orders", "unmatched-body", 2, 0,
            "greenish", "unique-2");
        MessageExt wrongTopic = queryMessageRecord("other", "wrong-topic-body", 3, 0,
            "green", "unique-3");
        MessageExt internalEmptyKey = queryMessageRecord("orders", "internal-empty-key", 4, 0,
            "alpha  beta", "unique-4");
        MessageExt trailingEmptyKey = queryMessageRecord("orders", "trailing-empty-key", 5, 0,
            "alpha ", "unique-5");
        MessageExt nonBreakingSpaceKey = queryMessageRecord("orders", "nbsp-key", 6, 0,
            "\u00a0", "unique-6");
        MessageExt javaWhitespaceKey = queryMessageRecord("orders", "em-space-key", 7, 0,
            "\u2003", "unique-7");
        MessageExt nextLineKey = queryMessageRecord("orders", "next-line-key", 8, 0,
            "\u0085", "unique-8");
        MessageExt fileSeparatorKey = queryMessageRecord("orders", "file-separator-key", 9, 0,
            "\u001c", "unique-9");
        byte[] responseBody = encodeQueryMessages(List.of(compressedMatch, wrongKey, wrongTopic,
            internalEmptyKey, trailingEmptyKey, nonBreakingSpaceKey, javaWhitespaceKey,
            nextLineKey, fileSeparatorKey));

        Map<String, Object> keyQueryDecompressed = queryMessageCall(false, true, keyHeader, responseBody);
        Map<String, Object> keyQueryCompressed = queryMessageCall(false, false, keyHeader, responseBody);
        Map<String, Object> uniqueQuery = queryMessageCall(true, false, uniqueHeader, responseBody);
        Map<String, Object> emptyKeyQuery = queryMessageCall(false, false, emptyKeyHeader, responseBody);
        Map<String, Object> nonBreakingSpaceQuery = queryMessageCall(false, false,
            nonBreakingSpaceHeader, responseBody);
        Map<String, Object> javaWhitespaceQuery = queryMessageCall(false, false,
            javaWhitespaceHeader, responseBody);
        Map<String, Object> nextLineQuery = queryMessageCall(false, false, nextLineHeader, responseBody);
        Map<String, Object> fileSeparatorQuery = queryMessageCall(false, false,
            fileSeparatorHeader, responseBody);

        Capture notFoundCapture = new Capture();
        notFoundCapture.response = RemotingCommand.createResponseCommand(ResponseCode.QUERY_NOT_FOUND,
            "query not found");
        List<MessageExt> notFoundMessages = new MqClientAdminImpl(notFoundCapture.proxy())
            .queryMessage(BROKER_ADDRESS, false, true, keyHeader, TIMEOUT_MILLIS).get();
        Map<String, Object> notFound = queryMessageCallResult(notFoundCapture, notFoundMessages);

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR,
            "query failed");
        Map<String, Object> errorOutcome = new LinkedHashMap<>();
        try {
            new MqClientAdminImpl(errorCapture.proxy())
                .queryMessage(BROKER_ADDRESS, false, true, keyHeader, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin query error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            errorOutcome.put("exceptionClass", clientException.getClass().getName());
            errorOutcome.put("responseCode", clientException.getResponseCode());
            errorOutcome.put("message", clientException.getMessage());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>(queryMessageCapture(errorCapture));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", errorOutcome);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("method", method.toGenericString());
        result.put("responseBodyBase64", Base64.getEncoder().encodeToString(responseBody));
        result.put("keyQueryDecompressed", keyQueryDecompressed);
        result.put("keyQueryCompressed", keyQueryCompressed);
        result.put("uniqueQuery", uniqueQuery);
        result.put("emptyKeyQuery", emptyKeyQuery);
        result.put("nonBreakingSpaceQuery", nonBreakingSpaceQuery);
        result.put("javaWhitespaceQuery", javaWhitespaceQuery);
        result.put("nextLineQuery", nextLineQuery);
        result.put("fileSeparatorQuery", fileSeparatorQuery);
        result.put("queryNotFound", notFound);
        result.put("brokerError", brokerError);
        return result;
    }

    private static QueryMessageRequestHeader queryHeader(String key) {
        QueryMessageRequestHeader header = new QueryMessageRequestHeader();
        header.setTopic("orders");
        header.setKey(key);
        header.setMaxNum(3);
        header.setBeginTimestamp(1_700_000_000_000L);
        header.setEndTimestamp(1_700_000_300_000L);
        header.setIndexType("KEY");
        return header;
    }

    private static MessageExt queryMessageRecord(String topic, String body, long queueOffset,
        int sysFlag, String keys, String uniqueKey) throws Exception {
        MessageExt message = new MessageExt();
        message.setTopic(topic);
        message.setBody(body.getBytes(StandardCharsets.UTF_8));
        message.setQueueId(1);
        message.setQueueOffset(queueOffset);
        message.setCommitLogOffset(1_700_000_000_000L + queueOffset);
        message.setSysFlag(sysFlag);
        message.setFlag(0);
        message.setBodyCRC(0);
        message.setBornTimestamp(1_700_000_100_000L + queueOffset);
        message.setStoreTimestamp(1_700_000_200_000L + queueOffset);
        message.setBornHost(new InetSocketAddress(InetAddress.getByName("192.0.2.1"), 10911));
        message.setStoreHost(new InetSocketAddress(InetAddress.getByName("192.0.2.2"), 10911));
        message.setReconsumeTimes(0);
        message.setPreparedTransactionOffset(0);
        MessageAccessor.putProperty(message, org.apache.rocketmq.common.message.MessageConst.PROPERTY_KEYS, keys);
        MessageAccessor.putProperty(message,
            org.apache.rocketmq.common.message.MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, uniqueKey);
        return message;
    }

    private static byte[] encodeQueryMessages(List<MessageExt> messages) throws Exception {
        List<byte[]> records = new ArrayList<>();
        int length = 0;
        for (MessageExt message : messages) {
            byte[] record = MessageDecoder.encode(message, true);
            records.add(record);
            length += record.length;
        }
        byte[] result = new byte[length];
        int offset = 0;
        for (byte[] record : records) {
            System.arraycopy(record, 0, result, offset, record.length);
            offset += record.length;
        }
        return result;
    }

    private static Map<String, Object> queryMessageCall(boolean uniqueKey, boolean decompressBody,
        QueryMessageRequestHeader header, byte[] responseBody) throws Exception {
        Capture capture = new Capture();
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody(responseBody);
        List<MessageExt> messages = new MqClientAdminImpl(capture.proxy())
            .queryMessage(BROKER_ADDRESS, uniqueKey, decompressBody, header, TIMEOUT_MILLIS).get();
        Map<String, Object> result = new LinkedHashMap<>(queryMessageCapture(capture));
        result.put("uniqueKeyFlag", uniqueKey);
        result.put("decompressBody", decompressBody);
        result.put("responseCode", capture.response.getCode());
        result.put("result", queryMessageSnapshots(messages));
        return result;
    }

    private static Map<String, Object> queryMessageCapture(Capture capture) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("address", capture.address);
        result.put("timeoutMillis", capture.timeoutMillis);
        result.put("requestCode", capture.request.getCode());
        result.put("requestExtFields", new TreeMap<>(capture.request.getExtFields()));
        return result;
    }

    private static Map<String, Object> queryMessageCallResult(Capture capture, List<MessageExt> messages) {
        Map<String, Object> result = new LinkedHashMap<>(queryMessageCapture(capture));
        result.put("responseCode", capture.response.getCode());
        result.put("responseRemark", capture.response.getRemark());
        result.put("result", queryMessageSnapshots(messages));
        return result;
    }

    private static List<Map<String, Object>> queryMessageSnapshots(List<MessageExt> messages) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (MessageExt message : messages) {
            result.add(queryMessageSnapshot(message));
        }
        return result;
    }

    private static Map<String, Object> queryMessageSnapshot(MessageExt message) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("topic", message.getTopic());
        result.put("messageId", message.getMsgId());
        result.put("offsetMessageId", message instanceof MessageClientExt
            ? ((MessageClientExt) message).getOffsetMsgId() : null);
        result.put("bodyBase64", message.getBody() == null
            ? null : Base64.getEncoder().encodeToString(message.getBody()));
        result.put("sysFlag", message.getSysFlag());
        result.put("keys", message.getKeys());
        result.put("properties", new TreeMap<>(message.getProperties()));
        return result;
    }

    private static Map<String, Object> viewMessage() throws Exception {
        Method method = MqClientAdmin.class.getMethod("viewMessage", String.class,
            ViewMessageRequestHeader.class, long.class);
        ViewMessageRequestHeader header = new ViewMessageRequestHeader();
        header.setTopic("orders");
        header.setOffset(4L);
        byte[] responseBody = MessageDecoder.encode(
            queryMessageRecord("orders", "view-body", 4, 0, "view-key", "view-unique"), false);

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        successCapture.response.setBody(responseBody);
        MessageExt message = new MqClientAdminImpl(successCapture.proxy())
            .viewMessage(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, Object> success = new LinkedHashMap<>(queryMessageCapture(successCapture));
        success.put("responseCode", successCapture.response.getCode());
        success.put("responseBodyBase64", Base64.getEncoder().encodeToString(responseBody));
        success.put("result", queryMessageSnapshot(message));

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(ResponseCode.TOPIC_NOT_EXIST,
            "view topic not found");
        Map<String, Object> errorOutcome = new LinkedHashMap<>();
        try {
            new MqClientAdminImpl(errorCapture.proxy())
                .viewMessage(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin view message error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            errorOutcome.put("exceptionClass", clientException.getClass().getName());
            errorOutcome.put("responseCode", clientException.getResponseCode());
            errorOutcome.put("message", clientException.getMessage());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>(queryMessageCapture(errorCapture));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", errorOutcome);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("method", method.toGenericString());
        result.put("success", success);
        result.put("brokerError", brokerError);
        return result;
    }

    private static Map<String, Object> deleteTopicInBroker() throws Exception {
        Method method = MqClientAdmin.class.getMethod("deleteTopicInBroker",
            String.class, DeleteTopicRequestHeader.class, long.class);
        DeleteTopicRequestHeader header = new DeleteTopicRequestHeader();
        header.setTopic("orders");
        header.setNamespace("tenant");
        header.setNamespaced(true);
        header.setBrokerName("broker-a");
        header.setOneway(false);
        header.setLo(true);

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        MqClientAdminImpl successAdmin = new MqClientAdminImpl(successCapture.proxy());
        successAdmin.deleteTopicInBroker(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", new TreeMap<>(successCapture.request.getExtFields()));
        success.put("responseCode", successCapture.response.getCode());
        success.put("completed", true);

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.TOPIC_NOT_EXIST, "topic not exist");
        MqClientAdminImpl errorAdmin = new MqClientAdminImpl(errorCapture.proxy());
        Map<String, Object> outcome = new LinkedHashMap<>();
        try {
            errorAdmin.deleteTopicInBroker(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin delete topic error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            outcome.put("exceptionClass", clientException.getClass().getName());
            outcome.put("responseCode", clientException.getResponseCode());
            outcome.put("message", clientException.getMessage());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>();
        brokerError.put("address", errorCapture.address);
        brokerError.put("timeoutMillis", errorCapture.timeoutMillis);
        brokerError.put("requestCode", errorCapture.request.getCode());
        brokerError.put("requestExtFields", new TreeMap<>(errorCapture.request.getExtFields()));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", outcome);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("method", method.toGenericString());
        result.put("success", success);
        result.put("brokerError", brokerError);
        return result;
    }

    private static Map<String, Object> deleteTopicInNameserver() throws Exception {
        Method method = MqClientAdmin.class.getMethod("deleteTopicInNameserver",
            String.class, DeleteTopicFromNamesrvRequestHeader.class, long.class);
        DeleteTopicFromNamesrvRequestHeader header = new DeleteTopicFromNamesrvRequestHeader();
        header.setTopic("orders");
        header.setClusterName("cluster-a");
        header.setNamespace("tenant");
        header.setNamespaced(true);
        header.setBrokerName("broker-a");
        header.setOneway(false);
        header.setLo(true);

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        MqClientAdminImpl successAdmin = new MqClientAdminImpl(successCapture.proxy());
        successAdmin.deleteTopicInNameserver(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", new TreeMap<>(successCapture.request.getExtFields()));
        success.put("responseCode", successCapture.response.getCode());
        success.put("completed", true);

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.TOPIC_NOT_EXIST, "topic not exist");
        MqClientAdminImpl errorAdmin = new MqClientAdminImpl(errorCapture.proxy());
        Map<String, Object> outcome = new LinkedHashMap<>();
        try {
            errorAdmin.deleteTopicInNameserver(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin nameserver delete error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            outcome.put("exceptionClass", clientException.getClass().getName());
            outcome.put("responseCode", clientException.getResponseCode());
            outcome.put("message", clientException.getMessage());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>();
        brokerError.put("address", errorCapture.address);
        brokerError.put("timeoutMillis", errorCapture.timeoutMillis);
        brokerError.put("requestCode", errorCapture.request.getCode());
        brokerError.put("requestExtFields", new TreeMap<>(errorCapture.request.getExtFields()));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", outcome);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("method", method.toGenericString());
        result.put("success", success);
        result.put("brokerError", brokerError);
        return result;
    }

    private static Map<String, Object> deleteKvConfig() throws Exception {
        Method method = MqClientAdmin.class.getMethod("deleteKvConfig",
            String.class, DeleteKVConfigRequestHeader.class, long.class);
        DeleteKVConfigRequestHeader header = new DeleteKVConfigRequestHeader();
        header.setNamespace("config-namespace");
        header.setKey("config-key");

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        MqClientAdminImpl successAdmin = new MqClientAdminImpl(successCapture.proxy());
        successAdmin.deleteKvConfig(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", new TreeMap<>(successCapture.request.getExtFields()));
        success.put("responseCode", successCapture.response.getCode());
        success.put("completed", true);

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.SYSTEM_ERROR, "kv config delete failed");
        MqClientAdminImpl errorAdmin = new MqClientAdminImpl(errorCapture.proxy());
        Map<String, Object> outcome = new LinkedHashMap<>();
        try {
            errorAdmin.deleteKvConfig(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin delete KV config error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            outcome.put("exceptionClass", clientException.getClass().getName());
            outcome.put("responseCode", clientException.getResponseCode());
            outcome.put("message", clientException.getMessage());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>();
        brokerError.put("address", errorCapture.address);
        brokerError.put("timeoutMillis", errorCapture.timeoutMillis);
        brokerError.put("requestCode", errorCapture.request.getCode());
        brokerError.put("requestExtFields", new TreeMap<>(errorCapture.request.getExtFields()));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", outcome);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("method", method.toGenericString());
        result.put("success", success);
        result.put("brokerError", brokerError);
        return result;
    }

    private static Map<String, Object> deleteSubscriptionGroup() throws Exception {
        Method method = MqClientAdmin.class.getMethod("deleteSubscriptionGroup",
            String.class, DeleteSubscriptionGroupRequestHeader.class, long.class);
        DeleteSubscriptionGroupRequestHeader header = new DeleteSubscriptionGroupRequestHeader();
        header.setGroupName("workers");
        header.setCleanOffset(true);
        header.setNamespace("tenant");
        header.setNamespaced(true);
        header.setBrokerName("broker-a");
        header.setOneway(false);

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        MqClientAdminImpl successAdmin = new MqClientAdminImpl(successCapture.proxy());
        successAdmin.deleteSubscriptionGroup(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", new TreeMap<>(successCapture.request.getExtFields()));
        success.put("responseCode", successCapture.response.getCode());
        success.put("completed", true);

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.SYSTEM_ERROR, "subscription group delete failed");
        MqClientAdminImpl errorAdmin = new MqClientAdminImpl(errorCapture.proxy());
        Map<String, Object> outcome = new LinkedHashMap<>();
        try {
            errorAdmin.deleteSubscriptionGroup(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin delete subscription group error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            outcome.put("exceptionClass", clientException.getClass().getName());
            outcome.put("responseCode", clientException.getResponseCode());
            outcome.put("message", clientException.getMessage());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>();
        brokerError.put("address", errorCapture.address);
        brokerError.put("timeoutMillis", errorCapture.timeoutMillis);
        brokerError.put("requestCode", errorCapture.request.getCode());
        brokerError.put("requestExtFields", new TreeMap<>(errorCapture.request.getExtFields()));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", outcome);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("method", method.toGenericString());
        result.put("success", success);
        result.put("brokerError", brokerError);
        return result;
    }

    private static Map<String, Object> invokeBrokerToResetOffset() throws Exception {
        Method method = MqClientAdmin.class.getMethod("invokeBrokerToResetOffset",
            String.class, ResetOffsetRequestHeader.class, long.class);
        ResetOffsetRequestHeader header = new ResetOffsetRequestHeader();
        header.setGroup("workers");
        header.setTopic("orders");
        header.setQueueId(1);
        header.setOffset(9L);
        header.setTimestamp(1_712_345_678_901L);
        header.setForce(true);
        header.setNamespace("tenant");
        header.setNamespaced(true);
        header.setBrokerName("broker-a");
        header.setOneway(false);

        ResetOffsetBody body = new ResetOffsetBody();
        body.getOffsetTable().put(new MessageQueue("orders", "broker-a", 1), 9L);
        byte[] bodyBytes = RemotingSerializable.encode(body);
        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        successCapture.response.setBody(bodyBytes);
        MqClientAdminImpl successAdmin = new MqClientAdminImpl(successCapture.proxy());
        Map<MessageQueue, Long> offsets = successAdmin
            .invokeBrokerToResetOffset(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map.Entry<MessageQueue, Long> entry : offsets.entrySet()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("topic", entry.getKey().getTopic());
            row.put("brokerName", entry.getKey().getBrokerName());
            row.put("queueId", entry.getKey().getQueueId());
            row.put("offset", entry.getValue());
            rows.add(row);
        }
        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", new TreeMap<>(successCapture.request.getExtFields()));
        success.put("responseCode", successCapture.response.getCode());
        success.put("responseBody", new String(bodyBytes, StandardCharsets.UTF_8));
        success.put("result", rows);

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.SYSTEM_ERROR, "reset offset failed");
        MqClientAdminImpl errorAdmin = new MqClientAdminImpl(errorCapture.proxy());
        Map<String, Object> errorOutcome = new LinkedHashMap<>();
        try {
            errorAdmin.invokeBrokerToResetOffset(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin reset offset error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            errorOutcome.put("exceptionClass", clientException.getClass().getName());
            errorOutcome.put("responseCode", clientException.getResponseCode());
            errorOutcome.put("message", clientException.getMessage());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>();
        brokerError.put("address", errorCapture.address);
        brokerError.put("timeoutMillis", errorCapture.timeoutMillis);
        brokerError.put("requestCode", errorCapture.request.getCode());
        brokerError.put("requestExtFields", new TreeMap<>(errorCapture.request.getExtFields()));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", errorOutcome);

        Capture noBodyCapture = new Capture();
        noBodyCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        MqClientAdminImpl noBodyAdmin = new MqClientAdminImpl(noBodyCapture.proxy());
        Map<String, Object> noBodyOutcome = new LinkedHashMap<>();
        try {
            noBodyAdmin.invokeBrokerToResetOffset(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin reset offset missing-body error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            noBodyOutcome.put("exceptionClass", clientException.getClass().getName());
            noBodyOutcome.put("responseCode", clientException.getResponseCode());
            noBodyOutcome.put("message", clientException.getMessage());
        }
        Map<String, Object> successWithoutBody = new LinkedHashMap<>();
        successWithoutBody.put("address", noBodyCapture.address);
        successWithoutBody.put("timeoutMillis", noBodyCapture.timeoutMillis);
        successWithoutBody.put("requestCode", noBodyCapture.request.getCode());
        successWithoutBody.put("requestExtFields", new TreeMap<>(noBodyCapture.request.getExtFields()));
        successWithoutBody.put("responseCode", noBodyCapture.response.getCode());
        successWithoutBody.put("responseRemark", noBodyCapture.response.getRemark());
        successWithoutBody.put("outcome", noBodyOutcome);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("method", method.toGenericString());
        result.put("success", success);
        result.put("brokerError", brokerError);
        result.put("successWithoutBody", successWithoutBody);
        return result;
    }

    private static Map<String, Object> updateOrCreateTopic() throws Exception {
        Method method = MqClientAdmin.class.getMethod("updateOrCreateTopic",
            String.class, CreateTopicRequestHeader.class, long.class);
        CreateTopicRequestHeader header = new CreateTopicRequestHeader();
        header.setTopic("orders");
        header.setDefaultTopic("TBW102");
        header.setReadQueueNums(4);
        header.setWriteQueueNums(4);
        header.setPerm(6);
        header.setTopicFilterType("SINGLE_TAG");
        header.setTopicSysFlag(3);
        header.setOrder(false);
        header.setAttributes("+unit=true");
        header.setForce(false);
        header.setNamespace("tenant");
        header.setNamespaced(true);
        header.setBrokerName("broker-a");
        header.setOneway(false);

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        MqClientAdminImpl successAdmin = new MqClientAdminImpl(successCapture.proxy());
        successAdmin.updateOrCreateTopic(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", new TreeMap<>(successCapture.request.getExtFields()));
        success.put("requestBody", successCapture.request.getBody());
        success.put("responseCode", successCapture.response.getCode());
        success.put("completed", true);

        CreateTopicRequestHeader invalidPermissionHeader = new CreateTopicRequestHeader();
        invalidPermissionHeader.setTopic("orders");
        invalidPermissionHeader.setPerm(8);
        Capture invalidPermissionCapture = new Capture();
        invalidPermissionCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        new MqClientAdminImpl(invalidPermissionCapture.proxy())
            .updateOrCreateTopic(BROKER_ADDRESS, invalidPermissionHeader, TIMEOUT_MILLIS).get();
        Map<String, Object> invalidPermission = new LinkedHashMap<>();
        invalidPermission.put("address", invalidPermissionCapture.address);
        invalidPermission.put("timeoutMillis", invalidPermissionCapture.timeoutMillis);
        invalidPermission.put("requestCode", invalidPermissionCapture.request.getCode());
        invalidPermission.put("requestExtFields", new TreeMap<>(invalidPermissionCapture.request.getExtFields()));
        invalidPermission.put("requestBody", invalidPermissionCapture.request.getBody());
        invalidPermission.put("responseCode", invalidPermissionCapture.response.getCode());
        invalidPermission.put("completed", true);

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.SYSTEM_ERROR, "topic update failed");
        MqClientAdminImpl errorAdmin = new MqClientAdminImpl(errorCapture.proxy());
        Map<String, Object> outcome = new LinkedHashMap<>();
        try {
            errorAdmin.updateOrCreateTopic(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin topic update error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            outcome.put("exceptionClass", clientException.getClass().getName());
            outcome.put("responseCode", clientException.getResponseCode());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>();
        brokerError.put("address", errorCapture.address);
        brokerError.put("timeoutMillis", errorCapture.timeoutMillis);
        brokerError.put("requestCode", errorCapture.request.getCode());
        brokerError.put("requestExtFields", new TreeMap<>(errorCapture.request.getExtFields()));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", outcome);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("method", method.toGenericString());
        result.put("success", success);
        result.put("invalidPermission", invalidPermission);
        result.put("brokerError", brokerError);
        return result;
    }
}
