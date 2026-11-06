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
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.rocketmq.remoting.rpc.RpcRequestHeader;

/** Invoked Java 5.5.1 oracle for custom RpcRequestHeader equality. */
public final class RequestHeaderIdentityFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";
    private static final Map<String, String> RUST_TO_JAVA_CLASSES = Map.ofEntries(
        Map.entry("AckMessageRequestHeader", "org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader"),
        Map.entry("ChangeInvisibleTimeRequestHeader", "org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader"),
        Map.entry("CheckTransactionStateRequestHeader", "org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader"),
        Map.entry("CloneGroupOffsetRequestHeader", "org.apache.rocketmq.remoting.protocol.header.CloneGroupOffsetRequestHeader"),
        Map.entry("ConsumeMessageDirectlyResultRequestHeader", "org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader"),
        Map.entry("ConsumerSendMsgBackRequestHeader", "org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader"),
        Map.entry("CreateTopicRequestHeader", "org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader"),
        Map.entry("DeleteTopicRequestHeader", "org.apache.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader"),
        Map.entry("DeleteTopicFromNamesrvRequestHeader", "org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader"),
        Map.entry("DeleteSubscriptionGroupRequestHeader", "org.apache.rocketmq.remoting.protocol.header.DeleteSubscriptionGroupRequestHeader"),
        Map.entry("EndTransactionRequestHeader", "org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader"),
        Map.entry("GetConsumerListByGroupRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader"),
        Map.entry("GetConsumerConnectionListRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetConsumerConnectionListRequestHeader"),
        Map.entry("GetConsumeStatsRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsRequestHeader"),
        Map.entry("GetConsumerRunningInfoRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader"),
        Map.entry("GetConsumerStatusRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetConsumerStatusRequestHeader"),
        Map.entry("GetEarliestMsgStoretimeRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeRequestHeader"),
        Map.entry("HeartbeatRequestHeader", "org.apache.rocketmq.remoting.protocol.header.HeartbeatRequestHeader"),
        Map.entry("LiteSubscriptionCtlRequestHeader", "org.apache.rocketmq.remoting.protocol.header.LiteSubscriptionCtlRequestHeader"),
        Map.entry("GetMaxOffsetRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader"),
        Map.entry("GetMinOffsetRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader"),
        Map.entry("GetTopicStatsInfoRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetTopicStatsInfoRequestHeader"),
        Map.entry("GetTopicConfigRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetTopicConfigRequestHeader"),
        Map.entry("GetRouteInfoRequestHeader", "org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader"),
        Map.entry("GetSubscriptionGroupConfigRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetSubscriptionGroupConfigRequestHeader"),
        Map.entry("LockBatchMqRequestHeader", "org.apache.rocketmq.remoting.protocol.header.LockBatchMqRequestHeader"),
        Map.entry("NotificationRequestHeader", "org.apache.rocketmq.remoting.protocol.header.NotificationRequestHeader"),
        Map.entry("NotifyUnsubscribeLiteRequestHeader", "org.apache.rocketmq.remoting.protocol.header.NotifyUnsubscribeLiteRequestHeader"),
        Map.entry("PollingInfoRequestHeader", "org.apache.rocketmq.remoting.protocol.header.PollingInfoRequestHeader"),
        Map.entry("PopMessageRequestHeader", "org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader"),
        Map.entry("PopLiteMessageRequestHeader", "org.apache.rocketmq.remoting.protocol.header.PopLiteMessageRequestHeader"),
        Map.entry("PullMessageRequestHeader", "org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader"),
        Map.entry("QueryConsumeQueueRequestHeader", "org.apache.rocketmq.remoting.protocol.header.QueryConsumeQueueRequestHeader"),
        Map.entry("QueryConsumeTimeSpanRequestHeader", "org.apache.rocketmq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader"),
        Map.entry("QueryConsumerOffsetRequestHeader", "org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader"),
        Map.entry("QueryCorrectionOffsetRequestHeader", "org.apache.rocketmq.remoting.protocol.header.QueryCorrectionOffsetHeader"),
        Map.entry("UpdateGroupForbiddenRequestHeader", "org.apache.rocketmq.remoting.protocol.header.UpdateGroupForbiddenRequestHeader"),
        Map.entry("QueryMessageRequestHeader", "org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader"),
        Map.entry("QuerySubscriptionByConsumerRequestHeader", "org.apache.rocketmq.remoting.protocol.header.QuerySubscriptionByConsumerRequestHeader"),
        Map.entry("QueryTopicConsumeByWhoRequestHeader", "org.apache.rocketmq.remoting.protocol.header.QueryTopicConsumeByWhoRequestHeader"),
        Map.entry("QueryTopicsByConsumerRequestHeader", "org.apache.rocketmq.remoting.protocol.header.QueryTopicsByConsumerRequestHeader"),
        Map.entry("RecallMessageRequestHeader", "org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader"),
        Map.entry("ReplyMessageRequestHeader", "org.apache.rocketmq.remoting.protocol.header.ReplyMessageRequestHeader"),
        Map.entry("ResetOffsetRequestHeader", "org.apache.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader"),
        Map.entry("SearchOffsetRequestHeader", "org.apache.rocketmq.remoting.protocol.header.SearchOffsetRequestHeader"),
        Map.entry("SendMessageRequestHeaderV1", "org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader"),
        Map.entry("SendMessageRequestHeaderV2", "org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeaderV2"),
        Map.entry("UnregisterClientRequestHeader", "org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader"),
        Map.entry("UnlockBatchMqRequestHeader", "org.apache.rocketmq.remoting.protocol.header.UnlockBatchMqRequestHeader"),
        Map.entry("UpdateConsumerOffsetRequestHeader", "org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader"));
    private static final String[] BASE_FIELDS = {"ns", "nsd", "bname", "oway"};

    private RequestHeaderIdentityFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(render()) + "\n";
        if (verify) {
            if (!Files.exists(path) || !json.equals(Files.readString(path))) {
                throw new IllegalStateException("Java request header identity fixture drift: " + path);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json, StandardCharsets.UTF_8);
        }
    }

    private static Map<String, Object> render() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        result.put("clientArtifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        result.put("sourceCommit", SOURCE_COMMIT);
        result.put("serverContacted", false);
        Map<String, Object> cases = new TreeMap<>();
        Map<String, Object> apiInventories = new TreeMap<>();
        for (Map.Entry<String, String> entry : RUST_TO_JAVA_CLASSES.entrySet()) {
            Class<?> type = Class.forName(entry.getValue());
            cases.put(entry.getKey(), identityCase(type));
            apiInventories.put(entry.getKey(), apiInventory(type));
        }
        result.put("identityCases", cases);
        result.put("apiInventories", apiInventories);
        return result;
    }

    private static Map<String, Object> apiInventory(Class<?> type) {
        Map<String, Object> result = new TreeMap<>();
        result.put("publicConstructors", Arrays.stream(type.getConstructors())
            .map(RequestHeaderIdentityFixtures::constructorSignature).sorted()
            .collect(Collectors.toList()));
        result.put("publicFields", Arrays.stream(type.getFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(Field::getName).sorted().collect(Collectors.toList()));
        result.put("declaredPublicMethods", Arrays.stream(type.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(RequestHeaderIdentityFixtures::methodSignature).sorted()
            .collect(Collectors.toList()));
        result.put("callablePublicMethods", Arrays.stream(type.getMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getDeclaringClass() != Object.class)
            .map(RequestHeaderIdentityFixtures::methodSignature).sorted()
            .collect(Collectors.toList()));
        return result;
    }

    private static String constructorSignature(Constructor<?> constructor) {
        return constructor.getDeclaringClass().getName() + "("
            + Arrays.stream(constructor.getParameterTypes()).map(Class::getTypeName)
                .collect(Collectors.joining(", "))
            + ")";
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getTypeName() + " " + method.getDeclaringClass().getName()
            + "." + method.getName() + "("
            + Arrays.stream(method.getParameterTypes()).map(Class::getTypeName)
                .collect(Collectors.joining(", "))
            + ")";
    }

    private static Map<String, Object> identityCase(Class<?> type) throws Exception {
        Object defaultsA = type.getDeclaredConstructor().newInstance();
        Object defaultsB = type.getDeclaredConstructor().newInstance();
        Object baseA = type.getDeclaredConstructor().newInstance();
        Object baseB = type.getDeclaredConstructor().newInstance();
        setBase(baseA, "namespace", Boolean.TRUE, "broker", Boolean.FALSE);
        setBase(baseB, "namespace", Boolean.TRUE, "broker", Boolean.FALSE);

        Map<String, Object> result = new TreeMap<>();
        result.put("javaClass", type.getName());
        result.put("equalsDeclaredBy", type.getMethod("equals", Object.class)
            .getDeclaringClass().getName());
        result.put("hashCodeDeclaredBy", type.getMethod("hashCode")
            .getDeclaringClass().getName());
        result.put("sameObjectEqualsItself", baseA.equals(baseA));
        result.put("distinctDefaultObjectsEquals", defaultsA.equals(defaultsB));
        result.put("equalBaseFieldsEquals", baseA.equals(baseB));
        result.put("equalBaseFieldsHashEqual", baseA.hashCode() == baseB.hashCode());
        if ("HeartbeatRequestHeader".equals(type.getSimpleName())
            || "LiteSubscriptionCtlRequestHeader".equals(type.getSimpleName())) {
            result.put("defaultHashCode", defaultsA.hashCode());
            result.put("baseHashCode", baseA.hashCode());
            result.put("defaultToString", defaultsA.toString());
            result.put("baseToString", baseA.toString());
        }
        result.putAll(payloadProbe(type, baseA));

        Map<String, Boolean> baseFieldDifferences = new TreeMap<>();
        for (String field : BASE_FIELDS) {
            Object changed = type.getDeclaredConstructor().newInstance();
            setBase(changed, "namespace", Boolean.TRUE, "broker", Boolean.FALSE);
            mutateBaseField(changed, field);
            baseFieldDifferences.put(field, baseA.equals(changed));
        }
        result.put("baseFieldDifferenceEquals", baseFieldDifferences);
        return result;
    }

    private static Map<String, Object> payloadProbe(Class<?> type, Object baseA) throws Exception {
        Object changed = type.getDeclaredConstructor().newInstance();
        setBase(changed, "namespace", Boolean.TRUE, "broker", Boolean.FALSE);
        for (Field field : type.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) || Modifier.isFinal(field.getModifiers())) continue;
            Object candidate = differentValue(field, changed);
            if (candidate == Unsupported.VALUE || Objects.equals(candidate, field.get(changed))) continue;
            field.setAccessible(true);
            field.set(changed, candidate);
            Map<String, Object> result = new TreeMap<>();
            result.put("payloadDifferenceTested", true);
            result.put("payloadField", field.getName());
            result.put("payloadDifferenceIgnored",
                baseA.equals(changed) && baseA.hashCode() == changed.hashCode());
            return result;
        }
        Map<String, Object> result = new TreeMap<>();
        result.put("payloadDifferenceTested", false);
        result.put("payloadField", "");
        result.put("payloadDifferenceIgnored", true);
        return result;
    }

    private static Object differentValue(Field field, Object target) throws Exception {
        Class<?> type = field.getType();
        field.setAccessible(true);
        Object current = field.get(target);
        if (type == String.class) return "payload".equals(current) ? "other-payload" : "payload";
        if (type == Boolean.class) return !Boolean.TRUE.equals(current);
        if (type == boolean.class) return !Boolean.TRUE.equals(current);
        if (type == Integer.class) return current instanceof Integer ? ((Integer) current) + 1 : 1;
        if (type == int.class) return current instanceof Integer ? ((Integer) current) + 1 : 1;
        if (type == Long.class) return current instanceof Long ? ((Long) current) + 1L : 1L;
        if (type == long.class) return current instanceof Long ? ((Long) current) + 1L : 1L;
        if (type == Short.class) return current instanceof Short ? (short) (((Short) current) + 1) : (short) 1;
        if (type == short.class) return current instanceof Short ? (short) (((Short) current) + 1) : (short) 1;
        if (type == Byte.class) return current instanceof Byte ? (byte) (((Byte) current) + 1) : (byte) 1;
        if (type == byte.class) return current instanceof Byte ? (byte) (((Byte) current) + 1) : (byte) 1;
        if (type == Double.class) return current instanceof Double ? ((Double) current) + 1d : 1d;
        if (type == double.class) return current instanceof Double ? ((Double) current) + 1d : 1d;
        if (type == Float.class) return current instanceof Float ? ((Float) current) + 1f : 1f;
        if (type == float.class) return current instanceof Float ? ((Float) current) + 1f : 1f;
        if (type.isEnum()) {
            for (Object constant : type.getEnumConstants()) {
                if (!Objects.equals(current, constant)) return constant;
            }
        }
        return Unsupported.VALUE;
    }

    private static void setBase(Object target, String namespace, Boolean namespaced,
                                String brokerName, Boolean oneway) throws Exception {
        setBaseField(target, "ns", namespace);
        setBaseField(target, "nsd", namespaced);
        setBaseField(target, "bname", brokerName);
        setBaseField(target, "oway", oneway);
    }

    private static void mutateBaseField(Object target, String field) throws Exception {
        Object value;
        switch (field) {
            case "ns": value = "other-namespace"; break;
            case "nsd": value = Boolean.FALSE; break;
            case "bname": value = "other-broker"; break;
            case "oway": value = Boolean.TRUE; break;
            default: throw new IllegalArgumentException(field);
        }
        setBaseField(target, field, value);
    }

    private static void setBaseField(Object target, String name, Object value) throws Exception {
        Field field = RpcRequestHeader.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private enum Unsupported { VALUE }
}
