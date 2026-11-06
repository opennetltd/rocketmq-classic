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
import com.alibaba.fastjson2.JSONWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.CheckRocksdbCqWriteProgressRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicStatsInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetSubscriptionGroupConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetAllSubscriptionGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetAllSubscriptionGroupResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetMasterFlushOffsetHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeQueueRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.QuerySubscriptionByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicsByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader;

/** Invokes Java 5.5.1 admin-query header reflection decoding through both mode hints. */
public final class QueryAdminHeaderDecodeModeFixtures {
    private QueryAdminHeaderDecodeModeFixtures() { }

    public static void main(String[] args) throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");

        Map<String, Object> cases = new TreeMap<>();
        cases.put("query-request-valid", decodeModes("query-request", fields(
            "topic", "orders", "key", "order-42", "maxNum", "32",
            "beginTimestamp", "1712345678000", "endTimestamp", "1712345679000",
            "indexType", "UNIQUE_KEY", "lastKey", "last-index-key", "ns", "tenant",
            "nsd", "true", "bname", "broker-a", "oway", "false", "lo", "true")));
        cases.put("query-request-missing", decodeModes("query-request", fields()));
        cases.put("query-request-invalid-numbers", decodeModes("query-request", fields(
            "topic", "orders", "key", "order-42", "maxNum", "not-an-int",
            "beginTimestamp", "not-a-long", "endTimestamp", "9223372036854775808")));
        cases.put("query-request-malformed-booleans", decodeModes("query-request", fields(
            "nsd", "malformed", "oway", "malformed", "lo", "malformed")));
        cases.put("query-consume-time-span-valid", decodeModes("query-consume-time-span", fields(
            "topic", "orders", "group", "workers", "ns", "tenant", "nsd", "true",
            "bname", "broker-a", "oway", "false", "lo", "true")));
        cases.put("delete-kv-config-valid", decodeModes("delete-kv-config", fields(
            "namespace", "config-namespace", "key", "config-key")));

        cases.put("query-response-valid", decodeModes("query-response", fields(
            "indexLastUpdateTimestamp", "1712345679000", "indexLastUpdatePhyoffset", "9876")));
        cases.put("query-response-missing", decodeModes("query-response", fields()));
        cases.put("query-response-invalid-numbers", decodeModes("query-response", fields(
            "indexLastUpdateTimestamp", "not-a-long", "indexLastUpdatePhyoffset", "9223372036854775808")));

        cases.put("reset-master-flush-offset-valid", decodeModes("reset-master-flush-offset",
            fields("masterFlushOffset", "9876543210")));
        cases.put("reset-master-flush-offset-missing", decodeModes("reset-master-flush-offset", fields()));
        cases.put("reset-master-flush-offset-invalid", decodeModes("reset-master-flush-offset",
            fields("masterFlushOffset", "not-a-long")));
        cases.put("export-rocksdb-config-valid", decodeModes("export-rocksdb-config",
            fields("configType", "topics;subscriptionGroups;consumerOffsets;")));
        cases.put("export-rocksdb-config-empty", decodeModes("export-rocksdb-config",
            fields("configType", "")));
        cases.put("export-rocksdb-config-missing", decodeModes("export-rocksdb-config", fields()));
        cases.put("check-rocksdb-cq-write-progress-valid", decodeModes("check-rocksdb-cq-write-progress",
            fields("topic", "orders", "checkStoreTime", "1712345678901")));
        cases.put("check-rocksdb-cq-write-progress-missing", decodeModes("check-rocksdb-cq-write-progress", fields()));
        cases.put("check-rocksdb-cq-write-progress-invalid-time", decodeModes("check-rocksdb-cq-write-progress",
            fields("topic", "orders", "checkStoreTime", "not-a-long")));
        cases.put("wipe-write-perm-request-valid", decodeModes("wipe-write-perm-request",
            fields("brokerName", "broker-a")));
        cases.put("wipe-write-perm-request-missing", decodeModes("wipe-write-perm-request", fields()));
        cases.put("add-write-perm-request-valid", decodeModes("add-write-perm-request",
            fields("brokerName", "broker-a")));
        cases.put("add-write-perm-request-missing", decodeModes("add-write-perm-request", fields()));
        cases.put("wipe-write-perm-response-valid", decodeModes("wipe-write-perm-response",
            fields("wipeTopicCount", "17")));
        cases.put("wipe-write-perm-response-missing", decodeModes("wipe-write-perm-response", fields()));
        cases.put("wipe-write-perm-response-invalid", decodeModes("wipe-write-perm-response",
            fields("wipeTopicCount", "not-an-int")));
        cases.put("add-write-perm-response-valid", decodeModes("add-write-perm-response",
            fields("addTopicCount", "-3")));
        cases.put("add-write-perm-response-missing", decodeModes("add-write-perm-response", fields()));
        cases.put("add-write-perm-response-invalid", decodeModes("add-write-perm-response",
            fields("addTopicCount", "not-an-int")));

        cases.put("view-request-valid", decodeModes("view-request", fields(
            "topic", "orders", "offset", "9876")));
        cases.put("view-request-missing", decodeModes("view-request", fields()));
        cases.put("view-request-invalid-offset", decodeModes("view-request", fields(
            "topic", "orders", "offset", "not-a-long")));

        cases.put("query-topics-by-consumer-valid", decodeModes("query-topics", fields(
            "group", "workers", "ns", "tenant", "nsd", "true", "bname", "broker-a",
            "oway", "false")));
        cases.put("query-topics-by-consumer-missing", decodeModes("query-topics", fields()));
        cases.put("query-topics-by-consumer-malformed-booleans", decodeModes("query-topics", fields(
            "group", "workers", "nsd", "malformed", "oway", "malformed")));
        cases.put("query-subscription-by-consumer-valid", decodeModes("query-subscription", fields(
            "group", "workers", "topic", "orders", "ns", "tenant", "nsd", "true",
            "bname", "broker-a", "oway", "false", "lo", "true")));
        cases.put("query-subscription-by-consumer-missing", decodeModes("query-subscription", fields()));
        cases.put("query-subscription-by-consumer-malformed-booleans", decodeModes("query-subscription", fields(
            "group", "workers", "topic", "orders", "nsd", "malformed", "oway", "malformed",
            "lo", "malformed")));
        cases.put("query-topic-consume-by-who-valid", decodeModes("query-topic-consumers", fields(
            "topic", "orders", "ns", "tenant", "nsd", "true", "bname", "broker-a",
            "oway", "false", "lo", "true")));
        cases.put("query-topic-consume-by-who-missing", decodeModes("query-topic-consumers", fields()));
        cases.put("query-topic-consume-by-who-malformed-booleans", decodeModes("query-topic-consumers", fields(
            "topic", "orders", "nsd", "malformed", "oway", "malformed", "lo", "malformed")));
        cases.put("topic-stats-info-valid", decodeModes("topic-stats-info", fields(
            "topic", "orders", "ns", "tenant", "nsd", "true", "bname", "broker-a",
            "oway", "false", "lo", "true")));
        cases.put("topic-stats-info-missing", decodeModes("topic-stats-info", fields()));
        cases.put("topic-stats-info-malformed-booleans", decodeModes("topic-stats-info", fields(
            "topic", "orders", "nsd", "malformed", "oway", "malformed", "lo", "malformed")));
        cases.put("query-consume-queue-valid", decodeModes("query-consume-queue", fields(
            "topic", "orders", "queueId", "2", "index", "9876543210", "count", "16",
            "consumerGroup", "workers", "ns", "tenant", "nsd", "true",
            "bname", "broker-a", "oway", "false", "lo", "true")));
        cases.put("query-consume-queue-missing", decodeModes("query-consume-queue", fields()));
        cases.put("query-consume-queue-invalid-numbers", decodeModes("query-consume-queue", fields(
            "queueId", "invalid", "index", "9223372036854775808", "count", "overflow")));
        cases.put("get-all-subscription-group-request-valid", decodeModes(
            "get-all-subscription-group-request", fields(
                "groupSeq", "2", "dataVersion", "{\"counter\":4,\"stateVersion\":7,\"timestamp\":1712345678901}",
                "maxGroupNum", "2000")));
        cases.put("get-all-subscription-group-request-missing", decodeModes(
            "get-all-subscription-group-request", fields()));
        cases.put("get-all-subscription-group-request-invalid-numbers", decodeModes(
            "get-all-subscription-group-request", fields("groupSeq", "invalid", "maxGroupNum", "overflow")));
        cases.put("get-all-subscription-group-response-valid", decodeModes(
            "get-all-subscription-group-response", fields("totalGroupNum", "5")));
        cases.put("get-all-subscription-group-response-missing", decodeModes(
            "get-all-subscription-group-response", fields()));
        cases.put("get-all-subscription-group-response-invalid", decodeModes(
            "get-all-subscription-group-response", fields("totalGroupNum", "not-an-int")));
        fixture.put("cases", cases);

        Map<String, Object> apiInventories = new TreeMap<>();
        apiInventories.put("QueryTopicsByConsumerRequestHeader",
            apiInventory(QueryTopicsByConsumerRequestHeader.class));
        apiInventories.put("QuerySubscriptionByConsumerRequestHeader",
            apiInventory(QuerySubscriptionByConsumerRequestHeader.class));
        apiInventories.put("QueryTopicConsumeByWhoRequestHeader",
            apiInventory(QueryTopicConsumeByWhoRequestHeader.class));
        apiInventories.put("QueryConsumeTimeSpanRequestHeader",
            apiInventory(QueryConsumeTimeSpanRequestHeader.class));
        apiInventories.put("GetTopicStatsInfoRequestHeader",
            apiInventory(GetTopicStatsInfoRequestHeader.class));
        apiInventories.put("QueryConsumeQueueRequestHeader",
            apiInventory(QueryConsumeQueueRequestHeader.class));
        apiInventories.put("GetTopicsByClusterRequestHeader",
            apiInventory(GetTopicsByClusterRequestHeader.class));
        apiInventories.put("GetSubscriptionGroupConfigRequestHeader",
            apiInventory(GetSubscriptionGroupConfigRequestHeader.class));
        apiInventories.put("GetAllSubscriptionGroupRequestHeader",
            apiInventory(GetAllSubscriptionGroupRequestHeader.class));
        apiInventories.put("GetAllSubscriptionGroupResponseHeader",
            apiInventory(GetAllSubscriptionGroupResponseHeader.class));
        apiInventories.put("ResetMasterFlushOffsetHeader",
            apiInventory(ResetMasterFlushOffsetHeader.class));
        apiInventories.put("ExportRocksDBConfigToJsonRequestHeader",
            apiInventory(ExportRocksDBConfigToJsonRequestHeader.class));
        apiInventories.put("CheckRocksdbCqWriteProgressRequestHeader",
            apiInventory(CheckRocksdbCqWriteProgressRequestHeader.class));
        apiInventories.put("ExportRocksDBConfigType",
            apiInventory(ExportRocksDBConfigToJsonRequestHeader.ConfigType.class));
        apiInventories.put("WipeWritePermOfBrokerRequestHeader",
            apiInventory(WipeWritePermOfBrokerRequestHeader.class));
        apiInventories.put("AddWritePermOfBrokerRequestHeader",
            apiInventory(AddWritePermOfBrokerRequestHeader.class));
        apiInventories.put("WipeWritePermOfBrokerResponseHeader",
            apiInventory(WipeWritePermOfBrokerResponseHeader.class));
        apiInventories.put("AddWritePermOfBrokerResponseHeader",
            apiInventory(AddWritePermOfBrokerResponseHeader.class));
        apiInventories.put("DeleteTopicRequestHeader",
            apiInventory(DeleteTopicRequestHeader.class));
        apiInventories.put("DeleteTopicFromNamesrvRequestHeader",
            apiInventory(DeleteTopicFromNamesrvRequestHeader.class));
        apiInventories.put("DeleteSubscriptionGroupRequestHeader",
            apiInventory(DeleteSubscriptionGroupRequestHeader.class));
        apiInventories.put("DeleteKVConfigRequestHeader",
            apiInventory(DeleteKVConfigRequestHeader.class));
        apiInventories.put("QueryMessageResponseHeader",
            apiInventory(QueryMessageResponseHeader.class));
        fixture.put("apiInventories", apiInventories);

        String json = JSON.toJSONString(fixture, JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) {
                throw new IllegalStateException("fixture drift: " + file);
            }
        } else {
            Files.createDirectories(file.getParent());
            Files.writeString(file, json);
        }
        System.out.println("Query admin header decode fixtures verified");
    }

    private static Map<String, Object> decodeModes(String type, Map<String, String> fields) {
        Map<String, Object> result = new TreeMap<>();
        result.put("fields", new TreeMap<>(fields));
        result.put("headerType", type);
        result.put("fast", decode(type, fields, true));
        result.put("reflection", decode(type, fields, false));
        return result;
    }

    private static Map<String, Object> decode(String type, Map<String, String> fields,
        boolean useFastEncode) {
        Map<String, Object> result = new TreeMap<>();
        int code = 12;
        if ("query-topics".equals(type)) {
            code = 343;
        } else if ("query-consume-time-span".equals(type)) {
            code = 303;
        } else if ("query-subscription".equals(type)) {
            code = 345;
        } else if ("query-topic-consumers".equals(type)) {
            code = 300;
        } else if ("topic-stats-info".equals(type)) {
            code = 202;
        } else if ("query-consume-queue".equals(type)) {
            code = 321;
        } else if ("delete-kv-config".equals(type)) {
            code = 102;
        } else if ("reset-master-flush-offset".equals(type)) {
            code = 908;
        } else if ("export-rocksdb-config".equals(type)) {
            code = 355;
        } else if ("check-rocksdb-cq-write-progress".equals(type)) {
            code = 354;
        } else if ("wipe-write-perm-request".equals(type)
            || "wipe-write-perm-response".equals(type)) {
            code = 205;
        } else if ("add-write-perm-request".equals(type)
            || "add-write-perm-response".equals(type)) {
            code = 327;
        } else if ("get-all-subscription-group-request".equals(type)
            || "get-all-subscription-group-response".equals(type)) {
            code = 201;
        }
        RemotingCommand command = RemotingCommand.createRequestCommand(code, null);
        command.setExtFields(new HashMap<>(fields));
        try {
            Class<? extends CommandCustomHeader> headerClass;
            switch (type) {
                case "query-request":
                    headerClass = QueryMessageRequestHeader.class;
                    break;
                case "query-consume-time-span":
                    headerClass = QueryConsumeTimeSpanRequestHeader.class;
                    break;
                case "query-response":
                    headerClass = QueryMessageResponseHeader.class;
                    break;
                case "view-request":
                    headerClass = ViewMessageRequestHeader.class;
                    break;
                case "query-topics":
                    headerClass = QueryTopicsByConsumerRequestHeader.class;
                    break;
                case "query-subscription":
                    headerClass = QuerySubscriptionByConsumerRequestHeader.class;
                    break;
                case "query-topic-consumers":
                    headerClass = QueryTopicConsumeByWhoRequestHeader.class;
                    break;
                case "topic-stats-info":
                    headerClass = GetTopicStatsInfoRequestHeader.class;
                    break;
                case "query-consume-queue":
                    headerClass = QueryConsumeQueueRequestHeader.class;
                    break;
                case "get-all-subscription-group-request":
                    headerClass = GetAllSubscriptionGroupRequestHeader.class;
                    break;
                case "get-all-subscription-group-response":
                    headerClass = GetAllSubscriptionGroupResponseHeader.class;
                    break;
                case "delete-kv-config":
                    headerClass = DeleteKVConfigRequestHeader.class;
                    break;
                case "reset-master-flush-offset":
                    headerClass = ResetMasterFlushOffsetHeader.class;
                    break;
                case "export-rocksdb-config":
                    headerClass = ExportRocksDBConfigToJsonRequestHeader.class;
                    break;
                case "check-rocksdb-cq-write-progress":
                    headerClass = CheckRocksdbCqWriteProgressRequestHeader.class;
                    break;
                case "wipe-write-perm-request":
                    headerClass = WipeWritePermOfBrokerRequestHeader.class;
                    break;
                case "add-write-perm-request":
                    headerClass = AddWritePermOfBrokerRequestHeader.class;
                    break;
                case "wipe-write-perm-response":
                    headerClass = WipeWritePermOfBrokerResponseHeader.class;
                    break;
                case "add-write-perm-response":
                    headerClass = AddWritePermOfBrokerResponseHeader.class;
                    break;
                default:
                    throw new IllegalArgumentException("unknown header type: " + type);
            }
            CommandCustomHeader header = command.decodeCommandCustomHeaderDirectly(
                headerClass, useFastEncode);
            result.put("outcome", "OK");
            result.put("decodedFields", readFields(header));

            RemotingCommand encoded = RemotingCommand.createRequestCommand(code, null);
            encoded.setExtFields(new HashMap<>());
            encoded.writeCustomHeader(header);
            encoded.makeCustomHeaderToNet();
            result.put("encodedFields", new TreeMap<>(encoded.getExtFields()));

            if (header instanceof ViewMessageRequestHeader) {
                result.put("equalsDeclaredBy", header.getClass()
                    .getMethod("equals", Object.class).getDeclaringClass().getName());
                result.put("hashCodeDeclaredBy", header.getClass()
                    .getMethod("hashCode").getDeclaringClass().getName());
                result.put("distinctDefaultObjectsEquals",
                    new ViewMessageRequestHeader().equals(new ViewMessageRequestHeader()));
            }
        } catch (Throwable error) {
            result.put("outcome", error.getClass().getSimpleName());
        }
        return result;
    }

    private static Map<String, Object> readFields(CommandCustomHeader header) throws Exception {
        Map<String, Object> fields = new TreeMap<>();
        Class<?> type = header.getClass();
        while (type != Object.class) {
            for (Field field : type.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                field.setAccessible(true);
                fields.put(field.getName(), field.get(header));
            }
            type = type.getSuperclass();
        }
        return fields;
    }

    private static Map<String, String> fields(String... pairs) {
        Map<String, String> fields = new TreeMap<>();
        for (int index = 0; index < pairs.length; index += 2) {
            fields.put(pairs[index], pairs[index + 1]);
        }
        return fields;
    }

    private static Map<String, Object> apiInventory(Class<?> type) {
        Map<String, Object> result = new TreeMap<>();
        result.put("publicConstructors", Arrays.stream(type.getConstructors())
            .map(QueryAdminHeaderDecodeModeFixtures::constructorSignature).sorted()
            .collect(Collectors.toList()));
        result.put("publicFields", Arrays.stream(type.getFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(Field::getName).sorted().collect(Collectors.toList()));
        result.put("declaredPublicMethods", Arrays.stream(type.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(QueryAdminHeaderDecodeModeFixtures::methodSignature).sorted()
            .collect(Collectors.toList()));
        result.put("callablePublicMethods", Arrays.stream(type.getMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getDeclaringClass() != Object.class)
            .map(QueryAdminHeaderDecodeModeFixtures::methodSignature).sorted()
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
}
