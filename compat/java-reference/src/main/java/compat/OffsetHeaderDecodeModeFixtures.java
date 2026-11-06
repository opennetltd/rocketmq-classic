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
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;

/** Invokes Java 5.5.1 reflective decoding for nullable consumer offset headers. */
public final class OffsetHeaderDecodeModeFixtures {
    private OffsetHeaderDecodeModeFixtures() { }

    public static void main(String[] args) throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");

        Map<String, Object> cases = new TreeMap<>();
        cases.put("query-missing-all", decodeModes("query", new TreeMap<>()));

        Map<String, String> queryInvalidQueueId = queryFields();
        queryInvalidQueueId.put("queueId", "not-an-integer");
        cases.put("query-invalid-queue-id", decodeModes("query", queryInvalidQueueId));

        Map<String, String> queryInvalidBoolean = queryFields();
        queryInvalidBoolean.put("setZeroIfNotFound", "not-a-boolean");
        cases.put("query-invalid-boolean", decodeModes("query", queryInvalidBoolean));
        cases.put("query-valid", decodeModes("query", queryFields()));

        cases.put("update-missing-all", decodeModes("update", new TreeMap<>()));

        Map<String, String> updateInvalidQueueId = updateFields();
        updateInvalidQueueId.put("queueId", "not-an-integer");
        cases.put("update-invalid-queue-id", decodeModes("update", updateInvalidQueueId));

        Map<String, String> updateInvalidCommitOffset = updateFields();
        updateInvalidCommitOffset.put("commitOffset", "not-a-long");
        cases.put("update-invalid-commit-offset", decodeModes("update", updateInvalidCommitOffset));
        cases.put("update-valid", decodeModes("update", updateFields()));

        cases.put("response-missing-all", decodeModes("response", new TreeMap<>()));

        Map<String, String> responseInvalidOffset = new TreeMap<>();
        responseInvalidOffset.put("offset", "not-a-long");
        cases.put("response-invalid-offset", decodeModes("response", responseInvalidOffset));

        Map<String, String> responseValid = new TreeMap<>();
        responseValid.put("offset", "9");
        cases.put("response-valid", decodeModes("response", responseValid));

        fixture.put("cases", cases);
        fixture.put("queryRequestApi", apiInventory(QueryConsumerOffsetRequestHeader.class));
        fixture.put("updateRequestApi", apiInventory(UpdateConsumerOffsetRequestHeader.class));
        fixture.put("queryResponseApi", apiInventory(QueryConsumerOffsetResponseHeader.class));
        QueryConsumerOffsetRequestHeader queryRequest = new QueryConsumerOffsetRequestHeader();
        queryRequest.setConsumerGroup("workers");
        queryRequest.setTopic("orders");
        queryRequest.setQueueId(2);
        queryRequest.setSetZeroIfNotFound(true);
        fixture.put("queryRequestToString", queryRequest.toString());
        UpdateConsumerOffsetRequestHeader updateRequest = new UpdateConsumerOffsetRequestHeader();
        updateRequest.setConsumerGroup("workers");
        updateRequest.setTopic("orders");
        updateRequest.setQueueId(2);
        updateRequest.setCommitOffset(9L);
        fixture.put("updateRequestToString", updateRequest.toString());
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
        System.out.println("Consumer offset request/response decode fixtures verified");
    }

    private static Map<String, Object> apiInventory(Class<?> type) {
        Map<String, Object> result = new TreeMap<>();
        result.put("publicConstructors", Arrays.stream(type.getConstructors())
            .map(OffsetHeaderDecodeModeFixtures::constructorSignature).sorted()
            .collect(Collectors.toList()));
        result.put("publicFields", Arrays.stream(type.getFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(Field::getName).sorted().collect(Collectors.toList()));
        result.put("declaredPublicMethods", Arrays.stream(type.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(OffsetHeaderDecodeModeFixtures::methodSignature).sorted()
            .collect(Collectors.toList()));
        result.put("callablePublicMethods", Arrays.stream(type.getMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getDeclaringClass() != Object.class)
            .map(OffsetHeaderDecodeModeFixtures::methodSignature).sorted()
            .collect(Collectors.toList()));
        return result;
    }

    private static String constructorSignature(Constructor<?> constructor) {
        return constructor.getDeclaringClass().getName() + "("
            + String.join(",", Arrays.stream(constructor.getParameterTypes())
                .map(Class::getTypeName).collect(Collectors.toList())) + ")";
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getTypeName() + " "
            + method.getDeclaringClass().getName() + "." + method.getName() + "("
            + String.join(",", Arrays.stream(method.getParameterTypes())
                .map(Class::getTypeName).collect(Collectors.toList())) + ")";
    }

    private static Map<String, Object> decodeModes(String headerType, Map<String, String> fields) {
        Map<String, Object> result = new TreeMap<>();
        result.put("fields", new TreeMap<>(fields));
        result.put("headerType", headerType);
        result.put("fast", decode(headerType, fields, true));
        result.put("reflection", decode(headerType, fields, false));
        return result;
    }

    private static Map<String, Object> decode(String headerType, Map<String, String> fields,
        boolean useFastEncode) {
        Map<String, Object> result = new TreeMap<>();
        RemotingCommand command = RemotingCommand.createRequestCommand(14, null);
        command.setExtFields(new HashMap<>(fields));
        try {
            CommandCustomHeader header;
            switch (headerType) {
                case "query":
                    header = command.decodeCommandCustomHeaderDirectly(
                        QueryConsumerOffsetRequestHeader.class, useFastEncode);
                    break;
                case "update":
                    header = command.decodeCommandCustomHeaderDirectly(
                        UpdateConsumerOffsetRequestHeader.class, useFastEncode);
                    break;
                case "response":
                    header = command.decodeCommandCustomHeaderDirectly(
                        QueryConsumerOffsetResponseHeader.class, useFastEncode);
                    break;
                default:
                    throw new IllegalArgumentException("unknown header type: " + headerType);
            }
            result.put("outcome", "OK");
            RemotingCommand encoded = RemotingCommand.createRequestCommand(14, null);
            encoded.setExtFields(new HashMap<>());
            encoded.writeCustomHeader(header);
            encoded.makeCustomHeaderToNet();
            result.put("encodedFields", new TreeMap<>(encoded.getExtFields()));
        } catch (Throwable error) {
            result.put("outcome", error.getClass().getSimpleName());
        }
        return result;
    }

    private static Map<String, String> queryFields() {
        Map<String, String> fields = new TreeMap<>();
        fields.put("consumerGroup", "workers");
        fields.put("topic", "orders");
        fields.put("queueId", "2");
        fields.put("setZeroIfNotFound", "true");
        fields.put("bname", "broker-a");
        fields.put("ns", "tenant");
        fields.put("nsd", "true");
        fields.put("lo", "false");
        fields.put("oway", "false");
        return fields;
    }

    private static Map<String, String> updateFields() {
        Map<String, String> fields = new TreeMap<>();
        fields.put("consumerGroup", "workers");
        fields.put("topic", "orders");
        fields.put("queueId", "2");
        fields.put("commitOffset", "9");
        fields.put("bname", "broker-a");
        fields.put("ns", "tenant");
        fields.put("nsd", "true");
        fields.put("lo", "true");
        fields.put("oway", "false");
        return fields;
    }
}
