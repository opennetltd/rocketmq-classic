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
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader;

/** Invokes Java 5.5.1 reflection decoding for offset-boundary response headers. */
public final class OffsetBoundaryHeaderDecodeModeFixtures {
    private OffsetBoundaryHeaderDecodeModeFixtures() { }

    public static void main(String[] args) throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");

        Map<String, Object> cases = new TreeMap<>();
        cases.put("search-missing", decodeModes("search", "offset", new TreeMap<>()));
        cases.put("search-invalid", decodeModes("search", "offset", invalid("offset")));
        cases.put("search-valid", decodeModes("search", "offset", valid("offset", "9")));
        cases.put("max-missing", decodeModes("max", "offset", new TreeMap<>()));
        cases.put("max-invalid", decodeModes("max", "offset", invalid("offset")));
        cases.put("max-valid", decodeModes("max", "offset", valid("offset", "10")));
        cases.put("min-missing", decodeModes("min", "offset", new TreeMap<>()));
        cases.put("min-invalid", decodeModes("min", "offset", invalid("offset")));
        cases.put("min-valid", decodeModes("min", "offset", valid("offset", "1")));
        cases.put("earliest-missing", decodeModes("earliest", "timestamp", new TreeMap<>()));
        cases.put("earliest-invalid", decodeModes("earliest", "timestamp", invalid("timestamp")));
        cases.put("earliest-valid", decodeModes("earliest", "timestamp", valid("timestamp", "-1")));
        cases.put("request-search-missing", decodeModes("request-search", "queueId", new TreeMap<>()));
        cases.put("request-search-valid", decodeModes("request-search", "timestamp", fields(
            "topic", "orders", "liteTopic", "lite-orders", "queueId", "7", "timestamp", "9", "boundaryType", "UPPER",
            "lo", "true", "ns", "tenant", "nsd", "true", "bname", "broker", "oway", "false")));
        cases.put("request-search-invalid", decodeModes("request-search", "queueId", fields(
            "topic", "orders", "queueId", "not-an-int", "timestamp", "not-a-long",
            "boundaryType", "not-a-boundary", "lo", "malformed", "nsd", "malformed", "oway", "malformed")));
        cases.put("request-max-missing", decodeModes("request-max", "queueId", new TreeMap<>()));
        cases.put("request-max-valid", decodeModes("request-max", "queueId", fields(
            "topic", "orders", "queueId", "7", "committed", "false", "lo", "true",
            "ns", "tenant", "nsd", "true", "bname", "broker", "oway", "false")));
        cases.put("request-max-invalid", decodeModes("request-max", "queueId", fields(
            "topic", "orders", "queueId", "not-an-int", "committed", "malformed",
            "lo", "malformed", "nsd", "malformed", "oway", "malformed")));
        cases.put("request-min-missing", decodeModes("request-min", "queueId", new TreeMap<>()));
        cases.put("request-min-valid", decodeModes("request-min", "queueId", fields(
            "topic", "orders", "queueId", "7", "lo", "true", "ns", "tenant",
            "nsd", "true", "bname", "broker", "oway", "false")));
        cases.put("request-min-invalid", decodeModes("request-min", "queueId", fields(
            "topic", "orders", "queueId", "not-an-int", "lo", "malformed",
            "nsd", "malformed", "oway", "malformed")));
        cases.put("request-earliest-missing", decodeModes("request-earliest", "queueId", new TreeMap<>()));
        cases.put("request-earliest-valid", decodeModes("request-earliest", "queueId", fields(
            "topic", "orders", "queueId", "7", "lo", "true", "ns", "tenant",
            "nsd", "true", "bname", "broker", "oway", "false")));
        cases.put("request-earliest-invalid", decodeModes("request-earliest", "queueId", fields(
            "topic", "orders", "queueId", "not-an-int", "lo", "malformed",
            "nsd", "malformed", "oway", "malformed")));
        fixture.put("cases", cases);

        Map<String, Object> apiInventories = new TreeMap<>();
        apiInventories.put("SearchOffsetRequestHeader", apiInventory(SearchOffsetRequestHeader.class));
        apiInventories.put("GetMaxOffsetRequestHeader", apiInventory(GetMaxOffsetRequestHeader.class));
        apiInventories.put("GetMinOffsetRequestHeader", apiInventory(GetMinOffsetRequestHeader.class));
        apiInventories.put("GetEarliestMsgStoretimeRequestHeader",
            apiInventory(GetEarliestMsgStoretimeRequestHeader.class));
        apiInventories.put("SearchOffsetResponseHeader", apiInventory(SearchOffsetResponseHeader.class));
        apiInventories.put("GetMaxOffsetResponseHeader", apiInventory(GetMaxOffsetResponseHeader.class));
        apiInventories.put("GetMinOffsetResponseHeader", apiInventory(GetMinOffsetResponseHeader.class));
        apiInventories.put("GetEarliestMsgStoretimeResponseHeader",
            apiInventory(GetEarliestMsgStoretimeResponseHeader.class));
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
        System.out.println("Offset-boundary request/response-header decode fixtures verified");
    }

    private static Map<String, String> invalid(String fieldName) {
        return valid(fieldName, "not-a-long");
    }

    private static Map<String, String> valid(String fieldName, String value) {
        Map<String, String> fields = new TreeMap<>();
        fields.put(fieldName, value);
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
            .map(OffsetBoundaryHeaderDecodeModeFixtures::constructorSignature).sorted()
            .collect(Collectors.toList()));
        result.put("publicFields", Arrays.stream(type.getFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(Field::getName).sorted().collect(Collectors.toList()));
        result.put("declaredPublicMethods", Arrays.stream(type.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(OffsetBoundaryHeaderDecodeModeFixtures::methodSignature).sorted()
            .collect(Collectors.toList()));
        result.put("callablePublicMethods", Arrays.stream(type.getMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getDeclaringClass() != Object.class)
            .map(OffsetBoundaryHeaderDecodeModeFixtures::methodSignature).sorted()
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

    private static Map<String, Object> decodeModes(String headerType, String fieldName,
        Map<String, String> fields) {
        Map<String, Object> result = new TreeMap<>();
        result.put("fields", new TreeMap<>(fields));
        result.put("headerType", headerType);
        result.put("fieldName", fieldName);
        result.put("fast", decode(headerType, fieldName, fields, true));
        result.put("reflection", decode(headerType, fieldName, fields, false));
        return result;
    }

    private static Map<String, Object> decode(String headerType, String fieldName,
        Map<String, String> fields, boolean useFastEncode) {
        Map<String, Object> result = new TreeMap<>();
        RemotingCommand command = RemotingCommand.createRequestCommand(29, null);
        command.setExtFields(new HashMap<>(fields));
        try {
            Class<? extends CommandCustomHeader> headerClass;
            switch (headerType) {
                case "search":
                    headerClass = SearchOffsetResponseHeader.class;
                    break;
                case "max":
                    headerClass = GetMaxOffsetResponseHeader.class;
                    break;
                case "min":
                    headerClass = GetMinOffsetResponseHeader.class;
                    break;
                case "earliest":
                    headerClass = GetEarliestMsgStoretimeResponseHeader.class;
                    break;
                case "request-search":
                    headerClass = SearchOffsetRequestHeader.class;
                    break;
                case "request-max":
                    headerClass = GetMaxOffsetRequestHeader.class;
                    break;
                case "request-min":
                    headerClass = GetMinOffsetRequestHeader.class;
                    break;
                case "request-earliest":
                    headerClass = GetEarliestMsgStoretimeRequestHeader.class;
                    break;
                default:
                    throw new IllegalArgumentException("unknown header type: " + headerType);
            }
            CommandCustomHeader header = command.decodeCommandCustomHeaderDirectly(
                headerClass, useFastEncode);
            result.put("outcome", "OK");
            Object value = readField(header, fieldName);
            result.put("value", value instanceof BoundaryType ? ((BoundaryType) value).name() : value);
            if (headerType.startsWith("request-")) {
                result.put("decodedFields", requestFields(headerType, header));
                if (header instanceof SearchOffsetRequestHeader) {
                    result.put("getterBoundaryType", ((SearchOffsetRequestHeader) header)
                        .getBoundaryType().name());
                }
            } else {
                recordOwnerUnboxResult(result, headerType, header);
            }

            RemotingCommand encoded = RemotingCommand.createRequestCommand(29, null);
            encoded.setExtFields(new HashMap<>());
            encoded.writeCustomHeader(header);
            encoded.makeCustomHeaderToNet();
            result.put("encodedFields", new TreeMap<>(encoded.getExtFields()));
        } catch (Throwable error) {
            result.put("outcome", error.getClass().getSimpleName());
        }
        return result;
    }

    private static void recordOwnerUnboxResult(Map<String, Object> result, String headerType,
        CommandCustomHeader header) {
        try {
            long value;
            switch (headerType) {
                case "search":
                    value = ((SearchOffsetResponseHeader) header).getOffset();
                    break;
                case "max":
                    value = ((GetMaxOffsetResponseHeader) header).getOffset();
                    break;
                case "min":
                    value = ((GetMinOffsetResponseHeader) header).getOffset();
                    break;
                case "earliest":
                    value = ((GetEarliestMsgStoretimeResponseHeader) header).getTimestamp();
                    break;
                default:
                    throw new IllegalArgumentException("unknown header type: " + headerType);
            }
            result.put("ownerOutcome", "OK");
            result.put("ownerValue", value);
        } catch (Throwable error) {
            result.put("ownerOutcome", error.getClass().getSimpleName());
        }
    }

    private static Object readField(Object target, String name) throws Exception {
        for (Class<?> type = target.getClass(); type != Object.class; type = type.getSuperclass()) {
            try {
                Field field = type.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(target);
            } catch (NoSuchFieldException ignored) {
                // Continue through inherited request-header fields.
            }
        }
        throw new NoSuchFieldException(name);
    }

    private static Map<String, Object> requestFields(String headerType,
        CommandCustomHeader header) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        String[] names = headerType.equals("request-search")
            ? new String[] {"topic", "liteTopic", "queueId", "timestamp", "boundaryType", "lo", "ns", "nsd", "bname", "oway"}
            : headerType.equals("request-max")
                ? new String[] {"topic", "queueId", "committed", "lo", "ns", "nsd", "bname", "oway"}
                : new String[] {"topic", "queueId", "lo", "ns", "nsd", "bname", "oway"};
        for (String name : names) {
            Object value = readField(header, name);
            result.put(name, value instanceof BoundaryType ? ((BoundaryType) value).name() : value);
        }
        return result;
    }
}
