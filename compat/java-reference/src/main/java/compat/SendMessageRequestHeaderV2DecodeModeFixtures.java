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
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeaderV2;

/** Invokes Java 5.5.1 fast and reflective compact send-request header decoders. */
public final class SendMessageRequestHeaderV2DecodeModeFixtures {
    private static final String[] FIELDS = {
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n",
        "ns", "nsd", "bname", "oway", "lo"
    };

    private SendMessageRequestHeaderV2DecodeModeFixtures() { }

    public static void main(String[] args) throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("conversion", convertFromV1());

        Map<String, Object> cases = new TreeMap<>();
        cases.put("missing-all-fields", decodeModes(new TreeMap<>()));

        Map<String, String> invalidDefaultQueueCount = validFields();
        invalidDefaultQueueCount.put("d", "not-an-integer");
        cases.put("invalid-default-topic-queue-count", decodeModes(invalidDefaultQueueCount));

        Map<String, String> invalidBornTimestamp = validFields();
        invalidBornTimestamp.put("g", "not-a-long");
        cases.put("invalid-born-timestamp", decodeModes(invalidBornTimestamp));

        Map<String, String> invalidOptionalRetryCount = validFields();
        invalidOptionalRetryCount.put("l", "not-an-integer");
        cases.put("invalid-optional-retry-count", decodeModes(invalidOptionalRetryCount));

        cases.put("valid-fields", decodeModes(validFields()));
        fixture.put("cases", cases);

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
        System.out.println("SendMessageRequestHeaderV2 fast/reflection decoder fixtures verified");
    }

    private static Map<String, Object> decodeModes(Map<String, String> fields) {
        Map<String, Object> result = new TreeMap<>();
        result.put("fields", new TreeMap<>(fields));
        result.put("fast", decode(fields, true));
        result.put("reflection", decode(fields, false));
        return result;
    }

    private static Map<String, Object> decode(Map<String, String> fields, boolean useFastEncode) {
        Map<String, Object> result = new TreeMap<>();
        RemotingCommand command = RemotingCommand.createRequestCommand(310, null);
        command.setExtFields(new HashMap<>(fields));
        try {
            SendMessageRequestHeaderV2 header = command.decodeCommandCustomHeaderDirectly(
                SendMessageRequestHeaderV2.class, useFastEncode);
            result.put("outcome", "OK");
            Map<String, Object> decoded = new TreeMap<>();
            for (String fieldName : FIELDS) {
                decoded.put(fieldName, readField(header, fieldName));
            }
            result.put("header", decoded);
        } catch (Throwable error) {
            result.put("outcome", error.getClass().getSimpleName());
        }
        return result;
    }

    private static Object readField(Object target, String name) throws Exception {
        for (Class<?> type = target.getClass(); type != Object.class; type = type.getSuperclass()) {
            try {
                Field field = type.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(target);
            } catch (NoSuchFieldException ignored) {
                // Continue through the inherited RPC header hierarchy.
            }
        }
        throw new NoSuchFieldException(name);
    }

    private static Map<String, Object> convertFromV1() throws Exception {
        org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader v1 =
            new org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader();
        v1.setProducerGroup("producer");
        v1.setTopic("orders");
        v1.setDefaultTopic("TBW102");
        v1.setDefaultTopicQueueNums(4);
        v1.setQueueId(2);
        v1.setSysFlag(1);
        v1.setBornTimestamp(1712345678901L);
        v1.setFlag(7);
        v1.setProperties("TAGS\u0001blue");
        v1.setReconsumeTimes(0);
        v1.setUnitMode(false);
        v1.setMaxReconsumeTimes(16);
        v1.setBatch(false);
        v1.setBrokerName("short-broker");
        v1.setNamespace("tenant");
        v1.setNamespaced(true);
        v1.setLo(true);
        v1.setOneway(false);

        SendMessageRequestHeaderV2 converted =
            SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(v1);
        Map<String, Object> result = new TreeMap<>();
        Map<String, Object> header = new TreeMap<>();
        for (String fieldName : FIELDS) {
            header.put(fieldName, readField(converted, fieldName));
        }
        result.put("header", header);

        RemotingCommand command = RemotingCommand.createRequestCommand(310, null);
        command.writeCustomHeader(converted);
        command.makeCustomHeaderToNet();
        result.put("encodedFields", new TreeMap<>(command.getExtFields()));
        return result;
    }

    private static Map<String, String> validFields() {
        Map<String, String> fields = new TreeMap<>();
        fields.put("a", "producer");
        fields.put("b", "orders");
        fields.put("c", "TBW102");
        fields.put("d", "4");
        fields.put("e", "2");
        fields.put("f", "1");
        fields.put("g", "1712345678901");
        fields.put("h", "7");
        fields.put("i", "TAGS\u0001blue");
        fields.put("j", "0");
        fields.put("k", "TRUE");
        fields.put("l", "16");
        fields.put("m", "false");
        fields.put("n", "short-broker");
        fields.put("ns", "tenant");
        fields.put("nsd", "true");
        fields.put("bname", "base-broker");
        fields.put("oway", "false");
        fields.put("lo", "true");
        return fields;
    }
}
