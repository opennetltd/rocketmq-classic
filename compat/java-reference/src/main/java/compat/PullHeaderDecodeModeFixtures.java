/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;

/** Invokes Java 5.5.1 fast and reflective Pull header decoders. */
public final class PullHeaderDecodeModeFixtures {
    private PullHeaderDecodeModeFixtures() { }

    public static void main(String[] args) throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");

        Map<String, Object> cases = new TreeMap<>();
        cases.put("missing-all-fields", decodeModes(new TreeMap<>()));

        Map<String, String> invalidQueueId = validFields();
        invalidQueueId.put("queueId", "not-an-integer");
        cases.put("invalid-queue-id", decodeModes(invalidQueueId));

        Map<String, String> invalidMaxMsgBytes = validFields();
        invalidMaxMsgBytes.put("maxMsgBytes", "not-an-integer");
        cases.put("invalid-max-msg-bytes", decodeModes(invalidMaxMsgBytes));

        Map<String, String> invalidRequestSource = validFields();
        invalidRequestSource.put("requestSource", "not-an-integer");
        cases.put("invalid-request-source", decodeModes(invalidRequestSource));

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
        System.out.println("PullMessageRequestHeader fast/reflection decoder fixtures verified");
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
        RemotingCommand command = RemotingCommand.createRequestCommand(11, null);
        command.setExtFields(new HashMap<>(fields));
        try {
            PullMessageRequestHeader header = command.decodeCommandCustomHeaderDirectly(
                PullMessageRequestHeader.class, useFastEncode);
            result.put("outcome", "OK");
            RemotingCommand encoded = RemotingCommand.createRequestCommand(11, null);
            encoded.setExtFields(new HashMap<>());
            encoded.writeCustomHeader(header);
            encoded.makeCustomHeaderToNet();
            result.put("encodedFields", new TreeMap<>(encoded.getExtFields()));
        } catch (Throwable error) {
            result.put("outcome", error.getClass().getSimpleName());
        }
        return result;
    }

    private static Map<String, String> validFields() {
        Map<String, String> fields = new TreeMap<>();
        fields.put("consumerGroup", "workers");
        fields.put("topic", "orders");
        fields.put("queueId", "2");
        fields.put("queueOffset", "9");
        fields.put("maxMsgNums", "32");
        fields.put("sysFlag", "0");
        fields.put("commitOffset", "8");
        fields.put("suspendTimeoutMillis", "15000");
        fields.put("subscription", "tag-a");
        fields.put("subVersion", "3");
        fields.put("expressionType", "TAG");
        fields.put("maxMsgBytes", "65536");
        fields.put("requestSource", "1");
        fields.put("proxyFrowardClientId", "proxy-a");
        fields.put("lo", "true");
        fields.put("ns", "tenant");
        fields.put("nsd", "true");
        fields.put("bname", "broker-a");
        fields.put("oway", "false");
        return fields;
    }
}
