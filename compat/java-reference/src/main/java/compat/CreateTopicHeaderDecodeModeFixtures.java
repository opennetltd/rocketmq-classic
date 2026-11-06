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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;

/** Invokes Java 5.5.1 CreateTopicRequestHeader decoding through both mode hints. */
public final class CreateTopicHeaderDecodeModeFixtures {
    private CreateTopicHeaderDecodeModeFixtures() { }

    public static void main(String[] args) throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");

        Map<String, Object> cases = new TreeMap<>();
        cases.put("valid", decodeModes(validFields()));

        Map<String, String> invalidReadQueues = validFields();
        invalidReadQueues.put("readQueueNums", "not-an-integer");
        cases.put("invalid-read-queues", decodeModes(invalidReadQueues));

        Map<String, String> missingWriteQueues = validFields();
        missingWriteQueues.remove("writeQueueNums");
        cases.put("missing-write-queues", decodeModes(missingWriteQueues));

        Map<String, String> invalidTopicSystemFlag = validFields();
        invalidTopicSystemFlag.put("topicSysFlag", "not-an-integer");
        cases.put("invalid-topic-system-flag", decodeModes(invalidTopicSystemFlag));

        Map<String, String> invalidOrder = validFields();
        invalidOrder.put("order", "not-a-boolean");
        cases.put("invalid-order-boolean", decodeModes(invalidOrder));

        Map<String, String> invalidFilterType = validFields();
        invalidFilterType.put("topicFilterType", "NOT_A_FILTER");
        cases.put("invalid-filter-type", decodeModes(invalidFilterType));

        Map<String, String> missingFilterType = validFields();
        missingFilterType.remove("topicFilterType");
        cases.put("missing-filter-type", decodeModes(missingFilterType));

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
        System.out.println("Create-topic header decode fixtures verified");
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
        RemotingCommand command = RemotingCommand.createRequestCommand(17, null);
        command.setExtFields(new HashMap<>(fields));
        try {
            CommandCustomHeader header = command.decodeCommandCustomHeaderDirectly(
                CreateTopicRequestHeader.class, useFastEncode);
            result.put("outcome", "OK");
            RemotingCommand encoded = RemotingCommand.createRequestCommand(17, null);
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
        fields.put("topic", "orders");
        fields.put("defaultTopic", "TBW102");
        fields.put("readQueueNums", "4");
        fields.put("writeQueueNums", "5");
        fields.put("perm", "6");
        fields.put("topicFilterType", "SINGLE_TAG");
        fields.put("topicSysFlag", "3");
        fields.put("order", "true");
        fields.put("attributes", "message.type=normal");
        fields.put("force", "true");
        fields.put("ns", "tenant");
        fields.put("nsd", "true");
        fields.put("bname", "broker-a");
        fields.put("oway", "false");
        fields.put("lo", "true");
        return fields;
    }
}
