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
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;

/** Invokes both Java custom-header decode modes for nullable change-invisibility fields. */
public final class ChangeInvisibleTimeHeaderDecodeModeFixtures {
    private ChangeInvisibleTimeHeaderDecodeModeFixtures() { }

    public static void main(String[] args) throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("clientArtifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("serverContacted", false);

        Map<String, Object> cases = new TreeMap<>();
        cases.put("invalid-numbers-and-valid-siblings", decodeModes(fields(
            "consumerGroup", "workers", "topic", "orders", "queueId", "not-an-int",
            "extraInfo", "checkpoint", "offset", "١٧", "invisibleTime",
            "9223372036854775808", "liteTopic", "lite-orders", "suspend", "not-true")));
        cases.put("missing-nullable-fields", decodeModes(fields()));
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
        System.out.println("Change-invisibility request header decode fixtures verified");
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
        RemotingCommand command = RemotingCommand.createRequestCommand(0, null);
        command.setExtFields(new HashMap<>(fields));
        try {
            ChangeInvisibleTimeRequestHeader header = command.decodeCommandCustomHeaderDirectly(
                ChangeInvisibleTimeRequestHeader.class, useFastEncode);
            result.put("outcome", "OK");

            RemotingCommand encoded = RemotingCommand.createRequestCommand(0, null);
            encoded.setExtFields(new HashMap<>());
            encoded.writeCustomHeader(header);
            encoded.makeCustomHeaderToNet();
            result.put("encodedFields", new TreeMap<>(encoded.getExtFields()));

            Map<String, Object> values = new TreeMap<>();
            values.put("consumerGroup", header.getConsumerGroup());
            values.put("topic", header.getTopic());
            values.put("queueId", header.getQueueId());
            values.put("extraInfo", header.getExtraInfo());
            values.put("offset", header.getOffset());
            values.put("invisibleTime", header.getInvisibleTime());
            values.put("liteTopic", header.getLiteTopic());
            values.put("suspend", header.isSuspend());
            result.put("values", values);
        } catch (Throwable error) {
            result.put("outcome", error.getClass().getSimpleName());
        }
        return result;
    }

    private static Map<String, String> fields(String... pairs) {
        Map<String, String> fields = new TreeMap<>();
        for (int index = 0; index < pairs.length; index += 2) {
            fields.put(pairs[index], pairs[index + 1]);
        }
        return fields;
    }
}
