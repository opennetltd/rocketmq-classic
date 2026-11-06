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
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;

/** Invokes Java 5.5.1 fast and reflective send-response header decoders. */
public final class SendMessageResponseHeaderDecodeModeFixtures {
    private SendMessageResponseHeaderDecodeModeFixtures() { }

    public static void main(String[] args) throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");

        Map<String, Object> cases = new TreeMap<>();
        cases.put("missing-all-fields", decodeModes(new TreeMap<>()));

        Map<String, String> invalidQueueId = validFields();
        invalidQueueId.put("queueId", "not-an-integer");
        cases.put("invalid-queue-id", decodeModes(invalidQueueId));

        Map<String, String> invalidQueueOffset = validFields();
        invalidQueueOffset.put("queueOffset", "not-a-long");
        cases.put("invalid-queue-offset", decodeModes(invalidQueueOffset));

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
        System.out.println("SendMessageResponseHeader fast/reflection decoder fixtures verified");
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
        RemotingCommand command = RemotingCommand.createRequestCommand(10, null);
        command.setExtFields(new HashMap<>(fields));
        try {
            SendMessageResponseHeader header = command.decodeCommandCustomHeaderDirectly(
                SendMessageResponseHeader.class, useFastEncode);
            result.put("outcome", "OK");
            Map<String, Object> decoded = new TreeMap<>();
            decoded.put("msgId", header.getMsgId());
            decoded.put("queueId", header.getQueueId());
            decoded.put("queueOffset", header.getQueueOffset());
            decoded.put("transactionId", header.getTransactionId());
            decoded.put("batchUniqId", header.getBatchUniqId());
            decoded.put("recallHandle", header.getRecallHandle());
            result.put("header", decoded);
        } catch (Throwable error) {
            result.put("outcome", error.getClass().getSimpleName());
        }
        return result;
    }

    private static Map<String, String> validFields() {
        Map<String, String> fields = new TreeMap<>();
        fields.put("msgId", "offset-id");
        fields.put("queueId", "2");
        fields.put("queueOffset", "9");
        fields.put("transactionId", "transaction-id");
        fields.put("batchUniqId", "batch-id");
        fields.put("recallHandle", "recall-handle");
        return fields;
    }
}
