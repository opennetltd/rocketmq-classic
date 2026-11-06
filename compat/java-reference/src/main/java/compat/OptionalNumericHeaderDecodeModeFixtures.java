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
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.ReplyMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;

/** Pins per-field reflection conversion failures for nullable and primitive header fields. */
public final class OptionalNumericHeaderDecodeModeFixtures {
    private OptionalNumericHeaderDecodeModeFixtures() { }

    public static void main(String[] args) throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("serverContacted", false);

        Map<String, Object> cases = new TreeMap<>();
        cases.put("send-back-overflow", decodeModes("send-back", fields(
            "group", "workers", "originTopic", "orders", "offset", "9",
            "delayLevel", "2", "maxReconsumeTimes", "2147483648")));
        cases.put("reset-offset-overflow", decodeModes("reset-offset", fields(
            "group", "workers", "topic", "orders", "queueId", "3",
            "offset", "9223372036854775808", "timestamp", "1712345678",
            "isForce", "true")));
        cases.put("reset-offset-invalid-queue", decodeModes("reset-offset", fields(
            "group", "workers", "topic", "orders", "queueId", "not-an-int",
            "offset", "9", "timestamp", "1712345678", "isForce", "true")));
        cases.put("consume-directly-one-invalid", decodeModes("consume-directly", fields(
            "consumerGroup", "workers", "topic", "orders", "topicSysFlag", "not-an-int",
            "groupSysFlag", "١٢")));
        cases.put("reply-message-invalid", decodeModes("reply-message", fields(
            "producerGroup", "reply-producers", "topic", "orders", "defaultTopic", "TBW102",
            "defaultTopicQueueNums", "4", "queueId", "1", "sysFlag", "0",
            "bornTimestamp", "1712345678000", "flag", "0", "properties", "",
            "reconsumeTimes", "not-an-int", "unitMode", "false", "bornHost", "127.0.0.1:1",
            "storeHost", "127.0.0.1:2", "storeTimestamp", "1712345678001")));
        cases.put("send-v1-invalid-and-valid", decodeModes("send-v1", fields(
            "producerGroup", "producers", "topic", "orders", "defaultTopic", "TBW102",
            "defaultTopicQueueNums", "4", "queueId", "1", "sysFlag", "0",
            "bornTimestamp", "1712345678000", "flag", "0", "unitMode", "false",
            "bornHost", "127.0.0.1:1", "storeHost", "127.0.0.1:2",
            "storeTimestamp", "1712345678001", "reconsumeTimes", "not-an-int",
            "maxReconsumeTimes", "٧")));
        cases.put("pop-defaults", decodeModes("pop", fields()));
        cases.put("pop-invalid-primitive-fields", decodeModes("pop", fields(
            "consumerGroup", "workers", "topic", "orders", "queueId", "not-an-int",
            "maxMsgNums", "2147483648", "invisibleTime", "not-a-long",
            "pollTime", "9223372036854775808", "initMode", "-2147483649",
            "order", "not-a-boolean")));
        cases.put("pop-valid-unicode-fields", decodeModes("pop", fields(
            "consumerGroup", "workers", "topic", "orders", "queueId", "١",
            "maxMsgNums", "١٦", "invisibleTime", "٣٠٠٠٠", "pollTime", "١٠٠٠",
            "bornTime", "١٧١٢٣٤٥٦٧٨٩٠١", "initMode", "٠", "order", "true",
            "ns", "tenant", "nsd", "not-a-boolean", "bname", "broker-a", "oway", "true")));
        cases.put("pop-lite-defaults", decodeModes("pop-lite", fields()));
        cases.put("pop-lite-invalid-primitive-fields", decodeModes("pop-lite", fields(
            "clientId", "client", "consumerGroup", "workers", "topic", "orders",
            "maxMsgNum", "2147483648", "invisibleTime", "not-a-long",
            "pollTime", "9223372036854775808", "bornTime", "-9223372036854775809",
            "nsd", "not-a-boolean")));
        cases.put("pop-lite-valid-unicode-fields", decodeModes("pop-lite", fields(
            "clientId", "client", "consumerGroup", "workers", "topic", "orders",
            "maxMsgNum", "١٦", "invisibleTime", "٣٠٠٠٠", "pollTime", "١٠٠٠",
            "bornTime", "١٧١٢٣٤٥٦٧٨٩٠١", "attemptId", "attempt",
            "ns", "tenant", "nsd", "not-a-boolean", "bname", "broker-a", "oway", "true")));
        cases.put("pop-response-defaults", decodeModes("pop-response", fields()));
        cases.put("pop-response-invalid-primitive-fields", decodeModes("pop-response", fields(
            "popTime", "not-a-long", "invisibleTime", "9223372036854775808",
            "reviveQid", "not-an-int", "restNum", "-9223372036854775809")));
        cases.put("pop-response-valid-unicode-fields", decodeModes("pop-response", fields(
            "popTime", "١٠", "invisibleTime", "٢٠", "reviveQid", "١", "restNum", "٣",
            "startOffsetInfo", "orders@0:100", "msgOffsetInfo", "orders@0:101",
            "orderCountInfo", "0")));
        cases.put("pop-lite-response-defaults", decodeModes("pop-lite-response", fields()));
        cases.put("pop-lite-response-invalid-primitive-fields", decodeModes(
            "pop-lite-response", fields("popTime", "not-a-long", "invisibleTime",
                "9223372036854775808", "reviveQid", "not-an-int")));
        cases.put("pop-lite-response-valid-unicode-fields", decodeModes(
            "pop-lite-response", fields("popTime", "١٠", "invisibleTime", "٢٠",
                "reviveQid", "١", "startOffsetInfo", "orders@0:100",
                "msgOffsetInfo", "orders@0:101", "orderCountInfo", "0")));
        fixture.put("cases", cases);

        Map<String, Object> popLiteTimeoutCases = new TreeMap<>();
        PopLiteMessageRequestHeader expired = new PopLiteMessageRequestHeader();
        expired.setBornTime(1L);
        expired.setPollTime(1L);
        popLiteTimeoutCases.put("elapsed", expired.isTimeoutTooMuch());
        PopLiteMessageRequestHeader future = new PopLiteMessageRequestHeader();
        future.setBornTime(Long.MAX_VALUE);
        future.setPollTime(0L);
        popLiteTimeoutCases.put("far-future", future.isTimeoutTooMuch());
        fixture.put("popLiteTimeoutCases", popLiteTimeoutCases);
        Map<String, Object> popTimeoutCases = new TreeMap<>();
        PopMessageRequestHeader expiredPop = new PopMessageRequestHeader();
        expiredPop.setBornTime(1L);
        expiredPop.setPollTime(1L);
        popTimeoutCases.put("elapsed", expiredPop.isTimeoutTooMuch());
        PopMessageRequestHeader futurePop = new PopMessageRequestHeader();
        futurePop.setBornTime(Long.MAX_VALUE);
        futurePop.setPollTime(0L);
        popTimeoutCases.put("far-future", futurePop.isTimeoutTooMuch());
        fixture.put("popTimeoutCases", popTimeoutCases);

        Map<String, Object> popHeaderApi = new TreeMap<>();
        PopMessageRequestHeader defaultPop = new PopMessageRequestHeader();
        PopLiteMessageRequestHeader defaultLitePop = new PopLiteMessageRequestHeader();
        popHeaderApi.put("popMessageToStringDefault", defaultPop.toString());
        popHeaderApi.put("popLiteToStringDefault", defaultLitePop.toString());
        PopMessageRequestHeader popApi = new PopMessageRequestHeader();
        popApi.setConsumerGroup("workers");
        popApi.setTopic("订单");
        popApi.setQueueId(-9);
        popApi.setMaxMsgNums(3);
        popApi.setInvisibleTime(30000L);
        popApi.setPollTime(1000L);
        popApi.setBornTime(1712345678901L);
        popApi.setInitMode(1);
        popApi.setExpType("TAG");
        popApi.setExp("blue");
        popApi.setOrder(null);
        popHeaderApi.put("popMessageNegativeQueueIdGetter", popApi.getQueueId());
        popHeaderApi.put("popMessageNullableOrderGetter", popApi.getOrder());
        popHeaderApi.put("popMessageNullableOrderPredicate", popApi.isOrder());
        popHeaderApi.put("popMessageToStringCustom", popApi.toString());
        popApi.setOrder(Boolean.TRUE);
        popHeaderApi.put("popMessageTrueOrderPredicate", popApi.isOrder());
        PopLiteMessageRequestHeader liteApi = new PopLiteMessageRequestHeader();
        liteApi.setConsumerGroup("workers");
        liteApi.setTopic("订单");
        liteApi.setMaxMsgNum(4);
        liteApi.setInvisibleTime(30000L);
        liteApi.setPollTime(1000L);
        liteApi.setBornTime(1712345678901L);
        liteApi.setAttemptId("attempt");
        liteApi.setClientId("client");
        popHeaderApi.put("popLiteToStringCustom", liteApi.toString());
        fixture.put("popHeaderApi", popHeaderApi);

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
        System.out.println("Optional numeric header decode fixtures verified");
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
        RemotingCommand command = RemotingCommand.createRequestCommand(0, null);
        command.setExtFields(new HashMap<>(fields));
        try {
            Class<? extends CommandCustomHeader> headerClass;
            switch (type) {
                case "send-back":
                    headerClass = ConsumerSendMsgBackRequestHeader.class;
                    break;
                case "reset-offset":
                    headerClass = ResetOffsetRequestHeader.class;
                    break;
                case "consume-directly":
                    headerClass = ConsumeMessageDirectlyResultRequestHeader.class;
                    break;
                case "reply-message":
                    headerClass = ReplyMessageRequestHeader.class;
                    break;
                case "send-v1":
                    headerClass = SendMessageRequestHeader.class;
                    break;
                case "pop":
                    headerClass = PopMessageRequestHeader.class;
                    break;
                case "pop-lite":
                    headerClass = PopLiteMessageRequestHeader.class;
                    break;
                case "pop-response":
                    headerClass = PopMessageResponseHeader.class;
                    break;
                case "pop-lite-response":
                    headerClass = PopLiteMessageResponseHeader.class;
                    break;
                default:
                    throw new IllegalArgumentException("unknown header type: " + type);
            }
            CommandCustomHeader header = command.decodeCommandCustomHeaderDirectly(
                headerClass, useFastEncode);
            result.put("outcome", "OK");

            RemotingCommand encoded = RemotingCommand.createRequestCommand(0, null);
            encoded.setExtFields(new HashMap<>());
            encoded.writeCustomHeader(header);
            encoded.makeCustomHeaderToNet();
            result.put("encodedFields", new TreeMap<>(encoded.getExtFields()));
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
