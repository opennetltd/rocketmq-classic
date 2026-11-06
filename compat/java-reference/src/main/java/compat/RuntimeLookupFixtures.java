package compat;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

/** Invokes actual topic broker selection and assignment decoding without a transport. */
public final class RuntimeLookupFixtures {
    private RuntimeLookupFixtures() { }

    public static void main(String[] args) throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("192.0.2.6");
        long bootTimestampWindowStart = System.currentTimeMillis();
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        long bootTimestampWindowEnd = System.currentTimeMillis();
        long bootTimestamp = runtime.getBootTimestamp();
        List<Object> cases = new ArrayList<>();
        try {
            add(cases, runtime, "missing-topic", null, new String[] {});
            add(cases, runtime, "empty-brokers", "[]", new String[] {});
            add(cases, runtime, "missing-broker-list", "MISSING", new String[] {});
            add(cases, runtime, "null-broker-list", "null", new String[] {});
            add(cases, runtime, "null-broker-row", "[null]", new String[] {});
            add(cases, runtime, "master-preferred", "[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"master:10911\",\"2\":\"slave:10911\"}}]", new String[] {"master:10911"});
            add(cases, runtime, "single-slave", "[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"2\":\"slave:10911\"}}]", new String[] {"slave:10911"});
            add(cases, runtime, "random-slaves", "[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"2\":\"a:10911\",\"3\":\"b:10911\"}}]", new String[] {"a:10911", "b:10911"});
            add(cases, runtime, "random-brokers", "[{\"brokerName\":\"a\",\"brokerAddrs\":{\"0\":\"a:10911\",\"2\":\"ignored:10911\"}},{\"brokerName\":\"b\",\"brokerAddrs\":{\"2\":\"b:10911\"}}]", new String[] {"a:10911", "b:10911"});
            add(cases, runtime, "empty-addresses", "[{\"brokerName\":\"broker\",\"brokerAddrs\":{}}]", new String[] {});
            add(cases, runtime, "null-address-map", "[{\"brokerName\":\"broker\",\"brokerAddrs\":null}]", new String[] {});
            add(cases, runtime, "null-broker-id-single-address", "[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"null\":\"null-id:10911\"}}]", new String[] {"null-id:10911"});
            add(cases, runtime, "null-broker-id-with-master", "[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"null\":\"null-id:10911\",\"0\":\"master:10911\"}}]", new String[] {"master:10911"});
            add(cases, runtime, "null-broker-id-with-slave", "[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"null\":\"null-id:10911\",\"2\":\"slave:10911\"}}]", new String[] {"null-id:10911", "slave:10911"});
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("fixture teardown"));
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint", "MQClientInstance.findBrokerAddrByTopic/findConsumerIdList/queryAssignment/getBootTimestamp/startScheduledTask registration/BrokerData.selectBrokerAddr/QueryAssignmentResponseBody.decode/GetConsumerListByGroupResponseBody.decode/ResetOffsetBody.decode/encode/GetConsumerStatusBody.decode/encode/CheckClientRequestBody and client remoting DTO inherited RemotingSerializable methods/QueueTimeSpan timestamp-string getters and public DTO constructor/getter/setter/toString methods");
        fixture.put("serverBaselineContacted", false);
        fixture.put("cases", cases);
        fixture.put("remotingSerializationWriterFeatures",
            Arrays.stream(com.alibaba.fastjson2.JSONWriter.Feature.values())
                .map(Enum::name)
                .collect(java.util.stream.Collectors.toList()));
        fixture.put("remotingSerializableWriterEscapingCase", remotingSerializableWriterEscapingCase());
        fixture.put("bootTimestampInsideConstructorWindow",
            bootTimestamp >= bootTimestampWindowStart && bootTimestamp <= bootTimestampWindowEnd);
        fixture.put("bootTimestampStableAfterFixtureCalls", runtime.getBootTimestamp() == bootTimestamp);
        String assignment = "{\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2},\"mode\":\"POP\",\"attachments\":{\"key\":\"value\"}}";
        org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody response =
            org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.decode(
                ("{\"messageQueueAssignments\":[" + assignment + "," + assignment + "]}").getBytes(java.nio.charset.StandardCharsets.UTF_8),
                org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.class);
        fixture.put("assignmentDuplicateCount", response.getMessageQueueAssignments().size());
        fixture.put("assignmentModeCases", assignmentModeCases());
        fixture.put("assignmentAttachmentCases", assignmentAttachmentCases());
        fixture.put("assignmentBodyCases", assignmentBodyCases());
        fixture.put("assignmentBodyRootCases", assignmentBodyRootCases());
        fixture.put("liteSubscriptionEnumCases", liteSubscriptionEnumCases());
        fixture.put("liteSubscriptionFieldCases", liteSubscriptionFieldCases());
        fixture.put("liteSubscriptionIdentityCases", liteSubscriptionIdentityCases());
        fixture.put("liteSubscriptionApiCases", liteSubscriptionApiCases());
        fixture.put("liteSubscriptionInputCases", liteSubscriptionInputCases());
        fixture.put("liteSubscriptionDtoApiCase", liteSubscriptionDtoApiCase());
        fixture.put("offsetOptionApiCase", offsetOptionApiCase());
        fixture.put("liteSubscriptionControlApiCase", liteSubscriptionControlApiCase());
        fixture.put("liteSubscriptionRootCases", liteSubscriptionRootCases());
        fixture.put("liteSubscriptionControlFieldCases", liteSubscriptionControlFieldCases());
        fixture.put("liteSubscriptionElementCases", liteSubscriptionElementCases());
        fixture.put("liteSubscriptionOffsetValueCases", liteSubscriptionOffsetValueCases());
        fixture.put("liteSubscriptionSetCases", liteSubscriptionSetCases());
        fixture.put("queryAssignmentEnumCases", queryAssignmentEnumCases());
        fixture.put("queryAssignmentFieldCases", queryAssignmentFieldCases());
        fixture.put("queryAssignmentRootCases", queryAssignmentRootCases());
        fixture.put("queryAssignmentRequestApiCase", queryAssignmentRequestApiCase());
        fixture.put("queryAssignmentNullInputsApiCase", queryAssignmentNullInputsApiCase());
        fixture.put("queryAssignmentEmptyInputsApiCase", queryAssignmentEmptyInputsApiCase());
        fixture.put("queryAssignmentResponseApiCase", queryAssignmentResponseApiCase());
        fixture.put("setRequestModeCases", setRequestModeCases());
        fixture.put("setRequestModeFieldCases", setRequestModeFieldCases());
        fixture.put("setRequestModeRootCases", setRequestModeRootCases());
        fixture.put("setRequestModeRequestApiCase", setRequestModeRequestApiCase());
        fixture.put("checkClientBodyApiCase", checkClientBodyApiCase());
        fixture.put("checkClientInheritedSerializationCases", checkClientInheritedSerializationCases());
        fixture.put("checkClientInheritedSerializationNullUtilities",
            checkClientInheritedSerializationNullUtilities());
        fixture.put("remotingSerializableBodyCases", remotingSerializableBodyCases());
        fixture.put("remotingSerializableBodyNullUtilities", remotingSerializableBodyNullUtilities());
        fixture.put("consumerListApiCase", consumerListApiCase());
        fixture.put("consumerListNullGroupApiCase", consumerListNullGroupApiCase());
        fixture.put("consumerListEmptyGroupApiCase", consumerListEmptyGroupApiCase());
        fixture.put("defaultTopicRouteApiCase", defaultTopicRouteApiCase());
        fixture.put("queryConsumeTimeSpanApiCase", queryConsumeTimeSpanApiCase());
        fixture.put("queueTimeSpanApiCase", queueTimeSpanApiCase());
        fixture.put("lockBatchRequestApiCase", lockBatchRequestApiCase());
        fixture.put("unlockBatchRequestApiCase", unlockBatchRequestApiCase());
        fixture.put("lockBatchResponseApiCase", lockBatchResponseApiCase());
        fixture.put("consumeDirectResultApiCase", consumeDirectResultApiCase());
        fixture.put("consumerStatusBodyApiCase", consumerStatusBodyApiCase());
        fixture.put("resetOffsetBodyApiCase", resetOffsetBodyApiCase());
        fixture.put("messageRequestModeWrapperCases", messageRequestModeWrapperCases());
        fixture.put("messageRequestModeWrapperApiCase", messageRequestModeWrapperApiCase());
        fixture.put("consumeDirectResultCases", consumeDirectResultCases());
        fixture.put("messageQueueInputCases", messageQueueInputCases());
        fixture.put("assignmentHashCollisionCases", assignmentHashCollisionCases());
        fixture.put("assignmentMethodCases", assignmentMethodCases());
        fixture.put("assignmentNullAttachmentApiCase", assignmentNullAttachmentApiCase());
        fixture.put("messageRequestModeMethodCases", messageRequestModeMethodCases());
        fixture.put("consumerListCases", consumerListCases());
        fixture.put("consumerListRootCases", consumerListRootCases());
        fixture.put("lockBatchRequestBodyCases", lockBatchRequestBodyCases());
        fixture.put("unlockBatchRequestBodyCases", unlockBatchRequestBodyCases());
        fixture.put("lockBatchResponseBodyCases", lockBatchResponseBodyCases());
        fixture.put("batchAckBodyInputCases", batchAckBodyInputCases());
        fixture.put("batchAckApiCase", batchAckApiCase());
        fixture.put("batchAckInheritedSerializationCases", batchAckInheritedSerializationCases());
        fixture.put("batchAckInheritedSerializationNullUtilities",
            batchAckInheritedSerializationNullUtilities());
        fixture.put("batchAckBitSetCapacityCase", batchAckBitSetCapacityCase());
        fixture.put("runtimeScheduledTaskCases", runtimeScheduledTaskCases());
        fixture.put("runtimeNullBrokerLookupRpcTopics", runtimeNullBrokerLookupRpcTopics());
        fixture.put("runtimeLookupInvocation", runtimeLookupInvocation());
        fixture.put("runtimeLookupVipMutation", runtimeLookupVipMutation());
        fixture.put("resetOffsetBodyInputCases", resetOffsetBodyInputCases());
        fixture.put("consumerStatusBodyInputCases", consumerStatusBodyInputCases());
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else {
            Files.createDirectories(file.getParent()); Files.writeString(file, json);
        }
        System.out.println("Runtime broker lookup verified: " + cases.size());
    }

    private static List<Object> assignmentModeCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(assignmentModeCase("missing", null));
        cases.add(assignmentModeCase("explicit-null", "null"));
        cases.add(assignmentModeCase("pull-name", "\"PULL\""));
        cases.add(assignmentModeCase("pop-name", "\"POP\""));
        cases.add(assignmentModeCase("lowercase-pop", "\"pop\""));
        cases.add(assignmentModeCase("mixed-case-pull", "\"Pull\""));
        cases.add(assignmentModeCase("ordinal-pull", "0"));
        cases.add(assignmentModeCase("ordinal-pop", "1"));
        cases.add(assignmentModeCase("numeric-string", "\"1\""));
        cases.add(assignmentModeCase("numeric-string-fraction", "\"1.9\""));
        cases.add(assignmentModeCase("fractional-ordinal", "1.9"));
        cases.add(assignmentModeCase("fractional-ordinal-precision-boundary", "0.9999999999999999999"));
        cases.add(assignmentModeCase("decimal-ordinal-wraps-to-zero", "4294967296.0"));
        cases.add(assignmentModeCase("integer-ordinal-wrap-boundary", "4294967296"));
        cases.add(assignmentModeCase("negative-fractional-ordinal", "-0.9"));
        cases.add(assignmentModeCase("negative-ordinal", "-1"));
        cases.add(assignmentModeCase("ordinal-out-of-range", "2"));
        cases.add(assignmentModeCase("long-overflow-ordinal", "9223372036854775808"));
        cases.add(assignmentModeCase("invalid-name", "\"NOT_A_MODE\""));
        cases.add(assignmentModeCase("boolean", "true"));
        return cases;
    }

    private static Map<String, Object> assignmentModeCase(String name, String modeValue) {
        String assignment = "{\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2}"
            + (modeValue == null ? "" : ",\"mode\":" + modeValue) + "}";
        String body = "{\"messageQueueAssignments\":[" + assignment + "]}";
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("input", com.alibaba.fastjson2.JSON.parseObject(body));
        item.put("rawInput", body);
        try {
            org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody decoded =
                org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.decode(
                    body.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.class);
            org.apache.rocketmq.common.message.MessageQueueAssignment value =
                decoded.getMessageQueueAssignments().iterator().next();
            item.put("outcome", "ACCEPTED");
            item.put("mode", value.getMode() == null ? null : value.getMode().name());
            byte[] encoded = decoded.encode();
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                new String(encoded, java.nio.charset.StandardCharsets.UTF_8)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> assignmentAttachmentCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(assignmentAttachmentCase("missing", null));
        cases.add(assignmentAttachmentCase("explicit-null", "null"));
        cases.add(assignmentAttachmentCase("scalar-values",
            "{\"number\":1,\"fraction\":1.9,\"boolean\":true,\"nullValue\":null}"));
        cases.add(assignmentAttachmentCase("numeric-string-values",
            "{\"plain\":9223372036854775807.123456789012345678901,\"exponent\":1e2}"));
        cases.add(assignmentAttachmentCase("nested-values",
            "{\"array\":[1,\"x\"],\"object\":{\"key\":\"value\"}}"));
        cases.add(assignmentAttachmentCase("array-map", "[]"));
        cases.add(assignmentAttachmentCase("scalar-map", "\"not-a-map\""));
        return cases;
    }

    private static Map<String, Object> assignmentAttachmentCase(String name, String attachmentsValue) {
        String assignment = "{\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2},\"mode\":\"POP\""
            + (attachmentsValue == null ? "" : ",\"attachments\":" + attachmentsValue) + "}";
        String body = "{\"messageQueueAssignments\":[" + assignment + "]}";
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("input", com.alibaba.fastjson2.JSON.parseObject(body));
        item.put("rawInput", body);
        try {
            org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody decoded =
                org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.decode(
                    body.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.class);
            org.apache.rocketmq.common.message.MessageQueueAssignment value =
                decoded.getMessageQueueAssignments().iterator().next();
            item.put("outcome", "ACCEPTED");
            item.put("attachments", value.getAttachments());
            byte[] encoded = decoded.encode();
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                new String(encoded, java.nio.charset.StandardCharsets.UTF_8)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> assignmentBodyCases() {
        String queue = "\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2}";
        List<Object> cases = new ArrayList<>();
        cases.add(assignmentBodyCase("missing", null, false));
        cases.add(assignmentBodyCase("explicit-null", "null", true));
        cases.add(assignmentBodyCase("empty", "[]", true));
        cases.add(assignmentBodyCase("single-default", "[{}]", true));
        cases.add(assignmentBodyCase("null-element", "[null]", true));
        cases.add(assignmentBodyCase("duplicate-null-elements", "[null,null]", true));
        cases.add(assignmentBodyCase("string-element", "[\"assignment\"]", true));
        cases.add(assignmentBodyCase("number-element", "[7]", true));
        cases.add(assignmentBodyCase("boolean-element", "[true]", true));
        cases.add(assignmentBodyCase("array-element", "[[]]", true));
        cases.add(assignmentBodyCase("nested-array-object-element", "[[{}]]", true));
        cases.add(assignmentBodyCase("mixed-null-object-elements", "[null,{}]", true));
        cases.add(assignmentBodyCase("mixed-scalar-object-elements", "[7,{}]", true));
        cases.add(assignmentBodyCase("object-field", "{" + queue + "}", true));
        cases.add(assignmentBodyCase("string-field", "\"assignment\"", true));
        cases.add(assignmentBodyCase("number-field", "7", true));
        cases.add(assignmentBodyCase("fraction-field", "1.5", true));
        cases.add(assignmentBodyCase("boolean-field", "true", true));
        cases.add(assignmentBodyCase("identical-duplicates", "[{" + queue + "},{" + queue + "}]", true));
        String pull = "{\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2},"
            + "\"mode\":\"PULL\"}";
        String pop = "{\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2},"
            + "\"mode\":\"POP\"}";
        cases.add(assignmentBodyCase("same-queue-different-mode", "[" + pull + "," + pop + "]", true));
        String attachmentHashCollisionLeft = "{\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2},"
            + "\"mode\":\"PULL\",\"attachments\":{\"Aa\":\"value\"}}";
        String attachmentHashCollisionRight = "{\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2},"
            + "\"mode\":\"PULL\",\"attachments\":{\"BB\":\"value\"}}";
        cases.add(assignmentBodyCase("same-mode-attachment-hash-collision",
            "[" + attachmentHashCollisionLeft + "," + attachmentHashCollisionRight + "]", true));
        String attachmentDifferentHashLeft = "{\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2},"
            + "\"mode\":\"PULL\",\"attachments\":{\"A\":\"value\"}}";
        String attachmentDifferentHashRight = "{\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2},"
            + "\"mode\":\"PULL\",\"attachments\":{\"B\":\"value\"}}";
        cases.add(assignmentBodyCase("same-mode-different-attachment-hash",
            "[" + attachmentDifferentHashLeft + "," + attachmentDifferentHashRight + "]", true));
        cases.add(assignmentBodyCase("duplicate-null-queue-assignments", "[{},{}]", true));
        cases.add(assignmentBodyRawCase("duplicate-root-field-last-wins",
            "{\"messageQueueAssignments\":[{}],\"messageQueueAssignments\":[]}"));
        cases.add(assignmentBodyCase("duplicate-nested-mode-last-wins",
            "[{\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2},"
                + "\"mode\":\"PULL\",\"mode\":\"POP\"}]", true));
        cases.add(assignmentBodyRawCase("duplicate-nested-queue-field-last-wins",
            "{\"messageQueueAssignments\":[{\"messageQueue\":{\"topic\":\"first\","
                + "\"topic\":\"last\",\"brokerName\":\"broker\",\"queueId\":2}}]}"));
        String[] messageQueueValues = {"null", "\"queue\"", "7", "true", "[]", "{}",
            "{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2}"};
        String[] messageQueueNames = {"null", "string", "number", "boolean", "array", "empty-object", "object"};
        for (int i = 0; i < messageQueueValues.length; i++) {
            cases.add(assignmentBodyRawCase("message-queue-" + messageQueueNames[i],
                "{\"messageQueueAssignments\":[{\"messageQueue\":" + messageQueueValues[i] + ",\"mode\":\"PULL\"}]}"));
        }
        return cases;
    }

    private static List<Object> assignmentBodyRootCases() {
        List<Object> cases = new ArrayList<>();
        String[][] roots = {
            {"null", "null"}, {"empty-object", "{}"}, {"empty-array", "[]"},
            {"string", "\"root\""}, {"integer", "7"}, {"fraction", "1.5"},
            {"boolean", "true"}
        };
        for (String[] root : roots) {
            String body = root[1];
            Map<String, Object> item = new TreeMap<>();
            item.put("name", "root-" + root[0]);
            item.put("input", com.alibaba.fastjson2.JSON.parse(body));
            item.put("rawInput", body);
            try {
                org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody decoded =
                    org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.decode(
                        body.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.class);
                item.put("outcome", "ACCEPTED");
                Set<org.apache.rocketmq.common.message.MessageQueueAssignment> assignments =
                    decoded.getMessageQueueAssignments();
                item.put("assignments", assignments);
                byte[] encoded = decoded.encode();
                item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                    new String(encoded, java.nio.charset.StandardCharsets.UTF_8)));
            } catch (RuntimeException error) {
                item.put("outcome", "REJECTED");
                item.put("error", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> liteSubscriptionEnumCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(liteSubscriptionEnumCase("action-duplicate-last-wins", "{\"subscriptionSet\":[{\"action\":\"PARTIAL_ADD\",\"action\":\"COMPLETE_REMOVE\"}]}"));
        cases.add(liteSubscriptionEnumCase("client-id-duplicate-last-wins", "{\"subscriptionSet\":[{\"clientId\":\"first\",\"clientId\":\"last\"}]}"));
        cases.add(liteSubscriptionEnumCase("group-duplicate-last-wins", "{\"subscriptionSet\":[{\"group\":\"first\",\"group\":\"last\"}]}"));
        cases.add(liteSubscriptionEnumCase("topic-duplicate-last-wins", "{\"subscriptionSet\":[{\"topic\":\"first\",\"topic\":\"last\"}]}"));
        cases.add(liteSubscriptionEnumCase("lite-topic-set-duplicate-last-wins", "{\"subscriptionSet\":[{\"liteTopicSet\":[\"first\"],\"liteTopicSet\":[\"last\"]}]}"));
        cases.add(liteSubscriptionEnumCase("offset-option-duplicate-last-wins", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":\"POLICY\",\"value\":1},\"offsetOption\":{\"type\":\"TIMESTAMP\",\"value\":2}}]}"));
        cases.add(liteSubscriptionEnumCase("version-duplicate-last-wins", "{\"subscriptionSet\":[{\"version\":1,\"version\":2}]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-duplicate-last-wins", "{\"subscriptionSet\":[{\"topic\":\"first\"}],\"subscriptionSet\":[{\"topic\":\"last\"}]}"));
        cases.add(liteSubscriptionEnumCase("offset-type-duplicate-last-wins", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":\"POLICY\",\"type\":\"TIMESTAMP\",\"value\":1}}]}"));
        cases.add(liteSubscriptionEnumCase("offset-value-duplicate-last-wins", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":\"POLICY\",\"value\":1,\"value\":2}}]}"));
        cases.add(liteSubscriptionEnumCase("action-name", "{\"subscriptionSet\":[{\"action\":\"PARTIAL_ADD\"}]}"));
        cases.add(liteSubscriptionEnumCase("action-ordinal", "{\"subscriptionSet\":[{\"action\":1}]}"));
        cases.add(liteSubscriptionEnumCase("action-numeric-string", "{\"subscriptionSet\":[{\"action\":\"1\"}]}"));
        cases.add(liteSubscriptionEnumCase("action-fractional", "{\"subscriptionSet\":[{\"action\":1.9}]}"));
        cases.add(liteSubscriptionEnumCase("action-out-of-range", "{\"subscriptionSet\":[{\"action\":4}]}"));
        cases.add(liteSubscriptionEnumCase("action-unknown-string", "{\"subscriptionSet\":[{\"action\":\"UNKNOWN\"}]}"));
        cases.add(liteSubscriptionEnumCase("offset-name", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":\"POLICY\",\"value\":1}}]}"));
        cases.add(liteSubscriptionEnumCase("offset-missing-value", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":\"POLICY\"}}]}"));
        cases.add(liteSubscriptionEnumCase("offset-ordinal", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":3,\"value\":1}}]}"));
        cases.add(liteSubscriptionEnumCase("offset-numeric-string", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":\"3\",\"value\":1}}]}"));
        cases.add(liteSubscriptionEnumCase("offset-fractional", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":2.9,\"value\":1}}]}"));
        cases.add(liteSubscriptionEnumCase("offset-out-of-range", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":4,\"value\":1}}]}"));
        cases.add(liteSubscriptionEnumCase("offset-unknown-string", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":\"UNKNOWN\",\"value\":1}}]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-null", "{\"subscriptionSet\":null}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-empty", "{\"subscriptionSet\":[]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-single-object", "{\"subscriptionSet\":{\"topic\":\"object\"}}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-singleton-object-array", "{\"subscriptionSet\":[{\"topic\":\"array\"}]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-null-element", "{\"subscriptionSet\":[null]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-string-element", "{\"subscriptionSet\":[\"text\"]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-number-element", "{\"subscriptionSet\":[42]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-boolean-element", "{\"subscriptionSet\":[true]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-scalar-string", "{\"subscriptionSet\":\"text\"}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-scalar-number", "{\"subscriptionSet\":42}"));
        return cases;
    }

    private static List<Object> liteSubscriptionFieldCases() {
        List<Object> cases = new ArrayList<>();
        String[][] fields = {
            {"action"}, {"clientId"}, {"group"}, {"topic"},
            {"liteTopicSet"}, {"offsetOption"}, {"version"}
        };
        String[][] shapes = {
            {"null", "null"}, {"numeric-string", "\"7\""}, {"integer", "0"},
            {"fraction", "1.5"}, {"boolean", "true"}, {"array", "[]"}, {"object", "{}"}
        };
        for (String[] field : fields) {
            for (String[] shape : shapes) {
                String value = shape[1];
                if (field[0].equals("action") && shape[0].equals("numeric-string")) {
                    value = "\"1\"";
                }
                if (field[0].equals("offsetOption") && shape[0].equals("object")) {
                    value = "{\"type\":\"POLICY\",\"value\":1}";
                }
                String input = "{\"subscriptionSet\":[{\"" + field[0] + "\":" + value + "}]}";
                cases.add(liteSubscriptionEnumCase(
                    "shape-" + field[0] + "-" + shape[0], input));
            }
        }
        return cases;
    }

    private static List<Object> liteSubscriptionIdentityCases() {
        List<Object> cases = new ArrayList<>();
        org.apache.rocketmq.common.lite.LiteSubscriptionDTO dto1 =
            new org.apache.rocketmq.common.lite.LiteSubscriptionDTO()
                .setGroup("workers").setTopic("orders").setVersion(7L);
        org.apache.rocketmq.common.lite.LiteSubscriptionDTO dto2 =
            new org.apache.rocketmq.common.lite.LiteSubscriptionDTO()
                .setGroup("workers").setTopic("orders").setVersion(7L);
        cases.add(liteIdentityCase("LiteSubscriptionDTO", dto1, dto2));

        org.apache.rocketmq.common.lite.LiteSubscription subscription1 =
            new org.apache.rocketmq.common.lite.LiteSubscription()
                .setGroup("workers").setTopic("orders");
        subscription1.addLiteTopic("lite-orders");
        subscription1.setUpdateTime(7L);
        org.apache.rocketmq.common.lite.LiteSubscription subscription2 =
            new org.apache.rocketmq.common.lite.LiteSubscription()
                .setGroup("workers").setTopic("orders");
        subscription2.addLiteTopic("lite-orders");
        subscription2.setUpdateTime(7L);
        cases.add(liteIdentityCase("LiteSubscription", subscription1, subscription2));

        org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody body1 =
            new org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody();
        body1.setSubscriptionSet(java.util.Set.of(dto1));
        org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody body2 =
            new org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody();
        body2.setSubscriptionSet(java.util.Set.of(dto2));
        cases.add(liteIdentityCase("LiteSubscriptionCtlRequestBody", body1, body2));
        return cases;
    }

    private static Map<String, Object> liteIdentityCase(String className, Object left, Object right) {
        Map<String, Object> item = new TreeMap<>();
        item.put("class", className);
        item.put("sameObjectEquals", left.equals(left));
        item.put("equalFieldDistinctObjectsEquals", left.equals(right));
        Set<Object> values = new HashSet<>();
        values.add(left);
        values.add(right);
        item.put("equalFieldHashSetSize", values.size());
        return item;
    }

    private static List<Object> liteSubscriptionApiCases() {
        List<Object> cases = new ArrayList<>();
        org.apache.rocketmq.common.lite.LiteSubscription subscription =
            new org.apache.rocketmq.common.lite.LiteSubscription();
        Map<String, Object> item = new TreeMap<>();
        item.put("groupSetterReturnsThis", subscription.setGroup("workers") == subscription);
        item.put("topicSetterReturnsThis", subscription.setTopic("orders") == subscription);
        subscription.setUpdateTime(7L);
        item.put("initialTime", subscription.getUpdateTime());
        Set<String> setterSet = new java.util.TreeSet<>(Set.of("lite-a"));
        item.put("setLiteTopicSetReturnsThis",
            subscription.setLiteTopicSet(setterSet) == subscription);
        item.put("setLiteTopicSetRetainsInputSet", subscription.getLiteTopicSet() == setterSet);
        item.put("setLiteTopicSetKeepsExisting", subscription.getLiteTopicSet().contains("lite-a"));
        item.put("setLiteTopicSetLeavesUpdateTime", subscription.getUpdateTime() == 7L);
        setterSet.add("setter-only");
        item.put("setterInputMutationVisible", subscription.getLiteTopicSet().contains("setter-only"));

        Set<String> liveSet = subscription.getLiteTopicSet();
        item.put("getterReturnsSameSetObject", liveSet == subscription.getLiteTopicSet());
        item.put("singleAddResult", subscription.addLiteTopic("lite-b"));
        item.put("duplicateAddResult", subscription.addLiteTopic("lite-b"));
        item.put("singleAddUpdatesTime", subscription.getUpdateTime() > 7L);
        subscription.addLiteTopic(java.util.Arrays.asList("lite-c", "lite-d"));
        item.put("collectionAddUpdatesTime", subscription.getUpdateTime() > 7L);
        item.put("getterReturnsLiveSet", liveSet.contains("lite-c"));
        item.put("singleRemoveResult", subscription.removeLiteTopic("lite-b"));
        subscription.removeLiteTopic(java.util.Arrays.asList("lite-c", "lite-d"));
        item.put("collectionRemoveClearsMembers",
            !subscription.getLiteTopicSet().contains("lite-c")
                && !subscription.getLiteTopicSet().contains("lite-d"));
        liveSet.add("getter-added");
        item.put("getterMutationVisibleToSubscription",
            subscription.getLiteTopicSet().contains("getter-added"));
        liveSet.remove("getter-added");

        long beforeSetLiteTopicSet = subscription.getUpdateTime();
        subscription.setLiteTopicSet(Set.of("lite-e"));
        item.put("setLiteTopicSetAddsWithoutClearing",
            subscription.getLiteTopicSet().contains("lite-a")
                && subscription.getLiteTopicSet().contains("lite-e"));
        item.put("setLiteTopicSetDoesNotUpdateTime",
            subscription.getUpdateTime() == beforeSetLiteTopicSet);
        subscription.removeLiteTopic("lite-e");
        subscription.setUpdateTime(7L);
        item.put("toString", subscription.toString());
        cases.add(item);
        return cases;
    }

    private static List<Object> liteSubscriptionInputCases() {
        List<Object> cases = new ArrayList<>();
        String[][] inputs = {
            {"missing-set", "{}"},
            {"null-set", "{\"liteTopicSet\":null}"},
            {"empty-array", "{\"liteTopicSet\":[]}"},
            {"string-singleton", "{\"liteTopicSet\":\"topic\"}"},
            {"empty-string", "{\"liteTopicSet\":\"\"}"},
            {"object-singleton", "{\"liteTopicSet\":{}}"},
            {"array-string-coercion", "{\"liteTopicSet\":[\"x\",1,true]}"},
            {"duplicate-set-members", "{\"liteTopicSet\":[\"x\",\"x\"]}"},
            {"null-member", "{\"liteTopicSet\":[null]}"},
            {"number-scalar", "{\"liteTopicSet\":7}"},
            {"boolean-scalar", "{\"liteTopicSet\":true}"},
            {"field-coercion", "{\"group\":7,\"topic\":true,\"updateTime\":\"9\",\"liteTopicSet\":\"topic\"}"},
            {"null-update-time", "{\"updateTime\":null}"},
            {"duplicate-fields", "{\"group\":\"first\",\"group\":\"last\",\"updateTime\":1,\"updateTime\":2}"},
            {"duplicate-set-adds", "{\"liteTopicSet\":[\"a\"],\"liteTopicSet\":[\"b\"]}"},
            {"unknown-client-id", "{\"clientId\":\"extra\"}"}
        };
        for (String[] input : inputs) {
            Map<String, Object> item = new TreeMap<>();
            item.put("name", input[0]);
            item.put("rawInput", input[1]);
            try {
                org.apache.rocketmq.common.lite.LiteSubscription decoded =
                    com.alibaba.fastjson2.JSON.parseObject(input[1],
                        org.apache.rocketmq.common.lite.LiteSubscription.class);
                item.put("outcome", "ACCEPTED");
                item.put("group", decoded.getGroup());
                item.put("topic", decoded.getTopic());
                item.put("liteTopicSet", new java.util.TreeSet<>(decoded.getLiteTopicSet()));
                if (input[1].contains("\"updateTime\"")) {
                    item.put("updateTime", decoded.getUpdateTime());
                } else {
                    item.put("updateTimeWasInitialized", decoded.getUpdateTime() > 0L);
                }
            } catch (RuntimeException error) {
                item.put("outcome", "REJECTED");
                item.put("error", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> liteSubscriptionDtoApiCase() {
        org.apache.rocketmq.common.lite.LiteSubscriptionDTO dto =
            new org.apache.rocketmq.common.lite.LiteSubscriptionDTO();
        Map<String, Object> item = new TreeMap<>();
        item.put("actionSetterReturnsThis",
            dto.setAction(org.apache.rocketmq.common.lite.LiteSubscriptionAction.COMPLETE_ADD) == dto);
        item.put("clientIdSetterReturnsThis", dto.setClientId("client") == dto);
        item.put("groupSetterReturnsThis", dto.setGroup("workers") == dto);
        item.put("topicSetterReturnsThis", dto.setTopic("orders") == dto);
        Set<String> topics = new java.util.TreeSet<>(Set.of("lite-a"));
        item.put("liteTopicSetSetterReturnsThis", dto.setLiteTopicSet(topics) == dto);
        item.put("liteTopicSetGetterReturnsSameSet", dto.getLiteTopicSet() == topics);
        item.put("liteTopicSet", new ArrayList<>(dto.getLiteTopicSet()));
        org.apache.rocketmq.common.lite.OffsetOption option =
            new org.apache.rocketmq.common.lite.OffsetOption(
                org.apache.rocketmq.common.lite.OffsetOption.Type.POLICY, 11L);
        dto.setOffsetOption(option);
        item.put("offsetOptionGetterReturnsSameObject", dto.getOffsetOption() == option);
        item.put("versionSetterReturnsThis", dto.setVersion(9L) == dto);
        item.put("action", dto.getAction().name());
        item.put("clientId", dto.getClientId());
        item.put("group", dto.getGroup());
        item.put("topic", dto.getTopic());
        item.put("offsetOptionType", dto.getOffsetOption().getType().name());
        item.put("offsetOptionValue", dto.getOffsetOption().getValue());
        item.put("version", dto.getVersion());
        topics.add("setter-added");
        item.put("liteTopicSetExternalMutationVisible",
            dto.getLiteTopicSet().contains("setter-added"));
        dto.getLiteTopicSet().add("getter-added");
        item.put("liteTopicSetGetterMutationVisibleToAssignedSet",
            topics.contains("getter-added"));
        item.put("mutatedLiteTopicSet", new ArrayList<>(dto.getLiteTopicSet()));
        option.setValue(12L);
        item.put("offsetOptionExternalMutationVisible", dto.getOffsetOption().getValue() == 12L);
        dto.getOffsetOption().setValue(13L);
        item.put("offsetOptionGetterMutationVisibleToAssignedObject", option.getValue() == 13L);
        item.put("mutatedOffsetOptionValue", dto.getOffsetOption().getValue());
        item.put("toString", dto.toString());
        return item;
    }

    private static Map<String, Object> offsetOptionApiCase() {
        org.apache.rocketmq.common.lite.OffsetOption option =
            new org.apache.rocketmq.common.lite.OffsetOption();
        Map<String, Object> item = new TreeMap<>();
        item.put("defaultType", option.getType() == null ? null : option.getType().name());
        item.put("defaultValue", option.getValue());
        item.put("policyLastValue", org.apache.rocketmq.common.lite.OffsetOption.POLICY_LAST_VALUE);
        item.put("policyMinValue", org.apache.rocketmq.common.lite.OffsetOption.POLICY_MIN_VALUE);
        item.put("policyMaxValue", org.apache.rocketmq.common.lite.OffsetOption.POLICY_MAX_VALUE);
        option.setType(org.apache.rocketmq.common.lite.OffsetOption.Type.TAIL_N);
        option.setValue(23L);
        item.put("type", option.getType().name());
        item.put("value", option.getValue());
        item.put("toString", option.toString());
        org.apache.rocketmq.common.lite.OffsetOption equalOption =
            new org.apache.rocketmq.common.lite.OffsetOption(
                org.apache.rocketmq.common.lite.OffsetOption.Type.TAIL_N, 23L);
        org.apache.rocketmq.common.lite.OffsetOption differentValueOption =
            new org.apache.rocketmq.common.lite.OffsetOption(
                org.apache.rocketmq.common.lite.OffsetOption.Type.TAIL_N, 24L);
        org.apache.rocketmq.common.lite.OffsetOption differentTypeOption =
            new org.apache.rocketmq.common.lite.OffsetOption(
                org.apache.rocketmq.common.lite.OffsetOption.Type.POLICY, 23L);
        item.put("equalValue", option.equals(equalOption));
        item.put("differentValue", option.equals(differentValueOption));
        item.put("differentType", option.equals(differentTypeOption));
        item.put("nullEqualsFalse", !option.equals(null));
        item.put("wrongTypeEqualsFalse", !option.equals("not an OffsetOption"));
        int optionHash = option.hashCode();
        item.put("hashCodeStableOnSameObject", optionHash == option.hashCode());
        item.put("equalInstancesHaveSameHashCode", optionHash == equalOption.hashCode());
        Set<org.apache.rocketmq.common.lite.OffsetOption> equalOptions = new HashSet<>();
        equalOptions.add(option);
        equalOptions.add(equalOption);
        item.put("equalHashSetSize", equalOptions.size());
        return item;
    }

    private static Map<String, Object> liteSubscriptionControlApiCase() {
        org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody body =
            new org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody();
        Map<String, Object> item = new TreeMap<>();
        item.put("defaultSubscriptionSetIsNull", body.getSubscriptionSet() == null);
        Set<org.apache.rocketmq.common.lite.LiteSubscriptionDTO> subscriptions =
            new java.util.LinkedHashSet<>();
        org.apache.rocketmq.common.lite.LiteSubscriptionDTO worker =
            new org.apache.rocketmq.common.lite.LiteSubscriptionDTO().setGroup("workers");
        subscriptions.add(worker);
        body.setSubscriptionSet(subscriptions);
        item.put("setterStoresSameSet", body.getSubscriptionSet() == subscriptions);
        item.put("setterElementIdentity", body.getSubscriptionSet().iterator().next() == worker);
        item.put("size", body.getSubscriptionSet().size());
        org.apache.rocketmq.common.lite.LiteSubscriptionDTO consumer =
            new org.apache.rocketmq.common.lite.LiteSubscriptionDTO().setGroup("consumers");
        subscriptions.add(consumer);
        item.put("externalMutationVisible", body.getSubscriptionSet().size() == 2);
        item.put("externalElementIdentity", body.getSubscriptionSet().contains(consumer));
        org.apache.rocketmq.common.lite.LiteSubscriptionDTO admin =
            new org.apache.rocketmq.common.lite.LiteSubscriptionDTO().setGroup("admins");
        body.getSubscriptionSet().add(admin);
        item.put("getterMutationVisibleToAssignedSet", subscriptions.size() == 3);
        item.put("getterElementIdentity", subscriptions.contains(admin));
        item.put("mutatedSize", body.getSubscriptionSet().size());
        return item;
    }

    private static List<Object> liteSubscriptionRootCases() {
        List<Object> cases = new ArrayList<>();
        String[][] roots = {
            {"null", "null"}, {"empty-object", "{}"}, {"empty-array", "[]"},
            {"string", "\"root\""}, {"integer", "7"}, {"fraction", "1.5"},
            {"boolean", "true"}
        };
        for (String[] root : roots) {
            String input = root[1];
            Map<String, Object> item = new TreeMap<>();
            item.put("name", "root-" + root[0]);
            item.put("rawInput", input);
            try {
                org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody decoded =
                    com.alibaba.fastjson2.JSON.parseObject(input,
                        org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody.class);
                decoded.getSubscriptionSet();
                item.put("outcome", "ACCEPTED");
                item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                    com.alibaba.fastjson2.JSON.toJSONString(decoded)));
            } catch (RuntimeException error) {
                item.put("outcome", "REJECTED");
                item.put("error", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> liteSubscriptionControlFieldCases() {
        List<Object> cases = new ArrayList<>();
        String[][] shapes = {
            {"null", "null"}, {"numeric-string", "\"7\""}, {"integer", "0"},
            {"fraction", "1.5"}, {"boolean", "true"}, {"array", "[]"}, {"object", "{}"}
        };
        for (String[] shape : shapes) {
            String input = "{\"subscriptionSet\":" + shape[1] + "}";
            cases.add(liteSubscriptionEnumCase("shape-subscriptionSet-" + shape[0], input));
        }
        return cases;
    }

    private static List<Object> liteSubscriptionElementCases() {
        List<Object> cases = new ArrayList<>();
        String[][] elements = {
            {"null", "null"}, {"string", "\"topic\""}, {"integer", "7"},
            {"fraction", "1.5"}, {"boolean", "true"}, {"array", "[]"}, {"object", "{}"}
        };
        for (String[] element : elements) {
            String input = "{\"subscriptionSet\":[{\"liteTopicSet\":[" + element[1] + "]}]}";
            cases.add(liteSubscriptionEnumCase("element-liteTopicSet-" + element[0], input));
        }
        return cases;
    }

    private static List<Object> liteSubscriptionOffsetValueCases() {
        List<Object> cases = new ArrayList<>();
        String[][] values = {
            {"null", "null"}, {"numeric-string", "\"7\""}, {"integer", "0"},
            {"fraction", "1.5"}, {"boolean", "true"}, {"array", "[]"}, {"object", "{}"}
        };
        for (String[] value : values) {
            String input = "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":\"POLICY\",\"value\":"
                + value[1] + "}}]}";
            cases.add(liteSubscriptionEnumCase("shape-offset-value-" + value[0], input));
        }
        return cases;
    }

    private static List<Object> liteSubscriptionSetCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(liteSubscriptionEnumCase("subscription-set-duplicate-null-members",
            "{\"subscriptionSet\":[null,null]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-distinct-equal-dtos",
            "{\"subscriptionSet\":[{\"topic\":\"same\"},{\"topic\":\"same\"}]}"));
        cases.add(liteSubscriptionEnumCase("lite-topic-set-duplicate-members",
            "{\"subscriptionSet\":[{\"liteTopicSet\":[\"b\",\"a\",\"b\",null,null]}]}"));
        cases.add(liteSubscriptionEnumCase("lite-topic-set-empty-string",
            "{\"subscriptionSet\":[{\"liteTopicSet\":\"\"}]}"));
        return cases;
    }

    private static Map<String, Object> liteSubscriptionEnumCase(String name, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("rawInput", rawInput);
        try {
            org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody decoded =
                com.alibaba.fastjson2.JSON.parseObject(rawInput,
                    org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody.class);
            item.put("outcome", "ACCEPTED");
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(decoded)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> queryAssignmentEnumCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(queryAssignmentEnumCase("name", "{\"messageModel\":\"CLUSTERING\"}"));
        cases.add(queryAssignmentEnumCase("ordinal", "{\"messageModel\":1}"));
        cases.add(queryAssignmentEnumCase("numeric-string", "{\"messageModel\":\"1\"}"));
        cases.add(queryAssignmentEnumCase("fractional", "{\"messageModel\":2.9}"));
        cases.add(queryAssignmentEnumCase("unknown-name", "{\"messageModel\":\"UNKNOWN\"}"));
        cases.add(queryAssignmentEnumCase("unknown-numeric-string", "{\"messageModel\":\"3\"}"));
        cases.add(queryAssignmentEnumCase("out-of-range", "{\"messageModel\":3}"));
        cases.add(queryAssignmentEnumCase("explicit-null", "{\"messageModel\":null}"));
        cases.add(queryAssignmentEnumCase("duplicate-name-then-ordinal",
            "{\"messageModel\":\"BROADCASTING\",\"messageModel\":1}"));
        cases.add(queryAssignmentEnumCase("duplicate-ordinal-then-name",
            "{\"messageModel\":1,\"messageModel\":\"BROADCASTING\"}"));
        return cases;
    }

    private static Map<String, Object> queryAssignmentEnumCase(String name, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("rawInput", rawInput);
        try {
            org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody decoded =
                com.alibaba.fastjson2.JSON.parseObject(rawInput,
                    org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody.class);
            item.put("outcome", "ACCEPTED");
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(decoded)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> setRequestModeCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(setRequestModeCase("missing", "{}"));
        cases.add(setRequestModeCase("name", "{\"mode\":\"POP\"}"));
        cases.add(setRequestModeCase("ordinal", "{\"mode\":1}"));
        cases.add(setRequestModeCase("numeric-string", "{\"mode\":\"1\"}"));
        cases.add(setRequestModeCase("fractional", "{\"mode\":1.9}"));
        cases.add(setRequestModeCase("unknown-name", "{\"mode\":\"UNKNOWN\"}"));
        cases.add(setRequestModeCase("unknown-numeric-string", "{\"mode\":\"2\"}"));
        cases.add(setRequestModeCase("out-of-range", "{\"mode\":2}"));
        cases.add(setRequestModeCase("explicit-null", "{\"mode\":null}"));
        cases.add(setRequestModeCase("boolean", "{\"mode\":true}"));
        cases.add(setRequestModeCase("primitive-default", "{\"topic\":\"orders\"}"));
        cases.add(setRequestModeCase("decimal-wrap", "{\"mode\":4294967296.0}"));
        cases.add(setRequestModeCase("duplicate-name-then-ordinal",
            "{\"mode\":\"PULL\",\"mode\":1}"));
        cases.add(setRequestModeCase("duplicate-ordinal-then-name",
            "{\"mode\":1,\"mode\":\"PULL\"}"));
        return cases;
    }

    private static Map<String, Object> setRequestModeCase(String name, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("rawInput", rawInput);
        try {
            org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody decoded =
                com.alibaba.fastjson2.JSON.parseObject(rawInput,
                    org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody.class);
            item.put("outcome", "ACCEPTED");
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(decoded)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> queryAssignmentFieldCases() {
        String[][] fields = {
            {"topic", "string"}, {"consumerGroup", "string"}, {"clientId", "string"},
            {"strategyName", "string"}, {"messageModel", "enum"}
        };
        return requestBodyFieldCases(
            "query-assignment", org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody.class,
            fields);
    }

    private static List<Object> setRequestModeFieldCases() {
        String[][] fields = {
            {"topic", "string"}, {"consumerGroup", "string"}, {"mode", "enum"},
            {"popShareQueueNum", "integer"}
        };
        return requestBodyFieldCases(
            "set-request-mode", org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody.class,
            fields);
    }

    private static List<Object> requestBodyFieldCases(String prefix, Class<?> type, String[][] fields) {
        String[][] shapes = {
            {"null", "null"}, {"numeric-string", "\"7\""}, {"integer", "7"},
            {"fraction", "1.5"}, {"boolean", "true"}, {"array", "[\"x\",1]"},
            {"object", "{\"b\":2,\"a\":1}"}
        };
        List<Object> cases = new ArrayList<>();
        for (String[] field : fields) {
            for (String[] shape : shapes) {
                cases.add(requestBodyFieldCase(prefix + "-" + field[0] + "-" + shape[0],
                    "{\"" + field[0] + "\":" + shape[1] + "}", type));
            }
        }
        return cases;
    }

    private static List<Object> requestBodyRootCases(String prefix, Class<?> type) {
        String[][] roots = {
            {"empty-object", "{}"}, {"null", "null"}, {"empty-array", "[]"},
            {"singleton-array", "[{}]"}, {"number", "7"}, {"boolean", "true"},
            {"string", "\"text\""}
        };
        List<Object> cases = new ArrayList<>();
        for (String[] root : roots) {
            cases.add(requestBodyFieldCase(prefix + "-root-" + root[0], root[1], type));
        }
        return cases;
    }

    private static List<Object> queryAssignmentRootCases() {
        return requestBodyRootCases(
            "query-assignment", org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody.class);
    }

    private static List<Object> setRequestModeRootCases() {
        return requestBodyRootCases(
            "set-request-mode", org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody.class);
    }

    private static Map<String, Object> requestBodyFieldCase(String name, String rawInput, Class<?> type) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("rawInput", rawInput);
        try {
            Object decoded = com.alibaba.fastjson2.JSON.parseObject(rawInput, type);
            item.put("outcome", "ACCEPTED");
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(decoded)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> consumeDirectResultCases() {
        List<Object> cases = new ArrayList<>();
        for (org.apache.rocketmq.remoting.protocol.body.CMResult value :
            org.apache.rocketmq.remoting.protocol.body.CMResult.values()) {
            cases.add(consumeDirectResultCase("name-" + value.name(),
                "{\"consumeResult\":\"" + value.name() + "\"}"));
        }
        for (int ordinal = 0;
            ordinal <= org.apache.rocketmq.remoting.protocol.body.CMResult.values().length;
            ordinal++) {
            cases.add(consumeDirectResultCase("ordinal-" + ordinal,
                "{\"consumeResult\":" + ordinal + "}"));
        }
        cases.add(consumeDirectResultCase("numeric-string", "{\"consumeResult\":\"1\"}"));
        cases.add(consumeDirectResultCase("fractional", "{\"consumeResult\":1.9}"));
        cases.add(consumeDirectResultCase("unknown-name", "{\"consumeResult\":\"UNKNOWN\"}"));
        cases.add(consumeDirectResultCase("unknown-numeric-string", "{\"consumeResult\":\"6\"}"));
        cases.add(consumeDirectResultCase("explicit-null", "{\"consumeResult\":null}"));
        cases.add(consumeDirectResultCase("boolean", "{\"consumeResult\":true}"));
        String[] fields = {"order", "autoCommit", "consumeResult", "remark", "spentTimeMills"};
        String[] tokens = {"null", "\"text\"", "2", "1.9", "true", "[]", "{}"};
        for (String field : fields) {
            for (String token : tokens) {
                cases.add(consumeDirectResultCase("shape-" + field + "-" + tokenName(token),
                    "{\"" + field + "\":" + token + "}"));
            }
        }
        cases.add(consumeDirectResultCase("root-null", "null"));
        cases.add(consumeDirectResultCase("root-empty-object", "{}"));
        cases.add(consumeDirectResultCase("root-empty-array", "[]"));
        cases.add(consumeDirectResultCase("root-string", "\"root\""));
        cases.add(consumeDirectResultCase("root-integer", "7"));
        cases.add(consumeDirectResultCase("root-fraction", "1.5"));
        cases.add(consumeDirectResultCase("root-boolean", "true"));
        cases.add(consumeDirectResultCase("duplicate-order-last-wins",
            "{\"order\":false,\"order\":true}"));
        cases.add(consumeDirectResultCase("duplicate-auto-commit-last-wins",
            "{\"autoCommit\":true,\"autoCommit\":false}"));
        cases.add(consumeDirectResultCase("duplicate-consume-result-last-wins",
            "{\"consumeResult\":\"CR_SUCCESS\",\"consumeResult\":1.9}"));
        cases.add(consumeDirectResultCase("duplicate-remark-last-wins",
            "{\"remark\":\"first\",\"remark\":42}"));
        cases.add(consumeDirectResultCase("duplicate-spent-time-last-wins",
            "{\"spentTimeMills\":9,\"spentTimeMills\":1.9}"));
        return cases;
    }

    private static List<Object> messageRequestModeWrapperCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(messageRequestModeWrapperCase("default-empty-map", "{}"));
        cases.add(messageRequestModeWrapperCase("explicit-null-map", "{\"messageRequestModeMap\":null}"));
        String[] tokens = {"null", "\"text\"", "2", "1.5", "true", "[]", "{}"};
        String[] names = {"null", "string", "integer", "fraction", "boolean", "array", "object"};
        for (int index = 0; index < tokens.length; index++) {
            cases.add(messageRequestModeWrapperCase("field-" + names[index],
                "{\"messageRequestModeMap\":" + tokens[index] + "}"));
            cases.add(messageRequestModeWrapperCase("topic-value-" + names[index],
                "{\"messageRequestModeMap\":{\"orders\":" + tokens[index] + "}}"));
            cases.add(messageRequestModeWrapperCase("group-value-" + names[index],
                "{\"messageRequestModeMap\":{\"orders\":{\"workers\":" + tokens[index] + "}}}"));
        }
        String[][] roots = {
            {"null", "null"}, {"empty-object", "{}"}, {"empty-array", "[]"},
            {"string", "\"root\""}, {"integer", "7"}, {"fraction", "1.5"},
            {"boolean", "true"}
        };
        for (String[] root : roots) {
            cases.add(messageRequestModeWrapperCase("root-" + root[0], root[1]));
        }
        cases.add(messageRequestModeWrapperCase("duplicate-root-last-wins",
            "{\"messageRequestModeMap\":{\"old\":{}},\"messageRequestModeMap\":{\"new\":{}}}"));
        cases.add(messageRequestModeWrapperCase("duplicate-topic-last-wins",
            "{\"messageRequestModeMap\":{\"orders\":{},\"orders\":{\"workers\":{}}}}"));
        cases.add(messageRequestModeWrapperCase("duplicate-group-last-wins",
            "{\"messageRequestModeMap\":{\"orders\":{\"workers\":{},\"workers\":{\"group-2\":{}}}}}"));
        return cases;
    }

    private static Map<String, Object> messageRequestModeWrapperCase(String name, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("rawInput", rawInput);
        try {
            org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper decoded =
                com.alibaba.fastjson2.JSON.parseObject(rawInput,
                    org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper.class);
            item.put("outcome", "ACCEPTED");
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(decoded)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static Map<String, Object> messageRequestModeWrapperApiCase() throws Exception {
        org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper wrapper =
            new org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper();
        Map<String, Object> item = new TreeMap<>();
        recordBodyApiInventory(item, wrapper.getClass());
        item.put("equalsDeclaringClass", wrapper.getClass().getMethod("equals", Object.class)
            .getDeclaringClass().getName());
        item.put("hashCodeDeclaringClass", wrapper.getClass().getMethod("hashCode")
            .getDeclaringClass().getName());
        item.put("sameInstanceEquals", wrapper.equals(wrapper));
        item.put("equalValuesDistinctInstanceEquals",
            wrapper.equals(new org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper()));
        item.put("defaultMapNonNull", wrapper.getMessageRequestModeMap() != null);
        item.put("defaultMapSize", wrapper.getMessageRequestModeMap().size());
        wrapper.setMessageRequestModeMap(null);
        item.put("nullSetterRemainsNull", wrapper.getMessageRequestModeMap() == null);

        org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody body =
            new org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody();
        body.setTopic("orders");
        body.setConsumerGroup("workers");
        body.setMode(org.apache.rocketmq.common.message.MessageRequestMode.POP);
        body.setPopShareQueueNum(2);
        java.util.concurrent.ConcurrentHashMap<String,
            org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody> groups =
                new java.util.concurrent.ConcurrentHashMap<>();
        groups.put("workers", body);
        java.util.concurrent.ConcurrentHashMap<String,
            java.util.concurrent.ConcurrentHashMap<String,
                org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody>> map =
                    new java.util.concurrent.ConcurrentHashMap<>();
        map.put("orders", groups);
        wrapper.setMessageRequestModeMap(map);
        item.put("setterRetainsMapIdentity", wrapper.getMessageRequestModeMap() == map);
        Map<String, java.util.concurrent.ConcurrentHashMap<String,
            org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody>> getterMap =
                wrapper.getMessageRequestModeMap();
        item.put("setterRetainsGroupMapIdentity", getterMap.get("orders") == groups);
        item.put("getterReturnsAssignedBody", getterMap.get("orders").get("workers") == body);
        map.put("caller-topic", new java.util.concurrent.ConcurrentHashMap<>());
        item.put("callerOuterMutationVisible", getterMap.containsKey("caller-topic"));
        map.remove("caller-topic");
        groups.put("caller-group", body);
        item.put("callerGroupMutationVisible", getterMap.get("orders").containsKey("caller-group"));
        groups.remove("caller-group");
        body.setMode(org.apache.rocketmq.common.message.MessageRequestMode.PULL);
        item.put("callerBodyMutationVisible", getterMap.get("orders").get("workers").getMode()
            == org.apache.rocketmq.common.message.MessageRequestMode.PULL);
        body.setMode(org.apache.rocketmq.common.message.MessageRequestMode.POP);
        getterMap.get("orders").get("workers")
            .setMode(org.apache.rocketmq.common.message.MessageRequestMode.PULL);
        item.put("getterBodyMutationVisibleToSetterObject",
            body.getMode() == org.apache.rocketmq.common.message.MessageRequestMode.PULL);
        body.setMode(org.apache.rocketmq.common.message.MessageRequestMode.POP);
        item.put("populatedMap", com.alibaba.fastjson2.JSON.parseObject(
            com.alibaba.fastjson2.JSON.toJSONString(wrapper.getMessageRequestModeMap())));
        getterMap.put("archive", new java.util.concurrent.ConcurrentHashMap<>());
        getterMap.get("orders").put("readers", body);
        item.put("getterMutationVisible", wrapper.getMessageRequestModeMap().get("archive")
            == getterMap.get("archive")
            && wrapper.getMessageRequestModeMap().get("orders").get("readers") == body);
        item.put("getterMutatedMap", wrapper.getMessageRequestModeMap());
        return item;
    }

    private static Map<String, Object> queryAssignmentRequestApiCase() {
        org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody body =
            new org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody();
        Map<String, Object> item = new TreeMap<>();
        recordBodyApiInventory(item, body.getClass());
        Map<String, Object> defaults = new TreeMap<>();
        defaults.put("topic", body.getTopic());
        defaults.put("consumerGroup", body.getConsumerGroup());
        defaults.put("clientId", body.getClientId());
        defaults.put("strategyName", body.getStrategyName());
        defaults.put("messageModel", body.getMessageModel() == null ? null : body.getMessageModel().name());
        item.put("defaults", defaults);

        body.setTopic("orders");
        body.setConsumerGroup("workers");
        body.setClientId("client-a");
        body.setStrategyName("AllocateByCircle");
        body.setMessageModel(org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.CLUSTERING);
        Map<String, Object> values = new TreeMap<>();
        values.put("topic", body.getTopic());
        values.put("consumerGroup", body.getConsumerGroup());
        values.put("clientId", body.getClientId());
        values.put("strategyName", body.getStrategyName());
        values.put("messageModel", body.getMessageModel().name());
        item.put("values", values);

        body.setTopic(null);
        body.setConsumerGroup(null);
        body.setClientId(null);
        body.setStrategyName(null);
        body.setMessageModel(null);
        item.put("nullSettersRemainNull", body.getTopic() == null && body.getConsumerGroup() == null
            && body.getClientId() == null && body.getStrategyName() == null && body.getMessageModel() == null);
        return item;
    }

    private static Map<String, Object> queryAssignmentNullInputsApiCase() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("query-assignment-null-inputs-api");
        Map<String, Object> result = new TreeMap<>();
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient) java.lang.reflect.Proxy.newProxyInstance(
                    RuntimeLookupFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            result.put("address", values[0]);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            result.put("requestCode", request.getCode());
                            result.put("body", com.alibaba.fastjson2.JSON.parseObject(
                                new String(request.getBody(), java.nio.charset.StandardCharsets.UTF_8)));
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                                    org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null);
                            response.setBody("{\"messageQueueAssignments\":[]}"
                                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        try {
            result.put("assignmentCount", api.queryAssignment("broker:10911", "topic", null, "client",
                null, null, 1000L).size());
        } finally {
            api.shutdown();
        }
        return result;
    }

    private static Map<String, Object> queryAssignmentEmptyInputsApiCase() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("query-assignment-empty-inputs-api");
        Map<String, Object> result = new TreeMap<>();
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient) java.lang.reflect.Proxy.newProxyInstance(
                    RuntimeLookupFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            result.put("address", values[0]);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            result.put("requestCode", request.getCode());
                            result.put("body", com.alibaba.fastjson2.JSON.parseObject(
                                new String(request.getBody(), java.nio.charset.StandardCharsets.UTF_8)));
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                                    org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null);
                            response.setBody("{\"messageQueueAssignments\":[]}"
                                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        try {
            result.put("assignmentCount", api.queryAssignment("broker:10911", "topic", "", "client",
                "", org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.BROADCASTING, 1000L).size());
        } finally {
            api.shutdown();
        }
        return result;
    }

    private static Map<String, Object> queryAssignmentResponseApiCase() {
        org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody body =
            new org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody();
        Map<String, Object> item = new TreeMap<>();
        recordBodyApiInventory(item, body.getClass());
        try {
            item.put("equalsDeclaringClass", body.getClass().getMethod("equals", Object.class)
                .getDeclaringClass().getName());
            item.put("hashCodeDeclaringClass", body.getClass().getMethod("hashCode")
                .getDeclaringClass().getName());
        } catch (NoSuchMethodException error) {
            throw new IllegalStateException(error);
        }
        item.put("sameInstanceEquals", body.equals(body));
        item.put("equalValuesDistinctInstanceEquals",
            body.equals(new org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody()));
        item.put("defaultAssignments", body.getMessageQueueAssignments());

        org.apache.rocketmq.common.message.MessageQueueAssignment assignment = messageQueueAssignment(
            2, org.apache.rocketmq.common.message.MessageRequestMode.POP);
        recordPublicApiInventory(item, "messageQueueAssignment", assignment.getClass());
        java.util.Set<org.apache.rocketmq.common.message.MessageQueueAssignment> assignments =
            new java.util.LinkedHashSet<>();
        assignments.add(assignment);
        body.setMessageQueueAssignments(assignments);
        item.put("setterRetainsSetIdentity", body.getMessageQueueAssignments() == assignments);
        org.apache.rocketmq.common.message.MessageQueueAssignment getterElement =
            body.getMessageQueueAssignments().iterator().next();
        item.put("getterReturnsAssignedElement", assignment == getterElement);
        assignment.setMode(org.apache.rocketmq.common.message.MessageRequestMode.PULL);
        item.put("setterElementMutationVisibleToGetter",
            getterElement.getMode() == org.apache.rocketmq.common.message.MessageRequestMode.PULL);
        assignment.setMode(org.apache.rocketmq.common.message.MessageRequestMode.POP);
        getterElement.setMode(org.apache.rocketmq.common.message.MessageRequestMode.PULL);
        item.put("getterElementMutationVisibleToSetterObject",
            assignment.getMode() == org.apache.rocketmq.common.message.MessageRequestMode.PULL);
        getterElement.setMode(org.apache.rocketmq.common.message.MessageRequestMode.POP);
        item.put("valueCount", body.getMessageQueueAssignments().size());
        item.put("values", messageQueueAssignmentApiValues(body.getMessageQueueAssignments()));
        item.put("assignedValues", messageQueueAssignmentApiValues(body.getMessageQueueAssignments()));

        org.apache.rocketmq.common.message.MessageQueueAssignment externalAssignment = messageQueueAssignment(
            3, org.apache.rocketmq.common.message.MessageRequestMode.PULL);
        assignments.remove(assignment);
        assignments.add(externalAssignment);
        item.put("externalMutationVisible", body.getMessageQueueAssignments().contains(externalAssignment));

        org.apache.rocketmq.common.message.MessageQueueAssignment getterAssignment = messageQueueAssignment(
            4, org.apache.rocketmq.common.message.MessageRequestMode.POP);
        body.getMessageQueueAssignments().remove(externalAssignment);
        body.getMessageQueueAssignments().add(getterAssignment);
        item.put("getterMutationVisibleToAssignedSet", assignments.contains(getterAssignment));
        item.put("mutatedValues", messageQueueAssignmentApiValues(body.getMessageQueueAssignments()));

        body.setMessageQueueAssignments(null);
        item.put("nullSetterRemainsNull", body.getMessageQueueAssignments() == null);
        return item;
    }

    private static org.apache.rocketmq.common.message.MessageQueueAssignment messageQueueAssignment(
        int queueId, org.apache.rocketmq.common.message.MessageRequestMode mode) {
        org.apache.rocketmq.common.message.MessageQueueAssignment assignment =
            new org.apache.rocketmq.common.message.MessageQueueAssignment();
        assignment.setMessageQueue(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", queueId));
        assignment.setMode(mode);
        return assignment;
    }

    private static List<Object> messageQueueAssignmentApiValues(
        Set<org.apache.rocketmq.common.message.MessageQueueAssignment> assignments) {
        List<Object> values = new ArrayList<>();
        for (org.apache.rocketmq.common.message.MessageQueueAssignment assignment : assignments) {
            Map<String, Object> value = new TreeMap<>();
            org.apache.rocketmq.common.message.MessageQueue queue = assignment.getMessageQueue();
            Map<String, Object> messageQueue = new TreeMap<>();
            messageQueue.put("topic", queue.getTopic());
            messageQueue.put("brokerName", queue.getBrokerName());
            messageQueue.put("queueId", queue.getQueueId());
            value.put("messageQueue", messageQueue);
            value.put("mode", assignment.getMode().name());
            value.put("attachments", assignment.getAttachments());
            values.add(value);
        }
        return values;
    }

    private static Map<String, Object> setRequestModeRequestApiCase() {
        org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody body =
            new org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody();
        Map<String, Object> item = new TreeMap<>();
        recordBodyApiInventory(item, body.getClass());
        Map<String, Object> defaults = new TreeMap<>();
        defaults.put("topic", body.getTopic());
        defaults.put("consumerGroup", body.getConsumerGroup());
        defaults.put("mode", body.getMode() == null ? null : body.getMode().name());
        defaults.put("popShareQueueNum", body.getPopShareQueueNum());
        item.put("defaults", defaults);

        body.setTopic("orders");
        body.setConsumerGroup("workers");
        body.setMode(org.apache.rocketmq.common.message.MessageRequestMode.POP);
        body.setPopShareQueueNum(2);
        Map<String, Object> values = new TreeMap<>();
        values.put("topic", body.getTopic());
        values.put("consumerGroup", body.getConsumerGroup());
        values.put("mode", body.getMode().name());
        values.put("popShareQueueNum", body.getPopShareQueueNum());
        item.put("values", values);

        body.setTopic(null);
        body.setConsumerGroup(null);
        body.setMode(null);
        body.setPopShareQueueNum(0);
        item.put("nullSettersRemainNull", body.getTopic() == null && body.getConsumerGroup() == null
            && body.getMode() == null);
        item.put("primitiveAfterReset", body.getPopShareQueueNum());
        return item;
    }

    private static Map<String, Object> checkClientBodyApiCase() throws Exception {
        org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody body =
            new org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody();
        Map<String, Object> item = new TreeMap<>();
        recordBodyApiInventory(item, body.getClass());
        item.put("bodyInheritedSerializableMethods", Arrays.stream(body.getClass().getMethods())
            .filter(method -> method.getDeclaringClass()
                == org.apache.rocketmq.remoting.protocol.RemotingSerializable.class)
            .map(RuntimeLookupFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        Map<String, Object> defaults = new TreeMap<>();
        defaults.put("clientId", body.getClientId());
        defaults.put("group", body.getGroup());
        defaults.put("subscriptionData", body.getSubscriptionData());
        defaults.put("namespace", body.getNamespace());
        item.put("defaults", defaults);
        item.put("equalsSameInstance", body.equals(body));
        item.put("equalsEqualFieldsDistinctInstance", body.equals(new org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody()));
        item.put("equalsDeclaringClass", body.getClass().getMethod("equals", Object.class)
            .getDeclaringClass().getName());
        item.put("hashCodeDeclaringClass", body.getClass().getMethod("hashCode")
            .getDeclaringClass().getName());

        org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData subscription =
            new org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData("orders", "*");
        subscription.setSubVersion(42L);
        subscription.setClassFilterMode(true);
        subscription.setExpressionType("TAG");
        subscription.setTagsSet(new HashSet<>(List.of("gold")));
        body.setClientId("client-a");
        body.setGroup("workers");
        body.setSubscriptionData(subscription);
        body.setNamespace("tenant");
        Map<String, Object> nested = new TreeMap<>();
        nested.put("topic", body.getSubscriptionData().getTopic());
        nested.put("subString", body.getSubscriptionData().getSubString());
        nested.put("subVersion", body.getSubscriptionData().getSubVersion());
        nested.put("classFilterMode", body.getSubscriptionData().isClassFilterMode());
        nested.put("expressionType", body.getSubscriptionData().getExpressionType());
        nested.put("tagsSet", new ArrayList<>(body.getSubscriptionData().getTagsSet()));
        Map<String, Object> values = new TreeMap<>();
        values.put("clientId", body.getClientId());
        values.put("group", body.getGroup());
        values.put("subscriptionData", nested);
        values.put("namespace", body.getNamespace());
        item.put("values", values);
        item.put("getterReturnsAssignedSubscription", body.getSubscriptionData() == subscription);

        subscription.setSubVersion(43L);
        item.put("externalMutationVisible", body.getSubscriptionData().getSubVersion() == 43L);
        body.getSubscriptionData().setSubString("orders*");
        item.put("getterMutationVisibleToAssignedSubscription",
            "orders*".equals(subscription.getSubString()));
        Map<String, Object> mutatedSubscription = new TreeMap<>();
        mutatedSubscription.put("topic", body.getSubscriptionData().getTopic());
        mutatedSubscription.put("subString", body.getSubscriptionData().getSubString());
        mutatedSubscription.put("subVersion", body.getSubscriptionData().getSubVersion());
        mutatedSubscription.put("classFilterMode", body.getSubscriptionData().isClassFilterMode());
        mutatedSubscription.put("expressionType", body.getSubscriptionData().getExpressionType());
        mutatedSubscription.put("tagsSet", new ArrayList<>(body.getSubscriptionData().getTagsSet()));
        item.put("mutatedSubscriptionData", mutatedSubscription);

        body.setClientId(null);
        body.setGroup(null);
        body.setSubscriptionData(null);
        body.setNamespace(null);
        item.put("nullSettersRemainNull", body.getClientId() == null && body.getGroup() == null
            && body.getSubscriptionData() == null && body.getNamespace() == null);
        return item;
    }

    private static List<Object> checkClientInheritedSerializationCases() {
        String[][] inputs = {
            {"default", "{}"},
            {"explicit-null", "{\"clientId\":null,\"group\":null,\"subscriptionData\":null,\"namespace\":null}"},
            {"populated", "{\"clientId\":\"client-a\",\"group\":\"workers\","
                + "\"subscriptionData\":{\"topic\":\"orders\",\"subString\":\"*\","
                + "\"subVersion\":42,\"classFilterMode\":true,\"expressionType\":\"TAG\","
                + "\"tagsSet\":[\"gold\"]},\"namespace\":\"tenant\"}"},
            {"browser-secure", "{\"clientId\":\"<client>(1)</client> & ops\","
                + "\"group\":\"workers\",\"subscriptionData\":{\"topic\":\"<orders>\","
                + "\"subString\":\"a(b)\",\"subVersion\":42,\"classFilterMode\":true,"
                + "\"expressionType\":\"TAG\",\"tagsSet\":[\"<gold>\"]},"
                + "\"namespace\":\"tenant\"}"}
        };
        List<Object> cases = new ArrayList<>();
        Class<org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody> type =
            org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody.class;
        for (String[] input : inputs) {
            byte[] raw = input[1].getBytes(java.nio.charset.StandardCharsets.UTF_8);
            org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody value =
                org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody.decode(raw, type);
            String instanceJson = value.toJson();
            byte[] instanceBytes = value.encode();
            List<org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody> decodedList =
                org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody.decodeList(
                    com.alibaba.fastjson2.JSON.toJSONBytes(java.util.Collections.singletonList(value)), type);
            Map<String, Object> item = new TreeMap<>();
            item.put("id", input[0]);
            item.put("rawInput", input[1]);
            item.put("instanceJson", instanceJson);
            item.put("instancePrettyJson", value.toJson(true));
            item.put("staticJson", org.apache.rocketmq.remoting.protocol.RemotingSerializable.toJson(value, false));
            item.put("staticPrettyJson", org.apache.rocketmq.remoting.protocol.RemotingSerializable.toJson(value, true));
            item.put("instanceEncodedJson", new String(instanceBytes, java.nio.charset.StandardCharsets.UTF_8));
            item.put("staticEncodedJson", new String(
                org.apache.rocketmq.remoting.protocol.RemotingSerializable.encode(value),
                java.nio.charset.StandardCharsets.UTF_8));
            item.put("featureEncodedJson", new String(value.encode(
                new com.alibaba.fastjson2.JSONWriter.Feature[0]), java.nio.charset.StandardCharsets.UTF_8));
            item.put("writerFeatureOverrides", writerFeatureOverrides(value));
            item.put("decodedJson", org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody.decode(
                instanceBytes, type).toJson());
            item.put("fromJsonJson", org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody.fromJson(
                instanceJson, type).toJson());
            item.put("decodedListJson", decodedList.get(0).toJson());
            item.put("decodedListCount", decodedList.size());
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> checkClientInheritedSerializationNullUtilities() {
        Class<org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody> type =
            org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody.class;
        byte[] jsonNull = "null".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Map<String, Object> item = new TreeMap<>();
        item.put("staticEncodeNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.encode((Object) null) == null);
        item.put("staticToJsonNull", org.apache.rocketmq.remoting.protocol.RemotingSerializable.toJson(null, false));
        item.put("decodeNullBytesReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(null, type) == null);
        item.put("decodeJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(jsonNull, type) == null);
        item.put("fromJsonNullInputReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.fromJson(null, type) == null);
        item.put("fromJsonJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.fromJson("null", type) == null);
        item.put("decodeListNullBytesReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(null, type) == null);
        item.put("decodeListJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(jsonNull, type) == null);
        List<org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody> nullMemberList =
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(
                "[null,{}]".getBytes(java.nio.charset.StandardCharsets.UTF_8), type);
        item.put("decodeListNullMemberCount", nullMemberList.size());
        item.put("decodeListPreservesNullMember", nullMemberList.get(0) == null);
        return item;
    }

    private static Map<String, Object> remotingSerializableBodyCases() {
        Map<String, Object> result = new TreeMap<>();
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody.class,
            new String[][] { {"default", "{}"}, {"populated", "{\"subscriptionSet\":[{"
                + "\"group\":\"workers\",\"topic\":\"orders\","
                + "\"liteTopicSet\":[\"blue\"]}]}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody.class,
            new String[][] { {"default", "{}"}, {"populated", "{\"topic\":\"orders\","
                + "\"consumerGroup\":\"workers\",\"clientId\":\"client-a\","
                + "\"strategyName\":\"Allocate\",\"messageModel\":\"CLUSTERING\"}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.class,
            new String[][] { {"default", "{}"}, {"populated", "{\"messageQueueAssignments\":[{"
                + "\"messageQueue\":{\"topic\":\"orders\",\"brokerName\":\"broker-a\","
                + "\"queueId\":1},\"mode\":\"POP\"}]}"},
                {"eof-trailing-text", "{}\u001atrailing"},
                {"eof-trailing-json", "{}\u001a{\"ignored\":true}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody.class,
            new String[][] { {"default", "{}"}, {"populated", "{\"consumeTimeSpanSet\":[{"
                + "\"messageQueue\":{\"topic\":\"orders\",\"brokerName\":\"broker-a\","
                + "\"queueId\":1},\"minTimeStamp\":10,\"maxTimeStamp\":20,"
                + "\"consumeTimeStamp\":15,\"delayTime\":5}]}"},
                {"eof-trailing-text", "{}\u001atrailing"},
                {"eof-trailing-json", "{}\u001a{\"ignored\":true}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody.class,
            new String[][] { {"default", "{}"}, {"populated", "{\"topic\":\"orders\","
                + "\"consumerGroup\":\"workers\",\"mode\":\"POP\","
                + "\"popShareQueueNum\":2}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper.class,
            new String[][] { {"default", "{}"}, {"populated", "{\"messageRequestModeMap\":{"
                + "\"orders\":{\"workers\":{\"topic\":\"orders\","
                + "\"consumerGroup\":\"workers\",\"mode\":\"POP\","
                + "\"popShareQueueNum\":2}}}}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody.class,
            new String[][] { {"default", "{}"}, {"populated", "{\"consumerGroup\":\"workers\","
                + "\"clientId\":\"client-a\",\"onlyThisBroker\":true,"
                + "\"mqSet\":[{\"topic\":\"orders\",\"brokerName\":\"broker-a\","
                + "\"queueId\":1}]}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody.class,
            new String[][] { {"default", "{}"}, {"populated", "{\"consumerGroup\":\"workers\","
                + "\"clientId\":\"client-a\",\"onlyThisBroker\":true,"
                + "\"mqSet\":[{\"topic\":\"orders\",\"brokerName\":\"broker-a\","
                + "\"queueId\":1}]}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody.class,
            new String[][] { {"default", "{}"}, {"populated", "{\"lockOKMQSet\":[{"
                + "\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":1}]}"},
                {"eof-trailing-text", "{}\u001atrailing"},
                {"eof-trailing-json", "{}\u001a{\"ignored\":true}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody.class,
            new String[][] { {"default", "{}"}, {"populated", "{\"consumerIdList\":["
                + "\"client-a\",null,\"client-b\"]}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult.class,
            new String[][] { {"default", "{}"}, {"populated", "{\"order\":true,"
                + "\"autoCommit\":false,\"consumeResult\":\"CR_SUCCESS\","
                + "\"remark\":\"ok\",\"spentTimeMills\":19}"},
                {"eof-trailing-text", "{}\u001atrailing"},
                {"eof-trailing-json", "{}\u001a{\"ignored\":true}"},
                {"browser-compatible-max-safe", "{\"spentTimeMills\":9007199254740991}"},
                {"browser-compatible-above-max-safe", "{\"spentTimeMills\":9007199254740992}"},
                {"browser-compatible-min-safe", "{\"spentTimeMills\":-9007199254740991}"},
                {"browser-compatible-below-min-safe", "{\"spentTimeMills\":-9007199254740992}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.GroupList.class,
            new String[][] { {"default", "{}"},
                {"eof-trailing-text", "{}\u001atrailing"},
                {"eof-trailing-json", "{}\u001a{\"ignored\":true}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.TopicList.class,
            new String[][] { {"default", "{}"},
                {"eof-trailing-text", "{}\u001atrailing"},
                {"eof-trailing-json", "{}\u001a{\"ignored\":true}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.QuerySubscriptionResponseBody.class,
            new String[][] { {"default", "{}"},
                {"eof-trailing-text", "{}\u001atrailing"},
                {"eof-trailing-json", "{}\u001a{\"ignored\":true}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.class,
            new String[][] { {"populated", "{\"replicasInfoTable\":{\"broker-a\":{"
                + "\"masterBrokerId\":0,\"masterAddress\":\"broker-a:10911\","
                + "\"masterEpoch\":9,\"syncStateSetEpoch\":10,"
                + "\"inSyncReplicas\":[],\"notInSyncReplicas\":[]}}}"},
                {"eof-trailing-text", "{}\u001atrailing"},
                {"eof-trailing-json", "{}\u001a{\"ignored\":true}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.EpochEntryCache.class,
            new String[][] { {"populated", "{\"clusterName\":\"cluster-a\","
                + "\"brokerName\":\"broker-a\",\"brokerId\":3,"
                + "\"epochList\":[{\"epoch\":4,\"startOffset\":10}],\"maxOffset\":20}"},
                {"eof-trailing-text", "{}\u001atrailing"},
                {"eof-trailing-json", "{}\u001a{\"ignored\":true}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup.class,
            new String[][] { {"default", "{}"},
                {"eof-trailing-text", "{}\u001atrailing"},
                {"eof-trailing-json", "{}\u001a{\"ignored\":true}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody.class,
            new String[][] { {"default", "{}"}, {"populated", "{\"messageQueueTable\":"
                + "{{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":1}:9},"
                + "\"consumerTable\":{}}"} });
        addRemotingSerializableBodyCases(result,
            org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody.class,
            new String[][] { {"default", "{}"}, {"populated", "{\"offsetTable\":"
                + "{{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":1}:9}}"} });
        return result;
    }

    private static <T extends org.apache.rocketmq.remoting.protocol.RemotingSerializable>
        void addRemotingSerializableBodyCases(Map<String, Object> result, Class<T> type, String[][] inputs) {
        Map<String, Object> dto = new TreeMap<>();
        dto.put("inheritedMethods", Arrays.stream(type.getMethods())
            .filter(method -> method.getDeclaringClass()
                == org.apache.rocketmq.remoting.protocol.RemotingSerializable.class)
            .map(RuntimeLookupFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        List<Object> cases = new ArrayList<>();
        java.util.TimeZone originalTimeZone = java.util.TimeZone.getDefault();
        try {
            java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));
            for (String[] input : inputs) {
                byte[] raw = input[1].getBytes(java.nio.charset.StandardCharsets.UTF_8);
                T value = org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(raw, type);
                Map<String, Object> item = new TreeMap<>();
                item.put("id", input[0]);
                item.put("rawInput", input[1]);
                item.putAll(remotingSerializableValueCases(value, type));
                if (type == org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody.class) {
                    Map<String, Object> byTimeZone = new TreeMap<>();
                    for (String zone : List.of("UTC", "Asia/Taipei")) {
                        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone(zone));
                        byTimeZone.put(zone, remotingSerializableValueCases(value, type));
                    }
                    java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));
                    item.put("byTimeZone", byTimeZone);
                }
                cases.add(item);
            }
        } finally {
            java.util.TimeZone.setDefault(originalTimeZone);
        }
        dto.put("cases", cases);
        result.put(type.getSimpleName(), dto);
    }

    private static <T extends org.apache.rocketmq.remoting.protocol.RemotingSerializable>
        Map<String, Object> remotingSerializableValueCases(T value, Class<T> type) {
        String instanceJson = value.toJson();
        byte[] instanceBytes = value.encode();
        List<T> decodedList = org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(
            com.alibaba.fastjson2.JSON.toJSONBytes(java.util.Collections.singletonList(value)), type);
        Map<String, Object> item = new TreeMap<>();
        item.put("instanceJson", instanceJson);
        item.put("instancePrettyJson", value.toJson(true));
        item.put("staticJson",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.toJson(value, false));
        item.put("staticPrettyJson",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.toJson(value, true));
        item.put("instanceEncodedJson", new String(instanceBytes, java.nio.charset.StandardCharsets.UTF_8));
        item.put("staticEncodedJson", new String(
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.encode(value),
            java.nio.charset.StandardCharsets.UTF_8));
        item.put("featureEncodedJson", new String(value.encode(
            new com.alibaba.fastjson2.JSONWriter.Feature[0]), java.nio.charset.StandardCharsets.UTF_8));
        item.put("writerFeatureOverrides", writerFeatureOverrides(value));
        item.put("decodedJson", org.apache.rocketmq.remoting.protocol.RemotingSerializable
            .decode(instanceBytes, type).toJson());
        item.put("fromJsonJson", org.apache.rocketmq.remoting.protocol.RemotingSerializable
            .fromJson(instanceJson, type).toJson());
        item.put("decodedListJson", decodedList.get(0).toJson());
        item.put("decodedListCount", decodedList.size());
        return item;
    }

    private static Map<String, Object> writerFeatureOverrides(
            org.apache.rocketmq.remoting.protocol.RemotingSerializable value) {
        String baseline = new String(value.encode(), java.nio.charset.StandardCharsets.UTF_8);
        Map<String, Object> overrides = new TreeMap<>();
        for (com.alibaba.fastjson2.JSONWriter.Feature feature
                : com.alibaba.fastjson2.JSONWriter.Feature.values()) {
            try {
                String encoded = new String(value.encode(
                    new com.alibaba.fastjson2.JSONWriter.Feature[] {feature}),
                    java.nio.charset.StandardCharsets.UTF_8);
                if (!baseline.equals(encoded)) {
                    overrides.put(feature.name(), encoded);
                }
            } catch (RuntimeException error) {
                overrides.put(feature.name(), Map.of("error", error.getClass().getSimpleName()));
            }
        }
        overrides.put("WriteBooleanAsNumber+WriteNonStringValueAsString", new String(value.encode(
            new com.alibaba.fastjson2.JSONWriter.Feature[] {
                com.alibaba.fastjson2.JSONWriter.Feature.WriteBooleanAsNumber,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringValueAsString}),
            java.nio.charset.StandardCharsets.UTF_8));
        overrides.put("BrowserSecure+UnquoteFieldName", new String(value.encode(
            new com.alibaba.fastjson2.JSONWriter.Feature[] {
                com.alibaba.fastjson2.JSONWriter.Feature.BrowserSecure,
                com.alibaba.fastjson2.JSONWriter.Feature.UnquoteFieldName}),
            java.nio.charset.StandardCharsets.UTF_8));
        overrides.put("IgnoreErrorGetter", new String(value.encode(
            new com.alibaba.fastjson2.JSONWriter.Feature[] {
                com.alibaba.fastjson2.JSONWriter.Feature.IgnoreErrorGetter}),
            java.nio.charset.StandardCharsets.UTF_8));
        return overrides;
    }

    private static Map<String, Object> remotingSerializableWriterEscapingCase() {
        String rawInput = "{\"clientId\":\"café-漢字-😀\",\"group\":\"scope-𝄞-line\\nquote\\\"\"}";
        org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody value =
            com.alibaba.fastjson2.JSON.parseObject(rawInput,
                org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody.class);
        Map<String, Object> result = new TreeMap<>();
        result.put("rawInput", rawInput);
        result.put("instanceEncodedJson", new String(value.encode(), java.nio.charset.StandardCharsets.UTF_8));
        result.put("escapeNoneAsciiJson", new String(value.encode(
            new com.alibaba.fastjson2.JSONWriter.Feature[] {
                com.alibaba.fastjson2.JSONWriter.Feature.EscapeNoneAscii}),
            java.nio.charset.StandardCharsets.UTF_8));
        return result;
    }

    private static Map<String, Object> remotingSerializableBodyNullUtilities() {
        Map<String, Object> result = new TreeMap<>();
        addRemotingSerializableNullUtilities(result,
            org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody.class);
        addRemotingSerializableNullUtilities(result,
            org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody.class);
        addRemotingSerializableNullUtilities(result,
            org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.class);
        addRemotingSerializableNullUtilities(result,
            org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody.class);
        addRemotingSerializableNullUtilities(result,
            org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody.class);
        addRemotingSerializableNullUtilities(result,
            org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper.class);
        addRemotingSerializableNullUtilities(result,
            org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody.class);
        addRemotingSerializableNullUtilities(result,
            org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody.class);
        addRemotingSerializableNullUtilities(result,
            org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody.class);
        addRemotingSerializableNullUtilities(result,
            org.apache.rocketmq.remoting.protocol.body.GroupList.class);
        addRemotingSerializableNullUtilities(result,
            org.apache.rocketmq.remoting.protocol.body.TopicList.class);
        addRemotingSerializableNullUtilities(result,
            org.apache.rocketmq.remoting.protocol.body.QuerySubscriptionResponseBody.class);
        addRemotingSerializableNullUtilities(result,
            org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody.class);
        addRemotingSerializableNullUtilities(result,
            org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult.class);
        addRemotingSerializableNullUtilities(result,
            org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody.class);
        addRemotingSerializableNullUtilities(result,
            org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody.class);
        return result;
    }

    private static <T extends org.apache.rocketmq.remoting.protocol.RemotingSerializable>
        void addRemotingSerializableNullUtilities(Map<String, Object> result, Class<T> type) {
        byte[] jsonNull = "null".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Map<String, Object> item = new TreeMap<>();
        item.put("staticEncodeNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.encode((Object) null) == null);
        item.put("staticToJsonNull", org.apache.rocketmq.remoting.protocol.RemotingSerializable.toJson(null, false));
        item.put("decodeNullBytesReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(null, type) == null);
        item.put("decodeJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(jsonNull, type) == null);
        item.put("fromJsonNullInputReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.fromJson(null, type) == null);
        item.put("fromJsonJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.fromJson("null", type) == null);
        item.put("decodeListNullBytesReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(null, type) == null);
        item.put("decodeListJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(jsonNull, type) == null);
        List<T> nullMemberList = org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(
            "[null,{}]".getBytes(java.nio.charset.StandardCharsets.UTF_8), type);
        item.put("decodeListNullMemberCount", nullMemberList.size());
        item.put("decodeListPreservesNullMember", nullMemberList.get(0) == null);
        result.put(type.getSimpleName(), item);
    }

    private static Map<String, Object> consumerListApiCase() throws Exception {
        org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody body =
            new org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody();
        Map<String, Object> item = new TreeMap<>();
        item.put("publicConstructors", publicConstructorSignatures(body.getClass()));
        item.put("declaredPublicMethods", declaredPublicMethodSignatures(body.getClass()));
        item.put("publicFields", Arrays.stream(body.getClass().getFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(Field::getName)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        item.put("defaultConsumerIds", body.getConsumerIdList());
        List<String> consumerIds = new ArrayList<>(List.of("client-a", "client-b"));
        body.setConsumerIdList(consumerIds);
        item.put("values", body.getConsumerIdList());
        item.put("getterReturnsAssignedList", body.getConsumerIdList() == consumerIds);
        item.put("assignedValues", new ArrayList<>(body.getConsumerIdList()));
        consumerIds.add("client-c");
        item.put("externalMutationVisible", body.getConsumerIdList().contains("client-c"));
        body.getConsumerIdList().add("client-d");
        item.put("getterMutationVisibleToAssignedList", consumerIds.contains("client-d"));
        item.put("mutatedValues", body.getConsumerIdList());
        item.put("equalsDeclaringClass", body.getClass().getMethod("equals", Object.class)
            .getDeclaringClass().getName());
        item.put("hashCodeDeclaringClass", body.getClass().getMethod("hashCode")
            .getDeclaringClass().getName());
        item.put("sameInstanceEquals", body.equals(body));
        org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody equalValues =
            new org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody();
        equalValues.setConsumerIdList(new ArrayList<>(body.getConsumerIdList()));
        item.put("equalValuesDistinctInstanceEquals", body.equals(equalValues));
        body.setConsumerIdList(null);
        item.put("nullSetterRemainsNull", body.getConsumerIdList() == null);
        return item;
    }

    private static Map<String, Object> defaultTopicRouteApiCase() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("default-topic-route-api");
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        String routeJson = "{\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":3,\"writeQueueNums\":4,\"perm\":6,\"topicSysFlag\":0}],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"127.0.0.1:10911\"}}],\"filterServerTable\":{}}";
        int[] invocation = {0};
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient) java.lang.reflect.Proxy.newProxyInstance(
                    RuntimeLookupFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("topic", request.getExtFields() == null ? null
                                : request.getExtFields().get("topic"));
                            call.put("timeoutMillis", values[2]);
                            requests.add(call);
                            int current = invocation[0]++;
                            int responseCode = current == 0
                                ? org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS
                                : org.apache.rocketmq.remoting.protocol.ResponseCode.TOPIC_NOT_EXIST;
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                                    responseCode, current == 0 ? null : "topic not exist");
                            if (current == 0) {
                                response.setBody(routeJson.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                            }
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        try {
            org.apache.rocketmq.remoting.protocol.route.TopicRouteData route =
                api.getDefaultTopicRouteInfoFromNameServer(1234L);
            Map<String, Object> decoded = new TreeMap<>();
            decoded.put("queueDataCount", route.getQueueDatas().size());
            decoded.put("readQueueNums", route.getQueueDatas().get(0).getReadQueueNums());
            decoded.put("writeQueueNums", route.getQueueDatas().get(0).getWriteQueueNums());
            decoded.put("brokerName", route.getQueueDatas().get(0).getBrokerName());
            result.put("successRoute", decoded);
            try {
                api.getDefaultTopicRouteInfoFromNameServer(1234L);
                result.put("topicNotExistOutcome", "returned");
            } catch (org.apache.rocketmq.client.exception.MQClientException error) {
                result.put("topicNotExistOutcome", error.getClass().getSimpleName());
                result.put("topicNotExistResponseCode", error.getResponseCode());
                result.put("topicNotExistMessage", error.getErrorMessage());
            }
        } finally {
            api.shutdown();
        }
        result.put("responseBody", routeJson);
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> queryConsumeTimeSpanApiCase() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("query-consume-time-span-api");
        config.setVipChannelEnabled(false);
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        String body = "{\"consumeTimeSpanSet\":["
            + "{\"messageQueue\":{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":1},\"minTimeStamp\":1000,\"maxTimeStamp\":9000,\"consumeTimeStamp\":5000,\"delayTime\":4000},"
            + "{\"messageQueue\":{\"topic\":\"orders\",\"brokerName\":\"broker-b\",\"queueId\":2},\"minTimeStamp\":2000,\"maxTimeStamp\":10000,\"consumeTimeStamp\":7000,\"delayTime\":3000}]}";
        int[] invocation = {0};
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient) java.lang.reflect.Proxy.newProxyInstance(
                    RuntimeLookupFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("topic", request.getExtFields().get("topic"));
                            call.put("group", request.getExtFields().get("group"));
                            call.put("timeoutMillis", values[2]);
                            requests.add(call);
                            int current = invocation[0]++;
                            int responseCode = current == 0
                                ? org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS
                                : org.apache.rocketmq.remoting.protocol.ResponseCode.TOPIC_NOT_EXIST;
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                                    responseCode, current == 0 ? null : "topic not exist");
                            if (current == 0) {
                                response.setBody(body.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                            }
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        try {
            List<org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan> spans =
                api.queryConsumeTimeSpan("127.0.0.1:10911", "orders", "consumer-a", 1234L);
            List<Map<String, Object>> rows = new ArrayList<>();
            for (org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan span : spans) {
                Map<String, Object> row = new TreeMap<>();
                row.put("topic", span.getMessageQueue().getTopic());
                row.put("brokerName", span.getMessageQueue().getBrokerName());
                row.put("queueId", span.getMessageQueue().getQueueId());
                row.put("minTimeStamp", span.getMinTimeStamp());
                row.put("maxTimeStamp", span.getMaxTimeStamp());
                row.put("consumeTimeStamp", span.getConsumeTimeStamp());
                row.put("delayTime", span.getDelayTime());
                row.put("timestampStringsByTimeZone", queueTimeSpanStringsByTimeZone(span));
                rows.add(row);
            }
            result.put("successRows", rows);
            try {
                api.queryConsumeTimeSpan("127.0.0.1:10911", "orders", "consumer-a", 1234L);
                result.put("topicNotExistOutcome", "returned");
            } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
                result.put("topicNotExistOutcome", error.getClass().getSimpleName());
                result.put("topicNotExistResponseCode", error.getResponseCode());
                result.put("topicNotExistMessage", error.getErrorMessage());
            }
        } finally {
            api.shutdown();
        }
        result.put("responseBody", body);
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> queueTimeSpanStringsByTimeZone(
            org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan span) {
        Map<String, Object> byTimeZone = new TreeMap<>();
        java.util.TimeZone originalTimeZone = java.util.TimeZone.getDefault();
        try {
            for (String zone : List.of("UTC", "Asia/Taipei")) {
                java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone(zone));
                Map<String, Object> values = new TreeMap<>();
                values.put("minTimeStampStr", span.getMinTimeStampStr());
                values.put("maxTimeStampStr", span.getMaxTimeStampStr());
                values.put("consumeTimeStampStr", span.getConsumeTimeStampStr());
                byTimeZone.put(zone, values);
            }
        } finally {
            java.util.TimeZone.setDefault(originalTimeZone);
        }
        return byTimeZone;
    }

    private static Map<String, Object> queueTimeSpanApiCase() throws Exception {
        org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan defaults =
            new org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan();
        org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan value =
            new org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan();
        org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan extremes =
            new org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan();
        org.apache.rocketmq.common.message.MessageQueue assignedQueue =
            new org.apache.rocketmq.common.message.MessageQueue("orders", "broker-a", 1);
        value.setMessageQueue(assignedQueue);
        value.setMinTimeStamp(1_000L);
        value.setMaxTimeStamp(9_000L);
        value.setConsumeTimeStamp(5_000L);
        value.setDelayTime(4_000L);
        extremes.setMinTimeStamp(Long.MIN_VALUE);
        extremes.setMaxTimeStamp(Long.MAX_VALUE);
        extremes.setConsumeTimeStamp(Long.MIN_VALUE);
        Map<String, Object> item = new TreeMap<>();
        item.put("publicConstructors", publicConstructorSignatures(value.getClass()));
        item.put("declaredPublicMethods", declaredPublicMethodSignatures(value.getClass()));
        Map<String, Object> defaultAccessors = new TreeMap<>();
        defaultAccessors.put("messageQueueNull", defaults.getMessageQueue() == null);
        defaultAccessors.put("minTimeStamp", defaults.getMinTimeStamp());
        defaultAccessors.put("maxTimeStamp", defaults.getMaxTimeStamp());
        defaultAccessors.put("consumeTimeStamp", defaults.getConsumeTimeStamp());
        defaultAccessors.put("delayTime", defaults.getDelayTime());
        item.put("defaultAccessors", defaultAccessors);
        Map<String, Object> accessorValues = new TreeMap<>();
        accessorValues.put("messageQueueReturnsAssignedObject", value.getMessageQueue() == assignedQueue);
        accessorValues.put("topic", value.getMessageQueue().getTopic());
        accessorValues.put("brokerName", value.getMessageQueue().getBrokerName());
        accessorValues.put("queueId", value.getMessageQueue().getQueueId());
        accessorValues.put("minTimeStamp", value.getMinTimeStamp());
        accessorValues.put("maxTimeStamp", value.getMaxTimeStamp());
        accessorValues.put("consumeTimeStamp", value.getConsumeTimeStamp());
        accessorValues.put("delayTime", value.getDelayTime());
        item.put("accessorValues", accessorValues);
        value.getMessageQueue().setQueueId(7);
        item.put("messageQueueGetterMutationVisibleToAssignedObject", assignedQueue.getQueueId() == 7);
        org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan nullQueue =
            new org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan();
        nullQueue.setMessageQueue(null);
        item.put("nullMessageQueueSetterPreservesNull", nullQueue.getMessageQueue() == null);
        Map<String, Object> defaultsByTimeZone = new TreeMap<>();
        Map<String, Object> valuesByTimeZone = new TreeMap<>();
        Map<String, Object> extremeValuesByTimeZone = queueTimeSpanStringsByTimeZone(extremes);
        java.util.TimeZone originalTimeZone = java.util.TimeZone.getDefault();
        try {
            for (String zone : List.of("UTC", "Asia/Taipei")) {
                java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone(zone));
                Map<String, Object> defaultValues = new TreeMap<>();
                defaultValues.put("minTimeStampStr", defaults.getMinTimeStampStr());
                defaultValues.put("maxTimeStampStr", defaults.getMaxTimeStampStr());
                defaultValues.put("consumeTimeStampStr", defaults.getConsumeTimeStampStr());
                defaultsByTimeZone.put(zone, defaultValues);
                Map<String, Object> values = new TreeMap<>();
                values.put("minTimeStampStr", value.getMinTimeStampStr());
                values.put("maxTimeStampStr", value.getMaxTimeStampStr());
                values.put("consumeTimeStampStr", value.getConsumeTimeStampStr());
                valuesByTimeZone.put(zone, values);
            }
        } finally {
            java.util.TimeZone.setDefault(originalTimeZone);
        }
        item.put("defaultsByTimeZone", defaultsByTimeZone);
        item.put("valuesByTimeZone", valuesByTimeZone);
        item.put("extremeValuesByTimeZone", extremeValuesByTimeZone);
        item.put("queryConsumeTimeSpanBodyApi", queryConsumeTimeSpanBodyApi(value, defaults));
        return item;
    }

    private static Map<String, Object> queryConsumeTimeSpanBodyApi(
        org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan first,
        org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan second) {
        Map<String, Object> result = new TreeMap<>();
        org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody defaults =
            new org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody();
        List<org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan> defaultSet =
            defaults.getConsumeTimeSpanSet();
        result.put("publicConstructors", publicConstructorSignatures(defaults.getClass()));
        result.put("declaredPublicMethods", declaredPublicMethodSignatures(defaults.getClass()));
        result.put("defaultListNonNull", defaultSet != null);
        result.put("defaultListEmpty", defaultSet != null && defaultSet.isEmpty());
        result.put("defaultGetterReturnsStoredList", defaults.getConsumeTimeSpanSet() == defaultSet);

        List<org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan> assigned = new ArrayList<>();
        assigned.add(first);
        defaults.setConsumeTimeSpanSet(assigned);
        List<org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan> returned =
            defaults.getConsumeTimeSpanSet();
        result.put("setterRetainsAssignedList", defaults.getConsumeTimeSpanSet() == assigned);
        result.put("getterReturnsAssignedList", returned == assigned);
        result.put("getterReturnsAssignedElement", returned.get(0) == first);
        returned.add(second);
        result.put("getterMutationVisibleToAssignedList", assigned.size() == 2);
        assigned.add(null);
        result.put("assignedListMutationVisibleToGetter",
            defaults.getConsumeTimeSpanSet().size() == 3
                && defaults.getConsumeTimeSpanSet().get(2) == null);
        defaults.setConsumeTimeSpanSet(null);
        result.put("nullSetterPreservesNull", defaults.getConsumeTimeSpanSet() == null);
        return result;
    }

    private static Map<String, Object> consumerListNullGroupApiCase() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("consumer-list-null-group-api");
        Map<String, Object> result = new TreeMap<>();
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient) java.lang.reflect.Proxy.newProxyInstance(
                    RuntimeLookupFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            result.put("address", values[0]);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            result.put("requestCode", request.getCode());
                            Map<String, String> fields = request.getExtFields();
                            result.put("consumerGroupFieldPresent",
                                fields != null && fields.containsKey("consumerGroup"));
                            result.put("consumerGroup", fields == null ? null : fields.get("consumerGroup"));
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                                    org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null);
                            response.setBody("{\"consumerIdList\":[\"client\"]}"
                                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        try {
            result.put("consumerIds", api.getConsumerIdListByGroup("broker:10911", null, 1000L));
        } finally {
            api.shutdown();
        }
        return result;
    }

    private static Map<String, Object> consumerListEmptyGroupApiCase() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("consumer-list-empty-group-api");
        Map<String, Object> result = new TreeMap<>();
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient) java.lang.reflect.Proxy.newProxyInstance(
                    RuntimeLookupFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            result.put("address", values[0]);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            result.put("requestCode", request.getCode());
                            Map<String, String> fields = request.getExtFields();
                            result.put("consumerGroupFieldPresent",
                                fields != null && fields.containsKey("consumerGroup"));
                            result.put("consumerGroup", fields == null ? null : fields.get("consumerGroup"));
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                                    org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null);
                            response.setBody("{\"consumerIdList\":[\"client\"]}"
                                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        try {
            result.put("consumerIds", api.getConsumerIdListByGroup("broker:10911", "", 1000L));
        } finally {
            api.shutdown();
        }
        return result;
    }

    private static List<Object> messageQueueApiValues(Set<org.apache.rocketmq.common.message.MessageQueue> queues) {
        if (queues == null) return null;
        List<Object> values = new ArrayList<>();
        for (org.apache.rocketmq.common.message.MessageQueue queue : queues) {
            if (queue == null) {
                values.add(null);
            } else {
                Map<String, Object> item = new TreeMap<>();
                item.put("topic", queue.getTopic());
                item.put("brokerName", queue.getBrokerName());
                item.put("queueId", queue.getQueueId());
                values.add(item);
            }
        }
        return values;
    }

    private static void recordObjectIdentity(Map<String, Object> item, Object body, Object equalValues)
            throws Exception {
        item.put("equalsDeclaringClass", body.getClass().getMethod("equals", Object.class)
            .getDeclaringClass().getName());
        item.put("hashCodeDeclaringClass", body.getClass().getMethod("hashCode")
            .getDeclaringClass().getName());
        item.put("sameInstanceEquals", body.equals(body));
        item.put("equalValuesDistinctInstanceEquals", body.equals(equalValues));
    }

    private static Set<org.apache.rocketmq.common.message.MessageQueue> oneQueueSet() {
        Set<org.apache.rocketmq.common.message.MessageQueue> queues = new java.util.LinkedHashSet<>();
        queues.add(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 2));
        return queues;
    }

    private static Map<String, Object> messageQueueSetElementAliasCase(
            Set<org.apache.rocketmq.common.message.MessageQueue> setterSet,
            Set<org.apache.rocketmq.common.message.MessageQueue> getterSet) {
        Map<String, Object> result = new TreeMap<>();
        org.apache.rocketmq.common.message.MessageQueue setterQueue = setterSet.iterator().next();
        org.apache.rocketmq.common.message.MessageQueue getterQueue = getterSet.iterator().next();
        int originalQueueId = setterQueue.getQueueId();
        result.put("getterReturnsAssignedElement", setterQueue == getterQueue);
        setterQueue.setQueueId(originalQueueId + 5);
        result.put("setterElementMutationVisibleToGetter",
            getterSet.iterator().next().getQueueId() == originalQueueId + 5);
        setterQueue.setQueueId(originalQueueId);
        getterQueue.setQueueId(originalQueueId + 6);
        result.put("getterElementMutationVisibleToSetterObject",
            setterQueue.getQueueId() == originalQueueId + 6);
        setterQueue.setQueueId(originalQueueId);
        return result;
    }

    private static Map<String, Object> lockBatchRequestApiCase() throws Exception {
        org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody body =
            new org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody();
        Map<String, Object> item = new TreeMap<>();
        recordBodyApiInventory(item, body.getClass());
        Map<String, Object> defaults = new TreeMap<>();
        defaults.put("consumerGroup", body.getConsumerGroup());
        defaults.put("clientId", body.getClientId());
        defaults.put("onlyThisBroker", body.isOnlyThisBroker());
        defaults.put("mqSet", messageQueueApiValues(body.getMqSet()));
        item.put("defaults", defaults);
        item.put("defaultText", body.toString());

        Set<org.apache.rocketmq.common.message.MessageQueue> queues = oneQueueSet();
        body.setConsumerGroup("workers");
        body.setClientId("client-a");
        body.setOnlyThisBroker(true);
        body.setMqSet(queues);
        Map<String, Object> values = new TreeMap<>();
        values.put("consumerGroup", body.getConsumerGroup());
        values.put("clientId", body.getClientId());
        values.put("onlyThisBroker", body.isOnlyThisBroker());
        values.put("mqSet", messageQueueApiValues(body.getMqSet()));
        item.put("values", values);
        item.put("getterReturnsAssignedSet", body.getMqSet() == queues);
        item.put("assignedValues", messageQueueApiValues(body.getMqSet()));
        item.putAll(messageQueueSetElementAliasCase(queues, body.getMqSet()));
        queues.remove(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 2));
        queues.add(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 3));
        item.put("externalMutationVisible", body.getMqSet().contains(
            new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 3)));
        body.getMqSet().remove(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 3));
        body.getMqSet().add(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 4));
        item.put("getterMutationVisibleToAssignedSet", queues.contains(
            new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 4)));
        values.put("mqSet", messageQueueApiValues(body.getMqSet()));
        item.put("mutatedValues", values);
        item.put("text", body.toString());
        org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody equalValues =
            new org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody();
        equalValues.setConsumerGroup(body.getConsumerGroup());
        equalValues.setClientId(body.getClientId());
        equalValues.setOnlyThisBroker(body.isOnlyThisBroker());
        equalValues.setMqSet(new java.util.HashSet<>(queues));
        recordObjectIdentity(item, body, equalValues);

        body.setConsumerGroup(null);
        body.setClientId(null);
        body.setOnlyThisBroker(false);
        body.setMqSet(null);
        item.put("nullSettersRemainNull", body.getConsumerGroup() == null && body.getClientId() == null
            && body.getMqSet() == null && !body.isOnlyThisBroker());
        return item;
    }

    private static Map<String, Object> unlockBatchRequestApiCase() throws Exception {
        org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody body =
            new org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody();
        Map<String, Object> item = new TreeMap<>();
        recordBodyApiInventory(item, body.getClass());
        Map<String, Object> defaults = new TreeMap<>();
        defaults.put("consumerGroup", body.getConsumerGroup());
        defaults.put("clientId", body.getClientId());
        defaults.put("onlyThisBroker", body.isOnlyThisBroker());
        defaults.put("mqSet", messageQueueApiValues(body.getMqSet()));
        item.put("defaults", defaults);
        item.put("defaultText", body.toString());

        Set<org.apache.rocketmq.common.message.MessageQueue> queues = oneQueueSet();
        body.setConsumerGroup("workers");
        body.setClientId("client-a");
        body.setOnlyThisBroker(true);
        body.setMqSet(queues);
        Map<String, Object> values = new TreeMap<>();
        values.put("consumerGroup", body.getConsumerGroup());
        values.put("clientId", body.getClientId());
        values.put("onlyThisBroker", body.isOnlyThisBroker());
        values.put("mqSet", messageQueueApiValues(body.getMqSet()));
        item.put("values", values);
        item.put("getterReturnsAssignedSet", body.getMqSet() == queues);
        item.put("assignedValues", messageQueueApiValues(body.getMqSet()));
        item.putAll(messageQueueSetElementAliasCase(queues, body.getMqSet()));
        queues.remove(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 2));
        queues.add(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 3));
        item.put("externalMutationVisible", body.getMqSet().contains(
            new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 3)));
        body.getMqSet().remove(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 3));
        body.getMqSet().add(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 4));
        item.put("getterMutationVisibleToAssignedSet", queues.contains(
            new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 4)));
        values.put("mqSet", messageQueueApiValues(body.getMqSet()));
        item.put("mutatedValues", values);
        item.put("text", body.toString());
        org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody equalValues =
            new org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody();
        equalValues.setConsumerGroup(body.getConsumerGroup());
        equalValues.setClientId(body.getClientId());
        equalValues.setOnlyThisBroker(body.isOnlyThisBroker());
        equalValues.setMqSet(new java.util.HashSet<>(queues));
        recordObjectIdentity(item, body, equalValues);

        body.setConsumerGroup(null);
        body.setClientId(null);
        body.setOnlyThisBroker(false);
        body.setMqSet(null);
        item.put("nullSettersRemainNull", body.getConsumerGroup() == null && body.getClientId() == null
            && body.getMqSet() == null && !body.isOnlyThisBroker());
        return item;
    }

    private static Map<String, Object> lockBatchResponseApiCase() throws Exception {
        org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody body =
            new org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody();
        Map<String, Object> item = new TreeMap<>();
        item.put("publicConstructors", publicConstructorSignatures(body.getClass()));
        item.put("declaredPublicMethods", declaredPublicMethodSignatures(body.getClass()));
        item.put("publicFields", Arrays.stream(body.getClass().getFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(Field::getName)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        item.put("defaultLockOKMQSet", messageQueueApiValues(body.getLockOKMQSet()));
        Set<org.apache.rocketmq.common.message.MessageQueue> queues = oneQueueSet();
        body.setLockOKMQSet(queues);
        item.put("values", messageQueueApiValues(body.getLockOKMQSet()));
        item.put("getterReturnsAssignedSet", body.getLockOKMQSet() == queues);
        item.put("assignedValues", messageQueueApiValues(body.getLockOKMQSet()));
        item.putAll(messageQueueSetElementAliasCase(queues, body.getLockOKMQSet()));
        queues.remove(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 2));
        queues.add(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 3));
        item.put("externalMutationVisible", body.getLockOKMQSet().contains(
            new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 3)));
        body.getLockOKMQSet().remove(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 3));
        body.getLockOKMQSet().add(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 4));
        item.put("getterMutationVisibleToAssignedSet", queues.contains(
            new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 4)));
        item.put("mutatedValues", messageQueueApiValues(body.getLockOKMQSet()));
        org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody equalValues =
            new org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody();
        equalValues.setLockOKMQSet(new java.util.HashSet<>(queues));
        recordObjectIdentity(item, body, equalValues);
        body.setLockOKMQSet(null);
        item.put("nullSetterRemainsNull", body.getLockOKMQSet() == null);
        return item;
    }

    private static Map<String, Object> consumeDirectResultApiCase() {
        org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult result =
            new org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult();
        Map<String, Object> item = new TreeMap<>();
        recordBodyApiInventory(item, result.getClass());
        Map<String, Object> defaults = new TreeMap<>();
        defaults.put("order", result.isOrder());
        defaults.put("autoCommit", result.isAutoCommit());
        defaults.put("consumeResult", result.getConsumeResult() == null ? null : result.getConsumeResult().name());
        defaults.put("remark", result.getRemark());
        defaults.put("spentTimeMills", result.getSpentTimeMills());
        item.put("defaults", defaults);
        item.put("defaultText", result.toString());

        result.setOrder(true);
        result.setAutoCommit(false);
        result.setConsumeResult(org.apache.rocketmq.remoting.protocol.body.CMResult.CR_SUCCESS);
        result.setRemark("consumed");
        result.setSpentTimeMills(42L);
        Map<String, Object> values = new TreeMap<>();
        values.put("order", result.isOrder());
        values.put("autoCommit", result.isAutoCommit());
        values.put("consumeResult", result.getConsumeResult().name());
        values.put("remark", result.getRemark());
        values.put("spentTimeMills", result.getSpentTimeMills());
        item.put("values", values);
        item.put("text", result.toString());

        result.setConsumeResult(null);
        result.setRemark(null);
        item.put("nullSettersRemainNull", result.getConsumeResult() == null && result.getRemark() == null);
        return item;
    }

    private static List<Object> queueOffsetApiValues(
            Map<org.apache.rocketmq.common.message.MessageQueue, Long> table) {
        if (table == null) return null;
        List<Object> rows = new ArrayList<>();
        for (Map.Entry<org.apache.rocketmq.common.message.MessageQueue, Long> entry : table.entrySet()) {
            org.apache.rocketmq.common.message.MessageQueue queue = entry.getKey();
            Map<String, Object> queueValue = new TreeMap<>();
            queueValue.put("topic", queue.getTopic());
            queueValue.put("brokerName", queue.getBrokerName());
            queueValue.put("queueId", queue.getQueueId());
            rows.add(Arrays.asList(queueValue, entry.getValue()));
        }
        return rows;
    }

    private static Map<String, Object> consumerTableApiValues(
            Map<String, Map<org.apache.rocketmq.common.message.MessageQueue, Long>> table) {
        if (table == null) return null;
        Map<String, Object> values = new TreeMap<>();
        for (Map.Entry<String, Map<org.apache.rocketmq.common.message.MessageQueue, Long>> entry
                : table.entrySet()) {
            values.put(entry.getKey(), queueOffsetApiValues(entry.getValue()));
        }
        return values;
    }

    private static Map<String, Object> consumerStatusBodyApiCase() {
        org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody body =
            new org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody();
        Map<String, Object> item = new TreeMap<>();
        recordBodyApiInventory(item, body.getClass());
        Map<String, Object> defaults = new TreeMap<>();
        defaults.put("messageQueueTable", queueOffsetApiValues(body.getMessageQueueTable()));
        defaults.put("consumerTable", consumerTableApiValues(body.getConsumerTable()));
        item.put("defaults", defaults);

        Map<org.apache.rocketmq.common.message.MessageQueue, Long> queueTable =
            new java.util.LinkedHashMap<>();
        queueTable.put(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 2), 42L);
        queueTable.put(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 3), null);
        Map<String, Map<org.apache.rocketmq.common.message.MessageQueue, Long>> consumerTable =
            new java.util.LinkedHashMap<>();
        consumerTable.put("workers", new java.util.LinkedHashMap<>(queueTable));
        consumerTable.put("empty", new java.util.LinkedHashMap<>());
        consumerTable.put("null-table", null);
        body.setMessageQueueTable(queueTable);
        body.setConsumerTable(consumerTable);

        Map<String, Object> values = new TreeMap<>();
        values.put("messageQueueTable", queueOffsetApiValues(body.getMessageQueueTable()));
        values.put("consumerTable", consumerTableApiValues(body.getConsumerTable()));
        item.put("values", values);
        item.put("getterReturnsAssignedMessageQueueTable", body.getMessageQueueTable() == queueTable);
        item.put("getterReturnsAssignedConsumerTable", body.getConsumerTable() == consumerTable);
        item.put("nestedGetterReturnsAssignedTable",
            body.getConsumerTable().get("workers") == consumerTable.get("workers"));

        queueTable.put(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 4), 99L);
        item.put("messageQueueTableExternalMutationVisible",
            body.getMessageQueueTable().containsKey(
                new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 4)));
        Map<org.apache.rocketmq.common.message.MessageQueue, Long> getterAddedTable =
            new java.util.LinkedHashMap<>();
        getterAddedTable.put(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 5), 100L);
        body.getConsumerTable().put("getter-added", getterAddedTable);
        item.put("consumerTableGetterMutationVisibleToAssignedMap", consumerTable.containsKey("getter-added"));
        body.getConsumerTable().get("workers").put(
            new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 6), 101L);
        item.put("nestedGetterMutationVisibleToAssignedTable",
            consumerTable.get("workers").containsKey(
                new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 6)));
        Map<String, Object> mutatedValues = new TreeMap<>();
        mutatedValues.put("messageQueueTable", queueOffsetApiValues(body.getMessageQueueTable()));
        mutatedValues.put("consumerTable", consumerTableApiValues(body.getConsumerTable()));
        item.put("mutatedValues", mutatedValues);

        body.setMessageQueueTable(null);
        body.setConsumerTable(null);
        item.put("nullSettersRemainNull", body.getMessageQueueTable() == null && body.getConsumerTable() == null);
        return item;
    }

    private static Map<String, Object> resetOffsetBodyApiCase() {
        org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody body =
            new org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody();
        Map<String, Object> item = new TreeMap<>();
        recordBodyApiInventory(item, body.getClass());
        item.put("defaults", queueOffsetApiValues(body.getOffsetTable()));

        Map<org.apache.rocketmq.common.message.MessageQueue, Long> offsetTable =
            new java.util.LinkedHashMap<>();
        offsetTable.put(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 2), 42L);
        offsetTable.put(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 3), null);
        body.setOffsetTable(offsetTable);
        item.put("values", queueOffsetApiValues(body.getOffsetTable()));
        item.put("getterReturnsAssignedMap", body.getOffsetTable() == offsetTable);
        org.apache.rocketmq.common.message.MessageQueue externalQueue =
            new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 4);
        offsetTable.put(externalQueue, 99L);
        item.put("externalMutationVisible", Long.valueOf(99L).equals(body.getOffsetTable().get(externalQueue)));
        org.apache.rocketmq.common.message.MessageQueue getterQueue =
            new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 5);
        body.getOffsetTable().put(getterQueue, 100L);
        item.put("getterMutationVisibleToAssignedMap", Long.valueOf(100L).equals(offsetTable.get(getterQueue)));
        item.put("mutatedValues", queueOffsetApiValues(body.getOffsetTable()));
        body.setOffsetTable(null);
        item.put("nullSetterRemainsNull", body.getOffsetTable() == null);
        return item;
    }

    private static String tokenName(String token) {
        if ("null".equals(token)) return "null";
        if (token.startsWith("\"")) return "string";
        if ("true".equals(token)) return "boolean";
        if ("[]".equals(token)) return "array";
        if ("{}".equals(token)) return "object";
        return token.contains(".") ? "fraction" : "integer";
    }

    private static Map<String, Object> consumeDirectResultCase(String name, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("rawInput", rawInput);
        try {
            org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult decoded =
                com.alibaba.fastjson2.JSON.parseObject(rawInput,
                    org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult.class);
            item.put("outcome", "ACCEPTED");
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(decoded)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> messageQueueInputCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(messageQueueInputCase("default", "{}"));
        cases.add(messageQueueInputCase("normal", "{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2}"));
        cases.add(messageQueueInputCase("scalar-coercions", "{\"topic\":42,\"brokerName\":true,\"queueId\":1.9}"));
        cases.add(messageQueueInputCase("high-precision-topic", "{\"topic\":9223372036854775807.123456789012345678901}"));
        cases.add(messageQueueInputCase("queue-minimum", "{\"queueId\":-2147483648}"));
        cases.add(messageQueueInputCase("queue-maximum", "{\"queueId\":2147483647}"));
        cases.add(messageQueueInputCase("queue-integer-overflow", "{\"queueId\":2147483648}"));
        cases.add(messageQueueInputCase("queue-decimal-wrap", "{\"queueId\":2147483648.0}"));
        cases.add(messageQueueInputCase("queue-fraction", "{\"queueId\":-1.9}"));
        cases.add(messageQueueInputCase("queue-exponent-string", "{\"queueId\":\"1e2\"}"));
        cases.add(messageQueueInputCase("queue-null", "{\"queueId\":null}"));
        cases.add(messageQueueInputCase("queue-boolean", "{\"queueId\":true}"));
        cases.add(messageQueueInputCase("queue-empty-object", "{\"queueId\":{}}"));
        cases.add(messageQueueInputCase("queue-array", "{\"queueId\":[]}"));
        cases.add(messageQueueInputCase("topic-object", "{\"topic\":{\"k\":1}}"));
        cases.add(messageQueueInputCase("topic-null", "{\"topic\":null,\"brokerName\":null}"));
        cases.add(messageQueueInputCase("duplicate-topic-last-wins",
            "{\"topic\":\"first\",\"topic\":\"last\"}"));
        cases.add(messageQueueInputCase("duplicate-broker-name-last-wins",
            "{\"brokerName\":\"first\",\"brokerName\":\"last\"}"));
        cases.add(messageQueueInputCase("duplicate-queue-id-last-wins",
            "{\"queueId\":1,\"queueId\":2.9}"));
        return cases;
    }

    private static Map<String, Object> messageQueueInputCase(String name, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("rawInput", rawInput);
        try {
            org.apache.rocketmq.common.message.MessageQueue decoded =
                com.alibaba.fastjson2.JSON.parseObject(rawInput,
                    org.apache.rocketmq.common.message.MessageQueue.class);
            item.put("outcome", "ACCEPTED");
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(decoded)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static String stringWithHashCode(int target) {
        long remaining = Integer.toUnsignedLong(target);
        char[] digits = new char[7];
        for (int index = digits.length - 1; index >= 0; index--) {
            digits[index] = (char) (remaining % 31);
            remaining /= 31;
        }
        if (remaining != 0) {
            throw new IllegalArgumentException("hash does not fit seven base-31 digits");
        }
        return new String(digits);
    }

    private static Map<String, Object> assignmentHashCollisionCases() {
        org.apache.rocketmq.common.message.MessageQueue queue =
            new org.apache.rocketmq.common.message.MessageQueue("topic", "broker", 2);
        org.apache.rocketmq.common.message.MessageQueueAssignment pull =
            new org.apache.rocketmq.common.message.MessageQueueAssignment();
        pull.setMessageQueue(queue);
        org.apache.rocketmq.common.message.MessageQueueAssignment pop =
            new org.apache.rocketmq.common.message.MessageQueueAssignment();
        pop.setMessageQueue(new org.apache.rocketmq.common.message.MessageQueue("topic", "broker", 2));
        pop.setMode(org.apache.rocketmq.common.message.MessageRequestMode.POP);

        // MessageQueueAssignment.hashCode includes enum identity hash and attachments,
        // but equals compares only the queue. Choose an attachment hash that forces
        // an equal-hash pair for this JVM, without emitting the process-local hash.
        int attachmentHash = 31 * (pull.getMode().hashCode() - pop.getMode().hashCode());
        Map<String, String> attachments = new HashMap<>();
        attachments.put("", stringWithHashCode(attachmentHash));
        pop.setAttachments(attachments);

        Set<org.apache.rocketmq.common.message.MessageQueueAssignment> assignments = new HashSet<>();
        assignments.add(pull);
        assignments.add(pop);
        Map<String, Object> result = new TreeMap<>();
        result.put("sameQueueEquals", pull.equals(pop));
        result.put("sameMode", pull.getMode() == pop.getMode());
        result.put("equalHash", pull.hashCode() == pop.hashCode());
        result.put("setSize", assignments.size());
        result.put("collisionDeduplicates", assignments.size() == 1);
        return result;
    }

    private static Map<String, Object> assignmentBodyCase(String name, String rawValue, boolean includeField) {
        String body = includeField ? "{\"messageQueueAssignments\":" + rawValue + "}" : "{}";
        return assignmentBodyRawCase(name, body);
    }

    private static Map<String, Object> assignmentBodyRawCase(String name, String body) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("input", com.alibaba.fastjson2.JSON.parseObject(body));
        item.put("rawInput", body);
        try {
            org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody decoded =
                org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.decode(
                    body.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.class);
            item.put("outcome", "ACCEPTED");
            Set<org.apache.rocketmq.common.message.MessageQueueAssignment> assignmentSet =
                decoded.getMessageQueueAssignments();
            if (assignmentSet == null) {
                item.put("assignments", null);
            } else {
                List<org.apache.rocketmq.common.message.MessageQueueAssignment> assignments =
                    new ArrayList<>(assignmentSet);
                assignments.sort(
                    Comparator.nullsFirst(Comparator.comparing(com.alibaba.fastjson2.JSON::toJSONString)));
                item.put("assignments", assignments);
            }
            byte[] encoded = decoded.encode();
            Map<String, Object> encodedBody = com.alibaba.fastjson2.JSON.parseObject(
                new String(encoded, java.nio.charset.StandardCharsets.UTF_8));
            Object encodedAssignments = encodedBody.get("messageQueueAssignments");
            if (encodedAssignments instanceof List<?>) {
                @SuppressWarnings("unchecked")
                List<Object> encodedRows = (List<Object>) encodedAssignments;
                encodedRows.sort(
                    Comparator.nullsFirst(Comparator.comparing(com.alibaba.fastjson2.JSON::toJSONString)));
            }
            item.put("encoded", encodedBody);
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> assignmentMethodCases() {
        List<Object> cases = new ArrayList<>();
        org.apache.rocketmq.common.message.MessageQueueAssignment empty =
            new org.apache.rocketmq.common.message.MessageQueueAssignment();
        Map<String, Object> emptyCase = new TreeMap<>();
        emptyCase.put("name", "default");
        emptyCase.put("messageQueue", null);
        emptyCase.put("mode", empty.getMode() == null ? null : empty.getMode().name());
        emptyCase.put("attachments", empty.getAttachments());
        emptyCase.put("text", empty.toString());
        emptyCase.put("equalsSelf", empty.equals(empty));
        emptyCase.put("equalsNull", empty.equals(null));
        emptyCase.put("equalsWrongType", empty.equals("not-an-assignment"));
        try {
            empty.equals(new org.apache.rocketmq.common.message.MessageQueueAssignment());
            emptyCase.put("equalsDistinctNullQueue", "NO_EXCEPTION");
        } catch (RuntimeException error) {
            emptyCase.put("equalsDistinctNullQueue", error.getClass().getSimpleName());
        }
        cases.add(emptyCase);

        org.apache.rocketmq.common.message.MessageQueue queue =
            new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 2);
        org.apache.rocketmq.common.message.MessageQueueAssignment populated =
            new org.apache.rocketmq.common.message.MessageQueueAssignment();
        populated.setMessageQueue(queue);
        populated.setMode(org.apache.rocketmq.common.message.MessageRequestMode.POP);
        Map<String, String> attachments = new TreeMap<>();
        attachments.put("key", "value");
        populated.setAttachments(attachments);
        Map<String, Object> populatedCase = new TreeMap<>();
        populatedCase.put("name", "populated");
        attachments.put("caller", "visible");
        populatedCase.put("attachmentsSetterMutationVisibleToGetter",
            "visible".equals(populated.getAttachments().get("caller")));
        Map<String, String> getterAttachments = populated.getAttachments();
        getterAttachments.put("getter", "visible");
        populatedCase.put("attachmentsGetterMutationVisibleToSetterMap",
            "visible".equals(attachments.get("getter")));
        populatedCase.put("attachmentsGetterReturnsSetterMap", getterAttachments == attachments);
        Map<String, Object> messageQueue = new TreeMap<>();
        messageQueue.put("topic", populated.getMessageQueue().getTopic());
        messageQueue.put("brokerName", populated.getMessageQueue().getBrokerName());
        messageQueue.put("queueId", populated.getMessageQueue().getQueueId());
        populatedCase.put("messageQueue", messageQueue);
        populatedCase.put("mode", populated.getMode().name());
        populatedCase.put("attachments", new TreeMap<>(populated.getAttachments()));
        populatedCase.put("text", populated.toString());
        populatedCase.put("mutatedAttachments", new TreeMap<>(populated.getAttachments()));
        org.apache.rocketmq.common.message.MessageQueueAssignment sameQueueDifferentMetadata =
            new org.apache.rocketmq.common.message.MessageQueueAssignment();
        sameQueueDifferentMetadata.setMessageQueue(
            new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 2));
        sameQueueDifferentMetadata.setMode(org.apache.rocketmq.common.message.MessageRequestMode.PULL);
        populatedCase.put("equalsSameQueueDifferentMetadata", populated.equals(sameQueueDifferentMetadata));
        org.apache.rocketmq.common.message.MessageQueueAssignment differentQueue =
            new org.apache.rocketmq.common.message.MessageQueueAssignment();
        differentQueue.setMessageQueue(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 3));
        populatedCase.put("equalsDifferentQueue", populated.equals(differentQueue));
        populatedCase.put("messageQueueGetterReturnsSetterObject", populated.getMessageQueue() == queue);
        queue.setQueueId(3);
        populatedCase.put("messageQueueSetterMutationVisibleToGetter",
            populated.getMessageQueue().getQueueId() == 3);
        queue.setQueueId(2);
        populated.getMessageQueue().setQueueId(4);
        populatedCase.put("messageQueueGetterMutationVisibleToSetterObject", queue.getQueueId() == 4);
        Map<String, Object> mutatedQueue = new TreeMap<>();
        mutatedQueue.put("topic", populated.getMessageQueue().getTopic());
        mutatedQueue.put("brokerName", populated.getMessageQueue().getBrokerName());
        mutatedQueue.put("queueId", populated.getMessageQueue().getQueueId());
        populatedCase.put("mutatedMessageQueue", mutatedQueue);
        cases.add(populatedCase);
        return cases;
    }

    private static Map<String, Object> assignmentNullAttachmentApiCase() {
        org.apache.rocketmq.common.message.MessageQueueAssignment assignment =
            new org.apache.rocketmq.common.message.MessageQueueAssignment();
        Map<String, String> setterMap = new java.util.LinkedHashMap<>();
        setterMap.put(null, "null-key");
        setterMap.put("ordinary", "value");
        setterMap.put("null", "literal-null-key");
        assignment.setAttachments(setterMap);
        Map<String, String> getterMap = assignment.getAttachments();
        setterMap.put("caller", "visible");
        getterMap.put("getter", "visible");

        List<Object> entries = new ArrayList<>();
        for (Map.Entry<String, String> entry : getterMap.entrySet()) {
            Map<String, Object> row = new TreeMap<>();
            row.put("key", entry.getKey());
            row.put("value", entry.getValue());
            entries.add(row);
        }
        Map<String, Object> result = new TreeMap<>();
        result.put("setterAcceptedNullKey", true);
        result.put("getterReturnsSetterMap", getterMap == setterMap);
        result.put("setterMutationVisibleToGetter", "visible".equals(getterMap.get("caller")));
        result.put("getterMutationVisibleToSetter", "visible".equals(setterMap.get("getter")));
        result.put("entries", entries);
        result.put("serializedAttachments", com.alibaba.fastjson2.JSON.toJSONString(getterMap));
        result.put("serializedAssignment", com.alibaba.fastjson2.JSON.toJSONString(assignment));
        return result;
    }

    private static List<Object> messageRequestModeMethodCases() {
        List<Object> cases = new ArrayList<>();
        for (org.apache.rocketmq.common.message.MessageRequestMode mode :
            org.apache.rocketmq.common.message.MessageRequestMode.values()) {
            Map<String, Object> item = new TreeMap<>();
            item.put("constant", mode.name());
            item.put("getName", mode.getName());
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> consumerListCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(consumerListCase("missing", null, false));
        cases.add(consumerListCase("explicit-null", "null", true));
        cases.add(consumerListCase("empty", "[]", true));
        cases.add(consumerListCase("strings-with-duplicates", "[\"b\",\"a\",\"b\"]", true));
        cases.add(consumerListCase("scalar-elements", "[1,1.9,true,null]", true));
        cases.add(consumerListCase("high-precision-numeric-strings",
            "[9223372036854775807.123456789012345678901,1e2]", true));
        cases.add(consumerListCase("nested-elements", "[[1,\"x\"],{\"key\":\"value\"}]", true));
        cases.add(consumerListCase("object-field", "{\"id\":\"client\"}", true));
        cases.add(consumerListCase("string-field", "\"client\"", true));
        cases.add(consumerListCase("number-field", "7", true));
        cases.add(consumerListCase("fraction-field", "1.9", true));
        cases.add(consumerListCase("boolean-field", "true", true));
        cases.add(consumerListCase("empty-string-field", "\"\"", true));
        cases.add(consumerListRawCase("duplicate-field-last-wins",
            "{\"consumerIdList\":[\"first\"],\"consumerIdList\":[\"last\"]}"));
        cases.add(consumerListRawCase("duplicate-field-earlier-scalar",
            "{\"consumerIdList\":7,\"consumerIdList\":[\"last\"]}"));
        cases.add(consumerListRawCase("duplicate-field-last-scalar",
            "{\"consumerIdList\":[\"first\"],\"consumerIdList\":7}"));
        cases.add(consumerListRawCase("eof-trailing-text",
            "{\"consumerIdList\":[\"before\"]}\u001atrailing"));
        cases.add(consumerListRawCase("eof-trailing-json",
            "{\"consumerIdList\":[\"before\"]}\u001a{\"consumerIdList\":[\"after\"]}"));
        return cases;
    }

    private static List<Object> consumerListRootCases() {
        List<Object> cases = new ArrayList<>();
        String[][] roots = {
            {"null", "null"}, {"empty-object", "{}"}, {"empty-array", "[]"},
            {"string", "\"root\""}, {"integer", "7"}, {"fraction", "1.5"},
            {"boolean", "true"}
        };
        for (String[] root : roots) {
            String body = root[1];
            Map<String, Object> item = new TreeMap<>();
            item.put("name", "root-" + root[0]);
            item.put("input", com.alibaba.fastjson2.JSON.parse(body));
            item.put("rawInput", body);
            try {
                org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody decoded =
                    org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody.decode(
                        body.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody.class);
                item.put("outcome", "ACCEPTED");
                item.put("ids", decoded.getConsumerIdList());
                byte[] encoded = decoded.encode();
                item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                    new String(encoded, java.nio.charset.StandardCharsets.UTF_8)));
            } catch (RuntimeException error) {
                item.put("outcome", "REJECTED");
                item.put("error", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> lockBatchRequestBodyCases() {
        return batchQueueBodyCases("lock-request",
            org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody.class,
            new String[] {"consumerGroup", "clientId", "onlyThisBroker", "mqSet"}, "mqSet");
    }

    private static List<Object> unlockBatchRequestBodyCases() {
        return batchQueueBodyCases("unlock-request",
            org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody.class,
            new String[] {"consumerGroup", "clientId", "onlyThisBroker", "mqSet"}, "mqSet");
    }

    private static List<Object> lockBatchResponseBodyCases() {
        return batchQueueBodyCases("lock-response",
            org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody.class,
            new String[] {"lockOKMQSet"}, "lockOKMQSet");
    }

    private static List<Object> batchAckBodyInputCases() {
        List<Object> cases = new ArrayList<>();
        String[] tokens = {"null", "\"text\"", "7", "1.5", "true", "[]", "{}"};
        String[] names = {"null", "string", "integer", "fraction", "boolean", "array", "object"};
        cases.add(batchAckBodyInputCase("default-empty", "{}"));
        cases.add(batchAckBodyInputCase("acks-null", "{\"acks\":null}"));
        cases.add(batchAckBodyInputCase("acks-empty", "{\"acks\":[]}"));
        cases.add(batchAckBodyInputCase("acks-singleton-object", "{\"acks\":{}}"));
        cases.add(batchAckBodyInputCase("acks-null-member", "{\"acks\":[null]}"));
        cases.add(batchAckBodyInputCase("acks-duplicate-members", "{\"acks\":[{},{}]}"));
        for (int i = 0; i < tokens.length; i++) {
            cases.add(batchAckBodyInputCase("broker-name-" + names[i],
                "{\"brokerName\":" + tokens[i] + "}"));
            cases.add(batchAckBodyInputCase("acks-field-" + names[i],
                "{\"acks\":" + tokens[i] + "}"));
        }
        String[] fields = {"c", "t", "r", "so", "q", "rq", "pt", "it", "b"};
        for (String field : fields) {
            for (int i = 0; i < tokens.length; i++) {
                cases.add(batchAckBodyInputCase("ack-" + field + "-" + names[i],
                    "{\"acks\":[{\"" + field + "\":" + tokens[i] + "}]}"));
            }
        }
        cases.add(batchAckBodyInputCase("base64-valid-bits",
            "{\"acks\":[{\"b\":\"BQ==\"}]}"));
        cases.add(batchAckBodyInputCase("base64-empty-bits",
            "{\"acks\":[{\"b\":\"\"}]}"));
        String[][] base64Inputs = {
            {"base64-unpadded", "BQ"},
            {"base64-unpadded-three-sextets", "BQE"},
            {"base64-partial-padding", "BQ="},
            {"base64-noncanonical-two-sextet-pad-bits", "BR=="},
            {"base64-noncanonical-three-sextet-pad-bits", "BFB="},
            {"base64-internal-tab", "B\tQ=="},
            {"base64-single-character", "B"},
            {"base64-extra-padding", "BQ==="},
            {"base64-surrounding-spaces", " BQ== "},
            {"base64-internal-space", "B Q=="},
            {"base64-tab-escape", "\tBQ=="},
            {"base64-newline-escape", "\nBQ=="},
            {"base64-carriage-return-escape", "\rBQ=="},
            {"base64-nonbreaking-space", "\u00a0BQ=="},
            {"base64-url-safe-alphabet", "-w=="},
            {"base64-invalid-symbol", "B$=="}
        };
        for (String[] input : base64Inputs) {
            cases.add(batchAckBodyInputCase(input[0],
                "{\"acks\":[{\"b\":\"" + input[1] + "\"}]}"));
        }
        cases.add(batchAckBodyInputCase("duplicate-body-field-last-wins",
            "{\"brokerName\":\"first\",\"brokerName\":\"last\"}"));
        cases.add(batchAckBodyInputCase("duplicate-ack-field-last-wins",
            "{\"acks\":[{\"so\":1,\"so\":2}]}"));
        cases.add(batchAckBodyInputCase("unknown-fields-ignored",
            "{\"unknown\":7,\"acks\":[{\"unknown\":true}]}"));
        String[][] roots = {
            {"null", "null"}, {"empty-object", "{}"}, {"empty-array", "[]"},
            {"string", "\"root\""}, {"integer", "7"}, {"fraction", "1.5"},
            {"boolean", "true"}
        };
        for (String[] root : roots) {
            cases.add(batchAckBodyInputCase("root-" + root[0], root[1]));
        }
        return cases;
    }

    private static Map<String, Object> batchAckBodyInputCase(String name, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("rawInput", rawInput);
        try {
            org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody decoded =
                org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody.decode(
                    rawInput.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody.class);
            item.put("outcome", "ACCEPTED");
            item.put("encoded", decoded == null ? null : com.alibaba.fastjson2.JSON.parseObject(
                new String(decoded.encode(), java.nio.charset.StandardCharsets.UTF_8)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static Map<String, Object> batchAckApiCase() throws Exception {
        org.apache.rocketmq.remoting.protocol.body.BatchAck ack =
            new org.apache.rocketmq.remoting.protocol.body.BatchAck();
        Map<String, Object> item = new TreeMap<>();
        recordPublicApiInventory(item, "batchAck",
            org.apache.rocketmq.remoting.protocol.body.BatchAck.class);
        recordPublicApiInventory(item, "body",
            org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody.class);
        Map<String, Object> defaults = new TreeMap<>();
        defaults.put("consumerGroup", ack.getConsumerGroup());
        defaults.put("topic", ack.getTopic());
        defaults.put("retry", ack.getRetry());
        defaults.put("startOffset", ack.getStartOffset());
        defaults.put("queueId", ack.getQueueId());
        defaults.put("reviveQueueId", ack.getReviveQueueId());
        defaults.put("popTime", ack.getPopTime());
        defaults.put("invisibleTime", ack.getInvisibleTime());
        defaults.put("bitSet", ack.getBitSet());
        item.put("defaults", defaults);

        ack.setConsumerGroup("workers");
        ack.setTopic("orders");
        ack.setRetry("0");
        ack.setStartOffset(100L);
        ack.setQueueId(1);
        ack.setReviveQueueId(2);
        ack.setPopTime(10L);
        ack.setInvisibleTime(20L);
        java.util.BitSet bits = new java.util.BitSet();
        bits.set(0);
        bits.set(2);
        ack.setBitSet(bits);
        Map<String, Object> values = new TreeMap<>();
        values.put("consumerGroup", ack.getConsumerGroup());
        values.put("topic", ack.getTopic());
        values.put("retry", ack.getRetry());
        values.put("startOffset", ack.getStartOffset());
        values.put("queueId", ack.getQueueId());
        values.put("reviveQueueId", ack.getReviveQueueId());
        values.put("popTime", ack.getPopTime());
        values.put("invisibleTime", ack.getInvisibleTime());
        values.put("bitSetBase64", java.util.Base64.getEncoder().encodeToString(
            ack.getBitSet().toByteArray()));
        values.put("bitSetCardinality", ack.getBitSet().cardinality());
        values.put("bitSetLength", ack.getBitSet().length());
        values.put("bitSetQueries", List.of(
            ack.getBitSet().get(0), ack.getBitSet().get(1), ack.getBitSet().get(2), ack.getBitSet().get(5)));
        item.put("values", values);
        item.put("toString", ack.toString());
        org.apache.rocketmq.remoting.protocol.body.BatchAck equalAck =
            new org.apache.rocketmq.remoting.protocol.body.BatchAck();
        equalAck.setConsumerGroup(ack.getConsumerGroup());
        equalAck.setTopic(ack.getTopic());
        equalAck.setRetry(ack.getRetry());
        equalAck.setStartOffset(ack.getStartOffset());
        equalAck.setQueueId(ack.getQueueId());
        equalAck.setReviveQueueId(ack.getReviveQueueId());
        equalAck.setPopTime(ack.getPopTime());
        equalAck.setInvisibleTime(ack.getInvisibleTime());
        equalAck.setBitSet((java.util.BitSet) ack.getBitSet().clone());
        Map<String, Object> ackIdentity = new TreeMap<>();
        recordObjectIdentity(ackIdentity, ack, equalAck);
        item.put("ackObjectIdentity", ackIdentity);
        item.put("bitSetGetterReturnsAssignedObject", ack.getBitSet() == bits);
        bits.set(5);
        item.put("bitSetMutationVisibleThroughGetter", ack.getBitSet().get(5));
        item.put("bitSetMutationVisibleThroughSetterObject", bits.get(5));
        item.put("mutatedBitSetBase64", java.util.Base64.getEncoder().encodeToString(
            ack.getBitSet().toByteArray()));
        java.util.BitSet clonedBits = (java.util.BitSet) ack.getBitSet().clone();
        clonedBits.clear(0);
        clonedBits.set(7);
        Map<String, Object> bitSetClone = new TreeMap<>();
        bitSetClone.put("cloneIsDistinctObject", clonedBits != ack.getBitSet());
        bitSetClone.put("sourceBase64", java.util.Base64.getEncoder().encodeToString(
            ack.getBitSet().toByteArray()));
        bitSetClone.put("cloneBase64", java.util.Base64.getEncoder().encodeToString(
            clonedBits.toByteArray()));
        bitSetClone.put("sourceRetainsClearedBit", ack.getBitSet().get(0));
        bitSetClone.put("sourceExcludesCloneBit", !ack.getBitSet().get(7));
        item.put("bitSetClone", bitSetClone);
        java.util.BitSet preallocatedCloneSource = new java.util.BitSet(130);
        java.util.BitSet preallocatedClone = (java.util.BitSet) preallocatedCloneSource.clone();
        preallocatedClone.set(7);
        Map<String, Object> bitSetPreallocatedClone = new TreeMap<>();
        bitSetPreallocatedClone.put("cloneIsDistinctObject", preallocatedClone != preallocatedCloneSource);
        bitSetPreallocatedClone.put("sourceSize", preallocatedCloneSource.size());
        bitSetPreallocatedClone.put("cloneSize", preallocatedClone.size());
        bitSetPreallocatedClone.put("sourceBase64", java.util.Base64.getEncoder().encodeToString(
            preallocatedCloneSource.toByteArray()));
        bitSetPreallocatedClone.put("cloneBase64", java.util.Base64.getEncoder().encodeToString(
            preallocatedClone.toByteArray()));
        bitSetPreallocatedClone.put("sourceExcludesCloneBit", !preallocatedCloneSource.get(7));
        item.put("bitSetPreallocatedClone", bitSetPreallocatedClone);
        java.util.BitSet operations = (java.util.BitSet) ack.getBitSet().clone();
        operations.clear(2);
        operations.flip(7);
        Map<String, Object> bitSetOperations = new TreeMap<>();
        bitSetOperations.put("cardinality", operations.cardinality());
        bitSetOperations.put("length", operations.length());
        bitSetOperations.put("isEmpty", operations.isEmpty());
        bitSetOperations.put("get", List.of(operations.get(0), operations.get(2), operations.get(5), operations.get(7)));
        bitSetOperations.put("base64", java.util.Base64.getEncoder().encodeToString(operations.toByteArray()));
        item.put("bitSetOperations", bitSetOperations);
        Map<String, Object> negativeIndexErrors = new TreeMap<>();
        try { ack.getBitSet().get(-1); } catch (RuntimeException error) { negativeIndexErrors.put("get", error.getClass().getSimpleName()); }
        try { ack.getBitSet().set(-1); } catch (RuntimeException error) { negativeIndexErrors.put("set", error.getClass().getSimpleName()); }
        try { ack.getBitSet().clear(-1); } catch (RuntimeException error) { negativeIndexErrors.put("clear", error.getClass().getSimpleName()); }
        try { ack.getBitSet().flip(-1); } catch (RuntimeException error) { negativeIndexErrors.put("flip", error.getClass().getSimpleName()); }
        item.put("bitSetNegativeIndexErrors", negativeIndexErrors);
        java.util.BitSet wordBoundaryBits = new java.util.BitSet();
        for (int index : new int[] {7, 8, 63, 64, 127}) wordBoundaryBits.set(index);
        ack.setBitSet(wordBoundaryBits);
        Map<String, Object> wordBoundary = new TreeMap<>();
        wordBoundary.put("base64", java.util.Base64.getEncoder().encodeToString(
            ack.getBitSet().toByteArray()));
        wordBoundary.put("cardinality", ack.getBitSet().cardinality());
        wordBoundary.put("length", ack.getBitSet().length());
        item.put("bitSetWordBoundary", wordBoundary);

        java.util.BitSet ranged = new java.util.BitSet();
        ranged.set(3, 12);
        ranged.clear(5, 7);
        ranged.flip(8, 14);
        java.util.BitSet slice = ranged.get(2, 14);
        java.util.BitSet intersects = new java.util.BitSet();
        intersects.set(4);
        java.util.BitSet disjoint = new java.util.BitSet();
        disjoint.set(30);
        Map<String, Object> bitSetRanges = new TreeMap<>();
        bitSetRanges.put("base64", java.util.Base64.getEncoder().encodeToString(ranged.toByteArray()));
        bitSetRanges.put("cardinality", ranged.cardinality());
        bitSetRanges.put("length", ranged.length());
        bitSetRanges.put("sliceBase64", java.util.Base64.getEncoder().encodeToString(slice.toByteArray()));
        bitSetRanges.put("nextSetBit", List.of(
            ranged.nextSetBit(0), ranged.nextSetBit(5), ranged.nextSetBit(8), ranged.nextSetBit(14)));
        bitSetRanges.put("nextClearBit", List.of(
            ranged.nextClearBit(3), ranged.nextClearBit(8), ranged.nextClearBit(14)));
        bitSetRanges.put("previousSetBit", List.of(
            ranged.previousSetBit(13), ranged.previousSetBit(11), ranged.previousSetBit(2),
            ranged.previousSetBit(-1)));
        bitSetRanges.put("previousClearBit", List.of(
            ranged.previousClearBit(13), ranged.previousClearBit(3), ranged.previousClearBit(-1)));
        bitSetRanges.put("intersects", ranged.intersects(intersects));
        bitSetRanges.put("disjoint", ranged.intersects(disjoint));
        item.put("bitSetRanges", bitSetRanges);

        Map<String, Object> bitSetRangeErrors = new TreeMap<>();
        try { ranged.set(-1, 2); } catch (RuntimeException error) { bitSetRangeErrors.put("setNegative", error.getClass().getSimpleName()); }
        try { ranged.set(3, 2); } catch (RuntimeException error) { bitSetRangeErrors.put("setReversed", error.getClass().getSimpleName()); }
        try { ranged.clear(-1, 2); } catch (RuntimeException error) { bitSetRangeErrors.put("clearNegative", error.getClass().getSimpleName()); }
        try { ranged.flip(3, 2); } catch (RuntimeException error) { bitSetRangeErrors.put("flipReversed", error.getClass().getSimpleName()); }
        try { ranged.get(-1, 2); } catch (RuntimeException error) { bitSetRangeErrors.put("getNegative", error.getClass().getSimpleName()); }
        try { ranged.get(3, 2); } catch (RuntimeException error) { bitSetRangeErrors.put("getReversed", error.getClass().getSimpleName()); }
        try { ranged.nextSetBit(-1); } catch (RuntimeException error) { bitSetRangeErrors.put("nextSetNegative", error.getClass().getSimpleName()); }
        try { ranged.nextClearBit(-1); } catch (RuntimeException error) { bitSetRangeErrors.put("nextClearNegative", error.getClass().getSimpleName()); }
        try { ranged.previousSetBit(-2); } catch (RuntimeException error) { bitSetRangeErrors.put("previousSetBelowMinusOne", error.getClass().getSimpleName()); }
        try { ranged.previousClearBit(-2); } catch (RuntimeException error) { bitSetRangeErrors.put("previousClearBelowMinusOne", error.getClass().getSimpleName()); }
        item.put("bitSetRangeErrors", bitSetRangeErrors);

        java.util.BitSet rangeWordBoundary = new java.util.BitSet();
        rangeWordBoundary.set(63, 66);
        rangeWordBoundary.clear(64, 65);
        rangeWordBoundary.flip(62, 64);
        Map<String, Object> rangeWordBoundaryValues = new TreeMap<>();
        rangeWordBoundaryValues.put("base64", java.util.Base64.getEncoder().encodeToString(
            rangeWordBoundary.toByteArray()));
        rangeWordBoundaryValues.put("sliceBase64", java.util.Base64.getEncoder().encodeToString(
            rangeWordBoundary.get(61, 66).toByteArray()));
        rangeWordBoundaryValues.put("cardinality", rangeWordBoundary.cardinality());
        rangeWordBoundaryValues.put("length", rangeWordBoundary.length());
        item.put("bitSetRangeWordBoundary", rangeWordBoundaryValues);

        java.util.BitSet booleanOperations = new java.util.BitSet();
        booleanOperations.set(2, 6);
        booleanOperations.set(4, false);
        booleanOperations.set(8, true);
        booleanOperations.set(1, 3, true);
        booleanOperations.set(2, 5, false);
        Map<String, Object> bitSetBooleanOperations = new TreeMap<>();
        bitSetBooleanOperations.put("base64", java.util.Base64.getEncoder().encodeToString(
            booleanOperations.toByteArray()));
        bitSetBooleanOperations.put("longs", java.util.Arrays.stream(booleanOperations.toLongArray())
            .boxed().collect(java.util.stream.Collectors.toList()));
        bitSetBooleanOperations.put("cardinality", booleanOperations.cardinality());
        bitSetBooleanOperations.put("length", booleanOperations.length());
        bitSetBooleanOperations.put("hashCode", booleanOperations.hashCode());
        bitSetBooleanOperations.put("toString", booleanOperations.toString());
        bitSetBooleanOperations.put("stream", booleanOperations.stream().boxed()
            .collect(java.util.stream.Collectors.toList()));
        java.util.BitSet cleared = (java.util.BitSet) booleanOperations.clone();
        cleared.clear();
        bitSetBooleanOperations.put("clear", cleared.isEmpty()
            && cleared.cardinality() == 0 && cleared.length() == 0);
        item.put("bitSetBooleanOperations", bitSetBooleanOperations);

        java.util.BitSet bulkLeft = new java.util.BitSet();
        for (int index : new int[] {0, 2, 5, 64, 127}) bulkLeft.set(index);
        java.util.BitSet bulkRight = new java.util.BitSet();
        for (int index : new int[] {1, 2, 5, 64, 126}) bulkRight.set(index);
        Map<String, Object> bitSetBulkOperations = new TreeMap<>();
        java.util.BitSet andResult = (java.util.BitSet) bulkLeft.clone();
        andResult.and(bulkRight);
        bitSetBulkOperations.put("and", java.util.Base64.getEncoder().encodeToString(andResult.toByteArray()));
        java.util.BitSet orResult = (java.util.BitSet) bulkLeft.clone();
        orResult.or(bulkRight);
        bitSetBulkOperations.put("or", java.util.Base64.getEncoder().encodeToString(orResult.toByteArray()));
        java.util.BitSet xorResult = (java.util.BitSet) bulkLeft.clone();
        xorResult.xor(bulkRight);
        bitSetBulkOperations.put("xor", java.util.Base64.getEncoder().encodeToString(xorResult.toByteArray()));
        java.util.BitSet andNotResult = (java.util.BitSet) bulkLeft.clone();
        andNotResult.andNot(bulkRight);
        bitSetBulkOperations.put("andNot", java.util.Base64.getEncoder().encodeToString(andNotResult.toByteArray()));
        bitSetBulkOperations.put("intersects", bulkLeft.intersects(bulkRight));
        bitSetBulkOperations.put("equalClone", bulkLeft.equals(bulkLeft.clone()));
        bitSetBulkOperations.put("leftLongs", java.util.Arrays.stream(bulkLeft.toLongArray())
            .boxed().collect(java.util.stream.Collectors.toList()));
        bitSetBulkOperations.put("leftHashCode", bulkLeft.hashCode());
        bitSetBulkOperations.put("leftToString", bulkLeft.toString());
        bitSetBulkOperations.put("leftStream", bulkLeft.stream().boxed()
            .collect(java.util.stream.Collectors.toList()));
        item.put("bitSetBulkOperations", bitSetBulkOperations);

        org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody body =
            new org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody();
        item.put("bodyDefaults", java.util.Arrays.asList(body.getBrokerName(), body.getAcks()));
        body.setBrokerName("broker-a");
        List<org.apache.rocketmq.remoting.protocol.body.BatchAck> acks = new ArrayList<>();
        acks.add(ack);
        body.setAcks(acks);
        item.put("bodyBrokerName", body.getBrokerName());
        item.put("bodyAckCount", body.getAcks().size());
        item.put("bodyReturnsAssignedList", body.getAcks() == acks);
        item.put("bodyAssignedQueueIds", java.util.List.of(body.getAcks().get(0).getQueueId()));
        item.put("bodyContainsAssignedAck", body.getAcks().get(0) == ack);
        ack.setQueueId(5);
        item.put("externalAckObjectMutationVisible", body.getAcks().get(0).getQueueId() == 5);
        body.getAcks().get(0).setQueueId(6);
        item.put("ackObjectMutationVisibleThroughBody", ack.getQueueId() == 6);
        acks.clear();
        org.apache.rocketmq.remoting.protocol.body.BatchAck externalAck =
            new org.apache.rocketmq.remoting.protocol.body.BatchAck();
        externalAck.setQueueId(3);
        acks.add(externalAck);
        item.put("externalMutationVisible", body.getAcks().get(0).getQueueId() == 3);
        body.getAcks().clear();
        org.apache.rocketmq.remoting.protocol.body.BatchAck getterAck =
            new org.apache.rocketmq.remoting.protocol.body.BatchAck();
        getterAck.setQueueId(4);
        body.getAcks().add(getterAck);
        item.put("getterMutationVisibleToAssignedList", acks.get(0).getQueueId() == 4);
        item.put("bodyMutatedQueueIds", java.util.List.of(body.getAcks().get(0).getQueueId()));
        org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody equalValues =
            new org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody();
        equalValues.setBrokerName(body.getBrokerName());
        equalValues.setAcks(new ArrayList<>(acks));
        recordObjectIdentity(item, body, equalValues);
        body.setAcks(null);
        item.put("bodyNullAcks", body.getAcks() == null);
        return item;
    }

    private static List<Object> batchAckInheritedSerializationCases() {
        String[][] inputs = {
            {"default", "{}"},
            {"explicit-null", "{\"brokerName\":null,\"acks\":null}"},
            {"populated", "{\"brokerName\":\"broker-a\",\"acks\":[null,{"
                + "\"consumerGroup\":\"workers\",\"topic\":\"orders\",\"retry\":\"0\","
                + "\"startOffset\":100,\"queueId\":1,\"reviveQueueId\":2,"
                + "\"popTime\":10,\"invisibleTime\":20,\"bitSet\":\"BQ==\"}]}"}
        };
        List<Object> cases = new ArrayList<>();
        for (String[] input : inputs) {
            org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody value =
                org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody.decode(
                    input[1].getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody.class);
            String instanceJson = value.toJson();
            byte[] instanceBytes = value.encode();
            List<org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody> decodedList =
                org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody.decodeList(
                    com.alibaba.fastjson2.JSON.toJSONBytes(java.util.Collections.singletonList(value)),
                    org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("id", input[0]);
            item.put("rawInput", input[1]);
            item.put("instanceJson", instanceJson);
            item.put("instancePrettyJson", value.toJson(true));
            item.put("staticJson",
                org.apache.rocketmq.remoting.protocol.RemotingSerializable.toJson(value, false));
            item.put("staticPrettyJson",
                org.apache.rocketmq.remoting.protocol.RemotingSerializable.toJson(value, true));
            item.put("instanceEncodedJson",
                new String(instanceBytes, java.nio.charset.StandardCharsets.UTF_8));
            item.put("staticEncodedJson", new String(
                org.apache.rocketmq.remoting.protocol.RemotingSerializable.encode(value),
                java.nio.charset.StandardCharsets.UTF_8));
            item.put("featureEncodedJson", new String(value.encode(
                new com.alibaba.fastjson2.JSONWriter.Feature[0]), java.nio.charset.StandardCharsets.UTF_8));
            item.put("writerFeatureOverrides", writerFeatureOverrides(value));
            item.put("decodedJson",
                org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody.decode(
                    instanceBytes,
                    org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody.class).toJson());
            item.put("fromJsonJson",
                org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody.fromJson(
                    instanceJson,
                    org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody.class).toJson());
            item.put("decodedListJson", decodedList.get(0).toJson());
            item.put("decodedListCount", decodedList.size());
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> batchAckInheritedSerializationNullUtilities() {
        Class<org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody> type =
            org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody.class;
        byte[] jsonNull = "null".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Map<String, Object> item = new TreeMap<>();
        item.put("staticEncodeNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.encode((Object) null) == null);
        item.put("staticToJsonNull", org.apache.rocketmq.remoting.protocol.RemotingSerializable.toJson(null, false));
        item.put("decodeNullBytesReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(null, type) == null);
        item.put("decodeJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(jsonNull, type) == null);
        item.put("fromJsonNullInputReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.fromJson(null, type) == null);
        item.put("fromJsonJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.fromJson("null", type) == null);
        item.put("decodeListNullBytesReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(null, type) == null);
        item.put("decodeListJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(jsonNull, type) == null);
        List<org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody> nullMemberList =
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(
                "[null,{}]".getBytes(java.nio.charset.StandardCharsets.UTF_8), type);
        item.put("decodeListNullMemberCount", nullMemberList.size());
        item.put("decodeListPreservesNullMember", nullMemberList.get(0) == null);
        return item;
    }

    private static List<String> publicConstructorSignatures(Class<?> type) {
        return Arrays.stream(type.getDeclaredConstructors())
            .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
            .map(constructor -> type.getName() + "(" + Arrays.stream(constructor.getParameterTypes())
                .map(Class::getName).collect(java.util.stream.Collectors.joining(",")) + ")")
            .sorted()
            .collect(java.util.stream.Collectors.toList());
    }

    private static void recordBodyApiInventory(Map<String, Object> item, Class<?> type) {
        recordPublicApiInventory(item, "body", type);
    }

    private static void recordPublicApiInventory(Map<String, Object> item, String prefix, Class<?> type) {
        item.put(prefix + "PublicConstructors", publicConstructorSignatures(type));
        item.put(prefix + "DeclaredPublicMethods", declaredPublicMethodSignatures(type));
        item.put(prefix + "CallablePublicMethods", callablePublicMethodSignatures(type));
        item.put(prefix + "PublicFields", Arrays.stream(type.getFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(Field::getName)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
    }

    private static List<String> declaredPublicMethodSignatures(Class<?> type) {
        return Arrays.stream(type.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(RuntimeLookupFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList());
    }

    private static List<String> callablePublicMethodSignatures(Class<?> type) {
        return Arrays.stream(type.getMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getDeclaringClass() != Object.class)
            .map(RuntimeLookupFixtures::methodSignature)
            .distinct()
            .sorted()
            .collect(java.util.stream.Collectors.toList());
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getName() + " " + method.getDeclaringClass().getName() + "."
            + method.getName() + "(" + Arrays.stream(method.getParameterTypes())
            .map(Class::getName)
            .collect(java.util.stream.Collectors.joining(",")) + ")";
    }

    private static Map<String, Object> batchAckBitSetCapacityCase() {
        Map<String, Object> item = new TreeMap<>();
        item.put("constructorSizes", List.of(
            new java.util.BitSet().size(), new java.util.BitSet(0).size(),
            new java.util.BitSet(1).size(), new java.util.BitSet(64).size(),
            new java.util.BitSet(65).size()));
        java.util.BitSet grown = new java.util.BitSet();
        grown.set(63);
        int afterBit63 = grown.size();
        grown.set(64);
        int afterBit64 = grown.size();
        grown.set(128);
        int afterBit128 = grown.size();
        grown.clear(128);
        item.put("growthAndClearSizes", List.of(afterBit63, afterBit64, afterBit128, grown.size()));
        item.put("valueOfSizes", List.of(
            java.util.BitSet.valueOf(new byte[] {1, 0, 0}).size(),
            java.util.BitSet.valueOf(new long[] {1, 0}).size(),
            java.util.BitSet.valueOf(new byte[] {}).size()));
        try {
            new java.util.BitSet(-1);
        } catch (RuntimeException error) {
            item.put("negativeCapacityError", error.getClass().getSimpleName());
        }
        java.util.BitSet source = new java.util.BitSet();
        source.set(0);
        source.set(64);
        item.put("sliceSize", source.get(0, 65).size());
        item.put("emptySliceSize", source.get(3, 3).size());
        java.util.BitSet narrow = new java.util.BitSet(1);
        java.util.BitSet wide = new java.util.BitSet(128);
        narrow.set(4);
        wide.set(4);
        item.put("equalWithDifferentCapacities", narrow.size() != wide.size() && narrow.equals(wide));
        return item;
    }

    private static List<Object> consumerStatusBodyInputCases() {
        String key = "{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":1}";
        String escapedKey = com.alibaba.fastjson2.JSON.toJSONString(key);
        String fastjsonKey = "{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":1}";
        String[][] inputs = {
            {"omitted", "{}"},
            {"null-fields", "{\"messageQueueTable\":null,\"consumerTable\":null}"},
            {"empty-fields", "{\"messageQueueTable\":{},\"consumerTable\":{}}"},
            {"quoted-message-queue-key", "{\"messageQueueTable\":{" + escapedKey + ":9}}"},
            {"fastjson-message-queue-key", "{\"messageQueueTable\":{" + fastjsonKey + ":9}}"},
            {"fastjson-key-null-topic", "{\"messageQueueTable\":{{\"topic\":null,\"brokerName\":\"broker-a\",\"queueId\":1}:9}}"},
            {"fastjson-key-null-broker-name", "{\"messageQueueTable\":{{\"topic\":\"orders\",\"brokerName\":null,\"queueId\":1}:9}}"},
            {"fastjson-key-null-names", "{\"messageQueueTable\":{{\"topic\":null,\"brokerName\":null,\"queueId\":1}:9}}"},
            {"fastjson-key-empty-object", "{\"messageQueueTable\":{{}:9}}"},
            {"fastjson-key-null-queue-id", "{\"messageQueueTable\":{{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":null}:9}}"},
            {"fastjson-key-duplicate-topic", "{\"messageQueueTable\":{{\"topic\":\"first\",\"topic\":\"last\",\"brokerName\":\"broker-a\",\"queueId\":1}:9}}"},
            {"nested-quoted-message-queue-key", "{\"consumerTable\":{\"consumer-a\":{" + escapedKey + ":9}}}"},
            {"nested-fastjson-message-queue-key", "{\"consumerTable\":{\"consumer-a\":{" + fastjsonKey + ":9}}}"},
            {"nested-fastjson-key-null-topic", "{\"consumerTable\":{\"consumer-a\":{{\"topic\":null,\"brokerName\":\"broker-a\",\"queueId\":1}:9}}}"},
            {"nested-fastjson-key-null-broker-name", "{\"consumerTable\":{\"consumer-a\":{{\"topic\":\"orders\",\"brokerName\":null,\"queueId\":1}:9}}}"},
            {"nested-fastjson-key-null-names", "{\"consumerTable\":{\"consumer-a\":{{\"topic\":null,\"brokerName\":null,\"queueId\":1}:9}}}"},
            {"nested-fastjson-key-empty-object", "{\"consumerTable\":{\"consumer-a\":{{}:9}}}"},
            {"nested-fastjson-key-null-queue-id", "{\"consumerTable\":{\"consumer-a\":{{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":null}:9}}}"},
            {"nested-fastjson-key-duplicate-topic", "{\"consumerTable\":{\"consumer-a\":{{\"topic\":\"first\",\"topic\":\"last\",\"brokerName\":\"broker-a\",\"queueId\":1}:9}}}"},
            {"nested-null-map", "{\"consumerTable\":{\"consumer-a\":null}}"},
            {"duplicate-table-entry-last-wins", "{\"messageQueueTable\":{" + fastjsonKey + ":7," + fastjsonKey + ":9}}"},
            {"duplicate-field-last-wins", "{\"messageQueueTable\":{},\"messageQueueTable\":{" + fastjsonKey + ":9}}"},
            {"message-table-scalar", "{\"messageQueueTable\":7}"},
            {"consumer-table-scalar", "{\"consumerTable\":7}"},
            {"message-offset-null", "{\"messageQueueTable\":{" + escapedKey + ":null}}"},
            {"invalid-message-queue-key", "{\"messageQueueTable\":{\"not-a-queue\":9}}"},
            {"message-table-array", "{\"messageQueueTable\":[]}"},
            {"message-table-string", "{\"messageQueueTable\":\"not-a-table\"}"},
            {"message-table-boolean", "{\"messageQueueTable\":true}"},
            {"consumer-table-array", "{\"consumerTable\":[]}"},
            {"consumer-table-string", "{\"consumerTable\":\"not-a-table\"}"},
            {"consumer-table-boolean", "{\"consumerTable\":true}"},
            {"root-null", "null"},
            {"root-array", "[]"},
            {"root-number", "7"},
            {"root-string", "\"body\""},
            {"root-boolean", "true"}
        };
        String[][] offsetValues = {
            {"null-value", "null"},
            {"numeric-string", "\"9\""},
            {"numeric-string-plus", "\"+9\""},
            {"numeric-string-whitespace", "\" 9 \""},
            {"numeric-string-fraction", "\"1.5\""},
            {"numeric-string-plus-fraction", "\"+1.5\""},
            {"numeric-string-empty", "\"\""},
            {"numeric-string-exponent", "\"1e3\""},
            {"numeric-string-exponent-overflow", "\"1e400\""},
            {"numeric-string-exponent-underflow", "\"1e-400\""},
            {"fraction-negative", "-1.5"},
            {"fraction-min", "-9223372036854775808.0"},
            {"fraction-below-min", "-9223372036854775809.0"},
            {"fraction-max", "9223372036854775807.0"},
            {"fraction-above-max", "9223372036854775808.0"},
            {"exponent-overflow", "1e30"},
            {"exponent-underflow", "1e-30"},
            {"exponent-overflow-extreme", "1e400"},
            {"exponent-underflow-extreme", "1e-400"},
            {"exponent-negative-overflow-extreme", "-1e400"},
            {"exponent-negative-underflow-extreme", "-1e-400"},
            {"exponent-negative-zero", "-0e-1000"},
            {"fraction", "1.5"},
            {"boolean", "true"},
            {"boolean-false", "false"},
            {"array", "[]"},
            {"object", "{}"},
            {"exponent", "1e3"},
            {"long-max", "9223372036854775807"},
            {"long-min", "-9223372036854775808"},
            {"long-overflow-positive", "9223372036854775808"},
            {"long-overflow-negative", "-9223372036854775809"}
        };
        List<String[]> inputRows = new ArrayList<>(Arrays.asList(inputs));
        for (String[] value : offsetValues) {
            inputRows.add(new String[] {"message-offset-" + value[0],
                "{\"messageQueueTable\":{" + fastjsonKey + ":" + value[1] + "}}"});
            inputRows.add(new String[] {"consumer-offset-" + value[0],
                "{\"consumerTable\":{\"consumer-a\":{" + fastjsonKey + ":" + value[1] + "}}}"});
        }
        String[][] queueFieldShapes = {
            {"null", "null"}, {"string", "\"field-value\""}, {"integer", "7"},
            {"fraction", "1.5"}, {"boolean", "true"}, {"array", "[]"}, {"object", "{}"}
        };
        for (String field : new String[] {"topic", "brokerName", "queueId"}) {
            for (String[] shape : queueFieldShapes) {
                String value = shape[1];
                if ("queueId".equals(field) && "string".equals(shape[0])) value = "\"7\"";
                String topic = "topic".equals(field) ? value : "\"orders\"";
                String broker = "brokerName".equals(field) ? value : "\"broker-a\"";
                String queueId = "queueId".equals(field) ? value : "1";
                String queueKey = "{\"topic\":" + topic + ",\"brokerName\":" + broker
                    + ",\"queueId\":" + queueId + "}";
                inputRows.add(new String[] {"message-key-" + field + "-" + shape[0],
                    "{\"messageQueueTable\":{" + queueKey + ":9}}"});
                inputRows.add(new String[] {"consumer-key-" + field + "-" + shape[0],
                    "{\"consumerTable\":{\"consumer-a\":{" + queueKey + ":9}}}"});
            }
        }
        List<Object> cases = new ArrayList<>();
        for (String[] input : inputRows) {
            Map<String, Object> item = new TreeMap<>();
            item.put("name", input[0]);
            item.put("rawInput", input[1]);
            try {
                org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody decoded =
                    org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody.decode(
                        input[1].getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody.class);
                item.put("outcome", "ACCEPTED");
                item.put("messageQueueTable", consumerStatusQueueRows(decoded.getMessageQueueTable()));
                item.put("consumerTable", consumerStatusConsumerRows(decoded.getConsumerTable()));
                item.put("encodedBody", new String(decoded.encode(), java.nio.charset.StandardCharsets.UTF_8));
            } catch (RuntimeException error) {
                item.put("outcome", "REJECTED");
                item.put("error", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> resetOffsetBodyInputCases() {
        String key = "{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":1}";
        String escapedKey = com.alibaba.fastjson2.JSON.toJSONString(key);
        String[][] inputs = {
            {"omitted", "{}"},
            {"explicit-null", "{\"offsetTable\":null}"},
            {"empty-table", "{\"offsetTable\":{}}"},
            {"fastjson-message-queue-key", "{\"offsetTable\":{" + key + ":9}}"},
            {"quoted-message-queue-key", "{\"offsetTable\":{" + escapedKey + ":9}}"},
            {"fastjson-key-null-topic", "{\"offsetTable\":{{\"topic\":null,\"brokerName\":\"broker-a\",\"queueId\":1}:9}}"},
            {"fastjson-key-null-broker-name", "{\"offsetTable\":{{\"topic\":\"orders\",\"brokerName\":null,\"queueId\":1}:9}}"},
            {"fastjson-key-null-names", "{\"offsetTable\":{{\"topic\":null,\"brokerName\":null,\"queueId\":1}:9}}"},
            {"fastjson-key-empty-object", "{\"offsetTable\":{{}:9}}"},
            {"fastjson-key-null-queue-id", "{\"offsetTable\":{{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":null}:9}}"},
            {"fastjson-key-duplicate-topic", "{\"offsetTable\":{{\"topic\":\"first\",\"topic\":\"last\",\"brokerName\":\"broker-a\",\"queueId\":1}:9}}"},
            {"duplicate-table-entry-last-wins", "{\"offsetTable\":{" + key + ":7," + key + ":9}}"},
            {"duplicate-field-last-wins", "{\"offsetTable\":{},\"offsetTable\":{" + key + ":9}}"},
            {"unknown-field-ignored", "{\"unknown\":7,\"offsetTable\":{" + key + ":9}}"},
            {"table-scalar", "{\"offsetTable\":7}"},
            {"table-array", "{\"offsetTable\":[]}"},
            {"table-string", "{\"offsetTable\":\"not-a-table\"}"},
            {"table-boolean", "{\"offsetTable\":true}"},
            {"offset-null", "{\"offsetTable\":{" + key + ":null}}"},
            {"invalid-message-queue-key", "{\"offsetTable\":{\"not-a-queue\":9}}"},
            {"root-null", "null"},
            {"root-array", "[]"},
            {"root-number", "7"},
            {"root-string", "\"body\""},
            {"root-boolean", "true"}
        };
        String[][] offsetValues = {
            {"numeric-string", "\"9\""},
            {"numeric-string-plus", "\"+9\""},
            {"numeric-string-whitespace", "\" 9 \""},
            {"numeric-string-fraction", "\"1.5\""},
            {"numeric-string-plus-fraction", "\"+1.5\""},
            {"numeric-string-empty", "\"\""},
            {"numeric-string-exponent", "\"1e3\""},
            {"numeric-string-exponent-overflow", "\"1e400\""},
            {"numeric-string-exponent-underflow", "\"1e-400\""},
            {"fraction-negative", "-1.5"},
            {"fraction-min", "-9223372036854775808.0"},
            {"fraction-below-min", "-9223372036854775809.0"},
            {"fraction-max", "9223372036854775807.0"},
            {"fraction-above-max", "9223372036854775808.0"},
            {"exponent-overflow", "1e30"},
            {"exponent-underflow", "1e-30"},
            {"exponent-overflow-extreme", "1e400"},
            {"exponent-underflow-extreme", "1e-400"},
            {"exponent-negative-overflow-extreme", "-1e400"},
            {"exponent-negative-underflow-extreme", "-1e-400"},
            {"exponent-negative-zero", "-0e-1000"},
            {"fraction", "1.5"},
            {"boolean", "true"},
            {"boolean-false", "false"},
            {"array", "[]"},
            {"object", "{}"},
            {"exponent", "1e3"},
            {"long-max", "9223372036854775807"},
            {"long-min", "-9223372036854775808"},
            {"long-overflow-positive", "9223372036854775808"},
            {"long-overflow-negative", "-9223372036854775809"}
        };
        List<String[]> inputRows = new ArrayList<>(Arrays.asList(inputs));
        for (String[] value : offsetValues) {
            inputRows.add(new String[] {"offset-" + value[0],
                "{\"offsetTable\":{" + key + ":" + value[1] + "}}"});
        }
        String[][] queueFieldShapes = {
            {"null", "null"}, {"string", "\"field-value\""}, {"integer", "7"},
            {"fraction", "1.5"}, {"boolean", "true"}, {"array", "[]"}, {"object", "{}"}
        };
        for (String field : new String[] {"topic", "brokerName", "queueId"}) {
            for (String[] shape : queueFieldShapes) {
                String value = shape[1];
                if ("queueId".equals(field) && "string".equals(shape[0])) value = "\"7\"";
                String topic = "topic".equals(field) ? value : "\"orders\"";
                String broker = "brokerName".equals(field) ? value : "\"broker-a\"";
                String queueId = "queueId".equals(field) ? value : "1";
                String queueKey = "{\"topic\":" + topic + ",\"brokerName\":" + broker
                    + ",\"queueId\":" + queueId + "}";
                inputRows.add(new String[] {"queue-key-" + field + "-" + shape[0],
                    "{\"offsetTable\":{" + queueKey + ":9}}"});
            }
        }
        List<Object> cases = new ArrayList<>();
        for (String[] input : inputRows) {
            Map<String, Object> item = new TreeMap<>();
            item.put("name", input[0]);
            item.put("rawInput", input[1]);
            try {
                org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody decoded =
                    org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody.decode(
                        input[1].getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody.class);
                item.put("outcome", "ACCEPTED");
                item.put("offsetTable", consumerStatusQueueRows(decoded.getOffsetTable()));
                item.put("encodedBody", new String(decoded.encode(), java.nio.charset.StandardCharsets.UTF_8));
            } catch (RuntimeException error) {
                item.put("outcome", "REJECTED");
                item.put("error", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static Object consumerStatusQueueRows(
        Map<org.apache.rocketmq.common.message.MessageQueue, Long> table) {
        if (table == null) return null;
        List<Object> rows = new ArrayList<>();
        for (Map.Entry<org.apache.rocketmq.common.message.MessageQueue, Long> entry : table.entrySet()) {
            org.apache.rocketmq.common.message.MessageQueue queue = entry.getKey();
            Map<String, Object> row = new TreeMap<>();
            row.put("topic", queue == null ? null : queue.getTopic());
            row.put("brokerName", queue == null ? null : queue.getBrokerName());
            row.put("queueId", queue == null ? null : queue.getQueueId());
            row.put("offset", entry.getValue());
            rows.add(row);
        }
        rows.sort(Comparator.comparing(com.alibaba.fastjson2.JSON::toJSONString));
        return rows;
    }

    private static Object consumerStatusConsumerRows(
        Map<String, Map<org.apache.rocketmq.common.message.MessageQueue, Long>> table) {
        if (table == null) return null;
        Map<String, Object> result = new TreeMap<>();
        for (Map.Entry<String, Map<org.apache.rocketmq.common.message.MessageQueue, Long>> entry : table.entrySet()) {
            result.put(entry.getKey(), consumerStatusQueueRows(entry.getValue()));
        }
        return result;
    }

    private static List<Object> batchQueueBodyCases(
        String prefix, Class<?> bodyType, String[] fields, String queueSetField) {
        List<Object> cases = new ArrayList<>();
        String queue = "{\"topic\":\"orders\",\"brokerName\":\"broker\",\"queueId\":2}";
        String[][] shapes = {
            {"null", "null"}, {"string", "\"text\""}, {"integer", "7"},
            {"fraction", "1.5"}, {"boolean", "true"}, {"array", "[]"},
            {"object", queue}
        };
        for (String field : fields) {
            for (String[] shape : shapes) {
                String value = shape[1];
                if (field.equals(queueSetField) && "object".equals(shape[0])) value = queue;
                cases.add(batchQueueBodyCase(prefix + "-" + field + "-" + shape[0], bodyType,
                    "{\"" + field + "\":" + value + "}"));
            }
        }
        cases.add(batchQueueBodyCase(prefix + "-" + queueSetField + "-member-null", bodyType,
            "{\"" + queueSetField + "\":[null]}"));
        cases.add(batchQueueBodyCase(prefix + "-" + queueSetField + "-member-string", bodyType,
            "{\"" + queueSetField + "\":[\"text\"]}"));
        cases.add(batchQueueBodyCase(prefix + "-" + queueSetField + "-member-integer", bodyType,
            "{\"" + queueSetField + "\":[7]}"));
        cases.add(batchQueueBodyCase(prefix + "-" + queueSetField + "-member-fraction", bodyType,
            "{\"" + queueSetField + "\":[1.5]}"));
        cases.add(batchQueueBodyCase(prefix + "-" + queueSetField + "-member-boolean", bodyType,
            "{\"" + queueSetField + "\":[true]}"));
        cases.add(batchQueueBodyCase(prefix + "-" + queueSetField + "-member-array", bodyType,
            "{\"" + queueSetField + "\":[[]]}"));
        cases.add(batchQueueBodyCase(prefix + "-" + queueSetField + "-member-object", bodyType,
            "{\"" + queueSetField + "\":[" + queue + "]}"));
        cases.add(batchQueueBodyCase(prefix + "-" + queueSetField + "-duplicate-equal-members", bodyType,
            "{\"" + queueSetField + "\":[" + queue + "," + queue + "]}"));
        cases.add(batchQueueBodyCase(prefix + "-" + queueSetField + "-duplicate-null-members", bodyType,
            "{\"" + queueSetField + "\":[null,null]}"));
        cases.add(batchQueueBodyCase(prefix + "-" + queueSetField + "-empty-string", bodyType,
            "{\"" + queueSetField + "\":\"\"}"));
        cases.add(batchQueueBodyCase(prefix + "-" + queueSetField + "-duplicate-field-last-wins", bodyType,
            "{\"" + queueSetField + "\":[null],\"" + queueSetField + "\":[" + queue + "]}"));
        String[][] roots = {
            {"null", "null"}, {"empty-object", "{}"}, {"empty-array", "[]"},
            {"string", "\"root\""}, {"integer", "7"}, {"fraction", "1.5"},
            {"boolean", "true"}
        };
        for (String[] root : roots) {
            cases.add(batchQueueBodyCase(prefix + "-root-" + root[0], bodyType, root[1]));
        }
        return cases;
    }

    private static Map<String, Object> batchQueueBodyCase(String name, Class<?> bodyType, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("rawInput", rawInput);
        item.put("input", com.alibaba.fastjson2.JSON.parse(rawInput));
        try {
            byte[] input = rawInput.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            Object decoded = bodyType.getMethod("decode", byte[].class, Class.class)
                .invoke(null, input, bodyType);
            item.put("outcome", "ACCEPTED");
            if (decoded == null) {
                item.put("encoded", null);
            } else {
                byte[] encoded = (byte[]) bodyType.getMethod("encode").invoke(decoded);
                item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                    new String(encoded, java.nio.charset.StandardCharsets.UTF_8)));
            }
        } catch (java.lang.reflect.InvocationTargetException error) {
            Throwable cause = error.getCause();
            item.put("outcome", "REJECTED");
            item.put("error", cause == null ? error.getClass().getSimpleName() : cause.getClass().getSimpleName());
        } catch (ReflectiveOperationException | RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static Map<String, Object> consumerListCase(String name, String rawValue, boolean includeField) {
        String body = includeField ? "{\"consumerIdList\":" + rawValue + "}" : "{}";
        return consumerListRawCase(name, body);
    }

    private static Map<String, Object> consumerListRawCase(String name, String body) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("input", com.alibaba.fastjson2.JSON.parseObject(body));
        item.put("rawInput", body);
        try {
            org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody decoded =
                org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody.decode(
                    body.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody.class);
            item.put("outcome", "ACCEPTED");
            item.put("ids", decoded.getConsumerIdList());
            byte[] encoded = decoded.encode();
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                new String(encoded, java.nio.charset.StandardCharsets.UTF_8)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static void add(List<Object> cases, MQClientInstance runtime, String name, String brokers, String[] candidates) {
        String input = brokers == null ? null : "MISSING".equals(brokers)
            ? "{\"queueDatas\":[],\"filterServerTable\":{}}"
            : "{\"queueDatas\":[],\"brokerDatas\":" + brokers + ",\"filterServerTable\":{}}";
        if (input != null) {
            com.alibaba.fastjson2.JSONObject json = com.alibaba.fastjson2.JSON.parseObject(input);
            com.alibaba.fastjson2.JSONArray brokerRows = json.getJSONArray("brokerDatas");
            if (brokerRows != null) {
                for (Object broker : brokerRows) {
                    if (broker != null) ((com.alibaba.fastjson2.JSONObject) broker).put("cluster", "cluster");
                }
            }
            input = com.alibaba.fastjson2.JSON.toJSONString(json,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue);
        }
        runtime.getTopicRouteTable().clear();
        if (input != null) runtime.getTopicRouteTable().put("topic", com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class));
        String outcome = "OK";
        try {
            for (int i = 0; i < 128; i++) {
                String result = runtime.findBrokerAddrByTopic("topic");
                if (result == null) outcome = "NULL";
                else if (!Arrays.asList(candidates).contains(result)) throw new AssertionError("unexpected address: " + result);
            }
        } catch (RuntimeException error) { outcome = error.getClass().getSimpleName(); }
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name); item.put("input", input == null ? null : com.alibaba.fastjson2.JSON.parseObject(input));
        item.put("candidates", Arrays.asList(candidates)); item.put("outcome", outcome);
        cases.add(item);
    }

    private static List<Object> runtimeScheduledTaskCases() throws Exception {
        List<Object> cases = new ArrayList<>();
        String[] nameServers = {null, "127.0.0.1:9876", null, "127.0.0.1:9876"};
        String[] names = {"nameserver-absent", "nameserver-configured",
            "nameserver-absent-mutated-configured", "nameserver-configured-mutated-absent"};
        for (int index = 0; index < nameServers.length; index++) {
            ClientConfig config = new ClientConfig();
            config.setClientIP("192.0.2.6");
            config.setNamesrvAddr(nameServers[index]);
            config.setPollNameServerInterval(23);
            config.setHeartbeatBrokerInterval(47);
            config.setPersistConsumerOffsetInterval(59);
            MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
            if (index >= 2) {
                config.setNamesrvAddr(index == 2 ? "late-after-construction:9876" : null);
                config.setPollNameServerInterval(71);
                config.setHeartbeatBrokerInterval(83);
                config.setPersistConsumerOffsetInterval(97);
            }
            List<Object> calls = new ArrayList<>();
            try {
                java.lang.reflect.Field executorField = MQClientInstance.class
                    .getDeclaredField("scheduledExecutorService");
                executorField.setAccessible(true);
                ((java.util.concurrent.ScheduledExecutorService) executorField.get(runtime)).shutdownNow();
                java.util.concurrent.ScheduledExecutorService recorder =
                    (java.util.concurrent.ScheduledExecutorService) java.lang.reflect.Proxy.newProxyInstance(
                        RuntimeLookupFixtures.class.getClassLoader(),
                        new Class<?>[] {java.util.concurrent.ScheduledExecutorService.class},
                        (proxy, method, arguments) -> {
                            if ("scheduleAtFixedRate".equals(method.getName())) {
                                Map<String, Object> call = new TreeMap<>();
                                call.put("initialDelay", ((Number) arguments[1]).longValue());
                                call.put("period", ((Number) arguments[2]).longValue());
                                call.put("unit", arguments[3].toString());
                                calls.add(call);
                                return null;
                            }
                            if ("shutdownNow".equals(method.getName())) return new ArrayList<>();
                            if ("isShutdown".equals(method.getName())
                                || "isTerminated".equals(method.getName())) return false;
                            if ("awaitTermination".equals(method.getName())) return true;
                            if ("equals".equals(method.getName())) return proxy == arguments[0];
                            if ("hashCode".equals(method.getName())) return System.identityHashCode(proxy);
                            if ("toString".equals(method.getName())) return "recording scheduled executor";
                            return null;
                        });
                executorField.set(runtime, recorder);
                Method startScheduledTask = MQClientInstance.class.getDeclaredMethod("startScheduledTask");
                startScheduledTask.setAccessible(true);
                startScheduledTask.invoke(runtime);
                Map<String, Object> item = new TreeMap<>();
                item.put("name", names[index]);
                item.put("scheduleAtFixedRateCalls", calls);
                cases.add(item);
            } finally {
                Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                    org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
                cleanup.setAccessible(true);
                cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("fixture teardown"));
            }
        }
        return cases;
    }

    private static final class RuntimeLookupApi extends org.apache.rocketmq.client.impl.MQClientAPIImpl {
        TopicRouteData route;
        final List<String> refreshTopics = new ArrayList<>();
        final List<Map<String, Object>> consumerCalls = new ArrayList<>();
        final List<Map<String, Object>> assignmentCalls = new ArrayList<>();
        boolean failConsumer;
        boolean failAssignment;

        RuntimeLookupApi(ClientConfig config) {
            super(new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient) java.lang.reflect.Proxy.newProxyInstance(
                    RuntimeLookupFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> method.getReturnType() == boolean.class ? false : null));
        }

        @Override
        public TopicRouteData getTopicRouteInfoFromNameServer(String topic, long timeout) {
            refreshTopics.add(topic);
            return route;
        }

        @Override
        public List<String> getConsumerIdListByGroup(String address, String group, long timeout)
            throws org.apache.rocketmq.remoting.exception.RemotingConnectException,
                org.apache.rocketmq.remoting.exception.RemotingSendRequestException,
                org.apache.rocketmq.remoting.exception.RemotingTimeoutException,
                org.apache.rocketmq.client.exception.MQBrokerException, InterruptedException {
            Map<String, Object> call = new TreeMap<>();
            call.put("address", address);
            call.put("group", group);
            call.put("timeout", timeout);
            consumerCalls.add(call);
            if (failConsumer) throw new org.apache.rocketmq.remoting.exception.RemotingConnectException("fixture");
            return Arrays.asList("b", "a", "b");
        }

        @Override
        public java.util.Set<org.apache.rocketmq.common.message.MessageQueueAssignment> queryAssignment(
            String address, String topic, String group, String clientId, String strategy,
            org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel messageModel, long timeout)
            throws org.apache.rocketmq.remoting.exception.RemotingException,
                org.apache.rocketmq.client.exception.MQBrokerException, InterruptedException {
            Map<String, Object> call = new TreeMap<>();
            call.put("address", address);
            call.put("topic", topic);
            call.put("group", group);
            call.put("clientId", clientId);
            call.put("strategy", strategy);
            call.put("messageModel", messageModel == null ? null : messageModel.name());
            call.put("timeout", timeout);
            assignmentCalls.add(call);
            if (failAssignment) throw new org.apache.rocketmq.remoting.exception.RemotingConnectException("fixture");
            return java.util.Collections.emptySet();
        }
    }

    private static Map<String, Object> runtimeLookupInvocation() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("192.0.2.6");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        RuntimeLookupApi api = new RuntimeLookupApi(config);
        Map<String, Object> result = new TreeMap<>();
        try {
            api.route = com.alibaba.fastjson2.JSON.parseObject(
                "{\"queueDatas\":[],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"broker:10911\"}}],\"filterServerTable\":{}}",
                TopicRouteData.class);
            java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            field.setAccessible(true);
            field.set(runtime, api);

            org.apache.rocketmq.client.impl.consumer.MQConsumerInner nonNullConsumer =
                (org.apache.rocketmq.client.impl.consumer.MQConsumerInner) java.lang.reflect.Proxy.newProxyInstance(
                    RuntimeLookupFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class},
                    (proxy, method, values) -> method.getReturnType() == boolean.class ? false : null);
            result.put("nullGroupConsumerRegistrationAccepted", runtime.registerConsumer(null, nonNullConsumer));
            result.put("nullOwnerConsumerRegistrationAccepted", runtime.registerConsumer("null-owner", null));
            result.put("nullGroupProducerRegistrationAccepted", runtime.registerProducer(null, null));
            result.put("nullOwnerProducerRegistrationAccepted", runtime.registerProducer("null-owner", null));
            result.put("registrationTablesRemainEmpty",
                runtime.getConsumerTable().isEmpty() && runtime.getProducerTable().isEmpty());
            try {
                runtime.unregisterConsumer(null);
                result.put("nullConsumerUnregisterOutcome", "OK");
            } catch (RuntimeException error) {
                result.put("nullConsumerUnregisterOutcome", error.getClass().getSimpleName());
            }
            try {
                runtime.unregisterProducer(null);
                result.put("nullProducerUnregisterOutcome", "OK");
            } catch (RuntimeException error) {
                result.put("nullProducerUnregisterOutcome", error.getClass().getSimpleName());
            }

            List<String> consumerIds = runtime.findConsumerIdList("topic", "consumer");
            result.put("consumerOutcome", consumerIds);
            result.put("consumerCalls", new ArrayList<>(api.consumerCalls));
            api.consumerCalls.clear();
            result.put("nullGroupConsumerOutcome", runtime.findConsumerIdList("topic", null));
            result.put("nullGroupConsumerCalls", new ArrayList<>(api.consumerCalls));
            api.consumerCalls.clear();
            result.put("emptyGroupConsumerOutcome", runtime.findConsumerIdList("topic", ""));
            result.put("emptyGroupConsumerCalls", new ArrayList<>(api.consumerCalls));
            api.consumerCalls.clear();
            config.setMqClientApiTimeout(150);
            runtime.findConsumerIdList("topic", "consumer");
            result.put("consumerCallsAfterTimeoutMutation", new ArrayList<>(api.consumerCalls));
            api.consumerCalls.clear();
            java.util.Set<org.apache.rocketmq.common.message.MessageQueueAssignment> assignments =
                runtime.queryAssignment("topic", "consumer", "strategy",
                    org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.BROADCASTING, 3000);
            result.put("assignmentOutcome", assignments == null ? null : assignments.size());
            result.put("assignmentCalls", new ArrayList<>(api.assignmentCalls));
            result.put("refreshTopics", new ArrayList<>(api.refreshTopics));

            api.assignmentCalls.clear();
            result.put("nullAssignmentFieldsOutcome",
                runtime.queryAssignment("topic", null, null, null, 3000).size());
            result.put("nullAssignmentFieldsCalls", new ArrayList<>(api.assignmentCalls));
            api.assignmentCalls.clear();
            result.put("emptyAssignmentFieldsOutcome", runtime.queryAssignment("topic", "", "",
                org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.BROADCASTING, 3000).size());
            result.put("emptyAssignmentFieldsCalls", new ArrayList<>(api.assignmentCalls));
            api.assignmentCalls.clear();
            for (int timeout : new int[] {0, -1}) {
                runtime.queryAssignment("topic", "consumer", "strategy",
                    org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.BROADCASTING, timeout);
            }
            result.put("nonpositiveAssignmentCalls", new ArrayList<>(api.assignmentCalls));
            api.assignmentCalls.clear();

            api.refreshTopics.clear();
            api.consumerCalls.clear();
            result.put("emptyTopicConsumerOutcome", runtime.findConsumerIdList("", "consumer"));
            result.put("emptyTopicConsumerCalls", new ArrayList<>(api.consumerCalls));
            api.consumerCalls.clear();
            result.put("emptyTopicAssignmentOutcome", runtime.queryAssignment("", "consumer", "strategy",
                org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.BROADCASTING, 3000).size());
            result.put("emptyTopicAssignmentCalls", new ArrayList<>(api.assignmentCalls));
            result.put("emptyTopicRefreshTopics", new ArrayList<>(api.refreshTopics));
            api.consumerCalls.clear();
            api.assignmentCalls.clear();

            api.failConsumer = true;
            result.put("consumerFailureOutcome", runtime.findConsumerIdList("topic", "consumer"));
            api.failAssignment = true;
            try {
                runtime.queryAssignment("topic", "consumer", "strategy",
                    org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.BROADCASTING, 3000);
                result.put("assignmentFailureOutcome", "OK");
            } catch (Exception error) {
                result.put("assignmentFailureOutcome", error.getClass().getSimpleName());
            }

            runtime.getTopicRouteTable().clear();
            api.route = com.alibaba.fastjson2.JSON.parseObject(
                "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":{}}",
                TopicRouteData.class);
            api.refreshTopics.clear();
            api.consumerCalls.clear();
            api.assignmentCalls.clear();
            result.put("noBrokerConsumerOutcome", runtime.findConsumerIdList("topic", "consumer"));
            result.put("noBrokerAssignmentOutcome", runtime.queryAssignment("topic", "consumer", "strategy",
                org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.BROADCASTING, 3000));
            result.put("noBrokerRefreshTopics", new ArrayList<>(api.refreshTopics));
            result.put("noBrokerConsumerCalls", new ArrayList<>(api.consumerCalls));
            result.put("noBrokerAssignmentCalls", new ArrayList<>(api.assignmentCalls));
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            try { cleanup.invoke(runtime, original, new IllegalStateException("lookup invocation fixture teardown")); }
            finally { api.shutdown(); }
        }
        return result;
    }

    private static Map<String, Object> nullTopicLookupOutcome(java.util.concurrent.Callable<?> call) {
        Map<String, Object> result = new TreeMap<>();
        try {
            Object value = call.call();
            result.put("outcome", "OK");
            result.put("isNull", value == null);
        } catch (Exception error) {
            result.put("outcome", error.getClass().getSimpleName());
        }
        return result;
    }

    private static Map<String, Object> runtimeNullBrokerLookupRpcTopics() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("null-broker-lookup-rpc-topics");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        Map<String, Object> result = new TreeMap<>();
        try {
            result.put("consumerList", nullTopicLookupOutcome(
                () -> runtime.findConsumerIdList(null, "consumer")));
            result.put("assignment", nullTopicLookupOutcome(() -> runtime.queryAssignment(null, "consumer",
                "strategy", org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.BROADCASTING, 3000)));
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, runtime.getMQClientAPIImpl(),
                new IllegalStateException("null broker lookup RPC topic fixture teardown"));
        }
        return result;
    }

    private static final class VipCaptureApi extends org.apache.rocketmq.client.impl.MQClientAPIImpl {
        VipCaptureApi(ClientConfig config, List<String> addresses) {
            super(new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient) java.lang.reflect.Proxy.newProxyInstance(
                    RuntimeLookupFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            addresses.add((String) values[0]);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                                    org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null);
                            response.setBody(("{\"consumerIdList\":[],\"messageQueueAssignments\":[]}")
                                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        }
    }

    private static Map<String, Object> runtimeLookupVipMutation() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("192.0.2.6");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        List<String> consumerAddresses = new ArrayList<>();
        VipCaptureApi api = new VipCaptureApi(config, consumerAddresses);
        Map<String, Object> result = new TreeMap<>();
        try {
            TopicRouteData route = com.alibaba.fastjson2.JSON.parseObject(
                "{\"queueDatas\":[],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"127.0.0.1:10913\"}}],\"filterServerTable\":{}}",
                TopicRouteData.class);
            runtime.getTopicRouteTable().put("vip-topic", route);
            java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            field.setAccessible(true);
            field.set(runtime, api);

            runtime.findConsumerIdList("vip-topic", "consumer");
            result.put("consumerAddressVipDisabled", new ArrayList<>(consumerAddresses));
            consumerAddresses.clear();
            runtime.queryAssignment("vip-topic", "consumer", "strategy",
                org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.BROADCASTING, 3000);
            result.put("assignmentAddressVipDisabled", new ArrayList<>(consumerAddresses));
            consumerAddresses.clear();

            config.setVipChannelEnabled(true);
            runtime.findConsumerIdList("vip-topic", "consumer");
            result.put("consumerAddressVipEnabled", new ArrayList<>(consumerAddresses));
            consumerAddresses.clear();
            runtime.queryAssignment("vip-topic", "consumer", "strategy",
                org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.BROADCASTING, 3000);
            result.put("assignmentAddressVipEnabled", new ArrayList<>(consumerAddresses));
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            try { cleanup.invoke(runtime, original, new IllegalStateException("VIP mutation fixture teardown")); }
            finally { api.shutdown(); }
        }
        return result;
    }
}
