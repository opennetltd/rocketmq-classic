/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.rocketmq.common.topic.TopicValidator;

/** Invoked Java 5.5.1 public topic-validation and classification oracle. */
public final class TopicValidatorFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private TopicValidatorFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(render()) + "\n";
        if (verify) {
            if (!Files.exists(path) || !json.equals(Files.readString(path))) {
                throw new IllegalStateException("Java TopicValidator fixture drift: " + path);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
    }

    private static Map<String, Object> render() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        result.put("sourceCommit", SOURCE_COMMIT);
        result.put("clientArtifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        result.put("entrypoints", List.of(
            "TopicValidator.isTopicOrGroupIllegal",
            "TopicValidator.validateTopic",
            "TopicValidator.validateGroup",
            "TopicValidator.isSystemTopic",
            "TopicValidator.isNotAllowedSendTopic",
            "TopicValidator.addSystemTopic",
            "TopicValidator.getSystemTopicSet",
            "TopicValidator.getNotAllowedSendTopicSet",
            "TopicValidator.ValidateResult.<init>(boolean, String)",
            "TopicValidator.ValidateResult.isValid",
            "TopicValidator.ValidateResult.getRemark",
            "TopicValidator.ValidateResult.equals/hashCode (inherited Object identity)"));
        result.put("constants", constants());
        result.put("validCharacters", validCharacters());
        result.put("topicCases", topicCases());
        result.put("groupCases", groupCases());
        result.put("validateResultConstructorCases", validateResultConstructorCases());
        result.put("validateResultIdentity", validateResultIdentity());
        result.put("classification", classification());
        result.put("nullableBehavior", nullableBehavior());
        result.put("validCharMutation", validCharMutation());
        result.put("serverContacted", false);
        return result;
    }

    private static Map<String, Object> constants() {
        Map<String, Object> values = new TreeMap<>();
        values.put("AUTO_CREATE_TOPIC_KEY_TOPIC", TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC);
        values.put("RMQ_SYS_SCHEDULE_TOPIC", TopicValidator.RMQ_SYS_SCHEDULE_TOPIC);
        values.put("RMQ_SYS_BENCHMARK_TOPIC", TopicValidator.RMQ_SYS_BENCHMARK_TOPIC);
        values.put("RMQ_SYS_TRANS_HALF_TOPIC", TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC);
        values.put("RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC", TopicValidator.RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC);
        values.put("RMQ_SYS_TRACE_TOPIC", TopicValidator.RMQ_SYS_TRACE_TOPIC);
        values.put("RMQ_SYS_TRANS_OP_HALF_TOPIC", TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC);
        values.put("RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC", TopicValidator.RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC);
        values.put("RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC", TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
        values.put("RMQ_SYS_SELF_TEST_TOPIC", TopicValidator.RMQ_SYS_SELF_TEST_TOPIC);
        values.put("RMQ_SYS_OFFSET_MOVED_EVENT", TopicValidator.RMQ_SYS_OFFSET_MOVED_EVENT);
        values.put("RMQ_SYS_ROCKSDB_OFFSET_TOPIC", TopicValidator.RMQ_SYS_ROCKSDB_OFFSET_TOPIC);
        values.put("SYSTEM_TOPIC_PREFIX", TopicValidator.SYSTEM_TOPIC_PREFIX);
        values.put("SYNC_BROKER_MEMBER_GROUP_PREFIX", TopicValidator.SYNC_BROKER_MEMBER_GROUP_PREFIX);
        return values;
    }

    private static String validCharacters() {
        StringBuilder result = new StringBuilder();
        for (int code = 0; code < TopicValidator.VALID_CHAR_BIT_MAP.length; code++) {
            if (TopicValidator.VALID_CHAR_BIT_MAP[code]) result.append((char) code);
        }
        return result.toString();
    }

    private static List<Map<String, Object>> topicCases() {
        Object[][] inputs = {
            {"null", null},
            {"empty", ""},
            {"spaces", "  "},
            {"ascii_whitespace", "\t\r\n"},
            {"java_control_separator_whitespace", "\u001c"},
            {"unicode_whitespace", "\u2003"},
            {"nonbreaking_space", "\u00a0"},
            {"normal", "orders"},
            {"every_default_valid_punctuation", "%|_-"},
            {"illegal_dot", "bad.topic"},
            {"unicode", "租戶-orders"},
            {"normal_max", "t".repeat(127)},
            {"normal_too_long", "t".repeat(128)},
            {"illegal_precedes_length", "bad." + "t".repeat(128)},
            {"retry_max", "%RETRY%" + "r".repeat(248)},
            {"retry_too_long", "%RETRY%" + "r".repeat(249)},
            {"dlq_max", "%DLQ%" + "d".repeat(250)},
            {"dlq_too_long", "%DLQ%" + "d".repeat(251)}
        };
        List<Map<String, Object>> cases = new ArrayList<>();
        for (Object[] input : inputs) {
            String name = (String) input[0];
            String value = (String) input[1];
            cases.add(validationCase(name, value, TopicValidator.validateTopic(value)));
        }
        return cases;
    }

    private static List<Map<String, Object>> groupCases() {
        Object[][] inputs = {
            {"null", null},
            {"empty", ""},
            {"spaces", "  "},
            {"unicode_whitespace", "\u2003"},
            {"nonbreaking_space", "\u00a0"},
            {"normal", "workers"},
            {"valid_punctuation", "%|_-"},
            {"illegal_dot", "bad.group"},
            {"unicode", "租戶-workers"},
            {"group_max", "g".repeat(120)},
            {"group_too_long", "g".repeat(121)},
            {"illegal_precedes_length", "bad." + "g".repeat(121)}
        };
        List<Map<String, Object>> cases = new ArrayList<>();
        for (Object[] input : inputs) {
            String name = (String) input[0];
            String value = (String) input[1];
            cases.add(validationCase(name, value, TopicValidator.validateGroup(value)));
        }
        return cases;
    }

    private static Map<String, Object> validationCase(
        String name, String value, TopicValidator.ValidateResult validation) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("value", value);
        item.put("valid", validation.isValid());
        item.put("remark", validation.getRemark());
        item.put("illegal", value != null && TopicValidator.isTopicOrGroupIllegal(value));
        return item;
    }

    private static List<Map<String, Object>> validateResultConstructorCases() {
        return List.of(
            validateResultConstructorCase("valid_null_remark", true, null),
            validateResultConstructorCase("valid_empty_remark", true, ""),
            validateResultConstructorCase("valid_remark", true, "valid result"),
            validateResultConstructorCase("invalid_null_remark", false, null),
            validateResultConstructorCase("invalid_empty_remark", false, ""),
            validateResultConstructorCase("invalid_remark", false, "invalid result"));
    }

    private static Map<String, Object> validateResultConstructorCase(
        String name, boolean valid, String remark) {
        TopicValidator.ValidateResult result = new TopicValidator.ValidateResult(valid, remark);
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("valid", result.isValid());
        item.put("remarkIsNull", result.getRemark() == null);
        if (result.getRemark() != null) item.put("remark", result.getRemark());
        return item;
    }

    private static Map<String, Object> validateResultIdentity() throws Exception {
        TopicValidator.ValidateResult first = new TopicValidator.ValidateResult(true, "same");
        TopicValidator.ValidateResult second = new TopicValidator.ValidateResult(true, "same");
        Map<String, Object> result = new TreeMap<>();
        result.put("equalsDeclaredBy", first.getClass().getMethod("equals", Object.class)
            .getDeclaringClass().getName());
        result.put("hashCodeDeclaredBy", first.getClass().getMethod("hashCode")
            .getDeclaringClass().getName());
        result.put("sameObjectEqualsItself", first.equals(first));
        result.put("distinctEqualFieldsEquals", first.equals(second));
        return result;
    }

    private static Map<String, Object> classification() {
        String customSystemTopic = "RMQ_SYS_CUSTOM_PHASE16";
        String customBlockedTopic = "PHASE16_BLOCKED_TOPIC";
        TopicValidator.addSystemTopic(customSystemTopic);
        TopicValidator.getNotAllowedSendTopicSet().add(customBlockedTopic);

        Map<String, Object> result = new TreeMap<>();
        result.put("systemTopics", new TreeSet<>(TopicValidator.getSystemTopicSet()));
        result.put("blockedTopics", new TreeSet<>(TopicValidator.getNotAllowedSendTopicSet()));
        result.put("customSystemTopic", customSystemTopic);
        result.put("customSystemTopicIsSystem", TopicValidator.isSystemTopic(customSystemTopic));
        result.put("customSystemTopicIsBlocked", TopicValidator.isNotAllowedSendTopic(customSystemTopic));
        result.put("customBlockedTopic", customBlockedTopic);
        result.put("customBlockedTopicIsSystem", TopicValidator.isSystemTopic(customBlockedTopic));
        result.put("customBlockedTopicIsBlocked", TopicValidator.isNotAllowedSendTopic(customBlockedTopic));
        result.put("traceTopicIsBlocked", TopicValidator.isNotAllowedSendTopic(TopicValidator.RMQ_SYS_TRACE_TOPIC));
        result.put("scheduleTopicIsBlocked", TopicValidator.isNotAllowedSendTopic(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC));
        return result;
    }

    private static Map<String, Object> nullableBehavior() {
        Map<String, Object> result = new TreeMap<>();
        result.put("illegalNullException", nullFailure(() -> TopicValidator.isTopicOrGroupIllegal(null)));
        result.put("illegalSomeValid", TopicValidator.isTopicOrGroupIllegal("valid-topic"));
        result.put("illegalSomeInvalid", TopicValidator.isTopicOrGroupIllegal("invalid.topic"));
        result.put("systemNullBeforeAddException", nullFailure(() -> TopicValidator.isSystemTopic(null)));
        result.put("systemSomeKnownTopic", TopicValidator.isSystemTopic("TBW102"));
        result.put("systemSomePrefixTopic", TopicValidator.isSystemTopic("rmq_sys_custom"));
        result.put("systemSomeUnknownTopic", TopicValidator.isSystemTopic("custom-topic"));
        result.put("blockedNullBeforeAdd", TopicValidator.isNotAllowedSendTopic(null));
        result.put("blockedSomeKnownTopic", TopicValidator.isNotAllowedSendTopic("SCHEDULE_TOPIC_XXXX"));
        result.put("blockedSomeUnknownTopic", TopicValidator.isNotAllowedSendTopic("custom-topic"));

        TopicValidator.addSystemTopic(null);
        result.put("systemNullEntryPresentAfterAdd", TopicValidator.getSystemTopicSet().contains(null));
        result.put("systemNullAfterAddIsSystem", TopicValidator.isSystemTopic(null));

        TopicValidator.getNotAllowedSendTopicSet().add(null);
        result.put("blockedNullEntryPresentAfterAdd", TopicValidator.getNotAllowedSendTopicSet().contains(null));
        result.put("blockedNullAfterAdd", TopicValidator.isNotAllowedSendTopic(null));
        return result;
    }

    private static String nullFailure(Runnable operation) {
        try {
            operation.run();
            return "no-exception";
        } catch (NullPointerException exception) {
            return exception.getClass().getSimpleName();
        }
    }

    private static Map<String, Object> validCharMutation() {
        boolean previous = TopicValidator.VALID_CHAR_BIT_MAP['~'];
        TopicValidator.VALID_CHAR_BIT_MAP['~'] = true;
        boolean illegalAfterMutation = TopicValidator.isTopicOrGroupIllegal("custom~");
        TopicValidator.ValidateResult validationAfterMutation = TopicValidator.validateTopic("custom~");
        TopicValidator.VALID_CHAR_BIT_MAP['~'] = previous;
        Map<String, Object> result = new TreeMap<>();
        result.put("wasInitiallyValid", previous);
        result.put("illegalAfterAllowing", illegalAfterMutation);
        result.put("topicValidationAfterAllowing", validationAfterMutation.isValid());
        result.put("restored", TopicValidator.VALID_CHAR_BIT_MAP['~'] == previous);
        return result;
    }
}
