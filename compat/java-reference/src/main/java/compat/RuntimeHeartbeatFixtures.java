package compat;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData;

/** Invokes actual runtime registration and private heartbeat preparation.
 * The transport is never started; finally releases all constructed resources. */
public final class RuntimeHeartbeatFixtures {
    private RuntimeHeartbeatFixtures() { }

    private static List<Object> subscriptionFieldCases() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{}",
            "{\"topic\":null,\"subString\":null,\"tagsSet\":null,\"codeSet\":null,\"expressionType\":null,\"subVersion\":0}",
            "{\"topic\":\"T\",\"subString\":\"tag\",\"tagsSet\":[\"tag\"],\"codeSet\":[7],\"expressionType\":\"SQL92\",\"subVersion\":42}",
            "{\"topic\":\"null-members\",\"tagsSet\":[null,\"tag\",null],\"codeSet\":[null,7,null]}",
            "{\"topic\":\"filter-source\",\"filterClassSource\":\"source-first\",\"filterClassSource\":\"source-last\"}"
        }) {
            long before = System.currentTimeMillis();
            SubscriptionData value = com.alibaba.fastjson2.JSON.parseObject(input, SubscriptionData.class);
            long after = System.currentTimeMillis();
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(value));
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
            item.put("rawInput", input);
            item.put("topicNull", value.getTopic() == null);
            item.put("subStringNull", value.getSubString() == null);
            item.put("tagsNull", value.getTagsSet() == null);
            item.put("codeNull", value.getCodeSet() == null);
            item.put("expressionNull", value.getExpressionType() == null);
            item.put("filterClassSource", value.getFilterClassSource());
            item.put("filterClassSourceNull", value.getFilterClassSource() == null);
            item.put("tagsEmpty", value.getTagsSet() != null && value.getTagsSet().isEmpty());
            item.put("codeEmpty", value.getCodeSet() != null && value.getCodeSet().isEmpty());
            item.put("tagsCount", value.getTagsSet() == null ? null : value.getTagsSet().size());
            item.put("codeCount", value.getCodeSet() == null ? null : value.getCodeSet().size());
            item.put("tagsHasNull", value.getTagsSet() != null && value.getTagsSet().contains(null));
            item.put("codeHasNull", value.getCodeSet() != null && value.getCodeSet().contains(null));
            item.put("versionCurrent", value.getSubVersion() >= before && value.getSubVersion() <= after);
            item.put("versionZero", value.getSubVersion() == 0L);
            item.put("versionFortyTwo", value.getSubVersion() == 42L);
            item.put("encodedHasTopic", encoded.containsKey("topic"));
            item.put("encodedHasSubString", encoded.containsKey("subString"));
            item.put("encodedHasTags", encoded.containsKey("tagsSet"));
            item.put("encodedHasCode", encoded.containsKey("codeSet"));
            item.put("encodedHasExpression", encoded.containsKey("expressionType"));
            item.put("encodedHasFilterClassSource", encoded.containsKey("filterClassSource"));
            item.put("encodedFilterClassSource", encoded.get("filterClassSource"));
            if (value.getTopic() != null && value.getTopic().equals("null-members")) {
                item.put("encodedTags", encoded.get("tagsSet"));
                item.put("encodedCode", encoded.get("codeSet"));
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> subscriptionConstructorCases() {
        List<Object> cases = new ArrayList<>();
        SubscriptionData[] values = {
            new SubscriptionData(),
            new SubscriptionData("topic", "tag"),
            new SubscriptionData(null, null)
        };
        String[] names = {"no-argument", "two-argument", "two-null-arguments"};
        for (int i = 0; i < values.length; i++) {
            SubscriptionData value = values[i];
            Map<String, Object> item = new TreeMap<>();
            item.put("case", names[i]);
            item.put("topic", value.getTopic());
            item.put("subString", value.getSubString());
            item.put("tagsEmpty", value.getTagsSet() != null && value.getTagsSet().isEmpty());
            item.put("codeEmpty", value.getCodeSet() != null && value.getCodeSet().isEmpty());
            item.put("expressionType", value.getExpressionType());
            item.put("versionCurrent", value.getSubVersion() > 0);
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> subscriptionMethods() {
        Map<String, Object> result = new TreeMap<>();
        result.put("subAll", SubscriptionData.SUB_ALL);
        List<Object> values = new ArrayList<>();
        for (String[] input : new String[][] {
            {"defaults", "{\"topic\":\"topic\",\"subString\":\"tag\",\"subVersion\":42}"},
            {"nullable", "{\"classFilterMode\":true,\"topic\":null,\"subString\":null,\"tagsSet\":null,\"codeSet\":null,\"subVersion\":-9,\"expressionType\":null}"},
            {"unicode", "{\"topic\":\"🚀\",\"subString\":\"Ω\",\"tagsSet\":[\"tag\"],\"codeSet\":[7],\"subVersion\":9223372036854775807,\"expressionType\":\"SQL92\"}"},
            {"sets", "{\"topic\":\"topic\",\"subString\":\"tagA||tagB\",\"tagsSet\":[\"alpha\",\"beta\"],\"codeSet\":[7,11],\"subVersion\":0,\"expressionType\":\"TAG\"}"},
            {"null-members", "{\"topic\":\"null-members\",\"subString\":\"tag\",\"tagsSet\":[null,\"alpha\"],\"codeSet\":[null,7],\"subVersion\":0,\"expressionType\":\"TAG\"}"}
        }) {
            SubscriptionData value = com.alibaba.fastjson2.JSON.parseObject(input[1], SubscriptionData.class);
            value.setFilterClassSource("ignored-in-text-and-equality");
            Map<String, Object> item = new TreeMap<>();
            item.put("case", input[0]);
            item.put("rawInput", input[1]);
            item.put("hashCode", value.hashCode());
            item.put("toString", value.toString());
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(value));
            item.put("encodedHasFilterClassSource", encoded.containsKey("filterClassSource"));
            item.put("encodedFilterClassSource", encoded.get("filterClassSource"));
            values.add(item);
        }
        result.put("values", values);

        String base = "{\"classFilterMode\":false,\"topic\":\"topic\",\"subString\":\"tag\","
            + "\"tagsSet\":[\"alpha\",\"beta\"],\"codeSet\":[7,11],\"subVersion\":42,"
            + "\"expressionType\":\"SQL92\"}";
        String nullSets = base.replace("\"tagsSet\":[\"alpha\",\"beta\"]", "\"tagsSet\":null")
            .replace("\"codeSet\":[7,11]", "\"codeSet\":null");
        String emptySets = base.replace("\"tagsSet\":[\"alpha\",\"beta\"]", "\"tagsSet\":[]")
            .replace("\"codeSet\":[7,11]", "\"codeSet\":[]");
        List<Object> equalCases = new ArrayList<>();
        for (String[] input : new String[][] {
            {"filter-class-source-ignored", base, base, "source-a", "source-b"},
            {"sub-version-compared-but-not-hashed", base, base.replace("\"subVersion\":42", "\"subVersion\":43"), "source", "source"},
            {"null-versus-empty-sets", nullSets, emptySets, "source", "source"},
            {"class-filter-mode", base, base.replace("\"classFilterMode\":false", "\"classFilterMode\":true"), "source", "source"},
            {"topic", base, base.replace("\"topic\":\"topic\"", "\"topic\":\"other\""), "source", "source"},
            {"expression-type", base, base.replace("\"expressionType\":\"SQL92\"", "\"expressionType\":\"TAG\""), "source", "source"},
            {"tags-set", base, base.replace("\"beta\"", "\"gamma\""), "source", "source"},
            {"tags-null-member", base, base.replace("\"tagsSet\":[\"alpha\",\"beta\"]", "\"tagsSet\":[\"alpha\",null]"), "source", "source"},
            {"code-set", base, base.replace("\"codeSet\":[7,11]", "\"codeSet\":[7,12]"), "source", "source"},
            {"code-null-member", base, base.replace("\"codeSet\":[7,11]", "\"codeSet\":[7,null]"), "source", "source"},
            {"subscription-string", base, base.replace("\"subString\":\"tag\"", "\"subString\":\"other\""), "source", "source"}
        }) {
            SubscriptionData left = com.alibaba.fastjson2.JSON.parseObject(input[1], SubscriptionData.class);
            SubscriptionData right = com.alibaba.fastjson2.JSON.parseObject(input[2], SubscriptionData.class);
            left.setFilterClassSource(input[3]);
            right.setFilterClassSource(input[4]);
            Map<String, Object> item = new TreeMap<>();
            item.put("case", input[0]);
            item.put("leftInput", input[1]);
            item.put("rightInput", input[2]);
            item.put("leftFilterClassSource", input[3]);
            item.put("rightFilterClassSource", input[4]);
            item.put("equal", left.equals(right));
            item.put("leftHashCode", left.hashCode());
            item.put("rightHashCode", right.hashCode());
            equalCases.add(item);
        }
        result.put("equalCases", equalCases);

        List<Object> compareCases = new ArrayList<>();
        for (String[] input : new String[][] {
            {"null-concatenation", "{\"topic\":null,\"subString\":null}", "{\"topic\":\"null\",\"subString\":\"null\"}"},
            {"utf16-order", "{\"topic\":\"\uE000\",\"subString\":\"x\"}", "{\"topic\":\"🚀\",\"subString\":\"x\"}"},
            {"delimiter-collision", "{\"topic\":\"A@\",\"subString\":\"B\"}", "{\"topic\":\"A\",\"subString\":\"@B\"}"},
            {"prefix", "{\"topic\":\"a\",\"subString\":\"\"}", "{\"topic\":\"a\",\"subString\":\"x\"}"}
        }) {
            SubscriptionData left = com.alibaba.fastjson2.JSON.parseObject(input[1], SubscriptionData.class);
            SubscriptionData right = com.alibaba.fastjson2.JSON.parseObject(input[2], SubscriptionData.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("case", input[0]);
            item.put("leftInput", input[1]);
            item.put("rightInput", input[2]);
            item.put("comparison", left.compareTo(right));
            compareCases.add(item);
        }
        result.put("compareCases", compareCases);

        SubscriptionData compareReceiver = new SubscriptionData("left", "tag");
        try {
            compareReceiver.compareTo(null);
            result.put("compareNullOtherException", null);
        } catch (Throwable error) {
            result.put("compareNullOtherException", error.getClass().getSimpleName());
        }
        return result;
    }

    private static List<String> sortedStringSet(java.util.Set<String> values) {
        if (values == null) return null;
        List<String> result = new ArrayList<>(values);
        result.sort(Comparator.nullsFirst(Comparator.naturalOrder()));
        return result;
    }

    private static List<Integer> sortedIntegerSet(java.util.Set<Integer> values) {
        if (values == null) return null;
        List<Integer> result = new ArrayList<>(values);
        result.sort(Comparator.nullsFirst(Comparator.naturalOrder()));
        return result;
    }

    private static Map<String, Object> subscriptionApiValues(SubscriptionData value) {
        Map<String, Object> result = new TreeMap<>();
        result.put("classFilterMode", value.isClassFilterMode());
        result.put("topic", value.getTopic());
        result.put("subString", value.getSubString());
        result.put("tagsSet", sortedStringSet(value.getTagsSet()));
        result.put("codeSet", sortedIntegerSet(value.getCodeSet()));
        result.put("subVersion", value.getSubVersion());
        result.put("expressionType", value.getExpressionType());
        result.put("filterClassSource", value.getFilterClassSource());
        return result;
    }

    private static List<Object> subscriptionSetApiValues(java.util.Set<SubscriptionData> values) {
        if (values == null) return null;
        List<Object> result = new ArrayList<>();
        for (SubscriptionData value : values)
            result.add(value == null ? null : subscriptionApiValues(value));
        return result;
    }

    private static Map<String, Object> subscriptionDataApiCase() {
        SubscriptionData value = new SubscriptionData();
        long before = System.currentTimeMillis();
        long defaultVersion = value.getSubVersion();
        long after = System.currentTimeMillis();
        Map<String, Object> item = new TreeMap<>();
        Map<String, Object> defaults = subscriptionApiValues(value);
        defaults.remove("subVersion");
        defaults.put("subVersionCurrent", defaultVersion >= before && defaultVersion <= after);
        item.put("defaults", defaults);

        value.setClassFilterMode(true);
        value.setTopic("orders");
        value.setSubString("*");
        java.util.Set<String> tags = new java.util.LinkedHashSet<>();
        tags.add("gold");
        value.setTagsSet(tags);
        java.util.Set<Integer> codes = new java.util.LinkedHashSet<>();
        codes.add(7);
        value.setCodeSet(codes);
        value.setSubVersion(42L);
        value.setExpressionType("SQL92");
        value.setFilterClassSource("source-a");
        item.put("values", subscriptionApiValues(value));
        item.put("getterReturnsAssignedTagsSet", value.getTagsSet() == tags);
        item.put("getterReturnsAssignedCodeSet", value.getCodeSet() == codes);
        tags.add("silver");
        item.put("externalTagMutationVisible", value.getTagsSet().contains("silver"));
        value.getTagsSet().add("bronze");
        item.put("tagGetterMutationVisibleToAssignedSet", tags.contains("bronze"));
        item.put("mutatedTags", sortedStringSet(value.getTagsSet()));
        codes.add(9);
        item.put("externalCodeMutationVisible", value.getCodeSet().contains(9));
        value.getCodeSet().add(11);
        item.put("codeGetterMutationVisibleToAssignedSet", codes.contains(11));
        item.put("mutatedCodes", sortedIntegerSet(value.getCodeSet()));

        value.setClassFilterMode(false);
        value.setTopic(null);
        value.setSubString(null);
        value.setTagsSet(null);
        value.setCodeSet(null);
        value.setSubVersion(0L);
        value.setExpressionType(null);
        value.setFilterClassSource(null);
        item.put("nullSettersRemainNull", value.getTopic() == null && value.getSubString() == null
            && value.getTagsSet() == null && value.getCodeSet() == null
            && value.getExpressionType() == null && value.getFilterClassSource() == null);
        Map<String, Object> primitiveAfterReset = new TreeMap<>();
        primitiveAfterReset.put("classFilterMode", value.isClassFilterMode());
        primitiveAfterReset.put("subVersion", value.getSubVersion());
        item.put("primitiveAfterReset", primitiveAfterReset);
        return item;
    }

    private static Map<String, Object> producerDataApiValues(ProducerData value) {
        Map<String, Object> result = new TreeMap<>();
        result.put("groupName", value.getGroupName());
        return result;
    }

    private static Map<String, Object> producerDataApiCase() {
        ProducerData value = new ProducerData();
        Map<String, Object> item = new TreeMap<>();
        item.put("defaults", producerDataApiValues(value));
        value.setGroupName("producers");
        item.put("values", producerDataApiValues(value));
        value.setGroupName(null);
        item.put("nullSetterRemainsNull", value.getGroupName() == null);
        return item;
    }

    private static Map<String, Object> consumerDataApiValues(ConsumerData value) {
        Map<String, Object> result = new TreeMap<>();
        result.put("groupName", value.getGroupName());
        result.put("consumeType", value.getConsumeType() == null ? null : value.getConsumeType().name());
        result.put("messageModel", value.getMessageModel() == null ? null : value.getMessageModel().name());
        result.put("consumeFromWhere", value.getConsumeFromWhere() == null ? null : value.getConsumeFromWhere().name());
        result.put("subscriptionDataSet", subscriptionSetApiValues(value.getSubscriptionDataSet()));
        result.put("unitMode", value.isUnitMode());
        return result;
    }

    private static Map<String, Object> consumerDataApiCase() {
        ConsumerData value = new ConsumerData();
        Map<String, Object> item = new TreeMap<>();
        item.put("defaults", consumerDataApiValues(value));
        value.setGroupName("workers");
        value.setConsumeType(ConsumeType.CONSUME_ACTIVELY);
        value.setMessageModel(MessageModel.CLUSTERING);
        value.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        SubscriptionData subscription = new SubscriptionData();
        subscription.setTopic("orders");
        subscription.setSubString("*");
        subscription.setSubVersion(42L);
        java.util.Set<SubscriptionData> subscriptions = new java.util.LinkedHashSet<>();
        subscriptions.add(subscription);
        value.setSubscriptionDataSet(subscriptions);
        value.setUnitMode(true);
        item.put("values", consumerDataApiValues(value));
        item.put("getterReturnsAssignedSubscriptionSet", value.getSubscriptionDataSet() == subscriptions);
        item.put("getterReturnsAssignedSubscriptionElement",
            value.getSubscriptionDataSet().iterator().next() == subscription);
        subscription.setSubVersion(43L);
        item.put("callerSubscriptionMutationVisibleThroughGetter",
            value.getSubscriptionDataSet().iterator().next().getSubVersion() == 43L);
        value.getSubscriptionDataSet().iterator().next().setSubVersion(44L);
        item.put("getterSubscriptionMutationVisibleToCaller", subscription.getSubVersion() == 44L);
        SubscriptionData externalSubscription = new SubscriptionData();
        externalSubscription.setTopic("payments");
        externalSubscription.setSubString("*");
        subscriptions.add(externalSubscription);
        item.put("externalSubscriptionMutationVisible", value.getSubscriptionDataSet().size() == 2);
        SubscriptionData getterSubscription = new SubscriptionData();
        getterSubscription.setTopic("refunds");
        getterSubscription.setSubString("*");
        value.getSubscriptionDataSet().add(getterSubscription);
        item.put("subscriptionGetterMutationVisibleToAssignedSet", subscriptions.size() == 3);
        item.put("mutatedSubscriptionDataSetSize", value.getSubscriptionDataSet().size());
        value.setGroupName(null);
        value.setConsumeType(null);
        value.setMessageModel(null);
        value.setConsumeFromWhere(null);
        value.setSubscriptionDataSet(null);
        value.setUnitMode(false);
        item.put("nullSettersRemainNull", value.getGroupName() == null && value.getConsumeType() == null
            && value.getMessageModel() == null && value.getConsumeFromWhere() == null
            && value.getSubscriptionDataSet() == null && !value.isUnitMode());
        return item;
    }

    private static List<Object> producerSetApiValues(java.util.Set<ProducerData> values) {
        if (values == null) return null;
        List<Object> result = new ArrayList<>();
        for (ProducerData value : values)
            result.add(value == null ? null : producerDataApiValues(value));
        return result;
    }

    private static List<Object> consumerSetApiValues(java.util.Set<ConsumerData> values) {
        if (values == null) return null;
        List<Object> result = new ArrayList<>();
        for (ConsumerData value : values)
            result.add(value == null ? null : consumerDataApiValues(value));
        return result;
    }

    private static Map<String, Object> heartbeatDataApiValues(HeartbeatData value) {
        Map<String, Object> result = new TreeMap<>();
        result.put("clientID", value.getClientID());
        result.put("producerDataSet", producerSetApiValues(value.getProducerDataSet()));
        result.put("consumerDataSet", consumerSetApiValues(value.getConsumerDataSet()));
        result.put("heartbeatFingerprint", value.getHeartbeatFingerprint());
        result.put("withoutSub", value.isWithoutSub());
        return result;
    }

    private static Map<String, Object> heartbeatDataApiCase() {
        HeartbeatData value = new HeartbeatData();
        Map<String, Object> item = new TreeMap<>();
        item.put("defaults", heartbeatDataApiValues(value));
        value.setClientID("client-a");
        ProducerData producer = new ProducerData();
        producer.setGroupName("producers");
        java.util.Set<ProducerData> producers = new java.util.LinkedHashSet<>();
        producers.add(producer);
        value.setProducerDataSet(producers);
        ConsumerData consumer = new ConsumerData();
        consumer.setGroupName("workers");
        java.util.Set<ConsumerData> consumers = new java.util.LinkedHashSet<>();
        consumers.add(consumer);
        value.setConsumerDataSet(consumers);
        value.setHeartbeatFingerprint(7);
        value.setWithoutSub(true);
        item.put("values", heartbeatDataApiValues(value));
        item.put("getterReturnsAssignedProducerSet", value.getProducerDataSet() == producers);
        item.put("getterReturnsAssignedConsumerSet", value.getConsumerDataSet() == consumers);
        item.put("getterReturnsAssignedProducer", value.getProducerDataSet().iterator().next() == producer);
        item.put("getterReturnsAssignedConsumer", value.getConsumerDataSet().iterator().next() == consumer);
        producer.setGroupName("caller-producer-update");
        item.put("callerProducerMutationVisibleThroughGetter",
            "caller-producer-update".equals(value.getProducerDataSet().iterator().next().getGroupName()));
        value.getProducerDataSet().iterator().next().setGroupName("getter-producer-update");
        item.put("getterProducerMutationVisibleToCaller",
            "getter-producer-update".equals(producer.getGroupName()));
        consumer.setGroupName("caller-consumer-update");
        item.put("callerConsumerMutationVisibleThroughGetter",
            "caller-consumer-update".equals(value.getConsumerDataSet().iterator().next().getGroupName()));
        value.getConsumerDataSet().iterator().next().setGroupName("getter-consumer-update");
        item.put("getterConsumerMutationVisibleToCaller",
            "getter-consumer-update".equals(consumer.getGroupName()));
        ProducerData externalProducer = new ProducerData();
        externalProducer.setGroupName("producers-b");
        producers.add(externalProducer);
        item.put("externalProducerMutationVisible", value.getProducerDataSet().size() == 2);
        ProducerData getterProducer = new ProducerData();
        getterProducer.setGroupName("producers-c");
        value.getProducerDataSet().add(getterProducer);
        item.put("producerGetterMutationVisibleToAssignedSet", producers.size() == 3);
        ConsumerData externalConsumer = new ConsumerData();
        externalConsumer.setGroupName("consumers");
        consumers.add(externalConsumer);
        item.put("externalConsumerMutationVisible", value.getConsumerDataSet().size() == 2);
        ConsumerData getterConsumer = new ConsumerData();
        getterConsumer.setGroupName("admins");
        value.getConsumerDataSet().add(getterConsumer);
        item.put("consumerGetterMutationVisibleToAssignedSet", consumers.size() == 3);
        Map<String, Object> mutatedSetSizes = new TreeMap<>();
        mutatedSetSizes.put("producerDataSet", value.getProducerDataSet().size());
        mutatedSetSizes.put("consumerDataSet", value.getConsumerDataSet().size());
        item.put("mutatedSetSizes", mutatedSetSizes);
        value.setClientID(null);
        value.setProducerDataSet(null);
        value.setConsumerDataSet(null);
        value.setHeartbeatFingerprint(0);
        value.setWithoutSub(false);
        item.put("nullSettersRemainNull", value.getClientID() == null && value.getProducerDataSet() == null
            && value.getConsumerDataSet() == null);
        Map<String, Object> primitiveAfterReset = new TreeMap<>();
        primitiveAfterReset.put("heartbeatFingerprint", value.getHeartbeatFingerprint());
        primitiveAfterReset.put("withoutSub", value.isWithoutSub());
        item.put("primitiveAfterReset", primitiveAfterReset);
        return item;
    }

    private static List<Object> identityMethodCases() throws Exception {
        ProducerData producerLeft = new ProducerData();
        producerLeft.setGroupName("same");
        ProducerData producerRight = new ProducerData();
        producerRight.setGroupName("same");

        ConsumerData consumerLeft = new ConsumerData();
        consumerLeft.setGroupName("same");
        ConsumerData consumerRight = new ConsumerData();
        consumerRight.setGroupName("same");

        HeartbeatData heartbeatLeft = new HeartbeatData();
        heartbeatLeft.setClientID("same");
        HeartbeatData heartbeatRight = new HeartbeatData();
        heartbeatRight.setClientID("same");

        List<Object> cases = new ArrayList<>();
        for (Object[] pair : new Object[][] {
            {"ProducerData", producerLeft, producerRight},
            {"ConsumerData", consumerLeft, consumerRight},
            {"HeartbeatData", heartbeatLeft, heartbeatRight}
        }) {
            Object left = pair[1];
            Object right = pair[2];
            Map<String, Object> item = new TreeMap<>();
            item.put("class", pair[0]);
            item.put("equalsDeclaringClass", left.getClass().getMethod("equals", Object.class)
                .getDeclaringClass().getName());
            item.put("hashCodeDeclaringClass", left.getClass().getMethod("hashCode")
                .getDeclaringClass().getName());
            item.put("sameInstanceEquals", left.equals(left));
            item.put("equalFieldsDistinctInstanceEquals", left.equals(right));
            java.util.Set<Object> values = new java.util.HashSet<>();
            values.add(left);
            values.add(right);
            item.put("equalFieldsSetSize", values.size());
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> ownerFieldCases() {
        Map<String, Object> result = new TreeMap<>();
        List<Object> producers = new ArrayList<>();
        for (String input : new String[] {"{}", "{\"groupName\":null}", "{\"groupName\":\"group\"}"}) {
            ProducerData value = com.alibaba.fastjson2.JSON.parseObject(input, ProducerData.class);
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(value));
            Map<String, Object> item = new TreeMap<>();
            item.put("rawInput", input);
            item.put("groupNull", value.getGroupName() == null);
            item.put("toString", value.toString());
            item.put("encodedHasGroup", encoded.containsKey("groupName"));
            producers.add(item);
        }
        result.put("producers", producers);
        List<Object> consumers = new ArrayList<>();
        for (String input : new String[] {
            "{}",
            "{\"groupName\":null,\"consumeType\":null,\"messageModel\":null,\"consumeFromWhere\":null,\"subscriptionDataSet\":null}",
            "{\"groupName\":\"group\",\"consumeType\":\"CONSUME_ACTIVELY\",\"messageModel\":\"CLUSTERING\",\"consumeFromWhere\":\"CONSUME_FROM_LAST_OFFSET\",\"subscriptionDataSet\":[],\"unitMode\":true}",
            "{\"groupName\":\"nested\",\"consumeType\":\"CONSUME_PASSIVELY\",\"messageModel\":\"BROADCASTING\",\"consumeFromWhere\":\"CONSUME_FROM_TIMESTAMP\",\"subscriptionDataSet\":[{\"topic\":\"topic\",\"subString\":\"tag\",\"tagsSet\":[\"tag\"],\"codeSet\":[7],\"subVersion\":42,\"expressionType\":\"SQL92\"}],\"unitMode\":false}"
        }) {
            ConsumerData value = com.alibaba.fastjson2.JSON.parseObject(input, ConsumerData.class);
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(value));
            Map<String, Object> item = new TreeMap<>();
            item.put("rawInput", input);
            item.put("toString", value.toString());
            item.put("groupNull", value.getGroupName() == null);
            item.put("consumeTypeNull", value.getConsumeType() == null);
            item.put("messageModelNull", value.getMessageModel() == null);
            item.put("consumeFromWhereNull", value.getConsumeFromWhere() == null);
            item.put("subscriptionsNull", value.getSubscriptionDataSet() == null);
            item.put("subscriptionsEmpty", value.getSubscriptionDataSet() != null && value.getSubscriptionDataSet().isEmpty());
            item.put("unitMode", value.isUnitMode());
            item.put("encodedHasGroup", encoded.containsKey("groupName"));
            item.put("encodedHasConsumeType", encoded.containsKey("consumeType"));
            item.put("encodedHasMessageModel", encoded.containsKey("messageModel"));
            item.put("encodedHasConsumeFromWhere", encoded.containsKey("consumeFromWhere"));
            item.put("encodedHasSubscriptions", encoded.containsKey("subscriptionDataSet"));
            consumers.add(item);
        }
        result.put("consumers", consumers);
        return result;
    }

    private static List<Object> consumerEnumCases() {
        List<Object> cases = new ArrayList<>();
        for (ConsumeType value : ConsumeType.values())
            recordConsumerEnum(cases, "consumeType", value.name());
        for (MessageModel value : MessageModel.values())
            recordConsumerEnum(cases, "messageModel", value.name());
        for (ConsumeFromWhere value : ConsumeFromWhere.values())
            recordConsumerEnum(cases, "consumeFromWhere", value.name());

        for (String[] input : new String[][] {
            {"consumeType", "\"NOT_A_CONSUME_TYPE\""},
            {"consumeType", "\"consume_pop\""},
            {"consumeType", "99"},
            {"consumeType", "-1"},
            {"consumeType", "1.0"},
            {"consumeType", "1.9"},
            {"consumeType", "-0.5"},
            {"consumeType", "3.0"},
            {"consumeType", "null"},
            {"consumeType", "\"1\""},
            {"consumeType", "\"1.9\""},
            {"consumeType", "\"02\""},
            {"consumeType", "\"+1\""},
            {"consumeType", "\"-0\""},
            {"consumeType", "\"-1\""},
            {"consumeType", "\"1.0\""},
            {"consumeType", "\" 1 \""},
            {"consumeType", "\"1e0\""},
            {"consumeType", "\"2147483648\""},
            {"consumeType", "true"},
            {"consumeType", "{}"},
            {"consumeType", "[]"},
            {"consumeType", "2.9"},
            {"consumeType", "9007199254740993.0"},
            {"consumeType", "4294967296.0"},
            {"consumeType", "9223372036854775808.0"},
            {"consumeType", "1e100"},
            {"consumeType", "1e-100"},
            {"messageModel", "\"NOT_A_MESSAGE_MODEL\""},
            {"messageModel", "\"2\""},
            {"messageModel", "\"3\""},
            {"messageModel", "\"-1\""},
            {"messageModel", "99"},
            {"consumeFromWhere", "\"NOT_A_CONSUME_FROM_WHERE\""},
            {"consumeFromWhere", "\"5\""},
            {"consumeFromWhere", "\"6\""},
            {"consumeFromWhere", "\"-1\""},
            {"consumeFromWhere", "99"}
        }) {
            recordConsumerEnumProbe(cases, input[0], input[1]);
        }
        for (int ordinal = 0; ordinal <= ConsumeType.values().length; ordinal++)
            recordConsumerEnumProbe(cases, "consumeType", Integer.toString(ordinal));
        for (int ordinal = 0; ordinal <= MessageModel.values().length; ordinal++)
            recordConsumerEnumProbe(cases, "messageModel", Integer.toString(ordinal));
        for (int ordinal = 0; ordinal <= ConsumeFromWhere.values().length; ordinal++)
            recordConsumerEnumProbe(cases, "consumeFromWhere", Integer.toString(ordinal));
        return cases;
    }

    private static List<Object> consumerEnumMethodCases() {
        List<Object> cases = new ArrayList<>();
        for (ConsumeType value : ConsumeType.values()) {
            Map<String, Object> item = new TreeMap<>();
            item.put("enum", "consumeType");
            item.put("constant", value.name());
            item.put("methodValue", value.getTypeCN());
            cases.add(item);
        }
        for (MessageModel value : MessageModel.values()) {
            Map<String, Object> item = new TreeMap<>();
            item.put("enum", "messageModel");
            item.put("constant", value.name());
            item.put("methodValue", value.getModeCN());
            cases.add(item);
        }
        return cases;
    }

    private static void recordConsumerEnumProbe(List<Object> cases, String field, String jsonValue) {
        String rawInput = "{\"" + field + "\":" + jsonValue + "}";
        Map<String, Object> item = new TreeMap<>();
        item.put("field", field);
        item.put("rawInput", rawInput);
        try {
            ConsumerData value = com.alibaba.fastjson2.JSON.parseObject(rawInput, ConsumerData.class);
            Object decoded = consumerEnumName(value, field);
            item.put("outcome", decoded == null ? "null" : "accepted");
            item.put("decodedName", decoded);
            item.put("encodedName", com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(value)).getString(field));
        } catch (Throwable error) {
            item.put("outcome", "rejected");
            item.put("error", error.getClass().getName());
        }
        cases.add(item);
    }

    private static void recordConsumerEnum(List<Object> cases, String field, String enumName) {
        String rawInput = "{\"" + field + "\":\"" + enumName + "\"}";
        ConsumerData value = com.alibaba.fastjson2.JSON.parseObject(rawInput, ConsumerData.class);
        Map<String, Object> item = new TreeMap<>();
        item.put("field", field);
        item.put("rawInput", rawInput);
        item.put("decodedName", consumerEnumName(value, field));
        item.put("encodedName", com.alibaba.fastjson2.JSON.parseObject(
            com.alibaba.fastjson2.JSON.toJSONString(value)).getString(field));
        item.put("outcome", "accepted");
        cases.add(item);
    }

    private static String consumerEnumName(ConsumerData value, String field) {
        switch (field) {
            case "consumeType": return value.getConsumeType() == null ? null : value.getConsumeType().name();
            case "messageModel": return value.getMessageModel() == null ? null : value.getMessageModel().name();
            case "consumeFromWhere": return value.getConsumeFromWhere() == null
                ? null : value.getConsumeFromWhere().name();
            default: throw new IllegalArgumentException("unknown consumer enum field: " + field);
        }
    }

    private static List<Object> heartbeatFieldCases() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{}",
            "{\"clientID\":null,\"producerDataSet\":null,\"consumerDataSet\":null,\"heartbeatFingerprint\":0,\"withoutSub\":false}",
            "{\"clientID\":\"client\",\"producerDataSet\":[{\"groupName\":\"p\"}],\"consumerDataSet\":[{\"groupName\":\"c\",\"subscriptionDataSet\":[{\"topic\":\"topic\",\"subString\":\"tag\",\"tagsSet\":[\"tag\"],\"codeSet\":[7],\"subVersion\":42,\"expressionType\":\"SQL92\"}]}],\"heartbeatFingerprint\":7,\"withoutSub\":true}",
            "{\"clientID\":\"duplicate-producers\",\"producerDataSet\":[{\"groupName\":\"same\"},{\"groupName\":\"same\"}],\"consumerDataSet\":[],\"heartbeatFingerprint\":0,\"withoutSub\":false}"
        }) {
            HeartbeatData value = com.alibaba.fastjson2.JSON.parseObject(input, HeartbeatData.class);
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(value));
            Map<String, Object> item = new TreeMap<>();
            item.put("rawInput", input);
            item.put("toString", value.toString());
            item.put("clientNull", value.getClientID() == null);
            item.put("producersNull", value.getProducerDataSet() == null);
            item.put("consumersNull", value.getConsumerDataSet() == null);
            item.put("producerCount", value.getProducerDataSet() == null ? -1 : value.getProducerDataSet().size());
            item.put("consumerCount", value.getConsumerDataSet() == null ? -1 : value.getConsumerDataSet().size());
            item.put("fingerprint", value.getHeartbeatFingerprint());
            item.put("computedFingerprint", value.computeHeartbeatFingerprint());
            item.put("withoutSub", value.isWithoutSub());
            item.put("encodedHasClient", encoded.containsKey("clientID"));
            item.put("encodedHasProducers", encoded.containsKey("producerDataSet"));
            item.put("encodedHasConsumers", encoded.containsKey("consumerDataSet"));
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> heartbeatSerializationMethodCases() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Object> cases = new ArrayList<>();
        Method featureEncode = null;
        for (Method method : HeartbeatData.class.getMethods()) {
            if ("encode".equals(method.getName()) && method.getParameterCount() == 1
                && method.getParameterTypes()[0].isArray()) {
                featureEncode = method;
                break;
            }
        }
        if (featureEncode == null) throw new IllegalStateException("inherited feature encode overload missing");
        result.put("featureEncodeParameterType", featureEncode.getParameterTypes()[0].getComponentType().getName());
        for (String[] testCase : new String[][] {
            {"default", "{}"},
            {"explicit-nulls", "{\"clientID\":null,\"producerDataSet\":null,"
                + "\"consumerDataSet\":null,\"heartbeatFingerprint\":0,\"withoutSub\":false}"},
            {"populated", "{\"clientID\":\"client\",\"producerDataSet\":[{\"groupName\":\"p\"}],"
                + "\"consumerDataSet\":[{\"groupName\":\"c\",\"consumeType\":\"CONSUME_ACTIVELY\","
                + "\"messageModel\":\"CLUSTERING\",\"consumeFromWhere\":\"CONSUME_FROM_LAST_OFFSET\","
                + "\"subscriptionDataSet\":[{\"topic\":\"topic\",\"subString\":\"tag\","
                + "\"tagsSet\":[\"tag\"],\"codeSet\":[7],\"subVersion\":42,"
                + "\"expressionType\":\"SQL92\"}],\"unitMode\":true}],"
                + "\"heartbeatFingerprint\":7,\"withoutSub\":true}"}
        }) {
            HeartbeatData value = com.alibaba.fastjson2.JSON.parseObject(testCase[1], HeartbeatData.class);
            byte[] instanceBytes = value.encode();
            String instanceJson = new String(instanceBytes, java.nio.charset.StandardCharsets.UTF_8);
            Object emptyFeatures = java.lang.reflect.Array.newInstance(
                featureEncode.getParameterTypes()[0].getComponentType(), 0);
            String featureJson = new String((byte[]) featureEncode.invoke(value, emptyFeatures),
                java.nio.charset.StandardCharsets.UTF_8);
            byte[] arrayBytes = com.alibaba.fastjson2.JSON.toJSONString(Collections.singletonList(value))
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            List<HeartbeatData> decodedList = HeartbeatData.decodeList(arrayBytes, HeartbeatData.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("case", testCase[0]);
            item.put("rawInput", testCase[1]);
            item.put("instanceJson", instanceJson);
            item.put("instancePrettyJson", value.toJson(true));
            item.put("staticJson", HeartbeatData.toJson(value, false));
            item.put("staticPrettyJson", HeartbeatData.toJson(value, true));
            item.put("instanceEncodedJson", instanceJson);
            item.put("staticEncodedJson", new String(HeartbeatData.encode(value),
                java.nio.charset.StandardCharsets.UTF_8));
            item.put("featureEncodedJson", featureJson);
            item.put("writerFeatureOverrides", writerFeatureOverrides(value));
            item.put("decodedJson", HeartbeatData.decode(instanceBytes, HeartbeatData.class).toJson());
            item.put("fromJsonJson", HeartbeatData.fromJson(instanceJson, HeartbeatData.class).toJson());
            item.put("decodedListCount", decodedList.size());
            item.put("decodedListJson", decodedList.get(0).toJson());
            cases.add(item);
        }
        result.put("cases", cases);
        result.put("staticEncodeNullIsNull", HeartbeatData.encode((Object) null) == null);
        result.put("staticToJsonNull", HeartbeatData.toJson(null, false));
        result.put("decodeNullIsNull", HeartbeatData.decode(null, HeartbeatData.class) == null);
        result.put("decodeListNullIsNull", HeartbeatData.decodeList(null, HeartbeatData.class) == null);
        result.put("fromJsonNullIsNull", HeartbeatData.fromJson(null, HeartbeatData.class) == null);
        byte[] jsonNull = "null".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        result.put("decodeJsonNullIsNull", HeartbeatData.decode(jsonNull, HeartbeatData.class) == null);
        result.put("decodeListJsonNullIsNull", HeartbeatData.decodeList(jsonNull, HeartbeatData.class) == null);
        result.put("fromJsonJsonNullIsNull", HeartbeatData.fromJson("null", HeartbeatData.class) == null);
        List<HeartbeatData> nullMemberList = HeartbeatData.decodeList(
            "[null,{}]".getBytes(java.nio.charset.StandardCharsets.UTF_8), HeartbeatData.class);
        result.put("decodeListPreservesNullMember", nullMemberList.size() == 2
            && nullMemberList.get(0) == null && nullMemberList.get(1) != null);
        return result;
    }

    private static Map<String, Object> writerFeatureOverrides(HeartbeatData value) {
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

    private static List<Object> heartbeatNullElementCases() {
        List<Object> cases = new ArrayList<>();
        for (String[] testCase : new String[][] {
            {"producer-null", "{\"producerDataSet\":[null,{\"groupName\":\"p\"},null],\"consumerDataSet\":[]}"},
            {"consumer-null", "{\"producerDataSet\":[],\"consumerDataSet\":[null,{\"groupName\":\"c\"},null]}"},
            {"subscription-null", "{\"producerDataSet\":[],\"consumerDataSet\":[{\"groupName\":\"c\","
                + "\"subscriptionDataSet\":[null,{\"topic\":\"topic\",\"subString\":\"tag\","
                + "\"tagsSet\":[],\"codeSet\":[],\"expressionType\":\"TAG\",\"subVersion\":1},null]}]}"}
        }) {
            HeartbeatData value = com.alibaba.fastjson2.JSON.parseObject(testCase[1], HeartbeatData.class);
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(value));
            Map<String, Object> item = new TreeMap<>();
            item.put("case", testCase[0]);
            item.put("rawInput", testCase[1]);
            item.put("toString", value.toString());
            item.put("producerCount", value.getProducerDataSet() == null ? null : value.getProducerDataSet().size());
            item.put("producerHasNull", value.getProducerDataSet() != null && value.getProducerDataSet().contains(null));
            item.put("consumerCount", value.getConsumerDataSet() == null ? null : value.getConsumerDataSet().size());
            item.put("consumerHasNull", value.getConsumerDataSet() != null && value.getConsumerDataSet().contains(null));
            item.put("encodedProducerDataSet", encoded.get("producerDataSet"));
            item.put("encodedConsumerDataSet", encoded.get("consumerDataSet"));
            if ("subscription-null".equals(testCase[0])) {
                ConsumerData consumer = value.getConsumerDataSet().iterator().next();
                item.put("subscriptionCount", consumer.getSubscriptionDataSet().size());
                item.put("subscriptionHasNull", consumer.getSubscriptionDataSet().contains(null));
            }
            try {
                item.put("computedFingerprint", value.computeHeartbeatFingerprint());
                item.put("fingerprintError", null);
            } catch (Throwable error) {
                item.put("computedFingerprint", null);
                item.put("fingerprintError", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> heartbeatAliasCases() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{\"clientId\":\"lower-d\",\"isWithoutSub\":true}",
            "{\"ClientID\":\"mixed-case\",\"WithoutSub\":true}",
            "{\"CLIENTID\":\"upper\",\"WITHOUTSUB\":true}"
        }) {
            HeartbeatData value = com.alibaba.fastjson2.JSON.parseObject(input, HeartbeatData.class);
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(value));
            Map<String, Object> item = new TreeMap<>();
            item.put("rawInput", input);
            item.put("clientID", value.getClientID());
            item.put("withoutSub", value.isWithoutSub());
            item.put("encodedClientID", encoded.get("clientID"));
            item.put("encodedWithoutSub", encoded.get("withoutSub"));
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> fingerprintNullCases() {
        List<Object> cases = new ArrayList<>();
        for (String name : new String[] {
            "empty", "null-producers", "null-consumers", "null-subscriptions"
        }) {
            HeartbeatData data = new HeartbeatData();
            if ("null-producers".equals(name)) data.setProducerDataSet(null);
            if ("null-consumers".equals(name)) data.setConsumerDataSet(null);
            if ("null-subscriptions".equals(name)) {
                ConsumerData consumer = new ConsumerData();
                consumer.setSubscriptionDataSet(null);
                data.getConsumerDataSet().add(consumer);
            }
            Map<String, Object> item = new TreeMap<>();
            item.put("case", name);
            item.put("body", com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(data)));
            try {
                item.put("fingerprint", data.computeHeartbeatFingerprint());
            } catch (Throwable error) {
                item.put("error", error.getClass().getName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> primitiveNullCases() {
        List<Object> cases = new ArrayList<>();
        for (String[] input : new String[][] {
            {"subscription-version", "{\"subVersion\":null}", "subscription"},
            {"subscription-class-filter", "{\"classFilterMode\":null}", "subscription"},
            {"heartbeat-fingerprint", "{\"heartbeatFingerprint\":null}", "heartbeat"},
            {"heartbeat-without-sub", "{\"withoutSub\":null}", "heartbeat"},
            {"consumer-unit-mode", "{\"unitMode\":null}", "consumer"}
        }) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", input[0]);
            item.put("rawInput", input[1]);
            try {
                switch (input[2]) {
                    case "subscription":
                        SubscriptionData subscription = com.alibaba.fastjson2.JSON.parseObject(input[1], SubscriptionData.class);
                        item.put("value", "subscription-version".equals(input[0])
                            ? subscription.getSubVersion() : subscription.isClassFilterMode());
                        break;
                    case "heartbeat":
                        HeartbeatData heartbeat = com.alibaba.fastjson2.JSON.parseObject(input[1], HeartbeatData.class);
                        item.put("value", "heartbeat-fingerprint".equals(input[0])
                            ? heartbeat.getHeartbeatFingerprint() : heartbeat.isWithoutSub());
                        break;
                    default:
                        ConsumerData consumer = com.alibaba.fastjson2.JSON.parseObject(input[1], ConsumerData.class);
                        item.put("value", consumer.isUnitMode());
                }
            } catch (Throwable error) {
                item.put("error", error.getClass().getName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static Object normalizeHeartbeatJson(Object value) {
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            Map<String, Object> normalized = new TreeMap<>();
            map.forEach((key, item) -> normalized.put(String.valueOf(key), normalizeHeartbeatJson(item)));
            return normalized;
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            List<Object> normalized = new ArrayList<>();
            for (Object item : list) normalized.add(normalizeHeartbeatJson(item));
            normalized.sort(Comparator.comparing(com.alibaba.fastjson2.JSON::toJSONString));
            return normalized;
        }
        return value;
    }

    private static Map<String, Object> heartbeatDtoInputCase(String id, String type, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("id", id);
        item.put("type", type);
        item.put("rawInput", rawInput);
        try {
            Object value;
            if ("subscription".equals(type)) {
                value = com.alibaba.fastjson2.JSON.parseObject(rawInput, SubscriptionData.class);
            } else if ("producer".equals(type)) {
                value = com.alibaba.fastjson2.JSON.parseObject(rawInput, ProducerData.class);
            } else if ("consumer".equals(type)) {
                value = com.alibaba.fastjson2.JSON.parseObject(rawInput, ConsumerData.class);
            } else if ("heartbeat".equals(type)) {
                value = com.alibaba.fastjson2.JSON.parseObject(rawInput, HeartbeatData.class);
            } else {
                throw new IllegalArgumentException("unknown DTO: " + type);
            }
            String encoded = com.alibaba.fastjson2.JSON.toJSONString(value,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue);
            item.put("outcome", "ACCEPTED");
            Map<String, Object> fields = com.alibaba.fastjson2.JSON.parseObject(encoded);
            if ("root-subscription-empty-object".equals(id)) {
                fields.remove("subVersion"); // Constructor timestamp is intentionally nondeterministic.
            }
            item.put("fields", normalizeHeartbeatJson(fields));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> heartbeatDtoInputCases() {
        List<Object> cases = new ArrayList<>();
        String[] rootDtoTypes = {"subscription", "producer", "consumer", "heartbeat"};
        String[][] rootShapes = {
            {"empty-object", "{}"}, {"null", "null"}, {"empty-array", "[]"},
            {"singleton-array", "[{}]"}, {"number", "0"}, {"boolean", "true"},
            {"string", "\"text\""}
        };
        for (String type : rootDtoTypes) {
            for (String[] shape : rootShapes) {
                cases.add(heartbeatDtoInputCase("root-" + type + "-" + shape[0], type, shape[1]));
            }
        }
        cases.add(heartbeatDtoInputCase("subscription-duplicate-class-filter-mode-last-wins", "subscription",
            "{\"subVersion\":0,\"classFilterMode\":false,\"classFilterMode\":true}"));
        cases.add(heartbeatDtoInputCase("subscription-duplicate-topic-last-wins", "subscription",
            "{\"subVersion\":0,\"topic\":\"first\",\"topic\":\"last\"}"));
        cases.add(heartbeatDtoInputCase("subscription-duplicate-sub-string-last-wins", "subscription",
            "{\"subVersion\":0,\"subString\":\"first\",\"subString\":\"last\"}"));
        cases.add(heartbeatDtoInputCase("subscription-duplicate-tags-set-last-wins", "subscription",
            "{\"subVersion\":0,\"tagsSet\":[\"first\"],\"tagsSet\":[\"last\"]}"));
        cases.add(heartbeatDtoInputCase("subscription-duplicate-code-set-last-wins", "subscription",
            "{\"subVersion\":0,\"codeSet\":[1],\"codeSet\":[2]}"));
        cases.add(heartbeatDtoInputCase("subscription-duplicate-sub-version-last-wins", "subscription",
            "{\"subVersion\":1,\"subVersion\":2}"));
        cases.add(heartbeatDtoInputCase("subscription-duplicate-expression-type-last-wins", "subscription",
            "{\"subVersion\":0,\"expressionType\":\"SQL92\",\"expressionType\":\"TAG\"}"));
        cases.add(heartbeatDtoInputCase("producer-duplicate-group-name-last-wins", "producer",
            "{\"groupName\":\"first\",\"groupName\":\"last\"}"));
        cases.add(heartbeatDtoInputCase("consumer-duplicate-group-name-last-wins", "consumer",
            "{\"groupName\":\"first\",\"groupName\":\"last\"}"));
        cases.add(heartbeatDtoInputCase("consumer-duplicate-consume-type-last-wins", "consumer",
            "{\"consumeType\":\"CONSUME_ACTIVELY\",\"consumeType\":\"CONSUME_POP\"}"));
        cases.add(heartbeatDtoInputCase("consumer-duplicate-message-model-last-wins", "consumer",
            "{\"messageModel\":\"CLUSTERING\",\"messageModel\":\"BROADCASTING\"}"));
        cases.add(heartbeatDtoInputCase("consumer-duplicate-consume-from-last-wins", "consumer",
            "{\"consumeFromWhere\":\"CONSUME_FROM_LAST_OFFSET\",\"consumeFromWhere\":\"CONSUME_FROM_FIRST_OFFSET\"}"));
        cases.add(heartbeatDtoInputCase("consumer-duplicate-subscription-set-last-wins", "consumer",
            "{\"subscriptionDataSet\":[{\"topic\":\"first\",\"subVersion\":0}],\"subscriptionDataSet\":[{\"topic\":\"last\",\"subVersion\":0}]}"));
        cases.add(heartbeatDtoInputCase("consumer-duplicate-unit-mode-last-wins", "consumer",
            "{\"unitMode\":false,\"unitMode\":true}"));
        cases.add(heartbeatDtoInputCase("heartbeat-duplicate-client-id-last-wins", "heartbeat",
            "{\"clientID\":\"first\",\"clientID\":\"last\"}"));
        cases.add(heartbeatDtoInputCase("heartbeat-duplicate-producer-set-last-wins", "heartbeat",
            "{\"producerDataSet\":[{\"groupName\":\"first\"}],\"producerDataSet\":[{\"groupName\":\"last\"}]}"));
        cases.add(heartbeatDtoInputCase("heartbeat-duplicate-consumer-set-last-wins", "heartbeat",
            "{\"consumerDataSet\":[{\"groupName\":\"first\"}],\"consumerDataSet\":[{\"groupName\":\"last\"}]}"));
        cases.add(heartbeatDtoInputCase("heartbeat-duplicate-fingerprint-last-wins", "heartbeat",
            "{\"heartbeatFingerprint\":1,\"heartbeatFingerprint\":2}"));
        cases.add(heartbeatDtoInputCase("heartbeat-duplicate-without-sub-last-wins", "heartbeat",
            "{\"withoutSub\":false,\"withoutSub\":true}"));
        cases.add(heartbeatDtoInputCase("subscription-scalars", "subscription",
            "{\"classFilterMode\":-1,\"topic\":42,\"subString\":true,\"tagsSet\":[1,true,[],{},null],\"codeSet\":[\"2\",1.9,true,null],\"subVersion\":\"1e2\",\"expressionType\":3,\"filterClassSource\":false}"));
        cases.add(heartbeatDtoInputCase("subscription-high-precision-number-to-string", "subscription",
            "{\"subVersion\":0,\"topic\":9223372036854775807.123456789012345678901}"));
        cases.add(heartbeatDtoInputCase("subscription-exponent-number-to-string", "subscription",
            "{\"subVersion\":0,\"topic\":1e2}"));
        cases.add(heartbeatDtoInputCase("subscription-decimal-exponent-number-to-string", "subscription",
            "{\"subVersion\":0,\"topic\":1.0e2}"));
        cases.add(heartbeatDtoInputCase("subscription-precision-exponent-number-to-string", "subscription",
            "{\"subVersion\":0,\"topic\":1.234567890123456789e2}"));
        cases.add(heartbeatDtoInputCase("subscription-tags-high-precision-number-to-string", "subscription",
            "{\"subVersion\":0,\"tagsSet\":[9223372036854775807.123456789012345678901]}"));
        cases.add(heartbeatDtoInputCase("subscription-decimal-primitives", "subscription",
            "{\"classFilterMode\":1.0,\"subVersion\":1.9}"));
        cases.add(heartbeatDtoInputCase("subscription-long-maximum", "subscription",
            "{\"subVersion\":9223372036854775807}"));
        cases.add(heartbeatDtoInputCase("subscription-long-overflow", "subscription",
            "{\"subVersion\":9223372036854775808}"));
        cases.add(heartbeatDtoInputCase("subscription-long-object-to-zero", "subscription",
            "{\"subVersion\":{\"x\":1}}"));
        cases.add(heartbeatDtoInputCase("subscription-long-empty-object-to-zero", "subscription",
            "{\"subVersion\":{}}"));
        cases.add(heartbeatDtoInputCase("subscription-long-empty-array", "subscription",
            "{\"subVersion\":[]}"));
        cases.add(heartbeatDtoInputCase("subscription-code-set-boundaries", "subscription",
            "{\"subVersion\":0,\"codeSet\":[-2147483648,2147483647]}"));
        cases.add(heartbeatDtoInputCase("subscription-code-set-overflow-positive", "subscription",
            "{\"subVersion\":0,\"codeSet\":[2147483648]}"));
        cases.add(heartbeatDtoInputCase("subscription-code-set-overflow-negative", "subscription",
            "{\"subVersion\":0,\"codeSet\":[-2147483649]}"));
        cases.add(heartbeatDtoInputCase("subscription-code-set-numeric-string-exponent", "subscription",
            "{\"subVersion\":0,\"codeSet\":[\"1e2\"]}"));
        cases.add(heartbeatDtoInputCase("subscription-code-set-wide-decimal", "subscription",
            "{\"subVersion\":0,\"codeSet\":[2147483648.9]}"));
        cases.add(heartbeatDtoInputCase("subscription-code-set-high-precision-decimal", "subscription",
            "{\"subVersion\":0,\"codeSet\":[9223372036854775807.0,9223372036854775808.0,-9223372036854775809.0]}"));
        cases.add(heartbeatDtoInputCase("subscription-code-set-decimal-long-maximum", "subscription",
            "{\"subVersion\":0,\"codeSet\":[9223372036854775807.0]}"));
        cases.add(heartbeatDtoInputCase("subscription-code-set-decimal-long-overflow-positive", "subscription",
            "{\"subVersion\":0,\"codeSet\":[9223372036854775808.0]}"));
        cases.add(heartbeatDtoInputCase("subscription-code-set-decimal-long-overflow-negative", "subscription",
            "{\"subVersion\":0,\"codeSet\":[-9223372036854775809.0]}"));
        cases.add(heartbeatDtoInputCase("subscription-tags-coercion-deduplicates", "subscription",
            "{\"subVersion\":0,\"tagsSet\":[\"tag\",\"tag\",7,\"7\",null,null]}"));
        cases.add(heartbeatDtoInputCase("subscription-invalid-code", "subscription",
            "{\"codeSet\":[\"not-an-integer\"],\"subVersion\":0}"));
        cases.add(heartbeatDtoInputCase("subscription-code-set-null", "subscription",
            "{\"subVersion\":0,\"codeSet\":null}"));
        cases.add(heartbeatDtoInputCase("consumer-subscription-set-scalar-string", "consumer",
            "{\"subscriptionDataSet\":\"topic\"}"));
        cases.add(heartbeatDtoInputCase("consumer-scalars", "consumer",
            "{\"groupName\":123,\"unitMode\":-1,\"subscriptionDataSet\":[{\"topic\":42,\"subVersion\":\"4\",\"classFilterMode\":1}]}"));
        cases.add(heartbeatDtoInputCase("consumer-unit-mode-other-number", "consumer",
            "{\"unitMode\":2}"));
        cases.add(heartbeatDtoInputCase("consumer-invalid-boolean", "consumer",
            "{\"unitMode\":\"yes\"}"));
        cases.add(heartbeatDtoInputCase("heartbeat-scalars", "heartbeat",
            "{\"clientID\":123,\"heartbeatFingerprint\":\"2\",\"withoutSub\":\"1\",\"producerDataSet\":[{\"groupName\":7}]}"));
        cases.add(heartbeatDtoInputCase("producer-string", "producer",
            "{\"groupName\":123}"));
        cases.add(heartbeatDtoInputCase("subscription-leading-plus-zero-version", "subscription",
            "{\"subVersion\":+02}"));
        cases.add(heartbeatDtoInputCase("subscription-number-long-suffix", "subscription",
            "{\"subVersion\":1L}"));
        cases.add(heartbeatDtoInputCase("subscription-number-double-suffix", "subscription",
            "{\"subVersion\":1D}"));
        cases.add(heartbeatDtoInputCase("subscription-number-float-suffix", "subscription",
            "{\"subVersion\":1F}"));
        cases.add(heartbeatDtoInputCase("subscription-number-byte-suffix", "subscription",
            "{\"subVersion\":1B}"));
        cases.add(heartbeatDtoInputCase("subscription-number-short-suffix", "subscription",
            "{\"subVersion\":1S}"));
        cases.add(heartbeatDtoInputCase("subscription-float-suffix-high-precision-to-string", "subscription",
            "{\"subVersion\":0,\"topic\":1.23456789F}"));
        cases.add(heartbeatDtoInputCase("subscription-float-suffix-exponent-to-string", "subscription",
            "{\"subVersion\":0,\"topic\":1e7F}"));
        cases.add(heartbeatDtoInputCase("subscription-double-suffix-exponent-to-string", "subscription",
            "{\"subVersion\":0,\"topic\":1e7D}"));
        cases.add(heartbeatDtoInputCase("subscription-long-suffix-to-string", "subscription",
            "{\"subVersion\":0,\"topic\":9223372036854775807L}"));
        cases.add(heartbeatDtoInputCase("subscription-lowercase-float-suffix-rejected", "subscription",
            "{\"subVersion\":1f}"));
        cases.add(heartbeatDtoInputCase("subscription-leading-dot-negative-version", "subscription",
            "{\"subVersion\":-.5}"));
        cases.add(heartbeatDtoInputCase("subscription-trailing-dot-version", "subscription",
            "{\"subVersion\":1.}"));
        cases.add(heartbeatDtoInputCase("subscription-trailing-dot-exponent-double-suffix", "subscription",
            "{\"subVersion\":1.e2D}"));
        cases.add(heartbeatDtoInputCase("subscription-leading-dot-positive-version", "subscription",
            "{\"subVersion\":.5}"));
        cases.add(heartbeatDtoInputCase("subscription-leading-dot-positive-float-version", "subscription",
            "{\"subVersion\":.5F}"));
        cases.add(heartbeatDtoInputCase("subscription-leading-dot-positive-topic-rejected", "subscription",
            "{\"subVersion\":0,\"topic\":.5}"));
        cases.add(heartbeatDtoInputCase("subscription-leading-dot-positive-float-topic-rejected", "subscription",
            "{\"subVersion\":0,\"topic\":.5F}"));
        cases.add(heartbeatDtoInputCase("subscription-leading-dot-explicit-positive-topic", "subscription",
            "{\"subVersion\":0,\"topic\":+.5}"));
        cases.add(heartbeatDtoInputCase("subscription-incomplete-exponent-version", "subscription",
            "{\"subVersion\":1e+}"));
        cases.add(heartbeatDtoInputCase("subscription-incomplete-exponent-topic", "subscription",
            "{\"subVersion\":0,\"topic\":1e+}"));
        cases.add(heartbeatDtoInputCase("subscription-incomplete-exponent-float-topic", "subscription",
            "{\"subVersion\":0,\"topic\":1e+F}"));
        cases.add(heartbeatDtoInputCase("subscription-incomplete-exponent-trailing-dot-topic", "subscription",
            "{\"subVersion\":0,\"topic\":1.E}"));
        cases.add(heartbeatDtoInputCase("heartbeat-leading-plus-zero-fingerprint", "heartbeat",
            "{\"heartbeatFingerprint\":+02}"));
        cases.add(heartbeatDtoInputCase("heartbeat-exponent-float-suffix-fingerprint", "heartbeat",
            "{\"heartbeatFingerprint\":1e2F}"));
        cases.add(heartbeatDtoInputCase("consumer-form-feed-and-leading-zero", "consumer",
            String.valueOf((char) 0x0c) + "{\"unitMode\":\f01}"));
        cases.add(heartbeatDtoInputCase("heartbeat-bom-prefix", "heartbeat",
            String.valueOf((char) 0xfeff) + "{\"withoutSub\":true}"));
        cases.add(heartbeatDtoInputCase("heartbeat-eof-marker-repeated", "heartbeat",
            "{\"clientID\":\"eof\"}\u001a\u001a"));
        cases.add(heartbeatDtoInputCase("heartbeat-eof-marker-separated", "heartbeat",
            "{\"clientID\":\"eof\"}\u001a \u001a"));
        cases.add(heartbeatDtoInputCase("heartbeat-eof-marker-before-trailing-text", "heartbeat",
            "{\"clientID\":\"eof\"}\u001a ignored"));
        cases.add(heartbeatDtoInputCase("heartbeat-eof-marker-before-trailing-json", "heartbeat",
            "{\"clientID\":\"eof\"}\u001a{\"clientID\":\"ignored\"}"));
        cases.add(heartbeatDtoInputCase("subscription-form-feed-before-version", "subscription",
            "{\"subVersion\":" + String.valueOf((char) 0x0c) + "+02}"));
        cases.add(heartbeatDtoInputCase("consumer-vertical-tab-prefix", "consumer",
            String.valueOf((char) 0x0b) + "{\"unitMode\":true}"));
        cases.add(heartbeatDtoInputCase("heartbeat-embedded-bom", "heartbeat",
            "{\"heartbeatFingerprint\":" + String.valueOf((char) 0xfeff) + "2}"));
        cases.add(heartbeatDtoInputCase("subscription-single-quoted-strings", "subscription",
            "{'topic':'single','subVersion':0}"));
        cases.add(heartbeatDtoInputCase("subscription-single-quoted-escapes", "subscription",
            "{'topic':'it\\'s \"quoted\"','subVersion':0}"));
        cases.add(heartbeatDtoInputCase("subscription-comment-markers-in-string", "subscription",
            "{\"topic\":\"// stays /* stays */\",\"subVersion\":0}"));
        cases.add(heartbeatDtoInputCase("subscription-unquoted-key-rejected", "subscription",
            "{topic:'unquoted-key','subVersion':0}"));
        cases.add(heartbeatDtoInputCase("subscription-trailing-object-comma", "subscription",
            "{\"topic\":\"trailing-object-comma\",\"subVersion\":0,}"));
        cases.add(heartbeatDtoInputCase("subscription-trailing-array-comma", "subscription",
            "{\"subVersion\":0,\"tagsSet\":[\"tag\",]}"));
        cases.add(heartbeatDtoInputCase("subscription-block-comment", "subscription",
            "/*prefix*/{\"topic\":\"block-comment\",\"subVersion\":0}"));
        cases.add(heartbeatDtoInputCase("subscription-line-comment", "subscription",
            "{\"topic\":\"line-comment\",// gap\n\"subVersion\":0}"));
        cases.add(heartbeatDtoInputCase("subscription-single-quoted-key", "subscription",
            "{'topic':'single-key','subVersion':0}"));
        cases.add(heartbeatDtoInputCase("subscription-short-control-escapes", "subscription",
            "{\"topic\":\"zero=\\0 one=\\1 two=\\2 three=\\3 four=\\4 five=\\5 six=\\6 seven=\\7 vertical=\\v form=\\F\",\"subVersion\":0}"));
        StringBuilder char1Escapes = new StringBuilder("{\"topic\":\"");
        for (char escaped : new char[] {
            '0', '1', '2', '3', '4', '5', '6', '7', 'b', 't', 'n', 'v', 'f', 'F', 'r',
            '"', '\'', '/', '\\', ' ', '#', '&', '(', ')', ',', '.', '@', '[', ']', '_', '~'
        }) {
            char1Escapes.append('\\').append(escaped);
        }
        char1Escapes.append("\",\"subVersion\":0}");
        cases.add(heartbeatDtoInputCase("subscription-char1-escape-table", "subscription",
            char1Escapes.toString()));
        cases.add(heartbeatDtoInputCase("subscription-hex-escapes", "subscription",
            "{\"topic\":\"ascii=\\x41 nul=\\x00 slash=\\x5c quote=\\x22\",\"subVersion\":0}"));
        cases.add(heartbeatDtoInputCase("subscription-identity-escapes", "subscription",
            "{\"topic\":\"\\#\\&\\[\\]\\@\\(\\)\\_\\,\\~\\.\",\"subVersion\":0}"));
        cases.add(heartbeatDtoInputCase("subscription-double-quoted-apostrophe-escape", "subscription",
            "{\"topic\":\"it\\'s\",\"subVersion\":0}"));
        cases.add(heartbeatDtoInputCase("subscription-single-quote-escaped-delimiters", "subscription",
            "{'topic':'single \\'quote\\' and \"double\"','subVersion':0}"));
        cases.add(heartbeatDtoInputCase("subscription-comments-between-tokens", "subscription",
            "{\"topic\"/*name*/:\"value\"/*value*/,\"subVersion\":0}"));
        cases.add(heartbeatDtoInputCase("subscription-comment-after-object-open", "subscription",
            "{/* before first key */\"topic\":\"value\",\"subVersion\":0}"));
        cases.add(heartbeatDtoInputCase("subscription-comment-after-object-comma", "subscription",
            "{\"topic\":\"value\",/* before next key */\"subVersion\":0}"));
        cases.add(heartbeatDtoInputCase("subscription-comment-after-array-open", "subscription",
            "{\"subVersion\":0,\"tagsSet\":[/* before first value */\"tag\"]}"));
        cases.add(heartbeatDtoInputCase("subscription-comment-after-array-comma", "subscription",
            "{\"subVersion\":0,\"tagsSet\":[\"first\",/* before next value */\"second\"]}"));
        cases.add(heartbeatDtoInputCase("subscription-comment-after-colon", "subscription",
            "{\"topic\":/* before value */\"value\",\"subVersion\":0}"));
        cases.add(heartbeatDtoInputCase("subscription-comment-before-object-close", "subscription",
            "{\"topic\":\"value\"/* before close */}"));
        cases.add(heartbeatDtoInputCase("subscription-comment-before-array-comma", "subscription",
            "{\"subVersion\":0,\"tagsSet\":[\"value\"/* before comma */,\"next\"]}"));
        cases.add(heartbeatDtoInputCase("subscription-comment-after-trailing-array-comma", "subscription",
            "{\"subVersion\":0,\"tagsSet\":[\"value\",/* before close */]}"));
        cases.add(heartbeatDtoInputCase("subscription-comment-after-nested-value", "subscription",
            "{\"subVersion\":0,\"tagsSet\":[{\"x\":1}/* before comma */,{\"y\":2}]}"));
        cases.add(heartbeatDtoInputCase("subscription-comment-after-trailing-comma", "subscription",
            "{\"topic\":\"value\",\"subVersion\":0, /* tail */}"));
        cases.add(heartbeatDtoInputCase("subscription-null-array-trailing-comma", "subscription",
            "{\"subVersion\":0,\"tagsSet\":[null,]}"));
        cases.add(heartbeatDtoInputCase("subscription-eof-line-comment", "subscription",
            "{\"topic\":\"eof-comment\",\"subVersion\":0}// end"));
        cases.add(heartbeatDtoInputCase("subscription-single-quoted-comment-markers", "subscription",
            "{'topic':'// stays /* stays */','subVersion':0}"));
        cases.add(heartbeatDtoInputCase("subscription-nested-trailing-commas", "subscription",
            "{\"subVersion\":0,\"tagsSet\":[{\"z\":1,\"a\":2,},],}"));
        cases.add(heartbeatDtoInputCase("subscription-leading-and-trailing-comments", "subscription",
            " /* start */ {'topic':'commented','subVersion':0,} /* end */"));
        cases.add(heartbeatDtoInputCase("subscription-unknown-escape-rejected", "subscription",
            "{\"topic\":\"unknown=\\q\",\"subVersion\":0}"));
        cases.add(heartbeatDtoInputCase("subscription-nonhex-x-lookup-zero", "subscription",
            "{\"topic\":\"invalid=\\xG1\",\"subVersion\":0}"));
        cases.add(heartbeatDtoInputCase("subscription-hex-escape-above-lookup-range-rejected", "subscription",
            "{\"topic\":\"invalid=\\xg1\",\"subVersion\":0}"));
        cases.add(heartbeatDtoInputCase("subscription-char1-escape-8-rejected", "subscription",
            "{\"topic\":\"invalid=\\8\",\"subVersion\":0}"));
        cases.add(heartbeatDtoInputCase("subscription-object-to-string", "subscription",
            "{\"subVersion\":0,\"topic\":{\"z\":1,\"a\":2}}"));
        cases.add(heartbeatDtoInputCase("subscription-array-to-string", "subscription",
            "{\"subVersion\":0,\"subString\":[true,null,\"x\",{\"z\":1,\"a\":2}]}"));
        cases.add(heartbeatDtoInputCase("subscription-tags-singleton-string", "subscription",
            "{\"subVersion\":0,\"tagsSet\":\"tag\"}"));
        cases.add(heartbeatDtoInputCase("subscription-tags-singleton-number", "subscription",
            "{\"subVersion\":0,\"tagsSet\":7}"));
        cases.add(heartbeatDtoInputCase("subscription-tags-singleton-boolean", "subscription",
            "{\"subVersion\":0,\"tagsSet\":true}"));
        cases.add(heartbeatDtoInputCase("subscription-tags-singleton-object", "subscription",
            "{\"subVersion\":0,\"tagsSet\":{\"tag\":true}}"));
        cases.add(heartbeatDtoInputCase("subscription-tags-object-member", "subscription",
            "{\"subVersion\":0,\"tagsSet\":[{\"z\":1,\"a\":2}]}"));
        cases.add(heartbeatDtoInputCase("subscription-codes-singleton-number", "subscription",
            "{\"subVersion\":0,\"codeSet\":7}"));
        cases.add(heartbeatDtoInputCase("subscription-codes-singleton-string", "subscription",
            "{\"subVersion\":0,\"codeSet\":\"8\"}"));
        cases.add(heartbeatDtoInputCase("subscription-codes-singleton-boolean", "subscription",
            "{\"subVersion\":0,\"codeSet\":true}"));
        cases.add(heartbeatDtoInputCase("subscription-codes-singleton-object", "subscription",
            "{\"subVersion\":0,\"codeSet\":{\"code\":7}}"));
        cases.add(heartbeatDtoInputCase("subscription-codes-object-member", "subscription",
            "{\"subVersion\":0,\"codeSet\":[{\"code\":7}]}"));
        cases.add(heartbeatDtoInputCase("producer-object-to-string", "producer",
            "{\"groupName\":{\"z\":1,\"a\":2}}"));
        cases.add(heartbeatDtoInputCase("heartbeat-object-to-string", "heartbeat",
            "{\"clientID\":{\"z\":1,\"a\":2}}"));
        cases.add(heartbeatDtoInputCase("consumer-subscriptions-singleton-object", "consumer",
            "{\"subscriptionDataSet\":{\"topic\":\"T\",\"subVersion\":0}}"));
        cases.add(heartbeatDtoInputCase("heartbeat-producers-singleton-object", "heartbeat",
            "{\"producerDataSet\":{\"groupName\":\"P\"}}"));
        cases.add(heartbeatDtoInputCase("heartbeat-consumers-singleton-object", "heartbeat",
            "{\"consumerDataSet\":{\"groupName\":\"C\"}}"));
        String[][] fields = {
            {"subscription", "classFilterMode"}, {"subscription", "topic"},
            {"subscription", "subString"}, {"subscription", "tagsSet"},
            {"subscription", "codeSet"}, {"subscription", "subVersion"},
            {"subscription", "expressionType"}, {"subscription", "filterClassSource"},
            {"producer", "groupName"},
            {"consumer", "groupName"}, {"consumer", "consumeType"},
            {"consumer", "messageModel"}, {"consumer", "consumeFromWhere"},
            {"consumer", "subscriptionDataSet"}, {"consumer", "unitMode"},
            {"heartbeat", "clientID"}, {"heartbeat", "producerDataSet"},
            {"heartbeat", "consumerDataSet"}, {"heartbeat", "heartbeatFingerprint"},
            {"heartbeat", "withoutSub"}
        };
        String[][] shapes = {
            {"null", "null"}, {"numeric-string", "\"7\""}, {"integer", "0"},
            {"fraction", "1.5"}, {"boolean", "true"}, {"array", "[]"}, {"object", "{}"}
        };
        for (String[] field : fields) {
            for (String[] shape : shapes) {
                String value = shape[1];
                if (field[0].equals("consumer") && field[1].equals("subscriptionDataSet")
                    && shape[0].equals("object")) {
                    value = "{\"topic\":\"T\",\"subVersion\":0}";
                }
                String rawInput = "{\"" + field[1] + "\":" + value
                    + (field[0].equals("subscription") && !field[1].equals("subVersion")
                        ? ",\"subVersion\":0" : "") + "}";
                cases.add(heartbeatDtoInputCase("shape-" + field[0] + "-" + field[1] + "-" + shape[0],
                    field[0], rawInput));
            }
        }
        int[] unicodeWhitespace = {
            0x0085, 0x00a0, 0x1680, 0x180e, 0x2000, 0x2001, 0x2002, 0x2003, 0x2004,
            0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200a, 0x2028, 0x2029, 0x202f,
            0x205f, 0x3000, 0xfeff
        };
        String[] positions = {
            "root-prefix", "object-open", "key-colon", "colon-value", "value-comma",
            "object-comma", "array-open", "array-comma", "array-element-close",
            "array-close", "nested-object-close", "object-close", "after-root",
            "string-key", "string-value"
        };
        for (int codePoint = 0; codePoint <= 0x20; codePoint++) {
            addHeartbeatWhitespaceCases(cases, codePoint, positions);
        }
        for (int codePoint : unicodeWhitespace) {
            addHeartbeatWhitespaceCases(cases, codePoint, positions);
        }
        int[] eofSeparators = new int[0x21 + unicodeWhitespace.length];
        for (int codePoint = 0; codePoint <= 0x20; codePoint++) {
            eofSeparators[codePoint] = codePoint;
        }
        System.arraycopy(unicodeWhitespace, 0, eofSeparators, 0x21, unicodeWhitespace.length);
        for (int codePoint : eofSeparators) {
            String separator = new String(Character.toChars(codePoint));
            String hex = String.format(java.util.Locale.ROOT, "%04x", codePoint);
            cases.add(heartbeatDtoInputCase("heartbeat-eof-marker-between-" + hex, "heartbeat",
                "{\"clientID\":\"eof\"}\u001a" + separator + "\u001a"));
            cases.add(heartbeatDtoInputCase("heartbeat-eof-marker-after-" + hex, "heartbeat",
                "{\"clientID\":\"eof\"}\u001a" + separator));
        }
        return cases;
    }

    private static void addHeartbeatWhitespaceCases(List<Object> cases, int codePoint, String[] positions) {
        String whitespace = new String(Character.toChars(codePoint));
        String hex = String.format(java.util.Locale.ROOT, "%04x", codePoint);
        for (String position : positions) {
            String rawInput;
            switch (position) {
                case "root-prefix":
                    rawInput = whitespace + "{\"clientID\":\"ws\"}";
                    break;
                case "object-open":
                    rawInput = "{" + whitespace + "\"clientID\":\"ws\"}";
                    break;
                case "key-colon":
                    rawInput = "{\"clientID\"" + whitespace + ":\"ws\"}";
                    break;
                case "colon-value":
                    rawInput = "{\"clientID\":" + whitespace + "\"ws\"}";
                    break;
                case "value-comma":
                    rawInput = "{\"clientID\":\"ws\"" + whitespace
                        + ",\"withoutSub\":true}";
                    break;
                case "object-comma":
                    rawInput = "{\"clientID\":\"ws\"," + whitespace
                        + "\"withoutSub\":true}";
                    break;
                case "array-open":
                    rawInput = "{\"producerDataSet\":[" + whitespace
                        + "{\"groupName\":\"ws\"}]}";
                    break;
                case "array-comma":
                    rawInput = "{\"producerDataSet\":[{\"groupName\":\"a\"}," + whitespace
                        + "{\"groupName\":\"b\"}]}";
                    break;
                case "array-element-close":
                    rawInput = "{\"producerDataSet\":[{\"groupName\":\"ws\"}" + whitespace + "]}";
                    break;
                case "array-close":
                    rawInput = "{\"producerDataSet\":[]" + whitespace + "}";
                    break;
                case "nested-object-close":
                    rawInput = "{\"producerDataSet\":[{\"groupName\":\"ws\"" + whitespace + "}]}";
                    break;
                case "object-close":
                    rawInput = "{\"clientID\":\"ws\"" + whitespace + "}";
                    break;
                case "after-root":
                    rawInput = "{\"clientID\":\"ws\"}" + whitespace;
                    break;
                case "string-key":
                    rawInput = "{\"clientID" + whitespace + "\":\"ws\"}";
                    break;
                case "string-value":
                    rawInput = "{\"clientID\":\"a" + whitespace + "b\"}";
                    break;
                default:
                    throw new IllegalArgumentException("unknown whitespace position: " + position);
            }
            cases.add(heartbeatDtoInputCase("heartbeat-whitespace-" + hex + "-" + position,
                "heartbeat", rawInput));
        }
    }

    private static Map<String, Object> nullOwnerSubscriptionPreparation() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("heartbeat-null-owner-subscriptions");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        Map<String, Object> item = new TreeMap<>();
        try {
            MQConsumerInner consumer = (MQConsumerInner) Proxy.newProxyInstance(
                MQConsumerInner.class.getClassLoader(), new Class<?>[] {MQConsumerInner.class},
                (proxy, method, values) -> {
                    switch (method.getName()) {
                        case "groupName": return "null-subscriptions";
                        case "consumeType": return ConsumeType.CONSUME_ACTIVELY;
                        case "messageModel": return MessageModel.CLUSTERING;
                        case "consumeFromWhere": return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
                        case "isUnitMode": return false;
                        case "subscriptions": return null;
                        default: return method.getReturnType() == boolean.class ? false : null;
                    }
                });
            if (!runtime.registerConsumer("null-subscriptions", consumer))
                throw new IllegalStateException("consumer registration");
            Method prepare = MQClientInstance.class.getDeclaredMethod("prepareHeartbeatData", boolean.class);
            prepare.setAccessible(true);
            try {
                prepare.invoke(runtime, false);
                item.put("fullOutcome", "OK");
            } catch (java.lang.reflect.InvocationTargetException error) {
                item.put("fullOutcome", error.getCause().getClass().getSimpleName());
            }
            HeartbeatData simple = (HeartbeatData) prepare.invoke(runtime, true);
            ConsumerData owner = simple.getConsumerDataSet().stream()
                .filter(value -> "null-subscriptions".equals(value.getGroupName())).findFirst().orElseThrow();
            item.put("simpleSubscriptionCount", owner.getSubscriptionDataSet().size());
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("fixture teardown"));
        }
        return item;
    }

    private static Map<String, Object> pushGroupNameMutationCase() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("push-group-name-mutation");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        DefaultMQPushConsumer facade = new DefaultMQPushConsumer("push-group-before");
        DefaultMQPushConsumerImpl impl = facade.getDefaultMQPushConsumerImpl();
        Map<String, Object> item = new TreeMap<>();
        try {
            item.put("defaultConstructorGroup", new DefaultMQPushConsumer().getConsumerGroup());
            item.put("facadeGroupBefore", facade.getConsumerGroup());
            item.put("implementationGroupBefore", impl.groupName());
            if (!runtime.registerConsumer("push-group-before", impl))
                throw new IllegalStateException("push group mutation registration");
            Method prepare = MQClientInstance.class.getDeclaredMethod("prepareHeartbeatData", boolean.class);
            prepare.setAccessible(true);
            HeartbeatData before = (HeartbeatData) prepare.invoke(runtime, false);
            item.put("heartbeatGroupBefore", before.getConsumerDataSet().iterator().next().getGroupName());

            facade.setConsumerGroup("push-group-after");
            item.put("facadeGroupAfter", facade.getConsumerGroup());
            item.put("implementationGroupAfter", impl.groupName());
            HeartbeatData after = (HeartbeatData) prepare.invoke(runtime, false);
            item.put("heartbeatGroupAfter", after.getConsumerDataSet().iterator().next().getGroupName());

            runtime.unregisterConsumer("push-group-after");
            item.put("originalRegistrationRemainsAfterCurrentGroupUnregister",
                runtime.selectConsumer("push-group-before") != null);

            facade.setConsumerGroup(null);
            item.put("facadeGroupNull", facade.getConsumerGroup() == null);
            item.put("implementationGroupNull", impl.groupName() == null);
            HeartbeatData afterNull = (HeartbeatData) prepare.invoke(runtime, false);
            item.put("heartbeatGroupNull", afterNull.getConsumerDataSet().iterator().next().getGroupName() == null);

            runtime.unregisterConsumer("push-group-before");
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("fixture teardown"));
        }
        return item;
    }

    private static void record(List<Object> cases, MQClientInstance runtime, String name,
        boolean withoutSub) throws Exception {
        Method prepare = MQClientInstance.class.getDeclaredMethod("prepareHeartbeatData", boolean.class);
        prepare.setAccessible(true);
        Map<String, Object> item = new TreeMap<>();
        item.put("case", name);
        HeartbeatData data = (HeartbeatData) prepare.invoke(runtime, withoutSub);
        item.put("body", com.alibaba.fastjson2.JSON.parseObject(
            com.alibaba.fastjson2.JSON.toJSONString(data)));
        item.put("fingerprint", data.computeHeartbeatFingerprint());
        cases.add(item);
    }

    private static int preparedProducerCount(MQClientInstance runtime) throws Exception {
        Method prepare = MQClientInstance.class.getDeclaredMethod("prepareHeartbeatData", boolean.class);
        prepare.setAccessible(true);
        HeartbeatData data = (HeartbeatData) prepare.invoke(runtime, false);
        return data.getProducerDataSet().size();
    }

    public static void main(String[] args) throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("192.0.2.6");
        config.setInstanceName("DEFAULT");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        List<Object> cases = new ArrayList<>();
        List<Object> multiplicityCases = new ArrayList<>();
        try {
            record(cases, runtime, "empty", false);
            DefaultMQProducer producer = new DefaultMQProducer("producer");
            if (!runtime.registerProducer("producer", producer.getDefaultMQProducerImpl()))
                throw new IllegalStateException("initial producer registration");
            if (runtime.registerProducer("producer", producer.getDefaultMQProducerImpl()))
                throw new IllegalStateException("duplicate producer accepted");
            record(cases, runtime, "producer-only", false);
            SubscriptionData[] subscription = {new SubscriptionData()};
            subscription[0].setTopic("topic");
            subscription[0].setSubString("*");
            subscription[0].setSubVersion(12L);
            MQConsumerInner consumer = (MQConsumerInner) Proxy.newProxyInstance(
                MQConsumerInner.class.getClassLoader(), new Class<?>[] {MQConsumerInner.class},
                (proxy, method, values) -> {
                    switch (method.getName()) {
                        case "groupName": return "consumer";
                        case "consumeType": return ConsumeType.CONSUME_ACTIVELY;
                        case "messageModel": return MessageModel.CLUSTERING;
                        case "consumeFromWhere": return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
                        case "isUnitMode": return false;
                        case "subscriptions": return Collections.singleton(subscription[0]);
                        default: return method.getReturnType() == boolean.class ? false : null;
                    }
                });
            if (!runtime.registerConsumer("consumer", consumer)) throw new IllegalStateException("consumer registration");
            if (runtime.registerConsumer("consumer", consumer)) throw new IllegalStateException("duplicate consumer accepted");
            record(cases, runtime, "both", false);
            subscription[0] = new SubscriptionData();
            subscription[0].setTopic("other");
            subscription[0].setSubString("tag");
            subscription[0].setSubVersion(13L);
            record(cases, runtime, "subscription-change", false);
            record(cases, runtime, "without-sub", true);
            runtime.unregisterProducer("producer"); // Empty Broker map; no network.
            record(cases, runtime, "consumer-only", false);
            runtime.unregisterConsumer("consumer");
            record(cases, runtime, "after-unregister", false);
            // ConsumerData inherits Object equality: distinct registration keys
            // must remain distinct even when their returned metadata is equal.
            if (!runtime.registerConsumer("registered-a", consumer)
                || !runtime.registerConsumer("registered-b", consumer))
                throw new IllegalStateException("distinct keys rejected");
            recordMultiplicity(multiplicityCases, runtime, "equal-full", false);
            recordMultiplicity(multiplicityCases, runtime, "equal-simple", true);
            runtime.unregisterConsumer("registered-b");
            MQConsumerInner otherConsumer = (MQConsumerInner) Proxy.newProxyInstance(
                MQConsumerInner.class.getClassLoader(), new Class<?>[] {MQConsumerInner.class},
                (proxy, method, values) -> {
                    if ("subscriptions".equals(method.getName())) {
                        SubscriptionData other = new SubscriptionData();
                        other.setTopic("second");
                        other.setSubString("*");
                        other.setSubVersion(14L);
                        return Collections.singleton(other);
                    }
                    return method.invoke(consumer, values);
                });
            if (!runtime.registerConsumer("registered-b", otherConsumer))
                throw new IllegalStateException("replacement registration rejected");
            recordMultiplicity(multiplicityCases, runtime, "different-full", false);
            recordMultiplicity(multiplicityCases, runtime, "different-simple", true);
            runtime.unregisterConsumer("registered-a");
            recordMultiplicity(multiplicityCases, runtime, "one-after-unregister", false);
            runtime.unregisterConsumer("registered-b");
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("fixture teardown"));
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint", "MQClientInstance constructor heartbeat ChannelEventListener selection/registerProducer/registerConsumer/prepareHeartbeatData/unregisterProducer/unregisterConsumer/sendHeartbeatToAllBrokerWithLock and live V2/concurrent-heartbeat flag selection plus concurrent flag and pool-size mutations; HeartbeatData/ConsumerData/ProducerData/SubscriptionData Fastjson scalar inputs; HeartbeatData.computeHeartbeatFingerprint/toString/duplicate-ProducerData decode/nullable producer-consumer-subscription set elements; ProducerData.toString; ConsumerData enum JSON names/toString; ConsumeType.getTypeCN/MessageModel.getModeCN; SubscriptionData.SUB_ALL/constructors/equals/hashCode/compareTo/toString; ProducerData/ConsumerData/HeartbeatData inherited Object equals/hashCode; HeartbeatData inherited RemotingSerializable encode/toJson/decode/decodeList/fromJson methods");
        fixture.put("serverBaselineContacted", false);
        fixture.put("cases", cases);
        fixture.put("multiplicityCases", multiplicityCases);
        fixture.put("subscriptionFieldCases", subscriptionFieldCases());
        fixture.put("subscriptionConstructorCases", subscriptionConstructorCases());
        fixture.put("subscriptionMethods", subscriptionMethods());
        fixture.put("subscriptionDataApiCase", subscriptionDataApiCase());
        fixture.put("producerDataApiCase", producerDataApiCase());
        fixture.put("consumerDataApiCase", consumerDataApiCase());
        fixture.put("heartbeatDataApiCase", heartbeatDataApiCase());
        fixture.put("identityMethodCases", identityMethodCases());
        fixture.put("ownerFieldCases", ownerFieldCases());
        fixture.put("consumerEnumCases", consumerEnumCases());
        fixture.put("consumerEnumMethodCases", consumerEnumMethodCases());
        fixture.put("heartbeatFieldCases", heartbeatFieldCases());
        fixture.put("heartbeatSerializationMethodCases", heartbeatSerializationMethodCases());
        List<Object> nullElementCases = heartbeatNullElementCases();
        fixture.put("heartbeatNullElementCases", nullElementCases);
        fixture.put("heartbeatAliasCases", heartbeatAliasCases());
        fixture.put("fingerprintNullCases", fingerprintNullCases());
        fixture.put("primitiveNullCases", primitiveNullCases());
        fixture.put("heartbeatDtoInputCases", heartbeatDtoInputCases());
        fixture.put("nullOwnerSubscriptionPreparation", nullOwnerSubscriptionPreparation());
        fixture.put("pushGroupNameMutation", pushGroupNameMutationCase());
        fixture.put("channelEventListenerCases", channelEventListenerCases());
        fixture.put("heartbeatV2SettingMutation", heartbeatV2SettingMutation());
        fixture.put("heartbeatVipAddress", heartbeatVipAddress());
        fixture.put("concurrentHeartbeatExecutorMutation", concurrentHeartbeatExecutorMutation());
        fixture.put("concurrentHeartbeatFlagMutationToSerial", concurrentHeartbeatFlagMutationToSerial());
        fixture.put("concurrentHeartbeatPoolSizeMutation", concurrentHeartbeatPoolSizeMutation());
        fixture.put("heartbeatV2WrapperSettingMutation", heartbeatV2WrapperSettingMutation());
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else {
            Files.createDirectories(file.getParent());
            Files.writeString(file, json);
        }
        System.out.println("Runtime heartbeat invoked cases verified: " + cases.size()
            + "; null-element cases: " + nullElementCases.size()
            + "; multiplicity cases: " + multiplicityCases.size());
    }

    private static void recordMultiplicity(List<Object> cases, MQClientInstance runtime,
        String name, boolean withoutSub) throws Exception {
        record(cases, runtime, name, withoutSub);
        @SuppressWarnings("unchecked")
        Map<String, Object> item = (Map<String, Object>) cases.get(cases.size() - 1);
        // Arbitrary JVM object-set iteration is not a fingerprint oracle.
        item.remove("fingerprint");
        com.alibaba.fastjson2.JSONObject body = (com.alibaba.fastjson2.JSONObject) item.get("body");
        int expectedCount = "one-after-unregister".equals(name) ? 1 : 2;
        if (body.getJSONArray("consumerDataSet").size() != expectedCount)
            throw new IllegalStateException("unexpected Java multiplicity: " + name);
        body.getJSONArray("consumerDataSet").sort(java.util.Comparator.comparing(
            com.alibaba.fastjson2.JSON::toJSONString)); // Preserve every duplicate.
    }

    private static List<Object> channelEventListenerCases() throws Exception {
        List<Object> cases = new ArrayList<>();
        for (boolean enabled : new boolean[] {false, true}) {
            ClientConfig config = new ClientConfig();
            config.setClientIP("192.0.2.6");
            config.setEnableHeartbeatChannelEventListener(enabled);
            MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
            try {
                Object remoting = runtime.getMQClientAPIImpl().getRemotingClient();
                Method listenerGetter = remoting.getClass().getMethod("getChannelEventListener");
                Object beforeMutation = listenerGetter.invoke(remoting);
                config.setEnableHeartbeatChannelEventListener(!enabled);
                Object afterMutation = listenerGetter.invoke(remoting);
                Map<String, Object> item = new TreeMap<>();
                item.put("configured", enabled);
                item.put("configAfterMutation", !enabled);
                item.put("listenerInitiallyPresent", beforeMutation != null);
                item.put("listenerAfterMutationPresent", afterMutation != null);
                item.put("sameListenerAfterMutation", beforeMutation == afterMutation);
                cases.add(item);
            } finally {
                Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                    org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
                cleanup.setAccessible(true);
                cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("listener fixture teardown"));
            }
        }
        return cases;
    }

    private static final class HeartbeatModeApi extends org.apache.rocketmq.client.impl.MQClientAPIImpl {
        private final List<String> modes;
        private final List<Boolean> concurrentExecutorThreads;
        private final List<String> addresses;

        HeartbeatModeApi(ClientConfig config, List<String> modes) {
            this(config, modes, null, null);
        }

        HeartbeatModeApi(ClientConfig config, List<String> modes, List<Boolean> concurrentExecutorThreads) {
            this(config, modes, concurrentExecutorThreads, null);
        }

        HeartbeatModeApi(ClientConfig config, List<String> modes, List<Boolean> concurrentExecutorThreads,
            List<String> addresses) {
            super(new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient) Proxy.newProxyInstance(
                    RuntimeHeartbeatFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> method.getReturnType() == boolean.class ? false : null));
            this.modes = modes;
            this.concurrentExecutorThreads = concurrentExecutorThreads;
            this.addresses = addresses;
        }

        private void record(String mode, String address) {
            modes.add(mode);
            if (addresses != null) addresses.add(address);
            if (concurrentExecutorThreads != null) {
                concurrentExecutorThreads.add(Thread.currentThread().getName().startsWith("MQClientConcurrentHeartbeatThread_"));
            }
        }

        @Override
        public int sendHeartbeat(String address, HeartbeatData heartbeat, long timeout) {
            record("V1", address);
            return 551;
        }

        @Override
        public org.apache.rocketmq.remoting.common.HeartbeatV2Result sendHeartbeatV2(
            String address, HeartbeatData heartbeat, long timeout) {
            record("V2", address);
            return new org.apache.rocketmq.remoting.common.HeartbeatV2Result(551, false, true);
        }
    }

    private static Map<String, Object> heartbeatVipAddress() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("heartbeat-vip-address");
        config.setVipChannelEnabled(true);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        List<String> modes = new ArrayList<>();
        List<String> addresses = new ArrayList<>();
        HeartbeatModeApi api = new HeartbeatModeApi(config, modes, null, addresses);
        DefaultMQProducer producer = new DefaultMQProducer("heartbeat-vip-address");
        Map<String, Object> result = new TreeMap<>();
        try {
            if (!runtime.registerProducer("heartbeat-vip-address", producer.getDefaultMQProducerImpl()))
                throw new IllegalStateException("heartbeat VIP fixture producer registration");
            java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            field.setAccessible(true);
            field.set(runtime, api);
            String inputAddress = "127.0.0.1:10911";
            result.put("vipEnabled", config.isVipChannelEnabled());
            result.put("inputAddress", inputAddress);
            result.put("outcome", runtime.sendHeartbeatToBroker(0, "broker", inputAddress));
            result.put("apiAddress", addresses.isEmpty() ? null : addresses.get(0));
            result.put("mode", modes.isEmpty() ? null : modes.get(0));
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            try {
                cleanup.invoke(runtime, original, new IllegalStateException("heartbeat VIP fixture teardown"));
            } finally {
                api.shutdown();
                producer.shutdown();
            }
        }
        return result;
    }

    private static Map<String, Object> heartbeatV2SettingMutation() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("192.0.2.6");
        config.setUseHeartbeatV2(false);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        List<String> modes = new ArrayList<>();
        HeartbeatModeApi api = new HeartbeatModeApi(config, modes);
        DefaultMQProducer producer = new DefaultMQProducer("heartbeat-mode");
        Map<String, Object> result = new TreeMap<>();
        try {
            if (!runtime.registerProducer("heartbeat-mode", producer.getDefaultMQProducerImpl()))
                throw new IllegalStateException("producer registration");
            java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            field.setAccessible(true);
            field.set(runtime, api);
            result.put("configuredBefore", config.isUseHeartbeatV2());
            result.put("v1Outcome", runtime.sendHeartbeatToBroker(0, "broker", "127.0.0.1:10911"));
            config.setUseHeartbeatV2(true);
            result.put("configuredAfterMutation", config.isUseHeartbeatV2());
            result.put("v2Outcome", runtime.sendHeartbeatToBroker(0, "broker", "127.0.0.1:10911"));
            result.put("modeCalls", new ArrayList<>(modes));
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            try { cleanup.invoke(runtime, original, new IllegalStateException("heartbeat mode fixture teardown")); }
            finally {
                api.shutdown();
                producer.shutdown();
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> concurrentHeartbeatExecutorMutation() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("concurrent-heartbeat-executor-mutation");
        config.setEnableConcurrentHeartbeat(false);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        DefaultMQProducer producer = new DefaultMQProducer("fixture-producer");
        Map<String, Object> result = new TreeMap<>();
        try {
            if (!runtime.registerProducer("fixture-producer", producer.getDefaultMQProducerImpl()))
                throw new IllegalStateException("concurrent heartbeat fixture producer registration");
            result.put("preparedProducerCount", preparedProducerCount(runtime));

            java.lang.reflect.Field brokerField = MQClientInstance.class.getDeclaredField("brokerAddrTable");
            brokerField.setAccessible(true);
            Map<String, java.util.HashMap<Long, String>> brokerTable =
                (Map<String, java.util.HashMap<Long, String>>) brokerField.get(runtime);
            java.util.HashMap<Long, String> addresses = new java.util.HashMap<>();
            addresses.put(0L, "127.0.0.1:10911");
            brokerTable.put("fixture-broker", addresses);

            java.lang.reflect.Field executorField = MQClientInstance.class.getDeclaredField("concurrentHeartbeatExecutor");
            executorField.setAccessible(true);
            result.put("executorPresentBeforeMutation", executorField.get(runtime) != null);
            config.setEnableConcurrentHeartbeat(true);
            result.put("configuredAfterMutation", config.isEnableConcurrentHeartbeat());
            result.put("executorPresentAfterMutation", executorField.get(runtime) != null);
            result.put("sendOutcome", runtime.sendHeartbeatToAllBrokerWithLock());
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            try {
                cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("concurrent heartbeat fixture teardown"));
            } finally {
                producer.shutdown();
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> concurrentHeartbeatFlagMutationToSerial() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("concurrent-heartbeat-to-serial");
        config.setEnableConcurrentHeartbeat(true);
        config.setConcurrentHeartbeatThreadPoolSize(2);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        List<String> modes = new ArrayList<>();
        HeartbeatModeApi api = new HeartbeatModeApi(config, modes);
        DefaultMQProducer producer = new DefaultMQProducer("fixture-producer");
        Map<String, Object> result = new TreeMap<>();
        try {
            if (!runtime.registerProducer("fixture-producer", producer.getDefaultMQProducerImpl()))
                throw new IllegalStateException("concurrent-to-serial fixture producer registration");
            result.put("preparedProducerCount", preparedProducerCount(runtime));

            java.lang.reflect.Field brokerField = MQClientInstance.class.getDeclaredField("brokerAddrTable");
            brokerField.setAccessible(true);
            Map<String, java.util.HashMap<Long, String>> brokerTable =
                (Map<String, java.util.HashMap<Long, String>>) brokerField.get(runtime);
            java.util.HashMap<Long, String> addresses = new java.util.HashMap<>();
            addresses.put(0L, "127.0.0.1:10911");
            brokerTable.put("fixture-broker", addresses);

            java.lang.reflect.Field executorField = MQClientInstance.class.getDeclaredField("concurrentHeartbeatExecutor");
            executorField.setAccessible(true);
            result.put("configuredBeforeMutation", config.isEnableConcurrentHeartbeat());
            result.put("executorPresentBeforeMutation", executorField.get(runtime) != null);
            config.setEnableConcurrentHeartbeat(false);
            result.put("configuredAfterMutation", config.isEnableConcurrentHeartbeat());
            result.put("executorPresentAfterMutation", executorField.get(runtime) != null);

            java.lang.reflect.Field apiField = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            apiField.setAccessible(true);
            apiField.set(runtime, api);
            result.put("sendOutcome", runtime.sendHeartbeatToAllBrokerWithLock());
            result.put("sendModes", new ArrayList<>(modes));
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            try { cleanup.invoke(runtime, original, new IllegalStateException("concurrent-to-serial fixture teardown")); }
            finally {
                try { api.shutdown(); }
                finally { producer.shutdown(); }
            }
        }
        return result;
    }

    private static Map<String, Object> concurrentHeartbeatPoolSizeMutation() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("concurrent-heartbeat-pool-size-mutation");
        config.setEnableConcurrentHeartbeat(true);
        config.setConcurrentHeartbeatThreadPoolSize(2);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        Map<String, Object> result = new TreeMap<>();
        try {
            java.lang.reflect.Field executorField = MQClientInstance.class.getDeclaredField("concurrentHeartbeatExecutor");
            executorField.setAccessible(true);
            java.util.concurrent.ThreadPoolExecutor executor =
                (java.util.concurrent.ThreadPoolExecutor) executorField.get(runtime);
            result.put("configuredBeforeMutation", config.getConcurrentHeartbeatThreadPoolSize());
            result.put("executorCoreSizeBeforeMutation", executor.getCorePoolSize());
            config.setConcurrentHeartbeatThreadPoolSize(1);
            result.put("configuredAfterMutation", config.getConcurrentHeartbeatThreadPoolSize());
            result.put("executorCoreSizeAfterMutation", executor.getCorePoolSize());
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("heartbeat pool-size fixture teardown"));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> heartbeatV2WrapperSettingMutation() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("heartbeat-v2-wrapper-mutation");
        config.setUseHeartbeatV2(false);
        config.setEnableConcurrentHeartbeat(true);
        config.setConcurrentHeartbeatThreadPoolSize(2);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        List<String> modes = new ArrayList<>();
        List<Boolean> sendOnConcurrentExecutor = new ArrayList<>();
        HeartbeatModeApi api = new HeartbeatModeApi(config, modes, sendOnConcurrentExecutor);
        DefaultMQProducer producer = new DefaultMQProducer("fixture-producer");
        Map<String, Object> result = new TreeMap<>();
        try {
            if (!runtime.registerProducer("fixture-producer", producer.getDefaultMQProducerImpl()))
                throw new IllegalStateException("V2 wrapper fixture producer registration");

            java.lang.reflect.Field brokerField = MQClientInstance.class.getDeclaredField("brokerAddrTable");
            brokerField.setAccessible(true);
            Map<String, java.util.HashMap<Long, String>> brokerTable =
                (Map<String, java.util.HashMap<Long, String>>) brokerField.get(runtime);
            java.util.HashMap<Long, String> addresses = new java.util.HashMap<>();
            addresses.put(0L, "127.0.0.1:10911");
            brokerTable.put("fixture-broker", addresses);

            java.lang.reflect.Field executorField = MQClientInstance.class.getDeclaredField("concurrentHeartbeatExecutor");
            executorField.setAccessible(true);
            result.put("configuredBeforeMutation", config.isUseHeartbeatV2());
            result.put("concurrentConfigured", config.isEnableConcurrentHeartbeat());
            result.put("executorPresent", executorField.get(runtime) != null);

            java.lang.reflect.Field apiField = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            apiField.setAccessible(true);
            apiField.set(runtime, api);
            result.put("v1WrapperOutcome", runtime.sendHeartbeatToAllBrokerWithLock());
            result.put("v1WithLockV2Outcome", runtime.sendHeartbeatToAllBrokerWithLockV2(false));
            config.setUseHeartbeatV2(true);
            result.put("configuredAfterMutation", config.isUseHeartbeatV2());
            result.put("v2WrapperOutcome", runtime.sendHeartbeatToAllBrokerWithLock());
            result.put("v2WithLockV2Outcome", runtime.sendHeartbeatToAllBrokerWithLockV2(false));
            result.put("sendModes", new ArrayList<>(modes));
            result.put("sendOnConcurrentExecutor", new ArrayList<>(sendOnConcurrentExecutor));
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            try { cleanup.invoke(runtime, original, new IllegalStateException("V2 wrapper mutation fixture teardown")); }
            finally {
                try { api.shutdown(); }
                finally { producer.shutdown(); }
            }
        }
        return result;
    }
}
