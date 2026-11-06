package compat;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Field;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageClientExt;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.common.sysflag.PullSysFlag;

/** Deterministic Java 5.5.1 message-record bytes and decoded outcomes. */
public final class MessageDecodeFixtures {
    private static final int MAGIC_V2 = -626843477;

    private MessageDecodeFixtures() { }

    public static void main(String[] args) throws Exception {
        Path output;
        boolean verify;
        if (args.length == 2) {
            output = Path.of(args[0]);
            verify = Boolean.parseBoolean(args[1]);
        } else if (args.length == 3 && "--output".equals(args[0])) {
            output = Path.of(args[1]);
            verify = Boolean.parseBoolean(args[2]);
        } else {
            throw new IllegalArgumentException("usage: MessageDecodeFixtures OUTPUT true|false");
        }
        run(output, verify);
    }

    public static void run(Path output, boolean verify) throws Exception {
        Map<String, Object> root = new TreeMap<>();
        root.put("provenance", Map.of(
            "client", "org.apache.rocketmq:rocketmq-client:5.5.1",
            "sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9",
            "serverContacted", false,
            "generator", "compat.MessageDecodeFixtures"
        ));
        root.put("messageClientExtApi", messageClientExtApi());
        root.put("messageExtPublicApi", messageExtPublicApi());
        root.put("messageExtHostApi", messageExtHostApi());
        root.put("messageExtSystemFlagsApi", messageExtSystemFlagsApi());
        root.put("messagePublicApi", messagePublicApi());
        root.put("messageQueuePublicApi", messageQueuePublicApi());
        root.put("messageDecoderPublicApi", messageDecoderPublicApi());
        root.put("messageSysFlagPublicApi", messageSysFlagPublicApi());
        root.put("pullSysFlagPublicApi", pullSysFlagPublicApi());
        root.put("messageConstantsApi", messageConstantsApi());
        root.put("messageVersionApi", messageVersionApi());
        root.put("messageDecoderPropertiesApi", messageDecoderPropertiesApi());
        root.put("messageDecoderCrcApi", messageDecoderCrcApi());
        root.put("messageCompactApi", messageCompactApi());
        root.put("messageBatchDecodeApi", messageBatchDecodeApi());
        root.put("messageExtEncodeApi", messageExtEncodeApi());
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(record("ipv4-v1", message("orders", "hello-v1", 17, 41,
            InetAddress.getByName("192.0.2.10"), 10911,
            InetAddress.getByName("198.51.100.20"), 10911, 0,
            Map.of("TAGS", "green", "UNIQ_KEY", "producer-unique-1")), null));
        cases.add(record("no-tags-v1", message("orders", "without-tags", 26, 50,
            InetAddress.getByName("192.0.2.18"), 10928,
            InetAddress.getByName("198.51.100.27"), 10929, 0, Map.of()), null));
        cases.add(record("born-ipv6-v1", message("orders", "hello-ipv6", 18, 42,
            InetAddress.getByName("2001:db8::10"), 10912,
            InetAddress.getByName("198.51.100.21"), 10913,
            MessageSysFlag.BORNHOST_V6_FLAG,
            Map.of("TAGS", "blue")), null));
        String longTopic = "long-" + "t".repeat(130);
        cases.add(record("store-ipv6-v2", message("long-v2", "hello-v2", 19, 43,
            InetAddress.getByName("192.0.2.12"), 10914,
            InetAddress.getByName("2001:db8::20"), 10915,
            MessageSysFlag.STOREHOSTADDRESS_V6_FLAG,
            Map.of("TAGS", "violet", "TRAN_MSG", "true", "UNIQ_KEY", "producer-unique-2")), longTopic));
        cases.add(record("born-and-store-ipv6-v1", message("dual-ipv6", "both-hosts-ipv6", 21, 45,
            InetAddress.getByName("2001:db8::30"), 10918,
            InetAddress.getByName("2001:db8::40"), 10919,
            MessageSysFlag.BORNHOST_V6_FLAG | MessageSysFlag.STOREHOSTADDRESS_V6_FLAG,
            Map.of("TAGS", "dual")), null));
        byte[] compressedBody = "zstd-body-".repeat(96).getBytes(StandardCharsets.UTF_8);
        cases.add(record("compressed-zstd-v1", message("compressed", compressedBody, 20, 44,
            InetAddress.getByName("192.0.2.13"), 10916,
            InetAddress.getByName("198.51.100.22"), 10917,
            MessageSysFlag.COMPRESSED_FLAG | MessageSysFlag.COMPRESSION_ZSTD_TYPE,
            Map.of("TAGS", "compressed")), null));
        cases.add(record("compressed-lz4-v1", message("compressed-lz4", compressedBody, 22, 46,
            InetAddress.getByName("192.0.2.14"), 10920,
            InetAddress.getByName("198.51.100.23"), 10921,
            MessageSysFlag.COMPRESSED_FLAG | MessageSysFlag.COMPRESSION_LZ4_TYPE,
            Map.of("TAGS", "compressed-lz4")), null));
        cases.add(record("compressed-zlib-v1", message("compressed-zlib", compressedBody, 23, 47,
            InetAddress.getByName("192.0.2.15"), 10922,
            InetAddress.getByName("198.51.100.24"), 10923,
            MessageSysFlag.COMPRESSED_FLAG | MessageSysFlag.COMPRESSION_ZLIB_TYPE,
            Map.of("TAGS", "compressed-zlib")), null));
        Message firstInner = new Message("orders", "inner-one".getBytes(StandardCharsets.UTF_8));
        firstInner.setFlag(301);
        MessageAccessor.putProperty(firstInner, "TAGS", "green");
        MessageAccessor.putProperty(firstInner, "UNIQ_KEY", "inner-unique-1");
        Message secondInner = new Message("orders", "inner-two".getBytes(StandardCharsets.UTF_8));
        secondInner.setFlag(302);
        MessageAccessor.putProperty(secondInner, "TAGS", "blue");
        byte[] innerBatch = MessageDecoder.encodeMessages(List.of(firstInner, secondInner));
        cases.add(record("inner-batch", message("orders", innerBatch, 24, 48,
            InetAddress.getByName("192.0.2.16"), 10924,
            InetAddress.getByName("198.51.100.25"), 10925,
            MessageSysFlag.INNER_BATCH_FLAG | MessageSysFlag.NEED_UNWRAP_FLAG,
            Map.of("TAGS", "outer")), null));
        cases.add(record("inner-batch-malformed", message("orders", new byte[] {0, 0, 0, 8}, 25, 49,
            InetAddress.getByName("192.0.2.17"), 10926,
            InetAddress.getByName("198.51.100.26"), 10927,
            MessageSysFlag.INNER_BATCH_FLAG | MessageSysFlag.NEED_UNWRAP_FLAG,
            Map.of("TAGS", "outer")), null));
        cases.add(record("empty-body-v1", message("orders", new byte[0], 27, 51,
            InetAddress.getByName("192.0.2.19"), 10930,
            InetAddress.getByName("198.51.100.28"), 10931, 0, Map.of()), null));

        root.put("cases", cases);
        String json = JSON.toJSONString(root, JSONWriter.Feature.MapSortField) + "\n";
        if (verify) {
            String checkedIn = Files.readString(output, StandardCharsets.UTF_8);
            if (!json.equals(checkedIn)) {
                throw new IllegalStateException("message decode fixture drift: " + output);
            }
            System.out.println("verified " + cases.size() + " Java message decode fixtures at " + output);
        } else {
            Files.createDirectories(output.getParent());
            Files.writeString(output, json, StandardCharsets.UTF_8);
            System.out.println("wrote " + cases.size() + " Java message decode fixtures to " + output);
        }
    }

    private static Map<String, Object> messagePublicApi() throws Exception {
        Message message = new Message("orders", "green", "first second", 17,
            new byte[] {0, -1, 42}, false);
        Map<String, Object> expected = new TreeMap<>();
        expected.put("messagePublicConstructors", publicConstructorSignatures(Message.class));
        expected.put("messagePublicMethods", publicMethodSignatures(Message.class));
        expected.put("messageAccessorPublicMethods", publicMethodSignatures(MessageAccessor.class));
        Message defaultMessage = new Message();
        expected.put("defaultTopicIsNull", defaultMessage.getTopic() == null);
        expected.put("defaultBodyIsNull", defaultMessage.getBody() == null);
        expected.put("defaultPropertiesIsNull", defaultMessage.getProperties() == null);
        expected.put("defaultToString", defaultMessage.toString());
        expected.put("messageEqualsSelf", defaultMessage.equals(defaultMessage));
        expected.put("messageEqualsDistinctSameState", defaultMessage.equals(new Message()));
        expected.put("messageEqualsDeclaringClass",
            Message.class.getMethod("equals", Object.class).getDeclaringClass().getName());
        expected.put("messageHashCodeDeclaringClass",
            Message.class.getMethod("hashCode").getDeclaringClass().getName());
        Message messageHashProbe = new Message();
        int messageHashBeforeMutation = messageHashProbe.hashCode();
        messageHashProbe.setTopic("hash-mutated-topic");
        expected.put("messageHashCodeStableAfterMutation",
            messageHashBeforeMutation == messageHashProbe.hashCode());
        MessageExt defaultMessageExt = new MessageExt();
        expected.put("messageExtEqualsSelf", defaultMessageExt.equals(defaultMessageExt));
        expected.put("messageExtEqualsDistinctSameState",
            defaultMessageExt.equals(new MessageExt()));
        expected.put("messageExtEqualsDeclaringClass",
            MessageExt.class.getMethod("equals", Object.class).getDeclaringClass().getName());
        expected.put("messageExtHashCodeDeclaringClass",
            MessageExt.class.getMethod("hashCode").getDeclaringClass().getName());
        MessageExt messageExtHashProbe = new MessageExt();
        int messageExtHashBeforeMutation = messageExtHashProbe.hashCode();
        messageExtHashProbe.setQueueId(9);
        expected.put("messageExtHashCodeStableAfterMutation",
            messageExtHashBeforeMutation == messageExtHashProbe.hashCode());
        Message messageExtBaseView = defaultMessageExt;
        expected.put("messageExtEqualsBaseView", defaultMessageExt.equals(messageExtBaseView));
        expected.put("baseViewEqualsMessageExt", messageExtBaseView.equals(defaultMessageExt));
        MessageClientExt defaultMessageClientExt = new MessageClientExt();
        expected.put("messageClientExtEqualsSelf",
            defaultMessageClientExt.equals(defaultMessageClientExt));
        expected.put("messageClientExtEqualsDistinctSameState",
            defaultMessageClientExt.equals(new MessageClientExt()));
        expected.put("messageClientExtEqualsDeclaringClass",
            MessageClientExt.class.getMethod("equals", Object.class).getDeclaringClass().getName());
        expected.put("messageClientExtHashCodeDeclaringClass",
            MessageClientExt.class.getMethod("hashCode").getDeclaringClass().getName());
        MessageClientExt messageClientHashProbe = new MessageClientExt();
        int messageClientHashBeforeMutation = messageClientHashProbe.hashCode();
        messageClientHashProbe.setTopic("hash-mutated-topic");
        expected.put("messageClientExtHashCodeStableAfterMutation",
            messageClientHashBeforeMutation == messageClientHashProbe.hashCode());
        MessageClientExt clientBaseView = defaultMessageClientExt;
        MessageExt clientExtView = defaultMessageClientExt;
        expected.put("messageClientExtEqualsBaseView",
            defaultMessageClientExt.equals(clientBaseView));
        expected.put("baseViewEqualsMessageClientExt",
            clientBaseView.equals(defaultMessageClientExt));
        expected.put("messageClientExtEqualsExtView",
            defaultMessageClientExt.equals(clientExtView));
        defaultMessage.getProperty("missing");
        expected.put("propertyLookupInitializesProperties",
            defaultMessage.getProperties() != null && defaultMessage.getProperties().isEmpty());

        Message nullableConstructor = new Message(null, (byte[]) null);
        expected.put("nullableConstructorTopicIsNull", nullableConstructor.getTopic() == null);
        expected.put("nullableConstructorBodyIsNull", nullableConstructor.getBody() == null);
        expected.put("nullableConstructorWaitStore", nullableConstructor.isWaitStoreMsgOK());
        Message nullableTagsConstructor = new Message(null, null, (byte[]) null);
        expected.put("nullableTagsConstructorTopicIsNull", nullableTagsConstructor.getTopic() == null);
        expected.put("nullableTagsConstructorBodyIsNull", nullableTagsConstructor.getBody() == null);
        expected.put("nullableTagsConstructorHasTags", nullableTagsConstructor.hasProperty("TAGS"));
        Message nullableKeysConstructor = new Message(null, null, null, (byte[]) null);
        expected.put("nullableKeysConstructorTopicIsNull", nullableKeysConstructor.getTopic() == null);
        expected.put("nullableKeysConstructorBodyIsNull", nullableKeysConstructor.getBody() == null);
        expected.put("nullableKeysConstructorHasTags", nullableKeysConstructor.hasProperty("TAGS"));
        expected.put("nullableKeysConstructorHasKeys", nullableKeysConstructor.hasProperty("KEYS"));
        Message nullableFields = new Message(null, null, null, -3, null, false);
        expected.put("nullableFieldsTopicIsNull", nullableFields.getTopic() == null);
        expected.put("nullableFieldsBodyIsNull", nullableFields.getBody() == null);
        expected.put("nullableFieldsWaitStore", nullableFields.isWaitStoreMsgOK());

        byte[] twoArgumentBody = new byte[] {1, 2};
        Message twoArgumentBodyMessage = new Message("two-argument", twoArgumentBody);
        expected.put("constructorBodyAliasTwoArguments",
            twoArgumentBodyMessage.getBody() == twoArgumentBody);
        twoArgumentBody[0] = 9;
        expected.put("constructorBodyMutationVisibleForTwoArguments",
            twoArgumentBodyMessage.getBody()[0] == 9);
        byte[] tagsBody = new byte[] {3, 4};
        Message tagsBodyMessage = new Message("with-tags", "green", tagsBody);
        expected.put("constructorBodyAliasWithTags", tagsBodyMessage.getBody() == tagsBody);
        byte[] tagsAndKeysBody = new byte[] {5, 6};
        Message tagsAndKeysBodyMessage =
            new Message("with-tags-and-keys", "blue", "key", tagsAndKeysBody);
        expected.put("constructorBodyAliasWithTagsAndKeys",
            tagsAndKeysBodyMessage.getBody() == tagsAndKeysBody);
        byte[] fullFieldsBody = new byte[] {7, 8};
        Message fullFieldsBodyMessage =
            new Message("with-fields", "yellow", "key", -11, fullFieldsBody, false);
        expected.put("constructorBodyAliasWithAllFields",
            fullFieldsBodyMessage.getBody() == fullFieldsBody);

        Message nullableSetters = new Message("before", new byte[] {1});
        nullableSetters.setTopic(null);
        nullableSetters.setBody(null);
        expected.put("nullableSetterTopicIsNull", nullableSetters.getTopic() == null);
        expected.put("nullableSetterBodyIsNull", nullableSetters.getBody() == null);

        byte[] callerBody = new byte[] {1, 2, 3};
        Message bodyAlias = new Message();
        bodyAlias.setBody(callerBody);
        expected.put("setBodyRetainsCallerArray", bodyAlias.getBody() == callerBody);
        callerBody[0] = 9;
        expected.put("setBodyCallerMutationValue", callerBody[0]);
        expected.put("setBodyCallerMutationVisibleToMessage",
            bodyAlias.getBody()[0] == callerBody[0]);
        bodyAlias.getBody()[1] = 8;
        expected.put("setBodyMessageMutationValue", bodyAlias.getBody()[1]);
        expected.put("setBodyMessageMutationVisibleToCaller", callerBody[1] == 8);
        byte[] replacementBody = new byte[] {4, 5, 6};
        bodyAlias.setBody(replacementBody);
        expected.put("setBodyReplacementRetainsNewArray", bodyAlias.getBody() == replacementBody);
        callerBody[2] = 10;
        expected.put("setBodyReplacementDetachesOldArray", bodyAlias.getBody()[2] == 6);

        Message nullableProperties = new Message();
        nullableProperties.setTags(null);
        nullableProperties.setBuyerId(null);
        nullableProperties.setInstanceId(null);
        expected.put("nullTagsGetterIsNull", nullableProperties.getTags() == null);
        expected.put("nullTagsRemainsPresent", nullableProperties.hasProperty("TAGS"));
        expected.put("nullBuyerGetterIsNull", nullableProperties.getBuyerId() == null);
        expected.put("nullBuyerRemainsPresent", nullableProperties.hasProperty("BUYER_ID"));
        expected.put("nullInstanceGetterIsNull",
            nullableProperties.getProperty("INSTANCE_ID") == null);
        expected.put("nullInstanceRemainsPresent", nullableProperties.hasProperty("INSTANCE_ID"));
        expected.put("nullablePropertyEntries", nullableProperties.getProperties().entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(entry -> java.util.Arrays.asList(entry.getKey(), entry.getValue()))
            .collect(java.util.stream.Collectors.toList()));
        Message nullableString = new Message();
        nullableString.setTags(null);
        expected.put("nullablePropertyString", nullableString.toString());
        expected.put("nullPropertyNameRejected",
            throwsIllegalArgument(() -> nullableProperties.putUserProperty(null, "value")));
        expected.put("nullPropertyValueRejected",
            throwsIllegalArgument(() -> nullableProperties.putUserProperty("key", null)));
        expected.put("nonBreakingSpaceUserPropertyNameAccepted",
            !throwsIllegalArgument(() -> new Message().putUserProperty("\u00a0", "value")));
        expected.put("emSpaceUserPropertyValueAccepted",
            !throwsIllegalArgument(() -> new Message().putUserProperty("key", "\u2003")));
        expected.put("asciiControlOnlyUserPropertyRejected",
            throwsIllegalArgument(() -> new Message().putUserProperty("\u001f ", "value")));
        String dynamicReservedProperty = "__RMQ_DYNAMIC_RESERVED_FIXTURE__";
        boolean dynamicWasReserved = MessageConst.STRING_HASH_SET.contains(dynamicReservedProperty);
        MessageConst.STRING_HASH_SET.add(dynamicReservedProperty);
        try {
            expected.put("mutatedReservedPropertyRejected",
                throwsRuntimeException(() -> new Message().putUserProperty(dynamicReservedProperty, "value")));
        } finally {
            if (!dynamicWasReserved) MessageConst.STRING_HASH_SET.remove(dynamicReservedProperty);
        }
        boolean nullWasReserved = MessageConst.STRING_HASH_SET.contains(null);
        MessageConst.STRING_HASH_SET.add(null);
        try {
            expected.put("mutatedNullReservedPropertyPresent", MessageConst.STRING_HASH_SET.contains(null));
            expected.put("nullReservedPropertyExceptionClass",
                runtimeExceptionClass(() -> new Message().putUserProperty(null, "value")));
        } finally {
            if (!nullWasReserved) MessageConst.STRING_HASH_SET.remove(null);
        }
        boolean emptyWasReserved = MessageConst.STRING_HASH_SET.contains("");
        MessageConst.STRING_HASH_SET.add("");
        try {
            expected.put("mutatedEmptyReservedPropertyPresent", MessageConst.STRING_HASH_SET.contains(""));
            expected.put("emptyReservedPropertyExceptionClass",
                runtimeExceptionClass(() -> new Message().putUserProperty("", "value")));
        } finally {
            if (!emptyWasReserved) MessageConst.STRING_HASH_SET.remove("");
        }
        boolean keysWasReserved = MessageConst.STRING_HASH_SET.remove(MessageConst.PROPERTY_KEYS);
        try {
            expected.put("removedReservedPropertyAccepted",
                keysWasReserved && !throwsRuntimeException(
                    () -> new Message().putUserProperty(MessageConst.PROPERTY_KEYS, "user-value")));
        } finally {
            if (keysWasReserved) MessageConst.STRING_HASH_SET.add(MessageConst.PROPERTY_KEYS);
        }

        Message nullNameLookup = new Message();
        expected.put("nullNameLookupIsNull", nullNameLookup.getProperty(null) == null);
        expected.put("nullNameLookupInitializesProperties", nullNameLookup.getProperties() != null);
        Message nullNameContains = new Message();
        expected.put("nullNameContainsIsFalse", !nullNameContains.hasProperty(null));
        expected.put("nullNameContainsKeepsPropertiesNull", nullNameContains.getProperties() == null);
        expected.put("nullUserNameLookupIsNull", new Message().getUserProperty(null) == null);

        Message replacedProperties = new Message();
        MessageAccessor.setProperties(replacedProperties,
            new HashMap<>(Map.of("old", "value")));
        expected.put("setPropertiesInitialValue", replacedProperties.getProperty("old"));
        Map<String, String> replacementProperties = new HashMap<>();
        replacementProperties.put("new", "replacement");
        MessageAccessor.setProperties(replacedProperties, replacementProperties);
        expected.put("setPropertiesReplacesOldKey", !replacedProperties.hasProperty("old"));
        expected.put("setPropertiesReplacementValue", replacedProperties.getProperty("new"));
        MessageAccessor.setProperties(replacedProperties, new HashMap<>());
        expected.put("setPropertiesEmptyMapIsPresent",
            replacedProperties.getProperties() != null && replacedProperties.getProperties().isEmpty());
        MessageAccessor.setProperties(replacedProperties, null);
        expected.put("setPropertiesNullMapIsNull", replacedProperties.getProperties() == null);

        Message sharedPropertiesMessage = new Message("shared-properties-topic", new byte[] {1});
        Map<String, String> callerProperties = new HashMap<>();
        callerProperties.put("initial", "caller");
        MessageAccessor.setProperties(sharedPropertiesMessage, callerProperties);
        expected.put("setPropertiesRetainsCallerMap", sharedPropertiesMessage.getProperties() == callerProperties);
        callerProperties.put("caller-mutation", "visible-to-message");
        expected.put("setPropertiesCallerMutationVisible",
            sharedPropertiesMessage.getProperty("caller-mutation"));
        sharedPropertiesMessage.getProperties().put("message-mutation", "visible-to-caller");
        expected.put("setPropertiesMessageMutationVisible",
            callerProperties.get("message-mutation"));

        Message accessorCloneSource = new Message("clone-topic", new byte[] {0, -1, 42});
        accessorCloneSource.setFlag(29);
        accessorCloneSource.setTransactionId("source-transaction");
        MessageAccessor.putProperty(accessorCloneSource, "clone-key", "clone-value");
        Message accessorClone = MessageAccessor.cloneMessage(accessorCloneSource);
        expected.put("accessorCloneIsDistinct", accessorClone != accessorCloneSource);
        expected.put("accessorCloneTopic", accessorClone.getTopic());
        expected.put("accessorCloneBodyBase64",
            Base64.getEncoder().encodeToString(accessorClone.getBody()));
        expected.put("accessorCloneFlag", accessorClone.getFlag());
        expected.put("accessorCloneProperty", accessorClone.getProperty("clone-key"));
        expected.put("accessorCloneTransactionIdIsNull", accessorClone.getTransactionId() == null);
        expected.put("accessorCloneSourceTransactionId", accessorCloneSource.getTransactionId());

        Message aliasSource = new Message("clone-alias-topic", new byte[] {1, 2, 3});
        Map<String, String> aliasProperties = new HashMap<>();
        aliasProperties.put("shared", "initial");
        MessageAccessor.setProperties(aliasSource, aliasProperties);
        Message aliasClone = MessageAccessor.cloneMessage(aliasSource);
        expected.put("accessorCloneSharesBodyArray", aliasClone.getBody() == aliasSource.getBody());
        expected.put("accessorCloneSharesPropertiesMap",
            aliasClone.getProperties() == aliasSource.getProperties());
        aliasClone.getBody()[0] = 9;
        expected.put("accessorCloneBodyMutationVisibleInSource", aliasSource.getBody()[0]);
        aliasSource.getProperties().put("source-only", "source");
        expected.put("accessorClonePropertyMutationVisibleInClone",
            aliasClone.getProperty("source-only"));
        aliasClone.getProperties().put("clone-only", "clone");
        expected.put("accessorClonePropertyMutationVisibleInSource",
            aliasSource.getProperty("clone-only"));
        aliasClone.setBody(new byte[] {7, 8});
        expected.put("accessorCloneSetBodyDetachesArray",
            aliasClone.getBody() != aliasSource.getBody());
        expected.put("accessorCloneSetBodyLeavesSourceArray", aliasSource.getBody()[0]);
        Map<String, String> detachedProperties = new HashMap<>();
        detachedProperties.put("detached", "value");
        MessageAccessor.setProperties(aliasClone, detachedProperties);
        expected.put("accessorCloneSetPropertiesDetachesMap",
            aliasClone.getProperties() != aliasSource.getProperties());
        aliasClone.getProperties().put("clone-after-detach", "clone");
        expected.put("accessorCloneDetachedPropertyMutationLeavesSource",
            !aliasSource.getProperties().containsKey("clone-after-detach"));

        Map<String, String> deepCopySource = new HashMap<>();
        deepCopySource.put(null, null);
        deepCopySource.put("present", "original");
        Map<String, String> deepCopy = MessageAccessor.deepCopyProperties(deepCopySource);
        expected.put("deepCopyPropertiesNullMapIsNull",
            MessageAccessor.deepCopyProperties(null) == null);
        expected.put("deepCopyPropertiesReturnsDistinctMap", deepCopy != deepCopySource);
        expected.put("deepCopyPropertiesPreservesNullKeyAndValue",
            deepCopy.containsKey(null) && deepCopy.get(null) == null);
        expected.put("deepCopyPropertiesInitialValue", deepCopy.get("present"));
        deepCopySource.put("source-only", "source");
        expected.put("deepCopyPropertiesIgnoresSourceMutation", !deepCopy.containsKey("source-only"));
        deepCopy.put("copy-only", "copy");
        expected.put("deepCopyPropertiesSourceIgnoresCopyMutation", !deepCopySource.containsKey("copy-only"));

        Message accessorHelperMessage = new Message();
        Map<String, Object> accessorHelperValues = new TreeMap<>();
        MessageAccessor.setTransferFlag(accessorHelperMessage, "transfer");
        accessorHelperValues.put("transferFlag", MessageAccessor.getTransferFlag(accessorHelperMessage));
        MessageAccessor.setCorrectionFlag(accessorHelperMessage, "correction");
        accessorHelperValues.put("correctionFlag", MessageAccessor.getCorrectionFlag(accessorHelperMessage));
        MessageAccessor.setOriginMessageId(accessorHelperMessage, "origin-id");
        accessorHelperValues.put("originMessageId", MessageAccessor.getOriginMessageId(accessorHelperMessage));
        MessageAccessor.setMQ2Flag(accessorHelperMessage, "mq2");
        accessorHelperValues.put("mq2Flag", MessageAccessor.getMQ2Flag(accessorHelperMessage));
        MessageAccessor.setReconsumeTime(accessorHelperMessage, "4");
        accessorHelperValues.put("reconsumeTime", MessageAccessor.getReconsumeTime(accessorHelperMessage));
        MessageAccessor.setMaxReconsumeTimes(accessorHelperMessage, "8");
        accessorHelperValues.put("maxReconsumeTimes", MessageAccessor.getMaxReconsumeTimes(accessorHelperMessage));
        MessageAccessor.setConsumeStartTimeStamp(accessorHelperMessage, "1700000000123");
        accessorHelperValues.put("consumeStartTimeStamp",
            MessageAccessor.getConsumeStartTimeStamp(accessorHelperMessage));
        MessageAccessor.setLiteTopic(accessorHelperMessage, "orders-lite");
        accessorHelperValues.put("liteTopic",
            accessorHelperMessage.getProperty(MessageConst.PROPERTY_LITE_TOPIC));
        expected.put("messageAccessorHelperValues", accessorHelperValues);
        MessageAccessor.setTransferFlag(accessorHelperMessage, null);
        accessorHelperValues.put("transferFlagNullIsPresent",
            accessorHelperMessage.hasProperty(MessageConst.PROPERTY_TRANSFER_FLAG));
        accessorHelperValues.put("transferFlagNullValue", MessageAccessor.getTransferFlag(accessorHelperMessage));

        Message nullKeyProperty = new Message();
        MessageAccessor.putProperty(nullKeyProperty, null, "value");
        expected.put("nullKeyPropertyPresent", nullKeyProperty.hasProperty(null));
        expected.put("nullKeyPropertyValue", nullKeyProperty.getProperty(null));
        expected.put("nullKeyPropertyString", nullKeyProperty.toString());
        expected.put("nullKeyPropertyEncoded",
            MessageDecoder.messageProperties2String(nullKeyProperty.getProperties()));
        MessageAccessor.clearProperty(nullKeyProperty, null);
        expected.put("nullKeyPropertyCleared", !nullKeyProperty.hasProperty(null));
        expected.put("nullKeyPropertyMapRemainsPresent", nullKeyProperty.getProperties() != null);

        Message nullKeyNullValue = new Message();
        MessageAccessor.putProperty(nullKeyNullValue, null, null);
        expected.put("nullKeyNullValuePresent", nullKeyNullValue.hasProperty(null));
        expected.put("nullKeyNullValueEncoded",
            MessageDecoder.messageProperties2String(nullKeyNullValue.getProperties()));

        Map<String, String> nullKeyReplacement = new HashMap<>();
        nullKeyReplacement.put(null, "replacement");
        Message nullKeySetProperties = new Message();
        MessageAccessor.setProperties(nullKeySetProperties, nullKeyReplacement);
        expected.put("setPropertiesNullKeyPresent", nullKeySetProperties.hasProperty(null));
        expected.put("setPropertiesNullKeyValue", nullKeySetProperties.getProperty(null));
        expected.put("setPropertiesNullKeyEncoded",
            MessageDecoder.messageProperties2String(nullKeySetProperties.getProperties()));

        Message collectionWithNull = new Message();
        collectionWithNull.setKeys(java.util.Arrays.asList("left", null, "right"));
        expected.put("collectionKeysWithNullMember", collectionWithNull.getKeys());
        expected.put("nullCollectionKeysThrows",
            throwsNullPointer(() -> new Message().setKeys((java.util.Collection<String>) null)));

        expected.put("topic", message.getTopic());
        expected.put("tags", message.getTags());
        expected.put("keys", message.getKeys());
        expected.put("flag", message.getFlag());
        expected.put("body", Base64.getEncoder().encodeToString(message.getBody()));
        expected.put("waitStore", message.isWaitStoreMsgOK());
        expected.put("propertyTags", message.getProperty("TAGS"));
        expected.put("userPropertyTags", message.getUserProperty("TAGS"));
        expected.put("missingProperty", message.getProperty("missing"));
        expected.put("hasTags", message.hasProperty("TAGS"));
        expected.put("hasMissing", message.hasProperty("missing"));

        Message directSetters = new Message();
        expected.put("defaultTransactionId", directSetters.getTransactionId());
        directSetters.setTags("setter-tag");
        directSetters.setKeys("setter-key");
        directSetters.setWaitStoreMsgOK(false);
        directSetters.setTransactionId("transaction-setter");
        expected.put("setTagsGetter", directSetters.getTags());
        expected.put("setKeysStringGetter", directSetters.getKeys());
        expected.put("setWaitStoreMsgOKFalse", directSetters.isWaitStoreMsgOK());
        expected.put("setWaitStoreMsgOKProperty",
            directSetters.getProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK));
        expected.put("setTransactionIdGetter", directSetters.getTransactionId());
        directSetters.setWaitStoreMsgOK(true);
        expected.put("setWaitStoreMsgOKTrue", directSetters.isWaitStoreMsgOK());
        directSetters.setTags(null);
        directSetters.setKeys((String) null);
        directSetters.setTransactionId(null);
        expected.put("nullSetterTagsRemainsPresent", directSetters.hasProperty("TAGS"));
        expected.put("nullSetterTagsGetter", directSetters.getTags());
        expected.put("nullSetterKeysRemainsPresent", directSetters.hasProperty("KEYS"));
        expected.put("nullSetterKeysGetter", directSetters.getKeys());
        expected.put("nullSetterTransactionId", directSetters.getTransactionId());

        message.setKeys(List.of("third", "fourth"));
        expected.put("collectionKeys", message.getKeys());
        message.setBuyerId("buyer-1");
        message.setInstanceId("instance-1");
        expected.put("buyerId", message.getBuyerId());
        expected.put("instanceIdProperty", message.getProperty("INSTANCE_ID"));

        Message priority = new Message();
        expected.put("absentPriority", priority.getPriority());
        expected.put("absentPriorityInitializesProperties", priority.getProperties() != null);
        priority.setPriority(17);
        expected.put("setPriority", priority.getPriority());
        expected.put("setPriorityProperty", priority.getProperty(MessageConst.PROPERTY_PRIORITY));
        expected.put("negativePriorityRejected", throwsIllegalArgument(() -> priority.setPriority(-1)));
        expected.put("negativePriorityErrorMessage",
            illegalArgumentMessage(() -> priority.setPriority(-1)));
        expected.put("priorityAfterNegative", priority.getPriority());
        priority.setPriority(0);
        expected.put("zeroPriority", priority.getPriority());

        List<Map<String, Object>> priorityInputs = new ArrayList<>();
        for (String value : List.of("", " ", "+", "++1", "+1", "-1", "2147483647",
            "-2147483648", "2147483648", "-2147483649", "\u0661\u0662")) {
            Message input = new Message();
            MessageAccessor.putProperty(input, MessageConst.PROPERTY_PRIORITY, value);
            Map<String, Object> result = new TreeMap<>();
            result.put("input", value);
            result.put("priority", input.getPriority());
            priorityInputs.add(result);
        }
        expected.put("priorityInputs", priorityInputs);

        message.setDelayTimeLevel(-7);
        message.setDelayTimeSec(Long.MIN_VALUE);
        message.setDelayTimeMs(9007199254740993L);
        message.setDeliverTimeMs(Long.MAX_VALUE);
        expected.put("delayTimeLevel", message.getDelayTimeLevel());
        expected.put("delayTimeSec", message.getDelayTimeSec());
        expected.put("delayTimeMs", message.getDelayTimeMs());
        expected.put("deliverTimeMs", message.getDeliverTimeMs());

        Message absent = new Message();
        expected.put("absentDelayTimeLevel", absent.getDelayTimeLevel());
        expected.put("absentDelayTimeSec", absent.getDelayTimeSec());
        expected.put("absentDelayTimeMs", absent.getDelayTimeMs());
        expected.put("absentDeliverTimeMs", absent.getDeliverTimeMs());
        MessageAccessor.putProperty(absent, "DELAY", "\u0661\u0662");
        MessageAccessor.putProperty(absent, "TIMER_DELAY_SEC", "+\u0663\u0664");
        expected.put("unicodeDelayTimeLevel", absent.getDelayTimeLevel());
        expected.put("unicodeDelayTimeSec", absent.getDelayTimeSec());
        MessageAccessor.putProperty(absent, "DELAY", "2147483648");
        expected.put("overflowDelayTimeLevelThrows", throwsNumberFormat(absent, "DELAY"));
        MessageAccessor.putProperty(absent, "TIMER_DELAY_MS", "9223372036854775808");
        expected.put("overflowDelayTimeMsThrows", throwsNumberFormat(absent, "TIMER_DELAY_MS"));

        List<Map<String, Object>> numericInputs = new ArrayList<>();
        for (String value : List.of("", " ", "+", "++1", "+1", "-1", "2147483647",
            "-2147483648", "2147483648", "-2147483649", "\u0661\u0662")) {
            numericInputs.add(numericPropertyInput("DELAY", value));
        }
        for (String value : List.of("", " ", "+", "++1", "+1", "-1", "9223372036854775807",
            "-9223372036854775808", "9223372036854775808", "-9223372036854775809", "+\u0663\u0664")) {
            numericInputs.add(numericPropertyInput("TIMER_DELAY_SEC", value));
            numericInputs.add(numericPropertyInput("TIMER_DELAY_MS", value));
            numericInputs.add(numericPropertyInput("TIMER_DELIVER_MS", value));
        }
        expected.put("numericPropertyInputs", numericInputs);

        Message diagnostic = new Message("invoice", new byte[] {0, -1, 42});
        expected.put("messageToString", diagnostic.toString());
        diagnostic.getProperties().put("DIRECT", "through-map");
        expected.put("directMapMutation", diagnostic.getProperty("DIRECT"));
        return expected;
    }

    private static List<String> publicMethodSignatures(Class<?> type) {
        return Arrays.stream(type.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(MessageDecodeFixtures::methodSignature)
            .sorted()
            .collect(Collectors.toList());
    }

    private static List<String> publicFieldSignatures(Class<?> type) {
        return Arrays.stream(type.getDeclaredFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(field -> Modifier.toString(field.getModifiers()) + " "
                + field.getGenericType().getTypeName() + " " + field.getName())
            .sorted()
            .collect(Collectors.toList());
    }

    private static List<String> publicConstructorSignatures(Class<?> type) {
        return Arrays.stream(type.getDeclaredConstructors())
            .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
            .map(MessageDecodeFixtures::constructorSignature)
            .sorted()
            .collect(Collectors.toList());
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getTypeName() + " " + method.getName() + "("
            + Arrays.stream(method.getParameterTypes())
                .map(Class::getTypeName)
                .collect(Collectors.joining(", "))
            + ")";
    }

    private static String constructorSignature(Constructor<?> constructor) {
        return constructor.getDeclaringClass().getSimpleName() + "("
            + Arrays.stream(constructor.getParameterTypes())
                .map(Class::getTypeName)
                .collect(Collectors.joining(", "))
            + ")";
    }

    private static Map<String, Object> messageDecoderCrcApi() {
        List<Map<String, Object>> cases = new ArrayList<>();
        for (int crc32 : new int[] {0, 1, 10, Integer.MAX_VALUE, -1, Integer.MIN_VALUE}) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(20);
            MessageDecoder.createCrc32(byteBuffer, crc32);
            ByteBuf byteBuf = Unpooled.buffer(20);
            MessageDecoder.createCrc32(byteBuf, crc32);
            byte[] byteBufBytes = new byte[byteBuf.readableBytes()];
            byteBuf.getBytes(byteBuf.readerIndex(), byteBufBytes);

            Map<String, Object> value = new TreeMap<>();
            value.put("crc32", crc32);
            value.put("byteBufferBase64", Base64.getEncoder().encodeToString(byteBuffer.array()));
            value.put("byteBufBase64", Base64.getEncoder().encodeToString(byteBufBytes));
            cases.add(value);
            byteBuf.release();
        }
        return Map.of("cases", cases);
    }

    private static Map<String, Object> messageDecoderPublicApi() {
        Map<String, Object> expected = new TreeMap<>();
        expected.put("declaredPublicConstructors", publicConstructorSignatures(MessageDecoder.class));
        expected.put("declaredPublicMethods", publicMethodSignatures(MessageDecoder.class));
        expected.put("charsetUtf8", MessageDecoder.CHARSET_UTF8.name());
        expected.put("messageMagicCode", MessageDecoder.MESSAGE_MAGIC_CODE);
        expected.put("messageMagicCodeV2", MessageDecoder.MESSAGE_MAGIC_CODE_V2);
        expected.put("blankMagicCode", MessageDecoder.BLANK_MAGIC_CODE);
        expected.put("messageMagicCodePosition", MessageDecoder.MESSAGE_MAGIC_CODE_POSITION);
        expected.put("messageFlagPosition", MessageDecoder.MESSAGE_FLAG_POSITION);
        expected.put("messagePhysicOffsetPosition", MessageDecoder.MESSAGE_PHYSIC_OFFSET_POSITION);
        expected.put("messageStoreTimestampPosition", MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION);
        expected.put("phyPosPosition", MessageDecoder.PHY_POS_POSITION);
        expected.put("queueOffsetPosition", MessageDecoder.QUEUE_OFFSET_POSITION);
        expected.put("sysflagPosition", MessageDecoder.SYSFLAG_POSITION);
        expected.put("nameValueSeparator", (int) MessageDecoder.NAME_VALUE_SEPARATOR);
        expected.put("propertySeparator", (int) MessageDecoder.PROPERTY_SEPARATOR);
        return expected;
    }

    private static Map<String, Object> messageQueuePublicApi() {
        Map<String, Object> expected = new TreeMap<>();
        expected.put("declaredPublicFields", publicFieldSignatures(MessageQueue.class));
        expected.put("publicConstructors", publicConstructorSignatures(MessageQueue.class));
        expected.put("declaredPublicMethods", publicMethodSignatures(MessageQueue.class));
        return expected;
    }

    private static Map<String, Object> messageConstantsApi() throws IllegalAccessException {
        Map<String, String> messageConst = new TreeMap<>();
        for (Field field : MessageConst.class.getDeclaredFields()) {
            int modifiers = field.getModifiers();
            if (field.getType() == String.class && Modifier.isPublic(modifiers)
                && Modifier.isStatic(modifiers) && Modifier.isFinal(modifiers)) {
                messageConst.put(field.getName(), (String) field.get(null));
            }
        }
        Map<String, String> mixAll = new TreeMap<>();
        mixAll.put("REPLY_TOPIC_POSTFIX", MixAll.REPLY_TOPIC_POSTFIX);
        mixAll.put("REPLY_MESSAGE_FLAG", MixAll.REPLY_MESSAGE_FLAG);

        List<String> reservedNames = new ArrayList<>(MessageConst.STRING_HASH_SET);
        reservedNames.sort(String::compareTo);
        Map<String, Object> expected = new TreeMap<>();
        expected.put("messageConst", messageConst);
        expected.put("messageConstPublicFields", publicFieldSignatures(MessageConst.class));
        expected.put("messageConstPublicConstructors", publicConstructorSignatures(MessageConst.class));
        expected.put("messageConstDeclaredPublicMethods", publicMethodSignatures(MessageConst.class));
        expected.put("mixAll", mixAll);
        expected.put("reservedNames", reservedNames);
        return expected;
    }

    private static Map<String, Object> messageSysFlagPublicApi() throws IllegalAccessException {
        Map<String, Object> expected = new TreeMap<>();
        expected.put("publicConstants", publicIntegerConstants(MessageSysFlag.class));
        expected.put("publicConstructors", publicConstructorSignatures(MessageSysFlag.class));
        expected.put("declaredPublicMethods", publicMethodSignatures(MessageSysFlag.class));
        return expected;
    }

    private static Map<String, Object> pullSysFlagPublicApi() throws IllegalAccessException {
        Map<String, Object> expected = new TreeMap<>();
        expected.put("publicConstants", publicIntegerConstants(PullSysFlag.class));
        expected.put("publicConstructors", publicConstructorSignatures(PullSysFlag.class));
        expected.put("declaredPublicMethods", publicMethodSignatures(PullSysFlag.class));
        return expected;
    }

    private static Map<String, Integer> publicIntegerConstants(Class<?> type) throws IllegalAccessException {
        Map<String, Integer> constants = new TreeMap<>();
        for (Field field : type.getDeclaredFields()) {
            int modifiers = field.getModifiers();
            if (field.getType() == int.class && Modifier.isPublic(modifiers)
                && Modifier.isStatic(modifiers) && Modifier.isFinal(modifiers)) {
                constants.put(field.getName(), field.getInt(null));
            }
        }
        return constants;
    }

    private static Map<String, Object> messageVersionApi() {
        Map<String, Object> expected = new TreeMap<>();
        expected.put("publicConstructors", publicConstructorSignatures(MessageVersion.class));
        expected.put("declaredPublicMethods", publicMethodSignatures(MessageVersion.class));
        List<Map<String, Object>> versions = new ArrayList<>();
        MessageVersion[] values = MessageVersion.values();
        for (int index = 0; index < values.length; index++) {
            MessageVersion version = values[index];
            Map<String, Object> item = new TreeMap<>();
            item.put("name", version.name());
            item.put("ordinal", version.ordinal());
            item.put("toString", version.toString());
            item.put("magicCode", version.getMagicCode());
            item.put("topicLengthSize", version.getTopicLengthSize());
            item.put("valueOfName", MessageVersion.valueOf(version.name()).name());
            item.put("valueOfMagicCode", MessageVersion.valueOfMagicCode(version.getMagicCode()).name());
            item.put("compareToPrevious", index == 0 ? null : version.compareTo(values[index - 1]));

            byte[] readBytes = version == MessageVersion.MESSAGE_VERSION_V1
                ? new byte[] {0x55, (byte) 0xFF, 0x7F}
                : new byte[] {0x55, (byte) 0xFF, (byte) 0xFE, 0x7F, (byte) 0xFF};
            ByteBuffer relative = ByteBuffer.wrap(readBytes);
            relative.position(1);
            item.put("relativeTopicLength", version.getTopicLength(relative));
            item.put("relativePositionAfterRead", relative.position());
            item.put("indexedTopicLength", version.getTopicLength(ByteBuffer.wrap(readBytes),
                version == MessageVersion.MESSAGE_VERSION_V1 ? 2 : 3));

            ByteBuffer output = ByteBuffer.allocate(version.getTopicLengthSize() + 1);
            output.position(1);
            version.putTopicLength(output, 0x123456);
            item.put("putBytesBase64", Base64.getEncoder().encodeToString(output.array()));
            item.put("positionAfterWrite", output.position());
            versions.add(item);
        }
        expected.put("values", versions);

        List<Map<String, Object>> narrowingCases = new ArrayList<>();
        for (MessageVersion version : values) {
            for (int topicLength : new int[] {
                -32769, -32768, -129, -128, -1, 0, 1, 127, 128, 32767, 32768,
                Integer.MIN_VALUE, Integer.MAX_VALUE
            }) {
                ByteBuffer output = ByteBuffer.allocate(version.getTopicLengthSize());
                version.putTopicLength(output, topicLength);
                ByteBuffer input = ByteBuffer.wrap(output.array());
                Map<String, Object> item = new TreeMap<>();
                item.put("version", version.name());
                item.put("topicLength", topicLength);
                item.put("bytesBase64", Base64.getEncoder().encodeToString(output.array()));
                item.put("readBack", version.getTopicLength(input));
                narrowingCases.add(item);
            }
        }
        expected.put("narrowingCases", narrowingCases);
        expected.put("invalidMagicCodeException", exceptionName(() -> MessageVersion.valueOfMagicCode(0)));
        expected.put("unknownEnumNameException", exceptionName(() -> MessageVersion.valueOf("MESSAGE_VERSION_V3")));
        expected.put("nullEnumNameException", exceptionName(() -> MessageVersion.valueOf(null)));
        expected.put("relativeUnderflowException", exceptionName(() ->
            MessageVersion.MESSAGE_VERSION_V2.getTopicLength(ByteBuffer.allocate(1))));
        expected.put("indexedOutOfBoundsException", exceptionName(() ->
            MessageVersion.MESSAGE_VERSION_V2.getTopicLength(ByteBuffer.allocate(1), 0)));
        return expected;
    }

    private static String exceptionName(Runnable action) {
        try {
            action.run();
            return null;
        } catch (RuntimeException exception) {
            return exception.getClass().getSimpleName();
        }
    }

    private static Map<String, Object> messageDecoderPropertiesApi() throws Exception {
        Map<String, Object> expected = new TreeMap<>();
        expected.put("nullMapEncoded", MessageDecoder.messageProperties2String(null));
        expected.put("emptyMapEncoded", MessageDecoder.messageProperties2String(new HashMap<>()));
        Map<String, String> nullValue = new HashMap<>();
        nullValue.put("ignored", null);
        expected.put("nullValueEncoded", MessageDecoder.messageProperties2String(nullValue));
        Map<String, String> nullKey = new HashMap<>();
        nullKey.put(null, "value");
        expected.put("nullKeyEncoded", MessageDecoder.messageProperties2String(nullKey));
        expected.put("nullInputDecoded", new TreeMap<>(MessageDecoder.string2messageProperties(null)));
        expected.put("emptyInputDecoded", new TreeMap<>(MessageDecoder.string2messageProperties("")));
        String encoded = "a\u0001one\u0002a\u0001two\u0002broken\u0002\u0001empty-key\u0002";
        expected.put("encodedInput", encoded);
        expected.put("encodedInputDecoded", new TreeMap<>(MessageDecoder.string2messageProperties(encoded)));

        MessageExt recordSource = message("properties-only", "body", 4, 9,
            InetAddress.getByName("192.0.2.44"), 10911,
            InetAddress.getByName("198.51.100.44"), 10912, 0, Map.of("key", "value"));
        byte[] record = MessageDecoder.encode(recordSource, false);
        ByteBuffer malformedStoreSize = ByteBuffer.wrap(record);
        malformedStoreSize.putInt(0, 0);
        Map<String, String> propertiesFromZeroStoreSize =
            MessageDecoder.decodeProperties(malformedStoreSize);
        expected.put("zeroStoreSizeRecordBase64", Base64.getEncoder().encodeToString(record));
        expected.put("zeroStoreSizeRecordProperties", propertiesFromZeroStoreSize == null
            ? null : new TreeMap<>(propertiesFromZeroStoreSize));
        expected.put("zeroStoreSizeRecordPosition", malformedStoreSize.position());

        List<Map<String, Object>> malformedStoreSizeDecodes = new ArrayList<>();
        for (int declaredStoreSize : new int[] {0, -1, Integer.MAX_VALUE}) {
            byte[] malformedRecord = record.clone();
            ByteBuffer.wrap(malformedRecord).putInt(0, declaredStoreSize);
            ByteBuffer decodeInput = ByteBuffer.wrap(malformedRecord);
            MessageExt decoded = MessageDecoder.decode(decodeInput);
            Map<String, Object> decodedCase = new TreeMap<>();
            decodedCase.put("declaredStoreSize", declaredStoreSize);
            decodedCase.put("recordBase64", Base64.getEncoder().encodeToString(malformedRecord));
            decodedCase.put("decoded", decodeSnapshot(decoded, decodeInput));
            decodedCase.put("decodedStoreSize", decoded.getStoreSize());
            malformedStoreSizeDecodes.add(decodedCase);
        }
        expected.put("malformedStoreSizeFullDecodes", malformedStoreSizeDecodes);

        byte[] malformedPropertiesRecord = record.clone();
        ByteBuffer malformedPropertiesBuffer = ByteBuffer.wrap(malformedPropertiesRecord);
        int sysFlag = malformedPropertiesBuffer.getInt(36);
        int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
        int bodySizePosition = 68 + bornHostLength + storeHostLength;
        int topicLengthPosition = bodySizePosition + 4 + malformedPropertiesBuffer.getInt(bodySizePosition);
        int topicLength = malformedPropertiesBuffer.get(topicLengthPosition);
        int propertiesPosition = topicLengthPosition + 1 + topicLength;

        int declaredBodyLength = malformedPropertiesBuffer.getInt(bodySizePosition);
        byte[] negativeBodyLengthRecord = new byte[record.length - declaredBodyLength];
        System.arraycopy(record, 0, negativeBodyLengthRecord, 0, bodySizePosition);
        ByteBuffer.wrap(negativeBodyLengthRecord).putInt(bodySizePosition, -1);
        int bodyTailOffset = bodySizePosition + 4 + declaredBodyLength;
        System.arraycopy(record, bodyTailOffset, negativeBodyLengthRecord, bodySizePosition + 4,
            record.length - bodyTailOffset);
        ByteBuffer negativeBodyDecodeInput = ByteBuffer.wrap(negativeBodyLengthRecord);
        MessageExt negativeBodyDecoded = MessageDecoder.decode(negativeBodyDecodeInput);
        Map<String, Object> negativeBodyDecode = new TreeMap<>();
        negativeBodyDecode.put("recordBase64",
            Base64.getEncoder().encodeToString(negativeBodyLengthRecord));
        negativeBodyDecode.put("decoded", decodeSnapshot(negativeBodyDecoded, negativeBodyDecodeInput));
        negativeBodyDecode.put("decodedStoreSize", negativeBodyDecoded.getStoreSize());
        expected.put("negativeBodyLengthFullDecode", negativeBodyDecode);

        byte[] negativePropertiesLengthFullRecord = record.clone();
        ByteBuffer.wrap(negativePropertiesLengthFullRecord).putShort(propertiesPosition, (short) -1);
        ByteBuffer negativePropertiesDecodeInput = ByteBuffer.wrap(negativePropertiesLengthFullRecord);
        MessageExt negativePropertiesDecoded = MessageDecoder.decode(negativePropertiesDecodeInput);
        Map<String, Object> negativePropertiesDecode = new TreeMap<>();
        negativePropertiesDecode.put("recordBase64",
            Base64.getEncoder().encodeToString(negativePropertiesLengthFullRecord));
        negativePropertiesDecode.put("decoded",
            decodeSnapshot(negativePropertiesDecoded, negativePropertiesDecodeInput));
        negativePropertiesDecode.put("decodedStoreSize", negativePropertiesDecoded.getStoreSize());
        expected.put("negativePropertiesLengthFullDecode", negativePropertiesDecode);

        malformedPropertiesBuffer.putShort(propertiesPosition, (short) 6);
        malformedPropertiesBuffer.position(propertiesPosition + 2);
        malformedPropertiesBuffer.put("broken".getBytes(StandardCharsets.UTF_8));
        ByteBuffer malformedPropertiesInput = ByteBuffer.wrap(malformedPropertiesRecord);
        Map<String, String> malformedProperties = MessageDecoder.decodeProperties(malformedPropertiesInput);
        expected.put("malformedPropertiesRecordBase64",
            Base64.getEncoder().encodeToString(malformedPropertiesRecord));
        expected.put("malformedPropertiesRecordProperties", malformedProperties == null
            ? null : new TreeMap<>(malformedProperties));
        expected.put("malformedPropertiesRecordPosition", malformedPropertiesInput.position());

        byte[] zeroPropertiesLengthRecord = record.clone();
        ByteBuffer zeroPropertiesLengthBuffer = ByteBuffer.wrap(zeroPropertiesLengthRecord);
        zeroPropertiesLengthBuffer.putShort(propertiesPosition, (short) 0);
        Map<String, String> zeroLengthProperties =
            MessageDecoder.decodeProperties(zeroPropertiesLengthBuffer);
        expected.put("zeroPropertiesLengthRecordBase64",
            Base64.getEncoder().encodeToString(zeroPropertiesLengthRecord));
        expected.put("zeroPropertiesLengthRecordProperties", zeroLengthProperties == null
            ? null : new TreeMap<>(zeroLengthProperties));
        expected.put("zeroPropertiesLengthRecordPosition", zeroPropertiesLengthBuffer.position());

        byte[] negativePropertiesLengthRecord = record.clone();
        ByteBuffer negativePropertiesLengthBuffer = ByteBuffer.wrap(negativePropertiesLengthRecord);
        negativePropertiesLengthBuffer.putShort(propertiesPosition, (short) -1);
        Map<String, String> negativeLengthProperties =
            MessageDecoder.decodeProperties(negativePropertiesLengthBuffer);
        expected.put("negativePropertiesLengthRecordBase64",
            Base64.getEncoder().encodeToString(negativePropertiesLengthRecord));
        expected.put("negativePropertiesLengthRecordProperties", negativeLengthProperties == null
            ? null : new TreeMap<>(negativeLengthProperties));
        expected.put("negativePropertiesLengthRecordPosition", negativePropertiesLengthBuffer.position());

        byte[] truncatedPropertiesRecord = record.clone();
        ByteBuffer truncatedPropertiesBuffer = ByteBuffer.wrap(truncatedPropertiesRecord);
        truncatedPropertiesBuffer.putShort(propertiesPosition, (short) 2000);
        String truncatedPropertiesError = exceptionName(
            () -> MessageDecoder.decodeProperties(truncatedPropertiesBuffer));
        expected.put("truncatedPropertiesRecordBase64",
            Base64.getEncoder().encodeToString(truncatedPropertiesRecord));
        expected.put("truncatedPropertiesRecordError", truncatedPropertiesError);
        expected.put("truncatedPropertiesRecordPosition", truncatedPropertiesBuffer.position());
        return expected;
    }

    private static Map<String, Object> messageCompactApi() throws Exception {
        Message first = new Message();
        first.setBody(new byte[] {0, -1, 42});
        first.setFlag(-17);
        MessageAccessor.setProperties(first, new java.util.HashMap<>(Map.of("k", "v")));
        Message second = new Message();
        second.setBody(new byte[0]);
        MessageAccessor.setProperties(second, new java.util.HashMap<>());

        byte[] firstBytes = MessageDecoder.encodeMessage(first);
        byte[] batchBytes = MessageDecoder.encodeMessages(List.of(first, second));
        Message decodedFirst = MessageDecoder.decodeMessage(ByteBuffer.wrap(firstBytes));
        List<Message> decodedBatch = MessageDecoder.decodeMessages(ByteBuffer.wrap(batchBytes));

        Map<String, Object> expected = new TreeMap<>();
        expected.put("singleEncodedBase64", Base64.getEncoder().encodeToString(firstBytes));
        expected.put("batchEncodedBase64", Base64.getEncoder().encodeToString(batchBytes));
        expected.put("singleDecoded", compactMessageSnapshot(decodedFirst));
        expected.put("batchDecoded", decodedBatch.stream()
            .map(MessageDecodeFixtures::compactMessageSnapshot)
            .collect(java.util.stream.Collectors.toList()));
        ByteBuffer innerMessageCountInput = ByteBuffer.wrap(batchBytes);
        expected.put("innerMessageCount", MessageDecoder.countInnerMsgNum(innerMessageCountInput));
        expected.put("innerMessageCountPositionAfter", innerMessageCountInput.position());
        ByteBuffer emptyInnerMessageCountInput = ByteBuffer.allocate(0);
        expected.put("emptyInnerMessageCount", MessageDecoder.countInnerMsgNum(emptyInnerMessageCountInput));
        expected.put("emptyInnerMessageCountPositionAfter", emptyInnerMessageCountInput.position());
        ByteBuffer positionedInnerMessageCountInput = ByteBuffer.allocate(batchBytes.length + 3);
        positionedInnerMessageCountInput.put(new byte[] {91, 92, 93}).put(batchBytes).flip();
        positionedInnerMessageCountInput.position(3);
        expected.put("positionedInnerMessageCount",
            MessageDecoder.countInnerMsgNum(positionedInnerMessageCountInput));
        expected.put("positionedInnerMessageCountPositionAfter", positionedInnerMessageCountInput.position());
        expected.putAll(shortCompactCountOutcome());
        expected.putAll(nonstandardCompactCountSizeOutcomes());

        InetSocketAddress bornHost = new InetSocketAddress(InetAddress.getByName("192.0.2.10"), 10911);
        InetSocketAddress storeHost = new InetSocketAddress(InetAddress.getByName("198.51.100.20"), 10912);
        MessageExt outer = new MessageExt();
        outer.setTopic("outer-topic");
        outer.setBody(batchBytes);
        outer.setQueueId(7);
        outer.setQueueOffset(100L);
        outer.setSysFlag(0x40);
        outer.setBornTimestamp(11L);
        outer.setBornHost(bornHost);
        outer.setStoreTimestamp(12L);
        outer.setStoreHost(storeHost);
        outer.setCommitLogOffset(13L);
        outer.setWaitStoreMsgOK(false);
        List<MessageExt> expanded = new ArrayList<>();
        MessageDecoder.decodeMessage(outer, expanded);
        expected.put("expanded", expanded.stream()
            .map(MessageDecodeFixtures::compactMessageExtSnapshot)
            .collect(java.util.stream.Collectors.toList()));
        expected.put("expandedClientExtApi", expandedClientExtApi(expanded.get(0)));
        expected.put("nullBodyEncodeThrows", throwsNullPointer(
            () -> MessageDecoder.encodeMessage(new Message())));
        expected.put("shortDecodeThrowsBufferUnderflow", throwsCompactDecodeBufferUnderflow());
        expected.put("negativeBodyDecodeThrows", throwsNegativeCompactBodyLength());
        expected.putAll(incompleteCompactCountOutcome());
        return expected;
    }

    private static Map<String, Object> messageBatchDecodeApi() throws Exception {
        MessageExt message = messageExtForEncoding(0, "192.0.2.10", "198.51.100.20");
        byte[] validRecord = MessageDecoder.encode(message, false);
        byte[] malformedBatch = java.util.Arrays.copyOf(validRecord, validRecord.length + 1);
        malformedBatch[malformedBatch.length - 1] = 0;
        ByteBuffer input = ByteBuffer.wrap(malformedBatch);
        List<MessageExt> decoded = MessageDecoder.decodesBatch(input, true, true, true);

        Map<String, Object> expected = new TreeMap<>();
        expected.put("validRecordBase64", Base64.getEncoder().encodeToString(validRecord));
        expected.put("malformedBatchBase64", Base64.getEncoder().encodeToString(malformedBatch));
        expected.put("decodedCount", decoded.size());
        expected.put("inputPosition", input.position());
        expected.put("inputLimit", input.limit());
        ByteBuffer clientDecodesDefaultInput = ByteBuffer.wrap(malformedBatch);
        List<MessageExt> clientDecodedDefault =
            MessageDecoder.decodes(clientDecodesDefaultInput);
        expected.put("decodesDefaultCount", clientDecodedDefault.size());
        expected.put("decodesDefaultInputPosition", clientDecodesDefaultInput.position());
        expected.put("decodesDefaultInputLimit", clientDecodesDefaultInput.limit());
        if (!clientDecodedDefault.isEmpty()) {
            expected.put("decodesDefaultFirstMessageClass",
                clientDecodedDefault.get(0).getClass().getSimpleName());
        }
        ByteBuffer clientDecodesInput = ByteBuffer.wrap(malformedBatch);
        List<MessageExt> clientDecoded = MessageDecoder.decodes(clientDecodesInput, true);
        expected.put("decodesCount", clientDecoded.size());
        expected.put("decodesInputPosition", clientDecodesInput.position());
        expected.put("decodesInputLimit", clientDecodesInput.limit());
        ByteBuffer clientDecodesNoBodyInput = ByteBuffer.wrap(malformedBatch);
        List<MessageExt> clientDecodedNoBody =
            MessageDecoder.decodes(clientDecodesNoBodyInput, false);
        expected.put("decodesNoBodyCount", clientDecodedNoBody.size());
        expected.put("decodesNoBodyInputPosition", clientDecodesNoBodyInput.position());
        expected.put("decodesNoBodyInputLimit", clientDecodesNoBodyInput.limit());
        ByteBuffer genericInput = ByteBuffer.wrap(malformedBatch);
        List<MessageExt> genericDecoded = MessageDecoder.decodesBatch(genericInput, true, true, false);
        expected.put("genericBatchCount", genericDecoded.size());
        expected.put("genericBatchInputPosition", genericInput.position());
        expected.put("genericBatchInputLimit", genericInput.limit());
        ByteBuffer genericNoBodyInput = ByteBuffer.wrap(malformedBatch);
        List<MessageExt> genericDecodedNoBody =
            MessageDecoder.decodesBatch(genericNoBodyInput, false, true, false);
        expected.put("genericBatchNoBodyCount", genericDecodedNoBody.size());
        expected.put("genericBatchNoBodyInputPosition", genericNoBodyInput.position());
        expected.put("genericBatchNoBodyInputLimit", genericNoBodyInput.limit());
        if (!genericDecoded.isEmpty()) {
            expected.put("genericBatchFirstMessageClass",
                genericDecoded.get(0).getClass().getSimpleName());
        }
        if (!decoded.isEmpty()) {
            MessageExt first = decoded.get(0);
            MessageClientExt clientFirst = (MessageClientExt) first;
            expected.put("firstMessageClass", first.getClass().getSimpleName());
            expected.put("firstTopic", first.getTopic());
            expected.put("firstMessageId", clientFirst.getMsgId());
            expected.put("firstOffsetMessageId", clientFirst.getOffsetMsgId());
            expected.put("firstQueueOffset", first.getQueueOffset());
        }
        return expected;
    }

    private static Map<String, Object> expandedClientExtApi(MessageExt message) {
        MessageClientExt clientExt = (MessageClientExt) message;
        Map<String, Object> result = new TreeMap<>();
        result.put("runtimeClass", message.getClass().getSimpleName());
        result.put("initialMessageId", clientExt.getMsgId());
        result.put("initialOffsetMessageId", clientExt.getOffsetMsgId());
        clientExt.setMsgId("ignored-message-id");
        result.put("messageIdAfterSetMessageId", clientExt.getMsgId());
        result.put("offsetMessageIdAfterSetMessageId", clientExt.getOffsetMsgId());
        clientExt.setOffsetMsgId("physical-offset-id");
        result.put("messageIdAfterSetOffsetMessageId", clientExt.getMsgId());
        result.put("offsetMessageIdAfterSetOffsetMessageId", clientExt.getOffsetMsgId());
        MessageAccessor.putProperty(clientExt,
            MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "unique-id");
        result.put("messageIdWithUniqueKey", clientExt.getMsgId());
        result.put("offsetMessageIdWithUniqueKey", clientExt.getOffsetMsgId());
        return result;
    }

    private static Map<String, Object> compactMessageSnapshot(Message message) {
        Map<String, Object> result = new TreeMap<>();
        result.put("topicIsNull", message.getTopic() == null);
        result.put("flag", message.getFlag());
        result.put("bodyBase64", Base64.getEncoder().encodeToString(message.getBody()));
        result.put("properties", message.getProperties() == null
            ? null : new TreeMap<>(message.getProperties()));
        result.put("waitStore", message.isWaitStoreMsgOK());
        return result;
    }

    private static Map<String, Object> compactMessageExtSnapshot(MessageExt message) {
        Map<String, Object> result = new TreeMap<>();
        result.put("topic", message.getTopic());
        result.put("queueId", message.getQueueId());
        result.put("queueOffset", message.getQueueOffset());
        result.put("flag", message.getFlag());
        result.put("bodyBase64", Base64.getEncoder().encodeToString(message.getBody()));
        result.put("properties", message.getProperties() == null
            ? null : new TreeMap<>(message.getProperties()));
        result.put("waitStore", message.isWaitStoreMsgOK());
        result.put("sysFlag", message.getSysFlag());
        result.put("bornTimestamp", message.getBornTimestamp());
        result.put("bornHost", message.getBornHost().toString());
        result.put("storeTimestamp", message.getStoreTimestamp());
        result.put("storeHost", message.getStoreHost().toString());
        result.put("commitLogOffset", message.getCommitLogOffset());
        return result;
    }

    private static boolean throwsCompactDecodeBufferUnderflow() {
        try {
            MessageDecoder.decodeMessage(ByteBuffer.allocate(1));
            return false;
        } catch (java.nio.BufferUnderflowException expected) {
            return true;
        } catch (Exception unexpected) {
            return false;
        }
    }

    private static boolean throwsNegativeCompactBodyLength() {
        try {
            ByteBuffer malformed = ByteBuffer.allocate(20);
            malformed.putInt(0).putInt(0).putInt(0).putInt(0).putInt(-1).flip();
            MessageDecoder.decodeMessage(malformed);
            return false;
        } catch (NegativeArraySizeException expected) {
            return true;
        } catch (Exception unexpected) {
            return false;
        }
    }

    private static Map<String, Object> incompleteCompactCountOutcome() {
        Map<String, Object> outcome = new TreeMap<>();
        ByteBuffer input = ByteBuffer.wrap(new byte[] {0, 0, 0, 5});
        try {
            MessageDecoder.countInnerMsgNum(input);
            outcome.put("incompleteCountThrowsIllegalArgument", false);
        } catch (IllegalArgumentException expected) {
            outcome.put("incompleteCountThrowsIllegalArgument", true);
        }
        outcome.put("incompleteCountPositionAfter", input.position());
        return outcome;
    }

    private static Map<String, Object> shortCompactCountOutcome() {
        Map<String, Object> outcome = new TreeMap<>();
        ByteBuffer input = ByteBuffer.wrap(new byte[] {0, 0, 0});
        try {
            MessageDecoder.countInnerMsgNum(input);
            outcome.put("shortCountThrowsBufferUnderflow", false);
        } catch (java.nio.BufferUnderflowException expected) {
            outcome.put("shortCountThrowsBufferUnderflow", true);
        }
        outcome.put("shortCountPositionAfter", input.position());
        return outcome;
    }

    private static Map<String, Object> nonstandardCompactCountSizeOutcomes() throws InterruptedException {
        Map<String, Object> outcome = new TreeMap<>();
        ByteBuffer subFour = ByteBuffer.wrap(new byte[] {0, 0, 0, 3, 0, 0, 0, 4});
        try {
            MessageDecoder.countInnerMsgNum(subFour);
            outcome.put("subFourCountThrowsIllegalArgument", false);
        } catch (IllegalArgumentException expected) {
            outcome.put("subFourCountThrowsIllegalArgument", true);
        }
        outcome.put("subFourCountPositionAfter", subFour.position());

        ByteBuffer negative = ByteBuffer.wrap(new byte[] {-1, -1, -1, -1});
        try {
            MessageDecoder.countInnerMsgNum(negative);
            outcome.put("negativeCountThrowsIllegalArgument", false);
        } catch (IllegalArgumentException expected) {
            outcome.put("negativeCountThrowsIllegalArgument", true);
        }
        outcome.put("negativeCountPositionAfter", negative.position());

        ByteBuffer rewinding = ByteBuffer.wrap(
            new byte[] {0, 0, 0, 8, 0, 0, 0, 0, -1, -1, -1, -3});
        try {
            MessageDecoder.countInnerMsgNum(rewinding);
            outcome.put("rewindingCountThrowsIllegalArgument", false);
        } catch (IllegalArgumentException expected) {
            outcome.put("rewindingCountThrowsIllegalArgument", true);
        }
        outcome.put("rewindingCountPositionAfter", rewinding.position());

        ByteBuffer zero = ByteBuffer.allocate(4);
        CountDownLatch zeroStarted = new CountDownLatch(1);
        Thread zeroCounter = new Thread(() -> {
            zeroStarted.countDown();
            MessageDecoder.countInnerMsgNum(zero);
        }, "phase18-zero-inner-count-probe");
        zeroCounter.setDaemon(true);
        zeroCounter.start();
        zeroStarted.await();
        zeroCounter.join(50L);
        outcome.put("zeroCountRemainsRunning", zeroCounter.isAlive());

        ByteBuffer negativeCycle = ByteBuffer.allocate(16);
        negativeCycle.putInt(4).putInt(8).position(12).putInt(-8).position(0);
        CountDownLatch negativeCycleStarted = new CountDownLatch(1);
        Thread negativeCycleCounter = new Thread(() -> {
            negativeCycleStarted.countDown();
            MessageDecoder.countInnerMsgNum(negativeCycle);
        }, "phase18-negative-cycle-inner-count-probe");
        negativeCycleCounter.setDaemon(true);
        negativeCycleCounter.start();
        negativeCycleStarted.await();
        negativeCycleCounter.join(50L);
        outcome.put("negativeCycleCountRemainsRunning", negativeCycleCounter.isAlive());
        return outcome;
    }

    private static Map<String, Object> messageExtEncodeApi() throws Exception {
        Map<String, Object> expected = new TreeMap<>();
        MessageExt ipv4 = messageExtForEncoding(0, "192.0.2.10", "198.51.100.20");
        byte[] ipv4Encoded = MessageDecoder.encode(ipv4, false);
        expected.put("ipv4StandardBase64", Base64.getEncoder().encodeToString(ipv4Encoded));
        expected.put("ipv4UniqueBase64", Base64.getEncoder().encodeToString(
            MessageDecoder.encodeUniquely(ipv4, false)));

        MessageExt ipv6 = messageExtForEncoding(
            MessageSysFlag.BORNHOST_V6_FLAG | MessageSysFlag.STOREHOSTADDRESS_V6_FLAG,
            "2001:db8::10", "2001:db8::20");
        expected.put("ipv6StandardBase64", Base64.getEncoder().encodeToString(
            MessageDecoder.encode(ipv6, false)));
        expected.put("ipv6UniqueBase64", Base64.getEncoder().encodeToString(
            MessageDecoder.encodeUniquely(ipv6, false)));

        ipv4.setStoreSize(ipv4Encoded.length + 3);
        expected.put("paddedStandardBase64", Base64.getEncoder().encodeToString(
            MessageDecoder.encode(ipv4, false)));
        MessageExt tooSmall = messageExtForEncoding(0, "192.0.2.10", "198.51.100.20");
        tooSmall.setStoreSize(1);
        expected.put("tooSmallStoreSizeThrows", throwsMessageExtEncodeBufferOverflow(tooSmall));

        List<Map<String, Object>> compressed = new ArrayList<>();
        for (int compressionType : new int[] {
            MessageSysFlag.COMPRESSION_ZLIB_TYPE,
            MessageSysFlag.COMPRESSION_LZ4_TYPE,
            MessageSysFlag.COMPRESSION_ZSTD_TYPE
        }) {
            MessageExt message = messageExtForEncoding(
                MessageSysFlag.COMPRESSED_FLAG | compressionType,
                "192.0.2.10", "198.51.100.20");
            byte[] encoded = MessageDecoder.encode(message, true);
            MessageExt decoded = MessageDecoder.decode(
                ByteBuffer.wrap(encoded), true, true, false, false, false);
            Map<String, Object> row = new TreeMap<>();
            row.put("compressionType", compressionType);
            row.put("decodedBodyBase64", Base64.getEncoder().encodeToString(decoded.getBody()));
            row.put("decodedSysFlag", decoded.getSysFlag());
            compressed.add(row);
        }
        expected.put("compressed", compressed);
        return expected;
    }

    private static MessageExt messageExtForEncoding(int sysFlag, String bornIp, String storeIp)
        throws Exception {
        MessageExt message = new MessageExt();
        message.setTopic("orders");
        message.setBody(new byte[] {0, -1, 42, 55});
        message.setFlag(-23);
        message.setBodyCRC(0x12345678);
        message.setQueueId(7);
        message.setQueueOffset(23L);
        message.setCommitLogOffset(4567L);
        message.setSysFlag(sysFlag);
        message.setBornTimestamp(123L);
        message.setBornHost(new InetSocketAddress(InetAddress.getByName(bornIp), 10911));
        message.setStoreTimestamp(234L);
        message.setStoreHost(new InetSocketAddress(InetAddress.getByName(storeIp), 10912));
        message.setReconsumeTimes(2);
        message.setPreparedTransactionOffset(345L);
        MessageAccessor.setProperties(message, new java.util.HashMap<>(Map.of("k", "v")));
        return message;
    }

    private static boolean throwsMessageExtEncodeBufferOverflow(MessageExt message) {
        try {
            MessageDecoder.encode(message, false);
            return false;
        } catch (java.nio.BufferOverflowException expected) {
            return true;
        } catch (Exception unexpected) {
            return false;
        }
    }

    private static Map<String, Object> numericPropertyInput(String property, String value) {
        Message message = new Message();
        MessageAccessor.putProperty(message, property, value);
        Map<String, Object> result = new TreeMap<>();
        result.put("property", property);
        result.put("input", value);
        try {
            long parsed = 0;
            switch (property) {
                case "DELAY":
                    parsed = message.getDelayTimeLevel();
                    break;
                case "TIMER_DELAY_SEC":
                    parsed = message.getDelayTimeSec();
                    break;
                case "TIMER_DELAY_MS":
                    parsed = message.getDelayTimeMs();
                    break;
                default:
                    parsed = message.getDeliverTimeMs();
            }
            result.put("accepted", true);
            result.put("value", parsed);
        } catch (NumberFormatException expected) {
            result.put("accepted", false);
        }
        return result;
    }

    private static boolean throwsNumberFormat(Message message, String property) {
        try {
            if ("DELAY".equals(property)) message.getDelayTimeLevel();
            else if ("TIMER_DELAY_SEC".equals(property)) message.getDelayTimeSec();
            else if ("TIMER_DELAY_MS".equals(property)) message.getDelayTimeMs();
            else message.getDeliverTimeMs();
            return false;
        } catch (NumberFormatException expected) {
            return true;
        }
    }

    private static boolean throwsIllegalArgument(Runnable action) {
        try {
            action.run();
            return false;
        } catch (IllegalArgumentException expected) {
            return true;
        }
    }

    private static String illegalArgumentMessage(Runnable action) {
        try {
            action.run();
            return null;
        } catch (IllegalArgumentException expected) {
            return expected.getMessage();
        }
    }

    private static boolean throwsRuntimeException(Runnable action) {
        try {
            action.run();
            return false;
        } catch (RuntimeException expected) {
            return true;
        }
    }

    private static String runtimeExceptionClass(Runnable action) {
        try {
            action.run();
            return null;
        } catch (RuntimeException expected) {
            return expected.getClass().getName();
        }
    }

    private static boolean throwsNullPointer(Runnable action) {
        try {
            action.run();
            return false;
        } catch (NullPointerException expected) {
            return true;
        }
    }

    private static boolean throwsBufferOverflow(Runnable action) {
        try {
            action.run();
            return false;
        } catch (java.nio.BufferOverflowException expected) {
            return true;
        } catch (RuntimeException unexpected) {
            return false;
        }
    }

    private static boolean throwsMessageIdNullPointer(String value) {
        try {
            MessageDecoder.decodeMessageId(value);
            return false;
        } catch (NullPointerException expected) {
            return true;
        } catch (Exception unexpected) {
            return false;
        }
    }

    private static boolean throwsMessageIdBufferUnderflow(String value) {
        try {
            MessageDecoder.decodeMessageId(value);
            return false;
        } catch (java.nio.BufferUnderflowException expected) {
            return true;
        } catch (Exception unexpected) {
            return false;
        }
    }

    private static boolean throwsMessageIdIllegalArgument(String value) {
        try {
            MessageDecoder.decodeMessageId(value);
            return false;
        } catch (IllegalArgumentException expected) {
            return true;
        } catch (Exception unexpected) {
            return false;
        }
    }

    private static Map<String, Object> messageExtPublicApi() {
        MessageExt defaults = new MessageExt();
        Map<String, Object> expected = new TreeMap<>();
        List<String> publicConstructors = new ArrayList<>();
        for (java.lang.reflect.Constructor<?> constructor : MessageExt.class.getConstructors()) {
            publicConstructors.add(Arrays.stream(constructor.getParameterTypes())
                .map(Class::getTypeName)
                .collect(Collectors.joining(", ", "(", ")")));
        }
        Collections.sort(publicConstructors);
        expected.put("publicConstructorCount", publicConstructors.size());
        expected.put("publicConstructors", publicConstructors);
        expected.put("declaredPublicMethods", publicMethodSignatures(MessageExt.class));
        expected.put("defaultQueueId", defaults.getQueueId());
        expected.put("defaultBornTimestamp", defaults.getBornTimestamp());
        expected.put("defaultStoreTimestamp", defaults.getStoreTimestamp());
        expected.put("defaultMessageId", defaults.getMsgId());
        expected.put("defaultSysFlag", defaults.getSysFlag());
        expected.put("defaultBodyCrc", defaults.getBodyCRC());
        expected.put("defaultQueueOffset", defaults.getQueueOffset());
        expected.put("defaultCommitLogOffset", defaults.getCommitLogOffset());
        expected.put("defaultStoreSize", defaults.getStoreSize());
        expected.put("defaultReconsumeTimes", defaults.getReconsumeTimes());
        expected.put("defaultPreparedTransactionOffset", defaults.getPreparedTransactionOffset());

        MessageExt message = new MessageExt();
        message.setQueueId(-17);
        message.setBornTimestamp(Long.MIN_VALUE + 17);
        message.setStoreTimestamp(Long.MAX_VALUE - 17);
        message.setMsgId("message-ext-id");
        message.setSysFlag(-33);
        message.setBodyCRC(Integer.MIN_VALUE);
        message.setQueueOffset(Long.MIN_VALUE);
        message.setCommitLogOffset(Long.MAX_VALUE);
        message.setStoreSize(-21);
        message.setReconsumeTimes(-4);
        message.setPreparedTransactionOffset(Long.MAX_VALUE - 1);
        expected.put("queueId", message.getQueueId());
        expected.put("bornTimestamp", message.getBornTimestamp());
        expected.put("storeTimestamp", message.getStoreTimestamp());
        expected.put("messageId", message.getMsgId());
        expected.put("sysFlag", message.getSysFlag());
        expected.put("bodyCrc", message.getBodyCRC());
        expected.put("queueOffset", message.getQueueOffset());
        expected.put("commitLogOffset", message.getCommitLogOffset());
        expected.put("storeSize", message.getStoreSize());
        expected.put("reconsumeTimes", message.getReconsumeTimes());
        expected.put("preparedTransactionOffset", message.getPreparedTransactionOffset());
        return expected;
    }

    private static Map<String, Object> messageClientExtApi() {
        MessageClientExt message = new MessageClientExt();
        Map<String, Object> expected = new TreeMap<>();
        java.lang.reflect.Constructor<?>[] publicConstructors = MessageClientExt.class.getConstructors();
        boolean hasPublicNoArgConstructor = false;
        for (java.lang.reflect.Constructor<?> constructor : publicConstructors) {
            if (constructor.getParameterCount() == 0) {
                hasPublicNoArgConstructor = true;
            }
        }
        expected.put("publicConstructorCount", publicConstructors.length);
        expected.put("publicNoArgConstructor", hasPublicNoArgConstructor);
        expected.put("declaredPublicMethods", publicMethodSignatures(MessageClientExt.class));
        expected.put("defaultMessageId", message.getMsgId());
        expected.put("defaultOffsetMessageId", message.getOffsetMsgId());
        message.setMsgId("ignored-initial");
        expected.put("setMessageIdOnDefault", message.getMsgId());
        expected.put("setMessageIdDoesNotSetOffset", message.getOffsetMsgId());
        message.setOffsetMsgId("offset-1");
        expected.put("offsetMessageId", message.getOffsetMsgId());
        expected.put("messageIdFallsBackToOffset", message.getMsgId());
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            "unique-1");
        expected.put("messageIdPrefersUnique", message.getMsgId());
        expected.put("offsetRemainsWhenUniqueExists", message.getOffsetMsgId());
        message.setMsgId("ignored-after-unique");
        expected.put("setMessageIdDoesNotReplaceUnique", message.getMsgId());
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            "unique-2");
        expected.put("messageIdReadsLiveUniqueProperty", message.getMsgId());
        message.setOffsetMsgId("offset-2");
        expected.put("offsetMessageIdReplaced", message.getOffsetMsgId());
        expected.put("uniqueSurvivesOffsetReplacement", message.getMsgId());
        MessageAccessor.clearProperty(message, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        expected.put("messageIdFallsBackAfterUniqueClear", message.getMsgId());
        message.setMsgId("ignored-after-clear");
        expected.put("setMessageIdDoesNotReplaceOffsetFallback", message.getMsgId());
        message.setOffsetMsgId(null);
        expected.put("offsetMessageIdCleared", message.getOffsetMsgId() == null);
        expected.put("messageIdAfterOffsetCleared", message.getMsgId() == null);

        String[] subtypeMessageId = {"custom-client-id"};
        MessageClientExt customSubtype = new MessageClientExt() {
            @Override
            public String getMsgId() {
                return subtypeMessageId[0];
            }

            @Override
            public void setMsgId(String msgId) {
                subtypeMessageId[0] = msgId == null ? null : "subtype-" + msgId;
            }
        };
        expected.put("customSubtypeMessageId", customSubtype.getMsgId());
        MessageAccessor.putProperty(customSubtype,
            MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "unique-shadow");
        expected.put("customSubtypeMessageIdIgnoresUnique", customSubtype.getMsgId());
        customSubtype.setMsgId("replacement");
        expected.put("customSubtypeMessageIdAfterSet", customSubtype.getMsgId());
        expected.put("customSubtypeOffsetUnaffected", customSubtype.getOffsetMsgId() == null);
        customSubtype.setMsgId(null);
        expected.put("customSubtypeMessageIdCleared", customSubtype.getMsgId() == null);
        message.setTopic("inherited-topic");
        expected.put("inheritedTopic", message.getTopic());
        message.setQueueId(-17);
        expected.put("inheritedQueueId", message.getQueueId());
        message.setStoreTimestamp(1_700_000_000_123L);
        expected.put("inheritedStoreTimestamp", message.getStoreTimestamp());
        return expected;
    }

    private static Map<String, Object> messageExtHostApi() throws Exception {
        InetSocketAddress born = new InetSocketAddress(
            InetAddress.getByAddress(new byte[] {(byte) 192, 0, 2, 10}), 10911);
        InetSocketAddress unresolved = InetSocketAddress.createUnresolved("broker.invalid", 10912);
        MessageExt message = new MessageExt();
        Map<String, Object> expected = new TreeMap<>();
        expected.put("defaultBornHostBytesThrowsNullPointer",
            throwsNullPointer(message::getBornHostBytes));
        expected.put("defaultBornHostBufferBytesThrowsNullPointer",
            throwsNullPointer(() -> message.getBornHostBytes(ByteBuffer.allocate(8))));
        expected.put("defaultStoreHostBytesThrowsNullPointer",
            throwsNullPointer(message::getStoreHostBytes));
        expected.put("defaultStoreHostBufferBytesThrowsNullPointer",
            throwsNullPointer(() -> message.getStoreHostBytes(ByteBuffer.allocate(20))));
        expected.put("messageIdPublicConstructors", publicConstructorSignatures(MessageId.class));
        expected.put("messageIdDeclaredPublicMethods", publicMethodSignatures(MessageId.class));
        expected.put("brokerNameDefault", message.getBrokerName());
        message.setBrokerName("broker-a");
        expected.put("brokerNameSet", message.getBrokerName());
        message.setBrokerName(null);
        expected.put("brokerNameCleared", message.getBrokerName());
        message.setBornHost(born);
        message.setStoreHost(unresolved);

        expected.put("bornHostClass", message.getBornHost().getClass().getName());
        expected.put("bornHostToString", message.getBornHost().toString());
        expected.put("bornHostString", message.getBornHostString());
        expected.put("bornHostNameString", message.getBornHostNameString());
        expected.put("bornHostGetterRetainsObject", message.getBornHost() == born);
        expected.put("ipv4SocketAddressString", NetworkUtil.socketAddress2String(born));
        expected.put("bornHostBytes", byteBufferBase64(message.getBornHostBytes()));
        expected.put("bornHostBufferBytes", byteBufferBase64(message.getBornHostBytes(ByteBuffer.allocate(8))));
        ByteBuffer shortIpv4Buffer = ByteBuffer.allocate(7);
        expected.put("ipv4ShortBufferOverflows",
            throwsBufferOverflow(() -> message.getBornHostBytes(shortIpv4Buffer)));
        expected.put("ipv4ShortBufferPosition", shortIpv4Buffer.position());
        expected.put("ipv4ShortBufferBytes",
            Base64.getEncoder().encodeToString(shortIpv4Buffer.array()));
        expected.put("storeHostClass", message.getStoreHost().getClass().getName());
        expected.put("storeHostToString", message.getStoreHost().toString());
        expected.put("storeHostUnresolved", ((InetSocketAddress) message.getStoreHost()).isUnresolved());
        expected.put("storeHostGetterRetainsObject", message.getStoreHost() == unresolved);

        InetSocketAddress ipv6 = new InetSocketAddress(InetAddress.getByName("2001:db8::20"), 10915);
        MessageExt ipv6Message = new MessageExt();
        ipv6Message.setBornHost(ipv6);
        expected.put("ipv6HostToString", ipv6Message.getBornHost().toString());
        expected.put("ipv6HostString", ipv6Message.getBornHostString());
        expected.put("ipv6HostNameString", ipv6Message.getBornHostNameString());
        expected.put("ipv6SocketAddressString", NetworkUtil.socketAddress2String(ipv6));
        expected.put("ipv6HostBytes", byteBufferBase64(ipv6Message.getBornHostBytes()));
        expected.put("ipv6HostBufferBytes", byteBufferBase64(ipv6Message.getBornHostBytes(ByteBuffer.allocate(20))));
        ByteBuffer shortIpv6Buffer = ByteBuffer.allocate(19);
        expected.put("ipv6ShortBufferOverflows",
            throwsBufferOverflow(() -> ipv6Message.getBornHostBytes(shortIpv6Buffer)));
        expected.put("ipv6ShortBufferPosition", shortIpv6Buffer.position());
        expected.put("ipv6ShortBufferBytes",
            Base64.getEncoder().encodeToString(shortIpv6Buffer.array()));
        ipv6Message.setStoreHost(ipv6);
        expected.put("ipv6StoreHostBytes", byteBufferBase64(ipv6Message.getStoreHostBytes()));

        boolean unresolvedBytesThrow = false;
        try {
            message.getStoreHostBytes();
        } catch (NullPointerException expectedFailure) {
            unresolvedBytesThrow = true;
        }
        expected.put("unresolvedHostBytesThrow", unresolvedBytesThrow);
        expected.put("unresolvedHostBufferBytesThrow",
            throwsNullPointer(() -> message.getStoreHostBytes(ByteBuffer.allocate(20))));

        InetSocketAddress fromHost = new InetSocketAddress("127.0.0.1", 10913);
        MessageExt fromHostMessage = new MessageExt();
        fromHostMessage.setBornHost(fromHost);
        expected.put("stringHostToString", fromHostMessage.getBornHost().toString());
        expected.put("stringHostAddress", fromHostMessage.getBornHostString());
        expected.put("stringHostName", fromHostMessage.getBornHostNameString());

        MessageExt constructed = new MessageExt(7, 13L, born, 17L, unresolved, "message-id");
        expected.put("constructorQueueId", constructed.getQueueId());
        expected.put("constructorBornTimestamp", constructed.getBornTimestamp());
        expected.put("constructorBornHost", constructed.getBornHost().toString());
        expected.put("constructorStoreTimestamp", constructed.getStoreTimestamp());
        expected.put("constructorStoreHost", constructed.getStoreHost().toString());
        expected.put("constructorMessageId", constructed.getMsgId());

        MessageExt nullHostConstructed = new MessageExt(-7, Long.MIN_VALUE, null,
            Long.MAX_VALUE, null, null);
        expected.put("nullHostConstructorQueueId", nullHostConstructed.getQueueId());
        expected.put("nullHostConstructorBornTimestamp", nullHostConstructed.getBornTimestamp());
        expected.put("nullHostConstructorBornHostIsNull",
            nullHostConstructed.getBornHost() == null);
        expected.put("nullHostConstructorStoreTimestamp", nullHostConstructed.getStoreTimestamp());
        expected.put("nullHostConstructorStoreHostIsNull",
            nullHostConstructed.getStoreHost() == null);
        expected.put("nullHostConstructorMessageIdIsNull", nullHostConstructed.getMsgId() == null);

        InetSocketAddress messageIdIpv4Address = new InetSocketAddress(
            InetAddress.getByAddress(new byte[] {(byte) 192, 0, 2, 10}), 10911);
        InetSocketAddress messageIdIpv6Address = new InetSocketAddress(
            InetAddress.getByName("2001:db8::20"), 10915);
        long messageIdIpv4Offset = -123456789012345L;
        long messageIdIpv6Offset = Long.MIN_VALUE + 17;
        String messageIdIpv4 = MessageDecoder.createMessageId(messageIdIpv4Address, messageIdIpv4Offset);
        String messageIdIpv6 = MessageDecoder.createMessageId(messageIdIpv6Address, messageIdIpv6Offset);
        expected.put("messageIdIpv4Created", messageIdIpv4);
        expected.put("messageIdIpv6Created", messageIdIpv6);

        ByteBuffer ipv4AddressBytes = ByteBuffer.allocate(8)
            .put(messageIdIpv4Address.getAddress().getAddress())
            .putInt(messageIdIpv4Address.getPort());
        ByteBuffer ipv4MessageIdOutput = ByteBuffer.allocate(16);
        expected.put("messageIdIpv4CreatedFromBytes", MessageDecoder.createMessageId(
            ipv4MessageIdOutput, ipv4AddressBytes.flip(), messageIdIpv4Offset));
        expected.put("messageIdIpv4AddressPositionAfter", ipv4AddressBytes.position());
        expected.put("messageIdIpv4OutputPositionAfter", ipv4MessageIdOutput.position());
        ByteBuffer ipv6AddressBytes = ByteBuffer.allocate(20)
            .put(messageIdIpv6Address.getAddress().getAddress())
            .putInt(messageIdIpv6Address.getPort());
        ByteBuffer ipv6MessageIdOutput = ByteBuffer.allocate(28);
        expected.put("messageIdIpv6CreatedFromBytes", MessageDecoder.createMessageId(
            ipv6MessageIdOutput, ipv6AddressBytes.flip(), messageIdIpv6Offset));
        expected.put("messageIdIpv6AddressPositionAfter", ipv6AddressBytes.position());
        expected.put("messageIdIpv6OutputPositionAfter", ipv6MessageIdOutput.position());
        ByteBuffer positionedAddress = ByteBuffer.allocate(8)
            .put(messageIdIpv4Address.getAddress().getAddress())
            .putInt(messageIdIpv4Address.getPort());
        positionedAddress.flip().position(2);
        ByteBuffer positionedMessageIdOutput = ByteBuffer.allocate(16);
        positionedMessageIdOutput.put(new byte[] {1, 2, 3, 4, 5});
        expected.put("messageIdIpv4CreatedFromPositionedBuffers", MessageDecoder.createMessageId(
            positionedMessageIdOutput, positionedAddress, messageIdIpv4Offset));
        expected.put("messageIdPositionedAddressPositionAfter", positionedAddress.position());
        expected.put("messageIdPositionedOutputPositionAfter", positionedMessageIdOutput.position());
        ByteBuffer shortAddressBytes = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
        ByteBuffer shortAddressOutput = ByteBuffer.allocate(28);
        expected.put("messageIdShortAddressOutput", MessageDecoder.createMessageId(
            shortAddressOutput, shortAddressBytes, 1234L));
        expected.put("messageIdShortAddressPositionAfter", shortAddressBytes.position());
        expected.put("messageIdShortOutputPositionAfter", shortAddressOutput.position());
        expected.putAll(messageIdBufferOverflowCases());

        MessageId decodedIpv4MessageId = MessageDecoder.decodeMessageId(messageIdIpv4);
        InetSocketAddress decodedIpv4Address = (InetSocketAddress) decodedIpv4MessageId.getAddress();
        expected.put("messageIdIpv4DecodedHost", decodedIpv4Address.getAddress().getHostAddress());
        expected.put("messageIdIpv4DecodedPort", decodedIpv4Address.getPort());
        expected.put("messageIdIpv4DecodedOffset", decodedIpv4MessageId.getOffset());
        MessageId decodedIpv6MessageId = MessageDecoder.decodeMessageId(messageIdIpv6);
        InetSocketAddress decodedIpv6Address = (InetSocketAddress) decodedIpv6MessageId.getAddress();
        expected.put("messageIdIpv6DecodedHost", decodedIpv6Address.getAddress().getHostAddress());
        expected.put("messageIdIpv6DecodedPort", decodedIpv6Address.getPort());
        expected.put("messageIdIpv6DecodedOffset", decodedIpv6MessageId.getOffset());
        expected.put("messageIdIpv6ExtraBytesIgnored",
            MessageDecoder.decodeMessageId(messageIdIpv6 + "FFFF").getOffset());

        MessageId mutableMessageId = new MessageId(messageIdIpv4Address, 42L);
        mutableMessageId.setAddress(messageIdIpv6Address);
        mutableMessageId.setOffset(-17L);
        InetSocketAddress mutableMessageIdAddress = (InetSocketAddress) mutableMessageId.getAddress();
        expected.put("messageIdSetterHost", mutableMessageIdAddress.getAddress().getHostAddress());
        expected.put("messageIdSetterPort", mutableMessageIdAddress.getPort());
        expected.put("messageIdSetterOffset", mutableMessageId.getOffset());
        MessageId nullableMessageId = new MessageId(null, 0L);
        expected.put("messageIdNullableAddress", nullableMessageId.getAddress() == null);
        nullableMessageId.setAddress(messageIdIpv4Address);
        nullableMessageId.setAddress(null);
        expected.put("messageIdNullableSetter", nullableMessageId.getAddress() == null);

        expected.put("messageIdEmptyThrowsNullPointer",
            throwsMessageIdNullPointer(""));
        expected.put("messageIdShortThrowsBufferUnderflow", throwsMessageIdBufferUnderflow("0"));
        expected.put("messageIdBadPortThrowsIllegalArgument",
            throwsMessageIdIllegalArgument("7F000001FFFFFFFF0000000000003039"));
        expected.put("messageIdInvalidHexHost",
            ((InetSocketAddress) MessageDecoder.decodeMessageId("GG00000100002A9F0000000000003039")
                .getAddress()).getAddress().getHostAddress());
        return expected;
    }

    private static Map<String, Object> messageIdBufferOverflowCases() {
        Map<String, Object> expected = new TreeMap<>();
        ByteBuffer tooSmallOutput = ByteBuffer.allocate(15);
        ByteBuffer smallOutputAddress = ByteBuffer.wrap(new byte[8]);
        try {
            MessageDecoder.createMessageId(tooSmallOutput, smallOutputAddress, 0L);
            expected.put("messageIdOutputLimitThrowsIllegalArgument", false);
        } catch (IllegalArgumentException expectedLimitFailure) {
            expected.put("messageIdOutputLimitThrowsIllegalArgument", true);
        }
        expected.put("messageIdOutputLimitAddressPosition", smallOutputAddress.position());
        expected.put("messageIdOutputLimitOutputPosition", tooSmallOutput.position());

        ByteBuffer tooLongAddress = ByteBuffer.wrap(new byte[29]);
        ByteBuffer tooLongOutput = ByteBuffer.allocate(28);
        try {
            MessageDecoder.createMessageId(tooLongOutput, tooLongAddress, 0L);
            expected.put("messageIdAddressPutOverflows", false);
        } catch (java.nio.BufferOverflowException expectedOverflow) {
            expected.put("messageIdAddressPutOverflows", true);
        }
        expected.put("messageIdAddressPutOverflowAddressPosition", tooLongAddress.position());
        expected.put("messageIdAddressPutOverflowOutputPosition", tooLongOutput.position());

        ByteBuffer offsetOverflowAddress = ByteBuffer.wrap(new byte[25]);
        ByteBuffer offsetOverflowOutput = ByteBuffer.allocate(28);
        try {
            MessageDecoder.createMessageId(offsetOverflowOutput, offsetOverflowAddress, 0L);
            expected.put("messageIdOffsetPutOverflows", false);
        } catch (java.nio.BufferOverflowException expectedOverflow) {
            expected.put("messageIdOffsetPutOverflows", true);
        }
        expected.put("messageIdOffsetPutOverflowAddressPosition", offsetOverflowAddress.position());
        expected.put("messageIdOffsetPutOverflowOutputPosition", offsetOverflowOutput.position());
        return expected;
    }

    private static Map<String, Object> messageExtSystemFlagsApi() {
        MessageExt message = new MessageExt();
        Map<String, Object> expected = new TreeMap<>();
        expected.put("topicSysFlagAbsentIsNull", message.getTopicSysFlag() == null);
        expected.put("groupSysFlagAbsentIsNull", message.getGroupSysFlag() == null);
        message.setTopicSysFlag(73);
        message.setGroupSysFlag(-19);
        expected.put("topicSysFlagSet", message.getTopicSysFlag());
        expected.put("groupSysFlagSet", message.getGroupSysFlag());
        message.setTopicSysFlag(null);
        message.setGroupSysFlag(null);
        expected.put("topicSysFlagCleared", message.getTopicSysFlag() == null);
        expected.put("groupSysFlagCleared", message.getGroupSysFlag() == null);

        MessageAccessor.putProperty(message, org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG, "");
        MessageAccessor.putProperty(message, org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_GROUP_CONFIG, "");
        expected.put("topicSysFlagEmptyIsNull", message.getTopicSysFlag() == null);
        expected.put("groupSysFlagEmptyIsNull", message.getGroupSysFlag() == null);
        MessageAccessor.putProperty(message, org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG, "bad");
        MessageAccessor.putProperty(message, org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_GROUP_CONFIG, "bad");
        expected.put("topicSysFlagMalformedThrows", throwsNumberFormat(message, true));
        expected.put("groupSysFlagMalformedThrows", throwsNumberFormat(message, false));
        MessageAccessor.putProperty(message, org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG, "\u0661");
        expected.put("topicSysFlagUnicodeDigit", message.getTopicSysFlag());

        message.setSysFlag(0x40);
        message.setBornHostV6Flag();
        expected.put("bornV6FlagAfterSetter", message.getSysFlag());
        message.setStoreHostAddressV6Flag();
        expected.put("storeV6FlagAfterSetter", message.getSysFlag());
        expected.put("singleTagFilterType", MessageExt.parseTopicFilterType(0).name());
        expected.put("multiTagFilterType", MessageExt.parseTopicFilterType(0x42).name());

        MessageExt diagnostic = new MessageExt();
        diagnostic.setTopic("invoice");
        diagnostic.setBody(new byte[] {0, -1, 42});
        diagnostic.setFlag(5);
        MessageAccessor.putProperty(diagnostic, "KEY", "value");
        expected.put("messageExtToString", diagnostic.toString());
        return expected;
    }

    private static boolean throwsNumberFormat(MessageExt message, boolean topic) {
        try {
            if (topic) message.getTopicSysFlag(); else message.getGroupSysFlag();
            return false;
        } catch (NumberFormatException expected) {
            return true;
        }
    }

    private static String byteBufferBase64(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static MessageExt message(String topic, String body, int queueId, long queueOffset,
        InetAddress born, int bornPort, InetAddress store, int storePort, int sysFlag,
        Map<String, String> properties) {
        return message(topic, body.getBytes(StandardCharsets.UTF_8), queueId, queueOffset,
            born, bornPort, store, storePort, sysFlag, properties);
    }

    private static MessageExt message(String topic, byte[] body, int queueId, long queueOffset,
        InetAddress born, int bornPort, InetAddress store, int storePort, int sysFlag,
        Map<String, String> properties) {
        MessageExt message = new MessageExt();
        message.setTopic(topic);
        message.setBody(body);
        message.setQueueId(queueId);
        message.setQueueOffset(queueOffset);
        message.setCommitLogOffset(1_700_000_000_000L + queueOffset);
        message.setSysFlag(sysFlag);
        message.setFlag(queueId * 3);
        message.setBodyCRC(0);
        message.setBornTimestamp(1_700_000_100_000L + queueOffset);
        message.setStoreTimestamp(1_700_000_200_000L + queueOffset);
        message.setBornHost(new InetSocketAddress(born, bornPort));
        message.setStoreHost(new InetSocketAddress(store, storePort));
        message.setReconsumeTimes(2);
        message.setPreparedTransactionOffset(1_600_000_000_000L + queueOffset);
        properties.forEach((key, value) -> MessageAccessor.putProperty(message, key, value));
        return message;
    }

    private static Map<String, Object> record(String name, MessageExt source, String v2Topic) throws Exception {
        byte[] encoded = MessageDecoder.encode(source, true);
        if (v2Topic != null) {
            encoded = convertToV2(encoded, source.getSysFlag(), v2Topic);
        }
        int bornHostLength = (source.getSysFlag() & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storeHostLength = (source.getSysFlag() & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
        int bodyLengthOffset = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornHostLength
            + 8 + storeHostLength + 4 + 8;
        int bodyLength = ByteBuffer.wrap(encoded).getInt(bodyLengthOffset);
        int bodyCrc = UtilAll.crc32(encoded, bodyLengthOffset + 4, bodyLength);
        ByteBuffer.wrap(encoded).putInt(8, bodyCrc);
        MessageExt decoded = MessageDecoder.decode(ByteBuffer.wrap(encoded), true, true, false, false, true);
        Map<String, Object> expected = new TreeMap<>();
        expected.put("topic", decoded.getTopic());
        expected.put("bodyBase64", decoded.getBody() == null
            ? null : Base64.getEncoder().encodeToString(decoded.getBody()));
        expected.put("flag", decoded.getFlag());
        expected.put("bodyCRC", decoded.getBodyCRC());
        expected.put("queueId", decoded.getQueueId());
        expected.put("queueOffset", decoded.getQueueOffset());
        expected.put("commitLogOffset", decoded.getCommitLogOffset());
        expected.put("sysFlag", decoded.getSysFlag());
        expected.put("bornTimestamp", decoded.getBornTimestamp());
        expected.put("storeTimestamp", decoded.getStoreTimestamp());
        expected.put("bornHost", decoded.getBornHost().toString());
        expected.put("storeHost", decoded.getStoreHost().toString());
        expected.put("reconsumeTimes", decoded.getReconsumeTimes());
        expected.put("preparedTransactionOffset", decoded.getPreparedTransactionOffset());
        expected.put("msgId", decoded.getMsgId());
        expected.put("properties", decoded.getProperties() == null
            ? Map.of() : new TreeMap<>(decoded.getProperties()));
        ByteBuffer propertiesBuffer = ByteBuffer.wrap(encoded);
        Map<String, String> propertiesDecodedDirectly = MessageDecoder.decodeProperties(propertiesBuffer);
        expected.put("recordProperties", propertiesDecodedDirectly == null
            ? null : new TreeMap<>(propertiesDecodedDirectly));
        expected.put("recordPropertiesPosition", propertiesBuffer.position());

        MessageExt propertiesStringDecoded = MessageDecoder.decode(
            ByteBuffer.wrap(encoded), true, true, false, true, true);
        expected.put("propertiesWithString", propertiesStringDecoded.getProperties() == null
            ? Map.of() : new TreeMap<>(propertiesStringDecoded.getProperties()));

        MessageClientExt clientDecoded = (MessageClientExt) MessageDecoder.decode(
            ByteBuffer.wrap(encoded), true, true, true, false, true);
        Map<String, Object> expectedClientIds = new TreeMap<>();
        expectedClientIds.put("msgId", clientDecoded.getMsgId());
        expectedClientIds.put("offsetMsgId", clientDecoded.getOffsetMsgId());

        List<Map<String, Object>> bodyOptions = new ArrayList<>();
        for (boolean readBody : new boolean[] {false, true}) {
            for (boolean decompressBody : new boolean[] {false, true}) {
                MessageExt optionDecoded = MessageDecoder.decode(
                    ByteBuffer.wrap(encoded), readBody, decompressBody, true, false, true);
                Map<String, Object> option = new TreeMap<>();
                option.put("readBody", readBody);
                option.put("decompressBody", decompressBody);
                option.put("bodyBase64", optionDecoded.getBody() == null ? null
                    : Base64.getEncoder().encodeToString(optionDecoded.getBody()));
                option.put("sysFlag", optionDecoded.getSysFlag());
                bodyOptions.add(option);
            }
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("name", name);
        result.put("recordBase64", Base64.getEncoder().encodeToString(encoded));
        result.put("sha256", hex(MessageDigest.getInstance("SHA-256").digest(encoded)));
        result.put("decoded", expected);
        result.put("clientDecoded", expectedClientIds);
        result.put("decodeOptions", bodyOptions);
        result.put("decodeOverloads", decodeOverloads(encoded));
        return result;
    }

    private static Map<String, Object> decodeOverloads(byte[] encoded) {
        Map<String, Object> outcomes = new TreeMap<>();
        ByteBuffer input = ByteBuffer.wrap(encoded);
        outcomes.put("decodeDefault", decodeSnapshot(MessageDecoder.decode(input), input));

        input = ByteBuffer.wrap(encoded);
        outcomes.put("decodeReadBodyFalse",
            decodeSnapshot(MessageDecoder.decode(input, false), input));
        input = ByteBuffer.wrap(encoded);
        outcomes.put("decodeReadBodyTrue",
            decodeSnapshot(MessageDecoder.decode(input, true), input));

        input = ByteBuffer.wrap(encoded);
        outcomes.put("decodeNoBodyNoDecompress",
            decodeSnapshot(MessageDecoder.decode(input, false, false), input));
        input = ByteBuffer.wrap(encoded);
        outcomes.put("decodeNoBodyDecompress",
            decodeSnapshot(MessageDecoder.decode(input, false, true), input));
        input = ByteBuffer.wrap(encoded);
        outcomes.put("decodeBodyNoDecompress",
            decodeSnapshot(MessageDecoder.decode(input, true, false), input));
        input = ByteBuffer.wrap(encoded);
        outcomes.put("decodeBodyDecompress",
            decodeSnapshot(MessageDecoder.decode(input, true, true), input));

        input = ByteBuffer.wrap(encoded);
        outcomes.put("decodeGenericFourArguments",
            decodeSnapshot(MessageDecoder.decode(input, true, true, false), input));
        input = ByteBuffer.wrap(encoded);
        outcomes.put("decodeClientFourArguments",
            decodeSnapshot(MessageDecoder.decode(input, true, true, true), input));

        input = ByteBuffer.wrap(encoded);
        outcomes.put("decodeGenericFiveArguments",
            decodeSnapshot(MessageDecoder.decode(input, true, true, false, false), input));
        input = ByteBuffer.wrap(encoded);
        outcomes.put("decodePropertiesStringFiveArguments",
            decodeSnapshot(MessageDecoder.decode(input, true, true, false, true), input));

        input = ByteBuffer.wrap(encoded);
        outcomes.put("decodeWithoutCrcCheck",
            decodeSnapshot(MessageDecoder.decode(input, true, true, false, false, false), input));
        input = ByteBuffer.wrap(encoded);
        outcomes.put("decodeWithCrcCheck",
            decodeSnapshot(MessageDecoder.decode(input, true, true, false, false, true), input));

        input = ByteBuffer.wrap(encoded);
        outcomes.put("clientDecodeReadBodyFalse",
            decodeSnapshot(MessageDecoder.clientDecode(input, false), input));
        input = ByteBuffer.wrap(encoded);
        outcomes.put("clientDecodeReadBodyTrue",
            decodeSnapshot(MessageDecoder.clientDecode(input, true), input));
        return outcomes;
    }

    private static Map<String, Object> decodeSnapshot(MessageExt message, ByteBuffer input) {
        Map<String, Object> snapshot = new TreeMap<>();
        snapshot.put("messageClass", message.getClass().getSimpleName());
        snapshot.put("topic", message.getTopic());
        snapshot.put("bodyBase64", message.getBody() == null
            ? null : Base64.getEncoder().encodeToString(message.getBody()));
        snapshot.put("sysFlag", message.getSysFlag());
        snapshot.put("properties", message.getProperties() == null
            ? null : new TreeMap<>(message.getProperties()));
        snapshot.put("messageId", message.getMsgId());
        snapshot.put("offsetMsgId", message instanceof MessageClientExt
            ? ((MessageClientExt) message).getOffsetMsgId() : null);
        snapshot.put("consumed", input.position());
        return snapshot;
    }

    private static byte[] convertToV2(byte[] v1, int sysFlag, String topic) {
        ByteBuffer input = ByteBuffer.wrap(v1);
        int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
        int bodyLengthOffset = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornHostLength
            + 8 + storeHostLength + 4 + 8;
        int bodyLength = input.getInt(bodyLengthOffset);
        int topicLengthOffset = bodyLengthOffset + 4 + bodyLength;
        int topicLength = Byte.toUnsignedInt(v1[topicLengthOffset]);
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int propertiesOffset = topicLengthOffset + 1 + topicLength;
        byte[] v2 = new byte[v1.length - 1 - topicLength + 2 + topicBytes.length];
        System.arraycopy(v1, 0, v2, 0, topicLengthOffset);
        ByteBuffer output = ByteBuffer.wrap(v2);
        output.putInt(0, v2.length);
        output.putInt(4, MAGIC_V2);
        output.position(topicLengthOffset);
        output.putShort((short) topicBytes.length);
        output.put(topicBytes);
        System.arraycopy(v1, propertiesOffset, v2, topicLengthOffset + 2 + topicBytes.length,
            v1.length - propertiesOffset);
        return v2;
    }

    private static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            value.append(String.format("%02X", b & 0xff));
        }
        return value.toString();
    }
}
