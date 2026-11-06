package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletionException;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.NotifyResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.entity.ClientGroup;
import org.apache.rocketmq.common.lite.LiteLagInfo;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.common.HeartbeatV2Result;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.TopicOffset;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.body.GetBrokerLiteInfoResponseBody;
import org.apache.rocketmq.remoting.protocol.body.GetLiteClientInfoResponseBody;
import org.apache.rocketmq.remoting.protocol.body.GetLiteGroupInfoResponseBody;
import org.apache.rocketmq.remoting.protocol.body.GetLiteTopicInfoResponseBody;
import org.apache.rocketmq.remoting.protocol.body.GetParentTopicInfoResponseBody;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody;
import org.apache.rocketmq.remoting.protocol.header.NotificationRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetLiteClientInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetLiteGroupInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetLiteTopicInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetParentTopicInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

/** Persisted invocation evidence: only RemotingClient is substituted. */
public final class ExtensionMoreInvokedFixtures {
    private ExtensionMoreInvokedFixtures() { }

    private static final class Capture {
        RemotingCommand request;
        RemotingCommand response;
        String mode;
        List<String> nameServerAddresses = new ArrayList<>(List.of("initial:9876"));
        RemotingClient proxy() {
            return (RemotingClient) Proxy.newProxyInstance(RemotingClient.class.getClassLoader(),
                new Class<?>[] {RemotingClient.class}, (proxy, method, args) -> {
                    if ("updateNameServerAddressList".equals(method.getName())) {
                        nameServerAddresses = new ArrayList<>((List<String>) args[0]);
                        return null;
                    }
                    if (method.getName().startsWith("invoke")) {
                        mode = method.getName();
                        request = (RemotingCommand) args[1];
                        if ("invokeSync".equals(mode)) return response;
                        if ("invoke".equals(mode)) return java.util.concurrent.CompletableFuture.completedFuture(response);
                        if ("invokeAsync".equals(mode)) {
                            ((InvokeCallback) args[3]).operationSucceed(response);
                            return null;
                        }
                        return null;
                    }
                    return method.getReturnType() == boolean.class ? false : null;
                });
        }
    }

    private static void check(boolean ok, String label) {
        if (!ok) throw new IllegalStateException(label);
    }

    private static Map<String, Object> command(RemotingCommand cmd) {
        cmd.makeCustomHeaderToNet();
        Map<String, Object> result = new TreeMap<>();
        result.put("code", cmd.getCode());
        result.put("ext", cmd.getExtFields() == null ? Collections.emptyMap() : new TreeMap<>(cmd.getExtFields()));
        result.put("bodyBase64", cmd.getBody() == null ? null : Base64.getEncoder().encodeToString(cmd.getBody()));
        result.put("version", cmd.getVersion());
        return result;
    }

    private static Map<String, Object> futureCommand(RemotingCommand cmd) {
        Map<String, Object> result = command(cmd);
        result.put("remark", cmd.getRemark());
        return result;
    }

    private static void record(Path dir, boolean verify, Capture c, String name, int code, String outcome)
        throws Exception {
        check("invokeSync".equals(c.mode) && c.request != null && c.request.getCode() == code,
            name + " dispatch");
        Map<String, Object> fixture = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("entrypoint", name.startsWith("parent-topic-info-")
            ? "MQClientAPIImpl.getParentTopicInfo"
            : name.startsWith("lite-client-info-") ? "MQClientAPIImpl.getLiteClientInfo"
            : name.startsWith("lite-group-info-") ? "MQClientAPIImpl.getLiteGroupInfo" : "MQClientAPIImpl");
        provenance.put("transport", "mock RemotingClient");
        provenance.put("serverBaselineContacted", false);
        fixture.put("provenance", provenance);
        fixture.put("case", name);
        fixture.put("mode", "SYNC");
        int responseVersion = c.response.getVersion();
        fixture.put("request", command(c.request));
        Map<String, Object> response = command(c.response);
        response.put("version", responseVersion);
        fixture.put("response", response);
        fixture.put("outcome", outcome);
        if (name.startsWith("lite-broker-info-")) {
            fixture.put("brokerLiteInfoApi", brokerLiteInfoApi());
        }
        if (name.startsWith("parent-topic-info-")) {
            fixture.put("parentTopicInfoApi", parentTopicInfoApi());
        }
        if (name.startsWith("lite-client-info-")) {
            fixture.put("liteClientInfoApi", liteClientInfoApi());
        }
        if (name.startsWith("lite-group-info-")) {
            fixture.put("liteGroupInfoApi", liteGroupInfoApi());
        }
        Path file = dir.resolve(name + ".json");
        String json = JSON.toJSONString(fixture) + "\n";
        if (verify) check(Files.exists(file) && json.equals(Files.readString(file)), "fixture drift: " + file);
        else {
            Files.createDirectories(dir);
            Files.writeString(file, json);
        }
    }

    private static Map<String, Object> notifyResultApi() {
        Map<String, Object> api = new TreeMap<>();
        api.put("publicFieldCount", NotifyResult.class.getFields().length);
        api.put("publicConstructorCount", NotifyResult.class.getConstructors().length);
        List<String> methods = Arrays.stream(NotifyResult.class.getDeclaredMethods())
            .filter(method -> java.lang.reflect.Modifier.isPublic(method.getModifiers()))
            .map(method -> method.getName() + ":" + method.getReturnType().getSimpleName() + "(" +
                Arrays.stream(method.getParameterTypes()).map(Class::getSimpleName)
                    .collect(java.util.stream.Collectors.joining(",")) + ")")
            .sorted().collect(java.util.stream.Collectors.toList());
        api.put("declaredPublicMethods", methods);
        NotifyResult result = new NotifyResult();
        api.put("defaultHasMsg", result.isHasMsg());
        api.put("defaultPollingFull", result.isPollingFull());
        result.setHasMsg(true);
        result.setPollingFull(true);
        api.put("populatedToString", result.toString());
        return api;
    }

    private static void recordNotification(Path dir, boolean verify, Capture c, String name,
        String entrypoint, String outcome) throws Exception {
        check("invoke".equals(c.mode) && c.request != null &&
            c.request.getCode() == RequestCode.NOTIFICATION, name + " dispatch");
        Map<String, Object> fixture = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("entrypoint", entrypoint);
        provenance.put("transport", "mock RemotingClient");
        provenance.put("serverBaselineContacted", false);
        fixture.put("provenance", provenance);
        fixture.put("case", name);
        fixture.put("mode", "ASYNC");
        fixture.put("notifyResultApi", notifyResultApi());
        fixture.put("request", command(c.request));
        fixture.put("response", command(c.response));
        fixture.put("outcome", outcome);
        Path file = dir.resolve(name + ".json");
        String json = JSON.toJSONString(fixture) + "\n";
        if (verify) check(Files.exists(file) && json.equals(Files.readString(file)), "fixture drift: " + file);
        else {
            Files.createDirectories(dir);
            Files.writeString(file, json);
        }
    }

    private static List<String> publicMethods(Class<?> type) {
        return Arrays.stream(type.getDeclaredMethods())
            .filter(method -> java.lang.reflect.Modifier.isPublic(method.getModifiers()))
            .map(java.lang.reflect.Method::toGenericString)
            .sorted().collect(java.util.stream.Collectors.toList());
    }

    private static List<String> publicFields(Class<?> type) {
        return Arrays.stream(type.getFields())
            .map(field -> (java.lang.reflect.Modifier.isFinal(field.getModifiers()) ? "final " : "") +
                field.getType().getSimpleName() + " " + field.getName())
            .sorted().collect(java.util.stream.Collectors.toList());
    }

    private static Map<String, Object> liteTopicInfoApi() {
        Map<String, Object> api = new TreeMap<>();
        api.put("requestHeaderConstructors", Arrays.stream(GetLiteTopicInfoRequestHeader.class.getConstructors())
            .map(java.lang.reflect.Constructor::toGenericString).sorted().collect(java.util.stream.Collectors.toList()));
        api.put("requestHeaderPublicFields", publicFields(GetLiteTopicInfoRequestHeader.class));
        api.put("requestHeaderPublicMethods", publicMethods(GetLiteTopicInfoRequestHeader.class));
        api.put("responseBodyConstructors", Arrays.stream(GetLiteTopicInfoResponseBody.class.getConstructors())
            .map(java.lang.reflect.Constructor::toGenericString).sorted().collect(java.util.stream.Collectors.toList()));
        api.put("responseBodyPublicFields", publicFields(GetLiteTopicInfoResponseBody.class));
        api.put("responseBodyPublicMethods", publicMethods(GetLiteTopicInfoResponseBody.class));
        api.put("clientGroupConstructors", Arrays.stream(ClientGroup.class.getConstructors())
            .map(java.lang.reflect.Constructor::toGenericString).sorted().collect(java.util.stream.Collectors.toList()));
        api.put("clientGroupPublicFields", publicFields(ClientGroup.class));
        api.put("clientGroupPublicMethods", publicMethods(ClientGroup.class));
        api.put("topicOffsetConstructors", Arrays.stream(TopicOffset.class.getConstructors())
            .map(java.lang.reflect.Constructor::toGenericString).sorted().collect(java.util.stream.Collectors.toList()));
        api.put("topicOffsetPublicFields", publicFields(TopicOffset.class));
        api.put("topicOffsetPublicMethods", publicMethods(TopicOffset.class));
        GetLiteTopicInfoResponseBody defaultBody = new GetLiteTopicInfoResponseBody();
        Map<String, Object> defaults = new TreeMap<>();
        defaults.put("parentTopic", defaultBody.getParentTopic());
        defaults.put("liteTopic", defaultBody.getLiteTopic());
        defaults.put("subscriberNull", defaultBody.getSubscriber() == null);
        defaults.put("topicOffsetNull", defaultBody.getTopicOffset() == null);
        defaults.put("shardingToBroker", defaultBody.isShardingToBroker());
        api.put("responseBodyDefaults", defaults);
        Set<ClientGroup> assignedSubscribers = new HashSet<>();
        defaultBody.setSubscriber(assignedSubscribers);
        api.put("subscriberGetterReturnsAssignedSet", defaultBody.getSubscriber() == assignedSubscribers);
        defaultBody.getSubscriber().add(new ClientGroup("client", "group"));
        api.put("subscriberGetterMutationVisible", assignedSubscribers.size() == 1);
        TopicOffset assignedOffset = new TopicOffset();
        defaultBody.setTopicOffset(assignedOffset);
        api.put("topicOffsetGetterReturnsAssignedObject", defaultBody.getTopicOffset() == assignedOffset);
        assignedOffset.setMinOffset(-1);
        api.put("topicOffsetMutationVisible", defaultBody.getTopicOffset().getMinOffset() == -1);
        ClientGroup nullGroup = new ClientGroup(null, null);
        api.put("clientGroupNullToString", nullGroup.toString());
        api.put("clientGroupEqualValuesCompareEqual",
            nullGroup.equals(new ClientGroup(null, null)));
        api.put("clientGroupDifferentValuesCompareUnequal",
            nullGroup.equals(new ClientGroup("client", "group")) == false);
        return api;
    }

    private static Map<String, Object> brokerLiteInfoApi() {
        Map<String, Object> api = new TreeMap<>();
        api.put("rpcMethods", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> java.lang.reflect.Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getBrokerLiteInfo"))
            .map(java.lang.reflect.Method::toGenericString).sorted()
            .collect(java.util.stream.Collectors.toList()));
        api.put("responseBodyConstructors", Arrays.stream(GetBrokerLiteInfoResponseBody.class.getConstructors())
            .map(java.lang.reflect.Constructor::toGenericString).sorted()
            .collect(java.util.stream.Collectors.toList()));
        api.put("responseBodyPublicFields", publicFields(GetBrokerLiteInfoResponseBody.class));
        api.put("responseBodyPublicMethods", publicMethods(GetBrokerLiteInfoResponseBody.class));
        GetBrokerLiteInfoResponseBody defaults = new GetBrokerLiteInfoResponseBody();
        Map<String, Object> values = new TreeMap<>();
        values.put("storeType", defaults.getStoreType());
        values.put("maxLmqNum", defaults.getMaxLmqNum());
        values.put("currentLmqNum", defaults.getCurrentLmqNum());
        values.put("liteSubscriptionCount", defaults.getLiteSubscriptionCount());
        values.put("orderInfoCount", defaults.getOrderInfoCount());
        values.put("cqTableSize", defaults.getCqTableSize());
        values.put("offsetTableSize", defaults.getOffsetTableSize());
        values.put("eventMapSize", defaults.getEventMapSize());
        values.put("topicMetaNull", defaults.getTopicMeta() == null);
        values.put("groupMetaNull", defaults.getGroupMeta() == null);
        api.put("responseBodyDefaults", values);
        return api;
    }

    private static Map<String, Object> parentTopicInfoApi() {
        Map<String, Object> api = new TreeMap<>();
        api.put("rpcMethods", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> java.lang.reflect.Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getParentTopicInfo"))
            .map(java.lang.reflect.Method::toGenericString).sorted()
            .collect(java.util.stream.Collectors.toList()));
        api.put("requestHeaderConstructors", Arrays.stream(GetParentTopicInfoRequestHeader.class.getConstructors())
            .map(java.lang.reflect.Constructor::toGenericString).sorted()
            .collect(java.util.stream.Collectors.toList()));
        api.put("requestHeaderPublicFields", publicFields(GetParentTopicInfoRequestHeader.class));
        api.put("requestHeaderPublicMethods", publicMethods(GetParentTopicInfoRequestHeader.class));
        api.put("responseBodyConstructors", Arrays.stream(GetParentTopicInfoResponseBody.class.getConstructors())
            .map(java.lang.reflect.Constructor::toGenericString).sorted()
            .collect(java.util.stream.Collectors.toList()));
        api.put("responseBodyPublicFields", publicFields(GetParentTopicInfoResponseBody.class));
        api.put("responseBodyPublicMethods", publicMethods(GetParentTopicInfoResponseBody.class));
        GetParentTopicInfoResponseBody defaults = new GetParentTopicInfoResponseBody();
        Map<String, Object> values = new TreeMap<>();
        values.put("topic", defaults.getTopic());
        values.put("ttl", defaults.getTtl());
        values.put("groupsNull", defaults.getGroups() == null);
        values.put("lmqNum", defaults.getLmqNum());
        values.put("liteTopicCount", defaults.getLiteTopicCount());
        api.put("responseBodyDefaults", values);
        Set<String> assigned = new HashSet<>();
        defaults.setGroups(assigned);
        api.put("groupsGetterReturnsAssignedSet", defaults.getGroups() == assigned);
        defaults.getGroups().add("workers");
        api.put("groupsGetterMutationVisible", assigned.size() == 1);
        return api;
    }

    private static Map<String, Object> liteClientInfoApi() {
        Map<String, Object> api = new TreeMap<>();
        api.put("rpcMethods", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> java.lang.reflect.Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getLiteClientInfo"))
            .map(java.lang.reflect.Method::toGenericString).sorted()
            .collect(java.util.stream.Collectors.toList()));
        api.put("requestHeaderConstructors", Arrays.stream(GetLiteClientInfoRequestHeader.class.getConstructors())
            .map(java.lang.reflect.Constructor::toGenericString).sorted()
            .collect(java.util.stream.Collectors.toList()));
        api.put("requestHeaderPublicFields", publicFields(GetLiteClientInfoRequestHeader.class));
        api.put("requestHeaderPublicMethods", publicMethods(GetLiteClientInfoRequestHeader.class));
        GetLiteClientInfoRequestHeader defaultHeader = new GetLiteClientInfoRequestHeader();
        api.put("requestHeaderDefaultMaxCount", defaultHeader.getMaxCount());
        defaultHeader.setMaxCount(0);
        try {
            defaultHeader.checkFields();
            api.put("nonpositiveMaxCountRejected", false);
        } catch (org.apache.rocketmq.remoting.exception.RemotingCommandException expected) {
            api.put("nonpositiveMaxCountRejected", true);
        }
        api.put("responseBodyConstructors", Arrays.stream(GetLiteClientInfoResponseBody.class.getConstructors())
            .map(java.lang.reflect.Constructor::toGenericString).sorted()
            .collect(java.util.stream.Collectors.toList()));
        api.put("responseBodyPublicFields", publicFields(GetLiteClientInfoResponseBody.class));
        api.put("responseBodyPublicMethods", publicMethods(GetLiteClientInfoResponseBody.class));
        GetLiteClientInfoResponseBody defaults = new GetLiteClientInfoResponseBody();
        Map<String, Object> values = new TreeMap<>();
        values.put("parentTopic", defaults.getParentTopic());
        values.put("group", defaults.getGroup());
        values.put("clientId", defaults.getClientId());
        values.put("lastAccessTime", defaults.getLastAccessTime());
        values.put("lastConsumeTime", defaults.getLastConsumeTime());
        values.put("liteTopicCount", defaults.getLiteTopicCount());
        values.put("liteTopicSetNull", defaults.getLiteTopicSet() == null);
        api.put("responseBodyDefaults", values);
        Set<String> assigned = new HashSet<>();
        defaults.setLiteTopicSet(assigned);
        api.put("topicSetGetterReturnsAssignedSet", defaults.getLiteTopicSet() == assigned);
        defaults.getLiteTopicSet().add("%LMQ%lite-parent%orders");
        api.put("topicSetGetterMutationVisible", assigned.size() == 1);
        return api;
    }

    private static Map<String, Object> liteGroupInfoApi() {
        Map<String, Object> api = new TreeMap<>();
        api.put("rpcMethods", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> java.lang.reflect.Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getLiteGroupInfo"))
            .map(java.lang.reflect.Method::toGenericString).sorted()
            .collect(java.util.stream.Collectors.toList()));
        api.put("requestHeaderConstructors", Arrays.stream(GetLiteGroupInfoRequestHeader.class.getConstructors())
            .map(java.lang.reflect.Constructor::toGenericString).sorted()
            .collect(java.util.stream.Collectors.toList()));
        api.put("requestHeaderPublicFields", publicFields(GetLiteGroupInfoRequestHeader.class));
        api.put("requestHeaderPublicMethods", publicMethods(GetLiteGroupInfoRequestHeader.class));
        api.put("responseBodyConstructors", Arrays.stream(GetLiteGroupInfoResponseBody.class.getConstructors())
            .map(java.lang.reflect.Constructor::toGenericString).sorted()
            .collect(java.util.stream.Collectors.toList()));
        api.put("responseBodyPublicFields", publicFields(GetLiteGroupInfoResponseBody.class));
        api.put("responseBodyPublicMethods", publicMethods(GetLiteGroupInfoResponseBody.class));
        api.put("offsetWrapperConstructors", Arrays.stream(OffsetWrapper.class.getConstructors())
            .map(java.lang.reflect.Constructor::toGenericString).sorted()
            .collect(java.util.stream.Collectors.toList()));
        api.put("offsetWrapperPublicFields", publicFields(OffsetWrapper.class));
        api.put("offsetWrapperPublicMethods", publicMethods(OffsetWrapper.class));
        api.put("liteLagInfoConstructors", Arrays.stream(LiteLagInfo.class.getConstructors())
            .map(java.lang.reflect.Constructor::toGenericString).sorted()
            .collect(java.util.stream.Collectors.toList()));
        api.put("liteLagInfoPublicFields", publicFields(LiteLagInfo.class));
        api.put("liteLagInfoPublicMethods", publicMethods(LiteLagInfo.class));

        GetLiteGroupInfoResponseBody defaults = new GetLiteGroupInfoResponseBody();
        Map<String, Object> responseDefaults = new TreeMap<>();
        responseDefaults.put("group", defaults.getGroup());
        responseDefaults.put("parentTopic", defaults.getParentTopic());
        responseDefaults.put("liteTopic", defaults.getLiteTopic());
        responseDefaults.put("earliestUnconsumedTimestamp", defaults.getEarliestUnconsumedTimestamp());
        responseDefaults.put("totalLagCount", defaults.getTotalLagCount());
        responseDefaults.put("offsetWrapperNull", defaults.getLiteTopicOffsetWrapper() == null);
        responseDefaults.put("lagCountTopKNull", defaults.getLagCountTopK() == null);
        responseDefaults.put("lagTimestampTopKNull", defaults.getLagTimestampTopK() == null);
        api.put("responseBodyDefaults", responseDefaults);
        LiteLagInfo lagDefaults = new LiteLagInfo();
        Map<String, Object> lagInfoDefaults = new TreeMap<>();
        lagInfoDefaults.put("liteTopic", lagDefaults.getLiteTopic());
        lagInfoDefaults.put("lagCount", lagDefaults.getLagCount());
        lagInfoDefaults.put("earliestUnconsumedTimestamp", lagDefaults.getEarliestUnconsumedTimestamp());
        lagInfoDefaults.put("consumerOffset", lagDefaults.getConsumerOffset());
        api.put("liteLagInfoDefaults", lagInfoDefaults);
        lagDefaults.setConsumerOffset(23);
        api.put("consumerOffsetSerialized",
            JSON.parseObject(JSON.toJSONString(lagDefaults)).containsKey("consumerOffset"));

        List<LiteLagInfo> assignedCount = new ArrayList<>();
        defaults.setLagCountTopK(assignedCount);
        api.put("lagCountGetterReturnsAssignedList", defaults.getLagCountTopK() == assignedCount);
        defaults.getLagCountTopK().add(new LiteLagInfo());
        api.put("lagCountGetterMutationVisible", assignedCount.size() == 1);
        List<LiteLagInfo> assignedTimestamp = new ArrayList<>();
        defaults.setLagTimestampTopK(assignedTimestamp);
        api.put("lagTimestampGetterReturnsAssignedList", defaults.getLagTimestampTopK() == assignedTimestamp);
        defaults.getLagTimestampTopK().add(null);
        api.put("lagTimestampGetterMutationVisible", assignedTimestamp.size() == 1);
        OffsetWrapper assignedOffset = new OffsetWrapper();
        defaults.setLiteTopicOffsetWrapper(assignedOffset);
        api.put("offsetGetterReturnsAssignedObject", defaults.getLiteTopicOffsetWrapper() == assignedOffset);
        assignedOffset.setBrokerOffset(12);
        api.put("offsetMutationVisible", defaults.getLiteTopicOffsetWrapper().getBrokerOffset() == 12);
        return api;
    }

    private static void recordLiteTopicInfo(Path dir, boolean verify, Capture capture, String name,
        String entrypoint, String outcome) throws Exception {
        check("invoke".equals(capture.mode) && capture.request != null &&
            capture.request.getCode() == RequestCode.GET_LITE_TOPIC_INFO, name + " dispatch");
        Map<String, Object> fixture = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("entrypoint", entrypoint);
        provenance.put("transport", "mock RemotingClient");
        provenance.put("serverBaselineContacted", false);
        fixture.put("provenance", provenance);
        fixture.put("case", name);
        fixture.put("mode", "ASYNC");
        fixture.put("liteTopicInfoApi", liteTopicInfoApi());
        fixture.put("request", command(capture.request));
        fixture.put("response", command(capture.response));
        fixture.put("outcome", outcome);
        Path file = dir.resolve(name + ".json");
        String json = JSON.toJSONString(fixture) + "\n";
        if (verify) check(Files.exists(file) && json.equals(Files.readString(file)), "fixture drift: " + file);
        else {
            Files.createDirectories(dir);
            Files.writeString(file, json);
        }
    }

    private static void recordExtensionFuture(Path dir, boolean verify, Capture capture, String name,
        String entrypoint, String mode, String outcome) throws Exception {
        String expectedRemotingMode = "ASYNC_CALLBACK".equals(mode) ? "invokeAsync" :
            "ONEWAY".equals(mode) ? "invokeOneway" : "invoke";
        check(expectedRemotingMode.equals(capture.mode), name + " remoting mode");
        Map<String, Object> fixture = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("entrypoint", entrypoint);
        provenance.put("transport", "mock RemotingClient");
        provenance.put("serverBaselineContacted", false);
        fixture.put("provenance", provenance);
        fixture.put("case", name);
        fixture.put("mode", mode);
        fixture.put("request", capture.request == null ? null : futureCommand(capture.request));
        fixture.put("response", capture.response == null ? null : futureCommand(capture.response));
        fixture.put("outcome", outcome);
        Path file = dir.resolve(name + ".json");
        String json = JSON.toJSONString(fixture) + "\n";
        if (verify) check(Files.exists(file) && json.equals(Files.readString(file)), "fixture drift: " + file);
        else {
            Files.createDirectories(dir);
            Files.writeString(file, json);
        }
    }

    private static void recordNameServerUpdate(Path dir, boolean verify, Capture capture, String name,
        String input, boolean updated) throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("entrypoint", "MQClientAPIExt.updateNameServerAddressList");
        provenance.put("transport", "mock RemotingClient");
        provenance.put("serverBaselineContacted", false);
        fixture.put("provenance", provenance);
        fixture.put("case", name);
        fixture.put("mode", "LOCAL");
        fixture.put("inputNamesrvAddr", input);
        fixture.put("returnValue", updated);
        fixture.put("resultingAddresses", capture.nameServerAddresses);
        Path file = dir.resolve(name + ".json");
        String json = JSON.toJSONString(fixture) + "\n";
        if (verify) check(Files.exists(file) && json.equals(Files.readString(file)), "fixture drift: " + file);
        else {
            Files.createDirectories(dir);
            Files.writeString(file, json);
        }
    }

    public static void main(String[] args) throws Exception {
        check(args.length > 0, "expected output directory");
        Path dir = Path.of(args[0]);
        boolean verify = Arrays.asList(args).contains("--verify");
        Capture c = new Capture();
        ClientConfig config = new ClientConfig();
        config.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(new NettyClientConfig(), null, null, config,
            null, nettyConfig -> c.proxy());
        HeartbeatData heartbeat = new HeartbeatData();
        heartbeat.setClientID("client");
        c.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        c.response.setVersion(551);
        check(api.sendHeartbeat("broker:10911", heartbeat, 10000) == 551, "heartbeat version");
        record(dir, verify, c, "heartbeat-success", RequestCode.HEART_BEAT, "version=551");
        for (int code : new int[] {1, 987654}) {
            c.response = RemotingCommand.createResponseCommand(code, "offline");
            try {
                api.sendHeartbeat("broker:10911", heartbeat, 10000);
                throw new IllegalStateException("heartbeat accepted error");
            } catch (MQBrokerException expected) {
                check(expected.getResponseCode() == code, "heartbeat error code");
            }
            record(dir, verify, c, code == 1 ? "heartbeat-error" : "heartbeat-unknown",
                RequestCode.HEART_BEAT, "MQBrokerException;code=" + code);
        }
        c.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        c.response.setVersion(551);
        c.response.addExtField(MixAll.IS_SUB_CHANGE, "true");
        c.response.addExtField(MixAll.IS_SUPPORT_HEART_BEAT_V2, "true");
        HeartbeatV2Result v2 = api.sendHeartbeatV2("broker:10911", heartbeat, 10000);
        check(v2.getVersion() == 551 && v2.isSubChange() && v2.isSupportV2(), "v2 fields");
        record(dir, verify, c, "heartbeat-v2-success", RequestCode.HEART_BEAT, "version=551;sub=true;support=true");
        c.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        v2 = api.sendHeartbeatV2("broker:10911", heartbeat, 10000);
        check(v2.getVersion() == c.response.getVersion() && !v2.isSubChange() && !v2.isSupportV2(), "v2 defaults");
        record(dir, verify, c, "heartbeat-v2-default", RequestCode.HEART_BEAT,
            "version=" + v2.getVersion() + ";sub=false;support=false");
        for (int code : new int[] {1, 987654}) {
            c.response = RemotingCommand.createResponseCommand(code, "offline");
            try {
                api.sendHeartbeatV2("broker:10911", heartbeat, 10000);
                throw new IllegalStateException("v2 accepted error");
            } catch (MQBrokerException expected) {
                check(expected.getResponseCode() == code, "v2 error code");
            }
            record(dir, verify, c, code == 1 ? "heartbeat-v2-error" : "heartbeat-v2-unknown",
                RequestCode.HEART_BEAT, "MQBrokerException;code=" + code);
        }
        for (int code : new int[] {0, 1, 987654}) {
            c.response = RemotingCommand.createResponseCommand(code, "offline");
            if (code == 0) api.unregisterClient("broker:10911", "client", "producer", "consumer", 10000);
            else try {
                api.unregisterClient("broker:10911", "client", "producer", "consumer", 10000);
                throw new IllegalStateException("unregister accepted error");
            } catch (MQBrokerException expected) {
                check(expected.getResponseCode() == code, "unregister error code");
            }
            record(dir, verify, c, code == 0 ? "unregister-success" : code == 1 ? "unregister-error" : "unregister-unknown",
                RequestCode.UNREGISTER_CLIENT, code == 0 ? "ok" : "MQBrokerException;code=" + code);
        }
        QueryAssignmentResponseBody assignments = new QueryAssignmentResponseBody();
        assignments.setMessageQueueAssignments(Collections.emptySet());
        c.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        c.response.setBody(assignments.encode());
        check(api.queryAssignment("broker:10911", "oracle-topic", "consumer", "client", "AVG",
            MessageModel.CLUSTERING, 10000).isEmpty(), "assignment empty");
        record(dir, verify, c, "assignment-empty", RequestCode.QUERY_ASSIGNMENT, "assignments=0");
        MessageQueueAssignment assigned = new MessageQueueAssignment();
        assigned.setMessageQueue(new MessageQueue("oracle-topic", "broker", 2));
        assigned.setMode(MessageRequestMode.POP);
        assigned.setAttachments(Collections.singletonMap("source", "oracle"));
        assignments.setMessageQueueAssignments(Collections.singleton(assigned));
        c.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        c.response.setBody(assignments.encode());
        check(api.queryAssignment("broker:10911", "oracle-topic", "consumer", "client", "AVG",
            MessageModel.CLUSTERING, 10000).contains(assigned), "assignment populated");
        record(dir, verify, c, "assignment-populated", RequestCode.QUERY_ASSIGNMENT,
            "assignments=1;topic=oracle-topic;broker=broker;queue=2;mode=POP;source=oracle");
        for (int code : new int[] {1, 987654}) {
            c.response = RemotingCommand.createResponseCommand(code, "offline");
            try {
                api.queryAssignment("broker:10911", "oracle-topic", "consumer", "client", "AVG",
                    MessageModel.CLUSTERING, 10000);
                throw new IllegalStateException("assignment accepted error");
            } catch (MQBrokerException expected) {
                check(expected.getResponseCode() == code, "assignment error code");
            }
            record(dir, verify, c, code == 1 ? "assignment-error" : "assignment-unknown",
                RequestCode.QUERY_ASSIGNMENT, "MQBrokerException;code=" + code);
        }
        c.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "no body");
        try {
            api.queryAssignment("broker:10911", "oracle-topic", "consumer", "client", "AVG",
                MessageModel.CLUSTERING, 10000);
            throw new IllegalStateException("assignment accepted absent body");
        } catch (NullPointerException expected) {
            record(dir, verify, c, "assignment-no-body", RequestCode.QUERY_ASSIGNMENT,
                "NullPointerException;malformed");
        }
        c.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "invalid JSON");
        c.response.setBody(new byte[] {'{'});
        try {
            api.queryAssignment("broker:10911", "oracle-topic", "consumer", "client", "AVG",
                MessageModel.CLUSTERING, 10000);
            throw new IllegalStateException("assignment accepted invalid JSON");
        } catch (com.alibaba.fastjson2.JSONException expected) {
            record(dir, verify, c, "assignment-invalid-json", RequestCode.QUERY_ASSIGNMENT,
                "JSONException;malformed");
        }
        for (int code : new int[] {0, 1, 987654}) {
            c.response = RemotingCommand.createResponseCommand(code, "offline");
            if (code == 0) api.setMessageRequestMode("broker:10911", "oracle-topic", "consumer",
                MessageRequestMode.POP, 2, 10000);
            else try {
                api.setMessageRequestMode("broker:10911", "oracle-topic", "consumer",
                    MessageRequestMode.POP, 2, 10000);
                throw new IllegalStateException("request mode accepted error");
            } catch (MQClientException expected) {
                check(expected.getResponseCode() == code, "request mode error code");
            }
            record(dir, verify, c, code == 0 ? "request-mode-success" : code == 1 ? "request-mode-error" : "request-mode-unknown",
                RequestCode.SET_MESSAGE_REQUEST_MODE, code == 0 ? "ok" : "MQClientException;code=" + code);
        }

        for (String name : new String[] {"notification-stats-success", "notification-bool-success",
            "notification-error", "notification-unknown", "notification-defaults"}) {
            Capture notification = new Capture();
            ClientConfig notificationConfig = new ClientConfig();
            notificationConfig.setVipChannelEnabled(false);
            MQClientAPIExt extension = new MQClientAPIExt(notificationConfig, new NettyClientConfig(),
                null, null, nettyConfig -> notification.proxy());
            NotificationRequestHeader header = new NotificationRequestHeader();
            header.setConsumerGroup("workers");
            header.setTopic("orders");
            header.setQueueId(2);
            header.setPollTime(3000);
            header.setBornTime(1000);
            header.setOrder(true);
            header.setAttemptId("attempt-1");
            header.setLiteConsumer(true);
            header.setClientId("client");
            header.setExpType("TAG");
            header.setExp("red || blue");
            switch (name) {
                case "notification-stats-success":
                    notification.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                    notification.response.addExtField("hasMsg", "true");
                    notification.response.addExtField("pollingFull", "true");
                    break;
                case "notification-bool-success":
                    notification.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                    notification.response.addExtField("hasMsg", "false");
                    notification.response.addExtField("pollingFull", "true");
                    break;
                case "notification-defaults":
                    notification.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                    notification.response.addExtField("hasMsg", "not-a-boolean");
                    break;
                case "notification-error":
                    notification.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
                    break;
                default:
                    notification.response = RemotingCommand.createResponseCommand(987654, "offline");
                    break;
            }
            String outcome;
            try {
                if ("notification-bool-success".equals(name)) {
                    outcome = "hasMsg=" + extension.notification("broker:10911", header, 10000).join();
                } else {
                    NotifyResult result = extension.notificationWithPollingStats("broker:10911", header, 10000).join();
                    outcome = "hasMsg=" + result.isHasMsg() + ";pollingFull=" + result.isPollingFull() +
                        ";toString=" + result;
                }
            } catch (CompletionException failure) {
                Throwable cause = failure.getCause();
                check(cause instanceof MQBrokerException, name + " exception category");
                outcome = "MQBrokerException;code=" + ((MQBrokerException) cause).getResponseCode();
            }
            recordNotification(dir, verify, notification, name,
                "notification-bool-success".equals(name) ? "MQClientAPIExt.notification" :
                    "MQClientAPIExt.notificationWithPollingStats", outcome);
        }

        for (String name : new String[] {"lite-topic-info-success", "lite-topic-info-defaults",
            "lite-topic-info-null-body", "lite-topic-info-null-inputs", "lite-topic-info-error",
            "lite-topic-info-unknown", "lite-topic-info-invalid-body"}) {
            Capture lite = new Capture();
            ClientConfig liteConfig = new ClientConfig();
            liteConfig.setVipChannelEnabled(false);
            MQClientAPIExt extension = new MQClientAPIExt(liteConfig, new NettyClientConfig(),
                null, null, nettyConfig -> lite.proxy());
            String parentTopic = "lite-parent";
            String liteTopic = "%LMQ%lite-parent%orders";
            if (name.equals("lite-topic-info-success")) {
                GetLiteTopicInfoResponseBody body = new GetLiteTopicInfoResponseBody();
                body.setParentTopic(parentTopic);
                body.setLiteTopic(liteTopic);
                Set<ClientGroup> subscribers = new HashSet<>();
                subscribers.add(new ClientGroup("client-1", "workers"));
                subscribers.add(new ClientGroup("client-1", "workers"));
                subscribers.add(new ClientGroup(null, null));
                subscribers.add(null);
                body.setSubscriber(subscribers);
                TopicOffset offset = new TopicOffset();
                offset.setMinOffset(-2);
                offset.setMaxOffset(17);
                offset.setLastUpdateTimestamp(1234);
                body.setTopicOffset(offset);
                body.setShardingToBroker(true);
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                lite.response.setBody(body.encode());
            } else if (name.equals("lite-topic-info-defaults")) {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                lite.response.setBody(new GetLiteTopicInfoResponseBody().encode());
            } else if (name.equals("lite-topic-info-null-body")) {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
            } else if (name.equals("lite-topic-info-error")) {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
            } else if (name.equals("lite-topic-info-unknown")) {
                lite.response = RemotingCommand.createResponseCommand(987654, "offline");
            } else if (name.equals("lite-topic-info-invalid-body")) {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                lite.response.setBody(new byte[] {'{'});
            } else {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                lite.response.setBody(new GetLiteTopicInfoResponseBody().encode());
            }
            String requestParent = name.equals("lite-topic-info-null-inputs") ? null : parentTopic;
            String requestLite = name.equals("lite-topic-info-null-inputs") ? null : liteTopic;
            String outcome;
            try {
                GetLiteTopicInfoResponseBody result = extension
                    .getLiteTopicInfoAsync("broker:10911", requestParent, requestLite, 10000).join();
                if (result == null) outcome = "null";
                else if (name.equals("lite-topic-info-success")) {
                    outcome = "parent=" + result.getParentTopic() + ";lite=" + result.getLiteTopic() +
                        ";subscribers=" + result.getSubscriber().size() + ";min=" +
                        result.getTopicOffset().getMinOffset() + ";max=" + result.getTopicOffset().getMaxOffset() +
                        ";updated=" + result.getTopicOffset().getLastUpdateTimestamp() +
                        ";sharding=" + result.isShardingToBroker();
                } else {
                    outcome = "parent=" + result.getParentTopic() + ";lite=" + result.getLiteTopic() +
                        ";subscriberNull=" + (result.getSubscriber() == null) +
                        ";offsetNull=" + (result.getTopicOffset() == null) +
                        ";sharding=" + result.isShardingToBroker();
                }
            } catch (CompletionException failure) {
                Throwable cause = failure.getCause();
                if (cause instanceof MQBrokerException) {
                    outcome = "MQBrokerException;code=" + ((MQBrokerException) cause).getResponseCode();
                } else {
                    outcome = cause.getClass().getSimpleName() + ";malformed";
                }
            }
            recordLiteTopicInfo(dir, verify, lite, name, "MQClientAPIExt.getLiteTopicInfoAsync", outcome);
        }

        for (String name : new String[] {"lite-broker-info-success", "lite-broker-info-defaults",
            "lite-broker-info-null-body", "lite-broker-info-error", "lite-broker-info-unknown",
            "lite-broker-info-invalid-body"}) {
            Capture lite = new Capture();
            ClientConfig liteConfig = new ClientConfig();
            liteConfig.setVipChannelEnabled(false);
            MQClientAPIImpl liteApi = new MQClientAPIImpl(new NettyClientConfig(), null, null, liteConfig,
                null, nettyConfig -> lite.proxy());
            if (name.equals("lite-broker-info-success")) {
                GetBrokerLiteInfoResponseBody body = new GetBrokerLiteInfoResponseBody();
                body.setStoreType("rocksdb");
                body.setMaxLmqNum(20);
                body.setCurrentLmqNum(3);
                body.setLiteSubscriptionCount(2);
                body.setOrderInfoCount(4);
                body.setCqTableSize(5);
                body.setOffsetTableSize(6);
                body.setEventMapSize(7);
                Map<String, Integer> topicMeta = new TreeMap<>();
                topicMeta.put("orders", 8);
                topicMeta.put("payments", 9);
                body.setTopicMeta(topicMeta);
                Map<String, Set<String>> groupMeta = new TreeMap<>();
                groupMeta.put("workers", new java.util.TreeSet<>(Set.of("orders", "payments")));
                body.setGroupMeta(groupMeta);
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                lite.response.setBody(body.encode());
            } else if (name.equals("lite-broker-info-defaults")) {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                lite.response.setBody(new GetBrokerLiteInfoResponseBody().encode());
            } else if (name.equals("lite-broker-info-null-body")) {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
            } else if (name.equals("lite-broker-info-error")) {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
            } else if (name.equals("lite-broker-info-invalid-body")) {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                lite.response.setBody(new byte[] {'{'});
            } else {
                lite.response = RemotingCommand.createResponseCommand(987654, "offline");
            }
            String outcome;
            try {
                GetBrokerLiteInfoResponseBody result = liteApi.getBrokerLiteInfo("broker:10911", 10000);
                if (result == null) {
                    outcome = "null";
                } else if (name.equals("lite-broker-info-success")) {
                    outcome = "store=" + result.getStoreType() + ";max=" + result.getMaxLmqNum() +
                        ";current=" + result.getCurrentLmqNum() + ";subscriptions=" +
                        result.getLiteSubscriptionCount() + ";order=" + result.getOrderInfoCount() +
                        ";cq=" + result.getCqTableSize() + ";offset=" + result.getOffsetTableSize() +
                        ";events=" + result.getEventMapSize() + ";topics=" + result.getTopicMeta().size() +
                        ";groups=" + result.getGroupMeta().size();
                } else {
                    outcome = "store=" + result.getStoreType() + ";max=" + result.getMaxLmqNum() +
                        ";current=" + result.getCurrentLmqNum() + ";topicMetaNull=" +
                        (result.getTopicMeta() == null) + ";groupMetaNull=" + (result.getGroupMeta() == null);
                }
            } catch (MQBrokerException failure) {
                outcome = "MQBrokerException;code=" + failure.getResponseCode();
            } catch (com.alibaba.fastjson2.JSONException failure) {
                outcome = "JSONException;malformed";
            } finally {
                liteApi.shutdown();
            }
            record(dir, verify, lite, name, RequestCode.GET_BROKER_LITE_INFO, outcome);
        }

        for (String name : new String[] {"parent-topic-info-success", "parent-topic-info-defaults",
            "parent-topic-info-null-body", "parent-topic-info-null-topic", "parent-topic-info-error",
            "parent-topic-info-unknown", "parent-topic-info-invalid-body"}) {
            Capture parent = new Capture();
            ClientConfig parentConfig = new ClientConfig();
            parentConfig.setVipChannelEnabled(false);
            MQClientAPIImpl parentApi = new MQClientAPIImpl(new NettyClientConfig(), null, null, parentConfig,
                null, nettyConfig -> parent.proxy());
            if (name.equals("parent-topic-info-success")) {
                GetParentTopicInfoResponseBody body = new GetParentTopicInfoResponseBody();
                body.setTopic("lite-parent");
                body.setTtl(3600);
                body.setGroups(new java.util.TreeSet<>(Arrays.asList("billing", "workers")));
                body.setLmqNum(3);
                body.setLiteTopicCount(12);
                parent.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                parent.response.setBody(body.encode());
            } else if (name.equals("parent-topic-info-defaults")) {
                parent.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                parent.response.setBody(new GetParentTopicInfoResponseBody().encode());
            } else if (name.equals("parent-topic-info-null-body")) {
                parent.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
            } else if (name.equals("parent-topic-info-error")) {
                parent.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
            } else if (name.equals("parent-topic-info-invalid-body")) {
                parent.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                parent.response.setBody(new byte[] {'{'});
            } else {
                parent.response = RemotingCommand.createResponseCommand(987654, "offline");
            }
            String topic = name.equals("parent-topic-info-null-topic") ? null : "lite-parent";
            String outcome;
            try {
                GetParentTopicInfoResponseBody result = parentApi.getParentTopicInfo("broker:10911", topic, 10000);
                if (result == null) {
                    outcome = "null";
                } else if (name.equals("parent-topic-info-success")) {
                    outcome = "topic=" + result.getTopic() + ";ttl=" + result.getTtl() + ";groups=" +
                        result.getGroups().size() + ";lmq=" + result.getLmqNum() + ";liteTopics=" +
                        result.getLiteTopicCount();
                } else {
                    outcome = "topic=" + result.getTopic() + ";ttl=" + result.getTtl() + ";groupsNull=" +
                        (result.getGroups() == null) + ";lmq=" + result.getLmqNum() + ";liteTopics=" +
                        result.getLiteTopicCount();
                }
            } catch (MQBrokerException failure) {
                outcome = "MQBrokerException;code=" + failure.getResponseCode();
            } catch (com.alibaba.fastjson2.JSONException failure) {
                outcome = "JSONException;malformed";
            } finally {
                parentApi.shutdown();
            }
            record(dir, verify, parent, name, RequestCode.GET_PARENT_TOPIC_INFO, outcome);
        }

        for (String name : new String[] {"lite-client-info-success", "lite-client-info-defaults",
            "lite-client-info-null-body", "lite-client-info-null-inputs", "lite-client-info-error",
            "lite-client-info-unknown", "lite-client-info-invalid-body"}) {
            Capture lite = new Capture();
            ClientConfig liteConfig = new ClientConfig();
            liteConfig.setVipChannelEnabled(false);
            MQClientAPIImpl liteApi = new MQClientAPIImpl(new NettyClientConfig(), null, null, liteConfig,
                null, nettyConfig -> lite.proxy());
            if (name.equals("lite-client-info-success")) {
                GetLiteClientInfoResponseBody body = new GetLiteClientInfoResponseBody();
                body.setParentTopic("lite-parent");
                body.setGroup("workers");
                body.setClientId("client-a");
                body.setLastAccessTime(1000);
                body.setLastConsumeTime(500);
                body.setLiteTopicCount(3);
                body.setLiteTopicSet(new java.util.LinkedHashSet<>(Arrays.asList(
                    "%LMQ%lite-parent%orders", null, "%LMQ%lite-parent%payments")));
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                lite.response.setBody(body.encode());
            } else if (name.equals("lite-client-info-defaults")) {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                lite.response.setBody(new GetLiteClientInfoResponseBody().encode());
            } else if (name.equals("lite-client-info-null-body")) {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
            } else if (name.equals("lite-client-info-error")) {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
            } else if (name.equals("lite-client-info-unknown")) {
                lite.response = RemotingCommand.createResponseCommand(987654, "offline");
            } else if (name.equals("lite-client-info-invalid-body")) {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                lite.response.setBody(new byte[] {'{'});
            } else {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                lite.response.setBody(new GetLiteClientInfoResponseBody().encode());
            }
            String parentTopic = name.equals("lite-client-info-null-inputs") ? null : "lite-parent";
            String group = name.equals("lite-client-info-null-inputs") ? null : "workers";
            String clientId = name.equals("lite-client-info-null-inputs") ? null : "client-a";
            String outcome;
            try {
                GetLiteClientInfoResponseBody result = liteApi.getLiteClientInfo(
                    "broker:10911", parentTopic, group, clientId, 10000);
                if (result == null) {
                    outcome = "null";
                } else if (name.equals("lite-client-info-success")) {
                    outcome = "parent=" + result.getParentTopic() + ";group=" + result.getGroup() +
                        ";client=" + result.getClientId() + ";lastAccess=" + result.getLastAccessTime() +
                        ";lastConsume=" + result.getLastConsumeTime() + ";liteTopics=" +
                        result.getLiteTopicCount() + ";topicSet=" + result.getLiteTopicSet().size() +
                        ";containsNull=" + result.getLiteTopicSet().contains(null);
                } else {
                    outcome = "parent=" + result.getParentTopic() + ";group=" + result.getGroup() +
                        ";client=" + result.getClientId() + ";lastAccess=" + result.getLastAccessTime() +
                        ";lastConsume=" + result.getLastConsumeTime() + ";liteTopics=" +
                        result.getLiteTopicCount() + ";topicSetNull=" + (result.getLiteTopicSet() == null);
                }
            } catch (MQBrokerException failure) {
                outcome = "MQBrokerException;code=" + failure.getResponseCode();
            } catch (com.alibaba.fastjson2.JSONException failure) {
                outcome = "JSONException;malformed";
            } finally {
                liteApi.shutdown();
            }
            record(dir, verify, lite, name, RequestCode.GET_LITE_CLIENT_INFO, outcome);
        }

        for (String name : new String[] {"lite-group-info-success", "lite-group-info-defaults",
            "lite-group-info-null-body", "lite-group-info-null-inputs", "lite-group-info-error",
            "lite-group-info-unknown", "lite-group-info-invalid-body"}) {
            Capture lite = new Capture();
            ClientConfig liteConfig = new ClientConfig();
            liteConfig.setVipChannelEnabled(false);
            MQClientAPIImpl liteApi = new MQClientAPIImpl(new NettyClientConfig(), null, null, liteConfig,
                null, nettyConfig -> lite.proxy());
            if (name.equals("lite-group-info-success")) {
                GetLiteGroupInfoResponseBody body = new GetLiteGroupInfoResponseBody();
                body.setGroup("workers");
                body.setParentTopic("lite-parent");
                body.setLiteTopic("%LMQ%lite-parent%orders");
                body.setEarliestUnconsumedTimestamp(1400);
                body.setTotalLagCount(40);
                OffsetWrapper wrapper = new OffsetWrapper();
                wrapper.setBrokerOffset(100);
                wrapper.setConsumerOffset(80);
                wrapper.setPullOffset(85);
                wrapper.setLastTimestamp(1700);
                body.setLiteTopicOffsetWrapper(wrapper);
                LiteLagInfo orders = new LiteLagInfo();
                orders.setLiteTopic("%LMQ%lite-parent%orders");
                orders.setLagCount(30);
                orders.setEarliestUnconsumedTimestamp(1600);
                orders.setConsumerOffset(70);
                LiteLagInfo payments = new LiteLagInfo();
                payments.setLiteTopic("%LMQ%lite-parent%payments");
                payments.setLagCount(10);
                payments.setEarliestUnconsumedTimestamp(1500);
                payments.setConsumerOffset(90);
                body.setLagCountTopK(new ArrayList<>(Arrays.asList(orders, null, payments)));
                body.setLagTimestampTopK(new ArrayList<>(Arrays.asList(payments, orders)));
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                lite.response.setBody(body.encode());
            } else if (name.equals("lite-group-info-defaults")) {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                lite.response.setBody(new GetLiteGroupInfoResponseBody().encode());
            } else if (name.equals("lite-group-info-null-body")) {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
            } else if (name.equals("lite-group-info-error")) {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
            } else if (name.equals("lite-group-info-unknown")) {
                lite.response = RemotingCommand.createResponseCommand(987654, "offline");
            } else if (name.equals("lite-group-info-invalid-body")) {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                lite.response.setBody(new byte[] {'{'});
            } else {
                lite.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                lite.response.setBody(new GetLiteGroupInfoResponseBody().encode());
            }
            String group = name.equals("lite-group-info-null-inputs") ? null : "workers";
            String liteTopic = name.equals("lite-group-info-null-inputs") ? null :
                "%LMQ%lite-parent%orders";
            int topK = name.equals("lite-group-info-null-inputs") ? -3 : 5;
            String outcome;
            try {
                GetLiteGroupInfoResponseBody result = liteApi.getLiteGroupInfo(
                    "broker:10911", group, liteTopic, topK, 10000);
                if (result == null) {
                    outcome = "null";
                } else if (name.equals("lite-group-info-success")) {
                    OffsetWrapper wrapper = result.getLiteTopicOffsetWrapper();
                    List<LiteLagInfo> countTopK = result.getLagCountTopK();
                    LiteLagInfo first = countTopK.get(0);
                    outcome = "group=" + result.getGroup() + ";parent=" + result.getParentTopic() +
                        ";lite=" + result.getLiteTopic() + ";earliest=" +
                        result.getEarliestUnconsumedTimestamp() + ";totalLag=" + result.getTotalLagCount() +
                        ";brokerOffset=" + wrapper.getBrokerOffset() + ";consumerOffset=" +
                        wrapper.getConsumerOffset() + ";pullOffset=" + wrapper.getPullOffset() +
                        ";lastTimestamp=" + wrapper.getLastTimestamp() + ";countTopK=" + countTopK.size() +
                        ";timestampTopK=" + result.getLagTimestampTopK().size() +
                        ";countNull=" + (countTopK.get(1) == null) + ";firstTopic=" + first.getLiteTopic() +
                        ";firstLag=" + first.getLagCount() + ";firstEarliest=" +
                        first.getEarliestUnconsumedTimestamp() + ";firstConsumerOffset=" +
                        first.getConsumerOffset();
                } else {
                    outcome = "group=" + result.getGroup() + ";parent=" + result.getParentTopic() +
                        ";lite=" + result.getLiteTopic() + ";earliest=" +
                        result.getEarliestUnconsumedTimestamp() + ";totalLag=" + result.getTotalLagCount() +
                        ";offsetNull=" + (result.getLiteTopicOffsetWrapper() == null) +
                        ";countTopKNull=" + (result.getLagCountTopK() == null) +
                        ";timestampTopKNull=" + (result.getLagTimestampTopK() == null);
                }
            } catch (MQBrokerException failure) {
                outcome = "MQBrokerException;code=" + failure.getResponseCode();
            } catch (com.alibaba.fastjson2.JSONException failure) {
                outcome = "JSONException;malformed";
            } finally {
                liteApi.shutdown();
            }
            record(dir, verify, lite, name, RequestCode.GET_LITE_GROUP_INFO, outcome);
        }

        HeartbeatData extHeartbeat = new HeartbeatData();
        extHeartbeat.setClientID("client");
        for (String name : new String[] {"heartbeat-future-success", "heartbeat-future-error",
            "heartbeat-future-unknown"}) {
            Capture capture = new Capture();
            ClientConfig extensionConfig = new ClientConfig();
            extensionConfig.setVipChannelEnabled(false);
            MQClientAPIExt extension = new MQClientAPIExt(extensionConfig, new NettyClientConfig(),
                null, null, nettyConfig -> capture.proxy());
            int code = name.endsWith("success") ? ResponseCode.SUCCESS :
                name.endsWith("error") ? ResponseCode.SYSTEM_ERROR : 987654;
            capture.response = RemotingCommand.createResponseCommand(code, code == 0 ? null : "offline");
            capture.response.setVersion(551);
            String outcome;
            try {
                outcome = "version=" + extension.sendHeartbeatAsync("broker:10911", extHeartbeat, 10000).join();
            } catch (CompletionException failure) {
                Throwable cause = failure.getCause();
                check(cause instanceof MQBrokerException, name + " exception category");
                outcome = "MQBrokerException;code=" + ((MQBrokerException) cause).getResponseCode();
            }
            recordExtensionFuture(dir, verify, capture, name, "MQClientAPIExt.sendHeartbeatAsync", "ASYNC", outcome);
        }
        {
            String name = "heartbeat-oneway-success";
            Capture capture = new Capture();
            ClientConfig extensionConfig = new ClientConfig();
            extensionConfig.setVipChannelEnabled(false);
            MQClientAPIExt extension = new MQClientAPIExt(extensionConfig, new NettyClientConfig(),
                null, null, nettyConfig -> capture.proxy());
            extension.sendHeartbeatOneway("broker:10911", extHeartbeat, 10000).join();
            recordExtensionFuture(dir, verify, capture, name, "MQClientAPIExt.sendHeartbeatOneway", "ONEWAY", "ok");
        }

        ConsumerSendMsgBackRequestHeader sendBack = new ConsumerSendMsgBackRequestHeader();
        sendBack.setGroup("workers");
        sendBack.setBrokerName("broker");
        sendBack.setOriginTopic("orders");
        sendBack.setOffset(19L);
        sendBack.setDelayLevel(3);
        sendBack.setOriginMsgId("retry-id");
        sendBack.setMaxReconsumeTimes(5);
        for (String name : new String[] {"send-back-future-success", "send-back-future-error"}) {
            Capture capture = new Capture();
            ClientConfig extensionConfig = new ClientConfig();
            extensionConfig.setVipChannelEnabled(false);
            MQClientAPIExt extension = new MQClientAPIExt(extensionConfig, new NettyClientConfig(),
                null, null, nettyConfig -> capture.proxy());
            int code = name.endsWith("success") ? ResponseCode.SUCCESS : ResponseCode.SYSTEM_ERROR;
            capture.response = RemotingCommand.createResponseCommand(code, code == 0 ? null : "offline");
            RemotingCommand result = extension.sendMessageBackAsync("broker:10911", sendBack, 10000).join();
            recordExtensionFuture(dir, verify, capture, name, "MQClientAPIExt.sendMessageBackAsync", "ASYNC",
                "rawCode=" + result.getCode() + ";remark=" + result.getRemark());
        }

        QueryConsumerOffsetRequestHeader queryOffset = new QueryConsumerOffsetRequestHeader();
        queryOffset.setConsumerGroup("workers");
        queryOffset.setTopic("orders");
        queryOffset.setQueueId(2);
        for (String name : new String[] {"query-offset-future-success", "query-offset-future-null-offset",
            "query-offset-future-not-found"}) {
            Capture capture = new Capture();
            ClientConfig extensionConfig = new ClientConfig();
            extensionConfig.setVipChannelEnabled(false);
            MQClientAPIExt extension = new MQClientAPIExt(extensionConfig, new NettyClientConfig(),
                null, null, nettyConfig -> capture.proxy());
            int code = name.endsWith("not-found") ? ResponseCode.QUERY_NOT_FOUND : ResponseCode.SUCCESS;
            capture.response = RemotingCommand.createResponseCommand(code, code == 0 ? null : "missing");
            if (code == 0 && !name.endsWith("null-offset")) capture.response.addExtField("offset", "17");
            String outcome;
            try {
                Long offset = extension.queryConsumerOffsetWithFuture("broker:10911", queryOffset, 10000).join();
                outcome = "offset=" + (offset == null ? "null" : offset);
            } catch (CompletionException failure) {
                Throwable cause = failure.getCause();
                check(cause instanceof MQBrokerException, name + " exception category");
                outcome = cause.getClass().getSimpleName() + ";code=" + ((MQBrokerException) cause).getResponseCode();
            }
            recordExtensionFuture(dir, verify, capture, name, "MQClientAPIExt.queryConsumerOffsetWithFuture", "ASYNC", outcome);
        }

        UpdateConsumerOffsetRequestHeader updateOffset = new UpdateConsumerOffsetRequestHeader();
        updateOffset.setConsumerGroup("workers");
        updateOffset.setTopic("orders");
        updateOffset.setQueueId(2);
        updateOffset.setCommitOffset(17L);
        for (String name : new String[] {"update-offset-future-success", "update-offset-future-error",
            "update-offset-future-unknown"}) {
            Capture capture = new Capture();
            ClientConfig extensionConfig = new ClientConfig();
            extensionConfig.setVipChannelEnabled(false);
            MQClientAPIExt extension = new MQClientAPIExt(extensionConfig, new NettyClientConfig(),
                null, null, nettyConfig -> capture.proxy());
            int code = name.endsWith("success") ? ResponseCode.SUCCESS :
                name.endsWith("error") ? ResponseCode.SYSTEM_ERROR : 987654;
            capture.response = RemotingCommand.createResponseCommand(code, code == 0 ? null : "offline");
            java.util.concurrent.CompletableFuture<Void> future = extension.updateConsumerOffsetAsync(
                "broker:10911", updateOffset, 10000);
            String outcome;
            try {
                future.join();
                outcome = "ok";
            } catch (CompletionException failure) {
                Throwable cause = failure.getCause();
                check(cause instanceof MQBrokerException, name + " exception category");
                outcome = "MQBrokerException;code=" + ((MQBrokerException) cause).getResponseCode();
            }
            recordExtensionFuture(dir, verify, capture, name, "MQClientAPIExt.updateConsumerOffsetAsync", "ASYNC", outcome);
        }
        {
            String name = "update-offset-oneway-success";
            Capture capture = new Capture();
            ClientConfig extensionConfig = new ClientConfig();
            extensionConfig.setVipChannelEnabled(false);
            MQClientAPIExt extension = new MQClientAPIExt(extensionConfig, new NettyClientConfig(),
                null, null, nettyConfig -> capture.proxy());
            extension.updateConsumerOffsetOneWay("broker:10911", updateOffset, 10000).join();
            recordExtensionFuture(dir, verify, capture, name, "MQClientAPIExt.updateConsumerOffsetOneWay", "ONEWAY", "ok");
        }

        GetConsumerListByGroupRequestHeader consumerListHeader = new GetConsumerListByGroupRequestHeader();
        consumerListHeader.setConsumerGroup("workers");
        for (String name : new String[] {"consumer-list-future-populated", "consumer-list-future-empty",
            "consumer-list-future-success-empty-body", "consumer-list-future-null-list",
            "consumer-list-future-error"}) {
            Capture capture = new Capture();
            ClientConfig extensionConfig = new ClientConfig();
            extensionConfig.setVipChannelEnabled(false);
            MQClientAPIExt extension = new MQClientAPIExt(extensionConfig, new NettyClientConfig(),
                null, null, nettyConfig -> capture.proxy());
            if (name.endsWith("populated")) {
                GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
                body.setConsumerIdList(Arrays.asList("client-a", "client-b"));
                capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                capture.response.setBody(body.encode());
            } else if (name.endsWith("empty")) {
                capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "no consumer");
            } else if (name.endsWith("success-empty-body")) {
                capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
            } else if (name.endsWith("null-list")) {
                GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
                body.setConsumerIdList(null);
                capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                capture.response.setBody(body.encode());
            } else {
                capture.response = RemotingCommand.createResponseCommand(987654, "offline");
            }
            String outcome;
            try {
                List<String> clients = extension.getConsumerListByGroupAsync(
                    "broker:10911", consumerListHeader, 10000).join();
                outcome = "clients=" + (clients == null ? "null" : String.join(",", clients));
            } catch (CompletionException failure) {
                Throwable cause = failure.getCause();
                check(cause instanceof MQBrokerException, name + " exception category");
                outcome = "MQBrokerException;code=" + ((MQBrokerException) cause).getResponseCode();
            }
            recordExtensionFuture(dir, verify, capture, name, "MQClientAPIExt.getConsumerListByGroupAsync", "ASYNC_CALLBACK", outcome);
        }

        GetMaxOffsetRequestHeader maxOffset = new GetMaxOffsetRequestHeader();
        maxOffset.setTopic("orders");
        maxOffset.setQueueId(2);
        maxOffset.setCommitted(true);
        GetMinOffsetRequestHeader minOffset = new GetMinOffsetRequestHeader();
        minOffset.setTopic("orders");
        minOffset.setQueueId(2);
        SearchOffsetRequestHeader searchOffset = new SearchOffsetRequestHeader();
        searchOffset.setTopic("orders");
        searchOffset.setQueueId(2);
        searchOffset.setTimestamp(1700000000000L);
        for (String name : new String[] {"max-offset-future-success", "max-offset-future-null-offset",
            "max-offset-future-error", "min-offset-future-success", "min-offset-future-null-offset",
            "min-offset-future-error", "search-offset-future-success", "search-offset-future-null-offset",
            "search-offset-future-error"}) {
            Capture capture = new Capture();
            ClientConfig extensionConfig = new ClientConfig();
            extensionConfig.setVipChannelEnabled(false);
            MQClientAPIExt extension = new MQClientAPIExt(extensionConfig, new NettyClientConfig(),
                null, null, nettyConfig -> capture.proxy());
            boolean success = name.endsWith("success") || name.endsWith("null-offset");
            capture.response = RemotingCommand.createResponseCommand(success ? ResponseCode.SUCCESS : ResponseCode.SYSTEM_ERROR,
                success ? null : "offline");
            if (success && !name.endsWith("null-offset")) capture.response.addExtField("offset", "17");
            String outcome;
            try {
                Long value = name.startsWith("max") ? extension.getMaxOffset("broker:10911", maxOffset, 10000).join() :
                    name.startsWith("min") ? extension.getMinOffset("broker:10911", minOffset, 10000).join() :
                        extension.searchOffset("broker:10911", searchOffset, 10000).join();
                outcome = "offset=" + (value == null ? "null" : value);
            } catch (CompletionException failure) {
                Throwable cause = failure.getCause();
                check(cause instanceof MQBrokerException, name + " exception category");
                outcome = "MQBrokerException;code=" + ((MQBrokerException) cause).getResponseCode();
            }
            String entrypoint = name.startsWith("max") ? "MQClientAPIExt.getMaxOffset" :
                name.startsWith("min") ? "MQClientAPIExt.getMinOffset" : "MQClientAPIExt.searchOffset";
            recordExtensionFuture(dir, verify, capture, name, entrypoint,
                name.startsWith("search") ? "ASYNC" : "ASYNC_CALLBACK", outcome);
        }

        LockBatchRequestBody lockBody = new LockBatchRequestBody();
        lockBody.setConsumerGroup("workers");
        lockBody.setClientId("client");
        lockBody.setMqSet(Collections.singleton(new MessageQueue("orders", "broker", 2)));
        for (String name : new String[] {"lock-batch-future-success", "lock-batch-future-error"}) {
            Capture capture = new Capture();
            ClientConfig extensionConfig = new ClientConfig();
            extensionConfig.setVipChannelEnabled(false);
            MQClientAPIExt extension = new MQClientAPIExt(extensionConfig, new NettyClientConfig(),
                null, null, nettyConfig -> capture.proxy());
            if (name.endsWith("success")) {
                LockBatchResponseBody responseBody = new LockBatchResponseBody();
                responseBody.setLockOKMQSet(Collections.singleton(new MessageQueue("orders", "broker", 2)));
                capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                capture.response.setBody(responseBody.encode());
            } else {
                capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
            }
            String outcome;
            try {
                Set<MessageQueue> queues = extension.lockBatchMQWithFuture("broker:10911", lockBody, 10000).join();
                outcome = "queues=" + (queues == null ? "null" : queues.size());
            } catch (CompletionException failure) {
                Throwable cause = failure.getCause();
                check(cause instanceof MQBrokerException, name + " exception category");
                outcome = "MQBrokerException;code=" + ((MQBrokerException) cause).getResponseCode();
            }
            recordExtensionFuture(dir, verify, capture, name, "MQClientAPIExt.lockBatchMQWithFuture", "ASYNC", outcome);
        }
        {
            String name = "unlock-batch-oneway-success";
            Capture capture = new Capture();
            ClientConfig extensionConfig = new ClientConfig();
            extensionConfig.setVipChannelEnabled(false);
            MQClientAPIExt extension = new MQClientAPIExt(extensionConfig, new NettyClientConfig(),
                null, null, nettyConfig -> capture.proxy());
            UnlockBatchRequestBody unlockBody = new UnlockBatchRequestBody();
            unlockBody.setConsumerGroup("workers");
            unlockBody.setClientId("client");
            unlockBody.setMqSet(Collections.singleton(new MessageQueue("orders", "broker", 2)));
            extension.unlockBatchMQOneway("broker:10911", unlockBody, 10000).join();
            recordExtensionFuture(dir, verify, capture, name, "MQClientAPIExt.unlockBatchMQOneway", "ONEWAY", "ok");
        }

        {
            String name = "invoke-raw-error-response";
            Capture capture = new Capture();
            ClientConfig extensionConfig = new ClientConfig();
            extensionConfig.setVipChannelEnabled(false);
            MQClientAPIExt extension = new MQClientAPIExt(extensionConfig, new NettyClientConfig(),
                null, null, nettyConfig -> capture.proxy());
            RemotingCommand rawRequest = RemotingCommand.createRequestCommand(987, null);
            rawRequest.setBody(new byte[] {1, 2, 3});
            capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "raw");
            RemotingCommand rawResponse = extension.invoke("broker:10911", rawRequest, 10000).join();
            recordExtensionFuture(dir, verify, capture, name, "MQClientAPIExt.invoke", "ASYNC",
                "rawCode=" + rawResponse.getCode() + ";remark=" + rawResponse.getRemark());
        }
        {
            String name = "invoke-oneway-success";
            Capture capture = new Capture();
            ClientConfig extensionConfig = new ClientConfig();
            extensionConfig.setVipChannelEnabled(false);
            MQClientAPIExt extension = new MQClientAPIExt(extensionConfig, new NettyClientConfig(),
                null, null, nettyConfig -> capture.proxy());
            RemotingCommand rawRequest = RemotingCommand.createRequestCommand(987, null);
            rawRequest.setBody(new byte[] {3, 2, 1});
            extension.invokeOneway("broker:10911", rawRequest, 10000).join();
            recordExtensionFuture(dir, verify, capture, name, "MQClientAPIExt.invokeOneway", "ONEWAY", "ok");
        }

        for (String name : new String[] {"nameserver-refresh-present", "nameserver-refresh-null",
            "nameserver-refresh-empty"}) {
            String namesrvAddr = name.endsWith("present") ? "namesrv-a:9876;namesrv-b:9876" :
                name.endsWith("empty") ? "" : null;
            Capture capture = new Capture();
            ClientConfig extensionConfig = new ClientConfig();
            extensionConfig.setVipChannelEnabled(false);
            extensionConfig.setNamesrvAddr(namesrvAddr);
            MQClientAPIExt extension = new MQClientAPIExt(extensionConfig, new NettyClientConfig(),
                null, null, nettyConfig -> capture.proxy());
            boolean updated = extension.updateNameServerAddressList();
            recordNameServerUpdate(dir, verify, capture, name, namesrvAddr, updated);
        }
    }
}
