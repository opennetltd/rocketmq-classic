package compat;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.PullAPIWrapper;
import org.apache.rocketmq.client.impl.consumer.RebalanceImpl;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/** Checks the deprecated facade accessor and its retained implementation identity. */
public final class PullImplementationAccessorFixtures {
    private PullImplementationAccessorFixtures() { }

    private static void check(boolean condition, String label) {
        if (!condition) throw new IllegalStateException(label);
    }

    private static String queueKey(MessageQueue queue) {
        return String.valueOf(queue.getTopic()) + "|" + String.valueOf(queue.getBrokerName())
            + "|" + queue.getQueueId();
    }

    private static List<String> queueKeys(Iterable<MessageQueue> queues) {
        TreeSet<String> keys = new TreeSet<>();
        for (MessageQueue queue : queues) keys.add(queueKey(queue));
        return new ArrayList<>(keys);
    }

    private static MessageExt message(String topic) {
        MessageExt message = new MessageExt();
        message.setTopic(topic);
        return message;
    }

    private static List<String> messageTopics(Iterable<MessageExt> messages) {
        List<String> topics = new ArrayList<>();
        for (MessageExt message : messages) topics.add(message.getTopic());
        return topics;
    }

    private static List<String> subscriptionRows(Iterable<SubscriptionData> subscriptions) {
        List<String> rows = new ArrayList<>();
        for (SubscriptionData subscription : subscriptions) {
            TreeSet<String> tags = new TreeSet<>(subscription.getTagsSet());
            TreeSet<Integer> codes = new TreeSet<>(subscription.getCodeSet());
            String version = subscription.getSubVersion() == 0 ? "0" : "nonzero";
            rows.add(subscription.getTopic() + "|" + subscription.getSubString() + "|"
                + subscription.getExpressionType() + "|" + subscription.isClassFilterMode() + "|"
                + version + "|" + tags + "|" + codes);
        }
        rows.sort(String::compareTo);
        return rows;
    }

    private static Map<String, String> normalizedRunningInfoProperties(
        ConsumerRunningInfo info, DefaultMQPullConsumer consumer) {
        Map<String, String> properties = new TreeMap<>();
        for (Map.Entry<Object, Object> entry : info.getProperties().entrySet()) {
            String key = String.valueOf(entry.getKey());
            Object value = entry.getValue();
            if ("namespaceInitialized".equals(key)) {
                continue;
            } else if ("defaultMQPullConsumerImpl".equals(key)) {
                properties.put(key, "<implementation>");
            } else if ("allocateMessageQueueStrategy".equals(key)) {
                properties.put(key, consumer.getAllocateMessageQueueStrategy().getName());
            } else if ("clientIP".equals(key)) {
                properties.put(key, value == null ? "null" : "<present>");
            } else if ("PROP_CONSUMER_START_TIMESTAMP".equals(key)) {
                properties.put(key, "<timestamp>");
            } else {
                properties.put(key, String.valueOf(value));
            }
        }
        List<String> topics = new ArrayList<>(consumer.getRegisterTopics());
        topics.sort(String::compareTo);
        properties.put("registerTopics", "[" + String.join(", ", topics) + "]");
        List<String> subscriptions = new ArrayList<>();
        for (SubscriptionData subscription : consumer.getRegisterSubscriptions()) {
            subscriptions.add(subscription.toString());
        }
        subscriptions.sort(String::compareTo);
        properties.put("registerSubscriptions", "[" + String.join(", ", subscriptions) + "]");
        return properties;
    }

    private static SubscriptionData tagSubscription(String topic, String tag, long version) {
        SubscriptionData subscription = new SubscriptionData();
        subscription.setTopic(topic);
        subscription.setSubString(tag);
        subscription.setSubVersion(version);
        subscription.getTagsSet().add(tag);
        subscription.getCodeSet().add(tag.hashCode());
        return subscription;
    }

    private static Map<String, Object> pullSendBackResults() throws Exception {
        String group = "pull-sendback";
        String namespacedGroup = "tenant%" + group;
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(group);
        consumer.setNamespace("tenant");
        // Pull start stores the namespaced group on the live facade.
        consumer.setConsumerGroup(namespacedGroup);
        DefaultMQPullConsumerImpl implementation = consumer.getDefaultMQPullConsumerImpl();

        RecordingMqClientInstance factory = recordingMqClientInstance();
        RecordingMqClientAPIImpl clientApi = recordingMqClientAPIImpl();
        RecordingDefaultMQProducer producer = new RecordingDefaultMQProducer("tenant");
        factory.clientApi = clientApi;
        factory.defaultMQProducer = producer;
        Field factoryField = DefaultMQPullConsumerImpl.class.getDeclaredField("mQClientFactory");
        factoryField.setAccessible(true);
        factoryField.set(implementation, factory);

        MessageExt message = message("tenant%orders");
        message.setBody(new byte[] {1});
        message.setCommitLogOffset(71);
        message.setMsgId("phase18-pull-sendback-message");
        implementation.sendMessageBack(message, 2, "broker-a");
        implementation.sendMessageBack(message, 3, "broker-a", "override-group");
        clientApi.failNextSendBack = true;
        implementation.sendMessageBack(message, 4, "broker-a", "override-group");

        Map<String, Object> result = new TreeMap<>();
        result.put("pullSendBackBrokerGroups", new ArrayList<>(clientApi.consumerGroups));
        result.put("pullSendBackFallbackRetryTopics", new ArrayList<>(producer.sentTopics));
        return result;
    }

    private static RecordingMqClientInstance recordingMqClientInstance() throws Exception {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Object unsafe = unsafeField.get(null);
        RecordingMqClientInstance factory = (RecordingMqClientInstance) unsafeClass
            .getMethod("allocateInstance", Class.class)
            .invoke(unsafe, RecordingMqClientInstance.class);
        factory.initialize();
        return factory;
    }

    private static RecordingMqClientAPIImpl recordingMqClientAPIImpl() throws Exception {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Object unsafe = unsafeField.get(null);
        RecordingMqClientAPIImpl clientApi = (RecordingMqClientAPIImpl) unsafeClass
            .getMethod("allocateInstance", Class.class)
            .invoke(unsafe, RecordingMqClientAPIImpl.class);
        clientApi.initialize();
        return clientApi;
    }

    public static void main(String[] args) throws Exception {
        check(args.length >= 1, "expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String group = "phase18-pull-implementation-accessor";
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(group);
        consumer.setNamespace("tenant");
        DefaultMQPullConsumerImpl first = consumer.getDefaultMQPullConsumerImpl();
        DefaultMQPullConsumerImpl second = consumer.getDefaultMQPullConsumerImpl();
        check(first != null, "implementation accessor non-null");
        check(first == second, "implementation accessor identity stable");
        check(group.equals(first.groupName()), "implementation retains configured group");
        String serviceStateBeforeStart = first.getServiceState().name();
        check(serviceStateBeforeStart.equals("CREATE_JUST"), "initial implementation state");
        first.setServiceState(null);
        boolean serviceStateSetterAcceptsNull = first.getServiceState() == null;
        first.setServiceState(ServiceState.CREATE_JUST);
        first.setServiceState(ServiceState.START_FAILED);
        boolean serviceStateSetterBlocksStart = false;
        try {
            first.start();
        } catch (MQClientException expectedStateFailure) {
            serviceStateSetterBlocksStart = true;
        }
        String serviceStateAfterRejectedStart = first.getServiceState().name();
        first.setServiceState(ServiceState.CREATE_JUST);
        check(first.getOffsetStore() == null, "offset store absent before start");
        check(first.getPullAPIWrapper() == null, "pull API wrapper absent before start");
        PullAPIWrapper replacementPullAPIWrapper = new PullAPIWrapper(null, group, false);
        MessageQueue wrapperQueue = new MessageQueue("orders", "broker-a", 2);
        long wrapperDefaultBrokerId = replacementPullAPIWrapper.getDefaultBrokerId();
        boolean wrapperConnectByUserBeforeMutation =
            replacementPullAPIWrapper.isConnectBrokerByUser();
        replacementPullAPIWrapper.updatePullFromWhichNode(wrapperQueue, 7L);
        long wrapperSuggestedBrokerId =
            replacementPullAPIWrapper.recalculatePullFromWhichNode(wrapperQueue);
        replacementPullAPIWrapper.setDefaultBrokerId(11L);
        replacementPullAPIWrapper.setConnectBrokerByUser(true);
        long wrapperBrokerIdAfterUserSelection =
            replacementPullAPIWrapper.recalculatePullFromWhichNode(wrapperQueue);
        boolean wrapperHasHookBeforeRegistration = replacementPullAPIWrapper.hasHook();
        ArrayList<org.apache.rocketmq.client.hook.FilterMessageHook> wrapperHooks = new ArrayList<>();
        wrapperHooks.add(new org.apache.rocketmq.client.hook.FilterMessageHook() {
            @Override
            public String hookName() {
                return "pull-api-wrapper-fixture";
            }

            @Override
            public void filterMessage(org.apache.rocketmq.client.hook.FilterMessageContext context) {
                context.setUnitMode(true);
            }
        });
        replacementPullAPIWrapper.registerFilterMessageHook(wrapperHooks);
        org.apache.rocketmq.client.hook.FilterMessageContext wrapperHookContext =
            new org.apache.rocketmq.client.hook.FilterMessageContext();
        replacementPullAPIWrapper.executeHook(wrapperHookContext);
        boolean wrapperHookMutatedContext = wrapperHookContext.isUnitMode();
        boolean wrapperHasHookAfterRegistration = replacementPullAPIWrapper.hasHook();
        boolean wrapperRandomNumNonnegative = replacementPullAPIWrapper.randomNum() >= 0;
        first.setPullAPIWrapper(replacementPullAPIWrapper);
        boolean pullAPIWrapperSetterRetainsIdentity =
            first.getPullAPIWrapper() == replacementPullAPIWrapper;
        first.setPullAPIWrapper(null);
        boolean pullAPIWrapperSetterAcceptsNull = first.getPullAPIWrapper() == null;
        RebalanceImpl rebalanceImplementation = first.getRebalanceImpl();
        check(rebalanceImplementation != null, "rebalance implementation constructed");
        boolean rebalanceImplementationIdentityStable =
            rebalanceImplementation == first.getRebalanceImpl();
        String groupNameBeforeStart = first.groupName();
        MessageModel messageModelBeforeMutation = first.messageModel();
        boolean unitModeBeforeMutation = first.isUnitMode();
        consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.setUnitMode(true);
        MessageModel messageModelAfterMutation = first.messageModel();
        boolean unitModeAfterMutation = first.isUnitMode();
        ConsumeType consumeType = first.consumeType();
        ConsumeFromWhere consumeFromWhere = first.consumeFromWhere();
        consumer.setEnableRebalance(false);
        boolean disabledTryRebalanceReturnsTrue = first.tryRebalance();
        first.doRebalance();
        consumer.setEnableRebalance(true);
        List<String> directHookEvents = new ArrayList<>();
        first.registerConsumeMessageHook(new ConsumeMessageHook() {
            @Override
            public String hookName() {
                return "pull-raw-hook-entrypoint";
            }

            @Override
            public void consumeMessageBefore(ConsumeMessageContext context) {
                directHookEvents.add("before:" + context.getStatus());
                context.setStatus("hook-before");
            }

            @Override
            public void consumeMessageAfter(ConsumeMessageContext context) {
                directHookEvents.add("after:" + context.getStatus() + ":" + context.isSuccess());
            }
        });
        ConsumeMessageContext directHookContext = new ConsumeMessageContext();
        directHookContext.setStatus("seed");
        first.executeHookBefore(directHookContext);
        directHookContext.setSuccess(true);
        first.executeHookAfter(directHookContext);
        List<String> initialSubscriptions = subscriptionRows(first.subscriptions());
        int maxReconsumeTimesDefault = consumer.getMaxReconsumeTimes();
        consumer.setMaxReconsumeTimes(-3);
        check(consumer.getMaxReconsumeTimes() == -3, "max reconsume setting mutation");
        consumer.setNamesrvAddr(null);
        consumer.setClientCallbackExecutorThreads(3);
        consumer.setConcurrentHeartbeatThreadPoolSize(8);
        ConsumerRunningInfo directRunningInfo = first.consumerRunningInfo();
        Map<String, String> directRunningInfoProperties = normalizedRunningInfoProperties(
            directRunningInfo, consumer);
        List<String> directRunningInfoSubscriptions =
            subscriptionRows(directRunningInfo.getSubscriptionSet());

        Set<MessageQueue> parseInput = new HashSet<>();
        parseInput.add(new MessageQueue("tenant%orders", "broker-a", 0));
        parseInput.add(new MessageQueue("orders", "broker-a", 0));
        parseInput.add(new MessageQueue("%RETRY%tenant%group-a", "broker-b", 1));
        parseInput.add(new MessageQueue("other%orders", "broker-c", 2));
        parseInput.add(new MessageQueue(null, "broker-d", 3));
        parseInput.add(new MessageQueue("tenant%nullable-broker", null, 4));
        Set<MessageQueue> parseOutput = first.parseSubscribeMessageQueues(parseInput);

        first.resetTopic(null);
        List<MessageExt> emptyMessages = new ArrayList<>();
        first.resetTopic(emptyMessages);
        List<MessageExt> resetInput = new ArrayList<>(List.of(
            message("tenant%orders"),
            message("orders"),
            message("other%orders"),
            message("%RETRY%tenant%group-a"),
            message("%RETRY%other%group-a"),
            message("%DLQ%tenant%group-a")
        ));
        first.resetTopic(resetInput);

        DefaultMQPullConsumer noNamespaceConsumer = new DefaultMQPullConsumer(group + "-no-namespace");
        List<MessageExt> noNamespaceMessages = new ArrayList<>(List.of(message("tenant%orders")));
        noNamespaceConsumer.getDefaultMQPullConsumerImpl().resetTopic(noNamespaceMessages);

        Map<String, SubscriptionData> subscriptionInner = first.getRebalanceImpl().getSubscriptionInner();
        String rebalanceTopic = "rebalance-topic";
        first.subscriptionAutomatically(rebalanceTopic);
        boolean unsubscribedTopicNeedsUpdate = first.isSubscribeTopicNeedUpdate("not-subscribed");
        boolean subscribedTopicNeedsUpdateBeforeInfo = first.isSubscribeTopicNeedUpdate(rebalanceTopic);
        Set<MessageQueue> rebalanceInfo = new HashSet<>();
        rebalanceInfo.add(new MessageQueue(rebalanceTopic, "broker-a", 0));
        first.updateTopicSubscribeInfo(rebalanceTopic, rebalanceInfo);
        boolean subscribeInfoStoredForSubscribedTopic = first.getRebalanceImpl()
            .getTopicSubscribeInfoTable().containsKey(rebalanceTopic);
        boolean subscribedTopicNeedsUpdateAfterInfo = first.isSubscribeTopicNeedUpdate(rebalanceTopic);
        first.updateTopicSubscribeInfo("not-subscribed", rebalanceInfo);
        boolean subscribeInfoStoredForUnsubscribedTopic = first.getRebalanceImpl()
            .getTopicSubscribeInfoTable().containsKey("not-subscribed");
        List<String> persistedQueues = new ArrayList<>();
        List<String> rawOffsetStoreCalls = new ArrayList<>();
        OffsetStore recordingOffsetStore = (OffsetStore) Proxy.newProxyInstance(
            OffsetStore.class.getClassLoader(), new Class<?>[] {OffsetStore.class},
            (proxy, method, arguments) -> {
                if ("persistAll".equals(method.getName())) {
                    for (Object queue : (Iterable<?>) arguments[0]) {
                        persistedQueues.add(queueKey((MessageQueue) queue));
                    }
                } else if ("readOffset".equals(method.getName())) {
                    MessageQueue queue = (MessageQueue) arguments[0];
                    rawOffsetStoreCalls.add("readOffset|" + queueKey(queue) + "|" + arguments[1]);
                    return 73L;
                } else if ("updateConsumeOffsetToBroker".equals(method.getName())) {
                    MessageQueue queue = (MessageQueue) arguments[0];
                    rawOffsetStoreCalls.add("updateConsumeOffsetToBroker|" + queueKey(queue) + "|"
                        + arguments[1] + "|" + arguments[2]);
                }
                return null;
            });
        MessageQueue assignedPersistQueue = new MessageQueue("assigned-topic", "broker-persist", 4);
        first.setOffsetStore(recordingOffsetStore);
        MessageQueue rawOffsetQueue = new MessageQueue("raw-offset-topic", "broker-offset", 3);
        first.updateConsumeOffsetToBroker(rawOffsetQueue, 37L, true);
        first.setServiceState(ServiceState.RUNNING);
        String serviceStateAfterSetter = first.getServiceState().name();
        long rawImplementationFetchOffset = first.fetchConsumeOffset(rawOffsetQueue, false);
        long facadeFetchOffset = consumer.fetchConsumeOffset(rawOffsetQueue, true);
        first.getRebalanceImpl().getProcessQueueTable().put(assignedPersistQueue, new ProcessQueue());
        first.persistConsumerOffset();
        first.getRebalanceImpl().getProcessQueueTable().clear();
        first.setServiceState(ServiceState.CREATE_JUST);
        first.setOffsetStore(null);
        persistedQueues.sort(String::compareTo);
        first.subscriptionAutomatically("raw-topic");
        SubscriptionData automaticSubscription = subscriptionInner.get("raw-topic");
        check(automaticSubscription != null, "automatic subscription inserted");
        check("raw-topic".equals(automaticSubscription.getTopic()), "automatic topic remains raw");
        first.subscriptionAutomatically("raw-topic");
        check(subscriptionInner.get("raw-topic") == automaticSubscription,
            "automatic subscription preserves existing entry");
        first.subscriptionAutomatically("");
        SubscriptionData emptyTopicSubscription = subscriptionInner.get("");
        check(emptyTopicSubscription != null, "empty topic subscription inserted");
        SubscriptionData existingSubscription = new SubscriptionData();
        existingSubscription.setTopic("existing-topic");
        existingSubscription.setSubString("tag-a");
        existingSubscription.setExpressionType("SQL92");
        existingSubscription.setSubVersion(7L);
        subscriptionInner.put("existing-topic", existingSubscription);
        first.subscriptionAutomatically("existing-topic");
        check(subscriptionInner.get("existing-topic") == existingSubscription,
            "automatic subscription preserves custom entry");
        first.unsubscribe("raw-topic");
        check(!subscriptionInner.containsKey("raw-topic"), "unsubscribe removes exact topic");
        first.unsubscribe("");
        check(!subscriptionInner.containsKey(""), "unsubscribe removes empty topic");
        first.unsubscribe("missing-topic");
        consumer.registerMessageQueueListener("registered-topic", null);
        String registeredTopic = "tenant%registered-topic";
        first.subscriptionAutomatically(registeredTopic);
        first.unsubscribe(registeredTopic);
        check(!subscriptionInner.containsKey(registeredTopic), "unsubscribe removes namespaced key as supplied");
        check(consumer.getRegisterTopics().contains(registeredTopic),
            "unsubscribe leaves facade topic registration unchanged");
        List<String> topicSubscriptions = subscriptionRows(first.subscriptions());
        consumer.addRegisterSubscriptions("explicit-topic", MessageSelector.byTag("red"));
        List<String> explicitSubscriptions = subscriptionRows(first.subscriptions());
        consumer.getRegisterSubscriptions().clear();
        consumer.getRegisterSubscriptions().add(tagSubscription("duplicate-topic", "red", 11));
        consumer.getRegisterSubscriptions().add(tagSubscription("duplicate-topic", "red", 12));
        List<String> duplicateVersionSubscriptions = subscriptionRows(first.subscriptions());

        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("entrypoint", "DefaultMQPullConsumer.getDefaultMQPullConsumerImpl");
        provenance.put("serverBaselineContacted", false);
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("provenance", provenance);
        fixture.put("consumerGroup", group);
        fixture.put("accessorNonNull", true);
        fixture.put("sameImplementationAcrossCalls", true);
        fixture.put("implementationGroup", first.groupName());
        fixture.put("serviceStateBeforeStart", serviceStateBeforeStart);
        fixture.put("serviceStateAfterSetter", serviceStateAfterSetter);
        fixture.put("serviceStateSetterAcceptsNull", serviceStateSetterAcceptsNull);
        fixture.put("serviceStateSetterBlocksStart", serviceStateSetterBlocksStart);
        fixture.put("serviceStateAfterRejectedStart", serviceStateAfterRejectedStart);
        fixture.put("offsetStoreIsNullBeforeStart", first.getOffsetStore() == null);
        fixture.put("pullApiWrapperIsNullBeforeStart", first.getPullAPIWrapper() == null);
        fixture.put("pullAPIWrapperSetterRetainsIdentity", pullAPIWrapperSetterRetainsIdentity);
        fixture.put("pullAPIWrapperSetterAcceptsNull", pullAPIWrapperSetterAcceptsNull);
        fixture.put("pullAPIWrapperDefaultBrokerId", wrapperDefaultBrokerId);
        fixture.put("pullAPIWrapperConnectByUserBeforeMutation", wrapperConnectByUserBeforeMutation);
        fixture.put("pullAPIWrapperSuggestedBrokerId", wrapperSuggestedBrokerId);
        fixture.put("pullAPIWrapperBrokerIdAfterUserSelection", wrapperBrokerIdAfterUserSelection);
        fixture.put("pullAPIWrapperHasHookBeforeRegistration", wrapperHasHookBeforeRegistration);
        fixture.put("pullAPIWrapperHasHookAfterRegistration", wrapperHasHookAfterRegistration);
        fixture.put("pullAPIWrapperHookMutatedContext", wrapperHookMutatedContext);
        fixture.put("pullAPIWrapperRandomNumNonnegative", wrapperRandomNumNonnegative);
        fixture.put("rebalanceImplementationPresent", first.getRebalanceImpl() != null);
        fixture.put("rebalanceImplementationClass", rebalanceImplementation.getClass().getSimpleName());
        fixture.put("rebalanceImplementationIdentityStable", rebalanceImplementationIdentityStable);
        fixture.put("maxReconsumeTimesDefault", maxReconsumeTimesDefault);
        fixture.put("maxReconsumeTimesAfterMutation", consumer.getMaxReconsumeTimes());
        fixture.put("parseSubscribeMessageQueuesInput", queueKeys(parseInput));
        fixture.put("parseSubscribeMessageQueuesOutput", queueKeys(parseOutput));
        fixture.put("resetTopicNullListNoOp", true);
        fixture.put("resetTopicEmptyListOutput", messageTopics(emptyMessages));
        fixture.put("resetTopicInput", List.of(
            "tenant%orders", "orders", "other%orders", "%RETRY%tenant%group-a",
            "%RETRY%other%group-a", "%DLQ%tenant%group-a"));
        fixture.put("resetTopicOutput", messageTopics(resetInput));
        fixture.put("resetTopicNoNamespaceInput", List.of("tenant%orders"));
        fixture.put("resetTopicNoNamespaceOutput", messageTopics(noNamespaceMessages));
        fixture.put("subscriptionAutomaticallyTopic", automaticSubscription.getTopic());
        fixture.put("subscriptionAutomaticallyInputTopic", "raw-topic");
        fixture.put("subscriptionAutomaticallyExpression", automaticSubscription.getSubString());
        fixture.put("subscriptionAutomaticallyExpressionType", automaticSubscription.getExpressionType());
        fixture.put("subscriptionAutomaticallyClassFilterMode", automaticSubscription.isClassFilterMode());
        fixture.put("subscriptionAutomaticallyVersionPositive", automaticSubscription.getSubVersion() > 0);
        fixture.put("subscriptionAutomaticallyTags", new TreeSet<>(automaticSubscription.getTagsSet()));
        fixture.put("subscriptionAutomaticallyCodes", new TreeSet<>(automaticSubscription.getCodeSet()));
        fixture.put("subscriptionAutomaticallyIdempotent", true);
        fixture.put("subscriptionAutomaticallyEmptyTopicInput", "");
        fixture.put("subscriptionAutomaticallyEmptyTopicOutput", emptyTopicSubscription.getTopic());
        fixture.put("subscriptionAutomaticallyExistingTopic", "existing-topic");
        fixture.put("subscriptionAutomaticallyExistingExpression", "tag-a");
        fixture.put("subscriptionAutomaticallyExistingExpressionType", "SQL92");
        fixture.put("subscriptionAutomaticallyExistingVersion", existingSubscription.getSubVersion());
        fixture.put("subscriptionAutomaticallyCustomEntryPreserved",
            subscriptionInner.get("existing-topic") == existingSubscription);
        fixture.put("unsubscribeRemovedExactTopic", !subscriptionInner.containsKey("raw-topic"));
        fixture.put("unsubscribeRemovedEmptyTopic", !subscriptionInner.containsKey(""));
        fixture.put("unsubscribeLeavesOtherTopic", subscriptionInner.containsKey("existing-topic"));
        fixture.put("unsubscribeRegisteredTopic", registeredTopic);
        fixture.put("unsubscribeLeavesFacadeRegistration",
            consumer.getRegisterTopics().contains(registeredTopic));
        fixture.put("subscriptionAutomaticallyDoesNotNamespace", "raw-topic".equals(automaticSubscription.getTopic()));
        fixture.put("unsubscribedTopicNeedsUpdate", unsubscribedTopicNeedsUpdate);
        fixture.put("subscribedTopicNeedsUpdateBeforeInfo", subscribedTopicNeedsUpdateBeforeInfo);
        fixture.put("subscribeInfoStoredForSubscribedTopic", subscribeInfoStoredForSubscribedTopic);
        fixture.put("subscribedTopicNeedsUpdateAfterInfo", subscribedTopicNeedsUpdateAfterInfo);
        fixture.put("subscribeInfoStoredForUnsubscribedTopic", subscribeInfoStoredForUnsubscribedTopic);
        fixture.put("persistConsumerOffsetOutputQueues", persistedQueues);
        fixture.put("rawOffsetStoreCalls", rawOffsetStoreCalls);
        fixture.put("rawImplementationFetchOffset", rawImplementationFetchOffset);
        fixture.put("facadeFetchOffset", facadeFetchOffset);
        fixture.put("directRunningInfoProperties", directRunningInfoProperties);
        fixture.put("directRunningInfoSubscriptions", directRunningInfoSubscriptions);
        fixture.put("consumerStartTimestampPositive", first.getConsumerStartTimestamp() > 0);
        fixture.put("groupNameBeforeStart", groupNameBeforeStart);
        fixture.put("messageModelBeforeMutation", messageModelBeforeMutation.name());
        fixture.put("messageModelAfterMutation", messageModelAfterMutation.name());
        fixture.put("unitModeBeforeMutation", unitModeBeforeMutation);
        fixture.put("unitModeAfterMutation", unitModeAfterMutation);
        fixture.put("consumeType", consumeType.name());
        fixture.put("consumeFromWhere", consumeFromWhere.name());
        fixture.put("disabledTryRebalanceReturnsTrue", disabledTryRebalanceReturnsTrue);
        fixture.put("directHookEvents", directHookEvents);
        fixture.put("subscriptionsInitially", initialSubscriptions);
        fixture.put("subscriptionsFromTopics", topicSubscriptions);
        fixture.put("subscriptionsFromExplicit", explicitSubscriptions);
        fixture.put("subscriptionsWithSameSpecDifferentVersions", duplicateVersionSubscriptions);
        fixture.putAll(pullSendBackResults());
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture) + "\n";
        if (verify) {
            check(Files.exists(path) && json.equals(Files.readString(path)), "fixture drift: " + path);
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
        System.out.println("Pull implementation accessor verified");
    }

    private static final class RecordingMqClientInstance extends MQClientInstance {
        private DefaultMQProducer defaultMQProducer;
        private MQClientAPIImpl clientApi;

        private RecordingMqClientInstance() {
            super(null, 1, "phase18-pull-sendback-fixture");
        }

        private void initialize() {
            defaultMQProducer = null;
            clientApi = null;
        }

        @Override
        public DefaultMQProducer getDefaultMQProducer() {
            return defaultMQProducer;
        }

        @Override
        public MQClientAPIImpl getMQClientAPIImpl() {
            return clientApi;
        }

        @Override
        public String findBrokerAddressInPublish(String brokerName) {
            return "broker-address";
        }
    }

    private static final class RecordingMqClientAPIImpl extends MQClientAPIImpl {
        private List<String> consumerGroups;
        private boolean failNextSendBack;

        private RecordingMqClientAPIImpl() {
            super(null, null, null, null, null, null);
        }

        private void initialize() {
            consumerGroups = new ArrayList<>();
            failNextSendBack = false;
        }

        @Override
        public void consumerSendMessageBack(
            String addr,
            String brokerName,
            MessageExt message,
            String consumerGroup,
            int delayLevel,
            long timeoutMillis,
            int maxConsumeRetryTimes
        ) {
            consumerGroups.add(consumerGroup);
            if (failNextSendBack) {
                failNextSendBack = false;
                throw new IllegalStateException("scripted Broker rejection");
            }
        }
    }

    private static final class RecordingDefaultMQProducer extends DefaultMQProducer {
        private final List<String> sentTopics = new ArrayList<>();

        private RecordingDefaultMQProducer(String namespace) {
            super("phase18-pull-sendback-fixture");
            setNamespace(namespace);
        }

        @Override
        public SendResult send(Message message) {
            sentTopics.add(withNamespace(message.getTopic()));
            return null;
        }
    }
}
