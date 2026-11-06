package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessagePopConcurrentlyService;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessagePopOrderlyService;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageService;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.PopProcessQueue;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.PullRequest;
import org.apache.rocketmq.client.impl.consumer.PullMessageService;
import org.apache.rocketmq.client.impl.consumer.RebalancePushImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.client.trace.TraceContext;
import org.apache.rocketmq.client.trace.hook.ConsumeMessageTraceHookImpl;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader;

/** Invokes the deprecated Push consumer implementation accessor on Java 5.5.1. */
public final class PushImplementationAccessorFixtures {
    private PushImplementationAccessorFixtures() { }

    private static void check(boolean condition, String label) {
        if (!condition) throw new IllegalStateException(label);
    }

    public static void main(String[] args) throws Exception {
        check(args.length >= 1, "expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);

        String group = "phase18-push-implementation-accessor";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        DefaultMQPushConsumerImpl first = consumer.getDefaultMQPushConsumerImpl();
        DefaultMQPushConsumerImpl second = consumer.getDefaultMQPushConsumerImpl();
        check(first != null, "implementation accessor non-null");
        check(first == second, "implementation accessor identity stable");
        Object firstRebalance = first.getRebalanceImpl();
        Object secondRebalance = first.getRebalanceImpl();
        check(firstRebalance != null, "rebalance implementation accessor non-null");
        check(firstRebalance == secondRebalance, "rebalance implementation identity stable");
        boolean runningInfoBeforeStartThrowsNullPointerException = false;
        try {
            first.consumerRunningInfo();
        } catch (NullPointerException expectedNullService) {
            runningInfoBeforeStartThrowsNullPointerException = true;
        }

        String initialServiceState = first.getServiceState().name();
        first.setServiceState(null);
        boolean serviceStateSetterAcceptsNull = first.getServiceState() == null;
        first.setServiceState(ServiceState.START_FAILED);
        boolean startFailedStateBlocksStart = false;
        try {
            first.start();
        } catch (MQClientException expectedStateFailure) {
            startFailedStateBlocksStart = true;
        }
        String serviceStateAfterRejectedStart = first.getServiceState().name();
        first.setServiceState(ServiceState.CREATE_JUST);
        int effectiveMaxReconsumeTimesDefault = first.getMaxReconsumeTimes();
        consumer.setMaxReconsumeTimes(7);
        int effectiveMaxReconsumeTimesCustom = first.getMaxReconsumeTimes();
        String groupName = first.groupName();
        String messageModel = first.messageModel().name();
        String consumeType = first.consumeType().name();
        String consumeFromWhere = first.consumeFromWhere().name();
        boolean unitMode = first.isUnitMode();
        boolean clientFactoryInitiallyNull = first.getmQClientFactory() == null;
        boolean messageQueueListenerInitiallyNull = first.getMessageQueueListener() == null;
        boolean rawMessageListenerInitiallyNull = first.getMessageListenerInner() == null;
        MessageListenerConcurrently rawMessageListener =
            (messages, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        first.registerMessageListener(rawMessageListener);
        boolean rawMessageListenerGetterPreservesIdentity =
            first.getMessageListenerInner() == rawMessageListener;
        boolean rawFacadeConsumerIdentityStable = first.getDefaultMQPushConsumer() == consumer;
        String rawFacadeConsumerGroup = first.getDefaultMQPushConsumer().getConsumerGroup();
        boolean consumeMessageServiceInitiallyNull = first.getConsumeMessageService() == null;
        AtomicInteger adjustThreadPoolDecreaseCalls = new AtomicInteger();
        ConsumeMessageService rawConsumeMessageService = (ConsumeMessageService) Proxy.newProxyInstance(
            ConsumeMessageService.class.getClassLoader(),
            new Class<?>[] {ConsumeMessageService.class},
            (proxy, method, methodArgs) -> {
                if ("decCorePoolSize".equals(method.getName())) {
                    adjustThreadPoolDecreaseCalls.incrementAndGet();
                }
                return "getCorePoolSize".equals(method.getName()) ? 7 : null;
            });
        first.setConsumeMessageService(rawConsumeMessageService);
        boolean consumeMessageServiceSetterPreservesIdentity =
            first.getConsumeMessageService() == rawConsumeMessageService;
        int adjustThreadPoolCorePoolSizeBefore = first.getConsumeMessageService().getCorePoolSize();
        first.adjustThreadPool();
        int adjustThreadPoolCorePoolSizeAfter = first.getConsumeMessageService().getCorePoolSize();
        boolean pauseBeforeSet = first.isPause();
        first.setPause(true);
        boolean pauseAfterSetTrue = first.isPause();
        first.setPause(false);
        boolean pauseAfterSetFalse = first.isPause();
        boolean consumeOrderlyBeforeSet = first.isConsumeOrderly();
        first.setConsumeOrderly(true);
        boolean consumeOrderlyAfterSetTrue = first.isConsumeOrderly();
        first.setConsumeOrderly(false);
        boolean consumeOrderlyAfterSetFalse = first.isConsumeOrderly();
        long pullDelayBeforeSet = pullTimeDelayMillsWhenException(first);
        first.setPullTimeDelayMillsWhenException(73);
        long pullDelayAfterSet = pullTimeDelayMillsWhenException(first);
        int subscriptionCountBefore = first.subscriptions().size();
        Object subscriptionTable = first.getSubscriptionInner();
        first.subscribe("orders", "*");
        boolean subscriptionInnerIdentityStable = subscriptionTable == first.getSubscriptionInner();
        List<String> subscriptionTopicsAfterSubscribe = first.subscriptions().stream()
            .map(SubscriptionData::getTopic)
            .sorted()
            .collect(Collectors.toList());
        boolean subscribeInfoNeededBeforeRoute = first.isSubscribeTopicNeedUpdate("orders");
        first.updateTopicSubscribeInfo("orders", Set.of(new MessageQueue("orders", "broker-a", 1)));
        boolean subscribeInfoNeededAfterRoute = first.isSubscribeTopicNeedUpdate("orders");
        boolean unregisteredTopicNeedsRoute = first.isSubscribeTopicNeedUpdate("unregistered");
        first.suspend();
        boolean tryRebalanceWhilePaused = first.tryRebalance();
        first.resume();
        RecordingMqClientInstance resetFactory = recordingMqClientInstance();
        first.setmQClientFactory(resetFactory);
        boolean clientFactorySetterPreservesIdentity = first.getmQClientFactory() == resetFactory;
        AtomicInteger immediateTaskRuns = new AtomicInteger();
        AtomicInteger delayedTaskRuns = new AtomicInteger();
        first.executeTask(immediateTaskRuns::incrementAndGet);
        first.executeTaskLater(delayedTaskRuns::incrementAndGet, 17L);
        first.getRebalanceImpl().setmQClientFactory(resetFactory);
        SubscriptionData emptySubscription = new SubscriptionData();
        emptySubscription.setTopic("empty");
        first.getRebalanceImpl().getSubscriptionInner().put("empty", emptySubscription);
        MessageQueue resetQueueA = new MessageQueue("orders", "broker-a", 0);
        MessageQueue resetQueueB = new MessageQueue("orders", "broker-a", 1);
        first.getRebalanceImpl().getTopicSubscribeInfoTable().put(
            "orders", Set.of(resetQueueA, resetQueueB));
        first.getRebalanceImpl().getTopicSubscribeInfoTable().put("empty", Set.of());
        first.resetOffsetByTimeStamp(12345);
        boolean offsetStoreInitiallyNull = first.getOffsetStore() == null;
        List<String> offsetUpdates = new ArrayList<>();
        List<String> persistedQueues = new ArrayList<>();
        OffsetStore directOffsetStore = (OffsetStore) Proxy.newProxyInstance(
            OffsetStore.class.getClassLoader(),
            new Class<?>[] {OffsetStore.class},
            (proxy, method, methodArgs) -> {
                if ("updateOffset".equals(method.getName())) {
                    MessageQueue queue = (MessageQueue) methodArgs[0];
                    offsetUpdates.add(queue.getTopic() + "|" + queue.getBrokerName() + "|"
                        + queue.getQueueId() + "|" + methodArgs[1] + "|" + methodArgs[2]);
                } else if ("readOffset".equals(method.getName())) {
                    return 41L;
                } else if ("persistAll".equals(method.getName())) {
                    @SuppressWarnings("unchecked")
                    Set<MessageQueue> queues = (Set<MessageQueue>) methodArgs[0];
                    queues.stream()
                        .map(queue -> queue.getTopic() + "|" + queue.getBrokerName() + "|"
                            + queue.getQueueId())
                        .sorted()
                        .forEach(persistedQueues::add);
                }
                return null;
            });
        first.setOffsetStore(directOffsetStore);
        boolean offsetStoreSetterPreservesIdentity = first.getOffsetStore() == directOffsetStore;
        MessageQueue assignedQueue = new MessageQueue("orders", "broker-a", 2);
        MessageQueue unassignedQueue = new MessageQueue("orders", "broker-b", 3);
        first.updateConsumeOffset(assignedQueue, 41);
        first.updateConsumeOffset(unassignedQueue, 99);
        first.getRebalanceImpl().getProcessQueueTable().put(assignedQueue, new ProcessQueue());
        first.getRebalanceImpl().getPopProcessQueueTable().put(
            new MessageQueue("orders", "broker-a", 4), new PopProcessQueue());
        first.setServiceState(ServiceState.RUNNING);
        Map<String, Object> rawRunningInfo = rawRunningInfoResults(first, resetFactory);
        first.persistConsumerOffset();
        first.setServiceState(ServiceState.CREATE_JUST);
        first.setOffsetStore(null);
        boolean offsetStoreSetterAcceptsNull = first.getOffsetStore() == null;
        boolean hasConsumeHookBeforeRegistration = first.hasHook();
        List<String> directHookEvents = new ArrayList<>();
        first.registerConsumeMessageHook(new ConsumeMessageHook() {
            @Override
            public String hookName() {
                return "push-raw-hook-fixture";
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
        boolean hasConsumeHookAfterRegistration = first.hasHook();
        boolean directHookConsumerGroupNull = directHookContext.getConsumerGroup() == null;
        QueryConsumeTimeSpanRequestHeader nullGroupTimeSpanHeader = new QueryConsumeTimeSpanRequestHeader();
        nullGroupTimeSpanHeader.setTopic("orders");
        nullGroupTimeSpanHeader.setGroup(null);
        RemotingCommand nullGroupTimeSpanRequest = RemotingCommand.createRequestCommand(
            RequestCode.QUERY_CONSUME_TIME_SPAN, nullGroupTimeSpanHeader);
        nullGroupTimeSpanRequest.makeCustomHeaderToNet();
        boolean queryConsumeTimeSpanNullGroupOmitted = nullGroupTimeSpanRequest.getExtFields() != null
            && !nullGroupTimeSpanRequest.getExtFields().containsKey("group");

        List<String> filterHookEvents = new ArrayList<>();
        first.registerFilterMessageHook(new FilterMessageHook() {
            @Override
            public String hookName() {
                return "push-raw-filter-hook-fixture";
            }

            @Override
            public void filterMessage(FilterMessageContext context) {
                filterHookEvents.add(context.isUnitMode() + ":" + context.getMsgList().size());
            }
        });
        List<MessageExt> popMessages = new ArrayList<>();
        MessageExt popMessageA = new MessageExt();
        popMessageA.setTopic("orders");
        popMessages.add(popMessageA);
        MessageExt popMessageB = new MessageExt();
        popMessageB.setTopic("orders");
        popMessages.add(popMessageB);
        java.lang.reflect.Method processPopResult = DefaultMQPushConsumerImpl.class
            .getDeclaredMethod("processPopResult", PopResult.class, SubscriptionData.class);
        processPopResult.setAccessible(true);
        processPopResult.invoke(first, new PopResult(PopStatus.FOUND, popMessages), new SubscriptionData());

        Map<String, Object> topicHelpers = topicHelperResults();
        Map<String, Object> corePoolSize = corePoolSizeResults();

        Map<String, Object> fixture = new LinkedHashMap<>();
        fixture.put("consumerStatsManagersAreInstanceScoped", consumerStatsManagersAreInstanceScoped());
        fixture.put("consumerStatsManagersShareFactoryIdentity", consumerStatsManagersShareFactoryIdentity());
        fixture.putAll(consumerStatsManagerApi());
        fixture.putAll(consumerStatsManagerSampling());
        fixture.put("implementationNonNull", first != null);
        fixture.put("implementationIdentityStable", first == second);
        fixture.put("rebalanceImplementationNonNull", firstRebalance != null);
        fixture.put("rebalanceImplementationIdentityStable", firstRebalance == secondRebalance);
        fixture.put("configuredConsumerGroup", first.groupName());
        fixture.put("initialServiceState", initialServiceState);
        fixture.put("serviceStateSetterAcceptsNull", serviceStateSetterAcceptsNull);
        fixture.put("startFailedStateBlocksStart", startFailedStateBlocksStart);
        fixture.put("serviceStateAfterRejectedStart", serviceStateAfterRejectedStart);
        fixture.put("runningInfoBeforeStartThrowsNullPointerException",
            runningInfoBeforeStartThrowsNullPointerException);
        fixture.put("effectiveMaxReconsumeTimesDefault", effectiveMaxReconsumeTimesDefault);
        fixture.put("effectiveMaxReconsumeTimesCustom", effectiveMaxReconsumeTimesCustom);
        fixture.put("groupName", groupName);
        fixture.put("messageModel", messageModel);
        fixture.put("consumeType", consumeType);
        fixture.put("consumeFromWhere", consumeFromWhere);
        fixture.put("unitMode", unitMode);
        fixture.put("clientFactoryInitiallyNull", clientFactoryInitiallyNull);
        fixture.put("clientFactorySetterPreservesIdentity", clientFactorySetterPreservesIdentity);
        fixture.put("executeTaskRuns", immediateTaskRuns.get());
        fixture.put("executeTaskServiceCalls", resetFactory.pullMessageService.immediateTaskCalls);
        fixture.put("executeTaskLaterRuns", delayedTaskRuns.get());
        fixture.put("executeTaskLaterDelayMillis", resetFactory.pullMessageService.delayedTaskMillis);
        fixture.put("consumeMessageServiceInitiallyNull", consumeMessageServiceInitiallyNull);
        fixture.put("consumeMessageServiceSetterPreservesIdentity", consumeMessageServiceSetterPreservesIdentity);
        fixture.put("consumeMessageServicePublicMethods", consumeMessageServicePublicMethods());
        fixture.put("consumeMessagePopOrderlyServicePublicConstructors",
            publicConstructorSignatures(ConsumeMessagePopOrderlyService.class));
        fixture.put("consumeMessagePopOrderlyServicePublicMethods",
            declaredPublicMethodSignatures(ConsumeMessagePopOrderlyService.class));
        fixture.put("messageQueueListenerInitiallyNull", messageQueueListenerInitiallyNull);
        fixture.put("rawMessageListenerInitiallyNull", rawMessageListenerInitiallyNull);
        fixture.put("rawMessageListenerGetterPreservesIdentity", rawMessageListenerGetterPreservesIdentity);
        fixture.put("rawFacadeConsumerIdentityStable", rawFacadeConsumerIdentityStable);
        fixture.put("rawFacadeConsumerGroup", rawFacadeConsumerGroup);
        fixture.put("adjustThreadPoolDecreaseCalls", adjustThreadPoolDecreaseCalls.get());
        fixture.put("adjustThreadPoolCorePoolSizeBefore", adjustThreadPoolCorePoolSizeBefore);
        fixture.put("adjustThreadPoolCorePoolSizeAfter", adjustThreadPoolCorePoolSizeAfter);
        fixture.put("pauseBeforeSet", pauseBeforeSet);
        fixture.put("pauseAfterSetTrue", pauseAfterSetTrue);
        fixture.put("pauseAfterSetFalse", pauseAfterSetFalse);
        fixture.put("consumeOrderlyBeforeSet", consumeOrderlyBeforeSet);
        fixture.put("consumeOrderlyAfterSetTrue", consumeOrderlyAfterSetTrue);
        fixture.put("consumeOrderlyAfterSetFalse", consumeOrderlyAfterSetFalse);
        fixture.put("pullDelayBeforeSet", pullDelayBeforeSet);
        fixture.put("pullDelayAfterSet", pullDelayAfterSet);
        fixture.put("timestampResetSearches", resetFactory.searches.stream()
            .sorted()
            .collect(Collectors.toList()));
        fixture.put("timestampResetCalls", resetFactory.resets);
        fixture.put("subscriptionCountBefore", subscriptionCountBefore);
        fixture.put("subscriptionInnerIdentityStable", subscriptionInnerIdentityStable);
        fixture.put("subscriptionTopicsAfterSubscribe", subscriptionTopicsAfterSubscribe);
        fixture.put("subscribeInfoNeededBeforeRoute", subscribeInfoNeededBeforeRoute);
        fixture.put("subscribeInfoNeededAfterRoute", subscribeInfoNeededAfterRoute);
        fixture.put("unregisteredTopicNeedsRoute", unregisteredTopicNeedsRoute);
        fixture.put("tryRebalanceWhilePaused", tryRebalanceWhilePaused);
        fixture.put("offsetStoreInitiallyNull", offsetStoreInitiallyNull);
        fixture.put("offsetStoreSetterPreservesIdentity", offsetStoreSetterPreservesIdentity);
        fixture.put("offsetUpdates", offsetUpdates);
        fixture.put("persistedQueues", persistedQueues);
        fixture.putAll(rawRunningInfo);
        fixture.put("offsetStoreSetterAcceptsNull", offsetStoreSetterAcceptsNull);
        fixture.put("hasConsumeHookBeforeRegistration", hasConsumeHookBeforeRegistration);
        fixture.put("directHookEvents", directHookEvents);
        fixture.put("directHookConsumerGroupNull", directHookConsumerGroupNull);
        fixture.put("queryConsumeTimeSpanNullGroupOmitted", queryConsumeTimeSpanNullGroupOmitted);
        fixture.put("hasConsumeHookAfterRegistration", hasConsumeHookAfterRegistration);
        fixture.put("pushPopFilterHookEvents", filterHookEvents);
        fixture.putAll(corePoolSize);
        fixture.put("rawImplementationPublicMethodSignatures", publicMethodSignatures());
        fixture.putAll(classFilterSubscribeResults());
        fixture.put("pushConsumeInitModes", pushConsumeInitModes());
        fixture.putAll(topicHelpers);
        fixture.putAll(consumeServiceGroupSnapshots());
        fixture.putAll(consumeServiceRetryTopicResults());
        fixture.putAll(consumeTraceGroupResults());
        fixture.putAll(orderlyLockedPullAttempts());
        fixture.putAll(pushQueueThresholdResults());
        String json = JSON.toJSONString(fixture) + "\n";
        if (verify) {
            check(Files.exists(path) && json.equals(Files.readString(path)), "fixture drift: " + path);
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
        System.out.println("Push implementation accessor verified");
    }

    private static Map<String, Object> corePoolSizeResults() {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            result.put("updateCorePoolSizeReturnType",
                DefaultMQPushConsumer.class.getMethod("updateCorePoolSize", int.class)
                    .getReturnType().getSimpleName());
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("missing updateCorePoolSize facade method", error);
        }

        DefaultMQPushConsumer unstartedConsumer = new DefaultMQPushConsumer("phase18-core-size-null");
        boolean nullServiceThrowsNullPointerException = false;
        try {
            unstartedConsumer.updateCorePoolSize(3);
        } catch (NullPointerException expected) {
            nullServiceThrowsNullPointerException = true;
        }
        result.put("updateCorePoolSizeNullServiceThrowsNullPointerException",
            nullServiceThrowsNullPointerException);

        DefaultMQPushConsumer concurrentConsumer = new DefaultMQPushConsumer("phase18-core-size-concurrent");
        concurrentConsumer.setConsumeThreadMin(2);
        concurrentConsumer.setConsumeThreadMax(8);
        DefaultMQPushConsumerImpl concurrentImplementation =
            concurrentConsumer.getDefaultMQPushConsumerImpl();
        ConsumeMessageConcurrentlyService concurrentService = new ConsumeMessageConcurrentlyService(
            concurrentImplementation,
            (messages, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        concurrentImplementation.setConsumeMessageService(concurrentService);
        result.put("concurrentCorePoolSizeBefore", concurrentService.getCorePoolSize());
        concurrentConsumer.updateCorePoolSize(6);
        result.put("concurrentCorePoolSizeAfterValid", concurrentService.getCorePoolSize());
        concurrentConsumer.updateCorePoolSize(8);
        concurrentConsumer.updateCorePoolSize(0);
        result.put("concurrentCorePoolSizeAfterInvalid", concurrentService.getCorePoolSize());
        concurrentConsumer.setConsumeThreadMax(10);
        boolean concurrentRaisedMaxUpdateThrowsIllegalArgumentException = false;
        try {
            concurrentConsumer.updateCorePoolSize(9);
        } catch (IllegalArgumentException expected) {
            concurrentRaisedMaxUpdateThrowsIllegalArgumentException = true;
        }
        result.put("concurrentRaisedMaxUpdateThrowsIllegalArgumentException",
            concurrentRaisedMaxUpdateThrowsIllegalArgumentException);
        result.put("concurrentCorePoolSizeAfterRaisedMaxFailure", concurrentService.getCorePoolSize());
        concurrentConsumer.updateCorePoolSize(8);
        result.put("concurrentCorePoolSizeAtExecutorMaximum", concurrentService.getCorePoolSize());
        concurrentService.shutdown(0);
        boolean concurrentShutdownRaisedMaxUpdateThrowsIllegalArgumentException = false;
        try {
            concurrentConsumer.updateCorePoolSize(9);
        } catch (IllegalArgumentException expected) {
            concurrentShutdownRaisedMaxUpdateThrowsIllegalArgumentException = true;
        }
        result.put("concurrentShutdownRaisedMaxUpdateThrowsIllegalArgumentException",
            concurrentShutdownRaisedMaxUpdateThrowsIllegalArgumentException);
        concurrentConsumer.updateCorePoolSize(7);
        result.put("concurrentCorePoolSizeAfterShutdownUpdate", concurrentService.getCorePoolSize());

        DefaultMQPushConsumer orderlyConsumer = new DefaultMQPushConsumer("phase18-core-size-orderly");
        orderlyConsumer.setConsumeThreadMin(2);
        orderlyConsumer.setConsumeThreadMax(8);
        orderlyConsumer.setMessageModel(MessageModel.BROADCASTING);
        DefaultMQPushConsumerImpl orderlyImplementation = orderlyConsumer.getDefaultMQPushConsumerImpl();
        ConsumeMessageOrderlyService orderlyService = new ConsumeMessageOrderlyService(
            orderlyImplementation,
            (messages, context) -> ConsumeOrderlyStatus.SUCCESS);
        orderlyImplementation.setConsumeMessageService(orderlyService);
        result.put("orderlyCorePoolSizeBefore", orderlyService.getCorePoolSize());
        orderlyConsumer.updateCorePoolSize(6);
        result.put("orderlyCorePoolSizeAfterValid", orderlyService.getCorePoolSize());
        orderlyConsumer.updateCorePoolSize(8);
        orderlyConsumer.updateCorePoolSize(-1);
        result.put("orderlyCorePoolSizeAfterInvalid", orderlyService.getCorePoolSize());
        orderlyConsumer.setConsumeThreadMax(10);
        boolean orderlyRaisedMaxUpdateThrowsIllegalArgumentException = false;
        try {
            orderlyConsumer.updateCorePoolSize(9);
        } catch (IllegalArgumentException expected) {
            orderlyRaisedMaxUpdateThrowsIllegalArgumentException = true;
        }
        result.put("orderlyRaisedMaxUpdateThrowsIllegalArgumentException",
            orderlyRaisedMaxUpdateThrowsIllegalArgumentException);
        result.put("orderlyCorePoolSizeAfterRaisedMaxFailure", orderlyService.getCorePoolSize());
        orderlyConsumer.updateCorePoolSize(8);
        result.put("orderlyCorePoolSizeAtExecutorMaximum", orderlyService.getCorePoolSize());
        orderlyService.shutdown(0);
        boolean orderlyShutdownRaisedMaxUpdateThrowsIllegalArgumentException = false;
        try {
            orderlyConsumer.updateCorePoolSize(9);
        } catch (IllegalArgumentException expected) {
            orderlyShutdownRaisedMaxUpdateThrowsIllegalArgumentException = true;
        }
        result.put("orderlyShutdownRaisedMaxUpdateThrowsIllegalArgumentException",
            orderlyShutdownRaisedMaxUpdateThrowsIllegalArgumentException);
        orderlyConsumer.updateCorePoolSize(7);
        result.put("orderlyCorePoolSizeAfterShutdownUpdate", orderlyService.getCorePoolSize());
        return result;
    }

    private static List<String> publicMethodSignatures() {
        return declaredPublicMethodSignatures(DefaultMQPushConsumerImpl.class);
    }

    private static List<String> declaredPublicMethodSignatures(Class<?> type) {
        return Arrays.stream(type.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> !method.isBridge() && !method.isSynthetic())
            .map(PushImplementationAccessorFixtures::signature)
            .sorted()
            .collect(Collectors.toList());
    }

    private static List<String> publicConstructorSignatures(Class<?> type) {
        return Arrays.stream(type.getConstructors())
            .map(constructor -> type.getSimpleName() + "("
                + Arrays.stream(constructor.getParameterTypes())
                    .map(Class::getSimpleName)
                    .collect(Collectors.joining(","))
                + ")")
            .sorted()
            .collect(Collectors.toList());
    }

    private static Map<String, Object> consumerStatsManagerApi() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("consumerStatsManagerPublicConstructors",
            Arrays.stream(ConsumerStatsManager.class.getConstructors())
                .map(constructor -> "ConsumerStatsManager(" + Arrays.stream(constructor.getParameterTypes())
                    .map(Class::getSimpleName)
                    .collect(Collectors.joining(",")) + ")")
                .sorted()
                .collect(Collectors.toList()));
        result.put("consumerStatsManagerPublicMethods",
            Arrays.stream(ConsumerStatsManager.class.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()))
                .filter(method -> !method.isBridge() && !method.isSynthetic())
                .map(PushImplementationAccessorFixtures::signature)
                .sorted()
                .collect(Collectors.toList()));
        return result;
    }

    private static Map<String, Object> consumerStatsManagerSampling() {
        List<Runnable> secondSamplers = new ArrayList<>();
        ScheduledExecutorService scheduler = (ScheduledExecutorService) Proxy.newProxyInstance(
            ScheduledExecutorService.class.getClassLoader(),
            new Class<?>[] {ScheduledExecutorService.class},
            (proxy, method, arguments) -> {
                if ("scheduleAtFixedRate".equals(method.getName())) {
                    long period = (long) arguments[2];
                    TimeUnit unit = (TimeUnit) arguments[3];
                    if (period == 10 && unit == TimeUnit.SECONDS) {
                        secondSamplers.add((Runnable) arguments[0]);
                    }
                }
                return null;
            });
        ConsumerStatsManager manager = new ConsumerStatsManager(scheduler);
        manager.incPullTPS("workers", "orders", 20);
        double beforeFirstSample = manager.consumeStatus("workers", "orders").getPullTPS();
        secondSamplers.forEach(Runnable::run);
        double afterFirstSample = stableTps(manager.consumeStatus("workers", "orders").getPullTPS());
        manager.incPullTPS("workers", "orders", 5);
        double afterPendingUpdate = stableTps(manager.consumeStatus("workers", "orders").getPullTPS());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("consumerStatsSamplingSecondSamplerCount", secondSamplers.size());
        result.put("consumerStatsSamplingBeforeFirstSamplePullTPS", beforeFirstSample);
        result.put("consumerStatsSamplingAfterFirstSamplePullTPS", afterFirstSample);
        result.put("consumerStatsSamplingAfterPendingUpdatePullTPS", afterPendingUpdate);
        return result;
    }

    private static double stableTps(double tps) {
        // StatsItem samples against wall-clock milliseconds, while the fixture
        // models an exact ten-second interval. Match RocketMQ's two-decimal
        // TPS display precision so a one-millisecond timing difference cannot
        // make the checked-in fixture drift (for example, 1.9998 vs. 2.0).
        return Math.round(tps * 100.0d) / 100.0d;
    }

    private static List<String> consumeMessageServicePublicMethods() {
        return declaredPublicMethodSignatures(ConsumeMessageService.class);
    }

    private static String signature(Method method) {
        String parameters = Arrays.stream(method.getParameterTypes())
            .map(Class::getSimpleName)
            .collect(Collectors.joining(","));
        return method.getName() + "(" + parameters + ")";
    }

    private static Map<String, Object> topicHelperResults() {
        String group = "phase18-push-topic-helpers";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamespace("tenant");
        DefaultMQPushConsumerImpl implementation = consumer.getDefaultMQPushConsumerImpl();

        Set<MessageQueue> subscribedQueues = Set.of(
            new MessageQueue("tenant%orders", "broker-a", 1),
            new MessageQueue("orders", "broker-b", 2),
            new MessageQueue("other%events", "broker-c", 3));
        List<String> parsedQueues = implementation.parseSubscribeMessageQueues(subscribedQueues).stream()
            .map(queue -> queue.getTopic() + "|" + queue.getBrokerName() + "|" + queue.getQueueId())
            .sorted()
            .collect(Collectors.toList());

        List<MessageExt> popRetryMessages = new ArrayList<>();
        for (String topic : List.of("%RETRY%" + group + "_orders", "%RETRY%other_orders", "orders")) {
            MessageExt message = new MessageExt();
            message.setTopic(topic);
            popRetryMessages.add(message);
        }
        implementation.tryResetPopRetryTopic(popRetryMessages, group);
        List<String> resetPopRetryTopics = popRetryMessages.stream()
            .map(MessageExt::getTopic)
            .collect(Collectors.toList());

        List<MessageExt> retryAndNamespaceMessages = new ArrayList<>();
        MessageExt retry = new MessageExt();
        retry.setTopic("%RETRY%" + group);
        MessageAccessor.putProperty(retry, MessageConst.PROPERTY_RETRY_TOPIC, "tenant%orders");
        retryAndNamespaceMessages.add(retry);
        MessageExt ordinary = new MessageExt();
        ordinary.setTopic("tenant%events");
        retryAndNamespaceMessages.add(ordinary);
        MessageExt otherNamespace = new MessageExt();
        otherNamespace.setTopic("other%events");
        retryAndNamespaceMessages.add(otherNamespace);
        implementation.resetRetryAndNamespace(retryAndNamespaceMessages, group);
        List<String> resetRetryAndNamespaceTopics = retryAndNamespaceMessages.stream()
            .map(MessageExt::getTopic)
            .collect(Collectors.toList());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("parsedSubscribeQueues", parsedQueues);
        result.put("resetPopRetryTopics", resetPopRetryTopics);
        result.put("resetRetryAndNamespaceTopics", resetRetryAndNamespaceTopics);
        return result;
    }

    private static Map<String, Object> classFilterSubscribeResults() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("phase18-class-filter-subscribe");
        consumer.setNamespace("tenant");
        DefaultMQPushConsumerImpl implementation = consumer.getDefaultMQPushConsumerImpl();
        consumer.subscribe("class-filter-orders", "compat.filters.OrderFilter", "source-v1");

        SubscriptionData beforeStart = implementation.getSubscriptionInner()
            .get("tenant%class-filter-orders");
        check(beforeStart != null, "class-filter subscription stored before client factory");

        RecordingMqClientInstance factory = recordingMqClientInstance();
        implementation.setmQClientFactory(factory);
        consumer.subscribe("class-filter-orders", "compat.filters.RiskFilter", "source-v2");
        SubscriptionData afterFactory = implementation.getSubscriptionInner()
            .get("tenant%class-filter-orders");
        check(afterFactory != null, "class-filter subscription stored with client factory");

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("classFilterBeforeStartTopic", beforeStart.getTopic());
        result.put("classFilterBeforeStartClassName", beforeStart.getSubString());
        result.put("classFilterBeforeStartMode", beforeStart.isClassFilterMode());
        result.put("classFilterBeforeStartSource", beforeStart.getFilterClassSource());
        result.put("classFilterAfterFactoryTopic", afterFactory.getTopic());
        result.put("classFilterAfterFactoryClassName", afterFactory.getSubString());
        result.put("classFilterAfterFactoryMode", afterFactory.isClassFilterMode());
        result.put("classFilterAfterFactorySource", afterFactory.getFilterClassSource());
        result.put("classFilterAfterFactoryHeartbeatCalls", factory.heartbeatCalls);
        com.alibaba.fastjson2.JSONObject wireData = JSON.parseObject(JSON.toJSONString(afterFactory));
        wireData.put("subVersion", 0L);
        result.put("classFilterAfterFactoryWireData", wireData);
        result.put("classFilterSourceSerialized",
            JSON.toJSONString(afterFactory).contains("filterClassSource"));
        return result;
    }

    private static Map<String, Object> pushConsumeInitModes() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("phase18-pop-init-mode");
        DefaultMQPushConsumerImpl implementation = consumer.getDefaultMQPushConsumerImpl();
        Map<String, Object> modes = new LinkedHashMap<>();
        for (ConsumeFromWhere policy : ConsumeFromWhere.values()) {
            consumer.setConsumeFromWhere(policy);
            modes.put(policy.name(), implementation.getRebalanceImpl().getConsumeInitMode());
        }
        return modes;
    }

    private static Map<String, Object> pushQueueThresholdResults() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("phase18-push-queue-thresholds");
        consumer.setPullThresholdForQueue(777);
        consumer.setPullThresholdSizeForQueue(77);
        consumer.setPullThresholdForTopic(20);
        consumer.setPullThresholdSizeForTopic(12);
        consumer.subscribe("orders", "*");
        consumer.subscribe("events", "*");

        DefaultMQPushConsumerImpl implementation = consumer.getDefaultMQPushConsumerImpl();
        RebalancePushImpl rebalance = (RebalancePushImpl) implementation.getRebalanceImpl();
        RecordingMqClientInstance factory = recordingMqClientInstance();
        implementation.setmQClientFactory(factory);
        rebalance.setmQClientFactory(factory);

        rebalance.messageQueueChanged("orders", Set.of(), Set.of());
        int zeroAssignmentQueueThreshold = consumer.getPullThresholdForQueue();
        int zeroAssignmentSizeThreshold = consumer.getPullThresholdSizeForQueue();

        MessageQueue orders0 = new MessageQueue("orders", "broker-a", 0);
        MessageQueue orders1 = new MessageQueue("orders", "broker-a", 1);
        MessageQueue events0 = new MessageQueue("events", "broker-b", 0);
        MessageQueue events1 = new MessageQueue("events", "broker-b", 1);
        rebalance.getProcessQueueTable().put(orders0, new ProcessQueue());
        rebalance.messageQueueChanged("orders", Set.of(orders0), Set.of(orders0));
        int singleAssignmentQueueThreshold = consumer.getPullThresholdForQueue();
        int singleAssignmentSizeThreshold = consumer.getPullThresholdSizeForQueue();

        rebalance.getProcessQueueTable().put(orders1, new ProcessQueue());
        rebalance.getProcessQueueTable().put(events0, new ProcessQueue());
        rebalance.getProcessQueueTable().put(events1, new ProcessQueue());
        rebalance.messageQueueChanged("orders", Set.of(orders0, orders1), Set.of(orders0, orders1));
        int fourAssignmentQueueThreshold = consumer.getPullThresholdForQueue();
        int fourAssignmentSizeThreshold = consumer.getPullThresholdSizeForQueue();

        rebalance.getProcessQueueTable().remove(events1);
        rebalance.messageQueueChanged("events", Set.of(events0), Set.of(events0));
        int threeAssignmentQueueThreshold = consumer.getPullThresholdForQueue();
        int threeAssignmentSizeThreshold = consumer.getPullThresholdSizeForQueue();

        rebalance.getProcessQueueTable().clear();
        rebalance.messageQueueChanged("orders", Set.of(), Set.of());
        int zeroAfterAssignmentQueueThreshold = consumer.getPullThresholdForQueue();
        int zeroAfterAssignmentSizeThreshold = consumer.getPullThresholdSizeForQueue();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("pushQueueThresholdZeroAssignment", List.of(
            zeroAssignmentQueueThreshold, zeroAssignmentSizeThreshold));
        result.put("pushQueueThresholdSingleAssignment", List.of(
            singleAssignmentQueueThreshold, singleAssignmentSizeThreshold));
        result.put("pushQueueThresholdFourAssignmentsAcrossTopics", List.of(
            fourAssignmentQueueThreshold, fourAssignmentSizeThreshold));
        result.put("pushQueueThresholdThreeAssignmentsAcrossTopics", List.of(
            threeAssignmentQueueThreshold, threeAssignmentSizeThreshold));
        result.put("pushQueueThresholdZeroAfterAssignment", List.of(
            zeroAfterAssignmentQueueThreshold, zeroAfterAssignmentSizeThreshold));
        result.put("pushQueueThresholdHeartbeatCalls", factory.heartbeatCalls);
        return result;
    }

    private static Map<String, Object> orderlyLockedPullAttempts() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("phase18-orderly-pull-offset");
        DefaultMQPushConsumerImpl implementation = consumer.getDefaultMQPushConsumerImpl();
        implementation.setServiceState(ServiceState.RUNNING);
        implementation.setConsumeOrderly(true);

        AtomicInteger storeReads = new AtomicInteger();
        OffsetStore offsetStore = (OffsetStore) Proxy.newProxyInstance(
            OffsetStore.class.getClassLoader(),
            new Class<?>[] {OffsetStore.class},
            (proxy, method, methodArgs) -> {
                if ("readOffset".equals(method.getName())) {
                    return storeReads.getAndIncrement() == 0 ? 15L : 21L;
                }
                return null;
            });
        implementation.setOffsetStore(offsetStore);

        MessageQueue queue = new MessageQueue("orders", "broker-a", 0);
        ProcessQueue processQueue = new ProcessQueue();
        processQueue.setLocked(true);
        PullRequest request = new PullRequest();
        request.setConsumerGroup("phase18-orderly-pull-offset");
        request.setMessageQueue(queue);
        request.setProcessQueue(processQueue);
        request.setNextOffset(8L);
        consumer.setConsumerGroup("phase18-orderly-pull-offset-mutated");

        try {
            implementation.pullMessage(request);
        } catch (RuntimeException expectedAfterOffsetRefresh) {
            // No live client factory is installed; the empty subscription path reaches
            // delayed pull scheduling after the first-lock offset refresh.
        }
        long firstAttemptOffset = request.getNextOffset();
        boolean previouslyLockedAfterFirstAttempt = request.isPreviouslyLocked();
        int storeReadsAfterFirstAttempt = storeReads.get();

        try {
            implementation.pullMessage(request);
        } catch (RuntimeException expectedAfterOffsetRefresh) {
            // The second attempt remains on the same controlled no-factory path.
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("orderlyFirstLockedPullOffset", firstAttemptOffset);
        result.put("orderlyFirstLockedPullPreviouslyLocked", previouslyLockedAfterFirstAttempt);
        result.put("orderlyFirstLockedPullStoreReads", storeReadsAfterFirstAttempt);
        result.put("orderlySecondLockedPullOffset", request.getNextOffset());
        result.put("orderlySecondLockedPullStoreReads", storeReads.get());
        result.put("orderlyPullStatsGroup", request.getConsumerGroup());
        result.put("orderlyPullFacadeGroupAfterMutation", consumer.getConsumerGroup());
        return result;
    }

    private static long pullTimeDelayMillsWhenException(DefaultMQPushConsumerImpl implementation)
        throws Exception {
        Field delay = DefaultMQPushConsumerImpl.class.getDeclaredField("pullTimeDelayMillsWhenException");
        delay.setAccessible(true);
        return delay.getLong(implementation);
    }

    private static Map<String, Object> consumeServiceGroupSnapshots() throws Exception {
        String group = "phase18-consume-service-group";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        DefaultMQPushConsumerImpl implementation = consumer.getDefaultMQPushConsumerImpl();
        MessageListenerConcurrently concurrentListener =
            (messages, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        MessageListenerOrderly orderlyListener =
            (messages, context) -> ConsumeOrderlyStatus.SUCCESS;
        ConsumeMessageConcurrentlyService concurrent =
            new ConsumeMessageConcurrentlyService(implementation, concurrentListener);
        ConsumeMessageOrderlyService orderly =
            new ConsumeMessageOrderlyService(implementation, orderlyListener);
        ConsumeMessagePopConcurrentlyService popConcurrent =
            new ConsumeMessagePopConcurrentlyService(implementation, concurrentListener);
        ConsumeMessagePopOrderlyService popOrderly =
            new ConsumeMessagePopOrderlyService(implementation, orderlyListener);
        implementation.getRebalanceImpl().setConsumerGroup(group);
        consumer.setConsumerGroup("phase18-consume-service-group-mutated");

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("consumeServiceGroupBeforeMutation", group);
        result.put("consumeServiceFacadeGroupAfterMutation", consumer.getConsumerGroup());
        result.put("concurrentConsumeServiceGroup", serviceConsumerGroup(concurrent));
        result.put("orderlyConsumeServiceGroup", serviceConsumerGroup(orderly));
        result.put("popConcurrentConsumeServiceGroup", serviceConsumerGroup(popConcurrent));
        result.put("popOrderlyConsumeServiceGroup", serviceConsumerGroup(popOrderly));
        result.put("rebalanceConsumerGroupAfterFacadeMutation", implementation.getRebalanceImpl().getConsumerGroup());
        concurrent.shutdown(100);
        orderly.shutdown(100);
        popConcurrent.shutdown(100);
        popOrderly.shutdown(100);
        return result;
    }

    private static String serviceConsumerGroup(Object service) throws Exception {
        Field group = service.getClass().getDeclaredField("consumerGroup");
        group.setAccessible(true);
        return (String) group.get(service);
    }

    private static Map<String, Object> consumeServiceRetryTopicResults() throws Exception {
        String group = "phase18-consume-retry-snapshot";
        String namespacedGroup = "tenant%" + group;
        String liveGroup = "phase18-consume-retry-live";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamespace("tenant");
        consumer.setConsumeMessageBatchMaxSize(3);
        consumer.setMessageModel(MessageModel.BROADCASTING);
        // DefaultMQPushConsumer.start() applies the namespace before creating
        // the consume services. Keep that captured group, then mutate the live
        // facade to exercise the service call paths after startup.
        consumer.setConsumerGroup(namespacedGroup);
        DefaultMQPushConsumerImpl implementation = consumer.getDefaultMQPushConsumerImpl();

        List<String> consumeHookBeforeGroups = Collections.synchronizedList(new ArrayList<>());
        List<String> consumeHookAfterGroups = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch consumeHookAfterCallbacks = new CountDownLatch(2);
        implementation.registerConsumeMessageHook(new ConsumeMessageHook() {
            @Override
            public String hookName() {
                return "phase18-live-group-hook-fixture";
            }

            @Override
            public void consumeMessageBefore(ConsumeMessageContext context) {
                consumeHookBeforeGroups.add(context.getConsumerGroup());
            }

            @Override
            public void consumeMessageAfter(ConsumeMessageContext context) {
                consumeHookAfterGroups.add(context.getConsumerGroup());
                consumeHookAfterCallbacks.countDown();
            }
        });

        AtomicReference<List<String>> concurrentCallbackTopics = new AtomicReference<>();
        CountDownLatch concurrentCallback = new CountDownLatch(1);
        MessageListenerConcurrently concurrentListener = (messages, context) -> {
            concurrentCallbackTopics.set(messages.stream().map(MessageExt::getTopic).collect(Collectors.toList()));
            concurrentCallback.countDown();
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        };
        AtomicReference<List<String>> concurrentDirectTopics = new AtomicReference<>();
        MessageListenerConcurrently concurrentDirectListener = (messages, context) -> {
            concurrentDirectTopics.set(messages.stream().map(MessageExt::getTopic).collect(Collectors.toList()));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        };
        AtomicReference<List<String>> orderlyDirectTopics = new AtomicReference<>();
        MessageListenerOrderly orderlyDirectListener = (messages, context) -> {
            orderlyDirectTopics.set(messages.stream().map(MessageExt::getTopic).collect(Collectors.toList()));
            return ConsumeOrderlyStatus.SUCCESS;
        };
        AtomicReference<List<String>> orderlyCallbackTopics = new AtomicReference<>();
        CountDownLatch orderlyCallback = new CountDownLatch(1);
        MessageListenerOrderly orderlyListener = (messages, context) -> {
            orderlyCallbackTopics.set(messages.stream().map(MessageExt::getTopic).collect(Collectors.toList()));
            orderlyCallback.countDown();
            return ConsumeOrderlyStatus.SUCCESS;
        };
        ConsumeMessageConcurrentlyService concurrent =
            new ConsumeMessageConcurrentlyService(implementation, concurrentListener);
        ConsumeMessageConcurrentlyService concurrentDirect =
            new ConsumeMessageConcurrentlyService(implementation, concurrentDirectListener);
        ConsumeMessageOrderlyService orderlyDirect =
            new ConsumeMessageOrderlyService(implementation, orderlyDirectListener);
        ConsumeMessageOrderlyService orderly = new ConsumeMessageOrderlyService(implementation, orderlyListener);

        consumer.setConsumerGroup(liveGroup);
        List<MessageExt> callbackMessages = List.of(
            retryTopicMessage("%RETRY%" + namespacedGroup + "_orders", null),
            retryTopicMessage("%RETRY%" + liveGroup, "tenant%retry"),
            retryTopicMessage("tenant%events", null));
        concurrent.submitConsumeRequest(callbackMessages, new ProcessQueue(),
            new MessageQueue("tenant%orders", "broker-a", 0), true);
        check(concurrentCallback.await(5, TimeUnit.SECONDS), "concurrent consume callback completed");

        ProcessQueue orderlyProcessQueue = new ProcessQueue();
        orderlyProcessQueue.putMessage(List.of(retryTopicMessage(
            "%RETRY%" + liveGroup, "tenant%orderly-live")));
        orderly.submitConsumeRequest(null, orderlyProcessQueue,
            new MessageQueue("tenant%orders", "broker-a", 0), true);
        check(orderlyCallback.await(5, TimeUnit.SECONDS), "orderly consume callback completed");
        check(consumeHookAfterCallbacks.await(5, TimeUnit.SECONDS), "consume hooks completed");

        concurrentDirect.consumeMessageDirectly(
            retryTopicMessage("%RETRY%" + namespacedGroup, "tenant%direct-concurrent"), "broker-a");
        orderlyDirect.consumeMessageDirectly(
            retryTopicMessage("%RETRY%" + namespacedGroup, "tenant%direct-orderly"), "broker-a");

        RecordingMqClientInstance factory = recordingMqClientInstance();
        RecordingDefaultMQProducer producer = new RecordingDefaultMQProducer("tenant");
        RecordingMqClientAPIImpl clientApi = recordingMqClientAPIImpl();
        factory.defaultMQProducer = producer;
        factory.clientApi = clientApi;
        implementation.setmQClientFactory(factory);
        implementation.sendMessageBack(retryTopicMessage("tenant%orders", null), 3, "broker-a");
        Method sendBackAsNormalMessage = DefaultMQPushConsumerImpl.class
            .getDeclaredMethod("sendMessageBackAsNormalMessage", MessageExt.class);
        sendBackAsNormalMessage.setAccessible(true);
        MessageExt sendBackMessage = retryTopicMessage("tenant%orders", null);
        sendBackMessage.setBody(new byte[] {1});
        sendBackAsNormalMessage.invoke(implementation, sendBackMessage);

        concurrent.shutdown(100);
        concurrentDirect.shutdown(100);
        orderlyDirect.shutdown(100);
        orderly.shutdown(100);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("concurrentConsumeCallbackTopics", concurrentCallbackTopics.get());
        result.put("concurrentConsumeDirectTopics", concurrentDirectTopics.get());
        result.put("orderlyConsumeDirectTopics", orderlyDirectTopics.get());
        result.put("orderlyConsumeCallbackTopics", orderlyCallbackTopics.get());
        result.put("consumeHookBeforeGroups", new ArrayList<>(consumeHookBeforeGroups));
        result.put("consumeHookAfterGroups", new ArrayList<>(consumeHookAfterGroups));
        result.put("brokerSendBackConsumerGroups", clientApi.consumerGroups);
        result.put("sendBackFallbackRetryTopics", producer.sentTopics);
        return result;
    }

    private static Map<String, Object> consumeTraceGroupResults() {
        MessageExt message = retryTopicMessage("tenant%orders", null);
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_TRACE_SWITCH, "false");
        ConsumeMessageContext context = new ConsumeMessageContext();
        context.setConsumerGroup("phase18-consume-retry-live");
        context.setMsgList(List.of(message));
        new ConsumeMessageTraceHookImpl(null).consumeMessageBefore(context);
        TraceContext traceContext = (TraceContext) context.getMqTraceContext();
        return Map.of("consumeTraceHookGroup", traceContext.getGroupName());
    }

    private static MessageExt retryTopicMessage(String topic, String retryTopic) {
        MessageExt message = new MessageExt();
        message.setTopic(topic);
        if (retryTopic != null) {
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_RETRY_TOPIC, retryTopic);
        }
        return message;
    }

    private static Map<String, Object> rawRunningInfoResults(
        DefaultMQPushConsumerImpl implementation,
        RecordingMqClientInstance factory) throws Exception {
        try {
            factory.initializeStats();
            Field consumeServiceField = DefaultMQPushConsumerImpl.class
                .getDeclaredField("consumeMessageService");
            consumeServiceField.setAccessible(true);
            consumeServiceField.set(implementation, Proxy.newProxyInstance(
                ConsumeMessageService.class.getClassLoader(),
                new Class<?>[] {ConsumeMessageService.class},
                (proxy, method, methodArgs) -> "getCorePoolSize".equals(method.getName()) ? 7 : null));
            ConsumerRunningInfo info = implementation.consumerRunningInfo();
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("consumerStatsManagerGetterPreservesFactoryIdentity",
                implementation.getConsumerStatsManager() == factory.stats);
            result.put("rawRunningInfoSubscriptionCount", info.getSubscriptionSet().size());
            result.put("rawRunningInfoQueueCount", info.getMqTable().size());
            result.put("rawRunningInfoPopQueueCount", info.getMqPopTable().size());
            result.put("rawRunningInfoStatusCount", info.getStatusTable().size());
            result.put("rawRunningInfoCorePoolSize",
                info.getProperties().getProperty("PROP_THREADPOOL_CORE_SIZE"));
            Map<MessageQueue, ProcessQueue> processQueues = new LinkedHashMap<>(
                implementation.getRebalanceImpl().getProcessQueueTable());
            Map<MessageQueue, PopProcessQueue> popProcessQueues = new LinkedHashMap<>(
                implementation.getRebalanceImpl().getPopProcessQueueTable());
            implementation.getRebalanceImpl().destroy();
            implementation.setServiceState(ServiceState.SHUTDOWN_ALREADY);
            ConsumerRunningInfo afterShutdown = implementation.consumerRunningInfo();
            result.put("rawRunningInfoAfterShutdownSucceeded", true);
            result.put("rawRunningInfoAfterShutdownQueueCount", afterShutdown.getMqTable().size());
            result.put("rawRunningInfoAfterShutdownPopQueueCount", afterShutdown.getMqPopTable().size());
            result.put("rawRunningInfoAfterShutdownStatusCount", afterShutdown.getStatusTable().size());
            result.put("rawRunningInfoAfterShutdownCorePoolSize",
                afterShutdown.getProperties().getProperty("PROP_THREADPOOL_CORE_SIZE"));
            implementation.getRebalanceImpl().getProcessQueueTable().putAll(processQueues);
            implementation.getRebalanceImpl().getPopProcessQueueTable().putAll(popProcessQueues);
            implementation.setServiceState(ServiceState.RUNNING);
            return result;
        } finally {
            factory.shutdownStats();
        }
    }

    private static boolean consumerStatsManagersAreInstanceScoped() throws Exception {
        MQClientInstance first = new MQClientInstance(new ClientConfig(), 101, "phase18-stats-scope");
        MQClientInstance second = new MQClientInstance(new ClientConfig(), 102, "phase18-stats-scope");
        Field schedulerField = MQClientInstance.class.getDeclaredField("scheduledExecutorService");
        schedulerField.setAccessible(true);
        ScheduledExecutorService firstScheduler = (ScheduledExecutorService) schedulerField.get(first);
        ScheduledExecutorService secondScheduler = (ScheduledExecutorService) schedulerField.get(second);
        try {
            return first.getClientId().equals(second.getClientId())
                && first.getConsumerStatsManager() != second.getConsumerStatsManager();
        } finally {
            firstScheduler.shutdownNow();
            secondScheduler.shutdownNow();
        }
    }

    private static boolean consumerStatsManagersShareFactoryIdentity() throws Exception {
        RecordingMqClientInstance factory = recordingMqClientInstance();
        DefaultMQPushConsumerImpl first = new DefaultMQPushConsumerImpl(
            new DefaultMQPushConsumer("phase18-stats-share-a"), null);
        DefaultMQPushConsumerImpl second = new DefaultMQPushConsumerImpl(
            new DefaultMQPushConsumer("phase18-stats-share-b"), null);
        try {
            first.setmQClientFactory(factory);
            second.setmQClientFactory(factory);
            return first.getConsumerStatsManager() == second.getConsumerStatsManager()
                && first.getConsumerStatsManager() == factory.stats;
        } finally {
            factory.shutdownStats();
        }
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

    private static final class RecordingMqClientInstance extends MQClientInstance {
        private List<String> searches;
        private List<Map<String, Object>> resets;
        private int heartbeatCalls;
        private MQAdminImpl admin;
        private RecordingPullMessageService pullMessageService;
        private ScheduledExecutorService statsScheduler;
        private ConsumerStatsManager stats;
        private DefaultMQProducer defaultMQProducer;
        private MQClientAPIImpl clientApi;

        private RecordingMqClientInstance() {
            super(new ClientConfig(), 1, "phase18-push-offset-reset-fixture");
        }

        private void initialize() {
            searches = new ArrayList<>();
            resets = new ArrayList<>();
            heartbeatCalls = 0;
            pullMessageService = new RecordingPullMessageService();
            this.admin = new MQAdminImpl(this) {
                @Override
                public long searchOffset(MessageQueue queue, long timestamp) throws MQClientException {
                    searches.add(queue.getTopic() + "|" + queue.getBrokerName() + "|"
                        + queue.getQueueId() + "|" + timestamp);
                    return 11;
                }
            };
        }

        @Override
        public MQAdminImpl getMQAdminImpl() {
            return admin;
        }

        @Override
        public PullMessageService getPullMessageService() {
            return pullMessageService;
        }

        @Override
        public ConsumerStatsManager getConsumerStatsManager() {
            return stats;
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

        @Override
        public boolean sendHeartbeatToAllBrokerWithLock() {
            heartbeatCalls++;
            return true;
        }

        @Override
        public boolean sendHeartbeatToAllBrokerWithLockV2(boolean isRebalance) {
            heartbeatCalls++;
            return true;
        }

        private void initializeStats() {
            statsScheduler = Executors.newSingleThreadScheduledExecutor();
            stats = new ConsumerStatsManager(statsScheduler);
        }

        private void shutdownStats() {
            if (statsScheduler != null) statsScheduler.shutdownNow();
        }

        @Override
        public synchronized void resetOffset(
            String topic, String group, Map<MessageQueue, Long> offsetTable) {
            List<String> offsets = offsetTable.entrySet().stream()
                .map(entry -> entry.getKey().getTopic() + "|" + entry.getKey().getBrokerName()
                    + "|" + entry.getKey().getQueueId() + "|" + entry.getValue())
                .sorted()
                .collect(Collectors.toList());
            Map<String, Object> reset = new LinkedHashMap<>();
            reset.put("topic", topic);
            reset.put("group", group);
            reset.put("offsets", offsets);
            resets.add(reset);
        }
    }

    private static final class RecordingMqClientAPIImpl extends MQClientAPIImpl {
        private List<String> consumerGroups;

        private RecordingMqClientAPIImpl() {
            super(null, null, null, null, null, null);
        }

        private void initialize() {
            consumerGroups = new ArrayList<>();
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
        }
    }

    private static final class RecordingDefaultMQProducer extends DefaultMQProducer {
        private final List<String> sentTopics = new ArrayList<>();

        private RecordingDefaultMQProducer(String namespace) {
            super("phase18-send-back-fixture");
            setNamespace(namespace);
        }

        @Override
        public SendResult send(Message message) {
            sentTopics.add(withNamespace(message.getTopic()));
            return null;
        }
    }

    private static final class RecordingPullMessageService extends PullMessageService {
        private int immediateTaskCalls;
        private final List<Long> delayedTaskMillis = new ArrayList<>();

        private RecordingPullMessageService() {
            super(null);
        }

        @Override
        public void executeTask(Runnable task) {
            immediateTaskCalls++;
            task.run();
        }

        @Override
        public void executeTaskLater(Runnable task, long delayMillis) {
            delayedTaskMillis.add(delayMillis);
            task.run();
        }
    }
}
