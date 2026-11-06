package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPushConsumer;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.LitePullConsumer;
import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.MQHelper;
import org.apache.rocketmq.client.impl.consumer.PopProcessQueue;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.PopRequest;
import org.apache.rocketmq.client.impl.consumer.PullRequest;
import org.apache.rocketmq.client.impl.consumer.RebalanceImpl;
import org.apache.rocketmq.client.impl.consumer.RebalancePullImpl;
import org.apache.rocketmq.client.impl.consumer.RebalancePushImpl;
import org.apache.rocketmq.remoting.protocol.body.PopProcessQueueInfo;
import org.apache.rocketmq.remoting.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

/** Pins the complete public Java consumer-facade and inherited API denominator. */
public final class ConsumerFacadeApiFixtures {
    private ConsumerFacadeApiFixtures() { }

    private static String constructorSignature(Constructor<?> constructor) {
        return constructor.getDeclaringClass().getName() + "("
            + Arrays.stream(constructor.getParameterTypes()).map(Class::getName)
                .collect(Collectors.joining(",")) + ")";
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getName() + " " + method.getDeclaringClass().getName() + "."
            + method.getName() + "(" + Arrays.stream(method.getParameterTypes()).map(Class::getName)
                .collect(Collectors.joining(",")) + ")";
    }

    private static Map<String, Object> publicApiSurface(Class<?> apiType) {
        List<String> constructors = Arrays.stream(apiType.getDeclaredConstructors())
            .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
            .map(ConsumerFacadeApiFixtures::constructorSignature).sorted().collect(Collectors.toList());
        List<String> declared = Arrays.stream(apiType.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(ConsumerFacadeApiFixtures::methodSignature).sorted().collect(Collectors.toList());
        List<String> callable = Arrays.stream(apiType.getMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getDeclaringClass() != Object.class)
            .map(ConsumerFacadeApiFixtures::methodSignature).distinct().sorted().collect(Collectors.toList());
        Map<String, Object> result = new TreeMap<>();
        result.put("className", apiType.getName());
        result.put("publicConstructors", constructors);
        result.put("declaredPublicMethods", declared);
        result.put("declaredPublicMethodCount", declared.size());
        result.put("publicMethods", callable);
        result.put("publicMethodCount", callable.size());
        return result;
    }

    private static Map<String, Object> popProcessQueueBehavior() {
        PopProcessQueue queue = new PopProcessQueue();
        queue.incFoundMsg(4);
        queue.decFoundMsg(-2);
        int ackResult = queue.ack();
        queue.setLastPopTimestamp(123L);
        queue.setDropped(true);

        PopProcessQueueInfo info = new PopProcessQueueInfo();
        queue.fillPopProcessQueueInfo(info);
        Map<String, Object> infoResult = new TreeMap<>();
        infoResult.put("waitAckCount", info.getWaitAckCount());
        infoResult.put("droped", info.isDroped());
        infoResult.put("lastPopTimestamp", info.getLastPopTimestamp());
        String nullInfoErrorClass = null;
        try {
            queue.fillPopProcessQueueInfo(null);
        } catch (Throwable error) {
            nullInfoErrorClass = error.getClass().getName();
        }
        Map<String, Object> result = new TreeMap<>();
        result.put("ackResult", ackResult);
        result.put("waitAckCount", queue.getWaiAckMsgCount());
        result.put("lastPopTimestamp", queue.getLastPopTimestamp());
        result.put("dropped", queue.isDropped());
        result.put("info", infoResult);
        result.put("nullInfoErrorClass", nullInfoErrorClass);
        result.put("toString", queue.toString());
        return result;
    }

    private static Map<String, Object> popRequestBehavior() {
        PopRequest empty = new PopRequest();
        Map<String, Object> defaults = new TreeMap<>();
        defaults.put("topic", empty.getTopic());
        defaults.put("consumerGroup", empty.getConsumerGroup());
        defaults.put("messageQueue", empty.getMessageQueue());
        defaults.put("popProcessQueue", empty.getPopProcessQueue());
        defaults.put("lockedFirst", empty.isLockedFirst());
        defaults.put("initMode", empty.getInitMode());
        defaults.put("mode", empty.getMessageRequestMode().name());
        defaults.put("hashCode", empty.hashCode());
        defaults.put("toString", empty.toString());

        PopRequest request = new PopRequest();
        MessageQueue queue = new MessageQueue("orders-😀", "broker-雪", 17);
        PopProcessQueue processQueue = new PopProcessQueue();
        request.setTopic("orders-😀");
        request.setConsumerGroup("group-雪");
        request.setMessageQueue(queue);
        request.setPopProcessQueue(processQueue);
        request.setLockedFirst(true);
        request.setInitMode(-7);

        PopRequest equalByValue = new PopRequest();
        equalByValue.setTopic("orders-😀");
        equalByValue.setConsumerGroup("group-雪");
        equalByValue.setMessageQueue(new MessageQueue("orders-😀", "broker-雪", 17));
        equalByValue.setPopProcessQueue(new PopProcessQueue());

        Map<String, Object> populated = new TreeMap<>();
        populated.put("topic", request.getTopic());
        populated.put("consumerGroup", request.getConsumerGroup());
        populated.put("messageQueue", request.getMessageQueue().toString());
        populated.put("messageQueuePreservesIdentity", request.getMessageQueue() == queue);
        populated.put("popProcessQueuePreservesIdentity", request.getPopProcessQueue() == processQueue);
        populated.put("lockedFirst", request.isLockedFirst());
        populated.put("initMode", request.getInitMode());
        populated.put("mode", request.getMessageRequestMode().name());
        populated.put("hashCode", request.hashCode());
        populated.put("toString", request.toString());
        populated.put("equalsIgnoresProcessQueueAndFlags", request.equals(equalByValue));
        populated.put("equalHashCode", equalByValue.hashCode());

        queue.setTopic("orders-mutated");
        populated.put("topicAfterMessageQueueMutation", request.getMessageQueue().getTopic());
        populated.put("toStringAfterMessageQueueMutation", request.toString());

        Map<String, Object> result = new TreeMap<>();
        result.put("defaults", defaults);
        result.put("populated", populated);
        return result;
    }

    private static Map<String, Object> pullRequestBehavior() {
        PullRequest empty = new PullRequest();
        Map<String, Object> defaults = new TreeMap<>();
        defaults.put("consumerGroup", empty.getConsumerGroup());
        defaults.put("messageQueue", empty.getMessageQueue());
        defaults.put("processQueue", empty.getProcessQueue());
        defaults.put("nextOffset", empty.getNextOffset());
        defaults.put("previouslyLocked", empty.isPreviouslyLocked());
        defaults.put("mode", empty.getMessageRequestMode().name());
        defaults.put("hashCode", empty.hashCode());
        defaults.put("toString", empty.toString());

        PullRequest request = new PullRequest();
        MessageQueue queue = new MessageQueue("orders-😀", "broker-雪", 17);
        ProcessQueue processQueue = new ProcessQueue();
        request.setConsumerGroup("group-雪");
        request.setMessageQueue(queue);
        request.setProcessQueue(processQueue);
        request.setNextOffset(-42L);
        request.setPreviouslyLocked(true);

        PullRequest equalByValue = new PullRequest();
        equalByValue.setConsumerGroup("group-雪");
        equalByValue.setMessageQueue(new MessageQueue("orders-😀", "broker-雪", 17));
        equalByValue.setProcessQueue(new ProcessQueue());

        Map<String, Object> populated = new TreeMap<>();
        populated.put("consumerGroup", request.getConsumerGroup());
        populated.put("messageQueue", request.getMessageQueue().toString());
        populated.put("messageQueuePreservesIdentity", request.getMessageQueue() == queue);
        populated.put("processQueuePreservesIdentity", request.getProcessQueue() == processQueue);
        populated.put("nextOffset", request.getNextOffset());
        populated.put("previouslyLocked", request.isPreviouslyLocked());
        populated.put("mode", request.getMessageRequestMode().name());
        populated.put("hashCode", request.hashCode());
        populated.put("toString", request.toString());
        populated.put("equalsIgnoresProcessQueueAndFlags", request.equals(equalByValue));
        populated.put("equalHashCode", equalByValue.hashCode());

        queue.setTopic("orders-mutated");
        populated.put("topicAfterMessageQueueMutation", request.getMessageQueue().getTopic());
        populated.put("toStringAfterMessageQueueMutation", request.toString());

        Map<String, Object> result = new TreeMap<>();
        result.put("defaults", defaults);
        result.put("populated", populated);
        return result;
    }

    private static Map<String, Object> rebalancePullBehavior() {
        RebalancePullImpl rebalance = new RebalancePullImpl("pull-group",
            MessageModel.CLUSTERING, null, null, null);
        rebalance.setConsumerGroup("pull-group-updated");
        rebalance.setMessageModel(MessageModel.BROADCASTING);
        AllocateMessageQueueAveragely strategy = new AllocateMessageQueueAveragely();
        rebalance.setAllocateMessageQueueStrategy(strategy);
        rebalance.dispatchPullRequest(List.of(new PullRequest()), 500L);
        rebalance.dispatchPopPullRequest(List.of(new PopRequest()), 500L);

        Map<String, Object> result = new TreeMap<>();
        result.put("groupAfterSet", rebalance.getConsumerGroup());
        result.put("messageModelAfterSet", rebalance.getMessageModel().name());
        result.put("strategyPreservesIdentity", rebalance.getAllocateMessageQueueStrategy() == strategy);
        result.put("clientRebalance", rebalance.clientRebalance("orders"));
        result.put("consumeType", rebalance.consumeType().name());
        result.put("pullOffset", rebalance.computePullFromWhere(null));
        try {
            result.put("pullOffsetWithException", rebalance.computePullFromWhereWithException(null));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        result.put("processQueueCreated", rebalance.createProcessQueue() != null);
        result.put("popProcessQueueCreated", rebalance.createPopProcessQueue() != null);
        try {
            rebalance.getConsumeInitMode();
            result.put("consumeInitModeError", null);
        } catch (UnsupportedOperationException e) {
            result.put("consumeInitModeError", e.getMessage());
        }
        return result;
    }

    private static Map<String, Object> rebalancePushBehavior() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("push-group");
        DefaultMQPushConsumerImpl implementation = new DefaultMQPushConsumerImpl(consumer, null);
        RebalancePushImpl rebalance = new RebalancePushImpl("push-group",
            MessageModel.CLUSTERING, consumer.getAllocateMessageQueueStrategy(), null, implementation);
        boolean clientRebalanceDefault = rebalance.clientRebalance("orders");
        consumer.setClientRebalance(false);
        boolean clientRebalanceClusteringDisabled = rebalance.clientRebalance("orders");
        rebalance.setMessageModel(MessageModel.BROADCASTING);
        boolean clientRebalanceBroadcasting = rebalance.clientRebalance("orders");
        rebalance.setMessageModel(MessageModel.CLUSTERING);
        rebalance.setConsumerGroup("push-group-updated");
        boolean strategyPreservesIdentity =
            rebalance.getAllocateMessageQueueStrategy() == consumer.getAllocateMessageQueueStrategy();
        boolean doRebalanceEmpty = rebalance.doRebalance(false);

        MessageQueue queue = new MessageQueue("orders", "broker-a", 0);
        ProcessQueue processQueue = rebalance.createProcessQueue();
        PopProcessQueue popProcessQueue = rebalance.createPopProcessQueue();
        rebalance.getProcessQueueTable().put(queue, processQueue);
        rebalance.getPopProcessQueueTable().put(queue, popProcessQueue);
        boolean removeUnnecessaryPop =
            rebalance.removeUnnecessaryPopMessageQueue(queue, popProcessQueue);
        rebalance.destroy();

        Map<String, Object> result = new TreeMap<>();
        result.put("clientRebalanceDefault", clientRebalanceDefault);
        result.put("clientRebalanceClusteringDisabled", clientRebalanceClusteringDisabled);
        result.put("clientRebalanceBroadcasting", clientRebalanceBroadcasting);
        result.put("groupAfterSet", rebalance.getConsumerGroup());
        result.put("messageModelAfterSet", rebalance.getMessageModel().name());
        result.put("strategyPreservesIdentity", strategyPreservesIdentity);
        result.put("consumeType", rebalance.consumeType().name());
        result.put("consumeInitMode", rebalance.getConsumeInitMode());
        result.put("doRebalanceEmpty", doRebalanceEmpty);
        result.put("processQueueCreated", processQueue != null);
        result.put("popProcessQueueCreated", popProcessQueue != null);
        result.put("removeUnnecessaryPop", removeUnnecessaryPop);
        result.put("processQueueDroppedByDestroy", processQueue.isDropped());
        result.put("popProcessQueueDroppedByDestroy", popProcessQueue.isDropped());
        result.put("processTableEmptyAfterDestroy", rebalance.getProcessQueueTable().isEmpty());
        result.put("popTableEmptyAfterDestroy", rebalance.getPopProcessQueueTable().isEmpty());
        return result;
    }

    private static MessageExt processQueueMessage(long offset, byte[] body) {
        MessageExt message = new MessageExt();
        message.setTopic("orders");
        message.setQueueId(0);
        message.setQueueOffset(offset);
        message.setBody(body);
        return message;
    }

    private static Map<String, Object> processQueueBehavior() {
        ProcessQueue queue = new ProcessQueue();
        MessageExt first = processQueueMessage(10L, new byte[] {1, 2});
        MessageExt second = processQueueMessage(11L, new byte[] {3});
        boolean dispatch = queue.putMessage(Arrays.asList(first, second));
        List<Long> initialTreeKeys = new ArrayList<>(queue.getMsgTreeMap().keySet());
        long initialCount = queue.getMsgCount().get();
        long initialSize = queue.getMsgSize().get();
        long initialSpan = queue.getMaxSpan();
        boolean containsFirst = queue.containsMessage(first);
        boolean containsNull = queue.containsMessage(null);
        boolean hasMessages = queue.hasTempMessage();
        long firstTaken = queue.takeMessages(1).get(0).getQueueOffset();
        List<Long> afterTakeTreeKeys = new ArrayList<>(queue.getMsgTreeMap().keySet());
        queue.rollback();
        List<Long> afterRollbackTreeKeys = new ArrayList<>(queue.getMsgTreeMap().keySet());
        queue.takeMessages(1);
        long committedOffset = queue.commit();
        List<Long> afterCommitTreeKeys = new ArrayList<>(queue.getMsgTreeMap().keySet());
        long nextOffset = queue.removeMessage(Arrays.asList(second));
        List<Long> finalTreeKeys = new ArrayList<>(queue.getMsgTreeMap().keySet());

        queue.setDropped(true);
        queue.setLocked(true);
        queue.setLastLockTimestamp(123L);
        queue.setLastPullTimestamp(124L);
        queue.setLastConsumeTimestamp(150L);
        queue.setMsgAccCnt(17L);
        queue.incTryUnlockTimes();
        queue.incTryUnlockTimes();
        ProcessQueueInfo info = new ProcessQueueInfo();
        queue.fillProcessQueueInfo(info);
        boolean nullInfoReturned;
        try {
            queue.fillProcessQueueInfo(null);
            nullInfoReturned = true;
        } catch (Throwable throwable) {
            nullInfoReturned = false;
        }

        Map<String, Object> infoResult = new TreeMap<>();
        infoResult.put("cachedMsgCount", info.getCachedMsgCount());
        infoResult.put("cachedMsgSizeInMiB", info.getCachedMsgSizeInMiB());
        infoResult.put("locked", info.isLocked());
        infoResult.put("tryUnlockTimes", info.getTryUnlockTimes());
        infoResult.put("lastLockTimestamp", info.getLastLockTimestamp());
        infoResult.put("droped", info.isDroped());
        infoResult.put("lastPullTimestamp", info.getLastPullTimestamp());
        infoResult.put("lastConsumeTimestamp", info.getLastConsumeTimestamp());

        ProcessQueue sizeNarrowingQueue = new ProcessQueue();
        sizeNarrowingQueue.getMsgSize().set(2_147_483_648L * 1024L * 1024L);
        ProcessQueueInfo sizeNarrowingInfo = new ProcessQueueInfo();
        sizeNarrowingQueue.fillProcessQueueInfo(sizeNarrowingInfo);

        Map<String, Object> result = new TreeMap<>();
        result.put("dispatch", dispatch);
        result.put("initialTreeKeys", initialTreeKeys);
        result.put("initialCount", initialCount);
        result.put("initialSize", initialSize);
        result.put("initialSpan", initialSpan);
        result.put("containsFirst", containsFirst);
        result.put("containsNull", containsNull);
        result.put("hasMessages", hasMessages);
        result.put("firstTaken", firstTaken);
        result.put("afterTakeTreeKeys", afterTakeTreeKeys);
        result.put("afterRollbackTreeKeys", afterRollbackTreeKeys);
        result.put("committedOffset", committedOffset);
        result.put("afterCommitTreeKeys", afterCommitTreeKeys);
        result.put("nextOffset", nextOffset);
        result.put("finalTreeKeys", finalTreeKeys);
        result.put("finalCount", queue.getMsgCount().get());
        result.put("finalSize", queue.getMsgSize().get());
        result.put("lastLockTimestamp", queue.getLastLockTimestamp());
        result.put("lastPullTimestamp", queue.getLastPullTimestamp());
        result.put("lockExpired", queue.isLockExpired());
        result.put("pullExpired", queue.isPullExpired());
        result.put("lastConsumeTimestamp", queue.getLastConsumeTimestamp());
        result.put("msgAccCnt", queue.getMsgAccCnt());
        result.put("tryUnlockTimes", queue.getTryUnlockTimes());
        result.put("dropped", queue.isDropped());
        result.put("locked", queue.isLocked());
        result.put("consumeLockPresent", queue.getConsumeLock() != null);
        result.put("info", infoResult);
        result.put("nullInfoReturned", nullInfoReturned);
        result.put("sizeMiBNarrowing", sizeNarrowingInfo.getCachedMsgSizeInMiB());

        ProcessQueue nullMessageListQueue = new ProcessQueue();
        String nullMessageListErrorClass = null;
        try {
            nullMessageListQueue.putMessage(null);
        } catch (Throwable error) {
            nullMessageListErrorClass = error.getClass().getName();
        }
        Map<String, Object> nullMessageList = new TreeMap<>();
        nullMessageList.put("errorClass", nullMessageListErrorClass);
        nullMessageList.put("count", nullMessageListQueue.getMsgCount().get());
        nullMessageList.put("size", nullMessageListQueue.getMsgSize().get());
        nullMessageList.put("treeKeys", new ArrayList<>(nullMessageListQueue.getMsgTreeMap().keySet()));
        result.put("nullMessageList", nullMessageList);

        ProcessQueue nullRemoveMessageQueue = new ProcessQueue();
        nullRemoveMessageQueue.putMessage(
            List.of(processQueueMessage(230L, new byte[] {2, 3, 0})));
        nullRemoveMessageQueue.setLastConsumeTimestamp(777L);
        long nullRemoveMessageNextOffset = nullRemoveMessageQueue.removeMessage(null);
        Map<String, Object> nullRemoveMessage = new TreeMap<>();
        nullRemoveMessage.put("nextOffset", nullRemoveMessageNextOffset);
        nullRemoveMessage.put("timestampUpdated", nullRemoveMessageQueue.getLastConsumeTimestamp() > 777L);
        nullRemoveMessage.put("count", nullRemoveMessageQueue.getMsgCount().get());
        nullRemoveMessage.put("size", nullRemoveMessageQueue.getMsgSize().get());
        nullRemoveMessage.put("treeKeys", new ArrayList<>(nullRemoveMessageQueue.getMsgTreeMap().keySet()));
        result.put("nullRemoveMessage", nullRemoveMessage);

        ProcessQueue nullRequeueMessageQueue = new ProcessQueue();
        nullRequeueMessageQueue.putMessage(
            List.of(processQueueMessage(231L, new byte[] {4, 5})));
        String nullRequeueMessageErrorClass = null;
        try {
            nullRequeueMessageQueue.makeMessageToConsumeAgain(null);
        } catch (Throwable error) {
            nullRequeueMessageErrorClass = error.getClass().getName();
        }
        Map<String, Object> nullRequeueMessage = new TreeMap<>();
        nullRequeueMessage.put("errorClass", nullRequeueMessageErrorClass);
        nullRequeueMessage.put("count", nullRequeueMessageQueue.getMsgCount().get());
        nullRequeueMessage.put("size", nullRequeueMessageQueue.getMsgSize().get());
        nullRequeueMessage.put("treeKeys", new ArrayList<>(nullRequeueMessageQueue.getMsgTreeMap().keySet()));
        result.put("nullRequeueMessage", nullRequeueMessage);

        ProcessQueue nullMessageElementQueue = new ProcessQueue();
        String nullMessageElementErrorClass = null;
        try {
            nullMessageElementQueue.putMessage(Arrays.asList(
                processQueueMessage(232L, new byte[] {6, 7, 8}), null));
        } catch (Throwable error) {
            nullMessageElementErrorClass = error.getClass().getName();
        }
        Map<String, Object> nullMessageElement = new TreeMap<>();
        nullMessageElement.put("errorClass", nullMessageElementErrorClass);
        nullMessageElement.put("countAfterError", nullMessageElementQueue.getMsgCount().get());
        nullMessageElement.put("sizeAfterError", nullMessageElementQueue.getMsgSize().get());
        nullMessageElement.put("treeKeysAfterError",
            new ArrayList<>(nullMessageElementQueue.getMsgTreeMap().keySet()));
        boolean nullMessageElementNextDispatch = nullMessageElementQueue.putMessage(
            List.of(processQueueMessage(233L, new byte[] {8})));
        nullMessageElement.put("nextDispatch", nullMessageElementNextDispatch);
        nullMessageElement.put("finalCount", nullMessageElementQueue.getMsgCount().get());
        nullMessageElement.put("finalSize", nullMessageElementQueue.getMsgSize().get());
        nullMessageElement.put("finalTreeKeys",
            new ArrayList<>(nullMessageElementQueue.getMsgTreeMap().keySet()));
        result.put("nullMessageElement", nullMessageElement);

        ProcessQueue nullRequeueElementQueue = new ProcessQueue();
        MessageExt nullRequeuePrefix = processQueueMessage(234L, new byte[] {9, 10});
        nullRequeueElementQueue.putMessage(List.of(nullRequeuePrefix));
        nullRequeueElementQueue.takeMessages(1);
        String nullRequeueElementErrorClass = null;
        try {
            nullRequeueElementQueue.makeMessageToConsumeAgain(
                Arrays.asList(nullRequeuePrefix, null));
        } catch (Throwable error) {
            nullRequeueElementErrorClass = error.getClass().getName();
        }
        Map<String, Object> nullRequeueElement = new TreeMap<>();
        nullRequeueElement.put("errorClass", nullRequeueElementErrorClass);
        nullRequeueElement.put("countAfterError", nullRequeueElementQueue.getMsgCount().get());
        nullRequeueElement.put("sizeAfterError", nullRequeueElementQueue.getMsgSize().get());
        nullRequeueElement.put("treeKeysAfterError",
            new ArrayList<>(nullRequeueElementQueue.getMsgTreeMap().keySet()));
        boolean nullRequeueElementNextDispatch = nullRequeueElementQueue.putMessage(
            List.of(processQueueMessage(235L, new byte[] {11})));
        nullRequeueElement.put("nextDispatch", nullRequeueElementNextDispatch);
        nullRequeueElement.put("finalCount", nullRequeueElementQueue.getMsgCount().get());
        nullRequeueElement.put("finalSize", nullRequeueElementQueue.getMsgSize().get());
        nullRequeueElement.put("finalTreeKeys",
            new ArrayList<>(nullRequeueElementQueue.getMsgTreeMap().keySet()));
        result.put("nullRequeueElement", nullRequeueElement);

        ProcessQueue nullRemoveElementQueue = new ProcessQueue();
        MessageExt nullRemoveFirst = processQueueMessage(240L, new byte[] {1, 2});
        nullRemoveElementQueue.putMessage(Arrays.asList(
            nullRemoveFirst, processQueueMessage(241L, new byte[] {3})));
        nullRemoveElementQueue.setLastConsumeTimestamp(777L);
        long nullRemoveElementNextOffset = nullRemoveElementQueue.removeMessage(
            Arrays.asList(nullRemoveFirst, null));
        Map<String, Object> nullRemoveElement = new TreeMap<>();
        nullRemoveElement.put("returnedNormally", true);
        nullRemoveElement.put("nextOffset", nullRemoveElementNextOffset);
        nullRemoveElement.put("timestampUpdated", nullRemoveElementQueue.getLastConsumeTimestamp() > 777L);
        nullRemoveElement.put("countAfterError", nullRemoveElementQueue.getMsgCount().get());
        nullRemoveElement.put("sizeAfterError", nullRemoveElementQueue.getMsgSize().get());
        nullRemoveElement.put("treeKeysAfterError",
            new ArrayList<>(nullRemoveElementQueue.getMsgTreeMap().keySet()));
        boolean nullRemoveElementNextDispatch = nullRemoveElementQueue.putMessage(
            List.of(processQueueMessage(242L, new byte[] {4})));
        nullRemoveElement.put("nextDispatch", nullRemoveElementNextDispatch);
        nullRemoveElement.put("finalCount", nullRemoveElementQueue.getMsgCount().get());
        nullRemoveElement.put("finalSize", nullRemoveElementQueue.getMsgSize().get());
        nullRemoveElement.put("finalTreeKeys",
            new ArrayList<>(nullRemoveElementQueue.getMsgTreeMap().keySet()));
        result.put("nullRemoveElement", nullRemoveElement);

        ProcessQueue negativeBatchQueue = new ProcessQueue();
        negativeBatchQueue.setLastConsumeTimestamp(777L);
        Map<String, Object> negativeBatchResult = new TreeMap<>();
        try {
            negativeBatchQueue.takeMessages(-1);
            negativeBatchResult.put("error", null);
        } catch (IllegalArgumentException e) {
            negativeBatchResult.put("error", e.toString());
        }
        negativeBatchResult.put("lastConsumeTimestamp", negativeBatchQueue.getLastConsumeTimestamp());
        result.put("negativeBatchSize", negativeBatchResult);
        queue.clear();

        ProcessQueue duplicateQueue = new ProcessQueue();
        MessageExt originalDuplicate = processQueueMessage(30L, new byte[] {7, 8});
        MessageExt replacementDuplicate = processQueueMessage(30L, new byte[] {9, 10, 11, 12});
        duplicateQueue.putMessage(Arrays.asList(originalDuplicate));
        boolean duplicateDispatch = duplicateQueue.putMessage(Arrays.asList(replacementDuplicate));
        result.put("duplicateDispatch", duplicateDispatch);
        result.put("duplicateCount", duplicateQueue.getMsgCount().get());
        result.put("duplicateSize", duplicateQueue.getMsgSize().get());
        result.put("duplicateBodySize", duplicateQueue.getMsgTreeMap().get(30L).getBody().length);

        ProcessQueue droppedQueue = new ProcessQueue();
        droppedQueue.setDropped(true);
        boolean droppedDispatch = droppedQueue.putMessage(
            Arrays.asList(processQueueMessage(40L, new byte[] {1})));
        result.put("droppedDispatch", droppedDispatch);
        result.put("droppedQueueCount", droppedQueue.getMsgCount().get());

        ProcessQueue inFlightRemovalQueue = new ProcessQueue();
        MessageExt inFlightMessage = processQueueMessage(50L, new byte[] {1, 2});
        inFlightRemovalQueue.putMessage(Arrays.asList(
            inFlightMessage, processQueueMessage(51L, new byte[] {3, 4, 5})));
        List<MessageExt> inFlightMessages = inFlightRemovalQueue.takeMessages(1);
        long removeInFlightNextOffset = inFlightRemovalQueue.removeMessage(inFlightMessages);
        Map<String, Object> removeInFlight = new TreeMap<>();
        removeInFlight.put("nextOffset", removeInFlightNextOffset);
        removeInFlight.put("readyTreeKeys", new ArrayList<>(inFlightRemovalQueue.getMsgTreeMap().keySet()));
        removeInFlight.put("countBeforeCommit", inFlightRemovalQueue.getMsgCount().get());
        removeInFlight.put("sizeBeforeCommit", inFlightRemovalQueue.getMsgSize().get());
        removeInFlight.put("commitOffset", inFlightRemovalQueue.commit());
        removeInFlight.put("countAfterCommit", inFlightRemovalQueue.getMsgCount().get());
        removeInFlight.put("sizeAfterCommit", inFlightRemovalQueue.getMsgSize().get());
        result.put("removeInFlight", removeInFlight);

        ProcessQueue dispatchAfterRemoveQueue = new ProcessQueue();
        dispatchAfterRemoveQueue.setLastPullTimestamp(500L);
        MessageExt removedMessage = processQueueMessage(60L, new byte[] {6});
        dispatchAfterRemoveQueue.putMessage(Arrays.asList(removedMessage));
        long removeLastNextOffset = dispatchAfterRemoveQueue.removeMessage(Arrays.asList(removedMessage));
        boolean dispatchAfterRemove = dispatchAfterRemoveQueue.putMessage(
            Arrays.asList(processQueueMessage(61L, new byte[] {7, 8})));
        Map<String, Object> dispatchAfterRemoveResult = new TreeMap<>();
        dispatchAfterRemoveResult.put("removeNextOffset", removeLastNextOffset);
        dispatchAfterRemoveResult.put("nextDispatch", dispatchAfterRemove);
        dispatchAfterRemoveResult.put("count", dispatchAfterRemoveQueue.getMsgCount().get());
        dispatchAfterRemoveResult.put("size", dispatchAfterRemoveQueue.getMsgSize().get());
        dispatchAfterRemoveResult.put("treeKeys", new ArrayList<>(dispatchAfterRemoveQueue.getMsgTreeMap().keySet()));
        dispatchAfterRemoveResult.put("lastPullTimestamp", dispatchAfterRemoveQueue.getLastPullTimestamp());
        result.put("dispatchAfterRemove", dispatchAfterRemoveResult);

        ProcessQueue malformedMaxOffsetQueue = new ProcessQueue();
        MessageExt malformedMaxOffsetMessage = processQueueMessage(70L, new byte[] {7, 0});
        MessageAccessor.putProperty(
            malformedMaxOffsetMessage, MessageConst.PROPERTY_MAX_OFFSET, "not-a-long");
        String maxOffsetError = "";
        try {
            malformedMaxOffsetQueue.putMessage(Arrays.asList(malformedMaxOffsetMessage));
        } catch (NumberFormatException e) {
            maxOffsetError = e.getClass().getName();
        }
        Map<String, Object> malformedMaxOffset = new TreeMap<>();
        malformedMaxOffset.put("errorClass", maxOffsetError);
        malformedMaxOffset.put("countAfterError", malformedMaxOffsetQueue.getMsgCount().get());
        malformedMaxOffset.put("sizeAfterError", malformedMaxOffsetQueue.getMsgSize().get());
        malformedMaxOffset.put("treeKeysAfterError", new ArrayList<>(malformedMaxOffsetQueue.getMsgTreeMap().keySet()));
        boolean dispatchAfterError = malformedMaxOffsetQueue.putMessage(
            Arrays.asList(processQueueMessage(71L, new byte[] {7, 1})));
        malformedMaxOffset.put("dispatchAfterError", dispatchAfterError);
        malformedMaxOffset.put("finalCount", malformedMaxOffsetQueue.getMsgCount().get());
        malformedMaxOffset.put("finalSize", malformedMaxOffsetQueue.getMsgSize().get());
        malformedMaxOffset.put("finalTreeKeys", new ArrayList<>(malformedMaxOffsetQueue.getMsgTreeMap().keySet()));
        result.put("malformedMaxOffset", malformedMaxOffset);

        ProcessQueue emptyCommitQueue = new ProcessQueue();
        String emptyCommitError = "";
        try {
            emptyCommitQueue.commit();
        } catch (java.util.NoSuchElementException e) {
            emptyCommitError = e.getClass().getName();
        }
        Map<String, Object> emptyCommit = new TreeMap<>();
        emptyCommit.put("errorClass", emptyCommitError);
        emptyCommit.put("count", emptyCommitQueue.getMsgCount().get());
        emptyCommit.put("size", emptyCommitQueue.getMsgSize().get());
        emptyCommit.put("treeKeys", new ArrayList<>(emptyCommitQueue.getMsgTreeMap().keySet()));
        result.put("emptyCommit", emptyCommit);

        ProcessQueue commitLifecycleQueue = new ProcessQueue();
        commitLifecycleQueue.putMessage(Arrays.asList(processQueueMessage(80L, new byte[] {8})));
        commitLifecycleQueue.takeMessages(1);
        commitLifecycleQueue.setLastConsumeTimestamp(801L);
        long commitLifecycleOffset = commitLifecycleQueue.commit();
        boolean commitLifecycleDispatch = commitLifecycleQueue.putMessage(
            Arrays.asList(processQueueMessage(81L, new byte[] {8, 1})));
        Map<String, Object> commitLifecycle = new TreeMap<>();
        commitLifecycle.put("commitOffset", commitLifecycleOffset);
        commitLifecycle.put("nextDispatch", commitLifecycleDispatch);
        commitLifecycle.put("lastConsumeTimestamp", commitLifecycleQueue.getLastConsumeTimestamp());
        commitLifecycle.put("count", commitLifecycleQueue.getMsgCount().get());
        commitLifecycle.put("size", commitLifecycleQueue.getMsgSize().get());
        commitLifecycle.put("treeKeys", new ArrayList<>(commitLifecycleQueue.getMsgTreeMap().keySet()));
        result.put("commitLifecycle", commitLifecycle);

        ProcessQueue rollbackLifecycleQueue = new ProcessQueue();
        MessageExt rollbackLifecycleMessage = processQueueMessage(90L, new byte[] {9});
        rollbackLifecycleQueue.putMessage(Arrays.asList(rollbackLifecycleMessage));
        rollbackLifecycleQueue.takeMessages(1);
        rollbackLifecycleQueue.takeMessages(1);
        rollbackLifecycleQueue.rollback();
        boolean rollbackLifecycleDispatch = rollbackLifecycleQueue.putMessage(
            Arrays.asList(processQueueMessage(91L, new byte[] {9, 1})));
        Map<String, Object> rollbackLifecycle = new TreeMap<>();
        rollbackLifecycle.put("nextDispatch", rollbackLifecycleDispatch);
        rollbackLifecycle.put("count", rollbackLifecycleQueue.getMsgCount().get());
        rollbackLifecycle.put("size", rollbackLifecycleQueue.getMsgSize().get());
        rollbackLifecycle.put("treeKeys", new ArrayList<>(rollbackLifecycleQueue.getMsgTreeMap().keySet()));
        result.put("rollbackLifecycle", rollbackLifecycle);

        ProcessQueue requeueLifecycleQueue = new ProcessQueue();
        MessageExt requeueLifecycleMessage = processQueueMessage(100L, new byte[] {10});
        requeueLifecycleQueue.putMessage(Arrays.asList(requeueLifecycleMessage));
        requeueLifecycleQueue.takeMessages(1);
        requeueLifecycleQueue.takeMessages(1);
        requeueLifecycleQueue.makeMessageToConsumeAgain(Arrays.asList(requeueLifecycleMessage));
        requeueLifecycleQueue.takeMessages(1);
        boolean requeueLifecycleDispatch = requeueLifecycleQueue.putMessage(
            Arrays.asList(processQueueMessage(101L, new byte[] {10, 1})));
        Map<String, Object> requeueLifecycle = new TreeMap<>();
        requeueLifecycle.put("nextDispatch", requeueLifecycleDispatch);
        requeueLifecycle.put("count", requeueLifecycleQueue.getMsgCount().get());
        requeueLifecycle.put("size", requeueLifecycleQueue.getMsgSize().get());
        requeueLifecycle.put("treeKeys", new ArrayList<>(requeueLifecycleQueue.getMsgTreeMap().keySet()));
        result.put("requeueLifecycle", requeueLifecycle);

        ProcessQueue duplicateInFlightQueue = new ProcessQueue();
        duplicateInFlightQueue.putMessage(
            Arrays.asList(processQueueMessage(110L, new byte[] {1, 2})));
        duplicateInFlightQueue.takeMessages(1);
        MessageExt duplicateReadyMessage = processQueueMessage(110L, new byte[] {3, 4, 5, 6});
        boolean duplicateInFlightDispatch = duplicateInFlightQueue.putMessage(
            Arrays.asList(duplicateReadyMessage));
        Map<String, Object> duplicateInFlightAfterPut = new TreeMap<>();
        duplicateInFlightAfterPut.put("dispatch", duplicateInFlightDispatch);
        duplicateInFlightAfterPut.put("count", duplicateInFlightQueue.getMsgCount().get());
        duplicateInFlightAfterPut.put("size", duplicateInFlightQueue.getMsgSize().get());
        duplicateInFlightAfterPut.put("treeKeys", new ArrayList<>(duplicateInFlightQueue.getMsgTreeMap().keySet()));
        duplicateInFlightAfterPut.put("readyBodySize",
            duplicateInFlightQueue.getMsgTreeMap().get(110L).getBody().length);
        long duplicateInFlightCommitOffset = duplicateInFlightQueue.commit();
        Map<String, Object> duplicateInFlightAfterCommit = new TreeMap<>();
        duplicateInFlightAfterCommit.put("commitOffset", duplicateInFlightCommitOffset);
        duplicateInFlightAfterCommit.put("count", duplicateInFlightQueue.getMsgCount().get());
        duplicateInFlightAfterCommit.put("size", duplicateInFlightQueue.getMsgSize().get());
        duplicateInFlightAfterCommit.put("treeKeys", new ArrayList<>(duplicateInFlightQueue.getMsgTreeMap().keySet()));
        duplicateInFlightAfterCommit.put("readyBodySize",
            duplicateInFlightQueue.getMsgTreeMap().get(110L).getBody().length);
        Map<String, Object> duplicateInFlight = new TreeMap<>();
        duplicateInFlight.put("afterPut", duplicateInFlightAfterPut);
        duplicateInFlight.put("afterCommit", duplicateInFlightAfterCommit);
        result.put("duplicateInFlight", duplicateInFlight);

        ProcessQueue duplicateRollbackQueue = new ProcessQueue();
        duplicateRollbackQueue.putMessage(
            Arrays.asList(processQueueMessage(120L, new byte[] {1, 2})));
        duplicateRollbackQueue.takeMessages(1);
        duplicateRollbackQueue.putMessage(
            Arrays.asList(processQueueMessage(120L, new byte[] {3, 4, 5, 6})));
        duplicateRollbackQueue.rollback();
        Map<String, Object> duplicateRollback = new TreeMap<>();
        duplicateRollback.put("count", duplicateRollbackQueue.getMsgCount().get());
        duplicateRollback.put("size", duplicateRollbackQueue.getMsgSize().get());
        duplicateRollback.put("treeKeys", new ArrayList<>(duplicateRollbackQueue.getMsgTreeMap().keySet()));
        duplicateRollback.put("readyBodySize",
            duplicateRollbackQueue.getMsgTreeMap().get(120L).getBody().length);
        result.put("duplicateRollback", duplicateRollback);

        ProcessQueue duplicateRemovalQueue = new ProcessQueue();
        duplicateRemovalQueue.putMessage(
            Arrays.asList(processQueueMessage(130L, new byte[] {1, 2})));
        duplicateRemovalQueue.takeMessages(1);
        MessageExt duplicateForRemoval = processQueueMessage(130L, new byte[] {3, 4, 5, 6});
        duplicateRemovalQueue.putMessage(Arrays.asList(duplicateForRemoval));
        long duplicateRemovalNextOffset = duplicateRemovalQueue.removeMessage(
            Arrays.asList(duplicateForRemoval));
        long duplicateRemovalCommitOffset = duplicateRemovalQueue.commit();
        Map<String, Object> duplicateRemoval = new TreeMap<>();
        duplicateRemoval.put("nextOffset", duplicateRemovalNextOffset);
        duplicateRemoval.put("commitOffset", duplicateRemovalCommitOffset);
        duplicateRemoval.put("count", duplicateRemovalQueue.getMsgCount().get());
        duplicateRemoval.put("size", duplicateRemovalQueue.getMsgSize().get());
        duplicateRemoval.put("treeKeys", new ArrayList<>(duplicateRemovalQueue.getMsgTreeMap().keySet()));
        result.put("duplicateRemoval", duplicateRemoval);

        ProcessQueue duplicateOffsetMaxQueue = new ProcessQueue();
        duplicateOffsetMaxQueue.putMessage(Arrays.asList(
            processQueueMessage(40L, new byte[] {4}),
            processQueueMessage(45L, new byte[] {5})));
        duplicateOffsetMaxQueue.putMessage(
            Arrays.asList(processQueueMessage(40L, new byte[] {4, 0})));
        long duplicateOffsetMaxNextOffset = duplicateOffsetMaxQueue.removeMessage(Arrays.asList(
            processQueueMessage(40L, new byte[] {4}),
            processQueueMessage(45L, new byte[] {5})));
        result.put("duplicateOffsetMaxNextOffset", duplicateOffsetMaxNextOffset);

        ProcessQueue clearLifecycleQueue = new ProcessQueue();
        clearLifecycleQueue.putMessage(Arrays.asList(processQueueMessage(140L, new byte[] {14})));
        clearLifecycleQueue.clear();
        boolean clearLifecycleDispatch = clearLifecycleQueue.putMessage(
            Arrays.asList(processQueueMessage(141L, new byte[] {14, 1})));
        Map<String, Object> clearLifecycle = new TreeMap<>();
        clearLifecycle.put("nextDispatch", clearLifecycleDispatch);
        clearLifecycle.put("count", clearLifecycleQueue.getMsgCount().get());
        clearLifecycle.put("size", clearLifecycleQueue.getMsgSize().get());
        clearLifecycle.put("treeKeys", new ArrayList<>(clearLifecycleQueue.getMsgTreeMap().keySet()));
        result.put("clearLifecycle", clearLifecycle);

        ProcessQueue mapQueue = new ProcessQueue();
        MessageExt original = processQueueMessage(20L, new byte[] {4});
        MessageExt direct = processQueueMessage(21L, new byte[] {5, 6});
        mapQueue.putMessage(Arrays.asList(original));
        long counterPrevious = mapQueue.getMsgCount().getAndAdd(4L);
        long counterSizeAfterAdd = mapQueue.getMsgSize().addAndGet(3L);
        mapQueue.getMsgTreeMap().put(21L, direct);
        mapQueue.getMsgTreeMap().remove(20L);
        result.put("directMapKeys", new ArrayList<>(mapQueue.getMsgTreeMap().keySet()));
        result.put("counterPrevious", counterPrevious);
        result.put("counterSizeAfterAdd", counterSizeAfterAdd);
        result.put("directMapCount", mapQueue.getMsgCount().get());
        result.put("directMapSize", mapQueue.getMsgSize().get());
        mapQueue.clear();
        return result;
    }

    private static long processQueueTryUnlockOverflow() throws ReflectiveOperationException {
        ProcessQueue queue = new ProcessQueue();
        Field tryUnlockTimesField = ProcessQueue.class.getDeclaredField("tryUnlockTimes");
        tryUnlockTimesField.setAccessible(true);
        ((AtomicLong) tryUnlockTimesField.get(queue)).set(Long.MAX_VALUE);
        queue.incTryUnlockTimes();
        return queue.getTryUnlockTimes();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) throw new IllegalArgumentException("expected fixture path");
        Path output = Path.of(args[0]);
        boolean verify = Arrays.asList(args).contains("--verify") || Arrays.asList(args).contains("true");
        Map<String, Object> fixture = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        provenance.put("serverBaselineContacted", false);
        fixture.put("provenance", provenance);
        Map<String, Object> apis = new TreeMap<>();
        apis.put("DefaultMQPushConsumer", publicApiSurface(DefaultMQPushConsumer.class));
        apis.put("MQPushConsumer", publicApiSurface(MQPushConsumer.class));
        apis.put("DefaultMQPullConsumer", publicApiSurface(DefaultMQPullConsumer.class));
        apis.put("MQPullConsumer", publicApiSurface(MQPullConsumer.class));
        apis.put("DefaultLitePullConsumer", publicApiSurface(DefaultLitePullConsumer.class));
        apis.put("LitePullConsumer", publicApiSurface(LitePullConsumer.class));
        apis.put("MQAdmin", publicApiSurface(MQAdmin.class));
        apis.put("MQHelper", publicApiSurface(MQHelper.class));
        fixture.put("apis", apis);
        Map<String, Object> implementationApis = new TreeMap<>();
        implementationApis.put("RebalanceImpl", publicApiSurface(RebalanceImpl.class));
        implementationApis.put("RebalancePushImpl", publicApiSurface(RebalancePushImpl.class));
        implementationApis.put("RebalancePullImpl", publicApiSurface(RebalancePullImpl.class));
        implementationApis.put("ProcessQueue", publicApiSurface(ProcessQueue.class));
        implementationApis.put("PopProcessQueue", publicApiSurface(PopProcessQueue.class));
        implementationApis.put("PullRequest", publicApiSurface(PullRequest.class));
        implementationApis.put("PopRequest", publicApiSurface(PopRequest.class));
        fixture.put("implementationApis", implementationApis);
        Map<String, Object> implementationBehaviors = new TreeMap<>();
        implementationBehaviors.put("popProcessQueue", popProcessQueueBehavior());
        implementationBehaviors.put("pullRequest", pullRequestBehavior());
        implementationBehaviors.put("popRequest", popRequestBehavior());
        implementationBehaviors.put("processQueue", processQueueBehavior());
        implementationBehaviors.put("processQueueTryUnlockOverflow", processQueueTryUnlockOverflow());
        implementationBehaviors.put("rebalancePush", rebalancePushBehavior());
        implementationBehaviors.put("rebalancePull", rebalancePullBehavior());
        fixture.put("implementationBehaviors", implementationBehaviors);

        String json = JSON.toJSONString(fixture) + "\n";
        if (verify) {
            if (!Files.exists(output) || !json.equals(Files.readString(output))) {
                throw new IllegalStateException("fixture drift: " + output);
            }
        } else {
            Files.createDirectories(output.getParent());
            Files.writeString(output, json);
        }
    }
}
