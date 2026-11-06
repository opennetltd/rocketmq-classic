/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.TopicMessageQueueChangeListener;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageService;
import org.apache.rocketmq.client.impl.consumer.DefaultLitePullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

/** Invoked Java 5.5.1 Consumer factory-start lifecycle oracle. */
public final class ConsumerLifecycleFixtures {
    private ConsumerLifecycleFixtures() { }

    private static MQClientInstance factory(DefaultMQPushConsumerImpl implementation)
        throws ReflectiveOperationException {
        Field field = DefaultMQPushConsumerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        return (MQClientInstance) field.get(implementation);
    }

    private static MQClientInstance factory(DefaultMQPullConsumerImpl implementation)
        throws ReflectiveOperationException {
        Field field = DefaultMQPullConsumerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        return (MQClientInstance) field.get(implementation);
    }

    private static DefaultLitePullConsumerImpl liteImplementation(DefaultLitePullConsumer consumer)
        throws ReflectiveOperationException {
        Field field = DefaultLitePullConsumer.class.getDeclaredField("defaultLitePullConsumerImpl");
        field.setAccessible(true);
        return (DefaultLitePullConsumerImpl) field.get(consumer);
    }

    private static MQClientInstance factory(DefaultLitePullConsumerImpl implementation)
        throws ReflectiveOperationException {
        Field field = DefaultLitePullConsumerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        return (MQClientInstance) field.get(implementation);
    }

    private static boolean hasLitePullQueueListener(
        DefaultLitePullConsumerImpl implementation, String topic) throws ReflectiveOperationException {
        Field field = DefaultLitePullConsumerImpl.class
            .getDeclaredField("topicMessageQueueChangeListenerMap");
        field.setAccessible(true);
        return ((Map<?, ?>) field.get(implementation)).containsKey(topic);
    }

    private static Map<String, Object> litePullMethodSynchronization()
        throws ReflectiveOperationException {
        Map<String, Object> result = new TreeMap<>();
        result.put("start", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("start").getModifiers()));
        result.put("isRunning", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("isRunning").getModifiers()));
        result.put("assignment", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("assignment").getModifiers()));
        result.put("seek", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("seek",
                org.apache.rocketmq.common.message.MessageQueue.class, long.class).getModifiers()));
        result.put("commitAll", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("commitAll").getModifiers()));
        result.put("commitMap", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("commit",
                java.util.Map.class, boolean.class).getModifiers()));
        result.put("commitSet", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("commit",
                java.util.Set.class, boolean.class).getModifiers()));
        result.put("assign", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("assign",
                java.util.Collection.class).getModifiers()));
        result.put("setSubExpressionForAssign", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("setSubExpressionForAssign",
                String.class, String.class).getModifiers()));
        result.put("poll", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("poll",
                long.class).getModifiers()));
        result.put("subscribeExpression", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("subscribe",
                String.class, String.class).getModifiers()));
        result.put("subscribeSelector", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("subscribe",
                String.class, MessageSelector.class).getModifiers()));
        result.put("subscribeWithListener", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("subscribe",
                String.class, String.class, MessageQueueListener.class).getModifiers()));
        result.put("unsubscribe", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("unsubscribe",
                String.class).getModifiers()));
        result.put("shutdown", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("shutdown").getModifiers()));
        result.put("registerTopicMessageQueueChangeListener", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("registerTopicMessageQueueChangeListener",
                String.class, TopicMessageQueueChangeListener.class).getModifiers()));
        result.put("updateNameServerAddr", Modifier.isSynchronized(
            DefaultLitePullConsumerImpl.class.getDeclaredMethod("updateNameServerAddr",
                String.class).getModifiers()));
        return result;
    }

    private static String consumerServiceState(Object implementation)
        throws ReflectiveOperationException {
        Field field = implementation.getClass().getDeclaredField("serviceState");
        field.setAccessible(true);
        return ((ServiceState) field.get(implementation)).name();
    }

    private static String factoryServiceState(MQClientInstance factory)
        throws ReflectiveOperationException {
        Field field = MQClientInstance.class.getDeclaredField("serviceState");
        field.setAccessible(true);
        return ((ServiceState) field.get(factory)).name();
    }

    private static void closeFactory(MQClientInstance factory, String group) throws Exception {
        if (factory == null) return;
        factory.unregisterConsumer(group);
        Field state = MQClientInstance.class.getDeclaredField("serviceState");
        state.setAccessible(true);
        if (factoryServiceState(factory) != ServiceState.SHUTDOWN_ALREADY.name()) {
            state.set(factory, ServiceState.RUNNING);
            factory.shutdown();
        }
        MQClientManager.getInstance().removeClientFactory(factory.getClientId());
    }

    private static void closePushServices(DefaultMQPushConsumerImpl implementation)
        throws Exception {
        ConsumeMessageService consume = implementation.getConsumeMessageService();
        if (consume != null) consume.shutdown(0);
        Field popField = DefaultMQPushConsumerImpl.class
            .getDeclaredField("consumeMessagePopService");
        popField.setAccessible(true);
        ConsumeMessageService pop = (ConsumeMessageService) popField.get(implementation);
        if (pop != null) pop.shutdown(0);
    }

    private static Map<String, Object> failedPushFactoryStart() throws Exception {
        String group = "consumer-start-push-" + System.nanoTime();
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr("");
        consumer.setPollNameServerInterval(0);
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) ->
            ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        Throwable startFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            startFailure = failure;
        }

        DefaultMQPushConsumerImpl implementation = consumer.getDefaultMQPushConsumerImpl();
        MQClientInstance factory = factory(implementation);
        Map<String, Object> result = new TreeMap<>();
        result.put("exception", startFailure == null ? "" : startFailure.getClass().getSimpleName());
        result.put("consumerRegistered", factory != null
            && factory.getConsumerTable().containsKey(group));
        result.put("consumerServiceState", implementation.getServiceState().name());
        result.put("factoryServiceState", factory == null ? "" : factoryServiceState(factory));
        closePushServices(implementation);
        closeFactory(factory, group);

        if (startFailure == null || !"IllegalArgumentException".equals(
            startFailure.getClass().getSimpleName())
            || !Boolean.TRUE.equals(result.get("consumerRegistered"))
            || !"START_FAILED".equals(result.get("consumerServiceState"))
            || !"START_FAILED".equals(result.get("factoryServiceState"))) {
            throw new IllegalStateException("Unexpected PushConsumer factory-start outcome: " + result,
                startFailure);
        }
        return result;
    }

    private static Map<String, Object> failedPullFactoryStart() throws Exception {
        String group = "consumer-start-pull-" + System.nanoTime();
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(group);
        consumer.setNamesrvAddr("");
        consumer.setPollNameServerInterval(0);
        Throwable startFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            startFailure = failure;
        }

        DefaultMQPullConsumerImpl implementation = consumer.getDefaultMQPullConsumerImpl();
        MQClientInstance factory = factory(implementation);
        Map<String, Object> result = new TreeMap<>();
        result.put("exception", startFailure == null ? "" : startFailure.getClass().getSimpleName());
        result.put("consumerRegistered", factory != null
            && factory.getConsumerTable().containsKey(group));
        result.put("consumerServiceState", implementation.getServiceState().name());
        result.put("factoryServiceState", factory == null ? "" : factoryServiceState(factory));
        closeFactory(factory, group);

        if (startFailure == null || !"IllegalArgumentException".equals(
            startFailure.getClass().getSimpleName())
            || !Boolean.TRUE.equals(result.get("consumerRegistered"))
            || !"START_FAILED".equals(result.get("consumerServiceState"))
            || !"START_FAILED".equals(result.get("factoryServiceState"))) {
            throw new IllegalStateException("Unexpected PullConsumer factory-start outcome: " + result,
                startFailure);
        }
        return result;
    }

    private static Map<String, Object> failedLitePullFactoryStart() throws Exception {
        String group = "consumer-start-lite-pull-" + System.nanoTime();
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(group);
        consumer.setNamesrvAddr("");
        consumer.setPollNameServerInterval(0);
        Throwable startFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            startFailure = failure;
        }

        DefaultLitePullConsumerImpl implementation = liteImplementation(consumer);
        MQClientInstance factory = factory(implementation);
        Throwable retryFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            retryFailure = failure;
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("exception", startFailure == null ? "" : startFailure.getClass().getSimpleName());
        result.put("retryException", retryFailure == null ? "" : retryFailure.getClass().getSimpleName());
        result.put("consumerRegistered", factory != null
            && factory.getConsumerTable().containsKey(group));
        result.put("consumerServiceState", consumerServiceState(implementation));
        result.put("factoryServiceState", factory == null ? "" : factoryServiceState(factory));
        closeFactory(factory, group);

        if (startFailure == null || !"IllegalArgumentException".equals(
            startFailure.getClass().getSimpleName())
            || retryFailure == null || !"MQClientException".equals(
                retryFailure.getClass().getSimpleName())
            || !Boolean.TRUE.equals(result.get("consumerRegistered"))
            || !"START_FAILED".equals(result.get("consumerServiceState"))
            || !"START_FAILED".equals(result.get("factoryServiceState"))) {
            throw new IllegalStateException("Unexpected LitePullConsumer factory-start outcome: " + result,
                startFailure);
        }
        return result;
    }

    private static Map<String, Object> failedLitePullMetadataSchedule() throws Exception {
        String group = "consumer-start-lite-metadata-schedule-" + System.nanoTime();
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(group);
        consumer.setNamesrvAddr("127.0.0.1:1");
        consumer.setTopicMetadataCheckIntervalMillis(0);
        Throwable startFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            startFailure = failure;
        }

        DefaultLitePullConsumerImpl implementation = liteImplementation(consumer);
        MQClientInstance factory = factory(implementation);
        Throwable retryFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            retryFailure = failure;
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("exception", startFailure == null ? "" : startFailure.getClass().getSimpleName());
        result.put("retryException", retryFailure == null ? "" : retryFailure.getClass().getSimpleName());
        result.put("consumerRegistered", factory != null
            && factory.getConsumerTable().containsKey(group));
        result.put("consumerServiceState", consumerServiceState(implementation));
        result.put("factoryServiceState", factory == null ? "" : factoryServiceState(factory));
        closeFactory(factory, group);

        if (startFailure == null || !"IllegalArgumentException".equals(
            startFailure.getClass().getSimpleName())
            || retryFailure == null || !"MQClientException".equals(
                retryFailure.getClass().getSimpleName())
            || !Boolean.TRUE.equals(result.get("consumerRegistered"))
            || !"START_FAILED".equals(result.get("consumerServiceState"))
            || !"RUNNING".equals(result.get("factoryServiceState"))) {
            throw new IllegalStateException("Unexpected LitePullConsumer metadata-schedule outcome: "
                + result, startFailure);
        }
        return result;
    }

    private static Map<String, Object> litePullConfigValidationPrecedesExecutor() throws Exception {
        String group = "consumer-start-lite-config-order-" + System.nanoTime();
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(group);
        consumer.setConsumerTimeoutMillisWhenSuspend(19_999);
        consumer.setPullThreadNums(-1);
        Throwable startFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            startFailure = failure;
        }

        DefaultLitePullConsumerImpl implementation = liteImplementation(consumer);
        MQClientInstance factory = factory(implementation);
        Throwable retryFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            retryFailure = failure;
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("exception", startFailure == null ? "" : startFailure.getClass().getSimpleName());
        result.put("retryException", retryFailure == null ? "" : retryFailure.getClass().getSimpleName());
        result.put("timeoutValidationReached", startFailure != null
            && startFailure.getMessage() != null
            && startFailure.getMessage().contains(
                "consumer consumerTimeoutMillisWhenSuspend must greater than brokerSuspendMaxTimeMillis"));
        result.put("consumerRegistered", factory != null
            && factory.getConsumerTable().containsKey(group));
        result.put("consumerServiceState", consumerServiceState(implementation));
        result.put("factoryServiceState", factory == null ? "" : factoryServiceState(factory));
        closeFactory(factory, group);

        if (!"MQClientException".equals(result.get("exception"))
            || !"MQClientException".equals(result.get("retryException"))
            || !Boolean.TRUE.equals(result.get("timeoutValidationReached"))
            || !Boolean.FALSE.equals(result.get("consumerRegistered"))
            || !"START_FAILED".equals(result.get("consumerServiceState"))
            || !"".equals(result.get("factoryServiceState"))) {
            throw new IllegalStateException("Unexpected LitePullConsumer config-order outcome: "
                + result, startFailure);
        }
        return result;
    }

    private static Map<String, Object> failedLitePullListenerRouteLookup() throws Exception {
        String group = "consumer-start-lite-listener-" + System.nanoTime();
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(group);
        consumer.setNamesrvAddr("127.0.0.1:1");
        consumer.setMqClientApiTimeout(100);
        consumer.registerTopicMessageQueueChangeListener("listener-route-failure",
            (TopicMessageQueueChangeListener) (topic, queues) -> { });
        Throwable startFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            startFailure = failure;
        }

        DefaultLitePullConsumerImpl implementation = liteImplementation(consumer);
        MQClientInstance factory = factory(implementation);
        Throwable retryFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            retryFailure = failure;
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("exception", startFailure == null ? "" : startFailure.getClass().getSimpleName());
        result.put("retryException", retryFailure == null ? "" : retryFailure.getClass().getSimpleName());
        result.put("consumerRegistered", factory != null
            && factory.getConsumerTable().containsKey(group));
        result.put("consumerServiceState", consumerServiceState(implementation));
        result.put("factoryServiceState", factory == null ? "" : factoryServiceState(factory));
        closeFactory(factory, group);

        if (startFailure == null || !"MQClientException".equals(
            startFailure.getClass().getSimpleName())
            || retryFailure == null || !"MQClientException".equals(
                retryFailure.getClass().getSimpleName())
            || !Boolean.FALSE.equals(result.get("consumerRegistered"))
            || !"SHUTDOWN_ALREADY".equals(result.get("consumerServiceState"))
            || !"SHUTDOWN_ALREADY".equals(result.get("factoryServiceState"))) {
            throw new IllegalStateException("Unexpected LitePull listener-route failure: " + result,
                startFailure);
        }
        return result;
    }

    private static Map<String, Object> failedLitePullListenerRegistrationRouteLookup()
        throws Exception {
        String group = "consumer-register-lite-listener-" + System.nanoTime();
        String topic = "listener-registration-route-failure";
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(group);
        consumer.setNamesrvAddr("127.0.0.1:1");
        consumer.setMqClientApiTimeout(100);
        consumer.start();

        Throwable registrationFailure = null;
        try {
            consumer.registerTopicMessageQueueChangeListener(topic,
                (TopicMessageQueueChangeListener) (changedTopic, queues) -> { });
        } catch (Throwable failure) {
            registrationFailure = failure;
        }
        Throwable retryFailure = null;
        try {
            consumer.registerTopicMessageQueueChangeListener(topic,
                (TopicMessageQueueChangeListener) (changedTopic, queues) -> { });
        } catch (Throwable failure) {
            retryFailure = failure;
        }

        DefaultLitePullConsumerImpl implementation = liteImplementation(consumer);
        MQClientInstance factory = factory(implementation);
        Map<String, Object> result = new TreeMap<>();
        result.put("exception", registrationFailure == null ? ""
            : registrationFailure.getClass().getSimpleName());
        result.put("retryException", retryFailure == null ? ""
            : retryFailure.getClass().getSimpleName());
        result.put("listenerRegistered", hasLitePullQueueListener(implementation, topic));
        result.put("consumerServiceState", consumerServiceState(implementation));
        result.put("factoryServiceState", factory == null ? "" : factoryServiceState(factory));
        consumer.shutdown();

        if (registrationFailure == null || !"MQClientException".equals(
            registrationFailure.getClass().getSimpleName())
            || retryFailure == null || !"MQClientException".equals(
                retryFailure.getClass().getSimpleName())
            || !Boolean.TRUE.equals(result.get("listenerRegistered"))
            || !"RUNNING".equals(result.get("consumerServiceState"))
            || !"RUNNING".equals(result.get("factoryServiceState"))) {
            throw new IllegalStateException(
                "Unexpected LitePull listener-registration route failure: " + result,
                registrationFailure);
        }
        return result;
    }

    private static Map<String, Object> failedLitePullBrokerSubscriptionCheck() throws Exception {
        String group = "consumer-broker-check-lite";
        String topic = "lite-broker-check";
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(group);
        consumer.setNamesrvAddr("127.0.0.1:1");
        consumer.setClientIP("192.0.2.1");
        consumer.setInstanceName(group + "-client");
        consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.setMqClientApiTimeout(100);
        consumer.subscribe(topic, MessageSelector.bySql("a > 1"));

        MQClientInstance factory = MQClientManager.getInstance()
            .getOrCreateMQClientInstance(consumer);
        Field apiField = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        apiField.setAccessible(true);
        MQClientAPIImpl originalApi = (MQClientAPIImpl) apiField.get(factory);
        Map<String, Object> checkRequest = new TreeMap<>();
        byte[] route = JSON.toJSONBytes(Map.of(
            "queueDatas", new Object[] {Map.of(
                "brokerName", "broker-a", "readQueueNums", 1, "writeQueueNums", 1,
                "perm", 6, "topicSysFlag", 0)},
            "brokerDatas", new Object[] {Map.of(
                "cluster", "cluster", "brokerName", "broker-a",
                "brokerAddrs", Map.of("0", "127.0.0.1:10911"))},
            "filterServerTable", Map.of()));
        RemotingClient transport = (RemotingClient) Proxy.newProxyInstance(
            RemotingClient.class.getClassLoader(), new Class<?>[] {RemotingClient.class},
            (proxy, method, values) -> {
                if ("invokeSync".equals(method.getName())) {
                    RemotingCommand request = (RemotingCommand) values[1];
                    if (request.getCode() == RequestCode.GET_ROUTEINFO_BY_TOPIC) {
                        RemotingCommand response = RemotingCommand.createResponseCommand(0, null);
                        response.setBody(route);
                        return response;
                    }
                    if (request.getCode() == RequestCode.CHECK_CLIENT_CONFIG) {
                        checkRequest.put("code", request.getCode());
                        Map<String, Object> body = JSON.parseObject(request.getBody());
                        Map<String, Object> subscription = JSON.parseObject(
                            JSON.toJSONString(body.get("subscriptionData")));
                        subscription.remove("subVersion");
                        body.put("subscriptionData", subscription);
                        checkRequest.put("body", body);
                        return RemotingCommand.createResponseCommand(44, "rejected by fixture");
                    }
                    return RemotingCommand.createResponseCommand(0, null);
                }
                return method.getReturnType() == boolean.class ? false : null;
            });
        MQClientAPIImpl api = new MQClientAPIImpl(new NettyClientConfig(), null, null,
            consumer, null, ignored -> transport);
        apiField.set(factory, api);

        Throwable startFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            startFailure = failure;
        }
        DefaultLitePullConsumerImpl implementation = liteImplementation(consumer);
        Map<String, Object> result = new TreeMap<>();
        result.put("exception", startFailure == null ? ""
            : startFailure.getClass().getSimpleName());
        result.put("consumerServiceState", consumerServiceState(implementation));
        result.put("factoryServiceState", factoryServiceState(factory));
        result.put("consumerRegistered", factory.getConsumerTable().containsKey(group));
        result.put("checkRequest", checkRequest);

        closeFactory(factory, group);
        originalApi.shutdown();
        api.shutdown();
        MQClientManager.getInstance().removeClientFactory(factory.getClientId());
        if (startFailure == null || !"MQClientException".equals(
            startFailure.getClass().getSimpleName())
            || !"SHUTDOWN_ALREADY".equals(result.get("consumerServiceState"))
            || !"SHUTDOWN_ALREADY".equals(result.get("factoryServiceState"))
            || !Boolean.FALSE.equals(result.get("consumerRegistered"))
            || !Integer.valueOf(RequestCode.CHECK_CLIENT_CONFIG).equals(checkRequest.get("code"))) {
            throw new IllegalStateException("Unexpected LitePull broker-check outcome: " + result,
                startFailure);
        }
        return result;
    }

    private static Map<String, Object> produce() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        result.put("pushFactoryStartFailure", failedPushFactoryStart());
        result.put("pullFactoryStartFailure", failedPullFactoryStart());
        result.put("litePullFactoryStartFailure", failedLitePullFactoryStart());
        result.put("litePullMetadataScheduleFailure", failedLitePullMetadataSchedule());
        result.put("litePullConfigValidationPrecedesExecutor",
            litePullConfigValidationPrecedesExecutor());
        result.put("litePullListenerRouteFailure", failedLitePullListenerRouteLookup());
        result.put("litePullListenerRegistrationRouteFailure",
            failedLitePullListenerRegistrationRouteLookup());
        result.put("litePullBrokerSubscriptionCheckFailure",
            failedLitePullBrokerSubscriptionCheck());
        result.put("litePullMethodSynchronization", litePullMethodSynchronization());
        return result;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1
            && (Boolean.parseBoolean(args[1]) || "--verify".equals(args[1]));
        String json = JSON.toJSONString(produce()) + "\n";
        if (verify) {
            if (!Files.exists(path) || !json.equals(Files.readString(path))) {
                throw new IllegalStateException("Java Consumer lifecycle fixture drift: " + path);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
    }
}
