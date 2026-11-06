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
import java.io.DataInputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.RequestFutureHolder;
import org.apache.rocketmq.client.producer.RequestResponseFuture;
import org.apache.rocketmq.common.ServiceState;

/** Invoked Java 5.5.1 Producer shutdown/group mutation oracle. */
public final class ProducerLifecycleFixtures {
    private ProducerLifecycleFixtures() { }

    private static void setImplementationField(DefaultMQProducer producer, String name,
        Object value) {
        try {
            Field field = DefaultMQProducerImpl.class.getDeclaredField(name);
            field.setAccessible(true);
            field.set(producer.getDefaultMQProducerImpl(), value);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Could not set Producer implementation field " + name, error);
        }
    }

    private static void prepareFactoryForShutdown(MQClientInstance factory) {
        try {
            Field field = MQClientInstance.class.getDeclaredField("serviceState");
            field.setAccessible(true);
            field.set(factory, ServiceState.RUNNING);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Could not prepare fixture client factory", error);
        }
    }

    private static String factoryServiceState(MQClientInstance factory) {
        try {
            Field field = MQClientInstance.class.getDeclaredField("serviceState");
            field.setAccessible(true);
            return ((ServiceState) field.get(factory)).name();
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Could not inspect fixture client factory", error);
        }
    }

    private static Map<String, Object> shutdownCase(String originalGroup, String currentGroup,
        String otherGroup) {
        DefaultMQProducer producer = new DefaultMQProducer(originalGroup);
        DefaultMQProducer other = otherGroup == null ? null : new DefaultMQProducer(otherGroup);
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        String clientId = "producer-shutdown-fixture-" + originalGroup + "-" + currentGroup;
        MQClientInstance factory = new MQClientInstance(new ClientConfig(), 0, clientId);
        Map<String, Object> result = new TreeMap<>();
        try {
            if (!factory.registerProducer(originalGroup, implementation)) {
                throw new IllegalStateException("Could not register original Producer group");
            }
            if (other != null && !factory.registerProducer(otherGroup,
                other.getDefaultMQProducerImpl())) {
                throw new IllegalStateException("Could not register current Producer group");
            }
            setImplementationField(producer, "mQClientFactory", factory);
            prepareFactoryForShutdown(factory);
            implementation.setServiceState(ServiceState.RUNNING);
            producer.setProducerGroup(currentGroup);
            try {
                implementation.shutdown(false);
                result.put("shutdownException", "");
            } catch (Throwable failure) {
                result.put("shutdownException", failure.getClass().getSimpleName());
            }
            result.put("originalGroup", originalGroup);
            result.put("currentGroupIsNull", currentGroup == null);
            result.put("originalGroupRemains",
                factory.getProducerTable().containsKey(originalGroup));
            result.put("currentGroupRemains", currentGroup != null
                && factory.getProducerTable().containsKey(currentGroup));
            result.put("otherGroupRemains", otherGroup != null
                && factory.getProducerTable().containsKey(otherGroup));
            result.put("clientFactoryServiceStateAfterShutdown", factoryServiceState(factory));
            result.put("serviceState", implementation.getServiceState().name());
            return result;
        } finally {
            RequestFutureHolder.getInstance().shutdown(implementation);
            prepareFactoryForShutdown(factory);
            factory.shutdown();
        }
    }

    private static Map<String, Object> requestFutureShutdown() {
        DefaultMQProducer producer = new DefaultMQProducer("request-future-shutdown");
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        RequestFutureHolder holder = RequestFutureHolder.getInstance();
        String correlationId = "request-future-shutdown-fixture";
        AtomicInteger callbacks = new AtomicInteger();
        holder.getRequestFutureTable().put(correlationId,
            new RequestResponseFuture(correlationId, 60_000,
                new org.apache.rocketmq.client.producer.RequestCallback() {
                    @Override
                    public void onSuccess(org.apache.rocketmq.common.message.Message message) {
                        callbacks.incrementAndGet();
                    }

                    @Override
                    public void onException(Throwable error) {
                        callbacks.incrementAndGet();
                    }
                }));
        holder.startScheduledTask(implementation);
        holder.shutdown(implementation);

        Map<String, Object> result = new TreeMap<>();
        result.put("pendingRemainsAfterProducerShutdown",
            holder.getRequestFutureTable().containsKey(correlationId));
        result.put("callbackCountAfterProducerShutdown", callbacks.get());
        try {
            Field scheduled = RequestFutureHolder.class
                .getDeclaredField("scheduledExecutorService");
            scheduled.setAccessible(true);
            result.put("timeoutScannerStopsAfterLastProducer",
                scheduled.get(holder) == null);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Could not inspect RequestFutureHolder scheduler", error);
        } finally {
            holder.getRequestFutureTable().remove(correlationId);
        }
        return result;
    }

    private static Map<String, Object> failedStartupRouteTransport() throws Exception {
        String group = "producer-start-route-transport-" + System.nanoTime();
        Map<String, Object> result = new TreeMap<>();
        AtomicInteger requestCount = new AtomicInteger();
        AtomicReference<Throwable> peerFailure = new AtomicReference<>();
        try (ServerSocket nameserver = new ServerSocket(0, 1,
            InetAddress.getByName("127.0.0.1"))) {
            nameserver.setSoTimeout(10_000);
            Thread peer = new Thread(() -> {
                try (Socket socket = nameserver.accept();
                    DataInputStream input = new DataInputStream(socket.getInputStream())) {
                    int length = input.readInt();
                    if (length < 0 || length > 16 * 1024 * 1024) {
                        throw new IllegalStateException("invalid route request frame length " + length);
                    }
                    byte[] frame = new byte[length];
                    input.readFully(frame);
                    requestCount.incrementAndGet();
                    // Closing without a response causes the Java remoting path
                    // to throw from initTopicRoute().
                } catch (Throwable failure) {
                    peerFailure.set(failure);
                }
            }, "producer-start-route-transport-fixture");
            peer.setDaemon(true);
            peer.start();

            DefaultMQProducer producer = new DefaultMQProducer(group);
            producer.setInstanceName("phase18-start-route-" + System.nanoTime());
            producer.setNamesrvAddr("127.0.0.1:" + nameserver.getLocalPort());
            producer.setTopics(Collections.singletonList("startup-topic"));
            Throwable startFailure = null;
            try {
                producer.start();
            } catch (Throwable failure) {
                startFailure = failure;
            }
            peer.join(12_000);
            if (peer.isAlive()) {
                throw new IllegalStateException("NameServer fixture did not finish");
            }
            if (peerFailure.get() != null) {
                throw new IllegalStateException("NameServer fixture failed", peerFailure.get());
            }

            DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
            Field factoryField = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
            factoryField.setAccessible(true);
            MQClientInstance factory = (MQClientInstance) factoryField.get(implementation);
            result.put("exception", startFailure == null ? "" : startFailure.getClass().getSimpleName());
            result.put("cause", startFailure == null || startFailure.getCause() == null
                ? "" : startFailure.getCause().getClass().getSimpleName());
            result.put("requestCount", requestCount.get());
            boolean producerRegistered = factory != null
                && factory.getProducerTable().containsKey(group);
            String producerServiceState = implementation.getServiceState().name();
            String factoryState = factory == null ? "" : factoryServiceState(factory);
            result.put("producerRegistered", producerRegistered);
            result.put("producerServiceState", producerServiceState);
            result.put("factoryServiceState", factoryState);

            if (factory != null) {
                factory.unregisterProducer(group);
                factory.shutdown();
                MQClientManager.getInstance().removeClientFactory(factory.getClientId());
            }
            if (startFailure == null || requestCount.get() != 1 || !producerRegistered
                || !"START_FAILED".equals(producerServiceState)) {
                throw new IllegalStateException("Unexpected Producer start route failure outcome: " + result);
            }
        }
        return result;
    }

    private static Map<String, Object> failedStartupFactoryStart() throws Exception {
        String group = "producer-start-factory-" + System.nanoTime();
        DefaultMQProducer producer = new DefaultMQProducer(group);
        producer.setInstanceName("phase18-start-factory-" + System.nanoTime());
        producer.setNamesrvAddr("");
        producer.setPollNameServerInterval(0);
        Throwable startFailure = null;
        try {
            producer.start();
        } catch (Throwable failure) {
            startFailure = failure;
        }

        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        Field factoryField = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
        factoryField.setAccessible(true);
        MQClientInstance factory = (MQClientInstance) factoryField.get(implementation);
        Map<String, Object> result = new TreeMap<>();
        result.put("exception", startFailure == null ? "" : startFailure.getClass().getSimpleName());
        result.put("producerRegistered", factory != null
            && factory.getProducerTable().containsKey(group));
        result.put("producerServiceState", implementation.getServiceState().name());
        result.put("factoryServiceState", factory == null ? "" : factoryServiceState(factory));

        if (factory != null) {
            factory.unregisterProducer(group);
            prepareFactoryForShutdown(factory);
            factory.shutdown();
            MQClientManager.getInstance().removeClientFactory(factory.getClientId());
        }
        if (startFailure == null || !"IllegalArgumentException".equals(
            startFailure.getClass().getSimpleName())
            || !Boolean.TRUE.equals(result.get("producerRegistered"))
            || !"START_FAILED".equals(result.get("producerServiceState"))
            || !"START_FAILED".equals(result.get("factoryServiceState"))) {
            throw new IllegalStateException("Unexpected Producer factory-start failure outcome: " + result,
                startFailure);
        }
        return result;
    }

    private static Map<String, Object> produce() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        result.put("sameGroup", shutdownCase("same-group", "same-group", null));
        result.put("changedToMissingGroup",
            shutdownCase("original-group", "missing-current-group", null));
        result.put("changedToOtherProducerGroup",
            shutdownCase("original-group-collision", "other-group", "other-group"));
        result.put("changedToNullGroup",
            shutdownCase("original-null-group", null, null));
        result.put("requestFutureShutdown", requestFutureShutdown());
        result.put("startupFactoryStartFailure", failedStartupFactoryStart());
        result.put("startupRouteTransportFailure", failedStartupRouteTransport());
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
                throw new IllegalStateException("Java Producer lifecycle fixture drift: " + path);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
    }
}
