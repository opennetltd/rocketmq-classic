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
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.body.ConsumeStatus;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.PopProcessQueueInfo;
import org.apache.rocketmq.remoting.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/** Java 5.5.1 response-body serialization fixture for ClientRemotingProcessor. */
public final class ConsumerRunningInfoFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private ConsumerRunningInfoFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(render()) + "\n";
        if (verify) {
            if (!Files.exists(path) || !json.equals(Files.readString(path))) {
                throw new IllegalStateException("Java consumer-running-info fixture drift: " + path);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json, StandardCharsets.UTF_8);
        }
    }

    private static Map<String, Object> render() throws Exception {
        ConsumerRunningInfo info = new ConsumerRunningInfo();
        Properties properties = new Properties();
        properties.setProperty(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, "127.0.0.1:9876;");
        properties.setProperty(ConsumerRunningInfo.PROP_CONSUME_ORDERLY, "true");
        properties.setProperty(ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE, "7");
        properties.setProperty(ConsumerRunningInfo.PROP_CONSUME_TYPE, "CONSUME_PASSIVELY");
        properties.setProperty(ConsumerRunningInfo.PROP_CLIENT_VERSION, "V5_5_1");
        properties.setProperty(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, "1700000000000");
        info.setProperties(properties);

        SubscriptionData subscription = new SubscriptionData("orders", "*");
        subscription.setSubVersion(1700000000001L);
        subscription.setTagsSet(new LinkedHashSet<>(List.of("green", "blue")));
        info.getSubscriptionSet().add(subscription);

        ProcessQueueInfo processQueueInfo = new ProcessQueueInfo();
        processQueueInfo.setCommitOffset(17L);
        processQueueInfo.setCachedMsgMinOffset(11L);
        processQueueInfo.setCachedMsgMaxOffset(20L);
        processQueueInfo.setCachedMsgCount(3);
        processQueueInfo.setCachedMsgSizeInMiB(2);
        processQueueInfo.setTransactionMsgMinOffset(2L);
        processQueueInfo.setTransactionMsgMaxOffset(3L);
        processQueueInfo.setTransactionMsgCount(1);
        processQueueInfo.setLocked(true);
        processQueueInfo.setTryUnlockTimes(4L);
        processQueueInfo.setLastLockTimestamp(1700000000002L);
        processQueueInfo.setDroped(false);
        processQueueInfo.setLastPullTimestamp(1700000000003L);
        processQueueInfo.setLastConsumeTimestamp(1700000000004L);
        info.getMqTable().put(new MessageQueue("orders", "broker-a", 1), processQueueInfo);

        PopProcessQueueInfo popProcessQueueInfo = new PopProcessQueueInfo();
        popProcessQueueInfo.setWaitAckCount(2);
        popProcessQueueInfo.setDroped(false);
        popProcessQueueInfo.setLastPopTimestamp(1700000000005L);
        info.getMqPopTable().put(new MessageQueue("orders", "broker-a", 2), popProcessQueueInfo);

        ConsumeStatus status = new ConsumeStatus();
        status.setPullRT(0.5d);
        status.setPullTPS(2.5d);
        status.setConsumeRT(3.5d);
        status.setConsumeOKTPS(4.5d);
        status.setConsumeFailedTPS(0.25d);
        status.setConsumeFailedMsgs(6L);
        info.getStatusTable().put("orders", status);
        info.getUserConsumerInfo().put("fixture", "value");
        info.setJstack("jstack-fixture");

        Map<String, Object> fixture = new LinkedHashMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", SOURCE_COMMIT);
        fixture.put("serverContacted", false);
        Map<String, String> propertyKeys = new LinkedHashMap<>();
        propertyKeys.put("PROP_NAMESERVER_ADDR", ConsumerRunningInfo.PROP_NAMESERVER_ADDR);
        propertyKeys.put("PROP_THREADPOOL_CORE_SIZE", ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE);
        propertyKeys.put("PROP_CONSUME_ORDERLY", ConsumerRunningInfo.PROP_CONSUME_ORDERLY);
        propertyKeys.put("PROP_CONSUME_TYPE", ConsumerRunningInfo.PROP_CONSUME_TYPE);
        propertyKeys.put("PROP_CLIENT_VERSION", ConsumerRunningInfo.PROP_CLIENT_VERSION);
        propertyKeys.put("PROP_CONSUMER_START_TIMESTAMP", ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP);
        fixture.put("propertyKeys", propertyKeys);
        fixture.put("identitySemantics", identitySemantics());
        fixture.put("apiInventories", apiInventories());
        Map<String, String> toStrings = new LinkedHashMap<>();
        toStrings.put("ProcessQueueInfo", processQueueInfo.toString());
        toStrings.put("PopProcessQueueInfo", popProcessQueueInfo.toString());
        fixture.put("toStrings", toStrings);
        fixture.put("body", new String(info.encode(), StandardCharsets.UTF_8));
        fixture.put("decodeInputCases", decodeInputCases());
        return fixture;
    }

    private static List<Map<String, Object>> decodeInputCases() {
        Map<String, String> inputs = new LinkedHashMap<>();
        inputs.put("eof-trailing-text", "{}\u001atrailing");
        inputs.put("eof-trailing-json", "{\"jstack\":\"before\"}\u001a{\"jstack\":\"after\"}");
        inputs.put("eof-marker-in-string", "{\"jstack\":\"before\u001aafter\"}");
        inputs.put("single-quoted-trailing-comma", "{'jstack':'single',}");

        return inputs.entrySet().stream().map(entry -> {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("id", entry.getKey());
            result.put("rawInput", entry.getValue());
            try {
                ConsumerRunningInfo decoded = RemotingSerializable.decode(
                    entry.getValue().getBytes(StandardCharsets.UTF_8), ConsumerRunningInfo.class);
                result.put("outcome", "ACCEPTED");
                result.put("encodedBody", new String(decoded.encode(), StandardCharsets.UTF_8));
            } catch (Throwable error) {
                result.put("outcome", "REJECTED");
                result.put("errorClass", error.getClass().getName());
            }
            return result;
        }).collect(Collectors.toList());
    }

    private static Map<String, Object> apiInventories() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ConsumerRunningInfo", apiInventory(ConsumerRunningInfo.class));
        result.put("ProcessQueueInfo", apiInventory(ProcessQueueInfo.class));
        result.put("PopProcessQueueInfo", apiInventory(PopProcessQueueInfo.class));
        result.put("ConsumeStatus", apiInventory(ConsumeStatus.class));
        return result;
    }

    private static Map<String, Object> apiInventory(Class<?> type) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("publicConstructors", Arrays.stream(type.getConstructors())
            .map(ConsumerRunningInfoFixtures::constructorSignature).sorted()
            .collect(Collectors.toList()));
        result.put("publicFields", Arrays.stream(type.getFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(Field::getName).sorted().collect(Collectors.toList()));
        result.put("declaredPublicMethods", Arrays.stream(type.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(ConsumerRunningInfoFixtures::methodSignature).sorted()
            .collect(Collectors.toList()));
        result.put("callablePublicMethods", Arrays.stream(type.getMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getDeclaringClass() != Object.class)
            .map(ConsumerRunningInfoFixtures::methodSignature).sorted()
            .collect(Collectors.toList()));
        return result;
    }

    private static String constructorSignature(Constructor<?> constructor) {
        return constructor.getDeclaringClass().getName() + "("
            + String.join(",", Arrays.stream(constructor.getParameterTypes())
                .map(Class::getTypeName).collect(Collectors.toList())) + ")";
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getTypeName() + " "
            + method.getDeclaringClass().getName() + "." + method.getName() + "("
            + String.join(",", Arrays.stream(method.getParameterTypes())
                .map(Class::getTypeName).collect(Collectors.toList())) + ")";
    }

    private static Map<String, Object> identitySemantics() throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ConsumerRunningInfo", identityCase(
            new ConsumerRunningInfo(), new ConsumerRunningInfo()));
        result.put("ProcessQueueInfo", identityCase(
            new ProcessQueueInfo(), new ProcessQueueInfo()));
        result.put("PopProcessQueueInfo", identityCase(
            new PopProcessQueueInfo(), new PopProcessQueueInfo()));
        result.put("ConsumeStatus", identityCase(
            new ConsumeStatus(), new ConsumeStatus()));
        return result;
    }

    private static Map<String, Object> identityCase(Object first, Object second) throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("equalsDeclaredBy", first.getClass().getMethod("equals", Object.class)
            .getDeclaringClass().getName());
        result.put("hashCodeDeclaredBy", first.getClass().getMethod("hashCode")
            .getDeclaringClass().getName());
        result.put("sameObjectEqualsItself", first.equals(first));
        result.put("distinctDefaultObjectsEquals", first.equals(second));
        return result;
    }
}
