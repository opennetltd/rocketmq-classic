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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageResponseHeader;

/** Invoked Java 5.5.1 identity-equality oracle for response headers. */
public final class HeaderResponseIdentityFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";
    private static final Map<String, String> RUST_TO_JAVA_CLASSES = Map.ofEntries(
        Map.entry("AddWritePermOfBrokerResponseHeader", "org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerResponseHeader"),
        Map.entry("ChangeInvisibleTimeResponseHeader", "org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeResponseHeader"),
        Map.entry("QueryConsumerOffsetResponseHeader", "org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader"),
        Map.entry("SearchOffsetResponseHeader", "org.apache.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader"),
        Map.entry("GetMaxOffsetResponseHeader", "org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetResponseHeader"),
        Map.entry("GetMinOffsetResponseHeader", "org.apache.rocketmq.remoting.protocol.header.GetMinOffsetResponseHeader"),
        Map.entry("GetEarliestMsgStoretimeResponseHeader", "org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeResponseHeader"),
        Map.entry("GetAllSubscriptionGroupResponseHeader", "org.apache.rocketmq.remoting.protocol.header.GetAllSubscriptionGroupResponseHeader"),
        Map.entry("GetAllTopicConfigResponseHeader", "org.apache.rocketmq.remoting.protocol.header.GetAllTopicConfigResponseHeader"),
        Map.entry("GetKvConfigResponseHeader", "org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVConfigResponseHeader"),
        Map.entry("PullMessageResponseHeader", "org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader"),
        Map.entry("QueryMessageResponseHeader", "org.apache.rocketmq.remoting.protocol.header.QueryMessageResponseHeader"),
        Map.entry("RecallMessageResponseHeader", "org.apache.rocketmq.remoting.protocol.header.RecallMessageResponseHeader"),
        Map.entry("SendMessageResponseHeader", "org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader"),
        Map.entry("UnregisterClientResponseHeader", "org.apache.rocketmq.remoting.protocol.header.UnregisterClientResponseHeader"),
        Map.entry("EndTransactionResponseHeader", "org.apache.rocketmq.remoting.protocol.header.EndTransactionResponseHeader"),
        Map.entry("CheckTransactionStateResponseHeader", "org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateResponseHeader"),
        Map.entry("NotificationResponseHeader", "org.apache.rocketmq.remoting.protocol.header.NotificationResponseHeader"),
        Map.entry("PopMessageResponseHeader", "org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader"),
        Map.entry("PopLiteMessageResponseHeader", "org.apache.rocketmq.remoting.protocol.header.PopLiteMessageResponseHeader"),
        Map.entry("WipeWritePermOfBrokerResponseHeader", "org.apache.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader"),
        Map.entry("PollingInfoResponseHeader", "org.apache.rocketmq.remoting.protocol.header.PollingInfoResponseHeader"));

    private HeaderResponseIdentityFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(render()) + "\n";
        if (verify) {
            if (!Files.exists(path) || !json.equals(Files.readString(path))) {
                throw new IllegalStateException("Java header response identity fixture drift: " + path);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json, StandardCharsets.UTF_8);
        }
    }

    private static Map<String, Object> render() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        result.put("clientArtifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        result.put("sourceCommit", SOURCE_COMMIT);
        result.put("serverContacted", false);
        Map<String, Object> identityCases = new TreeMap<>();
        Map<String, Object> apiInventories = new TreeMap<>();
        for (Map.Entry<String, String> entry : RUST_TO_JAVA_CLASSES.entrySet()) {
            Class<?> type = Class.forName(entry.getValue());
            Object first = type.getDeclaredConstructor().newInstance();
            Object second = type.getDeclaredConstructor().newInstance();
            identityCases.put(entry.getKey(), identityCase(type, first, second));
            apiInventories.put(entry.getKey(), apiInventory(type));
        }
        result.put("identityCases", identityCases);
        result.put("apiInventories", apiInventories);
        Class<?> recallType = Class.forName(RUST_TO_JAVA_CLASSES.get("RecallMessageResponseHeader"));
        Object recall = recallType.getDeclaredConstructor().newInstance();
        Map<String, Object> recallNullability = new TreeMap<>();
        recallNullability.put("defaultMsgIdIsNull", recallType.getMethod("getMsgId").invoke(recall) == null);
        recallType.getMethod("setMsgId", String.class).invoke(recall, (Object) null);
        recallNullability.put("msgIdAfterNullSetterIsNull", recallType.getMethod("getMsgId").invoke(recall) == null);
        RecallMessageResponseHeader decodedWithoutExtFields =
            RemotingCommand.createResponseCommand(0, "fixture")
                .decodeCommandCustomHeader(RecallMessageResponseHeader.class);
        recallNullability.put("nullExtFieldsDecodeReturnsNullMsgId", decodedWithoutExtFields.getMsgId() == null);
        RemotingCommand decodedWithEmptyExtFields = RemotingCommand.createResponseCommand(0, "fixture");
        decodedWithEmptyExtFields.setExtFields(new HashMap<>());
        try {
            decodedWithEmptyExtFields.decodeCommandCustomHeader(RecallMessageResponseHeader.class);
            recallNullability.put("emptyExtFieldsMissingMsgIdRejected", false);
        } catch (RemotingCommandException expected) {
            recallNullability.put("emptyExtFieldsMissingMsgIdRejected", true);
        }
        result.put("recallMessageIdNullability", recallNullability);
        return result;
    }

    private static Map<String, Object> identityCase(Class<?> type, Object first, Object second)
        throws Exception {
        Map<String, Object> result = new TreeMap<>();
        result.put("javaClass", type.getName());
        result.put("equalsDeclaredBy", type.getMethod("equals", Object.class)
            .getDeclaringClass().getName());
        result.put("hashCodeDeclaredBy", type.getMethod("hashCode")
            .getDeclaringClass().getName());
        result.put("sameObjectEqualsItself", first.equals(first));
        result.put("distinctDefaultObjectsEquals", first.equals(second));
        return result;
    }

    private static Map<String, Object> apiInventory(Class<?> type) {
        Map<String, Object> result = new TreeMap<>();
        result.put("publicConstructors", Arrays.stream(type.getConstructors())
            .map(HeaderResponseIdentityFixtures::constructorSignature).sorted()
            .collect(Collectors.toList()));
        result.put("publicFields", Arrays.stream(type.getFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(Field::getName).sorted().collect(Collectors.toList()));
        result.put("declaredPublicMethods", Arrays.stream(type.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(HeaderResponseIdentityFixtures::methodSignature).sorted()
            .collect(Collectors.toList()));
        result.put("callablePublicMethods", Arrays.stream(type.getMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getDeclaringClass() != Object.class)
            .map(HeaderResponseIdentityFixtures::methodSignature).sorted()
            .collect(Collectors.toList()));
        return result;
    }

    private static String constructorSignature(Constructor<?> constructor) {
        return constructor.getDeclaringClass().getName() + "("
            + Arrays.stream(constructor.getParameterTypes()).map(Class::getTypeName)
                .collect(Collectors.joining(", "))
            + ")";
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getTypeName() + " " + method.getDeclaringClass().getName()
            + "." + method.getName() + "("
            + Arrays.stream(method.getParameterTypes()).map(Class::getTypeName)
                .collect(Collectors.joining(", "))
            + ")";
    }
}
