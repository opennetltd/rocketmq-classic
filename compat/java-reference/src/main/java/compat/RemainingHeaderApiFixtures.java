/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Invoked Java 5.5.1 API inventory for request headers missing API evidence. */
public final class RemainingHeaderApiFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";
    private static final Map<String, String> RUST_TO_JAVA_CLASSES = Map.ofEntries(
        Map.entry("AddBrokerRequestHeader", "org.apache.rocketmq.remoting.protocol.header.AddBrokerRequestHeader"),
        Map.entry("CreateAclRequestHeader", "org.apache.rocketmq.remoting.protocol.header.CreateAclRequestHeader"),
        Map.entry("CreateUserRequestHeader", "org.apache.rocketmq.remoting.protocol.header.CreateUserRequestHeader"),
        Map.entry("DeleteAclRequestHeader", "org.apache.rocketmq.remoting.protocol.header.DeleteAclRequestHeader"),
        Map.entry("DeleteUserRequestHeader", "org.apache.rocketmq.remoting.protocol.header.DeleteUserRequestHeader"),
        Map.entry("GetAclRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetAclRequestHeader"),
        Map.entry("GetAllTopicConfigRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetAllTopicConfigRequestHeader"),
        Map.entry("GetConsumeStatsInBrokerRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsInBrokerHeader"),
        Map.entry("DeleteKvConfigRequestHeader", "org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteKVConfigRequestHeader"),
        Map.entry("GetKvConfigRequestHeader", "org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVConfigRequestHeader"),
        Map.entry("GetKvListByNamespaceRequestHeader", "org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVListByNamespaceRequestHeader"),
        Map.entry("GetLiteClientInfoRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetLiteClientInfoRequestHeader"),
        Map.entry("GetLiteGroupInfoRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetLiteGroupInfoRequestHeader"),
        Map.entry("GetLiteTopicInfoRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetLiteTopicInfoRequestHeader"),
        Map.entry("GetParentTopicInfoRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetParentTopicInfoRequestHeader"),
        Map.entry("GetUserRequestHeader", "org.apache.rocketmq.remoting.protocol.header.GetUserRequestHeader"),
        Map.entry("ListAclsRequestHeader", "org.apache.rocketmq.remoting.protocol.header.ListAclsRequestHeader"),
        Map.entry("ListUsersRequestHeader", "org.apache.rocketmq.remoting.protocol.header.ListUsersRequestHeader"),
        Map.entry("NotifyUnsubscribeLiteRequestHeader", "org.apache.rocketmq.remoting.protocol.header.NotifyUnsubscribeLiteRequestHeader"),
        Map.entry("PutKvConfigRequestHeader", "org.apache.rocketmq.remoting.protocol.header.namesrv.PutKVConfigRequestHeader"),
        Map.entry("RemoveBrokerRequestHeader", "org.apache.rocketmq.remoting.protocol.header.RemoveBrokerRequestHeader"),
        Map.entry("UpdateAclRequestHeader", "org.apache.rocketmq.remoting.protocol.header.UpdateAclRequestHeader"),
        Map.entry("UpdateUserRequestHeader", "org.apache.rocketmq.remoting.protocol.header.UpdateUserRequestHeader"),
        Map.entry("ViewBrokerStatsDataRequestHeader", "org.apache.rocketmq.remoting.protocol.header.ViewBrokerStatsDataRequestHeader"),
        Map.entry("ViewMessageRequestHeader", "org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader"));

    private RemainingHeaderApiFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(render()) + "\n";
        if (verify) {
            if (!Files.exists(path) || !json.equals(Files.readString(path))) {
                throw new IllegalStateException("Java remaining header API fixture drift: " + path);
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
        Map<String, Object> apiInventories = new TreeMap<>();
        for (Map.Entry<String, String> entry : RUST_TO_JAVA_CLASSES.entrySet()) {
            apiInventories.put(entry.getKey(), apiInventory(Class.forName(entry.getValue())));
        }
        result.put("apiInventories", apiInventories);
        return result;
    }

    private static Map<String, Object> apiInventory(Class<?> type) {
        Map<String, Object> result = new TreeMap<>();
        result.put("publicConstructors", Arrays.stream(type.getConstructors())
            .map(RemainingHeaderApiFixtures::constructorSignature).sorted()
            .collect(Collectors.toList()));
        result.put("publicFields", Arrays.stream(type.getFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(Field::getName).sorted().collect(Collectors.toList()));
        result.put("declaredPublicMethods", Arrays.stream(type.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(RemainingHeaderApiFixtures::methodSignature).sorted()
            .collect(Collectors.toList()));
        result.put("callablePublicMethods", Arrays.stream(type.getMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getDeclaringClass() != Object.class)
            .map(RemainingHeaderApiFixtures::methodSignature).sorted()
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
