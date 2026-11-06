/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.rocketmq.remoting.protocol.body.QuerySubscriptionResponseBody;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/** Invokes Java QuerySubscriptionResponseBody getters, setters, aliases, and JSON encoding. */
public final class QuerySubscriptionBodyFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private QuerySubscriptionBodyFixtures() { }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("usage: QuerySubscriptionBodyFixtures <fixture.json> [--verify]");
        }
        Path output = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(generate()) + "\n";
        if (verify) {
            if (!Files.exists(output) || !json.equals(Files.readString(output))) {
                throw new IllegalStateException("Java query subscription body fixture drift: " + output);
            }
            System.out.println("verified Java 5.5.1 query subscription body fixture at " + output);
        } else {
            Files.createDirectories(output.getParent());
            Files.writeString(output, json);
            System.out.println("wrote Java 5.5.1 query subscription body fixture to " + output);
        }
    }

    private static Map<String, Object> generate() throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("artifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", SOURCE_COMMIT);
        fixture.put("publicConstructors",
            Arrays.stream(QuerySubscriptionResponseBody.class.getConstructors())
                .map(QuerySubscriptionBodyFixtures::constructorSignature)
                .sorted()
                .collect(Collectors.toList()));
        fixture.put("publicFields", Arrays.stream(QuerySubscriptionResponseBody.class.getFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(java.lang.reflect.Field::getName)
            .sorted()
            .collect(Collectors.toList()));
        fixture.put("publicMethods", Arrays.stream(QuerySubscriptionResponseBody.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(QuerySubscriptionBodyFixtures::methodSignature)
            .sorted()
            .collect(Collectors.toList()));

        QuerySubscriptionResponseBody value = new QuerySubscriptionResponseBody();
        fixture.put("defaultSubscriptionDataNull", value.getSubscriptionData() == null);
        fixture.put("defaultGroupNull", value.getGroup() == null);
        fixture.put("defaultTopicNull", value.getTopic() == null);
        fixture.put("defaultJson", JSON.toJSONString(value));

        SubscriptionData assigned = new SubscriptionData("orders", "tag-a");
        assigned.setSubVersion(123L);
        value.setSubscriptionData(assigned);
        value.setGroup("workers");
        value.setTopic("orders");
        fixture.put("setterRetainsSubscriptionDataIdentity", value.getSubscriptionData() == assigned);
        SubscriptionData getter = value.getSubscriptionData();
        fixture.put("getterReturnsStoredSubscriptionDataIdentity", getter == assigned);
        getter.setSubString("tag-b");
        fixture.put("getterMutationVisibleToAssignedObject", "tag-b".equals(assigned.getSubString()));
        fixture.put("populatedJson", JSON.toJSONString(value));

        value.setSubscriptionData(null);
        value.setGroup(null);
        value.setTopic(null);
        fixture.put("settersPreserveNull", value.getSubscriptionData() == null
            && value.getGroup() == null && value.getTopic() == null);
        fixture.put("explicitNullJson", JSON.toJSONString(value));
        return fixture;
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getName() + " " + method.getName() + "(" +
            Arrays.stream(method.getParameterTypes()).map(Class::getName).collect(Collectors.joining(",")) + ")";
    }

    private static String constructorSignature(Constructor<?> constructor) {
        return constructor.getDeclaringClass().getName() + "(" +
            Arrays.stream(constructor.getParameterTypes()).map(Class::getName).collect(Collectors.joining(",")) + ")";
    }
}
