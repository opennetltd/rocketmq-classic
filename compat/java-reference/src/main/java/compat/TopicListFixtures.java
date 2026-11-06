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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.rocketmq.remoting.protocol.body.TopicList;

/** Invokes the pinned Java TopicList accessors, aliases, defaults, and JSON encoding. */
public final class TopicListFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private TopicListFixtures() { }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("usage: TopicListFixtures <fixture.json> [--verify]");
        }
        Path output = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(generate()) + "\n";
        if (verify) {
            if (!Files.exists(output) || !json.equals(Files.readString(output))) {
                throw new IllegalStateException("Java TopicList fixture drift: " + output);
            }
            System.out.println("verified Java 5.5.1 TopicList fixture at " + output);
        } else {
            Files.createDirectories(output.getParent());
            Files.writeString(output, json);
            System.out.println("wrote Java 5.5.1 TopicList fixture to " + output);
        }
    }

    private static Map<String, Object> generate() throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("artifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", SOURCE_COMMIT);
        fixture.put("publicConstructors", Arrays.stream(TopicList.class.getConstructors())
            .map(TopicListFixtures::constructorSignature)
            .sorted()
            .collect(Collectors.toList()));
        fixture.put("publicFields", Arrays.stream(TopicList.class.getFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(java.lang.reflect.Field::getName)
            .sorted()
            .collect(Collectors.toList()));
        fixture.put("publicMethods", Arrays.stream(TopicList.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(TopicListFixtures::methodSignature)
            .sorted()
            .collect(Collectors.toList()));

        TopicList value = new TopicList();
        Set<String> defaultSet = value.getTopicList();
        fixture.put("defaultTopicListNonNull", defaultSet != null);
        fixture.put("defaultTopicListEmpty", defaultSet != null && defaultSet.isEmpty());
        fixture.put("defaultBrokerAddrNull", value.getBrokerAddr() == null);
        fixture.put("defaultJson", JSON.toJSONString(value));

        Set<String> assigned = new HashSet<>();
        assigned.add("orders");
        value.setTopicList(assigned);
        fixture.put("setterRetainsSetIdentity", value.getTopicList() == assigned);
        Set<String> getter = value.getTopicList();
        fixture.put("getterReturnsStoredSetIdentity", getter == assigned);
        getter.add("billing");
        fixture.put("getterMutationVisibleToAssignedSet", assigned.contains("billing"));
        fixture.put("populatedJson", JSON.toJSONString(value));

        value.setTopicList(null);
        fixture.put("setterPreservesNull", value.getTopicList() == null);
        fixture.put("explicitNullJson", JSON.toJSONString(value));
        value.setBrokerAddr("broker-a");
        fixture.put("brokerAddrGetter", value.getBrokerAddr());
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
