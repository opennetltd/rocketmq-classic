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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.admin.TopicOffset;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;

/** Invokes the pinned TopicStatsTable and TopicOffset accessors and JSON behavior. */
public final class TopicStatsTableFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private TopicStatsTableFixtures() { }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("usage: TopicStatsTableFixtures <fixture.json> [--verify]");
        }
        Path output = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(generate()) + "\n";
        if (verify) {
            if (!Files.exists(output) || !json.equals(Files.readString(output))) {
                throw new IllegalStateException("Java TopicStatsTable fixture drift: " + output);
            }
            System.out.println("verified Java 5.5.1 TopicStatsTable fixture at " + output);
        } else {
            Files.createDirectories(output.getParent());
            Files.writeString(output, json);
            System.out.println("wrote Java 5.5.1 TopicStatsTable fixture to " + output);
        }
    }

    private static Map<String, Object> generate() throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("artifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", SOURCE_COMMIT);
        fixture.put("topicStatsTablePublicConstructors",
            Arrays.stream(TopicStatsTable.class.getConstructors())
                .map(TopicStatsTableFixtures::constructorSignature)
                .sorted()
                .collect(Collectors.toList()));
        fixture.put("topicStatsTablePublicFields", Arrays.stream(TopicStatsTable.class.getFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(java.lang.reflect.Field::getName)
            .sorted()
            .collect(Collectors.toList()));
        fixture.put("topicStatsTablePublicMethods", Arrays.stream(TopicStatsTable.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(TopicStatsTableFixtures::methodSignature)
            .sorted()
            .collect(Collectors.toList()));
        fixture.put("topicOffsetPublicConstructors",
            Arrays.stream(TopicOffset.class.getConstructors())
                .map(TopicStatsTableFixtures::constructorSignature)
                .sorted()
                .collect(Collectors.toList()));
        fixture.put("topicOffsetPublicFields", Arrays.stream(TopicOffset.class.getFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(java.lang.reflect.Field::getName)
            .sorted()
            .collect(Collectors.toList()));
        fixture.put("topicOffsetPublicMethods", Arrays.stream(TopicOffset.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(TopicStatsTableFixtures::methodSignature)
            .sorted()
            .collect(Collectors.toList()));

        TopicStatsTable value = new TopicStatsTable();
        Map<MessageQueue, TopicOffset> defaultTable = value.getOffsetTable();
        fixture.put("defaultTopicPutTps", value.getTopicPutTps());
        fixture.put("defaultOffsetTableNonNull", defaultTable != null);
        fixture.put("defaultOffsetTableEmpty", defaultTable != null && defaultTable.isEmpty());
        fixture.put("defaultJson", JSON.toJSONString(value));

        TopicOffset defaultOffset = new TopicOffset();
        fixture.put("defaultOffsetValues", Arrays.asList(
            defaultOffset.getMinOffset(),
            defaultOffset.getMaxOffset(),
            defaultOffset.getLastUpdateTimestamp()));
        fixture.put("defaultOffsetToString", defaultOffset.toString());

        TopicOffset setterProbe = new TopicOffset();
        setterProbe.setMinOffset(Long.MIN_VALUE);
        setterProbe.setMaxOffset(Long.MAX_VALUE);
        setterProbe.setLastUpdateTimestamp(-9);
        fixture.put("offsetSetterValues", Arrays.asList(
            setterProbe.getMinOffset(),
            setterProbe.getMaxOffset(),
            setterProbe.getLastUpdateTimestamp()));

        TopicOffset assignedOffset = new TopicOffset();
        assignedOffset.setMinOffset(10);
        assignedOffset.setMaxOffset(15);
        assignedOffset.setLastUpdateTimestamp(100);
        Map<MessageQueue, TopicOffset> assigned = new LinkedHashMap<>();
        MessageQueue firstQueue = new MessageQueue("orders", "broker-a", 1);
        MessageQueue secondQueue = new MessageQueue("orders", "broker-b", 2);
        assigned.put(firstQueue, assignedOffset);
        assigned.put(secondQueue, null);
        value.setOffsetTable(assigned);
        value.setTopicPutTps(1.25);
        fixture.put("setterRetainsMapIdentity", value.getOffsetTable() == assigned);
        Map<MessageQueue, TopicOffset> getter = value.getOffsetTable();
        fixture.put("getterReturnsStoredMapIdentity", getter == assigned);
        TopicOffset getterOffset = getter.get(firstQueue);
        fixture.put("getterReturnsStoredOffsetIdentity", getterOffset == assignedOffset);
        getterOffset.setMaxOffset(16);
        fixture.put("getterMutationVisibleToAssignedOffset", assignedOffset.getMaxOffset() == 16);
        getter.put(new MessageQueue("orders", "broker-c", 3), null);
        fixture.put("getterMutationVisibleToAssignedMap", assigned.size() == 3);
        fixture.put("topicPutTpsGetter", value.getTopicPutTps());
        fixture.put("populatedJson", JSON.toJSONString(value));
        fixture.put("writeNullMapValuesJson", JSON.toJSONString(value,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue));

        value.setOffsetTable(null);
        fixture.put("setterPreservesNull", value.getOffsetTable() == null);
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
