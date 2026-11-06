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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.common.topic.TopicValidator;

/** Invoked Java 5.5.1 namespace wrapping and unwrapping oracle. */
public final class NamespaceFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private NamespaceFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(render()) + "\n";
        if (verify) {
            if (!Files.exists(path) || !json.equals(Files.readString(path))) {
                throw new IllegalStateException("Java namespace fixture drift: " + path);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
    }

    private static Map<String, Object> render() {
        String customSystemTopic = "RMQ_SYS_CUSTOM_PHASE16";
        TopicValidator.addSystemTopic(customSystemTopic);
        Object[][] inputs = {
            {"normal", "tenant", "orders"},
            {"already_wrapped", "tenant", "tenant%orders"},
            {"multiple_markers", "tenant", "orders%with%markers"},
            {"retry", "tenant", "%RETRY%workers"},
            {"retry_already_wrapped", "tenant", "%RETRY%tenant%workers"},
            {"dlq", "tenant", "%DLQ%workers"},
            {"dlq_already_wrapped", "tenant", "%DLQ%tenant%workers"},
            {"system_topic", "tenant", "RMQ_SYS_TRACE_TOPIC"},
            {"system_consumer_group", "tenant", "CID_RMQ_SYS_HEART_BEAT"},
            {"custom_system_topic", "tenant", customSystemTopic},
            {"system_topic_prefix", "tenant", "rmq_sys_custom_topic"},
            {"empty_namespace", "", "orders"},
            {"empty_resource", "tenant", ""},
            {"null_namespace", null, "orders"},
            {"null_resource", "tenant", null},
            {"nulls", null, null},
            {"blank_resource", "tenant", " "},
            {"no_namespace_marker", "tenant", "orders%with%markers"},
            {"unicode", "租戶", "🚀-orders"},
            {"different_namespace", "tenant", "other%orders"}
        };
        List<Map<String, Object>> cases = new ArrayList<>();
        for (Object[] input : inputs) {
            String name = (String) input[0];
            String namespace = (String) input[1];
            String resource = (String) input[2];
            String wrapped = NamespaceUtil.wrapNamespace(namespace, resource);
            Map<String, Object> item = new TreeMap<>();
            item.put("name", name);
            item.put("namespace", namespace);
            item.put("resource", resource);
            item.put("wrapped", wrapped);
            item.put("wrappedAgain", NamespaceUtil.wrapNamespace(namespace, wrapped));
            item.put("unwrappedResource", NamespaceUtil.withoutNamespace(resource, namespace));
            item.put("unwrappedWrapped", NamespaceUtil.withoutNamespace(wrapped, namespace));
            item.put("withoutNamespace", NamespaceUtil.withoutNamespace(resource));
            item.put("isAlreadyWithNamespace", NamespaceUtil.isAlreadyWithNamespace(resource, namespace));
            item.put("wrapNamespaceAndRetry", NamespaceUtil.wrapNamespaceAndRetry(namespace, resource));
            item.put("namespaceFromResource", NamespaceUtil.getNamespaceFromResource(resource));
            item.put("withoutRetryAndDlq", NamespaceUtil.withOutRetryAndDLQ(resource));
            item.put("isRetryTopic", NamespaceUtil.isRetryTopic(resource));
            item.put("isDlqTopic", NamespaceUtil.isDLQTopic(resource));
            cases.add(item);
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("sourceCommit", SOURCE_COMMIT);
        result.put("clientArtifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        result.put("entrypoint", "all public NamespaceUtil methods and constants");
        result.put("customSystemTopic", customSystemTopic);
        result.put("namespaceSeparator", String.valueOf(NamespaceUtil.NAMESPACE_SEPARATOR));
        result.put("stringBlank", NamespaceUtil.STRING_BLANK);
        result.put("retryPrefixLength", NamespaceUtil.RETRY_PREFIX_LENGTH);
        result.put("dlqPrefixLength", NamespaceUtil.DLQ_PREFIX_LENGTH);
        result.put("serverContacted", false);
        result.put("cases", cases);
        return result;
    }
}
