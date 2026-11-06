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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody;
import org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody;
import org.apache.rocketmq.remoting.protocol.admin.TopicOffset;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;

/** Invoked Java 5.5.1 identity-equality oracle for protocol DTOs. */
public final class ProtocolDtoIdentityFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private ProtocolDtoIdentityFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(render()) + "\n";
        if (verify) {
            if (!Files.exists(path) || !json.equals(Files.readString(path))) {
                throw new IllegalStateException("Java protocol DTO identity fixture drift: " + path);
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
        result.put("identityCases", identityCases());
        return result;
    }

    private static Map<String, Object> identityCases() throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("QueryAssignmentRequestBody", identityCase(
            new QueryAssignmentRequestBody(), new QueryAssignmentRequestBody()));
        result.put("SetMessageRequestModeRequestBody", identityCase(
            new SetMessageRequestModeRequestBody(), new SetMessageRequestModeRequestBody()));
        result.put("GetConsumerStatusBody", identityCase(
            new GetConsumerStatusBody(), new GetConsumerStatusBody()));
        result.put("ResetOffsetBody", identityCase(new ResetOffsetBody(), new ResetOffsetBody()));
        result.put("ConsumeMessageDirectlyResult", identityCase(
            new ConsumeMessageDirectlyResult(), new ConsumeMessageDirectlyResult()));
        result.put("TopicOffset", identityCase(new TopicOffset(), new TopicOffset()));
        result.put("TopicStatsTable", identityCase(new TopicStatsTable(), new TopicStatsTable()));
        return result;
    }

    private static Map<String, Object> identityCase(Object first, Object second) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        result.put("equalsDeclaredBy", first.getClass().getMethod("equals", Object.class)
            .getDeclaringClass().getName());
        result.put("hashCodeDeclaredBy", first.getClass().getMethod("hashCode")
            .getDeclaringClass().getName());
        result.put("sameObjectEqualsItself", first.equals(first));
        result.put("distinctDefaultObjectsEquals", first.equals(second));
        return result;
    }
}
