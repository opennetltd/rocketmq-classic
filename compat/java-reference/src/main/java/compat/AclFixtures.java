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
import java.io.ByteArrayInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclSigner;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.acl.common.SigningAlgorithm;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;

/** Invoked Java 5.5.1 ACL signing and canonicalization oracle. */
public final class AclFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private AclFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(render()) + "\n";
        if (verify) {
            if (!Files.exists(path) || !json.equals(Files.readString(path))) {
                throw new IllegalStateException("Java ACL fixture drift: " + path);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
    }

    private static Map<String, Object> render() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        result.put("sourceCommit", SOURCE_COMMIT);
        result.put("clientArtifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        result.put("entrypoints", List.of(
            "AclClientRPCHook.doBeforeRequest",
            "AclClientRPCHook.parseRequestContent",
            "AclUtils.combineRequestContent",
            "AclSigner.calSignature"));
        result.put("aclUtilsPublicConstructors", publicConstructors(AclUtils.class));
        result.put("aclUtilsPublicMethods", publicMethods(AclUtils.class));
        result.put("serverContacted", false);

        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(signCase("default_hmac_sha1", "access-key", "12345678", null,
            Map.of("topic", "orders", "queueId", "1"), "RocketMQ"));
        cases.add(signCase("unicode_body_and_token", "ak-內容", "secret-鍵", "token-临时",
            Map.of("ReqT", "2", "ns", "租戶", "\uD800\uDC00", "astral-key",
                "\uE000", "bmp-key"), "body-內容"));
        Map<String, String> existing = new LinkedHashMap<>();
        existing.put("Signature", "stale-signature");
        existing.put("SecurityToken", "preexisting-token");
        existing.put("topic", "existing-fields");
        cases.add(signCase("existing_signature_and_token", "ak-existing", "key-existing", null,
            existing, ""));
        result.put("cases", cases);
        result.put("customHeaderCase", customHeaderCase());

        Map<String, String> algorithms = new TreeMap<>();
        for (SigningAlgorithm algorithm : SigningAlgorithm.values()) {
            algorithms.put(algorithm.toString(), AclSigner.calSignature(
                "RocketMQ", "12345678", algorithm, StandardCharsets.UTF_8));
        }
        result.put("algorithmSignatures", algorithms);
        result.put("credentialStates", credentialStates());
        result.put("yamlHookCases", yamlHookCases());
        result.put("utilityCases", utilityCases());
        result.put("exceptionCases", exceptionCases());
        result.put("canonicalization", "sort keys by Java String order; concatenate values except Signature; append body bytes");
        return result;
    }

    private static Map<String, Object> customHeaderCase() {
        CreateTopicRequestHeader header = new CreateTopicRequestHeader();
        header.setTopic("header-topic");
        header.setDefaultTopic("header-default");
        header.setReadQueueNums(3);
        RemotingCommand request = RemotingCommand.createRequestCommand(17, header);
        request.setBody("header-body".getBytes(StandardCharsets.UTF_8));
        new AclClientRPCHook(new SessionCredentials("header-access", "header-secret"))
            .doBeforeRequest("127.0.0.1:10911", request);

        SortedMap<String, String> fields = new TreeMap<>(request.getExtFields());
        StringBuilder content = new StringBuilder();
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            if (!SessionCredentials.SIGNATURE.equals(entry.getKey())) {
                content.append(entry.getValue());
            }
        }
        byte[] prefix = content.toString().getBytes(SessionCredentials.CHARSET);
        byte[] body = request.getBody();
        byte[] canonical = new byte[prefix.length + body.length];
        System.arraycopy(prefix, 0, canonical, 0, prefix.length);
        System.arraycopy(body, 0, canonical, prefix.length, body.length);

        Map<String, Object> result = new TreeMap<>();
        result.put("fields", fields);
        result.put("canonicalBase64", Base64.getEncoder().encodeToString(canonical));
        result.put("signature", fields.get(SessionCredentials.SIGNATURE));
        return result;
    }

    private static List<String> publicConstructors(Class<?> type) {
        return Arrays.stream(type.getDeclaredConstructors())
            .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
            .map(AclFixtures::constructorSignature)
            .sorted()
            .collect(Collectors.toList());
    }

    private static List<String> publicMethods(Class<?> type) {
        return Arrays.stream(type.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(AclFixtures::methodSignature)
            .sorted()
            .collect(Collectors.toList());
    }

    private static String constructorSignature(Constructor<?> constructor) {
        return Modifier.toString(constructor.getModifiers()) + " " + constructor.getDeclaringClass().getName()
            + "(" + parameterTypes(constructor.getParameterTypes()) + ")";
    }

    private static String methodSignature(Method method) {
        return Modifier.toString(method.getModifiers()) + " " + method.getReturnType().getName() + " " +
            method.getName() + "(" + parameterTypes(method.getParameterTypes()) + ")";
    }

    private static String parameterTypes(Class<?>[] types) {
        return String.join(",", Arrays.stream(types).map(Class::getName).collect(Collectors.toList()));
    }

    private static Map<String, Object> utilityCases() throws Exception {
        Map<String, Object> cases = new TreeMap<>();
        cases.put("defaultSignature", AclUtils.calSignature("RocketMQ".getBytes(StandardCharsets.UTF_8), "12345678"));
        cases.put("customCharsetSignature", AclSigner.calSignature(
            "é", "ç", SigningAlgorithm.HmacSHA256, java.nio.charset.StandardCharsets.ISO_8859_1));
        cases.put("combineBytes", List.of(
            bytePair(null, null),
            bytePair(new byte[0], "body".getBytes(StandardCharsets.UTF_8)),
            bytePair("prefix".getBytes(StandardCharsets.UTF_8), new byte[0]),
            bytePair(new byte[0], new byte[0]),
            bytePair("prefix".getBytes(StandardCharsets.UTF_8), "body".getBytes(StandardCharsets.UTF_8))));
        cases.put("addressPredicates", sortedMap(
            "ipv4Valid", AclUtils.isScope("192.168.0.255", 4),
            "ipv4OutOfRange", AclUtils.isScope("192.168.256.1", 4),
            "ipv4InvalidShape", AclUtils.isScope("192.168.1", 4),
            "ipv6ValidPrefix", AclUtils.isScope("2001:db8::1", 2),
            "ipv6OutOfRange", AclUtils.isScope("2001:10000::1", 2),
            "scopeValueLow", AclUtils.isScope(-1),
            "scopeValueMax", AclUtils.isScope(255),
            "ipv6ScopeMax", AclUtils.isIPv6Scope(65535),
            "ipv6ScopeTooLarge", AclUtils.isIPv6Scope(65536),
            "scopeTokenWhitespace", AclUtils.isScope(" 42 "),
            "scopeTokenOutOfRange", AclUtils.isScope("256"),
            "colon", AclUtils.isColon("2001:db8::1"),
            "asterisk", AclUtils.isAsterisk("192.168.*"),
            "comma", AclUtils.isComma("a,b"),
            "minus", AclUtils.isMinus("1-2")));
        cases.put("customRequestContent", customRequestContent());
        cases.put("segmentPredicates", sortedMap(
            "ipv4", AclUtils.isScope(new String[] {"192", "168", "1", "2"}, 3),
            "ipv4Bad", AclUtils.isScope(new String[] {"192", "999", "1", "2"}, 3),
            "ipv6", AclUtils.isIPv6Scope(new String[] {"2001", "DB8", "FFFF"}, 3),
            "ipv6Bad", AclUtils.isIPv6Scope(new String[] {"2001", "10000"}, 2)));
        cases.put("expandedIp", AclUtils.expandIP("2001:db8::1", 8));
        cases.put("expandedIpTurkishLocaleCases", expandedIpTurkishLocaleCases());
        cases.put("expandedIpLithuanianLocaleCases", expandedIpLithuanianLocaleCases());
        cases.put("processedIp", AclUtils.v6ipProcess("2::ac5:78:1-200:*"));
        cases.put("braceAddresses", AclUtils.getAddresses("192.168.{1,2}", "{1,2}"));
        cases.put("validIpv6Check", addressCheck("2::ac5:78:1-200:*"));
        cases.put("invalidIpv6Check", addressCheck("2::ac5-78:1"));
        cases.put("validVerify", addressCheck("192.168.0.1/4"));
        cases.put("invalidVerify", addressCheck("192.168.256.1/4"));
        cases.put("yamlReader", AclUtils.getYamlDataObject(
            new ByteArrayInputStream("accessKey: yaml-ak\nsecretKey: yaml-sk\n".getBytes(StandardCharsets.UTF_8)),
            com.alibaba.fastjson2.JSONObject.class));
        cases.put("yamlNull", AclUtils.getYamlDataObject(
            new ByteArrayInputStream(new byte[0]), com.alibaba.fastjson2.JSONObject.class));
        Path yamlPath = Files.createTempFile("rocketmq-acl-fixture", ".yaml");
        try {
            Files.writeString(yamlPath, "accessKey: yaml-path-ak\nsecretKey: yaml-path-sk\n");
            cases.put("yamlPath", AclUtils.getYamlDataObject(yamlPath.toString(),
                com.alibaba.fastjson2.JSONObject.class));
            cases.put("yamlPathHook", yamlFileHook(yamlPath.toString()));
        } finally {
            Files.deleteIfExists(yamlPath);
        }
        return cases;
    }

    private static List<Object> expandedIpTurkishLocaleCases() {
        Locale originalLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            List<Object> cases = new ArrayList<>();
            for (String input : new String[] {"i::1", "ı::1", "2001:db8::1"}) {
                Map<String, Object> item = new TreeMap<>();
                item.put("input", input);
                item.put("part", 8);
                item.put("output", AclUtils.expandIP(input, 8));
                cases.add(item);
            }
            return cases;
        } finally {
            Locale.setDefault(originalLocale);
        }
    }

    private static List<Object> expandedIpLithuanianLocaleCases() {
        Locale originalLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("lt-LT"));
            List<Object> cases = new ArrayList<>();
            for (String input : new String[] {
                "i\u0307::1",
                "j\u0307::1",
                "x\u0307::1",
                "ı\u0307::1",
                "i\u0323\u0307::1",
                "i\u0315\u0307::1",
                "i\u0334\u0307::1",
                "i\u0301\u0307::1",
                "i\u034f\u0307::1",
                "i\u0307\u0307::1",
                "į\u0307::1",
                "і\u0307::1",
                "\uD83D\uDE00::1"
            }) {
                Map<String, Object> item = new TreeMap<>();
                item.put("input", input);
                item.put("part", 8);
                item.put("output", AclUtils.expandIP(input, 8));
                cases.add(item);
            }
            return cases;
        } finally {
            Locale.setDefault(originalLocale);
        }
    }

    private static Map<String, Object> customRequestContent() {
        RemotingCommand request = RemotingCommand.createRequestCommand(100, null);
        byte[] body = "body-內容".getBytes(StandardCharsets.UTF_8);
        request.setBody(body);
        request.addExtField("unused", "request-field-not-in-map");
        SortedMap<String, String> fields = new TreeMap<>();
        fields.put("Signature", "omit-this-value");
        fields.put("tenant", "租戶");
        fields.put("\uE000", "bmp-last");
        fields.put("\uD800\uDC00", "supplementary-first");

        List<List<String>> orderedFields = new ArrayList<>();
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            orderedFields.add(List.of(entry.getKey(), entry.getValue()));
        }
        Map<String, Object> result = new TreeMap<>();
        result.put("fieldsInOrder", orderedFields);
        result.put("bodyBase64", Base64.getEncoder().encodeToString(body));
        result.put("combinedBase64", Base64.getEncoder().encodeToString(
            AclUtils.combineRequestContent(request, fields)));
        return result;
    }

    private static Map<String, Object> exceptionCases() {
        Map<String, Object> cases = new TreeMap<>();
        cases.put("statusCode", exceptionFields(new AclException("AUTH_FAILED", 17)));
        cases.put("statusCodeMessage", exceptionFields(new AclException("AUTH_FAILED", 17, "denied")));
        cases.put("message", exceptionFields(new AclException("invalid ACL")));
        cases.put("messageCause", exceptionFields(new AclException("invalid ACL", new IllegalStateException("cause"))));
        cases.put("statusCodeMessageCause", exceptionFields(new AclException(
            "AUTH_FAILED", 17, "denied", new IllegalStateException("cause"))));
        AclException mutable = new AclException("initial", 1);
        mutable.setStatus("UPDATED");
        mutable.setCode(23);
        cases.put("mutable", exceptionFields(mutable));
        return cases;
    }

    private static Map<String, Object> exceptionFields(AclException exception) {
        Map<String, Object> result = new TreeMap<>();
        result.put("status", exception.getStatus());
        result.put("code", exception.getCode());
        result.put("message", exception.getMessage());
        result.put("causeMessage", exception.getCause() == null ? null : exception.getCause().getMessage());
        return result;
    }

    private static Map<String, Object> bytePair(byte[] first, byte[] second) {
        byte[] combined = AclUtils.combineBytes(first, second);
        Map<String, Object> result = new TreeMap<>();
        result.put("firstBase64", first == null ? null : Base64.getEncoder().encodeToString(first));
        result.put("secondBase64", second == null ? null : Base64.getEncoder().encodeToString(second));
        result.put("combinedBase64", combined == null ? null : Base64.getEncoder().encodeToString(combined));
        result.put("reusesFirst", combined != null && combined == first);
        result.put("reusesSecond", combined != null && combined == second);
        return result;
    }

    private static String addressCheck(String input) {
        try {
            if (input.contains("/")) {
                String[] parts = input.split("/");
                AclUtils.verify(parts[0], Integer.parseInt(parts[1]));
            } else {
                AclUtils.IPv6AddressCheck(input);
            }
            return "ok";
        } catch (RuntimeException error) {
            return Objects.toString(error.getMessage(), "");
        }
    }

    private static Map<String, Object> yamlFileHook(String path) {
        RPCHook hook = AclUtils.getAclRPCHook(path);
        Map<String, Object> result = new TreeMap<>();
        result.put("hookPresent", hook != null);
        if (hook instanceof AclClientRPCHook) {
            result.put("credentials", credentialFields(((AclClientRPCHook) hook).getSessionCredentials()));
        }
        return result;
    }

    private static Map<String, Object> credentialStates() {
        Map<String, Object> states = new TreeMap<>();
        SessionCredentials twoArguments = new SessionCredentials("ak-two", "sk-two");
        states.put("twoArguments", credentialFields(twoArguments));

        SessionCredentials threeArguments = new SessionCredentials("ak-three", "sk-three", "token-three");
        states.put("threeArguments", credentialFields(threeArguments));

        Properties properties = new Properties();
        properties.setProperty(SessionCredentials.ACCESS_KEY, "  ak-property\t");
        properties.setProperty(SessionCredentials.SECRET_KEY, " sk-property ");
        properties.setProperty(SessionCredentials.SECURITY_TOKEN, " token-property ");
        SessionCredentials updated = new SessionCredentials(null, null);
        updated.setSignature("signature-property");
        updated.updateContent(properties);
        states.put("updatedProperties", credentialFields(updated));

        String keyFile = "# RocketMQ key file\n"
            + "AccessKey=file-ak\n"
            + "SecretKey=se\\u0063ret\n"
            + "SecurityToken=token-file\n";
        Properties keyProperties = org.apache.rocketmq.common.MixAll.string2Properties(keyFile);
        SessionCredentials fromKeyFile = new SessionCredentials(null, null);
        fromKeyFile.updateContent(keyProperties);
        states.put("keyFileProperties", credentialFields(fromKeyFile));

        SessionCredentials tokenA = new SessionCredentials("ak", "sk", "token-a");
        SessionCredentials tokenB = new SessionCredentials("ak", "sk", "token-b");
        states.put("tokenIgnoredByEquality", tokenA.equals(tokenB));
        states.put("tokenIgnoredByHash", tokenA.hashCode() == tokenB.hashCode());
        tokenB.setSignature("signature");
        states.put("signatureIncludedByEquality", !tokenA.equals(tokenB));
        return states;
    }

    private static Map<String, Object> yamlHookCases() throws Exception {
        Map<String, Object> cases = new TreeMap<>();
        cases.put("valid", yamlHook("accessKey: yaml-ak\nsecretKey: yaml-sk\n"));
        cases.put("missingSecret", yamlHook("accessKey: yaml-ak\n"));
        cases.put("blankAccess", yamlHook("accessKey: '  '\nsecretKey: yaml-sk\n"));
        cases.put("malformed", yamlHook("accessKey: [broken\nsecretKey: yaml-sk\n"));
        return cases;
    }

    private static Map<String, Object> yamlHook(String yaml) throws Exception {
        RPCHook hook = AclUtils.getAclRPCHook(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));
        Map<String, Object> result = new TreeMap<>();
        result.put("hookPresent", hook != null);
        if (hook instanceof AclClientRPCHook) {
            result.put("credentials", credentialFields(((AclClientRPCHook) hook).getSessionCredentials()));
        }
        return result;
    }

    private static Map<String, Object> credentialFields(SessionCredentials credentials) {
        Map<String, Object> fields = new TreeMap<>();
        fields.put("accessKey", credentials.getAccessKey());
        fields.put("secretKey", credentials.getSecretKey());
        fields.put("signature", credentials.getSignature());
        fields.put("securityToken", credentials.getSecurityToken());
        return fields;
    }

    private static Map<String, Object> signCase(
        String name,
        String accessKey,
        String secretKey,
        String securityToken,
        Map<String, String> inputFields,
        String bodyText) {
        RemotingCommand request = RemotingCommand.createRequestCommand(100, null);
        inputFields.forEach(request::addExtField);
        byte[] body = bodyText.getBytes(StandardCharsets.UTF_8);
        request.setBody(body);
        new AclClientRPCHook(new SessionCredentials(accessKey, secretKey, securityToken))
            .doBeforeRequest("127.0.0.1:10911", request);

        SortedMap<String, String> fields = new TreeMap<>(request.getExtFields());
        StringBuilder content = new StringBuilder();
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            if (!SessionCredentials.SIGNATURE.equals(entry.getKey())) {
                content.append(entry.getValue());
            }
        }
        byte[] prefix = content.toString().getBytes(SessionCredentials.CHARSET);
        byte[] canonical = new byte[prefix.length + body.length];
        System.arraycopy(prefix, 0, canonical, 0, prefix.length);
        System.arraycopy(body, 0, canonical, prefix.length, body.length);

        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("credentials", sortedMap(
            "accessKey", accessKey,
            "secretKey", secretKey,
            "securityToken", securityToken));
        item.put("inputFields", new TreeMap<>(inputFields));
        item.put("bodyBase64", Base64.getEncoder().encodeToString(body));
        item.put("canonicalBase64", Base64.getEncoder().encodeToString(canonical));
        item.put("fields", fields);
        item.put("signature", fields.get(SessionCredentials.SIGNATURE));
        return item;
    }

    private static Map<String, Object> sortedMap(Object... fields) {
        Map<String, Object> result = new TreeMap<>();
        for (int index = 0; index < fields.length; index += 2) {
            result.put((String) fields[index], fields[index + 1]);
        }
        return result;
    }
}
