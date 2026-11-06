package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.MessageUtils;

/** Invokes the pinned Java 5.5.1 public MessageUtils methods. */
public final class MessageUtilsFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private MessageUtilsFixtures() { }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("usage: MessageUtilsFixtures <fixture.json> [--verify]");
        }
        Path output = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(generate()) + "\n";
        if (verify) {
            if (!Files.exists(output) || !json.equals(Files.readString(output, StandardCharsets.UTF_8))) {
                throw new IllegalStateException("Java MessageUtils fixture drift: " + output);
            }
            System.out.println("verified Java 5.5.1 MessageUtils fixture at " + output);
        } else {
            Files.createDirectories(output.getParent());
            Files.writeString(output, json, StandardCharsets.UTF_8);
            System.out.println("wrote Java 5.5.1 MessageUtils fixture to " + output);
        }
    }

    private static Map<String, Object> generate() {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("artifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", SOURCE_COMMIT);
        fixture.put("publicMethods", publicMethods());
        fixture.put("shardingIndices", shardingIndices());
        fixture.put("messageShardingIndices", messageShardingIndices());
        fixture.put("distinctMessageIndexes", distinctMessageIndexes());
        fixture.put("deletePropertyCases", deletePropertyCases());
        return fixture;
    }

    private static List<String> publicMethods() {
        return Arrays.stream(MessageUtils.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(MessageUtilsFixtures::methodSignature)
            .sorted()
            .collect(Collectors.toList());
    }

    private static String methodSignature(Method method) {
        return method.getGenericReturnType().getTypeName() + " " + method.getName() + "("
            + Arrays.stream(method.getGenericParameterTypes())
                .map(type -> type.getTypeName())
                .collect(Collectors.joining(","))
            + ")";
    }

    private static List<Map<String, Object>> shardingIndices() {
        List<Map<String, Object>> cases = new ArrayList<>();
        for (String key : Arrays.asList("", "order-123", "鍵-🙂", "a\u0301", "four")) {
            for (int size : new int[] {1, 2, 3, 7, 16, -7}) {
                Map<String, Object> item = new TreeMap<>();
                item.put("key", key);
                item.put("indexSize", size);
                item.put("index", MessageUtils.getShardingKeyIndex(key, size));
                cases.add(item);
            }
        }
        Map<String, Object> zeroSize = new TreeMap<>();
        zeroSize.put("key", "zero");
        zeroSize.put("indexSize", 0);
        try {
            zeroSize.put("index", MessageUtils.getShardingKeyIndex("zero", 0));
        } catch (ArithmeticException exception) {
            zeroSize.put("error", exception.getClass().getSimpleName());
        }
        cases.add(zeroSize);
        return cases;
    }

    private static List<Map<String, Object>> messageShardingIndices() {
        List<Map<String, Object>> cases = new ArrayList<>();
        addMessageIndexCase(cases, "missing", null, 11);
        addMessageIndexCase(cases, "null-value", null, 11);
        addMessageIndexCase(cases, "empty", "", 11);
        addMessageIndexCase(cases, "unicode", "分片鍵🙂", 11);
        return cases;
    }

    private static void addMessageIndexCase(
        List<Map<String, Object>> cases, String name, String key, int indexSize) {
        MessageExt message = new MessageExt();
        if (!"missing".equals(name)) {
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_SHARDING_KEY, key);
        }
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("key", key);
        item.put("indexSize", indexSize);
        item.put("index", MessageUtils.getShardingKeyIndexByMsg(message, indexSize));
        cases.add(item);
    }

    private static List<Map<String, Object>> distinctMessageIndexes() {
        List<Map<String, Object>> cases = new ArrayList<>();
        List<MessageExt> messages = new ArrayList<>();
        messages.add(message("alpha"));
        messages.add(message("beta"));
        messages.add(message("alpha"));
        messages.add(message("gamma"));
        cases.add(indexSetCase("duplicates", messages, 5));
        cases.add(indexSetCase("empty", List.of(), 5));
        return cases;
    }

    private static MessageExt message(String shardingKey) {
        MessageExt message = new MessageExt();
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_SHARDING_KEY, shardingKey);
        return message;
    }

    private static Map<String, Object> indexSetCase(
        String name, Collection<MessageExt> messages, int indexSize) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("keys", messages.stream()
            .map(message -> message.getProperty(MessageConst.PROPERTY_SHARDING_KEY))
            .collect(Collectors.toList()));
        item.put("indexSize", indexSize);
        item.put("indexes", new TreeSet<>(MessageUtils.getShardingKeyIndexes(messages, indexSize)));
        return item;
    }

    private static List<Map<String, Object>> deletePropertyCases() {
        List<Map<String, Object>> cases = new ArrayList<>();
        addDeleteCase(cases, "null-input", null, "a");
        addDeleteCase(cases, "missing-key", "a\u00011\u0002b\u00012\u0002", "missing");
        addDeleteCase(cases, "leading-key", "a\u00011\u0002b\u00012\u0002", "a");
        addDeleteCase(cases, "middle-key", "a\u00011\u0002b\u00012\u0002c\u00013\u0002", "b");
        addDeleteCase(cases, "repeated-key", "a\u00011\u0002x\u00012\u0002a\u00013\u0002", "a");
        addDeleteCase(cases, "final-key-no-separator", "a\u00011\u0002b\u00012", "b");
        addDeleteCase(cases, "key-substring-in-value", "x\u0001label\u0002b\u00012\u0002", "a");
        addDeleteCase(cases, "key-substring-in-other-key", "aa\u00011\u0002b\u00012\u0002", "a");
        addDeleteCase(cases, "unicode-key", "鍵\u0001值\u0002x\u00011\u0002", "鍵");
        return cases;
    }

    private static void addDeleteCase(
        List<Map<String, Object>> cases, String name, String properties, String propertyName) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("properties", properties);
        item.put("propertyName", propertyName);
        item.put("result", MessageUtils.deleteProperty(properties, propertyName));
        cases.add(item);
    }
}
