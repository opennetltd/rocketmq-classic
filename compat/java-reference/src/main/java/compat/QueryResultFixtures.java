package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.common.message.MessageExt;

/** Invokes the pinned Java QueryResult constructor, getters and toString. */
public final class QueryResultFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private QueryResultFixtures() { }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("usage: QueryResultFixtures <fixture.json> [true|false|--verify]");
        }
        Path output = Path.of(args[0]);
        boolean verify = args.length > 1
            && (Boolean.parseBoolean(args[1]) || "--verify".equals(args[1]));
        String json = JSON.toJSONString(generate()) + "\n";
        if (verify) {
            if (!Files.exists(output) || !json.equals(Files.readString(output))) {
                throw new IllegalStateException("Java QueryResult fixture drift: " + output);
            }
            System.out.println("verified Java 5.5.1 QueryResult fixture at " + output);
        } else {
            Files.createDirectories(output.getParent());
            Files.writeString(output, json);
            System.out.println("wrote Java 5.5.1 QueryResult fixture to " + output);
        }
    }

    private static Map<String, Object> generate() throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("artifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", SOURCE_COMMIT);
        fixture.put("publicFields", publicFields());
        fixture.put("publicConstructors", publicConstructors());
        fixture.put("publicMethods", publicMethods());

        QueryResult nullList = new QueryResult(17L, null);
        fixture.put("nullListTimestamp", nullList.getIndexLastUpdateTimestamp());
        fixture.put("nullListGetterIsNull", nullList.getMessageList() == null);
        fixture.put("nullListToString", nullList.toString());

        List<MessageExt> messages = new ArrayList<>();
        messages.add(new MessageExt());
        messages.add(null);
        QueryResult result = new QueryResult(18L, messages);
        fixture.put("constructorRetainsListIdentity", result.getMessageList() == messages);
        fixture.put("nullElementPreserved", result.getMessageList().get(1) == null);
        fixture.put("messageExtToString", messages.get(0).toString());
        fixture.put("listToString", result.toString());
        messages.add(new MessageExt());
        fixture.put("toStringAfterCallerMutation", result.toString());
        fixture.put("messageCountAfterCallerMutation", result.getMessageList().size());
        return fixture;
    }

    private static List<String> publicFields() {
        return Arrays.stream(QueryResult.class.getFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(Field::getName)
            .sorted()
            .collect(Collectors.toList());
    }

    private static List<String> publicConstructors() {
        return Arrays.stream(QueryResult.class.getDeclaredConstructors())
            .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
            .map(QueryResultFixtures::constructorSignature)
            .sorted()
            .collect(Collectors.toList());
    }

    private static List<String> publicMethods() {
        return Arrays.stream(QueryResult.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(QueryResultFixtures::methodSignature)
            .sorted()
            .collect(Collectors.toList());
    }

    private static String constructorSignature(Constructor<?> constructor) {
        return constructor.getDeclaringClass().getName() + "(" + parameterTypes(constructor.getParameterTypes()) + ")";
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getName() + " " + method.getName() + "(" + parameterTypes(method.getParameterTypes()) + ")";
    }

    private static String parameterTypes(Class<?>[] types) {
        return String.join(",", Arrays.stream(types).map(Class::getName).collect(Collectors.toList()));
    }
}
