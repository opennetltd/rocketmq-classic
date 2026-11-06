package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

/** Reflects every public numeric request/response constant from the pinned Java remoting API. */
public final class CodeRegistryFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private CodeRegistryFixtures() { }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("usage: CodeRegistryFixtures <fixture.json> [--verify]");
        }
        Path output = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(generate()) + "\n";
        if (verify) {
            if (!Files.exists(output) || !json.equals(Files.readString(output))) {
                throw new IllegalStateException("Java request/response code registry drift: " + output);
            }
            System.out.println("verified Java 5.5.1 request/response code registry at " + output);
        } else {
            Files.createDirectories(output.getParent());
            Files.writeString(output, json);
            System.out.println("wrote Java 5.5.1 request/response code registry to " + output);
        }
    }

    private static Map<String, Object> generate() throws IllegalAccessException {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("artifact", "org.apache.rocketmq:rocketmq-remoting:5.5.1");
        fixture.put("sourceCommit", SOURCE_COMMIT);
        fixture.put("requestCode", publicIntConstants(RequestCode.class));
        fixture.put("responseCode", publicIntConstants(ResponseCode.class));
        fixture.put("remotingSysResponseCode", publicIntConstants(RemotingSysResponseCode.class));
        fixture.put("serverBaselineContacted", false);
        return fixture;
    }

    private static Map<String, Integer> publicIntConstants(Class<?> type) throws IllegalAccessException {
        Map<String, Integer> values = new TreeMap<>();
        // getFields() includes public constants inherited by ResponseCode from
        // RemotingSysResponseCode, which are part of Java's public API surface.
        for (Field field : type.getFields()) {
            int modifiers = field.getModifiers();
            if (field.getType() == int.class
                && Modifier.isPublic(modifiers)
                && Modifier.isStatic(modifiers)
                && Modifier.isFinal(modifiers)) {
                values.put(field.getName(), field.getInt(null));
            }
        }
        return values;
    }
}
