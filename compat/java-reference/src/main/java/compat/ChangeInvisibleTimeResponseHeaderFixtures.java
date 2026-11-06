package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeResponseHeader;

/** Direct public-API and reflection-decoder oracle for the POP response header. */
public final class ChangeInvisibleTimeResponseHeaderFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private ChangeInvisibleTimeResponseHeaderFixtures() {
    }

    private static void check(boolean condition, String label) {
        if (!condition) throw new IllegalStateException(label);
    }

    private static Map<String, Object> values(ChangeInvisibleTimeResponseHeader header) {
        Map<String, Object> result = new TreeMap<>();
        result.put("popTime", header.getPopTime());
        result.put("invisibleTime", header.getInvisibleTime());
        result.put("reviveQid", header.getReviveQid());
        return result;
    }

    private static Map<String, Object> decode(Map<String, String> fields, boolean nullMap,
        boolean useFastEncode) throws Exception {
        RemotingCommand command = RemotingCommand.createResponseCommand(0, null);
        command.setExtFields(nullMap ? null : new HashMap<>(fields));
        ChangeInvisibleTimeResponseHeader header = command.decodeCommandCustomHeaderDirectly(
            ChangeInvisibleTimeResponseHeader.class, useFastEncode);
        return values(header);
    }

    private static Map<String, Object> caseValues(Map<String, String> fields, boolean nullMap)
        throws Exception {
        Map<String, Object> result = new TreeMap<>();
        result.put("fields", nullMap ? null : fields);
        result.put("fast", decode(fields, nullMap, true));
        result.put("reflection", decode(fields, nullMap, false));
        return result;
    }

    private static String signature(Method method) {
        StringBuilder result = new StringBuilder(method.getName()).append('(');
        Class<?>[] parameters = method.getParameterTypes();
        for (int index = 0; index < parameters.length; index++) {
            if (index > 0) result.append(',');
            result.append(parameters[index].getTypeName());
        }
        return result.append("):").append(method.getReturnType().getTypeName()).toString();
    }

    public static void main(String[] args) throws Exception {
        check(args.length >= 1, "fixture output path");
        java.nio.file.Path output = java.nio.file.Path.of(args[0]);
        boolean verify = Arrays.asList(args).contains("--verify");
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", SOURCE_COMMIT);
        fixture.put("headerType", ChangeInvisibleTimeResponseHeader.class.getName());
        fixture.put("publicConstructor", java.lang.reflect.Modifier.isPublic(
            ChangeInvisibleTimeResponseHeader.class.getConstructor().getModifiers()));
        fixture.put("publicFieldCount", Arrays.stream(ChangeInvisibleTimeResponseHeader.class.getFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .count());

        TreeSet<String> methods = new TreeSet<>();
        for (Method method : ChangeInvisibleTimeResponseHeader.class.getDeclaredMethods()) {
            if (java.lang.reflect.Modifier.isPublic(method.getModifiers())) {
                methods.add(signature(method));
            }
        }
        fixture.put("declaredPublicMethods", methods);

        ChangeInvisibleTimeResponseHeader defaults = new ChangeInvisibleTimeResponseHeader();
        fixture.put("defaults", values(defaults));
        ChangeInvisibleTimeResponseHeader assigned = new ChangeInvisibleTimeResponseHeader();
        assigned.setPopTime(Long.MIN_VALUE);
        assigned.setInvisibleTime(Long.MAX_VALUE);
        assigned.setReviveQid(Integer.MIN_VALUE);
        fixture.put("accessors", values(assigned));

        Map<String, Object> identity = new TreeMap<>();
        identity.put("equalsDeclaringClass", ChangeInvisibleTimeResponseHeader.class
            .getMethod("equals", Object.class).getDeclaringClass().getName());
        identity.put("hashCodeDeclaringClass", ChangeInvisibleTimeResponseHeader.class
            .getMethod("hashCode").getDeclaringClass().getName());
        identity.put("selfEquals", assigned.equals(assigned));
        identity.put("distinctDefaultEquals",
            defaults.equals(new ChangeInvisibleTimeResponseHeader()));
        fixture.put("identity", identity);

        Map<String, Object> cases = new TreeMap<>();
        Map<String, String> empty = new TreeMap<>();
        cases.put("emptyMap", caseValues(empty, false));
        Map<String, String> valid = new TreeMap<>();
        valid.put("popTime", "-9223372036854775808");
        valid.put("invisibleTime", "9223372036854775807");
        valid.put("reviveQid", "-2147483648");
        cases.put("signedBoundaries", caseValues(valid, false));
        Map<String, String> malformed = new TreeMap<>();
        malformed.put("popTime", "bad");
        malformed.put("invisibleTime", "456");
        malformed.put("reviveQid", "2147483648");
        cases.put("malformedFieldsRetainDefaults", caseValues(malformed, false));
        Map<String, String> unicode = new TreeMap<>();
        unicode.put("popTime", "١٢٣");
        unicode.put("invisibleTime", "٤٥٦");
        unicode.put("reviveQid", "٢");
        cases.put("unicodeDecimalDigits", caseValues(unicode, false));
        cases.put("nullMap", caseValues(empty, true));
        fixture.put("cases", cases);

        RemotingCommand encoded = RemotingCommand.createResponseCommandWithHeader(0, assigned);
        encoded.makeCustomHeaderToNet();
        fixture.put("encodedFields", new TreeMap<>(encoded.getExtFields()));

        String json = JSON.toJSONString(fixture) + "\n";
        if (verify) {
            check(java.nio.file.Files.exists(output)
                && java.nio.file.Files.readString(output).equals(json),
                "fixture drift " + output);
        } else {
            java.nio.file.Files.createDirectories(output.getParent());
            java.nio.file.Files.writeString(output, json);
        }
    }
}
