package compat;

import java.lang.reflect.Field;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.utils.NetworkUtil;

/** Address helpers plus a named-scope interface oracle; no DNS or Broker. */
public final class RuntimeAddressFixtures {
    private RuntimeAddressFixtures() { }

    public static void main(String[] args) throws Exception {
        List<Object> addresses = new ArrayList<>();
        String[] literals = {"0.0.0.0", "9.255.255.255", "10.0.0.0", "10.255.255.255",
            "11.0.0.0", "11.0.0.1", "126.255.255.255", "127.0.0.2", "127.255.255.255",
            "128.0.0.0", "100.64.0.0", "169.254.0.0", "169.254.1.1", "172.15.255.255",
            "172.16.0.0", "172.16.0.1", "172.31.255.255", "172.32.0.0", "192.0.0.9",
            "192.0.2.1", "192.167.255.255", "192.168.0.0", "192.168.1.1",
            "192.168.255.255", "192.169.0.0", "198.18.0.0", "224.0.0.0",
            "255.255.255.255", "::", "::1", "fe7f::1", "fe80::1", "febf::1", "fec0::1",
            "feff::1", "ff00::1", "fc00::1", "fdff::1", "2001:db8::ABCD", "ffff::1",
            "::ffff:192.0.2.1"};
        for (String literal : literals) {
            // Numeric literals avoid name lookup. Keep mapped IPv6 as 16 bytes:
            // InetAddress.getByName otherwise converts it to Inet4Address.
            InetAddress ip = literal.equals("::ffff:192.0.2.1")
                ? Inet6Address.getByAddress(null, new byte[] {0,0,0,0,0,0,0,0,0,0,-1,-1,-64,0,2,1}, -1)
                : InetAddress.getByName(literal);
            add(addresses, literal, 0, ip);
        }
        add(addresses, "fe80::1", 7, Inet6Address.getByAddress(null,
            InetAddress.getByName("fe80::1").getAddress(), 7));
        List<Object> scopeCases = new ArrayList<>();
        var interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements() && scopeCases.isEmpty()) {
            NetworkInterface networkInterface = interfaces.nextElement();
            if (!networkInterface.isLoopback()) continue;
            var interfaceAddresses = networkInterface.getInetAddresses();
            while (interfaceAddresses.hasMoreElements()) {
                InetAddress address = interfaceAddresses.nextElement();
                if (address instanceof Inet6Address) {
                    Inet6Address ipv6 = (Inet6Address) address;
                    Inet6Address namedScope = Inet6Address.getByAddress(
                        null, ipv6.getAddress(), networkInterface);
                    Inet6Address unscoped = Inet6Address.getByAddress(
                        null, ipv6.getAddress(), -1);
                    Map<String, Object> item = new TreeMap<>();
                    item.put("interfaceName", networkInterface.getName());
                    item.put("ip", unscoped.getHostAddress());
                    item.put("scope", namedScope.getScopeId());
                    item.put("normalized", NetworkUtil.normalizeHostAddress(namedScope));
                    scopeCases.add(item);
                    break;
                }
            }
        }
        List<Object> clientAddressCases = new ArrayList<>();
        List<Object> javaPortDigitCases = new ArrayList<>();
        List<Object> javaHostCodeUnitRanges = new ArrayList<>();
        List<Object> javaHostCodePointRanges = new ArrayList<>();
        List<Object> channelTableKeyCases = new ArrayList<>();
        Map<String, Object> javaPortRangeCase = new TreeMap<>();
        AddressProbe addressProbe = new AddressProbe();
        int acceptedHostCodeUnitCount = 0;
        int acceptedHostCodePointCount = 0;
        try {
            for (int codeUnit = 0; codeUnit <= Character.MAX_VALUE; codeUnit++) {
                String digit = String.valueOf((char) codeUnit);
                try {
                    int parsed = Integer.parseInt(digit);
                    java.net.InetSocketAddress.createUnresolved("127.0.0.1", parsed);
                    Map<String, Object> item = new TreeMap<>();
                    item.put("codeUnit", codeUnit);
                    item.put("port", parsed);
                    javaPortDigitCases.add(item);
                } catch (RuntimeException rejected) {
                    // Rejections are represented by an absent code unit. Rust checks
                    // the entire BMP domain against this direct JDK oracle.
                }
            }
            List<Integer> acceptedHostCodeUnits = new ArrayList<>();
            for (int codeUnit = 0; codeUnit <= Character.MAX_VALUE; codeUnit++) {
                String expectedHost = "broker" + (char) codeUnit + ".example";
                String address = expectedHost + ":10911";
                try {
                    String[] hostAndPort = addressProbe.splitAddress(address);
                    int port = Integer.parseInt(hostAndPort[1]);
                    java.net.InetSocketAddress.createUnresolved(hostAndPort[0], port);
                    if (hostAndPort.length == 2 && expectedHost.equals(hostAndPort[0]) && port == 10911) {
                        acceptedHostCodeUnits.add(codeUnit);
                    }
                } catch (RuntimeException rejected) {
                    // Rejected UTF-16 units are represented by gaps in the ranges.
                }
            }
            if (!acceptedHostCodeUnits.isEmpty()) {
                int rangeStart = acceptedHostCodeUnits.get(0);
                int previous = rangeStart;
                for (int index = 1; index <= acceptedHostCodeUnits.size(); index++) {
                    if (index < acceptedHostCodeUnits.size()
                        && acceptedHostCodeUnits.get(index) == previous + 1) {
                        previous = acceptedHostCodeUnits.get(index);
                        continue;
                    }
                    List<Integer> range = new ArrayList<>();
                    range.add(rangeStart);
                    range.add(previous);
                    javaHostCodeUnitRanges.add(range);
                    if (index < acceptedHostCodeUnits.size()) {
                        int current = acceptedHostCodeUnits.get(index);
                        rangeStart = current;
                        previous = current;
                    }
                }
            }
            acceptedHostCodeUnitCount = acceptedHostCodeUnits.size();
            List<Integer> acceptedHostCodePoints = new ArrayList<>();
            for (int codePoint = Character.MIN_CODE_POINT; codePoint <= Character.MAX_CODE_POINT; codePoint++) {
                if (codePoint >= Character.MIN_SURROGATE && codePoint <= Character.MAX_SURROGATE) continue;
                String expectedHost = "broker" + new String(Character.toChars(codePoint)) + ".example";
                String address = expectedHost + ":10911";
                try {
                    String[] hostAndPort = addressProbe.splitAddress(address);
                    int port = Integer.parseInt(hostAndPort[1]);
                    java.net.InetSocketAddress.createUnresolved(hostAndPort[0], port);
                    if (hostAndPort.length == 2 && expectedHost.equals(hostAndPort[0]) && port == 10911) {
                        acceptedHostCodePoints.add(codePoint);
                    }
                } catch (RuntimeException rejected) {
                    // Rejected Unicode scalar values are represented by gaps in the ranges.
                }
            }
            if (!acceptedHostCodePoints.isEmpty()) {
                int rangeStart = acceptedHostCodePoints.get(0);
                int previous = rangeStart;
                for (int index = 1; index <= acceptedHostCodePoints.size(); index++) {
                    if (index < acceptedHostCodePoints.size()
                        && acceptedHostCodePoints.get(index) == previous + 1) {
                        previous = acceptedHostCodePoints.get(index);
                        continue;
                    }
                    List<Integer> range = new ArrayList<>();
                    range.add(rangeStart);
                    range.add(previous);
                    javaHostCodePointRanges.add(range);
                    if (index < acceptedHostCodePoints.size()) {
                        int current = acceptedHostCodePoints.get(index);
                        rangeStart = current;
                        previous = current;
                    }
                }
            }
            acceptedHostCodePointCount = acceptedHostCodePoints.size();
            String supplementaryDigit = new String(Character.toChars(0x104A1));
            for (String address : new String[] {":10911", "127.0.0.1:10911", "broker.example:10911",
                    "Broker.Example:10911", "127.0.0.1:010911",
                    "2001:db8::1:10911", "::1:10911", "[2001:db8::1]:10911",
                    "127.0.0.1:0", "127.0.0.1:65535", "127.0.0.1:65536", "127.0.0.1:-1",
                    "127.0.0.1:+1", "127.0.0.1:-0", "127.0.0.1:+0001", "127.0.0.1:00000",
                    "127.0.0.1:+\u0661", "127.0.0.1:1 ", "127.0.0.1: 1", "127.0.0.1:\t1",
                    "127.0.0.1:\uFF0B1", "127.0.0.1:\u22121", "127.0.0.1:00000000000000001",
                    "127.0.0.1:2147483647", "127.0.0.1:-2147483648", "127.0.0.1:-2147483649",
                    "127.0.0.1:\u0661\u0660\u0669\u0661\u0661",
                    "127.0.0.1:\uff11\uff12\uff13\uff14",
                    "127.0.0.1:1\u0662\uff13",
                    "127.0.0.1:\u0966\u0967\u0966\u0966\u0966",
                    "127.0.0.1:-\u0661", "127.0.0.1:\u0666\u0665\u0665\u0663\u0666",
                    "127.0.0.1:\u0662\u0661\u0664\u0667\u0664\u0668\u0663\u0666\u0664\u0668",
                    "127.0.0.1:" + supplementaryDigit,
                    "fe80::1%7:10911", "[fe80::1%7]:10911",
                    "fe80::1%lo0:10911", "[fe80::1%lo0]:10911", "", "broker.example",
                    "broker.example:", "broker.example: ", "host:1:2", "host::2",
                    "[2001:db8::1:10911", "[2001:db8::1]10911", "[2001:db8::1]junk:10911",
                    " host:10911", "host :10911"}) {
                Map<String, Object> item = new TreeMap<>();
                item.put("address", address);
                String[] hostAndPort;
                try {
                    hostAndPort = addressProbe.splitAddress(address);
                } catch (RuntimeException rejected) {
                    item.put("splitError", rejected.getClass().getSimpleName());
                    item.put("socketAddressAccepted", false);
                    clientAddressCases.add(item);
                    continue;
                }
                if (hostAndPort.length != 2) {
                    item.put("splitError", "split-length-" + hostAndPort.length);
                    item.put("socketAddressAccepted", false);
                    clientAddressCases.add(item);
                    continue;
                }
                boolean socketAddressAccepted;
                Integer portNumber = null;
                try {
                    portNumber = Integer.parseInt(hostAndPort[1]);
                    java.net.InetSocketAddress.createUnresolved(hostAndPort[0], portNumber);
                    socketAddressAccepted = true;
                } catch (RuntimeException rejected) {
                    socketAddressAccepted = false;
                }
                item.put("host", hostAndPort[0]);
                item.put("port", hostAndPort[1]);
                item.put("portNumber", portNumber);
                item.put("socketAddressAccepted", socketAddressAccepted);
                if (socketAddressAccepted && hostAndPort[0].isEmpty()) {
                    java.net.InetSocketAddress resolved =
                        new java.net.InetSocketAddress(hostAndPort[0], portNumber);
                    item.put("resolvedHostAddress", resolved.getAddress().getHostAddress());
                    item.put("resolvedHostIsLoopback",
                        resolved.getAddress() != null && resolved.getAddress().isLoopbackAddress());
                }
                if (hostAndPort[0].contains("%")) {
                    String scope = hostAndPort[0].substring(hostAndPort[0].lastIndexOf('%') + 1);
                    if (scope.endsWith("]")) scope = scope.substring(0, scope.length() - 1);
                    if (scope.matches("\\d+")) {
                        boolean scopedHostResolved;
                        try {
                            InetAddress.getByName(hostAndPort[0]);
                            scopedHostResolved = true;
                        } catch (java.net.UnknownHostException rejected) {
                            scopedHostResolved = false;
                        }
                        item.put("scopedHostResolved", scopedHostResolved);
                    }
                }
                clientAddressCases.add(item);
            }
            int acceptedPorts = 0;
            long acceptedPortSum = 0;
            boolean allPortsAccepted = true;
            for (int port = 0; port <= 65535; port++) {
                String address = "127.0.0.1:" + port;
                try {
                    String[] hostAndPort = addressProbe.splitAddress(address);
                    int parsedPort = Integer.parseInt(hostAndPort[1]);
                    java.net.InetSocketAddress.createUnresolved(hostAndPort[0], parsedPort);
                    if (hostAndPort.length != 2 || !"127.0.0.1".equals(hostAndPort[0])
                        || parsedPort != port) {
                        allPortsAccepted = false;
                    }
                    acceptedPorts++;
                    acceptedPortSum += parsedPort;
                } catch (RuntimeException rejected) {
                    allPortsAccepted = false;
                }
            }
            javaPortRangeCase.put("minimum", 0);
            javaPortRangeCase.put("maximum", 65535);
            javaPortRangeCase.put("acceptedCount", acceptedPorts);
            javaPortRangeCase.put("acceptedPortSum", acceptedPortSum);
            javaPortRangeCase.put("allAcceptedAndPreserved", allPortsAccepted);
            String[][] keyPairs = {
                {"127.0.0.1:10911", "127.0.0.1:010911"},
                {"Broker.Example:10911", "broker.example:10911"},
                {"2001:DB8::1:10911", "2001:db8::1:10911"},
                {"[2001:DB8::1]:10911", "[2001:db8::1]:10911"}
            };
            for (String[] pair : keyPairs) {
                Map<String, Object> item = new TreeMap<>();
                item.put("firstAddress", pair[0]);
                item.put("secondAddress", pair[1]);
                item.put("javaChannelTableKeysDistinct",
                    addressProbe.channelTableKeysDistinct(pair[0], pair[1]));
                channelTableKeyCases.add(item);
            }
        } finally {
            addressProbe.shutdown();
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("javaRuntime", System.getProperty("java.runtime.version"));
        fixture.put("javaVendor", System.getProperty("java.vendor"));
        fixture.put("entrypoint", "NetworkUtil.normalizeHostAddress/UtilAll.isInternalIP/isInternalV6IP/InetAddress.isLoopbackAddress/NettyRemotingClient.getHostAndPort/Integer.parseInt/Character.digit/channelTables");
        fixture.put("serverBaselineContacted", false);
        fixture.put("addresses", addresses);
        fixture.put("scopeCases", scopeCases);
        fixture.put("clientAddressCases", clientAddressCases);
        fixture.put("javaPortDigitCases", javaPortDigitCases);
        fixture.put("javaPortRangeCase", javaPortRangeCase);
        fixture.put("javaHostCodeUnitRanges", javaHostCodeUnitRanges);
        fixture.put("javaHostCodeUnitAcceptedCount", acceptedHostCodeUnitCount);
        fixture.put("javaHostCodePointRanges", javaHostCodePointRanges);
        fixture.put("javaHostCodePointAcceptedCount", acceptedHostCodePointCount);
        fixture.put("channelTableKeyCases", channelTableKeyCases);
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else if (args.length > 1 && "--verify-portable".equals(args[1])) {
            verifyPortableFixture(fixture, file);
        } else {
            Files.createDirectories(file.getParent());
            Files.writeString(file, json);
        }
        System.out.println("Runtime address helpers verified: " + addresses.size()
            + "; named IPv6 scopes verified: " + scopeCases.size()
            + "; Java client endpoint splits verified: " + clientAddressCases.size()
            + "; Java BMP port-digit code units verified: " + javaPortDigitCases.size()
            + "; Java BMP host code units verified: " + acceptedHostCodeUnitCount
            + "; Java Unicode host code points verified: " + acceptedHostCodePointCount);
    }

    @SuppressWarnings("unchecked")
    private static void verifyPortableFixture(Map<String, Object> actual, Path file) throws Exception {
        Map<String, Object> expected = (Map<String, Object>) com.alibaba.fastjson2.JSON.parseObject(
            Files.readString(file), Map.class);
        String expectedRuntime = runtimeMajor(String.valueOf(expected.get("javaRuntime")));
        String actualRuntime = runtimeMajor(String.valueOf(actual.get("javaRuntime")));
        if (!expectedRuntime.equals(actualRuntime)) {
            throw new IllegalStateException("Java runtime major drift: expected " + expectedRuntime
                + ", got " + actualRuntime);
        }
        expected.remove("javaRuntime");
        expected.remove("javaVendor");
        expected.remove("scopeCases");
        Map<String, Object> portableActual = (Map<String, Object>) com.alibaba.fastjson2.JSON.parseObject(
            com.alibaba.fastjson2.JSON.toJSONString(actual), Map.class);
        portableActual.remove("javaRuntime");
        portableActual.remove("javaVendor");
        portableActual.remove("scopeCases");
        removeNumericScopeResolution(expected);
        removeNumericScopeResolution(portableActual);
        if (!portableActual.equals(expected)) {
            List<String> drift = new ArrayList<>();
            findDifferences(expected, portableActual, "$", drift);
            throw new IllegalStateException("portable fixture drift in " + drift + ": " + file);
        }
    }

    private static void removeNumericScopeResolution(Map<String, Object> fixture) {
        Object casesValue = fixture.get("clientAddressCases");
        if (!(casesValue instanceof List<?>)) return;
        for (Object caseValue : (List<?>) casesValue) {
            if (caseValue instanceof Map<?, ?>) ((Map<?, ?>) caseValue).remove("scopedHostResolved");
        }
    }

    private static void findDifferences(Object expected, Object actual, String path, List<String> differences) {
        if (differences.size() >= 20) return;
        if (expected instanceof Map<?, ?> && actual instanceof Map<?, ?>) {
            Map<?, ?> expectedMap = (Map<?, ?>) expected;
            Map<?, ?> actualMap = (Map<?, ?>) actual;
            for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
                Object key = entry.getKey();
                if (!actualMap.containsKey(key)) {
                    differences.add(path + "." + key + " is missing");
                } else {
                    findDifferences(entry.getValue(), actualMap.get(key), path + "." + key, differences);
                }
                if (differences.size() >= 20) return;
            }
            for (Object key : actualMap.keySet()) {
                if (!expectedMap.containsKey(key)) differences.add(path + "." + key + " is unexpected");
                if (differences.size() >= 20) return;
            }
            return;
        }
        if (expected instanceof List<?> && actual instanceof List<?>) {
            List<?> expectedList = (List<?>) expected;
            List<?> actualList = (List<?>) actual;
            if (expectedList.size() != actualList.size()) {
                differences.add(path + ".length expected=" + expectedList.size() + " actual=" + actualList.size());
            }
            int count = Math.min(expectedList.size(), actualList.size());
            for (int index = 0; index < count; index++) {
                findDifferences(expectedList.get(index), actualList.get(index), path + "[" + index + "]", differences);
                if (differences.size() >= 20) return;
            }
            return;
        }
        if (!java.util.Objects.equals(expected, actual)) {
            differences.add(path + " expected=" + expected + " actual=" + actual);
        }
    }

    private static String runtimeMajor(String version) {
        int separator = version.indexOf('.');
        return separator < 0 ? version : version.substring(0, separator);
    }

    private static void add(List<Object> addresses, String literal, int scope, InetAddress ip) {
        Map<String, Object> item = new TreeMap<>();
        item.put("ip", literal); item.put("scope", scope);
        item.put("internal", ip instanceof Inet6Address ? UtilAll.isInternalV6IP(ip) : UtilAll.isInternalIP(ip.getAddress()));
        item.put("loopback", ip.isLoopbackAddress());
        item.put("normalized", NetworkUtil.normalizeHostAddress(ip));
        addresses.add(item);
    }

    private static final class AddressProbe extends org.apache.rocketmq.remoting.netty.NettyRemotingClient {
        private AddressProbe() {
            super(new org.apache.rocketmq.remoting.netty.NettyClientConfig());
        }

        private String[] splitAddress(String address) {
            return getHostAndPort(address);
        }

        @SuppressWarnings("unchecked")
        private boolean channelTableKeysDistinct(String first, String second) throws Exception {
            Field channelTablesField = getClass().getSuperclass().getDeclaredField("channelTables");
            channelTablesField.setAccessible(true);
            Map<String, Object> channelTables = (Map<String, Object>) channelTablesField.get(this);
            channelTables.clear();
            try {
                channelTables.put(first, Boolean.TRUE);
                channelTables.put(second, Boolean.TRUE);
                return channelTables.size() == 2;
            } finally {
                channelTables.clear();
            }
        }
    }
}
