package compat;

import com.alibaba.fastjson2.JSON;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.common.utils.NetworkUtil;

/** Pinned Linux JDK scope-ID and /proc/net/if_inet6 oracle; no Broker or socket connection. */
public final class RuntimeLinuxAddressFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private RuntimeLinuxAddressFixtures() { }

    public static void main(String[] args) throws Exception {
        if (!"Linux".equals(System.getProperty("os.name"))) {
            throw new IllegalStateException("Linux address fixture requires a Linux JVM");
        }
        if (args.length == 3 && "--capture-candidates".equals(args[1])) {
            captureCurrentCandidates(Paths.get(args[2]));
            return;
        }
        List<String> procRows = Files.readAllLines(Paths.get("/proc/net/if_inet6"), StandardCharsets.US_ASCII);
        List<Object> loopbackCandidates = new ArrayList<>();
        for (InetAddress address : NetworkUtil.getLocalInetAddressList()) {
            if (!(address instanceof Inet6Address) || !address.isLoopbackAddress()) {
                continue;
            }
            Inet6Address ipv6 = (Inet6Address) address;
            NetworkInterface networkInterface = NetworkInterface.getByInetAddress(address);
            if (networkInterface == null) {
                throw new IllegalStateException("IPv6 loopback has no NetworkInterface");
            }
            String expectedHex = hex(address.getAddress());
            String procRow = null;
            for (String row : procRows) {
                String[] fields = row.trim().split("\\s+");
                if (fields.length >= 6 && expectedHex.equalsIgnoreCase(fields[0])
                    && networkInterface.getName().equals(fields[5])) {
                    procRow = row;
                    break;
                }
            }
            if (procRow == null) {
                throw new IllegalStateException("IPv6 loopback is missing from /proc/net/if_inet6");
            }
            Map<String, Object> candidate = new TreeMap<>();
            candidate.put("interfaceName", networkInterface.getName());
            candidate.put("ip", InetAddress.getByAddress(address.getAddress()).getHostAddress());
            candidate.put("scopeId", ipv6.getScopeId());
            candidate.put("hostAddress", ipv6.getHostAddress());
            candidate.put("normalized", NetworkUtil.normalizeHostAddress(ipv6));
            candidate.put("procEntryAddress", expectedHex);
            candidate.put("procEntryInterface", networkInterface.getName());
            loopbackCandidates.add(candidate);
        }

        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", SOURCE_COMMIT);
        fixture.put("entrypoint", "NetworkUtil.getLocalInetAddressList/normalizeHostAddress + /proc/net/if_inet6");
        fixture.put("serverBaselineContacted", false);
        fixture.put("linuxIpv6LoopbackCandidates", loopbackCandidates);
        String json = JSON.toJSONString(fixture) + "\n";
        Path file = Paths.get(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(new String(Files.readAllBytes(file), StandardCharsets.UTF_8))) {
                throw new IllegalStateException("fixture drift: " + file);
            }
        } else {
            Files.createDirectories(file.getParent());
            Files.write(file, json.getBytes(StandardCharsets.UTF_8));
        }
        System.out.println("Linux Java IPv6 loopback candidates verified: " + loopbackCandidates.size());
    }

    private static void captureCurrentCandidates(Path file) throws Exception {
        List<Object> candidates = new ArrayList<>();
        for (InetAddress address : NetworkUtil.getLocalInetAddressList()) {
            NetworkInterface networkInterface = NetworkInterface.getByInetAddress(address);
            if (networkInterface == null) {
                throw new IllegalStateException("address has no NetworkInterface: " + address);
            }
            Map<String, Object> candidate = new TreeMap<>();
            candidate.put("interfaceName", networkInterface.getName());
            candidate.put("hostAddress", address.getHostAddress());
            candidate.put("normalized", NetworkUtil.normalizeHostAddress(address));
            candidates.add(candidate);
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", SOURCE_COMMIT);
        fixture.put("entrypoint", "NetworkUtil.getLocalInetAddressList/normalizeHostAddress");
        fixture.put("serverBaselineContacted", false);
        fixture.put("linuxCandidates", candidates);
        Files.createDirectories(file.getParent());
        Files.write(file, (JSON.toJSONString(fixture) + "\n").getBytes(StandardCharsets.UTF_8));
        System.out.println("Captured Java Linux address candidates: " + candidates.size());
    }

    private static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder(bytes.length * 2);
        for (byte item : bytes) {
            value.append(String.format("%02x", item & 0xff));
        }
        return value.toString();
    }
}
