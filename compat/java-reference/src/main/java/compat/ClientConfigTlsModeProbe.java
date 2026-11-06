package compat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;

/** Isolated property probe; run in a fresh JVM before ClientConfig is initialized. */
public final class ClientConfigTlsModeProbe {
    private ClientConfigTlsModeProbe() { }

    public static void main(String[] args) throws Exception {
        System.setProperty("tls.server.mode", "enforcing");
        System.clearProperty("tls.enable");
        ClientConfig config = new ClientConfig();
        NettyClientConfig netty = new NettyClientConfig();
        System.setProperty("tls.enable", "true");
        ClientConfig configAfterPropertyMutation = new ClientConfig();
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("systemProperties", Map.of("tls.server.mode", "enforcing"));
        fixture.put("tlsEnable", TlsSystemConfig.tlsEnable);
        fixture.put("tlsMode", TlsSystemConfig.tlsMode.name());
        fixture.put("clientConfigUseTLS", config.isUseTLS());
        fixture.put("clientConfigUseTLSAfterPropertyMutation", configAfterPropertyMutation.isUseTLS());
        fixture.put("nettyClientConfigDefaultUseTLS", netty.isUseTLS());
        netty.setUseTLS(config.isUseTLS());
        fixture.put("runtimeCopiedUseTLS", netty.isUseTLS());
        fixture.put("tlsEnableAfterPropertyMutation", TlsSystemConfig.tlsEnable);
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) {
                throw new IllegalStateException("fixture drift: " + file);
            }
        } else {
            Files.createDirectories(file.getParent());
            Files.writeString(file, json);
        }
        System.out.println("ClientConfig TLS mode property probe verified");
    }
}
