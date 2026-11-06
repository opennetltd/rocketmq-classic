package compat;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.common.lite.LiteSubscriptionAction;
import org.apache.rocketmq.common.lite.LiteSubscriptionDTO;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

/** Invoked MQClientAPIExt evidence with only the remoting transport substituted. */
public final class InvokedExtFixtures {
    private InvokedExtFixtures() { }

    private static final class Capture {
        RemotingCommand request;
        RemotingCommand response;
        String method;

        RemotingClient proxy() {
            return (RemotingClient) Proxy.newProxyInstance(RemotingClient.class.getClassLoader(),
                new Class<?>[] {RemotingClient.class}, (proxy, invoked, args) -> {
                    if ("invoke".equals(invoked.getName())) {
                        method = invoked.getName();
                        request = (RemotingCommand) args[1];
                        return CompletableFuture.completedFuture(response);
                    }
                    return invoked.getReturnType() == boolean.class ? false : null;
                });
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }

    private static String quote(String value) {
        if (value == null) return "null";
        StringBuilder result = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            if (c == '"' || c == '\\') result.append('\\');
            if (c < 32) result.append(String.format("\\u%04x", (int) c));
            else result.append(c);
        }
        return result.append('"').toString();
    }

    private static String command(RemotingCommand command) {
        command.makeCustomHeaderToNet();
        StringBuilder ext = new StringBuilder("{");
        Map<String, String> fields = command.getExtFields();
        if (fields != null) {
            for (Map.Entry<String, String> entry : new TreeMap<>(fields).entrySet()) {
                if (ext.length() > 1) ext.append(',');
                ext.append(quote(entry.getKey())).append(':').append(quote(entry.getValue()));
            }
        }
        ext.append('}');
        return "{\"code\":" + command.getCode() + ",\"ext\":" + ext
            + ",\"bodyBase64\":" + quote(command.getBody() == null ? null
                : Base64.getEncoder().encodeToString(command.getBody())) + "}";
    }

    private static String fixture(String name, Capture capture, String outcome) {
        check("invoke".equals(capture.method) && capture.request != null
            && capture.request.getCode() == RequestCode.LITE_SUBSCRIPTION_CTL, name + " dispatch");
        return "{\"provenance\":{\"client\":\"org.apache.rocketmq:rocketmq-client:5.5.1\","
            + "\"entrypoint\":\"MQClientAPIExt.syncLiteSubscriptionAsync\","
            + "\"transport\":\"mock RemotingClient\",\"serverBaselineContacted\":false},"
            + "\"case\":" + quote(name) + ",\"mode\":\"ASYNC\",\"request\":" + command(capture.request)
            + ",\"response\":" + command(capture.response) + ",\"outcome\":" + quote(outcome) + "}\n";
    }

    public static void run(Path output, boolean verify) throws Exception {
        Capture capture = new Capture();
        ClientConfig config = new ClientConfig();
        config.setVipChannelEnabled(false);
        MQClientAPIExt api = new MQClientAPIExt(config, new NettyClientConfig(), null, null,
            ignored -> capture.proxy());
        LiteSubscriptionDTO dto = new LiteSubscriptionDTO().setAction(LiteSubscriptionAction.COMPLETE_ADD)
            .setClientId("client").setGroup("workers").setTopic("orders")
            .setLiteTopicSet(Set.of("lite-orders"));
        for (int code : new int[] {ResponseCode.SUCCESS, ResponseCode.SYSTEM_ERROR, 987654}) {
            String name = code == 0 ? "lite-subscription-success" : code == 1
                ? "lite-subscription-error" : "lite-subscription-unknown";
            capture.request = null;
            capture.method = null;
            capture.response = RemotingCommand.createResponseCommand(code, code == 0 ? null : "offline");
            String outcome;
            try {
                api.syncLiteSubscriptionAsync("broker:10911", dto, 10000).join();
                check(code == ResponseCode.SUCCESS, name + " accepted failure");
                outcome = "ok";
            } catch (CompletionException expected) {
                check(expected.getCause() instanceof MQBrokerException, name + " exception type");
                check(((MQBrokerException) expected.getCause()).getResponseCode() == code,
                    name + " exception code");
                outcome = "MQBrokerException;code=" + code;
            }
            Path file = output.resolve(name + ".json");
            String content = fixture(name, capture, outcome);
            if (verify) check(Files.exists(file) && content.equals(Files.readString(file)),
                "fixture drift: " + file);
            else {
                Files.createDirectories(output);
                Files.writeString(file, content, StandardCharsets.UTF_8);
            }
        }
    }
}