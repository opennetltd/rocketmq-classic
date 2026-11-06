package compat;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.rocketmq.remoting.netty.NettyEncoder;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/** Probes the pinned Netty decoder limit and encoder behavior in a fresh JVM. */
public final class NettyFrameLimitProbe {
    private NettyFrameLimitProbe() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("expected fixture path and mode");
        }
        String fixture = Files.readString(Path.of(args[0]));
        String property = stringValue(fixture, "property");
        boolean configured = "configured".equals(args[1]);
        if (!configured && !"default".equals(args[1])) {
            throw new IllegalArgumentException("unknown mode: " + args[1]);
        }
        int expected = intValue(fixture, configured ? "configured" : "default");
        if (configured) {
            String raw = System.getProperty(property);
            if (raw == null || Integer.parseInt(raw) != expected) {
                throw new IllegalStateException("probe JVM did not receive the configured property");
            }
        } else {
            System.clearProperty(property);
        }

        Class<?> decoder = Class.forName("org.apache.rocketmq.remoting.netty.NettyDecoder");
        Field field = decoder.getDeclaredField("FRAME_MAX_LENGTH");
        field.setAccessible(true);
        int actual = field.getInt(null);
        if (actual != expected) {
            throw new IllegalStateException("NettyDecoder frame limit " + actual + " != " + expected);
        }
        if (configured) {
            int bodySize = intValue(fixture, "encoderProbeBodySize");
            RemotingCommand command = RemotingCommand.createRequestCommand(0, null);
            command.setBody(new byte[bodySize]);
            ByteBuf encoded = Unpooled.buffer();
            try {
                new NettyEncoder().encode(null, command, encoded);
                if (encoded.readableBytes() <= expected) {
                    throw new IllegalStateException("NettyEncoder applied the decoder frame limit");
                }
            } finally {
                encoded.release();
            }
        }
    }

    private static String stringValue(String json, String name) {
        Matcher matcher = Pattern.compile("\\\"" + Pattern.quote(name) + "\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"")
            .matcher(json);
        if (!matcher.find()) {
            throw new IllegalArgumentException("missing fixture string: " + name);
        }
        return matcher.group(1);
    }

    private static int intValue(String json, String name) {
        Matcher matcher = Pattern.compile("\\\"" + Pattern.quote(name) + "\\\"\\s*:\\s*(\\d+)")
            .matcher(json);
        if (!matcher.find()) {
            throw new IllegalArgumentException("missing fixture integer: " + name);
        }
        return Integer.parseInt(matcher.group(1));
    }
}
