package compat;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

/** Invoked 5.5.1 inbound shutdown response behavior. */
public final class RuntimeRemotingShutdownFixtures {
    private static final int REQUEST_CODE = 200_081;

    private RuntimeRemotingShutdownFixtures() { }

    private static final class ContextCapture extends ChannelInboundHandlerAdapter {
        ChannelHandlerContext context;

        @Override
        public void handlerAdded(ChannelHandlerContext context) {
            this.context = context;
        }
    }

    private static final class CountingProcessor implements NettyRequestProcessor {
        final AtomicInteger rejects = new AtomicInteger();
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public RemotingCommand processRequest(ChannelHandlerContext context, RemotingCommand request) {
            calls.incrementAndGet();
            return null;
        }

        @Override
        public boolean rejectRequest() {
            rejects.incrementAndGet();
            return true;
        }
    }

    private static void setShuttingDown(NettyRemotingClient client, boolean value) throws Exception {
        Field field = NettyRemotingAbstract.class.getDeclaredField("isShuttingDown");
        field.setAccessible(true);
        ((AtomicBoolean) field.get(client)).set(value);
    }

    private static Map<String, Object> invoke(
        NettyRemotingClient client, String name, boolean shuttingDown, int version, int requestCode,
        CountingProcessor processor) throws Exception {
        setShuttingDown(client, shuttingDown);
        if (processor != null) client.registerProcessor(REQUEST_CODE, processor, null);

        ContextCapture capture = new ContextCapture();
        EmbeddedChannel channel = new EmbeddedChannel(capture);
        try {
            RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, null);
            request.setVersion(version);
            request.setOpaque(701 + version);
            client.processRequestCommand(capture.context, request);
            channel.runPendingTasks();
            RemotingCommand response = channel.readOutbound();

            Map<String, Object> item = new TreeMap<>();
            item.put("name", name);
            item.put("shuttingDown", shuttingDown);
            item.put("requestVersion", version);
            item.put("requestCode", requestCode);
            item.put("responsePresent", response != null);
            item.put("processorCalls", processor == null ? 0 : processor.calls.get());
            item.put("rejectCalls", processor == null ? 0 : processor.rejects.get());
            if (response != null) {
                item.put("responseCode", response.getCode());
                item.put("remark", response.getRemark());
                item.put("opaque", response.getOpaque());
            }
            return item;
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public static void main(String[] args) throws Exception {
        NettyRemotingClient client = new NettyRemotingClient(new NettyClientConfig());
        try {
            int threshold = MQVersion.Version.V5_3_1.ordinal();
            List<Object> cases = new ArrayList<>();
            CountingProcessor atThreshold = new CountingProcessor();
            cases.add(invoke(client, "shutdown-at-v5_3_1", true, threshold, REQUEST_CODE, atThreshold));

            CountingProcessor newer = new CountingProcessor();
            cases.add(invoke(client, "shutdown-above-v5_3_1", true, threshold + 1, REQUEST_CODE, newer));

            CountingProcessor accepting = new CountingProcessor();
            cases.add(invoke(client, "running-above-v5_3_1", false, threshold + 1, REQUEST_CODE, accepting));

            cases.add(invoke(client, "unsupported-above-v5_3_1", true, threshold + 1,
                REQUEST_CODE + 1, null));

            Map<String, Object> fixture = new TreeMap<>();
            fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
            fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
            fixture.put("entrypoint", "NettyRemotingAbstract.processRequestCommand");
            fixture.put("shutdownVersionThreshold", threshold);
            fixture.put("goAwayResponseCode", ResponseCode.GO_AWAY);
            fixture.put("busyResponseCode", RemotingSysResponseCode.SYSTEM_BUSY);
            fixture.put("unsupportedResponseCode", RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED);
            fixture.put("serverBaselineContacted", false);
            fixture.put("cases", cases);

            String json = com.alibaba.fastjson2.JSON.toJSONString(fixture) + "\n";
            Path file = Path.of(args[0]);
            if (args.length > 1 && "--verify".equals(args[1])) {
                if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
            } else {
                Files.createDirectories(file.getParent());
                Files.writeString(file, json);
            }
            System.out.println("Runtime remoting shutdown cases verified: " + cases.size());
        } finally {
            client.shutdown();
        }
    }
}
