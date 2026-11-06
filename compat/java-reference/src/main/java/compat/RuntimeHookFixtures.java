package compat;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.rpchook.NamespaceRpcHook;
import org.apache.rocketmq.common.AbortProcessException;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.apache.rocketmq.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.rpchook.StreamTypeRPCHook;
import org.apache.rocketmq.remoting.rpchook.DynamicalExtFieldRPCHook;

/** Invokes pinned client hooks and remoting request handling with local fixtures only. */
public final class RuntimeHookFixtures {
    public static void main(String[] args) throws Exception {
        List<Object> cases = new ArrayList<>();
        String oldZone = System.getProperty("rocketmq.zone");
        String oldMode = System.getProperty("rocketmq.zone.mode");
        try {
            for (String namespace : new String[] {null, "", "v2"}) {
                for (boolean stream : new boolean[] {false, true}) {
                    for (String zone : new String[] {"", " ", "zone-a", "\u00a0", "\u2003", "\u0085"}) {
                        System.setProperty("rocketmq.zone", zone);
                        System.setProperty("rocketmq.zone.mode", zone);
                        ClientConfig config = new ClientConfig(); config.setNamespaceV2(namespace);
                        RemotingCommand command = RemotingCommand.createRequestCommand(34, null);
                        command.addExtField("ns", "seed-ns"); command.addExtField("nsd", "false");
                        command.addExtField("ReqT", "seed-type");
                        command.addExtField("__ZONE_NAME", "seed-zone"); command.addExtField("__ZONE_MODE", "seed-mode");
                        new NamespaceRpcHook(config).doBeforeRequest("peer", command);
                        if (stream) new StreamTypeRPCHook().doBeforeRequest("peer", command);
                        Map<String, Object> item = new TreeMap<>();
                        item.put("namespaceV2", namespace); item.put("stream", stream); item.put("zone", zone);
                        item.put("beforeUser", new TreeMap<>(command.getExtFields()));
                        // Explicit user override/signing boundary between stream and dynamic.
                        command.addExtField("ns", "signed-ns"); command.addExtField("nsd", "signed-flag");
                        command.addExtField("ReqT", "signature-type");
                        command.addExtField("__ZONE_NAME", "user-zone"); command.addExtField("__ZONE_MODE", "user-mode");
                        new DynamicalExtFieldRPCHook().doBeforeRequest("peer", command);
                        item.put("afterDynamic", new TreeMap<>(command.getExtFields()));
                        cases.add(item);
                    }
                }
            }
        } finally {
            if (oldZone == null) System.clearProperty("rocketmq.zone"); else System.setProperty("rocketmq.zone", oldZone);
            if (oldMode == null) System.clearProperty("rocketmq.zone.mode"); else System.setProperty("rocketmq.zone.mode", oldMode);
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint", "NamespaceRpcHook/StreamTypeRPCHook/DynamicalExtFieldRPCHook.doBeforeRequest; MQClientAPIImpl constructor order");
        fixture.put("afterResponseMutation", afterResponseMutation());
        fixture.put("requestPipelineMutation", requestPipelineMutation());
        fixture.put("inboundFailureFlows", inboundFailureFlows());
        fixture.put("outboundWriteFlows", outboundWriteFlows());
        fixture.put("serverBaselineContacted", false); fixture.put("cases", cases);
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture, com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else { Files.createDirectories(file.getParent()); Files.writeString(file, json); }
        System.out.println("Runtime hook invoked cases verified: " + cases.size());
    }

    private static Map<String, Object> afterResponseMutation() {
        RemotingCommand request = RemotingCommand.createRequestCommand(900_008, null);
        RemotingCommand response = RemotingCommand.createResponseCommand(900_009, "wire response");
        RPCHook hook = new RPCHook() {
            @Override
            public void doBeforeRequest(String remoteAddr, RemotingCommand command) { }

            @Override
            public void doAfterResponse(String remoteAddr, RemotingCommand command, RemotingCommand result) {
                command.setCode(900_020);
                command.setRemark("mutated request by hook");
                command.addExtField("request-hook", "changed");
                result.setCode(900_010);
                result.setRemark("mutated by hook");
                result.addExtField("hook", "changed");
            }
        };
        hook.doAfterResponse("peer", request, response);

        Map<String, Object> result = new TreeMap<>();
        result.put("requestCode", request.getCode());
        result.put("requestRemark", request.getRemark());
        result.put("requestExtFields", new TreeMap<>(request.getExtFields()));
        result.put("code", response.getCode());
        result.put("remark", response.getRemark());
        result.put("extFields", new TreeMap<>(response.getExtFields()));
        return result;
    }

    private static Map<String, Object> requestPipelineMutation() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(900_014, null);
        request.setOpaque(41);
        RequestPipeline pipeline = (context, command) -> {
            command.setCode(900_015);
            command.setRemark("mutated by pipeline");
            command.setOpaque(42);
            command.addExtField("pipeline", "changed");
        };
        pipeline.execute(null, request);

        Map<String, Object> result = new TreeMap<>();
        result.put("code", request.getCode());
        result.put("remark", request.getRemark());
        result.put("inputOpaque", 41);
        result.put("opaque", request.getOpaque());
        result.put("extFields", new TreeMap<>(request.getExtFields()));
        return result;
    }

    private static final class InlineExecutor extends AbstractExecutorService {
        private boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return shutdown;
        }

        @Override
        public void execute(Runnable command) {
            if (shutdown) throw new IllegalStateException("executor is shut down");
            command.run();
        }
    }

    private static final class FixtureRemoting extends NettyRemotingAbstract {
        FixtureRemoting() {
            super(1, 1);
        }

        @Override
        public ChannelEventListener getChannelEventListener() {
            return null;
        }

        @Override
        public ExecutorService getCallbackExecutor() {
            return null;
        }

        void register(int code, NettyRequestProcessor processor) {
            processorTable.put(code, Pair.of(processor, new InlineExecutor()));
        }
    }

    private static Map<String, Object> inboundFailureFlows() {
        List<Object> rows = new ArrayList<>();
        for (String failure : new String[] {
            "before", "pipeline", "processor", "after",
            "abort_before", "abort_pipeline", "abort_after", "oneway_processor"
        }) {
            List<String> calls = new ArrayList<>();
            FixtureRemoting remoting = new FixtureRemoting();
            int code = 900_030;
            boolean oneway = failure.startsWith("oneway_");
            String point = failure.replace("abort_", "").replace("oneway_", "");
            boolean abort = failure.startsWith("abort_");
            remoting.register(code, (ctx, request) -> {
                calls.add("processor");
                if (point.equals("processor")) {
                    throw new IllegalStateException("processor failed");
                }
                return RemotingCommand.createResponseCommand(0, "ok");
            });
            remoting.registerRPCHook(new RPCHook() {
                @Override
                public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
                    calls.add("before");
                    if (point.equals("before")) {
                        if (abort) throw new AbortProcessException(831, "before aborted");
                        throw new IllegalStateException("before failed");
                    }
                }

                @Override
                public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
                    calls.add("after:" + (response == null ? "null" : response.getCode()));
                    if (point.equals("after")) {
                        if (abort) throw new AbortProcessException(832, "after aborted");
                        throw new IllegalStateException("after failed");
                    }
                }
            });
            remoting.setRequestPipeline((ctx, request) -> {
                calls.add("pipeline");
                if (point.equals("pipeline")) {
                    if (abort) throw new AbortProcessException(833, "pipeline aborted");
                    throw new IllegalStateException("pipeline failed");
                }
            });

            EmbeddedChannel channel = new EmbeddedChannel();
            channel.pipeline().addLast(new ChannelInboundHandlerAdapter());
            RemotingCommand request = RemotingCommand.createRequestCommand(code, null);
            request.setOpaque(71);
            if (oneway) request.markOnewayRPC();
            ChannelHandlerContext ctx = channel.pipeline().firstContext();
            remoting.processRequestCommand(ctx, request);
            channel.runPendingTasks();
            RemotingCommand response = channel.readOutbound();

            Map<String, Object> row = new TreeMap<>();
            row.put("case", failure);
            row.put("calls", calls);
            row.put("responseCode", response == null ? null : response.getCode());
            row.put("responseOpaque", response == null ? null : response.getOpaque());
            String remark = response == null ? null : response.getRemark();
            row.put("responseRemark", response == null ? null
                : response.getCode() == RemotingSysResponseCode.SYSTEM_ERROR && remark != null
                    ? remark.substring(0, remark.indexOf(':')) : remark);
            row.put("responsePresent", response != null);
            rows.add(row);
            channel.finishAndReleaseAll();
        }
        Map<String, Object> result = new TreeMap<>();
        result.put("entrypoint", "NettyRemotingAbstract.processRequestCommand with EmbeddedChannel and direct ExecutorService");
        result.put("cases", rows);
        return result;
    }

    private static Map<String, Object> outboundWriteFlows() throws Exception {
        FixtureRemoting syncRemoting = new FixtureRemoting();
        AtomicReference<ChannelPromise> syncPromise = new AtomicReference<>();
        EmbeddedChannel syncChannel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
                RemotingCommand request = (RemotingCommand) message;
                RemotingCommand response = RemotingCommand.createResponseCommand(
                    RemotingSysResponseCode.SUCCESS, "queued write response");
                response.setOpaque(request.getOpaque());
                syncRemoting.processResponseCommand(context, response);
                syncPromise.set(promise);
            }
        });
        RemotingCommand request = RemotingCommand.createRequestCommand(900_040, null);
        RemotingCommand response = syncRemoting.invokeSyncImpl(syncChannel, request, 1_000);
        boolean syncReturnedBeforeWrite = response != null
            && response.getOpaque() == request.getOpaque()
            && syncPromise.get() != null
            && !syncPromise.get().isDone();
        syncPromise.get().setSuccess();
        syncChannel.runPendingTasks();
        syncChannel.finishAndReleaseAll();

        FixtureRemoting onewayRemoting = new FixtureRemoting();
        AtomicReference<ChannelPromise> onewayPromise = new AtomicReference<>();
        EmbeddedChannel onewayChannel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
                onewayPromise.set(promise);
            }
        });
        java.lang.reflect.Field permitField = NettyRemotingAbstract.class
            .getDeclaredField("semaphoreOneway");
        permitField.setAccessible(true);
        Semaphore onewaySemaphore = (Semaphore) permitField.get(onewayRemoting);
        RemotingCommand oneway = RemotingCommand.createRequestCommand(900_041, null);
        onewayRemoting.invokeOnewayImpl(onewayChannel, oneway, 1_000);
        int permitsWhileWritePending = onewaySemaphore.availablePermits();
        boolean onewayReturnedBeforeWrite = onewayPromise.get() != null
            && !onewayPromise.get().isDone();
        onewayPromise.get().setSuccess();
        onewayChannel.runPendingTasks();
        int permitsAfterWrite = onewaySemaphore.availablePermits();
        onewayChannel.finishAndReleaseAll();

        Map<String, Object> result = new TreeMap<>();
        result.put("syncReturnedBeforeWriteCompletion", syncReturnedBeforeWrite);
        result.put("onewayReturnedBeforeWriteCompletion", onewayReturnedBeforeWrite);
        result.put("onewayPermitsWhileWritePending", permitsWhileWritePending);
        result.put("onewayPermitsAfterWriteCompletion", permitsAfterWrite);
        result.put("entrypoint", "NettyRemotingAbstract.invokeSyncImpl/invokeOnewayImpl with EmbeddedChannel delayed write promises");
        return result;
    }
}
