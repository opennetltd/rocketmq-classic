package compat;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;

/** Verifies that live NettyClientConfig settings are read by an existing client. */
public final class RemotingLiveConfigProbe {
    private RemotingLiveConfigProbe() {
    }

    public static void main(String[] args) throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setClientCloseSocketIfTimeout(true);
        NettyRemotingClient client = new NettyRemotingClient(config);
        CompletableFuture<Boolean> peerStayedOpen = new CompletableFuture<>();
        Thread peer;
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            server.setSoTimeout(3000);
            peer = new Thread(() -> observePeer(server, peerStayedOpen), "live-config-peer");
            peer.start();
            client.start();
            config.setClientCloseSocketIfTimeout(false);
            try {
                client.invokeSync(
                    "127.0.0.1:" + server.getLocalPort(),
                    RemotingCommand.createRequestCommand(0, null),
                    2000);
                throw new IllegalStateException("invokeSync unexpectedly received a response");
            } catch (RemotingTimeoutException expected) {
                // The Java client must consult the retained config at timeout handling time.
            }
            if (!peerStayedOpen.get(3, TimeUnit.SECONDS)) {
                throw new IllegalStateException("post-construction false setting did not keep the socket open");
            }
        } finally {
            client.shutdown();
        }
        peer.join(3000);
        if (peer.isAlive()) {
            throw new IllegalStateException("scripted peer did not terminate after client shutdown");
        }
        verifyGoAwayReconnectSetting();
        verifyCallbackExecutorSetting();
        verifyScanAvailableNameServerSetting();
        verifyClientWorkerThreadsSetting();
        verifyConnectTimeoutSetting();
        verifyProxyConnectTimeoutSetting();
        verifyBootstrapOptionSettings();
        verifySignedBootstrapOptionSettings();
        verifyUseTlsSetting();
        verifyChannelPipelineSettings();
        System.out.println("Remoting live config mutation verified");
    }

    private static void verifyCallbackExecutorSetting() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        NettyRemotingClient client = new NettyRemotingClient(config);
        ExecutorService custom = Executors.newSingleThreadExecutor();
        try {
            client.start();
            client.setCallbackExecutor(custom);
            config.setDisableCallbackExecutor(true);
            if (client.getCallbackExecutor() != null) {
                throw new IllegalStateException("disabled callback executor remained externally visible");
            }
            client.setCallbackExecutor(custom);
            if (client.getCallbackExecutor() != null) {
                throw new IllegalStateException("setting a custom executor bypassed the live disable flag");
            }
            config.setDisableCallbackExecutor(false);
            if (client.getCallbackExecutor() != custom) {
                throw new IllegalStateException("re-enabled callback executor did not retain the custom executor");
            }
        } finally {
            client.shutdown();
            custom.shutdown();
        }
    }

    private static void verifyScanAvailableNameServerSetting() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setScanAvailableNameSrv(false);
        NettyRemotingClient client = new NettyRemotingClient(config);
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            server.setSoTimeout(3000);
            client.updateNameServerAddressList(
                java.util.Collections.singletonList("127.0.0.1:" + server.getLocalPort()));
            config.setScanAvailableNameSrv(true);
            client.start();
            try (Socket accepted = server.accept()) {
                if (!accepted.isConnected()) {
                    throw new IllegalStateException("start-time scan did not establish its probe connection");
                }
            }
        } finally {
            client.shutdown();
        }
    }

    private static void verifyClientWorkerThreadsSetting() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setClientWorkerThreads(1);
        NettyRemotingClient client = new NettyRemotingClient(config);
        try {
            config.setClientWorkerThreads(3);
            client.start();
            Field field = NettyRemotingClient.class.getDeclaredField("defaultEventExecutorGroup");
            field.setAccessible(true);
            Object executorGroup = field.get(client);
            if (!(executorGroup instanceof Iterable<?>)) {
                throw new IllegalStateException("client worker executor group was not initialized");
            }
            int count = 0;
            for (Object ignored : (Iterable<?>) executorGroup) {
                count++;
            }
            if (count != 3) {
                throw new IllegalStateException("start did not use the live worker count: " + count);
            }
        } finally {
            client.shutdown();
        }
    }

    private static void verifyConnectTimeoutSetting() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setConnectTimeoutMillis(1);
        NettyRemotingClient client = new NettyRemotingClient(config);
        try {
            config.setConnectTimeoutMillis(2345);
            client.start();
            Field field = NettyRemotingClient.class.getDeclaredField("bootstrap");
            field.setAccessible(true);
            Bootstrap bootstrap = (Bootstrap) field.get(client);
            Object configured = bootstrap.config().options().get(ChannelOption.CONNECT_TIMEOUT_MILLIS);
            if (!Integer.valueOf(2345).equals(configured)) {
                throw new IllegalStateException("start did not use the live connect timeout: " + configured);
            }
        } finally {
            client.shutdown();
        }
    }

    private static void verifyProxyConnectTimeoutSetting() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setConnectTimeoutMillis(1);
        config.setSocksProxyConfig(
            "{\"127.0.0.0/8\":{\"addr\":\"127.0.0.1:1080\"},"
                + "\"10.0.0.0/8\":{\"addr\":\"127.0.0.1:1080\"}}");
        NettyRemotingClient client = new NettyRemotingClient(config);
        try {
            client.start();
            config.setConnectTimeoutMillis(2345);

            Field directField = NettyRemotingClient.class.getDeclaredField("bootstrap");
            directField.setAccessible(true);
            Bootstrap direct = (Bootstrap) directField.get(client);
            Object directTimeout = direct.config().options().get(ChannelOption.CONNECT_TIMEOUT_MILLIS);
            if (!Integer.valueOf(1).equals(directTimeout)) {
                throw new IllegalStateException("direct bootstrap changed after start: " + directTimeout);
            }

            java.lang.reflect.Method fetchBootstrap = NettyRemotingClient.class
                .getDeclaredMethod("fetchBootstrap", String.class);
            fetchBootstrap.setAccessible(true);
            Bootstrap firstProxy = (Bootstrap) fetchBootstrap.invoke(client, "127.0.0.1:10911");
            Object firstTimeout = firstProxy.config().options().get(ChannelOption.CONNECT_TIMEOUT_MILLIS);
            if (!Integer.valueOf(2345).equals(firstTimeout)) {
                throw new IllegalStateException("first proxy bootstrap missed live timeout: " + firstTimeout);
            }

            config.setConnectTimeoutMillis(6789);
            Bootstrap reusedProxy = (Bootstrap) fetchBootstrap.invoke(client, "127.0.0.1:10911");
            if (reusedProxy != firstProxy) {
                throw new IllegalStateException("proxy bootstrap was not reused for its CIDR");
            }
            Object cachedTimeout = reusedProxy.config().options().get(ChannelOption.CONNECT_TIMEOUT_MILLIS);
            if (!Integer.valueOf(2345).equals(cachedTimeout)) {
                throw new IllegalStateException("cached proxy bootstrap changed timeout: " + cachedTimeout);
            }

            Bootstrap secondProxy = (Bootstrap) fetchBootstrap.invoke(client, "10.0.0.1:10911");
            Object secondTimeout = secondProxy.config().options().get(ChannelOption.CONNECT_TIMEOUT_MILLIS);
            if (!Integer.valueOf(6789).equals(secondTimeout)) {
                throw new IllegalStateException("new proxy bootstrap missed latest timeout: " + secondTimeout);
            }
        } finally {
            client.shutdown();
        }
    }

    private static void verifyChannelPipelineSettings() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setClientChannelMaxIdleTimeSeconds(1);
        config.setDisableNettyWorkerGroup(false);
        NettyRemotingClient client = new NettyRemotingClient(config);
        Thread firstPeer = null;
        Thread secondPeer = null;
        try {
            client.start();
            config.setClientChannelMaxIdleTimeSeconds(2);
            config.setDisableNettyWorkerGroup(true);
            try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
                String address = "127.0.0.1:" + server.getLocalPort();
                firstPeer = respondToOne(server, "live-channel-config-peer-one");
                verifySuccess(client.invokeSync(address,
                    RemotingCommand.createRequestCommand(0, null), 2000), "first channel config");
                Channel channel = channelFor(client, address);
                IdleStateHandler idle = channel.pipeline().get(IdleStateHandler.class);
                if (idle == null || idle.getAllIdleTimeInMillis() != 2000) {
                    throw new IllegalStateException("channel did not use the live idle timeout: "
                        + (idle == null ? null : idle.getAllIdleTimeInMillis()));
                }
                ChannelHandlerContext last = channel.pipeline().lastContext();
                if (last.executor() != channel.eventLoop()) {
                    throw new IllegalStateException("disabled worker group still dispatched on an executor group");
                }
            }

            config.setClientChannelMaxIdleTimeSeconds(3);
            config.setDisableNettyWorkerGroup(false);
            try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
                String address = "127.0.0.1:" + server.getLocalPort();
                secondPeer = respondToOne(server, "live-channel-config-peer-two");
                verifySuccess(client.invokeSync(address,
                    RemotingCommand.createRequestCommand(0, null), 2000), "second channel config");
                Channel channel = channelFor(client, address);
                IdleStateHandler idle = channel.pipeline().get(IdleStateHandler.class);
                if (idle == null || idle.getAllIdleTimeInMillis() != 3000) {
                    throw new IllegalStateException("new channel did not use the updated idle timeout: "
                        + (idle == null ? null : idle.getAllIdleTimeInMillis()));
                }
                ChannelHandlerContext last = channel.pipeline().lastContext();
                if (last.executor() == channel.eventLoop()) {
                    throw new IllegalStateException("enabled worker group did not dispatch on its executor group");
                }
            }
        } finally {
            client.shutdown();
            joinPeer(firstPeer, "first channel config");
            joinPeer(secondPeer, "second channel config");
        }
    }

    private static void verifyBootstrapOptionSettings() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setSocksProxyConfig(
            "{\"127.0.0.0/8\":{\"addr\":\"127.0.0.1:1080\"},"
                + "\"10.0.0.0/8\":{\"addr\":\"127.0.0.1:1080\"}}");
        NettyRemotingClient client = new NettyRemotingClient(config);
        try {
            config.setClientSocketSndBufSize(8192);
            config.setClientSocketRcvBufSize(16384);
            config.setWriteBufferLowWaterMark(4096);
            config.setWriteBufferHighWaterMark(24576);
            config.setClientPooledByteBufAllocatorEnable(true);
            client.start();

            Field directField = NettyRemotingClient.class.getDeclaredField("bootstrap");
            directField.setAccessible(true);
            Bootstrap direct = (Bootstrap) directField.get(client);
            Map<ChannelOption<?>, Object> directOptions = direct.config().options();
            assertIntOption(directOptions, ChannelOption.SO_SNDBUF, 8192, "direct send buffer");
            assertIntOption(directOptions, ChannelOption.SO_RCVBUF, 16384, "direct receive buffer");
            Object directWatermarks = directOptions.get(ChannelOption.WRITE_BUFFER_WATER_MARK);
            if (!(directWatermarks instanceof WriteBufferWaterMark)
                || ((WriteBufferWaterMark) directWatermarks).low() != 4096
                || ((WriteBufferWaterMark) directWatermarks).high() != 24576) {
                throw new IllegalStateException("start did not capture direct write watermarks: "
                    + directWatermarks);
            }
            if (directOptions.get(ChannelOption.ALLOCATOR) != PooledByteBufAllocator.DEFAULT) {
                throw new IllegalStateException("start did not capture the direct allocator setting");
            }

            config.setClientSocketSndBufSize(12288);
            config.setClientSocketRcvBufSize(20480);
            config.setWriteBufferLowWaterMark(2048);
            config.setWriteBufferHighWaterMark(12288);
            config.setClientPooledByteBufAllocatorEnable(false);
            assertIntOption(directOptions, ChannelOption.SO_SNDBUF, 8192, "direct send buffer snapshot");
            assertIntOption(directOptions, ChannelOption.SO_RCVBUF, 16384, "direct receive buffer snapshot");

            java.lang.reflect.Method fetchBootstrap = NettyRemotingClient.class
                .getDeclaredMethod("fetchBootstrap", String.class);
            fetchBootstrap.setAccessible(true);
            Bootstrap firstProxy = (Bootstrap) fetchBootstrap.invoke(client, "127.0.0.1:10911");
            Map<ChannelOption<?>, Object> firstProxyOptions = firstProxy.config().options();
            assertIntOption(firstProxyOptions, ChannelOption.SO_SNDBUF, 12288, "first proxy send buffer");
            assertIntOption(firstProxyOptions, ChannelOption.SO_RCVBUF, 20480, "first proxy receive buffer");
            if (firstProxyOptions.containsKey(ChannelOption.WRITE_BUFFER_WATER_MARK)
                || firstProxyOptions.containsKey(ChannelOption.ALLOCATOR)) {
                throw new IllegalStateException("proxy Bootstrap unexpectedly configured direct-only options");
            }

            config.setClientSocketSndBufSize(24576);
            config.setClientSocketRcvBufSize(32768);
            Bootstrap reusedProxy = (Bootstrap) fetchBootstrap.invoke(client, "127.0.0.1:10911");
            if (reusedProxy != firstProxy) {
                throw new IllegalStateException("proxy Bootstrap was not reused for its CIDR");
            }
            assertIntOption(reusedProxy.config().options(), ChannelOption.SO_SNDBUF, 12288,
                "cached proxy send buffer");
            assertIntOption(reusedProxy.config().options(), ChannelOption.SO_RCVBUF, 20480,
                "cached proxy receive buffer");

            Bootstrap secondProxy = (Bootstrap) fetchBootstrap.invoke(client, "10.0.0.1:10911");
            assertIntOption(secondProxy.config().options(), ChannelOption.SO_SNDBUF, 24576,
                "new proxy send buffer");
            assertIntOption(secondProxy.config().options(), ChannelOption.SO_RCVBUF, 32768,
                "new proxy receive buffer");
        } finally {
            client.shutdown();
        }
    }

    private static void verifySignedBootstrapOptionSettings() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setSocksProxyConfig("{\"127.0.0.0/8\":{\"addr\":\"127.0.0.1:1080\"}}");
        config.setClientSocketSndBufSize(-8192);
        config.setClientSocketRcvBufSize(0);
        config.setWriteBufferLowWaterMark(-3);
        config.setWriteBufferHighWaterMark(12);
        NettyRemotingClient client = new NettyRemotingClient(config);
        try {
            client.start();
            Field directField = NettyRemotingClient.class.getDeclaredField("bootstrap");
            directField.setAccessible(true);
            Bootstrap direct = (Bootstrap) directField.get(client);
            Map<ChannelOption<?>, Object> directOptions = direct.config().options();
            if (directOptions.containsKey(ChannelOption.SO_SNDBUF)
                || directOptions.containsKey(ChannelOption.SO_RCVBUF)
                || directOptions.containsKey(ChannelOption.WRITE_BUFFER_WATER_MARK)) {
                throw new IllegalStateException("direct Bootstrap installed nonpositive or disabled options");
            }

            java.lang.reflect.Method fetchBootstrap = NettyRemotingClient.class
                .getDeclaredMethod("fetchBootstrap", String.class);
            fetchBootstrap.setAccessible(true);
            Bootstrap proxy = (Bootstrap) fetchBootstrap.invoke(client, "127.0.0.1:10911");
            Map<ChannelOption<?>, Object> proxyOptions = proxy.config().options();
            assertIntOption(proxyOptions, ChannelOption.SO_SNDBUF, -8192, "signed proxy send buffer");
            assertIntOption(proxyOptions, ChannelOption.SO_RCVBUF, 0, "zero proxy receive buffer");
            if (proxyOptions.containsKey(ChannelOption.WRITE_BUFFER_WATER_MARK)) {
                throw new IllegalStateException("proxy Bootstrap installed direct-only write watermarks");
            }
        } finally {
            client.shutdown();
        }
    }

    private static void verifyUseTlsSetting() throws Exception {
        boolean previousTestMode = TlsSystemConfig.tlsTestModeEnable;
        TlsSystemConfig.tlsTestModeEnable = true;
        NettyRemotingClient client = null;
        Thread peer = null;
        try {
            NettyClientConfig config = new NettyClientConfig();
            config.setUseTLS(true);
            client = new NettyRemotingClient(config);
            try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
                client.start();
                config.setUseTLS(false);
                String address = "127.0.0.1:" + server.getLocalPort();
                peer = respondToOne(server, "live-config-use-tls-peer");
                verifySuccess(client.invokeSync(address,
                    RemotingCommand.createRequestCommand(0, null), 2000), "live useTLS setting");
                Channel channel = channelFor(client, address);
                if (channel.pipeline().get(SslHandler.class) != null) {
                    throw new IllegalStateException("new channel ignored the live TLS disable setting");
                }
            }
        } finally {
            if (client != null) {
                client.shutdown();
            }
            TlsSystemConfig.tlsTestModeEnable = previousTestMode;
            joinPeer(peer, "live useTLS setting");
        }
    }

    private static void assertIntOption(Map<ChannelOption<?>, Object> options, ChannelOption<Integer> option,
        int expected, String label) {
        Object actual = options.get(option);
        if (!Integer.valueOf(expected).equals(actual)) {
            throw new IllegalStateException(label + " mismatch: expected " + expected + ", got " + actual);
        }
    }

    private static Thread respondToOne(ServerSocket server, String name) {
        Thread peer = new Thread(() -> {
            try (Socket socket = server.accept()) {
                RemotingCommand request = readCommand(socket);
                RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                response.setSerializeTypeCurrentRPC(request.getSerializeTypeCurrentRPC());
                response.setOpaque(request.getOpaque());
                writeCommand(socket.getOutputStream(), response);
                socket.getInputStream().read();
            } catch (IOException error) {
                throw new RuntimeException(error);
            }
        }, name);
        peer.start();
        return peer;
    }

    private static void joinPeer(Thread peer, String label) throws InterruptedException {
        if (peer == null) {
            return;
        }
        peer.join(3000);
        if (peer.isAlive()) {
            throw new IllegalStateException(label + " peer did not terminate after client shutdown");
        }
    }

    private static Channel channelFor(NettyRemotingClient client, String address) throws Exception {
        Field field = NettyRemotingClient.class.getDeclaredField("channelTables");
        field.setAccessible(true);
        Map<?, ?> channels = (Map<?, ?>) field.get(client);
        Object wrapper = channels.get(address);
        if (wrapper == null) {
            throw new IllegalStateException("client channel table did not retain " + address);
        }
        java.lang.reflect.Method getChannel = wrapper.getClass().getDeclaredMethod("getChannel");
        getChannel.setAccessible(true);
        return (Channel) getChannel.invoke(wrapper);
    }

    private static void verifySuccess(RemotingCommand response, String label) {
        if (response == null || response.getCode() != ResponseCode.SUCCESS) {
            throw new IllegalStateException(label + " did not return a successful response");
        }
    }

    private static void verifyGoAwayReconnectSetting() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setEnableReconnectForGoAway(false);
        NettyRemotingClient client = new NettyRemotingClient(config);
        CompletableFuture<Boolean> retryObserved = new CompletableFuture<>();
        Thread peer;
        try (ServerSocket server = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
            server.setSoTimeout(5000);
            peer = new Thread(() -> observeGoAwayRetry(server, retryObserved), "live-config-go-away-peer");
            peer.start();
            client.start();
            config.setEnableReconnectForGoAway(true);
            RemotingCommand response = client.invokeSync(
                "127.0.0.1:" + server.getLocalPort(),
                RemotingCommand.createRequestCommand(0, null),
                3000);
            if (response == null || response.getCode() != ResponseCode.SUCCESS) {
                throw new IllegalStateException("post-start reconnect setting did not retry after GO_AWAY");
            }
            if (!retryObserved.get(3, TimeUnit.SECONDS)) {
                throw new IllegalStateException("replacement connection did not carry the retry");
            }
        } finally {
            client.shutdown();
        }
        peer.join(3000);
        if (peer.isAlive()) {
            throw new IllegalStateException("GO_AWAY scripted peer did not terminate after client shutdown");
        }
    }

    private static void observeGoAwayRetry(ServerSocket server, CompletableFuture<Boolean> retryObserved) {
        try (Socket first = server.accept()) {
            RemotingCommand request = readCommand(first);
            RemotingCommand goAway = RemotingCommand.createResponseCommand(ResponseCode.GO_AWAY, "retry");
            goAway.setSerializeTypeCurrentRPC(request.getSerializeTypeCurrentRPC());
            goAway.setOpaque(request.getOpaque());
            writeCommand(first.getOutputStream(), goAway);
        } catch (IOException error) {
            retryObserved.completeExceptionally(error);
            return;
        }

        try (Socket second = server.accept()) {
            RemotingCommand retry = readCommand(second);
            RemotingCommand success = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
            success.setSerializeTypeCurrentRPC(retry.getSerializeTypeCurrentRPC());
            success.setOpaque(retry.getOpaque());
            writeCommand(second.getOutputStream(), success);
            retryObserved.complete(true);
        } catch (IOException error) {
            retryObserved.completeExceptionally(error);
        }
    }

    private static RemotingCommand readCommand(Socket socket) throws IOException {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int frameLength = input.readInt();
        if (frameLength <= 0 || frameLength > 16 * 1024 * 1024) {
            throw new IOException("invalid Remoting frame length: " + frameLength);
        }
        byte[] frame = new byte[frameLength];
        input.readFully(frame);
        try {
            return RemotingCommand.decode(ByteBuffer.wrap(frame));
        } catch (Exception error) {
            throw new IOException("failed to decode scripted Remoting frame", error);
        }
    }

    private static void writeCommand(OutputStream output, RemotingCommand command) throws IOException {
        ByteBuffer encoded = command.encode();
        byte[] frame = new byte[encoded.remaining()];
        encoded.get(frame);
        output.write(frame);
        output.flush();
    }

    private static void observePeer(ServerSocket server, CompletableFuture<Boolean> peerStayedOpen) {
        try (Socket socket = server.accept()) {
            DataInputStream input = new DataInputStream(socket.getInputStream());
            int frameLength = input.readInt();
            byte[] frame = new byte[frameLength];
            input.readFully(frame);
            socket.setSoTimeout(2500);
            try {
                peerStayedOpen.complete(input.read() >= 0);
            } catch (SocketTimeoutException expected) {
                peerStayedOpen.complete(true);
            }
        } catch (IOException error) {
            peerStayedOpen.completeExceptionally(error);
        }
    }
}

