package compat;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.ReplyMessageRequestHeader;

/** Routing metadata only; payload decoding/transaction handling belongs to later phases. */
public final class RuntimeInboundRoutingFixtures {
    private RuntimeInboundRoutingFixtures() { }

    private static final class ContextCapture extends ChannelInboundHandlerAdapter {
        ChannelHandlerContext context;
        @Override public void handlerAdded(ChannelHandlerContext context) { this.context = context; }
    }

    private static Map<String, Object> consumerIdsChanged(boolean groupPresent) throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("inbound-consumer-ids-" + groupPresent);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        EmbeddedChannel channel = null;
        Map<String, Object> item = new TreeMap<>();
        try {
            java.lang.reflect.Field serviceField = MQClientInstance.class.getDeclaredField("rebalanceService");
            serviceField.setAccessible(true);
            Object rebalanceService = serviceField.get(runtime);
            java.lang.reflect.Field notifiedField = org.apache.rocketmq.common.ServiceThread.class
                .getDeclaredField("hasNotified");
            notifiedField.setAccessible(true);
            AtomicBoolean notified = (AtomicBoolean) notifiedField.get(rebalanceService);
            item.put("wakeupBefore", notified.get());

            ContextCapture capture = new ContextCapture();
            channel = new EmbeddedChannel(capture);
            RemotingCommand request = RemotingCommand.createRequestCommand(40, null);
            if (groupPresent) request.setExtFields(new HashMap<>(Collections.singletonMap("consumerGroup", "consumer-a")));
            RemotingCommand response = new ClientRemotingProcessor(runtime).processRequest(capture.context, request);
            item.put("groupPresent", groupPresent);
            item.put("responsePresent", response != null);
            item.put("wakeupAfter", notified.get());
            return item;
        } finally {
            if (channel != null) channel.finishAndReleaseAll();
            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod(
                "cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class,
                Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, original, new IllegalStateException("inbound code 40 fixture teardown"));
        }
    }

    private static Map<String, Object> replyAddressCase(
        String name, String bornHost, String storeHost) throws Exception {
        ReplyMessageRequestHeader header = new ReplyMessageRequestHeader();
        header.setProducerGroup("reply-producer");
        header.setTopic("cluster-east_REPLY_TOPIC");
        header.setDefaultTopic("TBW102");
        header.setDefaultTopicQueueNums(4);
        header.setQueueId(2);
        header.setSysFlag(0);
        header.setBornTimestamp(1_700_000_000_123L);
        header.setFlag(7);
        header.setProperties("CORRELATION_ID\u0001unmatched-reply-fixture\u0002MSG_TYPE\u0001reply\u0002");
        header.setBornHost(bornHost);
        header.setStoreHost(storeHost);
        header.setStoreTimestamp(1_700_000_001_234L);

        RemotingCommand request = RemotingCommand.createRequestCommand(
            RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, header);
        request.makeCustomHeaderToNet();
        request.setBody(new byte[0]);
        RemotingCommand response = new ClientRemotingProcessor(null).processRequest(null, request);

        Map<String, Object> item = new TreeMap<>();
        item.put("case", name);
        item.put("bornHost", bornHost);
        item.put("storeHost", storeHost);
        item.put("responseCode", response.getCode());
        item.put("outcome", response.getCode() == ResponseCode.SUCCESS
            ? "SUCCESS" : response.getCode() == ResponseCode.SYSTEM_ERROR
            ? "SYSTEM_ERROR" : "OTHER");
        return item;
    }

    public static void main(String[] args) throws Exception {
        List<Object> cases = new ArrayList<>();
        for (int version : new int[] {1,2}) for (boolean bornV6 : new boolean[] {false,true}) for (boolean storeV6 : new boolean[] {false,true}) {
            MessageExt message = new MessageExt(); message.setTopic("topic"); message.setBody(new byte[] {0,-1,42});
            message.setBornHost(new InetSocketAddress(bornV6 ? "::1" : "127.0.0.1",100));
            message.setStoreHost(new InetSocketAddress(storeV6 ? "::1" : "127.0.0.1",200));
            message.setSysFlag((bornV6 ? MessageSysFlag.BORNHOST_V6_FLAG : 0) | (storeV6 ? MessageSysFlag.STOREHOSTADDRESS_V6_FLAG : 0));
            String group = bornV6 ? "producer-b" : "producer-a";
            org.apache.rocketmq.common.message.MessageAccessor.setProperties(message,new TreeMap<>(Map.of("PGROUP",group,"UNIQ_KEY","id")));
            byte[] body = MessageDecoder.encode(message,false);
            if (version == 2) {
                // encode emits V1. Adapt only the version/topic-width fields,
                // then invoke actual Java decodeProperties as the V2 oracle.
                int topicPosition = 88 + (bornV6 ? 12 : 0) + (storeV6 ? 12 : 0) + message.getBody().length;
                ByteBuffer converted = ByteBuffer.allocate(body.length+1);
                converted.put(body,0,topicPosition).putShort((short)5).put(body,topicPosition+1,body.length-topicPosition-1);
                converted.putInt(0,body.length+1); converted.putInt(4,MessageDecoder.MESSAGE_MAGIC_CODE_V2); body = converted.array();
            }
            Map<String,Object> item = new TreeMap<>();
            item.put("case","v"+version+"-born"+(bornV6 ? 6 : 4)+"-store"+(storeV6 ? 6 : 4));
            item.put("group",group); item.put("body",Base64.getEncoder().encodeToString(body));
            item.put("properties",MessageDecoder.decodeProperties(ByteBuffer.wrap(body))); cases.add(item);
        }
        List<Object> propertyCases = new ArrayList<>();
        String[] propertyInputs = {
            "PGROUP\u0001\u0002",
            "PGROUP\u0001producer-a\u0002",
            "AB\u0001\u0002",
            "AB\u0001",
            "A\u0001x\u0002A\u0001\u0002",
            "A\u0001x\u0002A\u0001y\u0002",
            "A\u0001v\u0001tail\u0002",
            "\u0001value\u0002",
            "😀\u0001x\u0002"
        };
        for (String input : propertyInputs) {
            Map<String,Object> item = new TreeMap<>();
            item.put("input", input);
            item.put("properties", MessageDecoder.string2messageProperties(input));
            propertyCases.add(item);
        }
        RemotingCommand undispatched = new ClientRemotingProcessor(null).processRequest(
            null,
            RemotingCommand.createRequestCommand(200073, null));
        Map<String,Object> undispatchedCase = new TreeMap<>();
        undispatchedCase.put("requestCode", 200073);
        undispatchedCase.put("responsePresent", undispatched != null);
        if (undispatched != null) undispatchedCase.put("responseCode", undispatched.getCode());
        List<Object> consumerIdsChangedCases = List.of(
            consumerIdsChanged(true),
            consumerIdsChanged(false));
        List<Object> replyAddressCases = List.of(
            replyAddressCase("ipv4", "127.0.0.1:10911", "127.0.0.1:10912"),
            replyAddressCase("unbracketed-ipv6", "::1:10911", "::1:10912"),
            replyAddressCase("missing-separator", "127.0.0.1", "127.0.0.1:10912"),
            replyAddressCase("invalid-port", "127.0.0.1:10911", "127.0.0.1:port"),
            replyAddressCase("port-overflow", "127.0.0.1:65536", "127.0.0.1:10912"),
            replyAddressCase("unicode-decimal-port", "127.0.0.1:\u0661\u0660\u0669\u0661\u0661", "127.0.0.1:10912"),
            replyAddressCase("zero-port", "127.0.0.1:0", "127.0.0.1:65535"));
        Map<String,Object> fixture = new TreeMap<>(); fixture.put("client","org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit","efa1be59d6fc747ad220f036c86030f05f0419f9"); fixture.put("entrypoint","MessageDecoder.encode/decodeProperties/string2messageProperties; MessageVersion; NetworkUtil.string2SocketAddress; ClientRemotingProcessor.processRequest/receiveReplyMessage");
        fixture.put("serverBaselineContacted",false); fixture.put("cases",cases); fixture.put("propertyCases",propertyCases);
        fixture.put("undispatchedCases",List.of(undispatchedCase)); fixture.put("consumerIdsChangedCases",consumerIdsChangedCases);
        fixture.put("replyAddressCases", replyAddressCases);
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture) + "\n"; Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else { Files.createDirectories(file.getParent()); Files.writeString(file,json); }
        System.out.println("Runtime inbound routing invoked cases verified: " + (cases.size() + consumerIdsChangedCases.size() + replyAddressCases.size()));
    }
}
