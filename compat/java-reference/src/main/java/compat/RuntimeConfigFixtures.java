package compat;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.lang.ref.WeakReference;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.utils.NameServerAddressUtils;
import org.apache.rocketmq.client.ClientConfig;

/** Executes pinned ClientConfig methods and local SOCKS transport paths without a Broker. */
public final class RuntimeConfigFixtures {
    private RuntimeConfigFixtures() { }

    public static void main(String[] args) throws Exception {
        List<Object> identities = new ArrayList<>();
        String[] units = {null, "", " \t\n", "unit-a", " unit-a ", "\u2003", "\u00a0", "\u0085", "\u202f"};
        for (String ip : new String[] {"192.0.2.4", "[2001:db8::4]", "", null}) {
            for (String unit : units) {
                for (boolean stream : new boolean[] {false, true}) {
                    ClientConfig config = new ClientConfig();
                    config.setClientIP(ip);
                    config.setInstanceName("instance");
                    config.setUnitName(unit);
                    config.setNamespace("ignored-namespace");
                    config.setEnableStreamRequestType(stream);
                    Map<String, Object> item = new TreeMap<>();
                    item.put("ip", ip);
                    item.put("instance", "instance");
                    item.put("unit", unit);
                    item.put("stream", stream);
                    item.put("id", config.buildMQClientId());
                    identities.add(item);
                }
            }
        }
        ClientConfig config = new ClientConfig();
        config.setClientIP("192.0.2.4");
        config.setInstanceName(null);
        Map<String, Object> nullInstance = new TreeMap<>();
        nullInstance.put("ip", "192.0.2.4");
        nullInstance.put("instance", null);
        nullInstance.put("unit", null);
        nullInstance.put("stream", false);
        nullInstance.put("id", config.buildMQClientId());
        identities.add(nullInstance);

        Map<String, Object> defaults = new TreeMap<>();
        config = new ClientConfig();
        defaults.put("pollNameServerInterval", config.getPollNameServerInterval());
        defaults.put("heartbeatBrokerInterval", config.getHeartbeatBrokerInterval());
        defaults.put("persistConsumerOffsetInterval", config.getPersistConsumerOffsetInterval());
        defaults.put("pullTimeDelayMillsWhenException", config.getPullTimeDelayMillsWhenException());
        defaults.put("mqClientApiTimeout", config.getMqClientApiTimeout());
        defaults.put("detectTimeout", config.getDetectTimeout());
        defaults.put("detectInterval", config.getDetectInterval());
        defaults.put("traceMsgBatchNum", config.getTraceMsgBatchNum());
        defaults.put("maxPageSizeInGetMetadata", config.getMaxPageSizeInGetMetadata());
        defaults.put("accessChannel", config.getAccessChannel().toString());
        defaults.put("enableConcurrentHeartbeat", config.isEnableConcurrentHeartbeat());
        defaults.put("enableHeartbeatChannelEventListener", config.isEnableHeartbeatChannelEventListener());

        config.setTraceMsgBatchNum(99);
        config.setMaxPageSizeInGetMetadata(77);
        config.setAccessChannel(org.apache.rocketmq.client.AccessChannel.CLOUD);
        config.setNamespaceV2("v2");
        config.setEnableConcurrentHeartbeat(true);
        config.setConcurrentHeartbeatThreadPoolSize(7);
        ClientConfig copy = config.cloneClientConfig();
        Map<String, Object> clone = new TreeMap<>();
        clone.put("traceMsgBatchNum", copy.getTraceMsgBatchNum());
        clone.put("maxPageSizeInGetMetadata", copy.getMaxPageSizeInGetMetadata());
        clone.put("accessChannel", copy.getAccessChannel().toString());
        clone.put("namespaceV2", copy.getNamespaceV2());
        clone.put("enableConcurrentHeartbeat", copy.isEnableConcurrentHeartbeat());
        clone.put("concurrentHeartbeatThreadPoolSize", copy.getConcurrentHeartbeatThreadPoolSize());
        config.setClientIP("manager-copy");
        config.setNamesrvAddr("MQ_INST_a_b.example:9876");
        config.setNamespace(null); // Caller initialized; manager clone must reset its flag.
        org.apache.rocketmq.client.impl.MQClientManager manager = org.apache.rocketmq.client.impl.MQClientManager.getInstance();
        org.apache.rocketmq.client.impl.factory.MQClientInstance runtime = manager.getOrCreateMQClientInstance(config);
        Map<String, Object> managed = new TreeMap<>();
        try {
            ClientConfig actual = runtime.getClientConfig();
            managed.put("traceMsgBatchNum", actual.getTraceMsgBatchNum());
            managed.put("maxPageSizeInGetMetadata", actual.getMaxPageSizeInGetMetadata());
            managed.put("accessChannel", actual.getAccessChannel().toString());
            managed.put("namespaceV2", actual.getNamespaceV2());
            managed.put("namespace", actual.getNamespace());
            managed.put("inputNamespaceStillNull", config.getNamespace() == null);
            managed.put("enableConcurrentHeartbeat", actual.isEnableConcurrentHeartbeat());
            managed.put("concurrentHeartbeatThreadPoolSize", actual.getConcurrentHeartbeatThreadPoolSize());
            managed.put("sharedIdentity", runtime == manager.getOrCreateMQClientInstance(config));
            managed.put("configGetterSameObject", actual == runtime.getClientConfig());
            actual.setMqClientApiTimeout(4321);
            managed.put("configMutationVisible", runtime.getClientConfig().getMqClientApiTimeout() == 4321);
        } finally {
            try {
                java.lang.reflect.Method cleanup = runtime.getClass().getDeclaredMethod("cleanupAfterConstructionFailure",
                    org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
                cleanup.setAccessible(true);
                cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("manager fixture teardown"));
            } finally {
                manager.removeClientFactory(runtime.getClientId(), runtime);
            }
        }
        List<Object> endpoints = new ArrayList<>();
        for (String endpoint : new String[] {null, "", "broker:9876", "http://broker:9876", " http://broker:9876 ",
            "https://MQ_INST_a_b.example:80", "MQ_INST_a_b.example:80", "http://MQ_INST_a_b.example:80",
            "http://MQ_INST_a_b.example/path", "http://MQ_INST_a_b.example\n", "http://MQ_INST_a_b.example\u2028", "MQ_INST_é_b.example"}) {
            ClientConfig c = new ClientConfig();
            c.setNamesrvAddr(endpoint);
            Map<String, Object> item = new TreeMap<>();
            item.put("endpoint", endpoint); item.put("address", c.getNamesrvAddr());
            try { item.put("namespace", c.getNamespace()); item.put("outcome", "OK"); }
            catch (RuntimeException error) { item.put("outcome", error.getClass().getSimpleName()); }
            endpoints.add(item);
        }
        List<Object> resources = new ArrayList<>();
        for (String namespace : new String[] {null, "", "ns"}) {
            for (String resource : new String[] {null, "", "topic", "ns%topic", "other%topic", "%topic", "%RETRY%group", "%DLQ%group",
                "%RETRY%ns%group", "%DLQ%other%group", "TBW102", "SCHEDULE_TOPIC_XXXX", "BenchmarkTest", "rmq_sys_custom", "CID_RMQ_SYS_custom", "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC", " ", "ns%"}) {
                ClientConfig c = new ClientConfig(); c.setNamespace(namespace);
                Map<String, Object> item = new TreeMap<>(); item.put("namespace", namespace); item.put("resource", resource);
                item.put("with", c.withNamespace(resource)); item.put("without", c.withoutNamespace(resource));
                resources.add(item);
            }
        }
        Map<String, Object> pid = new TreeMap<>();
        for (String instance : new String[] {"DEFAULT", "custom", "default", ""}) {
            ClientConfig c = new ClientConfig(); c.setInstanceName(instance); c.changeInstanceNameToPID();
            String changed = c.getInstanceName(); c.changeInstanceNameToPID();
            pid.put(instance, instance.equals("DEFAULT") ? changed.matches(org.apache.rocketmq.common.UtilAll.getPid() + "#-?\\d+") && changed.equals(c.getInstanceName()) : instance.equals(changed));
        }
        ClientConfig nullPidConfig = new ClientConfig();
        nullPidConfig.setInstanceName(null);
        String nullInstancePidOutcome = "OK";
        try {
            nullPidConfig.changeInstanceNameToPID();
        } catch (Throwable error) {
            nullInstancePidOutcome = error.getClass().getSimpleName();
        }
        Map<String, Object> fixture = new TreeMap<>();
        List<Object> rebalanceLongs = new ArrayList<>();
        for (String input : new String[] {"٢٠٠٠٠", "+１２３４", "-١", "1٢3", "०", "9223372036854775807",
            "-9223372036854775808", "９２２３３７２０３６８５４７７５８０７", "-９２２３３７２０３６８５４７７５８０８",
            "9223372036854775808", "-9223372036854775809", "９２２３３７２０３６８５４７７５８０８",
            "", "+", "-", " 1", "1 ", "²", "𝟚", "NaN"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("input", input);
            try { item.put("value", Long.parseLong(input)); item.put("outcome", "OK"); }
            catch (NumberFormatException error) { item.put("outcome", "NumberFormatException"); }
            rebalanceLongs.add(item);
        }
        fixture.put("rebalanceLongs", rebalanceLongs);
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint", "ClientConfig.buildMQClientId/toString/cloneClientConfig/resetClientConfig/getNamesrvAddr/getNamespace/withNamespace/withoutNamespace/queuesWithNamespace/changeInstanceNameToPID; NettyClientConfig clientAsync/clientOnewaySemaphoreValue getter/setter; java.util.concurrent.Semaphore negative-permit admission; MessageQueue.setTopic/setBrokerName/setQueueId; MQClientManager.getOrCreateMQClientInstance/removeClientFactory/getFactoryTable; MQClientInstance.constructor/getClientConfig/getNettyClientConfig/getProducerTable/getConsumerTable/selectProducer/selectConsumer/registerProducer/registerConsumer/unregisterConsumer; MQClientAPIImpl.getRemotingClient/getNameServerAddressList/NettyRemotingClient.publicExecutor (reflective capacity observation); NettyRemotingClient.start/invokeSync (local authenticated and no-auth SOCKS5 round trips); RemotingCommand.encode/getType (null language JSON/ROCKETMQ); LanguageCode/SerializeType/TlsMode/RequestType/RemotingCommandType/NettyEventType generated enum and custom value lookups; Long.parseLong (RebalanceService property parser)");
        fixture.put("serverBaselineContacted", false);
        fixture.put("clientConfigAutoIPUsesNetworkUtil", java.util.Objects.equals(
            new ClientConfig().getClientIP(),
            org.apache.rocketmq.common.utils.NetworkUtil.getLocalAddress()));
        fixture.put("identities", identities);
        fixture.put("defaults", defaults);
        fixture.put("remotingSemaphoreConfig", remotingSemaphoreConfig());
        fixture.put("nameServerAddressCases", nameServerAddressCases());
        fixture.put("nameServerAddressUtils", nameServerAddressUtils());
        fixture.put("clone", clone);
        fixture.put("copyFields", clientConfigCopies());
        fixture.put("clientConfigToString", clientConfigToString());
        fixture.put("messageQueueSetters", messageQueueSetters());
        fixture.put("queuesWithNamespace", queuesWithNamespace());
        fixture.put("managed", managed);
        fixture.put("managerRemoval", managerRemoval(config));
        fixture.put("managerSharing", managerSharing());
        fixture.put("nameServerAddressSnapshotCases", nameServerAddressSnapshotCases());
        fixture.put("managerTable", managerTable(config));
        fixture.put("nullableSettings", nullableSettings());
        fixture.put("accessChannelMethodCases", accessChannelMethodCases());
        fixture.put("serviceNameCases", serviceNameCases());
        fixture.put("serviceThreadCases", serviceThreadCases());
        fixture.put("serviceLifecycleCases", serviceLifecycleCases());
        fixture.put("serviceThreadMakeStopWakeup", serviceThreadMakeStopWakeupCase());
        fixture.put("concurrentHeartbeatConstructionCases", concurrentHeartbeatConstructionCases());
        fixture.put("runtimeStartScheduleCases", runtimeStartScheduleCases());
        fixture.put("runtimeRestartAfterShutdown", runtimeRestartAfterShutdown());
        fixture.put("languageCodeMethodCases", languageCodeMethodCases());
        fixture.put("serializeTypeMethodCases", serializeTypeMethodCases());
        fixture.put("compressionTypeMethodCases", compressionTypeMethodCases());
        fixture.put("compressionTypeTurkishLocaleCases", compressionTypeTurkishLocaleCases());
        fixture.put("clientStatusEnumMethodCases", clientStatusEnumMethodCases());
        fixture.put("protocolEnumMethodCases", protocolEnumMethodCases());
        fixture.put("tlsModeMethodCases", tlsModeMethodCases());
        fixture.put("remotingEnumMethodCases", remotingEnumMethodCases());
        fixture.put("nettyEventTypeMethodCases", enumMethodCases(
            org.apache.rocketmq.remoting.netty.NettyEventType::values,
            org.apache.rocketmq.remoting.netty.NettyEventType::valueOf,
            new String[] {null, "", "CONNECT", "connect", "IDLE", "ACTIVE", "UNKNOWN"}));
        fixture.put("socksLeadingCodeUnits", socksLeadingCodeUnits());
        fixture.put("socksControlBoundaryCases", socksControlBoundaryCases());
        fixture.put("socksUnicodeTriviaCases", socksUnicodeTriviaCases());
        fixture.put("socksBmpBoundaryObservations", socksBmpBoundaryObservations());
        fixture.put("socksEscapeFollowerObservations", socksEscapeFollowerObservations());
        fixture.put("socksEscapeFollowerProbedCodeUnitCount", Character.MAX_VALUE + 1);
        fixture.put("socksSupplementaryEscapeFollowerCases", socksSupplementaryEscapeFollowerCases());
        fixture.put("socksSubstituteEofObservations", socksSubstituteEofObservations());
        fixture.put("socksFieldShapeCases", socksProxyFieldShapeCases());
        fixture.put("socksConstruction", socksConstruction());
        fixture.put("socksConnection", socksConnection(true));
        fixture.put("socksConnectionNoAuth", socksConnection(false));
        fixture.put("socksCidr", socksCidr());
        fixture.put("socksProxySelection", socksProxySelection());
        fixture.put("callbackThreadsConstruction", callbackThreadsConstruction());
        fixture.put("callbackThreadsMutation", callbackThreadsMutation());
        fixture.put("registryTables", registryTables());
        fixture.put("endpoints", endpoints);
        fixture.put("resources", resources);
        fixture.put("pid", pid);
        fixture.put("instanceNameNullPidOutcome", nullInstancePidOutcome);
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else {
            Files.createDirectories(file.getParent());
            Files.writeString(file, json);
        }
        System.out.println("ClientConfig identities/defaults/clone verified: " + identities.size());
    }

    private static Map<String, Object> remotingSemaphoreConfig() throws InterruptedException {
        org.apache.rocketmq.remoting.netty.NettyClientConfig defaults =
            new org.apache.rocketmq.remoting.netty.NettyClientConfig();
        Map<String, Object> result = new TreeMap<>();
        Map<String, Object> defaultValues = new TreeMap<>();
        defaultValues.put("async", defaults.getClientAsyncSemaphoreValue());
        defaultValues.put("oneway", defaults.getClientOnewaySemaphoreValue());
        result.put("defaults", defaultValues);

        List<Object> cases = new ArrayList<>();
        for (int value : new int[] {Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -2, -1, 0, 1, Integer.MAX_VALUE}) {
            org.apache.rocketmq.remoting.netty.NettyClientConfig config =
                new org.apache.rocketmq.remoting.netty.NettyClientConfig();
            config.setClientAsyncSemaphoreValue(value);
            config.setClientOnewaySemaphoreValue(value);
            Semaphore async = new Semaphore(config.getClientAsyncSemaphoreValue(), true);
            Semaphore oneway = new Semaphore(config.getClientOnewaySemaphoreValue(), true);

            Map<String, Object> item = new TreeMap<>();
            item.put("input", value);
            item.put("async", config.getClientAsyncSemaphoreValue());
            item.put("oneway", config.getClientOnewaySemaphoreValue());
            item.put("asyncAvailablePermits", async.availablePermits());
            item.put("onewayAvailablePermits", oneway.availablePermits());
            boolean asyncAcquired = async.tryAcquire(0, TimeUnit.MILLISECONDS);
            boolean onewayAcquired = oneway.tryAcquire(0, TimeUnit.MILLISECONDS);
            item.put("asyncAcquireImmediate", asyncAcquired);
            item.put("onewayAcquireImmediate", onewayAcquired);
            item.put("asyncAvailableAfterAcquire", async.availablePermits());
            item.put("onewayAvailableAfterAcquire", oneway.availablePermits());
            cases.add(item);
        }
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> nameServerAddressCases() {
        Map<String, Object> cases = new TreeMap<>();
        cases.put("systemPropertyKey", MixAll.NAMESRV_ADDR_PROPERTY);
        cases.put("environmentKey", MixAll.NAMESRV_ADDR_ENV);
        String previous = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY);
        try {
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "system:9876");
            cases.put("directGetterSystemPropertyValue", NameServerAddressUtils.getNameServerAddresses());
            cases.put("systemPropertyValue", new ClientConfig().getNamesrvAddr());
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "");
            cases.put("directGetterExplicitBlankValue", NameServerAddressUtils.getNameServerAddresses());
            cases.put("explicitBlankPropertyValue", new ClientConfig().getNamesrvAddr());
        } finally {
            if (previous == null) {
                System.clearProperty(MixAll.NAMESRV_ADDR_PROPERTY);
            } else {
                System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, previous);
            }
        }
        return cases;
    }

    private static Map<String, Object> nameServerAddressUtils() {
        Map<String, Object> result = new TreeMap<>();
        result.put("publicFields", Arrays.stream(NameServerAddressUtils.class.getDeclaredFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(field -> field.getType().getTypeName() + " " + field.getName())
            .sorted().collect(Collectors.toList()));
        result.put("publicMethods", Arrays.stream(NameServerAddressUtils.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(RuntimeConfigFixtures::methodSignature)
            .sorted().collect(Collectors.toList()));
        Map<String, Object> patterns = new TreeMap<>();
        patterns.put("INSTANCE_PREFIX", NameServerAddressUtils.INSTANCE_PREFIX);
        patterns.put("INSTANCE_REGEX", NameServerAddressUtils.INSTANCE_REGEX);
        patterns.put("ENDPOINT_PREFIX", NameServerAddressUtils.ENDPOINT_PREFIX);
        patterns.put("NAMESRV_ENDPOINT_PATTERN", NameServerAddressUtils.NAMESRV_ENDPOINT_PATTERN.pattern());
        patterns.put("INST_ENDPOINT_PATTERN", NameServerAddressUtils.INST_ENDPOINT_PATTERN.pattern());
        result.put("publicValues", patterns);

        List<Object> nameserverPatterns = new ArrayList<>();
        for (String endpoint : new String[] {null, "http://broker:9876", "http://", "http://broker:9876\n",
            "http://broker:9876\r", "http://broker:9876\u0085", " http://broker:9876",
            "https://broker:9876", "http://broker:9876/path"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("input", endpoint);
            try {
                item.put("matches", NameServerAddressUtils.NAMESRV_ENDPOINT_PATTERN.matcher(endpoint).matches());
                item.put("outcome", "OK");
            } catch (Throwable error) {
                item.put("outcome", error.getClass().getSimpleName());
            }
            nameserverPatterns.add(item);
        }
        result.put("namesrvPatternCases", nameserverPatterns);

        List<Object> instanceEndpoints = new ArrayList<>();
        for (String endpoint : new String[] {null, "MQ_INST_a_b.example", "https://MQ_INST_a_b.example",
            "http://MQ_INST_a_b.example", "http-MQ_INST_a_b.example", "MQ_INST__b.example",
            "MQ_INST__a_b.example", "MQ_INST_a__b.example", "MQ_INST_a_.example", "MQ_INST_a_b",
            "MQ_INST_a_b.example://path", "MQ_INST_é_b.example", "MQ_INST_a_b.example\u2028"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("input", endpoint);
            try {
                item.put("valid", NameServerAddressUtils.validateInstanceEndpoint(endpoint));
                item.put("outcome", "OK");
            } catch (Throwable error) {
                item.put("outcome", error.getClass().getSimpleName());
            }
            instanceEndpoints.add(item);
        }
        result.put("instanceEndpointCases", instanceEndpoints);

        List<Object> instanceIds = new ArrayList<>();
        for (String endpoint : new String[] {null, "", "MQ_INST_a_b.example", "https://MQ_INST_a_b.example:80",
            "prefix/path/MQ_INST_a_b.example/path", ".empty", "nodot", "path/.first"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("input", endpoint);
            try {
                item.put("value", NameServerAddressUtils.parseInstanceIdFromEndpoint(endpoint));
                item.put("outcome", "OK");
            } catch (Throwable error) {
                item.put("outcome", error.getClass().getSimpleName());
            }
            instanceIds.add(item);
        }
        result.put("instanceIdCases", instanceIds);

        List<Object> nameserverAddresses = new ArrayList<>();
        for (String endpoint : new String[] {null, "", "http://broker:9876", "path/to/addr", "trailing/", "plain"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("input", endpoint);
            item.put("value", NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(endpoint));
            nameserverAddresses.add(item);
        }
        result.put("namesrvAddressCases", nameserverAddresses);
        return result;
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getTypeName() + " " + method.getName() + "(" +
            Arrays.stream(method.getParameterTypes()).map(Class::getTypeName).collect(Collectors.joining(",")) + ")";
    }

    private static Map<String, Object> managerRemoval(ClientConfig config) throws Exception {
        org.apache.rocketmq.client.impl.MQClientManager manager = org.apache.rocketmq.client.impl.MQClientManager.getInstance();
        java.util.concurrent.ConcurrentMap<String, org.apache.rocketmq.client.impl.factory.MQClientInstance> live = manager.getFactoryTable();
        org.apache.rocketmq.client.impl.factory.MQClientInstance first = manager.getOrCreateMQClientInstance(config);
        org.apache.rocketmq.client.impl.factory.MQClientInstance replacement = null;
        Map<String, Object> result = new TreeMap<>();
        String id = first.getClientId();
        try {
            try {
                manager.removeClientFactory((String) null);
                result.put("removeByNullId", "RETURNED");
            } catch (Throwable error) {
                result.put("removeByNullId", error.getClass().getSimpleName());
            }
            try {
                manager.removeClientFactory((String) null, first);
                result.put("removeByNullIdAndInstance", "RETURNED");
            } catch (Throwable error) {
                result.put("removeByNullIdAndInstance", error.getClass().getSimpleName());
            }
            try {
                manager.removeClientFactory(id, (org.apache.rocketmq.client.impl.factory.MQClientInstance) null);
                result.put("removeByNullInstance", "RETURNED");
            } catch (Throwable error) {
                result.put("removeByNullInstance", error.getClass().getSimpleName());
            }
            try {
                manager.removeClientFactory((String) null,
                    (org.apache.rocketmq.client.impl.factory.MQClientInstance) null);
                result.put("removeByNullIdAndNullInstance", "RETURNED");
            } catch (Throwable error) {
                result.put("removeByNullIdAndNullInstance", error.getClass().getSimpleName());
            }
            result.put("liveSeesFirst", live.get(id) == first);
            manager.removeClientFactory(id);
            result.put("absentAfterUnconditional", !live.containsKey(id));
            replacement = manager.getOrCreateMQClientInstance(config);
            result.put("replacementIdentity", replacement != first);
            result.put("liveSeesReplacement", live.get(id) == replacement);
            manager.removeClientFactory(id, first);
            result.put("staleIdentityKeepsReplacement", live.get(id) == replacement);
            manager.removeClientFactory(id);
            result.put("unconditionalRemovesReplacement", !live.containsKey(id));
            manager.removeClientFactory(id); // Absent key is a no-op.
            result.put("absentRemovalStaysAbsent", !live.containsKey(id));
        } finally {
            try {
                cleanupRuntime(first);
            } finally {
                try {
                    if (replacement != null) cleanupRuntime(replacement);
                } finally {
                    manager.removeClientFactory(id, first);
                    if (replacement != null) manager.removeClientFactory(id, replacement);
                }
            }
        }
        return result;
    }

    private static Map<String, Object> managerSharing() throws Exception {
        org.apache.rocketmq.client.impl.MQClientManager manager = org.apache.rocketmq.client.impl.MQClientManager.getInstance();
        ClientConfig firstConfig = new ClientConfig();
        firstConfig.setClientIP("198.51.100.223");
        firstConfig.setInstanceName("phase6-manager-sharing");
        firstConfig.setNamesrvAddr("");
        org.apache.rocketmq.remoting.RPCHook firstHook = new org.apache.rocketmq.remoting.RPCHook() {
            @Override
            public void doBeforeRequest(String remoteAddr, org.apache.rocketmq.remoting.protocol.RemotingCommand request) { }
            @Override
            public void doAfterResponse(String remoteAddr, org.apache.rocketmq.remoting.protocol.RemotingCommand request,
                org.apache.rocketmq.remoting.protocol.RemotingCommand response) { }
        };
        org.apache.rocketmq.remoting.RPCHook laterHook = new org.apache.rocketmq.remoting.RPCHook() {
            @Override
            public void doBeforeRequest(String remoteAddr, org.apache.rocketmq.remoting.protocol.RemotingCommand request) { }
            @Override
            public void doAfterResponse(String remoteAddr, org.apache.rocketmq.remoting.protocol.RemotingCommand request,
                org.apache.rocketmq.remoting.protocol.RemotingCommand response) { }
        };
        org.apache.rocketmq.client.impl.factory.MQClientInstance first =
            manager.getOrCreateMQClientInstance(firstConfig, firstHook);
        String runtimeClientId = first.getClientId();
        ClientConfig laterConfig = firstConfig.cloneClientConfig();
        laterConfig.setNamesrvAddr("different-nameserver:9876");
        laterConfig.setNamespaceV2("different-namespace");
        laterConfig.setUseTLS(true);
        laterConfig.setMqClientApiTimeout(1234);
        org.apache.rocketmq.client.impl.factory.MQClientInstance later = null;
        Map<String, Object> result = new TreeMap<>();
        try {
            later = manager.getOrCreateMQClientInstance(laterConfig, laterHook);
            ClientConfig retained = first.getClientConfig();
            @SuppressWarnings("unchecked")
            List<org.apache.rocketmq.remoting.RPCHook> registeredHooks = (List<org.apache.rocketmq.remoting.RPCHook>)
                first.getMQClientAPIImpl().getRemotingClient().getClass().getMethod("getRPCHook")
                    .invoke(first.getMQClientAPIImpl().getRemotingClient());
            result.put("sameClientId", first.getClientId().equals(later.getClientId()));
            result.put("sameRuntimeIdentity", first == later);
            result.put("firstHookRetained", registeredHooks.contains(firstHook));
            result.put("laterHookIgnored", !registeredHooks.contains(laterHook));
            result.put("retainedNameserver", retained.getNamesrvAddr());
            result.put("retainedNamespaceV2", retained.getNamespaceV2());
            result.put("retainedTls", retained.isUseTLS());
            result.put("retainedTimeout", retained.getMqClientApiTimeout());
            result.put("clientIdBeforeConfigMutation", runtimeClientId);
            retained.setClientIP("203.0.113.44");
            retained.setInstanceName("mutated-runtime-instance");
            retained.setUnitName("mutated-runtime-unit");
            result.put("clientIdAfterConfigMutation", first.getClientId());
            result.put("configDerivedClientIdAfterMutation", retained.buildMQClientId());
            result.put("laterConfigUnchanged", "different-nameserver:9876".equals(laterConfig.getNamesrvAddr())
                && "different-namespace".equals(laterConfig.getNamespaceV2()) && laterConfig.isUseTLS()
                && laterConfig.getMqClientApiTimeout() == 1234);
        } finally {
            try {
                cleanupRuntime(first);
            } finally {
                manager.removeClientFactory(runtimeClientId, first);
            }
        }
        return result;
    }

    private static List<Object> nameServerAddressSnapshotCases() throws Exception {
        List<Object> cases = new ArrayList<>();
        int index = 0;
        for (String initial : new String[] {null, "initial-nameserver:9876"}) {
            ClientConfig config = new ClientConfig();
            config.setClientIP("192.0.2.80");
            config.setInstanceName("phase6-nameserver-snapshot-" + index++);
            config.setNamesrvAddr(initial);
            org.apache.rocketmq.client.impl.factory.MQClientInstance runtime =
                new org.apache.rocketmq.client.impl.factory.MQClientInstance(config, 0, config.buildMQClientId());
            try {
                Map<String, Object> item = new TreeMap<>();
                item.put("configuredAtConstruction", initial);
                List<String> before = runtime.getMQClientAPIImpl().getNameServerAddressList();
                item.put("activeAddressesBeforeMutation", before == null ? null : new ArrayList<>(before));
                String changed = initial == null ? "added-after-construction:9876" : "changed-after-construction:9876";
                config.setNamesrvAddr(changed);
                item.put("configAfterMutation", config.getNamesrvAddr());
                List<String> after = runtime.getMQClientAPIImpl().getNameServerAddressList();
                item.put("activeAddressesAfterMutation", after == null ? null : new ArrayList<>(after));
                cases.add(item);
            } finally {
                cleanupRuntime(runtime);
            }
        }
        return cases;
    }

    private static Map<String, Object> clientConfigCopies() throws Exception {
        ClientConfig source = new ClientConfig();
        source.setNamesrvAddr("changed-nameserver");
        source.setClientIP(null);
        source.setInstanceName(null);
        source.setClientCallbackExecutorThreads(Integer.MIN_VALUE);
        source.setNamespace("changed-namespace");
        source.setNamespaceV2("changed-v2");
        source.setAccessChannel(org.apache.rocketmq.client.AccessChannel.CLOUD);
        source.setPollNameServerInterval(Integer.MIN_VALUE);
        source.setHeartbeatBrokerInterval(Integer.MAX_VALUE);
        source.setPersistConsumerOffsetInterval(-1);
        source.setPullTimeDelayMillsWhenException(Long.MIN_VALUE);
        source.setTraceMsgBatchNum(-10);
        source.setUnitMode(true);
        source.setUnitName("changed-unit");
        source.setDecodeReadBody(false);
        source.setDecodeDecompressBody(false);
        source.setVipChannelEnabled(true);
        source.setUseHeartbeatV2(true);
        source.setUseTLS(true);
        source.setSocksProxyConfig("changed-proxy");
        source.setMqClientApiTimeout(Integer.MIN_VALUE);
        source.setDetectTimeout(-200);
        source.setDetectInterval(Integer.MAX_VALUE);
        source.setLanguage(org.apache.rocketmq.remoting.protocol.LanguageCode.CPP);
        source.setEnableStreamRequestType(true);
        source.setSendLatencyEnable(true);
        source.setStartDetectorEnable(true);
        source.setEnableHeartbeatChannelEventListener(false);
        source.setEnableConcurrentHeartbeat(true);
        source.setConcurrentHeartbeatThreadPoolSize(-7);
        source.setEnableTrace(true);
        source.setTraceTopic("changed-trace");
        source.setMaxPageSizeInGetMetadata(-2000);

        ClientConfig copy = source.cloneClientConfig();
        ClientConfig reset = new ClientConfig();
        reset.setNamespace(null);
        reset.setAccessChannel(org.apache.rocketmq.client.AccessChannel.CLOUD);
        reset.setTraceMsgBatchNum(42);
        reset.setMaxPageSizeInGetMetadata(43);
        reset.resetClientConfig(source);

        Map<String, Object> result = new TreeMap<>();
        result.put("source", clientConfigFields(source));
        result.put("clone", clientConfigFields(copy));
        result.put("reset", clientConfigFields(reset));
        result.put("sourceGetters", clientConfigGetters(source));
        result.put("cloneGetters", clientConfigGetters(copy));
        result.put("resetGetters", clientConfigGetters(reset));
        return result;
    }

    private static Map<String, Object> clientConfigGetters(ClientConfig config) {
        Map<String, Object> fields = new TreeMap<>();
        fields.put("namesrvAddr", config.getNamesrvAddr());
        fields.put("clientIP", config.getClientIP());
        fields.put("instanceName", config.getInstanceName());
        fields.put("clientCallbackExecutorThreads", config.getClientCallbackExecutorThreads());
        fields.put("namespace", config.getNamespace());
        fields.put("namespaceV2", config.getNamespaceV2());
        fields.put("accessChannel", config.getAccessChannel());
        fields.put("pollNameServerInterval", config.getPollNameServerInterval());
        fields.put("heartbeatBrokerInterval", config.getHeartbeatBrokerInterval());
        fields.put("persistConsumerOffsetInterval", config.getPersistConsumerOffsetInterval());
        fields.put("pullTimeDelayMillsWhenException", config.getPullTimeDelayMillsWhenException());
        fields.put("traceMsgBatchNum", config.getTraceMsgBatchNum());
        fields.put("unitMode", config.isUnitMode());
        fields.put("unitName", config.getUnitName());
        fields.put("decodeReadBody", config.isDecodeReadBody());
        fields.put("decodeDecompressBody", config.isDecodeDecompressBody());
        fields.put("vipChannelEnabled", config.isVipChannelEnabled());
        fields.put("useHeartbeatV2", config.isUseHeartbeatV2());
        fields.put("useTLS", config.isUseTLS());
        fields.put("socksProxyConfig", config.getSocksProxyConfig());
        fields.put("mqClientApiTimeout", config.getMqClientApiTimeout());
        fields.put("detectTimeout", config.getDetectTimeout());
        fields.put("detectInterval", config.getDetectInterval());
        fields.put("language", config.getLanguage());
        fields.put("enableStreamRequestType", config.isEnableStreamRequestType());
        fields.put("sendLatencyEnable", config.isSendLatencyEnable());
        fields.put("startDetectorEnable", config.isStartDetectorEnable());
        fields.put("enableHeartbeatChannelEventListener", config.isEnableHeartbeatChannelEventListener());
        fields.put("enableConcurrentHeartbeat", config.isEnableConcurrentHeartbeat());
        fields.put("concurrentHeartbeatThreadPoolSize", config.getConcurrentHeartbeatThreadPoolSize());
        fields.put("enableTrace", config.isEnableTrace());
        fields.put("traceTopic", config.getTraceTopic());
        fields.put("maxPageSizeInGetMetadata", config.getMaxPageSizeInGetMetadata());
        return fields;
    }

    private static Map<String, Object> clientConfigFields(ClientConfig config) throws Exception {
        Map<String, Object> fields = new TreeMap<>();
        for (java.lang.reflect.Field field : ClientConfig.class.getDeclaredFields()) {
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers())
                || "namespaceInitialized".equals(field.getName())) {
                continue;
            }
            field.setAccessible(true);
            Object value = field.get(config);
            fields.put(field.getName(), value instanceof Enum<?> ? value.toString() : value);
        }
        return fields;
    }

    private static List<Object> clientConfigToString() throws Exception {
        List<Object> cases = new ArrayList<>();
        ClientConfig nullable = new ClientConfig();
        nullable.setNamesrvAddr(null);
        nullable.setClientIP(null);
        nullable.setInstanceName(null);
        nullable.setClientCallbackExecutorThreads(-17);
        nullable.setNamespaceV2(null);
        nullable.setAccessChannel(null);
        nullable.setPollNameServerInterval(-1);
        nullable.setHeartbeatBrokerInterval(Integer.MIN_VALUE);
        nullable.setPersistConsumerOffsetInterval(Integer.MAX_VALUE);
        nullable.setPullTimeDelayMillsWhenException(Long.MIN_VALUE);
        nullable.setUnitMode(true);
        nullable.setUnitName("unit'quoted");
        nullable.setDecodeReadBody(false);
        nullable.setDecodeDecompressBody(true);
        nullable.setVipChannelEnabled(true);
        nullable.setUseHeartbeatV2(false);
        nullable.setUseTLS(true);
        nullable.setSocksProxyConfig("proxy-secret");
        nullable.setMqClientApiTimeout(-3);
        nullable.setDetectTimeout(4);
        nullable.setDetectInterval(-5);
        nullable.setLanguage(null);
        nullable.setEnableStreamRequestType(true);
        nullable.setSendLatencyEnable(false);
        nullable.setStartDetectorEnable(true);
        nullable.setEnableHeartbeatChannelEventListener(false);
        nullable.setEnableTrace(true);
        nullable.setTraceTopic("trace'quoted");
        nullable.setEnableConcurrentHeartbeat(true);
        nullable.setConcurrentHeartbeatThreadPoolSize(7);
        nullable.setTraceMsgBatchNum(81);
        nullable.setMaxPageSizeInGetMetadata(82);
        Map<String, Object> nullCase = new TreeMap<>();
        nullCase.put("case", "null-fields-uninitialized-namespace");
        nullCase.put("input", clientConfigToStringInput(nullable));
        nullCase.put("toString", nullable.toString());
        cases.add(nullCase);

        ClientConfig populated = new ClientConfig();
        populated.setNamesrvAddr("ns.example:9876");
        populated.setClientIP("198.51.100.7");
        populated.setInstanceName("instance");
        populated.setClientCallbackExecutorThreads(4);
        populated.setNamespace("tenant");
        populated.setNamespaceV2("tenant-v2");
        populated.setAccessChannel(org.apache.rocketmq.client.AccessChannel.CLOUD);
        populated.setPollNameServerInterval(30_000);
        populated.setHeartbeatBrokerInterval(30_001);
        populated.setPersistConsumerOffsetInterval(5_000);
        populated.setPullTimeDelayMillsWhenException(1_000L);
        populated.setUnitMode(false);
        populated.setUnitName("unit");
        populated.setDecodeReadBody(true);
        populated.setDecodeDecompressBody(false);
        populated.setVipChannelEnabled(false);
        populated.setUseHeartbeatV2(true);
        populated.setUseTLS(false);
        populated.setSocksProxyConfig("{\"user\":\"fixture-secret\"}");
        populated.setMqClientApiTimeout(3_000);
        populated.setDetectTimeout(200);
        populated.setDetectInterval(2_000);
        populated.setLanguage(org.apache.rocketmq.remoting.protocol.LanguageCode.CPP);
        populated.setEnableStreamRequestType(false);
        populated.setSendLatencyEnable(true);
        populated.setStartDetectorEnable(false);
        populated.setEnableHeartbeatChannelEventListener(true);
        populated.setEnableTrace(false);
        populated.setTraceTopic("trace-topic");
        populated.setEnableConcurrentHeartbeat(false);
        populated.setConcurrentHeartbeatThreadPoolSize(8);
        populated.setTraceMsgBatchNum(83);
        populated.setMaxPageSizeInGetMetadata(84);
        Map<String, Object> populatedCase = new TreeMap<>();
        populatedCase.put("case", "populated-fields-initialized-namespace");
        populatedCase.put("input", clientConfigToStringInput(populated));
        populatedCase.put("toString", populated.toString());
        cases.add(populatedCase);
        for (org.apache.rocketmq.remoting.protocol.LanguageCode language
            : org.apache.rocketmq.remoting.protocol.LanguageCode.values()) {
            ClientConfig languageCase = populated.cloneClientConfig();
            languageCase.setLanguage(language);
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "language-" + language.name());
            item.put("input", clientConfigToStringInput(languageCase));
            item.put("toString", languageCase.toString());
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> clientConfigToStringInput(ClientConfig config) throws Exception {
        Map<String, Object> fields = clientConfigFields(config);
        java.lang.reflect.Field namespaceInitialized = ClientConfig.class.getDeclaredField("namespaceInitialized");
        namespaceInitialized.setAccessible(true);
        fields.put("namespaceInitialized", namespaceInitialized.getBoolean(config));
        return fields;
    }

    private static Map<String, Object> messageQueueSetters() {
        org.apache.rocketmq.common.message.MessageQueue queue =
            new org.apache.rocketmq.common.message.MessageQueue();
        Map<String, Object> result = new TreeMap<>();
        result.put("defaults", queueRow(queue));
        queue.setTopic("topic");
        queue.setBrokerName("broker");
        queue.setQueueId(-17);
        result.put("populated", queueRow(queue));
        queue.setTopic(null);
        queue.setBrokerName(null);
        queue.setQueueId(Integer.MIN_VALUE);
        result.put("nullable", queueRow(queue));
        return result;
    }

    private static List<Object> queuesWithNamespace() {
        List<Object> cases = new ArrayList<>();
        cases.add(queueNamespaceCase("null-collection-null-namespace", null, null));
        cases.add(queueNamespaceCase("null-collection-empty-namespace", "", null));
        cases.add(queueNamespaceCase("null-collection-populated-namespace", "tenant", null));

        List<org.apache.rocketmq.common.message.MessageQueue> emptyNamespace = new ArrayList<>();
        emptyNamespace.add(new org.apache.rocketmq.common.message.MessageQueue("topic", "broker", 1));
        emptyNamespace.add(null);
        cases.add(queueNamespaceCase("null-element-empty-namespace", "", emptyNamespace));

        List<org.apache.rocketmq.common.message.MessageQueue> partial = new ArrayList<>();
        partial.add(new org.apache.rocketmq.common.message.MessageQueue("topic", "broker", 1));
        partial.add(null);
        partial.add(new org.apache.rocketmq.common.message.MessageQueue("after", "broker", 2));
        cases.add(queueNamespaceCase("null-element-after-mutation", "tenant", partial));

        List<org.apache.rocketmq.common.message.MessageQueue> populated = new ArrayList<>();
        populated.add(new org.apache.rocketmq.common.message.MessageQueue("topic", "broker", 1));
        populated.add(new org.apache.rocketmq.common.message.MessageQueue("tenant%existing", "broker", 2));
        populated.add(new org.apache.rocketmq.common.message.MessageQueue(null, "broker", 3));
        cases.add(queueNamespaceCase("populated-collection", "tenant", populated));
        return cases;
    }

    private static Map<String, Object> queueNamespaceCase(
        String name, String namespace, Collection<org.apache.rocketmq.common.message.MessageQueue> queues) {
        ClientConfig config = new ClientConfig();
        config.setNamespace(namespace);
        Map<String, Object> item = new TreeMap<>();
        item.put("case", name);
        item.put("namespace", namespace);
        item.put("inputQueues", queueRows(queues));
        try {
            Collection<org.apache.rocketmq.common.message.MessageQueue> returned =
                config.queuesWithNamespace(queues);
            item.put("outcome", "OK");
            item.put("returnedSameCollection", returned == queues);
        } catch (Throwable error) {
            item.put("outcome", error.getClass().getSimpleName());
            item.put("returnedSameCollection", false);
        }
        item.put("outputQueues", queueRows(queues));
        return item;
    }

    private static List<Object> queueRows(
        Collection<org.apache.rocketmq.common.message.MessageQueue> queues) {
        if (queues == null) return null;
        List<Object> result = new ArrayList<>();
        for (org.apache.rocketmq.common.message.MessageQueue queue : queues) {
            result.add(queue == null ? null : queueRow(queue));
        }
        return result;
    }

    private static Map<String, Object> queueRow(org.apache.rocketmq.common.message.MessageQueue queue) {
        Map<String, Object> result = new TreeMap<>();
        result.put("topic", queue.getTopic());
        result.put("brokerName", queue.getBrokerName());
        result.put("queueId", queue.getQueueId());
        return result;
    }

    private static void cleanupRuntime(org.apache.rocketmq.client.impl.factory.MQClientInstance runtime) throws Exception {
        java.lang.reflect.Method cleanup = runtime.getClass().getDeclaredMethod("cleanupAfterConstructionFailure",
            org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
        cleanup.setAccessible(true);
        cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("manager removal fixture teardown"));
    }

    private static void awaitUnchecked(CountDownLatch latch, String description) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out waiting for " + description);
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for " + description, error);
        }
    }

    private static void captureThrowable(Map<String, Object> result, String key, Runnable operation) {
        try {
            operation.run();
            result.put(key, "RETURNED");
        } catch (Throwable error) {
            result.put(key, error.getClass().getSimpleName());
        }
    }

    private static void captureBoolean(Map<String, Object> result, String key,
        java.util.function.Supplier<Boolean> operation) {
        try {
            result.put(key, operation.get());
        } catch (Throwable error) {
            result.put(key, error.getClass().getSimpleName());
        }
    }

    private static Map<String, Object> nullableTableViewInputs(String prefix, java.util.Set<?> keys,
        Collection<?> values, java.util.Set<?> entries, Object presentKey, Object presentValue) {
        Map<String, Object> result = new TreeMap<>();
        captureBoolean(result, prefix + "KeyContainsNull", () -> keys.contains(null));
        captureBoolean(result, prefix + "KeyRemoveNull", () -> keys.remove(null));
        captureBoolean(result, prefix + "ValuesContainsNull", () -> values.contains(null));
        captureBoolean(result, prefix + "ValuesRemoveNull", () -> values.remove(null));
        captureBoolean(result, prefix + "EntriesContainsNull", () -> entries.contains(null));
        captureBoolean(result, prefix + "EntriesRemoveNull", () -> entries.remove(null));
        captureBoolean(result, prefix + "EntriesContainsNullKey",
            () -> entries.contains(new java.util.AbstractMap.SimpleImmutableEntry<>(null, presentValue)));
        captureBoolean(result, prefix + "EntriesRemoveNullKey",
            () -> entries.remove(new java.util.AbstractMap.SimpleImmutableEntry<>(null, presentValue)));
        captureBoolean(result, prefix + "EntriesContainsNullValue",
            () -> entries.contains(new java.util.AbstractMap.SimpleImmutableEntry<>(presentKey, null)));
        captureBoolean(result, prefix + "EntriesRemoveNullValue",
            () -> entries.remove(new java.util.AbstractMap.SimpleImmutableEntry<>(presentKey, null)));
        captureThrowable(result, prefix + "KeyRemoveIfNull", () -> keys.removeIf(null));
        captureThrowable(result, prefix + "ValuesRemoveIfNull", () -> values.removeIf(null));
        captureThrowable(result, prefix + "EntriesRemoveIfNull", () -> entries.removeIf(null));
        captureThrowable(result, prefix + "KeyRemoveAllNull", () -> keys.removeAll(null));
        captureThrowable(result, prefix + "ValuesRemoveAllNull", () -> values.removeAll(null));
        captureThrowable(result, prefix + "EntriesRemoveAllNull", () -> entries.removeAll(null));
        captureThrowable(result, prefix + "KeyRetainAllNull", () -> keys.retainAll(null));
        captureThrowable(result, prefix + "ValuesRetainAllNull", () -> values.retainAll(null));
        captureThrowable(result, prefix + "EntriesRetainAllNull", () -> entries.retainAll(null));
        return result;
    }

    private static <T> Map<String, Object> nullableEntrySetValueInputs(String prefix,
        java.util.concurrent.ConcurrentMap<String, T> source) {
        Map<String, Object> result = new TreeMap<>();
        java.util.concurrent.ConcurrentHashMap<String, T> probe = isolatedTableProbe(prefix, source);
        Map.Entry<String, T> entry = probe.entrySet().iterator().next();
        String key = entry.getKey();
        T value = entry.getValue();
        captureThrowable(result, prefix + "EntrySetValueNull", () -> entry.setValue(null));
        result.put(prefix + "EntrySetValueNullEntryUnchanged", entry.getValue() == value);
        result.put(prefix + "EntrySetValueNullTableUnchanged", probe.get(key) == value);
        return result;
    }

    private static <T> void captureTableViewCollectionCase(Map<String, Object> result, String caseName,
        java.util.concurrent.ConcurrentMap<String, T> source, String viewName, String operation,
        Collection<?> members) {
        java.util.concurrent.ConcurrentMap<String, T> table = new java.util.concurrent.ConcurrentHashMap<>(source);
        Collection<?> view;
        if ("Key".equals(viewName)) {
            view = table.keySet();
        } else if ("Values".equals(viewName)) {
            view = table.values();
        } else {
            view = table.entrySet();
        }
        captureBoolean(result, caseName, () -> "RemoveAll".equals(operation)
            ? view.removeAll(members) : view.retainAll(members));
        result.put(caseName + "KeysAfter", new ArrayList<>(new java.util.TreeSet<>(table.keySet())));
    }

    private static <T> java.util.concurrent.ConcurrentHashMap<String, T> isolatedTableProbe(String prefix,
        java.util.concurrent.ConcurrentMap<String, T> source) {
        java.util.concurrent.ConcurrentHashMap<String, T> probe = new java.util.concurrent.ConcurrentHashMap<>();
        if ("factory".equals(prefix)) {
            probe.put("manager-copy@DEFAULT", source.get("manager-copy@DEFAULT"));
        } else {
            probe.put("nullable-default-present", source.get("nullable-default-present"));
            String otherKey = "producer".equals(prefix) ? "strong-producer" : "strong-consumer";
            probe.put(otherKey, source.get(otherKey));
        }
        return probe;
    }

    private static <T> Map<String, Object> nullableTableViewNullElements(String prefix,
        java.util.concurrent.ConcurrentMap<String, T> source) {
        Map<String, Object> result = new TreeMap<>();
        java.util.concurrent.ConcurrentHashMap<String, T> probe = isolatedTableProbe(prefix, source);
        for (String viewName : List.of("Key", "Values", "Entries")) {
            Object matchedMember;
            if ("Key".equals(viewName)) {
                matchedMember = probe.keySet().iterator().next();
            } else if ("Values".equals(viewName)) {
                matchedMember = probe.values().iterator().next();
            } else {
                matchedMember = probe.entrySet().iterator().next();
            }
            List<Object> nullOnly = new ArrayList<>();
            nullOnly.add(null);
            captureTableViewCollectionCase(result, prefix + viewName + "RemoveAllNullElement", probe,
                viewName, "RemoveAll", nullOnly);
            captureTableViewCollectionCase(result, prefix + viewName + "RetainAllNullElement", probe,
                viewName, "RetainAll", nullOnly);
            List<Object> nullThenMatch = new ArrayList<>();
            nullThenMatch.add(null);
            nullThenMatch.add(matchedMember);
            captureTableViewCollectionCase(result, prefix + viewName + "RemoveAllNullThenMatch", probe,
                viewName, "RemoveAll", nullThenMatch);
            List<Object> matchThenNull = new ArrayList<>();
            matchThenNull.add(matchedMember);
            matchThenNull.add(null);
            captureTableViewCollectionCase(result, prefix + viewName + "RemoveAllMatchThenNull", probe,
                viewName, "RemoveAll", matchThenNull);
            captureTableViewCollectionCase(result, prefix + viewName + "RetainAllNullAndMatch", probe,
                viewName, "RetainAll", nullThenMatch);
        }
        return result;
    }

    private static <T> Map<String, Object> nullableRemappingInputs(String prefix,
        java.util.concurrent.ConcurrentMap<String, T> table, T value) {
        Map<String, Object> result = new TreeMap<>();
        captureThrowable(result, prefix + "ComputeIfAbsentNullKey",
            () -> table.computeIfAbsent(null, key -> value));
        captureThrowable(result, prefix + "ComputeIfAbsentNullFunction",
            () -> table.computeIfAbsent("null-compute-if-absent-function", null));
        captureThrowable(result, prefix + "ComputeIfPresentNullKey",
            () -> table.computeIfPresent(null, (key, current) -> value));
        captureThrowable(result, prefix + "ComputeIfPresentNullFunction",
            () -> table.computeIfPresent("null-compute-if-present-function", null));
        captureThrowable(result, prefix + "ComputeNullKey",
            () -> table.compute(null, (key, current) -> value));
        captureThrowable(result, prefix + "ComputeNullFunction",
            () -> table.compute("null-compute-function", null));
        captureThrowable(result, prefix + "MergeNullKey",
            () -> table.merge(null, value, (current, next) -> next));
        captureThrowable(result, prefix + "MergeNullValue",
            () -> table.merge("null-merge-value", null, (current, next) -> next));
        captureThrowable(result, prefix + "MergeNullFunction",
            () -> table.merge("null-merge-function", value, null));
        return result;
    }

    private static <T> Map<String, Object> nullableTableBulkInputs(String prefix,
        java.util.concurrent.ConcurrentMap<String, T> table) {
        Map<String, Object> result = new TreeMap<>();
        captureThrowable(result, prefix + "PutAllNullMap", () -> table.putAll(null));
        captureThrowable(result, prefix + "ForEachNullAction", () -> table.forEach(null));
        captureThrowable(result, prefix + "ReplaceAllNullFunction", () -> table.replaceAll(null));

        T value = table.values().iterator().next();
        java.util.concurrent.ConcurrentMap<String, T> nullKeyTable = isolatedTableProbe(prefix, table);
        Map<String, T> nullKeyInputs = new LinkedHashMap<>();
        nullKeyInputs.put("put-all-before-null-key", value);
        nullKeyInputs.put(null, value);
        nullKeyInputs.put("put-all-after-null-key", value);
        captureThrowable(result, prefix + "PutAllNullKey", () -> nullKeyTable.putAll(nullKeyInputs));
        result.put(prefix + "PutAllNullKeyKeysAfter",
            new ArrayList<>(new java.util.TreeSet<>(nullKeyTable.keySet())));

        java.util.concurrent.ConcurrentMap<String, T> nullValueTable = isolatedTableProbe(prefix, table);
        Map<String, T> nullValueInputs = new LinkedHashMap<>();
        nullValueInputs.put("put-all-before-null-value", value);
        nullValueInputs.put("put-all-null-value", null);
        nullValueInputs.put("put-all-after-null-value", value);
        captureThrowable(result, prefix + "PutAllNullValue", () -> nullValueTable.putAll(nullValueInputs));
        result.put(prefix + "PutAllNullValueKeysAfter",
            new ArrayList<>(new java.util.TreeSet<>(nullValueTable.keySet())));
        result.putAll(nullableHashMapPutAllInputs(prefix, value));
        return result;
    }

    private static String hashMapKeyAtBucket(String stem, int bucket) {
        for (int suffix = 0; suffix < 100_000; suffix++) {
            String key = stem + suffix;
            int hash = key.hashCode();
            if (((hash ^ (hash >>> 16)) & 15) == bucket) return key;
        }
        throw new IllegalStateException("could not find HashMap bucket " + bucket);
    }

    private static <T> Map<String, Object> nullableHashMapPutAllInputs(String prefix, T value) {
        Map<String, Object> result = new TreeMap<>();
        String beforeNullValue = hashMapKeyAtBucket(prefix + "-hash-before-", 2);
        String nullValueKey = hashMapKeyAtBucket(prefix + "-hash-null-", 5);
        String afterNullValue = hashMapKeyAtBucket(prefix + "-hash-after-", 9);
        Map<String, T> source = new java.util.HashMap<>();
        source.put(afterNullValue, value);
        source.put(nullValueKey, null);
        source.put(beforeNullValue, value);
        result.put(prefix + "HashMapPutAllNullValueVisitOrder", new ArrayList<>(source.keySet()));
        result.put(prefix + "HashMapPutAllNullValueKey", nullValueKey);

        java.util.concurrent.ConcurrentHashMap<String, T> target = new java.util.concurrent.ConcurrentHashMap<>();
        target.put("seed", value);
        captureThrowable(result, prefix + "HashMapPutAllNullValue", () -> target.putAll(source));
        result.put(prefix + "HashMapPutAllNullValueKeysAfter",
            new ArrayList<>(new java.util.TreeSet<>(target.keySet())));
        result.putAll(nullableTreeMapPutAllInputs(prefix, value));
        return result;
    }

    private static <T> Map<String, Object> nullableTreeMapPutAllInputs(String prefix, T value) {
        Map<String, Object> result = new TreeMap<>();
        String beforeNullValue = "a-put-all-before-null-value";
        String nullValueKey = "m-put-all-null-value";
        String afterNullValue = "z-put-all-after-null-value";
        Map<String, T> source = new java.util.TreeMap<>();
        source.put(afterNullValue, value);
        source.put(nullValueKey, null);
        source.put(beforeNullValue, value);
        result.put(prefix + "TreeMapPutAllNullValueVisitOrder", new ArrayList<>(source.keySet()));
        result.put(prefix + "TreeMapPutAllNullValueKey", nullValueKey);

        java.util.concurrent.ConcurrentHashMap<String, T> target = new java.util.concurrent.ConcurrentHashMap<>();
        target.put("seed", value);
        captureThrowable(result, prefix + "TreeMapPutAllNullValue", () -> target.putAll(source));
        result.put(prefix + "TreeMapPutAllNullValueKeysAfter",
            new ArrayList<>(new java.util.TreeSet<>(target.keySet())));
        return result;
    }

    private static <T> Map<String, Object> nullableReplaceAllResults(String prefix,
        T original, T replacement) {
        Map<String, Object> result = new TreeMap<>();
        java.util.concurrent.ConcurrentHashMap<String, T> table = new java.util.concurrent.ConcurrentHashMap<>();
        table.put("a", original);
        table.put("b", original);
        List<String> visited = new ArrayList<>();
        captureThrowable(result, prefix + "ReplaceAllNullResult", () -> table.replaceAll((key, current) -> {
            visited.add(key);
            return visited.size() == 1 ? replacement : null;
        }));
        result.put(prefix + "ReplaceAllNullResultVisited", visited);
        result.put(prefix + "ReplaceAllNullResultAUpdated", table.get("a") == replacement);
        result.put(prefix + "ReplaceAllNullResultBOriginal", table.get("b") == original);
        return result;
    }

    private static Map<String, Object> managerTable(ClientConfig config) throws Exception {
        org.apache.rocketmq.client.impl.MQClientManager manager = org.apache.rocketmq.client.impl.MQClientManager.getInstance();
        java.util.concurrent.ConcurrentMap<String, org.apache.rocketmq.client.impl.factory.MQClientInstance> live = manager.getFactoryTable();
        org.apache.rocketmq.client.impl.factory.MQClientInstance first = manager.getOrCreateMQClientInstance(config);
        manager.removeClientFactory(first.getClientId());
        org.apache.rocketmq.client.impl.factory.MQClientInstance second = manager.getOrCreateMQClientInstance(config);
        String id = first.getClientId();
        java.util.Set<String> keyView = live.keySet();
        java.util.Collection<org.apache.rocketmq.client.impl.factory.MQClientInstance> valuesView = live.values();
        java.util.Set<Map.Entry<String, org.apache.rocketmq.client.impl.factory.MQClientInstance>> entryView = live.entrySet();
        Map<String, Object> result = new TreeMap<>();
        try {
            Map<String, Object> nullableGetOrCreateInputs = new TreeMap<>();
            captureThrowable(nullableGetOrCreateInputs, "nullConfig",
                () -> manager.getOrCreateMQClientInstance(null));
            captureThrowable(nullableGetOrCreateInputs, "nullConfigWithNullHook",
                () -> manager.getOrCreateMQClientInstance(null, null));
            result.put("nullableGetOrCreateInputs", nullableGetOrCreateInputs);
            try {
                live.get(null);
                result.put("nullKeyGet", "RETURNED");
            } catch (Throwable error) {
                result.put("nullKeyGet", error.getClass().getSimpleName());
            }
            try {
                live.containsKey(null);
                result.put("nullKeyContainsKey", "RETURNED");
            } catch (Throwable error) {
                result.put("nullKeyContainsKey", error.getClass().getSimpleName());
            }
            try {
                live.getOrDefault(null, first);
                result.put("nullKeyGetOrDefault", "RETURNED");
            } catch (Throwable error) {
                result.put("nullKeyGetOrDefault", error.getClass().getSimpleName());
            }
            try {
                live.containsValue(null);
                result.put("nullValueContainsValue", "RETURNED");
            } catch (Throwable error) {
                result.put("nullValueContainsValue", error.getClass().getSimpleName());
            }
            result.put("nullDefaultOnPresent", live.getOrDefault(id, null) == second ? "MAPPED" : "OTHER");
            result.put("nullDefaultOnMissing",
                live.getOrDefault("missing-null-default", null) == null ? "NULL" : "OTHER");
            result.put("nullableTableViewInputs", nullableTableViewInputs("factory", keyView,
                valuesView, entryView, id, second));
            result.put("nullableEntrySetValueInputs", nullableEntrySetValueInputs("factory", live));
            result.put("nullableTableViewNullElements", nullableTableViewNullElements("factory", live));
            result.put("nullableRemappingInputs", nullableRemappingInputs("factory", live, first));
            result.put("nullableBulkInputs", nullableTableBulkInputs("factory", live));
            result.put("nullableReplaceAllResults", nullableReplaceAllResults("factory", first, second));
            Map<String, Object> nullableTableMutations = new TreeMap<>();
            captureThrowable(nullableTableMutations, "factoryPutNullKey", () -> live.put(null, first));
            captureThrowable(nullableTableMutations, "factoryPutNullValue", () -> live.put("null-put-value", null));
            captureThrowable(nullableTableMutations, "factoryPutIfAbsentNullKey",
                () -> live.putIfAbsent(null, first));
            captureThrowable(nullableTableMutations, "factoryPutIfAbsentNullValue",
                () -> live.putIfAbsent("null-put-if-absent-value", null));
            captureThrowable(nullableTableMutations, "factoryReplaceNullKey",
                () -> live.replace(null, first));
            captureThrowable(nullableTableMutations, "factoryReplaceNullValue",
                () -> live.replace("null-replace-value", null));
            captureThrowable(nullableTableMutations, "factoryConditionalReplaceNullKey",
                () -> live.replace(null, first, second));
            captureThrowable(nullableTableMutations, "factoryConditionalReplaceNullExpected",
                () -> live.replace("null-replace-expected", null, second));
            captureThrowable(nullableTableMutations, "factoryConditionalReplaceNullValue",
                () -> live.replace("null-replace-new-value", first, null));
            captureThrowable(nullableTableMutations, "factoryRemoveNullKey", () -> live.remove(null));
            captureThrowable(nullableTableMutations, "factoryConditionalRemoveNullKey",
                () -> live.remove(null, first));
            nullableTableMutations.put("factoryConditionalRemoveNullValue",
                live.remove("null-remove-value", null));
            result.put("nullableTableMutations", nullableTableMutations);
            result.put("retainedViewSeesSecond", live.get(id) == second);
            result.put("containsValueSecond", live.containsValue(second));
            result.put("containsValueAbsent", !live.containsValue(first));
            result.put("getOrDefaultPresent", live.getOrDefault(id, first) == second);
            result.put("getOrDefaultAbsent", live.getOrDefault("missing", first) == first);
            result.put("putReturnsPrevious", live.put(id, first) == second);
            result.put("managerSeesPut", manager.getOrCreateMQClientInstance(config) == first);
            result.put("putIfAbsentKeepsCurrent", live.putIfAbsent(id, second) == first && live.get(id) == first);
            result.put("staleReplaceFails", !live.replace(id, second, first));
            result.put("matchingReplaceSucceeds", live.replace(id, first, second));
            result.put("replaceReturnsPrevious", live.replace(id, first) == second);
            result.put("staleRemoveFails", !live.remove(id, second));
            result.put("matchingRemoveSucceeds", live.remove(id, first));
            result.put("absentReplaceReturnsNull", live.replace(id, second) == null);
            result.put("absentRemoveReturnsNull", live.remove(id) == null);
            result.put("absentPutIfAbsentReturnsNull", live.putIfAbsent(id, first) == null);
            live.put("", second); // Empty and arbitrary alias keys are allowed.
            result.put("sizeWithAlias", live.size());
            result.put("aliasHasSecond", live.get("") == second);
            Map<String, org.apache.rocketmq.client.impl.factory.MQClientInstance> bulk = new TreeMap<>();
            bulk.put("bulk-a", first);
            bulk.put("bulk-b", second);
            live.putAll(bulk);
            result.put("putAllAdds", live.get("bulk-a") == first && live.get("bulk-b") == second
                && live.size() == 4);
            Map<String, org.apache.rocketmq.client.impl.factory.MQClientInstance> expected = new TreeMap<>();
            expected.put(id, first);
            expected.put("", second);
            expected.put("bulk-a", first);
            expected.put("bulk-b", second);
            Map<String, Boolean> visited = new TreeMap<>();
            live.forEach((key, value) -> visited.put(key, expected.get(key) == value));
            result.put("forEachVisitsCurrentMappings", visited.size() == expected.size()
                && visited.entrySet().stream().allMatch(entry -> Boolean.TRUE.equals(entry.getValue()))
                && expected.keySet().containsAll(visited.keySet()));
            result.put("keyViewSeesBulk", keyView.contains("bulk-a") && keyView.size() == 4);
            result.put("valuesViewSeesBulk", valuesView.contains(first) && valuesView.contains(second)
                && valuesView.size() == 4);
            result.put("entryViewSeesBulk", entryView.contains(new java.util.AbstractMap.SimpleImmutableEntry<>("bulk-a", first))
                && entryView.size() == 4);
            result.put("keyViewRemovesBulk", keyView.removeIf("bulk-b"::equals));
            result.put("entryViewRemovesBulk", entryView.removeIf(entry -> entry.getKey().equals("bulk-a")
                && entry.getValue() == first));
            result.put("valuesViewRemovesCurrent", valuesView.removeIf(value -> value == first));
            result.put("viewsSeeRemovals", !keyView.contains("bulk-b") && !entryView.contains(
                new java.util.AbstractMap.SimpleImmutableEntry<>("bulk-a", first)) && !keyView.contains(id)
                && valuesView.size() == 1);
            live.put("bulk-c", first);
            live.put("bulk-d", second);
            result.put("keyViewRemoveAll", keyView.removeAll(java.util.Collections.singleton("bulk-c")));
            result.put("valuesViewRemoveAll", valuesView.removeAll(java.util.Collections.singleton(second))
                && live.isEmpty());
            live.put("keep", first);
            live.put("drop", second);
            result.put("entryViewRetainAll", entryView.retainAll(java.util.Collections.singleton(
                new java.util.AbstractMap.SimpleImmutableEntry<>("keep", first)))
                && live.size() == 1 && live.get("keep") == first);
            result.put("computeIfAbsentMaps", live.computeIfAbsent("compute-a", key -> first) == first);
            result.put("computeIfAbsentKeepsCurrent", live.computeIfAbsent("compute-a", key -> second) == first);
            result.put("computeIfAbsentNullLeavesAbsent", live.computeIfAbsent("compute-null", key -> null) == null
                && !live.containsKey("compute-null"));
            result.put("computeIfPresentReplaces", live.computeIfPresent("compute-a", (key, value) -> second) == second
                && live.get("compute-a") == second);
            result.put("computeIfPresentNullRemoves", live.computeIfPresent("compute-a", (key, value) -> null) == null
                && !live.containsKey("compute-a"));
            result.put("computeReplaces", live.compute("compute-a", (key, value) -> first) == first
                && live.get("compute-a") == first);
            result.put("computeAbsentInserts", live.compute("compute-b", (key, value) -> second) == second);
            result.put("computeNullRemoves", live.compute("compute-b", (key, value) -> null) == null
                && !live.containsKey("compute-b"));
            result.put("computeAbsentNullLeavesAbsent", live.compute("compute-null-absent", (key, value) -> null) == null
                && !live.containsKey("compute-null-absent"));
            result.put("mergeCombines", live.merge("compute-a", second, (oldValue, newValue) -> newValue) == second);
            result.put("mergeAbsentInserts", live.merge("merge-new", first, (oldValue, newValue) -> null) == first);
            live.put("compute-a", first);
            AtomicInteger concurrentRemappingCalls = new AtomicInteger();
            CountDownLatch remappingEntered = new CountDownLatch(1);
            CountDownLatch releaseRemapping = new CountDownLatch(1);
            CountDownLatch writerStarted = new CountDownLatch(1);
            CountDownLatch writerFinished = new CountDownLatch(1);
            AtomicReference<Throwable> writerFailure = new AtomicReference<>();
            AtomicReference<org.apache.rocketmq.client.impl.factory.MQClientInstance> remappingResult =
                new AtomicReference<>();
            AtomicReference<org.apache.rocketmq.client.impl.factory.MQClientInstance> remappingReadResult =
                new AtomicReference<>();
            Thread remappingThread = new Thread(() -> remappingResult.set(live.compute("compute-a", (key, oldValue) -> {
                concurrentRemappingCalls.incrementAndGet();
                remappingReadResult.set(live.get(key));
                remappingEntered.countDown();
                awaitUnchecked(releaseRemapping, "remapping release");
                return second;
            })), "java-factory-table-remapping-oracle");
            Thread writerThread = new Thread(() -> {
                writerStarted.countDown();
                try {
                    live.put("compute-a", second);
                } catch (Throwable error) {
                    writerFailure.set(error);
                } finally {
                    writerFinished.countDown();
                }
            }, "java-factory-table-remapping-writer");
            remappingThread.setDaemon(true);
            writerThread.setDaemon(true);
            remappingThread.start();
            try {
                awaitUnchecked(remappingEntered, "remapping callback");
                writerThread.start();
                awaitUnchecked(writerStarted, "same-key writer");
                result.put("remappingWriterWaitsForCallback", !writerFinished.await(100, TimeUnit.MILLISECONDS));
            } finally {
                releaseRemapping.countDown();
            }
            remappingThread.join(5000);
            writerThread.join(5000);
            if (remappingThread.isAlive() || writerThread.isAlive()) {
                throw new IllegalStateException("ConcurrentHashMap remapping fixture threads did not finish");
            }
            if (writerFailure.get() != null) {
                throw new IllegalStateException("ConcurrentHashMap same-key writer failed", writerFailure.get());
            }
            result.put("remappingCallbackRunsOnce", concurrentRemappingCalls.get() == 1);
            result.put("remappingCallbackReadsCurrent", remappingReadResult.get() == first);
            result.put("remappingReturnsAndKeepsSecond", remappingResult.get() == second
                && live.get("compute-a") == second);
            live.put(id, first);
            AtomicInteger removalRemappingCalls = new AtomicInteger();
            CountDownLatch removalRemappingEntered = new CountDownLatch(1);
            CountDownLatch releaseRemovalRemapping = new CountDownLatch(1);
            CountDownLatch identityRemovalStarted = new CountDownLatch(1);
            CountDownLatch identityRemovalFinished = new CountDownLatch(1);
            AtomicReference<Throwable> identityRemovalFailure = new AtomicReference<>();
            AtomicReference<org.apache.rocketmq.client.impl.factory.MQClientInstance> removalRemappingResult =
                new AtomicReference<>();
            Thread removalRemappingThread = new Thread(() -> removalRemappingResult.set(
                live.computeIfPresent(id, (key, oldValue) -> {
                    removalRemappingCalls.incrementAndGet();
                    if (oldValue != first) {
                        throw new IllegalStateException("unexpected mapping before identity-removal race");
                    }
                    removalRemappingEntered.countDown();
                    awaitUnchecked(releaseRemovalRemapping, "identity-removal remapping release");
                    return second;
                })), "java-factory-table-identity-removal-remapping");
            Thread identityRemovalThread = new Thread(() -> {
                identityRemovalStarted.countDown();
                try {
                    manager.removeClientFactory(id, first);
                } catch (Throwable error) {
                    identityRemovalFailure.set(error);
                } finally {
                    identityRemovalFinished.countDown();
                }
            }, "java-factory-table-identity-removal");
            removalRemappingThread.setDaemon(true);
            identityRemovalThread.setDaemon(true);
            removalRemappingThread.start();
            try {
                awaitUnchecked(removalRemappingEntered, "identity-removal remapping callback");
                identityRemovalThread.start();
                awaitUnchecked(identityRemovalStarted, "identity-safe manager removal");
                result.put("managerIdentityRemovalWaitsForRemapping",
                    !identityRemovalFinished.await(100, TimeUnit.MILLISECONDS));
            } finally {
                releaseRemovalRemapping.countDown();
            }
            removalRemappingThread.join(5000);
            identityRemovalThread.join(5000);
            if (removalRemappingThread.isAlive() || identityRemovalThread.isAlive()) {
                throw new IllegalStateException("ConcurrentHashMap manager-removal fixture threads did not finish");
            }
            if (identityRemovalFailure.get() != null) {
                throw new IllegalStateException("ConcurrentHashMap identity-safe removal failed",
                    identityRemovalFailure.get());
            }
            result.put("managerIdentityRemovalRemappingRunsOnce", removalRemappingCalls.get() == 1);
            result.put("managerIdentityRemovalKeepsRemappingResult",
                removalRemappingResult.get() == second && live.get(id) == second);
            live.put("merge-remove", second);
            result.put("mergeNullRemoves", live.merge("merge-remove", first, (oldValue, newValue) -> null) == null
                && !live.containsKey("merge-remove"));
            result.putAll(replaceAllRace(live, first, second));
            live.replaceAll((key, value) -> second);
            result.put("replaceAllUpdatesMappings", live.size() == 3
                && live.values().stream().allMatch(value -> value == second));
            live.clear();
            result.put("retainedViewEmptyAfterClear", live.isEmpty());
            result.put("collectionViewsEmptyAfterClear", keyView.isEmpty() && valuesView.isEmpty() && entryView.isEmpty());
            live.put("iterator-key", first);
            java.util.Iterator<String> keyIterator = keyView.iterator();
            keyIterator.next();
            keyIterator.remove();
            result.put("keyIteratorRemove", live.isEmpty());
            live.put("iterator-value", first);
            java.util.Iterator<org.apache.rocketmq.client.impl.factory.MQClientInstance> valueIterator = valuesView.iterator();
            valueIterator.next();
            valueIterator.remove();
            result.put("valuesIteratorRemove", live.isEmpty());
            live.put("iterator-entry", first);
            java.util.Iterator<Map.Entry<String, org.apache.rocketmq.client.impl.factory.MQClientInstance>> entryIterator =
                entryView.iterator();
            entryIterator.next();
            entryIterator.remove();
            result.put("entryIteratorRemove", live.isEmpty());
            live.put("entry-set-value", first);
            java.util.Iterator<Map.Entry<String, org.apache.rocketmq.client.impl.factory.MQClientInstance>> setValueIterator =
                live.entrySet().iterator();
            Map.Entry<String, org.apache.rocketmq.client.impl.factory.MQClientInstance> setValueEntry =
                setValueIterator.next();
            result.put("entrySetValueReturnsPrevious", setValueEntry.setValue(second) == first);
            result.put("entrySetValueUpdatesEntry", setValueEntry.getValue() == second);
            result.put("entrySetValueUpdatesTable", live.get("entry-set-value") == second);
            live.put("entry-set-value", first);
            result.put("entrySetValueEntryIsSnapshot", setValueEntry.getValue() == second);
            result.put("entrySetValueReturnsCapturedPrevious", setValueEntry.setValue(second) == second);
            result.put("entrySetValueReplacesCurrentMapping", live.get("entry-set-value") == second);
            live.clear();
        } finally {
            live.remove(id);
            live.remove("");
            try { cleanupRuntime(first); } finally { cleanupRuntime(second); }
        }
        return result;
    }

    private static <T> T tableOwner(Class<T> type) {
        return tableOwner(type, new Object());
    }

    private static final class TableOwnerHandler implements java.lang.reflect.InvocationHandler {
        private final Object equalityKey;
        TableOwnerHandler(Object equalityKey) { this.equalityKey = equalityKey; }
        private static Object equalityValue(Object key) {
            if (key instanceof java.util.concurrent.atomic.AtomicReference<?>) {
                return ((java.util.concurrent.atomic.AtomicReference<?>) key).get();
            }
            return key;
        }
        @Override public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) {
            if ("equals".equals(method.getName())) {
                Object other = args[0];
                if (other == proxy) return true;
                if (other == null || !java.lang.reflect.Proxy.isProxyClass(other.getClass())) return false;
                java.lang.reflect.InvocationHandler otherHandler = java.lang.reflect.Proxy.getInvocationHandler(other);
                return otherHandler instanceof TableOwnerHandler
                    && java.util.Objects.equals(equalityValue(equalityKey),
                        equalityValue(((TableOwnerHandler) otherHandler).equalityKey));
            }
            if ("hashCode".equals(method.getName())) return equalityValue(equalityKey).hashCode();
            throw new IllegalStateException("map fixture must not invoke owner callback: " + method.getName());
        }
    }

    private static <T> T tableOwner(Class<T> type, Object equalityKey) {
        return type.cast(java.lang.reflect.Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
            new TableOwnerHandler(equalityKey)));
    }

    private static <T> Map<String, Object> tableMutations(java.util.concurrent.ConcurrentMap<String, T> table,
        T first, T second, java.util.function.Function<String, T> select) {
        Map<String, Object> result = new TreeMap<>();
        result.put("initialEmpty", table.isEmpty());
        result.put("firstPutAbsent", table.put("group", first) == null);
        result.put("selectorSeesFirst", select.apply("group") == first);
        result.put("putReturnsFirst", table.put("group", second) == first);
        result.put("selectorSeesReplacement", select.apply("group") == second);
        result.put("putIfAbsentKeepsSecond", table.putIfAbsent("group", first) == second);
        result.put("staleReplaceRejected", !table.replace("group", first, second));
        result.put("conditionalReplaceAccepted", table.replace("group", second, first));
        result.put("replaceReturnsFirst", table.replace("group", second) == first);
        result.put("staleRemoveRejected", !table.remove("group", first));
        result.put("conditionalRemoveAccepted", table.remove("group", second));
        result.put("absentReplaceNull", table.replace("group", first) == null);
        result.put("absentRemoveNull", table.remove("group") == null);
        result.put("putIfAbsentAbsentNull", table.putIfAbsent("group", first) == null);
        result.put("emptyKeyAccepted", table.put("", second) == null);
        result.put("sizeTwo", table.size() == 2);
        table.clear();
        result.put("retainedViewSeesClear", table.isEmpty());
        result.put("selectorSeesClear", select.apply("group") == null);
        result.put("putAfterClearAccepted", table.put("group", second) == null && select.apply("group") == second);
        table.clear();
        return result;
    }

    private static <T> Map<String, Object> tableDefaultOperations(
        java.util.concurrent.ConcurrentMap<String, T> table, T first, T second) {
        Map<String, Object> result = new TreeMap<>();
        table.clear();
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        result.put("computeIfAbsentInsert", table.computeIfAbsent("a", key -> {
            calls.incrementAndGet(); return first;
        }) == first);
        result.put("computeIfAbsentNoRemap", table.computeIfAbsent("a", key -> {
            calls.incrementAndGet(); return second;
        }) == first && calls.get() == 1);
        result.put("computeIfPresentUpdate", table.computeIfPresent("a", (key, old) -> second) == second);
        result.put("computeIfPresentAbsent", table.computeIfPresent("missing", (key, old) -> second) == null);
        result.put("getOrDefaultPresent", table.getOrDefault("a", first) == second);
        result.put("getOrDefaultAbsent", table.getOrDefault("missing", second) == second);
        result.put("computeInsert", table.compute("b", (key, old) -> first) == first);
        result.put("computeRemove", table.compute("b", (key, old) -> null) == null && !table.containsKey("b"));
        result.put("mergeInsert", table.merge("c", first, (old, value) -> second) == first);
        result.put("mergeUpdate", table.merge("c", second, (old, value) -> second) == second);
        table.replaceAll((key, old) -> first);
        result.put("replaceAll", table.get("a") == first && table.get("c") == first);
        result.putAll(replaceAllRace(table, first, second));
        java.util.concurrent.atomic.AtomicInteger visited = new java.util.concurrent.atomic.AtomicInteger();
        table.forEach((key, value) -> visited.incrementAndGet());
        result.put("forEachCount", visited.get());
        Map<String, T> additions = new TreeMap<>();
        additions.put("d", second);
        additions.put("e", first);
        table.putAll(additions);
        result.put("putAllAdds", table.get("d") == second && table.get("e") == first && table.size() == 4);
        result.put("keys", new java.util.TreeSet<>(table.keySet()));
        result.put("valuesCount", table.values().size());
        table.put("remapping-race", first);
        AtomicInteger remappingCalls = new AtomicInteger();
        CountDownLatch remappingEntered = new CountDownLatch(1);
        CountDownLatch releaseRemapping = new CountDownLatch(1);
        CountDownLatch writerStarted = new CountDownLatch(1);
        CountDownLatch writerFinished = new CountDownLatch(1);
        AtomicReference<T> remappingRead = new AtomicReference<>();
        AtomicReference<T> remappingResult = new AtomicReference<>();
        AtomicReference<T> writerPrevious = new AtomicReference<>();
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        Thread remappingThread = new Thread(() -> remappingResult.set(table.compute("remapping-race", (key, old) -> {
            remappingCalls.incrementAndGet();
            remappingRead.set(table.get(key));
            remappingEntered.countDown();
            awaitUnchecked(releaseRemapping, "registry remapping release");
            return second;
        })), "java-registry-table-remapping-oracle");
        Thread writerThread = new Thread(() -> {
            writerStarted.countDown();
            try {
                writerPrevious.set(table.put("remapping-race", first));
            } catch (Throwable error) {
                writerFailure.set(error);
            } finally {
                writerFinished.countDown();
            }
        }, "java-registry-table-remapping-writer");
        remappingThread.setDaemon(true);
        writerThread.setDaemon(true);
        remappingThread.start();
        try {
            awaitUnchecked(remappingEntered, "registry remapping callback");
            writerThread.start();
            awaitUnchecked(writerStarted, "registry same-key writer");
            result.put("remappingWriterWaitsForCallback", !writerFinished.await(100, TimeUnit.MILLISECONDS));
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while checking registry remapping", error);
        } finally {
            releaseRemapping.countDown();
        }
        try {
            remappingThread.join(5000);
            writerThread.join(5000);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while joining registry remapping fixture threads", error);
        }
        if (remappingThread.isAlive() || writerThread.isAlive()) {
            throw new IllegalStateException("ConcurrentHashMap registry remapping fixture threads did not finish");
        }
        if (writerFailure.get() != null) {
            throw new IllegalStateException("ConcurrentHashMap registry same-key writer failed", writerFailure.get());
        }
        result.put("remappingCallbackRunsOnce", remappingCalls.get() == 1);
        result.put("remappingCallbackReadsCurrent", remappingRead.get() == first);
        result.put("remappingReturnsAndKeepsWriterValue", remappingResult.get() == second
            && writerPrevious.get() == second && table.get("remapping-race") == first);
        table.remove("remapping-race");
        table.clear();
        return result;
    }

    private static <T> Map<String, Object> replaceAllRace(
        java.util.concurrent.ConcurrentMap<String, T> table, T first, T second) {
        Map<String, Object> result = new TreeMap<>();
        String key = "replace-all-race";
        table.put(key, first);
        AtomicInteger callbackCalls = new AtomicInteger();
        AtomicReference<T> initialCallbackValue = new AtomicReference<>();
        AtomicReference<T> retryCallbackValue = new AtomicReference<>();
        AtomicReference<T> writerPrevious = new AtomicReference<>();
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        CountDownLatch writerStarted = new CountDownLatch(1);
        CountDownLatch writerFinished = new CountDownLatch(1);
        Thread replaceAllThread = new Thread(() -> {
            try {
                table.replaceAll((currentKey, oldValue) -> {
                    if (!currentKey.equals(key)) {
                        return oldValue;
                    }
                    int call = callbackCalls.incrementAndGet();
                    if (call == 1) {
                        initialCallbackValue.set(oldValue);
                        callbackEntered.countDown();
                        awaitUnchecked(releaseCallback, "replaceAll callback release");
                    } else if (call == 2) {
                        retryCallbackValue.set(oldValue);
                    }
                    return first;
                });
            } catch (Throwable error) {
                callbackFailure.set(error);
            }
        }, "java-replace-all-oracle");
        Thread writerThread = new Thread(() -> {
            writerStarted.countDown();
            try {
                writerPrevious.set(table.put(key, second));
            } catch (Throwable error) {
                writerFailure.set(error);
            } finally {
                writerFinished.countDown();
            }
        }, "java-replace-all-writer");
        replaceAllThread.setDaemon(true);
        writerThread.setDaemon(true);
        replaceAllThread.start();
        try {
            awaitUnchecked(callbackEntered, "replaceAll callback");
            writerThread.start();
            awaitUnchecked(writerStarted, "replaceAll same-key writer");
            result.put("replaceAllWriterRunsDuringCallback", writerFinished.await(100, TimeUnit.MILLISECONDS));
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while checking replaceAll", error);
        } finally {
            releaseCallback.countDown();
        }
        try {
            replaceAllThread.join(5000);
            writerThread.join(5000);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while joining replaceAll fixture threads", error);
        }
        if (replaceAllThread.isAlive() || writerThread.isAlive()) {
            throw new IllegalStateException("ConcurrentHashMap replaceAll fixture threads did not finish");
        }
        if (callbackFailure.get() != null || writerFailure.get() != null) {
            throw new IllegalStateException("ConcurrentHashMap replaceAll fixture failed",
                callbackFailure.get() != null ? callbackFailure.get() : writerFailure.get());
        }
        result.put("replaceAllCallbackRetriesAfterWriter", callbackCalls.get() == 2
            && initialCallbackValue.get() == first && retryCallbackValue.get() == second);
        result.put("replaceAllWriterObservedOldValue", writerPrevious.get() == first);
        result.put("replaceAllFinalValueIsComputed", table.get(key) == first);
        table.remove(key);
        return result;
    }

    private static <T> Map<String, Object> replaceAllEquivalentValueRace(
        java.util.concurrent.ConcurrentMap<String, T> table, T first, T equalWriter, T computed, String prefix) {
        return replaceAllEquivalentValueRace(table, first, equalWriter, computed, prefix, () -> { });
    }

    private static <T> Map<String, Object> replaceAllEquivalentValueRace(
        java.util.concurrent.ConcurrentMap<String, T> table, T first, T equalWriter, T computed, String prefix,
        Runnable beforeCallbackBlocks) {
        Map<String, Object> result = new TreeMap<>();
        String key = "replace-all-equal-race";
        table.put(key, first);
        AtomicInteger callbackCalls = new AtomicInteger();
        AtomicReference<T> initialCallbackValue = new AtomicReference<>();
        AtomicReference<T> writerPrevious = new AtomicReference<>();
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        CountDownLatch writerStarted = new CountDownLatch(1);
        CountDownLatch writerFinished = new CountDownLatch(1);
        Thread replaceAllThread = new Thread(() -> {
            try {
                table.replaceAll((currentKey, oldValue) -> {
                    if (!currentKey.equals(key)) {
                        return oldValue;
                    }
                    if (callbackCalls.incrementAndGet() == 1) {
                        initialCallbackValue.set(oldValue);
                        beforeCallbackBlocks.run();
                        callbackEntered.countDown();
                        awaitUnchecked(releaseCallback, "equal-value replaceAll callback release");
                    }
                    return computed;
                });
            } catch (Throwable error) {
                callbackFailure.set(error);
            }
        }, "java-replace-all-equal-oracle");
        Thread writerThread = new Thread(() -> {
            writerStarted.countDown();
            try {
                writerPrevious.set(table.put(key, equalWriter));
            } catch (Throwable error) {
                writerFailure.set(error);
            } finally {
                writerFinished.countDown();
            }
        }, "java-replace-all-equal-writer");
        replaceAllThread.setDaemon(true);
        writerThread.setDaemon(true);
        replaceAllThread.start();
        try {
            awaitUnchecked(callbackEntered, "equal-value replaceAll callback");
            writerThread.start();
            awaitUnchecked(writerStarted, "equal-value replaceAll writer");
            result.put(prefix + "ReplaceAllEqualWriterRunsDuringCallback",
                writerFinished.await(100, TimeUnit.MILLISECONDS));
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while checking equal-value replaceAll", error);
        } finally {
            releaseCallback.countDown();
        }
        try {
            replaceAllThread.join(5000);
            writerThread.join(5000);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while joining equal-value replaceAll fixture threads", error);
        }
        if (replaceAllThread.isAlive() || writerThread.isAlive()) {
            throw new IllegalStateException("ConcurrentHashMap equal-value replaceAll threads did not finish");
        }
        if (callbackFailure.get() != null || writerFailure.get() != null) {
            throw new IllegalStateException("ConcurrentHashMap equal-value replaceAll fixture failed",
                callbackFailure.get() != null ? callbackFailure.get() : writerFailure.get());
        }
        result.put(prefix + "ReplaceAllEqualWriterPreservesSingleCallback",
            callbackCalls.get() == 1 && initialCallbackValue.get() == first);
        result.put(prefix + "ReplaceAllEqualWriterObservedOldValue", writerPrevious.get() == first);
        result.put(prefix + "ReplaceAllEqualWriterFinalValueIsComputed", table.get(key) == computed);
        table.remove(key);
        return result;
    }

    private static <T> Map<String, Object> tableLiveViews(
        java.util.concurrent.ConcurrentMap<String, T> table, T first, T second) {
        Map<String, Object> result = new TreeMap<>();
        table.clear();
        java.util.Set<String> keys = table.keySet();
        java.util.Collection<T> values = table.values();
        java.util.Set<Map.Entry<String, T>> entries = table.entrySet();
        table.put("a", first);
        result.put("retainedViewsSeeInitialPut", keys.contains("a") && values.contains(first)
            && entries.contains(new java.util.AbstractMap.SimpleImmutableEntry<>("a", first)));
        table.put("b", second);
        result.put("retainedViewsSeeLaterPut", keys.size() == 2 && values.size() == 2 && entries.size() == 2);
        result.put("keyViewRemove", keys.remove("a") && !table.containsKey("a"));
        result.put("valueViewRemove", values.remove(second) && !table.containsKey("b"));
        table.put("c", first);
        result.put("entryViewRemove", entries.remove(new java.util.AbstractMap.SimpleImmutableEntry<>("c", first))
            && !table.containsKey("c"));
        table.put("d", second);
        result.put("retainedViewsSeeRepopulation", keys.contains("d") && values.contains(second)
            && entries.contains(new java.util.AbstractMap.SimpleImmutableEntry<>("d", second)));
        keys.clear();
        result.put("keyViewClear", table.isEmpty());
        table.put("e", first);
        values.clear();
        result.put("valueViewClear", table.isEmpty());
        table.put("f", second);
        entries.clear();
        result.put("entryViewClear", table.isEmpty());

        table.put("remove-key", first);
        table.put("keep-key", second);
        result.put("keyViewRemoveIf", keys.removeIf(key -> key.equals("remove-key"))
            && !table.containsKey("remove-key"));
        table.clear();
        table.put("keep-key", first);
        table.put("drop-key", second);
        result.put("keyViewRetainAll", keys.retainAll(java.util.Collections.singleton("keep-key"))
            && table.size() == 1 && table.containsKey("keep-key"));

        table.clear();
        table.put("remove-value", first);
        table.put("keep-value", second);
        result.put("valueViewRemoveIf", values.removeIf(value -> value == first)
            && !table.containsKey("remove-value"));
        table.clear();
        table.put("keep-value", second);
        table.put("drop-value", first);
        result.put("valueViewRetainAll", values.retainAll(java.util.Collections.singleton(second))
            && table.size() == 1 && table.containsKey("keep-value"));

        table.clear();
        table.put("remove-entry", first);
        table.put("keep-entry", second);
        result.put("entryViewRemoveIf", entries.removeIf(entry -> entry.getKey().equals("remove-entry"))
            && !table.containsKey("remove-entry"));
        table.clear();
        table.put("keep-entry", second);
        table.put("drop-entry", first);
        result.put("entryViewRetainAll", entries.retainAll(java.util.Collections.singleton(
            new java.util.AbstractMap.SimpleImmutableEntry<>("keep-entry", second)))
            && table.size() == 1 && table.containsKey("keep-entry"));

        table.clear();
        table.put("remove-all-key", first);
        table.put("keep-all-key", second);
        result.put("keyViewRemoveAll", keys.removeAll(java.util.Collections.singleton("remove-all-key"))
            && !table.containsKey("remove-all-key"));
        table.clear();
        table.put("remove-all-value", first);
        table.put("keep-all-value", second);
        result.put("valueViewRemoveAll", values.removeAll(java.util.Collections.singleton(first))
            && !table.containsKey("remove-all-value"));
        table.clear();
        table.put("remove-all-entry", first);
        table.put("keep-all-entry", second);
        result.put("entryViewRemoveAll", entries.removeAll(java.util.Collections.singleton(
            new java.util.AbstractMap.SimpleImmutableEntry<>("remove-all-entry", first)))
            && !table.containsKey("remove-all-entry"));

        table.clear();
        table.put("entry-set-value", first);
        Map.Entry<String, T> setValueEntry = entries.iterator().next();
        result.put("entrySetValueReturnsPrevious", setValueEntry.setValue(second) == first);
        result.put("entrySetValueUpdatesEntry", setValueEntry.getValue() == second);
        result.put("entrySetValueUpdatesTable", table.get("entry-set-value") == second);
        table.put("entry-set-value", first);
        result.put("entrySetValueEntryIsSnapshot", setValueEntry.getValue() == second);
        result.put("entrySetValueReturnsCapturedPrevious", setValueEntry.setValue(first) == second);
        result.put("entrySetValueReplacesCurrentMapping", table.get("entry-set-value") == first);
        table.clear();
        return result;
    }

    private static Map<String, Object> registryTables() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("registry-tables");
        config.setNamesrvAddr("");
        org.apache.rocketmq.client.impl.factory.MQClientInstance runtime =
            new org.apache.rocketmq.client.impl.factory.MQClientInstance(config, 0, config.buildMQClientId());
        Map<String, Object> result = new TreeMap<>();
        try {
            org.apache.rocketmq.client.impl.producer.MQProducerInner producerA = tableOwner(org.apache.rocketmq.client.impl.producer.MQProducerInner.class);
            org.apache.rocketmq.client.impl.producer.MQProducerInner producerB = tableOwner(org.apache.rocketmq.client.impl.producer.MQProducerInner.class);
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumerA = tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class);
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumerB = tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class);
            result.put("producer", tableMutations(runtime.getProducerTable(), producerA, producerB, runtime::selectProducer));
            result.put("consumer", tableMutations(runtime.getConsumerTable(), consumerA, consumerB, runtime::selectConsumer));
            result.put("registrationRetainsOwners", registrationRetainsOwners(runtime));
            Map<String, Object> nullableSelectors = new TreeMap<>();
            try {
                runtime.selectProducer(null);
                nullableSelectors.put("producerNullGroup", "RETURNED");
            } catch (Throwable error) {
                nullableSelectors.put("producerNullGroup", error.getClass().getSimpleName());
            }
            try {
                runtime.selectConsumer(null);
                nullableSelectors.put("consumerNullGroup", "RETURNED");
            } catch (Throwable error) {
                nullableSelectors.put("consumerNullGroup", error.getClass().getSimpleName());
            }
            result.put("nullableSelectors", nullableSelectors);
            Map<String, Object> nullableTableReads = new TreeMap<>();
            try {
                runtime.getProducerTable().get(null);
                nullableTableReads.put("producerNullKey", "RETURNED");
            } catch (Throwable error) {
                nullableTableReads.put("producerNullKey", error.getClass().getSimpleName());
            }
            try {
                runtime.getConsumerTable().get(null);
                nullableTableReads.put("consumerNullKey", "RETURNED");
            } catch (Throwable error) {
                nullableTableReads.put("consumerNullKey", error.getClass().getSimpleName());
            }
            try {
                runtime.getProducerTable().containsKey(null);
                nullableTableReads.put("producerNullContainsKey", "RETURNED");
            } catch (Throwable error) {
                nullableTableReads.put("producerNullContainsKey", error.getClass().getSimpleName());
            }
            try {
                runtime.getConsumerTable().containsKey(null);
                nullableTableReads.put("consumerNullContainsKey", "RETURNED");
            } catch (Throwable error) {
                nullableTableReads.put("consumerNullContainsKey", error.getClass().getSimpleName());
            }
            try {
                runtime.getProducerTable().getOrDefault(null, producerA);
                nullableTableReads.put("producerNullGetOrDefault", "RETURNED");
            } catch (Throwable error) {
                nullableTableReads.put("producerNullGetOrDefault", error.getClass().getSimpleName());
            }
            try {
                runtime.getConsumerTable().getOrDefault(null, consumerA);
                nullableTableReads.put("consumerNullGetOrDefault", "RETURNED");
            } catch (Throwable error) {
                nullableTableReads.put("consumerNullGetOrDefault", error.getClass().getSimpleName());
            }
            try {
                runtime.getProducerTable().containsValue(null);
                nullableTableReads.put("producerNullContainsValue", "RETURNED");
            } catch (Throwable error) {
                nullableTableReads.put("producerNullContainsValue", error.getClass().getSimpleName());
            }
            try {
                runtime.getConsumerTable().containsValue(null);
                nullableTableReads.put("consumerNullContainsValue", "RETURNED");
            } catch (Throwable error) {
                nullableTableReads.put("consumerNullContainsValue", error.getClass().getSimpleName());
            }
            nullableTableReads.put("producerNullDefaultOnMissing",
                runtime.getProducerTable().getOrDefault("missing-null-default", null) == null ? "NULL" : "OTHER");
            nullableTableReads.put("consumerNullDefaultOnMissing",
                runtime.getConsumerTable().getOrDefault("missing-null-default", null) == null ? "NULL" : "OTHER");
            runtime.getProducerTable().put("nullable-default-present", producerA);
            runtime.getConsumerTable().put("nullable-default-present", consumerA);
            nullableTableReads.put("producerNullDefaultOnPresent",
                runtime.getProducerTable().getOrDefault("nullable-default-present", null) == producerA ? "MAPPED" : "OTHER");
            nullableTableReads.put("consumerNullDefaultOnPresent",
                runtime.getConsumerTable().getOrDefault("nullable-default-present", null) == consumerA ? "MAPPED" : "OTHER");
            result.put("nullableTableReads", nullableTableReads);
            Map<String, Object> nullableTableViewInputs = new TreeMap<>();
            nullableTableViewInputs.put("producer", nullableTableViewInputs("producer",
                runtime.getProducerTable().keySet(), runtime.getProducerTable().values(),
                runtime.getProducerTable().entrySet(), "nullable-default-present", producerA));
            nullableTableViewInputs.put("consumer", nullableTableViewInputs("consumer",
                runtime.getConsumerTable().keySet(), runtime.getConsumerTable().values(),
                runtime.getConsumerTable().entrySet(), "nullable-default-present", consumerA));
            result.put("nullableTableViewInputs", nullableTableViewInputs);
            Map<String, Object> nullableEntrySetValueInputs = new TreeMap<>();
            nullableEntrySetValueInputs.put("producer",
                nullableEntrySetValueInputs("producer", runtime.getProducerTable()));
            nullableEntrySetValueInputs.put("consumer",
                nullableEntrySetValueInputs("consumer", runtime.getConsumerTable()));
            result.put("nullableEntrySetValueInputs", nullableEntrySetValueInputs);
            Map<String, Object> nullableTableViewNullElements = new TreeMap<>();
            nullableTableViewNullElements.put("producer",
                nullableTableViewNullElements("producer", runtime.getProducerTable()));
            nullableTableViewNullElements.put("consumer",
                nullableTableViewNullElements("consumer", runtime.getConsumerTable()));
            result.put("nullableTableViewNullElements", nullableTableViewNullElements);
            Map<String, Object> nullableRemappingInputs = new TreeMap<>();
            nullableRemappingInputs.put("producer",
                nullableRemappingInputs("producer", runtime.getProducerTable(), producerA));
            nullableRemappingInputs.put("consumer",
                nullableRemappingInputs("consumer", runtime.getConsumerTable(), consumerA));
            result.put("nullableRemappingInputs", nullableRemappingInputs);
            Map<String, Object> nullableBulkInputs = new TreeMap<>();
            nullableBulkInputs.put("producer", nullableTableBulkInputs("producer", runtime.getProducerTable()));
            nullableBulkInputs.put("consumer", nullableTableBulkInputs("consumer", runtime.getConsumerTable()));
            result.put("nullableBulkInputs", nullableBulkInputs);
            Map<String, Object> nullableReplaceAllResults = new TreeMap<>();
            nullableReplaceAllResults.put("producer", nullableReplaceAllResults("producer", producerA, producerB));
            nullableReplaceAllResults.put("consumer", nullableReplaceAllResults("consumer", consumerA, consumerB));
            result.put("nullableReplaceAllResults", nullableReplaceAllResults);
            Map<String, Object> nullableTableMutations = new TreeMap<>();
            captureThrowable(nullableTableMutations, "producerPutNullKey",
                () -> runtime.getProducerTable().put(null, producerA));
            captureThrowable(nullableTableMutations, "producerPutNullValue",
                () -> runtime.getProducerTable().put("null-put-value", null));
            captureThrowable(nullableTableMutations, "producerPutIfAbsentNullKey",
                () -> runtime.getProducerTable().putIfAbsent(null, producerA));
            captureThrowable(nullableTableMutations, "producerPutIfAbsentNullValue",
                () -> runtime.getProducerTable().putIfAbsent("null-put-if-absent-value", null));
            captureThrowable(nullableTableMutations, "producerReplaceNullKey",
                () -> runtime.getProducerTable().replace(null, producerA));
            captureThrowable(nullableTableMutations, "producerReplaceNullValue",
                () -> runtime.getProducerTable().replace("null-replace-value", null));
            captureThrowable(nullableTableMutations, "producerConditionalReplaceNullKey",
                () -> runtime.getProducerTable().replace(null, producerA, producerB));
            captureThrowable(nullableTableMutations, "producerConditionalReplaceNullExpected",
                () -> runtime.getProducerTable().replace("null-replace-expected", null, producerB));
            captureThrowable(nullableTableMutations, "producerConditionalReplaceNullValue",
                () -> runtime.getProducerTable().replace("null-replace-new-value", producerA, null));
            captureThrowable(nullableTableMutations, "producerRemoveNullKey",
                () -> runtime.getProducerTable().remove(null));
            captureThrowable(nullableTableMutations, "producerConditionalRemoveNullKey",
                () -> runtime.getProducerTable().remove(null, producerA));
            nullableTableMutations.put("producerConditionalRemoveNullValue",
                runtime.getProducerTable().remove("null-remove-value", null));
            captureThrowable(nullableTableMutations, "consumerPutNullKey",
                () -> runtime.getConsumerTable().put(null, consumerA));
            captureThrowable(nullableTableMutations, "consumerPutNullValue",
                () -> runtime.getConsumerTable().put("null-put-value", null));
            captureThrowable(nullableTableMutations, "consumerPutIfAbsentNullKey",
                () -> runtime.getConsumerTable().putIfAbsent(null, consumerA));
            captureThrowable(nullableTableMutations, "consumerPutIfAbsentNullValue",
                () -> runtime.getConsumerTable().putIfAbsent("null-put-if-absent-value", null));
            captureThrowable(nullableTableMutations, "consumerReplaceNullKey",
                () -> runtime.getConsumerTable().replace(null, consumerA));
            captureThrowable(nullableTableMutations, "consumerReplaceNullValue",
                () -> runtime.getConsumerTable().replace("null-replace-value", null));
            captureThrowable(nullableTableMutations, "consumerConditionalReplaceNullKey",
                () -> runtime.getConsumerTable().replace(null, consumerA, consumerB));
            captureThrowable(nullableTableMutations, "consumerConditionalReplaceNullExpected",
                () -> runtime.getConsumerTable().replace("null-replace-expected", null, consumerB));
            captureThrowable(nullableTableMutations, "consumerConditionalReplaceNullValue",
                () -> runtime.getConsumerTable().replace("null-replace-new-value", consumerA, null));
            captureThrowable(nullableTableMutations, "consumerRemoveNullKey",
                () -> runtime.getConsumerTable().remove(null));
            captureThrowable(nullableTableMutations, "consumerConditionalRemoveNullKey",
                () -> runtime.getConsumerTable().remove(null, consumerA));
            nullableTableMutations.put("consumerConditionalRemoveNullValue",
                runtime.getConsumerTable().remove("null-remove-value", null));
            result.put("nullableTableMutations", nullableTableMutations);
            result.put("producerDefaultOperations",
                tableDefaultOperations(runtime.getProducerTable(), producerA, producerB));
            result.put("consumerDefaultOperations",
                tableDefaultOperations(runtime.getConsumerTable(), consumerA, consumerB));
            result.put("producerLiveViews", tableLiveViews(runtime.getProducerTable(), producerA, producerB));
            result.put("consumerLiveViews", tableLiveViews(runtime.getConsumerTable(), consumerA, consumerB));
            Map<String, Object> equalValues = new TreeMap<>();
            org.apache.rocketmq.client.impl.producer.MQProducerInner producerEqualA =
                tableOwner(org.apache.rocketmq.client.impl.producer.MQProducerInner.class, "producer-equal");
            org.apache.rocketmq.client.impl.producer.MQProducerInner producerEqualB =
                tableOwner(org.apache.rocketmq.client.impl.producer.MQProducerInner.class, "producer-equal");
            org.apache.rocketmq.client.impl.producer.MQProducerInner producerReplacement =
                tableOwner(org.apache.rocketmq.client.impl.producer.MQProducerInner.class, "producer-replacement");
            org.apache.rocketmq.client.impl.producer.MQProducerInner producerOther =
                tableOwner(org.apache.rocketmq.client.impl.producer.MQProducerInner.class, "producer-other");
            equalValues.putAll(replaceAllEquivalentValueRace(runtime.getProducerTable(), producerEqualA,
                producerEqualB, producerReplacement, "producer"));
            AtomicReference<Object> producerMutableEqualityKey = new AtomicReference<>("producer-mutable-before");
            equalValues.putAll(replaceAllEquivalentValueRace(runtime.getProducerTable(),
                tableOwner(org.apache.rocketmq.client.impl.producer.MQProducerInner.class,
                    producerMutableEqualityKey),
                tableOwner(org.apache.rocketmq.client.impl.producer.MQProducerInner.class,
                    "producer-mutable-after"),
                producerReplacement, "producerMutable",
                () -> producerMutableEqualityKey.set("producer-mutable-after")));
            runtime.getProducerTable().put("equal-replace", producerEqualA);
            equalValues.put("producerContainsEqualValue", runtime.getProducerTable().containsValue(producerEqualB));
            equalValues.put("producerReplaceByEqualValue",
                runtime.getProducerTable().replace("equal-replace", producerEqualB, producerReplacement));
            runtime.getProducerTable().put("equal-remove", producerEqualA);
            equalValues.put("producerRemoveByEqualValue",
                runtime.getProducerTable().remove("equal-remove", producerEqualB));
            runtime.getProducerTable().put("equal-values-remove", producerEqualA);
            equalValues.put("producerValuesRemoveByEqualValue",
                runtime.getProducerTable().values().remove(producerEqualB));
            runtime.getProducerTable().put("equal-entry-remove", producerEqualA);
            Map.Entry<String, org.apache.rocketmq.client.impl.producer.MQProducerInner> producerEntry =
                new java.util.AbstractMap.SimpleImmutableEntry<>("equal-entry-remove", producerEqualB);
            equalValues.put("producerEntryContainsEqualValue", runtime.getProducerTable().entrySet().contains(producerEntry));
            equalValues.put("producerEntryRemoveByEqualValue", runtime.getProducerTable().entrySet().remove(producerEntry));
            runtime.getProducerTable().put("equal-values-remove-all", producerEqualA);
            equalValues.put("producerValuesRemoveAllByEqualValue",
                runtime.getProducerTable().values().removeAll(java.util.Collections.singleton(producerEqualB)));
            runtime.getProducerTable().put("equal-values-retain-all", producerEqualA);
            runtime.getProducerTable().put("other-values-retain-all", producerOther);
            equalValues.put("producerValuesRetainAllByEqualValue",
                runtime.getProducerTable().values().retainAll(java.util.Collections.singleton(producerEqualB))
                    && runtime.getProducerTable().containsKey("equal-values-retain-all")
                    && !runtime.getProducerTable().containsKey("other-values-retain-all"));
            runtime.getProducerTable().put("equal-entry-retain-all", producerEqualA);
            runtime.getProducerTable().put("other-entry-retain-all", producerOther);
            equalValues.put("producerEntryRetainAllByEqualValue",
                runtime.getProducerTable().entrySet().retainAll(java.util.Collections.singleton(
                    new java.util.AbstractMap.SimpleImmutableEntry<>("equal-entry-retain-all", producerEqualB)))
                    && runtime.getProducerTable().containsKey("equal-entry-retain-all")
                    && !runtime.getProducerTable().containsKey("other-entry-retain-all"));
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumerEqualA =
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class, "consumer-equal");
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumerEqualB =
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class, "consumer-equal");
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumerReplacement =
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class, "consumer-replacement");
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumerOther =
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class, "consumer-other");
            equalValues.putAll(replaceAllEquivalentValueRace(runtime.getConsumerTable(), consumerEqualA,
                consumerEqualB, consumerReplacement, "consumer"));
            AtomicReference<Object> consumerMutableEqualityKey = new AtomicReference<>("consumer-mutable-before");
            equalValues.putAll(replaceAllEquivalentValueRace(runtime.getConsumerTable(),
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class,
                    consumerMutableEqualityKey),
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class,
                    "consumer-mutable-after"),
                consumerReplacement, "consumerMutable",
                () -> consumerMutableEqualityKey.set("consumer-mutable-after")));
            runtime.getConsumerTable().put("equal-replace", consumerEqualA);
            equalValues.put("consumerContainsEqualValue", runtime.getConsumerTable().containsValue(consumerEqualB));
            equalValues.put("consumerReplaceByEqualValue",
                runtime.getConsumerTable().replace("equal-replace", consumerEqualB, consumerReplacement));
            runtime.getConsumerTable().put("equal-remove", consumerEqualA);
            equalValues.put("consumerRemoveByEqualValue",
                runtime.getConsumerTable().remove("equal-remove", consumerEqualB));
            runtime.getConsumerTable().put("equal-values-remove", consumerEqualA);
            equalValues.put("consumerValuesRemoveByEqualValue",
                runtime.getConsumerTable().values().remove(consumerEqualB));
            runtime.getConsumerTable().put("equal-entry-remove", consumerEqualA);
            Map.Entry<String, org.apache.rocketmq.client.impl.consumer.MQConsumerInner> consumerEntry =
                new java.util.AbstractMap.SimpleImmutableEntry<>("equal-entry-remove", consumerEqualB);
            equalValues.put("consumerEntryContainsEqualValue", runtime.getConsumerTable().entrySet().contains(consumerEntry));
            equalValues.put("consumerEntryRemoveByEqualValue", runtime.getConsumerTable().entrySet().remove(consumerEntry));
            runtime.getConsumerTable().put("equal-values-remove-all", consumerEqualA);
            equalValues.put("consumerValuesRemoveAllByEqualValue",
                runtime.getConsumerTable().values().removeAll(java.util.Collections.singleton(consumerEqualB)));
            runtime.getConsumerTable().put("equal-values-retain-all", consumerEqualA);
            runtime.getConsumerTable().put("other-values-retain-all", consumerOther);
            equalValues.put("consumerValuesRetainAllByEqualValue",
                runtime.getConsumerTable().values().retainAll(java.util.Collections.singleton(consumerEqualB))
                    && runtime.getConsumerTable().containsKey("equal-values-retain-all")
                    && !runtime.getConsumerTable().containsKey("other-values-retain-all"));
            runtime.getConsumerTable().put("equal-entry-retain-all", consumerEqualA);
            runtime.getConsumerTable().put("other-entry-retain-all", consumerOther);
            equalValues.put("consumerEntryRetainAllByEqualValue",
                runtime.getConsumerTable().entrySet().retainAll(java.util.Collections.singleton(
                    new java.util.AbstractMap.SimpleImmutableEntry<>("equal-entry-retain-all", consumerEqualB)))
                    && runtime.getConsumerTable().containsKey("equal-entry-retain-all")
                    && !runtime.getConsumerTable().containsKey("other-entry-retain-all"));
            java.util.concurrent.atomic.AtomicReference<Object> storedKey =
                new java.util.concurrent.atomic.AtomicReference<>("dynamic-before");
            java.util.concurrent.atomic.AtomicReference<Object> probeKey =
                new java.util.concurrent.atomic.AtomicReference<>("dynamic-before");
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner dynamicStored =
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class, storedKey);
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner dynamicProbe =
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class, probeKey);
            runtime.getConsumerTable().put("dynamic-equality", dynamicStored);
            equalValues.put("dynamicContainsBeforeMutation",
                runtime.getConsumerTable().containsValue(dynamicProbe));
            storedKey.set("dynamic-after");
            equalValues.put("dynamicContainsAfterStoredMutation",
                !runtime.getConsumerTable().containsValue(dynamicProbe));
            probeKey.set("dynamic-after");
            equalValues.put("dynamicContainsAfterBothMutations",
                runtime.getConsumerTable().containsValue(dynamicProbe));
            java.util.concurrent.atomic.AtomicReference<Object> replaceStoredKey =
                new java.util.concurrent.atomic.AtomicReference<>("replace-before");
            java.util.concurrent.atomic.AtomicReference<Object> replaceExpectedKey =
                new java.util.concurrent.atomic.AtomicReference<>("replace-before");
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner replaceStored =
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class, replaceStoredKey);
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner replaceExpected =
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class, replaceExpectedKey);
            runtime.getConsumerTable().put("dynamic-replace", replaceStored);
            replaceExpectedKey.set("stale");
            equalValues.put("dynamicReplaceRejectsChangedExpected",
                !runtime.getConsumerTable().replace("dynamic-replace", replaceExpected, consumerReplacement));
            replaceStoredKey.set("replace-after");
            replaceExpectedKey.set("replace-after");
            equalValues.put("dynamicReplaceMatchesCurrentEquality",
                runtime.getConsumerTable().replace("dynamic-replace", replaceExpected, consumerReplacement));
            result.put("customEquality", equalValues);
            java.util.concurrent.ConcurrentMap<String, org.apache.rocketmq.client.impl.consumer.MQConsumerInner> retained = runtime.getConsumerTable();
            result.put("consumerRegisterAccepted", runtime.registerConsumer("registered", consumerA));
            result.put("consumerDirectPutReturnsRegistered", retained.put("registered", consumerB) == consumerA);
            result.put("consumerRegisterAfterDirectPutRejected", !runtime.registerConsumer("registered", consumerA));
            runtime.unregisterConsumer("registered"); // Empty Broker table: no outbound RPC.
            result.put("consumerUnregisterRemovesCurrentMapping", !retained.containsKey("registered"));
            result.put("adminRegistration", adminRegistration(runtime));
        } finally { cleanupRuntime(runtime); }
        return result;
    }

    private static Map<String, Object> registrationRetainsOwners(
        org.apache.rocketmq.client.impl.factory.MQClientInstance runtime) {
        Map<String, Object> result = new TreeMap<>();
        WeakReference<org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl> producer =
            registerProducerWithoutRetaining(runtime);
        org.apache.rocketmq.client.impl.producer.MQProducerInner selectedProducer =
            runtime.selectProducer("strong-producer");
        result.put("producerWeakReferenceStillLive", producer.get() != null);
        result.put("producerSelectorReturnsRegisteredOwner", selectedProducer != null && selectedProducer == producer.get());

        WeakReference<org.apache.rocketmq.client.impl.consumer.MQConsumerInner> consumer =
            registerConsumerWithoutRetaining(runtime);
        org.apache.rocketmq.client.impl.consumer.MQConsumerInner selectedConsumer =
            runtime.selectConsumer("strong-consumer");
        result.put("consumerWeakReferenceStillLive", consumer.get() != null);
        result.put("consumerSelectorReturnsRegisteredOwner", selectedConsumer != null && selectedConsumer == consumer.get());
        return result;
    }

    private static WeakReference<org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl>
        registerProducerWithoutRetaining(
            org.apache.rocketmq.client.impl.factory.MQClientInstance runtime) {
        org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl owner =
            new org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl(
                new org.apache.rocketmq.client.producer.DefaultMQProducer("strong-producer"));
        if (!runtime.registerProducer("strong-producer", owner)) {
            throw new IllegalStateException("producer registration rejected");
        }
        return new WeakReference<>(owner);
    }

    private static WeakReference<org.apache.rocketmq.client.impl.consumer.MQConsumerInner>
        registerConsumerWithoutRetaining(
            org.apache.rocketmq.client.impl.factory.MQClientInstance runtime) {
        org.apache.rocketmq.client.impl.consumer.MQConsumerInner owner =
            tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class);
        if (!runtime.registerConsumer("strong-consumer", owner)) {
            throw new IllegalStateException("consumer registration rejected");
        }
        return new WeakReference<>(owner);
    }

    private static Map<String, Object> adminRegistration(
        org.apache.rocketmq.client.impl.factory.MQClientInstance runtime) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        java.lang.reflect.Field field = runtime.getClass().getDeclaredField("adminExtTable");
        field.setAccessible(true);
        java.util.concurrent.ConcurrentMap<String, org.apache.rocketmq.client.admin.MQAdminExtInner> table =
            (java.util.concurrent.ConcurrentMap<String, org.apache.rocketmq.client.admin.MQAdminExtInner>) field.get(runtime);
        org.apache.rocketmq.client.admin.MQAdminExtInner first =
            tableOwner(org.apache.rocketmq.client.admin.MQAdminExtInner.class, "admin-first");
        org.apache.rocketmq.client.admin.MQAdminExtInner second =
            tableOwner(org.apache.rocketmq.client.admin.MQAdminExtInner.class, "admin-second");
        result.put("nullGroupRejected", !runtime.registerAdminExt(null, first));
        result.put("nullAdminRejected", !runtime.registerAdminExt("admin", null));
        result.put("emptyAfterNullRejected", table.isEmpty());
        result.put("firstRegistrationAccepted", runtime.registerAdminExt("admin", first));
        result.put("firstOwnerRetained", table.get("admin") == first);
        result.put("duplicateRejected", !runtime.registerAdminExt("admin", second));
        result.put("duplicateKeepsFirstOwner", table.get("admin") == first);
        result.put("groupUnregisterRemovesOwner", removeAdmin(runtime, table, "admin"));
        result.put("unregisterMissingIsNoOp", removeAdmin(runtime, table, "missing"));
        boolean nullUnregisterThrows;
        try {
            runtime.unregisterAdminExt(null);
            nullUnregisterThrows = false;
        } catch (NullPointerException expected) {
            nullUnregisterThrows = true;
        }
        result.put("nullUnregisterThrows", nullUnregisterThrows);
        result.put("emptyGroupAccepted", runtime.registerAdminExt("", first));
        result.put("emptyGroupRetainsOwner", table.get("") == first);
        runtime.unregisterAdminExt("");
        result.put("emptyGroupRemoved", !table.containsKey(""));
        result.put("secondRegistrationAcceptedAfterRemoval", runtime.registerAdminExt("admin", second));
        result.put("secondOwnerRetainedAfterRemoval", table.get("admin") == second);
        runtime.unregisterAdminExt("admin");
        result.put("shutdownGate", adminShutdownGate());
        return result;
    }

    private static Map<String, Object> adminShutdownGate() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        ClientConfig config = new ClientConfig();
        config.setClientIP("admin-shutdown-gate");
        config.setNamesrvAddr("");
        org.apache.rocketmq.client.impl.MQClientManager manager =
            org.apache.rocketmq.client.impl.MQClientManager.getInstance();
        org.apache.rocketmq.client.impl.factory.MQClientInstance runtime =
            manager.getOrCreateMQClientInstance(config);
        org.apache.rocketmq.client.admin.MQAdminExtInner admin =
            tableOwner(org.apache.rocketmq.client.admin.MQAdminExtInner.class, "shutdown-admin");
        try {
            runtime.start();
            result.put("registrationAccepted", runtime.registerAdminExt("shutdown-admin", admin));
            runtime.shutdown();
            result.put("shutdownBlockedByAdmin",
                manager.getFactoryTable().get(runtime.getClientId()) == runtime);
            runtime.unregisterAdminExt("shutdown-admin");
            runtime.shutdown();
            result.put("shutdownAfterAdminRemoval",
                !manager.getFactoryTable().containsKey(runtime.getClientId()));
        } finally {
            runtime.unregisterAdminExt("shutdown-admin");
            runtime.shutdown();
            manager.removeClientFactory(runtime.getClientId(), runtime);
        }
        return result;
    }

    private static boolean removeAdmin(
        org.apache.rocketmq.client.impl.factory.MQClientInstance runtime,
        java.util.concurrent.ConcurrentMap<String, org.apache.rocketmq.client.admin.MQAdminExtInner> table,
        String group) {
        runtime.unregisterAdminExt(group);
        return !table.containsKey(group);
    }

    private static List<Object> callbackThreadsConstruction() throws Exception {
        List<Object> cases = new ArrayList<>();
        for (int threads : new int[] {Integer.MIN_VALUE, -1, 0, 1, 4, Integer.MAX_VALUE}) {
            ClientConfig config = new ClientConfig();
            config.setClientIP("callback-threads-" + threads);
            config.setNamesrvAddr("");
            config.setClientCallbackExecutorThreads(threads);
            Map<String, Object> item = new TreeMap<>();
            item.put("input", threads);
            org.apache.rocketmq.client.impl.factory.MQClientInstance runtime = null;
            try {
                runtime = new org.apache.rocketmq.client.impl.factory.MQClientInstance(config, 0, config.buildMQClientId());
                item.put("outcome", "OK");
                item.put("raw", runtime.getNettyClientConfig().getClientCallbackExecutorThreads());
                org.apache.rocketmq.remoting.netty.NettyRemotingClient remoting =
                    (org.apache.rocketmq.remoting.netty.NettyRemotingClient) runtime.getMQClientAPIImpl().getRemotingClient();
                java.lang.reflect.Field field = remoting.getClass().getDeclaredField("publicExecutor");
                field.setAccessible(true);
                java.util.concurrent.ThreadPoolExecutor executor = (java.util.concurrent.ThreadPoolExecutor) field.get(remoting);
                item.put("effective", executor.getCorePoolSize());
                item.put("activeAtConstruction", executor.getPoolSize());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
            } finally {
                if (runtime != null) cleanupRuntime(runtime);
            }
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> callbackThreadsMutation() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("callback-threads-mutation");
        config.setNamesrvAddr("");
        config.setClientCallbackExecutorThreads(1);
        org.apache.rocketmq.client.impl.factory.MQClientInstance runtime = null;
        Map<String, Object> result = new TreeMap<>();
        try {
            runtime = new org.apache.rocketmq.client.impl.factory.MQClientInstance(
                config, 0, config.buildMQClientId());
            org.apache.rocketmq.remoting.netty.NettyRemotingClient remoting =
                (org.apache.rocketmq.remoting.netty.NettyRemotingClient)
                    runtime.getMQClientAPIImpl().getRemotingClient();
            java.lang.reflect.Field field = remoting.getClass().getDeclaredField("publicExecutor");
            field.setAccessible(true);
            java.util.concurrent.ThreadPoolExecutor executor =
                (java.util.concurrent.ThreadPoolExecutor) field.get(remoting);
            result.put("configIdentity", runtime.getClientConfig() == config);
            result.put("configuredAtConstruction", config.getClientCallbackExecutorThreads());
            result.put("nettyAtConstruction", runtime.getNettyClientConfig().getClientCallbackExecutorThreads());
            result.put("executorCapacityAtConstruction", executor.getCorePoolSize());
            runtime.getClientConfig().setClientCallbackExecutorThreads(2);
            result.put("configAfterMutation", runtime.getClientConfig().getClientCallbackExecutorThreads());
            result.put("nettyAfterMutation", runtime.getNettyClientConfig().getClientCallbackExecutorThreads());
            result.put("executorCapacityAfterMutation", executor.getCorePoolSize());
        } finally {
            if (runtime != null) cleanupRuntime(runtime);
        }
        return result;
    }

    /** Direct Fastjson 2.0.63 source-path probe for every possible leading UTF-16 unit. */
    private static Map<String, Object> socksLeadingCodeUnits() {
        Map<String, Object> cases = new TreeMap<>();
        String route = "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}";
        com.alibaba.fastjson2.TypeReference<Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>> type =
            new com.alibaba.fastjson2.TypeReference<>() { };
        for (String[] probe : new String[][] {{"route", route}, {"null", "null"}}) {
            List<Object> accepted = new ArrayList<>();
            for (int codeUnit = 0; codeUnit <= Character.MAX_VALUE; codeUnit++) {
                String input = (char) codeUnit + probe[1];
                try {
                    com.alibaba.fastjson2.JSON.parseObject(input, type);
                    accepted.add(codeUnit);
                } catch (RuntimeException rejected) {
                    // The compact golden stores successful units; native checks the whole BMP.
                }
            }
            cases.put(probe[0], accepted);
        }
        return cases;
    }

    /** Direct Fastjson whitespace scan for C0 units at each JSON token boundary. */
    private static Map<String, Object> socksControlBoundaryCases() {
        Map<String, Object> result = new TreeMap<>();
        String[][] boundaries = {
            {"root_after_open", "{", "\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}"},
            {"root_before_colon", "{\"10.0.0.0/8\"", ":{\"addr\":\"proxy:1080\"}}"},
            {"root_after_colon", "{\"10.0.0.0/8\":", "{\"addr\":\"proxy:1080\"}}"},
            {"proxy_after_open", "{\"10.0.0.0/8\":{", "\"addr\":\"proxy:1080\"}}"},
            {"proxy_before_close", "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"", "}}"},
            {"root_before_close", "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}", "}"}
        };
        com.alibaba.fastjson2.TypeReference<Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>> type =
            new com.alibaba.fastjson2.TypeReference<>() { };
        for (String[] boundary : boundaries) {
            List<Object> accepted = new ArrayList<>();
            for (int codeUnit = 0; codeUnit <= 32; codeUnit++) {
                String input = boundary[1] + (char) codeUnit + boundary[2];
                try {
                    com.alibaba.fastjson2.JSON.parseObject(input, type);
                    accepted.add(codeUnit);
                } catch (RuntimeException rejected) {
                    // Keep only accepted C0 units in the compact source oracle.
                }
            }
            result.put(boundary[0], accepted);
        }
        return result;
    }

    /** Direct Java trivia candidates across Unicode whitespace and control categories. */
    private static Map<String, Object> socksUnicodeTriviaCases() {
        Map<String, Object> result = new TreeMap<>();
        String[][] boundaries = {
            {"root_after_open", "{", "\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}"},
            {"root_before_colon", "{\"10.0.0.0/8\"", ":{\"addr\":\"proxy:1080\"}}"},
            {"root_after_colon", "{\"10.0.0.0/8\":", "{\"addr\":\"proxy:1080\"}}"},
            {"proxy_after_open", "{\"10.0.0.0/8\":{", "\"addr\":\"proxy:1080\"}}"},
            {"proxy_before_close", "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"", "}}"},
            {"root_before_close", "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}", "}"}
        };
        com.alibaba.fastjson2.TypeReference<Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>> type =
            new com.alibaba.fastjson2.TypeReference<>() { };
        for (String[] boundary : boundaries) {
            List<Object> cases = new ArrayList<>();
            for (int codeUnit = 0; codeUnit <= Character.MAX_VALUE; codeUnit++) {
                char character = (char) codeUnit;
                if (!Character.isWhitespace(character) && !Character.isSpaceChar(character)
                    && !Character.isISOControl(character)) {
                    continue;
                }
                String input = boundary[1] + character + boundary[2];
                Map<String, Object> item = new TreeMap<>();
                item.put("codeUnit", codeUnit);
                try {
                    com.alibaba.fastjson2.JSON.parseObject(input, type);
                    item.put("accepted", true);
                } catch (RuntimeException rejected) {
                    item.put("accepted", false);
                }
                cases.add(item);
            }
            result.put(boundary[0], cases);
        }
        return result;
    }

    /** Direct Java semantic oracle for every BMP unit at six SOCKS JSON token boundaries. */
    private static Map<String, Object> socksBmpBoundaryObservations() {
        Map<String, Object> result = new TreeMap<>();
        String[][] boundaries = {
            {"root_after_open", "{", "\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}"},
            {"root_before_colon", "{\"10.0.0.0/8\"", ":{\"addr\":\"proxy:1080\"}}"},
            {"root_after_colon", "{\"10.0.0.0/8\":", "{\"addr\":\"proxy:1080\"}}"},
            {"proxy_after_open", "{\"10.0.0.0/8\":{", "\"addr\":\"proxy:1080\"}}"},
            {"proxy_before_close", "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"", "}}"},
            {"root_before_close", "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}", "}"}
        };
        com.alibaba.fastjson2.TypeReference<Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>> type =
            new com.alibaba.fastjson2.TypeReference<>() { };
        for (String[] boundary : boundaries) {
            Map<String, Map<String, Object>> grouped = new TreeMap<>();
            for (int codeUnit = 0; codeUnit <= Character.MAX_VALUE; codeUnit++) {
                String input = boundary[1] + (char) codeUnit + boundary[2];
                try {
                    Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig> parsed =
                        com.alibaba.fastjson2.JSON.parseObject(input, type);
                    Map<String, Object> observation = new TreeMap<>();
                    observation.put("isNull", parsed == null);
                    if (parsed == null) {
                        observation.put("entries", null);
                    } else {
                        List<String> keys = new ArrayList<>(parsed.keySet());
                        keys.sort((left, right) -> {
                            if (left == null) return right == null ? 0 : -1;
                            if (right == null) return 1;
                            return left.compareTo(right);
                        });
                        List<Object> entries = new ArrayList<>();
                        for (String key : keys) {
                            Map<String, Object> entry = new TreeMap<>();
                            entry.put("key", key);
                            org.apache.rocketmq.remoting.proxy.SocksProxyConfig route = parsed.get(key);
                            if (route == null) {
                                entry.put("route", null);
                            } else {
                                Map<String, Object> fields = new TreeMap<>();
                                fields.put("addr", route.getAddr());
                                fields.put("username", route.getUsername());
                                fields.put("password", route.getPassword());
                                entry.put("route", fields);
                            }
                            entries.add(entry);
                        }
                        observation.put("entries", entries);
                    }
                    String signature = com.alibaba.fastjson2.JSON.toJSONString(
                        observation, com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue);
                    Map<String, Object> group = grouped.computeIfAbsent(signature, ignored -> {
                        Map<String, Object> item = new TreeMap<>();
                        item.put("codeUnits", new ArrayList<Integer>());
                        item.put("observation", observation);
                        return item;
                    });
                    @SuppressWarnings("unchecked")
                    List<Integer> codeUnits = (List<Integer>) group.get("codeUnits");
                    codeUnits.add(codeUnit);
                } catch (RuntimeException rejected) {
                    // Rejections need no row; native checks all unlisted units reject.
                }
            }
            result.put(boundary[0], new ArrayList<>(grouped.values()));
        }
        return result;
    }

    private static List<Object> socksEscapeFollowerObservations() {
        Map<String, Map<String, Object>> grouped = new TreeMap<>();
        com.alibaba.fastjson2.TypeReference<Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>> type =
            new com.alibaba.fastjson2.TypeReference<>() { };
        String prefix = "{\"10.0.0.0/8\":{\"addr\":\"A" + (char) 92;
        String suffix = "Z\"}}";
        for (int codeUnit = 0; codeUnit <= Character.MAX_VALUE; codeUnit++) {
            String input = prefix + (char) codeUnit + suffix;
            try {
                Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig> parsed =
                    com.alibaba.fastjson2.JSON.parseObject(input, type);
                org.apache.rocketmq.remoting.proxy.SocksProxyConfig route = parsed.get("10.0.0.0/8");
                Map<String, Object> observation = new TreeMap<>();
                observation.put("addr", route.getAddr());
                observation.put("username", route.getUsername());
                observation.put("password", route.getPassword());
                String signature = com.alibaba.fastjson2.JSON.toJSONString(
                    observation, com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue);
                Map<String, Object> group = grouped.computeIfAbsent(signature, ignored -> {
                    Map<String, Object> item = new TreeMap<>();
                    item.put("codeUnits", new ArrayList<Integer>());
                    item.put("observation", observation);
                    return item;
                });
                @SuppressWarnings("unchecked")
                List<Integer> codeUnits = (List<Integer>) group.get("codeUnits");
                codeUnits.add(codeUnit);
            } catch (RuntimeException rejected) {
                // Unlisted UTF-16 units are rejected by Fastjson's char1 escape table.
            }
        }
        return new ArrayList<>(grouped.values());
    }

    private static Map<String, Object> socksSupplementaryEscapeFollowerCases() {
        Map<String, Object> result = new TreeMap<>();
        List<Object> accepted = new ArrayList<>();
        List<Object> acceptedRanges = new ArrayList<>();
        com.alibaba.fastjson2.TypeReference<Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>> type =
            new com.alibaba.fastjson2.TypeReference<>() { };
        String prefix = "{\"10.0.0.0/8\":{\"addr\":\"A" + (char) 92;
        String suffix = "Z\"}}";
        for (int high = Character.MIN_HIGH_SURROGATE; high <= Character.MAX_HIGH_SURROGATE; high++) {
            int codePoint = Character.toCodePoint((char) high, Character.MIN_LOW_SURROGATE);
            String input = prefix + new String(Character.toChars(codePoint)) + suffix;
            try {
                Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig> parsed =
                    com.alibaba.fastjson2.JSON.parseObject(input, type);
                org.apache.rocketmq.remoting.proxy.SocksProxyConfig route = parsed.get("10.0.0.0/8");
                Map<String, Object> item = new TreeMap<>();
                item.put("codePoint", codePoint);
                item.put("addr", replaceUnpairedSurrogates(route.getAddr()));
                accepted.add(item);
            } catch (RuntimeException rejected) {
                // Unlisted supplementary scalars are rejected by the char1 table.
            }
        }
        int rangeStart = -1;
        int rangeEnd = -1;
        String rangeAddr = null;
        int acceptedCodePointCount = 0;
        for (int codePoint = Character.MIN_SUPPLEMENTARY_CODE_POINT;
             codePoint <= Character.MAX_CODE_POINT; codePoint++) {
            String input = prefix + new String(Character.toChars(codePoint)) + suffix;
            try {
                Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig> parsed =
                    com.alibaba.fastjson2.JSON.parseObject(input, type);
                org.apache.rocketmq.remoting.proxy.SocksProxyConfig route = parsed.get("10.0.0.0/8");
                String addr = replaceUnpairedSurrogates(route.getAddr());
                acceptedCodePointCount++;
                if (rangeEnd + 1 == codePoint && addr.equals(rangeAddr)) {
                    rangeEnd = codePoint;
                } else {
                    addSupplementaryEscapeRange(acceptedRanges, rangeStart, rangeEnd, rangeAddr);
                    rangeStart = codePoint;
                    rangeEnd = codePoint;
                    rangeAddr = addr;
                }
            } catch (RuntimeException rejected) {
                // Accepted supplementary followers are recorded as compact scalar ranges.
                addSupplementaryEscapeRange(acceptedRanges, rangeStart, rangeEnd, rangeAddr);
                rangeStart = -1;
                rangeEnd = -1;
                rangeAddr = null;
            }
        }
        addSupplementaryEscapeRange(acceptedRanges, rangeStart, rangeEnd, rangeAddr);
        result.put("probedHighSurrogateCount", Character.MAX_HIGH_SURROGATE - Character.MIN_HIGH_SURROGATE + 1);
        result.put("accepted", accepted);
        result.put("probedCodePointCount",
            Character.MAX_CODE_POINT - Character.MIN_SUPPLEMENTARY_CODE_POINT + 1);
        result.put("acceptedCodePointCount", acceptedCodePointCount);
        result.put("acceptedRanges", acceptedRanges);
        return result;
    }

    private static void addSupplementaryEscapeRange(
        List<Object> ranges, int start, int end, String addr) {
        if (start < 0) return;
        Map<String, Object> range = new TreeMap<>();
        range.put("start", start);
        range.put("end", end);
        range.put("addr", addr);
        ranges.add(range);
    }

    private static String replaceUnpairedSurrogates(String value) {
        StringBuilder normalized = new StringBuilder(value.length());
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (Character.isHighSurrogate(current) && index + 1 < value.length()
                && Character.isLowSurrogate(value.charAt(index + 1))) {
                normalized.append(current).append(value.charAt(++index));
            } else if (Character.isSurrogate(current)) {
                normalized.append('\uFFFD');
            } else {
                normalized.append(current);
            }
        }
        return normalized.toString();
    }

    private static Map<String, Object> socksSubstituteEofObservations() {
        Map<String, Object> result = new TreeMap<>();
        String[][] boundaries = {
            {"root_after_open", "{", "\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}"},
            {"root_before_close", "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}", "}"}
        };
        com.alibaba.fastjson2.TypeReference<Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>> type =
            new com.alibaba.fastjson2.TypeReference<>() { };
        for (String[] boundary : boundaries) {
            Map<String, Object> item = new TreeMap<>();
            String input = boundary[1] + (char) 0x1a + boundary[2];
            try {
                Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig> parsed =
                    com.alibaba.fastjson2.JSON.parseObject(input, type);
                item.put("outcome", "OK");
                if (parsed == null) {
                    item.put("keys", null);
                } else {
                    List<String> keys = new ArrayList<>(parsed.keySet());
                    keys.sort(String::compareTo);
                    item.put("keys", keys);
                    org.apache.rocketmq.remoting.proxy.SocksProxyConfig route = parsed.get("10.0.0.0/8");
                    item.put("addr", route == null ? null : route.getAddr());
                }
            } catch (RuntimeException rejected) {
                item.put("outcome", rejected.getClass().getSimpleName());
            }
            result.put(boundary[0], item);
        }
        return result;
    }

    /** Direct Fastjson value-shape matrix for every SocksProxyConfig String field. */
    private static List<Object> socksProxyFieldShapeCases() {
        List<Object> cases = new ArrayList<>();
        String[][] fields = {{"addr", "\"proxy:1080\""}, {"username", "\"user\""},
            {"password", "\"secret\""}};
        String[][] shapes = {{"null", "null"}, {"numeric-string", "\"7\""}, {"integer", "0"},
            {"fraction", "1.5"}, {"boolean", "true"}, {"array", "[]"}, {"object", "{}"}};
        com.alibaba.fastjson2.TypeReference<Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>> type =
            new com.alibaba.fastjson2.TypeReference<>() { };
        for (String[] field : fields) {
            for (String[] shape : shapes) {
                StringBuilder route = new StringBuilder("{");
                for (int index = 0; index < fields.length; index++) {
                    if (index > 0) route.append(',');
                    String property = fields[index][0];
                    route.append('"').append(property).append("\":");
                    if (property.equals(field[0])) {
                        route.append(shape[1]);
                    } else {
                        route.append(fields[index][1]);
                    }
                }
                route.append('}');
                String input = "{\"10.0.0.0/8\":" + route + "}";
                Map<String, Object> item = new TreeMap<>();
                item.put("id", "shape-" + field[0] + "-" + shape[0]);
                item.put("input", input);
                try {
                    Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig> parsed =
                        com.alibaba.fastjson2.JSON.parseObject(input, type);
                    item.put("outcome", "OK");
                    org.apache.rocketmq.remoting.proxy.SocksProxyConfig value =
                        parsed == null ? null : parsed.get("10.0.0.0/8");
                    item.put("routePresent", value != null);
                    item.put("parsedAddr", value == null ? null : value.getAddr());
                    item.put("parsedUsername", value == null ? null : value.getUsername());
                    item.put("parsedPassword", value == null ? null : value.getPassword());
                } catch (RuntimeException rejected) {
                    item.put("outcome", rejected.getClass().getSimpleName());
                }
                cases.add(item);
            }
        }
        return cases;
    }

    private static List<Object> socksConstruction() throws Exception {
        List<Object> cases = new ArrayList<>();
        String[] inputs = {null, "", " ", "\t\r\n", "\f", "\b", "\u000b", "\u00a0", "\u2003", "null", "{}", "invalid",
            "{'10.0.0.0/8':{'addr':'proxy:1080'}}",
            "{10.0.0.0/8:{addr:'proxy:1080'}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",}}",
            "{/* comment */\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",\"extra\":7}}",
            "{\"10.0.0.0/8\":{}}", "{\"10.0.0.0/8\":null}",
            "{\"10.0.0.0/8\":{\"addr\":123}}",
            "{\"10.0.0.0/8\":{\"addr\":true}}",
            "{\"10.0.0.0/8\":{\"addr\":null}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",\"username\":123}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",\"password\":false}}",
            "{\"10.0.0.0/8\":{\"addr\":[]}}",
            "{\"10.0.0.0/8\":{addr:'proxy:1080',username:user,password:pass}}",
            "{\"10.0.0.0/8\":{\"addr\":0x2a}}",
            "{\"10.0.0.0/8\":{\"addr\":undefined}}",
            "{\"10.0.0.0/8\":{\"addr\":NaN}}",
            "{\"10.0.0.0/8\":{\"addr\":new Date(0)}}",
            "{\"10.0.0.0/8\":{\"addr\":\"first\",\"addr\":\"last\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"} // route comment\n}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy\\x3a1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":Infinity}}",
            "{\"10.0.0.0/8\":{\"addr\":-Infinity}}",
            "{\"10.0.0.0/8\":{addr:\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":{'addr':\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":+123}}",
            "{\"10.0.0.0/8\":{\"addr\":0123}}",
            "{\"10.0.0.0/8\":{\"addr\":0b10}}",
            "{\"10.0.0.0/8\":{\"addr\":+0123}}",
            "{\"10.0.0.0/8\":{\"addr\":-0123}}",
            "{\"10.0.0.0/8\":{\"addr\":0123.5}}",
            "{\"10.0.0.0/8\":{\"addr\":0o10}}",
            "{\"10.0.0.0/8\":{\"addr\":+0x10}}",
            "{\"10.0.0.0/8\":{\"addr\":[1,true,null,\"x\"]}}",
            "{\"10.0.0.0/8\":{\"addr\":{\"host\":\"proxy\",\"port\":1080}}}",
            "{\"10.0.0.0/8\":{\"addr\":{\"port\":1080,\"host\":\"proxy\"}}}",
            "{\"10.0.0.0/8\":{\"addr\":{\"quote\":\"a\\\"b\",\"newline\":\"a\\nb\",\"unicode\":\"λ\",\"slash\":\"/\"}}}",
            "{\"10.0.0.0/8\":{\"addr\":-.5}}",
            "{\"10.0.0.0/8\":{\"addr\":1e3}}",
            "{\"10.0.0.0/8\":{\"addr\":1E+3}}",
            "{\"10.0.0.0/8\":{\"addr\":1e-3}}",
            "{\"10.0.0.0/8\":{\"addr\":-0.0}}",
            "{\"10.0.0.0/8\":{\"addr\":1.234567890123456789}}",
            "{\"10.0.0.0/8\":{\"addr\":1e309}}",
            "{\"10.0.0.0/8\":{\"addr\":1e-1000}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy\\u003a1080\"}}",
            "{\"10.0.0.0/8\":{\"\\u0061ddr\":\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy\\/1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "x" + "\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "x1\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "xG0\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "x0G\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "xG1\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "u123\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "u12G4\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "u1G34\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "uG234\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "'single\"}}",
            "{\"10.0.0.0/8\":{'addr':'proxy" + ((char) 92) + "'single'}}",
            "{\"10.0.0.0/8\":{'addr':'proxy" + ((char) 92) + "\"double'}}",
            "{\"10.0.0.0/8\":{\"addr\":\"line" + ((char) 92) + "\nnext\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"line" + ((char) 92) + "\r\nnext\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"trailing" + ((char) 92),
            "{\"10.0.0.0/8\":{\"addr\":\"\\uD83D\\uDE80\"}}",
            "{\"10.0.0.0/8\":{\"addr\":1e-4}}",
            "{\"10.0.0.0/8\":{\"addr\":1e-7}}",
            "{\"10.0.0.0/8\":{\"addr\":1e6}}",
            "{\"10.0.0.0/8\":{\"addr\":1e7}}",
            "{\"10.0.0.0/8\":{\"addr\":1.25E+4}}",
            "{\"10.0.0.0/8\":{\"addr\":-0e2}}",
            "{\"10.0.0.0/8\":{\"addr\":1.2300}}",
            "{\"10.0.0.0/8\":{\"addr\":-.5e-3}}",
            "{\"10.0.0.0/8\":{\"addr\":1.e3}}",
            "{\"10.0.0.0/8\":{\"addr\":+.5e3}}",
            "{\"10.0.0.0/8\":{\"addr\":-1e309}}",
            "{\"10.0.0.0/8\":{\"addr\":.5e-3}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy\\v:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy\\0:1080\"}}",
            "\uFEFF{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}",
            "{\f\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}",
            "{\u2028\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}",
            "// leading comment\n{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":/* value comment */{\"addr\":\"proxy:1080\"}}",
            "{\u000b\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy\\q:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"raw\u0001control\"}}",
            "{} trailing",
            "{}/* trailing comment */",
            "{\"10.0.0.0/8\":{\"addr\":\"line\u2028separator\"}}",
            "/* \"\u2028 */{\"10.0.0.0/8\":{\"addr\":\"inside\u2028string\"}}",
            "{\"10.0.0.0/8\":// value comment\n{\"addr\":\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"http://proxy:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy/*literal*/:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}// trailing line comment\n}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}/* between route and root close */}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",/* field separator */\"username\":\"u\"}}",
            "{// comment with digits 123\r\"10.0.0.0/8\":{\"addr\":1e-4}}",
            "{// comment with digits 123\u2028\"10.0.0.0/8\":{\"addr\":1e-4}}",
            "{// comment with digits 123\u2029\"10.0.0.0/8\":{\"addr\":1e-4}}",
            "{\"10.0.0.0/8\":{\"username\":\"u\"// field comment\r,\"addr\":1e3}}",
            "{\"10.0.0.0/8\":{\"addr\":1D}}",
            "{\"10.0.0.0/8\":{\"addr\":1.25F}}",
            "{\"10.0.0.0/8\":{\"addr\":123L}}",
            "{\"10.0.0.0/8\":{\"addr\":1e7F}}",
            "{\"10.0.0.0/8\":{\"addr\":1e-4F}}",
            "{\"10.0.0.0/8\":{\"addr\":-0.0F}}",
            "{\"10.0.0.0/8\":{\"addr\":1e7D}}",
            "{\"10.0.0.0/8\":{\"addr\":-0.0D}}",
            "{\"10.0.0.0/8\":{\"addr\":-123L}}",
            "{\"10.0.0.0/8\":{\"addr\":9223372036854775807L}}",
            "{\"10.0.0.0/8\":{\"addr\":-9223372036854775808L}}",
            "{\"10.0.0.0/8\":{\"addr\":9223372036854775808L}}",
            "{\"10.0.0.0/8\":{\"addr\":\"" + ((char) 92) + "uD800\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"" + ((char) 92) + "uDC00\"}}"};
        List<String> allInputs = new ArrayList<>(java.util.Arrays.asList(inputs));
        for (int escaped = 0; escaped <= 0x7f; escaped++) {
            String input = "{\"10.0.0.0/8\":{\"addr\":\"A" + ((char) 92) + ((char) escaped) + "Z\"}}";
            allInputs.add(input);
        }
        for (int escaped = 0x80; escaped <= 0xff; escaped++) {
            String input = "{\"10.0.0.0/8\":{\"addr\":\"A" + ((char) 92) + ((char) escaped) + "Z\"}}";
            allInputs.add(input);
        }
        String slash = String.valueOf((char) 92);
        String[] unicodeEscapeInputs = {
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u1" + ((char) 0x03bb) + "34Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u12" + ((char) 0x03bb) + "4Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u12" + ((char) 0x0661) + "4Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u12" + ((char) 0xff11) + "4Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u1" + new String(Character.toChars(0x1d7da)) + "34Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "x" + ((char) 0x03bb) + "0Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "x0" + ((char) 0x0661) + "Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u12\t4Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u12\n4Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + new String(Character.toChars(0x1d7da)) + "Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u" + ((char) 0x03bb) + "234Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u123" + ((char) 0x03bb) + "Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u12" + new String(Character.toChars(0x1d7da)) + "4Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u123" + new String(Character.toChars(0x1d7da)) + "Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "uD83" + new String(Character.toChars(0x1d7da)) + "Z\"}}"
        };
        allInputs.addAll(java.util.Arrays.asList(unicodeEscapeInputs));
        allInputs.add("\u00a0{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{\u2003\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{\"10.0.0.0/8\":\u3000{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}\u2029}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}\uFEFF");
        allInputs.add("[]");
        allInputs.add("\"text\"");
        allInputs.add("true");
        allInputs.add("42");
        allInputs.add("[{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}]");
        allInputs.add("{\"10.0.0.0/8\":[]}");
        allInputs.add("{\"10.0.0.0/8\":\"proxy:1080\"}");
        allInputs.add("{\"10.0.0.0/8\":7}");
        allInputs.add("{\"10.0.0.0/8\":true}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"first\"},\"10.0.0.0/8\":{\"addr\":\"last\"}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":proxy}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":proxy:1080}}");
        allInputs.add("{\"10.0.0.0/8\":{addr:\"proxy:1080\",},}");
        allInputs.add("{'10.0.0.0/8':{'addr':'proxy:1080',},}");
        allInputs.add("/* lead */{\"10.0.0.0/8\":/* route */{\"addr\":\"proxy:1080\"}/* inner */}/* tail */");
        allInputs.add("// lead\r{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{\"10.0.0.0/8\":{\"username\":\"u\"// field\r,\"addr\":\"proxy:1080\"}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":[\"first\",\"last\",]}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",,}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}} // eof comment");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"first\",\"addr\":\"last\",\"username\":\"u1\",\"username\":\"u2\",\"password\":\"p1\",\"password\":\"p2\"}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",\"username\":\"first\",\"username\":null,\"password\":false}}");
        allInputs.add("{\"10.0.0.0/8\":{\"ADDR\":\"upper-proxy:1080\",\"USERNAME\":\"upper-user\",\"PASSWORD\":\"upper-password\"}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",\"userName\":\"camel-user\",\"password\":\"pw\"}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"} // eof with root open");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\" // eof with route open");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":[\"first\",\"last\" // eof with array open");
        allInputs.add("{0x2a:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{0b10:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{0o10:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{default:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{10:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{true:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{false:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{null:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1.5:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e2:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{+10:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{0123:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{123L:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1.25F:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1D:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e7F:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1:{\"addr\":\"first\"},01:{\"addr\":\"last\"}}");
        allInputs.add("{01:{\"addr\":\"first\"},1:{\"addr\":\"last\"}}");
        allInputs.add("{1:{\"addr\":\"first\"},01:{\"addr\":\"middle\"},1:{\"addr\":\"last\"}}");
        allInputs.add("{1:{\"addr\":\"first\"},01:{\"addr\":\"middle\"},01:{\"addr\":\"last\"}}");
        allInputs.add("{1:{\"addr\":\"first\"},\"1\":{\"addr\":\"last\"}}");
        allInputs.add("{\"1\":{\"addr\":\"first\"},1:{\"addr\":\"last\"}}");
        allInputs.add("{true:{\"addr\":\"first\"},\"true\":{\"addr\":\"last\"}}");
        allInputs.add("{\"true\":{\"addr\":\"first\"},true:{\"addr\":\"last\"}}");
        allInputs.add("{null:{\"addr\":\"null-key\"},'null':{\"addr\":\"string-key\"}}");
        allInputs.add("{-0x2a:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{+0b10:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-0o10:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{.5F:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-10:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-0:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-0.0:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-.5:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{+.5:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{.5:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e+2:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e-2:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1E7:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e-4F:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-10L:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{+10L:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1.0D:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e+7D:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{NaN:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{Infinity:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-Infinity:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{undefined:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{TRUE:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{False:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{NULL:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{Null:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{falsehood:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e309:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-1e309:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e-1000:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-1e-1000:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e-1000:{\"addr\":\"first\"},0.0:{\"addr\":\"last\"}}");
        allInputs.add("{0.0:{\"addr\":\"first\"},1e-1000:{\"addr\":\"last\"}}");
        allInputs.add("{1e309:{\"addr\":\"first\"},'Infinity':{\"addr\":\"last\"}}");
        allInputs.add("{-0e-1000:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-1e-1000D:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-1e-1000F:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-1e-1000D:{\"addr\":\"first\"},0.0:{\"addr\":\"last\"}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",\"username\":\"user\",\"password\":\"secret\"},\"192.168.0.0/16\":{\"$ref\":\"$['10.0.0.0/8']\"}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",\"username\":\"user\"},\"192.168.0.0/16\":{\"$ref\":\"$['10.0.0.0/8']\"},\"172.16.0.0/12\":{\"$ref\":\"$['192.168.0.0/16']\"}}");
        allInputs.add("{\"192.168.0.0/16\":{\"$ref\":\"$['missing']\"}}");
        allInputs.add("{\"192.168.0.0/16\":{\"$ref\":\"$['192.168.0.0/16']\"}}");
        allInputs.add("{\"192.168.0.0/16\":{\"$ref\":\"$['172.16.0.0/12']\"},\"172.16.0.0/12\":{\"$ref\":\"$['192.168.0.0/16']\"}}");
        allInputs.add("{\"192.168.0.0/16\":{\"$ref\":\"#-1\"}}");
        allInputs.add("{\"192.168.0.0/16\":{\"$ref\":\"$\"}}");
        for (int i = 0; i < allInputs.size(); i++) {
            String input = allInputs.get(i);
            ClientConfig config = new ClientConfig();
            config.setClientIP("socks-construction-" + i);
            config.setNamesrvAddr("");
            config.setSocksProxyConfig(input);
            Map<String, Object> item = new TreeMap<>();
            item.put("input", input);
            org.apache.rocketmq.client.impl.factory.MQClientInstance runtime = null;
            try {
                runtime = new org.apache.rocketmq.client.impl.factory.MQClientInstance(config, 0, config.buildMQClientId());
                item.put("outcome", "OK");
                item.put("rawPreserved", java.util.Objects.equals(input, runtime.getNettyClientConfig().getSocksProxyConfig()));
                if (i >= 19 && "OK".equals(item.get("outcome"))) {
                    java.lang.reflect.Field remotingField = org.apache.rocketmq.client.impl.MQClientAPIImpl.class.getDeclaredField("remotingClient");
                    remotingField.setAccessible(true);
                    Object remoting = remotingField.get(runtime.getMQClientAPIImpl());
                    java.lang.reflect.Field proxyField = org.apache.rocketmq.remoting.netty.NettyRemotingClient.class.getDeclaredField("proxyMap");
                    proxyField.setAccessible(true);
                    @SuppressWarnings("unchecked")
                    Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig> routes =
                        (Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>) proxyField.get(remoting);
                    if (i >= allInputs.size() - 60) {
                        List<String> routeKeys = new ArrayList<>(routes.keySet());
                        routeKeys.sort(java.util.Comparator.nullsFirst(java.util.Comparator.naturalOrder()));
                        item.put("routeKeys", routeKeys);
                    }
                    if (routes.containsKey("1")) {
                        item.put("routeKeyOneAddr", routes.get("1").getAddr());
                    }
                    if (routes.containsKey("true")) {
                        item.put("routeKeyTrueAddr", routes.get("true").getAddr());
                    }
                    if (routes.containsKey(null)) {
                        item.put("routeKeyNullAddr", routes.get(null).getAddr());
                    }
                    if (routes.containsKey("null")) {
                        item.put("routeKeyStringNullAddr", routes.get("null").getAddr());
                    }
                    org.apache.rocketmq.remoting.proxy.SocksProxyConfig route = routes.get("10.0.0.0/8");
                    item.put("routePresent", route != null);
                    String parsedAddr = route == null ? null : route.getAddr();
                    if (hasUnpairedSurrogate(parsedAddr)) {
                        List<Integer> codeUnits = new ArrayList<>();
                        for (char codeUnit : parsedAddr.toCharArray()) codeUnits.add((int) codeUnit);
                        item.put("parsedAddrUtf16", codeUnits);
                    } else {
                        item.put("parsedAddr", parsedAddr);
                    }
                    item.put("parsedUsername", route == null ? null : route.getUsername());
                    item.put("parsedPassword", route == null ? null : route.getPassword());
                    if (input.contains("192.168.0.0/16")) {
                        org.apache.rocketmq.remoting.proxy.SocksProxyConfig referenced = routes.get("192.168.0.0/16");
                        item.put("referencedRouteAddr", referenced == null ? null : referenced.getAddr());
                        item.put("referencedRouteUsername", referenced == null ? null : referenced.getUsername());
                        item.put("referencedRoutePassword", referenced == null ? null : referenced.getPassword());
                        item.put("referenceSameObject", route != null && route == referenced);
                    }
                    if (input.contains("172.16.0.0/12")) {
                        org.apache.rocketmq.remoting.proxy.SocksProxyConfig chained = routes.get("172.16.0.0/12");
                        item.put("chainedReferenceAddr", chained == null ? null : chained.getAddr());
                        item.put("chainedReferenceUsername", chained == null ? null : chained.getUsername());
                        item.put("chainedReferencePassword", chained == null ? null : chained.getPassword());
                        item.put("chainedReferenceSameObject", route != null && route == chained);
                    }
                }
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
            } finally {
                if (runtime != null) cleanupRuntime(runtime);
            }
            cases.add(item);
        }
        return cases;
    }

    private static boolean hasUnpairedSurrogate(String value) {
        if (value == null) return false;
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (Character.isHighSurrogate(current)) {
                if (i + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(i + 1))) return true;
                i++;
            } else if (Character.isLowSurrogate(current)) {
                return true;
            }
        }
        return false;
    }

    private static List<Object> socksCidr() {
        List<Object> cases = new ArrayList<>();
        String[][] inputs = {
            {"10.20.4.5", "10.20.0.0/16"}, {"10.21.4.5", "10.20.0.0/16"},
            {"10.20.4.5", "10.20.0.0/0"}, {"10.20.4.5", "10.20.0.0/33"},
            {"10.20.4.5", "10.20.0.0/-1"}, {"10.20.4.5", "266.20.0.0/16"},
            {"10.20.4.5", "10.20.0.0/16/ignored"}, {"10.20.4.5.99", "10.20.0.0/16"},
            {"10.20.4.5.", "10.20.0.0/16"}, {"10.20.4.", "10.20.0.0/16"},
            {"10.20.4.5", "10.20.0.0/not-a-prefix"}, {"10.20.4.5", "10.20.0.0"}
        };
        for (String[] input : inputs) {
            Map<String, Object> item = new TreeMap<>();
            item.put("address", input[0]);
            item.put("cidr", input[1]);
            try {
                item.put("outcome", "OK");
                item.put("match", org.apache.rocketmq.remoting.common.RemotingHelper.ipInCIDR(input[0], input[1]));
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> socksConnection(boolean usePasswordAuth) throws Exception {
        final String username = "oracle-user";
        final String password = "oracle-pass";
        Map<String, Object> result = new TreeMap<>();
        try (java.net.ServerSocket brokerListener =
                 new java.net.ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"));
             java.net.ServerSocket proxyListener =
                 new java.net.ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"))) {
            brokerListener.setSoTimeout(8000);
            proxyListener.setSoTimeout(8000);
            java.util.concurrent.ExecutorService workers =
                java.util.concurrent.Executors.newCachedThreadPool();
            try {
            final int brokerPort = brokerListener.getLocalPort();
            final int proxyPort = proxyListener.getLocalPort();
            java.util.concurrent.Future<Map<String, Object>> brokerOutcome = workers.submit(() -> {
                try (java.net.Socket broker = brokerListener.accept()) {
                    broker.setSoTimeout(8000);
                    byte[] frame = readFrame(broker.getInputStream());
                    org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                        org.apache.rocketmq.remoting.protocol.RemotingCommand.decode(
                            java.util.Arrays.copyOfRange(frame, 4, frame.length));
                    if (request.getCode() != 1234 || !java.util.Arrays.equals(request.getBody(),
                        "socks-oracle-request".getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
                        throw new IllegalStateException("unexpected Java SOCKS remoting request");
                    }
                    org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                        org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(0, "socks-oracle-ok");
                    response.setOpaque(request.getOpaque());
                    response.setSerializeTypeCurrentRPC(request.getSerializeTypeCurrentRPC());
                    java.nio.ByteBuffer encoded = response.encode();
                    byte[] bytes = new byte[encoded.remaining()];
                    encoded.get(bytes);
                    broker.getOutputStream().write(bytes);
                    broker.getOutputStream().flush();
                    Map<String, Object> observed = new TreeMap<>();
                    observed.put("requestCode", request.getCode());
                    observed.put("requestBodyBytes", request.getBody() == null ? 0 : request.getBody().length);
                    observed.put("opaqueMatched", request.getOpaque() == response.getOpaque());
                    return observed;
                }
            });
            java.util.concurrent.Future<Map<String, Object>> proxyOutcome = workers.submit(() -> {
                try (java.net.Socket client = proxyListener.accept()) {
                    client.setSoTimeout(8000);
                    java.io.InputStream clientInput = client.getInputStream();
                    java.io.OutputStream clientOutput = client.getOutputStream();
                    int version = clientInput.read();
                    int methodCount = clientInput.read();
                    byte[] methods = readExactly(clientInput, methodCount);
                    boolean offeredPasswordAuth = false;
                    boolean offeredNoAuth = false;
                    for (byte method : methods) {
                        offeredPasswordAuth |= method == 2;
                        offeredNoAuth |= method == 0;
                    }
                    if (version != 5 || (usePasswordAuth && !offeredPasswordAuth)
                        || (!usePasswordAuth && !offeredNoAuth)) {
                        throw new java.io.IOException("required SOCKS5 authentication method was not offered");
                    }
                    int authVersion = -1;
                    String receivedUsername = null;
                    String receivedPassword = null;
                    if (usePasswordAuth) {
                        clientOutput.write(new byte[] {5, 2});
                        clientOutput.flush();
                        authVersion = clientInput.read();
                        int usernameLength = clientInput.read();
                        receivedUsername = new String(readExactly(clientInput, usernameLength),
                            java.nio.charset.StandardCharsets.UTF_8);
                        int passwordLength = clientInput.read();
                        receivedPassword = new String(readExactly(clientInput, passwordLength),
                            java.nio.charset.StandardCharsets.UTF_8);
                        if (authVersion != 1 || !username.equals(receivedUsername)
                            || !password.equals(receivedPassword)) {
                            throw new java.io.IOException("SOCKS5 username/password did not match");
                        }
                        clientOutput.write(new byte[] {1, 0});
                    } else {
                        clientOutput.write(new byte[] {5, 0});
                    }
                    clientOutput.flush();

                    byte[] requestPrefix = readExactly(clientInput, 4);
                    int addressType = requestPrefix[3] & 0xff;
                    String targetHost;
                    if (addressType == 1) {
                        targetHost = java.net.InetAddress.getByAddress(readExactly(clientInput, 4)).getHostAddress();
                    } else if (addressType == 3) {
                        targetHost = new String(readExactly(clientInput, clientInput.read()),
                            java.nio.charset.StandardCharsets.UTF_8);
                    } else if (addressType == 4) {
                        targetHost = java.net.InetAddress.getByAddress(readExactly(clientInput, 16)).getHostAddress();
                    } else {
                        throw new java.io.IOException("unsupported SOCKS5 address type " + addressType);
                    }
                    int targetPort = (clientInput.read() << 8) | clientInput.read();
                    if ((requestPrefix[0] & 0xff) != 5 || (requestPrefix[1] & 0xff) != 1) {
                        throw new java.io.IOException("unexpected SOCKS5 connect request");
                    }
                    if (!"127.0.0.1".equals(targetHost) || targetPort != brokerPort) {
                        throw new java.io.IOException("SOCKS5 connect target did not match local Broker");
                    }

                    try (java.net.Socket upstream = new java.net.Socket(
                        "127.0.0.1", brokerPort)) {
                        upstream.setSoTimeout(8000);
                        clientOutput.write(new byte[] {5, 0, 0, 1, 127, 0, 0, 1, 0, 0});
                        clientOutput.flush();
                        java.util.concurrent.Future<Long> upload = workers.submit(() ->
                            transfer(clientInput, upstream.getOutputStream()));
                        long downloaded = transfer(upstream.getInputStream(), clientOutput);
                        long uploaded = upload.get(5, java.util.concurrent.TimeUnit.SECONDS);
                        Map<String, Object> observed = new TreeMap<>();
                        observed.put("authAccepted", usePasswordAuth
                            ? authVersion == 1 && username.equals(receivedUsername)
                                && password.equals(receivedPassword)
                            : offeredNoAuth);
                        observed.put("authenticationMode", usePasswordAuth ? "USERNAME_PASSWORD" : "NO_AUTH");
                        observed.put("connectTargetHost", targetHost);
                        observed.put("connectTargetMatched", "127.0.0.1".equals(targetHost)
                            && targetPort == brokerPort);
                        observed.put("requestBytesForwarded", uploaded > 0);
                        observed.put("responseBytesForwarded", downloaded > 0);
                        if (uploaded == 0 || downloaded == 0) {
                            throw new java.io.IOException("SOCKS5 tunnel did not forward both directions");
                        }
                        return observed;
                    }
                }
            });

            org.apache.rocketmq.remoting.netty.NettyClientConfig config =
                new org.apache.rocketmq.remoting.netty.NettyClientConfig();
            config.setUseTLS(false);
            config.setClientSocketSndBufSize(64 * 1024);
            config.setClientSocketRcvBufSize(64 * 1024);
            String proxy = "{\"addr\":\"127.0.0.1:" + proxyPort + "\""
                + (usePasswordAuth ? ",\"username\":\"" + username + "\",\"password\":\""
                    + password + "\"" : "") + "}";
            config.setSocksProxyConfig("{\"127.0.0.0/8\":" + proxy + "}");
            org.apache.rocketmq.remoting.netty.NettyRemotingClient client =
                new org.apache.rocketmq.remoting.netty.NettyRemotingClient(config);
            try {
                client.start();
                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                    org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(1234, null);
                request.setBody("socks-oracle-request".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                    client.invokeSync("127.0.0.1:" + brokerPort, request, 7000);
                if (response == null || response.getCode() != 0
                    || !"socks-oracle-ok".equals(response.getRemark())) {
                    throw new IllegalStateException("Java SOCKS remoting response did not match");
                }
                if (response.getOpaque() != request.getOpaque()) {
                    throw new IllegalStateException("Java SOCKS remoting response opaque did not match request");
                }
                result.put("outcome", "OK");
                result.put("responseCode", response.getCode());
                result.put("responseRemark", response.getRemark());
                result.put("responseOpaqueMatched", true);
            } catch (Exception error) {
                result.put("clientError", error.getClass().getSimpleName() + ": " + error.getMessage());
                try {
                    result.put("proxyDiagnostic", proxyOutcome.get(1, java.util.concurrent.TimeUnit.SECONDS));
                } catch (Exception proxyError) {
                    result.put("proxyDiagnostic", proxyError.getClass().getSimpleName() + ": " + proxyError.getMessage());
                }
                try {
                    result.put("brokerDiagnostic", brokerOutcome.get(1, java.util.concurrent.TimeUnit.SECONDS));
                } catch (Exception brokerError) {
                    result.put("brokerDiagnostic", brokerError.getClass().getSimpleName() + ": " + brokerError.getMessage());
                }
                System.err.println("SOCKS connection fixture diagnostics: " + result);
                throw error;
            } finally {
                client.shutdown();
            }
            result.putAll(proxyOutcome.get(8, java.util.concurrent.TimeUnit.SECONDS));
            result.putAll(brokerOutcome.get(8, java.util.concurrent.TimeUnit.SECONDS));
            } finally {
                workers.shutdownNow();
                workers.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
            }
        }
        return result;
    }

    private static byte[] readFrame(java.io.InputStream input) throws java.io.IOException {
        byte[] lengthBytes = readExactly(input, 4);
        int length = java.nio.ByteBuffer.wrap(lengthBytes).getInt();
        if (length < 4 || length > 1024 * 1024) {
            throw new java.io.IOException("invalid remoting frame length " + length);
        }
        byte[] frame = new byte[4 + length];
        System.arraycopy(lengthBytes, 0, frame, 0, 4);
        System.arraycopy(readExactly(input, length), 0, frame, 4, length);
        return frame;
    }

    private static byte[] readExactly(java.io.InputStream input, int length) throws java.io.IOException {
        if (length < 0 || length > 65535) throw new java.io.IOException("invalid read length " + length);
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) throw new java.io.EOFException("truncated stream input");
        return bytes;
    }

    private static long transfer(java.io.InputStream input, java.io.OutputStream output) throws java.io.IOException {
        byte[] buffer = new byte[8192];
        long count = 0;
        int read;
        while ((read = input.read(buffer)) >= 0) {
            output.write(buffer, 0, read);
            count += read;
        }
        output.flush();
        return count;
    }

    private static List<Object> socksProxySelection() throws Exception {
        List<Object> cases = new ArrayList<>();
        String[][] inputs = {
            {"{\"10.0.0.0/8\":{\"addr\":\"proxy-a:1080\"}}", "10.20.1.2:9876", "10.20.1.2"},
            {"{\"10.0.0.0/8\":{\"addr\":\"proxy-a:1080\"}}", "192.0.2.3:9876", "192.0.2.3"},
            {"{\"0.0.0.0/0\":{\"addr\":\"proxy-default:1080\"}}", "broker.example:9876", "broker.example"},
            {"{\"0.0.0.0/0\":{\"addr\":\"proxy-default:1080\"}}", "[2001:db8::1]:9876", "[2001:db8::1]"},
            {"{\"10.20.0.0/33\":{\"addr\":\"proxy-shift:1080\"}}", "10.20.4.5:9876", "10.20.4.5"},
            {"{\"10.0.0.0/8\":null}", "10.20.1.2:9876", "10.20.1.2"},
            {"{\"10.0.0.0/8\":{}}", "10.20.1.2:9876", "10.20.1.2"},
            {"{\"not-a-cidr\":{\"addr\":\"proxy-invalid:1080\"}}", "10.20.1.2:9876", "10.20.1.2"},
            {"{}", "192.0.2.3:9876", "192.0.2.3"},
            {"{\"10.0.0.0/8\":{\"addr\":\"proxy-reference:1080\"},\"192.168.0.0/16\":{\"$ref\":\"$['10.0.0.0/8']\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"192.168.0.0/16\":{\"$ref\":\"$['missing']\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"192.168.0.0/16\":{\"$ref\":\"$['192.168.0.0/16']\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"192.168.0.0/16\":{\"$ref\":\"$['172.16.0.0/12']\"},\"172.16.0.0/12\":{\"$ref\":\"$['192.168.0.0/16']\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"192.168.0.0/16\":{\"$ref\":\"#-1\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"192.168.0.0/16\":{\"$ref\":\"$\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"target\":{\"addr\":\"proxy-dot:1080\"},\"192.168.0.0/16\":{\"$ref\":\"$.target\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"target\":{\"addr\":\"proxy-double-quote:1080\"},\"192.168.0.0/16\":{\"$ref\":\"$[\\\"target\\\"]\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"target\":{\"addr\":\"proxy-spaces:1080\"},\"192.168.0.0/16\":{\"$ref\":\"$[ 'target' ]\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"0\":{\"addr\":\"proxy-index:1080\"},\"192.168.0.0/16\":{\"$ref\":\"$[0]\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"192.168.0.0/16\":{\"$ref\":\"@\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"192.168.0.0/16\":{\"$ref\":\"..\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"192.168.0.0/16\":{\"$ref\":\"#-2\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"target\":{\"addr\":\"proxy-plain:1080\"},\"192.168.0.0/16\":{\"$ref\":\"target\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"192.168.0.0/16\":{\"$ref\":\"\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"192.168.0.0/16\":{\"$ref\":\" \"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"target\":{\"addr\":\"proxy-nested:1080\"},\"192.168.0.0/16\":{\"$ref\":\"target.addr\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"192.168.0.0/16\":{\"$ref\":\"0\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"target\":{\"addr\":\"proxy-recursive:1080\"},\"192.168.0.0/16\":{\"$ref\":\"$..target\"}}", "192.168.1.9:9876", "192.168.1.9"},
            {"{\"10.0.0.0/8\":{\"addr\":\"proxy-8\"},\"10.0.0.0/9\":{\"addr\":\"proxy-9\"},\"10.0.0.0/10\":{\"addr\":\"proxy-10\"},\"10.0.0.0/11\":{\"addr\":\"proxy-11\"}}", "10.20.1.2:9876", "10.20.1.2"},
            {"{\"10.0.0.0/11\":{\"addr\":\"proxy-11\"},\"10.0.0.0/10\":{\"addr\":\"proxy-10\"},\"10.0.0.0/9\":{\"addr\":\"proxy-9\"},\"10.0.0.0/8\":{\"addr\":\"proxy-8\"}}", "10.20.1.2:9876", "10.20.1.2"},
            {"{\"10.0.0.0/10\":{\"addr\":\"proxy-10\"},\"10.0.0.0/8\":{\"addr\":\"proxy-8\"},\"10.0.0.0/11\":{\"addr\":\"proxy-11\"},\"10.0.0.0/9\":{\"addr\":\"proxy-9\"}}", "10.20.1.2:9876", "10.20.1.2"},
            {"{\"10.0.00.0/8\":{\"addr\":\"source-bucket-9\"},\"010.0.0.0/8\":{\"addr\":\"source-bucket-1\"}}", "10.20.1.2:9876", "10.20.1.2"}
        };
        for (int i = 0; i < inputs.length; i++) {
            ClientConfig config = new ClientConfig();
            config.setClientIP("socks-selection-" + i);
            config.setNamesrvAddr("");
            config.setSocksProxyConfig(inputs[i][0]);
            Map<String, Object> item = new TreeMap<>();
            item.put("config", inputs[i][0]);
            item.put("address", inputs[i][1]);
            item.put("host", inputs[i][2]);
            org.apache.rocketmq.client.impl.factory.MQClientInstance runtime = null;
            try {
                runtime = new org.apache.rocketmq.client.impl.factory.MQClientInstance(
                    config, 0, config.buildMQClientId());
                java.lang.reflect.Field remotingField =
                    org.apache.rocketmq.client.impl.MQClientAPIImpl.class.getDeclaredField("remotingClient");
                remotingField.setAccessible(true);
                Object remoting = remotingField.get(runtime.getMQClientAPIImpl());
                java.lang.reflect.Method select = remoting.getClass().getDeclaredMethod("getProxy", String.class);
                select.setAccessible(true);
                @SuppressWarnings("unchecked")
                Map.Entry<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig> selected =
                    (Map.Entry<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>) select.invoke(remoting, inputs[i][1]);
                item.put("outcome", "OK");
                item.put("proxyCidr", selected == null ? null : selected.getKey());
                item.put("proxyAddress", selected == null || selected.getValue() == null
                    ? null : selected.getValue().getAddr());
            } catch (java.lang.reflect.InvocationTargetException error) {
                item.put("outcome", error.getCause().getClass().getSimpleName());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
            } finally {
                if (runtime != null) cleanupRuntime(runtime);
            }
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> nullableSettings() throws Exception {
        ClientConfig input = new ClientConfig();
        input.setClientIP("nullable-settings");
        input.setNamesrvAddr("");
        input.setAccessChannel(null);
        input.setSocksProxyConfig(null);
        input.setLanguage(null);
        Map<String, Object> result = new TreeMap<>();
        result.put("inputAccessNull", input.getAccessChannel() == null);
        result.put("inputSocksNull", input.getSocksProxyConfig() == null);
        result.put("inputLanguageNull", input.getLanguage() == null);
        ClientConfig copy = input.cloneClientConfig();
        result.put("cloneAccess", copy.getAccessChannel().toString());
        result.put("cloneSocksNull", copy.getSocksProxyConfig() == null);
        result.put("cloneLanguageNull", copy.getLanguage() == null);
        ClientConfig receiver = new ClientConfig();
        receiver.setAccessChannel(org.apache.rocketmq.client.AccessChannel.CLOUD);
        receiver.resetClientConfig(input);
        result.put("resetAccess", receiver.getAccessChannel().toString());
        result.put("resetSocksNull", receiver.getSocksProxyConfig() == null);
        result.put("resetLanguageNull", receiver.getLanguage() == null);
        org.apache.rocketmq.client.impl.factory.MQClientInstance runtime =
            new org.apache.rocketmq.client.impl.factory.MQClientInstance(copy, 0, copy.buildMQClientId());
        try {
            result.put("runtimeConstructed", true);
            result.put("runtimeSocksNull", runtime.getNettyClientConfig().getSocksProxyConfig() == null);
            result.put("runtimeLanguageNull", runtime.getClientConfig().getLanguage() == null);
        } finally { cleanupRuntime(runtime); }
        for (org.apache.rocketmq.remoting.protocol.SerializeType type : org.apache.rocketmq.remoting.protocol.SerializeType.values()) {
            org.apache.rocketmq.remoting.protocol.RemotingCommand command =
                org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(34, null);
            command.setLanguage(null);
            command.setSerializeTypeCurrentRPC(type);
            Map<String, Object> outcome = new TreeMap<>();
            try {
                java.nio.ByteBuffer bytes = command.encode();
                outcome.put("outcome", "OK");
                bytes.getInt();
                int headerLength = bytes.getInt() & 0x00ffffff;
                byte[] header = new byte[headerLength]; bytes.get(header);
                if (type == org.apache.rocketmq.remoting.protocol.SerializeType.JSON) {
                    com.alibaba.fastjson2.JSONObject json = com.alibaba.fastjson2.JSON.parseObject(new String(header, java.nio.charset.StandardCharsets.UTF_8));
                    outcome.put("languageKeyPresent", json.containsKey("language"));
                    outcome.put("decodedLanguage", org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(header,
                        org.apache.rocketmq.remoting.protocol.RemotingCommand.class).getLanguage().toString());
                    byte[] explicitNull = "{\"code\":34,\"language\":null}"
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    outcome.put("explicitNullDecodedLanguageNull",
                        org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(explicitNull,
                            org.apache.rocketmq.remoting.protocol.RemotingCommand.class).getLanguage() == null);
                    byte[] unknownName = "{\"code\":34,\"language\":\"not-a-language\"}"
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    org.apache.rocketmq.remoting.protocol.RemotingCommand unknownDecoded =
                        org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(unknownName,
                            org.apache.rocketmq.remoting.protocol.RemotingCommand.class);
                    outcome.put("unknownLanguageDecodedNull", unknownDecoded.getLanguage() == null);
                }
            } catch (RuntimeException error) { outcome.put("outcome", error.getClass().getSimpleName()); }
            result.put(type.name(), outcome);
        }
        return result;
    }

    private static List<Object> languageCodeMethodCases() {
        List<Object> cases = new ArrayList<>();
        org.apache.rocketmq.remoting.protocol.LanguageCode[] values =
            org.apache.rocketmq.remoting.protocol.LanguageCode.values();
        List<String> names = new ArrayList<>();
        for (org.apache.rocketmq.remoting.protocol.LanguageCode value : values) {
            names.add(value.name());
        }
        for (org.apache.rocketmq.remoting.protocol.LanguageCode value : values) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "constant");
            item.put("name", value.name());
            item.put("ordinal", value.ordinal());
            item.put("text", value.toString());
            item.put("values", names);
            item.put("valueOfName",
                org.apache.rocketmq.remoting.protocol.LanguageCode.valueOf(value.name()).name());
            item.put("compareNext", value.compareTo(values[(value.ordinal() + 1) % values.length]));
            org.apache.rocketmq.remoting.protocol.LanguageCode[] mutated =
                org.apache.rocketmq.remoting.protocol.LanguageCode.values();
            mutated[0] = org.apache.rocketmq.remoting.protocol.LanguageCode.NODE_JS;
            item.put("valuesDefensiveCopy",
                org.apache.rocketmq.remoting.protocol.LanguageCode.values()[0]
                    == org.apache.rocketmq.remoting.protocol.LanguageCode.JAVA);
            item.put("code", value.getCode());
            org.apache.rocketmq.remoting.protocol.LanguageCode fromByte =
                org.apache.rocketmq.remoting.protocol.LanguageCode.valueOf(value.getCode());
            org.apache.rocketmq.remoting.protocol.LanguageCode fromName =
                org.apache.rocketmq.remoting.protocol.LanguageCode.getCode(value.name());
            item.put("valueOfByte", fromByte == null ? null : fromByte.name());
            item.put("getCodeByName", fromName == null ? null : fromName.name());
            cases.add(item);
        }
        for (byte input : new byte[] {0, 13, 14, 127, -1, -128}) {
            org.apache.rocketmq.remoting.protocol.LanguageCode decoded =
                org.apache.rocketmq.remoting.protocol.LanguageCode.valueOf(input);
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "byte");
            item.put("input", input);
            item.put("decoded", decoded == null ? null : decoded.name());
            cases.add(item);
        }
        for (String input : new String[] {null, "", "JAVA", "java", "NODE_JS", "UNKNOWN"}) {
            org.apache.rocketmq.remoting.protocol.LanguageCode decoded =
                org.apache.rocketmq.remoting.protocol.LanguageCode.getCode(input);
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "name");
            item.put("input", input);
            item.put("decoded", decoded == null ? null : decoded.name());
            cases.add(item);
        }
        for (String input : new String[] {null, "", "JAVA", "java", "NODE_JS", "UNKNOWN"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "enumNameLookup");
            item.put("input", input);
            try {
                item.put("outcome", "OK");
                item.put("decoded",
                    org.apache.rocketmq.remoting.protocol.LanguageCode.valueOf(input).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> serializeTypeMethodCases() {
        List<Object> cases = new ArrayList<>();
        org.apache.rocketmq.remoting.protocol.SerializeType[] values =
            org.apache.rocketmq.remoting.protocol.SerializeType.values();
        List<String> names = new ArrayList<>();
        for (org.apache.rocketmq.remoting.protocol.SerializeType value : values) {
            names.add(value.name());
        }
        for (org.apache.rocketmq.remoting.protocol.SerializeType value : values) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "constant");
            item.put("name", value.name());
            item.put("code", value.getCode());
            item.put("ordinal", value.ordinal());
            item.put("text", value.toString());
            item.put("values", names);
            item.put("valueOfName",
                org.apache.rocketmq.remoting.protocol.SerializeType.valueOf(value.name()).name());
            org.apache.rocketmq.remoting.protocol.SerializeType fromByte =
                org.apache.rocketmq.remoting.protocol.SerializeType.valueOf(value.getCode());
            item.put("valueOfByte", fromByte == null ? null : fromByte.name());
            item.put("compareNext", value.compareTo(values[(value.ordinal() + 1) % values.length]));
            org.apache.rocketmq.remoting.protocol.SerializeType[] mutated =
                org.apache.rocketmq.remoting.protocol.SerializeType.values();
            mutated[0] = org.apache.rocketmq.remoting.protocol.SerializeType.ROCKETMQ;
            item.put("valuesDefensiveCopy",
                org.apache.rocketmq.remoting.protocol.SerializeType.values()[0]
                    == org.apache.rocketmq.remoting.protocol.SerializeType.JSON);
            cases.add(item);
        }
        for (byte input : new byte[] {0, 1, 2, 127, -1, -128}) {
            org.apache.rocketmq.remoting.protocol.SerializeType decoded =
                org.apache.rocketmq.remoting.protocol.SerializeType.valueOf(input);
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "byte");
            item.put("input", input);
            item.put("decoded", decoded == null ? null : decoded.name());
            cases.add(item);
        }
        for (String input : new String[] {null, "", "JSON", "json", "ROCKETMQ", "UNKNOWN"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "enumNameLookup");
            item.put("input", input);
            try {
                item.put("outcome", "OK");
                item.put("decoded",
                    org.apache.rocketmq.remoting.protocol.SerializeType.valueOf(input).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> compressionTypeMethodCases() {
        List<Object> cases = new ArrayList<>();
        org.apache.rocketmq.common.compression.CompressionType[] values =
            org.apache.rocketmq.common.compression.CompressionType.values();
        List<String> names = new ArrayList<>();
        for (org.apache.rocketmq.common.compression.CompressionType value : values) {
            names.add(value.name());
        }
        for (org.apache.rocketmq.common.compression.CompressionType value : values) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "constant");
            item.put("name", value.name());
            item.put("ordinal", value.ordinal());
            item.put("text", value.toString());
            item.put("values", names);
            item.put("value", value.getValue());
            item.put("compressionFlag", value.getCompressionFlag());
            item.put("valueOfName",
                org.apache.rocketmq.common.compression.CompressionType.valueOf(value.name()).name());
            item.put("compareNext", value.compareTo(values[(value.ordinal() + 1) % values.length]));
            org.apache.rocketmq.common.compression.CompressionType[] mutated =
                org.apache.rocketmq.common.compression.CompressionType.values();
            mutated[0] = org.apache.rocketmq.common.compression.CompressionType.ZLIB;
            item.put("valuesDefensiveCopy",
                org.apache.rocketmq.common.compression.CompressionType.values()[0]
                    == org.apache.rocketmq.common.compression.CompressionType.LZ4);
            cases.add(item);
        }
        for (String input : new String[] {null, "", "LZ4", "lz4", "ZSTD", "ZLIB", "UNKNOWN"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "enumNameLookup");
            item.put("input", input);
            try {
                item.put("outcome", "OK");
                item.put("decoded",
                    org.apache.rocketmq.common.compression.CompressionType.valueOf(input).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            cases.add(item);
        }
        for (String input : new String[] {null, "", "LZ4", " lz4 ", " zstd ", "ZLIB", "\u00a0ZLIB\u00a0", "snappy"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "of");
            item.put("input", input);
            try {
                item.put("outcome", "OK");
                item.put("decoded", org.apache.rocketmq.common.compression.CompressionType.of(input).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            cases.add(item);
        }
        for (int input : new int[] {Integer.MIN_VALUE, -1, 0, 1, 2, 3, 4, 7, 8, Integer.MAX_VALUE}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "findByValue");
            item.put("input", input);
            try {
                item.put("outcome", "OK");
                item.put("decoded", org.apache.rocketmq.common.compression.CompressionType.findByValue(input).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            cases.add(item);
        }
        for (int input : new int[] {-1, 0, 1, 2, 3, 4, 7, 8, 255}) {
            int flag = (input << 8) | 0x4000;
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "messageSysFlag");
            item.put("input", input);
            item.put("flag", flag);
            try {
                item.put("outcome", "OK");
                item.put("decoded", org.apache.rocketmq.common.sysflag.MessageSysFlag.getCompressionType(flag).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> compressionTypeTurkishLocaleCases() {
        Locale originalLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            List<Object> cases = new ArrayList<>();
            for (String input : new String[] {"zlib", "zLiB", "zlıb", "ZLIB", "lz4", "zstd"}) {
                Map<String, Object> item = new TreeMap<>();
                item.put("input", input);
                try {
                    item.put("outcome", "OK");
                    item.put("decoded",
                        org.apache.rocketmq.common.compression.CompressionType.of(input).name());
                } catch (RuntimeException error) {
                    item.put("outcome", error.getClass().getSimpleName());
                    item.put("message", error.getMessage());
                }
                cases.add(item);
            }
            return cases;
        } finally {
            Locale.setDefault(originalLocale);
        }
    }

    private static Map<String, Object> clientStatusEnumMethodCases() {
        Map<String, Object> cases = new TreeMap<>();
        cases.put("PullStatus", enumMethodCases(
            org.apache.rocketmq.client.consumer.PullStatus::values,
            org.apache.rocketmq.client.consumer.PullStatus::valueOf,
            new String[] {null, "", "FOUND", "found", "NO_NEW_MSG", "UNKNOWN"}));
        cases.put("PopStatus", enumMethodCases(
            org.apache.rocketmq.client.consumer.PopStatus::values,
            org.apache.rocketmq.client.consumer.PopStatus::valueOf,
            new String[] {null, "", "FOUND", "found", "POLLING_FULL", "POLLING_NOT_FOUND", "UNKNOWN"}));
        cases.put("AckStatus", enumMethodCases(
            org.apache.rocketmq.client.consumer.AckStatus::values,
            org.apache.rocketmq.client.consumer.AckStatus::valueOf,
            new String[] {null, "", "OK", "ok", "NO_EXIST", "UNKNOWN"}));
        cases.put("SendStatus", enumMethodCases(
            org.apache.rocketmq.client.producer.SendStatus::values,
            org.apache.rocketmq.client.producer.SendStatus::valueOf,
            new String[] {null, "", "SEND_OK", "send_ok", "FLUSH_DISK_TIMEOUT", "SLAVE_NOT_AVAILABLE", "UNKNOWN"}));
        return cases;
    }

    private static Map<String, Object> protocolEnumMethodCases() {
        Map<String, Object> cases = new TreeMap<>();
        cases.put("MessageRequestMode", enumMethodCases(
            org.apache.rocketmq.common.message.MessageRequestMode::values,
            org.apache.rocketmq.common.message.MessageRequestMode::valueOf,
            new String[] {null, "", "PULL", "pull", "UNKNOWN"}));
        cases.put("LiteSubscriptionAction", enumMethodCases(
            org.apache.rocketmq.common.lite.LiteSubscriptionAction::values,
            org.apache.rocketmq.common.lite.LiteSubscriptionAction::valueOf,
            new String[] {null, "", "PARTIAL_ADD", "partial_add", "UNKNOWN"}));
        cases.put("OffsetOptionType", enumMethodCases(
            org.apache.rocketmq.common.lite.OffsetOption.Type::values,
            org.apache.rocketmq.common.lite.OffsetOption.Type::valueOf,
            new String[] {null, "", "POLICY", "policy", "TAIL_N", "UNKNOWN"}));
        cases.put("ConsumeMessageResult", enumMethodCases(
            org.apache.rocketmq.remoting.protocol.body.CMResult::values,
            org.apache.rocketmq.remoting.protocol.body.CMResult::valueOf,
            new String[] {null, "", "CR_SUCCESS", "cr_success", "CR_RETURN_NULL", "UNKNOWN"}));
        cases.put("ConsumeType", enumMethodCases(
            org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType::values,
            org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType::valueOf,
            new String[] {null, "", "CONSUME_ACTIVELY", "consume_actively", "UNKNOWN"}));
        cases.put("MessageModel", enumMethodCases(
            org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel::values,
            org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel::valueOf,
            new String[] {null, "", "BROADCASTING", "broadcasting", "UNKNOWN"}));
        cases.put("ConsumeFromWhere", enumMethodCases(
            org.apache.rocketmq.common.consumer.ConsumeFromWhere::values,
            org.apache.rocketmq.common.consumer.ConsumeFromWhere::valueOf,
            new String[] {null, "", "CONSUME_FROM_LAST_OFFSET", "consume_from_last_offset", "UNKNOWN"}));
        return cases;
    }

    private static Map<String, Object> tlsModeMethodCases() {
        Map<String, Object> result = new TreeMap<>();
        List<Object> enumCases = new ArrayList<>();
        org.apache.rocketmq.remoting.common.TlsMode[] values =
            org.apache.rocketmq.remoting.common.TlsMode.values();
        List<String> names = new ArrayList<>();
        for (org.apache.rocketmq.remoting.common.TlsMode value : values) {
            names.add(value.name());
        }
        for (org.apache.rocketmq.remoting.common.TlsMode value : values) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "constant");
            item.put("name", value.name());
            item.put("ordinal", value.ordinal());
            item.put("text", value.toString());
            item.put("getName", value.getName());
            item.put("values", names);
            item.put("valueOfName", org.apache.rocketmq.remoting.common.TlsMode.valueOf(value.name()).name());
            item.put("compareNext", value.compareTo(values[(value.ordinal() + 1) % values.length]));
            org.apache.rocketmq.remoting.common.TlsMode[] mutated =
                org.apache.rocketmq.remoting.common.TlsMode.values();
            mutated[0] = values[values.length - 1];
            item.put("valuesDefensiveCopy",
                org.apache.rocketmq.remoting.common.TlsMode.values()[0] == values[0]);
            enumCases.add(item);
        }
        for (String input : new String[] {
            null, "", "DISABLED", "disabled", "permissive", "enforcing", "unknown"
        }) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "lookup");
            item.put("input", input);
            try {
                item.put("outcome", "OK");
                item.put("decoded", org.apache.rocketmq.remoting.common.TlsMode.valueOf(input).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            enumCases.add(item);
        }
        result.put("enum", enumCases);

        List<Object> parseCases = new ArrayList<>();
        for (String input : new String[] {
            null, "", "disabled", "permissive", "enforcing", "DISABLED", "ENFORCING",
            " enforcing", "enforcing ", "other"
        }) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "parse");
            item.put("input", input);
            org.apache.rocketmq.remoting.common.TlsMode parsed =
                org.apache.rocketmq.remoting.common.TlsMode.parse(input);
            item.put("parsed", parsed.name());
            item.put("getName", parsed.getName());
            parseCases.add(item);
        }
        result.put("parse", parseCases);
        return result;
    }

    private static Map<String, Object> remotingEnumMethodCases() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        result.put("RemotingCommandType", enumMethodCases(
            org.apache.rocketmq.remoting.protocol.RemotingCommandType::values,
            org.apache.rocketmq.remoting.protocol.RemotingCommandType::valueOf,
            new String[] {null, "", "REQUEST_COMMAND", "request_command", "RESPONSE_COMMAND", "UNKNOWN"}));
        result.put("RequestType", enumMethodCases(
            org.apache.rocketmq.remoting.protocol.RequestType::values,
            name -> org.apache.rocketmq.remoting.protocol.RequestType.valueOf(name),
            new String[] {null, "", "STREAM", "stream", "UNKNOWN"}));
        result.put("RequestSource", requestSourceMethodCases());
        result.put("MessageQueueRouteState", enumMethodCases(
            org.apache.rocketmq.remoting.protocol.route.MessageQueueRouteState::values,
            org.apache.rocketmq.remoting.protocol.route.MessageQueueRouteState::valueOf,
            new String[] {null, "", "Expired", "ReadOnly", "Normal", "WriteOnly", "expired", "UNKNOWN"}));

        List<Object> requestTypeBytes = new ArrayList<>();
        for (byte input : new byte[] {Byte.MIN_VALUE, -1, 0, 1, Byte.MAX_VALUE}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("input", input);
            item.put("case", "byteLookup");
            org.apache.rocketmq.remoting.protocol.RequestType decoded =
                org.apache.rocketmq.remoting.protocol.RequestType.valueOf(input);
            item.put("outcome", "OK");
            item.put("decoded", decoded == null ? null : decoded.name());
            item.put("getCode", decoded == null ? null : decoded.getCode());
            requestTypeBytes.add(item);
        }
        result.put("requestTypeBytes", requestTypeBytes);

        List<Object> commandTypes = new ArrayList<>();
        org.apache.rocketmq.remoting.protocol.RemotingCommand request =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(1, null);
        addCommandTypeCase(commandTypes, "request", request);
        org.apache.rocketmq.remoting.protocol.RemotingCommand response =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(1, "remark");
        addCommandTypeCase(commandTypes, "response", response);
        request.markResponseType();
        addCommandTypeCase(commandTypes, "markedRequest", request);
        result.put("commandGetType", commandTypes);

        List<Object> commandFlags = new ArrayList<>();
        org.apache.rocketmq.remoting.protocol.RemotingCommand flaggedRequest =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(1, null);
        addCommandFlagCase(commandFlags, "default", flaggedRequest);
        flaggedRequest.markOnewayRPC();
        addCommandFlagCase(commandFlags, "oneway", flaggedRequest);
        flaggedRequest.markResponseType();
        addCommandFlagCase(commandFlags, "onewayResponse", flaggedRequest);
        flaggedRequest.markOnewayRPC();
        flaggedRequest.markResponseType();
        addCommandFlagCase(commandFlags, "repeatedMarks", flaggedRequest);
        flaggedRequest.setFlag(1 << 8);
        flaggedRequest.markOnewayRPC();
        flaggedRequest.markResponseType();
        addCommandFlagCase(commandFlags, "preservesUnknownBits", flaggedRequest);
        result.put("commandFlags", commandFlags);

        List<Object> commandSuspended = new ArrayList<>();
        org.apache.rocketmq.remoting.protocol.RemotingCommand suspendedRequest =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(1, null);
        addCommandSuspendedCase(commandSuspended, "default", suspendedRequest);
        suspendedRequest.setSuspended(true);
        addCommandSuspendedCase(commandSuspended, "setTrue", suspendedRequest);
        suspendedRequest.setSuspended(false);
        addCommandSuspendedCase(commandSuspended, "setFalse", suspendedRequest);
        result.put("commandSuspended", commandSuspended);
        result.put("commandProtocolHelpers", commandProtocolHelperCases());
        result.put("commandConstructorDefaults", commandConstructorDefaultCases());
        result.put("commandFactories", commandFactoryCases());
        result.put("commandCustomHeaders", commandCustomHeaderCases());
        result.put("commandInfrastructure", commandInfrastructureCases());
        result.put("commandToString", commandToStringCases());
        result.put("commandNullableExtFields", commandNullableExtFieldCases());
        return result;
    }

    private static Map<String, Object> commandNullableExtFieldCases() {
        org.apache.rocketmq.remoting.protocol.RemotingCommand command =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(90, null);
        java.util.HashMap<String, String> fields = new java.util.HashMap<>();
        fields.put(null, "null-key-value");
        fields.put("null-value", null);
        fields.put("kept", "value");
        command.setExtFields(fields);
        command.setSerializeTypeCurrentRPC(org.apache.rocketmq.remoting.protocol.SerializeType.JSON);

        Map<String, Object> result = new TreeMap<>();
        result.put("nullKeyPresent", command.getExtFields().containsKey(null));
        result.put("nullKeyValue", command.getExtFields().get(null));
        result.put("nullValueKeyPresent", command.getExtFields().containsKey("null-value"));
        result.put("nullValue", command.getExtFields().get("null-value"));
        result.put("nonNullValue", command.getExtFields().get("kept"));
        result.put("display", command.toString());
        result.put("jsonHeader", new String(
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.encode(command),
            java.nio.charset.StandardCharsets.UTF_8));
        result.put("rocketMqMapBase64", java.util.Base64.getEncoder().encodeToString(
            org.apache.rocketmq.remoting.protocol.RocketMQSerializable.mapSerialize(fields)));

        org.apache.rocketmq.remoting.protocol.RemotingCommand added =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(91, null);
        added.addExtField(null, "null-key-added");
        added.addExtField("null-value-added", null);
        result.put("addNullKeyPresent", added.getExtFields().containsKey(null));
        result.put("addNullKeyValue", added.getExtFields().get(null));
        result.put("addNullValueKeyPresent", added.getExtFields().containsKey("null-value-added"));

        org.apache.rocketmq.remoting.protocol.RemotingCommand putIfAbsent =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(92, null);
        java.util.HashMap<String, String> nullValue = new java.util.HashMap<>();
        nullValue.put("field", null);
        putIfAbsent.setExtFields(nullValue);
        putIfAbsent.addExtFieldIfNotExist("field", "replacement");
        result.put("putIfAbsentReplacesNull", "replacement".equals(putIfAbsent.getExtFields().get("field")));
        return result;
    }

    private static Map<String, Object> commandConstructorDefaultCases() throws Exception {
        java.lang.reflect.Constructor<org.apache.rocketmq.remoting.protocol.RemotingCommand> constructor =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        org.apache.rocketmq.remoting.protocol.RemotingCommand command = constructor.newInstance();
        org.apache.rocketmq.remoting.protocol.RemotingCommand next = constructor.newInstance();
        Map<String, Object> result = new TreeMap<>();
        result.put("code", command.getCode());
        result.put("language", command.getLanguage().name());
        result.put("version", command.getVersion());
        result.put("opaqueIncrement", next.getOpaque() - command.getOpaque());
        result.put("flag", command.getFlag());
        result.put("remarkNull", command.getRemark() == null);
        result.put("extFieldsNull", command.getExtFields() == null);
        result.put("bodyNull", command.getBody() == null);
        result.put("serialization", command.getSerializeTypeCurrentRPC().name());
        result.put("suspended", command.isSuspended());
        result.put("processTimerNull", command.getProcessTimer() == null);
        result.put("callbackListNull", command.getCallbackList() == null);
        return result;
    }

    private static Map<String, Object> commandFactoryCases() {
        List<Object> cases = new ArrayList<>();
        List<org.apache.rocketmq.remoting.protocol.RemotingCommand> commands = new ArrayList<>();
        org.apache.rocketmq.remoting.protocol.RemotingCommand request =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(71, null);
        org.apache.rocketmq.remoting.protocol.RemotingCommand response =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(72, "accepted");
        org.apache.rocketmq.remoting.protocol.RemotingCommand error =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.buildErrorResponse(73, null);
        commands.add(request);
        commands.add(response);
        commands.add(error);

        org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader requestHeader =
            new org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader();
        requestHeader.setTopic("orders");
        org.apache.rocketmq.remoting.protocol.RemotingCommand requestWithHeader =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(74, requestHeader);
        requestWithHeader.makeCustomHeaderToNet();
        commands.add(requestWithHeader);

        org.apache.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader responseHeader =
            new org.apache.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader();
        responseHeader.setOffset(88L);
        org.apache.rocketmq.remoting.protocol.RemotingCommand responseWithHeader =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommandWithHeader(75, responseHeader);
        responseWithHeader.makeCustomHeaderToNet();
        commands.add(responseWithHeader);

        org.apache.rocketmq.remoting.protocol.RemotingCommand responseWithDefaultHeader =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                org.apache.rocketmq.remoting.protocol.header.UnregisterClientResponseHeader.class);
        responseWithDefaultHeader.makeCustomHeaderToNet();
        commands.add(responseWithDefaultHeader);

        org.apache.rocketmq.remoting.protocol.RemotingCommand responseWithClassHeader =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                76, "class-header", org.apache.rocketmq.remoting.protocol.header.UnregisterClientResponseHeader.class);
        responseWithClassHeader.makeCustomHeaderToNet();
        commands.add(responseWithClassHeader);

        org.apache.rocketmq.remoting.protocol.RemotingCommand errorWithClassHeader =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.buildErrorResponse(
                77, "built", org.apache.rocketmq.remoting.protocol.header.UnregisterClientResponseHeader.class);
        errorWithClassHeader.makeCustomHeaderToNet();
        commands.add(errorWithClassHeader);

        for (int index = 0; index < commands.size(); index++) {
            cases.add(commandFactoryCase("factory" + index, commands.get(index)));
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("commands", cases);
        List<Integer> deltas = new ArrayList<>();
        for (int index = 1; index < commands.size(); index++) {
            deltas.add(commands.get(index).getOpaque() - commands.get(index - 1).getOpaque());
        }
        result.put("requestIdDeltas", deltas);

        org.apache.rocketmq.remoting.protocol.RemotingCommand beforeFailedFactories =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(78, null);
        result.put("missingDefaultHeaderReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                79, "missing", MissingDefaultHeader.class) == null);
        result.put("inaccessibleDefaultHeaderReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                80, "inaccessible", InaccessibleDefaultHeader.class) == null);
        result.put("throwingDefaultHeaderReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                81, "throwing", ThrowingDefaultHeader.class) == null);
        try {
            org.apache.rocketmq.remoting.protocol.RemotingCommand.buildErrorResponse(
                82, "error", MissingDefaultHeader.class);
            result.put("errorFactoryHeaderFailure", "NO_ERROR");
        } catch (Exception buildError) {
            result.put("errorFactoryHeaderFailure", buildError.getClass().getSimpleName());
        }
        org.apache.rocketmq.remoting.protocol.RemotingCommand afterFailedFactories =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(83, null);
        result.put("failedHeaderFactoryOpaqueDelta",
            afterFailedFactories.getOpaque() - beforeFailedFactories.getOpaque());
        return result;
    }

    private static Map<String, Object> commandFactoryCase(
        String name, org.apache.rocketmq.remoting.protocol.RemotingCommand command) {
        Map<String, Object> result = new TreeMap<>();
        result.put("name", name);
        result.put("code", command.getCode());
        result.put("language", command.getLanguage() == null ? null : command.getLanguage().name());
        result.put("version", command.getVersion());
        result.put("opaque", command.getOpaque());
        result.put("flag", command.getFlag());
        result.put("remark", command.getRemark());
        result.put("extFields", command.getExtFields());
        result.put("serialization", command.getSerializeTypeCurrentRPC().name());
        return result;
    }

    private static Map<String, Object> commandCustomHeaderCases() throws Exception {
        org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader header =
            new org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader();
        header.setTopic("custom-topic");
        header.setAcceptStandardJsonOnly(true);
        header.setNamespace("custom-ns");
        header.setNamespaced(false);
        header.setBrokerName("broker-a");
        header.setOneway(true);

        org.apache.rocketmq.remoting.protocol.RemotingCommand command =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(78, null);
        command.addExtField("oracle", "5.5.1");
        command.writeCustomHeader(header);

        Map<String, Object> result = new TreeMap<>();
        result.put("attachedHeaderPresent", command.readCustomHeader() != null);
        result.put("readReturnsSameInstance", command.readCustomHeader() == header);
        header.setTopic("mutated-after-attach");
        result.put("attachedHeaderTopicAfterMutation",
            ((org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader)
                command.readCustomHeader()).getTopic());
        result.put("attachedMutationVisibleToCaller",
            "mutated-after-attach".equals(header.getTopic()));
        org.apache.rocketmq.remoting.protocol.RemotingCommand decodedWithoutAttachedHeader =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(82, null);
        decodedWithoutAttachedHeader.setExtFields(new HashMap<>(command.getExtFields()));
        result.put("readReturnsNullWithoutAttachedHeader",
            decodedWithoutAttachedHeader.readCustomHeader() == null);
        org.apache.rocketmq.remoting.protocol.RemotingCommand nullHeaderCommand =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(83, header);
        nullHeaderCommand.writeCustomHeader(null);
        result.put("writeNullClearsAttachedHeader", nullHeaderCommand.readCustomHeader() == null);
        result.put("extFieldsBeforeMake", command.getExtFields() == null
            ? null : new TreeMap<>(command.getExtFields()));
        command.makeCustomHeaderToNet();
        result.put("extFieldsAfterMake", new TreeMap<>(command.getExtFields()));
        ((org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader)
            command.readCustomHeader()).setTopic("mutated-through-command");
        result.put("commandMutationVisibleToCaller",
            "mutated-through-command".equals(header.getTopic()));

        org.apache.rocketmq.remoting.protocol.RemotingCommand addFieldCommand =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(79, null);
        addFieldCommand.addExtField("added", "value");
        result.put("addExtField", new TreeMap<>(addFieldCommand.getExtFields()));

        org.apache.rocketmq.remoting.protocol.RemotingCommand addIfMissingCommand =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(80, null);
        addIfMissingCommand.addExtField("existing", "original");
        addIfMissingCommand.addExtFieldIfNotExist("existing", "replacement");
        addIfMissingCommand.addExtFieldIfNotExist("new", "value");
        result.put("addExtFieldIfNotExist", new TreeMap<>(addIfMissingCommand.getExtFields()));

        try {
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(81, null)
                .addExtFieldIfNotExist("missing-map", "value");
            result.put("addExtFieldIfNotExistWithoutMap", "NO_ERROR");
        } catch (Exception e) {
            result.put("addExtFieldIfNotExistWithoutMap", e.getClass().getSimpleName());
        }

        org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader cached =
            command.decodeCommandCustomHeader(
                org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader.class,
                true);
        org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader cachedAgain =
            command.decodeCommandCustomHeader(
                org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader.class,
                true);
        org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader direct =
            command.decodeCommandCustomHeaderDirectly(
                org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader.class,
                true);
        result.put("cachedHeader", routeHeaderFields(cached));
        result.put("cachedRepeatedSameInstance", cached == cachedAgain);
        result.put("directHeader", routeHeaderFields(direct));
        result.put("directDistinctFromCached", direct != cached);
        result.put("checkFields", commandCustomHeaderCheckFields());
        return result;
    }

    private static Map<String, Object> commandCustomHeaderCheckFields() {
        Map<String, Object> result = new TreeMap<>();

        Map<String, String> createTopicBase = new LinkedHashMap<>();
        createTopicBase.put("topic", "topic");
        createTopicBase.put("defaultTopic", "default");
        createTopicBase.put("readQueueNums", "4");
        createTopicBase.put("writeQueueNums", "4");
        createTopicBase.put("perm", "6");
        result.put("createTopic", customHeaderValidationCases(
            org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader.class,
            createTopicBase, "topicFilterType", new String[] {"SINGLE_TAG", "MULTI_TAG", "SQL92", "INVALID"}));

        Map<String, String> transactionBase = new LinkedHashMap<>();
        transactionBase.put("producerGroup", "producer");
        transactionBase.put("tranStateTableOffset", "1");
        transactionBase.put("commitLogOffset", "2");
        transactionBase.put("msgId", "message");
        result.put("endTransaction", customHeaderValidationCases(
            org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader.class,
            transactionBase, "commitOrRollback", new String[] {"0", "4", "8", "12", "16"}));

        Map<String, String> transactionResponseBase = new LinkedHashMap<>();
        transactionResponseBase.put("producerGroup", "producer");
        transactionResponseBase.put("tranStateTableOffset", "1");
        transactionResponseBase.put("commitLogOffset", "2");
        result.put("checkTransactionStateResponse", customHeaderValidationCases(
            org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateResponseHeader.class,
            transactionResponseBase, "commitOrRollback", new String[] {"0", "4", "8", "12", "16"}));

        Map<String, Object> custom = new TreeMap<>();
        custom.put("zero", customHeaderDecodeOutcome(
            ValidatedCustomHeader.class, Map.of("value", "0")));
        custom.put("negative", customHeaderDecodeOutcome(
            ValidatedCustomHeader.class, Map.of("value", "-1")));
        custom.put("missing", customHeaderDecodeOutcome(
            ValidatedCustomHeader.class, Map.of()));
        result.put("custom", custom);
        return result;
    }

    private static List<Object> customHeaderValidationCases(
        Class<? extends org.apache.rocketmq.remoting.CommandCustomHeader> headerClass,
        Map<String, String> baseFields, String field, String[] values) {
        List<Object> cases = new ArrayList<>();
        for (String value : values) {
            Map<String, String> fields = new LinkedHashMap<>(baseFields);
            fields.put(field, value);
            Map<String, Object> item = new TreeMap<>();
            item.put("value", value);
            item.put("outcome", customHeaderDecodeOutcome(headerClass, fields));
            cases.add(item);
        }
        return cases;
    }

    private static String customHeaderDecodeOutcome(
        Class<? extends org.apache.rocketmq.remoting.CommandCustomHeader> headerClass,
        Map<String, String> fields) {
        org.apache.rocketmq.remoting.protocol.RemotingCommand command =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(91, null);
        command.setExtFields(new HashMap<>(fields));
        try {
            command.decodeCommandCustomHeader(headerClass);
            return "OK";
        } catch (Throwable error) {
            return error.getClass().getSimpleName();
        }
    }

    private static Map<String, Object> routeHeaderFields(
        org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader header) {
        Map<String, Object> result = new TreeMap<>();
        result.put("topic", header.getTopic());
        result.put("acceptStandardJsonOnly", header.getAcceptStandardJsonOnly());
        result.put("ns", header.getNamespace());
        result.put("nsd", header.getNamespaced());
        result.put("bname", header.getBrokerName());
        result.put("oway", header.getOneway());
        return result;
    }

    public static final class MissingDefaultHeader
        implements org.apache.rocketmq.remoting.CommandCustomHeader {
        public MissingDefaultHeader(String ignored) { }
        @Override public void checkFields() { }
    }

    public static final class InaccessibleDefaultHeader
        implements org.apache.rocketmq.remoting.CommandCustomHeader {
        private InaccessibleDefaultHeader() { }
        @Override public void checkFields() { }
    }

    public static final class ThrowingDefaultHeader
        implements org.apache.rocketmq.remoting.CommandCustomHeader {
        public ThrowingDefaultHeader() { throw new IllegalStateException("fixture"); }
        @Override public void checkFields() { }
    }

    public static final class ValidatedCustomHeader
        implements org.apache.rocketmq.remoting.CommandCustomHeader {
        private Integer value;

        @Override
        public void checkFields() throws org.apache.rocketmq.remoting.exception.RemotingCommandException {
            if (value == null || value < 0) {
                throw new org.apache.rocketmq.remoting.exception.RemotingCommandException(
                    "value must be non-negative");
            }
        }
    }

    private static Map<String, Object> commandInfrastructureCases() {
        org.apache.rocketmq.remoting.protocol.RemotingCommand command =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(84, null);
        Map<String, Object> result = new TreeMap<>();
        result.put("processTimerInitiallyNull", command.getProcessTimer() == null);
        result.put("callbackListInitiallyNull", command.getCallbackList() == null);

        com.google.common.base.Stopwatch timer = com.google.common.base.Stopwatch.createUnstarted();
        command.setProcessTimer(timer);
        result.put("processTimerSameInstance", command.getProcessTimer() == timer);
        result.put("processTimerInitiallyRunning", timer.isRunning());
        timer.start();
        result.put("processTimerRunningAfterStart", timer.isRunning());
        result.put("processTimerElapsedNonnegative", timer.elapsed(TimeUnit.NANOSECONDS) >= 0);
        try {
            timer.start();
            result.put("processTimerStartWhileRunning", "NO_ERROR");
        } catch (Throwable error) {
            result.put("processTimerStartWhileRunning", error.getClass().getSimpleName());
        }
        timer.stop();
        result.put("processTimerRunningAfterStop", timer.isRunning());
        timer.reset();
        result.put("processTimerRunningAfterReset", timer.isRunning());
        result.put("processTimerElapsedAfterReset", timer.elapsed(TimeUnit.NANOSECONDS));
        try {
            timer.stop();
            result.put("processTimerStopWhileStopped", "NO_ERROR");
        } catch (Throwable error) {
            result.put("processTimerStopWhileStopped", error.getClass().getSimpleName());
        }
        command.setProcessTimer(null);
        result.put("processTimerNullAfterClear", command.getProcessTimer() == null);

        java.util.concurrent.atomic.AtomicInteger callbackCount =
            new java.util.concurrent.atomic.AtomicInteger();
        java.util.List<org.apache.rocketmq.remoting.CommandCallback> callbacks = new ArrayList<>();
        callbacks.add(callbackCount::incrementAndGet);
        command.setCallbackList(callbacks);
        result.put("callbackListSameInstance", command.getCallbackList() == callbacks);
        command.getCallbackList().add(callbackCount::incrementAndGet);
        for (org.apache.rocketmq.remoting.CommandCallback callback : command.getCallbackList()) {
            callback.accept();
        }
        result.put("callbackListSizeAfterMutation", command.getCallbackList().size());
        result.put("callbackListInvocationCount", callbackCount.get());
        command.setCallbackList(null);
        result.put("callbackListNullAfterClear", command.getCallbackList() == null);
        return result;
    }

    private static List<Object> commandToStringCases() {
        List<Object> result = new ArrayList<>();

        org.apache.rocketmq.remoting.protocol.RemotingCommand empty =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(0, null);
        empty.setLanguage(null);
        empty.setVersion(-1);
        empty.setOpaque(17);
        empty.setFlag(0);
        empty.setRemark(null);
        empty.setExtFields(null);
        empty.setSerializeTypeCurrentRPC(org.apache.rocketmq.remoting.protocol.SerializeType.JSON);
        result.add(commandToStringCase("nullFields", empty));

        org.apache.rocketmq.remoting.protocol.RemotingCommand populated =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(7, null);
        populated.setLanguage(org.apache.rocketmq.remoting.protocol.LanguageCode.RUST);
        populated.setVersion(15);
        populated.setOpaque(42);
        populated.setFlag(259);
        populated.setRemark("accepted");
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("zone", "west");
        populated.setExtFields(extFields);
        populated.setSerializeTypeCurrentRPC(org.apache.rocketmq.remoting.protocol.SerializeType.ROCKETMQ);
        result.add(commandToStringCase("populated", populated));

        org.apache.rocketmq.remoting.protocol.RemotingCommand multipleExtFields =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(8, null);
        multipleExtFields.setLanguage(org.apache.rocketmq.remoting.protocol.LanguageCode.JAVA);
        multipleExtFields.setVersion(16);
        multipleExtFields.setOpaque(43);
        multipleExtFields.setFlag(0);
        multipleExtFields.setRemark("multiple fields");
        java.util.HashMap<String, String> unorderedFields = new java.util.HashMap<>();
        unorderedFields.put("Z", "upper");
        unorderedFields.put("a", "lower");
        multipleExtFields.setExtFields(unorderedFields);
        multipleExtFields.setSerializeTypeCurrentRPC(
            org.apache.rocketmq.remoting.protocol.SerializeType.JSON);
        result.add(commandToStringCase("multipleExtFields", multipleExtFields));

        org.apache.rocketmq.remoting.protocol.RemotingCommand signedFlag =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(-1, null);
        signedFlag.setLanguage(org.apache.rocketmq.remoting.protocol.LanguageCode.JAVA);
        signedFlag.setOpaque(-1);
        signedFlag.setFlag(-1);
        signedFlag.setSerializeTypeCurrentRPC(org.apache.rocketmq.remoting.protocol.SerializeType.JSON);
        result.add(commandToStringCase("signedFlag", signedFlag));

        return result;
    }

    private static Map<String, Object> commandToStringCase(
        String name, org.apache.rocketmq.remoting.protocol.RemotingCommand command) {
        Map<String, Object> result = new TreeMap<>();
        result.put("name", name);
        Map<String, Object> fields = new TreeMap<>();
        fields.put("code", command.getCode());
        fields.put("language", command.getLanguage() == null ? null : command.getLanguage().name());
        fields.put("version", command.getVersion());
        fields.put("opaque", command.getOpaque());
        fields.put("flag", command.getFlag());
        fields.put("remark", command.getRemark());
        fields.put("extFields", command.getExtFields());
        fields.put("serialization", command.getSerializeTypeCurrentRPC().name());
        result.put("fields", fields);
        result.put("result", command.toString());
        return result;
    }

    private static Map<String, Object> commandProtocolHelperCases() {
        Map<String, Object> result = new TreeMap<>();
        result.put("serializeTypeProperty",
            org.apache.rocketmq.remoting.protocol.RemotingCommand.SERIALIZE_TYPE_PROPERTY);
        result.put("serializeTypeEnv",
            org.apache.rocketmq.remoting.protocol.RemotingCommand.SERIALIZE_TYPE_ENV);
        result.put("remotingVersionKey",
            org.apache.rocketmq.remoting.protocol.RemotingCommand.REMOTING_VERSION_KEY);
        result.put("serializeTypeConfig",
            org.apache.rocketmq.remoting.protocol.RemotingCommand.getSerializeTypeConfigInThisServer().name());

        List<Object> headerLengths = new ArrayList<>();
        for (int input : new int[] {Integer.MIN_VALUE, -1, 0, 1, 0x12345678, Integer.MAX_VALUE}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("input", input);
            item.put("result", org.apache.rocketmq.remoting.protocol.RemotingCommand.getHeaderLength(input));
            headerLengths.add(item);
        }
        result.put("headerLengths", headerLengths);

        List<Object> protocolTypes = new ArrayList<>();
        for (int source : new int[] {Integer.MIN_VALUE, -1, 0, 1, 0x12345678, Integer.MAX_VALUE}) {
            for (org.apache.rocketmq.remoting.protocol.SerializeType type :
                org.apache.rocketmq.remoting.protocol.SerializeType.values()) {
                int marked = org.apache.rocketmq.remoting.protocol.RemotingCommand.markProtocolType(source, type);
                Map<String, Object> item = new TreeMap<>();
                item.put("source", source);
                item.put("type", type.name());
                item.put("marked", marked);
                item.put("decoded", org.apache.rocketmq.remoting.protocol.RemotingCommand
                    .getProtocolType(marked).name());
                protocolTypes.add(item);
            }
        }
        result.put("protocolTypes", protocolTypes);

        List<Object> protocolTypeLookups = new ArrayList<>();
        for (int source : new int[] {0, 0x01000000, 0x02000000, 0xff000000}) {
            org.apache.rocketmq.remoting.protocol.SerializeType decoded =
                org.apache.rocketmq.remoting.protocol.RemotingCommand.getProtocolType(source);
            Map<String, Object> item = new TreeMap<>();
            item.put("source", source);
            item.put("decoded", decoded == null ? null : decoded.name());
            protocolTypeLookups.add(item);
        }
        result.put("protocolTypeLookups", protocolTypeLookups);

        int firstRequestId = org.apache.rocketmq.remoting.protocol.RemotingCommand.createNewRequestId();
        int secondRequestId = org.apache.rocketmq.remoting.protocol.RemotingCommand.createNewRequestId();
        result.put("requestIdDelta", secondRequestId - firstRequestId);
        return result;
    }

    private static void addCommandFlagCase(
        List<Object> cases, String name,
        org.apache.rocketmq.remoting.protocol.RemotingCommand command
    ) {
        Map<String, Object> item = new TreeMap<>();
        item.put("case", name);
        item.put("flag", command.getFlag());
        item.put("type", command.getType().name());
        item.put("isOneway", command.isOnewayRPC());
        item.put("isResponse", command.isResponseType());
        cases.add(item);
    }

    private static void addCommandSuspendedCase(
        List<Object> cases, String name,
        org.apache.rocketmq.remoting.protocol.RemotingCommand command
    ) {
        Map<String, Object> item = new TreeMap<>();
        item.put("case", name);
        item.put("suspended", command.isSuspended());
        cases.add(item);
    }

    private static Map<String, Object> requestSourceMethodCases() {
        Map<String, Object> result = new TreeMap<>();
        result.put("enum", enumMethodCases(
            org.apache.rocketmq.remoting.protocol.RequestSource::values,
            org.apache.rocketmq.remoting.protocol.RequestSource::valueOf,
            new String[] {null, "", "SDK", "PROXY_FOR_ORDER", "proxy_for_order", "UNKNOWN"}));
        result.put("systemPropertyKey",
            org.apache.rocketmq.remoting.protocol.RequestSource.SYSTEM_PROPERTY_KEY);

        List<Object> values = new ArrayList<>();
        for (org.apache.rocketmq.remoting.protocol.RequestSource source :
            org.apache.rocketmq.remoting.protocol.RequestSource.values()) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "value");
            item.put("name", source.name());
            item.put("value", source.getValue());
            values.add(item);
        }
        result.put("values", values);

        List<Object> integers = new ArrayList<>();
        for (Integer input : new Integer[] {
            null, Integer.MIN_VALUE, -2, -1, 0, 1, 2, 3, Integer.MAX_VALUE
        }) {
            Map<String, Object> item = new TreeMap<>();
            item.put("input", input);
            item.put("valid", org.apache.rocketmq.remoting.protocol.RequestSource.isValid(input));
            item.put("parsed",
                org.apache.rocketmq.remoting.protocol.RequestSource.parseInteger(input).name());
            integers.add(item);
        }
        result.put("integers", integers);
        return result;
    }

    private static void addCommandTypeCase(
        List<Object> cases,
        String label,
        org.apache.rocketmq.remoting.protocol.RemotingCommand command) {
        Map<String, Object> item = new TreeMap<>();
        item.put("case", label);
        item.put("type", command.getType().name());
        item.put("isResponse", command.isResponseType());
        cases.add(item);
    }

    private static <E extends Enum<E>> List<Object> enumMethodCases(
        java.util.function.Supplier<E[]> valuesMethod,
        java.util.function.Function<String, E> valueOfMethod,
        String[] lookupInputs) {
        List<Object> cases = new ArrayList<>();
        E[] values = valuesMethod.get();
        List<String> names = new ArrayList<>();
        for (E value : values) names.add(value.name());
        for (E value : values) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "constant");
            item.put("name", value.name());
            item.put("ordinal", value.ordinal());
            item.put("text", value.toString());
            item.put("values", names);
            item.put("valueOfName", valueOfMethod.apply(value.name()).name());
            item.put("compareNext", value.compareTo(values[(value.ordinal() + 1) % values.length]));
            E[] mutated = valuesMethod.get();
            mutated[0] = values[values.length - 1];
            item.put("valuesDefensiveCopy", valuesMethod.get()[0] == values[0]);
            cases.add(item);
        }
        for (String input : lookupInputs) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "lookup");
            item.put("input", input);
            try {
                item.put("outcome", "OK");
                item.put("decoded", valueOfMethod.apply(input).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> accessChannelMethodCases() {
        List<Object> cases = new ArrayList<>();
        org.apache.rocketmq.client.AccessChannel[] values =
            org.apache.rocketmq.client.AccessChannel.values();
        List<String> names = new ArrayList<>();
        for (org.apache.rocketmq.client.AccessChannel value : values) {
            names.add(value.name());
        }
        for (org.apache.rocketmq.client.AccessChannel value : values) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "constant");
            item.put("name", value.name());
            item.put("ordinal", value.ordinal());
            item.put("text", value.toString());
            item.put("values", names);
            item.put("valueOf", org.apache.rocketmq.client.AccessChannel.valueOf(value.name()).name());
            item.put("compareNext", value.compareTo(values[(value.ordinal() + 1) % values.length]));
            org.apache.rocketmq.client.AccessChannel[] mutated =
                org.apache.rocketmq.client.AccessChannel.values();
            mutated[0] = org.apache.rocketmq.client.AccessChannel.CLOUD;
            item.put("valuesDefensiveCopy",
                org.apache.rocketmq.client.AccessChannel.values()[0]
                    == org.apache.rocketmq.client.AccessChannel.LOCAL);
            cases.add(item);
        }
        for (String input : new String[] {null, "", "LOCAL", "local", "CLOUD", "UNKNOWN"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "lookup");
            item.put("input", input);
            try {
                item.put("outcome", "OK");
                item.put("decoded", org.apache.rocketmq.client.AccessChannel.valueOf(input).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> serviceNameCases() {
        List<Object> cases = new ArrayList<>();
        org.apache.rocketmq.client.impl.consumer.RebalanceService rebalance =
            new org.apache.rocketmq.client.impl.consumer.RebalanceService(null);
        org.apache.rocketmq.client.impl.consumer.PullMessageService pull =
            new org.apache.rocketmq.client.impl.consumer.PullMessageService(null);
        Map<String, Object> rebalanceCase = new TreeMap<>();
        rebalanceCase.put("class", "RebalanceService");
        rebalanceCase.put("serviceName", rebalance.getServiceName());
        cases.add(rebalanceCase);
        Map<String, Object> pullCase = new TreeMap<>();
        pullCase.put("class", "PullMessageService");
        pullCase.put("serviceName", pull.getServiceName());
        cases.add(pullCase);
        return cases;
    }

    private static List<Object> serviceThreadCases() {
        List<Object> cases = new ArrayList<>();
        org.apache.rocketmq.common.ServiceThread[] services = {
            new org.apache.rocketmq.client.impl.consumer.RebalanceService(null),
            new org.apache.rocketmq.client.impl.consumer.PullMessageService(null)
        };
        for (org.apache.rocketmq.common.ServiceThread service : services) {
            Map<String, Object> row = new TreeMap<>();
            row.put("class", service.getServiceName());
            row.put("joinTimeMillis", service.getJoinTime());
            row.put("initiallyDaemon", service.isDaemon());
            row.put("initiallyStopped", service.isStopped());
            service.setDaemon(true);
            row.put("daemonAfterSet", service.isDaemon());
            service.wakeup();
            row.put("stoppedAfterWakeup", service.isStopped());
            service.makeStop();
            row.put("stoppedAfterMakeStopBeforeStart", service.isStopped());
            service.shutdown();
            row.put("stoppedAfterShutdownBeforeStart", service.isStopped());
            if (service instanceof org.apache.rocketmq.client.impl.consumer.PullMessageService) {
                org.apache.rocketmq.client.impl.consumer.PullMessageService pull =
                    (org.apache.rocketmq.client.impl.consumer.PullMessageService) service;
                row.put("scheduledExecutorShutdown", pull.getScheduledExecutorService().isShutdown());
            }
            cases.add(row);
        }
        return cases;
    }

    private static List<Object> concurrentHeartbeatConstructionCases() throws Exception {
        List<Object> cases = new ArrayList<>();
        for (int poolSize : new int[] {0, -1, 1}) {
            ClientConfig config = new ClientConfig();
            config.setClientIP("concurrent-heartbeat-pool-" + poolSize);
            config.setInstanceName("fixture");
            config.setNamesrvAddr("");
            config.setEnableConcurrentHeartbeat(true);
            config.setConcurrentHeartbeatThreadPoolSize(poolSize);
            Map<String, Object> item = new TreeMap<>();
            item.put("poolSize", poolSize);
            org.apache.rocketmq.client.impl.factory.MQClientInstance runtime = null;
            try {
                runtime = new org.apache.rocketmq.client.impl.factory.MQClientInstance(
                    config, 0, config.buildMQClientId());
                java.lang.reflect.Field executorField = runtime.getClass()
                    .getDeclaredField("concurrentHeartbeatExecutor");
                executorField.setAccessible(true);
                java.util.concurrent.ThreadPoolExecutor executor =
                    (java.util.concurrent.ThreadPoolExecutor) executorField.get(runtime);
                item.put("outcome", "OK");
                item.put("executorCorePoolSize", executor.getCorePoolSize());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
            } finally {
                if (runtime != null) cleanupRuntime(runtime);
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> runtimeStartScheduleCases() throws Exception {
        List<Object> cases = new ArrayList<>();
        Object[][] invalidIntervals = {
            {"pollNameServerInterval", 0},
            {"heartbeatBrokerInterval", -1},
            {"persistConsumerOffsetInterval", 0}
        };
        for (Object[] invalid : invalidIntervals) {
            String fieldName = (String) invalid[0];
            int interval = (Integer) invalid[1];
            ClientConfig config = new ClientConfig();
            config.setClientIP("invalid-runtime-schedule-" + fieldName);
            config.setInstanceName("fixture");
            config.setNamesrvAddr("");
            switch (fieldName) {
                case "pollNameServerInterval": config.setPollNameServerInterval(interval); break;
                case "heartbeatBrokerInterval": config.setHeartbeatBrokerInterval(interval); break;
                case "persistConsumerOffsetInterval": config.setPersistConsumerOffsetInterval(interval); break;
                default: throw new IllegalArgumentException("unknown scheduled interval " + fieldName);
            }
            org.apache.rocketmq.client.impl.factory.MQClientInstance runtime =
                new org.apache.rocketmq.client.impl.factory.MQClientInstance(
                    config, 0, config.buildMQClientId());
            Map<String, Object> item = new TreeMap<>();
            item.put("field", fieldName);
            item.put("interval", interval);
            try {
                runtime.start();
                item.put("outcome", "OK");
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
            }
            java.lang.reflect.Field stateField = runtime.getClass().getDeclaredField("serviceState");
            stateField.setAccessible(true);
            item.put("serviceState", stateField.get(runtime).toString());
            java.lang.reflect.Field schedulerField = runtime.getClass()
                .getDeclaredField("scheduledExecutorService");
            schedulerField.setAccessible(true);
            item.put("scheduledExecutorShutdown",
                ((java.util.concurrent.ScheduledExecutorService) schedulerField.get(runtime)).isShutdown());
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> runtimeRestartAfterShutdown() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("runtime-restart-after-shutdown");
        config.setInstanceName("fixture");
        config.setNamesrvAddr("");
        config.setPollNameServerInterval(60_000);
        config.setHeartbeatBrokerInterval(60_000);
        config.setPersistConsumerOffsetInterval(60_000);
        org.apache.rocketmq.client.impl.factory.MQClientInstance runtime =
            new org.apache.rocketmq.client.impl.factory.MQClientInstance(
                config, 0, config.buildMQClientId());
        Map<String, Object> result = new TreeMap<>();
        try {
            java.lang.reflect.Field stateField = runtime.getClass().getDeclaredField("serviceState");
            stateField.setAccessible(true);
            stateField.set(runtime, Enum.valueOf((Class) stateField.getType(), "RUNNING"));
            result.put("stateBeforeShutdown", stateField.get(runtime).toString());
            runtime.shutdown();
            result.put("shutdownCall", "OK");
            result.put("stateAfterShutdown", stateField.get(runtime).toString());
            result.put("schedulerShutdownAfterShutdown",
                runtime.getScheduledExecutorService().isShutdown());
            try {
                runtime.start();
                result.put("secondStart", "OK");
            } catch (RuntimeException error) {
                result.put("secondStart", error.getClass().getSimpleName());
            }
            result.put("stateAfterSecondStart", stateField.get(runtime).toString());
            result.put("schedulerShutdownAfterSecondStart",
                runtime.getScheduledExecutorService().isShutdown());
        } finally {
            // Only the state-machine terminal branch is started here; lower
            // workers were never activated, so this also guarantees teardown.
            cleanupRuntime(runtime);
        }
        return result;
    }

    private static List<Object> serviceLifecycleCases() throws InterruptedException {
        List<Object> cases = new ArrayList<>();
        WorkerRunProbe rebalanceProbe = new WorkerRunProbe();
        org.apache.rocketmq.client.impl.consumer.RebalanceService rebalance =
            new org.apache.rocketmq.client.impl.consumer.RebalanceService(null) {
                @Override
                public void run() {
                    rebalanceProbe.run();
                }
            };
        cases.add(serviceLifecycleCase(rebalance, rebalanceProbe));

        WorkerRunProbe pullProbe = new WorkerRunProbe();
        org.apache.rocketmq.client.impl.consumer.PullMessageService pull =
            new org.apache.rocketmq.client.impl.consumer.PullMessageService(null) {
                @Override
                public void run() {
                    pullProbe.run();
                }
            };
        cases.add(serviceLifecycleCase(pull, pullProbe));
        return cases;
    }

    private static Map<String, Object> serviceLifecycleCase(
        org.apache.rocketmq.common.ServiceThread service, WorkerRunProbe probe
    ) throws InterruptedException {
        service.start();
        if (!probe.started.await(5, TimeUnit.SECONDS)) {
            probe.release.countDown();
            service.shutdown(true);
            throw new IllegalStateException("ServiceThread probe did not start: " + service.getServiceName());
        }
        service.start();
        boolean stoppedAfterStart = service.isStopped();
        service.makeStop();
        boolean stoppedAfterMakeStop = service.isStopped();
        boolean threadAliveAfterMakeStop = probe.thread.get().isAlive();
        probe.release.countDown();
        service.shutdown(true);

        Map<String, Object> row = new TreeMap<>();
        row.put("class", service.getServiceName());
        row.put("runsAfterTwoStarts", probe.runs.get());
        row.put("stoppedAfterStart", stoppedAfterStart);
        row.put("stoppedAfterMakeStop", stoppedAfterMakeStop);
        row.put("threadAliveAfterMakeStop", threadAliveAfterMakeStop);
        row.put("stoppedAfterShutdown", service.isStopped());
        row.put("threadAliveAfterShutdown", probe.thread.get().isAlive());
        if (service instanceof org.apache.rocketmq.client.impl.consumer.PullMessageService) {
            org.apache.rocketmq.client.impl.consumer.PullMessageService pull =
                (org.apache.rocketmq.client.impl.consumer.PullMessageService) service;
            row.put("scheduledExecutorShutdown", pull.getScheduledExecutorService().isShutdown());
        }
        return row;
    }

    private static Map<String, Object> serviceThreadMakeStopWakeupCase() throws InterruptedException {
        WaitForRunningService service = new WaitForRunningService();
        service.start();
        boolean enteredWait = service.enteredWait.await(5, TimeUnit.SECONDS);
        service.makeStop();
        boolean returnedAfterMakeStop = service.returned.await(1, TimeUnit.SECONDS);
        service.shutdown(true);

        Map<String, Object> row = new TreeMap<>();
        row.put("enteredWait", enteredWait);
        row.put("stoppedAfterMakeStop", service.isStopped());
        row.put("returnedAfterMakeStop", returnedAfterMakeStop);
        row.put("workerAliveAfterShutdown", service.workerAlive());
        return row;
    }

    private static final class WaitForRunningService extends org.apache.rocketmq.common.ServiceThread {
        private final CountDownLatch enteredWait = new CountDownLatch(1);
        private final CountDownLatch returned = new CountDownLatch(1);

        @Override
        public void run() {
            enteredWait.countDown();
            waitForRunning(60_000);
            returned.countDown();
        }

        private boolean workerAlive() {
            return thread != null && thread.isAlive();
        }
    }

    private static final class WorkerRunProbe {
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicReference<Thread> thread = new AtomicReference<>();
        private final AtomicInteger runs = new AtomicInteger();

        private void run() {
            thread.set(Thread.currentThread());
            runs.incrementAndGet();
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
