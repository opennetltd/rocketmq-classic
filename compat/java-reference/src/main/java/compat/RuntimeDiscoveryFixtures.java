package compat;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.namesrv.DefaultTopAddressing;
import org.apache.rocketmq.common.namesrv.TopAddressing;

/** Actual pinned URL/normalization/callback methods; no external network. */
public final class RuntimeDiscoveryFixtures {
    private RuntimeDiscoveryFixtures() { }
    public static void main(String[] args) throws Exception {
        List<Object> normalization = new ArrayList<>();
        Method clear = DefaultTopAddressing.class.getDeclaredMethod("clearNewLine", String.class);
        clear.setAccessible(true);
        for (String input : new String[] {"", " \t\r\n", " a;b \n", "a\nb\rc", "a\rb\nc", "\u0000a\u0020", "\u00a0a\u00a0", "\u3000"}) {
            Map<String,Object> item = new TreeMap<>();
            item.put("input",input); item.put("result",clear.invoke(null,input)); normalization.add(item);
        }
        List<Object> urls = new ArrayList<>();
        String domainKey = "rocketmq.namesrv.domain", subgroupKey = "rocketmq.namesrv.domain.subgroup";
        String oldDomain = System.getProperty(domainKey), oldSubgroup = System.getProperty(subgroupKey);
        try {
            for (String domain : new String[] {"jmenv.tbsite.net", "host:81", "[::1]:8080", "", ":bad"}) {
                System.setProperty(domainKey,domain); System.setProperty(subgroupKey,"custom");
                Map<String,Object> item = new TreeMap<>(); item.put("domain",domain); item.put("subgroup","custom");
                item.put("result",MixAll.getWSAddr()); urls.add(item);
            }
        } finally {
            if (oldDomain == null) System.clearProperty(domainKey); else System.setProperty(domainKey,oldDomain);
            if (oldSubgroup == null) System.clearProperty(subgroupKey); else System.setProperty(subgroupKey,oldSubgroup);
        }
        List<Object> callbacks = new ArrayList<>();
        ClientConfig config = new ClientConfig(); config.setClientIP("192.0.2.6"); config.setNamesrvAddr("explicit:9876");
        MQClientInstance runtime = new MQClientInstance(config,0,config.buildMQClientId());
        try {
            for (String input : new String[] {null,"a:9876;b:9876;;","",";",null,"changed:9876"}) {
                Map<String,Object> item = new TreeMap<>(); item.put("input",input);
                item.put("result",runtime.getMQClientAPIImpl().onNameServerAddressChange(input));
                List<String> addresses = new ArrayList<>(runtime.getMQClientAPIImpl().getNameServerAddressList());
                Collections.sort(addresses); item.put("addresses",addresses); callbacks.add(item);
            }
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class,Throwable.class);
            cleanup.setAccessible(true); cleanup.invoke(runtime,runtime.getMQClientAPIImpl(),new IllegalStateException("fixture teardown"));
        }
        Map<String,Object> fixture = new TreeMap<>();
        fixture.put("client","org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit","efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint","DefaultTopAddressing.clearNewLine; MixAll.getWSAddr; MQClientAPIImpl.onNameServerAddressChange");
        fixture.put("serverBaselineContacted",false); fixture.put("normalization",normalization); fixture.put("urls",urls); fixture.put("callbacks",callbacks);
        fixture.put("providerSequence", providerSequence());
        fixture.put("nsAddrProperty", nsAddrProperty());
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture,com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else { Files.createDirectories(file.getParent()); Files.writeString(file,json); }
        System.out.println("Runtime discovery invoked cases verified: " + (normalization.size()+urls.size()+callbacks.size()+2));
    }

    private static Map<String, Object> providerSequence() throws Exception {
        List<String> fetchCalls = new ArrayList<>();
        List<String> callbackCalls = new ArrayList<>();
        String[] names = {"first", "second", "third"};
        String[] addresses = {null, "second:9876", "third:9876"};
        List<TopAddressing> providers = new ArrayList<>();
        for (int index = 0; index < names.length; index++) {
            String name = names[index];
            String address = addresses[index];
            providers.add((TopAddressing) java.lang.reflect.Proxy.newProxyInstance(
                RuntimeDiscoveryFixtures.class.getClassLoader(),
                new Class<?>[] {TopAddressing.class},
                (proxy, method, arguments) -> {
                    if ("fetchNSAddr".equals(method.getName())) {
                        fetchCalls.add(name);
                        return address;
                    }
                    if ("registerChangeCallBack".equals(method.getName())) {
                        callbackCalls.add(name);
                    }
                    return null;
                }));
        }
        DefaultTopAddressing addressing = new DefaultTopAddressing("http://unused.invalid", null);
        java.lang.reflect.Field field = DefaultTopAddressing.class.getDeclaredField("topAddressingList");
        field.setAccessible(true);
        field.set(addressing, providers);
        Map<String, Object> result = new TreeMap<>();
        result.put("fetchResult", addressing.fetchNSAddr());
        result.put("fetchCalls", fetchCalls);
        addressing.registerChangeCallBack(changed -> changed);
        result.put("callbackCalls", callbackCalls);
        return result;
    }

    private static Map<String, Object> nsAddrProperty() {
        DefaultTopAddressing addressing = new DefaultTopAddressing("http://unused.invalid", null);
        Map<String, Object> result = new TreeMap<>();
        result.put("initial", addressing.getNsAddr());
        addressing.setNsAddr("discovered:9876");
        result.put("afterSet", addressing.getNsAddr());
        addressing.setNsAddr(null);
        result.put("afterClear", addressing.getNsAddr());
        return result;
    }
}
