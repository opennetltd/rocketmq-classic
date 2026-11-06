package compat;

import com.alibaba.fastjson2.JSON;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.common.NameserverAccessConfig;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;

/** Direct invocation of the pinned public MQClientAPIFactory surface. */
public final class RuntimeApiFactoryFixtures {
    private RuntimeApiFactoryFixtures() { }

    public static void main(String[] args) throws Exception {
        Path fixturePath = Path.of(args[0]);
        Map<String, Object> fixture = observe();
        String json = JSON.toJSONString(fixture) + "\n";
        if (Arrays.asList(args).contains("--verify")) {
            String expected = Files.readString(fixturePath);
            if (!expected.equals(json)) {
                throw new IllegalStateException("MQClientAPIFactory fixture drift\nexpected="
                    + expected + "\nactual=" + json);
            }
            System.out.println("Runtime MQClientAPIFactory invoked cases verified");
        } else {
            Files.createDirectories(fixturePath.getParent());
            Files.writeString(fixturePath, json);
            System.out.println("Wrote " + fixturePath);
        }
    }

    private static Map<String, Object> observe() throws Exception {
        String vipProperty = ClientConfig.SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY;
        String namesrvAddressProperty = "rocketmq.namesrv.addr";
        String domainProperty = "rocketmq.namesrv.domain";
        String subgroupProperty = "rocketmq.namesrv.domain.subgroup";
        Map<String, String> previous = new TreeMap<>();
        for (String key : List.of(vipProperty, namesrvAddressProperty, domainProperty, subgroupProperty)) {
            previous.put(key, System.getProperty(key));
        }

        Map<String, Object> fixture = new TreeMap<>();
        try {
            System.setProperty(vipProperty, "true");
            System.setProperty(domainProperty, "before.example");
            System.setProperty(subgroupProperty, "before-subgroup");

            String nullAccessConfigError;
            try {
                new MQClientAPIFactory(null, "null-access-", 0, null, null, null);
                nullAccessConfigError = "NONE";
            } catch (Throwable error) {
                nullAccessConfigError = error.getClass().getSimpleName();
            }
            fixture.put("nullNameserverAccessConfigError", nullAccessConfigError);
            fixture.put("vipPropertyAfterNullNameserverAccessConfig", System.getProperty(vipProperty));

            MQClientAPIFactory nullPrefixFactory = new MQClientAPIFactory(
                new NameserverAccessConfig("127.0.0.1:9876", "", null),
                null, 1, null, null, null);
            try {
                nullPrefixFactory.start();
                fixture.put("nullNamePrefixInstanceName", instanceName(nullPrefixFactory.getClients()[0]));
            } finally {
                nullPrefixFactory.shutdown();
            }

            AtomicInteger nullPrefixInjectedCreatorCalls = new AtomicInteger();
            MQClientAPIFactory nullPrefixInjectedFactory = new MQClientAPIFactory(
                new NameserverAccessConfig("127.0.0.1:9876", "", null),
                null, 1, null, null, null,
                args -> {
                    nullPrefixInjectedCreatorCalls.incrementAndGet();
                    return new NettyRemotingClient(
                        (NettyClientConfig) args[0], (ChannelEventListener) args[1]);
                });
            try {
                nullPrefixInjectedFactory.start();
                fixture.put("nullNamePrefixInjectedCreatorInstanceName",
                    instanceName(nullPrefixInjectedFactory.getClients()[0]));
                fixture.put("nullNamePrefixInjectedCreatorCalls", nullPrefixInjectedCreatorCalls.get());
            } finally {
                nullPrefixInjectedFactory.shutdown();
            }

            System.setProperty(vipProperty, "true");
            System.setProperty(domainProperty, "before.example");
            System.setProperty(subgroupProperty, "before-subgroup");

            ScheduledExecutorService missingAddressScheduler = Executors.newSingleThreadScheduledExecutor();
            try {
                String missingAddressError;
                try {
                    new MQClientAPIFactory(new NameserverAccessConfig(null, "", null),
                        "missing-", 0, null, null, missingAddressScheduler);
                    missingAddressError = "NONE";
                } catch (Throwable error) {
                    missingAddressError = error.getClass().getSimpleName();
                }
                fixture.put("missingStaticAddressError", missingAddressError);
                fixture.put("vipPropertyAfterMissingStaticAddress", System.getProperty(vipProperty));
            } finally {
                missingAddressScheduler.shutdownNow();
            }

            System.setProperty(vipProperty, "true");
            System.setProperty(domainProperty, "before.example");
            System.setProperty(subgroupProperty, "before-subgroup");
            ScheduledExecutorService missingSubgroupScheduler = Executors.newSingleThreadScheduledExecutor();
            try {
                String missingSubgroupError;
                try {
                    new MQClientAPIFactory(new NameserverAccessConfig(null,
                        "failed-domain.example", null), "bad-domain-", 0, null, null,
                        missingSubgroupScheduler);
                    missingSubgroupError = "NONE";
                } catch (Throwable error) {
                    missingSubgroupError = error.getClass().getSimpleName();
                }
                fixture.put("missingDomainSubgroupError", missingSubgroupError);
                fixture.put("vipPropertyAfterMissingDomainSubgroup", System.getProperty(vipProperty));
                fixture.put("domainPropertyAfterMissingSubgroup", System.getProperty(domainProperty));
                fixture.put("subgroupPropertyAfterMissingSubgroup", System.getProperty(subgroupProperty));
            } finally {
                missingSubgroupScheduler.shutdownNow();
            }

            System.setProperty(vipProperty, "true");
            System.setProperty(domainProperty, "before.example");
            System.setProperty(subgroupProperty, "before-subgroup");
            NameserverAccessConfig access = new NameserverAccessConfig(
                "127.0.0.1:9876;127.0.0.2:9876", "", null);
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            MQClientAPIFactory factory = new MQClientAPIFactory(access, "fixture-", 2, null, null, scheduler);
            try {
                fixture.put("nameserverAccessConfig", Arrays.asList(
                    access.getNamesrvAddr(), access.getNamesrvDomain(), access.getNamesrvDomainSubgroup()));
                fixture.put("vipPropertyAfterInit", System.getProperty(vipProperty));
                fixture.put("namesrvAddressPropertyAfterInit", System.getProperty(namesrvAddressProperty));
                fixture.put("clientsNullBeforeStart", factory.getClients() == null);
                String beforeStartError;
                try {
                    factory.getClient();
                    beforeStartError = "NONE";
                } catch (Throwable error) {
                    beforeStartError = error.getClass().getSimpleName();
                }
                fixture.put("getClientBeforeStartError", beforeStartError);
                String nullAddressBeforeStartError;
                try {
                    factory.onNameServerAddressChange(null);
                    nullAddressBeforeStartError = "NONE";
                } catch (Throwable error) {
                    nullAddressBeforeStartError = error.getClass().getSimpleName();
                }
                fixture.put("nullAddressBeforeStartError", nullAddressBeforeStartError);

                factory.start();
                MQClientAPIExt[] clients = factory.getClients();
                fixture.put("clientCountAfterStart", clients.length);
                fixture.put("clientInstanceNames", Arrays.stream(clients).map(RuntimeApiFactoryFixtures::instanceName).collect(Collectors.toList()));
                fixture.put("clientAddressesAfterStart", Arrays.stream(clients)
                    .map(client -> sorted(client.getNameServerAddressList())).collect(Collectors.toList()));
                MQClientAPIExt selected = factory.getClient();
                fixture.put("selectedClientBelongsToPool", Arrays.stream(clients).anyMatch(client -> client == selected));
                MQClientAPIExt first = clients[0];
                clients[0] = clients[1];
                fixture.put("getClientsReturnsSameArray", factory.getClients() == clients);
                fixture.put("arrayMutationVisible", factory.getClients()[0] == clients[1]);
                clients[0] = first;

                MQClientAPIExt second = clients[1];
                clients[1] = null;
                fixture.put("addressChangeWithNullSlotInput", "127.0.0.4:9876");
                String changedAddressNullSlotError;
                try {
                    factory.onNameServerAddressChange("127.0.0.4:9876");
                    changedAddressNullSlotError = "NONE";
                } catch (Throwable error) {
                    changedAddressNullSlotError = error.getClass().getSimpleName();
                }
                fixture.put("addressChangeWithNullSlotError", changedAddressNullSlotError);
                fixture.put("clientAddressesAfterAddressChangeNullSlotFailure", Arrays.stream(clients)
                    .map(client -> client == null ? null : sorted(client.getNameServerAddressList()))
                    .collect(Collectors.toList()));
                String nullAddressNullSlotError;
                try {
                    factory.onNameServerAddressChange(null);
                    nullAddressNullSlotError = "NONE";
                } catch (Throwable error) {
                    nullAddressNullSlotError = error.getClass().getSimpleName();
                }
                fixture.put("nullAddressWithNullSlotError", nullAddressNullSlotError);
                fixture.put("clientAddressesAfterNullAddressNullSlotFailure", Arrays.stream(clients)
                    .map(client -> client == null ? null : sorted(client.getNameServerAddressList()))
                    .collect(Collectors.toList()));
                clients[1] = second;

                factory.onNameServerAddressChange("127.0.0.3:9876");
                fixture.put("clientAddressesAfterChange", Arrays.stream(factory.getClients())
                    .map(client -> sorted(client.getNameServerAddressList())).collect(Collectors.toList()));
                factory.onNameServerAddressChange(null);
                fixture.put("clientAddressesAfterNullChange", Arrays.stream(factory.getClients())
                    .map(client -> sorted(client.getNameServerAddressList())).collect(Collectors.toList()));
                clients[1] = null;
                String shutdownWithNullSlotError;
                try {
                    factory.shutdown();
                    shutdownWithNullSlotError = "NONE";
                } catch (Throwable error) {
                    shutdownWithNullSlotError = error.getClass().getSimpleName();
                }
                fixture.put("shutdownWithNullSlotError", shutdownWithNullSlotError);
                factory.shutdown();
                fixture.put("clientArrayRetainedAfterShutdown", factory.getClients() == clients);
                // The Java factory cannot shut down an object after its public
                // array slot is replaced with null; close this fixture-owned
                // client explicitly so its Netty event loops do not outlive
                // the oracle process.
                second.shutdown();
            } finally {
                try {
                    factory.shutdown();
                } catch (Throwable ignored) {
                    // The active path has already shut down; cleanup is best effort.
                }
                scheduler.shutdownNow();
            }

            ScheduledExecutorService emptyScheduler = Executors.newSingleThreadScheduledExecutor();
            MQClientAPIFactory emptyFactory = new MQClientAPIFactory(
                new NameserverAccessConfig("127.0.0.1:9876", "", null),
                "empty-", 0, null, null, emptyScheduler);
            try {
                emptyFactory.start();
                String emptyGetClientError;
                try {
                    emptyFactory.getClient();
                    emptyGetClientError = "NONE";
                } catch (Throwable error) {
                    emptyGetClientError = error.getClass().getSimpleName();
                }
                fixture.put("zeroClientGetClientError", emptyGetClientError);
                String emptyShutdownError;
                try {
                    emptyFactory.shutdown();
                    emptyShutdownError = "NONE";
                } catch (Throwable error) {
                    emptyShutdownError = error.getClass().getSimpleName();
                }
                fixture.put("zeroClientShutdownError", emptyShutdownError);
            } finally {
                emptyScheduler.shutdownNow();
            }

            ScheduledExecutorService negativeScheduler = Executors.newSingleThreadScheduledExecutor();
            MQClientAPIFactory negativeFactory = new MQClientAPIFactory(
                new NameserverAccessConfig("127.0.0.1:9876", "", null),
                "negative-", -1, null, null, negativeScheduler);
            try {
                String negativeCountStartError;
                try {
                    negativeFactory.start();
                    negativeCountStartError = "NONE";
                } catch (Throwable error) {
                    negativeCountStartError = error.getClass().getSimpleName();
                }
                fixture.put("negativeClientCountStartError", negativeCountStartError);
                fixture.put("clientsNullAfterNegativeCountStart", negativeFactory.getClients() == null);
            } finally {
                negativeScheduler.shutdownNow();
            }

            System.setProperty(domainProperty, "factory-domain.example");
            System.setProperty(subgroupProperty, "factory-subgroup");
            ScheduledExecutorService domainScheduler = Executors.newSingleThreadScheduledExecutor();
            try {
                new MQClientAPIFactory(new NameserverAccessConfig(null, "factory-domain.example",
                    "factory-subgroup"), "domain-", 0, null, null, domainScheduler);
                fixture.put("domainPropertyAfterInit", System.getProperty(domainProperty));
                fixture.put("domainSubgroupPropertyAfterInit", System.getProperty(subgroupProperty));
            } finally {
                domainScheduler.shutdownNow();
            }

            MQClientAPIFactory staticNullSchedulerFactory = new MQClientAPIFactory(
                new NameserverAccessConfig("127.0.0.1:9876", "", null),
                "static-null-scheduler-", 1, null, null, null);
            String staticNullSchedulerStartError;
            try {
                staticNullSchedulerFactory.start();
                staticNullSchedulerStartError = "NONE";
                staticNullSchedulerFactory.shutdown();
            } catch (Throwable error) {
                staticNullSchedulerStartError = error.getClass().getSimpleName();
            }
            fixture.put("staticNullSchedulerStartError", staticNullSchedulerStartError);

            ServerSocket nullSchedulerDiscoveryServer = new ServerSocket(0);
            nullSchedulerDiscoveryServer.setSoTimeout(5000);
            AtomicReference<Throwable> nullSchedulerDiscoveryFailure = new AtomicReference<>();
            Thread nullSchedulerDiscoveryResponder = new Thread(() -> {
                try (Socket socket = nullSchedulerDiscoveryServer.accept()) {
                    socket.setSoTimeout(5000);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(
                        socket.getInputStream(), StandardCharsets.UTF_8));
                    while (true) {
                        String line = reader.readLine();
                        if (line == null || line.isEmpty()) {
                            break;
                        }
                    }
                    byte[] body = "127.0.0.13:9876".getBytes(StandardCharsets.UTF_8);
                    OutputStream output = socket.getOutputStream();
                    output.write(("HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=UTF-8\r\n"
                        + "Content-Length: " + body.length + "\r\nConnection: close\r\n\r\n")
                        .getBytes(StandardCharsets.US_ASCII));
                    output.write(body);
                    output.flush();
                } catch (Throwable error) {
                    nullSchedulerDiscoveryFailure.set(error);
                }
            }, "api-factory-null-scheduler-discovery-fixture");
            nullSchedulerDiscoveryResponder.setDaemon(true);
            nullSchedulerDiscoveryResponder.start();
            MQClientAPIFactory nullSchedulerFactory = new MQClientAPIFactory(
                new NameserverAccessConfig(null,
                    "127.0.0.1:" + nullSchedulerDiscoveryServer.getLocalPort(), "factory-fixture"),
                "domain-null-scheduler-", 1, null, null, null);
            String domainNullSchedulerStartError;
            try {
                nullSchedulerFactory.start();
                domainNullSchedulerStartError = "NONE";
            } catch (Throwable error) {
                domainNullSchedulerStartError = error.getClass().getSimpleName();
            }
            fixture.put("domainNullSchedulerStartError", domainNullSchedulerStartError);
            fixture.put("domainNullSchedulerClientSlots", Arrays.stream(nullSchedulerFactory.getClients())
                .map(client -> client == null ? null : "started")
                .collect(Collectors.toList()));
            nullSchedulerDiscoveryServer.close();
            nullSchedulerDiscoveryResponder.join(5000);
            if (nullSchedulerDiscoveryResponder.isAlive()) {
                throw new IllegalStateException("Null-scheduler NameServer discovery fixture did not finish");
            }
            if (nullSchedulerDiscoveryFailure.get() != null) {
                throw new IllegalStateException("Null-scheduler NameServer discovery fixture failed",
                    nullSchedulerDiscoveryFailure.get());
            }

            String[] discoveredAddresses = {"127.0.0.11:9876", "127.0.0.12:9876"};
            ServerSocket discoveryServer = new ServerSocket(0);
            discoveryServer.setSoTimeout(5000);
            AtomicReference<Throwable> discoveryFailure = new AtomicReference<>();
            Thread discoveryResponder = new Thread(() -> {
                try {
                    for (String address : discoveredAddresses) {
                        try (Socket socket = discoveryServer.accept()) {
                            socket.setSoTimeout(5000);
                            BufferedReader reader = new BufferedReader(new InputStreamReader(
                                socket.getInputStream(), StandardCharsets.UTF_8));
                            while (true) {
                                String line = reader.readLine();
                                if (line == null || line.isEmpty()) {
                                    break;
                                }
                            }
                            byte[] body = address.getBytes(StandardCharsets.UTF_8);
                            OutputStream output = socket.getOutputStream();
                            output.write(("HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=UTF-8\r\n"
                                + "Content-Length: " + body.length + "\r\nConnection: close\r\n\r\n")
                                .getBytes(StandardCharsets.US_ASCII));
                            output.write(body);
                            output.flush();
                        }
                    }
                } catch (Throwable error) {
                    discoveryFailure.set(error);
                }
            }, "api-factory-discovery-fixture");
            discoveryResponder.setDaemon(true);
            discoveryResponder.start();
            ScheduledThreadPoolExecutor discoveryScheduler = new ScheduledThreadPoolExecutor(1);
            MQClientAPIFactory discoveryFactory = new MQClientAPIFactory(
                new NameserverAccessConfig(null, "127.0.0.1:" + discoveryServer.getLocalPort(),
                    "factory-fixture"), "domain-discovery-", 2, null, null, discoveryScheduler);
            try {
                discoveryFactory.start();
                fixture.put("domainRefreshScheduleCountAfterStart", discoveryScheduler.getQueue().size());
                fixture.put("domainClientAddressesAfterStart", Arrays.stream(discoveryFactory.getClients())
                    .map(client -> sorted(client.getNameServerAddressList())).collect(Collectors.toList()));
                discoveryFactory.shutdown();
            } finally {
                try {
                    discoveryFactory.shutdown();
                } catch (Throwable ignored) {
                    // The active path has already shut down; cleanup is best effort.
                }
                discoveryScheduler.shutdownNow();
                discoveryServer.close();
            }
            discoveryResponder.join(5000);
            if (discoveryResponder.isAlive()) {
                throw new IllegalStateException("NameServer discovery fixture did not finish");
            }
            if (discoveryFailure.get() != null) {
                throw new IllegalStateException("NameServer discovery fixture failed", discoveryFailure.get());
            }

            AtomicInteger rejectedScheduleCalls = new AtomicInteger();
            ScheduledThreadPoolExecutor rejectingScheduler = new ScheduledThreadPoolExecutor(1) {
                @Override
                public ScheduledFuture<?> scheduleAtFixedRate(
                    Runnable command, long initialDelay, long period, TimeUnit unit
                ) {
                    rejectedScheduleCalls.incrementAndGet();
                    throw new RejectedExecutionException("fixture rejection");
                }
            };
            ServerSocket rejectedDiscoveryServer = new ServerSocket(0);
            rejectedDiscoveryServer.setSoTimeout(5000);
            AtomicReference<Throwable> rejectedDiscoveryFailure = new AtomicReference<>();
            Thread rejectedDiscoveryResponder = new Thread(() -> {
                try (Socket socket = rejectedDiscoveryServer.accept()) {
                    socket.setSoTimeout(5000);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(
                        socket.getInputStream(), StandardCharsets.UTF_8));
                    while (true) {
                        String line = reader.readLine();
                        if (line == null || line.isEmpty()) {
                            break;
                        }
                    }
                    byte[] body = "127.0.0.14:9876".getBytes(StandardCharsets.UTF_8);
                    OutputStream output = socket.getOutputStream();
                    output.write(("HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=UTF-8\r\n"
                        + "Content-Length: " + body.length + "\r\nConnection: close\r\n\r\n")
                        .getBytes(StandardCharsets.US_ASCII));
                    output.write(body);
                    output.flush();
                } catch (Throwable error) {
                    rejectedDiscoveryFailure.set(error);
                }
            }, "api-factory-rejected-discovery-fixture");
            rejectedDiscoveryResponder.setDaemon(true);
            rejectedDiscoveryResponder.start();
            MQClientAPIFactory rejectedFactory = new MQClientAPIFactory(
                new NameserverAccessConfig(null,
                    "127.0.0.1:" + rejectedDiscoveryServer.getLocalPort(), "factory-fixture"),
                "domain-rejected-", 1, null, null, rejectingScheduler);
            try {
                String rejectedStartError;
                try {
                    rejectedFactory.start();
                    rejectedStartError = "NONE";
                } catch (Throwable error) {
                    rejectedStartError = error.getClass().getSimpleName();
                }
                fixture.put("domainRefreshRejectedStartError", rejectedStartError);
                fixture.put("domainRefreshRejectedScheduleCalls", rejectedScheduleCalls.get());
                fixture.put("domainRefreshRejectedClientSlots", Arrays.stream(rejectedFactory.getClients())
                    .map(client -> client == null ? null : "started")
                    .collect(Collectors.toList()));
            } finally {
                rejectingScheduler.shutdownNow();
                rejectedDiscoveryServer.close();
            }
            rejectedDiscoveryResponder.join(5000);
            if (rejectedDiscoveryResponder.isAlive()) {
                throw new IllegalStateException("Rejected NameServer discovery fixture did not finish");
            }
            if (rejectedDiscoveryFailure.get() != null) {
                throw new IllegalStateException("Rejected NameServer discovery fixture failed",
                    rejectedDiscoveryFailure.get());
            }

            List<String> constructors = new ArrayList<>();
            for (Constructor<?> constructor : MQClientAPIFactory.class.getConstructors()) {
                constructors.add(constructor.toGenericString());
            }
            constructors.sort(String::compareTo);
            fixture.put("publicConstructors", constructors);
            List<String> methods = new ArrayList<>();
            for (Method method : MQClientAPIFactory.class.getMethods()) {
                if (method.getDeclaringClass() != Object.class) {
                    methods.add(method.toGenericString());
                }
            }
            methods.sort(String::compareTo);
            fixture.put("publicMethods", methods);
            return fixture;
        } finally {
            for (Map.Entry<String, String> entry : previous.entrySet()) {
                if (entry.getValue() == null) System.clearProperty(entry.getKey());
                else System.setProperty(entry.getKey(), entry.getValue());
            }
        }
    }

    private static String instanceName(MQClientAPIExt client) {
        try {
            var field = MQClientAPIExt.class.getDeclaredField("clientConfig");
            field.setAccessible(true);
            return ((ClientConfig) field.get(client)).getInstanceName();
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException(error);
        }
    }

    private static List<String> sorted(List<String> addresses) {
        List<String> result = new ArrayList<>(addresses);
        result.sort(String::compareTo);
        return result;
    }
}
