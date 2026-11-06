package compat;

import com.alibaba.fastjson2.JSON;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragelyByCircle;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueByConfig;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueByMachineRoom;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueConsistentHash;
import org.apache.rocketmq.common.message.MessageQueue;

/** Java 5.5.1 allocation oracle. This invokes the public client strategies directly. */
public final class RebalanceFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private RebalanceFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("usage: RebalanceFixtures <fixture.json> [--verify]");
        }
        Path output = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(generate()) + "\n";
        if (verify) {
            if (!Files.exists(output) || !json.equals(Files.readString(output))) {
                throw new IllegalStateException("Java allocation fixture drift: " + output);
            }
            System.out.println("verified Java 5.5.1 allocation fixtures at " + output);
        } else {
            Files.createDirectories(output.getParent());
            Files.writeString(output, json);
            System.out.println("wrote Java 5.5.1 allocation fixtures to " + output);
        }
    }

    private static Map<String, Object> generate() {
        Map<String, Object> root = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("sourceCommit", SOURCE_COMMIT);
        provenance.put("sourceClasses", List.of(
            "AllocateMessageQueueStrategy",
            "AbstractAllocateMessageQueueStrategy",
            "AllocateMessageQueueAveragely",
            "AllocateMessageQueueAveragelyByCircle",
            "AllocateMessageQueueByConfig",
            "AllocateMessageQueueByMachineRoom",
            "AllocateMessageQueueConsistentHash",
            "HashFunction",
            "ConsistentHashRouter",
            "VirtualNode",
            "MessageQueue.compareTo",
            "MessageQueue.toString"));
        provenance.put("serverBaselineContacted", false);
        root.put("provenance", provenance);

        List<Map<String, Object>> cases = new ArrayList<>();
        List<Scenario> scenarios = List.of(
            scenario("zero_queues", List.of(), List.of("consumer-c", "consumer-a", "consumer-b")),
            scenario("one_queue_three_clients", queues(1, "orders"),
                List.of("consumer-c", "consumer-a", "consumer-b")),
            scenario("ten_queues_three_clients", queues(10, "orders"),
                List.of("consumer-c", "consumer-a", "consumer-b")),
            scenario("one_queue_one_client", queues(1, "orders"), List.of("consumer-a")),
            scenario("unicode_order", List.of(
                new MessageQueue("\uE000-topic", "roomB@broker-z", 0),
                new MessageQueue("😀-topic", "roomA@broker-y", 1),
                new MessageQueue("α-topic", "roomA@broker-x", 2),
                new MessageQueue("😀-topic", "roomA@broker-x", 0)),
                List.of("\uE000-client", "😀-client", "α-client")));

        for (String strategyName : List.of("AVG", "AVG_BY_CIRCLE", "CONFIG", "MACHINE_ROOM",
            "CONSISTENT_HASH", "CONSISTENT_HASH_CUSTOM")) {
            for (Scenario scenario : scenarios) {
                cases.add(runCase(strategyName, scenario));
            }
        }
        root.put("cases", cases);
        return root;
    }

    private static Scenario scenario(String id, List<MessageQueue> queues, List<String> consumers) {
        return new Scenario(id, queues, consumers);
    }

    private static List<MessageQueue> queues(int count, String topic) {
        List<MessageQueue> queues = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            queues.add(new MessageQueue(topic, "room" + (char) ('A' + i % 2) + "@broker-" + (i % 3), i));
        }
        return queues;
    }

    private static Map<String, Object> runCase(String strategyName, Scenario scenario) {
        AllocateMessageQueueStrategy strategy = strategy(strategyName, scenario.queues);
        List<MessageQueue> queues = new ArrayList<>(scenario.queues);
        List<String> consumers = new ArrayList<>(scenario.consumers);
        Collections.sort(queues);
        Collections.sort(consumers);

        Map<String, Object> result = new TreeMap<>();
        result.put("id", strategyName.toLowerCase() + "_" + scenario.id);
        result.put("strategy", strategyName);
        result.put("strategyName", strategy.getName());
        result.put("consumerGroup", "rebalance-fixture-group");
        result.put("queues", queueRecords(queues));
        result.put("consumerIds", consumers);
        result.put("strategyConfig", strategyConfig(strategyName, scenario.queues));
        Map<String, Object> assignments = new LinkedHashMap<>();
        for (String current : consumers) {
            assignments.put(current, allocate(strategy, current, queues, consumers));
        }
        String missingClient = "missing-client";
        assignments.put(missingClient, allocate(strategy, missingClient, queues, consumers));
        result.put("assignments", assignments);
        Map<String, Object> validation = new TreeMap<>();
        validation.put("emptyCurrentClient", validationOutcome(strategy, "", queues, consumers));
        validation.put("emptyQueueList", validationOutcome(strategy,
            consumers.isEmpty() ? "consumer-a" : consumers.get(0), List.of(), consumers));
        validation.put("emptyConsumerIdList", validationOutcome(strategy, "consumer-a", queues, List.of()));
        result.put("validation", validation);
        return result;
    }

    private static String validationOutcome(AllocateMessageQueueStrategy strategy, String current,
        List<MessageQueue> queues, List<String> consumers) {
        try {
            strategy.allocate("rebalance-fixture-group", current, queues, consumers);
            return "NONE";
        } catch (IllegalArgumentException error) {
            return error.getMessage();
        }
    }

    private static List<String> allocate(AllocateMessageQueueStrategy strategy, String current,
        List<MessageQueue> queues, List<String> consumers) {
        try {
            List<MessageQueue> assigned = strategy.allocate("rebalance-fixture-group", current, queues, consumers);
            if (assigned == null) {
                return null;
            }
            List<String> keys = new ArrayList<>();
            for (MessageQueue queue : assigned) {
                keys.add(queueKey(queue));
            }
            return keys;
        } catch (IllegalArgumentException error) {
            return null;
        }
    }

    private static AllocateMessageQueueStrategy strategy(String name, List<MessageQueue> queues) {
        switch (name) {
            case "AVG":
                return new AllocateMessageQueueAveragely();
            case "AVG_BY_CIRCLE":
                return new AllocateMessageQueueAveragelyByCircle();
            case "CONFIG":
                return byConfig(queues);
            case "MACHINE_ROOM":
                AllocateMessageQueueByMachineRoom byRoom = new AllocateMessageQueueByMachineRoom();
                byRoom.setConsumeridcs(new HashSet<>(Set.of("roomA", "roomB")));
                return byRoom;
            case "CONSISTENT_HASH":
                return new AllocateMessageQueueConsistentHash();
            case "CONSISTENT_HASH_CUSTOM":
                return new AllocateMessageQueueConsistentHash(10,
                    key -> key.hashCode() % 7 - 3);
            default:
                throw new IllegalArgumentException("unknown strategy " + name);
        }
    }

    private static AllocateMessageQueueByConfig byConfig(List<MessageQueue> queues) {
        AllocateMessageQueueByConfig byConfig = new AllocateMessageQueueByConfig();
        byConfig.setMessageQueueList(queues.isEmpty()
            ? Collections.emptyList() : List.of(queues.get(queues.size() - 1), queues.get(0)));
        return byConfig;
    }

    private static Map<String, Object> strategyConfig(String name, List<MessageQueue> queues) {
        Map<String, Object> config = new TreeMap<>();
        switch (name) {
            case "CONFIG":
                AllocateMessageQueueByConfig strategy = byConfig(queues);
                config.put("messageQueueList", queueRecords(strategy.getMessageQueueList()));
                break;
            case "MACHINE_ROOM":
                config.put("consumeridcs", List.of("roomA", "roomB"));
                break;
            case "CONSISTENT_HASH":
                config.put("virtualNodeCnt", 10);
                break;
            case "CONSISTENT_HASH_CUSTOM":
                config.put("virtualNodeCnt", 10);
                config.put("hashFunction", "java-string-hash-code-mod-7-minus-3");
                break;
            default:
                break;
        }
        return config;
    }

    private static List<Map<String, Object>> queueRecords(List<MessageQueue> queues) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (MessageQueue queue : queues) {
            Map<String, Object> record = new TreeMap<>();
            record.put("topic", queue.getTopic());
            record.put("brokerName", queue.getBrokerName());
            record.put("queueId", queue.getQueueId());
            result.add(record);
        }
        return result;
    }

    private static String queueKey(MessageQueue queue) {
        return queue.getTopic() + "|" + queue.getBrokerName() + "|" + queue.getQueueId();
    }

    private static final class Scenario {
        private final String id;
        private final List<MessageQueue> queues;
        private final List<String> consumers;

        private Scenario(String id, List<MessageQueue> queues, List<String> consumers) {
            this.id = id;
            this.queues = queues;
            this.consumers = consumers;
        }
    }
}
