package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.hook.CheckForbiddenContext;
import org.apache.rocketmq.client.hook.CheckForbiddenHook;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.ProduceAccumulator;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.compression.Compressor;
import org.apache.rocketmq.common.compression.CompressionType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

/** Offline Java 5.5.1 producer API and send-flow oracle. */
public final class ProducerCoreFixtures {
    private ProducerCoreFixtures() { }

    @FunctionalInterface
    private interface Check { void run() throws Exception; }

    private static Map<String, Object> row(Object input, boolean valid) {
        Map<String, Object> row = new TreeMap<>();
        row.put("input", input);
        row.put("valid", valid);
        return row;
    }

    private static boolean accepts(Check check) {
        try {
            check.run();
            return true;
        } catch (Exception expected) {
            return false;
        }
    }

    private static Map<String, Object> permissionOutcome(Object input, Check check) {
        Map<String, Object> result = new TreeMap<>();
        result.put("input", input);
        try {
            check.run();
            result.put("accepted", true);
            result.put("responseCode", -1);
            result.put("errorClass", "none");
            result.put("message", "");
        } catch (MQClientException error) {
            result.put("accepted", false);
            result.put("responseCode", error.getResponseCode());
            result.put("errorClass", error.getClass().getSimpleName());
            result.put("message", error.getMessage());
        } catch (Exception error) {
            result.put("accepted", false);
            result.put("responseCode", -1);
            result.put("errorClass", error.getClass().getSimpleName());
            result.put("message", error.getMessage());
        }
        return result;
    }

    private static long totalAccumulatorHoldBytes(DefaultMQProducer producer) {
        try {
            Field producerAccumulator = DefaultMQProducer.class.getDeclaredField("produceAccumulator");
            producerAccumulator.setAccessible(true);
            Object accumulator = producerAccumulator.get(producer);
            Field totalHoldSize = accumulator.getClass().getDeclaredField("totalHoldSize");
            totalHoldSize.setAccessible(true);
            return totalHoldSize.getLong(accumulator);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Could not inspect pinned accumulator limit", error);
        }
    }

    private static boolean isBatchable(DefaultMQProducer producer, Message message) {
        try {
            Method canBatch = DefaultMQProducer.class.getDeclaredMethod("canBatch", Message.class);
            canBatch.setAccessible(true);
            return (Boolean) canBatch.invoke(producer, message);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Could not invoke pinned auto-batch eligibility", error);
        }
    }

    private static Semaphore asyncSendNumSemaphore(DefaultMQProducer producer) {
        try {
            Field implementationField = DefaultMQProducer.class.getDeclaredField("defaultMQProducerImpl");
            implementationField.setAccessible(true);
            Object implementation = implementationField.get(producer);
            Field semaphoreField = implementation.getClass().getDeclaredField("semaphoreAsyncSendNum");
            semaphoreField.setAccessible(true);
            return (Semaphore) semaphoreField.get(implementation);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Could not inspect pinned async-send semaphore", error);
        }
    }

    private static boolean acceptsMessage(DefaultMQProducer producer, String topic, byte[] body,
        String multiDispatch) {
        Message message = new Message();
        message.setTopic(topic);
        message.setBody(body);
        if (multiDispatch != null) {
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_INNER_MULTI_DISPATCH, multiDispatch);
        }
        return accepts(() -> Validators.checkMessage(message, producer));
    }

    private static Map<String, Object> produce() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer();
        Map<String, Object> result = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        provenance.put("entrypoints", List.of("DefaultMQProducer", "Validators", "TopicValidator",
            "DefaultMQProducerImpl.tryToCompressMessage", "MessageClientIDSetter.createUniqID",
            "MessageBatch.generateFromList", "MessageBatch.encode", "MQFaultStrategy",
            "LatencyFaultToleranceImpl.detectByOneRound", "MQClientAPIImpl.getMaxOffset",
            "MQClientAPIImpl.resumeCheckHalfMessage"));
        provenance.put("serverContacted", false);
        result.put("provenance", provenance);
        result.put("producerImplementationApi", producerImplementationApi());
        result.put("producerFacadeApi", producerFacadeApi());
        result.put("validatorsApi", publicApiSurface(Validators.class));
        result.put("permNameApi", publicApiSurface(PermName.class));
        result.put("permNamePublicFields", publicFieldSignatures(PermName.class));
        result.put("getBrokerConfigApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getBrokerConfig"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("coldDataFlowAndReadAheadApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> Set.of("updateColdDataFlowCtrGroupConfig",
                "removeColdDataFlowCtrGroupConfig", "getColdDataFlowCtrInfo",
                "setCommitLogReadAheadMode").contains(method.getName()))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("coldDataFlowAndReadAheadOperations", coldDataFlowAndReadAheadOperations());
        result.put("registerClientApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("registerClient"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("registerClientOperations", registerClientOperations());
        result.put("topicListFromNameServerApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getTopicListFromNameServer"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("topicListFromNameServerOperations", topicListFromNameServerOperations());
        result.put("systemTopicListFromBrokerApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getSystemTopicListFromBroker"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("systemTopicListFromBrokerOperations", systemTopicListFromBrokerOperations());
        result.put("systemTopicListApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getSystemTopicList"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("systemTopicListOperations", systemTopicListOperations());
        result.put("createSubscriptionGroupApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("createSubscriptionGroup"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("createSubscriptionGroupOperations", createSubscriptionGroupOperations());
        result.put("kvConfigApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> Set.of("getKVConfigValue", "putKVConfigValue", "deleteKVConfigValue",
                "getKVListByNamespace").contains(method.getName()))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("kvConfigOperations", kvConfigOperations());
        result.put("brokerMaintenanceApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> Set.of("cleanExpiredConsumeQueue", "deleteExpiredCommitLog",
                "cleanUnusedTopicByAddr").contains(method.getName()))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("brokerMaintenanceOperations", brokerMaintenanceOperations());
        result.put("fetchConsumeStatsInBrokerApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("fetchConsumeStatsInBroker"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("getConsumeStatsInBrokerHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsInBrokerHeader.class));
        result.put("consumeStatsListApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.ConsumeStatsList.class));
        result.put("fetchConsumeStatsInBrokerOperations", fetchConsumeStatsInBrokerOperations());
        result.put("viewBrokerStatsDataApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("viewBrokerStatsData"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("viewBrokerStatsDataRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.ViewBrokerStatsDataRequestHeader.class));
        result.put("brokerStatsDataApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.BrokerStatsData.class));
        result.put("brokerStatsItemApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.BrokerStatsItem.class));
        result.put("viewBrokerStatsDataOperations", viewBrokerStatsDataOperations());
        result.put("getTopicConfigApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getTopicConfig"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("getAllTopicConfigApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getAllTopicConfig"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("getAllTopicConfigRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.GetAllTopicConfigRequestHeader.class));
        result.put("getAllTopicConfigResponseHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.GetAllTopicConfigResponseHeader.class));
        result.put("dataVersionApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.DataVersion.class));
        result.put("topicConfigSerializeWrapperApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper.class));
        result.put("getAllTopicConfigOperations", getAllTopicConfigOperations());
        result.put("getAllSubscriptionGroupApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getAllSubscriptionGroup"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("getAllSubscriptionGroupRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.GetAllSubscriptionGroupRequestHeader.class));
        result.put("getAllSubscriptionGroupResponseHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.GetAllSubscriptionGroupResponseHeader.class));
        result.put("subscriptionGroupWrapperApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper.class));
        result.put("metadataWrapperIdentity", metadataWrapperIdentity());
        result.put("getAllSubscriptionGroupOperations", getAllSubscriptionGroupOperations());
        result.put("queryConsumeQueueApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("queryConsumeQueue"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("queryConsumeQueueRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.QueryConsumeQueueRequestHeader.class));
        result.put("queryConsumeQueueResponseBodyApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody.class));
        result.put("consumeQueueDataApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.ConsumeQueueData.class));
        result.put("queryConsumeQueueOperations", queryConsumeQueueOperations());
        result.put("createStaticTopicApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("createStaticTopic"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("createStaticTopicRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader.class));
        result.put("staticTopicConfigApi", publicApiSurface(TopicConfig.class));
        result.put("staticTopicConfigPublicFields", publicFieldSignatures(TopicConfig.class));
        result.put("topicMessageTypeApi", publicApiSurface(TopicMessageType.class));
        result.put("topicConfigModelOperations", topicConfigModelOperations());
        result.put("createStaticTopicOperations", createStaticTopicOperations());
        result.put("createUserApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("createUser"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("createUserRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.CreateUserRequestHeader.class));
        result.put("userInfoApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.UserInfo.class));
        result.put("createUserOperations", createUserOperations());
        result.put("updateUserApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("updateUser"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("updateUserRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.UpdateUserRequestHeader.class));
        result.put("updateUserOperations", updateUserOperations());
        result.put("deleteUserApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("deleteUser"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("deleteUserRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.DeleteUserRequestHeader.class));
        result.put("deleteUserOperations", deleteUserOperations());
        result.put("listUserApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("listUser"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("listUsersRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.ListUsersRequestHeader.class));
        result.put("listUserOperations", listUserOperations());
        result.put("getUserApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getUser"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("getUserRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.GetUserRequestHeader.class));
        result.put("getUserOperations", getUserOperations());
        result.put("getAclApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getAcl"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("getAclRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.GetAclRequestHeader.class));
        result.put("aclInfoApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.AclInfo.class));
        result.put("aclPolicyInfoApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.AclInfo.PolicyInfo.class));
        result.put("aclPolicyEntryInfoApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.AclInfo.PolicyEntryInfo.class));
        result.put("getAclOperations", getAclOperations());
        result.put("createAclApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("createAcl"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("createAclRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.CreateAclRequestHeader.class));
        result.put("createAclOperations", createAclOperations());
        result.put("updateAclApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("updateAcl"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("updateAclRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.UpdateAclRequestHeader.class));
        result.put("updateAclOperations", updateAclOperations());
        result.put("deleteAclApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("deleteAcl"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("deleteAclRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.DeleteAclRequestHeader.class));
        result.put("deleteAclOperations", deleteAclOperations());
        result.put("listAclApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("listAcl"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("listAclsRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.ListAclsRequestHeader.class));
        result.put("listAclOperations", listAclOperations());
        result.put("getTopicConfigRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.GetTopicConfigRequestHeader.class));
        result.put("topicConfigAndQueueMappingApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping.class));
        result.put("topicQueueMappingDetailApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail.class));
        result.put("logicQueueMappingItemApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem.class));
        result.put("getTopicConfigOperations", getTopicConfigOperations());
        result.put("subscriptionGroupConfigApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getSubscriptionGroupConfig"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("subscriptionGroupConfigOperations", subscriptionGroupConfigOperations());
        result.put("createSubscriptionGroupListApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("createSubscriptionGroupList"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("createSubscriptionGroupListOperations", createSubscriptionGroupListOperations());
        result.put("createTopicListApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("createTopicList"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("createTopicListOperations", createTopicListOperations());
        result.put("deleteTopicInBrokerListApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("deleteTopicInBrokerList"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("deleteTopicInBrokerListOperations", deleteTopicInBrokerListOperations());
        result.put("deleteSubscriptionGroupListApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("deleteSubscriptionGroupList"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("deleteSubscriptionGroupListOperations", deleteSubscriptionGroupListOperations());
        result.put("getTopicsByClusterApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getTopicsByCluster"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("getTopicsByClusterOperations", getTopicsByClusterOperations());
        result.put("getClusterListApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getClusterList"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("getClusterListOperations", getClusterListOperations());
        result.put("resetMasterFlushOffsetApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("resetMasterFlushOffset"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("resetMasterFlushOffsetOperations", resetMasterFlushOffsetOperations());
        result.put("exportRocksDBConfigToJsonApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("exportRocksDBConfigToJson"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("exportRocksDBConfigTypeApi", Arrays.stream(
            org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader.ConfigType.class
                .getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("exportRocksDBConfigTypeParsing", exportRocksDBConfigTypeParsing());
        result.put("exportRocksDBConfigToJsonOperations", exportRocksDBConfigToJsonOperations());
        result.put("checkRocksdbCqWriteProgressApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("checkRocksdbCqWriteProgress"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("checkRocksdbCqWriteResultApi",
            publicApiSurface(org.apache.rocketmq.common.CheckRocksdbCqWriteResult.class));
        result.put("checkRocksdbCqWriteResultPublicFields", publicFieldSignatures(
            org.apache.rocketmq.common.CheckRocksdbCqWriteResult.class));
        result.put("checkRocksdbCqWriteStatusApi",
            publicApiSurface(org.apache.rocketmq.common.CheckRocksdbCqWriteResult.CheckStatus.class));
        result.put("checkRocksdbCqWriteStatusValues", Arrays.stream(
            org.apache.rocketmq.common.CheckRocksdbCqWriteResult.CheckStatus.values())
            .map(status -> {
                Map<String, Object> row = new TreeMap<>();
                row.put("name", status.name());
                row.put("ordinal", status.ordinal());
                row.put("value", status.getValue());
                return row;
            })
            .collect(java.util.stream.Collectors.toList()));
        result.put("checkRocksdbCqWriteProgressOperations", checkRocksdbCqWriteProgressOperations());
        result.put("controllerAdminApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> Set.of("getControllerMetaData", "getInSyncStateData", "getBrokerEpochCache",
                "getControllerConfig", "updateControllerConfig", "electMaster",
                "cleanControllerBrokerData").contains(method.getName()))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("controllerAdminOperations", controllerAdminOperations());
        result.put("controllerDtoApi", controllerDtoApi());
        result.put("controllerDtoOperations", controllerDtoOperations());
        result.put("wipeWritePermOfBrokerApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("wipeWritePermOfBroker"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("addWritePermOfBrokerApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("addWritePermOfBroker"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("writePermissionOperations", writePermissionOperations());
        result.put("addBrokerApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("addBroker"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("removeBrokerApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("removeBroker"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("cloneGroupOffsetApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("cloneGroupOffset"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("cloneGroupOffsetRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.CloneGroupOffsetRequestHeader.class));
        result.put("cloneGroupOffsetOperations", cloneGroupOffsetOperations());
        result.put("invokeBrokerToGetConsumerStatusApi",
            Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()))
                .filter(method -> method.getName().equals("invokeBrokerToGetConsumerStatus"))
                .map(ProducerCoreFixtures::methodSignature)
                .sorted()
                .collect(java.util.stream.Collectors.toList()));
        result.put("invokeBrokerToGetConsumerStatusOperations",
            invokeBrokerToGetConsumerStatusOperations());
        result.put("queryCorrectionOffsetApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("queryCorrectionOffset"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("queryCorrectionOffsetRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.QueryCorrectionOffsetHeader.class));
        result.put("queryCorrectionOffsetOperations", queryCorrectionOffsetOperations());
        result.put("updateAndGetGroupForbiddenApi",
            Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()))
                .filter(method -> method.getName().equals("updateAndGetGroupForbidden"))
                .map(ProducerCoreFixtures::methodSignature)
                .sorted()
                .collect(java.util.stream.Collectors.toList()));
        result.put("updateGroupForbiddenRequestHeaderApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.UpdateGroupForbiddenRequestHeader.class));
        result.put("groupForbiddenDtoApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.subscription.GroupForbidden.class));
        result.put("groupForbiddenDtoOperations", groupForbiddenDtoOperations());
        result.put("updateAndGetGroupForbiddenOperations", updateAndGetGroupForbiddenOperations());
        result.put("getBrokerHAStatusApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("getBrokerHAStatus"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("haRuntimeInfoApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo.class));
        result.put("haConnectionRuntimeInfoApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo.HAConnectionRuntimeInfo.class));
        result.put("haClientRuntimeInfoApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo.HAClientRuntimeInfo.class));
        result.put("getBrokerHAStatusOperations", getBrokerHAStatusOperations());
        result.put("addBrokerRequestHeaderApi",
            publicApiSurface(org.apache.rocketmq.remoting.protocol.header.AddBrokerRequestHeader.class));
        result.put("removeBrokerRequestHeaderApi",
            publicApiSurface(org.apache.rocketmq.remoting.protocol.header.RemoveBrokerRequestHeader.class));
        result.put("addRemoveBrokerOperations", addRemoveBrokerOperations());
        result.put("unitTopicListsApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> Set.of("getUnitTopicList", "getHasUnitSubTopicList",
                "getHasUnitSubUnUnitTopicList").contains(method.getName()))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("unitTopicListsOperations", unitTopicListsOperations());
        result.put("nameServerConfigApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> Set.of("updateNameServerConfig", "getNameServerConfig")
                .contains(method.getName()))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("nameServerConfigOperations", nameServerConfigOperations());
        result.put("resumeCheckHalfMessageApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("resumeCheckHalfMessage"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("resumeCheckHalfMessageOperations", resumeCheckHalfMessageOperations());
        result.put("exportPopRecordApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("exportPopRecord"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("exportPopRecordOperations", exportPopRecordOperations());
        result.put("switchTimerEngineApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("switchTimerEngine"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("switchTimerEngineOperations", switchTimerEngineOperations());
        result.put("triggerLiteDispatchApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getName().equals("triggerLiteDispatch"))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("triggerLiteDispatchOperations", triggerLiteDispatchOperations());
        result.put("producerDiagnosticsApi", Arrays.stream(MQClientAPIImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> Set.of("getProducerConnectionList", "getAllProducerInfo",
                "getBrokerRuntimeInfo").contains(method.getName()))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
        result.put("producerConnectionDtoApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.ProducerConnection.class));
        result.put("producerInfoDtoApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.ProducerInfo.class));
        result.put("producerTableInfoDtoApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo.class));
        result.put("kvTableDtoApi", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.KVTable.class));
        result.put("producerDiagnosticsOperations", producerDiagnosticsOperations());
        List<Map<String, Object>> brokerConfigProperties = new ArrayList<>();
        for (String input : Arrays.asList(
            "brokerPermission=7\nbrokerRole=ASYNC_MASTER\n",
            "",
            "escaped\\ key:hello\\:world\n",
            "unicode=\\u4E2D\n",
            "malformed=\\u12G4\n",
            "nonAscii=中文\n",
            "trailing=value\\")) {
            brokerConfigProperties.add(brokerConfigProperties(input));
        }
        result.put("brokerConfigProperties", brokerConfigProperties);
        List<Map<String, Object>> permNameCases = new ArrayList<>();
        for (int permission : Arrays.asList(-1, 0, 1, 2, 3, 4, 6, 7, 8, 15)) {
            Map<String, Object> row = new TreeMap<>();
            row.put("input", permission);
            row.put("text", PermName.perm2String(permission));
            row.put("readable", PermName.isReadable(permission));
            row.put("writeable", PermName.isWriteable(permission));
            row.put("inherited", PermName.isInherited(permission));
            row.put("priority", PermName.isPriority(permission));
            row.put("accessible", PermName.isAccessible(permission));
            row.put("valid", PermName.isValid(permission));
            permNameCases.add(row);
        }
        result.put("permNameCases", permNameCases);
        List<Map<String, Object>> permNameStringCases = new ArrayList<>();
        for (String permission : Arrays.asList("0", "+7", "٧", "8", "-1")) {
            permNameStringCases.add(row(permission, PermName.isValid(permission)));
        }
        for (String permission : Arrays.asList("", " 7", "2147483648", "--1")) {
            permNameStringCases.add(permissionOutcome(permission,
                () -> PermName.isValid(permission)));
        }
        result.put("permNameStringCases", permNameStringCases);
        result.put("messageBatchPublicApi", messageBatchPublicApi());
        result.put("messageBatchInputSemantics", messageBatchInputSemantics());
        result.put("messageBatchValidationFailures", messageBatchValidationFailures());
        result.put("topicPublishInfoApi", publicApiSurface(TopicPublishInfo.class));
        result.put("topicPublishInfoQueueFilterApi",
            publicApiSurface(TopicPublishInfo.QueueFilter.class));
        result.put("threadLocalIndexApi", threadLocalIndexApi());
        result.put("producerConstructorCases", producerConstructorCases());
        result.put("runtimeAccumulatorSettings", runtimeAccumulatorSettings());
        result.put("implementationHookExecution", implementationHookExecution());
        result.put("implementationStateAccessors", implementationStateAccessors());
        result.put("implementationSemaphoreSettings", implementationSemaphoreSettings());
        result.put("implementationPublishInfoTable", implementationPublishInfoTable());
        result.put("implementationPublishInfoTableAdvanced", implementationPublishInfoTableAdvanced());
        result.put("implementationQueueFilterSelection", implementationQueueFilterSelection());
        result.put("implementationSelectorFailure", implementationSelectorFailure());
        result.put("implementationSelectorMessageSnapshot", implementationSelectorMessageSnapshot());
        result.put("producerClientFactoryApi", publicApiSurface(MQClientInstance.class));
        result.put("mqFaultStrategyApi", publicApiSurface(MQFaultStrategy.class));
        result.put("mqFaultStrategyBrokerFilterApi", publicApiSurface(MQFaultStrategy.BrokerFilter.class));
        result.put("produceAccumulatorApi", publicApiSurface(ProduceAccumulator.class));
        result.put("produceAccumulatorClientIdCache", produceAccumulatorClientIdCache());

        Map<String, Object> defaults = new TreeMap<>();
        defaults.put("producerGroup", producer.getProducerGroup());
        defaults.put("createTopicKey", producer.getCreateTopicKey());
        defaults.put("defaultTopicQueueNums", producer.getDefaultTopicQueueNums());
        defaults.put("sendMsgTimeout", producer.getSendMsgTimeout());
        defaults.put("sendMsgMaxTimeoutPerRequest", producer.getSendMsgMaxTimeoutPerRequest());
        defaults.put("compressMsgBodyOverHowmuch", producer.getCompressMsgBodyOverHowmuch());
        defaults.put("compressType", producer.getCompressType().name());
        defaults.put("compressLevel", producer.getCompressLevel());
        defaults.put("retryTimesWhenSendFailed", producer.getRetryTimesWhenSendFailed());
        defaults.put("retryTimesWhenSendAsyncFailed", producer.getRetryTimesWhenSendAsyncFailed());
        defaults.put("retryAnotherBrokerWhenNotStoreOK", producer.isRetryAnotherBrokerWhenNotStoreOK());
        defaults.put("maxMessageSize", producer.getMaxMessageSize());
        defaults.put("sendMessageWithVIPChannel", producer.isSendMessageWithVIPChannel());
        defaults.put("autoBatch", producer.getAutoBatch());
        defaults.put("batchMaxDelayMs", producer.getBatchMaxDelayMs());
        defaults.put("batchMaxBytes", producer.getBatchMaxBytes());
        defaults.put("totalBatchMaxBytes", producer.getTotalBatchMaxBytes());
        defaults.put("enableBackpressureForAsyncMode", producer.isEnableBackpressureForAsyncMode());
        defaults.put("backPressureForAsyncSendNum", producer.getBackPressureForAsyncSendNum());
        defaults.put("backPressureForAsyncSendSize", producer.getBackPressureForAsyncSendSize());
        defaults.put("retryResponseCodes", new TreeSet<>(producer.getRetryResponseCodes()));
        result.put("defaults", defaults);
        result.put("executorSettings", executorSettings());
        result.put("checkForbiddenContext", checkForbiddenContext());
        result.put("sendMessageContext", sendMessageContext());

        Map<String, Object> batchSettings = new TreeMap<>();
        DefaultMQProducer configuredBatch = new DefaultMQProducer("batch-settings");
        configuredBatch.setInstanceName("configured-batch-settings");
        configuredBatch.batchMaxDelayMs(25);
        configuredBatch.batchMaxBytes(65_536);
        configuredBatch.totalBatchMaxBytes(1_048_576);
        configuredBatch.initProduceAccumulator();
        batchSettings.put("configuredBatchMaxDelayMs", configuredBatch.getBatchMaxDelayMs());
        batchSettings.put("configuredBatchMaxBytes", configuredBatch.getBatchMaxBytes());
        batchSettings.put("configuredTotalBatchMaxBytes", configuredBatch.getTotalBatchMaxBytes());
        batchSettings.put("configuredTotalBatchHoldBytes", totalAccumulatorHoldBytes(configuredBatch));
        result.put("batchSettings", batchSettings);

        DefaultMQProducer initializedAccumulator = new DefaultMQProducer("accumulator-defaults");
        initializedAccumulator.setInstanceName("accumulator-defaults");
        initializedAccumulator.initProduceAccumulator();
        Map<String, Object> accumulatorDefaults = new TreeMap<>();
        accumulatorDefaults.put("batchMaxDelayMs", initializedAccumulator.getBatchMaxDelayMs());
        accumulatorDefaults.put("batchMaxBytes", initializedAccumulator.getBatchMaxBytes());
        accumulatorDefaults.put("totalBatchMaxBytes", initializedAccumulator.getTotalBatchMaxBytes());
        accumulatorDefaults.put("totalBatchHoldBytes", totalAccumulatorHoldBytes(initializedAccumulator));
        result.put("accumulatorDefaults", accumulatorDefaults);

        DefaultMQProducer autoBatchLifecycle = new DefaultMQProducer("auto-batch-lifecycle");
        autoBatchLifecycle.setInstanceName("auto-batch-lifecycle");
        boolean autoBatchBeforeAccumulator = autoBatchLifecycle.getAutoBatch();
        autoBatchLifecycle.setAutoBatch(true);
        autoBatchLifecycle.initProduceAccumulator();
        Map<String, Object> autoBatch = new TreeMap<>();
        autoBatch.put("enabledBeforeAccumulator", autoBatchBeforeAccumulator);
        autoBatch.put("enabledAfterAccumulator", autoBatchLifecycle.getAutoBatch());
        autoBatchLifecycle.setAutoBatch(false);
        autoBatch.put("disabledAfterRuntimeToggle", autoBatchLifecycle.getAutoBatch());
        autoBatchLifecycle.setAutoBatch(true);
        autoBatch.put("enabledAfterRuntimeToggle", autoBatchLifecycle.getAutoBatch());
        result.put("autoBatchLifecycle", autoBatch);
        result.put("sendInputMutation", sendInputMutation());
        result.put("batchSendInputMutation", batchSendInputMutation());

        DefaultMQProducer compressorProducer = new DefaultMQProducer("compressor-settings");
        List<Map<String, Object>> compressorTypes = new ArrayList<>();
        byte[] compressorInput = new byte[] {0, 1, 2, 3, 127, (byte) 255};
        for (CompressionType type : CompressionType.values()) {
            compressorProducer.setCompressType(type);
            Compressor compressor = compressorProducer.getCompressor();
            byte[] compressed = compressor.compress(compressorInput,
                compressorProducer.getCompressLevel());
            Map<String, Object> compressorType = new TreeMap<>();
            compressorType.put("type", compressorProducer.getCompressType().name());
            compressorType.put("class", compressor.getClass().getSimpleName());
            compressorType.put("roundTrip",
                Arrays.equals(compressorInput, compressor.decompress(compressed)));
            compressorTypes.add(compressorType);
        }
        compressorProducer.setCompressType(CompressionType.ZLIB);
        Compressor retainedCompressor = compressorProducer.getCompressor();
        compressorProducer.setCompressType(CompressionType.LZ4);
        byte[] retainedCompressed = retainedCompressor.compress(compressorInput,
            compressorProducer.getCompressLevel());
        Map<String, Object> compressorSettings = new TreeMap<>();
        compressorSettings.put("types", compressorTypes);
        compressorSettings.put("currentTypeAfterUpdate",
            compressorProducer.getCompressType().name());
        compressorSettings.put("retainedRoundTripAfterUpdate",
            Arrays.equals(compressorInput, retainedCompressor.decompress(retainedCompressed)));
        compressorSettings.put("retainedClassAfterUpdate",
            retainedCompressor.getClass().getSimpleName());
        compressorSettings.put("currentClassAfterUpdate",
            compressorProducer.getCompressor().getClass().getSimpleName());
        result.put("compressorSettings", compressorSettings);

        DefaultMQProducer autoBatchEligibility = new DefaultMQProducer("auto-batch-eligibility");
        autoBatchEligibility.setInstanceName("auto-batch-eligibility");
        autoBatchEligibility.initProduceAccumulator();
        List<Map<String, Object>> autoBatchCases = new ArrayList<>();
        autoBatchCases.add(row("regular", isBatchable(autoBatchEligibility,
            new Message("orders", new byte[] {1}))));
        Message delayedAutoBatch = new Message("orders", new byte[] {1});
        MessageAccessor.putProperty(delayedAutoBatch, MessageConst.PROPERTY_TIMER_DELAY_MS, "100");
        autoBatchCases.add(row("delayed", isBatchable(autoBatchEligibility, delayedAutoBatch)));
        autoBatchCases.add(row("retry-topic", isBatchable(autoBatchEligibility,
            new Message("%RETRY%workers", new byte[] {1}))));
        Message groupedAutoBatch = new Message("orders", new byte[] {1});
        MessageAccessor.putProperty(groupedAutoBatch, MessageConst.PROPERTY_PRODUCER_GROUP, "transaction-producer");
        autoBatchCases.add(row("producer-group", isBatchable(autoBatchEligibility, groupedAutoBatch)));
        result.put("autoBatchEligibility", autoBatchCases);

        DefaultMQProducer backpressureSettings = new DefaultMQProducer("backpressure-settings");
        backpressureSettings.setBackPressureForAsyncSendNum(1);
        backpressureSettings.setBackPressureForAsyncSendSize(1);
        Map<String, Object> backpressure = new TreeMap<>();
        backpressure.put("clampedSendNum", backpressureSettings.getBackPressureForAsyncSendNum());
        backpressure.put("clampedSendSize", backpressureSettings.getBackPressureForAsyncSendSize());
        result.put("backpressureSettings", backpressure);
        result.put("runtimeSettings", runtimeSettings());
        result.put("runtimeSetterFlows", runtimeSetterFlows());
        result.put("perRequestTimeoutRetry", perRequestTimeoutRetry());
        result.put("missingBrokerAddressRetry", missingBrokerAddressRetry());
        result.put("missingBrokerAddressExplicit", missingBrokerAddressExplicit());
        result.put("checkForbiddenRetry", checkForbiddenRetry());
        result.put("checkForbiddenDefaultAsync", checkForbiddenDefaultAsync());
        result.put("checkForbiddenExplicitAsync", checkForbiddenExplicitAsync());
        result.put("checkForbiddenDeadline", checkForbiddenDeadline());

        List<Map<String, Object>> groups = new ArrayList<>();
        for (String value : Arrays.asList(" ", "g".repeat(120), "g".repeat(121), "bad.group", "a|b_%")) {
            groups.add(row(value, accepts(() -> Validators.checkGroup(value))));
        }
        result.put("groups", groups);

        List<Map<String, Object>> topics = new ArrayList<>();
        for (String value : Arrays.asList(" ", "t".repeat(127), "t".repeat(128), "bad.topic",
            "bad." + "t".repeat(124), "%RETRY%" + "t".repeat(121))) {
            topics.add(row(value, accepts(() -> Validators.checkTopic(value))));
        }
        result.put("topics", topics);

        List<Map<String, Object>> systemTopics = new ArrayList<>();
        for (String value : Arrays.asList("TBW102", "RMQ_SYS_TRACE_TOPIC", "SCHEDULE_TOPIC_XXXX", "rmq_sys_custom")) {
            Map<String, Object> row = new TreeMap<>();
            row.put("topic", value);
            row.put("system", TopicValidator.isSystemTopic(value));
            row.put("sendAllowed", accepts(() -> Validators.isNotAllowedSendTopic(value))
                && !TopicValidator.isNotAllowedSendTopic(value));
            // isNotAllowedSendTopic returns normally for both allowed and forbidden topics.
            row.put("sendForbidden", TopicValidator.isNotAllowedSendTopic(value));
            systemTopics.add(row);
        }
        result.put("systemTopics", systemTopics);

        List<Map<String, Object>> topicPermissions = new ArrayList<>();
        for (int permission : Arrays.asList(-1, 0, 4, 6, 7, 8, 15)) {
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setPerm(permission);
            topicPermissions.add(permissionOutcome(permission,
                () -> Validators.checkTopicConfig(topicConfig)));
        }
        result.put("topicPermissions", topicPermissions);

        List<Map<String, Object>> brokerPermissions = new ArrayList<>();
        for (String permission : Arrays.asList("0", "+7", "٧", "8", "-1", "", " 7", "2147483648")) {
            Properties properties = new Properties();
            properties.setProperty("brokerPermission", permission);
            brokerPermissions.add(permissionOutcome(permission,
                () -> Validators.checkBrokerConfig(properties)));
        }
        Properties missingBrokerPermission = new Properties();
        brokerPermissions.add(permissionOutcome("missing",
            () -> Validators.checkBrokerConfig(missingBrokerPermission)));
        result.put("brokerPermissions", brokerPermissions);

        List<Map<String, Object>> messages = new ArrayList<>();
        messages.add(row("null-message", accepts(() -> Validators.checkMessage(null, producer))));
        messages.add(row("null-body", accepts(() -> Validators.checkMessage(new Message("orders", (byte[]) null), producer))));
        messages.add(row("empty-body", accepts(() -> Validators.checkMessage(new Message("orders", new byte[0]), producer))));
        messages.add(row("body-max", acceptsMessage(producer, "orders", new byte[producer.getMaxMessageSize()], null)));
        messages.add(row("body-max-plus-one", acceptsMessage(producer, "orders", new byte[producer.getMaxMessageSize() + 1], null)));
        messages.add(row("multi-dispatch-separator", acceptsMessage(producer, "orders", new byte[] {1}, "a/b")));
        result.put("messages", messages);

        Map<String, Object> compression = new TreeMap<>();
        int threshold = producer.getCompressMsgBodyOverHowmuch();
        compression.put("threshold", threshold);
        compression.put("cases", compressionCases(producer, threshold));
        result.put("compression", compression);
        result.put("runtimeCompressionSettings", runtimeCompressionSettings());
        result.put("compressionProperties", compressionProperties());

        Map<String, Object> faultStrategy = new TreeMap<>();
        faultStrategy.put("latencyMaxMillis", longs(producer.getLatencyMax()));
        faultStrategy.put("notAvailableDurationMillis", longs(producer.getNotAvailableDuration()));
        faultStrategy.put("sendLatencyFaultEnable", producer.isSendLatencyFaultEnable());
        faultStrategy.put("startDetectorEnable", producer.isStartDetectorEnable());
        faultStrategy.put("detectTimeoutMillis", producer.getDetectTimeout());
        faultStrategy.put("detectIntervalMillis", producer.getDetectInterval());
        faultStrategy.put("scheduleInitialDelaySeconds", 3);
        faultStrategy.put("schedulePeriodSeconds", 3);
        faultStrategy.put("probeRequestCode", RequestCode.GET_MAX_OFFSET);
        faultStrategy.put("probeQueueId", 0);
        faultStrategy.put("probeCommitted", true);
        result.put("faultStrategy", faultStrategy);
        result.put("faultItemApi", publicApiSurface(LatencyFaultToleranceImpl.FaultItem.class));
        result.put("faultItemBehavior", faultItemBehavior());
        result.put("runtimeFaultStrategy", runtimeFaultStrategy());
        result.put("runtimeFaultStrategyNullArrays", runtimeFaultStrategyNullArrays());

        String uniqueId = MessageClientIDSetter.createUniqID();
        int ipBytes = MessageClientIDSetter.getIPFromID(uniqueId).length;
        Map<String, Object> uniqueIdShape = new TreeMap<>();
        uniqueIdShape.put("hexUppercase", uniqueId.matches("[0-9A-F]+"));
        uniqueIdShape.put("supportedIpBytes", ipBytes == 4 || ipBytes == 16);
        uniqueIdShape.put("lengthMatchesIpBytes", uniqueId.length() == ipBytes * 2 + 24);
        uniqueIdShape.put("processIdBytes", 2);
        uniqueIdShape.put("processIdentityBytes", 4);
        uniqueIdShape.put("monthTimestampBytes", 4);
        uniqueIdShape.put("counterBytes", 2);
        result.put("uniqueId", uniqueIdShape);
        result.put("uniqueIdHelpers", uniqueIdHelpers());

        result.put("batchValidation", batchCases());
        result.put("batchEncoding", batchEncoding());
        return result;
    }

    private static Map<String, Object> uniqueIdHelpers() {
        String ipv4 = "C000020A123456789ABC010203040005";
        String ipv6 = "20010DB8000000000000000000000020123456789ABC010203040005";
        Map<String, Object> result = new TreeMap<>();
        result.put("ipv4Id", ipv4);
        result.put("ipv4AddressBytes", toHex(MessageClientIDSetter.getIPFromID(ipv4)));
        result.put("ipv4Address", MessageClientIDSetter.getIPStrFromID(ipv4));
        result.put("ipv4Pid", MessageClientIDSetter.getPidFromID(ipv4));
        result.put("ipv4NearlyTimeMonthOffsetMillis",
            nearlyTimeMonthOffsetMillis(MessageClientIDSetter.getNearlyTimeFromID(ipv4)));
        Map<String, Object> messageClientIdSetterApi = publicApiSurface(MessageClientIDSetter.class);
        List<String> publicFields = Arrays.stream(MessageClientIDSetter.class.getDeclaredFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(field -> Modifier.toString(field.getModifiers()) + " "
                + field.getGenericType().getTypeName() + " " + field.getName())
            .sorted()
            .collect(java.util.stream.Collectors.toList());
        messageClientIdSetterApi.put("declaredPublicFields", publicFields);
        result.put("publicApi", messageClientIdSetterApi);
        result.put("ipv6Id", ipv6);
        result.put("ipv6AddressBytes", toHex(MessageClientIDSetter.getIPFromID(ipv6)));
        result.put("ipv6Address", MessageClientIDSetter.getIPStrFromID(ipv6));
        result.put("ipv6Pid", MessageClientIDSetter.getPidFromID(ipv6));
        result.put("ipv6NearlyTimeMonthOffsetMillis",
            nearlyTimeMonthOffsetMillis(MessageClientIDSetter.getNearlyTimeFromID(ipv6)));
        result.put("oddTrailingNibbleIgnored",
            toHex(MessageClientIDSetter.getIPFromID("C000020A1")));
        result.put("invalidHexNibbles", toHex(MessageClientIDSetter.getIPFromID("G0H0I0J0")));
        result.put("fakeIpLength", MessageClientIDSetter.createFakeIP().length);
        result.put("shortIpThrows", !"none".equals(failureType(
            () -> MessageClientIDSetter.getIPFromID("AA"))));
        result.put("shortPidThrows", !"none".equals(failureType(
            () -> MessageClientIDSetter.getPidFromID("AA"))));
        result.put("shortTimestampThrows", !"none".equals(failureType(
            () -> MessageClientIDSetter.getNearlyTimeFromID("AA"))));
        return result;
    }

    private static long nearlyTimeMonthOffsetMillis(Date nearlyTime) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(nearlyTime);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return nearlyTime.getTime() - calendar.getTimeInMillis();
    }

    private static Map<String, Object> sendInputMutation() {
        DefaultMQProducer producer = new DefaultMQProducer("send-input-mutation");
        producer.setInstanceName("send-input-mutation");
        producer.batchMaxBytes(1);
        producer.initProduceAccumulator();
        Message message = new Message("orders", new byte[] {1, 2, 3});
        String sendOutcome = "returned";
        try {
            producer.sendByAccumulator(message, null, null);
        } catch (Throwable error) {
            sendOutcome = error.getClass().getSimpleName();
        }
        String uniqueId = message.getProperty("UNIQ_KEY");

        Message directMessage = new Message("orders", new byte[] {1, 2, 3});
        String directSendOutcome = "returned";
        try {
            producer.sendDirect(directMessage, null, null);
        } catch (Throwable error) {
            directSendOutcome = error.getClass().getSimpleName();
        }
        String directUniqueId = directMessage.getProperty("UNIQ_KEY");

        Map<String, Object> result = new TreeMap<>();
        result.put("uniqueIdPresent", uniqueId != null);
        result.put("uniqueIdLengthMatchesAddressFamily", uniqueId == null
            || uniqueId.length() == 32 || uniqueId.length() == 56);
        result.put("topic", message.getTopic());
        result.put("sendOutcome", sendOutcome);
        result.put("directUniqueIdPresent", directUniqueId != null);
        result.put("directSendOutcome", directSendOutcome);
        return result;
    }

    private static Map<String, Object> batchSendInputMutation() {
        DefaultMQProducer producer = new DefaultMQProducer("batch-input-mutation");
        producer.setInstanceName("batch-input-mutation");
        producer.setNamespace("batch-tenant");

        Message first = new Message("orders", new byte[] {1, 2, 3});
        Message second = new Message("orders", new byte[] {4, 5, 6});
        String sendOutcome = "returned";
        try {
            producer.send(Arrays.asList(first, second));
        } catch (Throwable error) {
            sendOutcome = error.getClass().getSimpleName();
        }

        Message mixedFirst = new Message("orders", new byte[] {1});
        Message mixedSecond = new Message("payments", new byte[] {2});
        String mixedTopicOutcome = "returned";
        String mixedTopicErrorMessage = "";
        try {
            producer.send(Arrays.asList(mixedFirst, mixedSecond));
        } catch (Throwable error) {
            mixedTopicOutcome = error.getClass().getSimpleName();
            mixedTopicErrorMessage = error.getMessage();
        }

        producer.setMaxMessageSize(2);
        Message aggregateSize = new Message("orders", new byte[] {1});
        String aggregateSizeOutcome = "returned";
        String aggregateSizeErrorMessage = "";
        try {
            producer.send(Arrays.asList(aggregateSize));
        } catch (Throwable error) {
            aggregateSizeOutcome = error.getClass().getSimpleName();
            aggregateSizeErrorMessage = error.getMessage();
        }

        Message partialFirst = new Message("orders", new byte[] {1});
        Message partialSecond = new Message("orders", new byte[] {2, 3, 4});
        String partialValidationOutcome = "returned";
        String partialValidationErrorMessage = "";
        try {
            producer.send(Arrays.asList(partialFirst, partialSecond));
        } catch (Throwable error) {
            partialValidationOutcome = error.getClass().getSimpleName();
            partialValidationErrorMessage = error.getMessage();
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("sendOutcome", sendOutcome);
        result.put("firstTopic", first.getTopic());
        result.put("secondTopic", second.getTopic());
        result.put("firstUniqueIdPresent", first.getProperty("UNIQ_KEY") != null);
        result.put("secondUniqueIdPresent", second.getProperty("UNIQ_KEY") != null);
        result.put("mixedTopicOutcome", mixedTopicOutcome);
        result.put("mixedTopicErrorMessage", mixedTopicErrorMessage);
        result.put("mixedFirstTopic", mixedFirst.getTopic());
        result.put("mixedSecondTopic", mixedSecond.getTopic());
        result.put("mixedFirstUniqueIdPresent", mixedFirst.getProperty("UNIQ_KEY") != null);
        result.put("mixedSecondUniqueIdPresent", mixedSecond.getProperty("UNIQ_KEY") != null);
        result.put("aggregateSizeOutcome", aggregateSizeOutcome);
        result.put("aggregateSizeErrorMessage", aggregateSizeErrorMessage);
        result.put("aggregateSizeTopic", aggregateSize.getTopic());
        result.put("aggregateSizeUniqueIdPresent", aggregateSize.getProperty("UNIQ_KEY") != null);
        result.put("partialValidationOutcome", partialValidationOutcome);
        result.put("partialValidationErrorMessage", partialValidationErrorMessage);
        result.put("partialFirstTopic", partialFirst.getTopic());
        result.put("partialSecondTopic", partialSecond.getTopic());
        result.put("partialFirstUniqueIdPresent", partialFirst.getProperty("UNIQ_KEY") != null);
        result.put("partialSecondUniqueIdPresent", partialSecond.getProperty("UNIQ_KEY") != null);
        return result;
    }

    private static Map<String, Object> producerImplementationApi() {
        Object implementation = new DefaultMQProducer("producer-implementation-api")
            .getDefaultMQProducerImpl();
        Class<?> implementationClass = implementation.getClass();
        List<String> constructors = Arrays.stream(implementationClass.getConstructors())
            .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
            .map(ProducerCoreFixtures::constructorSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList());
        List<String> methods = Arrays.stream(implementationClass.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList());
        Map<String, Object> result = new TreeMap<>();
        result.put("className", implementationClass.getName());
        result.put("publicConstructors", constructors);
        result.put("publicMethods", methods);
        result.put("publicMethodCount", methods.size());
        return result;
    }

    private static Map<String, Object> producerFacadeApi() {
        Map<String, Object> result = new TreeMap<>();
        result.put("DefaultMQProducer", publicApiSurface(DefaultMQProducer.class));
        result.put("MQProducer", publicApiSurface(MQProducer.class));
        result.put("MQAdmin", publicApiSurface(MQAdmin.class));
        return result;
    }

    private static Map<String, Object> messageBatchPublicApi() {
        Map<String, Object> result = publicApiSurface(MessageBatch.class);
        List<String> fields = Arrays.stream(MessageBatch.class.getDeclaredFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(field -> Modifier.toString(field.getModifiers()) + " "
                + field.getGenericType().getTypeName() + " " + field.getName())
            .sorted()
            .collect(java.util.stream.Collectors.toList());
        result.put("declaredPublicFields", fields);
        return result;
    }

    private static Map<String, Object> messageBatchInputSemantics() {
        Map<String, Object> result = new TreeMap<>();
        MessageBatch nullList = new MessageBatch(null);
        List<Message> nullElement = Arrays.asList((Message) null);
        result.put("nullConstructorAccepted", nullList != null);
        result.put("nullConstructorIteratorFailure", failureType(nullList::iterator));
        result.put("nullConstructorEncodeFailure", failureType(nullList::encode));
        result.put("nullElementIteratorReturnsNull",
            new MessageBatch(nullElement).iterator().next() == null);
        result.put("nullElementEncodeFailure",
            failureType(() -> new MessageBatch(nullElement).encode()));
        result.put("emptyConstructorEncodedLength",
            new MessageBatch(new ArrayList<>()).encode().length);
        result.put("nullGenerateFromListFailure",
            failureType(() -> MessageBatch.generateFromList(null)));
        result.put("emptyGenerateFromListFailure",
            failureType(() -> MessageBatch.generateFromList(new ArrayList<>())));
        result.put("nullElementGenerateFromListFailure",
            failureType(() -> MessageBatch.generateFromList(nullElement)));
        return result;
    }

    private static Map<String, Object> messageBatchValidationFailures() {
        Map<String, Object> result = new TreeMap<>();
        result.put("empty", failureDetails(() -> MessageBatch.generateFromList(new ArrayList<>())));
        result.put("mixedTopics", failureDetails(() -> MessageBatch.generateFromList(List.of(
            new Message("orders", new byte[] {1}), new Message("payments", new byte[] {2})))));
        result.put("waitStoreMismatch", failureDetails(() -> {
            Message first = new Message("orders", new byte[] {1});
            Message second = new Message("orders", new byte[] {2});
            second.setWaitStoreMsgOK(false);
            MessageBatch.generateFromList(List.of(first, second));
        }));
        result.put("delayed", failureDetails(() -> {
            Message message = new Message("orders", new byte[] {1});
            message.setDelayTimeLevel(1);
            MessageBatch.generateFromList(List.of(message));
        }));
        result.put("timerDelayMs", failureDetails(() -> {
            Message message = new Message("orders", new byte[] {1});
            message.setDelayTimeMs(1);
            MessageBatch.generateFromList(List.of(message));
        }));
        result.put("timerDelaySec", failureDetails(() -> {
            Message message = new Message("orders", new byte[] {1});
            message.setDelayTimeSec(1);
            MessageBatch.generateFromList(List.of(message));
        }));
        result.put("timerDeliverMs", failureDetails(() -> {
            Message message = new Message("orders", new byte[] {1});
            message.setDeliverTimeMs(1);
            MessageBatch.generateFromList(List.of(message));
        }));
        result.put("retryTopic", failureDetails(() -> MessageBatch.generateFromList(List.of(
            new Message("%RETRY%workers", new byte[] {1})))));
        return result;
    }

    private static Map<String, Object> failureDetails(Check check) {
        Map<String, Object> result = new TreeMap<>();
        try {
            check.run();
            result.put("class", "none");
            result.put("message", "");
        } catch (Throwable error) {
            result.put("class", error.getClass().getName());
            result.put("message", error.getMessage());
        }
        return result;
    }

    private static Map<String, Object> threadLocalIndexApi() {
        ThreadLocalIndex index = new ThreadLocalIndex();
        Map<String, Object> result = new TreeMap<>();
        result.put("publicApi", publicApiSurface(ThreadLocalIndex.class));
        result.put("initialString", index.toString());
        int incremented = index.incrementAndGet();
        result.put("incrementedNonnegative", incremented >= 0);
        result.put("incrementMatchesMask",
            (threadLocalIndexValue(index) & Integer.MAX_VALUE) == incremented);
        index.reset();
        result.put("resetValueNonnegative", threadLocalIndexValue(index) >= 0);

        try {
            java.lang.reflect.Field field = ThreadLocalIndex.class.getDeclaredField("threadLocalIndex");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            ThreadLocal<Integer> values = (ThreadLocal<Integer>) field.get(index);
            values.set(Integer.MAX_VALUE);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("failed to seed ThreadLocalIndex overflow", error);
        }
        result.put("overflowFirst", index.incrementAndGet());
        result.put("overflowSecond", index.incrementAndGet());
        result.put("afterOverflowString", index.toString());
        return result;
    }

    private static int threadLocalIndexValue(ThreadLocalIndex index) {
        String text = index.toString();
        return Integer.parseInt(text.substring(text.indexOf('=') + 1, text.length() - 1));
    }

    private static Map<String, Object> producerConstructorCases() {
        RPCHook hook = new RPCHook() {
            @Override
            public void doBeforeRequest(String remoteAddr, RemotingCommand request) { }

            @Override
            public void doAfterResponse(String remoteAddr, RemotingCommand request,
                RemotingCommand response) { }
        };
        List<String> topics = new ArrayList<>(List.of("constructor-topic-a", "constructor-topic-b"));
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(producerConstructorCase("no-arg", new DefaultMQProducer(), null, null));
        cases.add(producerConstructorCase("rpc-hook", new DefaultMQProducer(hook), hook, null));
        cases.add(producerConstructorCase("group", new DefaultMQProducer("constructor-group"), null, null));
        cases.add(producerConstructorCase("group-rpc-hook",
            new DefaultMQProducer("constructor-group-hook", hook), hook, null));
        cases.add(producerConstructorCase("group-rpc-hook-topics",
            new DefaultMQProducer("constructor-group-topics", hook, topics), hook, topics));
        cases.add(producerConstructorCase("group-trace",
            new DefaultMQProducer("constructor-group-trace", true, "constructor-trace-topic"), null, null));
        cases.add(producerConstructorCase("group-rpc-hook-trace",
            new DefaultMQProducer("constructor-group-hook-trace", hook, true,
                "constructor-hook-trace-topic"), hook, null));
        cases.add(producerConstructorCase("group-rpc-hook-topics-trace",
            new DefaultMQProducer("constructor-group-full", hook, topics, true,
                "constructor-full-trace-topic"), hook, topics));
        cases.add(producerConstructorCase("namespace-group",
            new DefaultMQProducer("constructor-namespace", "constructor-legacy-group"), null, null));
        cases.add(producerConstructorCase("namespace-group-rpc-hook",
            new DefaultMQProducer("constructor-namespace-hook", "constructor-legacy-hook-group", hook),
            hook, null));
        cases.add(producerConstructorCase("namespace-group-rpc-hook-trace",
            new DefaultMQProducer("constructor-namespace-trace", "constructor-legacy-trace-group", hook,
                true, "constructor-legacy-trace-topic"), hook, null));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        result.put("count", cases.size());
        List<Map<String, Object>> nullableCases = new ArrayList<>();
        nullableCases.add(producerConstructorCase("null-producer-group",
            new DefaultMQProducer((String) null), null, null));
        nullableCases.add(producerConstructorCase("null-producer-group-with-namespace",
            new DefaultMQProducer("constructor-null-group-namespace", (String) null), null, null));
        DefaultMQProducer nullGroupSetter = new DefaultMQProducer("constructor-group-before-null");
        String groupBeforeSetter = nullGroupSetter.getProducerGroup();
        nullGroupSetter.setProducerGroup(null);
        Map<String, Object> nullGroupSetterCase = producerConstructorCase(
            "null-producer-group-setter", nullGroupSetter, null, null);
        nullGroupSetterCase.put("producerGroupBeforeSetter", groupBeforeSetter);
        nullableCases.add(nullGroupSetterCase);
        nullableCases.add(producerConstructorCase("null-namespace",
            new DefaultMQProducer((String) null, "constructor-null-namespace-group"), null, null));
        nullableCases.add(producerConstructorCase("null-initial-topics",
            new DefaultMQProducer("constructor-null-topics", hook, (List<String>) null), hook, null));
        nullableCases.add(producerConstructorCase("null-trace-topic",
            new DefaultMQProducer("constructor-null-trace-topic", hook, true, null), hook, null));
        result.put("nullableCases", nullableCases);
        result.put("nullableCaseCount", nullableCases.size());
        result.put("nullProducerGroupStart", producerStartOutcome(new DefaultMQProducer((String) null)));
        result.put("nullNamespacedProducerGroupStart", producerStartOutcome(
            new DefaultMQProducer("constructor-null-group-namespace", (String) null)));
        return result;
    }

    private static Map<String, Object> producerStartOutcome(DefaultMQProducer producer) {
        Map<String, Object> result = new TreeMap<>();
        try {
            producer.start();
            result.put("failureClass", "");
            result.put("failureMessage", "");
        } catch (Throwable failure) {
            result.put("failureClass", failure.getClass().getName());
            result.put("failureMessage", failure.getMessage() == null ? "" : failure.getMessage());
        }
        result.put("serviceState", producer.getDefaultMQProducerImpl().getServiceState().name());
        return result;
    }

    private static Map<String, Object> producerConstructorCase(String name, DefaultMQProducer producer,
        RPCHook suppliedHook, List<String> suppliedTopics) {
        Map<String, Object> result = new TreeMap<>();
        result.put("name", name);
        result.put("producerGroupIsNull", producer.getProducerGroup() == null);
        result.put("producerGroup", producer.getProducerGroup() == null ? "" : producer.getProducerGroup());
        result.put("namespaceIsNull", producer.getNamespace() == null);
        result.put("namespace", producer.getNamespace() == null ? "" : producer.getNamespace());
        result.put("rpcHookSupplied", suppliedHook != null);
        result.put("rpcHookRetainedByFacade", suppliedHook != null
            && readProducerField(producer, "rpcHook") == suppliedHook);
        result.put("rpcHookRetainedByImplementation",
            suppliedHook != null && readImplementationField(producer, "rpcHook") == suppliedHook);
        result.put("topicsIsNull", producer.getTopics() == null);
        result.put("topics", producer.getTopics() == null ? List.of() : new ArrayList<>(producer.getTopics()));
        result.put("topicsInputIdentityRetained", suppliedTopics != null
            && suppliedTopics == producer.getTopics());
        result.put("enableTrace", producer.isEnableTrace());
        result.put("traceTopicIsNull", producer.getTraceTopic() == null);
        result.put("traceTopic", producer.getTraceTopic() == null ? "" : producer.getTraceTopic());
        return result;
    }

    private static Object readProducerField(DefaultMQProducer producer, String fieldName) {
        try {
            Field field = DefaultMQProducer.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(producer);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Could not inspect producer constructor field " + fieldName, error);
        }
    }

    private static Object readImplementationField(DefaultMQProducer producer, String fieldName) {
        try {
            Object implementation = producer.getDefaultMQProducerImpl();
            Field field = implementation.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(implementation);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Could not inspect producer implementation field " + fieldName, error);
        }
    }

    private static Map<String, Object> produceAccumulatorClientIdCache() {
        ClientConfig firstConfig = new ClientConfig();
        firstConfig.setClientIP("192.0.2.18");
        firstConfig.setInstanceName("phase18-accumulator-cache");
        ClientConfig sameIdConfig = firstConfig.cloneClientConfig();
        ClientConfig otherIdConfig = firstConfig.cloneClientConfig();
        otherIdConfig.setInstanceName("phase18-accumulator-cache-other");

        MQClientManager manager = MQClientManager.getInstance();
        ProduceAccumulator first = manager.getOrCreateProduceAccumulator(firstConfig);
        ProduceAccumulator sameId = manager.getOrCreateProduceAccumulator(sameIdConfig);
        ProduceAccumulator otherId = manager.getOrCreateProduceAccumulator(otherIdConfig);
        Map<String, Object> result = new TreeMap<>();
        result.put("clientId", firstConfig.buildMQClientId());
        result.put("sameClientId", sameIdConfig.buildMQClientId());
        result.put("otherClientId", otherIdConfig.buildMQClientId());
        result.put("sameClientIdReusesAccumulator", first == sameId);
        result.put("differentClientIdUsesDifferentAccumulator", first != otherId);
        return result;
    }

    private static Map<String, Object> runtimeAccumulatorSettings() {
        DefaultMQProducer producer = new DefaultMQProducer("runtime-accumulator-settings");
        producer.setInstanceName("runtime-accumulator-settings");
        Map<String, Object> result = new TreeMap<>();
        result.put("beforeInitializationDelay", producer.getBatchMaxDelayMs());
        result.put("beforeInitializationBytes", producer.getBatchMaxBytes());
        result.put("beforeInitializationTotalBytes", producer.getTotalBatchMaxBytes());

        producer.initProduceAccumulator();
        result.put("initializedDelay", producer.getBatchMaxDelayMs());
        result.put("initializedBytes", producer.getBatchMaxBytes());
        result.put("initializedTotalGetterBytes", producer.getTotalBatchMaxBytes());
        result.put("initializedTotalHoldBytes", totalAccumulatorHoldBytes(producer));

        producer.batchMaxDelayMs(25);
        producer.batchMaxBytes(65_536);
        producer.totalBatchMaxBytes(1_048_576);
        result.put("updatedDelay", producer.getBatchMaxDelayMs());
        result.put("updatedBytes", producer.getBatchMaxBytes());
        result.put("updatedTotalGetterBytes", producer.getTotalBatchMaxBytes());
        result.put("updatedTotalHoldBytes", totalAccumulatorHoldBytes(producer));

        result.put("invalidDelayFailure", failureType(() -> producer.batchMaxDelayMs(0)));
        result.put("delayAfterInvalidUpdate", producer.getBatchMaxDelayMs());
        result.put("requestedDelayAfterInvalidUpdate", readProducerField(producer, "batchMaxDelayMs"));
        result.put("invalidBytesFailure", failureType(() -> producer.batchMaxBytes(0)));
        result.put("bytesAfterInvalidUpdate", producer.getBatchMaxBytes());
        result.put("requestedBytesAfterInvalidUpdate", readProducerField(producer, "batchMaxBytes"));
        result.put("invalidTotalBytesFailure", failureType(() -> producer.totalBatchMaxBytes(0)));
        result.put("totalHoldBytesAfterInvalidUpdate", totalAccumulatorHoldBytes(producer));
        result.put("requestedTotalBytesAfterInvalidUpdate", readProducerField(producer, "totalBatchMaxBytes"));
        result.put("reinitializationAfterInvalidUpdateFailure", failureType(producer::initProduceAccumulator));
        result.put("delayAfterReinitializationFailure", producer.getBatchMaxDelayMs());
        producer.shutdown();

        DefaultMQProducer configured = new DefaultMQProducer("configured-before-accumulator");
        configured.setInstanceName("configured-before-accumulator");
        configured.batchMaxDelayMs(25);
        configured.batchMaxBytes(65_536);
        configured.totalBatchMaxBytes(1_048_576);
        result.put("configuredBeforeInitializationDelay", configured.getBatchMaxDelayMs());
        result.put("configuredBeforeInitializationBytes", configured.getBatchMaxBytes());
        result.put("configuredBeforeInitializationTotalBytes", configured.getTotalBatchMaxBytes());
        configured.initProduceAccumulator();
        result.put("configuredAfterInitializationDelay", configured.getBatchMaxDelayMs());
        result.put("configuredAfterInitializationBytes", configured.getBatchMaxBytes());
        result.put("configuredAfterInitializationTotalGetterBytes", configured.getTotalBatchMaxBytes());
        result.put("configuredAfterInitializationTotalHoldBytes", totalAccumulatorHoldBytes(configured));
        configured.shutdown();

        DefaultMQProducer invalidBeforeInitialization = new DefaultMQProducer("invalid-before-accumulator");
        invalidBeforeInitialization.setInstanceName("invalid-before-accumulator");
        invalidBeforeInitialization.batchMaxDelayMs(0);
        result.put("invalidBeforeInitializationSetterFailure", "none");
        result.put("invalidBeforeInitializationInitFailure",
            failureType(invalidBeforeInitialization::initProduceAccumulator));
        result.put("invalidBeforeInitializationEffectiveDelay", invalidBeforeInitialization.getBatchMaxDelayMs());
        invalidBeforeInitialization.shutdown();
        return result;
    }

    private static String failureType(Check check) {
        try {
            check.run();
            return "none";
        } catch (Throwable error) {
            return error.getClass().getName();
        }
    }

    private static Map<String, Object> implementationPublishInfoTable() {
        DefaultMQProducer producer = new DefaultMQProducer("producer-publish-info-table");
        var implementation = producer.getDefaultMQProducerImpl();
        ConcurrentMap<String, TopicPublishInfo> table = implementation.getTopicPublishInfoTable();
        TopicPublishInfo info = new TopicPublishInfo();
        MessageQueue original = new MessageQueue("orders", "broker-a", 0);
        MessageQueue replacement = new MessageQueue("orders", "broker-b", 1);
        List<MessageQueue> originalList = new ArrayList<>(List.of(original));
        info.setMessageQueueList(originalList);
        TopicRouteData route = new TopicRouteData();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("broker-b");
        queueData.setWriteQueueNums(3);
        route.setQueueDatas(new ArrayList<>(List.of(queueData)));
        info.setTopicRouteData(route);
        info.setOrderTopic(true);
        info.setHaveTopicRouterInfo(true);
        ThreadLocalIndex sendIndex = new ThreadLocalIndex();
        info.setSendWhichQueue(sendIndex);

        table.put("orders", info);
        TopicPublishInfo fromTable = table.get("orders");
        List<MessageQueue> retainedList = fromTable.getMessageQueueList();
        retainedList.clear();
        retainedList.add(replacement);

        Map<String, Object> result = new TreeMap<>();
        result.put("getterReturnsLiveMap", table == implementation.getTopicPublishInfoTable());
        result.put("tableValueRetainsIdentity", fromTable == info);
        result.put("queueListRetainsIdentity", retainedList == fromTable.getMessageQueueList());
        result.put("queueListMutationAffectsOk", fromTable.ok());
        result.put("queueListMutationAffectsSelection", fromTable.selectOneMessageQueue() == replacement);
        result.put("orderTopic", fromTable.isOrderTopic());
        result.put("haveTopicRouterInfo", fromTable.isHaveTopicRouterInfo());
        result.put("sendIndexRetainsIdentity", fromTable.getSendWhichQueue() == sendIndex);
        result.put("writeQueueNumsByBroker", fromTable.getWriteQueueNumsByBroker("broker-b"));
        result.put("routeDataRetainsIdentity", fromTable.getTopicRouteData() == route);

        List<MessageQueue> staleList = fromTable.getMessageQueueList();
        fromTable.setMessageQueueList(new ArrayList<>(List.of(original)));
        staleList.clear();
        result.put("listSetterReplacesAlias", fromTable.getMessageQueueList().size() == 1
            && fromTable.getMessageQueueList().get(0) == original);
        TopicPublishInfo replacementInfo = new TopicPublishInfo();
        TopicPublishInfo previous = table.put("orders", replacementInfo);
        result.put("tablePutReturnsPreviousIdentity", previous == info);
        result.put("replacementValueIsVisible", table.get("orders") == replacementInfo);
        result.put("toStringContainsState", fromTable.toString().contains("haveTopicRouterInfo=true"));
        table.remove("orders");
        result.put("removeAffectsRetainedMap", !table.containsKey("orders") && table.isEmpty());
        return result;
    }

    private static Map<String, Object> implementationPublishInfoTableAdvanced() {
        DefaultMQProducer producer = new DefaultMQProducer("producer-publish-info-table-advanced");
        ConcurrentMap<String, TopicPublishInfo> table =
            producer.getDefaultMQProducerImpl().getTopicPublishInfoTable();
        TopicPublishInfo first = new TopicPublishInfo();
        TopicPublishInfo second = new TopicPublishInfo();
        TopicPublishInfo third = new TopicPublishInfo();
        Map<String, Object> result = new TreeMap<>();

        TopicPublishInfo computed = table.computeIfAbsent("compute", key -> first);
        TopicPublishInfo replaced = table.computeIfPresent("compute", (key, current) -> second);
        boolean computeIfPresentIdentity = replaced == second && table.get("compute") == second;
        TopicPublishInfo merged = table.merge("compute", third, (current, incoming) -> current);
        boolean mergeKeepsCurrentIdentity = merged == second && table.get("compute") == second;
        boolean computeRemoved = table.compute("compute", (key, current) -> null) == null
            && !table.containsKey("compute");
        result.put("computeIfAbsentIdentity", computed == first);
        result.put("computeIfPresentIdentity", computeIfPresentIdentity);
        result.put("mergeKeepsCurrentIdentity", mergeKeepsCurrentIdentity);
        result.put("computeNullRemoves", computeRemoved);

        table.put("replace", first);
        TopicPublishInfo replacedValue = table.replace("replace", second);
        boolean conditionalReplace = table.replace("replace", second, third)
            && table.get("replace") == third;
        result.put("replaceReturnsPreviousIdentity", replacedValue == first);
        result.put("conditionalReplaceUsesIdentity", conditionalReplace);
        table.replaceAll((key, current) -> second);
        result.put("replaceAllUsesLiveValues", table.get("replace") == second);

        Set<String> keys = table.keySet();
        table.put("key-view", first);
        result.put("keyViewRemovesFromMap", keys.remove("key-view") && !table.containsKey("key-view"));
        table.put("key-bulk-remove", first);
        table.put("key-bulk-retain", second);
        boolean keyBulk = keys.removeAll(Set.of("key-bulk-remove"))
            && !table.containsKey("key-bulk-remove")
            && keys.retainAll(Set.of("key-bulk-retain"))
            && !table.containsKey("replace") && table.containsKey("key-bulk-retain")
            && table.size() == 1;
        result.put("keyViewBulkMutation", keyBulk);

        Map<String, TopicPublishInfo> putAllRows = new LinkedHashMap<>();
        putAllRows.put("put-all-before", first);
        putAllRows.put("put-all-null", null);
        putAllRows.put("put-all-after", third);
        boolean putAllRejected = false;
        try {
            table.putAll(putAllRows);
        } catch (NullPointerException expected) {
            putAllRejected = true;
        }
        result.put("putAllNullValueKeepsEarlier", putAllRejected
            && table.containsKey("put-all-before") && !table.containsKey("put-all-after"));
        table.remove("put-all-before");

        var values = table.values();
        table.put("value-view", first);
        result.put("valuesViewRemovesByIdentity", values.remove(first) && !table.containsKey("value-view"));
        table.put("value-bulk-a", first);
        table.put("value-bulk-b", first);
        table.put("value-bulk-c", second);
        boolean valuesRemoveAll = values.removeAll(List.of(first))
            && !table.containsKey("value-bulk-a") && !table.containsKey("value-bulk-b")
            && table.containsKey("value-bulk-c");
        table.put("value-retain-a", first);
        table.put("value-retain-b", second);
        boolean valuesRetainAll = values.retainAll(List.of(first))
            && table.containsKey("value-retain-a") && !table.containsKey("value-retain-b")
            && !table.containsKey("replace") && !table.containsKey("key-bulk-retain")
            && !table.containsKey("value-bulk-c");
        result.put("valuesViewBulkMutation", valuesRemoveAll && valuesRetainAll);
        var entries = table.entrySet();
        table.put("entry-view", first);
        var entry = entries.stream().filter(candidate -> "entry-view".equals(candidate.getKey()))
            .findFirst().orElseThrow();
        TopicPublishInfo entryPrevious = entry.setValue(third);
        boolean entryWrite = entryPrevious == first && table.get("entry-view") == third;
        result.put("entryViewSetValueWritesThrough", entryWrite);
        result.put("entryViewRemoveUsesKeyAndIdentity",
            entries.remove(new java.util.AbstractMap.SimpleEntry<>("entry-view", third))
                && !table.containsKey("entry-view"));
        table.put("entry-bulk-remove", first);
        table.put("entry-bulk-retain", third);
        boolean entryRemoveAll = entries.removeAll(Set.of(
            new java.util.AbstractMap.SimpleEntry<>("entry-bulk-remove", first)));
        boolean entryRetainAll = entries.retainAll(Set.of(
            new java.util.AbstractMap.SimpleEntry<>("entry-bulk-retain", third)));
        result.put("entryViewBulkMutation", entryRemoveAll && entryRetainAll
            && table.size() == 1 && table.get("entry-bulk-retain") == third);
        table.put("key-remove-if", first);
        result.put("keyViewRemoveIfMutation",
            keys.removeIf("key-remove-if"::equals) && !table.containsKey("key-remove-if"));
        table.put("value-remove-if-match", first);
        table.put("value-remove-if-keep", second);
        result.put("valuesViewRemoveIfMutation", values.removeIf(value -> value == first)
            && !table.containsKey("value-remove-if-match")
            && table.get("value-remove-if-keep") == second);
        table.put("entry-remove-if-match", first);
        table.put("entry-remove-if-keep", third);
        result.put("entryViewRemoveIfMutation", entries.removeIf(candidate ->
            "entry-remove-if-match".equals(candidate.getKey()))
            && !table.containsKey("entry-remove-if-match")
            && table.get("entry-remove-if-keep") == third);
        table.clear();
        result.put("clearAffectsRetainedViews", keys.isEmpty() && values.isEmpty() && entries.isEmpty());

        result.put("getOrDefaultMissingIdentity", table.getOrDefault("missing", second) == second);
        table.put("map-op-present", first);
        result.put("getOrDefaultPresentIdentity",
            table.getOrDefault("map-op-present", second) == first);
        result.put("containsValueIdentity", table.containsValue(first));
        TopicPublishInfo putIfAbsentPrevious = table.putIfAbsent("map-op-present", second);
        result.put("putIfAbsentKeepsCurrentIdentity",
            putIfAbsentPrevious == first && table.get("map-op-present") == first);
        TopicPublishInfo absentPrevious = table.putIfAbsent("map-op-absent", second);
        result.put("putIfAbsentInserts", absentPrevious == null
            && table.get("map-op-absent") == second);
        result.put("conditionalRemoveUsesIdentity",
            table.remove("map-op-absent", second) && !table.containsKey("map-op-absent"));
        table.put("for-each-a", first);
        table.put("for-each-b", second);
        List<String> forEachRows = new ArrayList<>();
        table.forEach((key, value) -> forEachRows.add(key + ":" + (value == first ? "first" : "other")));
        result.put("forEachVisitsLiveEntries", forEachRows.size() == 3
            && forEachRows.contains("map-op-present:first")
            && forEachRows.contains("for-each-a:first")
            && forEachRows.contains("for-each-b:other"));
        result.put("sizeAndEmptyMatch", table.size() == 3 && !table.isEmpty());
        table.clear();
        result.put("clearEmptiesLiveMap", table.isEmpty() && table.size() == 0);

        table.put("nullable", first);
        result.put("nullGetKey", nullPointerClass(() -> table.get(null)));
        result.put("nullGetOrDefaultKey", nullPointerClass(() -> table.getOrDefault(null, second)));
        result.put("nullContainsKey", nullPointerClass(() -> table.containsKey(null)));
        result.put("nullContainsValue", nullPointerClass(() -> table.containsValue(null)));
        result.put("nullPutKey", nullPointerClass(() -> table.put(null, second)));
        result.put("nullPutValue", nullPointerClass(() -> table.put("nullable-value", null)));
        result.put("nullPutIfAbsentKey", nullPointerClass(() -> table.putIfAbsent(null, second)));
        result.put("nullPutIfAbsentValue", nullPointerClass(() -> table.putIfAbsent("nullable", null)));
        result.put("nullPutAllMap", nullPointerClass(() -> table.putAll(null)));
        result.put("nullRemoveKey", nullPointerClass(() -> table.remove(null)));
        result.put("nullConditionalRemoveValue", !table.remove("nullable", null));
        result.put("nullReplaceKey", nullPointerClass(() -> table.replace(null, second)));
        result.put("nullReplaceValue", nullPointerClass(() -> table.replace("nullable", null)));
        result.put("nullConditionalReplaceExpected",
            nullPointerClass(() -> table.replace("nullable", null, second)));
        result.put("nullConditionalReplaceValue",
            nullPointerClass(() -> table.replace("nullable", first, null)));
        result.put("nullComputeIfAbsentKey",
            nullPointerClass(() -> table.computeIfAbsent(null, key -> second)));
        result.put("nullComputeIfPresentKey",
            nullPointerClass(() -> table.computeIfPresent(null, (key, old) -> second)));
        result.put("nullComputeKey", nullPointerClass(() -> table.compute(null, (key, old) -> second)));
        result.put("nullMergeKey", nullPointerClass(() -> table.merge(null, second, (old, value) -> value)));
        result.put("nullMergeValue",
            nullPointerClass(() -> table.merge("nullable", null, (old, value) -> value)));
        result.put("nullComputeIfAbsentCallback",
            nullPointerClass(() -> table.computeIfAbsent("nullable", null)));
        result.put("nullComputeIfPresentCallback",
            nullPointerClass(() -> table.computeIfPresent("nullable", null)));
        result.put("nullComputeCallback", nullPointerClass(() -> table.compute("nullable", null)));
        result.put("nullMergeCallback",
            nullPointerClass(() -> table.merge("nullable", second, null)));
        result.put("nullForEachCallback", nullPointerClass(() -> table.forEach(null)));
        result.put("nullReplaceAllCallback", nullPointerClass(() -> table.replaceAll(null)));
        table.put("compute-present-null", second);
        result.put("computeIfAbsentNullResultAllowed",
            table.computeIfAbsent("compute-absent-null", key -> null) == null
                && !table.containsKey("compute-absent-null"));
        result.put("computeIfPresentNullResultRemoves",
            table.computeIfPresent("compute-present-null", (key, old) -> null) == null
                && !table.containsKey("compute-present-null"));
        table.put("compute-null-result", second);
        result.put("computeNullResultRemoves",
            table.compute("compute-null-result", (key, old) -> null) == null
                && !table.containsKey("compute-null-result"));
        table.put("merge-null-result", second);
        result.put("mergeNullResultRemoves",
            table.merge("merge-null-result", first, (old, value) -> null) == null
                && !table.containsKey("merge-null-result"));
        table.put("replace-all-null-result", first);
        result.put("nullReplaceAllResult",
            nullPointerClass(() -> table.replaceAll((key, value) -> null)));
        result.put("nullReplaceAllResultPreservesMapping",
            table.get("replace-all-null-result") == first);
        result.put("nullGetOrDefaultFallbackAllowed",
            table.getOrDefault("missing", null) == null);
        result.put("nullArgumentsPreserveMapping", table.get("nullable") == first);
        result.put("nullKeySetContains", nullPointerClass(() -> table.keySet().contains(null)));
        result.put("nullKeySetRemove", nullPointerClass(() -> table.keySet().remove(null)));
        result.put("nullValuesContains", nullPointerClass(() -> table.values().contains(null)));
        result.put("nullValuesRemove", !table.values().remove(null));
        result.put("nullEntrySetContains", !table.entrySet().contains(null));
        result.put("nullEntrySetRemove", !table.entrySet().remove(null));
        result.put("nullKeyEntrySetContains",
            !table.entrySet().contains(new java.util.AbstractMap.SimpleEntry<>(null, first)));
        result.put("nullValueEntrySetContains",
            !table.entrySet().contains(new java.util.AbstractMap.SimpleEntry<>("nullable", null)));
        result.put("nullKeyEntrySetRemove",
            !table.entrySet().remove(new java.util.AbstractMap.SimpleEntry<>(null, first)));
        result.put("nullValueEntrySetRemove",
            !table.entrySet().remove(new java.util.AbstractMap.SimpleEntry<>("nullable", null)));
        result.put("nullKeySetRemoveAll", nullPointerClass(() -> table.keySet().removeAll(null)));
        result.put("nullKeySetRetainAll", nullPointerClass(() -> table.keySet().retainAll(null)));
        result.put("nullValuesRemoveAll", nullPointerClass(() -> table.values().removeAll(null)));
        result.put("nullValuesRetainAll", nullPointerClass(() -> table.values().retainAll(null)));
        result.put("nullEntrySetRemoveAll", nullPointerClass(() -> table.entrySet().removeAll(null)));
        result.put("nullEntrySetRetainAll", nullPointerClass(() -> table.entrySet().retainAll(null)));
        result.put("nullKeySetRemoveIf", nullPointerClass(() -> table.keySet().removeIf(null)));
        result.put("nullValuesRemoveIf", nullPointerClass(() -> table.values().removeIf(null)));
        result.put("nullEntrySetRemoveIf", nullPointerClass(() -> table.entrySet().removeIf(null)));
        var nullableEntry = table.entrySet().stream()
            .filter(candidate -> "nullable".equals(candidate.getKey())).findFirst().orElseThrow();
        result.put("nullEntrySetValue", nullPointerClass(() -> nullableEntry.setValue(null)));
        result.put("nullEntrySetValuePreservesMapping",
            nullableEntry.getValue() == first && table.get("nullable") == first);
        return result;
    }

    private static Map<String, Object> implementationQueueFilterSelection() {
        TopicPublishInfo info = new TopicPublishInfo();
        MessageQueue queue = new MessageQueue("orders", "broker-a", 2);
        info.setMessageQueueList(new ArrayList<>(List.of(queue)));
        Map<String, Object> result = new TreeMap<>();
        result.put("nullFilterArraySelection", queueId(info.selectOneMessageQueue((TopicPublishInfo.QueueFilter[]) null)));
        result.put("emptyFilterArraySelection", queueId(info.selectOneMessageQueue(new TopicPublishInfo.QueueFilter[0])));
        result.put("nullFilterWithQueue", nullPointerClass(() -> info.selectOneMessageQueue((TopicPublishInfo.QueueFilter) null)));

        TopicPublishInfo empty = new TopicPublishInfo();
        empty.setMessageQueueList(new ArrayList<>());
        result.put("nullFilterWithEmptyQueueList", queueId(empty.selectOneMessageQueue((TopicPublishInfo.QueueFilter) null)));

        TopicPublishInfo missing = new TopicPublishInfo();
        missing.setMessageQueueList(null);
        result.put("nullFilterWithNullQueueList", queueId(missing.selectOneMessageQueue((TopicPublishInfo.QueueFilter) null)));

        List<String> filterOrder = new ArrayList<>();
        TopicPublishInfo.QueueFilter[] filters = {
            candidate -> {
                filterOrder.add("reject");
                return false;
            },
            candidate -> {
                filterOrder.add("accept");
                return true;
            }
        };
        result.put("nonShortCircuitSelection", queueId(info.selectOneMessageQueue(filters)));
        result.put("nonShortCircuitFilterOrder", filterOrder);
        return result;
    }

    private static Map<String, Object> implementationSelectorFailure() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-selector-failure");
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        org.apache.rocketmq.client.ClientConfig clientConfig = new org.apache.rocketmq.client.ClientConfig();
        clientConfig.setClientIP("producer-selector-failure");
        MQClientInstance factory = new MQClientInstance(clientConfig, 0, clientConfig.buildMQClientId());
        try {
            Field factoryField = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
            factoryField.setAccessible(true);
            factoryField.set(implementation, factory);
            implementation.setServiceState(ServiceState.RUNNING);

            TopicPublishInfo topicPublishInfo = new TopicPublishInfo();
            topicPublishInfo.setMessageQueueList(new ArrayList<>(List.of(
                new MessageQueue("orders", "broker-a", 0))));
            implementation.updateTopicPublishInfo("orders", topicPublishInfo);

            Map<String, Object> result = new TreeMap<>();
            Message message = new Message("orders", new byte[] {1});
            result.put("invoke", selectorFailureCase(implementation, message, true));
            result.put("send", selectorFailureCase(implementation, message, false));
            return result;
        } finally {
            implementation.setServiceState(ServiceState.SHUTDOWN_ALREADY);
            Field factoryState = MQClientInstance.class.getDeclaredField("serviceState");
            factoryState.setAccessible(true);
            factoryState.set(factory, ServiceState.RUNNING);
            factory.shutdown();
        }
    }

    private static Map<String, Object> implementationSelectorMessageSnapshot() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-selector-snapshot");
        producer.setNamespace("tenant");
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        org.apache.rocketmq.client.ClientConfig clientConfig = new org.apache.rocketmq.client.ClientConfig();
        clientConfig.setClientIP("producer-selector-snapshot");
        clientConfig.setNamespace("tenant");
        MQClientInstance factory = new MQClientInstance(clientConfig, 0, clientConfig.buildMQClientId());
        try {
            Field factoryField = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
            factoryField.setAccessible(true);
            factoryField.set(implementation, factory);
            implementation.setServiceState(ServiceState.RUNNING);

            TopicPublishInfo topicPublishInfo = new TopicPublishInfo();
            topicPublishInfo.setMessageQueueList(new ArrayList<>(List.of(
                new MessageQueue("tenant%orders", "broker-a", 0))));
            implementation.updateTopicPublishInfo("tenant%orders", topicPublishInfo);

            Message message = new Message("tenant%orders", new byte[] {1, 2});
            message.setFlag(7);
            message.setTransactionId("source-transaction-id");
            MessageAccessor.putProperty(message, "fixture-key", "fixture-value");
            Map<String, Object> result = new TreeMap<>();
            MessageQueue selected = implementation.invokeMessageQueueSelector(message,
                (queues, selectorMessage, argument) -> {
                    result.put("selectorMessageIsDistinct", selectorMessage != message);
                    result.put("selectorTopic", selectorMessage.getTopic());
                    result.put("selectorBodyShared", selectorMessage.getBody() == message.getBody());
                    result.put("selectorFlag", selectorMessage.getFlag());
                    result.put("selectorPropertiesShared",
                        selectorMessage.getProperties() == message.getProperties());
                    result.put("selectorProperty", selectorMessage.getProperty("fixture-key"));
                    result.put("selectorTransactionIdIsNull", selectorMessage.getTransactionId() == null);
                    result.put("selectorQueueTopic", queues.get(0).getTopic());
                    return queues.get(0);
                }, new Object(), 10_000);
            result.put("selectedQueueTopic", selected.getTopic());
            result.put("sourceTopic", message.getTopic());
            result.put("sourceTransactionId", message.getTransactionId());
            return result;
        } finally {
            implementation.setServiceState(ServiceState.SHUTDOWN_ALREADY);
            Field factoryState = MQClientInstance.class.getDeclaredField("serviceState");
            factoryState.setAccessible(true);
            factoryState.set(factory, ServiceState.RUNNING);
            factory.shutdown();
        }
    }

    private static Map<String, Object> selectorFailureCase(DefaultMQProducerImpl implementation,
        Message message, boolean invokeOnly) {
        Map<String, Object> result = new TreeMap<>();
        AtomicInteger selectorCalls = new AtomicInteger();
        List<Integer> queueCounts = new ArrayList<>();
        Object selectorArgument = new Object();
        MessageQueueSelector selector = (queues, selectedMessage, argument) -> {
            selectorCalls.incrementAndGet();
            queueCounts.add(queues.size());
            if (selectedMessage == message || !"orders".equals(selectedMessage.getTopic())
                || argument != selectorArgument) {
                throw new IllegalStateException("selector inputs did not match");
            }
            throw new IllegalStateException("fixture-selector-failure");
        };
        Throwable failure = null;
        try {
            if (invokeOnly) {
                implementation.invokeMessageQueueSelector(message, selector, selectorArgument, 10_000);
            } else {
                implementation.send(message, selector, selectorArgument, 10_000);
            }
        } catch (Throwable error) {
            failure = error;
        }
        result.put("selectorCalls", selectorCalls.get());
        result.put("queueCounts", queueCounts);
        result.put("errorClass", failure == null ? "none" : failure.getClass().getSimpleName());
        result.put("errorMessage", failure == null ? "" : failure.getMessage());
        Throwable cause = failure == null ? null : failure.getCause();
        result.put("causeClass", cause == null ? "none" : cause.getClass().getSimpleName());
        result.put("causeMessage", cause == null ? "" : cause.getMessage());
        return result;
    }

    private static Integer queueId(MessageQueue queue) {
        return queue == null ? null : queue.getQueueId();
    }

    private static String nullPointerClass(Runnable action) {
        try {
            action.run();
            return "none";
        } catch (NullPointerException expected) {
            return expected.getClass().getSimpleName();
        }
    }

    private static String constructorSignature(Constructor<?> constructor) {
        return constructor.getDeclaringClass().getName() + "(" + Arrays.stream(constructor.getParameterTypes())
            .map(Class::getName)
            .collect(java.util.stream.Collectors.joining(",")) + ")";
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getName() + " " + method.getDeclaringClass().getName() + "."
            + method.getName() + "(" + Arrays.stream(method.getParameterTypes())
            .map(Class::getName)
            .collect(java.util.stream.Collectors.joining(",")) + ")";
    }

    private static Map<String, Object> publicApiSurface(Class<?> apiType) {
        List<String> constructors = Arrays.stream(apiType.getDeclaredConstructors())
            .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
            .map(ProducerCoreFixtures::constructorSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList());
        List<String> declaredMethods = Arrays.stream(apiType.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList());
        List<String> methods = Arrays.stream(apiType.getMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getDeclaringClass() != Object.class)
            .map(ProducerCoreFixtures::methodSignature)
            .distinct()
            .sorted()
            .collect(java.util.stream.Collectors.toList());
        Map<String, Object> result = new TreeMap<>();
        result.put("className", apiType.getName());
        result.put("publicConstructors", constructors);
        result.put("declaredPublicMethods", declaredMethods);
        result.put("declaredPublicMethodCount", declaredMethods.size());
        result.put("publicMethods", methods);
        result.put("publicMethodCount", methods.size());
        return result;
    }

    private static List<String> publicFieldSignatures(Class<?> apiType) {
        return Arrays.stream(apiType.getDeclaredFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(field -> field.getType().getName() + " " + field.getDeclaringClass().getName()
                + "." + field.getName())
            .sorted()
            .collect(java.util.stream.Collectors.toList());
    }

    private static Map<String, Object> brokerConfigProperties(String input) {
        Map<String, Object> result = new TreeMap<>();
        result.put("input", input);
        Properties properties = MixAll.string2Properties(input);
        result.put("isNull", properties == null);
        Map<String, String> entries = new TreeMap<>();
        if (properties != null) {
            for (String name : properties.stringPropertyNames()) {
                entries.put(name, properties.getProperty(name));
            }
        }
        result.put("entries", entries);
        return result;
    }

    private static Map<String, Object> coldDataFlowAndReadAheadOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        java.util.ArrayDeque<org.apache.rocketmq.remoting.protocol.RemotingCommand> replies =
            new java.util.ArrayDeque<>();
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "update rejected"));
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "remove rejected"));
        org.apache.rocketmq.remoting.protocol.RemotingCommand info =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null);
        info.setBody("coldData=enabled\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        replies.add(info);
        org.apache.rocketmq.remoting.protocol.RemotingCommand emptyInfo =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null);
        emptyInfo.setBody(new byte[0]);
        replies.add(emptyInfo);
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "unavailable"));
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, "updated"));
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, ""));
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "mode rejected"));

        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("cold-data-flow-api");
        config.setVipChannelEnabled(false);
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                    java.lang.reflect.Proxy.newProxyInstance(
                        ProducerCoreFixtures.class.getClassLoader(),
                        new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                        (proxy, method, values) -> {
                            if ("invokeSync".equals(method.getName())) {
                                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                    (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                                Map<String, Object> call = new TreeMap<>();
                                call.put("address", values[0]);
                                call.put("requestCode", request.getCode());
                                call.put("timeoutMillis", values[2]);
                                call.put("extFields", request.getExtFields());
                                call.put("body", request.getBody() == null ? null
                                    : new String(request.getBody(), java.nio.charset.StandardCharsets.UTF_8));
                                org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                    replies.removeFirst();
                                call.put("responseCode", response.getCode());
                                call.put("responseRemark", response.getRemark());
                                call.put("responseBody", response.getBody() == null ? null
                                    : new String(response.getBody(), java.nio.charset.StandardCharsets.UTF_8));
                                requests.add(call);
                                return response;
                            }
                            return method.getReturnType() == boolean.class ? false : null;
                        }));
        try {
            java.util.Properties properties = new java.util.Properties();
            api.updateColdDataFlowCtrGroupConfig("127.0.0.1:10913", new java.util.Properties(), 1234L);
            result.put("callsAfterEmptyUpdate", requests.size());
            properties.setProperty("group", "workers");
            api.updateColdDataFlowCtrGroupConfig("127.0.0.1:10913", properties, 1234L);
            result.put("updateOutcome", "returned");
            try {
                api.updateColdDataFlowCtrGroupConfig("127.0.0.1:10913", properties, 1234L);
                result.put("updateErrorOutcome", "returned");
            } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
                result.put("updateErrorOutcome", error.getClass().getSimpleName());
                result.put("updateErrorCode", error.getResponseCode());
                result.put("updateErrorMessage", error.getMessage());
            }
            api.removeColdDataFlowCtrGroupConfig("127.0.0.1:10913", null, 1234L);
            api.removeColdDataFlowCtrGroupConfig("127.0.0.1:10913", "", 1234L);
            result.put("callsAfterNullAndEmptyRemove", requests.size());
            api.removeColdDataFlowCtrGroupConfig("127.0.0.1:10913", "workers", 1234L);
            result.put("removeOutcome", "returned");
            try {
                api.removeColdDataFlowCtrGroupConfig("127.0.0.1:10913", "workers", 1234L);
                result.put("removeErrorOutcome", "returned");
            } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
                result.put("removeErrorOutcome", error.getClass().getSimpleName());
                result.put("removeErrorCode", error.getResponseCode());
                result.put("removeErrorMessage", error.getMessage());
            }
            result.put("infoValue", api.getColdDataFlowCtrInfo("127.0.0.1:10913", 1234L));
            result.put("emptyInfoValue", api.getColdDataFlowCtrInfo("127.0.0.1:10913", 1234L));
            result.put("missingInfoValue", api.getColdDataFlowCtrInfo("127.0.0.1:10913", 1234L));
            try {
                api.getColdDataFlowCtrInfo("127.0.0.1:10913", 1234L);
                result.put("infoErrorOutcome", "returned");
            } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
                result.put("infoErrorOutcome", error.getClass().getSimpleName());
                result.put("infoErrorCode", error.getResponseCode());
                result.put("infoErrorMessage", error.getMessage());
            }
            result.put("setModeResult", api.setCommitLogReadAheadMode(
                "127.0.0.1:10913", "ASYNC", 1234L));
            result.put("emptyModeResult", api.setCommitLogReadAheadMode(
                "127.0.0.1:10913", "SYNC", 1234L));
            result.put("nullModeResult", api.setCommitLogReadAheadMode(
                "127.0.0.1:10913", null, 1234L));
            try {
                api.setCommitLogReadAheadMode("127.0.0.1:10913", "INVALID", 1234L);
                result.put("modeErrorOutcome", "returned");
            } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
                result.put("modeErrorOutcome", error.getClass().getSimpleName());
                result.put("modeErrorCode", error.getResponseCode());
                result.put("modeErrorMessage", error.getMessage());
            }
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> registerClientOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        java.util.ArrayDeque<org.apache.rocketmq.remoting.protocol.RemotingCommand> replies =
            new java.util.ArrayDeque<>();
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "registration rejected"));

        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("register-client-api");
        config.setVipChannelEnabled(true);
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                    java.lang.reflect.Proxy.newProxyInstance(
                        ProducerCoreFixtures.class.getClassLoader(),
                        new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                        (proxy, method, values) -> {
                            if ("invokeSync".equals(method.getName())) {
                                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                    (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                                Map<String, Object> call = new TreeMap<>();
                                call.put("address", values[0]);
                                call.put("requestCode", request.getCode());
                                call.put("timeoutMillis", values[2]);
                                call.put("extFields", request.getExtFields());
                                call.put("body", request.getBody() == null ? null
                                    : new String(request.getBody(), java.nio.charset.StandardCharsets.UTF_8));
                                org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                    replies.removeFirst();
                                call.put("responseCode", response.getCode());
                                call.put("responseRemark", response.getRemark());
                                requests.add(call);
                                return response;
                            }
                            return method.getReturnType() == boolean.class ? false : null;
                        }));
        try {
            org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData heartbeat =
                new org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData();
            heartbeat.setClientID("registration-client");
            result.put("success", api.registerClient("127.0.0.1:10913", heartbeat, 4321L));
            result.put("nonSuccess", api.registerClient("127.0.0.1:10913", heartbeat, 4321L));
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> topicListFromNameServerOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        java.util.ArrayDeque<org.apache.rocketmq.remoting.protocol.RemotingCommand> replies =
            new java.util.ArrayDeque<>();
        org.apache.rocketmq.remoting.protocol.body.TopicList topicList =
            new org.apache.rocketmq.remoting.protocol.body.TopicList();
        topicList.setTopicList(new java.util.HashSet<>(List.of("orders", "payments")));
        topicList.setBrokerAddr("broker-a:10911");
        org.apache.rocketmq.remoting.protocol.RemotingCommand success =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null);
        success.setBody(topicList.encode());
        replies.add(success);
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "nameserver unavailable"));

        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("topic-list-nameserver-api");
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                    java.lang.reflect.Proxy.newProxyInstance(
                        ProducerCoreFixtures.class.getClassLoader(),
                        new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                        (proxy, method, values) -> {
                            if ("invokeSync".equals(method.getName())) {
                                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                    (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                                Map<String, Object> call = new TreeMap<>();
                                call.put("addressWasNull", values[0] == null);
                                call.put("requestCode", request.getCode());
                                call.put("timeoutMillis", values[2]);
                                call.put("extFieldsPresent", request.getExtFields() != null
                                    && !request.getExtFields().isEmpty());
                                call.put("requestBodyPresent", request.getBody() != null);
                                org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                    replies.removeFirst();
                                call.put("responseCode", response.getCode());
                                call.put("responseRemark", response.getRemark());
                                call.put("responseBodyBase64", response.getBody() == null ? null
                                    : java.util.Base64.getEncoder().encodeToString(response.getBody()));
                                requests.add(call);
                                return response;
                            }
                            return method.getReturnType() == boolean.class ? false : null;
                        }));
        try {
            org.apache.rocketmq.remoting.protocol.body.TopicList received =
                api.getTopicListFromNameServer(2345L);
            result.put("topics", new TreeSet<>(received.getTopicList()));
            result.put("brokerAddr", received.getBrokerAddr());
            try {
                api.getTopicListFromNameServer(3456L);
                result.put("errorOutcome", "returned");
            } catch (org.apache.rocketmq.client.exception.MQClientException error) {
                result.put("errorOutcome", error.getClass().getSimpleName());
                result.put("errorCode", error.getResponseCode());
                result.put("errorMessage", error.getMessage());
            }
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> systemTopicListFromBrokerOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        java.util.ArrayDeque<org.apache.rocketmq.remoting.protocol.RemotingCommand> replies =
            new java.util.ArrayDeque<>();
        org.apache.rocketmq.remoting.protocol.body.TopicList topicList =
            new org.apache.rocketmq.remoting.protocol.body.TopicList();
        topicList.setTopicList(new java.util.HashSet<>(List.of("rmq_sys_TRACE_DATA", "rmq_sys_TRANS_HALF_TOPIC")));
        org.apache.rocketmq.remoting.protocol.RemotingCommand success =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null);
        success.setBody(topicList.encode());
        replies.add(success);
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "system topics unavailable"));
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));

        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("system-topic-list-api");
        config.setVipChannelEnabled(false);
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                    java.lang.reflect.Proxy.newProxyInstance(
                        ProducerCoreFixtures.class.getClassLoader(),
                        new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                        (proxy, method, values) -> {
                            if ("invokeSync".equals(method.getName())) {
                                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                    (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                                Map<String, Object> call = new TreeMap<>();
                                call.put("address", values[0]);
                                call.put("requestCode", request.getCode());
                                call.put("timeoutMillis", values[2]);
                                call.put("extFieldsPresent", request.getExtFields() != null
                                    && !request.getExtFields().isEmpty());
                                call.put("requestBodyPresent", request.getBody() != null);
                                org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                    replies.removeFirst();
                                call.put("responseCode", response.getCode());
                                call.put("responseRemark", response.getRemark());
                                call.put("responseBodyBase64", response.getBody() == null ? null
                                    : java.util.Base64.getEncoder().encodeToString(response.getBody()));
                                requests.add(call);
                                return response;
                            }
                            return method.getReturnType() == boolean.class ? false : null;
                        }));
        try {
            org.apache.rocketmq.remoting.protocol.body.TopicList received =
                api.getSystemTopicListFromBroker("broker-a:10911", 2345L);
            result.put("topics", new TreeSet<>(received.getTopicList()));
            try {
                api.getSystemTopicListFromBroker("broker-a:10911", 3456L);
                result.put("errorOutcome", "returned");
            } catch (org.apache.rocketmq.client.exception.MQClientException error) {
                result.put("errorOutcome", error.getClass().getSimpleName());
                result.put("errorCode", error.getResponseCode());
                result.put("errorMessage", error.getMessage());
            }
            try {
                api.getSystemTopicListFromBroker("broker-a:10911", 4567L);
                result.put("missingBodyOutcome", "returned");
            } catch (org.apache.rocketmq.client.exception.MQClientException error) {
                result.put("missingBodyOutcome", error.getClass().getSimpleName());
                result.put("missingBodyCode", error.getResponseCode());
            }
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> systemTopicListOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(systemTopicListCase("merge",
            systemTopicListReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, true,
                List.of("rmq_sys_CLUSTER", "rmq_sys_TRACE_DATA"), "broker-a:10911"),
            systemTopicListReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, true,
                List.of("rmq_sys_TRACE_DATA", "rmq_sys_TRANS_HALF_TOPIC"), null)));
        cases.add(systemTopicListCase("empty-name-server-list",
            systemTopicListReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, true,
                List.of(), "broker-a:10911"), null));
        cases.add(systemTopicListCase("blank-broker-address",
            systemTopicListReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, true,
                List.of("rmq_sys_CLUSTER"), "  "), null));
        cases.add(systemTopicListCase("null-name-server-list",
            systemTopicListReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, true,
                null, "broker-a:10911"), null));
        cases.add(systemTopicListCase("name-server-error",
            systemTopicListReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
                "system topics unavailable", false, null, null), null));
        cases.add(systemTopicListCase("name-server-missing-body",
            systemTopicListReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
                false, null, null), null));
        cases.add(systemTopicListCase("broker-error",
            systemTopicListReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, true,
                List.of("rmq_sys_CLUSTER"), "broker-a:10911"),
            systemTopicListReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
                "system topics unavailable", false, null, null)));
        result.put("cases", cases);
        return result;
    }

    private static org.apache.rocketmq.remoting.protocol.RemotingCommand systemTopicListReply(
        int code, String remark, boolean bodyPresent, List<String> topics, String brokerAddr) {
        org.apache.rocketmq.remoting.protocol.RemotingCommand reply =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(code, remark);
        if (bodyPresent) {
            org.apache.rocketmq.remoting.protocol.body.TopicList topicList =
                new org.apache.rocketmq.remoting.protocol.body.TopicList();
            topicList.setTopicList(topics == null ? null : new java.util.HashSet<>(topics));
            topicList.setBrokerAddr(brokerAddr);
            reply.setBody(topicList.encode());
        }
        return reply;
    }

    private static Map<String, Object> systemTopicListCase(
        String name,
        org.apache.rocketmq.remoting.protocol.RemotingCommand nameServerReply,
        org.apache.rocketmq.remoting.protocol.RemotingCommand brokerReply) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> calls = new ArrayList<>();
        java.util.ArrayDeque<org.apache.rocketmq.remoting.protocol.RemotingCommand> replies =
            new java.util.ArrayDeque<>();
        replies.add(nameServerReply);
        if (brokerReply != null) replies.add(brokerReply);
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("system-topic-list-api");
        config.setVipChannelEnabled(false);
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                    java.lang.reflect.Proxy.newProxyInstance(
                        ProducerCoreFixtures.class.getClassLoader(),
                        new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                        (proxy, method, values) -> {
                            if ("invokeSync".equals(method.getName())) {
                                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                    (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                                Map<String, Object> call = new TreeMap<>();
                                call.put("addressWasNull", values[0] == null);
                                if (values[0] != null) call.put("address", values[0]);
                                call.put("requestCode", request.getCode());
                                call.put("timeoutMillis", values[2]);
                                call.put("extFieldsPresent", request.getExtFields() != null
                                    && !request.getExtFields().isEmpty());
                                call.put("requestBodyPresent", request.getBody() != null);
                                calls.add(call);
                                return replies.removeFirst();
                            }
                            return method.getReturnType() == boolean.class ? false : null;
                        }));
        result.put("name", name);
        result.put("nameServerResponseCode", nameServerReply.getCode());
        result.put("nameServerResponseRemark", nameServerReply.getRemark());
        result.put("nameServerResponseBodyBase64", nameServerReply.getBody() == null ? null
            : java.util.Base64.getEncoder().encodeToString(nameServerReply.getBody()));
        if (brokerReply != null) {
            result.put("brokerResponseCode", brokerReply.getCode());
            result.put("brokerResponseRemark", brokerReply.getRemark());
            result.put("brokerResponseBodyBase64", brokerReply.getBody() == null ? null
                : java.util.Base64.getEncoder().encodeToString(brokerReply.getBody()));
        }
        try {
            org.apache.rocketmq.remoting.protocol.body.TopicList received = api.getSystemTopicList(2345L);
            result.put("outcome", "returned");
            result.put("topics", received.getTopicList() == null ? null : new TreeSet<>(received.getTopicList()));
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("calls", calls);
        return result;
    }

    private static Map<String, Object> createSubscriptionGroupOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        SubscriptionGroupConfig workers = new SubscriptionGroupConfig();
        workers.setGroupName("workers");
        workers.setRetryQueueNums(7);
        SubscriptionGroupConfig defaults = new SubscriptionGroupConfig();
        cases.add(createSubscriptionGroupCase("populated", workers,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createSubscriptionGroupCase("default", defaults,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createSubscriptionGroupCase("null-config", null,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createSubscriptionGroupCase("broker-error", workers,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
            "subscription group rejected"));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> createSubscriptionGroupCase(
        String name,
        SubscriptionGroupConfig config,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig clientConfig =
            new org.apache.rocketmq.client.ClientConfig();
        clientConfig.setClientIP("create-subscription-group-api");
        clientConfig.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            clientConfig, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFieldsPresent", request.getExtFields() != null
                                && !request.getExtFields().isEmpty());
                            call.put("requestBodyBase64", request.getBody() == null ? null
                                : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                            requests.add(call);
                            return org.apache.rocketmq.remoting.protocol.RemotingCommand
                                .createResponseCommand(responseCode, responseRemark);
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("config", config);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            api.createSubscriptionGroup("broker-a:10911", config, 2345L);
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> createSubscriptionGroupListOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig workers =
            new org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig();
        workers.setGroupName("workers");
        workers.setRetryQueueNums(7);
        org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig analytics =
            new org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig();
        analytics.setGroupName("analytics");
        analytics.setConsumeBroadcastEnable(false);
        analytics.setConsumeMessageOrderly(true);
        cases.add(createSubscriptionGroupListCase("populated",
            List.of(workers, analytics), org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createSubscriptionGroupListCase("empty",
            List.of(), org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createSubscriptionGroupListCase("broker-error",
            List.of(workers), org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
            "subscription groups rejected"));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> kvConfigOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(kvConfigCase("get-value", "get", "tenant", "timeout", null,
            List.of(), List.of(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS),
            java.util.Collections.singletonList(null), List.of("resolved-value"), java.util.Collections.singletonList(null)));
        cases.add(kvConfigCase("get-missing-value", "get", "tenant", "missing", null,
            List.of(), List.of(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS),
            java.util.Collections.singletonList(null), java.util.Collections.singletonList(null), java.util.Collections.singletonList(null)));
        cases.add(kvConfigCase("get-error", "get", "tenant", "denied", null,
            List.of(), List.of(org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR),
            List.of("lookup rejected"), java.util.Collections.singletonList(null), java.util.Collections.singletonList(null)));
        cases.add(kvConfigCase("put-last-error", "put", "tenant", "key", "value",
            List.of("ns-a:9876", "ns-b:9876", "ns-c:9876"),
            List.of(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
                org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
                org.apache.rocketmq.remoting.protocol.ResponseCode.NO_PERMISSION),
            Arrays.asList((String) null, "first put rejected", "last put rejected"),
            java.util.Collections.nCopies(3, null),
            java.util.Collections.nCopies(3, null)));
        cases.add(kvConfigCase("delete-broadcast", "delete", "tenant", "key", null,
            List.of("ns-a:9876", "ns-b:9876"),
            List.of(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
                org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS),
            Arrays.asList((String) null, (String) null),
            java.util.Collections.nCopies(2, null), java.util.Collections.nCopies(2, null)));
        cases.add(kvConfigCase("delete-no-configured-servers", "delete", "tenant", "key", null,
            List.of(), List.of(), List.of(), List.of(), List.of()));
        cases.add(kvConfigCase("list-populated", "list", "tenant", null, null,
            List.of(), List.of(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS),
            java.util.Collections.singletonList(null), java.util.Collections.singletonList(null),
            List.of("{\"table\":{\"alpha\":\"one\",\"beta\":\"two\"}}")));
        cases.add(kvConfigCase("list-missing-body", "list", "tenant", null, null,
            List.of(), List.of(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS),
            java.util.Collections.singletonList(null), java.util.Collections.singletonList(null), java.util.Collections.singletonList(null)));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> kvConfigCase(
        String name,
        String operation,
        String namespace,
        String key,
        String value,
        List<String> nameServers,
        List<Integer> responseCodes,
        List<String> responseRemarks,
        List<String> responseValues,
        List<String> responseBodies) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig clientConfig =
            new org.apache.rocketmq.client.ClientConfig();
        clientConfig.setClientIP("kv-config-api");
        clientConfig.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            clientConfig, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("getNameServerAddressList".equals(method.getName())) {
                            return nameServers.isEmpty() ? null : nameServers;
                        }
                        if ("invokeSync".equals(method.getName())) {
                            int index = requests.size();
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields() == null ? null
                                : new TreeMap<>(request.getExtFields()));
                            call.put("requestBodyBase64", request.getBody() == null ? null
                                : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                            requests.add(call);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand reply =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                                    responseCodes.get(index), responseRemarks.get(index));
                            if (responseValues.get(index) != null) {
                                reply.addExtField("value", responseValues.get(index));
                            }
                            if (responseBodies.get(index) != null) {
                                reply.setBody(responseBodies.get(index).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                            }
                            call.put("responseCode", responseCodes.get(index));
                            call.put("responseRemark", responseRemarks.get(index));
                            return reply;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("operation", operation);
        result.put("namespace", namespace);
        result.put("key", key);
        result.put("value", value);
        result.put("nameServers", nameServers);
        try {
            switch (operation) {
                case "get":
                    result.put("outcome", "returned");
                    result.put("result", api.getKVConfigValue(namespace, key, 2345L));
                    break;
                case "put":
                    api.putKVConfigValue(namespace, key, value, 2345L);
                    result.put("outcome", "returned");
                    break;
                case "delete":
                    api.deleteKVConfigValue(namespace, key, 2345L);
                    result.put("outcome", "returned");
                    break;
                case "list":
                    result.put("outcome", "returned");
                    result.put("result", api.getKVListByNamespace(namespace, 2345L));
                    break;
                default:
                    throw new IllegalArgumentException("unknown KV operation: " + operation);
            }
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> brokerMaintenanceOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        for (String method : List.of("cleanExpiredConsumeQueue", "deleteExpiredCommitLog",
            "cleanUnusedTopicByAddr")) {
            cases.add(brokerMaintenanceCase(method + "-success", method,
                org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
            cases.add(brokerMaintenanceCase(method + "-error", method,
                org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
                method + " rejected"));
        }
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> fetchConsumeStatsInBrokerOperations() throws Exception {
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(fetchConsumeStatsInBrokerCase("stats-populated", true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
            consumeStatsListSample()));
        cases.add(fetchConsumeStatsInBrokerCase("stats-defaults", false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "{}"));
        cases.add(fetchConsumeStatsInBrokerCase("stats-null-list", false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
            "{\"brokerAddr\":null,\"consumeStatsList\":null,\"totalDiff\":0,\"totalInflightDiff\":0}"));
        cases.add(fetchConsumeStatsInBrokerCase("stats-null-primitives", false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
            "{\"totalDiff\":null,\"totalInflightDiff\":null}"));
        cases.add(fetchConsumeStatsInBrokerCase("stats-success-no-body", false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, null));
        cases.add(fetchConsumeStatsInBrokerCase("stats-broker-error", true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
            "broker consume stats unavailable", null));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        return result;
    }

    private static String consumeStatsListSample() {
        org.apache.rocketmq.remoting.protocol.body.ConsumeStatsList statsList =
            new org.apache.rocketmq.remoting.protocol.body.ConsumeStatsList();
        org.apache.rocketmq.remoting.protocol.admin.ConsumeStats stats =
            new org.apache.rocketmq.remoting.protocol.admin.ConsumeStats();
        stats.setConsumeTps(3.5);
        org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper offsets =
            new org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper();
        offsets.setBrokerOffset(100L);
        offsets.setConsumerOffset(85L);
        offsets.setPullOffset(95L);
        stats.setOffsetTable(Map.of(
            new MessageQueue("orders", "broker-a", 0), offsets));
        Map<String, List<org.apache.rocketmq.remoting.protocol.admin.ConsumeStats>> group =
            new TreeMap<>();
        group.put("workers", List.of(stats));
        statsList.setConsumeStatsList(List.of(group));
        statsList.setBrokerAddr("broker-a:10911");
        statsList.setTotalDiff(15L);
        statsList.setTotalInflightDiff(10L);
        return statsList.toJson();
    }

    private static Map<String, Object> fetchConsumeStatsInBrokerCase(
        String name, boolean isOrder, int responseCode, String responseRemark,
        String responseBody) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("fetch-consume-stats-in-broker-api");
        config.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand
                                    .createResponseCommand(responseCode, responseRemark);
                            if (responseBody != null) {
                                response.setBody(responseBody.getBytes(
                                    java.nio.charset.StandardCharsets.UTF_8));
                            }
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("timeoutMillis", 2345L);
        result.put("isOrder", isOrder);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseBody", responseBody);
        try {
            org.apache.rocketmq.remoting.protocol.body.ConsumeStatsList stats =
                api.fetchConsumeStatsInBroker("broker-a:10911", isOrder, 2345L);
            result.put("outcome", "returned");
            result.put("resultJson", stats.toJson());
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> viewBrokerStatsDataOperations() throws Exception {
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(viewBrokerStatsDataCase("stats-populated", "SNDBYTES", "orders@0",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
            brokerStatsDataSample()));
        cases.add(viewBrokerStatsDataCase("stats-defaults", "SNDBYTES", "orders@0",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "{}"));
        cases.add(viewBrokerStatsDataCase("stats-null-primitives", "SNDBYTES", "orders@0",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
            "{\"statsMinute\":{\"sum\":null,\"tps\":null,\"avgpt\":null}}"));
        cases.add(viewBrokerStatsDataCase("stats-null-result", null, null,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "null"));
        cases.add(viewBrokerStatsDataCase("stats-success-no-body", "SNDBYTES", "orders@0",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, null));
        cases.add(viewBrokerStatsDataCase("stats-broker-error", "SNDBYTES", "orders@0",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
            "broker statistics unavailable", null));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        return result;
    }

    private static String brokerStatsDataSample() {
        org.apache.rocketmq.remoting.protocol.body.BrokerStatsData stats =
            new org.apache.rocketmq.remoting.protocol.body.BrokerStatsData();
        org.apache.rocketmq.remoting.protocol.body.BrokerStatsItem minute =
            new org.apache.rocketmq.remoting.protocol.body.BrokerStatsItem();
        minute.setSum(1200L);
        minute.setTps(20.0);
        minute.setAvgpt(1.25);
        stats.setStatsMinute(minute);
        org.apache.rocketmq.remoting.protocol.body.BrokerStatsItem hour =
            new org.apache.rocketmq.remoting.protocol.body.BrokerStatsItem();
        hour.setSum(43200L);
        hour.setTps(12.0);
        hour.setAvgpt(1.5);
        stats.setStatsHour(hour);
        org.apache.rocketmq.remoting.protocol.body.BrokerStatsItem day =
            new org.apache.rocketmq.remoting.protocol.body.BrokerStatsItem();
        day.setSum(1000000L);
        day.setTps(11.574);
        day.setAvgpt(1.75);
        stats.setStatsDay(day);
        return stats.toJson();
    }

    private static Map<String, Object> viewBrokerStatsDataCase(
        String name, String statsName, String statsKey, int responseCode,
        String responseRemark, String responseBody) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("view-broker-stats-data-api");
        config.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand
                                    .createResponseCommand(responseCode, responseRemark);
                            if (responseBody != null) {
                                response.setBody(responseBody.getBytes(
                                    java.nio.charset.StandardCharsets.UTF_8));
                            }
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("timeoutMillis", 2345L);
        result.put("statsName", statsName);
        result.put("statsKey", statsKey);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseBody", responseBody);
        try {
            org.apache.rocketmq.remoting.protocol.body.BrokerStatsData stats =
                api.viewBrokerStatsData("broker-a:10911", statsName, statsKey, 2345L);
            result.put("outcome", "returned");
            result.put("resultIsNull", stats == null);
            if (stats != null) result.put("resultJson", stats.toJson());
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> getTopicConfigOperations() throws Exception {
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(getTopicConfigCase("config-populated", "orders",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
            topicConfigAndMappingSample()));
        cases.add(getTopicConfigCase("config-defaults", "orders",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "{}"));
        cases.add(getTopicConfigCase("config-null-primitives", "orders",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
            "{\"readQueueNums\":null,\"writeQueueNums\":null,\"perm\":null,"
                + "\"topicSysFlag\":null,\"order\":null,\"attributes\":null}"));
        cases.add(getTopicConfigCase("config-mapping-defaults", "orders",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
            "{\"mappingDetail\":{}}"));
        cases.add(getTopicConfigCase("config-null-result", "orders",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "null"));
        cases.add(getTopicConfigCase("config-success-no-body", "orders",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, null));
        cases.add(getTopicConfigCase("config-null-topic", null,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "{}"));
        cases.add(getTopicConfigCase("config-empty-topic", "",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "{}"));
        cases.add(getTopicConfigCase("config-topic-not-exist", "missing-topic",
            org.apache.rocketmq.remoting.protocol.ResponseCode.TOPIC_NOT_EXIST,
            "topic does not exist", null));
        cases.add(getTopicConfigCase("config-broker-error", "orders",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
            "topic config unavailable", null));
        cases.add(getTopicConfigCase("config-invalid-json", "orders",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "{"));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> topicConfigModelOperations() {
        Map<String, Object> result = new TreeMap<>();
        result.put("topicConfigAndQueueMappingApi", topicConfigAndQueueMappingApiCases());
        TopicConfig defaults = new TopicConfig();
        result.put("default", topicConfigModelState(defaults));
        result.put("defaultEncoded", defaults.encode());
        result.put("constructors", Arrays.asList(
            topicConfigModelState(new TopicConfig("orders")),
            topicConfigModelState(new TopicConfig("orders", 2, 3)),
            topicConfigModelState(new TopicConfig("orders", 2, 3, 7)),
            topicConfigModelState(new TopicConfig("orders", 2, 3, 7, 11))));
        int defaultReadQueueNums = TopicConfig.defaultReadQueueNums;
        int defaultWriteQueueNums = TopicConfig.defaultWriteQueueNums;
        try {
            TopicConfig.defaultReadQueueNums = 27;
            TopicConfig.defaultWriteQueueNums = 29;
            result.put("mutableStaticDefaults", topicConfigModelState(new TopicConfig("dynamic")));
        } finally {
            TopicConfig.defaultReadQueueNums = defaultReadQueueNums;
            TopicConfig.defaultWriteQueueNums = defaultWriteQueueNums;
        }

        TopicConfig setterState = new TopicConfig();
        setterState.setTopicName("setter-topic");
        setterState.setReadQueueNums(31);
        setterState.setWriteQueueNums(32);
        setterState.setPerm(7);
        setterState.setTopicFilterType(org.apache.rocketmq.common.TopicFilterType.MULTI_TAG);
        setterState.setTopicSysFlag(33);
        setterState.setOrder(true);
        setterState.setAttributes(new HashMap<>(Map.of("message.type", "FIFO")));
        result.put("setterState", topicConfigModelState(setterState));

        Map<String, String> nullableAttributeMap = new HashMap<>();
        nullableAttributeMap.put(null, "null-key");
        nullableAttributeMap.put("null", "string-null-key");
        nullableAttributeMap.put("nullable-value", null);
        TopicConfig nullableAttributeConfig = new TopicConfig("nullable-attributes");
        nullableAttributeConfig.setAttributes(nullableAttributeMap);
        Map<String, Object> nullableAttributeApi = new TreeMap<>();
        Map<String, String> nullableAttributeGetter = nullableAttributeConfig.getAttributes();
        nullableAttributeApi.put("setterKeepsMapIdentity", nullableAttributeGetter == nullableAttributeMap);
        nullableAttributeApi.put("getterKeepsMapIdentity",
            nullableAttributeConfig.getAttributes() == nullableAttributeMap);
        nullableAttributeGetter.put("from-getter", "getter-mutation");
        nullableAttributeMap.put("from-caller", "caller-mutation");
        nullableAttributeApi.put("mutationsVisibleThroughGetter",
            nullableAttributeConfig.getAttributes().containsKey("from-caller"));
        nullableAttributeApi.put("mutationsVisibleThroughCaller",
            nullableAttributeMap.containsKey("from-getter"));
        List<Map.Entry<String, String>> nullableAttributeEntries =
            new ArrayList<>(nullableAttributeMap.entrySet());
        nullableAttributeEntries.sort(Map.Entry.comparingByKey(
            Comparator.nullsFirst(String::compareTo)));
        List<Map<String, Object>> nullableAttributeEntryStates = new ArrayList<>();
        for (Map.Entry<String, String> entry : nullableAttributeEntries) {
            Map<String, Object> entryState = new TreeMap<>();
            entryState.put("keyIsNull", entry.getKey() == null);
            entryState.put("key", entry.getKey() == null ? "" : entry.getKey());
            entryState.put("valueIsNull", entry.getValue() == null);
            entryState.put("value", entry.getValue() == null ? "" : entry.getValue());
            nullableAttributeEntryStates.add(entryState);
        }
        nullableAttributeApi.put("entries", nullableAttributeEntryStates);
        nullableAttributeApi.put("attributesJson", JSON.toJSONString(nullableAttributeMap));
        nullableAttributeApi.put("encoded", nullableAttributeConfig.encode());
        nullableAttributeApi.put("toString", nullableAttributeConfig.toString());
        String nullableKeyDecodeInput =
            "decoded-null-key 1 2 6 SINGLE_TAG {\"null\":\"decoded-string-key\"}";
        TopicConfig nullableKeyDecoded = new TopicConfig("before");
        nullableAttributeApi.put("decodeInput", nullableKeyDecodeInput);
        nullableAttributeApi.put("decodeResult", nullableKeyDecoded.decode(nullableKeyDecodeInput));
        nullableAttributeApi.put("decodedStringNullKeyPresent",
            nullableKeyDecoded.getAttributes().containsKey("null"));
        nullableAttributeApi.put("decodedJavaNullKeyPresent",
            nullableKeyDecoded.getAttributes().containsKey(null));
        nullableAttributeApi.put("decodedEncoded", nullableKeyDecoded.encode());
        result.put("nullableAttributeApi", nullableAttributeApi);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("lite.topic.expiration", "17");
        attributes.put("message.type", "NORMAL");
        TopicConfig lite = new TopicConfig("lite-orders", 4, 5, 6, 8);
        lite.setAttributes(attributes);
        lite.setTopicMessageType(TopicMessageType.LITE);
        lite.setLiteTopicExpiration(23);
        result.put("lite", topicConfigModelState(lite));
        result.put("liteEncoded", lite.encode());

        Map<String, String> hashCollisionAttributes = new HashMap<>();
        hashCollisionAttributes.put("BB", "second");
        hashCollisionAttributes.put("Aa", "first");
        TopicConfig hashCollision = new TopicConfig("collision");
        hashCollision.setAttributes(hashCollisionAttributes);
        result.put("hashCollisionEncoded", hashCollision.encode());
        result.put("hashCollisionToString", hashCollision.toString());
        result.put("hashCollisionDecodeInput",
            "collision 16 16 6 SINGLE_TAG {\"BB\":\"second\",\"Aa\":\"first\"}");
        Map<String, String> reverseHashCollisionAttributes = new HashMap<>();
        reverseHashCollisionAttributes.put("Aa", "first");
        reverseHashCollisionAttributes.put("BB", "second");
        TopicConfig reverseHashCollision = new TopicConfig("collision");
        reverseHashCollision.setAttributes(reverseHashCollisionAttributes);
        result.put("hashCollisionReverseEncoded", reverseHashCollision.encode());

        TopicConfig copied = new TopicConfig(lite);
        result.put("copyEquals", lite.equals(copied));
        result.put("copyHashEqual", lite.hashCode() == copied.hashCode());
        copied.getAttributes().put("lite.topic.expiration", "29");
        result.put("copyRetainsAttributeAlias", lite.getLiteTopicExpiration() == 29);

        TopicConfig decoded = new TopicConfig("before");
        decoded.setReadQueueNums(40);
        String decodeInput = "wire-topic 7 8 6 MULTI_TAG {\"lite.topic.expiration\":\"31\",\"message.type\":\"LITE\"}";
        result.put("decodeInput", decodeInput);
        try {
            new TopicConfig("retained").decode(null);
            result.put("nullDecodeError", "none");
        } catch (RuntimeException error) {
            result.put("nullDecodeError", error.getClass().getSimpleName());
        }
        result.put("decodeResult", decoded.decode(decodeInput));
        result.put("decoded", topicConfigModelState(decoded));
        result.put("decodedEncoded", decoded.encode());

        TopicConfig trailing = new TopicConfig("retained");
        String trailingInput = "trailing 1 2 6 SINGLE_TAG ";
        result.put("trailingInput", trailingInput);
        result.put("trailingResult", trailing.decode(trailingInput));
        result.put("trailingState", topicConfigModelState(trailing));

        TopicConfig unicodeNumbers = new TopicConfig("retained");
        String unicodeNumberInput = "unicode ١ ٢ ٦ SINGLE_TAG";
        result.put("unicodeNumberInput", unicodeNumberInput);
        result.put("unicodeNumberResult", unicodeNumbers.decode(unicodeNumberInput));
        result.put("unicodeNumberState", topicConfigModelState(unicodeNumbers));

        TopicConfig invalidFilter = new TopicConfig("retained");
        String invalidFilterInput = "filter 1 2 6 bad-filter";
        result.put("invalidFilterInput", invalidFilterInput);
        try {
            invalidFilter.decode(invalidFilterInput);
            result.put("invalidFilterError", "none");
        } catch (RuntimeException error) {
            result.put("invalidFilterError", error.getClass().getSimpleName());
        }
        result.put("invalidFilterState", topicConfigModelState(invalidFilter));

        TopicConfig tooShort = new TopicConfig("retained");
        result.put("tooShortInput", "ignored 1 2");
        result.put("tooShortResult", tooShort.decode((String) result.get("tooShortInput")));
        result.put("tooShortState", topicConfigModelState(tooShort));

        TopicConfig malformedAttributes = new TopicConfig("retained");
        malformedAttributes.setAttributes(new HashMap<>(Map.of("message.type", "LITE")));
        result.put("malformedAttributesInput", "changed 1 2 6 SINGLE_TAG {bad");
        result.put("malformedAttributesResult", malformedAttributes.decode(
            (String) result.get("malformedAttributesInput")));
        result.put("malformedAttributesState", topicConfigModelState(malformedAttributes));

        TopicConfig partial = new TopicConfig("retained");
        partial.setReadQueueNums(90);
        result.put("partialDecodeInput", "partial 12 invalid 6 SINGLE_TAG");
        try {
            partial.decode((String) result.get("partialDecodeInput"));
            result.put("partialDecodeError", "none");
        } catch (RuntimeException error) {
            result.put("partialDecodeError", error.getClass().getSimpleName());
        }
        result.put("partialDecodeState", topicConfigModelState(partial));

        TopicConfig unknownType = new TopicConfig("orders");
        unknownType.setAttributes(new HashMap<>(Map.of("message.type", "unknown")));
        try {
            unknownType.getTopicMessageType();
            result.put("unknownMessageTypeError", "none");
        } catch (RuntimeException error) {
            result.put("unknownMessageTypeError", error.getClass().getSimpleName());
        }
        TopicConfig nullAttributes = new TopicConfig("orders");
        nullAttributes.setAttributes(null);
        result.put("nullAttributesMessageType", nullAttributes.getTopicMessageType().getValue());
        result.put("nullAttributesLiteExpiration", nullAttributes.getLiteTopicExpiration());

        TopicConfig invalidExpiration = new TopicConfig("orders");
        Map<String, String> invalidExpirationAttributes = new HashMap<>();
        invalidExpirationAttributes.put("lite.topic.expiration", "not-an-int");
        invalidExpirationAttributes.put("message.type", "LITE");
        invalidExpiration.setAttributes(invalidExpirationAttributes);
        result.put("invalidExpiration", topicConfigModelState(invalidExpiration));

        Map<String, Object> precedence = new TreeMap<>();
        Map<String, String> transaction = new HashMap<>();
        transaction.put(MessageConst.PROPERTY_TRANSACTION_PREPARED, "TrUe");
        transaction.put(MessageConst.PROPERTY_DELAY_TIME_LEVEL, "1");
        precedence.put("transaction", TopicMessageType.parseFromMessageProperty(transaction).getValue());
        Map<String, String> fifo = new HashMap<>();
        fifo.put(MessageConst.PROPERTY_SHARDING_KEY, "key");
        fifo.put(MessageConst.PROPERTY_PRIORITY, "1");
        precedence.put("fifo", TopicMessageType.parseFromMessageProperty(fifo).getValue());
        Map<String, String> liteProperty = new HashMap<>();
        liteProperty.put(MessageConst.PROPERTY_LITE_TOPIC, "orders");
        precedence.put("lite", TopicMessageType.parseFromMessageProperty(liteProperty).getValue());
        precedence.put("normal", TopicMessageType.parseFromMessageProperty(new HashMap<>()).getValue());
        Map<String, String> nullProperty = new HashMap<>();
        nullProperty.put(MessageConst.PROPERTY_DELAY_TIME_LEVEL, null);
        precedence.put("nullDelay", TopicMessageType.parseFromMessageProperty(nullProperty).getValue());
        result.put("messageTypePrecedence", precedence);
        result.put("messageTypeValues", Arrays.stream(TopicMessageType.values())
            .map(TopicMessageType::getValue).collect(java.util.stream.Collectors.toList()));
        result.put("messageTypeMetrics", TopicMessageType.LITE.getMetricsValue());
        Locale originalLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            result.put("messageTypeMetricsTurkish", Arrays.stream(TopicMessageType.values())
                .map(TopicMessageType::getMetricsValue)
                .collect(java.util.stream.Collectors.toList()));
        } finally {
            Locale.setDefault(originalLocale);
        }
        return result;
    }

    private static org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail
    topicConfigMappingDetail(String topic, String broker, long logicOffset) {
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail detail =
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail(
                topic, 1, broker, 1);
        ConcurrentMap<Integer, List<org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem>>
            hostedQueues = new java.util.concurrent.ConcurrentHashMap<>();
        hostedQueues.put(0, new ArrayList<>(Arrays.asList(
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                1, 2, broker, logicOffset, 0, 3, 10, 20))));
        detail.setHostedQueues(hostedQueues);
        return detail;
    }

    private static Map<String, Object> topicConfigAndQueueMappingState(
            org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping value) {
        Map<String, Object> result = new TreeMap<>();
        result.put("topicConfig", topicConfigModelState(value));
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail mappingDetail =
            value.getMappingDetail();
        result.put("mappingDetail", mappingDetail == null ? null : JSON.parseObject(
            JSON.toJSONString(mappingDetail,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        result.put("toString", value.toString());
        return result;
    }

    private static Map<String, Object> topicConfigAndQueueMappingPair(
            String name,
            org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping left,
            org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping right) {
        Map<String, Object> result = new TreeMap<>();
        result.put("case", name);
        result.put("left", JSON.parseObject(JSON.toJSONString(left,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        result.put("right", JSON.parseObject(JSON.toJSONString(right,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        result.put("equals", left.equals(right));
        result.put("hashCodeEqual", left.hashCode() == right.hashCode());
        result.put("leftToString", left.toString());
        result.put("rightToString", right.toString());
        return result;
    }

    private static Map<String, Object> topicConfigAndQueueMappingApiCases() {
        Map<String, Object> result = new TreeMap<>();
        org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping defaults =
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping();
        result.put("default", topicConfigAndQueueMappingState(defaults));

        TopicConfig source = new TopicConfig("source-topic", 3, 4, 6, 7);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("owner", "initial");
        source.setAttributes(attributes);
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail detail =
            topicConfigMappingDetail("mapping-topic", "broker", 0);
        org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping wrapper =
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping(
                source, detail);
        result.put("sourceBefore", topicConfigModelState(source));
        result.put("detailBefore", JSON.parseObject(JSON.toJSONString(detail,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        result.put("constructed", topicConfigAndQueueMappingState(wrapper));
        result.put("getterPreservesMappingIdentity", wrapper.getMappingDetail() == detail);

        TopicConfig nullFields = new TopicConfig();
        nullFields.setTopicName(null);
        nullFields.setTopicFilterType(null);
        nullFields.setAttributes(null);
        result.put("nullFields", topicConfigAndQueueMappingState(
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping(
                nullFields, null)));

        org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping inheritedSetters =
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping(
                new TopicConfig("inherited-before", 1, 2, 3, 4),
                topicConfigMappingDetail("inherited-mapping", "broker", 0));
        inheritedSetters.setTopicName("inherited-after");
        inheritedSetters.setReadQueueNums(41);
        inheritedSetters.setWriteQueueNums(42);
        inheritedSetters.setPerm(5);
        inheritedSetters.setTopicFilterType(org.apache.rocketmq.common.TopicFilterType.MULTI_TAG);
        inheritedSetters.setTopicSysFlag(43);
        inheritedSetters.setOrder(true);
        result.put("inheritedSetters", topicConfigAndQueueMappingState(inheritedSetters));

        source.setTopicName("source-mutated");
        source.setReadQueueNums(99);
        attributes.put("owner", "updated");
        result.put("afterSourceMutation", topicConfigAndQueueMappingState(wrapper));
        detail.setTopic("mapping-mutated");
        result.put("afterMappingMutation", topicConfigAndQueueMappingState(wrapper));

        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail replacement =
            topicConfigMappingDetail("replacement", "broker-2", 4);
        wrapper.setMappingDetail(replacement);
        result.put("setterPreservesMappingIdentity", wrapper.getMappingDetail() == replacement);
        wrapper.setMappingDetail(null);
        result.put("setterAcceptsNull", wrapper.getMappingDetail() == null);
        try {
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping(
                null, null);
            result.put("nullConfigConstructorError", "none");
        } catch (RuntimeException error) {
            result.put("nullConfigConstructorError", error.getClass().getSimpleName());
        }

        TopicConfig equalConfig = new TopicConfig("orders", 2, 3, 6, 8);
        equalConfig.setAttributes(new HashMap<>(Map.of("owner", "compat")));
        TopicConfig differentConfig = new TopicConfig(equalConfig);
        differentConfig.setTopicName("other-topic");
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail leftDetail =
            topicConfigMappingDetail("mapping-topic", "broker", 0);
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail equalDetail =
            topicConfigMappingDetail("mapping-topic", "broker", 0);
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail metadataOnlyDetail =
            topicConfigMappingDetail("other-mapping-topic", "broker", 0);
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail differentDetail =
            topicConfigMappingDetail("mapping-topic", "broker", 1);
        org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping equalLeft =
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping(
                equalConfig, leftDetail);
        List<Object> equality = new ArrayList<>();
        equality.add(topicConfigAndQueueMappingPair("default",
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping(),
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping()));
        equality.add(topicConfigAndQueueMappingPair("same-populated", equalLeft,
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping(
                new TopicConfig(equalConfig), equalDetail)));
        equality.add(topicConfigAndQueueMappingPair("config-field", equalLeft,
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping(
                differentConfig, equalDetail)));
        equality.add(topicConfigAndQueueMappingPair("mapping-metadata-only", equalLeft,
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping(
                new TopicConfig(equalConfig), metadataOnlyDetail)));
        equality.add(topicConfigAndQueueMappingPair("mapping-content", equalLeft,
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping(
                new TopicConfig(equalConfig), differentDetail)));
        equality.add(topicConfigAndQueueMappingPair("null-vs-empty-mapping", equalLeft,
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping(
                new TopicConfig(equalConfig), null)));
        result.put("equality", equality);
        return result;
    }

    private static Map<String, Object> topicConfigModelState(TopicConfig config) {
        Map<String, Object> result = new TreeMap<>();
        result.put("topicName", config.getTopicName());
        result.put("readQueueNums", config.getReadQueueNums());
        result.put("writeQueueNums", config.getWriteQueueNums());
        result.put("perm", config.getPerm());
        result.put("topicFilterType", config.getTopicFilterType() == null
            ? null : config.getTopicFilterType().name());
        result.put("topicSysFlag", config.getTopicSysFlag());
        result.put("order", config.isOrder());
        result.put("attributes", config.getAttributes() == null
            ? null : new TreeMap<>(config.getAttributes()));
        result.put("topicMessageType", config.getTopicMessageType().getValue());
        result.put("liteTopicExpiration", config.getLiteTopicExpiration());
        result.put("toString", config.toString());
        return result;
    }

    private static Map<String, Object> createStaticTopicOperations() throws Exception {
        List<Map<String, Object>> cases = new ArrayList<>();
        org.apache.rocketmq.common.TopicConfig populated = new org.apache.rocketmq.common.TopicConfig("orders");
        populated.setReadQueueNums(8);
        populated.setWriteQueueNums(6);
        populated.setPerm(7);
        populated.setTopicSysFlag(3);
        populated.setOrder(true);
        populated.setTopicFilterType(org.apache.rocketmq.common.TopicFilterType.MULTI_TAG);
        populated.setAttributes(new TreeMap<>(Map.of("owner", "compat", "tier", "gold")));
        org.apache.rocketmq.common.TopicConfig defaults = new org.apache.rocketmq.common.TopicConfig("defaults");
        org.apache.rocketmq.common.TopicConfig invalidPermission = new org.apache.rocketmq.common.TopicConfig("invalid-perm");
        invalidPermission.setPerm(-7);

        cases.add(createStaticTopicCase("populated", populated, "TBW102",
            staticTopicMappingSample(), true, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createStaticTopicCase("defaults", defaults, "TBW102",
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail(), false, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createStaticTopicCase("null-default-topic", new org.apache.rocketmq.common.TopicConfig("null-default"),
            null, new org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail(), false, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createStaticTopicCase("empty-default-topic", new org.apache.rocketmq.common.TopicConfig("empty-default"),
            "", new org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail(), true, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createStaticTopicCase("invalid-permission-is-sent", invalidPermission, "TBW102",
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail(), false, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createStaticTopicCase("broker-error", populated, "TBW102",
            staticTopicMappingSample(), true, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "static topic rejected"));
        cases.add(createStaticTopicCase("vip-address", populated, "TBW102",
            staticTopicMappingSample(), true, true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> createUserOperations() throws Exception {
        org.apache.rocketmq.remoting.protocol.body.UserInfo populated =
            org.apache.rocketmq.remoting.protocol.body.UserInfo.of(
                "alice", "fixture-password", "ADMIN", "ACTIVE");
        org.apache.rocketmq.remoting.protocol.body.UserInfo nullableFields =
            new org.apache.rocketmq.remoting.protocol.body.UserInfo();
        nullableFields.setUsername("nullable-fields");
        org.apache.rocketmq.remoting.protocol.body.UserInfo nullUsername =
            new org.apache.rocketmq.remoting.protocol.body.UserInfo();
        nullUsername.setPassword("pw-only");
        nullUsername.setUserType("USER");
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(createUserCase("populated", populated, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createUserCase("defaults", new org.apache.rocketmq.remoting.protocol.body.UserInfo(), false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createUserCase("nullable-fields", nullableFields, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createUserCase("null-username", nullUsername, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createUserCase("broker-error", populated, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "user rejected"));
        cases.add(createUserCase("vip-enabled-address-unchanged", populated, true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> createUserCase(
        String name,
        org.apache.rocketmq.remoting.protocol.body.UserInfo userInfo,
        boolean vipEnabled,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("create-user-api");
        config.setVipChannelEnabled(vipEnabled);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            RemotingCommand request = (RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields() == null ? null
                                : new TreeMap<>(request.getExtFields()));
                            call.put("requestBodyBase64", request.getBody() == null ? null
                                : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                            requests.add(call);
                            return RemotingCommand.createResponseCommand(responseCode, responseRemark);
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("userInfoJson", JSON.toJSONString(userInfo));
        result.put("vipEnabled", vipEnabled);
        result.put("timeoutMillis", 2345L);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            api.createUser("broker-a:10911", userInfo, 2345L);
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> updateUserOperations() throws Exception {
        org.apache.rocketmq.remoting.protocol.body.UserInfo populated =
            org.apache.rocketmq.remoting.protocol.body.UserInfo.of(
                "alice", "fixture-password", "ADMIN", "ACTIVE");
        org.apache.rocketmq.remoting.protocol.body.UserInfo nullUsername =
            new org.apache.rocketmq.remoting.protocol.body.UserInfo();
        nullUsername.setPassword("pw-only");
        nullUsername.setUserType("USER");
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(updateUserCase("populated", populated, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(updateUserCase("defaults", new org.apache.rocketmq.remoting.protocol.body.UserInfo(), false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(updateUserCase("null-username", nullUsername, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(updateUserCase("broker-error", populated, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "user update rejected"));
        cases.add(updateUserCase("vip-enabled-address-unchanged", populated, true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> updateUserCase(
        String name,
        org.apache.rocketmq.remoting.protocol.body.UserInfo userInfo,
        boolean vipEnabled,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("update-user-api");
        config.setVipChannelEnabled(vipEnabled);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            RemotingCommand request = (RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields() == null ? null
                                : new TreeMap<>(request.getExtFields()));
                            call.put("requestBodyBase64", request.getBody() == null ? null
                                : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                            requests.add(call);
                            return RemotingCommand.createResponseCommand(responseCode, responseRemark);
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("userInfoJson", JSON.toJSONString(userInfo));
        result.put("vipEnabled", vipEnabled);
        result.put("timeoutMillis", 2345L);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            api.updateUser("broker-a:10911", userInfo, 2345L);
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> deleteUserOperations() throws Exception {
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(deleteUserCase("populated", "alice", false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteUserCase("null-username", null, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteUserCase("empty-username", "", false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteUserCase("broker-error", "alice", false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "user deletion rejected"));
        cases.add(deleteUserCase("vip-enabled-address-unchanged", "alice", true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> deleteUserCase(
        String name,
        String username,
        boolean vipEnabled,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("delete-user-api");
        config.setVipChannelEnabled(vipEnabled);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            RemotingCommand request = (RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields() == null ? null
                                : new TreeMap<>(request.getExtFields()));
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            return RemotingCommand.createResponseCommand(responseCode, responseRemark);
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("username", username);
        result.put("vipEnabled", vipEnabled);
        result.put("timeoutMillis", 2345L);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            api.deleteUser("broker-a:10911", username, 2345L);
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> listUserOperations() throws Exception {
        org.apache.rocketmq.remoting.protocol.body.UserInfo alice =
            org.apache.rocketmq.remoting.protocol.body.UserInfo.of(
                "alice", "fixture-password", "ADMIN", "ACTIVE");
        org.apache.rocketmq.remoting.protocol.body.UserInfo bob =
            org.apache.rocketmq.remoting.protocol.body.UserInfo.of(
                "bob", null, "USER", null);
        List<org.apache.rocketmq.remoting.protocol.body.UserInfo> populated = new ArrayList<>();
        populated.add(alice);
        populated.add(null);
        populated.add(bob);
        byte[] populatedBody = JSON.toJSONString(populated)
            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] emptyBody = "[]".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] oneDefaultBody = "[{}]".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] nullBody = "null".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] objectBody = "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(listUserCase("populated", "ali", populatedBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(listUserCase("empty-filter", "", emptyBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(listUserCase("null-filter", null, oneDefaultBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(listUserCase("null-body", "alice", null, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(listUserCase("json-null", "alice", nullBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(listUserCase("object-root", "alice", objectBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(listUserCase("broker-error", "ali", null, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "user listing rejected"));
        cases.add(listUserCase("vip-enabled-address-unchanged", "ali", populatedBody, true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> listUserCase(
        String name,
        String filter,
        byte[] responseBody,
        boolean vipEnabled,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("list-user-api");
        config.setVipChannelEnabled(vipEnabled);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            RemotingCommand request = (RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields() == null ? null
                                : new TreeMap<>(request.getExtFields()));
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            RemotingCommand reply = RemotingCommand.createResponseCommand(
                                responseCode, responseRemark);
                            reply.setBody(responseBody);
                            return reply;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("filter", filter);
        result.put("vipEnabled", vipEnabled);
        result.put("timeoutMillis", 2345L);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseBodyBase64", responseBody == null ? null
            : java.util.Base64.getEncoder().encodeToString(responseBody));
        try {
            List<org.apache.rocketmq.remoting.protocol.body.UserInfo> users =
                api.listUser("broker-a:10911", filter, 2345L);
            result.put("outcome", "returned");
            result.put("resultIsNull", users == null);
            result.put("resultJson", JSON.toJSONString(users));
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } catch (RuntimeException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> getUserOperations() throws Exception {
        org.apache.rocketmq.remoting.protocol.body.UserInfo alice =
            org.apache.rocketmq.remoting.protocol.body.UserInfo.of(
                "alice", "fixture-password", "ADMIN", "ACTIVE");
        byte[] populatedBody = JSON.toJSONString(alice)
            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] defaultBody = "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] nullBody = "null".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(getUserCase("populated", "alice", populatedBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getUserCase("defaults", "alice", defaultBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getUserCase("null-username", null, populatedBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getUserCase("empty-username", "", defaultBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getUserCase("null-body", "alice", null, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getUserCase("json-null", "alice", nullBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getUserCase("eof-trailing-text", "alice",
            (JSON.toJSONString(alice) + "\u001atrailing")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8), false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getUserCase("eof-trailing-json", "alice",
            (JSON.toJSONString(alice) + "\u001a{\"ignored\":true}")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8), false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getUserCase("broker-error", "alice", null, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "user lookup rejected"));
        cases.add(getUserCase("vip-enabled-address-unchanged", "alice", populatedBody, true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> getUserCase(
        String name,
        String username,
        byte[] responseBody,
        boolean vipEnabled,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("get-user-api");
        config.setVipChannelEnabled(vipEnabled);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            RemotingCommand request = (RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields() == null ? null
                                : new TreeMap<>(request.getExtFields()));
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            RemotingCommand reply = RemotingCommand.createResponseCommand(
                                responseCode, responseRemark);
                            reply.setBody(responseBody);
                            return reply;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("username", username);
        result.put("vipEnabled", vipEnabled);
        result.put("timeoutMillis", 2345L);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseBodyBase64", responseBody == null ? null
            : java.util.Base64.getEncoder().encodeToString(responseBody));
        try {
            org.apache.rocketmq.remoting.protocol.body.UserInfo user =
                api.getUser("broker-a:10911", username, 2345L);
            result.put("outcome", "returned");
            result.put("resultIsNull", user == null);
            result.put("resultJson", JSON.toJSONString(user));
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } catch (RuntimeException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> getAclOperations() throws Exception {
        org.apache.rocketmq.remoting.protocol.body.AclInfo populated =
            org.apache.rocketmq.remoting.protocol.body.AclInfo.of(
                "subject", Arrays.asList("topic-a", "topic-b"),
                Arrays.asList("PUB", "SUB"), Arrays.asList("127.0.0.1", "::1"), "ALLOW");
        org.apache.rocketmq.remoting.protocol.body.AclInfo nullPolicies =
            new org.apache.rocketmq.remoting.protocol.body.AclInfo();
        nullPolicies.setSubject("null-policies");
        org.apache.rocketmq.remoting.protocol.body.AclInfo emptyPolicies =
            new org.apache.rocketmq.remoting.protocol.body.AclInfo();
        emptyPolicies.setSubject("empty-policies");
        emptyPolicies.setPolicies(new ArrayList<>());
        org.apache.rocketmq.remoting.protocol.body.AclInfo nullPolicyMember =
            new org.apache.rocketmq.remoting.protocol.body.AclInfo();
        nullPolicyMember.setSubject("null-policy-member");
        nullPolicyMember.setPolicies(java.util.Collections.singletonList(null));
        byte[] populatedBody = JSON.toJSONString(populated)
            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] nullPoliciesBody = JSON.toJSONString(nullPolicies)
            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] emptyPoliciesBody = JSON.toJSONString(emptyPolicies)
            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] nullPolicyMemberBody = JSON.toJSONString(nullPolicyMember)
            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] nullBody = "null".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(getAclCase("populated", "subject", populatedBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getAclCase("defaults", "subject", "{}".getBytes(
            java.nio.charset.StandardCharsets.UTF_8), false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getAclCase("null-subject", null, populatedBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getAclCase("empty-subject", "", emptyPoliciesBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getAclCase("null-policies", "null-policies", nullPoliciesBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getAclCase("empty-policies", "empty-policies", emptyPoliciesBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getAclCase("null-policy-member", "null-policy-member", nullPolicyMemberBody,
            false, org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getAclCase("null-body", "subject", null, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getAclCase("json-null", "subject", nullBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(getAclCase("broker-error", "subject", null, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "ACL lookup rejected"));
        cases.add(getAclCase("vip-enabled-address-unchanged", "subject", populatedBody, true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> getAclCase(
        String name,
        String subject,
        byte[] responseBody,
        boolean vipEnabled,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("get-acl-api");
        config.setVipChannelEnabled(vipEnabled);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            RemotingCommand request = (RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields() == null ? null
                                : new TreeMap<>(request.getExtFields()));
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            RemotingCommand reply = RemotingCommand.createResponseCommand(
                                responseCode, responseRemark);
                            reply.setBody(responseBody);
                            return reply;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("subject", subject);
        result.put("vipEnabled", vipEnabled);
        result.put("timeoutMillis", 2345L);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseBodyBase64", responseBody == null ? null
            : java.util.Base64.getEncoder().encodeToString(responseBody));
        try {
            org.apache.rocketmq.remoting.protocol.body.AclInfo aclInfo =
                api.getAcl("broker-a:10911", subject, 2345L);
            result.put("outcome", "returned");
            result.put("resultIsNull", aclInfo == null);
            result.put("resultJson", JSON.toJSONString(aclInfo));
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } catch (RuntimeException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> createAclOperations() throws Exception {
        return aclOperations(false);
    }

    private static Map<String, Object> updateAclOperations() throws Exception {
        return aclOperations(true);
    }

    private static Map<String, Object> aclOperations(boolean update) throws Exception {
        org.apache.rocketmq.remoting.protocol.body.AclInfo populated =
            org.apache.rocketmq.remoting.protocol.body.AclInfo.of(
                "subject", Arrays.asList("topic-a", "topic-b"),
                Arrays.asList("PUB", "SUB"), Arrays.asList("127.0.0.1"), "ALLOW");
        org.apache.rocketmq.remoting.protocol.body.AclInfo nullSubject =
            org.apache.rocketmq.remoting.protocol.body.AclInfo.of(
                null, Arrays.asList("topic-a"), Arrays.asList("PUB"), null, "ALLOW");
        org.apache.rocketmq.remoting.protocol.body.AclInfo emptySubject =
            org.apache.rocketmq.remoting.protocol.body.AclInfo.of(
                "", Arrays.asList("topic-a"), Arrays.asList("PUB"), Arrays.asList(), "ALLOW");
        org.apache.rocketmq.remoting.protocol.body.AclInfo nullableMembers =
            org.apache.rocketmq.remoting.protocol.body.AclInfo.of(
                "nullable-members", Arrays.asList("topic-c", null),
                Arrays.asList("PUB", null), null, "ALLOW");
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(aclOperationCase(update, "populated", populated, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(aclOperationCase(update, "defaults", new org.apache.rocketmq.remoting.protocol.body.AclInfo(), false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(aclOperationCase(update, "null-subject", nullSubject, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(aclOperationCase(update, "empty-subject", emptySubject, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(aclOperationCase(update, "nullable-members", nullableMembers, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(aclOperationCase(update, "broker-error", populated, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
            update ? "ACL update rejected" : "ACL creation rejected"));
        cases.add(aclOperationCase(update, "vip-enabled-address-unchanged", populated, true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> aclOperationCase(
        boolean update,
        String name,
        org.apache.rocketmq.remoting.protocol.body.AclInfo aclInfo,
        boolean vipEnabled,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP(update ? "update-acl-api" : "create-acl-api");
        config.setVipChannelEnabled(vipEnabled);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            RemotingCommand request = (RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields() == null ? null
                                : new TreeMap<>(request.getExtFields()));
                            call.put("requestBodyBase64", request.getBody() == null ? null
                                : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                            requests.add(call);
                            return RemotingCommand.createResponseCommand(responseCode, responseRemark);
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("aclInfoJson", JSON.toJSONString(aclInfo));
        result.put("vipEnabled", vipEnabled);
        result.put("timeoutMillis", 2345L);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            if (update) {
                api.updateAcl("broker-a:10911", aclInfo, 2345L);
            } else {
                api.createAcl("broker-a:10911", aclInfo, 2345L);
            }
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> deleteAclOperations() throws Exception {
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(deleteAclCase("populated", "subject", "topic-a", false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteAclCase("defaults", null, null, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteAclCase("null-subject", null, "topic-a", false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteAclCase("null-resource", "subject", null, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteAclCase("empty-fields", "", "", false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteAclCase("broker-error", "subject", "topic-a", false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "ACL deletion rejected"));
        cases.add(deleteAclCase("vip-enabled-address-unchanged", "subject", "topic-a", true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> deleteAclCase(
        String name,
        String subject,
        String resource,
        boolean vipEnabled,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("delete-acl-api");
        config.setVipChannelEnabled(vipEnabled);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            RemotingCommand request = (RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields() == null ? null
                                : new TreeMap<>(request.getExtFields()));
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            return RemotingCommand.createResponseCommand(responseCode, responseRemark);
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("subject", subject);
        result.put("resource", resource);
        result.put("vipEnabled", vipEnabled);
        result.put("timeoutMillis", 2345L);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            api.deleteAcl("broker-a:10911", subject, resource, 2345L);
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> listAclOperations() throws Exception {
        org.apache.rocketmq.remoting.protocol.body.AclInfo alice =
            org.apache.rocketmq.remoting.protocol.body.AclInfo.of(
                "alice", Arrays.asList("topic-a", "topic-b"), Arrays.asList("PUB", "SUB"),
                Arrays.asList("127.0.0.1"), "ALLOW");
        org.apache.rocketmq.remoting.protocol.body.AclInfo bob =
            new org.apache.rocketmq.remoting.protocol.body.AclInfo();
        bob.setSubject("bob");
        List<org.apache.rocketmq.remoting.protocol.body.AclInfo> populated = new ArrayList<>();
        populated.add(alice);
        populated.add(null);
        populated.add(bob);
        byte[] populatedBody = JSON.toJSONString(populated)
            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] emptyBody = "[]".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] oneDefaultBody = "[{}]".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] nullBody = "null".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] objectBody = "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(listAclCase("populated", "ali", "topic", populatedBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(listAclCase("empty-filters", "", "", emptyBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(listAclCase("null-filters", null, null, oneDefaultBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(listAclCase("null-body", "alice", "topic-a", null, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(listAclCase("json-null", "alice", "topic-a", nullBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(listAclCase("object-root", "alice", "topic-a", objectBody, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(listAclCase("broker-error", "ali", "topic", null, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
            "ACL listing rejected"));
        cases.add(listAclCase("vip-enabled-address-unchanged", "ali", "topic", populatedBody,
            true, org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> listAclCase(
        String name,
        String subjectFilter,
        String resourceFilter,
        byte[] responseBody,
        boolean vipEnabled,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("list-acl-api");
        config.setVipChannelEnabled(vipEnabled);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            RemotingCommand request = (RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields() == null ? null
                                : new TreeMap<>(request.getExtFields()));
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            RemotingCommand reply = RemotingCommand.createResponseCommand(
                                responseCode, responseRemark);
                            reply.setBody(responseBody);
                            return reply;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("subjectFilter", subjectFilter);
        result.put("resourceFilter", resourceFilter);
        result.put("vipEnabled", vipEnabled);
        result.put("timeoutMillis", 2345L);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseBodyBase64", responseBody == null ? null
            : java.util.Base64.getEncoder().encodeToString(responseBody));
        try {
            List<org.apache.rocketmq.remoting.protocol.body.AclInfo> acls =
                api.listAcl("broker-a:10911", subjectFilter, resourceFilter, 2345L);
            result.put("outcome", "returned");
            result.put("resultIsNull", acls == null);
            result.put("resultJson", JSON.toJSONString(acls));
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } catch (RuntimeException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail
        staticTopicMappingSample() {
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail detail =
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail(
                "orders", 4, "broker-a", 12L);
        detail.setScope("__global__");
        detail.setDirty(true);
        detail.setCurrIdMap(new java.util.concurrent.ConcurrentHashMap<>(Map.of(2, 1)));
        org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem item =
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                1, 1, "broker-a", 100L, 20L, 80L, 1000L, 2000L);
        detail.getHostedQueues().put(2, List.of(item));
        return detail;
    }

    private static Map<String, Object> createStaticTopicCase(
        String name,
        org.apache.rocketmq.common.TopicConfig topicConfig,
        String defaultTopic,
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail mappingDetail,
        boolean force,
        boolean vipEnabled,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("create-static-topic-api");
        config.setVipChannelEnabled(vipEnabled);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            RemotingCommand request = (RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields() == null ? null
                                : new TreeMap<>(request.getExtFields()));
                            call.put("requestBodyPresent", request.getBody() != null);
                            call.put("requestBodyBase64", request.getBody() == null ? null
                                : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                            requests.add(call);
                            return RemotingCommand.createResponseCommand(responseCode, responseRemark);
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("defaultTopic", defaultTopic);
        result.put("topicConfigJson", JSON.toJSONString(topicConfig));
        result.put("mappingDetailJson", JSON.toJSONString(mappingDetail));
        result.put("force", force);
        result.put("vipEnabled", vipEnabled);
        result.put("timeoutMillis", 2345L);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            api.createStaticTopic("broker-a:10911", defaultTopic, topicConfig, mappingDetail,
                force, 2345L);
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> queryConsumeQueueOperations() throws Exception {
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(queryConsumeQueueCase("queue-populated", "orders", 2, 9_876_543_210L,
            16, "workers", false, org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
            queryConsumeQueueBodyJson()));
        cases.add(queryConsumeQueueCase("queue-defaults", "orders", 0, 0, 0, "workers", false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "{}"));
        cases.add(queryConsumeQueueCase("queue-null-result", "orders", 1, 4, 3, "workers", false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "null"));
        cases.add(queryConsumeQueueCase("queue-null-fields-and-row", "orders", 1, 4, 3, "workers",
            false, org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
            "{\"subscriptionData\":null,\"filterData\":null,\"queueData\":[null,{}],"
                + "\"maxQueueIndex\":null,\"minQueueIndex\":null}"));
        cases.add(queryConsumeQueueCase("queue-success-no-body", "orders", 1, 4, 3, "workers", false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, null));
        cases.add(queryConsumeQueueCase("queue-null-topic-group", null, 0, 0, 0, null, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "{}"));
        cases.add(queryConsumeQueueCase("queue-broker-error", "orders", 1, 4, 3, "workers", false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
            "consume queue unavailable", null));
        cases.add(queryConsumeQueueCase("queue-invalid-json", "orders", 1, 4, 3, "workers", false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "{"));
        cases.add(queryConsumeQueueCase("queue-vip-address", "orders", 2, 13, 4, "workers", true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "{}"));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        result.put("consumeQueueDataToString", queryConsumeQueueRowSample().toString());
        return result;
    }

    private static String queryConsumeQueueBodyJson() {
        org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData subscription =
            new org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData("orders", "*");
        subscription.setSubVersion(1234L);
        org.apache.rocketmq.remoting.protocol.body.ConsumeQueueData row = queryConsumeQueueRowSample();
        org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody body =
            new org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody();
        body.setSubscriptionData(subscription);
        body.setFilterData("filter-data");
        body.setQueueData(List.of(row));
        body.setMaxQueueIndex(20L);
        body.setMinQueueIndex(4L);
        return JSON.toJSONString(body);
    }

    private static org.apache.rocketmq.remoting.protocol.body.ConsumeQueueData
        queryConsumeQueueRowSample() {
        org.apache.rocketmq.remoting.protocol.body.ConsumeQueueData row =
            new org.apache.rocketmq.remoting.protocol.body.ConsumeQueueData();
        row.setPhysicOffset(987_654_321L);
        row.setPhysicSize(128);
        row.setTagsCode(171_234L);
        row.setExtendDataJson("{\"filterBitMap\":\"AQID\"}");
        row.setBitMap("1010");
        row.setEval(true);
        row.setMsg("message-body");
        return row;
    }

    private static Map<String, Object> queryConsumeQueueCase(
        String name, String topic, int queueId, long index, int count, String consumerGroup,
        boolean vipEnabled, int responseCode, String responseRemark, String responseBody) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("query-consume-queue-api");
        config.setVipChannelEnabled(vipEnabled);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            RemotingCommand request = (RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            RemotingCommand response = RemotingCommand
                                .createResponseCommand(responseCode, responseRemark);
                            if (responseBody != null) {
                                response.setBody(responseBody.getBytes(
                                    java.nio.charset.StandardCharsets.UTF_8));
                            }
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("topic", topic);
        result.put("queueId", queueId);
        result.put("index", index);
        result.put("count", count);
        result.put("consumerGroup", consumerGroup);
        result.put("vipEnabled", vipEnabled);
        result.put("timeoutMillis", 2345L);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseBody", responseBody);
        try {
            org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody received =
                api.queryConsumeQueue("broker-a:10911", topic, queueId, index, count,
                    consumerGroup, 2345L);
            result.put("outcome", "returned");
            result.put("resultIsNull", received == null);
            if (received != null) result.put("resultJson", JSON.toJSONString(received));
        } catch (MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } catch (RuntimeException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static String topicConfigAndMappingSample() {
        org.apache.rocketmq.common.TopicConfig topicConfig =
            new org.apache.rocketmq.common.TopicConfig("orders");
        topicConfig.setReadQueueNums(8);
        topicConfig.setWriteQueueNums(6);
        topicConfig.setPerm(7);
        topicConfig.setTopicSysFlag(3);
        topicConfig.setOrder(true);
        topicConfig.setAttributes(new java.util.TreeMap<>(
            Map.of("owner", "compat", "tier", "gold")));
        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail detail =
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail(
                "orders", 4, "broker-a", 12L);
        detail.setScope("__global__");
        detail.setDirty(true);
        detail.setCurrIdMap(new java.util.concurrent.ConcurrentHashMap<>(Map.of(2, 1)));
        org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem item =
            new org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem(
                1, 1, "broker-a", 100L, 20L, 80L, 1000L, 2000L);
        detail.getHostedQueues().put(2, List.of(item));
        org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping response =
            new org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping(
                topicConfig, detail);
        return JSON.toJSONString(response);
    }

    private static Map<String, Object> getTopicConfigCase(
        String name, String topic, int responseCode, String responseRemark, String responseBody)
        throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("get-topic-config-api");
        config.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand
                                    .createResponseCommand(responseCode, responseRemark);
                            if (responseBody != null) {
                                response.setBody(responseBody.getBytes(
                                    java.nio.charset.StandardCharsets.UTF_8));
                            }
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("topic", topic);
        result.put("timeoutMillis", 2345L);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseBody", responseBody);
        try {
            org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping received =
                api.getTopicConfig("broker-a:10911", topic, 2345L);
            result.put("outcome", "returned");
            result.put("resultIsNull", received == null);
            if (received != null) result.put("resultJson", JSON.toJSONString(received));
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } catch (RuntimeException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static org.apache.rocketmq.remoting.protocol.DataVersion metadataWrapperVersion() {
        org.apache.rocketmq.remoting.protocol.DataVersion version =
            new org.apache.rocketmq.remoting.protocol.DataVersion();
        version.setStateVersion(7);
        version.setTimestamp(1500);
        version.setCounter(new java.util.concurrent.atomic.AtomicLong(5));
        return version;
    }

    private static Map<String, Object> metadataWrapperIdentity() throws Exception {
        Map<String, Object> result = new TreeMap<>();

        org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper topicFirst =
            new org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper();
        org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper topicSecond =
            new org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper();
        java.util.concurrent.ConcurrentMap<String, TopicConfig> firstTopics =
            new java.util.concurrent.ConcurrentHashMap<>();
        java.util.concurrent.ConcurrentMap<String, TopicConfig> secondTopics =
            new java.util.concurrent.ConcurrentHashMap<>();
        firstTopics.put("orders", new TopicConfig("orders"));
        secondTopics.put("orders", new TopicConfig("orders"));
        topicFirst.setTopicConfigTable(firstTopics);
        topicSecond.setTopicConfigTable(secondTopics);
        topicFirst.setDataVersion(metadataWrapperVersion());
        topicSecond.setDataVersion(metadataWrapperVersion());
        result.put("topicConfigWrapperSameStateEncodedEqual",
            Arrays.equals(topicFirst.encode(), topicSecond.encode()));
        result.put("topicConfigWrapperDistinctSameStateEquals", topicFirst.equals(topicSecond));
        result.put("topicConfigWrapperSameReferenceEquals", topicFirst.equals(topicFirst));
        result.put("topicConfigWrapperEqualsOwner",
            topicFirst.getClass().getMethod("equals", Object.class).getDeclaringClass().getName());
        result.put("topicConfigWrapperHashCodeOwner",
            topicFirst.getClass().getMethod("hashCode").getDeclaringClass().getName());

        org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper groupFirst =
            new org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper();
        org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper groupSecond =
            new org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper();
        org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig firstGroup =
            new org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig();
        org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig secondGroup =
            new org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig();
        firstGroup.setGroupName("workers");
        secondGroup.setGroupName("workers");
        java.util.concurrent.ConcurrentMap<String,
            org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig> firstGroups =
                new java.util.concurrent.ConcurrentHashMap<>();
        java.util.concurrent.ConcurrentMap<String,
            org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig> secondGroups =
                new java.util.concurrent.ConcurrentHashMap<>();
        firstGroups.put("workers", firstGroup);
        secondGroups.put("workers", secondGroup);
        java.util.concurrent.ConcurrentMap<String, java.util.concurrent.ConcurrentMap<String, Integer>>
            firstForbidden = new java.util.concurrent.ConcurrentHashMap<>();
        java.util.concurrent.ConcurrentMap<String, java.util.concurrent.ConcurrentMap<String, Integer>>
            secondForbidden = new java.util.concurrent.ConcurrentHashMap<>();
        firstForbidden.put("workers", new java.util.concurrent.ConcurrentHashMap<>(Map.of("orders", 1)));
        secondForbidden.put("workers", new java.util.concurrent.ConcurrentHashMap<>(Map.of("orders", 1)));
        groupFirst.setSubscriptionGroupTable(firstGroups);
        groupSecond.setSubscriptionGroupTable(secondGroups);
        groupFirst.setForbiddenTable(firstForbidden);
        groupSecond.setForbiddenTable(secondForbidden);
        groupFirst.setDataVersion(metadataWrapperVersion());
        groupSecond.setDataVersion(metadataWrapperVersion());
        result.put("subscriptionGroupWrapperSameStateEncodedEqual",
            Arrays.equals(groupFirst.encode(), groupSecond.encode()));
        result.put("subscriptionGroupWrapperDistinctSameStateEquals", groupFirst.equals(groupSecond));
        result.put("subscriptionGroupWrapperSameReferenceEquals", groupFirst.equals(groupFirst));
        result.put("subscriptionGroupWrapperEqualsOwner",
            groupFirst.getClass().getMethod("equals", Object.class).getDeclaringClass().getName());
        result.put("subscriptionGroupWrapperHashCodeOwner",
            groupFirst.getClass().getMethod("hashCode").getDeclaringClass().getName());
        return result;
    }

    // These paginated APIs recompute remaining time from System.nanoTime(). The
    // exact millisecond value varies between runs, so capture whether each
    // attempted page still has a positive budget. The delayed timeout cases
    // verify that an exhausted budget prevents a later page request.
    private static boolean timeoutBudgetPositive(Object timeoutMillis) {
        return ((Number) timeoutMillis).longValue() > 0;
    }

    private static Map<String, Object> getAllTopicConfigOperations() throws Exception {
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(getAllTopicConfigCase("snapshot-single-page", 2345L, List.of(
            allTopicConfigResponse(0, null, topicConfigWrapperJson(7, 1000, 4,
                "orders", "payments"), 2))));
        cases.add(getAllTopicConfigCase("snapshot-multiple-pages", 2345L, List.of(
            allTopicConfigResponse(0, null, topicConfigWrapperJson(7, 1100, 5, "orders"), 3),
            allTopicConfigResponse(0, null, topicConfigWrapperJson(7, 1100, 5, "payments"), 3))));
        cases.add(getAllTopicConfigCase("snapshot-version-changed", 2345L, List.of(
            allTopicConfigResponse(0, null, topicConfigWrapperJson(7, 1200, 6, "stale"), 3),
            allTopicConfigResponse(0, null, topicConfigWrapperJson(8, 1300, 7, "discarded"), 2),
            allTopicConfigResponse(0, null, topicConfigWrapperJson(8, 1300, 7,
                "orders", "payments"), 2))));
        cases.add(getAllTopicConfigCase("snapshot-legacy-no-total", 2345L, List.of(
            allTopicConfigResponse(0, null, topicConfigWrapperJson(7, 1400, 8,
                "orders", "payments"), null))));
        cases.add(getAllTopicConfigCase("snapshot-broker-error", 2345L, List.of(
            allTopicConfigResponse(org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
                "topic snapshot unavailable", null, null))));
        cases.add(getAllTopicConfigCase("snapshot-topic-not-exist", 2345L, List.of(
            allTopicConfigResponse(org.apache.rocketmq.remoting.protocol.ResponseCode.TOPIC_NOT_EXIST,
                "topic metadata unavailable", null, null))));
        cases.add(getAllTopicConfigCase("snapshot-missing-body", 2345L, List.of(
            allTopicConfigResponse(0, null, null, 1))));
        cases.add(getAllTopicConfigCase("snapshot-null-table", 2345L, List.of(
            allTopicConfigResponse(0, null,
                "{\"dataVersion\":{\"counter\":1,\"stateVersion\":7,\"timestamp\":1500},\"topicConfigTable\":null}",
                1))));
        cases.add(getAllTopicConfigCase("snapshot-invalid-json", 2345L, List.of(
            allTopicConfigResponse(0, null, "{", 1))));
        cases.add(getAllTopicConfigCase("snapshot-invalid-total", 2345L, List.of(
            allTopicConfigResponseWithFields(0, null, topicConfigWrapperJson(7, 1500, 9, "orders"),
                Map.of("totalTopicNum", "invalid")))));
        cases.add(getAllTopicConfigCase("snapshot-zero-timeout", 0L, List.of()));
        cases.add(getAllTopicConfigCase("snapshot-timeout-after-page", 20L, List.of(
            allTopicConfigResponse(0, null, topicConfigWrapperJson(7, 1600, 10, "orders"), 3),
            allTopicConfigResponse(0, null, topicConfigWrapperJson(7, 1600, 10, "payments"), 3)), 30));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> getAllSubscriptionGroupOperations() throws Exception {
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(getAllSubscriptionGroupCase("snapshot-single-page", 2345L, List.of(
            allSubscriptionGroupResponse(0, null,
                subscriptionGroupWrapperJson(7, 1000, 4, "workers", "analytics"), 2))));
        cases.add(getAllSubscriptionGroupCase("snapshot-multiple-pages", 2345L, List.of(
            allSubscriptionGroupResponse(0, null,
                subscriptionGroupWrapperJson(7, 1100, 5, "workers"), 3),
            allSubscriptionGroupResponse(0, null,
                subscriptionGroupWrapperJson(7, 1100, 5, "analytics"), 3))));
        cases.add(getAllSubscriptionGroupCase("snapshot-version-changed", 2345L, List.of(
            allSubscriptionGroupResponse(0, null,
                subscriptionGroupWrapperJson(7, 1200, 6, "stale"), 3),
            allSubscriptionGroupResponse(0, null,
                subscriptionGroupWrapperJson(8, 1300, 7, "discarded"), 2),
            allSubscriptionGroupResponse(0, null,
                subscriptionGroupWrapperJson(8, 1300, 7, "workers", "analytics"), 2))));
        cases.add(getAllSubscriptionGroupCase("snapshot-legacy-no-total", 2345L, List.of(
            allSubscriptionGroupResponse(0, null,
                subscriptionGroupWrapperJson(7, 1400, 8, "workers", "analytics"), null))));
        cases.add(getAllSubscriptionGroupCase("snapshot-broker-error", 2345L, List.of(
            allSubscriptionGroupResponse(org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
                "subscription group snapshot unavailable", null, null))));
        cases.add(getAllSubscriptionGroupCase("snapshot-missing-body", 2345L, List.of(
            allSubscriptionGroupResponse(0, null, null, 1))));
        cases.add(getAllSubscriptionGroupCase("snapshot-null-group-table", 2345L, List.of(
            allSubscriptionGroupResponse(0, null,
                "{\"dataVersion\":{\"counter\":1,\"stateVersion\":7,\"timestamp\":1500},\"subscriptionGroupTable\":null,\"forbiddenTable\":{}}",
                1))));
        cases.add(getAllSubscriptionGroupCase("snapshot-null-forbidden-table", 2345L, List.of(
            allSubscriptionGroupResponse(0, null,
                "{\"dataVersion\":{\"counter\":1,\"stateVersion\":7,\"timestamp\":1500},\"subscriptionGroupTable\":{},\"forbiddenTable\":null}",
                1))));
        cases.add(getAllSubscriptionGroupCase("snapshot-invalid-json", 2345L, List.of(
            allSubscriptionGroupResponse(0, null, "{", 1))));
        cases.add(getAllSubscriptionGroupCase("snapshot-invalid-total", 2345L, List.of(
            allSubscriptionGroupResponseWithFields(0, null,
                subscriptionGroupWrapperJson(7, 1500, 9, "workers"),
                Map.of("totalGroupNum", "invalid")))));
        cases.add(getAllSubscriptionGroupCase("snapshot-timeout-after-page", 20L, List.of(
            allSubscriptionGroupResponse(0, null,
                subscriptionGroupWrapperJson(7, 1600, 10, "workers"), 3),
            allSubscriptionGroupResponse(0, null,
                subscriptionGroupWrapperJson(7, 1600, 10, "analytics"), 3)), 30));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        return result;
    }

    private static String subscriptionGroupWrapperJson(
        long stateVersion, long timestamp, long counter, String... groups) throws Exception {
        org.apache.rocketmq.remoting.protocol.DataVersion dataVersion =
            new org.apache.rocketmq.remoting.protocol.DataVersion();
        dataVersion.setStateVersion(stateVersion);
        dataVersion.setTimestamp(timestamp);
        dataVersion.setCounter(new java.util.concurrent.atomic.AtomicLong(counter));
        java.util.concurrent.ConcurrentMap<String,
            org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig> groupTable =
                new java.util.concurrent.ConcurrentHashMap<>();
        java.util.concurrent.ConcurrentMap<String, java.util.concurrent.ConcurrentMap<String, Integer>> forbiddenTable =
            new java.util.concurrent.ConcurrentHashMap<>();
        for (String group : groups) {
            org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig config =
                new org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig();
            config.setGroupName(group);
            if ("workers".equals(group)) {
                config.setRetryQueueNums(5);
                config.setConsumeBroadcastEnable(false);
            }
            if ("analytics".equals(group)) config.setConsumeMessageOrderly(true);
            groupTable.put(group, config);
            String topic = "workers".equals(group) ? "orders"
                : "analytics".equals(group) ? "alerts" : group + "-topic";
            forbiddenTable.put(group, new java.util.concurrent.ConcurrentHashMap<>(
                Map.of(topic, 1)));
        }
        org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper wrapper =
            new org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper();
        wrapper.setDataVersion(dataVersion);
        wrapper.setSubscriptionGroupTable(groupTable);
        wrapper.setForbiddenTable(forbiddenTable);
        return new String(wrapper.encode(), java.nio.charset.StandardCharsets.UTF_8);
    }

    private static org.apache.rocketmq.remoting.protocol.RemotingCommand allSubscriptionGroupResponse(
        int code, String remark, String body, Integer totalGroupNum) {
        if (totalGroupNum == null) {
            return allSubscriptionGroupResponseWithFields(code, remark, body, null);
        }
        org.apache.rocketmq.remoting.protocol.header.GetAllSubscriptionGroupResponseHeader header =
            new org.apache.rocketmq.remoting.protocol.header.GetAllSubscriptionGroupResponseHeader();
        header.setTotalGroupNum(totalGroupNum);
        org.apache.rocketmq.remoting.protocol.RemotingCommand response =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommandWithHeader(code, header);
        response.setRemark(remark);
        if (body != null) response.setBody(body.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        response.makeCustomHeaderToNet();
        return response;
    }

    private static org.apache.rocketmq.remoting.protocol.RemotingCommand allSubscriptionGroupResponseWithFields(
        int code, String remark, String body, Map<String, String> fields) {
        org.apache.rocketmq.remoting.protocol.RemotingCommand response =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(code, remark);
        if (fields != null) response.setExtFields(new java.util.HashMap<>(fields));
        if (body != null) response.setBody(body.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return response;
    }

    private static Map<String, Object> getAllSubscriptionGroupCase(
        String name, long timeoutMillis,
        List<org.apache.rocketmq.remoting.protocol.RemotingCommand> replyList) {
        return getAllSubscriptionGroupCase(name, timeoutMillis, replyList, 0);
    }

    private static Map<String, Object> getAllSubscriptionGroupCase(
        String name, long timeoutMillis,
        List<org.apache.rocketmq.remoting.protocol.RemotingCommand> replyList,
        long firstResponseDelayMillis) {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        List<Map<String, Object>> responses = new ArrayList<>();
        java.util.ArrayDeque<org.apache.rocketmq.remoting.protocol.RemotingCommand> replies =
            new java.util.ArrayDeque<>(replyList);
        for (org.apache.rocketmq.remoting.protocol.RemotingCommand reply : replyList) {
            Map<String, Object> row = new TreeMap<>();
            row.put("code", reply.getCode());
            row.put("remark", reply.getRemark());
            row.put("extFields", reply.getExtFields());
            row.put("body", reply.getBody() == null ? null
                : new String(reply.getBody(), java.nio.charset.StandardCharsets.UTF_8));
            row.put("delayMillis", responses.isEmpty() ? firstResponseDelayMillis : 0);
            responses.add(row);
        }
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("get-all-subscription-group-api");
        config.setVipChannelEnabled(false);
        config.setMaxPageSizeInGetMetadata(2000);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutBudgetPositive", timeoutBudgetPositive(values[2]));
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response = replies.removeFirst();
                            long delayMillis = ((Number) responses.get(requests.size() - 1).get("delayMillis")).longValue();
                            if (delayMillis > 0) Thread.sleep(delayMillis);
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("timeoutMillis", timeoutMillis);
        result.put("responses", responses);
        try {
            org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper value =
                api.getAllSubscriptionGroup("broker-a:10911", timeoutMillis);
            result.put("outcome", "returned");
            result.put("resultJson", JSON.toJSONString(value));
            Map<String, Integer> retryQueueNums = new TreeMap<>();
            for (Map.Entry<String, org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig> entry
                    : value.getSubscriptionGroupTable().entrySet()) {
                retryQueueNums.put(entry.getKey(), entry.getValue().getRetryQueueNums());
            }
            result.put("resultRetryQueueNums", retryQueueNums);
            result.put("resultGroups", new TreeSet<>(value.getSubscriptionGroupTable().keySet()));
            Map<String, Map<String, Integer>> forbidden = new TreeMap<>();
            for (Map.Entry<String, java.util.concurrent.ConcurrentMap<String, Integer>> entry
                    : value.getForbiddenTable().entrySet()) {
                forbidden.put(entry.getKey(), new TreeMap<>(entry.getValue()));
            }
            result.put("resultForbiddenTable", forbidden);
            org.apache.rocketmq.remoting.protocol.DataVersion version = value.getDataVersion();
            if (version == null) {
                result.put("resultDataVersion", null);
            } else {
                Map<String, Object> versionFields = new TreeMap<>();
                versionFields.put("stateVersion", version.getStateVersion());
                versionFields.put("timestamp", version.getTimestamp());
                versionFields.put("counter",
                    version.getCounter() == null ? null : version.getCounter().get());
                result.put("resultDataVersion", versionFields);
            }
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } catch (Throwable error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static String topicConfigWrapperJson(
        long stateVersion, long timestamp, long counter, String... topics) throws Exception {
        org.apache.rocketmq.remoting.protocol.DataVersion dataVersion =
            new org.apache.rocketmq.remoting.protocol.DataVersion();
        dataVersion.setStateVersion(stateVersion);
        dataVersion.setTimestamp(timestamp);
        dataVersion.setCounter(new java.util.concurrent.atomic.AtomicLong(counter));
        java.util.concurrent.ConcurrentMap<String, org.apache.rocketmq.common.TopicConfig> table =
            new java.util.concurrent.ConcurrentHashMap<>();
        for (String topic : topics) table.put(topic, new org.apache.rocketmq.common.TopicConfig(topic));
        org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper wrapper =
            new org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper();
        wrapper.setDataVersion(dataVersion);
        wrapper.setTopicConfigTable(table);
        return new String(wrapper.encode(), java.nio.charset.StandardCharsets.UTF_8);
    }

    private static org.apache.rocketmq.remoting.protocol.RemotingCommand allTopicConfigResponse(
        int code, String remark, String body, Integer totalTopicNum) {
        if (totalTopicNum == null) {
            return allTopicConfigResponseWithFields(code, remark, body, null);
        }
        org.apache.rocketmq.remoting.protocol.header.GetAllTopicConfigResponseHeader header =
            new org.apache.rocketmq.remoting.protocol.header.GetAllTopicConfigResponseHeader();
        header.setTotalTopicNum(totalTopicNum);
        org.apache.rocketmq.remoting.protocol.RemotingCommand response =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommandWithHeader(code, header);
        response.setRemark(remark);
        if (body != null) response.setBody(body.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        response.makeCustomHeaderToNet();
        return response;
    }

    private static org.apache.rocketmq.remoting.protocol.RemotingCommand allTopicConfigResponseWithFields(
        int code, String remark, String body, Map<String, String> fields) {
        org.apache.rocketmq.remoting.protocol.RemotingCommand response =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(code, remark);
        if (fields != null) response.setExtFields(new java.util.HashMap<>(fields));
        if (body != null) response.setBody(body.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return response;
    }

    private static Map<String, Object> getAllTopicConfigCase(
        String name, long timeoutMillis,
        List<org.apache.rocketmq.remoting.protocol.RemotingCommand> replyList) {
        return getAllTopicConfigCase(name, timeoutMillis, replyList, 0);
    }

    private static Map<String, Object> getAllTopicConfigCase(
        String name, long timeoutMillis,
        List<org.apache.rocketmq.remoting.protocol.RemotingCommand> replyList,
        long firstResponseDelayMillis) {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        List<Map<String, Object>> responses = new ArrayList<>();
        java.util.ArrayDeque<org.apache.rocketmq.remoting.protocol.RemotingCommand> replies =
            new java.util.ArrayDeque<>(replyList);
        for (org.apache.rocketmq.remoting.protocol.RemotingCommand reply : replyList) {
            Map<String, Object> row = new TreeMap<>();
            row.put("code", reply.getCode());
            row.put("remark", reply.getRemark());
            row.put("extFields", reply.getExtFields());
            row.put("body", reply.getBody() == null ? null
                : new String(reply.getBody(), java.nio.charset.StandardCharsets.UTF_8));
            row.put("delayMillis", responses.isEmpty() ? firstResponseDelayMillis : 0);
            responses.add(row);
        }
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("get-all-topic-config-api");
        config.setVipChannelEnabled(false);
        config.setMaxPageSizeInGetMetadata(2000);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutBudgetPositive", timeoutBudgetPositive(values[2]));
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response = replies.removeFirst();
                            long delayMillis = ((Number) responses.get(requests.size() - 1).get("delayMillis")).longValue();
                            if (delayMillis > 0) Thread.sleep(delayMillis);
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("timeoutMillis", timeoutMillis);
        result.put("responses", responses);
        try {
            org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper value =
                api.getAllTopicConfig("broker-a:10911", timeoutMillis);
            result.put("outcome", "returned");
            result.put("resultJson", JSON.toJSONString(value));
            Map<String, Integer> readQueueNums = new TreeMap<>();
            for (Map.Entry<String, org.apache.rocketmq.common.TopicConfig> entry
                    : value.getTopicConfigTable().entrySet()) {
                readQueueNums.put(entry.getKey(), entry.getValue().getReadQueueNums());
            }
            result.put("resultReadQueueNums", readQueueNums);
            result.put("resultTopics", new TreeSet<>(value.getTopicConfigTable().keySet()));
            org.apache.rocketmq.remoting.protocol.DataVersion version = value.getDataVersion();
            if (version == null) {
                result.put("resultDataVersion", null);
            } else {
                Map<String, Object> versionFields = new TreeMap<>();
                versionFields.put("stateVersion", version.getStateVersion());
                versionFields.put("timestamp", version.getTimestamp());
                versionFields.put("counter",
                    version.getCounter() == null ? null : version.getCounter().get());
                result.put("resultDataVersion", versionFields);
            }
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } catch (Throwable error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> brokerMaintenanceCase(
        String name,
        String operation,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig clientConfig =
            new org.apache.rocketmq.client.ClientConfig();
        clientConfig.setClientIP("broker-maintenance-api");
        clientConfig.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            clientConfig, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyBase64", request.getBody() == null ? null
                                : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                            call.put("responseCode", responseCode);
                            call.put("responseRemark", responseRemark);
                            requests.add(call);
                            return org.apache.rocketmq.remoting.protocol.RemotingCommand
                                .createResponseCommand(responseCode, responseRemark);
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("operation", operation);
        result.put("address", "broker-a:10911");
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            boolean value;
            switch (operation) {
                case "cleanExpiredConsumeQueue":
                    value = api.cleanExpiredConsumeQueue("broker-a:10911", 2345L);
                    break;
                case "deleteExpiredCommitLog":
                    value = api.deleteExpiredCommitLog("broker-a:10911", 2345L);
                    break;
                case "cleanUnusedTopicByAddr":
                    value = api.cleanUnusedTopicByAddr("broker-a:10911", 2345L);
                    break;
                default:
                    throw new IllegalArgumentException("unknown Broker maintenance operation: " + operation);
            }
            result.put("outcome", "returned");
            result.put("result", value);
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> subscriptionGroupConfigOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig populated =
            new org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig();
        populated.setGroupName("workers");
        populated.setRetryQueueNums(3);
        populated.setConsumeMessageOrderly(true);
        cases.add(subscriptionGroupConfigCase("populated", "workers",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, populated));
        cases.add(subscriptionGroupConfigCase("null-group", null,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
            new org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig()));
        cases.add(subscriptionGroupConfigCase("empty-group", "",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, null));
        cases.add(subscriptionGroupConfigCase("error", "missing-group",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
            "subscription group lookup rejected", null));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> subscriptionGroupConfigCase(
        String name,
        String group,
        int responseCode,
        String responseRemark,
        org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig responseConfig)
        throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig clientConfig =
            new org.apache.rocketmq.client.ClientConfig();
        clientConfig.setClientIP("get-subscription-group-config-api");
        clientConfig.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            clientConfig, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyBase64", request.getBody() == null ? null
                                : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                            requests.add(call);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand
                                    .createResponseCommand(responseCode, responseRemark);
                            if (responseConfig != null) response.setBody(
                                org.apache.rocketmq.remoting.protocol.RemotingSerializable
                                    .encode(responseConfig));
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("group", group);
        result.put("address", "broker-a:10911");
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseBodyBase64", responseConfig == null ? null
            : java.util.Base64.getEncoder().encodeToString(
                org.apache.rocketmq.remoting.protocol.RemotingSerializable.encode(responseConfig)));
        try {
            org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig received =
                api.getSubscriptionGroupConfig("broker-a:10911", group, 2345L);
            result.put("outcome", "returned");
            result.put("resultIsNull", received == null);
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> createSubscriptionGroupListCase(
        String name,
        List<org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig> configs,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("create-subscription-group-list-api");
        config.setVipChannelEnabled(false);
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                    java.lang.reflect.Proxy.newProxyInstance(
                        ProducerCoreFixtures.class.getClassLoader(),
                        new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                        (proxy, method, values) -> {
                            if ("invokeSync".equals(method.getName())) {
                                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                    (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                                Map<String, Object> call = new TreeMap<>();
                                call.put("address", values[0]);
                                call.put("requestCode", request.getCode());
                                call.put("timeoutMillis", values[2]);
                                call.put("extFieldsPresent", request.getExtFields() != null
                                    && !request.getExtFields().isEmpty());
                                call.put("requestBodyBase64", request.getBody() == null ? null
                                    : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                                requests.add(call);
                                org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                    org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                                        responseCode, responseRemark);
                                return response;
                            }
                            return method.getReturnType() == boolean.class ? false : null;
                        }));
        result.put("name", name);
        result.put("configs", configs);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            api.createSubscriptionGroupList("broker-a:10911", configs, 2345L);
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> createTopicListOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        org.apache.rocketmq.common.TopicConfig orders =
            new org.apache.rocketmq.common.TopicConfig("orders");
        org.apache.rocketmq.common.TopicConfig payments =
            new org.apache.rocketmq.common.TopicConfig("payments");
        payments.setReadQueueNums(8);
        payments.setWriteQueueNums(7);
        payments.setPerm(4);
        payments.setOrder(true);
        payments.setTopicSysFlag(3);
        payments.setTopicFilterType(org.apache.rocketmq.common.TopicFilterType.MULTI_TAG);
        payments.setAttributes(new TreeMap<>(Map.of("+unit", "true")));
        cases.add(createTopicListCase("populated", List.of(orders, payments),
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createTopicListCase("empty", List.of(),
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createTopicListCase("null-list", null,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createTopicListCase("null-element", Arrays.asList(orders, null),
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(createTopicListCase("broker-error", List.of(orders),
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "topics rejected"));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> createTopicListCase(
        String name,
        List<org.apache.rocketmq.common.TopicConfig> configs,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("create-topic-list-api");
        config.setVipChannelEnabled(false);
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                    java.lang.reflect.Proxy.newProxyInstance(
                        ProducerCoreFixtures.class.getClassLoader(),
                        new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                        (proxy, method, values) -> {
                            if ("invokeSync".equals(method.getName())) {
                                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                    (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                                Map<String, Object> call = new TreeMap<>();
                                call.put("address", values[0]);
                                call.put("requestCode", request.getCode());
                                call.put("timeoutMillis", values[2]);
                                call.put("extFieldsPresent", request.getExtFields() != null
                                    && !request.getExtFields().isEmpty());
                                call.put("requestBodyBase64", request.getBody() == null ? null
                                    : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                                requests.add(call);
                                return org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                                    responseCode, responseRemark);
                            }
                            return method.getReturnType() == boolean.class ? false : null;
                        }));
        result.put("name", name);
        result.put("configs", configs);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            api.createTopicList("broker-a:10911", configs, 2345L);
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> deleteTopicInBrokerListOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(deleteListCase("populated", List.of("orders", "payments"), false, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteListCase("empty", List.of(), false, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteListCase("null-list", null, false, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteListCase("null-element", Arrays.asList("orders", null), false, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteListCase("broker-error", List.of("orders"), false, false,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "topic deletion rejected"));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> deleteSubscriptionGroupListOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(deleteListCase("populated-clean", List.of("workers", "analytics"), true, true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteListCase("populated-keep-offset", List.of("workers"), false, true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteListCase("empty", List.of(), true, true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteListCase("null-list", null, false, true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteListCase("null-element", Arrays.asList("workers", null), true, true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(deleteListCase("broker-error", List.of("workers"), true, true,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "group deletion rejected"));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> deleteListCase(
        String name,
        List<String> values,
        boolean cleanOffset,
        boolean subscriptionGroups,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("delete-batch-list-api");
        config.setVipChannelEnabled(false);
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                    java.lang.reflect.Proxy.newProxyInstance(
                        ProducerCoreFixtures.class.getClassLoader(),
                        new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                        (proxy, method, args) -> {
                            if ("invokeSync".equals(method.getName())) {
                                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                    (org.apache.rocketmq.remoting.protocol.RemotingCommand) args[1];
                                Map<String, Object> call = new TreeMap<>();
                                call.put("address", args[0]);
                                call.put("requestCode", request.getCode());
                                call.put("timeoutMillis", args[2]);
                                call.put("extFieldsPresent", request.getExtFields() != null
                                    && !request.getExtFields().isEmpty());
                                call.put("requestBodyBase64", request.getBody() == null ? null
                                    : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                                requests.add(call);
                                return org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                                    responseCode, responseRemark);
                            }
                            return method.getReturnType() == boolean.class ? false : null;
                        }));
        result.put("name", name);
        result.put("values", values);
        result.put("cleanOffset", cleanOffset);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            if (subscriptionGroups) {
                api.deleteSubscriptionGroupList("broker-a:10911", values, cleanOffset, 2345L);
            } else {
                api.deleteTopicInBrokerList("broker-a:10911", values, 2345L);
            }
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> getTopicsByClusterOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        org.apache.rocketmq.remoting.protocol.body.TopicList topics =
            new org.apache.rocketmq.remoting.protocol.body.TopicList();
        topics.setTopicList(new java.util.HashSet<>(List.of("orders", "payments")));
        topics.setBrokerAddr("broker-a:10911");
        cases.add(getTopicsByClusterCase("success", "cluster-a",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, topics, true));
        cases.add(getTopicsByClusterCase("null-cluster", null,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, topics, true));
        cases.add(getTopicsByClusterCase("error", "cluster-b",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "cluster lookup rejected", null, false));
        cases.add(getTopicsByClusterCase("missing-body", "cluster-c",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, null, false));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> getTopicsByClusterCase(
        String name,
        String cluster,
        int responseCode,
        String responseRemark,
        org.apache.rocketmq.remoting.protocol.body.TopicList topicList,
        boolean bodyPresent) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("get-topics-by-cluster-api");
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                    java.lang.reflect.Proxy.newProxyInstance(
                        ProducerCoreFixtures.class.getClassLoader(),
                        new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                        (proxy, method, values) -> {
                            if ("invokeSync".equals(method.getName())) {
                                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                    (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                                Map<String, Object> call = new TreeMap<>();
                                call.put("addressWasNull", values[0] == null);
                                call.put("requestCode", request.getCode());
                                call.put("timeoutMillis", values[2]);
                                call.put("extFields", request.getExtFields());
                                org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader header =
                                    (org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader)
                                        request.readCustomHeader();
                                call.put("headerCluster", header == null ? null : header.getCluster());
                                call.put("requestBodyPresent", request.getBody() != null);
                                requests.add(call);
                                org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                    org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                                        responseCode, responseRemark);
                                if (bodyPresent) response.setBody(topicList.encode());
                                return response;
                            }
                            return method.getReturnType() == boolean.class ? false : null;
                        }));
        result.put("name", name);
        result.put("cluster", cluster);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseBodyBase64", bodyPresent
            ? java.util.Base64.getEncoder().encodeToString(topicList.encode()) : null);
        try {
            org.apache.rocketmq.remoting.protocol.body.TopicList received = api.getTopicsByCluster(cluster, 2345L);
            result.put("outcome", "returned");
            result.put("topics", new TreeSet<>(received.getTopicList()));
            result.put("brokerAddr", received.getBrokerAddr());
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> getClusterListOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(getClusterListCase("named-topic", "orders", 2345L));
        cases.add(getClusterListCase("null-topic", null, -1L));
        cases.add(getClusterListCase("empty-topic", "", 0L));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> addRemoveBrokerOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(addRemoveBrokerCase("add-success", "add", "/etc/rocketmq/broker.conf",
            null, null, null, 0, null));
        cases.add(addRemoveBrokerCase("add-null-path-error", "add", null,
            null, null, null, org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
            "broker registration rejected"));
        cases.add(addRemoveBrokerCase("remove-success", "remove", null,
            "cluster-a", "broker-a", Long.MIN_VALUE, 0, null));
        cases.add(addRemoveBrokerCase("remove-null-names-error", "remove", null,
            null, null, Long.MAX_VALUE, org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
            "broker removal rejected"));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> cloneGroupOffsetOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(cloneGroupOffsetCase("clone-success", "source", "destination", "orders",
            false, org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(cloneGroupOffsetCase("clone-null-inputs", null, null, null,
            true, org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(cloneGroupOffsetCase("clone-error", "source", "destination", "orders",
            true, org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
            "group offset clone rejected"));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> invokeBrokerToGetConsumerStatusOperations()
        throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(invokeBrokerToGetConsumerStatusCase("status-success", "orders", "workers",
            "consumer-a", org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
            null, "{\"consumerTable\":{\"consumer-a\":{}}}"));
        cases.add(invokeBrokerToGetConsumerStatusCase("status-null-inputs", null, null, null,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
            "{\"consumerTable\":{}}"));
        cases.add(invokeBrokerToGetConsumerStatusCase("status-error", "orders", "workers",
            "consumer-a", org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
            "consumer status rejected", null));
        cases.add(invokeBrokerToGetConsumerStatusCase("status-success-no-body", "orders",
            "workers", "consumer-a", org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
            null, null));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> queryCorrectionOffsetOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(queryCorrectionOffsetCase("correction-success", "orders", "workers",
            "ignored-group", 0, null, "{\"correctionOffsets\":{\"1\":7,\"3\":11,\"5\":null}}"));
        cases.add(queryCorrectionOffsetCase("correction-empty-filter", "orders", "workers",
            "", 0, null, "{}"));
        cases.add(queryCorrectionOffsetCase("correction-null-inputs", null, null,
            null, 0, null, "{\"correctionOffsets\":{}}"));
        cases.add(queryCorrectionOffsetCase("correction-error", "orders", "workers",
            null, org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
            "correction offset query rejected", null));
        cases.add(queryCorrectionOffsetCase("correction-success-no-body", "orders", "workers",
            null, org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, null));
        cases.add(queryCorrectionOffsetCase("correction-null-map", "orders", "workers",
            null, org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
            null, "{\"correctionOffsets\":null}"));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> queryCorrectionOffsetCase(
        String name, String topic, String group, String filterGroup, int responseCode,
        String responseRemark, String responseBody) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("query-correction-offset-api");
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand
                                    .createResponseCommand(responseCode, responseRemark);
                            if (responseBody != null) {
                                response.setBody(responseBody.getBytes(
                                    java.nio.charset.StandardCharsets.UTF_8));
                            }
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("timeoutMillis", 2345L);
        result.put("topic", topic);
        result.put("group", group);
        result.put("filterGroup", filterGroup);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseBody", responseBody);
        try {
            java.util.Set<String> filterGroups = filterGroup == null
                ? null : new java.util.LinkedHashSet<>(java.util.Collections.singletonList(filterGroup));
            Map<Integer, Long> offsets = api.queryCorrectionOffset(
                "broker-a:10911", topic, group, filterGroups, 2345L);
            result.put("outcome", "returned");
            result.put("nullOffsetPresent", offsets != null
                && offsets.containsKey(5) && offsets.get(5) == null);
            Map<String, Long> jsonOffsets = new TreeMap<>();
            if (offsets != null) {
                offsets.forEach((queueId, offset) -> jsonOffsets.put(
                    Integer.toString(queueId), offset));
            }
            result.put("correctionOffsets", offsets == null ? null : jsonOffsets);
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> updateAndGetGroupForbiddenOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(updateAndGetGroupForbiddenCase("forbidden-success", "orders", "workers",
            true, true, 0, null,
            "{\"topic\":\"orders\",\"group\":\"workers\",\"readable\":false}"));
        cases.add(updateAndGetGroupForbiddenCase("forbidden-null-header", null, null,
            null, false, 0, null, "{}"));
        cases.add(updateAndGetGroupForbiddenCase("forbidden-no-body", "orders", "workers",
            false, false, 0, null, null));
        cases.add(updateAndGetGroupForbiddenCase("forbidden-error", "orders", "workers",
            true, false, org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
            "group forbidden update rejected", null));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> groupForbiddenDtoOperations() {
        org.apache.rocketmq.remoting.protocol.subscription.GroupForbidden populated =
            new org.apache.rocketmq.remoting.protocol.subscription.GroupForbidden();
        populated.setTopic("orders");
        populated.setGroup("workers");
        populated.setReadable(false);
        org.apache.rocketmq.remoting.protocol.subscription.GroupForbidden defaultValue =
            new org.apache.rocketmq.remoting.protocol.subscription.GroupForbidden();
        Map<String, Object> result = new TreeMap<>();
        result.put("populatedHashCode", populated.hashCode());
        result.put("populatedToString", populated.toString());
        result.put("populatedJson", populated.toJson());
        result.put("populatedEqualsDefault", populated.equals(defaultValue));
        result.put("defaultHashCode", defaultValue.hashCode());
        result.put("defaultToString", defaultValue.toString());
        result.put("defaultJson", defaultValue.toJson());
        return result;
    }

    private static Map<String, Object> getBrokerHAStatusOperations() throws Exception {
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(getBrokerHAStatusCase("ha-populated", 0, null, haruntimeInfoSample()));
        cases.add(getBrokerHAStatusCase("ha-defaults", 0, null, "{}"));
        cases.add(getBrokerHAStatusCase("ha-success-no-body", 0, null, null));
        cases.add(getBrokerHAStatusCase("ha-error", 1, "HA status unavailable", null));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        return result;
    }

    private static String haruntimeInfoSample() {
        org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo status =
            new org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo();
        status.setMaster(true);
        status.setMasterCommitLogMaxOffset(987654321L);
        status.setInSyncSlaveNums(2);

        org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo.HAConnectionRuntimeInfo connection =
            new org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo.HAConnectionRuntimeInfo();
        connection.setAddr("10.0.0.2:10912");
        connection.setSlaveAckOffset(987654300L);
        connection.setDiff(21L);
        connection.setInSync(true);
        connection.setTransferredByteInSecond(4096L);
        connection.setTransferFromWhere(987650000L);
        status.setHaConnectionInfo(Arrays.asList(connection));

        org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo.HAClientRuntimeInfo client =
            new org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo.HAClientRuntimeInfo();
        client.setMasterAddr("10.0.0.1:10912");
        client.setTransferredByteInSecond(2048L);
        client.setMaxOffset(987654321L);
        client.setLastReadTimestamp(123456789L);
        client.setLastWriteTimestamp(123456790L);
        client.setMasterFlushOffset(987654320L);
        status.setHaClientRuntimeInfo(client);
        return status.toJson();
    }

    private static Map<String, Object> getBrokerHAStatusCase(
        String name, int responseCode, String responseRemark, String responseBody) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("broker-ha-status-api");
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand
                                    .createResponseCommand(responseCode, responseRemark);
                            if (responseBody != null) {
                                response.setBody(responseBody.getBytes(
                                    java.nio.charset.StandardCharsets.UTF_8));
                            }
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("timeoutMillis", 2345L);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseBody", responseBody);
        try {
            org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo status =
                api.getBrokerHAStatus("broker-a:10911", 2345L);
            result.put("outcome", "returned");
            result.put("resultIsNull", status == null);
            if (status != null) {
                result.put("resultJson", status.toJson());
            }
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> updateAndGetGroupForbiddenCase(
        String name, String topic, String group, Boolean readable, boolean withInheritedFields,
        int responseCode, String responseRemark, String responseBody) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("group-forbidden-api");
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand
                                    .createResponseCommand(responseCode, responseRemark);
                            if (responseBody != null) {
                                response.setBody(responseBody.getBytes(
                                    java.nio.charset.StandardCharsets.UTF_8));
                            }
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("timeoutMillis", 2345L);
        result.put("topic", topic);
        result.put("group", group);
        result.put("readable", readable);
        result.put("withInheritedFields", withInheritedFields);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseBody", responseBody);
        try {
            org.apache.rocketmq.remoting.protocol.header.UpdateGroupForbiddenRequestHeader header =
                new org.apache.rocketmq.remoting.protocol.header.UpdateGroupForbiddenRequestHeader();
            header.setTopic(topic);
            header.setGroup(group);
            header.setReadable(readable);
            if (withInheritedFields) {
                header.setNamespace("tenant");
                header.setNamespaced(true);
                header.setBrokerName("broker-a");
                header.setOneway(false);
                header.setLo(true);
            }
            org.apache.rocketmq.remoting.protocol.subscription.GroupForbidden forbidden =
                api.updateAndGetGroupForbidden("broker-a:10911", header, 2345L);
            result.put("outcome", "returned");
            result.put("resultIsNull", forbidden == null);
            if (forbidden != null) {
                result.put("resultTopic", forbidden.getTopic());
                result.put("resultGroup", forbidden.getGroup());
                result.put("resultReadable", forbidden.getReadable());
            }
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> invokeBrokerToGetConsumerStatusCase(
        String name, String topic, String group, String clientAddr, int responseCode,
        String responseRemark, String responseBody) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("broker-consumer-status-api");
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand
                                    .createResponseCommand(responseCode, responseRemark);
                            if (responseBody != null) {
                                response.setBody(responseBody.getBytes(
                                    java.nio.charset.StandardCharsets.UTF_8));
                            }
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("timeoutMillis", 2345L);
        result.put("topic", topic);
        result.put("group", group);
        result.put("clientAddr", clientAddr);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseBodyPresent", responseBody != null);
        result.put("responseBody", responseBody);
        try {
            Map<String, Map<org.apache.rocketmq.common.message.MessageQueue, Long>> status =
                api.invokeBrokerToGetConsumerStatus(
                    "broker-a:10911", topic, group, clientAddr, 2345L);
            result.put("outcome", "returned");
            result.put("consumerIds", status == null ? null : new ArrayList<>(status.keySet()));
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> cloneGroupOffsetCase(
        String name, String srcGroup, String destGroup, String topic, boolean offline,
        int responseCode, String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("clone-group-offset-api");
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            config, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyPresent", request.getBody() != null);
                            requests.add(call);
                            return org.apache.rocketmq.remoting.protocol.RemotingCommand
                                .createResponseCommand(responseCode, responseRemark);
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("timeoutMillis", 2345L);
        result.put("srcGroup", srcGroup);
        result.put("destGroup", destGroup);
        result.put("topic", topic);
        result.put("offline", offline);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            api.cloneGroupOffset("broker-a:10911", srcGroup, destGroup, topic, offline, 2345L);
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> addRemoveBrokerCase(
        String name, String operation, String configPath, String clusterName, String brokerName,
        Long brokerId, int responseCode, String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig clientConfig = new org.apache.rocketmq.client.ClientConfig();
        clientConfig.setClientIP("broker-admin-api");
        clientConfig.setVipChannelEnabled(true);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            clientConfig, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyBase64", request.getBody() == null ? null
                                : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                            requests.add(call);
                            return org.apache.rocketmq.remoting.protocol.RemotingCommand
                                .createResponseCommand(responseCode, responseRemark);
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("operation", operation);
        result.put("address", "broker-a:10911");
        result.put("timeoutMillis", 2345L);
        result.put("configPath", configPath);
        result.put("clusterName", clusterName);
        result.put("brokerName", brokerName);
        result.put("brokerId", brokerId);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            if ("add".equals(operation)) {
                api.addBroker("broker-a:10911", configPath, 2345L);
            } else {
                api.removeBroker("broker-a:10911", clusterName, brokerName, brokerId, 2345L);
            }
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> getClusterListCase(String name, String topic, long timeoutMillis)
        throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("get-cluster-list-api");
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
            ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", ((org.apache.rocketmq.remoting.protocol.RemotingCommand)
                                values[1]).getCode());
                            requests.add(call);
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("topic", topic);
        result.put("timeoutMillis", timeoutMillis);
        try {
            Set<String> received = api.getClusterList(topic, timeoutMillis);
            result.put("outcome", "returned");
            result.put("resultClass", received.getClass().getName());
            result.put("size", received.size());
            result.put("sameSingleton", received == api.getClusterList(null, 0L));
            try {
                received.add("mutation-probe");
                result.put("mutationThrows", false);
            } catch (UnsupportedOperationException error) {
                result.put("mutationThrows", true);
            }
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> resetMasterFlushOffsetOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(resetMasterFlushOffsetCase("success", Long.MAX_VALUE,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(resetMasterFlushOffsetCase("broker-error", -7L,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "flush offset rejected"));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> resetMasterFlushOffsetCase(
        String name, long masterFlushOffset, int responseCode, String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig clientConfig =
            new org.apache.rocketmq.client.ClientConfig();
        clientConfig.setClientIP("reset-master-flush-offset-api");
        clientConfig.setVipChannelEnabled(true);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            clientConfig, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyBase64", request.getBody() == null ? null
                                : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                            requests.add(call);
                            return org.apache.rocketmq.remoting.protocol.RemotingCommand
                                .createResponseCommand(responseCode, responseRemark);
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("masterFlushOffset", masterFlushOffset);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            api.resetMasterFlushOffset("broker-a:10911", masterFlushOffset);
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> exportRocksDBConfigTypeParsing() {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        for (String input : List.of("topics;subscriptionGroups;consumerOffsets;", "", "toPics; subScriptiongroups")) {
            Map<String, Object> row = new TreeMap<>();
            row.put("input", input);
            List<org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader.ConfigType>
                parsed = org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader.ConfigType
                    .fromString(input);
            row.put("outcome", "returned");
            row.put("enumNames", parsed.stream().map(Enum::name).collect(java.util.stream.Collectors.toList()));
            row.put("typeNames", parsed.stream()
                .map(org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader.ConfigType::getTypeName)
                .collect(java.util.stream.Collectors.toList()));
            row.put("serialized", org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader.ConfigType
                .toString(parsed));
            cases.add(row);
        }
        Map<String, Object> unknown = new TreeMap<>();
        unknown.put("input", "topics; subscription");
        try {
            org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader.ConfigType
                .fromString("topics; subscription");
            unknown.put("outcome", "returned");
        } catch (IllegalArgumentException error) {
            unknown.put("outcome", error.getClass().getSimpleName());
            unknown.put("errorMessage", error.getMessage());
        }
        cases.add(unknown);
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> exportRocksDBConfigToJsonOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader.ConfigType
            types = org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader.ConfigType.TOPICS;
        cases.add(exportRocksDBConfigToJsonCase("all-success", Arrays.asList(
            types,
            org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader.ConfigType.SUBSCRIPTION_GROUPS,
            org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader.ConfigType.CONSUMER_OFFSETS),
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(exportRocksDBConfigToJsonCase("empty-success", List.of(),
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(exportRocksDBConfigToJsonCase("broker-error", List.of(types),
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "RocksDB export rejected"));
        cases.add(exportRocksDBConfigToJsonCase("null-list", null,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> exportRocksDBConfigToJsonCase(
        String name,
        List<org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader.ConfigType> configTypes,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig clientConfig = new org.apache.rocketmq.client.ClientConfig();
        clientConfig.setClientIP("export-rocksdb-config-api");
        clientConfig.setVipChannelEnabled(true);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            clientConfig, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyBase64", request.getBody() == null ? null
                                : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                            requests.add(call);
                            return org.apache.rocketmq.remoting.protocol.RemotingCommand
                                .createResponseCommand(responseCode, responseRemark);
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("configTypeNames", configTypes == null ? null
            : configTypes.stream().map(Enum::name).collect(java.util.stream.Collectors.toList()));
        result.put("configType", configTypes == null ? null
            : org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader.ConfigType
                .toString(configTypes));
        try {
            api.exportRocksDBConfigToJson("broker-a:10911", configTypes, 2345L);
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } catch (RuntimeException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> writePermissionOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        for (String operation : List.of("wipe", "add")) {
            cases.add(writePermissionCase(operation, "success", "broker-a",
                org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
                operation.equals("wipe") ? "17" : "-3"));
            cases.add(writePermissionCase(operation, "null-broker-name", null,
                org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "0"));
            cases.add(writePermissionCase(operation, "empty-broker-name", "",
                org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "0"));
            cases.add(writePermissionCase(operation, "missing-response-header", "broker-a",
                org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, null));
            cases.add(writePermissionCase(operation, "malformed-response-count", "broker-a",
                org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "not-an-int"));
            cases.add(writePermissionCase(operation, "broker-error", "broker-a",
                org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
                "write permission update rejected", null));
        }
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> checkRocksdbCqWriteProgressOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(checkRocksdbCqWriteProgressCase("success-in-progress", "orders", 1_712_345_678_901L,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
            "{\"checkResult\":\"write in progress\",\"checkStatus\":2}"));
        cases.add(checkRocksdbCqWriteProgressCase("null-topic-zero-time", null, 0L,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
            "{}"));
        cases.add(checkRocksdbCqWriteProgressCase("empty-topic-negative-time", "", -1L,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null,
            "{\"checkResult\":\"\",\"checkStatus\":3}"));
        cases.add(checkRocksdbCqWriteProgressCase("json-null", "orders", 17L,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "null"));
        cases.add(checkRocksdbCqWriteProgressCase("missing-body", "orders", 18L,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, null));
        cases.add(checkRocksdbCqWriteProgressCase("empty-body", "orders", 19L,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, ""));
        cases.add(checkRocksdbCqWriteProgressCase("malformed-body", "orders", 20L,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "{"));
        cases.add(checkRocksdbCqWriteProgressCase("broker-error", "orders", 21L,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "check rejected", null));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> checkRocksdbCqWriteProgressCase(
        String name,
        String topic,
        long checkStoreTime,
        int responseCode,
        String responseRemark,
        String responseBody) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig clientConfig = new org.apache.rocketmq.client.ClientConfig();
        clientConfig.setClientIP("check-rocksdb-cq-write-progress-api");
        clientConfig.setVipChannelEnabled(true);
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            clientConfig, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyBase64", request.getBody() == null ? null
                                : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                            requests.add(call);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand
                                    .createResponseCommand(responseCode, responseRemark);
                            if (responseBody != null) {
                                response.setBody(responseBody.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                            }
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("name", name);
        result.put("address", "broker-a:10911");
        result.put("topic", topic);
        result.put("checkStoreTime", checkStoreTime);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseBody", responseBody);
        try {
            org.apache.rocketmq.common.CheckRocksdbCqWriteResult received =
                api.checkRocksdbCqWriteProgress("broker-a:10911", topic, checkStoreTime, 2345L);
            result.put("outcome", "returned");
            result.put("resultIsNull", received == null);
            result.put("checkResult", received == null ? null : received.getCheckResult());
            result.put("checkStatus", received == null ? null : received.getCheckStatus());
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } catch (RuntimeException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> controllerDtoApi() {
        Map<String, Object> result = new TreeMap<>();
        result.put("getMetaDataResponseHeader", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader.class));
        result.put("electMasterResponseHeader", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader.class));
        result.put("brokerReplicasInfo", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.class));
        result.put("replicasInfo", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicasInfo.class));
        result.put("replicaIdentity", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicaIdentity.class));
        result.put("epochEntryCache", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.EpochEntryCache.class));
        result.put("brokerMemberGroup", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup.class));
        result.put("epochEntry", publicApiSurface(
            org.apache.rocketmq.remoting.protocol.EpochEntry.class));
        return result;
    }

    private static Map<String, Object> controllerDtoOperations() {
        Map<String, Object> result = new TreeMap<>();
        org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicaIdentity alive =
            new org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicaIdentity(
                "broker-a", 0L, "broker-a:10911", true);
        org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicaIdentity dead =
            new org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicaIdentity(
                "broker-a", 0L, "broker-a:10911", false);
        result.put("replicaEqualsIgnoresAlive", alive.equals(dead));
        result.put("replicaEqualsWrongType", alive.equals("not-a-replica"));
        result.put("replicaHashIgnoresAlive", alive.hashCode() == dead.hashCode());
        result.put("replicaHashCode", alive.hashCode());
        result.put("replicaToString", alive.toString());

        org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicasInfo replicas =
            new org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicasInfo(
                0L, "broker-a:10911", 7, 8, List.of(dead), List.of());
        result.put("isExistInSyncIgnoresAlive", replicas.isExistInSync(
            "broker-a", 0L, "broker-a:10911"));
        result.put("isExistInNotSync", replicas.isExistInNotSync(
            "broker-a", 0L, "broker-a:10911"));
        result.put("isExistInAllReplicas", replicas.isExistInAllReplicas(
            "broker-a", 0L, "broker-a:10911"));
        result.put("nullQueryAfterEmptyNameMismatch", replicas.isExistInSync(
            "other", null, null));
        result.put("nullQueryException", nullPointerClass(() -> replicas.isExistInSync(
            null, 0L, "broker-a:10911")));

        org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicasInfo nullSync =
            new org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicasInfo(
                0L, "broker-a:10911", 7, 8, null, List.of());
        result.put("nullSyncListException", nullPointerClass(() -> nullSync.isExistInSync(
            "broker-a", 0L, "broker-a:10911")));
        org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicasInfo shortCircuit =
            new org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicasInfo(
                0L, "broker-a:10911", 7, 8, List.of(dead), null);
        result.put("allReplicasShortCircuits", shortCircuit.isExistInAllReplicas(
            "broker-a", 0L, "broker-a:10911"));
        org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicasInfo noMatch =
            new org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicasInfo(
                0L, "broker-a:10911", 7, 8, List.of(), null);
        result.put("allReplicasSecondListException", nullPointerClass(() -> noMatch.isExistInAllReplicas(
            "broker-a", 0L, "broker-a:10911")));

        org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo table =
            new org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo();
        table.addReplicaInfo(null, null);
        result.put("nullableReplicaMapEntry", table.getReplicasInfoTable().containsKey(null)
            && table.getReplicasInfoTable().get(null) == null);
        org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo nullTable =
            new org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo();
        nullTable.setReplicasInfoTable(null);
        result.put("nullReplicaTableException", nullPointerClass(() -> nullTable.addReplicaInfo(
            "broker-a", replicas)));

        org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup emptyGroup =
            new org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup();
        result.put("emptyMinimumBrokerId", emptyGroup.minimumBrokerId());
        org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup nullAddresses =
            new org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup();
        nullAddresses.setBrokerAddrs(null);
        result.put("nullBrokerAddrsException", nullPointerClass(nullAddresses::minimumBrokerId));
        org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup nullBrokerId =
            new org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup();
        Map<Long, String> addresses = new HashMap<>();
        addresses.put(null, "broker-null:10911");
        nullBrokerId.setBrokerAddrs(addresses);
        result.put("nullBrokerIdException", nullPointerClass(nullBrokerId::minimumBrokerId));
        org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup members =
            new org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup("cluster-a", "broker-a");
        members.getBrokerAddrs().put(8L, "broker-8:10911");
        members.getBrokerAddrs().put(2L, "broker-2:10911");
        result.put("minimumBrokerId", members.minimumBrokerId());
        result.put("brokerMemberGroupHashCode", members.hashCode());
        result.put("brokerMemberGroupToString", members.toString());

        org.apache.rocketmq.remoting.protocol.EpochEntry epoch =
            new org.apache.rocketmq.remoting.protocol.EpochEntry(4, 10L);
        result.put("epochEntryDefaultEnd", epoch.getEndOffset());
        result.put("epochEntryHashCode", epoch.hashCode());
        result.put("epochEntryToString", epoch.toString());
        result.put("epochEntryCopyConstructor", new org.apache.rocketmq.remoting.protocol.EpochEntry(epoch).toString());
        org.apache.rocketmq.remoting.protocol.body.EpochEntryCache epochCache =
            new org.apache.rocketmq.remoting.protocol.body.EpochEntryCache(
                "cluster-a", "broker-a", 2L, List.of(epoch), 20L);
        result.put("epochEntryCacheToString", epochCache.toString());
        result.put("metadataHeaderToString", new org.apache.rocketmq.remoting.protocol.header.controller
            .GetMetaDataResponseHeader("cluster-a", "controller-1", "controller-a:9878", true, "c-1,c-2")
            .toString());
        result.put("electionHeaderToString", new org.apache.rocketmq.remoting.protocol.header.controller
            .ElectMasterResponseHeader(7L, "broker-a:10911", 11, 12).toString());
        return result;
    }

    private static Map<String, Object> controllerAdminOperations() throws Exception {
        String controllerAddress = "controller-a:9878";
        List<Map<String, Object>> scriptedReplies = new ArrayList<>();
        scriptedReplies.add(controllerAdminReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
            Map.of("group", "cluster-a", "controllerLeaderId", "controller-1",
                "controllerLeaderAddress", controllerAddress, "isLeader", "true", "peers", "controller-1"),
            null));
        scriptedReplies.add(controllerAdminReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
            Map.of("group", "cluster-a", "controllerLeaderId", "controller-1",
                "controllerLeaderAddress", controllerAddress, "isLeader", "true", "peers", "controller-1"),
            null));
        scriptedReplies.add(controllerAdminReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
            null,
            "{\"replicasInfoTable\":{\"broker-a\":{\"masterBrokerId\":0,\"masterAddress\":\"broker-a:10911\",\"masterEpoch\":9,\"syncStateSetEpoch\":10,\"inSyncReplicas\":[{\"brokerName\":\"broker-a\",\"brokerId\":0,\"brokerAddress\":\"broker-a:10911\",\"alive\":true}],\"notInSyncReplicas\":[]}}}"));
        scriptedReplies.add(controllerAdminReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
            null,
            "{\"clusterName\":\"cluster-a\",\"brokerName\":\"broker-a\",\"brokerId\":3,\"epochList\":[{\"epoch\":4,\"startOffset\":10}],\"maxOffset\":20}"));
        scriptedReplies.add(controllerAdminReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
            null, "alpha=one\nbeta=two\n"));
        scriptedReplies.add(controllerAdminReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
            null, null));
        scriptedReplies.add(controllerAdminReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
            Map.of("group", "cluster-a", "controllerLeaderId", "controller-1",
                "controllerLeaderAddress", controllerAddress, "isLeader", "true", "peers", "controller-1"),
            null));
        scriptedReplies.add(controllerAdminReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
            Map.of("masterBrokerId", "7", "masterAddress", "broker-a:10911",
                "masterEpoch", "11", "syncStateSetEpoch", "12"),
            "{\"cluster\":\"cluster-a\",\"brokerName\":\"broker-a\",\"brokerAddrs\":{\"7\":\"broker-a:10911\"}}"));
        scriptedReplies.add(controllerAdminReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
            Map.of("group", "cluster-a", "controllerLeaderId", "controller-1",
                "controllerLeaderAddress", controllerAddress, "isLeader", "true", "peers", "controller-1"),
            null));
        scriptedReplies.add(controllerAdminReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
            null, null));

        List<Map<String, Object>> requests = new ArrayList<>();
        AtomicInteger replyIndex = new AtomicInteger();
        org.apache.rocketmq.client.ClientConfig clientConfig = new org.apache.rocketmq.client.ClientConfig();
        clientConfig.setClientIP("controller-admin-api");
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            clientConfig, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            RemotingCommand request = (RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            Map<String, String> recordedFields = request.getExtFields() == null ? null
                                : new TreeMap<>(request.getExtFields());
                            if (recordedFields != null && recordedFields.containsKey("invokeTime")) {
                                recordedFields.put("invokeTime", "1700000000000");
                            }
                            call.put("extFields", recordedFields);
                            call.put("requestBodyBase64", request.getBody() == null ? null
                                : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                            requests.add(call);

                            Map<String, Object> script = scriptedReplies.get(replyIndex.getAndIncrement());
                            RemotingCommand response = RemotingCommand.createResponseCommand(
                                (Integer) script.get("code"), (String) script.get("remark"));
                            @SuppressWarnings("unchecked")
                            Map<String, String> fields = (Map<String, String>) script.get("extFields");
                            if (fields != null) response.setExtFields(new HashMap<>(fields));
                            String body = (String) script.get("body");
                            if (body != null) response.setBody(body.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));

        Map<String, Object> result = new TreeMap<>();
        org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader metadata =
            api.getControllerMetaData(controllerAddress);
        result.put("metadataLeader", metadata.getControllerLeaderAddress());
        org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo replicas =
            api.getInSyncStateData(controllerAddress, List.of("broker-a"));
        result.put("syncStateMasterEpoch", replicas.getReplicasInfoTable().get("broker-a").getMasterEpoch());
        org.apache.rocketmq.remoting.protocol.body.EpochEntryCache epoch =
            api.getBrokerEpochCache("broker-a:10911");
        result.put("epochCacheMaxOffset", epoch.getMaxOffset());
        result.put("epochEntryEndOffsetWhenMissing", epoch.getEpochList().get(0).getEndOffset());

        Map<String, Properties> configs = api.getControllerConfig(List.of("controller-config:9878"), 2345L);
        result.put("controllerConfigAlpha", configs.get("controller-config:9878").getProperty("alpha"));
        Properties updated = new Properties();
        updated.setProperty("alpha", "one");
        updated.setProperty("beta", "two");
        api.updateControllerConfig(updated, List.of("controller-update:9878"), 2345L);

        org.apache.rocketmq.common.Pair<
            org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader,
            org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup> election =
            api.electMaster(controllerAddress, "cluster-a", "broker-a", 7L);
        result.put("electedBrokerId", election.getObject1().getMasterBrokerId());
        result.put("electedMemberAddress", election.getObject2().getBrokerAddrs().get(7L));
        api.cleanControllerBrokerData(controllerAddress, "cluster-a", "broker-a", "1,2", true);
        result.put("requests", requests);
        api.shutdown();
        return result;
    }

    private static Map<String, Object> controllerAdminReply(
        int code,
        Map<String, String> extFields,
        String body) {
        Map<String, Object> result = new TreeMap<>();
        result.put("code", code);
        result.put("remark", null);
        result.put("extFields", extFields);
        result.put("body", body);
        return result;
    }

    private static Map<String, Object> writePermissionCase(
        String operation,
        String name,
        String brokerName,
        int responseCode,
        String responseRemark,
        String responseCount) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig clientConfig = new org.apache.rocketmq.client.ClientConfig();
        clientConfig.setClientIP("write-permission-api");
        clientConfig.setVipChannelEnabled(true);
        String responseField = "wipe".equals(operation) ? "wipeTopicCount" : "addTopicCount";
        MQClientAPIImpl api = new MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null,
            clientConfig, null, ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            request.makeCustomHeaderToNet();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFields", request.getExtFields());
                            call.put("requestBodyBase64", request.getBody() == null ? null
                                : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                            requests.add(call);
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                org.apache.rocketmq.remoting.protocol.RemotingCommand
                                    .createResponseCommand(responseCode, responseRemark);
                            if (responseCount != null) {
                                HashMap<String, String> responseFields = new HashMap<>();
                                responseFields.put(responseField, responseCount);
                                response.setExtFields(responseFields);
                            }
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
        result.put("operation", operation);
        result.put("name", name);
        result.put("address", "namesrv-a:9876");
        result.put("brokerName", brokerName);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseField", responseField);
        result.put("responseCount", responseCount);
        try {
            if ("wipe".equals(operation)) {
                result.put("result", api.wipeWritePermOfBroker("namesrv-a:9876", brokerName, 2345L));
            } else {
                result.put("result", api.addWritePermOfBroker("namesrv-a:9876", brokerName, 2345L));
            }
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } catch (RuntimeException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> unitTopicListsOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        org.apache.rocketmq.remoting.protocol.body.TopicList topics =
            new org.apache.rocketmq.remoting.protocol.body.TopicList();
        topics.setTopicList(new java.util.HashSet<>(List.of(
            "orders", "%RETRY%workers", "payments")));
        topics.setBrokerAddr("broker-a:10911");
        for (String methodName : List.of(
            "getUnitTopicList", "getHasUnitSubTopicList", "getHasUnitSubUnUnitTopicList")) {
            cases.add(unitTopicListCase(methodName, true,
                org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, topics, true));
            cases.add(unitTopicListCase(methodName, false,
                org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, topics, true));
            cases.add(unitTopicListCase(methodName, false,
                org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
                "unit topic lookup rejected", null, false));
            cases.add(unitTopicListCase(methodName, false,
                org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, null, false));
        }
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> unitTopicListCase(
        String methodName,
        boolean unitMode,
        int responseCode,
        String responseRemark,
        org.apache.rocketmq.remoting.protocol.body.TopicList topicList,
        boolean bodyPresent) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("unit-topic-list-api");
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                    java.lang.reflect.Proxy.newProxyInstance(
                        ProducerCoreFixtures.class.getClassLoader(),
                        new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                        (proxy, method, values) -> {
                            if ("invokeSync".equals(method.getName())) {
                                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                    (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                                Map<String, Object> call = new TreeMap<>();
                                call.put("addressWasNull", values[0] == null);
                                call.put("requestCode", request.getCode());
                                call.put("timeoutMillis", values[2]);
                                call.put("extFieldsPresent", request.getExtFields() != null
                                    && !request.getExtFields().isEmpty());
                                call.put("requestBodyPresent", request.getBody() != null);
                                org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                                    org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                                        responseCode, responseRemark);
                                if (bodyPresent) response.setBody(topicList.encode());
                                call.put("responseCode", response.getCode());
                                call.put("responseRemark", response.getRemark());
                                call.put("responseBodyBase64", response.getBody() == null ? null
                                    : java.util.Base64.getEncoder().encodeToString(response.getBody()));
                                requests.add(call);
                                return response;
                            }
                            return method.getReturnType() == boolean.class ? false : null;
                        }));
        result.put("method", methodName);
        result.put("unitMode", unitMode);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        result.put("responseBodyBase64", bodyPresent
            ? java.util.Base64.getEncoder().encodeToString(topicList.encode()) : null);
        try {
            org.apache.rocketmq.remoting.protocol.body.TopicList received;
            switch (methodName) {
                case "getUnitTopicList": received = api.getUnitTopicList(unitMode, 2345L); break;
                case "getHasUnitSubTopicList": received = api.getHasUnitSubTopicList(unitMode, 2345L); break;
                case "getHasUnitSubUnUnitTopicList":
                    received = api.getHasUnitSubUnUnitTopicList(unitMode, 2345L); break;
                default: throw new IllegalArgumentException("unknown unit topic-list method: " + methodName);
            }
            result.put("outcome", "returned");
            result.put("topics", new TreeSet<>(received.getTopicList()));
            result.put("brokerAddr", received.getBrokerAddr());
        } catch (org.apache.rocketmq.client.exception.MQClientException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorMessage", error.getMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> nameServerConfigOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> updates = new ArrayList<>();
        java.util.Properties empty = new java.util.Properties();
        updates.add(updateNameServerConfigCase("empty-properties", empty, null,
            List.of("ns-unused:9876"), List.of()));
        java.util.Properties twoProperties = new java.util.Properties();
        twoProperties.setProperty("alpha", "one");
        twoProperties.setProperty("beta", "two");
        updates.add(updateNameServerConfigCase("explicit-success", twoProperties,
            List.of("ns-a:9876", "ns-b:9876"), List.of("ns-unused:9876"), List.of(
                nameServerConfigReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, null),
                nameServerConfigReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, null))));
        java.util.Properties oneProperty = new java.util.Properties();
        oneProperty.setProperty("rocketmq.namesrv.config", "enabled");
        updates.add(updateNameServerConfigCase("null-list-fallback", oneProperty, null,
            List.of("ns-c:9876"), List.of(
                nameServerConfigReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, null))));
        updates.add(updateNameServerConfigCase("empty-list-fallback", oneProperty, List.of(),
            List.of("ns-d:9876"), List.of(
                nameServerConfigReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, null))));
        updates.add(updateNameServerConfigCase("no-configured-servers", oneProperty, null,
            List.of(), List.of()));
        updates.add(updateNameServerConfigCase("last-error-after-all", oneProperty,
            List.of("ns-error-a:9876", "ns-ok:9876", "ns-error-b:9876"), List.of(), List.of(
                nameServerConfigReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
                    "first config rejected", null),
                nameServerConfigReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, null),
                nameServerConfigReply(org.apache.rocketmq.remoting.protocol.ResponseCode.NO_PERMISSION,
                    "last config rejected", null))));
        result.put("update", updates);

        List<Map<String, Object>> gets = new ArrayList<>();
        gets.add(getNameServerConfigCase("null-list-fallback", null,
            List.of("ns-get-a:9876", "ns-get-b:9876"), List.of(
                nameServerConfigReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
                    null, "alpha=one\n"),
                nameServerConfigReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
                    null, "beta=two\n"))));
        gets.add(getNameServerConfigCase("empty-list-fallback", List.of(),
            List.of("ns-empty:9876"), List.of(
                nameServerConfigReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "\n"))));
        gets.add(getNameServerConfigCase("explicit-error-stops", List.of("ns-error:9876", "ns-unused:9876"),
            List.of(), List.of(
                nameServerConfigReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR,
                    "config read rejected", null),
                nameServerConfigReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null, "later=x\n"))));
        gets.add(getNameServerConfigCase("malformed-properties", List.of("ns-malformed:9876"),
            List.of(), List.of(nameServerConfigReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
                null, "broken=\\u12G4\n"))));
        gets.add(getNameServerConfigCase("missing-body", List.of("ns-no-body:9876"),
            List.of(), List.of(nameServerConfigReply(org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS,
                null, null))));
        gets.add(getNameServerConfigCase("no-configured-servers", null, List.of(), List.of()));
        result.put("get", gets);
        return result;
    }

    private static Map<String, Object> resumeCheckHalfMessageOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(resumeCheckHalfMessageCase("success", "orders", "msg-1",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(resumeCheckHalfMessageCase("non-success", "orders", "msg-2",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "resume rejected"));
        cases.add(resumeCheckHalfMessageCase("null-topic", null, "msg-3",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(resumeCheckHalfMessageCase("null-msg-id", "orders", null,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(resumeCheckHalfMessageCase("both-fields-null", null, null,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> resumeCheckHalfMessageCase(
        String name,
        String topic,
        String msgId,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        java.util.ArrayDeque<org.apache.rocketmq.remoting.protocol.RemotingCommand> replies =
            new java.util.ArrayDeque<>();
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            responseCode, responseRemark));

        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("resume-check-half-message");
        config.setVipChannelEnabled(true);
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                    java.lang.reflect.Proxy.newProxyInstance(
                        ProducerCoreFixtures.class.getClassLoader(),
                        new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                        (proxy, method, values) -> {
                            if ("invokeSync".equals(method.getName())) {
                                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                    (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                                request.makeCustomHeaderToNet();
                                org.apache.rocketmq.remoting.protocol.RemotingCommand reply = replies.removeFirst();
                                Map<String, Object> call = new TreeMap<>();
                                call.put("address", values[0]);
                                call.put("requestCode", request.getCode());
                                call.put("timeoutMillis", values[2]);
                                call.put("extFields", request.getExtFields());
                                call.put("requestBodyPresent", request.getBody() != null);
                                call.put("responseCode", reply.getCode());
                                call.put("responseRemark", reply.getRemark());
                                requests.add(call);
                                return reply;
                            }
                            return method.getReturnType() == boolean.class ? false : null;
                        }));
        result.put("name", name);
        result.put("topic", topic);
        result.put("msgId", msgId);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            result.put("result", api.resumeCheckHalfMessage(
                "127.0.0.1:10913", topic, msgId, 2345L));
        } finally {
            api.shutdown();
        }
        result.put("requestExtFields", requests.get(0).get("extFields"));
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> exportPopRecordOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(exportPopRecordCase("success",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(exportPopRecordCase("non-success",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "rejected"));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> exportPopRecordCase(
        String name,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        java.util.ArrayDeque<org.apache.rocketmq.remoting.protocol.RemotingCommand> replies =
            new java.util.ArrayDeque<>();
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            responseCode, responseRemark));

        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("export-pop-record");
        config.setVipChannelEnabled(true);
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                    java.lang.reflect.Proxy.newProxyInstance(
                        ProducerCoreFixtures.class.getClassLoader(),
                        new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                        (proxy, method, values) -> {
                            if ("invokeSync".equals(method.getName())) {
                                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                    (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                                org.apache.rocketmq.remoting.protocol.RemotingCommand reply = replies.removeFirst();
                                Map<String, Object> call = new TreeMap<>();
                                call.put("address", values[0]);
                                call.put("requestCode", request.getCode());
                                call.put("timeoutMillis", values[2]);
                                call.put("extFieldsPresent", request.getExtFields() != null
                                    && !request.getExtFields().isEmpty());
                                call.put("requestBodyPresent", request.getBody() != null);
                                call.put("responseCode", reply.getCode());
                                call.put("responseRemark", reply.getRemark());
                                requests.add(call);
                                return reply;
                            }
                            return method.getReturnType() == boolean.class ? false : null;
                        }));
        result.put("name", name);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            api.exportPopRecord("127.0.0.1:10913", 2345L);
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorRemark", error.getErrorMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> switchTimerEngineOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(switchTimerEngineCase("success", "TIMER",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(switchTimerEngineCase("empty-engine-type", "",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(switchTimerEngineCase("non-success", "INVALID",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "engine rejected"));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> switchTimerEngineCase(
        String name,
        String engineType,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        java.util.ArrayDeque<org.apache.rocketmq.remoting.protocol.RemotingCommand> replies =
            new java.util.ArrayDeque<>();
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            responseCode, responseRemark));

        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("switch-timer-engine");
        config.setVipChannelEnabled(true);
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                    java.lang.reflect.Proxy.newProxyInstance(
                        ProducerCoreFixtures.class.getClassLoader(),
                        new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                        (proxy, method, values) -> {
                            if ("invokeSync".equals(method.getName())) {
                                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                    (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                                org.apache.rocketmq.remoting.protocol.RemotingCommand reply = replies.removeFirst();
                                Map<String, Object> call = new TreeMap<>();
                                call.put("address", values[0]);
                                call.put("requestCode", request.getCode());
                                call.put("timeoutMillis", values[2]);
                                call.put("extFields", request.getExtFields());
                                call.put("requestBodyPresent", request.getBody() != null);
                                call.put("responseCode", reply.getCode());
                                call.put("responseRemark", reply.getRemark());
                                requests.add(call);
                                return reply;
                            }
                            return method.getReturnType() == boolean.class ? false : null;
                        }));
        result.put("name", name);
        result.put("engineType", engineType);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            api.switchTimerEngine("127.0.0.1:10913", engineType, 2345L);
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorRemark", error.getErrorMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> triggerLiteDispatchOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(triggerLiteDispatchCase("success", "lite-group", "lite-client",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(triggerLiteDispatchCase("nullable-fields", null, null,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        cases.add(triggerLiteDispatchCase("non-success", "rejected-group", "rejected-client",
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, "dispatch rejected"));
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> triggerLiteDispatchCase(
        String name,
        String group,
        String clientId,
        int responseCode,
        String responseRemark) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        java.util.ArrayDeque<org.apache.rocketmq.remoting.protocol.RemotingCommand> replies =
            new java.util.ArrayDeque<>();
        replies.add(org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
            responseCode, responseRemark));

        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("trigger-lite-dispatch");
        config.setVipChannelEnabled(true);
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                    java.lang.reflect.Proxy.newProxyInstance(
                        ProducerCoreFixtures.class.getClassLoader(),
                        new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                        (proxy, method, values) -> {
                            if ("invokeSync".equals(method.getName())) {
                                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                    (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                                request.makeCustomHeaderToNet();
                                org.apache.rocketmq.remoting.protocol.RemotingCommand reply = replies.removeFirst();
                                Map<String, Object> call = new TreeMap<>();
                                call.put("address", values[0]);
                                call.put("requestCode", request.getCode());
                                call.put("timeoutMillis", values[2]);
                                call.put("requestBodyPresent", request.getBody() != null);
                                call.put("responseCode", reply.getCode());
                                call.put("responseRemark", reply.getRemark());
                                requests.add(call);
                                return reply;
                            }
                            return method.getReturnType() == boolean.class ? false : null;
                        }));
        result.put("name", name);
        result.put("group", group);
        result.put("clientId", clientId);
        Map<String, String> expectedExtFields = new TreeMap<>();
        if (group != null) expectedExtFields.put("group", group);
        if (clientId != null) expectedExtFields.put("clientId", clientId);
        result.put("expectedExtFields", expectedExtFields);
        result.put("responseCode", responseCode);
        result.put("responseRemark", responseRemark);
        try {
            api.triggerLiteDispatch("127.0.0.1:10913", group, clientId, 2345L);
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorRemark", error.getErrorMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> producerDiagnosticsOperations() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        org.apache.rocketmq.remoting.protocol.body.Connection connection =
            new org.apache.rocketmq.remoting.protocol.body.Connection();
        connection.setClientId("producer-client");
        connection.setClientAddr("192.0.2.9:1234");
        connection.setLanguage(org.apache.rocketmq.remoting.protocol.LanguageCode.JAVA);
        connection.setVersion(1);
        org.apache.rocketmq.remoting.protocol.body.ProducerConnection producerConnection =
            new org.apache.rocketmq.remoting.protocol.body.ProducerConnection();
        producerConnection.setConnectionSet(new java.util.HashSet<>(List.of(connection)));
        result.put("producerConnectionDefaultJson", JSON.toJSONString(
            new org.apache.rocketmq.remoting.protocol.body.ProducerConnection()));
        Map<String, Object> producerConnectionSetSemantics = new TreeMap<>();
        java.util.HashSet<org.apache.rocketmq.remoting.protocol.body.Connection> assignedConnectionSet =
            new java.util.HashSet<>(List.of(connection));
        org.apache.rocketmq.remoting.protocol.body.ProducerConnection aliasProbe =
            new org.apache.rocketmq.remoting.protocol.body.ProducerConnection();
        aliasProbe.setConnectionSet(assignedConnectionSet);
        producerConnectionSetSemantics.put("dtoGetterReturnsAssignedSet",
            aliasProbe.getConnectionSet() == assignedConnectionSet);
        org.apache.rocketmq.remoting.protocol.body.Connection addedThroughGetter =
            new org.apache.rocketmq.remoting.protocol.body.Connection();
        producerConnectionSetSemantics.put("dtoGetterMutationVisibleInAssignedSet",
            aliasProbe.getConnectionSet().add(addedThroughGetter)
                && assignedConnectionSet.contains(addedThroughGetter));
        java.util.HashSet<org.apache.rocketmq.remoting.protocol.body.Connection> identitySet =
            new java.util.HashSet<>();
        producerConnectionSetSemantics.put("firstSameReferenceAdd", identitySet.add(connection));
        producerConnectionSetSemantics.put("secondSameReferenceAdd", identitySet.add(connection));
        org.apache.rocketmq.remoting.protocol.body.Connection sameFieldsDifferentReference =
            new org.apache.rocketmq.remoting.protocol.body.Connection();
        sameFieldsDifferentReference.setClientId(connection.getClientId());
        sameFieldsDifferentReference.setClientAddr(connection.getClientAddr());
        sameFieldsDifferentReference.setLanguage(connection.getLanguage());
        sameFieldsDifferentReference.setVersion(connection.getVersion());
        producerConnectionSetSemantics.put("differentReferenceAdd",
            identitySet.add(sameFieldsDifferentReference));
        producerConnectionSetSemantics.put("sizeBeforeNull", identitySet.size());
        producerConnectionSetSemantics.put("firstNullAdd", identitySet.add(null));
        producerConnectionSetSemantics.put("secondNullAdd", identitySet.add(null));
        producerConnectionSetSemantics.put("sizeAfterNull", identitySet.size());
        result.put("producerConnectionSetSemantics", producerConnectionSetSemantics);
        org.apache.rocketmq.remoting.protocol.body.ProducerConnection nullConnections =
            new org.apache.rocketmq.remoting.protocol.body.ProducerConnection();
        nullConnections.setConnectionSet(null);
        Map<String, Object> dtoNullSemantics = new TreeMap<>();
        dtoNullSemantics.put("producerConnectionSetIsNull", nullConnections.getConnectionSet() == null);
        dtoNullSemantics.put("producerConnectionJson", JSON.toJSONString(nullConnections));
        result.put("getProducerConnectionList", producerDiagnosticCases(
            "getProducerConnectionList", producerConnection, "workers"));

        org.apache.rocketmq.remoting.protocol.body.ProducerInfo producerInfo =
            new org.apache.rocketmq.remoting.protocol.body.ProducerInfo(
                "producer-client", "192.0.2.9", org.apache.rocketmq.remoting.protocol.LanguageCode.JAVA,
                1, 1_700_000_000_123L);
        Map<String, Object> producerInfoString = new TreeMap<>();
        producerInfoString.put("value", producerInfo.toString());
        try {
            new org.apache.rocketmq.remoting.protocol.body.ProducerInfo(
                "producer-client", "192.0.2.9", null, 1, 1_700_000_000_123L).toString();
            producerInfoString.put("nullLanguageOutcome", "returned");
        } catch (NullPointerException expected) {
            producerInfoString.put("nullLanguageOutcome", expected.getClass().getSimpleName());
        }
        result.put("producerInfoToString", producerInfoString);
        org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo producerTableInfo =
            new org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo(
                new HashMap<>(Map.of("workers", List.of(producerInfo))));
        org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo nullProducerTableInfo =
            new org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo(null);
        dtoNullSemantics.put("producerTableInfoDataIsNull", nullProducerTableInfo.getData() == null);
        dtoNullSemantics.put("producerTableInfoJson", JSON.toJSONString(nullProducerTableInfo));
        result.put("getAllProducerInfo", producerDiagnosticCases(
            "getAllProducerInfo", producerTableInfo, null));

        org.apache.rocketmq.remoting.protocol.body.KVTable kvTable =
            new org.apache.rocketmq.remoting.protocol.body.KVTable();
        result.put("kvTableDefaultJson", JSON.toJSONString(
            new org.apache.rocketmq.remoting.protocol.body.KVTable()));
        org.apache.rocketmq.remoting.protocol.body.KVTable nullKvTable =
            new org.apache.rocketmq.remoting.protocol.body.KVTable();
        nullKvTable.setTable(null);
        dtoNullSemantics.put("kvTableIsNull", nullKvTable.getTable() == null);
        dtoNullSemantics.put("kvTableJson", JSON.toJSONString(nullKvTable));
        HashMap<String, String> runtimeTable = new HashMap<>();
        runtimeTable.put("brokerName", "broker-a");
        runtimeTable.put("brokerVersion", "5.5.1");
        kvTable.setTable(runtimeTable);
        result.put("getBrokerRuntimeInfo", producerDiagnosticCases(
            "getBrokerRuntimeInfo", kvTable, null));
        result.put("dtoNullSemantics", dtoNullSemantics);
        return result;
    }

    private static Map<String, Object> producerDiagnosticCases(
        String methodName, Object successBody, String producerGroup) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(producerDiagnosticCase(methodName, producerGroup,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, successBody));
        cases.add(producerDiagnosticCase(methodName, producerGroup,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SYSTEM_ERROR, null));
        cases.add(producerDiagnosticCase(methodName, producerGroup,
            org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, null));
        if (producerGroup != null) {
            cases.add(producerDiagnosticCase(methodName, null,
                org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS, successBody));
        }
        result.put("cases", cases);
        return result;
    }

    private static Map<String, Object> producerDiagnosticCase(
        String methodName, String producerGroup, int responseCode, Object responseBody) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        java.util.ArrayDeque<org.apache.rocketmq.remoting.protocol.RemotingCommand> replies =
            new java.util.ArrayDeque<>();
        org.apache.rocketmq.remoting.protocol.RemotingCommand reply =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(responseCode,
                responseCode == org.apache.rocketmq.remoting.protocol.ResponseCode.SUCCESS ? null : "diagnostic rejected");
        if (responseBody != null) reply.setBody(((org.apache.rocketmq.remoting.protocol.RemotingSerializable) responseBody).encode());
        replies.add(reply);
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("producer-diagnostics-api");
        config.setVipChannelEnabled(true);
        org.apache.rocketmq.client.impl.MQClientAPIImpl api =
            new org.apache.rocketmq.client.impl.MQClientAPIImpl(
                new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                    java.lang.reflect.Proxy.newProxyInstance(
                        ProducerCoreFixtures.class.getClassLoader(),
                        new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                        (proxy, method, values) -> {
                            if ("invokeSync".equals(method.getName())) {
                                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                    (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                                request.makeCustomHeaderToNet();
                                org.apache.rocketmq.remoting.protocol.RemotingCommand response = replies.removeFirst();
                                Map<String, Object> call = new TreeMap<>();
                                call.put("address", values[0]);
                                call.put("requestCode", request.getCode());
                                call.put("timeoutMillis", values[2]);
                                call.put("extFields", request.getExtFields());
                                call.put("extFieldsPresent", request.getExtFields() != null
                                    && !request.getExtFields().isEmpty());
                                call.put("requestBodyPresent", request.getBody() != null);
                                requests.add(call);
                                return response;
                            }
                            return method.getReturnType() == boolean.class ? false : null;
                        }));
        result.put("method", methodName);
        result.put("producerGroup", producerGroup);
        result.put("responseCode", responseCode);
        result.put("responseBody", reply.getBody() == null ? null
            : new String(reply.getBody(), java.nio.charset.StandardCharsets.UTF_8));
        try {
            Object value;
            switch (methodName) {
                case "getProducerConnectionList":
                    value = api.getProducerConnectionList("127.0.0.1:10913", producerGroup, 2345L);
                    if (value != null) {
                        org.apache.rocketmq.remoting.protocol.body.ProducerConnection connections =
                            (org.apache.rocketmq.remoting.protocol.body.ProducerConnection) value;
                        List<Map<String, Object>> rows = new ArrayList<>();
                        for (org.apache.rocketmq.remoting.protocol.body.Connection item : connections.getConnectionSet()) {
                            Map<String, Object> row = new TreeMap<>();
                            row.put("clientId", item.getClientId());
                            row.put("clientAddr", item.getClientAddr());
                            row.put("language", item.getLanguage() == null ? null : item.getLanguage().name());
                            row.put("version", item.getVersion());
                            rows.add(row);
                        }
                        rows.sort(java.util.Comparator.comparing(row -> String.valueOf(row.get("clientId"))));
                        result.put("result", rows);
                    }
                    break;
                case "getAllProducerInfo":
                    org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo producers =
                        api.getAllProducerInfo("127.0.0.1:10913", 2345L);
                    result.put("result", producers == null ? null : JSON.parseObject(JSON.toJSONString(producers.getData())));
                    break;
                case "getBrokerRuntimeInfo":
                    org.apache.rocketmq.remoting.protocol.body.KVTable runtime =
                        api.getBrokerRuntimeInfo("127.0.0.1:10913", 2345L);
                    result.put("result", runtime == null ? null : runtime.getTable());
                    break;
                default:
                    throw new IllegalArgumentException("unexpected producer diagnostic: " + methodName);
            }
            result.putIfAbsent("result", null);
            result.put("outcome", "returned");
        } catch (org.apache.rocketmq.client.exception.MQBrokerException error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorCode", error.getResponseCode());
            result.put("errorRemark", error.getErrorMessage());
        } finally {
            api.shutdown();
        }
        result.put("requests", requests);
        return result;
    }

    private static org.apache.rocketmq.remoting.protocol.RemotingCommand nameServerConfigReply(
        int responseCode,
        String responseRemark,
        String responseBody) {
        org.apache.rocketmq.remoting.protocol.RemotingCommand response =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(
                responseCode, responseRemark);
        if (responseBody != null) response.setBody(responseBody.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return response;
    }

    private static org.apache.rocketmq.client.impl.MQClientAPIImpl nameServerConfigApi(
        List<String> configuredAddresses,
        List<org.apache.rocketmq.remoting.protocol.RemotingCommand> replies,
        List<Map<String, Object>> requests,
        AtomicInteger nameServerListLookups) {
        java.util.ArrayDeque<org.apache.rocketmq.remoting.protocol.RemotingCommand> replyQueue =
            new java.util.ArrayDeque<>(replies);
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("name-server-config-api");
        return new org.apache.rocketmq.client.impl.MQClientAPIImpl(
            new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
            ignored -> (org.apache.rocketmq.remoting.RemotingClient)
                java.lang.reflect.Proxy.newProxyInstance(
                    ProducerCoreFixtures.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> {
                        if ("getNameServerAddressList".equals(method.getName())) {
                            nameServerListLookups.incrementAndGet();
                            return configuredAddresses;
                        }
                        if ("invokeSync".equals(method.getName())) {
                            org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                                (org.apache.rocketmq.remoting.protocol.RemotingCommand) values[1];
                            org.apache.rocketmq.remoting.protocol.RemotingCommand response = replyQueue.removeFirst();
                            Map<String, Object> call = new TreeMap<>();
                            call.put("address", values[0]);
                            call.put("requestCode", request.getCode());
                            call.put("timeoutMillis", values[2]);
                            call.put("extFieldsPresent", request.getExtFields() != null
                                && !request.getExtFields().isEmpty());
                            call.put("requestBodyBase64", request.getBody() == null ? null
                                : java.util.Base64.getEncoder().encodeToString(request.getBody()));
                            call.put("responseCode", response.getCode());
                            call.put("responseRemark", response.getRemark());
                            call.put("responseBodyBase64", response.getBody() == null ? null
                                : java.util.Base64.getEncoder().encodeToString(response.getBody()));
                            requests.add(call);
                            return response;
                        }
                        return method.getReturnType() == boolean.class ? false : null;
                    }));
    }

    private static Map<String, Object> updateNameServerConfigCase(
        String name,
        java.util.Properties properties,
        List<String> addresses,
        List<String> configuredAddresses,
        List<org.apache.rocketmq.remoting.protocol.RemotingCommand> replies) {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        AtomicInteger nameServerListLookups = new AtomicInteger();
        org.apache.rocketmq.client.impl.MQClientAPIImpl api = nameServerConfigApi(
            configuredAddresses, replies, requests, nameServerListLookups);
        result.put("name", name);
        result.put("addressInput", addresses);
        result.put("configuredAddresses", configuredAddresses);
        Map<String, String> inputProperties = new TreeMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            inputProperties.put(entry.getKey().toString(), entry.getValue().toString());
        }
        result.put("properties", inputProperties);
        try {
            api.updateNameServerConfig(properties, addresses, 2345L);
            result.put("outcome", "returned");
        } catch (Throwable error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorMessage", error.getMessage());
            if (error instanceof org.apache.rocketmq.client.exception.MQClientException) {
                result.put("errorCode", ((org.apache.rocketmq.client.exception.MQClientException) error)
                    .getResponseCode());
            }
        } finally {
            api.shutdown();
        }
        result.put("nameServerListLookups", nameServerListLookups.get());
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> getNameServerConfigCase(
        String name,
        List<String> addresses,
        List<String> configuredAddresses,
        List<org.apache.rocketmq.remoting.protocol.RemotingCommand> replies) {
        Map<String, Object> result = new TreeMap<>();
        List<Map<String, Object>> requests = new ArrayList<>();
        AtomicInteger nameServerListLookups = new AtomicInteger();
        org.apache.rocketmq.client.impl.MQClientAPIImpl api = nameServerConfigApi(
            configuredAddresses, replies, requests, nameServerListLookups);
        result.put("name", name);
        result.put("addressInput", addresses);
        result.put("configuredAddresses", configuredAddresses);
        try {
            Map<String, java.util.Properties> configs = api.getNameServerConfig(addresses, 2345L);
            result.put("outcome", "returned");
            if (configs == null) {
                result.put("configurations", null);
                result.put("nullProperties", List.of());
            } else {
                Map<String, Object> converted = new TreeMap<>();
                List<String> nullProperties = new ArrayList<>();
                for (Map.Entry<String, java.util.Properties> entry : configs.entrySet()) {
                    if (entry.getValue() == null) {
                        converted.put(entry.getKey(), null);
                        nullProperties.add(entry.getKey());
                    } else {
                        Map<String, String> values = new TreeMap<>();
                        for (Map.Entry<Object, Object> property : entry.getValue().entrySet()) {
                            values.put(property.getKey().toString(), property.getValue().toString());
                        }
                        converted.put(entry.getKey(), values);
                    }
                }
                result.put("configurations", converted);
                result.put("nullProperties", nullProperties);
            }
        } catch (Throwable error) {
            result.put("outcome", error.getClass().getSimpleName());
            result.put("errorMessage", error.getMessage());
            if (error instanceof org.apache.rocketmq.client.exception.MQClientException) {
                result.put("errorCode", ((org.apache.rocketmq.client.exception.MQClientException) error)
                    .getResponseCode());
            }
        } finally {
            api.shutdown();
        }
        result.put("nameServerListLookups", nameServerListLookups.get());
        result.put("requests", requests);
        return result;
    }

    private static Map<String, Object> implementationHookExecution() {
        DefaultMQProducer producer = new DefaultMQProducer("producer-implementation-hooks");
        var implementation = producer.getDefaultMQProducerImpl();
        Map<String, Object> result = new TreeMap<>();
        result.put("checkForbiddenInitiallyPresent", implementation.hasCheckForbiddenHook());
        result.put("sendMessageInitiallyPresent", implementation.hasSendMessageHook());

        List<String> checkEvents = new ArrayList<>();
        implementation.registerCheckForbiddenHook(new CheckForbiddenHook() {
            @Override
            public String hookName() {
                return "fixture-check-first";
            }

            @Override
            public void checkForbidden(CheckForbiddenContext context) {
                checkEvents.add("first");
                MessageAccessor.putProperty(context.getMessage(), "DIRECT_CHECK_HOOK", "mutated");
                context.getMq().setQueueId(4);
            }
        });
        implementation.registerCheckForbiddenHook(new CheckForbiddenHook() {
            @Override
            public String hookName() {
                return "fixture-check-reject";
            }

            @Override
            public void checkForbidden(CheckForbiddenContext context) throws MQClientException {
                checkEvents.add("reject:" + context.getMessage().getProperty("DIRECT_CHECK_HOOK"));
                throw new MQClientException("fixture forbidden", null);
            }
        });
        implementation.registerCheckForbiddenHook(new CheckForbiddenHook() {
            @Override
            public String hookName() {
                return "fixture-check-last";
            }

            @Override
            public void checkForbidden(CheckForbiddenContext context) {
                checkEvents.add("unreached");
            }
        });
        Message checkMessage = new Message("orders", new byte[] {1});
        MessageQueue checkQueue = new MessageQueue("orders", "broker-a", 1);
        CheckForbiddenContext checkContext = new CheckForbiddenContext();
        checkContext.setGroup("producer-implementation-hooks");
        checkContext.setMessage(checkMessage);
        checkContext.setMq(checkQueue);
        checkContext.setBrokerAddr("127.0.0.1:10911");
        checkContext.setCommunicationMode(CommunicationMode.SYNC);
        String checkFailure = null;
        try {
            implementation.executeCheckForbiddenHook(checkContext);
        } catch (MQClientException expected) {
            checkFailure = expected.getMessage();
        }
        result.put("checkForbiddenPresentAfterRegistration", implementation.hasCheckForbiddenHook());
        result.put("checkEvents", checkEvents);
        result.put("checkFailure", checkFailure);
        result.put("checkMessageMarker", checkMessage.getProperty("DIRECT_CHECK_HOOK"));
        result.put("checkQueueId", checkQueue.getQueueId());

        List<String> sendEvents = new ArrayList<>();
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "fixture-send-first";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                sendEvents.add("before:first");
                MessageAccessor.putProperty(context.getMessage(), "DIRECT_SEND_HOOK", "before");
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                sendEvents.add("after:first");
                MessageAccessor.putProperty(context.getMessage(), "DIRECT_SEND_HOOK", "after");
            }
        });
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "fixture-send-throws";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                sendEvents.add("before:throws");
                throw new IllegalStateException("fixture before failure");
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                sendEvents.add("after:throws");
                throw new IllegalStateException("fixture after failure");
            }
        });
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "fixture-send-last";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                sendEvents.add("before:last:" + context.getMessage().getProperty("DIRECT_SEND_HOOK"));
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                sendEvents.add("after:last:" + context.getMessage().getProperty("DIRECT_SEND_HOOK"));
            }
        });
        Message sendMessage = new Message("orders", new byte[] {2});
        SendMessageContext sendContext = new SendMessageContext();
        sendContext.setProducer(implementation);
        sendContext.setProducerGroup("producer-implementation-hooks");
        sendContext.setMessage(sendMessage);
        sendContext.setMq(new MessageQueue("orders", "broker-a", 2));
        sendContext.setBrokerAddr("127.0.0.1:10911");
        sendContext.setCommunicationMode(CommunicationMode.SYNC);
        implementation.executeSendMessageHookBefore(sendContext);
        implementation.executeSendMessageHookAfter(sendContext);
        result.put("sendMessagePresentAfterRegistration", implementation.hasSendMessageHook());
        result.put("sendEvents", sendEvents);
        result.put("sendMessageMarker", sendMessage.getProperty("DIRECT_SEND_HOOK"));
        return result;
    }

    private static Map<String, Object> implementationStateAccessors() {
        DefaultMQProducer producer = new DefaultMQProducer("producer-implementation-state");
        producer.setUnitMode(true);
        var implementation = producer.getDefaultMQProducerImpl();
        Map<String, Object> result = new TreeMap<>();
        result.put("initialServiceState", implementation.getServiceState().name());
        result.put("serviceStateValues", Arrays.stream(ServiceState.values())
            .map(ServiceState::name)
            .collect(java.util.stream.Collectors.toList()));
        result.put("serviceStateOrdinals", Arrays.stream(ServiceState.values())
            .map(ServiceState::ordinal)
            .collect(java.util.stream.Collectors.toList()));
        result.put("defaultProducerIdentity", implementation.getDefaultMQProducer() == producer);
        result.put("defaultProducerImplIdentity", producer.getDefaultMQProducerImpl() == implementation);
        result.put("defaultProducerGroup", implementation.getDefaultMQProducer().getProducerGroup());
        result.put("unitMode", implementation.isUnitMode());
        result.put("clientFactoryInitiallyNull",
            implementation.getMqClientFactory() == null && implementation.getmQClientFactory() == null);
        implementation.setServiceState(null);
        result.put("nullServiceState", implementation.getServiceState() == null);
        return result;
    }

    private static Map<String, Object> implementationSemaphoreSettings() throws InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("producer-implementation-semaphores");
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        Map<String, Object> result = new TreeMap<>();
        result.put("initialCountAvailable", implementation.getSemaphoreAsyncSendNumAvailablePermits());
        result.put("initialByteAvailable", implementation.getSemaphoreAsyncSendSizeAvailablePermits());

        Semaphore currentCount = asyncSendNumSemaphore(producer);
        currentCount.acquire(2);
        implementation.setSemaphoreAsyncSendNum(7);
        asyncSendNumSemaphore(producer).release(2);
        result.put("countAfterRawReplaceAndInFlightRelease",
            implementation.getSemaphoreAsyncSendNumAvailablePermits());
        producer.setBackPressureForAsyncSendNum(20);
        result.put("countAfterFacadeResize", implementation.getSemaphoreAsyncSendNumAvailablePermits());
        result.put("facadeCountLimitAfterResize", producer.getBackPressureForAsyncSendNum());

        implementation.setSemaphoreAsyncSendNum(-3);
        implementation.setSemaphoreAsyncSendSize(4096);
        result.put("negativeCountAvailable", implementation.getSemaphoreAsyncSendNumAvailablePermits());
        result.put("byteAvailableAfterRawReplace", implementation.getSemaphoreAsyncSendSizeAvailablePermits());
        implementation.setSemaphoreAsyncSendSize(-8192);
        result.put("negativeByteAvailable", implementation.getSemaphoreAsyncSendSizeAvailablePermits());
        result.put("facadeByteLimitUnchanged", producer.getBackPressureForAsyncSendSize());
        return result;
    }

    private static Map<String, Object> executorSettings() {
        DefaultMQProducer producer = new DefaultMQProducer("producer-executor-fixture");
        ExecutorService defaultExecutor = producer.getDefaultMQProducerImpl().getAsyncSenderExecutor();
        ExecutorService customExecutor = Executors.newSingleThreadExecutor();
        Map<String, Object> result = new TreeMap<>();
        result.put("defaultAsyncExecutorStable",
            defaultExecutor == producer.getDefaultMQProducerImpl().getAsyncSenderExecutor());
        producer.setAsyncSenderExecutor(customExecutor);
        result.put("customAsyncExecutorRetained",
            customExecutor == producer.getDefaultMQProducerImpl().getAsyncSenderExecutor());
        producer.setAsyncSenderExecutor(null);
        result.put("nullAsyncExecutorRestoresDefault",
            defaultExecutor == producer.getDefaultMQProducerImpl().getAsyncSenderExecutor());
        try {
            producer.setCallbackExecutor(null);
            result.put("callbackExecutorBeforeStart", "no-exception");
        } catch (NullPointerException exception) {
            result.put("callbackExecutorBeforeStart", exception.getClass().getSimpleName());
        } finally {
            customExecutor.shutdownNow();
        }
        return result;
    }

    private static Map<String, Object> runtimeSettings() {
        DefaultMQProducer producer = new DefaultMQProducer("producer-runtime-settings");
        Map<String, Object> result = new TreeMap<>();
        producer.setProducerGroup("producer-runtime-group");
        producer.setSendMessageWithVIPChannel(true);
        producer.setTopics(new ArrayList<>(List.of("startup-topic")));
        result.put("producerGroupAfterSetter", producer.getProducerGroup());
        result.put("sendMessageWithVIPChannelAfterSetter", producer.isSendMessageWithVIPChannel());
        result.put("topicsAfterSetter", new ArrayList<>(producer.getTopics()));
        producer.setTopics(null);
        result.put("topicsIsNullAfterSetter", producer.getTopics() == null);
        producer.setCreateTopicKey("runtime-default-topic");
        producer.setDefaultTopicQueueNums(7);
        producer.setSendMsgTimeout(6_100);
        producer.setSendMsgMaxTimeoutPerRequest(800);
        producer.setRetryTimesWhenSendFailed(3);
        producer.setRetryTimesWhenSendAsyncFailed(4);
        producer.setRetryAnotherBrokerWhenNotStoreOK(true);
        producer.setMaxMessageSize(123_456);
        result.put("createTopicKey", producer.getCreateTopicKey());
        result.put("defaultTopicQueueNums", producer.getDefaultTopicQueueNums());
        result.put("sendMsgTimeout", producer.getSendMsgTimeout());
        result.put("sendMsgMaxTimeoutPerRequest", producer.getSendMsgMaxTimeoutPerRequest());
        result.put("retryTimesWhenSendFailed", producer.getRetryTimesWhenSendFailed());
        result.put("retryTimesWhenSendAsyncFailed", producer.getRetryTimesWhenSendAsyncFailed());
        result.put("retryAnotherBrokerWhenNotStoreOK", producer.isRetryAnotherBrokerWhenNotStoreOK());
        result.put("maxMessageSize", producer.getMaxMessageSize());

        producer.setEnableBackpressureForAsyncMode(true);
        result.put("backpressureEnabledAfterToggle", producer.isEnableBackpressureForAsyncMode());

        try {
            Semaphore beforeResize = asyncSendNumSemaphore(producer);
            beforeResize.acquire(7);
            producer.setBackPressureForAsyncSendNum(12);
            result.put("sendNumPermitsAfterResize", producer.getDefaultMQProducerImpl()
                .getSemaphoreAsyncSendNumAvailablePermits());
            Semaphore afterResize = asyncSendNumSemaphore(producer);
            afterResize.release(7);
            result.put("sendNumPermitsAfterInFlightRelease", producer.getDefaultMQProducerImpl()
                .getSemaphoreAsyncSendNumAvailablePermits());
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Could not inspect pinned async-send permit adjustment", error);
        }

        producer.setBackPressureForAsyncSendNum(12);
        producer.setBackPressureForAsyncSendSize(2 * 1024 * 1024);
        result.put("sendNumAfterUpdate", producer.getBackPressureForAsyncSendNum());
        result.put("sendSizeAfterUpdate", producer.getBackPressureForAsyncSendSize());
        result.put("sendSizePermitsAfterUpdate", producer.getDefaultMQProducerImpl()
            .getSemaphoreAsyncSendSizeAvailablePermits());
        producer.setBackPressureForAsyncSendNum(1);
        producer.setBackPressureForAsyncSendSize(1);
        result.put("clampedSendNumAfterUpdate", producer.getBackPressureForAsyncSendNum());
        result.put("clampedSendSizeAfterUpdate", producer.getBackPressureForAsyncSendSize());

        Set<Integer> liveRetryCodes = producer.getRetryResponseCodes();
        liveRetryCodes.add(9001);
        result.put("setMutationVisibleThroughGetter", producer.getRetryResponseCodes().contains(9001));
        producer.addRetryResponseCode(9002);
        result.put("addMethodVisibleThroughExistingSet", liveRetryCodes.contains(9002));
        liveRetryCodes.remove(9001);
        result.put("setRemovalVisibleThroughGetter", !producer.getRetryResponseCodes().contains(9001));
        return result;
    }

    private static Map<String, Object> runtimeSetterFlows() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("runtime-setter-flow-start-group");
        producer.setTopics(new ArrayList<>(List.of("startup-topic")));
        producer.setSendMessageWithVIPChannel(true);
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        MQClientInstance client = installProducerSendFixtureRoute(
            producer, implementation, "startup-topic", "runtime-setter-flow-fixture");

        Method initTopicRoute = DefaultMQProducerImpl.class.getDeclaredMethod("initTopicRoute");
        initTopicRoute.setAccessible(true);
        initTopicRoute.invoke(implementation);

        producer.setProducerGroup("runtime-setter-flow-live-group");
        producer.setSendMessageWithVIPChannel(false);
        List<String> laterTopics = new ArrayList<>(List.of("later-topic"));
        producer.setTopics(laterTopics);
        boolean topicListIdentityAfterSetter = producer.getTopics() == laterTopics;
        laterTopics.add("caller-added-topic");

        String[] brokerAddressAtSend = {""};
        String[] producerGroupAtSend = {""};
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "runtime-setter-flow-fixture";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                brokerAddressAtSend[0] = context.getBrokerAddr();
                producerGroupAtSend[0] = context.getProducerGroup();
                try {
                    Thread.sleep(50);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
            }
        });

        String outcome;
        try {
            implementation.send(new Message("startup-topic", new byte[] {1}),
                new MessageQueue("startup-topic", "broker-a", 0), 10L);
            outcome = "success";
        } catch (Exception expected) {
            outcome = expected.getClass().getSimpleName();
        } finally {
            shutdownUnstartedClientExecutors(client);
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("producerGroupAfterSetter", producer.getProducerGroup());
        result.put("producerGroupAtSend", producerGroupAtSend[0]);
        result.put("vipChannelAfterSetter", producer.isSendMessageWithVIPChannel());
        result.put("brokerAddressAtSend", brokerAddressAtSend[0]);
        result.put("topicsAfterSetter", new ArrayList<>(producer.getTopics()));
        result.put("topicListIdentityAfterSetter", topicListIdentityAfterSetter);
        result.put("publishTopicsAfterSetter", new TreeSet<>(implementation.getPublishTopicList()));
        result.put("outcome", outcome);
        result.put("serverContacted", false);
        return result;
    }

    private static void shutdownUnstartedClientExecutors(MQClientInstance client) {
        client.getScheduledExecutorService().shutdownNow();
    }

    private static MQClientInstance installProducerSendFixtureRoute(
        DefaultMQProducer producer, DefaultMQProducerImpl implementation, String topic, String clientId)
        throws Exception {
        implementation.setServiceState(ServiceState.RUNNING);
        MQClientInstance client = new MQClientInstance(producer, 0, clientId);
        Field clientFactory = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
        clientFactory.setAccessible(true);
        clientFactory.set(implementation, client);

        Field brokerAddressTable = MQClientInstance.class.getDeclaredField("brokerAddrTable");
        brokerAddressTable.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<String, HashMap<Long, String>> addresses =
            (ConcurrentMap<String, HashMap<Long, String>>) brokerAddressTable.get(client);
        addresses.put("broker-a", new HashMap<>(Map.of(MixAll.MASTER_ID, "127.0.0.1:10911")));
        addresses.put("broker-b", new HashMap<>(Map.of(MixAll.MASTER_ID, "127.0.0.1:20911")));

        TopicPublishInfo topicInfo = new TopicPublishInfo();
        topicInfo.setMessageQueueList(List.of(
            new MessageQueue(topic, "broker-a", 0),
            new MessageQueue(topic, "broker-b", 0)));
        implementation.updateTopicPublishInfo(topic, topicInfo);
        return client;
    }

    private static Map<String, Object> perRequestTimeoutRetry() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-timeout-retry-fixture");
        producer.setSendMsgTimeout(3_000);
        List<Map<String, Object>> configuredValues = new ArrayList<>();
        for (int timeoutMs : List.of(-2, -1, 0, 125, 6_000)) {
            producer.setSendMsgMaxTimeoutPerRequest(timeoutMs);
            Map<String, Object> value = new TreeMap<>();
            value.put("input", timeoutMs);
            value.put("readback", producer.getSendMsgMaxTimeoutPerRequest());
            configuredValues.add(value);
        }
        producer.setSendMsgMaxTimeoutPerRequest(0);
        producer.setRetryTimesWhenSendFailed(1);
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        MQClientInstance client = installProducerSendFixtureRoute(
            producer, implementation, "orders", "producer-timeout-retry-fixture");

        AtomicInteger beforeAttempts = new AtomicInteger();
        AtomicInteger afterAttempts = new AtomicInteger();
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "per-request-timeout-retry-fixture";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                beforeAttempts.incrementAndGet();
                try {
                    // Ensure sendKernelImpl consumes the zero request budget before transport.
                    Thread.sleep(5);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                afterAttempts.incrementAndGet();
            }
        });

        String outcome;
        try {
            implementation.send(new Message("orders", new byte[] {1}));
            outcome = "success";
        } catch (Exception expected) {
            outcome = expected.getClass().getSimpleName();
        } finally {
            shutdownUnstartedClientExecutors(client);
        }
        Map<String, Object> result = new TreeMap<>();
        result.put("retryTimesWhenSendFailed", producer.getRetryTimesWhenSendFailed());
        result.put("sendMsgTimeout", producer.getSendMsgTimeout());
        result.put("sendMsgMaxTimeoutPerRequest", producer.getSendMsgMaxTimeoutPerRequest());
        result.put("configuredValues", configuredValues);
        result.put("beforeAttempts", beforeAttempts.get());
        result.put("afterAttempts", afterAttempts.get());
        result.put("outcome", outcome);
        return result;
    }

    private static Map<String, Object> checkForbiddenRetry() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-check-forbidden-retry-fixture");
        producer.setSendMsgTimeout(3_000);
        producer.setRetryTimesWhenSendFailed(1);
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        MQClientInstance client = installProducerSendFixtureRoute(
            producer, implementation, "orders", "producer-check-forbidden-retry-fixture");

        AtomicInteger checkCalls = new AtomicInteger();
        AtomicInteger sendBeforeCalls = new AtomicInteger();
        AtomicInteger sendAfterCalls = new AtomicInteger();
        implementation.registerCheckForbiddenHook(new CheckForbiddenHook() {
            @Override
            public String hookName() {
                return "check-forbidden-retry-fixture";
            }

            @Override
            public void checkForbidden(CheckForbiddenContext context) throws MQClientException {
                checkCalls.incrementAndGet();
                throw new MQClientException("fixture forbidden", null);
            }
        });
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "send-hook-after-forbidden-retry-fixture";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                sendBeforeCalls.incrementAndGet();
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                sendAfterCalls.incrementAndGet();
            }
        });

        String outcome;
        String finalMessage = "";
        int responseCode = -1;
        try {
            implementation.send(new Message("orders", new byte[] {1}));
            outcome = "success";
        } catch (MQClientException expected) {
            outcome = expected.getClass().getSimpleName();
            finalMessage = expected.getMessage();
            responseCode = expected.getResponseCode();
        } finally {
            shutdownUnstartedClientExecutors(client);
        }
        Map<String, Object> result = new TreeMap<>();
        result.put("retryTimesWhenSendFailed", producer.getRetryTimesWhenSendFailed());
        result.put("checkCalls", checkCalls.get());
        result.put("sendBeforeCalls", sendBeforeCalls.get());
        result.put("sendAfterCalls", sendAfterCalls.get());
        result.put("outcome", outcome);
        result.put("responseCode", responseCode);
        result.put("messageContainsAttemptCount", finalMessage.contains("Send [2] times"));
        return result;
    }

    private static Map<String, Object> missingBrokerAddressRetry() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-missing-broker-address-fixture");
        producer.setSendMsgTimeout(3_000);
        producer.setRetryTimesWhenSendFailed(1);
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        MQClientInstance client = installProducerSendFixtureRoute(
            producer, implementation, "orders", "producer-missing-broker-address-fixture");

        Field brokerAddressTable = MQClientInstance.class.getDeclaredField("brokerAddrTable");
        brokerAddressTable.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<String, HashMap<Long, String>> addresses =
            (ConcurrentMap<String, HashMap<Long, String>>) brokerAddressTable.get(client);
        addresses.clear();

        MQClientException failure = null;
        try {
            implementation.send(new Message("orders", new byte[] {1}));
        } catch (MQClientException expected) {
            failure = expected;
        } finally {
            shutdownUnstartedClientExecutors(client);
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("retryTimesWhenSendFailed", producer.getRetryTimesWhenSendFailed());
        result.put("outcome", failure == null ? "success" : failure.getClass().getSimpleName());
        result.put("responseCode", failure == null ? -1 : failure.getResponseCode());
        result.put("messageContainsAttemptCount",
            failure != null && failure.getMessage().contains("Send [2] times"));
        result.put("messageContainsBothBrokers", failure != null
            && failure.getMessage().contains("broker-a")
            && failure.getMessage().contains("broker-b"));
        result.put("causeClass", failure != null && failure.getCause() != null
            ? failure.getCause().getClass().getSimpleName() : "none");
        result.put("causeMessageContainsBrokerFailure", failure != null && failure.getCause() != null
            && failure.getCause().getMessage().contains("The broker[")
            && failure.getCause().getMessage().contains(" not exist"));
        return result;
    }

    private static Map<String, Object> missingBrokerAddressExplicit() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-missing-broker-explicit-fixture");
        producer.setSendMsgTimeout(3_000);
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        MQClientInstance client = installProducerSendFixtureRoute(
            producer, implementation, "orders", "producer-missing-broker-explicit-fixture");

        Field brokerAddressTable = MQClientInstance.class.getDeclaredField("brokerAddrTable");
        brokerAddressTable.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<String, HashMap<Long, String>> addresses =
            (ConcurrentMap<String, HashMap<Long, String>>) brokerAddressTable.get(client);
        addresses.clear();

        MQClientException failure = null;
        try {
            implementation.send(new Message("orders", new byte[] {1}),
                new MessageQueue("orders", "broker-a", 0), 3_000L);
        } catch (MQClientException expected) {
            failure = expected;
        } finally {
            shutdownUnstartedClientExecutors(client);
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("outcome", failure == null ? "success" : failure.getClass().getSimpleName());
        result.put("responseCode", failure == null ? -1 : failure.getResponseCode());
        result.put("messageContainsMissingBroker", failure != null
            && failure.getMessage().contains("The broker[broker-a] not exist"));
        result.put("causeClass", failure != null && failure.getCause() != null
            ? failure.getCause().getClass().getSimpleName() : "none");
        return result;
    }

    private static Map<String, Object> checkForbiddenDefaultAsync() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-check-forbidden-async-fixture");
        producer.setSendMsgTimeout(3_000);
        producer.setRetryTimesWhenSendAsyncFailed(3);
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        MQClientInstance client = installProducerSendFixtureRoute(
            producer, implementation, "orders", "producer-check-forbidden-async-fixture");

        AtomicInteger checkCalls = new AtomicInteger();
        AtomicInteger sendBeforeCalls = new AtomicInteger();
        AtomicInteger sendAfterCalls = new AtomicInteger();
        implementation.registerCheckForbiddenHook(new CheckForbiddenHook() {
            @Override
            public String hookName() {
                return "check-forbidden-default-async-fixture";
            }

            @Override
            public void checkForbidden(CheckForbiddenContext context) throws MQClientException {
                checkCalls.incrementAndGet();
                throw new MQClientException("fixture forbidden", null);
            }
        });
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "send-hook-after-forbidden-default-async-fixture";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                sendBeforeCalls.incrementAndGet();
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                sendAfterCalls.incrementAndGet();
            }
        });

        Throwable failure = null;
        try {
            Method sendDefaultImpl = DefaultMQProducerImpl.class.getDeclaredMethod(
                "sendDefaultImpl", Message.class, CommunicationMode.class,
                org.apache.rocketmq.client.producer.SendCallback.class, long.class);
            sendDefaultImpl.setAccessible(true);
            sendDefaultImpl.invoke(implementation, new Message("orders", new byte[] {1}),
                CommunicationMode.ASYNC, null, 3_000L);
        } catch (InvocationTargetException expected) {
            failure = expected.getCause();
        } finally {
            shutdownUnstartedClientExecutors(client);
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("retryTimesWhenSendAsyncFailed", producer.getRetryTimesWhenSendAsyncFailed());
        result.put("checkCalls", checkCalls.get());
        result.put("sendBeforeCalls", sendBeforeCalls.get());
        result.put("sendAfterCalls", sendAfterCalls.get());
        result.put("outcome", failure == null ? "success" : failure.getClass().getSimpleName());
        result.put("responseCode", failure instanceof MQClientException
            ? ((MQClientException) failure).getResponseCode() : -1);
        result.put("messageContainsAttemptCount",
            failure != null && failure.getMessage().contains("Send [1] times"));
        result.put("causeClass", failure != null && failure.getCause() != null
            ? failure.getCause().getClass().getSimpleName() : "none");
        return result;
    }

    private static Map<String, Object> checkForbiddenExplicitAsync() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-check-forbidden-explicit-async-fixture");
        producer.setSendMsgTimeout(3_000);
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        MQClientInstance client = installProducerSendFixtureRoute(
            producer, implementation, "orders", "producer-check-forbidden-explicit-async-fixture");

        AtomicInteger checkCalls = new AtomicInteger();
        AtomicInteger sendBeforeCalls = new AtomicInteger();
        AtomicInteger sendAfterCalls = new AtomicInteger();
        implementation.registerCheckForbiddenHook(new CheckForbiddenHook() {
            @Override
            public String hookName() {
                return "check-forbidden-explicit-async-fixture";
            }

            @Override
            public void checkForbidden(CheckForbiddenContext context) throws MQClientException {
                checkCalls.incrementAndGet();
                throw new MQClientException("fixture forbidden", null);
            }
        });
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "send-hook-after-forbidden-explicit-async-fixture";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                sendBeforeCalls.incrementAndGet();
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                sendAfterCalls.incrementAndGet();
            }
        });

        Throwable failure = null;
        try {
            Method sendKernelImpl = DefaultMQProducerImpl.class.getDeclaredMethod(
                "sendKernelImpl", Message.class, MessageQueue.class, CommunicationMode.class,
                org.apache.rocketmq.client.producer.SendCallback.class, TopicPublishInfo.class, long.class);
            sendKernelImpl.setAccessible(true);
            sendKernelImpl.invoke(implementation, new Message("orders", new byte[] {1}),
                new MessageQueue("orders", "broker-a", 0), CommunicationMode.ASYNC, null, null, 3_000L);
        } catch (InvocationTargetException expected) {
            failure = expected.getCause();
        } finally {
            shutdownUnstartedClientExecutors(client);
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("checkCalls", checkCalls.get());
        result.put("sendBeforeCalls", sendBeforeCalls.get());
        result.put("sendAfterCalls", sendAfterCalls.get());
        result.put("outcome", failure == null ? "success" : failure.getClass().getSimpleName());
        result.put("responseCode", failure instanceof MQClientException
            ? ((MQClientException) failure).getResponseCode() : -1);
        result.put("messageContainsAttemptCount",
            failure != null && failure.getMessage().contains("Send [1] times"));
        result.put("causeClass", failure != null && failure.getCause() != null
            ? failure.getCause().getClass().getSimpleName() : "none");
        return result;
    }

    private static Map<String, Object> checkForbiddenDeadline() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-check-forbidden-deadline-fixture");
        producer.setSendMsgTimeout(20);
        producer.setRetryTimesWhenSendFailed(1);
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        MQClientInstance client = installProducerSendFixtureRoute(
            producer, implementation, "orders", "producer-check-forbidden-deadline-fixture");

        AtomicInteger checkCalls = new AtomicInteger();
        AtomicInteger sendBeforeCalls = new AtomicInteger();
        AtomicInteger sendAfterCalls = new AtomicInteger();
        implementation.registerCheckForbiddenHook(new CheckForbiddenHook() {
            @Override
            public String hookName() {
                return "check-forbidden-deadline-fixture";
            }

            @Override
            public void checkForbidden(CheckForbiddenContext context) throws MQClientException {
                checkCalls.incrementAndGet();
                try {
                    Thread.sleep(80);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
                throw new MQClientException("fixture forbidden", null);
            }
        });
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "send-hook-after-forbidden-deadline-fixture";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                sendBeforeCalls.incrementAndGet();
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                sendAfterCalls.incrementAndGet();
            }
        });

        String outcome;
        try {
            implementation.send(new Message("orders", new byte[] {1}));
            outcome = "success";
        } catch (Exception expected) {
            outcome = expected.getClass().getSimpleName();
        } finally {
            shutdownUnstartedClientExecutors(client);
        }
        Map<String, Object> result = new TreeMap<>();
        result.put("sendMsgTimeout", producer.getSendMsgTimeout());
        result.put("retryTimesWhenSendFailed", producer.getRetryTimesWhenSendFailed());
        result.put("checkCalls", checkCalls.get());
        result.put("sendBeforeCalls", sendBeforeCalls.get());
        result.put("sendAfterCalls", sendAfterCalls.get());
        result.put("outcome", outcome);
        return result;
    }

    private static Map<String, Object> checkForbiddenContext() {
        CheckForbiddenContext nullGroup = new CheckForbiddenContext();
        nullGroup.setGroup(null);
        CheckForbiddenContext context = new CheckForbiddenContext();
        context.setNameSrvAddr("namesrv.example:9876");
        context.setGroup("producer-group");
        context.setMessage(new Message("orders", "body".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        context.setMq(new MessageQueue("orders", "broker-a", 7));
        context.setBrokerAddr("broker.example:10911");
        context.setCommunicationMode(CommunicationMode.ASYNC);
        context.setSendResult(null);
        context.setException(null);
        context.setArg("opaque-argument");
        context.setUnitMode(true);

        Map<String, Object> result = new TreeMap<>();
        result.put("nullGroupIsNull", nullGroup.getGroup() == null);
        result.put("nameSrvAddr", context.getNameSrvAddr());
        result.put("group", context.getGroup());
        result.put("messageTopic", context.getMessage().getTopic());
        result.put("messageBody", new String(context.getMessage().getBody(), java.nio.charset.StandardCharsets.UTF_8));
        result.put("queueTopic", context.getMq().getTopic());
        result.put("queueBroker", context.getMq().getBrokerName());
        result.put("queueId", context.getMq().getQueueId());
        result.put("brokerAddr", context.getBrokerAddr());
        result.put("communicationMode", context.getCommunicationMode().name());
        result.put("sendResultIsNull", context.getSendResult() == null);
        result.put("exceptionIsNull", context.getException() == null);
        result.put("arg", context.getArg());
        result.put("unitMode", context.isUnitMode());
        result.put("communicationModes", Arrays.stream(CommunicationMode.values())
            .map(Enum::name).collect(java.util.stream.Collectors.toList()));
        return result;
    }

    private static Map<String, Object> sendMessageContext() {
        SendMessageContext defaults = new SendMessageContext();
        Map<String, Object> defaultValues = new TreeMap<>();
        defaultValues.put("producerGroupIsNull", defaults.getProducerGroup() == null);
        defaultValues.put("messageIsNull", defaults.getMessage() == null);
        defaultValues.put("queueIsNull", defaults.getMq() == null);
        defaultValues.put("brokerAddrIsNull", defaults.getBrokerAddr() == null);
        defaultValues.put("bornHostIsNull", defaults.getBornHost() == null);
        defaultValues.put("communicationModeIsNull", defaults.getCommunicationMode() == null);
        defaultValues.put("sendResultIsNull", defaults.getSendResult() == null);
        defaultValues.put("exceptionIsNull", defaults.getException() == null);
        defaultValues.put("traceContextIsNull", defaults.getMqTraceContext() == null);
        defaultValues.put("propsIsNull", defaults.getProps() == null);
        defaultValues.put("producerIsNull", defaults.getProducer() == null);
        defaultValues.put("messageType", defaults.getMsgType().name());
        defaultValues.put("namespaceIsNull", defaults.getNamespace() == null);

        SendMessageContext context = new SendMessageContext();
        context.setProducerGroup("producer-group");
        context.setMessage(new Message("orders", "body".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        context.setMq(new MessageQueue("orders", "broker-a", 7));
        context.setBrokerAddr("broker.example:10911");
        context.setBornHost("192.0.2.1");
        context.setProducer(new DefaultMQProducer().getDefaultMQProducerImpl());
        context.setCommunicationMode(CommunicationMode.ASYNC);
        context.setException(new IllegalStateException("hook failure"));
        context.setMqTraceContext("trace-context");
        context.setProps(Map.of("source", "hook"));
        context.setMsgType(MessageType.Delay_Msg);
        context.setNamespace("tenant");
        Map<String, Object> values = new TreeMap<>();
        values.put("producerGroup", context.getProducerGroup());
        values.put("messageTopic", context.getMessage().getTopic());
        values.put("messageBody", new String(context.getMessage().getBody(), java.nio.charset.StandardCharsets.UTF_8));
        values.put("queueTopic", context.getMq().getTopic());
        values.put("queueBroker", context.getMq().getBrokerName());
        values.put("queueId", context.getMq().getQueueId());
        values.put("brokerAddr", context.getBrokerAddr());
        values.put("bornHost", context.getBornHost());
        values.put("communicationMode", context.getCommunicationMode().name());
        values.put("producerIsNull", context.getProducer() == null);
        values.put("exceptionClass", context.getException().getClass().getSimpleName());
        values.put("exceptionMessage", context.getException().getMessage());
        values.put("traceContext", context.getMqTraceContext());
        values.put("props", context.getProps());
        values.put("messageType", context.getMsgType().name());
        values.put("namespace", context.getNamespace());

        Map<String, Object> result = new TreeMap<>();
        result.put("defaults", defaultValues);
        result.put("values", values);
        result.put("communicationModes", Arrays.stream(CommunicationMode.values())
            .map(Enum::name).collect(java.util.stream.Collectors.toList()));
        return result;
    }

    private static List<Long> longs(long[] values) {
        List<Long> result = new ArrayList<>(values.length);
        for (long value : values) result.add(value);
        return result;
    }

    private static List<Map<String, Object>> compressionCases(DefaultMQProducer producer, int threshold) {
        try {
            Field implementationField = DefaultMQProducer.class.getDeclaredField("defaultMQProducerImpl");
            implementationField.setAccessible(true);
            Object implementation = implementationField.get(producer);
            Method compress = implementation.getClass().getDeclaredMethod("tryToCompressMessage", Message.class);
            compress.setAccessible(true);
            List<Map<String, Object>> cases = new ArrayList<>();
            for (int size : new int[] {threshold - 1, threshold, threshold + 1}) {
                Message message = new Message("orders", new byte[size]);
                Map<String, Object> row = new TreeMap<>();
                row.put("bodyLength", size);
                row.put("compressed", compress.invoke(implementation, message));
                cases.add(row);
            }
            return cases;
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("cannot invoke pinned producer compression method", error);
        }
    }

    private static Map<String, Object> runtimeCompressionSettings() {
        DefaultMQProducer producer = new DefaultMQProducer("runtime-compression-settings");
        producer.setCompressMsgBodyOverHowmuch(8);
        Map<String, Object> result = new TreeMap<>();
        result.put("threshold", producer.getCompressMsgBodyOverHowmuch());
        result.put("thresholdCases", compressionCases(producer, 8));

        producer.setCompressMsgBodyOverHowmuch(1);
        result.put("thresholdAfterUpdate", producer.getCompressMsgBodyOverHowmuch());
        result.put("level1", compressionAtCurrentLevel(producer, 1));
        result.put("level9", compressionAtCurrentLevel(producer, 9));
        return result;
    }

    private static Map<String, Object> compressionAtCurrentLevel(DefaultMQProducer producer, int level) {
        try {
            Field implementationField = DefaultMQProducer.class.getDeclaredField("defaultMQProducerImpl");
            implementationField.setAccessible(true);
            Object implementation = implementationField.get(producer);
            Method compress = implementation.getClass().getDeclaredMethod("tryToCompressMessage", Message.class);
            compress.setAccessible(true);
            byte[] body = new byte[16_384];
            Arrays.fill(body, (byte) 'x');
            Message message = new Message("orders", body);
            producer.setCompressLevel(level);
            boolean compressed = (Boolean) compress.invoke(implementation, message);
            Map<String, Object> result = new TreeMap<>();
            result.put("level", producer.getCompressLevel());
            result.put("compressed", compressed);
            result.put("compressedBodyLength", message.getBody().length);
            return result;
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("cannot invoke pinned producer compression method", error);
        }
    }

    private static Map<String, Object> faultItemBehavior() {
        LatencyFaultToleranceImpl owner = new LatencyFaultToleranceImpl(null, null);
        LatencyFaultToleranceImpl anotherOwner = new LatencyFaultToleranceImpl(null, null);
        LatencyFaultToleranceImpl.FaultItem defaults = owner.new FaultItem("broker-a");
        Map<String, Object> result = new TreeMap<>();
        result.put("defaultName", defaults.getName());
        result.put("defaultCurrentLatency", defaults.getCurrentLatency());
        result.put("defaultStartTimestamp", defaults.getStartTimestamp());
        result.put("defaultAvailable", defaults.isAvailable());
        result.put("defaultReachable", defaults.isReachable());
        result.put("defaultToString", defaults.toString());

        long before = System.currentTimeMillis();
        defaults.updateNotAvailableDuration(10_000);
        long unavailableUntil = defaults.getStartTimestamp();
        result.put("positiveDurationMovedStartForward", unavailableUntil > before);
        result.put("positiveDurationMakesUnavailable", !defaults.isAvailable());
        defaults.updateNotAvailableDuration(0);
        result.put("zeroDurationRetainsStart", defaults.getStartTimestamp() == unavailableUntil);
        defaults.updateNotAvailableDuration(-1);
        result.put("negativeDurationRetainsStart", defaults.getStartTimestamp() == unavailableUntil);
        LatencyFaultToleranceImpl.FaultItem overflowDuration = owner.new FaultItem("overflow");
        overflowDuration.updateNotAvailableDuration(Long.MAX_VALUE);
        result.put("overflowDurationRetainsStart", overflowDuration.getStartTimestamp() == 0);
        defaults.setStartTimestamp(Long.MAX_VALUE);
        defaults.updateNotAvailableDuration(1);
        result.put("shorterDurationDoesNotMoveStart", defaults.getStartTimestamp() == Long.MAX_VALUE);

        LatencyFaultToleranceImpl.FaultItem equal = owner.new FaultItem("same");
        equal.setCurrentLatency(50);
        equal.setStartTimestamp(700);
        equal.setReachable(true);
        equal.setCheckStamp(1);
        LatencyFaultToleranceImpl.FaultItem equalDifferentFlags = anotherOwner.new FaultItem("same");
        equalDifferentFlags.setCurrentLatency(50);
        equalDifferentFlags.setStartTimestamp(700);
        equalDifferentFlags.setReachable(false);
        equalDifferentFlags.setCheckStamp(Long.MAX_VALUE);
        result.put("equalAcrossOwners", equal.equals(equalDifferentFlags));
        result.put("equalHashCodes", equal.hashCode() == equalDifferentFlags.hashCode());
        result.put("equalHashCode", equal.hashCode());
        result.put("equalityIgnoresReachableAndCheckStamp", equal.equals(equalDifferentFlags));
        result.put("differentNameNotEqual",
            !equal.equals(owner.new FaultItem("different")));
        LatencyFaultToleranceImpl.FaultItem differentLatency = owner.new FaultItem("same");
        differentLatency.setCurrentLatency(51);
        differentLatency.setStartTimestamp(700);
        result.put("differentLatencyNotEqual", !equal.equals(differentLatency));
        LatencyFaultToleranceImpl.FaultItem differentStart = owner.new FaultItem("same");
        differentStart.setCurrentLatency(50);
        differentStart.setStartTimestamp(701);
        result.put("differentStartNotEqual", !equal.equals(differentStart));
        result.put("wrongTypeEquals", equal.equals("same"));
        result.put("nullEquals", equal.equals(null));
        result.put("equalToString", equal.toString());

        LatencyFaultToleranceImpl.FaultItem available = owner.new FaultItem("available");
        available.setCurrentLatency(10_000);
        available.setStartTimestamp(Long.MIN_VALUE);
        LatencyFaultToleranceImpl.FaultItem unavailable = owner.new FaultItem("unavailable");
        unavailable.setCurrentLatency(0);
        unavailable.setStartTimestamp(Long.MAX_VALUE);
        result.put("availabilityPrecedesLatency", available.compareTo(unavailable));
        result.put("unavailableFollowsAvailable", unavailable.compareTo(available));

        LatencyFaultToleranceImpl.FaultItem lowLatency = owner.new FaultItem("low-latency");
        lowLatency.setCurrentLatency(-5);
        lowLatency.setStartTimestamp(Long.MIN_VALUE);
        LatencyFaultToleranceImpl.FaultItem highLatency = owner.new FaultItem("high-latency");
        highLatency.setCurrentLatency(5);
        highLatency.setStartTimestamp(Long.MIN_VALUE);
        result.put("signedLatencyOrder", lowLatency.compareTo(highLatency));
        LatencyFaultToleranceImpl.FaultItem tieDifferentName = owner.new FaultItem("different-name");
        tieDifferentName.setCurrentLatency(5);
        tieDifferentName.setStartTimestamp(Long.MIN_VALUE);
        result.put("comparisonIgnoresName", lowLatency.compareTo(tieDifferentName));

        LatencyFaultToleranceImpl.FaultItem earlyStart = owner.new FaultItem("early");
        earlyStart.setCurrentLatency(5);
        earlyStart.setStartTimestamp(10);
        LatencyFaultToleranceImpl.FaultItem lateStart = owner.new FaultItem("late");
        lateStart.setCurrentLatency(5);
        lateStart.setStartTimestamp(20);
        result.put("startTimestampOrder", earlyStart.compareTo(lateStart));
        result.put("nullCompareFailure", caughtClass(() -> equal.compareTo(null)));
        result.put("wrongTypeComparableFailure", caughtClass(() -> ((Comparable) equal).compareTo("same")));

        LatencyFaultToleranceImpl.FaultItem nullName = owner.new FaultItem(null);
        LatencyFaultToleranceImpl.FaultItem otherNullName = anotherOwner.new FaultItem(null);
        result.put("nullName", nullName.getName());
        result.put("nullNamesCompareEqual", nullName.equals(otherNullName));
        result.put("nullNameHash", nullName.hashCode());
        result.put("unicodeNameHash", owner.new FaultItem("A\uD83D\uDE00").hashCode());
        LatencyFaultToleranceImpl.FaultItem extremeHash = owner.new FaultItem("hash");
        extremeHash.setCurrentLatency(Long.MIN_VALUE);
        extremeHash.setStartTimestamp(Long.MAX_VALUE);
        result.put("extremeLongHash", extremeHash.hashCode());
        return result;
    }

    private static String caughtClass(Check check) {
        try {
            check.run();
            return "none";
        } catch (Exception error) {
            return error.getClass().getSimpleName();
        }
    }

    private static Map<String, Object> runtimeFaultStrategy() {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("runtime-fault-strategy");
            long[] latencyMax = {10, 20};
            long[] notAvailable = {100, 300};
            producer.setLatencyMax(latencyMax);
            producer.setNotAvailableDuration(notAvailable);
            producer.setSendLatencyFaultEnable(true);
            producer.setStartDetectorEnable(true);
            long[] returnedLatencyMax = producer.getLatencyMax();
            long[] returnedNotAvailable = producer.getNotAvailableDuration();
            latencyMax[1] = 16;
            returnedLatencyMax[0] = 12;
            notAvailable[0] = 200;
            returnedNotAvailable[1] = 400;

            Field implementationField = DefaultMQProducer.class.getDeclaredField("defaultMQProducerImpl");
            implementationField.setAccessible(true);
            Object implementation = implementationField.get(producer);
            Object fault = implementation.getClass().getMethod("getMqFaultStrategy").invoke(implementation);
            Method update = fault.getClass().getMethod("updateFaultItem",
                String.class, long.class, boolean.class, boolean.class);
            update.invoke(fault, "enabled-broker", 18L, false, true);
            Field toleranceField = fault.getClass().getDeclaredField("latencyFaultTolerance");
            toleranceField.setAccessible(true);
            Object tolerance = toleranceField.get(fault);
            Method isAvailable = tolerance.getClass().getMethod("isAvailable", String.class);

            Map<String, Object> result = new TreeMap<>();
            result.put("initialLatencyMaxMillis", longs(new long[] {10, 20}));
            result.put("initialNotAvailableDurationMillis", longs(new long[] {100, 300}));
            result.put("latencyInputMutation", indexedValue(1, 16));
            result.put("latencyGetterMutation", indexedValue(0, 12));
            result.put("durationInputMutation", indexedValue(0, 200));
            result.put("durationGetterMutation", indexedValue(1, 400));
            result.put("latencyMaxMillis", longs(producer.getLatencyMax()));
            result.put("notAvailableDurationMillis", longs(producer.getNotAvailableDuration()));
            result.put("latencyArrayIdentityRetained", producer.getLatencyMax() == latencyMax);
            result.put("durationArrayIdentityRetained", producer.getNotAvailableDuration() == notAvailable);
            result.put("latencyGetterIdentityRetained", producer.getLatencyMax() == returnedLatencyMax);
            result.put("durationGetterIdentityRetained", producer.getNotAvailableDuration() == returnedNotAvailable);
            result.put("sendLatencyFaultEnableAfterToggle", producer.isSendLatencyFaultEnable());
            result.put("enabledBrokerAvailable", isAvailable.invoke(tolerance, "enabled-broker"));

            producer.setSendLatencyFaultEnable(false);
            update.invoke(fault, "disabled-broker", 15L, false, true);
            result.put("sendLatencyFaultEnableAfterDisable", producer.isSendLatencyFaultEnable());
            result.put("disabledBrokerAvailable", isAvailable.invoke(tolerance, "disabled-broker"));
            producer.setSendLatencyFaultEnable(true);
            long[] priorLatencyMax = producer.getLatencyMax();
            long[] replacementLatencyMax = {20};
            producer.setLatencyMax(replacementLatencyMax);
            priorLatencyMax[0] = 0;
            long[] priorNotAvailable = producer.getNotAvailableDuration();
            long[] replacementNotAvailable = {0};
            producer.setNotAvailableDuration(replacementNotAvailable);
            priorNotAvailable[0] = 999;
            update.invoke(fault, "updated-table-broker", 15L, false, true);
            result.put("sendLatencyFaultEnableAfterReenable", producer.isSendLatencyFaultEnable());
            result.put("latencyReplacementValues", longs(producer.getLatencyMax()));
            result.put("durationReplacementValues", longs(producer.getNotAvailableDuration()));
            result.put("latencyReplacementIdentityRetained", producer.getLatencyMax() == replacementLatencyMax);
            result.put("durationReplacementIdentityRetained", producer.getNotAvailableDuration() == replacementNotAvailable);
            result.put("updatedTableBrokerAvailable", isAvailable.invoke(tolerance, "updated-table-broker"));
            result.put("startDetectorEnableAfterToggle", producer.isStartDetectorEnable());
            producer.setStartDetectorEnable(false);
            result.put("startDetectorEnableAfterDisable", producer.isStartDetectorEnable());
            return result;
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("cannot invoke pinned producer fault strategy", error);
        }
    }

    private static Map<String, Object> runtimeFaultStrategyNullArrays() {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("runtime-fault-null-arrays");
            producer.setSendLatencyFaultEnable(true);
            Field implementationField = DefaultMQProducer.class.getDeclaredField("defaultMQProducerImpl");
            implementationField.setAccessible(true);
            Object implementation = implementationField.get(producer);
            Object fault = implementation.getClass().getMethod("getMqFaultStrategy").invoke(implementation);
            Method update = fault.getClass().getMethod("updateFaultItem",
                String.class, long.class, boolean.class, boolean.class);

            Map<String, Object> result = new TreeMap<>();
            producer.setLatencyMax(null);
            result.put("latencyMaxGetterIsNull", producer.getLatencyMax() == null);
            result.put("nullLatencyMaxUpdateFailure", invokeFaultUpdate(update, fault, 10L));

            producer.setLatencyMax(new long[] {10L});
            producer.setNotAvailableDuration(null);
            result.put("notAvailableDurationGetterIsNull", producer.getNotAvailableDuration() == null);
            result.put("unmatchedLatencyWithNullDurationFailure", invokeFaultUpdate(update, fault, 9L));
            result.put("matchedLatencyWithNullDurationFailure", invokeFaultUpdate(update, fault, 10L));

            producer.setNotAvailableDuration(new long[] {100L});
            producer.setLatencyMax(new long[] {10L, 20L});
            result.put("shortDurationTableFailure", invokeFaultUpdate(update, fault, 20L));

            producer.setLatencyMax(new long[] {20L});
            producer.setNotAvailableDuration(new long[] {100L, 200L});
            result.put("extraDurationRowsFailure", invokeFaultUpdate(update, fault, 20L));
            return result;
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("cannot invoke pinned producer null fault-table cases", error);
        }
    }

    private static String invokeFaultUpdate(Method update, Object fault, long latency) {
        try {
            update.invoke(fault, "fault-table-input", latency, false, true);
            return "none";
        } catch (InvocationTargetException error) {
            return error.getCause().getClass().getName();
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("cannot invoke pinned producer fault-table update", error);
        }
    }

    private static Map<String, Object> indexedValue(int index, long value) {
        Map<String, Object> result = new TreeMap<>();
        result.put("index", index);
        result.put("value", value);
        return result;
    }

    private static Map<String, Object> compressionProperties() {
        Map<String, Object> result = new TreeMap<>();
        String previousType = System.getProperty(MixAll.MESSAGE_COMPRESS_TYPE);
        String previousLevel = System.getProperty(MixAll.MESSAGE_COMPRESS_LEVEL);
        try {
            System.setProperty(MixAll.MESSAGE_COMPRESS_TYPE, " lz4 ");
            System.setProperty(MixAll.MESSAGE_COMPRESS_LEVEL, "7");
            DefaultMQProducer configured = new DefaultMQProducer();
            result.put("inputType", " lz4 ");
            result.put("inputLevel", "7");
            result.put("parsedType", configured.getCompressType().name());
            result.put("parsedLevel", configured.getCompressLevel());

            System.setProperty(MixAll.MESSAGE_COMPRESS_TYPE, "unsupported");
            result.put("invalidTypeAccepted", accepts(() -> new DefaultMQProducer()));
            System.setProperty(MixAll.MESSAGE_COMPRESS_TYPE, "ZLIB");
            System.setProperty(MixAll.MESSAGE_COMPRESS_LEVEL, "invalid");
            result.put("invalidLevelAccepted", accepts(() -> new DefaultMQProducer()));
        } finally {
            restoreProperty(MixAll.MESSAGE_COMPRESS_TYPE, previousType);
            restoreProperty(MixAll.MESSAGE_COMPRESS_LEVEL, previousLevel);
        }
        return result;
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }

    private static List<Map<String, Object>> batchCases() {
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(row("empty", accepts(() -> MessageBatch.generateFromList(List.of()))));
        cases.add(row("mixed-topics", accepts(() -> MessageBatch.generateFromList(List.of(
            new Message("orders", new byte[] {1}), new Message("payments", new byte[] {2}))))));
        cases.add(row("wait-store-mismatch", accepts(() -> {
            Message first = new Message("orders", new byte[] {1});
            Message second = new Message("orders", new byte[] {2});
            second.setWaitStoreMsgOK(false);
            MessageBatch.generateFromList(List.of(first, second));
        })));
        cases.add(row("delayed", accepts(() -> {
            Message message = new Message("orders", new byte[] {1});
            message.setDelayTimeLevel(1);
            MessageBatch.generateFromList(List.of(message));
        })));
        cases.add(row("retry-topic", accepts(() -> MessageBatch.generateFromList(List.of(
            new Message("%RETRY%workers", new byte[] {1}))))));
        cases.add(row("different-tags-allowed", accepts(() -> {
            Message first = new Message("orders", new byte[] {1});
            Message second = new Message("orders", new byte[] {2});
            first.setTags("one");
            second.setTags("two");
            MessageBatch.generateFromList(List.of(first, second));
        })));
        return cases;
    }

    private static Map<String, Object> batchEncoding() {
        MessageBatch batch = MessageBatch.generateFromList(List.of(
            new Message("orders", "first".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
            new Message("orders", "second".getBytes(java.nio.charset.StandardCharsets.UTF_8))));
        byte[] body = batch.encode();
        Map<String, Object> result = new TreeMap<>();
        result.put("bodyHex", toHex(body));
        result.put("length", body.length);
        return result;
    }

    private static String toHex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) result.append(String.format("%02X", value & 0xFF));
        return result.toString();
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1
            && (Boolean.parseBoolean(args[1]) || "--verify".equals(args[1]));
        String json = JSON.toJSONString(produce()) + "\n";
        if (verify) {
            String expected = Files.exists(path) ? Files.readString(path) : "";
            if (!json.equals(expected)) {
                int mismatch = 0;
                while (mismatch < json.length() && mismatch < expected.length()
                    && json.charAt(mismatch) == expected.charAt(mismatch)) {
                    mismatch++;
                }
                int start = Math.max(0, mismatch - 80);
                int expectedEnd = Math.min(expected.length(), mismatch + 160);
                int actualEnd = Math.min(json.length(), mismatch + 160);
                throw new IllegalStateException("Java producer fixture drift: " + path
                    + " at char " + mismatch + " expected=" + expected.substring(start, expectedEnd)
                    + " actual=" + json.substring(start, actualEnd));
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
    }
}
