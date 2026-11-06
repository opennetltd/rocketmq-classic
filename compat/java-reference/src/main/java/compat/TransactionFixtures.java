package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.EndTransactionContext;
import org.apache.rocketmq.client.hook.EndTransactionHook;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;

/** Invoked Java 5.5.1 transaction producer API and source-call behavior oracle. */
public final class TransactionFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private TransactionFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(render()) + "\n";
        if (verify) {
            if (!Files.exists(path) || !json.equals(Files.readString(path))) {
                throw new IllegalStateException("Java transaction fixture drift: " + path);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
    }

    private static Map<String, Object> render() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("sourceCommit", SOURCE_COMMIT);
        provenance.put("entrypoints", List.of(
            "TransactionMQProducer",
            "DefaultMQProducerImpl.initTransactionEnv",
            "DefaultMQProducerImpl.sendMessageInTransaction",
            "DefaultMQProducerImpl.endTransaction",
            "DefaultMQProducerImpl.checkTransactionState",
            "ClientRemotingProcessor.checkTransactionState",
            "MessageDecoder.decodeMessageId",
            "RemotingCommand.makeCustomHeaderToNet"));
        provenance.put("serverContacted", false);
        result.put("provenance", provenance);

        TransactionMQProducer defaults = new TransactionMQProducer();
        Map<String, Object> defaultSettings = new TreeMap<>();
        defaultSettings.put("producerGroup", defaults.getProducerGroup());
        defaultSettings.put("checkThreadPoolMinSize", defaults.getCheckThreadPoolMinSize());
        defaultSettings.put("checkThreadPoolMaxSize", defaults.getCheckThreadPoolMaxSize());
        defaultSettings.put("checkRequestHoldMax", defaults.getCheckRequestHoldMax());
        defaultSettings.put("executorServiceAbsent", defaults.getExecutorService() == null);
        defaultSettings.put("transactionListenerAbsent", defaults.getTransactionListener() == null);
        defaultSettings.put("transactionCheckListenerAbsent", defaults.getTransactionCheckListener() == null);
        result.put("defaults", defaultSettings);

        List<String> constructors = Arrays.stream(TransactionMQProducer.class.getConstructors())
            .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
            .map(TransactionFixtures::constructorSignature)
            .sorted()
            .collect(Collectors.toList());
        result.put("constructors", constructors);

        List<String> methods = Arrays.stream(TransactionMQProducer.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(TransactionFixtures::methodSignature)
            .sorted()
            .collect(Collectors.toList());
        result.put("declaredPublicMethods", methods);

        result.put("transactionListenerMethods", Arrays.stream(TransactionListener.class.getDeclaredMethods())
            .map(TransactionFixtures::methodSignature)
            .sorted()
            .collect(Collectors.toList()));
        result.put("transactionCheckListenerMethods", Arrays.stream(TransactionCheckListener.class.getDeclaredMethods())
            .map(TransactionFixtures::methodSignature)
            .sorted()
            .collect(Collectors.toList()));
        result.put("transactionSendResultMethods", Arrays.stream(TransactionSendResult.class.getMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(TransactionFixtures::methodSignature)
            .sorted()
            .collect(Collectors.toList()));
        result.put("localTransactionStates", Arrays.stream(LocalTransactionState.values())
            .map(Enum::name)
            .collect(Collectors.toList()));

        TransactionMQProducer configured = new TransactionMQProducer("transaction-configured");
        configured.setCheckThreadPoolMinSize(2);
        configured.setCheckThreadPoolMaxSize(5);
        configured.setCheckRequestHoldMax(17);
        Map<String, Object> configuredSettings = new TreeMap<>();
        configuredSettings.put("checkThreadPoolMinSize", configured.getCheckThreadPoolMinSize());
        configuredSettings.put("checkThreadPoolMaxSize", configured.getCheckThreadPoolMaxSize());
        configuredSettings.put("checkRequestHoldMax", configured.getCheckRequestHoldMax());
        result.put("configuredSettings", configuredSettings);

        AtomicInteger localExecutions = new AtomicInteger();
        TransactionListener listener = new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object argument) {
                localExecutions.incrementAndGet();
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(org.apache.rocketmq.common.message.MessageExt message) {
                return LocalTransactionState.UNKNOW;
            }
        };
        configured.setTransactionListener(listener);
        configured.setTransactionCheckListener(message -> LocalTransactionState.ROLLBACK_MESSAGE);
        Map<String, Object> listenerSetters = new TreeMap<>();
        listenerSetters.put("transactionListenerRetained", configured.getTransactionListener() == listener);
        listenerSetters.put("legacyCheckListenerPresent", configured.getTransactionCheckListener() != null);
        result.put("listenerSetters", listenerSetters);

        Map<String, Object> transactionFlow = new TreeMap<>();
        Map<String, Object> statusPolicy = new TreeMap<>();
        statusPolicy.put("SEND_OK", "run local listener; null/error/panic => UNKNOW");
        statusPolicy.put("FLUSH_DISK_TIMEOUT", "ROLLBACK_MESSAGE");
        statusPolicy.put("FLUSH_SLAVE_TIMEOUT", "ROLLBACK_MESSAGE");
        statusPolicy.put("SLAVE_NOT_AVAILABLE", "ROLLBACK_MESSAGE");
        statusPolicy.put("other", "UNKNOW");
        transactionFlow.put("halfSendStatusPolicy", statusPolicy);
        transactionFlow.put("brokerCheckListenerPrecedence",
            "deprecated TransactionCheckListener takes precedence; otherwise TransactionListener.checkLocalTransaction");
        transactionFlow.put("repeatedChecksInvokeCallbackAgain", true);
        transactionFlow.put("shutdownExecutorWaitsForTermination", false);
        transactionFlow.put("delayedMessageProperties", List.of(
            MessageConst.PROPERTY_DELAY_TIME_LEVEL,
            MessageConst.PROPERTY_TIMER_DELAY_MS,
            MessageConst.PROPERTY_TIMER_DELAY_SEC,
            MessageConst.PROPERTY_TIMER_DELIVER_MS));
        Map<String, Object> decodedOffsets = new TreeMap<>();
        decodedOffsets.put("offsetMsgId",
            MessageDecoder.decodeMessageId("7F00000100002A9F0000000000003039").getOffset());
        decodedOffsets.put("fallbackMsgId",
            MessageDecoder.decodeMessageId("7F00000100002A9F000000000000303A").getOffset());
        transactionFlow.put("commitLogOffsetDecode", decodedOffsets);
        transactionFlow.put("initialEndTransaction", initialEndTransactionFields());
        transactionFlow.put("endTransactionHooks", endTransactionHooks());
        Map<String, Object> brokerCheckMessage = brokerCheckMessage();
        transactionFlow.put("brokerCheckMessage", brokerCheckMessage);
        transactionFlow.put("brokerCheckRequest", brokerCheckRequestFields());
        Map<String, Object> checkEndCases = new TreeMap<>();
        Map<String, Object> withUniqueKey = new TreeMap<>();
        withUniqueKey.put("commit", brokerCheckEndTransactionFields(
            "id", MessageSysFlag.TRANSACTION_COMMIT_TYPE));
        withUniqueKey.put("rollback", brokerCheckEndTransactionFields(
            "id", MessageSysFlag.TRANSACTION_ROLLBACK_TYPE));
        withUniqueKey.put("unknown", brokerCheckEndTransactionFields(
            "id", MessageSysFlag.TRANSACTION_NOT_TYPE));
        Map<String, Object> withoutUniqueKey = new TreeMap<>();
        withoutUniqueKey.put("commit", brokerCheckEndTransactionFields(
            (String) brokerCheckMessage.get("messageId"), MessageSysFlag.TRANSACTION_COMMIT_TYPE));
        withoutUniqueKey.put("rollback", brokerCheckEndTransactionFields(
            (String) brokerCheckMessage.get("messageId"), MessageSysFlag.TRANSACTION_ROLLBACK_TYPE));
        withoutUniqueKey.put("unknown", brokerCheckEndTransactionFields(
            (String) brokerCheckMessage.get("messageId"), MessageSysFlag.TRANSACTION_NOT_TYPE));
        checkEndCases.put("withUniqueKey", withUniqueKey);
        checkEndCases.put("withoutUniqueKey", withoutUniqueKey);
        transactionFlow.put("brokerCheckEndTransaction", checkEndCases);
        result.put("transactionFlow", transactionFlow);

        TransactionMQProducer missingListener = new TransactionMQProducer("transaction-missing-listener");
        Message untouched = new Message("orders", new byte[] {1});
        Map<String, Object> missingListenerOutcome = new TreeMap<>();
        try {
            missingListener.sendMessageInTransaction(untouched, null);
            missingListenerOutcome.put("throws", false);
        } catch (MQClientException exception) {
            missingListenerOutcome.put("throws", true);
            missingListenerOutcome.put("message", exception.getMessage());
        }
        missingListenerOutcome.put("topicAfter", untouched.getTopic());
        missingListenerOutcome.put("preparedPropertyAfter", untouched.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED));
        result.put("sendWithoutListener", missingListenerOutcome);

        TransactionMQProducer mutation = new TransactionMQProducer("transaction-message-mutation");
        mutation.setNamespace("tx-fixture");
        mutation.setTransactionListener(listener);
        Message message = new Message("orders", new byte[] {1});
        Map<String, Object> mutationOutcome = new TreeMap<>();
        try {
            mutation.sendMessageInTransaction(message, null);
            mutationOutcome.put("throws", false);
        } catch (MQClientException exception) {
            mutationOutcome.put("throws", true);
            mutationOutcome.put("message", exception.getMessage());
        }
        mutationOutcome.put("topicAfter", message.getTopic());
        mutationOutcome.put("preparedPropertyAfter", message.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED));
        mutationOutcome.put("producerGroupPropertyAfter", message.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP));
        mutationOutcome.put("localExecutions", localExecutions.get());
        result.put("messageBeforeTransportFailure", mutationOutcome);

        TransactionMQProducer startWithoutListener = new TransactionMQProducer("transaction-start-without-listener");
        startWithoutListener.setInstanceName("transaction-start-without-listener-fixture");
        startWithoutListener.setNamesrvAddr("127.0.0.1:1");
        boolean startSucceeded = false;
        String startError = null;
        try {
            startWithoutListener.start();
            startSucceeded = true;
        } catch (Exception exception) {
            startError = exception.getClass().getName() + ": " + exception.getMessage();
        } finally {
            startWithoutListener.shutdown();
        }
        Map<String, Object> startOutcome = new TreeMap<>();
        startOutcome.put("succeededWithoutListener", startSucceeded);
        startOutcome.put("error", startError);
        result.put("startWithoutListener", startOutcome);

        return result;
    }

    private static Map<String, Object> endTransactionHooks() {
        TransactionMQProducer producer = new TransactionMQProducer("transaction-producer");
        var implementation = producer.getDefaultMQProducerImpl();
        Map<String, Object> result = new TreeMap<>();
        result.put("initiallyPresent", implementation.hasEndTransactionHook());
        List<String> events = new ArrayList<>();
        List<Map<String, Object>> contexts = new ArrayList<>();
        Message message = new Message("orders", new byte[] {1});
        message.setTransactionId("MSG-001");
        implementation.registerEndTransactionHook(new EndTransactionHook() {
            @Override
            public String hookName() {
                return "fixture-first";
            }

            @Override
            public void endTransaction(EndTransactionContext context) {
                String phase = context.isFromTransactionCheck() ? "check" : "local";
                events.add("first:" + phase);
                MessageAccessor.putProperty(context.getMessage(), "END_HOOK_MARKER", phase);
            }
        });
        implementation.registerEndTransactionHook(new EndTransactionHook() {
            @Override
            public String hookName() {
                return "fixture-throws";
            }

            @Override
            public void endTransaction(EndTransactionContext context) {
                String phase = context.isFromTransactionCheck() ? "check" : "local";
                events.add("throws:" + phase);
                throw new IllegalStateException("fixture hook failure");
            }
        });
        implementation.registerEndTransactionHook(new EndTransactionHook() {
            @Override
            public String hookName() {
                return "fixture-last";
            }

            @Override
            public void endTransaction(EndTransactionContext context) {
                String phase = context.isFromTransactionCheck() ? "check" : "local";
                events.add("last:" + phase);
                Map<String, Object> observed = new TreeMap<>();
                observed.put("producerGroup", context.getProducerGroup());
                observed.put("topic", context.getMessage().getTopic());
                observed.put("brokerAddr", context.getBrokerAddr());
                observed.put("msgId", context.getMsgId());
                observed.put("transactionIdPresent", context.getTransactionId() != null);
                if (context.getTransactionId() != null) {
                    observed.put("transactionId", context.getTransactionId());
                }
                observed.put("transactionState", context.getTransactionState().name());
                observed.put("fromTransactionCheck", context.isFromTransactionCheck());
                observed.put("messageMutation", context.getMessage().getProperty("END_HOOK_MARKER"));
                contexts.add(observed);
            }
        });
        result.put("presentAfterRegistration", implementation.hasEndTransactionHook());
        implementation.doExecuteEndTransactionHook(message, "MSG-001", "10.0.0.1:10911",
            LocalTransactionState.COMMIT_MESSAGE, false);
        Message checkMessage = new Message("orders", new byte[] {1});
        checkMessage.setTransactionId("id");
        implementation.doExecuteEndTransactionHook(checkMessage, "id", "127.0.0.1:10911",
            LocalTransactionState.ROLLBACK_MESSAGE, true);
        Message manualMessage = new Message("orders", new byte[] {1});
        EndTransactionContext manualContext = new EndTransactionContext();
        manualContext.setProducerGroup("direct-producer");
        manualContext.setMessage(manualMessage);
        manualContext.setBrokerAddr("192.0.2.1:10911");
        manualContext.setMsgId("manual-msg");
        manualContext.setTransactionId("TX-MANUAL");
        manualContext.setTransactionState(LocalTransactionState.UNKNOW);
        manualContext.setFromTransactionCheck(true);
        implementation.executeEndTransactionHook(manualContext);
        result.put("events", events);
        result.put("contexts", contexts);
        result.put("finalMessageMarker", manualMessage.getProperty("END_HOOK_MARKER"));
        return result;
    }

    private static Map<String, Object> initialEndTransactionFields() {
        Map<String, Object> cases = new TreeMap<>();
        cases.put("commit", endTransactionFields(
            "orders", "transaction-producer", 271L, 12345L,
            MessageSysFlag.TRANSACTION_COMMIT_TYPE, false, "MSG-001", "TX-001", "broker-a"));
        cases.put("rollback", endTransactionFields(
            "orders", "transaction-producer", 271L, 12345L,
            MessageSysFlag.TRANSACTION_ROLLBACK_TYPE, false, "MSG-001", "TX-001", "broker-a"));
        cases.put("unknown", endTransactionFields(
            "orders", "transaction-producer", 271L, 12345L,
            MessageSysFlag.TRANSACTION_NOT_TYPE, false, "MSG-001", "TX-001", "broker-a"));
        return cases;
    }

    private static Map<String, Object> brokerCheckMessage() throws Exception {
        MessageExt message = brokerCheckMessageRecord();
        byte[] encoded = MessageDecoder.encode(message, false);
        String messageId = MessageDecoder.decode(ByteBuffer.wrap(encoded)).getMsgId();
        Map<String, Object> result = new TreeMap<>();
        result.put("body", Base64.getEncoder().encodeToString(encoded));
        result.put("messageId", messageId);
        return result;
    }

    private static Map<String, Object> brokerCheckRequestFields() throws Exception {
        String messageId = brokerCheckMessageId();
        CheckTransactionStateRequestHeader header = new CheckTransactionStateRequestHeader();
        header.setTopic("orders");
        header.setTranStateTableOffset(271L);
        header.setCommitLogOffset(12345L);
        header.setMsgId(messageId);
        header.setTransactionId("TX-CHECK-001");
        header.setOffsetMsgId(messageId);
        header.setNamespace("fixture-namespace");
        header.setNamespaced(true);
        header.setBrokerName("broker-a");
        header.setOneway(false);
        return serializedFields(RequestCode.CHECK_TRANSACTION_STATE, header);
    }

    private static String brokerCheckMessageId() throws Exception {
        byte[] encoded = MessageDecoder.encode(brokerCheckMessageRecord(), false);
        return MessageDecoder.decode(ByteBuffer.wrap(encoded)).getMsgId();
    }

    private static MessageExt brokerCheckMessageRecord() {
        MessageExt message = new MessageExt();
        message.setTopic("orders");
        message.setBody("half-message".getBytes(StandardCharsets.UTF_8));
        message.setBornHost(new InetSocketAddress("127.0.0.1", 10911));
        message.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
        message.setQueueId(0);
        message.setQueueOffset(271L);
        message.setCommitLogOffset(12345L);
        MessageAccessor.putProperty(message, "PGROUP", "producer-a");
        MessageAccessor.putProperty(message, "UNIQ_KEY", "id");
        return message;
    }

    private static Map<String, Object> brokerCheckEndTransactionFields(String msgId, int state) {
        return endTransactionFields(
            "orders", "transaction-producer", 271L, 12345L,
            state, true, msgId, "TX-CHECK-001", "broker-a");
    }

    private static Map<String, Object> endTransactionFields(
        String topic,
        String producerGroup,
        long transactionStateTableOffset,
        long commitLogOffset,
        int commitOrRollback,
        boolean fromTransactionCheck,
        String msgId,
        String transactionId,
        String brokerName) {
        EndTransactionRequestHeader header = new EndTransactionRequestHeader();
        header.setTopic(topic);
        header.setProducerGroup(producerGroup);
        header.setTranStateTableOffset(transactionStateTableOffset);
        header.setCommitLogOffset(commitLogOffset);
        header.setCommitOrRollback(commitOrRollback);
        header.setFromTransactionCheck(fromTransactionCheck);
        header.setMsgId(msgId);
        header.setTransactionId(transactionId);
        header.setBrokerName(brokerName);
        return serializedFields(RequestCode.END_TRANSACTION, header);
    }

    private static Map<String, Object> serializedFields(int requestCode, Object header) {
        RemotingCommand command = RemotingCommand.createRequestCommand(requestCode,
            (org.apache.rocketmq.remoting.CommandCustomHeader) header);
        command.makeCustomHeaderToNet();
        return new TreeMap<>(command.getExtFields());
    }

    private static String constructorSignature(Constructor<?> constructor) {
        return constructor.getDeclaringClass().getName() + "(" + Arrays.stream(constructor.getParameterTypes())
            .map(Class::getName)
            .collect(Collectors.joining(",")) + ")";
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getName() + " " + method.getDeclaringClass().getName() + "."
            + method.getName() + "(" + Arrays.stream(method.getParameterTypes())
            .map(Class::getName)
            .collect(Collectors.joining(",")) + ")";
    }
}
