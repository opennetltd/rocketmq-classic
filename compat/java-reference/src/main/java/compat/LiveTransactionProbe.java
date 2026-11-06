package compat;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/** Sends a committed or rolled-back transaction to the local 5.3.4 Broker. */
public final class LiveTransactionProbe {
    private LiveTransactionProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 7 && "p14".equals(args[0])) {
            p14(args);
            return;
        }
        if (args.length != 6) {
            throw new IllegalArgumentException(
                "usage: LiveTransactionProbe <namesrv> <topic> <producer-group> <commit|rollback> <key> <body>");
        }
        LocalTransactionState decision;
        if ("commit".equals(args[3])) {
            decision = LocalTransactionState.COMMIT_MESSAGE;
        } else if ("rollback".equals(args[3])) {
            decision = LocalTransactionState.ROLLBACK_MESSAGE;
        } else {
            throw new IllegalArgumentException("transaction decision must be commit or rollback");
        }

        TransactionMQProducer producer = new TransactionMQProducer(args[2]);
        producer.setNamesrvAddr(args[0]);
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object argument) {
                return decision;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt message) {
                return LocalTransactionState.UNKNOW;
            }
        });
        try {
            producer.start();
            Message message = new Message(args[1], args[5].getBytes(StandardCharsets.UTF_8));
            message.setKeys(args[4]);
            TransactionSendResult result = producer.sendMessageInTransaction(message, args[3]);
            System.out.printf("TX\t%s\t%s\t%s\t%s%n",
                result.getSendStatus().name(), result.getLocalTransactionState().name(),
                args[4], result.getMsgId());
        } finally {
            producer.shutdown();
        }
    }

    private static void p14(String[] args) throws Exception {
        LocalTransactionState decision;
        if ("commit".equals(args[4])) {
            decision = LocalTransactionState.COMMIT_MESSAGE;
        } else if ("rollback".equals(args[4])) {
            decision = LocalTransactionState.ROLLBACK_MESSAGE;
        } else if ("unknown".equals(args[4])) {
            decision = LocalTransactionState.UNKNOW;
        } else {
            throw new IllegalArgumentException("P14 decision must be commit, rollback, or unknown");
        }

        AtomicInteger checks = new AtomicInteger();
        TransactionMQProducer producer = new TransactionMQProducer(args[3]);
        producer.setNamesrvAddr(args[1]);
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object argument) {
                return decision;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt message) {
                checks.incrementAndGet();
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        try {
            producer.start();
            Message message = new Message(args[2], args[6].getBytes(StandardCharsets.UTF_8));
            message.setKeys(args[5]);
            long startedAt = System.nanoTime();
            TransactionSendResult result = producer.sendMessageInTransaction(message, args[4]);
            long checkElapsedMs = 0;
            if ("unknown".equals(args[4])) {
                long deadline = System.nanoTime() + 25_000_000_000L;
                while (checks.get() == 0 && System.nanoTime() < deadline) {
                    Thread.sleep(50);
                }
                if (checks.get() == 0) {
                    throw new IllegalStateException("Broker transaction check did not arrive within 25 seconds");
                }
                checkElapsedMs = (System.nanoTime() - startedAt) / 1_000_000L;
            }
            System.out.printf("P14\tTX\t%s\t%s\t%s\t%s\t%d\t%d%n",
                result.getSendStatus().name(), result.getLocalTransactionState().name(),
                args[5], result.getMessageQueue().getBrokerName(),
                result.getMessageQueue().getQueueId(), result.getQueueOffset());
            if ("unknown".equals(args[4])) {
                System.out.printf("P14\tCHECK\tCOMMIT\t%d\t%d%n", checks.get(), checkElapsedMs);
            }
        } finally {
            producer.shutdown();
        }
    }
}
