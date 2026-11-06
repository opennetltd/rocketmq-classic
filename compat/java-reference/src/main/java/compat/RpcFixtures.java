package compat;

import com.alibaba.fastjson2.JSON;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.SerializeType;
import org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;

/** Offline replies built with pinned RocketMQ classes, not a simulated Broker. */
public final class RpcFixtures {
    private RpcFixtures() {
    }

    public static void generate(Path output) throws Exception {
        Files.createDirectories(output);
        SendMessageResponseHeader send = new SendMessageResponseHeader();
        send.setMsgId("offset-msg-1");
        send.setQueueId(2);
        send.setQueueOffset(99L);
        send.setTransactionId("tx-1");
        for (int code : new int[] {ResponseCode.SUCCESS, ResponseCode.FLUSH_DISK_TIMEOUT,
                ResponseCode.FLUSH_SLAVE_TIMEOUT, ResponseCode.SLAVE_NOT_AVAILABLE}) {
            write(output, "send-" + code, 10, code, send, null);
        }
        PullMessageResponseHeader pull = new PullMessageResponseHeader();
        pull.setNextBeginOffset(10L);
        pull.setMinOffset(0L);
        pull.setMaxOffset(100L);
        pull.setSuggestWhichBrokerId(1L);
        for (int code : new int[] {ResponseCode.SUCCESS, ResponseCode.PULL_NOT_FOUND,
                ResponseCode.PULL_RETRY_IMMEDIATELY, ResponseCode.PULL_OFFSET_MOVED}) {
            write(output, "pull-" + code, 11, code, pull,
                code == ResponseCode.SUCCESS ? new byte[] {1, 2, 3} : null);
        }
        QueryConsumerOffsetResponseHeader offset = new QueryConsumerOffsetResponseHeader();
        offset.setOffset(9L);
        write(output, "offset-success", 14, ResponseCode.SUCCESS, offset, null);
        write(output, "offset-not-found", 14, ResponseCode.QUERY_NOT_FOUND, null, null);
        write(output, "route-error", 105, ResponseCode.TOPIC_NOT_EXIST, null, null);
        write(output, "send-unknown", 10, 987, null, null);
        write(output, "send-malformed", 10, ResponseCode.SUCCESS, null, null);
        write(output, "pull-malformed", 11, ResponseCode.SUCCESS, null, null);
        write(output, "offset-malformed", 14, ResponseCode.SUCCESS, null, null);
    }

    public static void verify(Path frames, Path oracle) throws Exception {
        int count = 0;
        try (var files = Files.list(oracle)) {
            for (Path metadata : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".json"))::iterator) {
                Path rustPath = frames.resolve(metadata.getFileName().toString().replace(".json", ".bin"));
                if (!Files.isRegularFile(rustPath)) throw new IllegalStateException("missing Rust frame: " + rustPath);
                Map<String, Object> expected = JSON.parseObject(Files.readString(metadata));
                byte[] frame = Files.readAllBytes(rustPath);
                if (frame.length < 8 || ByteBuffer.wrap(frame).getInt() != frame.length - 4) {
                    throw new IllegalStateException("invalid Rust frame: " + rustPath);
                }
                RemotingCommand actual = RemotingCommand.decode(Arrays.copyOfRange(frame, 4, frame.length));
                if (actual.getCode() != ((Number) expected.get("response_code")).intValue()
                        || actual.isResponseType() != Boolean.TRUE.equals(expected.get("response"))
                        || !(actual.getExtFields() == null ? Map.of() : actual.getExtFields()).equals(expected.get("ext_fields"))
                        || !hex(actual.getBody() == null ? new byte[0] : actual.getBody()).equals(expected.get("body_hex"))) {
                    throw new IllegalStateException("Rust/Java semantic mismatch: " + rustPath);
                }
                count++;
            }
        }
        if (count != 15) throw new IllegalStateException("expected 15 oracle RPC frames, got " + count);
        System.out.println("Verified " + count + " Rust RPC frames with Java 5.5.1 decoder");
    }

    private static void write(Path output, String id, int requestCode, int code,
            org.apache.rocketmq.remoting.CommandCustomHeader header, byte[] body) throws Exception {
        RemotingCommand response = header == null
            ? RemotingCommand.createResponseCommand(code, "offline-remark")
            : RemotingCommand.createResponseCommandWithHeader(code, header);
        response.setLanguage(LanguageCode.JAVA);
        response.setVersion(0);
        response.setOpaque(101);
        response.setSerializeTypeCurrentRPC(SerializeType.JSON);
        response.setBody(body);
        byte[] frame = bytes(response.encode());
        Path binary = output.resolve("rpc-" + id + ".bin");
        Files.write(binary, frame);
        // Decode the encoded bytes rather than interpreting the construction object.
        RemotingCommand decoded = RemotingCommand.decode(Arrays.copyOfRange(frame, 4, frame.length));
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("java_client_version", "5.5.1");
        evidence.put("server_baseline_not_contacted", "5.3.4");
        evidence.put("scenario_id", "P5-RPC-" + id.toUpperCase());
        evidence.put("request_code", requestCode);
        evidence.put("response_code", decoded.getCode());
        evidence.put("response", decoded.isResponseType());
        evidence.put("ext_fields", decoded.getExtFields() == null ? Map.of() : decoded.getExtFields());
        evidence.put("body_hex", decoded.getBody() == null ? "" : hex(decoded.getBody()));
        evidence.put("frame_sha256", hex(MessageDigest.getInstance("SHA-256").digest(frame)));
        evidence.put("java_outcome", outcome(requestCode, decoded));
        Files.writeString(output.resolve("rpc-" + id + ".json"), JSON.toJSONString(evidence) + "\n",
            StandardCharsets.UTF_8);
    }

    // MQClientAPIImpl.processSendResponse/processPullResponse/queryConsumerOffset (5.5.1).
    // These labels are source-derived; this offline harness does not invoke MQClientAPIImpl.
    private static String outcome(int request, RemotingCommand response) {
        int code = response.getCode();
        if (request == 10) {
            if (code == ResponseCode.SUCCESS || code == ResponseCode.FLUSH_DISK_TIMEOUT
                    || code == ResponseCode.FLUSH_SLAVE_TIMEOUT || code == ResponseCode.SLAVE_NOT_AVAILABLE) {
                return response.getExtFields() != null && response.getExtFields().containsKey("msgId")
                    ? (code == 0 ? "SEND_OK" : code == ResponseCode.FLUSH_DISK_TIMEOUT ? "FLUSH_DISK_TIMEOUT"
                    : code == ResponseCode.FLUSH_SLAVE_TIMEOUT ? "FLUSH_SLAVE_TIMEOUT" : "SLAVE_NOT_AVAILABLE")
                    : "MALFORMED_SUCCESS";
            }
        } else if (request == 11) {
            if (code == ResponseCode.SUCCESS || code == ResponseCode.PULL_NOT_FOUND
                    || code == ResponseCode.PULL_RETRY_IMMEDIATELY || code == ResponseCode.PULL_OFFSET_MOVED) {
                return response.getExtFields() != null && response.getExtFields().containsKey("nextBeginOffset")
                    ? (code == 0 ? "FOUND" : code == ResponseCode.PULL_NOT_FOUND ? "NO_NEW_MSG"
                    : code == ResponseCode.PULL_RETRY_IMMEDIATELY ? "NO_MATCHED_MSG" : "OFFSET_ILLEGAL")
                    : "MALFORMED_SUCCESS";
            }
        } else if (request == 14) {
            if (code == ResponseCode.QUERY_NOT_FOUND) return "OFFSET_NOT_FOUND";
            if (code == ResponseCode.SUCCESS) return response.getExtFields() != null
                && response.getExtFields().containsKey("offset") ? "OFFSET" : "MALFORMED_SUCCESS";
        }
        return "BROKER_ERROR";
    }

    private static byte[] bytes(ByteBuffer buffer) {
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte value : bytes) result.append(String.format("%02x", value));
        return result.toString();
    }
}