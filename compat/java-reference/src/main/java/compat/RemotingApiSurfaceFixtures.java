/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package compat;

import com.alibaba.fastjson2.JSON;
import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributeType;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.netty.channel.Channel;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsManager;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.SerializeType;

/** Public source/API inventory for the Java remoting response-future capability. */
public final class RemotingApiSurfaceFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private RemotingApiSurfaceFixtures() { }

    private static final class CallbackTrace implements InvokeCallback {
        final List<String> events = new ArrayList<>();
        Throwable failure;

        @Override
        public void operationComplete(ResponseFuture responseFuture) {
            events.add("complete");
        }

        @Override
        public void operationFail(Throwable throwable) {
            failure = throwable;
            events.add("fail");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        boolean turkish = Arrays.asList(args).contains("--turkish");
        Locale originalLocale = Locale.getDefault();
        String json;
        try {
            if (turkish) Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            json = JSON.toJSONString(turkish ? renderTurkishCodeDescriptions() : render()) + "\n";
        } finally {
            if (turkish) Locale.setDefault(originalLocale);
        }
        if (verify) {
            if (!Files.exists(path)) {
                throw new IllegalStateException("Java remoting API fixture missing: " + path);
            }
            String expected = Files.readString(path);
            if (!json.equals(expected)) {
                Map<String, Object> actualFixture = JSON.parseObject(json);
                Map<String, Object> expectedFixture = JSON.parseObject(expected);
                Set<String> keys = new HashSet<>(actualFixture.keySet());
                keys.addAll(expectedFixture.keySet());
                Map<String, Boolean> changedKeys = new TreeMap<>();
                for (String key : keys) {
                    if (!java.util.Objects.equals(actualFixture.get(key), expectedFixture.get(key))) {
                        changedKeys.put(key, true);
                    }
                }
                throw new IllegalStateException("Java remoting API fixture drift: " + path
                    + "; differing keys=" + changedKeys.keySet()
                    + "; actual responseFutureToStringBehavior="
                    + actualFixture.get("responseFutureToStringBehavior"));
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
    }

    private static Map<String, Object> renderTurkishCodeDescriptions() throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("artifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", SOURCE_COMMIT);
        fixture.put("requestCodeDescriptions", codeDescriptions(RequestCode.class,
            RemotingHelper::getRequestCodeDesc));
        fixture.put("responseCodeDescriptions", codeDescriptions(ResponseCode.class,
            RemotingHelper::getResponseCodeDesc));
        fixture.put("unknownRequestCodeDescription", RemotingHelper.getRequestCodeDesc(Integer.MAX_VALUE));
        fixture.put("unknownResponseCodeDescription", RemotingHelper.getResponseCodeDesc(Integer.MIN_VALUE));
        return fixture;
    }

    private static Map<String, Object> render() throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("artifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", SOURCE_COMMIT);
        fixture.put("remotingCommandPublicConstructors", publicConstructors(RemotingCommand.class));
        fixture.put("remotingCommandPublicMethods", publicMethods(RemotingCommand.class));
        fixture.put("remotingCommandIdentityBehavior", remotingCommandIdentityBehavior());
        fixture.put("remotingCommandJsonHeaderInputs", remotingCommandJsonHeaderInputs());
        fixture.put("processTimerIdentityBehavior", processTimerIdentityBehavior());
        fixture.put("remotingHeaderEncodingBehavior", remotingHeaderEncodingBehavior());
        fixture.put("responseFuturePublicConstructors", publicConstructors(ResponseFuture.class));
        fixture.put("responseFuturePublicMethods", publicMethods(ResponseFuture.class));
        fixture.put("semaphoreReleaseOnlyOncePublicConstructors",
            publicConstructors(SemaphoreReleaseOnlyOnce.class));
        fixture.put("semaphoreReleaseOnlyOncePublicMethods", publicMethods(SemaphoreReleaseOnlyOnce.class));
        fixture.put("semaphoreReleaseBehavior", semaphoreReleaseBehavior());
        fixture.put("responseFuturePermitReleaseBehavior", responseFuturePermitReleaseBehavior());
        fixture.put("responseFutureToStringBehavior", responseFutureToStringBehavior());
        fixture.put("responseFutureWaitResponseInterruptionBehavior",
            responseFutureWaitResponseInterruptionBehavior());
        fixture.put("responseFutureCallbackFailureBehavior", responseFutureCallbackFailureBehavior());
        fixture.put("remotingMetricsConstantFields", publicFields(RemotingMetricsConstant.class));
        fixture.put("attributeKeyPublicMethods", publicMethods(AttributeKey.class));
        fixture.put("attributesPublicMethods", publicMethods(Attributes.class));
        fixture.put("attributesBuilderPublicMethods", publicMethods(AttributesBuilder.class));
        fixture.put("remotingMetricsManagerPublicConstructors",
            publicConstructors(RemotingMetricsManager.class));
        fixture.put("remotingMetricsManagerPublicFields", publicFields(RemotingMetricsManager.class));
        fixture.put("remotingMetricsManagerPublicMethods", publicMethods(RemotingMetricsManager.class));
        fixture.put("remotingMetricsHostMethods", publicMethods(NettyRemotingAbstract.class).stream()
            .filter(method -> method.contains("getRemotingMetricsManager(")
                || method.contains("setRemotingMetricsManager("))
            .collect(Collectors.toList()));
        fixture.put("requestCodeDescriptions", codeDescriptions(RequestCode.class,
            RemotingHelper::getRequestCodeDesc));
        fixture.put("responseCodeDescriptions", codeDescriptions(ResponseCode.class,
            RemotingHelper::getResponseCodeDesc));
        fixture.put("unknownRequestCodeDescription", RemotingHelper.getRequestCodeDesc(Integer.MAX_VALUE));
        fixture.put("unknownResponseCodeDescription", RemotingHelper.getResponseCodeDesc(Integer.MIN_VALUE));
        fixture.put("remotingMetricsBehavior", remotingMetricsBehavior());
        fixture.put("attributesBuilderBehavior", attributesBuilderBehavior());
        return fixture;
    }

    private static List<Map<String, Object>> remotingCommandJsonHeaderInputs() throws Exception {
        String[][] inputs = {
            {"eof-trailing-text", "{\"code\":34,\"opaque\":17,\"remark\":\"before\"}\u001atrailing"},
            {"eof-trailing-json", "{\"code\":34,\"opaque\":17,\"remark\":\"before\"}\u001a{\"ignored\":true}"},
            {"single-quoted-trailing-comma", "{'code':34,'opaque':17,'remark':'single',}"}
        };
        List<Map<String, Object>> cases = new ArrayList<>();
        for (String[] input : inputs) {
            Map<String, Object> item = new TreeMap<>();
            item.put("name", input[0]);
            item.put("input", input[1]);
            try {
                byte[] header = input[1].getBytes(java.nio.charset.StandardCharsets.UTF_8);
                java.nio.ByteBuffer encoded = java.nio.ByteBuffer.allocate(header.length + 4);
                encoded.putInt(RemotingCommand.markProtocolType(header.length, SerializeType.JSON));
                encoded.put(header);
                RemotingCommand command = RemotingCommand.decode(encoded.array());
                item.put("outcome", "returned");
                item.put("code", command.getCode());
                item.put("opaque", command.getOpaque());
                item.put("remark", command.getRemark());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("errorMessage", error.getMessage());
            }
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> remotingCommandIdentityBehavior() throws NoSuchMethodException {
        RemotingCommand first = RemotingCommand.createRequestCommand(17, null);
        RemotingCommand second = RemotingCommand.createRequestCommand(17, null);
        second.setOpaque(first.getOpaque());

        Set<RemotingCommand> commands = new HashSet<>();
        commands.add(first);
        commands.add(second);

        Map<String, Object> result = new TreeMap<>();
        result.put("equalsDeclaringClass", RemotingCommand.class.getMethod("equals", Object.class)
            .getDeclaringClass().getName());
        result.put("hashCodeDeclaringClass", RemotingCommand.class.getMethod("hashCode")
            .getDeclaringClass().getName());
        result.put("equalVisibleState", first.toString().equals(second.toString()));
        result.put("sameInstanceEquals", first.equals(first));
        result.put("distinctEqualStateEquals", first.equals(second));
        result.put("sameInstanceHashStable", first.hashCode() == first.hashCode());
        result.put("distinctEqualStateHashSetSize", commands.size());
        return result;
    }

    private static Map<String, Object> processTimerIdentityBehavior() throws NoSuchMethodException {
        Stopwatch first = Stopwatch.createUnstarted();
        Stopwatch second = Stopwatch.createUnstarted();
        RemotingCommand command = RemotingCommand.createRequestCommand(18, null);
        command.setProcessTimer(first);

        Set<Stopwatch> timers = new HashSet<>();
        timers.add(first);
        timers.add(second);

        Map<String, Object> result = new TreeMap<>();
        result.put("equalsDeclaringClass", Stopwatch.class.getMethod("equals", Object.class)
            .getDeclaringClass().getName());
        result.put("hashCodeDeclaringClass", Stopwatch.class.getMethod("hashCode")
            .getDeclaringClass().getName());
        result.put("equalStoppedState", !first.isRunning() && !second.isRunning()
            && first.elapsed().equals(second.elapsed()));
        result.put("processTimerGetterReturnsSameObject", command.getProcessTimer() == first);
        result.put("sameInstanceEquals", first.equals(first));
        result.put("distinctEqualStateEquals", first.equals(second));
        result.put("distinctEqualStateHashSetSize", timers.size());
        return result;
    }

    private static Map<String, Object> remotingHeaderEncodingBehavior() {
        Map<String, Object> result = new TreeMap<>();
        for (SerializeType serialization : SerializeType.values()) {
            RemotingCommand command = headerEncodingCommand(serialization);
            List<Map<String, Object>> bodyLengths = new ArrayList<>();
            for (int bodyLength : new int[] {-1, 0, 1, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
                Map<String, Object> item = new TreeMap<>();
                item.put("bodyLength", bodyLength);
                item.put("hex", hex(command.encodeHeader(bodyLength).array()));
                bodyLengths.add(item);
            }

            Map<String, Object> codec = new TreeMap<>();
            codec.put("bodyLengths", bodyLengths);
            codec.put("noArgumentHex", hex(command.encodeHeader().array()));

            ByteBuf output = Unpooled.buffer();
            output.writeBytes(new byte[] {(byte) 0xa1, (byte) 0xb2});
            int appendStart = output.writerIndex();
            command.fastEncodeHeader(output);
            Map<String, Object> append = new TreeMap<>();
            append.put("initialHex", "a1b2");
            append.put("prefixPreserved", output.getByte(0) == (byte) 0xa1
                && output.getByte(1) == (byte) 0xb2);
            append.put("suffixHex", ByteBufUtil.hexDump(output, appendStart,
                output.writerIndex() - appendStart));
            append.put("writerIndex", output.writerIndex());
            codec.put("fastAppend", append);
            output.release();
            result.put(serialization.name(), codec);
        }
        return result;
    }

    private static RemotingCommand headerEncodingCommand(SerializeType serialization) {
        RemotingCommand command = RemotingCommand.createRequestCommand(17, null);
        command.setVersion(1);
        command.setOpaque(42);
        command.setFlag(2);
        command.setRemark("header");
        command.setBody(new byte[] {1, 2, 3});
        command.setSerializeTypeCurrentRPC(serialization);
        return command;
    }

    private static String hex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] result = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int value = bytes[i] & 0xff;
            result[i * 2] = digits[value >>> 4];
            result[i * 2 + 1] = digits[value & 0x0f];
        }
        return new String(result);
    }

    private static List<String> publicFields(Class<?> type) {
        return Arrays.stream(type.getDeclaredFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()))
            .map(field -> field.getType().getName() + " " + field.getName())
            .sorted()
            .collect(Collectors.toList());
    }

    private static List<Map<String, Object>> codeDescriptions(Class<?> type, IntFunction<String> describe)
        throws IllegalAccessException {
        List<Map<String, Object>> cases = new ArrayList<>();
        Set<Integer> seenCodes = new java.util.HashSet<>();
        for (java.lang.reflect.Field field : type.getFields()) {
            if (field.getType() != int.class) continue;
            int code = field.getInt(null);
            if (!seenCodes.add(code)) {
                throw new IllegalStateException("duplicate code in " + type.getName() + ": " + code);
            }
            Map<String, Object> testCase = new TreeMap<>();
            testCase.put("code", code);
            testCase.put("field", field.getName());
            testCase.put("description", describe.apply(code));
            cases.add(testCase);
        }
        cases.sort((left, right) -> Integer.compare((Integer) left.get("code"), (Integer) right.get("code")));
        return cases;
    }

    private static Map<String, Object> remotingMetricsBehavior() {
        RemotingMetricsManager manager = new RemotingMetricsManager();
        Map<String, Object> result = new TreeMap<>();
        result.put("defaultAttributesEmpty", manager.newAttributesBuilder().build().asMap().isEmpty());
        result.put("defaultHistogramClass", manager.getRpcLatency().getClass().getName());

        manager.setAttributesBuilderSupplier(() -> Attributes.builder().put("base", "value"));
        Attributes attributes = manager.newAttributesBuilder().build();
        result.put("suppliedBaseAttribute", attributes.get(AttributeKey.stringKey("base")));
        result.put("suppliedProtocolType", attributes.get(AttributeKey.stringKey(
            RemotingMetricsConstant.LABEL_PROTOCOL_TYPE)));
        result.put("supplierGetterPresent", manager.getAttributesBuilderSupplier() != null);

        Promise<Object> success = new DefaultPromise<>(ImmediateEventExecutor.INSTANCE);
        success.setSuccess(new Object());
        Promise<Object> cancelled = new DefaultPromise<>(ImmediateEventExecutor.INSTANCE);
        cancelled.cancel(false);
        Promise<Object> failed = new DefaultPromise<>(ImmediateEventExecutor.INSTANCE);
        failed.setFailure(new IllegalStateException("fixture failure"));
        result.put("writeResults", Arrays.asList(
            manager.getWriteAndFlushResult(success),
            manager.getWriteAndFlushResult(cancelled),
            manager.getWriteAndFlushResult(failed)));

        result.put("metricsViewCount", manager.getMetricsView().size());
        result.put("metricsViewBoundariesMilliseconds",
            Arrays.asList(1, 3, 5, 7, 10, 100, 1000, 2000, 3000));
        return result;
    }

    private static Map<String, Object> attributesBuilderBehavior() {
        AttributesBuilder values = Attributes.builder()
            .put("string", "value")
            .put("boolean", true)
            .put("long", 42L)
            .put("double", 1.25d)
            .put("string_array", new String[] {"first", "second"})
            .put("boolean_array", new boolean[] {true, false})
            .put("long_array", new long[] {3, 4})
            .put("double_array", new double[] {2.5d, 3.5d})
            .put(AttributeKey.longKey("long_from_int"), 7);

        AttributesBuilder operations = Attributes.builder()
            .put("removed", "remove")
            .put("filtered", 9L)
            .put("type_mismatch", "keep")
            .putAll(Attributes.builder().put("copied", "put_all").build())
            .remove(AttributeKey.stringKey("removed"))
            .remove(AttributeKey.longKey("type_mismatch"))
            .removeIf(key -> key.getKey().equals("filtered")
                && key.getType() == AttributeType.LONG);

        Attributes builtValues = values.build();
        AttributesBuilder toBuilder = builtValues.toBuilder().put("from_to_builder", "copied");
        Map<String, Object> typedLookup = new TreeMap<>();
        typedLookup.put("double", builtValues.get(AttributeKey.doubleKey("double")));
        typedLookup.put("wrongTypeReturnsNull",
            builtValues.get(AttributeKey.stringKey("double")) == null);
        Map<String, Object> result = new TreeMap<>();
        result.put("values", attributeValues(builtValues));
        result.put("typedLookup", typedLookup);
        result.put("afterToBuilder", attributeValues(toBuilder.build()));
        result.put("afterBuilderOperations", attributeValues(operations.build()));
        return result;
    }

    private static Map<String, Object> attributeValues(Attributes attributes) {
        Map<String, Object> values = new TreeMap<>();
        attributes.asMap().forEach((key, value) -> {
            Map<String, Object> entry = new TreeMap<>();
            entry.put("type", key.getType().name());
            entry.put("value", value);
            values.put(key.getKey(), entry);
        });
        return values;
    }

    private static List<String> publicConstructors(Class<?> type) {
        return Arrays.stream(type.getDeclaredConstructors())
            .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
            .map(RemotingApiSurfaceFixtures::constructorSignature)
            .sorted()
            .collect(Collectors.toList());
    }

    private static List<String> publicMethods(Class<?> type) {
        return Arrays.stream(type.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(RemotingApiSurfaceFixtures::methodSignature)
            .sorted()
            .collect(Collectors.toList());
    }

    private static String constructorSignature(Constructor<?> constructor) {
        return constructor.getDeclaringClass().getName() + "(" + parameterTypes(constructor.getParameterTypes()) + ")";
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getName() + " " + method.getName() + "(" +
            parameterTypes(method.getParameterTypes()) + ")";
    }

    private static String parameterTypes(Class<?>[] types) {
        return String.join(",", Arrays.stream(types).map(Class::getName).collect(Collectors.toList()));
    }

    private static Map<String, Object> semaphoreReleaseBehavior() {
        Semaphore semaphore = new Semaphore(0);
        SemaphoreReleaseOnlyOnce releaseOnce = new SemaphoreReleaseOnlyOnce(semaphore);
        releaseOnce.release();
        releaseOnce.release();

        SemaphoreReleaseOnlyOnce noSemaphore = new SemaphoreReleaseOnlyOnce(null);
        noSemaphore.release();
        noSemaphore.release();
        Map<String, Object> result = new TreeMap<>();
        result.put("returnsSameSemaphore", releaseOnce.getSemaphore() == semaphore);
        result.put("permitsAfterRepeatedRelease", semaphore.availablePermits());
        result.put("nullSemaphoreReturnedAsNull", noSemaphore.getSemaphore() == null);
        result.put("nullSemaphoreReleaseIsNoOp", true);
        return result;
    }

    private static Map<String, Object> responseFuturePermitReleaseBehavior() {
        Semaphore semaphore = new Semaphore(0);
        ResponseFuture future = new ResponseFuture(null, 42, 1000, null,
            new SemaphoreReleaseOnlyOnce(semaphore));
        future.release();
        future.release();
        Map<String, Object> result = new TreeMap<>();
        result.put("opaque", future.getOpaque());
        result.put("permitsAfterRepeatedRelease", semaphore.availablePermits());
        return result;
    }

    private static Map<String, Object> responseFutureCallbackFailureBehavior() {
        Map<String, Object> result = new TreeMap<>();
        Throwable cause = new IllegalStateException("fixture-cause");

        CallbackTrace sendCallback = new CallbackTrace();
        ResponseFuture sendFuture = new ResponseFuture(remoteChannel(), 51,
            RemotingCommand.createRequestCommand(1, null),
            1000, sendCallback, null);
        sendFuture.setCause(cause);
        sendFuture.setSendRequestOK(false);
        sendFuture.executeInvokeCallback();
        result.put("sendFailure", callbackResult(sendCallback, cause));

        CallbackTrace timeoutCallback = new CallbackTrace();
        ResponseFuture timeoutFuture = new ResponseFuture(remoteChannel(), 52,
            RemotingCommand.createRequestCommand(1, null),
            -1, timeoutCallback, null);
        timeoutFuture.setCause(cause);
        timeoutFuture.executeInvokeCallback();
        result.put("timeoutFailure", callbackResult(timeoutCallback, cause));

        CallbackTrace genericCallback = new CallbackTrace();
        ResponseFuture genericFuture = new ResponseFuture(remoteChannel(), 53,
            RemotingCommand.createRequestCommand(1, null),
            Long.MAX_VALUE, genericCallback, null);
        genericFuture.setCause(cause);
        genericFuture.executeInvokeCallback();
        result.put("genericFailure", callbackResult(genericCallback, cause));

        CallbackTrace nullRequestCallback = new CallbackTrace();
        ResponseFuture nullRequestFuture = new ResponseFuture(remoteChannel(), 54, Long.MAX_VALUE,
            nullRequestCallback, null);
        nullRequestFuture.setCause(cause);
        String thrown = null;
        try {
            nullRequestFuture.executeInvokeCallback();
        } catch (Throwable error) {
            thrown = error.getClass().getName();
        }
        Map<String, Object> nullRequest = new TreeMap<>();
        nullRequest.put("throwable", thrown);
        nullRequest.put("callbackEvents", nullRequestCallback.events);
        result.put("nullRequestGenericFailure", nullRequest);
        return result;
    }

    private static Map<String, Object> responseFutureToStringBehavior() {
        ResponseFuture future = new ResponseFuture(null, 42, 1000, null, null);
        String value = future.toString();
        Map<String, Object> result = new TreeMap<>();
        result.put("responseCommandNull", value.contains("responseCommand=null"));
        result.put("causeNull", value.contains("cause=null"));
        result.put("callbackNull", value.contains("invokeCallback=null"));
        result.put("opaque", extractLong(value, "opaque="));
        result.put("timeoutMillis", extractLong(value, "timeoutMillis="));
        result.put("beginTimestampPresent", extractLong(value, "beginTimestamp=") > 0);
        result.put("latchCount", extractLong(value.substring(value.lastIndexOf("countDownLatch=")), "Count = "));
        RemotingCommand response = RemotingCommand.createResponseCommand(206, "fixture");
        response.setOpaque(42);
        response.setVersion(0);
        response.setSerializeTypeCurrentRPC(SerializeType.JSON);
        future.setResponseCommand(response);
        result.put("responseCommandRendering", response.toString());
        result.put("responseCommandEmbedded", future.toString().contains("responseCommand=" + response));
        future.putResponse(RemotingCommand.createResponseCommand(0, null));
        String completed = future.toString();
        result.put("latchCountAfterResponse",
            extractLong(completed.substring(completed.lastIndexOf("countDownLatch=")), "Count = "));
        return result;
    }

    private static long extractLong(String value, String key) {
        Matcher matcher = Pattern.compile(Pattern.quote(key) + "(-?[0-9]+)").matcher(value);
        if (!matcher.find()) throw new IllegalStateException("missing " + key + " in " + value);
        return Long.parseLong(matcher.group(1));
    }

    private static Map<String, Object> responseFutureWaitResponseInterruptionBehavior() throws Exception {
        ResponseFuture future = new ResponseFuture(null, 55, 5000, null, null);
        CompletableFuture<Map<String, Object>> result = new CompletableFuture<>();
        Thread waiter = new Thread(() -> {
            Map<String, Object> outcome = new TreeMap<>();
            try {
                outcome.put("returnedResponseNull", future.waitResponse(5000) == null);
                outcome.put("exceptionClass", null);
            } catch (InterruptedException error) {
                outcome.put("returnedResponseNull", true);
                outcome.put("exceptionClass", error.getClass().getName());
                outcome.put("interruptFlagCleared", !Thread.currentThread().isInterrupted());
            }
            result.complete(outcome);
        }, "response-future-interrupt-fixture");
        waiter.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (waiter.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
            Thread.yield();
        }
        if (waiter.getState() != Thread.State.TIMED_WAITING) {
            waiter.interrupt();
            waiter.join(3000);
            throw new IllegalStateException("waitResponse fixture thread did not enter timed wait");
        }
        waiter.interrupt();
        waiter.join(3000);
        if (waiter.isAlive()) throw new IllegalStateException("interrupted waitResponse did not return");
        return result.get(1, TimeUnit.SECONDS);
    }

    private static Map<String, Object> callbackResult(CallbackTrace callback, Throwable cause) {
        Map<String, Object> result = new TreeMap<>();
        result.put("failureClass", callback.failure == null ? null : callback.failure.getClass().getName());
        result.put("failureCauseSameInstance", callback.failure != null && callback.failure.getCause() == cause);
        result.put("callbackEvents", callback.events);
        return result;
    }

    private static Channel remoteChannel() {
        return (Channel) java.lang.reflect.Proxy.newProxyInstance(
            Channel.class.getClassLoader(),
            new Class<?>[] {Channel.class},
            (proxy, method, args) -> {
                if ("remoteAddress".equals(method.getName())) {
                    return new InetSocketAddress("127.0.0.1", 10911);
                }
                Class<?> returnType = method.getReturnType();
                if (returnType == boolean.class) return false;
                if (returnType == int.class) return 0;
                if (returnType == long.class) return 0L;
                return null;
            });
    }
}
