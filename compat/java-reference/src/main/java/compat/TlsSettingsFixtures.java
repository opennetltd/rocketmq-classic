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
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.net.ssl.SSLEngine;
import org.apache.rocketmq.remoting.netty.TlsHelper;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;

/** Invoked Java 5.5.1 TLS cipher and protocol property oracle. */
public final class TlsSettingsFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private TlsSettingsFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(render()) + "\n";
        if (verify) {
            String recorded = Files.exists(path) ? Files.readString(path) : "<missing>";
            if (!json.equals(recorded)) {
                throw new IllegalStateException(
                    "Java TLS settings fixture drift: " + path + "\nrecorded=" + recorded + "\nactual=" + json);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
    }

    private static Map<String, Object> render() throws Exception {
        boolean configFilePathSnapshot = inspectConfigFilePathSnapshot();
        String ciphers = TlsSystemConfig.tlsCiphers;
        String protocols = TlsSystemConfig.tlsProtocols;
        Path configFile = Files.createTempFile("rocketmq-tls-settings", ".properties");
        String fileCiphers = "TLS_AES_128_GCM_SHA256";
        String fileProtocols = "TLSv1.3";
        Files.writeString(configFile,
            "tls.ciphers=" + fileCiphers + "\ntls.protocols=" + fileProtocols + "\n");
        try {
            Method extract = TlsHelper.class.getDeclaredMethod("extractTlsConfigFromFile", java.io.File.class);
            extract.setAccessible(true);
            extract.invoke(null, configFile.toFile());
        } finally {
            Files.deleteIfExists(configFile);
        }

        Map<String, Object> settings = new TreeMap<>();
        settings.put("ciphers", ciphers);
        settings.put("protocols", protocols);
        Map<String, Object> effectiveSettings = new TreeMap<>();
        effectiveSettings.put("ciphers", TlsSystemConfig.tlsCiphers);
        effectiveSettings.put("protocols", TlsSystemConfig.tlsProtocols);
        Map<String, Object> fileValues = new TreeMap<>();
        fileValues.put("ciphers", fileCiphers);
        fileValues.put("protocols", fileProtocols);

        String previousConfigFile = TlsSystemConfig.tlsConfigFile;
        String previousCiphers = TlsSystemConfig.tlsCiphers;
        String previousProtocols = TlsSystemConfig.tlsProtocols;
        boolean previousTestMode = TlsSystemConfig.tlsTestModeEnable;
        String clientCipher = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256";
        String clientProtocol = "TLSv1.2";
        String[] clientEnabledCiphers;
        String[] clientEnabledProtocols;
        Map<String, Object> unsupportedClientContext = new TreeMap<>();
        Map<String, Object> unsupportedProtocolContext = new TreeMap<>();
        Path emptyConfigFile = Files.createTempFile("rocketmq-tls-client", ".properties");
        try {
            TlsSystemConfig.tlsConfigFile = emptyConfigFile.toString();
            TlsSystemConfig.tlsCiphers = clientCipher;
            TlsSystemConfig.tlsProtocols = clientProtocol;
            TlsSystemConfig.tlsTestModeEnable = true;
            SslContext clientContext = TlsHelper.buildSslContext(true);
            SSLEngine clientEngine = clientContext.newEngine(ByteBufAllocator.DEFAULT);
            clientEnabledCiphers = clientEngine.getEnabledCipherSuites();
            clientEnabledProtocols = clientEngine.getEnabledProtocols();
            clientEngine.closeOutbound();

            TlsSystemConfig.tlsCiphers = "INVALID_CLIENT_CIPHER";
            TlsSystemConfig.tlsProtocols = "INVALID_CLIENT_PROTOCOL";
            try {
                SslContext invalidContext = TlsHelper.buildSslContext(true);
                SSLEngine invalidEngine = invalidContext.newEngine(ByteBufAllocator.DEFAULT);
                unsupportedClientContext.put("outcome", "success");
                unsupportedClientContext.put("enabledCipherSuites", invalidEngine.getEnabledCipherSuites());
                unsupportedClientContext.put("enabledProtocols", invalidEngine.getEnabledProtocols());
                invalidEngine.closeOutbound();
            } catch (Throwable error) {
                unsupportedClientContext.put("outcome", "failure");
                unsupportedClientContext.put("errorClass", error.getClass().getName());
                unsupportedClientContext.put("errorMessage", error.getMessage());
            }

            TlsSystemConfig.tlsCiphers = clientCipher;
            TlsSystemConfig.tlsProtocols = "INVALID_CLIENT_PROTOCOL";
            try {
                SslContext invalidProtocolContext = TlsHelper.buildSslContext(true);
                SSLEngine invalidProtocolEngine = invalidProtocolContext.newEngine(ByteBufAllocator.DEFAULT);
                unsupportedProtocolContext.put("outcome", "success");
                unsupportedProtocolContext.put("enabledProtocols", invalidProtocolEngine.getEnabledProtocols());
                invalidProtocolEngine.closeOutbound();
            } catch (Throwable error) {
                unsupportedProtocolContext.put("outcome", "failure");
                unsupportedProtocolContext.put("errorClass", error.getClass().getName());
                unsupportedProtocolContext.put("errorMessage", error.getMessage());
            }
        } finally {
            TlsSystemConfig.tlsConfigFile = previousConfigFile;
            TlsSystemConfig.tlsCiphers = previousCiphers;
            TlsSystemConfig.tlsProtocols = previousProtocols;
            TlsSystemConfig.tlsTestModeEnable = previousTestMode;
            Files.deleteIfExists(emptyConfigFile);
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("sourceCommit", SOURCE_COMMIT);
        result.put("clientArtifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        result.put("entrypoints", new String[] {
            "TlsSystemConfig.TLS_CIPHERS/TLS_PROTOCOLS",
            "TlsSystemConfig.tlsCiphers/tlsProtocols",
            "TlsHelper.moreTlsConfig",
            "TlsHelper.extractTlsConfigFromFile",
            "TlsHelper.buildSslContext(true)",
            "TlsHelper.DecryptionStrategy.decryptPrivateKey(String, boolean)",
            "TlsHelper.registerDecryptionStrategy(DecryptionStrategy)"
        });
        result.put("decryptionStrategyApi", inspectDecryptionStrategyApi());
        result.put("propertyFileReload", inspectPropertyFileReload());
        result.put("propertyFileAcrossSources", inspectPropertyFileAcrossSources());
        result.put("configFilePathSnapshot", configFilePathSnapshot);
        Map<String, Object> propertyNames = new TreeMap<>();
        propertyNames.put("ciphers", TlsSystemConfig.TLS_CIPHERS);
        propertyNames.put("protocols", TlsSystemConfig.TLS_PROTOCOLS);
        result.put("propertyNames", propertyNames);
        result.put("settings", settings);
        result.put("configFileValues", fileValues);
        result.put("effectiveSettings", effectiveSettings);
        Map<String, Object> clientContext = new TreeMap<>();
        clientContext.put("configuredCipher", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        clientContext.put("configuredProtocol", "TLSv1.2");
        clientContext.put("enabledCipherSuites", clientEnabledCiphers);
        clientContext.put("enabledProtocols", clientEnabledProtocols);
        clientContext.put("configuredCipherEnabled", Arrays.asList(clientEnabledCiphers)
            .contains("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"));
        clientContext.put("configuredProtocolEnabled", Arrays.asList(clientEnabledProtocols)
            .equals(java.util.Collections.singletonList("TLSv1.2")));
        result.put("clientContext", clientContext);
        result.put("unsupportedClientContext", unsupportedClientContext);
        result.put("unsupportedProtocolContext", unsupportedProtocolContext);
        result.put("systemClientContext", inspectClientContext(ciphers, protocols));
        result.put("fileClientContext", inspectClientContext(
            (String) effectiveSettings.get("ciphers"),
            (String) effectiveSettings.get("protocols")));
        result.put("configFileOverridesSettings",
            !java.util.Objects.equals(ciphers, TlsSystemConfig.tlsCiphers)
                || !java.util.Objects.equals(protocols, TlsSystemConfig.tlsProtocols));
        result.put("serverContacted", false);
        return result;
    }

    private static boolean inspectConfigFilePathSnapshot() throws Exception {
        Path directory = Files.createTempDirectory("rocketmq-tls-path-snapshot");
        Path firstConfigFile = directory.resolve("first.properties");
        Path secondConfigFile = directory.resolve("second.properties");
        Files.writeString(firstConfigFile, "tls.test.mode.enable=false\n");
        Files.writeString(secondConfigFile, "tls.test.mode.enable=true\n");
        String previousConfigProperty = System.getProperty("tls.config.file");
        String initialConfigFile = null;
        boolean previousTestMode = true;
        boolean previousAuthServer = false;
        boolean initialized = false;
        try {
            System.setProperty("tls.config.file", firstConfigFile.toString());
            initialConfigFile = TlsSystemConfig.tlsConfigFile;
            previousTestMode = TlsSystemConfig.tlsTestModeEnable;
            previousAuthServer = TlsSystemConfig.tlsClientAuthServer;
            initialized = true;
            System.setProperty("tls.config.file", secondConfigFile.toString());
            TlsHelper.buildSslContext(true);
            return firstConfigFile.toString().equals(initialConfigFile)
                && initialConfigFile.equals(TlsSystemConfig.tlsConfigFile)
                && !TlsSystemConfig.tlsTestModeEnable;
        } finally {
            if (initialized) {
                TlsSystemConfig.tlsConfigFile = initialConfigFile;
                TlsSystemConfig.tlsTestModeEnable = previousTestMode;
                TlsSystemConfig.tlsClientAuthServer = previousAuthServer;
            }
            if (previousConfigProperty == null) {
                System.clearProperty("tls.config.file");
            } else {
                System.setProperty("tls.config.file", previousConfigProperty);
            }
            Files.deleteIfExists(secondConfigFile);
            Files.deleteIfExists(firstConfigFile);
            Files.deleteIfExists(directory);
        }
    }

    private static Map<String, Object> inspectPropertyFileReload() throws Exception {
        String previousConfigFile = TlsSystemConfig.tlsConfigFile;
        boolean previousTestMode = TlsSystemConfig.tlsTestModeEnable;
        boolean previousAuthServer = TlsSystemConfig.tlsClientAuthServer;
        Path configFile = Files.createTempFile("rocketmq-tls-reload", ".properties");
        List<Boolean> observedTestModes = new ArrayList<>();
        List<Boolean> observedAuthServer = new ArrayList<>();
        try {
            TlsSystemConfig.tlsConfigFile = configFile.toString();
            List<String> configSnapshots = List.of(
                "tls.test.mode.enable=true\ntls.client.authServer=false\n",
                "tls.test.mode.enable=false\ntls.client.authServer=true\n",
                "tls.test.mode.enable=true\n");
            for (String configSnapshot : configSnapshots) {
                Files.writeString(configFile, configSnapshot);
                TlsHelper.buildSslContext(true);
                observedTestModes.add(TlsSystemConfig.tlsTestModeEnable);
                observedAuthServer.add(TlsSystemConfig.tlsClientAuthServer);
            }
        } finally {
            TlsSystemConfig.tlsConfigFile = previousConfigFile;
            TlsSystemConfig.tlsTestModeEnable = previousTestMode;
            TlsSystemConfig.tlsClientAuthServer = previousAuthServer;
            Files.deleteIfExists(configFile);
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("observedTestModes", observedTestModes);
        result.put("observedAuthServer", observedAuthServer);
        result.put("reloadsOnEachContextBuild", observedTestModes.equals(Arrays.asList(true, false, true))
            && observedAuthServer.equals(Arrays.asList(false, true, true)));
        result.put("serverContacted", false);
        return result;
    }

    private static Map<String, Object> inspectPropertyFileAcrossSources() throws Exception {
        String previousConfigFile = TlsSystemConfig.tlsConfigFile;
        String previousAuthServerProperty = System.getProperty("tls.client.authServer");
        String previousTlsEnableProperty = System.getProperty("tls.enable");
        boolean previousTestMode = TlsSystemConfig.tlsTestModeEnable;
        boolean previousAuthServer = TlsSystemConfig.tlsClientAuthServer;
        boolean previousTlsEnable = TlsSystemConfig.tlsEnable;
        Path firstConfigFile = Files.createTempFile("rocketmq-tls-source-first", ".properties");
        Path secondConfigFile = Files.createTempFile("rocketmq-tls-source-second", ".properties");
        List<Boolean> observedTestModes = new ArrayList<>();
        List<Boolean> observedAuthServer = new ArrayList<>();
        List<Boolean> observedTlsEnable = new ArrayList<>();
        try {
            TlsSystemConfig.tlsEnable = false;
            TlsSystemConfig.tlsClientAuthServer = true;
            TlsSystemConfig.tlsConfigFile = firstConfigFile.toString();
            Files.writeString(firstConfigFile, "tls.test.mode.enable=false\ntls.enable=true\n");
            TlsHelper.buildSslContext(true);
            observedTestModes.add(TlsSystemConfig.tlsTestModeEnable);
            observedAuthServer.add(TlsSystemConfig.tlsClientAuthServer);
            observedTlsEnable.add(TlsSystemConfig.tlsEnable);

            System.setProperty("tls.client.authServer", "false");
            System.setProperty("tls.enable", "true");
            TlsSystemConfig.tlsConfigFile = secondConfigFile.toString();
            Files.writeString(secondConfigFile, "tls.test.mode.enable=false\ntls.enable=true\n");
            TlsHelper.buildSslContext(true);
            observedTestModes.add(TlsSystemConfig.tlsTestModeEnable);
            observedAuthServer.add(TlsSystemConfig.tlsClientAuthServer);
            observedTlsEnable.add(TlsSystemConfig.tlsEnable);
        } finally {
            TlsSystemConfig.tlsConfigFile = previousConfigFile;
            if (previousAuthServerProperty == null) {
                System.clearProperty("tls.client.authServer");
            } else {
                System.setProperty("tls.client.authServer", previousAuthServerProperty);
            }
            if (previousTlsEnableProperty == null) {
                System.clearProperty("tls.enable");
            } else {
                System.setProperty("tls.enable", previousTlsEnableProperty);
            }
            TlsSystemConfig.tlsTestModeEnable = previousTestMode;
            TlsSystemConfig.tlsClientAuthServer = previousAuthServer;
            TlsSystemConfig.tlsEnable = previousTlsEnable;
            Files.deleteIfExists(firstConfigFile);
            Files.deleteIfExists(secondConfigFile);
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("observedTestModes", observedTestModes);
        result.put("observedAuthServer", observedAuthServer);
        result.put("laterSystemPropertyAuthServer", false);
        result.put("observedTlsEnable", observedTlsEnable);
        result.put("laterSystemPropertyTlsEnable", true);
        result.put("systemPropertyTlsEnableRetained",
            observedTlsEnable.equals(Arrays.asList(false, false)));
        result.put("omittedPropertiesRetainProcessState",
            observedTestModes.equals(Arrays.asList(false, false))
                && observedAuthServer.equals(Arrays.asList(true, true)));
        result.put("serverContacted", false);
        return result;
    }

    private static Map<String, Object> inspectDecryptionStrategyApi() throws Exception {
        Class<?> strategyType = TlsHelper.DecryptionStrategy.class;
        Method decrypt = strategyType.getDeclaredMethod("decryptPrivateKey", String.class, boolean.class);
        Method register = TlsHelper.class.getDeclaredMethod("registerDecryptionStrategy", strategyType);
        Field strategyField = TlsHelper.class.getDeclaredField("decryptionStrategy");
        strategyField.setAccessible(true);
        Object previous = strategyField.get(null);

        List<Map<String, Object>> calls = new ArrayList<>();
        TlsHelper.DecryptionStrategy strategy = (path, forClient) -> {
            String key = forClient ? "client-key" : "server-key";
            Map<String, Object> call = new TreeMap<>();
            call.put("path", path);
            call.put("forClient", forClient);
            call.put("returned", key);
            calls.add(call);
            return new ByteArrayInputStream(key.getBytes(StandardCharsets.UTF_8));
        };

        boolean registered = false;
        boolean nullRetained = false;
        String nullRegistrationErrorClass = null;
        try {
            register.invoke(null, strategy);
            TlsHelper.DecryptionStrategy current = (TlsHelper.DecryptionStrategy) strategyField.get(null);
            try (InputStream client = current.decryptPrivateKey("client.key", true);
                InputStream server = current.decryptPrivateKey("server.key", false)) {
                registered = "client-key".equals(new String(client.readAllBytes(), StandardCharsets.UTF_8))
                    && "server-key".equals(new String(server.readAllBytes(), StandardCharsets.UTF_8));
            }
            register.invoke(null, new Object[] {null});
            nullRetained = strategyField.get(null) == null;

            String previousConfigFile = TlsSystemConfig.tlsConfigFile;
            String previousClientKeyPath = TlsSystemConfig.tlsClientKeyPath;
            boolean previousTestMode = TlsSystemConfig.tlsTestModeEnable;
            Path emptyConfigFile = Files.createTempFile("rocketmq-tls-null-strategy", ".properties");
            try {
                TlsSystemConfig.tlsConfigFile = emptyConfigFile.toString();
                TlsSystemConfig.tlsClientKeyPath = "missing-client.key";
                TlsSystemConfig.tlsTestModeEnable = false;
                try {
                    TlsHelper.buildSslContext(true);
                } catch (Throwable error) {
                    nullRegistrationErrorClass = error.getClass().getName();
                }
            } finally {
                TlsSystemConfig.tlsConfigFile = previousConfigFile;
                TlsSystemConfig.tlsClientKeyPath = previousClientKeyPath;
                TlsSystemConfig.tlsTestModeEnable = previousTestMode;
                Files.deleteIfExists(emptyConfigFile);
            }
        } finally {
            register.invoke(null, new Object[] {previous});
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("nestedInterfacePublic", Modifier.isPublic(strategyType.getModifiers()));
        result.put("decryptMethod", decrypt.getReturnType().getName()
            + " decryptPrivateKey(java.lang.String, boolean)");
        result.put("decryptThrowsIOException", Arrays.asList(decrypt.getExceptionTypes())
            .contains(java.io.IOException.class));
        result.put("registerPublicStaticVoid", Modifier.isPublic(register.getModifiers())
            && Modifier.isStatic(register.getModifiers()) && register.getReturnType() == void.class);
        result.put("defaultStrategyNonNull", previous != null);
        result.put("registrationAffectsStrategy", registered);
        result.put("nullRegistrationRetained", nullRetained);
        result.put("nullRegistrationErrorClass", nullRegistrationErrorClass);
        result.put("registeredCalls", calls);
        return result;
    }

    private static Map<String, Object> inspectClientContext(String ciphers, String protocols) throws Exception {
        String previousConfigFile = TlsSystemConfig.tlsConfigFile;
        String previousCiphers = TlsSystemConfig.tlsCiphers;
        String previousProtocols = TlsSystemConfig.tlsProtocols;
        boolean previousTestMode = TlsSystemConfig.tlsTestModeEnable;
        Path emptyConfigFile = Files.createTempFile("rocketmq-tls-client-settings", ".properties");
        try {
            TlsSystemConfig.tlsConfigFile = emptyConfigFile.toString();
            TlsSystemConfig.tlsCiphers = ciphers;
            TlsSystemConfig.tlsProtocols = protocols;
            TlsSystemConfig.tlsTestModeEnable = true;
            SSLEngine engine = TlsHelper.buildSslContext(true).newEngine(ByteBufAllocator.DEFAULT);
            Map<String, Object> result = new TreeMap<>();
            result.put("configuredCiphers", ciphers);
            result.put("configuredProtocols", protocols);
            result.put("enabledCipherSuites", engine.getEnabledCipherSuites());
            result.put("enabledProtocols", engine.getEnabledProtocols());
            engine.beginHandshake();
            ByteBuffer networkBytes = ByteBuffer.allocate(32768);
            engine.wrap(ByteBuffer.allocate(0), networkBytes);
            networkBytes.flip();
            byte[] clientHello = new byte[networkBytes.remaining()];
            networkBytes.get(clientHello);
            result.put("clientHello", inspectClientHello(clientHello));
            engine.closeOutbound();
            return result;
        } finally {
            TlsSystemConfig.tlsConfigFile = previousConfigFile;
            TlsSystemConfig.tlsCiphers = previousCiphers;
            TlsSystemConfig.tlsProtocols = previousProtocols;
            TlsSystemConfig.tlsTestModeEnable = previousTestMode;
            Files.deleteIfExists(emptyConfigFile);
        }
    }

    private static Map<String, Object> inspectClientHello(byte[] record) {
        if (record.length < 9 || record[0] != 22 || record[5] != 1) {
            throw new IllegalStateException("expected a TLS ClientHello record");
        }
        int offset = 9 + 2 + 32;
        int sessionIdLength = record[offset++] & 0xff;
        offset += sessionIdLength;
        int cipherBytesLength = readUnsignedShort(record, offset);
        offset += 2;
        java.util.List<Integer> ciphers = new java.util.ArrayList<>();
        for (int end = offset + cipherBytesLength; offset < end; offset += 2) {
            ciphers.add(readUnsignedShort(record, offset));
        }
        int compressionLength = record[offset++] & 0xff;
        offset += compressionLength;
        int extensionsLength = readUnsignedShort(record, offset);
        offset += 2;
        int extensionsEnd = offset + extensionsLength;
        java.util.List<Integer> protocols = new java.util.ArrayList<>();
        while (offset < extensionsEnd) {
            int extensionType = readUnsignedShort(record, offset);
            int extensionLength = readUnsignedShort(record, offset + 2);
            offset += 4;
            if (extensionType == 43) {
                int versionsOffset = offset + 1;
                int versionsLength = record[offset] & 0xff;
                for (int end = versionsOffset + versionsLength; versionsOffset < end; versionsOffset += 2) {
                    protocols.add(readUnsignedShort(record, versionsOffset));
                }
            }
            offset += extensionLength;
        }
        Map<String, Object> result = new TreeMap<>();
        result.put("cipherSuiteIds", ciphers);
        result.put("supportedVersionIds", protocols);
        return result;
    }

    private static int readUnsignedShort(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 8) | (bytes[offset + 1] & 0xff);
    }

}
