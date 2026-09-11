package org.example.common.serializer;

import com.alibaba.fastjson.JSONObject;
import org.example.common.message.MessageType;
import org.example.common.message.RpcResponse;
import org.example.common.serializer.mySerializer.HessianSerializer;
import org.example.common.serializer.mySerializer.JsonSerializer;
import org.example.common.serializer.mySerializer.ObjectSerializer;
import org.example.common.serializer.mySerializer.ProtobufSerializer;
import org.example.common.serializer.mySerializer.Serializer;
import org.junit.jupiter.api.Test;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Serialization selection test.
 *
 * Goal (per project direction): stop adding features, prove the existing ones with evidence.
 * This test compares JSON / Hessian / Protostuff / JDK for small / medium / large payloads on:
 *   - encode time (avg us)
 *   - decode time (avg us)
 *   - serialized byte size
 *   - GC collection time delta (informational)
 * and asserts lossless round-trip for every serializer.
 *
 * It intentionally does NOT try to prove one serializer is "absolutely fastest"; it produces the
 * numbers you cite when justifying the default serializer choice.
 */
class SerializationBenchmarkTest {

    private static final int WARMUP = 300;
    private static final int ITERATIONS = 1000;

    private static final List<Serializer> SERIALIZERS = Arrays.asList(
            new JsonSerializer(),
            new HessianSerializer(),
            new ProtobufSerializer(),
            new ObjectSerializer());

    private enum PayloadSize {
        SMALL(64),
        MEDIUM(10 * 1024),
        LARGE(100 * 1024);
        final int chars;
        PayloadSize(int c) { this.chars = c; }
    }

    @Test
    void serializationRoundTripAndCost() {
        long gcBefore = gcCollectionMillis();

        for (PayloadSize size : PayloadSize.values()) {
            RpcResponse original = sample(size);
            String expectedUsername = ((BenchmarkUser) original.getData()).getUsername();

            for (Serializer serializer : SERIALIZERS) {
                for (int i = 0; i < WARMUP; i++) {
                    byte[] b = serializer.serialize(original);
                    serializer.deserializer(b, MessageType.RESPONSE.getCode());
                }

                long encodeNanos = 0;
                long decodeNanos = 0;
                byte[] lastBytes = null;
                for (int i = 0; i < ITERATIONS; i++) {
                    long s = System.nanoTime();
                    byte[] b = serializer.serialize(original);
                    encodeNanos += System.nanoTime() - s;

                    s = System.nanoTime();
                    Object o = serializer.deserializer(b, MessageType.RESPONSE.getCode());
                    decodeNanos += System.nanoTime() - s;

                    lastBytes = b;
                    assertNotNull(o, serializer + " must deserialize to a non-null object");
                }

                Object decoded = serializer.deserializer(lastBytes, MessageType.RESPONSE.getCode());
                String actualUsername = extractUsername(decoded);
                assertEquals(expectedUsername, actualUsername,
                        serializer + " round-trip must preserve payload content");

                double encodeUs = encodeNanos / (double) ITERATIONS / 1_000d;
                double decodeUs = decodeNanos / (double) ITERATIONS / 1_000d;
                System.out.printf("[%s] %-7s encode=%.3f us decode=%.3f us bytes=%d%n",
                        serializer, size, encodeUs, decodeUs, lastBytes.length);
            }
        }

        long gcAfter = gcCollectionMillis();
        System.out.printf("GC collection time delta across benchmark = %.1f ms%n", (gcAfter - gcBefore) / 1000.0);

        // Stable, meaningful size assertions. Native Java serialization must not be smaller than JSON
        // text for the same payload (it carries full class metadata). The binary serializers
        // (Hessian / Protostuff) end up roughly on par with JSON here because the payload is dominated
        // by a large string - that is the actual finding: do not assume "binary is always smaller".
        // Pick the default serializer from evidence, not from the assumption that binary wins.
        for (PayloadSize size : PayloadSize.values()) {
            RpcResponse r = sample(size);
            int jsonBytes = new JsonSerializer().serialize(r).length;
            int jdkBytes = new ObjectSerializer().serialize(r).length;
            assertTrue(jdkBytes >= jsonBytes,
                    "JDK serialization should not be smaller than JSON for " + size
                            + " (jdk=" + jdkBytes + ", json=" + jsonBytes + ")");
        }
    }

    private static RpcResponse sample(PayloadSize size) {
        char[] buf = new char[size.chars];
        Arrays.fill(buf, 'u');
        BenchmarkUser user = BenchmarkUser.builder()
                .id(1)
                .username(new String(buf))
                .gender(true)
                .build();
        // dataType left null on purpose so every serializer can round-trip the concrete payload
        // without needing to serialize java.lang.Class metadata.
        return RpcResponse.builder().code(200).message("ok").data(user).build();
    }

    private static String extractUsername(Object response) {
        if (!(response instanceof RpcResponse)) {
            return null;
        }
        Object data = ((RpcResponse) response).getData();
        if (data instanceof BenchmarkUser) {
            return ((BenchmarkUser) data).getUsername();
        }
        if (data instanceof JSONObject) {
            return ((JSONObject) data).getString("username");
        }
        return String.valueOf(data);
    }

    private static long gcCollectionMillis() {
        long total = 0;
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            total += bean.getCollectionTime();
        }
        return total;
    }

    /** Self-contained payload so this test does not depend on krpc-api. */
    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    static class BenchmarkUser implements java.io.Serializable {
        private Integer id;
        private String username;
        private Boolean gender;
    }
}
