package org.example.trace;

import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

@Slf4j
public class TraceIdGenerator {

    public static final SnowflakeIdGenerator SNOWFLAKE_ID_GENERATOR = new SnowflakeIdGenerator(0L);

    public static String generateTraceId() {
        return Long.toHexString(SNOWFLAKE_ID_GENERATOR.nextId());
    }

    public static String generateTraceIdUUID() {
        UUID uuid = UUID.randomUUID();
        String uuidString = uuid.toString();
        return uuidString.replace("-", "");
    }

    public static String generateSpanId() {
        return Long.toHexString(SNOWFLAKE_ID_GENERATOR.nextId());
    }

    static class SnowflakeIdGenerator {
        private final long workerId;

        private final long epoch = 1609459200000L; // January 1, 2021

        private long sequence = 0L;

        private long lastTimeStamp = -1L;

        public SnowflakeIdGenerator(long workerId) {
            if (workerId < 0 || workerId > 1023) {
                throw new IllegalArgumentException("workerId必须在0-1023之间！");
            }
            this.workerId = workerId;
        }

        public synchronized long nextId() {
            long current = System.currentTimeMillis();

            if (current < lastTimeStamp) {
                throw new RuntimeException("时钟回拨！");
            } else if (current == lastTimeStamp) {
                sequence = (sequence + 1) & 0xFFF;
                if (sequence == 0) {
                    current = waitNextMillis(lastTimeStamp); // 等待下一毫秒
                }
            } else {
                sequence = 0L;
            }
            lastTimeStamp = current;
            return ((current - epoch) << 22) | (workerId << 12) | sequence;
        }

        private long waitNextMillis(long lastTimeStamp) {
            long current = System.currentTimeMillis();
            while (current < lastTimeStamp) {
                current = System.currentTimeMillis();
            }
            return current;
        }
    }
}
