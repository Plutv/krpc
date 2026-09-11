package org.example.server.ratelimit.provider;

import lombok.extern.slf4j.Slf4j;
import org.example.server.ratelimit.RateLimit;
import org.example.server.ratelimit.TokenBucketRateLimitImpl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class RateLimitProvider {
    private static final int DEFAULT_CAPACITY = 100;
    private static final int DEFAULT_RATE = 10;

    private final Map<String, RateLimit> rateLimitMap = new ConcurrentHashMap<>();

    public RateLimit getRateLimit(String interfaceName) {
        return rateLimitMap.computeIfAbsent(interfaceName, key -> {
            log.info("Create rate limiter, interface={}, capacity={}, rate={}/s",
                    key, DEFAULT_CAPACITY, DEFAULT_RATE);
            return new TokenBucketRateLimitImpl(DEFAULT_CAPACITY, DEFAULT_RATE);
        });
    }
}
