package org.example.client.circuitBreaker;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class CircuitBreakerProvider {
    private final Map<String, CircuitBreaker> circuitBreakerMap = new ConcurrentHashMap<>();

    public synchronized CircuitBreaker getCircuitBreaker(String serviceName) {
        return circuitBreakerMap.computeIfAbsent(serviceName, key -> {
            log.info("Create circuit breaker for service: {}", serviceName);
            return new CircuitBreaker(3, 0.6, 3, 5000);
        });
    }
}
