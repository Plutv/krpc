package org.example.client.serviceCenter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

final class NodeHealthTracker {
    private final int failureThreshold;
    private final ConcurrentMap<String, AtomicInteger> failureCounts = new ConcurrentHashMap<>();

    NodeHealthTracker(int failureThreshold) {
        if (failureThreshold <= 0) {
            throw new IllegalArgumentException("failureThreshold must be positive");
        }
        this.failureThreshold = failureThreshold;
    }

    boolean recordFailure(String serviceName, String address) {
        int failures = failureCounts.computeIfAbsent(key(serviceName, address), ignored -> new AtomicInteger())
                .incrementAndGet();
        return failures >= failureThreshold;
    }

    void recordSuccess(String serviceName, String address) {
        failureCounts.remove(key(serviceName, address));
    }

    void remove(String serviceName, String address) {
        failureCounts.remove(key(serviceName, address));
    }

    private String key(String serviceName, String address) {
        return serviceName + '\0' + address;
    }
}
