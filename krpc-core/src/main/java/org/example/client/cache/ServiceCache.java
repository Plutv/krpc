package org.example.client.cache;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public class ServiceCache {
    private final ConcurrentMap<String, CopyOnWriteArrayList<String>> cache = new ConcurrentHashMap<>();

    public void addServiceToCache(String serviceName, String address) {
        if (serviceName == null || serviceName.isEmpty() || address == null || address.isEmpty()) {
            return;
        }
        CopyOnWriteArrayList<String> addresses = cache.computeIfAbsent(serviceName, key -> new CopyOnWriteArrayList<>());
        if (!addresses.contains(address)) {
            addresses.add(address);
            log.info("Add service to cache, service={}, address={}", serviceName, address);
        }
    }

    public void replaceServiceAddress(String serviceName, String oldAddress, String newAddress) {
        if (serviceName == null || serviceName.isEmpty()) {
            return;
        }
        CopyOnWriteArrayList<String> addresses = cache.computeIfAbsent(serviceName, key -> new CopyOnWriteArrayList<>());
        if (oldAddress != null && !oldAddress.isEmpty()) {
            addresses.remove(oldAddress);
        }
        if (newAddress != null && !newAddress.isEmpty() && !addresses.contains(newAddress)) {
            addresses.add(newAddress);
        }
    }

    public List<String> getServiceFromCache(String serviceName) {
        List<String> addresses = cache.get(serviceName);
        if (addresses == null || addresses.isEmpty()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(addresses);
    }

    public void setServiceToCache(String serviceName, List<String> addresses) {
        if (serviceName == null || serviceName.isEmpty()) {
            return;
        }
        CopyOnWriteArrayList<String> target = cache.computeIfAbsent(serviceName, key -> new CopyOnWriteArrayList<>());
        target.clear();
        if (addresses != null) {
            for (String address : addresses) {
                if (address != null && !address.isEmpty() && !target.contains(address)) {
                    target.add(address);
                }
            }
        }
    }

    public void delete(String serviceName, String address) {
        if (serviceName == null || serviceName.isEmpty() || address == null || address.isEmpty()) {
            return;
        }
        CopyOnWriteArrayList<String> addressList = cache.get(serviceName);
        if (addressList == null) {
            return;
        }
        boolean removed = addressList.remove(address);
        if (removed) {
            log.info("Remove service from cache, service={}, address={}", serviceName, address);
            if (addressList.isEmpty()) {
                cache.remove(serviceName);
            }
            return;
        }
        log.warn("Remove from cache ignored, service={}, address={} not found", serviceName, address);
    }

    public Set<String> serviceNames() {
        return new java.util.HashSet<>(cache.keySet());
    }
}
