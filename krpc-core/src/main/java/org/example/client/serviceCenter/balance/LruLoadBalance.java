package org.example.client.serviceCenter.balance;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LruLoadBalance implements LoadBalance {
    private final Map<String, Long> lastAccessMap = new ConcurrentHashMap<>();

    @Override
    public String balance(List<String> addressList) {
        if (addressList == null || addressList.isEmpty()) {
            throw new IllegalArgumentException("addressList is empty");
        }

        long now = System.currentTimeMillis();
        String chosen = null;
        long leastRecent = Long.MAX_VALUE;
        for (String address : addressList) {
            long lastAccess = lastAccessMap.getOrDefault(address, 0L);
            if (lastAccess < leastRecent) {
                leastRecent = lastAccess;
                chosen = address;
            }
        }

        if (chosen == null) {
            chosen = addressList.get(0);
        }
        lastAccessMap.put(chosen, now);
        return chosen;
    }

    @Override
    public void addNode(String node) {
        if (node != null && !node.isEmpty()) {
            lastAccessMap.putIfAbsent(node, 0L);
        }
    }

    @Override
    public void delNode(String node) {
        lastAccessMap.remove(node);
    }

    @Override
    public String toString() {
        return "LRU";
    }
}
