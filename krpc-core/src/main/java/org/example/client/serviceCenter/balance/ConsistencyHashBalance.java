package org.example.client.serviceCenter.balance;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

public class ConsistencyHashBalance implements LoadBalance {
    private static final int VIRTUAL_NODE_NUM = 64;

    private final SortedMap<Integer, String> shards = new TreeMap<>();
    private final Set<String> realNodes = new HashSet<>();

    @Override
    public synchronized String balance(List<String> addressList) {
        return balance(addressList, UUID.randomUUID().toString());
    }

    @Override
    public synchronized String balance(List<String> addressList, String requestKey) {
        if (addressList == null || addressList.isEmpty()) {
            throw new IllegalArgumentException("addressList is empty");
        }
        refreshRingIfNeeded(addressList);
        if (shards.isEmpty()) {
            throw new IllegalStateException("hash ring is empty");
        }

        String key = (requestKey == null || requestKey.isEmpty()) ? UUID.randomUUID().toString() : requestKey;
        int hash = getHash(key);
        SortedMap<Integer, String> subMap = shards.tailMap(hash);
        Integer target = subMap.isEmpty() ? shards.firstKey() : subMap.firstKey();
        String virtualNode = shards.get(target);
        return virtualNode.substring(0, virtualNode.indexOf("&&VN"));
    }

    @Override
    public synchronized void addNode(String node) {
        if (node == null || node.isEmpty() || realNodes.contains(node)) {
            return;
        }
        realNodes.add(node);
        for (int i = 0; i < VIRTUAL_NODE_NUM; i++) {
            String virtualNode = toVirtualNode(node, i);
            shards.put(getHash(virtualNode), virtualNode);
        }
    }

    @Override
    public synchronized void delNode(String node) {
        if (node == null || node.isEmpty() || !realNodes.contains(node)) {
            return;
        }
        realNodes.remove(node);
        for (int i = 0; i < VIRTUAL_NODE_NUM; i++) {
            String virtualNode = toVirtualNode(node, i);
            shards.remove(getHash(virtualNode), virtualNode);
        }
    }

    private void refreshRingIfNeeded(List<String> serviceList) {
        Set<String> newNodes = new HashSet<>(serviceList);
        if (newNodes.equals(realNodes)) {
            return;
        }
        shards.clear();
        realNodes.clear();
        for (String node : newNodes) {
            addNode(node);
        }
    }

    private String toVirtualNode(String node, int index) {
        return node + "&&VN" + index;
    }

    private int getHash(String str) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (int i = 0; i < str.length(); i++) {
            hash = (hash ^ str.charAt(i)) * p;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        return hash == Integer.MIN_VALUE ? 0 : Math.abs(hash);
    }

    @Override
    public String toString() {
        return "ConsistencyHash";
    }
}
