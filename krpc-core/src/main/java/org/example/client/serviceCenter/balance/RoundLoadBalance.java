package org.example.client.serviceCenter.balance;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundLoadBalance implements LoadBalance {
    private final AtomicInteger choose = new AtomicInteger(0);
    private final List<String> addressList = new CopyOnWriteArrayList<>();

    @Override
    public String balance(List<String> candidates) {
        List<String> target = (candidates == null || candidates.isEmpty()) ? addressList : candidates;
        if (target.isEmpty()) {
            throw new IllegalArgumentException("addressList is empty");
        }
        int currentChoose = choose.getAndUpdate(i -> (i + 1) % target.size());
        return target.get(currentChoose);
    }

    @Override
    public void addNode(String node) {
        if (node != null && !node.isEmpty() && !addressList.contains(node)) {
            addressList.add(node);
        }
    }

    @Override
    public void delNode(String node) {
        addressList.remove(node);
    }

    @Override
    public String toString() {
        return "Round";
    }
}
