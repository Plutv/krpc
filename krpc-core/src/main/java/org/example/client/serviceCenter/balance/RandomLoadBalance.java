package org.example.client.serviceCenter.balance;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class RandomLoadBalance implements LoadBalance {
    private final List<String> addressList = new CopyOnWriteArrayList<>();

    @Override
    public String balance(List<String> candidates) {
        List<String> target = (candidates == null || candidates.isEmpty()) ? addressList : candidates;
        if (target.isEmpty()) {
            throw new IllegalArgumentException("addressList is empty");
        }
        int choose = ThreadLocalRandom.current().nextInt(target.size());
        return target.get(choose);
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
        return "Random";
    }
}
