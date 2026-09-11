package org.example.client.serviceCenter.balance;

import java.util.List;

public interface LoadBalance {
    String balance(List<String> addressList);

    default String balance(List<String> addressList, String requestKey) {
        return balance(addressList);
    }

    void addNode(String node);

    void delNode(String node);
}
