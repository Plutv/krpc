package org.example.client.serviceCenter.zkWatcher;

/**
 * Zookeeper node change callback.
 * ZkServiceCenter coordinates cache update + load-balance update.
 */
public interface ServiceChangeListener {

    void onAdd(String serviceName, String address);

    void onRemove(String serviceName, String address);

    default void onReplace(String serviceName, String oldAddress, String newAddress) {
        onRemove(serviceName, oldAddress);
        onAdd(serviceName, newAddress);
    }

    default void onRetryAdd(String serviceName) {
    }

    default void onRetryRemove(String serviceName) {
    }

    default void onRetryAdd(String serviceName, String address) {
        onRetryAdd(serviceName);
    }

    default void onRetryRemove(String serviceName, String address) {
        onRetryRemove(serviceName);
    }
}
