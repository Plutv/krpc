package org.example.client.serviceCenter;

import java.net.InetSocketAddress;

public interface ServiceCenter extends AutoCloseable {
    InetSocketAddress serviceDiscovery(String serviceName);

    default InetSocketAddress serviceDiscovery(String serviceName, String requestKey) {
        return serviceDiscovery(serviceName);
    }

    boolean checkRetry(String serviceName);

    default void markNodeAsDown(String serviceName, InetSocketAddress address) {
    }

    default void markNodeAsUp(String serviceName, InetSocketAddress address) {
    }

    @Override
    default void close() {
    }
}
