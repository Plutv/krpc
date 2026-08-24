package org.example.server.serviceRegister;

import java.net.InetSocketAddress;

public interface ServiceRegister extends AutoCloseable {
    void register(String serviceName, InetSocketAddress serviceAddress, boolean canRetry);

    void unregister(String serviceName, InetSocketAddress serviceAddress, boolean canRetry);

    @Override
    default void close() {
    }
}
