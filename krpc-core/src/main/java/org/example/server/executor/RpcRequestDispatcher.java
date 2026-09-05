package org.example.server.executor;

public interface RpcRequestDispatcher {
    boolean dispatch(Runnable task);

    int activeCount();

    int queueSize();

    void shutdownGracefully();
}
