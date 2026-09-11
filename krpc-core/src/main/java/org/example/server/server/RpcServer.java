package org.example.server.server;

import java.util.concurrent.TimeUnit;

public interface RpcServer {
    void start(int port);

    void stop();

    /**
     * Block until the server is actually accepting connections, or the timeout elapses.
     *
     * @return true if the listening socket was bound within the timeout, false otherwise.
     */
    default boolean awaitStarted(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    /** @return true if the listening socket has been bound. */
    default boolean isBound() {
        return false;
    }
}
