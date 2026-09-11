package org.example.server.executor;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcRequestExecutorTest {

    @Test
    void rejectsNewTaskWhenWorkersAndBoundedQueueAreFull() throws Exception {
        RpcRequestExecutor executor = new RpcRequestExecutor(1, 1);
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            assertTrue(executor.dispatch(() -> {
                running.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
            assertTrue(running.await(1, TimeUnit.SECONDS));
            assertTrue(executor.dispatch(() -> { }));
            assertFalse(executor.dispatch(() -> { }),
                    "third task must be rejected instead of growing memory usage");
        } finally {
            release.countDown();
            executor.shutdownGracefully();
        }
    }
}
