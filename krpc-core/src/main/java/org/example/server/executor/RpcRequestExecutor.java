package org.example.server.executor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class RpcRequestExecutor {
    private final ThreadPoolExecutor executor;

    public RpcRequestExecutor(int workerThreads, int queueCapacity) {
        if (workerThreads <= 0) {
            throw new IllegalArgumentException("workerThreads must be positive");
        }
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be positive");
        }
        this.executor = new ThreadPoolExecutor(
                workerThreads,
                workerThreads,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(queueCapacity),
                new NamedThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    public boolean submit(Runnable task) {
        try {
            executor.execute(task);
            return true;
        } catch (RejectedExecutionException ignored) {
            return false;
        }
    }

    public int activeCount() {
        return executor.getActiveCount();
    }

    public int queueSize() {
        return executor.getQueue().size();
    }

    public void shutdownGracefully() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static final class NamedThreadFactory implements ThreadFactory {
        private final AtomicInteger sequence = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "krpc-business-" + sequence.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        }
    }
}
