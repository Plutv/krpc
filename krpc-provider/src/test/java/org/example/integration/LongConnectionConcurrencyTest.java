package org.example.integration;

import org.example.KRpcApplication;
import org.example.client.netty.PendingRequests;
import org.example.client.rpcClient.RpcClient;
import org.example.client.rpcClient.impl.NettyRpcClient;
import org.example.common.message.RpcRequest;
import org.example.common.message.RpcResponse;
import org.example.config.KRpcConfig;
import org.example.pojo.User;
import org.example.provider.impl.UserServiceImpl;
import org.example.server.provider.ServiceProvider;
import org.example.server.ratelimit.RateLimit;
import org.example.server.ratelimit.provider.RateLimitProvider;
import org.example.server.server.RpcServer;
import org.example.server.server.impl.NettyRpcServer;
import org.example.server.serviceRegister.ServiceRegister;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Long-connection + concurrency correctness test.
 *
 * Goal: prove the single-connection multiplexing + requestId correlation is correct under load.
 * It is NOT about "long connection is faster" - it asserts:
 *   - 100% success on a healthy server under high concurrency on one shared channel,
 *   - the observed in-flight (Pending) request peak is > 0 (real multiplexing),
 *   - no PendingRequests leak after the run,
 *   - a failed connection cleans up its pending entry instead of leaking it.
 */
class LongConnectionConcurrencyTest {

    private static final int PORT = 19999;

    static {
        // Force a consistent serializer before any Netty initializer class loads.
        KRpcApplication.initialize(KRpcConfig.builder()
                .serializer("Hessian")
                .tracingEnabled(false)
                .build());
    }

    private static Thread serverThread;
    private static RpcServer server;

    @BeforeAll
    static void startServer() throws InterruptedException {
        ServiceProvider serviceProvider = new ServiceProvider(
                "127.0.0.1", PORT, new NoopServiceRegister(), new UnlimitedRateLimitProvider());
        serviceProvider.provideServiceInterface(new UserServiceImpl(), false);

        server = new NettyRpcServer(serviceProvider);
        serverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                server.start(PORT);
            }
        });
        serverThread.start();
        // Wait for the listening socket to actually be bound instead of guessing with a fixed sleep.
        // Under load the bind can take longer than any constant delay, which previously caused every
        // request to hit a not-yet-listening port (Connection refused) and flake the test.
        assertTrue(server.awaitStarted(15, TimeUnit.SECONDS),
                "server must be bound and accepting connections before the test fires requests");
    }

    @AfterAll
    static void stopServer() throws InterruptedException {
        if (server != null) {
            server.stop();
        }
        if (serverThread != null) {
            serverThread.join(3000);
        }
        NettyRpcClient.shutdown();
    }

    @Test
    void multiplexedChannelHandlesHighConcurrencyWithoutLeak() throws Exception {
        RpcClient client = new NettyRpcClient("127.0.0.1", PORT);
        int total = 2000;
        int concurrency = 64;
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CountDownLatch latch = new CountDownLatch(total);
        AtomicInteger success = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        java.util.concurrent.atomic.AtomicReference<String> firstFailure = new java.util.concurrent.atomic.AtomicReference<String>();
        long[] latencies = new long[total];

        AtomicInteger pendingPeak = new AtomicInteger();
        Thread sampler = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    int s = PendingRequests.size();
                    int cur;
                    do {
                        cur = pendingPeak.get();
                    } while (cur < s && !pendingPeak.compareAndSet(cur, s));
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        });
        sampler.start();

        for (int i = 0; i < total; i++) {
            final int idx = i;
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    long start = System.nanoTime();
                    try {
                        RpcRequest req = RpcRequest.builder()
                                .interfaceName("org.example.service.UserService")
                                .methodName("getUserByUserId")
                                .params(new Object[]{idx})
                                .paramsType(new Class[]{Integer.class})
                                .build();
                        RpcResponse resp = client.sendRequest(req);
                        if (resp != null && resp.getCode() == 200) {
                            success.incrementAndGet();
                        } else {
                            failed.incrementAndGet();
                            if (firstFailure.compareAndSet(null, "code=" + (resp == null ? "null" : resp.getCode()) + " msg=" + (resp == null ? "" : resp.getMessage()))) {
                                // recorded
                            }
                        }
                    } catch (Exception e) {
                        failed.incrementAndGet();
                        if (firstFailure.compareAndSet(null, "exception=" + e.getClass().getSimpleName() + ": " + e.getMessage())) {
                            // recorded
                        }
                    } finally {
                        latencies[idx] = System.nanoTime() - start;
                        latch.countDown();
                    }
                }
            });
        }
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        sampler.interrupt();
        sampler.join();
        pool.shutdown();

        double p50 = percentile(latencies, 0.50);
        double p99 = percentile(latencies, 0.99);
        System.out.printf("concurrency: total=%d success=%d failed=%d pendingPeak=%d p50=%.2fms p99=%.2fms%n",
                total, success.get(), failed.get(), pendingPeak.get(), p50, p99);
        if (firstFailure.get() != null) {
            System.out.println("first failure reason: " + firstFailure.get());
        }

        assertEquals(0, failed.get(), "all concurrent requests should succeed on a healthy server");
        assertEquals(0, PendingRequests.size(), "no pending requests should leak after the run");
        assertTrue(pendingPeak.get() > 0, "should observe concurrent in-flight requests (real multiplexing)");
    }

    @Test
    void failedConnectionCleansPendingRequest() {
        // Port 1 is not listened on: connect fails and the request must not leak.
        RpcClient client = new NettyRpcClient("127.0.0.1", 1);
        RpcRequest req = RpcRequest.builder()
                .interfaceName("org.example.service.UserService")
                .methodName("getUserByUserId")
                .params(new Object[]{1})
                .paramsType(new Class[]{Integer.class})
                .build();
        RpcResponse resp = client.sendRequest(req);
        // resp is a fail response (not null); the key assertion is no leak.
        assertEquals(0, PendingRequests.size(), "failed connection must clean up its pending entry");
    }

    private static double percentile(long[] sortedSource, double percentile) {
        long[] sorted = sortedSource.clone();
        java.util.Arrays.sort(sorted);
        if (sorted.length == 0) {
            return 0D;
        }
        int index = (int) Math.ceil(percentile * sorted.length) - 1;
        index = Math.max(0, Math.min(index, sorted.length - 1));
        return sorted[index] / 1_000_000D;
    }

    static final class NoopServiceRegister implements ServiceRegister {
        @Override
        public void register(String serviceName, InetSocketAddress serviceAddress, boolean canRetry) {
        }

        @Override
        public void unregister(String serviceName, InetSocketAddress serviceAddress, boolean canRetry) {
        }
    }

    static final class UnlimitedRateLimitProvider extends RateLimitProvider {
        @Override
        public RateLimit getRateLimit(String interfaceName) {
            return new RateLimit() {
                @Override
                public boolean getToken() {
                    return true;
                }
            };
        }
    }
}
