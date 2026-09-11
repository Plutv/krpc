package org.example.integration;

import org.example.KRpcApplication;
import org.example.client.rpcClient.RpcClient;
import org.example.client.rpcClient.impl.NettyRpcClient;
import org.example.common.message.RpcRequest;
import org.example.common.message.RpcResponse;
import org.example.config.KRpcConfig;
import org.example.server.executor.RpcRequestDispatcher;
import org.example.server.executor.RpcRequestExecutor;
import org.example.server.provider.ServiceProvider;
import org.example.server.ratelimit.RateLimit;
import org.example.server.ratelimit.provider.RateLimitProvider;
import org.example.server.server.impl.NettyRpcServer;
import org.example.server.serviceRegister.ServiceRegister;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventLoopIsolationBenchmarkTest {
    private static final int DIRECT_PORT = 20001;
    private static final int ISOLATED_PORT = 20002;
    private static final int TOTAL_REQUESTS = 120;
    private static final int CONCURRENCY = 48;
    private static final int SLOW_EVERY = 6;
    private static final int SLOW_MILLIS = 100;

    static {
        KRpcApplication.initialize(KRpcConfig.builder()
                .serializer("Hessian")
                .tracingEnabled(false)
                .build());
    }

    @AfterAll
    static void shutdownClient() {
        NettyRpcClient.shutdown();
    }

    @Test
    void boundedBusinessExecutorPreventsSlowCallsFromBlockingFastCalls() throws Exception {
        BenchmarkResult direct = runScenario(
                "event-loop-direct", DIRECT_PORT, new DirectRequestDispatcher());
        BenchmarkResult isolated = runScenario(
                "business-pool", ISOLATED_PORT, new RpcRequestExecutor(8, 256));

        System.out.printf(
                "event-loop-isolation: direct[qps=%.1f fastP95=%.2fms fastP99=%.2fms total=%.2fms] "
                        + "isolated[qps=%.1f fastP95=%.2fms fastP99=%.2fms total=%.2fms] "
                        + "qpsGain=%.2fx p95Reduction=%.1f%%%n",
                direct.qps, direct.fastP95Millis, direct.fastP99Millis, direct.totalMillis,
                isolated.qps, isolated.fastP95Millis, isolated.fastP99Millis, isolated.totalMillis,
                isolated.qps / direct.qps,
                (1D - isolated.fastP95Millis / direct.fastP95Millis) * 100D);

        assertEquals(TOTAL_REQUESTS, direct.successCount);
        assertEquals(TOTAL_REQUESTS, isolated.successCount);
        assertTrue(isolated.fastP95Millis < direct.fastP95Millis * 0.60D,
                "business isolation should prevent slow calls from dominating fast-call P95");
        assertTrue(isolated.qps > direct.qps * 2D,
                "parallel business execution should improve mixed-workload throughput");
    }

    private BenchmarkResult runScenario(String name, int port, RpcRequestDispatcher dispatcher) throws Exception {
        ServiceProvider provider = new ServiceProvider(
                "127.0.0.1", port, new NoopServiceRegister(), new UnlimitedRateLimitProvider());
        provider.provideServiceInterface(new LatencyProbeServiceImpl(), false);
        NettyRpcServer server = new NettyRpcServer(provider, dispatcher);
        Thread serverThread = new Thread(() -> server.start(port), "benchmark-server-" + name);
        serverThread.start();
        assertTrue(server.awaitStarted(10, TimeUnit.SECONDS));

        try {
            RpcClient client = new NettyRpcClient("127.0.0.1", port);
            for (int i = 0; i < 20; i++) {
                assertEquals(200, invoke(client, 0).getCode());
            }

            ExecutorService callers = Executors.newFixedThreadPool(CONCURRENCY);
            CountDownLatch finished = new CountDownLatch(TOTAL_REQUESTS);
            List<Long> fastLatencies = Collections.synchronizedList(new ArrayList<Long>());
            AtomicInteger success = new AtomicInteger();
            long scenarioStart = System.nanoTime();

            for (int i = 0; i < TOTAL_REQUESTS; i++) {
                final boolean slow = i % SLOW_EVERY == 0;
                callers.submit(() -> {
                    long start = System.nanoTime();
                    try {
                        RpcResponse response = invoke(client, slow ? SLOW_MILLIS : 0);
                        if (response != null && response.getCode() == 200) {
                            success.incrementAndGet();
                        }
                    } finally {
                        if (!slow) {
                            fastLatencies.add(System.nanoTime() - start);
                        }
                        finished.countDown();
                    }
                });
            }

            assertTrue(finished.await(20, TimeUnit.SECONDS));
            long totalNanos = System.nanoTime() - scenarioStart;
            callers.shutdown();
            assertTrue(callers.awaitTermination(5, TimeUnit.SECONDS));

            return new BenchmarkResult(
                    success.get(),
                    TOTAL_REQUESTS / (totalNanos / 1_000_000_000D),
                    percentileMillis(fastLatencies, 0.95D),
                    percentileMillis(fastLatencies, 0.99D),
                    totalNanos / 1_000_000D);
        } finally {
            server.stop();
            serverThread.join(10000);
            assertTrue(!serverThread.isAlive(), "benchmark server must stop cleanly");
        }
    }

    private RpcResponse invoke(RpcClient client, int delayMillis) {
        RpcRequest request = RpcRequest.builder()
                .interfaceName(LatencyProbeService.class.getName())
                .methodName("execute")
                .params(new Object[]{delayMillis})
                .paramsType(new Class[]{Integer.class})
                .build();
        return client.sendRequest(request);
    }

    private double percentileMillis(List<Long> source, double percentile) {
        List<Long> sorted = new ArrayList<>(source);
        Collections.sort(sorted);
        int index = (int) Math.ceil(percentile * sorted.size()) - 1;
        index = Math.max(0, Math.min(index, sorted.size() - 1));
        return sorted.get(index) / 1_000_000D;
    }

    public interface LatencyProbeService {
        String execute(Integer delayMillis);
    }

    public static final class LatencyProbeServiceImpl implements LatencyProbeService {
        @Override
        public String execute(Integer delayMillis) {
            if (delayMillis != null && delayMillis > 0) {
                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("slow call interrupted", e);
                }
            }
            return "ok";
        }
    }

    private static final class DirectRequestDispatcher implements RpcRequestDispatcher {
        @Override
        public boolean dispatch(Runnable task) {
            task.run();
            return true;
        }

        @Override
        public int activeCount() {
            return 1;
        }

        @Override
        public int queueSize() {
            return 0;
        }

        @Override
        public void shutdownGracefully() {
        }
    }
    private static final class BenchmarkResult {
        private final int successCount;
        private final double qps;
        private final double fastP95Millis;
        private final double fastP99Millis;
        private final double totalMillis;

        private BenchmarkResult(int successCount, double qps, double fastP95Millis,
                                double fastP99Millis, double totalMillis) {
            this.successCount = successCount;
            this.qps = qps;
            this.fastP95Millis = fastP95Millis;
            this.fastP99Millis = fastP99Millis;
            this.totalMillis = totalMillis;
        }
    }

    private static final class NoopServiceRegister implements ServiceRegister {
        @Override
        public void register(String serviceName, InetSocketAddress serviceAddress, boolean canRetry) {
        }

        @Override
        public void unregister(String serviceName, InetSocketAddress serviceAddress, boolean canRetry) {
        }
    }

    private static final class UnlimitedRateLimitProvider extends RateLimitProvider {
        @Override
        public RateLimit getRateLimit(String interfaceName) {
            return () -> true;
        }
    }
}
