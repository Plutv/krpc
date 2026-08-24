package org.example.benchmark;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.example.client.netty.PendingRequests;
import org.example.client.netty.initializer.NettyClientInitializer;
import org.example.client.rpcClient.RpcClient;
import org.example.client.rpcClient.impl.NettyRpcClient;
import org.example.common.message.RpcRequest;
import org.example.common.message.RpcResponse;
import org.example.service.UserService;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class RpcThroughputBenchmark {
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 9999;
    private static final int DEFAULT_TOTAL_REQUESTS = 3000;
    private static final int DEFAULT_CONCURRENCY = 100;
    private static final int DEFAULT_WARMUP_REQUESTS = 300;

    public static void main(String[] args) throws InterruptedException {
        BenchmarkConfig config = BenchmarkConfig.fromArgs(args);
        printHeader(config);

        PerRequestNettyRpcClient perRequestClient = new PerRequestNettyRpcClient(config.host, config.port);
        NettyRpcClient multiplexClient = new NettyRpcClient(config.host, config.port);
        try {
            runWarmup("per-request-connection", perRequestClient, config.warmupRequests, config.concurrency);
            BenchResult perRequestResult = runRound(
                    "per-request-connection", perRequestClient, config.totalRequests, config.concurrency);

            runWarmup("single-connection-multiplex", multiplexClient, config.warmupRequests, config.concurrency);
            BenchResult multiplexResult = runRound(
                    "single-connection-multiplex", multiplexClient, config.totalRequests, config.concurrency);

            printComparison(perRequestResult, multiplexResult);
        } finally {
            perRequestClient.shutdown();
            NettyRpcClient.shutdown();
        }
    }

    private static void printHeader(BenchmarkConfig config) {
        System.out.println("========== RPC Throughput Benchmark ==========");
        System.out.println("Please start provider first: org.example.provider.BenchmarkProvider");
        System.out.println("Benchmark provider disables registry, rate limiting, tracing and business logging.");
        System.out.println("Host: " + config.host + ", Port: " + config.port);
        System.out.println("Total Requests: " + config.totalRequests
                + ", Concurrency: " + config.concurrency
                + ", Warmup Requests: " + config.warmupRequests);
        System.out.println("==============================================");
    }

    private static void runWarmup(String mode, RpcClient client, int warmupRequests, int concurrency)
            throws InterruptedException {
        if (warmupRequests <= 0) {
            return;
        }
        int warmupConcurrency = Math.min(concurrency, warmupRequests);
        BenchResult warmup = runRound(mode + "-warmup", client, warmupRequests, warmupConcurrency);
        System.out.println("[Warmup] " + warmup);
    }

    private static BenchResult runRound(String mode, RpcClient client, int totalRequests, int concurrency)
            throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CountDownLatch latch = new CountDownLatch(totalRequests);
        AtomicInteger success = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        long[] latencyNanos = new long[totalRequests];

        long startNs = System.nanoTime();
        for (int i = 0; i < totalRequests; i++) {
            final int requestNo = i;
            pool.submit(() -> {
                long requestStartNs = System.nanoTime();
                try {
                    RpcRequest request = buildRequest(requestNo);
                    RpcResponse response = client.sendRequest(request);
                    if (response != null && response.getCode() == 200) {
                        success.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                    }
                } catch (Exception e) {
                    failed.incrementAndGet();
                } finally {
                    latencyNanos[requestNo] = System.nanoTime() - requestStartNs;
                    latch.countDown();
                }
            });
        }

        latch.await();
        long costNs = System.nanoTime() - startNs;

        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        BenchResult result = BenchResult.of(
                mode, totalRequests, success.get(), failed.get(), costNs, latencyNanos);
        System.out.println(result);
        return result;
    }

    private static RpcRequest buildRequest(int i) {
        return RpcRequest.builder()
                .interfaceName(UserService.class.getName())
                .methodName("getUserByUserId")
                .params(new Object[]{i})
                .paramsType(new Class[]{Integer.class})
                .build();
    }

    private static void printComparison(BenchResult perRequest, BenchResult multiplex) {
        if (perRequest.qps <= 0) {
            System.out.println("Per-request baseline qps is 0, skip comparison.");
            return;
        }
        double uplift = (multiplex.qps - perRequest.qps) / perRequest.qps * 100.0;
        System.out.println("---------- Comparison ----------");
        System.out.println("Baseline(per-request): " + perRequest.qps + " req/s");
        System.out.println("Multiplex(single-channel): " + multiplex.qps + " req/s");
        System.out.printf("Throughput uplift: %.2f%%%n", uplift);
        System.out.println("--------------------------------");
    }

    private static final class PerRequestNettyRpcClient implements RpcClient {
        private static final long REQUEST_TIMEOUT_MS = 5000L;

        private final String host;
        private final int port;
        private final EventLoopGroup eventLoopGroup;
        private final Bootstrap bootstrap;

        private PerRequestNettyRpcClient(String host, int port) {
            this.host = host;
            this.port = port;
            this.eventLoopGroup = new NioEventLoopGroup();
            this.bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new NettyClientInitializer());
        }

        @Override
        public RpcResponse sendRequest(RpcRequest request) {
            String requestId = java.util.UUID.randomUUID().toString();
            request.setRequestId(requestId);
            CompletableFuture<RpcResponse> responseFuture = new CompletableFuture<>();
            PendingRequests.put(requestId, responseFuture);

            Channel channel = null;
            try {
                channel = bootstrap.connect(host, port).sync().channel();
                Channel finalChannel = channel;
                channel.writeAndFlush(request).addListener((ChannelFutureListener) future -> {
                    if (!future.isSuccess()) {
                        PendingRequests.fail(requestId, future.cause());
                        finalChannel.close();
                    }
                });

                RpcResponse response = responseFuture.get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                channel.close().sync();
                return response;
            } catch (TimeoutException e) {
                PendingRequests.remove(requestId);
                return RpcResponse.fail("rpc request timeout");
            } catch (Exception e) {
                PendingRequests.remove(requestId);
                return RpcResponse.fail("rpc request failed");
            } finally {
                if (channel != null && channel.isOpen()) {
                    channel.close();
                }
            }
        }

        private void shutdown() {
            eventLoopGroup.shutdownGracefully().syncUninterruptibly();
        }
    }

    private static final class BenchmarkConfig {
        private final String host;
        private final int port;
        private final int totalRequests;
        private final int concurrency;
        private final int warmupRequests;

        private BenchmarkConfig(String host, int port, int totalRequests, int concurrency, int warmupRequests) {
            this.host = host;
            this.port = port;
            this.totalRequests = totalRequests;
            this.concurrency = concurrency;
            this.warmupRequests = warmupRequests;
        }

        private static BenchmarkConfig fromArgs(String[] args) {
            String host = DEFAULT_HOST;
            int port = DEFAULT_PORT;
            int total = DEFAULT_TOTAL_REQUESTS;
            int concurrency = DEFAULT_CONCURRENCY;
            int warmup = DEFAULT_WARMUP_REQUESTS;

            for (String arg : args) {
                if (arg.startsWith("--host=")) {
                    host = arg.substring("--host=".length());
                } else if (arg.startsWith("--port=")) {
                    port = Integer.parseInt(arg.substring("--port=".length()));
                } else if (arg.startsWith("--total=")) {
                    total = Integer.parseInt(arg.substring("--total=".length()));
                } else if (arg.startsWith("--concurrency=")) {
                    concurrency = Integer.parseInt(arg.substring("--concurrency=".length()));
                } else if (arg.startsWith("--warmup=")) {
                    warmup = Integer.parseInt(arg.substring("--warmup=".length()));
                }
            }
            return new BenchmarkConfig(host, port, total, concurrency, warmup);
        }
    }

    private static final class BenchResult {
        private final String mode;
        private final int total;
        private final int success;
        private final int failed;
        private final double costSeconds;
        private final double qps;
        private final double p50Millis;
        private final double p95Millis;
        private final double p99Millis;

        private BenchResult(String mode, int total, int success, int failed,
                            double costSeconds, double qps,
                            double p50Millis, double p95Millis, double p99Millis) {
            this.mode = mode;
            this.total = total;
            this.success = success;
            this.failed = failed;
            this.costSeconds = costSeconds;
            this.qps = qps;
            this.p50Millis = p50Millis;
            this.p95Millis = p95Millis;
            this.p99Millis = p99Millis;
        }

        private static BenchResult of(String mode, int total, int success, int failed,
                                      long costNs, long[] latencyNanos) {
            double seconds = Math.max(0.000001D, costNs / 1_000_000_000.0D);
            double qps = success / seconds;
            long[] sorted = Arrays.copyOf(latencyNanos, latencyNanos.length);
            Arrays.sort(sorted);
            return new BenchResult(mode, total, success, failed, seconds, qps,
                    percentileMillis(sorted, 0.50D),
                    percentileMillis(sorted, 0.95D),
                    percentileMillis(sorted, 0.99D));
        }

        private static double percentileMillis(long[] sorted, double percentile) {
            if (sorted.length == 0) {
                return 0D;
            }
            int index = (int) Math.ceil(percentile * sorted.length) - 1;
            return sorted[Math.max(0, Math.min(index, sorted.length - 1))] / 1_000_000D;
        }

        @Override
        public String toString() {
            return String.format("[%s] total=%d success=%d failed=%d cost=%.3fs "
                            + "qps=%.2f req/s p50=%.3fms p95=%.3fms p99=%.3fms",
                    mode, total, success, failed, costSeconds, qps,
                    p50Millis, p95Millis, p99Millis);
        }
    }
}
