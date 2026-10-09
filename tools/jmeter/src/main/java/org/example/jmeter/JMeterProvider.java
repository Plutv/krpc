package org.example.jmeter;

import org.example.KRpcApplication;
import org.example.config.KRpcConfig;
import org.example.server.provider.ServiceProvider;
import org.example.server.ratelimit.RateLimit;
import org.example.server.ratelimit.provider.RateLimitProvider;
import org.example.server.server.impl.NettyRpcServer;
import org.example.server.serviceRegister.ServiceRegister;

import java.net.InetSocketAddress;

public final class JMeterProvider {
    private JMeterProvider() {
    }

    public static void main(String[] args) {
        int port = Integer.getInteger("krpc.bench.port", 9999);
        String serializer = System.getProperty("krpc.bench.serializer", "hessian");
        KRpcApplication.initialize(KRpcConfig.builder()
                .serializer(serializer)
                .tracingEnabled(false)
                .businessThreads(Integer.getInteger("krpc.bench.threads", 16))
                .businessQueueCapacity(Integer.getInteger("krpc.bench.queue", 1024))
                .build());

        ServiceProvider provider = new ServiceProvider(
                "127.0.0.1", port, new NoopRegistry(), new UnlimitedRateLimitProvider());
        provider.provideServiceInterface(new EchoServiceImpl(), false);
        NettyRpcServer server = new NettyRpcServer(provider);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "jmeter-provider-stop"));
        System.out.printf("RPC echo provider: port=%d serializer=%s; registry/rate-limit/tracing disabled%n",
                port, serializer);
        server.start(port);
        if (!server.isBound()) {
            // start() blocks until shutdown or bind failure; never claim a successful startup on failure.
            System.out.println("RPC provider stopped (check logs for bind/startup failures).");
        }
    }

    public static final class EchoServiceImpl implements EchoService {
        @Override
        public String echo(String payload, Integer delayMillis) {
            if (delayMillis == null || delayMillis < 0 || delayMillis > 30000) {
                throw new IllegalArgumentException("delayMillis must be between 0 and 30000");
            }
            if (delayMillis > 0) {
                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("echo interrupted", e);
                }
            }
            return payload;
        }
    }

    static final class NoopRegistry implements ServiceRegister {
        @Override
        public void register(String serviceName, InetSocketAddress address, boolean canRetry) {
        }

        @Override
        public void unregister(String serviceName, InetSocketAddress address, boolean canRetry) {
        }
    }

    static final class UnlimitedRateLimitProvider extends RateLimitProvider {
        @Override
        public RateLimit getRateLimit(String interfaceName) {
            return () -> true;
        }
    }
}
