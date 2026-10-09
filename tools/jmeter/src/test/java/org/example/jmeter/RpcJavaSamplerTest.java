package org.example.jmeter;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.example.KRpcApplication;
import org.example.client.netty.PendingRequests;
import org.example.config.KRpcConfig;
import org.example.server.provider.ServiceProvider;
import org.example.server.server.impl.NettyRpcServer;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RpcJavaSamplerTest {
    @Test
    void samplersShareTransportWithoutOneThreadsTeardownClosingOtherCalls() throws Exception {
        try (RunningProvider provider = new RunningProvider(new JMeterProvider.EchoServiceImpl())) {
            RpcJavaSampler first = new RpcJavaSampler();
            RpcJavaSampler second = new RpcJavaSampler();
            JavaSamplerContext context = context(first, provider.port);
            first.setupTest(context);
            second.setupTest(context);
            try {
                assertTrue(first.runTest(context).isSuccessful());
                first.teardownTest(context);
                assertTrue(second.runTest(context).isSuccessful());
                assertEquals(0, PendingRequests.size());
            } finally {
                first.teardownTest(context);
                second.teardownTest(context);
            }
        }
    }

    @Test
    void successfulStatusWithWrongPayloadIsNotCountedAsSuccess() throws Exception {
        try (RunningProvider provider = new RunningProvider(new WrongEcho())) {
            RpcJavaSampler sampler = new RpcJavaSampler();
            JavaSamplerContext context = context(sampler, provider.port);
            sampler.setupTest(context);
            try {
                org.apache.jmeter.samplers.SampleResult result = sampler.runTest(context);
                assertFalse(result.isSuccessful());
                assertEquals("MISMATCH", result.getResponseCode());
            } finally {
                sampler.teardownTest(context);
            }
        }
    }

    private JavaSamplerContext context(RpcJavaSampler sampler, int port) {
        Arguments arguments = sampler.getDefaultParameters();
        arguments.getArgument(1).setValue(Integer.toString(port));
        return new JavaSamplerContext(arguments);
    }

    public static final class WrongEcho implements EchoService {
        @Override
        public String echo(String payload, Integer delayMillis) {
            return "wrong-response";
        }
    }

    private static final class RunningProvider implements AutoCloseable {
        final int port;
        final NettyRpcServer server;
        final Thread thread;

        RunningProvider(EchoService implementation) throws Exception {
            try (ServerSocket socket = new ServerSocket(0)) {
                port = socket.getLocalPort();
            }
            KRpcApplication.initialize(KRpcConfig.builder().serializer("hessian").tracingEnabled(false).build());
            ServiceProvider provider = new ServiceProvider("127.0.0.1", port,
                    new JMeterProvider.NoopRegistry(), new JMeterProvider.UnlimitedRateLimitProvider());
            provider.provideServiceInterface(implementation, false);
            server = new NettyRpcServer(provider);
            thread = new Thread(() -> server.start(port), "jmeter-test-provider");
            thread.start();
            if (!server.awaitStarted(10, TimeUnit.SECONDS) || !server.isBound()) {
                server.stop();
                throw new IllegalStateException("Test provider did not start");
            }
        }

        @Override
        public void close() throws InterruptedException {
            server.stop();
            thread.join(10000);
            assertFalse(thread.isAlive(), "provider must terminate");
        }
    }
}
