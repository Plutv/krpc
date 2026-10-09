package org.example.jmeter;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.example.KRpcApplication;
import org.example.client.netty.PendingRequests;
import org.example.client.rpcClient.impl.NettyRpcClient;
import org.example.common.message.RpcRequest;
import org.example.common.message.RpcResponse;
import org.example.config.KRpcConfig;

import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public final class RpcJavaSampler extends AbstractJavaSamplerClient {
    private static final Object LIFECYCLE_LOCK = new Object();
    private static int activeSamplers;
    private static String configuredSerializer;

    private NettyRpcClient client;
    private String padding;
    private int slowPercent;
    private int slowMillis;
    private boolean acquired;

    @Override
    public Arguments getDefaultParameters() {
        Arguments parameters = new Arguments();
        parameters.addArgument("host", "127.0.0.1");
        parameters.addArgument("port", "9999");
        parameters.addArgument("serializer", "hessian");
        parameters.addArgument("payloadBytes", "1024");
        parameters.addArgument("slowPercent", "0");
        parameters.addArgument("slowMillis", "100");
        return parameters;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {
        int port = boundedInt(context, "port", 9999, 1, 65535);
        int payloadBytes = boundedInt(context, "payloadBytes", 1024, 36, 1024 * 1024);
        slowPercent = boundedInt(context, "slowPercent", 0, 0, 100);
        slowMillis = boundedInt(context, "slowMillis", 100, 0, 30000);
        String serializer = context.getParameter("serializer", "hessian").trim().toLowerCase(Locale.ROOT);
        if (!Arrays.asList("hessian", "protostuff", "json", "jdk").contains(serializer)) {
            throw new IllegalArgumentException("serializer must be hessian/protostuff/json/jdk");
        }
        char[] chars = new char[payloadBytes - 36];
        Arrays.fill(chars, 'x');
        padding = new String(chars);
        synchronized (LIFECYCLE_LOCK) {
            // NettyClientInitializer resolves its serializer once per JVM, so forbid mixed configurations.
            if (configuredSerializer != null && !configuredSerializer.equals(serializer)) {
                throw new IllegalArgumentException("Restart JMeter before changing serializer");
            }
            if (configuredSerializer == null) {
                KRpcApplication.initialize(KRpcConfig.builder().serializer(serializer).tracingEnabled(false).build());
                configuredSerializer = serializer;
            }
            client = new NettyRpcClient(context.getParameter("host", "127.0.0.1"), port);
            activeSamplers++;
            acquired = true;
        }
    }

    @Override
    public SampleResult runTest(JavaSamplerContext context) {
        boolean slow = ThreadLocalRandom.current().nextInt(100) < slowPercent;
        SampleResult result = new SampleResult();
        result.setSampleLabel(context.getParameter("labelPrefix", "RPC echo") + (slow ? " slow" : " fast"));
        result.setDataType(SampleResult.TEXT);
        result.sampleStart();
        try {
            if (!acquired) {
                throw new IllegalStateException("Sampler setup failed; check jmeter.log");
            }
            String expected = UUID.randomUUID().toString() + padding;
            RpcRequest request = RpcRequest.builder()
                    .interfaceName(EchoService.class.getName())
                    .methodName("echo")
                    .params(new Object[]{expected, slow ? slowMillis : 0})
                    .paramsType(new Class<?>[]{String.class, Integer.class})
                    .build();
            RpcResponse response = client.sendRequest(request);
            if (response == null) {
                result.setResponseCode("NO_RESPONSE");
                result.setResponseMessage("RPC returned null");
                result.setSuccessful(false);
            } else if (response.getCode() != 200) {
                result.setResponseCode(Integer.toString(response.getCode()));
                result.setResponseMessage(response.getMessage());
                result.setSuccessful(false);
            } else if (!expected.equals(response.getData())) {
                result.setResponseCode("MISMATCH");
                result.setResponseMessage("Echo differs from this call's unique payload");
                result.setSuccessful(false);
            } else {
                result.setResponseCode("200");
                result.setResponseMessage("RPC echo verified");
                result.setSuccessful(true);
                // Retain a small diagnostic value, not every payload in a long load test.
                result.setResponseData("echo verified", "UTF-8");
            }
        } catch (Exception e) {
            result.setResponseCode("CLIENT_EXCEPTION");
            result.setResponseMessage(e.getClass().getSimpleName() + ": " + e.getMessage());
            result.setSuccessful(false);
        } finally {
            result.sampleEnd();
        }
        return result;
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        synchronized (LIFECYCLE_LOCK) {
            if (!acquired) {
                return;
            }
            acquired = false;
            if (--activeSamplers == 0) {
                System.out.printf("RPC sampler cleanup: pending=%d lateResponses=%d%n",
                        PendingRequests.size(), PendingRequests.lateResponseCount());
                NettyRpcClient.shutdown();
            }
        }
    }

    private static int boundedInt(JavaSamplerContext context, String name, int defaultValue, int min, int max) {
        int value = Integer.parseInt(context.getParameter(name, Integer.toString(defaultValue)));
        if (value < min || value > max) {
            throw new IllegalArgumentException(name + " must be between " + min + " and " + max);
        }
        return value;
    }
}
