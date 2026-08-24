package org.example.client.rpcClient.impl;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.example.client.netty.PendingRequests;
import org.example.client.netty.initializer.NettyClientInitializer;
import org.example.client.rpcClient.RpcClient;
import org.example.client.serviceCenter.ServiceCenter;
import org.example.common.message.RpcRequest;
import org.example.common.message.RpcResponse;
import org.example.common.trace.TraceContext;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class NettyRpcClient implements RpcClient {
    private static final long REQUEST_TIMEOUT_MILLIS = 5000L;
    private static final int CONNECT_TIMEOUT_MILLIS = 3000;
    private static final int WRITE_BUFFER_LOW_WATER_MARK = 64 * 1024;
    private static final int WRITE_BUFFER_HIGH_WATER_MARK = 128 * 1024;

    private String host;
    private int port;

    public static final Bootstrap bootstrap;
    public static final EventLoopGroup eventLoopGroup;
    private static final ConcurrentMap<String, Channel> CHANNEL_CACHE = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, Object> CHANNEL_LOCKS = new ConcurrentHashMap<>();

    private ServiceCenter serviceCenter;

    public NettyRpcClient(ServiceCenter serviceCenter) {
        this.serviceCenter = serviceCenter;
    }

    public NettyRpcClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    static {
        eventLoopGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK,
                        new WriteBufferWaterMark(WRITE_BUFFER_LOW_WATER_MARK, WRITE_BUFFER_HIGH_WATER_MARK))
                .handler(new NettyClientInitializer());
    }

    @Override
    public RpcResponse sendRequest(RpcRequest request) {
        long requestDeadlineNanos = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(REQUEST_TIMEOUT_MILLIS);
        if (request.getTraceId() == null || request.getTraceId().isEmpty()) {
            request.setTraceId(TraceContext.getTraceId());
        }
        if (request.getSpanId() == null || request.getSpanId().isEmpty()) {
            request.setSpanId(TraceContext.getSpanId());
        }

        String requestId = UUID.randomUUID().toString();
        request.setRequestId(requestId);

        InetSocketAddress address = resolveAddress(request);
        if (address == null) {
            return RpcResponse.fail("Service discovery returned null address");
        }

        CompletableFuture<RpcResponse> responseFuture = new CompletableFuture<>();

        try {
            Channel channel = getOrCreateChannel(address);
            if (!channel.isWritable()) {
                return RpcResponse.fail("rpc client is overloaded");
            }
            PendingRequests.put(requestId, responseFuture, channel);
            ChannelFuture writeFuture = channel.writeAndFlush(request);
            writeFuture.addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    PendingRequests.fail(requestId, future.cause());
                }
            });

            long remainingNanos = requestDeadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                throw new TimeoutException("request deadline exceeded before waiting for response");
            }
            RpcResponse response = responseFuture.get(remainingNanos, TimeUnit.NANOSECONDS);
            if (response != null && serviceCenter != null) {
                serviceCenter.markNodeAsUp(request.getInterfaceName(), address);
            }
            return response;
        } catch (TimeoutException e) {
            log.error("rpc request timeout, requestId={}, service={}, address={}",
                    requestId, request.getInterfaceName(), address);
            if (serviceCenter != null) {
                serviceCenter.markNodeAsDown(request.getInterfaceName(), address);
            }
            return RpcResponse.fail("rpc request timeout");
        } catch (InterruptedException e) {
            log.error("rpc request was interrupted: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
            return RpcResponse.fail("rpc request interrupted");
        } catch (Exception e) {
            log.error("rpc request failed: {}", e.getMessage(), e);
            if (serviceCenter != null) {
                serviceCenter.markNodeAsDown(request.getInterfaceName(), address);
            }
            return RpcResponse.fail("rpc request failed");
        } finally {
            PendingRequests.remove(requestId);
        }
    }

    private InetSocketAddress resolveAddress(RpcRequest request) {
        if (serviceCenter != null) {
            InetSocketAddress address = serviceCenter.serviceDiscovery(request.getInterfaceName(), buildRequestKey(request));
            if (address == null) {
                log.error("Service discovery returned null for interface={}", request.getInterfaceName());
            }
            return address;
        }

        if (host != null && !host.isEmpty() && port > 0) {
            return new InetSocketAddress(host, port);
        }

        log.error("No available serviceCenter or fixed address for request: {}", request.getInterfaceName());
        return null;
    }

    private String buildRequestKey(RpcRequest request) {
        Object[] params = request.getParams() == null ? new Object[0] : request.getParams();
        return request.getInterfaceName() + "#" + request.getMethodName() + "#" + Arrays.deepHashCode(params);
    }

    private Channel getOrCreateChannel(InetSocketAddress address) throws InterruptedException, TimeoutException {
        String channelKey = address.getHostString() + ":" + address.getPort();
        Channel cachedChannel = CHANNEL_CACHE.get(channelKey);
        if (isChannelAvailable(cachedChannel)) {
            return cachedChannel;
        }

        Object channelLock = CHANNEL_LOCKS.computeIfAbsent(channelKey, key -> new Object());
        synchronized (channelLock) {
            cachedChannel = CHANNEL_CACHE.get(channelKey);
            if (isChannelAvailable(cachedChannel)) {
                return cachedChannel;
            }

            ChannelFuture connectFuture = bootstrap.connect(address.getHostString(), address.getPort());
            if (!connectFuture.await(CONNECT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                connectFuture.cancel(true);
                connectFuture.channel().close();
                throw new TimeoutException("connect timeout: " + channelKey);
            }
            if (!connectFuture.isSuccess()) {
                throw new IllegalStateException("failed to connect to " + channelKey, connectFuture.cause());
            }

            Channel channel = connectFuture.channel();
            CHANNEL_CACHE.put(channelKey, channel);
            channel.closeFuture().addListener((ChannelFutureListener) future -> {
                if (CHANNEL_CACHE.remove(channelKey, channel)) {
                    CHANNEL_LOCKS.remove(channelKey, channelLock);
                }
            });
            return channel;
        }
    }

    private boolean isChannelAvailable(Channel channel) {
        return channel != null && channel.isActive();
    }

    // Shut down the shared Netty event loop when the client is no longer needed.
    public static void shutdown() {
        try {
            if (eventLoopGroup != null) {
                eventLoopGroup.shutdownGracefully().sync();
            }
        } catch (InterruptedException e) {
            log.error("failed to shut down Netty event loop: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }
}
