package org.example.server.server.impl;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.example.KRpcApplication;
import org.example.config.KRpcConfig;
import org.example.server.executor.RpcRequestDispatcher;
import org.example.server.executor.RpcRequestExecutor;
import org.example.server.netty.initializer.NettyServerInitializer;
import org.example.server.provider.ServiceProvider;
import org.example.server.server.RpcServer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public class NettyRpcServer implements RpcServer {
    private ServiceProvider serviceProvider;
    private final RpcRequestDispatcher requestDispatcher;

    private ChannelFuture channelFuture;

    // Readiness signal: counted down once the listening socket is actually bound, so callers can
    // reliably wait for "server is accepting connections" instead of guessing with an arbitrary sleep.
    private final CountDownLatch startedLatch = new CountDownLatch(1);
    private volatile boolean bound = false;

    public NettyRpcServer(ServiceProvider serviceProvider) {
        this(serviceProvider, createDefaultDispatcher());
    }

    public NettyRpcServer(ServiceProvider serviceProvider, RpcRequestDispatcher requestDispatcher) {
        this.serviceProvider = serviceProvider;
        this.requestDispatcher = requestDispatcher;
    }

    private static RpcRequestDispatcher createDefaultDispatcher() {
        KRpcConfig config = KRpcApplication.getRpcConfig();
        return new RpcRequestExecutor(config.getBusinessThreads(), config.getBusinessQueueCapacity());
    }

    @Override
    public void start(int port) {
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        NioEventLoopGroup workGroup = new NioEventLoopGroup();
        System.out.println("netty 服务器启动");
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workGroup).channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new NettyServerInitializer(serviceProvider, requestDispatcher));
            channelFuture = serverBootstrap.bind(port).sync();
            bound = true;
            startedLatch.countDown();
            log.info("Netty 服务已绑定端口 {}", port);
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            log.error("Netty 服务启动被中断", e);
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            log.error("Netty 服务启动失败", t);
        } finally {
            requestDispatcher.shutdownGracefully();
            shutdown(bossGroup, workGroup);
            serviceProvider.close();
            bound = false;
        }
    }

    /**
     * Block until the server is actually accepting connections, or the timeout elapses.
     *
     * @return true if the listening socket was bound within the timeout, false otherwise.
     */
    public boolean awaitStarted(long timeout, TimeUnit unit) throws InterruptedException {
        return startedLatch.await(timeout, unit);
    }

    /** @return true if the listening socket has been bound. */
    public boolean isBound() {
        return bound;
    }

    @Override
    public void stop() {
        if (channelFuture != null) {
            try {
                channelFuture.channel().close().sync();
                bound = false;
                log.info("Netty服务主通道已关闭");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("关闭Netty服务主通道时中断", e.getMessage(), e);
            }
        } else {
            log.warn("Netty服务尚未启动，无法关闭！");
        }
    }

    private void shutdown(NioEventLoopGroup bossGroup, NioEventLoopGroup workGroup) {
        if (bossGroup != null) {
            bossGroup.shutdownGracefully().syncUninterruptibly();
        }
        if (workGroup != null) {
            workGroup.shutdownGracefully().syncUninterruptibly();
        }
    }
}
