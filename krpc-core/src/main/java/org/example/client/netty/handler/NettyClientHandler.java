package org.example.client.netty.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.nio.channels.ClosedChannelException;
import lombok.extern.slf4j.Slf4j;
import org.example.client.netty.PendingRequests;
import org.example.common.message.RpcResponse;

@Slf4j
public class NettyClientHandler extends SimpleChannelInboundHandler<RpcResponse> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcResponse rpcResponse) throws Exception {
        PendingRequests.complete(rpcResponse);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        int failedCount = PendingRequests.failChannel(ctx.channel(), new ClosedChannelException());
        if (failedCount > 0) {
            log.warn("Channel became inactive, failed {} in-flight requests, channel={}",
                    failedCount, ctx.channel().id());
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Netty client handler caught exception", cause);
        ctx.close();
    }
}
