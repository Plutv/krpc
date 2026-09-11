package org.example.server.netty.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HeartbeatHandler extends ChannelDuplexHandler {
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (!(evt instanceof IdleStateEvent)) {
            super.userEventTriggered(ctx, evt);
            return;
        }

        IdleStateEvent idleStateEvent = (IdleStateEvent) evt;
        IdleState idleState = idleStateEvent.state();
        if (idleState == IdleState.READER_IDLE) {
            log.warn("No inbound data for a while, closing channel: {}", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        super.userEventTriggered(ctx, evt);
    }
}
