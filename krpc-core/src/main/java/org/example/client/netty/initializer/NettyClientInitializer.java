package org.example.client.netty.initializer;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;
import org.example.KRpcApplication;
import org.example.client.netty.handler.HeartbeatHandler;
import org.example.client.netty.handler.NettyClientHandler;
import org.example.common.serializer.myCode.MyDecoder;
import org.example.common.serializer.myCode.MyEncoder;
import org.example.common.serializer.mySerializer.ProtobufSerializer;
import org.example.common.serializer.mySerializer.Serializer;

import java.util.concurrent.TimeUnit;

@Slf4j
public class NettyClientInitializer extends ChannelInitializer<SocketChannel> {
    private static final Serializer SERIALIZER = resolveSerializer();

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new IdleStateHandler(0, 8,0, TimeUnit.SECONDS));
        pipeline.addLast(new HeartbeatHandler());
        pipeline.addLast(new MyDecoder());
        pipeline.addLast(new MyEncoder(SERIALIZER));

        pipeline.addLast(new NettyClientHandler());

    }

    private static Serializer resolveSerializer() {
        String serializerName = KRpcApplication.getRpcConfig().getSerializer();
        try {
            Serializer serializer = Serializer.getSerializerByName(serializerName);
            log.info("Netty client using serializer={}", serializer);
            return serializer;
        } catch (Exception e) {
            log.warn("Invalid serializer '{}', fallback to Protobuf", serializerName, e);
            return new ProtobufSerializer();
        }
    }
}
