package org.example.server.netty.initializer;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.KRpcApplication;
import org.example.common.serializer.mySerializer.ProtobufSerializer;
import org.example.common.serializer.mySerializer.Serializer;
import org.example.server.netty.handler.HeartbeatHandler;
import org.example.server.netty.handler.NettyServerHandler;
import org.example.server.executor.RpcRequestDispatcher;
import org.example.server.provider.ServiceProvider;
import org.example.common.serializer.myCode.MyDecoder;
import org.example.common.serializer.myCode.MyEncoder;

import java.util.concurrent.TimeUnit;

@AllArgsConstructor
@Slf4j
public class NettyServerInitializer extends ChannelInitializer<SocketChannel> {
    private static final Serializer SERIALIZER = resolveSerializer();
    private ServiceProvider serviceProvider;
    private RpcRequestDispatcher requestDispatcher;
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast(new IdleStateHandler(10, 20,0, TimeUnit.SECONDS));
        pipeline.addLast(new HeartbeatHandler());
        pipeline.addLast(new MyDecoder());
        pipeline.addLast(new MyEncoder(SERIALIZER));

        pipeline.addLast(new NettyServerHandler(serviceProvider, requestDispatcher));
    }

    private static Serializer resolveSerializer() {
        String serializerName = KRpcApplication.getRpcConfig().getSerializer();
        try {
            Serializer serializer = Serializer.getSerializerByName(serializerName);
            log.info("Netty server using serializer={}", serializer);
            return serializer;
        } catch (Exception e) {
            log.warn("Invalid serializer '{}', fallback to Protobuf", serializerName, e);
            return new ProtobufSerializer();
        }
    }
}
