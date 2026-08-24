package org.example.common.serializer.myCode;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.example.common.message.RpcRequest;
import org.example.common.serializer.mySerializer.HessianSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Protocol encode/decode round-trip test.
 *
 * Validates the custom frame (magic + version + trace + message type + serializer + body) survives
 * a full encode -> decode cycle and reassembles a frame that arrives fragmented (half-packet),
 * which is the boundary the decoder's reader-index rollback is meant to handle.
 */
class ProtocolRoundTripTest {

    private EmbeddedChannel channel;

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void encodesThenDecodesRequestLosslessly() {
        RpcRequest request = RpcRequest.builder()
                .requestId("req-1")
                .traceId("trace-1")
                .spanId("span-1")
                .interfaceName("org.example.service.UserService")
                .methodName("getUserByUserId")
                .params(new Object[]{123})
                .paramsType(new Class[]{Integer.class})
                .build();

        channel = new EmbeddedChannel(new MyDecoder(), new MyEncoder(new HessianSerializer()));

        assertTrue(channel.writeOutbound(request));
        ByteBuf encoded = (ByteBuf) channel.readOutbound();
        assertNotNull(encoded);

        assertTrue(channel.writeInbound(encoded));
        RpcRequest decoded = channel.readInbound();
        assertNotNull(decoded);

        assertEquals(request.getRequestId(), decoded.getRequestId());
        assertEquals(request.getInterfaceName(), decoded.getInterfaceName());
        assertEquals(request.getMethodName(), decoded.getMethodName());
        assertEquals(request.getTraceId(), decoded.getTraceId());
    }

    @Test
    void reassemblesFragmentedFrame() {
        RpcRequest request = RpcRequest.builder()
                .requestId("req-2")
                .interfaceName("org.example.service.UserService")
                .methodName("getUserByUserId")
                .params(new Object[]{1})
                .paramsType(new Class[]{Integer.class})
                .build();

        channel = new EmbeddedChannel(new MyDecoder(), new MyEncoder(new HessianSerializer()));
        assertTrue(channel.writeOutbound(request));
        ByteBuf encoded = (ByteBuf) channel.readOutbound();
        byte[] all = new byte[encoded.readableBytes()];
        encoded.readBytes(all);
        encoded.release();

        int split = all.length / 3;
        ByteBuf first = Unpooled.wrappedBuffer(all, 0, split);
        ByteBuf second = Unpooled.wrappedBuffer(all, split, all.length - split);

        assertFalse(channel.writeInbound(first), "first fragment must not produce a full message yet");
        assertTrue(channel.writeInbound(second), "second fragment must complete the message");
        RpcRequest decoded = channel.readInbound();
        assertNotNull(decoded);
        assertEquals(request.getRequestId(), decoded.getRequestId());
    }
}
