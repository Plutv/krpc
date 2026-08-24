package org.example.common.serializer.myCode;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.TooLongFrameException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.common.message.MessageType;
import org.example.common.message.RpcRequest;
import org.example.common.message.RpcResponse;
import org.example.common.serializer.mySerializer.Serializer;
import org.example.common.trace.TraceContext;

import java.nio.charset.StandardCharsets;

@Slf4j
@AllArgsConstructor
public class MyEncoder extends MessageToByteEncoder {
    private Serializer serializer;

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        log.debug("Encoding message of type: {}", msg.getClass());

        String traceId = null;
        String spanId = null;
        if (msg instanceof RpcRequest) {
            RpcRequest request = (RpcRequest) msg;
            traceId = request.getTraceId();
            spanId = request.getSpanId();
        }
        if (traceId == null || traceId.isEmpty()) {
            traceId = TraceContext.getTraceId();
        }
        if (spanId == null || spanId.isEmpty()) {
            spanId = TraceContext.getSpanId();
        }
        String traceMsg = (traceId == null ? "" : traceId) + ";" + (spanId == null ? "" : spanId);
        byte[] traceBytes = traceMsg.getBytes(StandardCharsets.UTF_8);
        if (traceBytes.length > ProtocolConstants.MAX_TRACE_LENGTH) {
            throw new TooLongFrameException("traceLength exceeds limit: " + traceBytes.length);
        }

        out.writeShort(ProtocolConstants.MAGIC);
        out.writeByte(ProtocolConstants.VERSION);
        out.writeInt(traceBytes.length);
        out.writeBytes(traceBytes);

        if (msg instanceof RpcRequest) {
            out.writeShort(MessageType.REQUEST.getCode());
        } else if (msg instanceof RpcResponse) {
            out.writeShort(MessageType.RESPONSE.getCode());
        } else {
            log.error("unknown message type: {}", msg.getClass());
            throw new IllegalArgumentException("unknown message type: {}" + msg.getClass());
        }
        out.writeShort(serializer.getType());
        byte[] serializeBytes = serializer.serialize(msg);
        if (serializeBytes.length > ProtocolConstants.MAX_BODY_LENGTH) {
            throw new TooLongFrameException("bodyLength exceeds limit: " + serializeBytes.length);
        }
        out.writeInt(serializeBytes.length);
        out.writeBytes(serializeBytes);
    }
}
