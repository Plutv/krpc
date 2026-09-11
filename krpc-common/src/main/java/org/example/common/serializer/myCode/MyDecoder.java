package org.example.common.serializer.myCode;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;
import org.example.common.message.MessageType;
import org.example.common.message.RpcRequest;
import org.example.common.serializer.mySerializer.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class MyDecoder extends ByteToMessageDecoder {
    private static final int MAGIC_BYTES = 2;
    private static final int VERSION_BYTES = 1;
    private static final int INT_BYTES = 4;
    private static final int SHORT_BYTES = 2;
    private static final int MIN_FIXED_LENGTH = SHORT_BYTES + SHORT_BYTES + INT_BYTES;
    private static final int FRAME_PREFIX_LENGTH = MAGIC_BYTES + VERSION_BYTES + INT_BYTES;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < FRAME_PREFIX_LENGTH) {
            return;
        }

        in.markReaderIndex();

        short magic = in.readShort();
        if (magic != ProtocolConstants.MAGIC) {
            throw new IllegalArgumentException("Invalid magic: " + magic);
        }

        byte version = in.readByte();
        if (version != ProtocolConstants.VERSION) {
            throw new IllegalArgumentException("Unsupported protocol version: " + version);
        }

        int traceLength = in.readInt();
        if (traceLength < 0) {
            throw new IllegalArgumentException("traceLength is negative");
        }
        if (traceLength > ProtocolConstants.MAX_TRACE_LENGTH) {
            throw new TooLongFrameException("traceLength exceeds limit: " + traceLength);
        }

        long remainingHeaderLength = (long) traceLength + MIN_FIXED_LENGTH;
        if (in.readableBytes() < remainingHeaderLength) {
            in.resetReaderIndex();
            return;
        }

        byte[] traceBytes = new byte[traceLength];
        in.readBytes(traceBytes);
        String[] traceMetadata = deserializeTraceMsg(traceBytes);

        short messageType = in.readShort();
        if (messageType != MessageType.REQUEST.getCode() && messageType != MessageType.RESPONSE.getCode()) {
            throw new IllegalArgumentException("Unsupported message type: " + messageType);
        }

        short serializerType = in.readShort();
        Serializer serializer = Serializer.getSerializerByCode(serializerType);
        if (serializer == null) {
            throw new IllegalArgumentException("No serializer for type: " + serializerType);
        }

        int bodyLength = in.readInt();
        if (bodyLength < 0) {
            throw new IllegalArgumentException("bodyLength is negative");
        }
        if (bodyLength > ProtocolConstants.MAX_BODY_LENGTH) {
            throw new TooLongFrameException("bodyLength exceeds limit: " + bodyLength);
        }

        if (in.readableBytes() < bodyLength) {
            in.resetReaderIndex();
            return;
        }

        byte[] bodyBytes = new byte[bodyLength];
        in.readBytes(bodyBytes);
        Object deserialize = serializer.deserializer(bodyBytes, messageType);
        if (deserialize instanceof RpcRequest) {
            RpcRequest request = (RpcRequest) deserialize;
            if (request.getTraceId() == null || request.getTraceId().isEmpty()) {
                request.setTraceId(traceMetadata[0]);
            }
            if (request.getSpanId() == null || request.getSpanId().isEmpty()) {
                request.setSpanId(traceMetadata[1]);
            }
        }
        out.add(deserialize);
    }

    private String[] deserializeTraceMsg(byte[] bytes) {
        String traceMsg = new String(bytes, StandardCharsets.UTF_8);
        String[] msgs = traceMsg.split(";", -1);
        String traceId = msgs.length > 0 ? msgs[0] : "";
        String spanId = msgs.length > 1 ? msgs[1] : "";
        return new String[]{traceId, spanId};
    }
}
