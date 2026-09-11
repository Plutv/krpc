package org.example.common.serializer.myCode;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import org.example.common.message.MessageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MyDecoderTest {
    private final EmbeddedChannel channel = new EmbeddedChannel(new MyDecoder());

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void rejectsOversizedTraceBeforeAllocatingBuffer() {
        ByteBuf frame = Unpooled.buffer();
        frame.writeShort(ProtocolConstants.MAGIC);
        frame.writeByte(ProtocolConstants.VERSION);
        frame.writeInt(ProtocolConstants.MAX_TRACE_LENGTH + 1);

        assertThrows(TooLongFrameException.class, () -> channel.writeInbound(frame));
    }

    @Test
    void rejectsOversizedBodyBeforeAllocatingBuffer() {
        ByteBuf frame = Unpooled.buffer();
        frame.writeShort(ProtocolConstants.MAGIC);
        frame.writeByte(ProtocolConstants.VERSION);
        frame.writeInt(0);
        frame.writeShort(MessageType.REQUEST.getCode());
        frame.writeShort(0);
        frame.writeInt(ProtocolConstants.MAX_BODY_LENGTH + 1);

        assertThrows(TooLongFrameException.class, () -> channel.writeInbound(frame));
    }

    @Test
    void waitsForACompleteFrameWhenBodyIsFragmented() {
        ByteBuf partialFrame = Unpooled.buffer();
        partialFrame.writeShort(ProtocolConstants.MAGIC);
        partialFrame.writeByte(ProtocolConstants.VERSION);
        partialFrame.writeInt(0);
        partialFrame.writeShort(MessageType.REQUEST.getCode());
        partialFrame.writeShort(0);
        partialFrame.writeInt(4);
        partialFrame.writeByte(1);

        assertFalse(channel.writeInbound(partialFrame));
    }
}
