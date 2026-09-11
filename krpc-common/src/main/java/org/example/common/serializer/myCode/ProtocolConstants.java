package org.example.common.serializer.myCode;

public final class ProtocolConstants {
    public static final short MAGIC = (short) 0xCAFE;
    public static final byte VERSION = 1;
    public static final int MAX_TRACE_LENGTH = 4 * 1024;
    public static final int MAX_BODY_LENGTH = 8 * 1024 * 1024;

    private ProtocolConstants() {
    }
}
