package org.example.common.serializer.mySerializer;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.example.common.message.MessageType;
import org.example.common.message.RpcRequest;
import org.example.common.message.RpcResponse;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProtobufSerializer implements Serializer {

    // 缓存 Schema，避免每次重新解析，提高性能
    private static final Map<Class<?>, Schema<?>> schemaCache = new ConcurrentHashMap<>();

    // 为 Protobuf 分配一个唯一的类型码，假设为 1
    // 0 可能被 JSON 或 Java 原生占用
    @Override
    public int getType() {
        return 3;
    }

    @Override
    public byte[] serialize(Object obj) {
        Class cls = obj.getClass();
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        try {
            Schema schema = getSchema(cls);
            return ProtostuffIOUtil.toByteArray(obj, schema, buffer);
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed", e);
        } finally {
            buffer.clear();
        }
    }

    /**
     * 在你的 Decoder 中，调用的是 serializer.deserializer(bytes, messageType)
     * 所以我们需要根据 messageType 判断要反序列化成 RpcRequest 还是 RpcResponse
     */
    @Override
    public Object deserializer(byte[] bytes, int messageType) {
        Class<?> cls;
        // 根据 messageType 确定目标类
        // 注意：这里假设 MessageType 枚举能对应到具体类，或者你需要这里硬编码判断
        if (messageType == MessageType.REQUEST.getCode()) {
            cls = RpcRequest.class;
        } else if (messageType == MessageType.RESPONSE.getCode()) {
            cls = RpcResponse.class;
        } else {
            throw new RuntimeException("Unknown message type code: " + messageType);
        }

        try {
            Schema schema = getSchema(cls);
            Object message = schema.newMessage();
            ProtostuffIOUtil.mergeFrom(bytes, message, schema);
            return message;
        } catch (Exception e) {
            throw new RuntimeException("Deserialization failed", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Schema getSchema(Class cls) {
        Schema schema = schemaCache.get(cls);
        if (schema == null) {
            schema = RuntimeSchema.getSchema(cls);
            if (schema != null) {
                schemaCache.put(cls, schema);
            }
        }
        return schema;
    }

    @Override
    public String toString() {
        return "Protobuf";
    }
}
