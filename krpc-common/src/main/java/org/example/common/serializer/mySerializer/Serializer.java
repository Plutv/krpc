package org.example.common.serializer.mySerializer;

public interface Serializer {
    byte[] serialize(Object obj);

    Object deserializer(byte[] bytes, int messageType);

    int getType();

    static Serializer getSerializerByCode(int code) {
        switch (code) {
            case 0:
                return new ObjectSerializer();
            case 1:
                return new JsonSerializer();
            case 2:
                return new HessianSerializer();
            case 3:
                return new ProtobufSerializer();
            default:
                return null;
        }
    }

    static Serializer getSerializerByName(String name) {
        if (name == null || name.trim().isEmpty()) {
            return new JsonSerializer();
        }

        String normalized = name.trim().toLowerCase();
        switch (normalized) {
            case "jdk":
            case "java":
            case "object":
                return new ObjectSerializer();
            case "json":
            case "fastjson":
                return new JsonSerializer();
            case "hessian":
                return new HessianSerializer();
            case "protobuf":
            case "protostuff":
            case "proto":
                return new ProtobufSerializer();
            default:
                throw new IllegalArgumentException("Unsupported serializer name: " + name);
        }
    }
}
