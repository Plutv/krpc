package org.example.common.serializer.mySerializer;


import com.alibaba.fastjson.JSONObject;
import org.example.common.message.RpcRequest;
import org.example.common.message.RpcResponse;

public class JsonSerializer implements org.example.common.serializer.mySerializer.Serializer {
    @Override
    public byte[] serialize(Object obj) {
        byte[] bytes = JSONObject.toJSONBytes(obj);
        return bytes;
    }

    @Override
    public Object deserializer(byte[] bytes, int messageType) {
        Object obj = null;
        switch (messageType) {
            case 0:
                RpcRequest request = JSONObject.parseObject(bytes, RpcRequest.class);
                Object[] objects = new Object[request.getParamsType().length];
                for(int i = 0; i < objects.length; i++) {
                    Class<?> paramsType = request.getParamsType()[i];
                    if (!paramsType.isAssignableFrom(request.getParams()[i].getClass())) {
                        objects[i] = JSONObject.toJavaObject((JSONObject) request.getParams()[i], request.getParamsType()[i]);
                    } else {
                        objects[i] = request.getParams()[i];
                    }
                }
                request.setParams(objects);
                obj = request;
                break;
            case 1:
                RpcResponse response = JSONObject.parseObject(bytes, RpcResponse.class);
                Class<?> dataType = response.getDataType();
                if (dataType != null && response.getData() != null
                        && !dataType.isAssignableFrom(response.getData().getClass())) {
                    response.setData(JSONObject.toJavaObject((JSONObject)response.getData(), dataType));
                }
                obj = response;
                break;
            default:
                System.out.println("not support this message type!");
                throw new RuntimeException();
        }
        return obj;
    }

    @Override
    public int getType() {
        return 1;
    }

    @Override
    public String toString() {
        return "Json";
    }
}
