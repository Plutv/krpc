package org.example.common.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RpcResponse implements Serializable {
    private String requestId;

    private int code;
    private String message;
    private Object data;
    private Class<?> dataType;

    public static RpcResponse success(Object data) {
        return RpcResponse.builder()
                .code(200)
                .message("success")
                .dataType(data == null ? Object.class : data.getClass())
                .data(data)
                .build();
    }

    public static RpcResponse fail() {
        return RpcResponse.builder().code(500).message("server error").build();
    }

    public static RpcResponse fail(String msg) {
        return RpcResponse.builder().code(500).message(msg).build();
    }

    public static RpcResponse fail(int code, String msg) {
        return RpcResponse.builder().code(code).message(msg).build();
    }
}
