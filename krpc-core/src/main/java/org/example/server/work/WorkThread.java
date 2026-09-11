package org.example.server.work;

import lombok.AllArgsConstructor;
import org.example.common.message.RpcRequest;
import org.example.common.message.RpcResponse;
import org.example.server.provider.ServiceProvider;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;

@AllArgsConstructor
public class WorkThread implements Runnable {
    private final Socket socket;
    private final ServiceProvider serviceProvider;

    @Override
    public void run() {
        try {
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            RpcRequest rpcRequest = (RpcRequest) ois.readObject();
            RpcResponse rpcResponse = getResponse(rpcRequest);
            oos.writeObject(rpcResponse);
            oos.flush();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private RpcResponse getResponse(RpcRequest rpcRequest) {
        Object service = serviceProvider.getService(rpcRequest.getInterfaceName());
        String methodName = rpcRequest.getMethodName();
        try {
            Method method = service.getClass().getMethod(methodName, rpcRequest.getParamsType());
            Object invoke = method.invoke(service, rpcRequest.getParams());
            RpcResponse response = RpcResponse.success(invoke);
            response.setRequestId(rpcRequest.getRequestId());
            return response;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
            RpcResponse response = RpcResponse.fail();
            response.setRequestId(rpcRequest.getRequestId());
            return response;
        }
    }
}
