package org.example.client.rpcClient.impl;

import org.example.common.message.RpcRequest;
import org.example.common.message.RpcResponse;
import org.example.client.rpcClient.RpcClient;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.UUID;

public class SimpleSocketRpcClient implements RpcClient {
    private String host;
    private int port;

    public SimpleSocketRpcClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public RpcResponse sendRequest(RpcRequest rpcRequest) {
        try {
            if (rpcRequest.getRequestId() == null || rpcRequest.getRequestId().isEmpty()) {
                rpcRequest.setRequestId(UUID.randomUUID().toString());
            }
            Socket socket = new Socket(host, port);
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            oos.writeObject(rpcRequest);
            oos.flush();
            RpcResponse response = (RpcResponse) ois.readObject();
            return response;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}
