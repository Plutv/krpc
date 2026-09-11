package org.example.client.proxy;

import lombok.AllArgsConstructor;
import org.example.client.circuitBreaker.CircuitBreaker;
import org.example.client.circuitBreaker.CircuitBreakerProvider;
import org.example.client.retry.GuavaRetry;
import org.example.client.serviceCenter.ServiceCenter;
import org.example.client.serviceCenter.ZkServiceCenter;
import org.example.common.message.RpcRequest;
import org.example.common.message.RpcResponse;
import org.example.client.rpcClient.impl.NettyRpcClient;
import org.example.client.rpcClient.RpcClient;
import org.example.client.rpcClient.impl.SimpleSocketRpcClient;
import org.example.trace.interceptor.ClientTraceInterceptor;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;

@AllArgsConstructor
public class ClientProxy implements InvocationHandler {

    private RpcClient rpcClient;

    private ServiceCenter serviceCenter;

    private CircuitBreakerProvider circuitBreakerProvider;

    public ClientProxy() throws InterruptedException {
        serviceCenter = new ZkServiceCenter();
        rpcClient = new NettyRpcClient(serviceCenter);
        circuitBreakerProvider = new CircuitBreakerProvider();
    }

    public ClientProxy(String host, int port, int choose) {
        circuitBreakerProvider = new CircuitBreakerProvider();
        switch (choose) {
            case 0:
                serviceCenter = buildFixedServiceCenter(host, port);
                rpcClient = new NettyRpcClient(serviceCenter);
                break;
            case 1:
                rpcClient = new SimpleSocketRpcClient(host, port);
                break;
            default:
                throw new IllegalArgumentException("Unsupported rpc client type: " + choose);
        }
    }

    public ClientProxy(String host, int port) {
        circuitBreakerProvider = new CircuitBreakerProvider();
        serviceCenter = buildFixedServiceCenter(host, port);
        rpcClient = new NettyRpcClient(serviceCenter);
    }


    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        ClientTraceInterceptor.beforeInvoke();
        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(this, args);
        }

        RpcRequest request = RpcRequest.builder().interfaceName(method.getDeclaringClass().getName())
                .methodName(method.getName())
                .params(args)
                .paramsType(method.getParameterTypes()).build();

        String breakerKey = request.getInterfaceName() + "#" + request.getMethodName();
        CircuitBreaker circuitBreaker = circuitBreakerProvider.getCircuitBreaker(breakerKey);

        if (!circuitBreaker.allowRequest()) {
            return null;
        }

        try {
            RpcResponse response;
            if (serviceCenter != null && serviceCenter.checkRetry(request.getInterfaceName())) {
                response = new GuavaRetry().sendServiceWithRetry(request, rpcClient);
            } else {
                response = rpcClient.sendRequest(request);
            }

            if (response != null && response.getCode() == 200) {
                circuitBreaker.recordSuccess();
            } else {
                circuitBreaker.recordFailure();
            }

            return response == null ? null : response.getData();
        } catch (Throwable t) {
            circuitBreaker.recordFailure();
            throw t;
        } finally {
            ClientTraceInterceptor.afterInvoke(method.getName());
        }
    }

    public <T>T getProxy(Class<T> clazz) {
        Object o = Proxy.newProxyInstance(clazz.getClassLoader(), new Class[]{clazz}, this);
        return (T)o;
    }

    private ServiceCenter buildFixedServiceCenter(String host, int port) {
        return new ServiceCenter() {
            @Override
            public InetSocketAddress serviceDiscovery(String serviceName) {
                return new InetSocketAddress(host, port);
            }

            @Override
            public boolean checkRetry(String serviceName) {
                return false;
            }
        };
    }
}
