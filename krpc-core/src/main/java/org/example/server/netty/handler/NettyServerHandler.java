package org.example.server.netty.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.example.KRpcApplication;
import org.example.common.message.RequestType;
import org.example.common.message.RpcRequest;
import org.example.common.message.RpcResponse;
import org.example.common.trace.TraceContext;
import org.example.server.executor.RpcRequestExecutor;
import org.example.server.provider.ServiceProvider;
import org.example.server.ratelimit.RateLimit;
import org.example.trace.interceptor.ServerTraceInterceptor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Slf4j
public class NettyServerHandler extends SimpleChannelInboundHandler<RpcRequest> {
    private final ServiceProvider serviceProvider;
    private final RpcRequestExecutor requestExecutor;

    public NettyServerHandler(ServiceProvider serviceProvider, RpcRequestExecutor requestExecutor) {
        this.serviceProvider = serviceProvider;
        this.requestExecutor = requestExecutor;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RpcRequest rpcRequest) {
        if (rpcRequest == null) {
            log.error("Illegal request: request is null");
            return;
        }

        if (RequestType.HEARTBEAT == rpcRequest.getRequestType()) {
            log.debug("Received heartbeat from {}", channelHandlerContext.channel().remoteAddress());
            return;
        }

        boolean accepted = requestExecutor.submit(() -> handleRequest(channelHandlerContext, rpcRequest));
        if (!accepted) {
            RpcResponse response = RpcResponse.fail(503, "server overloaded");
            response.setRequestId(rpcRequest.getRequestId());
            channelHandlerContext.writeAndFlush(response);
            log.warn("Business executor overloaded, active={}, queued={}, service={}",
                    requestExecutor.activeCount(), requestExecutor.queueSize(), rpcRequest.getInterfaceName());
        }
    }

    private void handleRequest(ChannelHandlerContext context, RpcRequest rpcRequest) {
        boolean tracingEnabled = Boolean.TRUE.equals(KRpcApplication.getRpcConfig().getTracingEnabled());
        if (tracingEnabled) {
            ServerTraceInterceptor.beforeHandle(rpcRequest.getTraceId(), rpcRequest.getSpanId());
        } else {
            TraceContext.clear();
        }
        try {
            RpcResponse response;
            try {
                response = getResponse(rpcRequest);
            } catch (RuntimeException e) {
                log.error("Unhandled business dispatch failure, service={}, method={}",
                        rpcRequest.getInterfaceName(), rpcRequest.getMethodName(), e);
                response = RpcResponse.fail(500, "server dispatch error");
            }
            response.setRequestId(rpcRequest.getRequestId());
            context.writeAndFlush(response);
        } finally {
            if (tracingEnabled) {
                ServerTraceInterceptor.afterHandle(rpcRequest.getMethodName());
            } else {
                TraceContext.clear();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Netty server handler caught exception", cause);
        ctx.close();
    }

    RpcResponse getResponse(RpcRequest rpcRequest) {
        String interfaceName = rpcRequest.getInterfaceName();
        if (interfaceName == null || interfaceName.isEmpty()) {
            return RpcResponse.fail("interfaceName is empty");
        }
        if (serviceProvider.isServiceDegraded(interfaceName)) {
            log.warn("Service degraded, reject request, interface={}", interfaceName);
            return RpcResponse.fail(503, "service degraded");
        }

        RateLimit rateLimit = serviceProvider.getRateLimitProvider().getRateLimit(interfaceName);
        if (!rateLimit.getToken()) {
            log.warn("Rate limited on interface={}", interfaceName);
            return RpcResponse.fail(429, "rate limited");
        }

        Object service = serviceProvider.getService(interfaceName);
        if (service == null) {
            log.error("Service not found, interfaceName={}", interfaceName);
            return RpcResponse.fail("service not found");
        }

        String methodName = rpcRequest.getMethodName();
        try {
            Method method = service.getClass().getMethod(methodName, rpcRequest.getParamsType());
            Object invoke = method.invoke(service, rpcRequest.getParams());
            serviceProvider.recordInvokeSuccess(interfaceName);
            return RpcResponse.success(invoke);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            serviceProvider.recordInvokeFailure(interfaceName);
            log.error("Method invoke failed, interfaceName={}, methodName={}", interfaceName, methodName, e);
            return RpcResponse.fail();
        }
    }
}
