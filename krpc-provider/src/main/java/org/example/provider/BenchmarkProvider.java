package org.example.provider;

import org.example.KRpcApplication;
import org.example.config.KRpcConfig;
import org.example.pojo.User;
import org.example.provider.impl.UserServiceImpl;
import org.example.server.provider.ServiceProvider;
import org.example.server.ratelimit.RateLimit;
import org.example.server.ratelimit.provider.RateLimitProvider;
import org.example.server.server.RpcServer;
import org.example.server.server.impl.NettyRpcServer;
import org.example.server.serviceRegister.ServiceRegister;
import org.example.service.UserService;

import java.net.InetSocketAddress;

public class BenchmarkProvider {
    private static final int PORT = 9999;
    private static final RateLimit ALLOW_ALL = () -> true;

    public static void main(String[] args) {
        KRpcApplication.initialize(KRpcConfig.builder()
                .serializer("Hessian")
                .tracingEnabled(false)
                .build());

        ServiceProvider serviceProvider = new ServiceProvider(
                "127.0.0.1", PORT, new NoopServiceRegister(), new UnlimitedRateLimitProvider());
        serviceProvider.provideServiceInterface(new FixedUserService(), false);

        RpcServer rpcServer = new NettyRpcServer(serviceProvider);
        rpcServer.start(PORT);
    }

    public static final class FixedUserService extends UserServiceImpl implements UserService {
        private final User response = User.builder()
                .id(1)
                .username("benchmark-user")
                .gender(true)
                .build();

        @Override
        public User getUserByUserId(Integer id) {
            return response;
        }
    }

    private static final class UnlimitedRateLimitProvider extends RateLimitProvider {
        @Override
        public RateLimit getRateLimit(String interfaceName) {
            return ALLOW_ALL;
        }
    }

    private static final class NoopServiceRegister implements ServiceRegister {
        @Override
        public void register(String serviceName, InetSocketAddress serviceAddress, boolean canRetry) {
        }

        @Override
        public void unregister(String serviceName, InetSocketAddress serviceAddress, boolean canRetry) {
        }
    }
}
