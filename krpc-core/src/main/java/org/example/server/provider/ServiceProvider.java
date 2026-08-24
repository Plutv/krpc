package org.example.server.provider;

import lombok.extern.slf4j.Slf4j;
import org.example.server.ratelimit.provider.RateLimitProvider;
import org.example.server.serviceRegister.ServiceRegister;
import org.example.server.serviceRegister.impl.ZKServiceRegister;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ServiceProvider {
    private static final int FAILURE_DEGRADE_THRESHOLD = 5;

    private final Map<String, Object> interfaceProvider;
    private final Map<String, Boolean> retryServiceMap;
    private final Map<String, AtomicInteger> failureCounter;
    private final Set<String> degradedServices;

    private final int port;
    private final String host;
    private final ServiceRegister serviceRegister;
    private final RateLimitProvider rateLimitProvider;
    private final AtomicBoolean closed = new AtomicBoolean();

    public ServiceProvider(String host, int port) {
        this(host, port, new ZKServiceRegister(), new RateLimitProvider());
    }

    public ServiceProvider(String host, int port, ServiceRegister serviceRegister,
                           RateLimitProvider rateLimitProvider) {
        this.host = host;
        this.port = port;
        this.interfaceProvider = new ConcurrentHashMap<>();
        this.retryServiceMap = new ConcurrentHashMap<>();
        this.failureCounter = new ConcurrentHashMap<>();
        this.degradedServices = ConcurrentHashMap.newKeySet();
        this.serviceRegister = serviceRegister;
        this.rateLimitProvider = rateLimitProvider;
    }

    public ServiceProvider() {
        this("127.0.0.1", 9999);
    }

    public void provideServiceInterface(Object service, boolean canRetry) {
        Class<?>[] interfaceClasses = service.getClass().getInterfaces();
        for (Class<?> clazz : interfaceClasses) {
            String serviceName = clazz.getName();
            interfaceProvider.put(serviceName, service);
            retryServiceMap.put(serviceName, canRetry);
            serviceRegister.register(serviceName, new InetSocketAddress(host, port), canRetry);
            log.info("Register service success, service={}, canRetry={}", serviceName, canRetry);
        }
    }

    public Object getService(String interfaceName) {
        return interfaceProvider.get(interfaceName);
    }

    public RateLimitProvider getRateLimitProvider() {
        return rateLimitProvider;
    }

    public boolean isServiceDegraded(String interfaceName) {
        return degradedServices.contains(interfaceName);
    }

    public void recordInvokeSuccess(String interfaceName) {
        failureCounter.remove(interfaceName);
    }

    public void recordInvokeFailure(String interfaceName) {
        int failed = failureCounter
                .computeIfAbsent(interfaceName, key -> new AtomicInteger(0))
                .incrementAndGet();
        if (failed >= FAILURE_DEGRADE_THRESHOLD) {
            degradeService(interfaceName);
        }
    }

    private void degradeService(String interfaceName) {
        if (!degradedServices.add(interfaceName)) {
            return;
        }
        boolean canRetry = Boolean.TRUE.equals(retryServiceMap.get(interfaceName));
        InetSocketAddress address = new InetSocketAddress(host, port);
        serviceRegister.unregister(interfaceName, address, canRetry);
        log.error("Service degraded and unregistered after consecutive failures, service={}, address={}",
                interfaceName, host + ":" + port);
    }

    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        InetSocketAddress address = new InetSocketAddress(host, port);
        for (String interfaceName : interfaceProvider.keySet()) {
            serviceRegister.unregister(interfaceName, address,
                    Boolean.TRUE.equals(retryServiceMap.get(interfaceName)));
        }
        serviceRegister.close();
    }
}
