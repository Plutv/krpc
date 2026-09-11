package org.example.server.serviceRegister.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.example.KRpcApplication;
import org.example.config.KRpcConfig;
import org.example.server.serviceRegister.ServiceRegister;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ZKServiceRegister implements ServiceRegister {
    private static final String ROOT_PATH = "MyRpc";
    private static final String RETRY_PATH = "CanRetry";
    private static final byte[] EMPTY_DATA = new byte[0];

    private final CuratorFramework client;
    private final ConcurrentMap<String, PersistentNode> managedNodes = new ConcurrentHashMap<>();

    public ZKServiceRegister() {
        RetryPolicy policy = new ExponentialBackoffRetry(1000, 3);
        KRpcConfig config = KRpcApplication.getRpcConfig();
        this.client = CuratorFrameworkFactory.builder()
                .connectString(config.getRegistryAddress())
                .sessionTimeoutMs(config.getRegistrySessionTimeoutMillis())
                .retryPolicy(policy)
                .namespace(ROOT_PATH)
                .build();
        this.client.start();
        log.info("zookeeper connected");
    }

    @Override
    public void register(String serviceName, InetSocketAddress serviceAddress, boolean canRetry) {
        try {
            String serviceRoot = "/" + serviceName;
            ensurePersistentPath(serviceRoot);
            String servicePath = serviceRoot + "/" + getServiceAddress(serviceAddress);
            startManagedNode(servicePath);

            if (canRetry) {
                String retryRoot = "/" + RETRY_PATH;
                ensurePersistentPath(retryRoot);
                String retryServiceRoot = retryRoot + "/" + serviceName;
                if (client.checkExists().forPath(retryServiceRoot) != null
                        && client.checkExists().forPath(retryServiceRoot).getEphemeralOwner() != 0L) {
                    return;
                }
                ensurePersistentPath(retryServiceRoot);
                String retryPath = retryServiceRoot + "/" + getServiceAddress(serviceAddress);
                startManagedNode(retryPath);
            }
        } catch (Exception e) {
            log.error("Register service failed, service={}, address={}", serviceName, serviceAddress, e);
        }
    }

    @Override
    public void unregister(String serviceName, InetSocketAddress serviceAddress, boolean canRetry) {
        try {
            String servicePath = "/" + serviceName + "/" + getServiceAddress(serviceAddress);
            closeManagedNode(servicePath);

            if (canRetry) {
                String retryPath = "/" + RETRY_PATH + "/" + serviceName + "/"
                        + getServiceAddress(serviceAddress);
                closeManagedNode(retryPath);
            }
        } catch (Exception e) {
            log.error("Unregister service failed, service={}, address={}", serviceName, serviceAddress, e);
        }
    }

    private void ensurePersistentPath(String path) throws Exception {
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
        }
    }

    private void startManagedNode(String path) throws Exception {
        PersistentNode node = new PersistentNode(client, CreateMode.EPHEMERAL, false, path, EMPTY_DATA);
        PersistentNode existing = managedNodes.putIfAbsent(path, node);
        if (existing != null) {
            return;
        }

        try {
            node.start();
            if (!node.waitForInitialCreate(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out creating registry node: " + path);
            }
        } catch (Exception e) {
            managedNodes.remove(path, node);
            node.close();
            throw e;
        }
    }

    private void closeManagedNode(String path) throws Exception {
        PersistentNode node = managedNodes.remove(path);
        if (node != null) {
            node.close();
            return;
        }
        if (client.checkExists().forPath(path) != null) {
            client.delete().forPath(path);
        }
    }

    private String getServiceAddress(InetSocketAddress serviceAddress) {
        return serviceAddress.getHostString() + ":" + serviceAddress.getPort();
    }

    @Override
    public void close() {
        for (PersistentNode node : new ArrayList<>(managedNodes.values())) {
            try {
                node.close();
            } catch (Exception e) {
                log.warn("Close managed registry node failed", e);
            }
        }
        managedNodes.clear();
        client.close();
    }
}
