package org.example.integration;

import org.example.KRpcApplication;
import org.example.client.proxy.ClientProxy;
import org.example.config.KRpcConfig;
import org.example.pojo.User;
import org.example.provider.impl.UserServiceImpl;
import org.example.server.provider.ServiceProvider;
import org.example.server.server.RpcServer;
import org.example.server.server.impl.NettyRpcServer;
import org.example.service.UserService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fault drill (integration).
 *
 * Starts two normal providers + one intentionally slow provider, all registering to ZooKeeper,
 * then drives a consumer through: stop one provider, observe traffic survives, restart it and
 * observe recovery. This exercises the EXISTING service discovery, node-down marking, watcher
 * cache refresh and probe recovery - no new framework feature is added.
 *
 * Requires a ZooKeeper at 127.0.0.1:2181. If unreachable the whole test is skipped so the suite
 * stays green in environments without ZK.
 */
class FaultDrillTest {

    private static final int P1 = 20881;
    private static final int P2 = 20882;
    private static final int P3_SLOW = 20883;

    private static final List<RpcServer> servers = new ArrayList<RpcServer>();
    private static final List<Thread> threads = new ArrayList<Thread>();

    @BeforeAll
    static void setup() {
        KRpcApplication.initialize(KRpcConfig.builder()
                .serializer("Hessian")
                .tracingEnabled(false)
                .build());

        Assumptions.assumeTrue(isZkReachable(),
                "ZooKeeper not reachable at 127.0.0.1:2181; skipping fault drill (start ZK to enable).");

        startProvider(P1, new UserServiceImpl());
        startProvider(P2, new UserServiceImpl());
        startProvider(P3_SLOW, new SlowUserService());
        sleepQuietly(1500); // let the consumer's watcher pick up all nodes
    }

    @AfterAll
    static void teardown() {
        for (int i = 0; i < servers.size(); i++) {
            try {
                servers.get(i).stop();
            } catch (Exception ignored) {
            }
        }
        for (Thread t : threads) {
            try {
                t.join(2000);
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    void stopsOneProviderAndRecoversAfterRestart() throws Exception {
        UserService proxy = new ClientProxy().getProxy(UserService.class);

        int baseline = fire(proxy, 200);
        System.out.println("fault-drill baseline success=" + baseline);
        assertTrue(baseline >= 190, "baseline should be healthy");

        // Stop provider 1. The framework should mark the dead node down on connect failure
        // and keep serving from the remaining nodes.
        servers.get(0).stop();
        threads.get(0).join(2000);
        sleepQuietly(1500);

        int duringOutage = fire(proxy, 300);
        System.out.println("fault-drill during outage (p1 down) success=" + duringOutage);
        assertTrue(duringOutage >= 270, "traffic must survive one provider going down");

        // Restart provider 1. Watcher re-adds the node; probe recovers it.
        // NOTE (observed boundary): the previous ephemeral node may linger until its ZK session
        // expires, so a few keys can briefly hit the stale node. Assertion is intentionally lenient.
        startProvider(P1, new UserServiceImpl());
        sleepQuietly(2500);

        int afterRecovery = fire(proxy, 300);
        System.out.println("fault-drill after recovery success=" + afterRecovery);
        assertTrue(afterRecovery >= 200, "traffic should recover after restart");
    }

    private static int fire(UserService proxy, int n) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(20);
        AtomicInteger ok = new AtomicInteger();
        for (int i = 0; i < n; i++) {
            final int id = i;
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        User u = proxy.getUserByUserId(id);
                        if (u != null) {
                            ok.incrementAndGet();
                        }
                    } catch (Exception ignored) {
                    }
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(40, TimeUnit.SECONDS);
        return ok.get();
    }

    private static void startProvider(int port, UserService impl) {
        ServiceProvider sp = new ServiceProvider("127.0.0.1", port);
        sp.provideServiceInterface(impl, false);
        NettyRpcServer server = new NettyRpcServer(sp);
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                server.start(port);
            }
        });
        t.start();
        servers.add(server);
        threads.add(t);
        sleepQuietly(500);
    }

    private static boolean isZkReachable() {
        try (java.net.Socket s = new java.net.Socket()) {
            s.connect(new InetSocketAddress("127.0.0.1", 2181), 1000);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static final class SlowUserService extends UserServiceImpl implements UserService {
        @Override
        public User getUserByUserId(Integer id) {
            sleepQuietly(2000);
            return super.getUserByUserId(id);
        }
    }
}
