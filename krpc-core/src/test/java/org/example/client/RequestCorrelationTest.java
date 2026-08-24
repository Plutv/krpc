package org.example.client;

import io.netty.channel.embedded.EmbeddedChannel;
import org.example.client.netty.PendingRequests;
import org.example.client.netty.handler.NettyClientHandler;
import org.example.common.message.RpcResponse;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Request correlation and lifecycle test.
 *
 * The single-connection multiplexing design keys every in-flight call by requestId in a shared map.
 * This test proves the correlation is correct under out-of-order and concurrent responses, and that
 * entries are removed so the map never leaks (which would otherwise grow without bound on a long-running client).
 */
class RequestCorrelationTest {

    @Test
    void completesEachFutureWithMatchingRequestIdRegardlessOfOrder() throws Exception {
        int n = 200;
        List<String> ids = new ArrayList<String>();
        for (int i = 0; i < n; i++) {
            ids.add("corr-" + i);
        }

        List<CompletableFuture<RpcResponse>> futures = new ArrayList<CompletableFuture<RpcResponse>>();
        for (String id : ids) {
            CompletableFuture<RpcResponse> f = new CompletableFuture<RpcResponse>();
            PendingRequests.put(id, f);
            futures.add(f);
        }

        // Server replies out of order (reversed): correlation must still match by requestId.
        for (int i = n - 1; i >= 0; i--) {
            RpcResponse r = RpcResponse.success("data-" + i);
            r.setRequestId(ids.get(i));
            PendingRequests.complete(r);
        }

        for (int i = 0; i < n; i++) {
            RpcResponse r = futures.get(i).get();
            assertEquals("data-" + i, r.getData());
            assertEquals(ids.get(i), r.getRequestId());
        }
        assertEquals(0, PendingRequests.size(), "all futures must be removed after completion");
    }

    @Test
    void failCompletesExceptionallyAndRemovesEntry() {
        String id = "fail-1";
        CompletableFuture<RpcResponse> f = new CompletableFuture<RpcResponse>();
        PendingRequests.put(id, f);
        PendingRequests.fail(id, new RuntimeException("boom"));

        assertTrue(f.isCompletedExceptionally());
        assertEquals(0, PendingRequests.size(), "failed entry must be removed");
    }

    @Test
    void channelCloseFailsAllRequestsBoundToThatConnection() {
        EmbeddedChannel channel = new EmbeddedChannel(new NettyClientHandler());
        CompletableFuture<RpcResponse> first = new CompletableFuture<RpcResponse>();
        CompletableFuture<RpcResponse> second = new CompletableFuture<RpcResponse>();
        PendingRequests.put("closed-1", first, channel);
        PendingRequests.put("closed-2", second, channel);

        channel.close();

        assertTrue(first.isCompletedExceptionally());
        assertTrue(second.isCompletedExceptionally());
        assertEquals(0, PendingRequests.size(), "channel close must not leave calls waiting for timeout");
    }

    @Test
    void lateResponseForUnknownRequestIdIsIgnored() {
        // A response arriving after timeout/cleanup (requestId no longer pending) must not throw.
        RpcResponse late = RpcResponse.success("x");
        late.setRequestId("ghost");
        PendingRequests.complete(late);
        assertEquals(0, PendingRequests.size());
    }

    @Test
    void concurrentCorrelationDoesNotCrossTalk() throws Exception {
        int n = 500;
        List<String> ids = new ArrayList<String>();
        for (int i = 0; i < n; i++) {
            ids.add("cc-" + i);
        }

        List<CompletableFuture<RpcResponse>> futures = new ArrayList<CompletableFuture<RpcResponse>>();
        for (String id : ids) {
            CompletableFuture<RpcResponse> f = new CompletableFuture<RpcResponse>();
            PendingRequests.put(id, f);
            futures.add(f);
        }

        // Server thread completes in random order.
        List<String> shuffled = new ArrayList<String>(ids);
        Collections.shuffle(shuffled);
        CountDownLatch latch = new CountDownLatch(n);
        for (final String id : shuffled) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    RpcResponse r = RpcResponse.success("v-" + id);
                    r.setRequestId(id);
                    PendingRequests.complete(r);
                    latch.countDown();
                }
            }).start();
        }
        latch.await();

        for (int i = 0; i < n; i++) {
            String id = "cc-" + i;
            RpcResponse r = futures.get(i).get();
            assertEquals("v-" + id, r.getData(), "future must receive its own payload, not a cross-talk");
        }
        assertEquals(0, PendingRequests.size(), "no leak after concurrent completion");
    }
}
