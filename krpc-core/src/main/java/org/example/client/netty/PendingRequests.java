package org.example.client.netty;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.example.common.message.RpcResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public final class PendingRequests {
    private static final ConcurrentMap<String, PendingRequest> REQUEST_MAP = new ConcurrentHashMap<>();
    private static final AtomicLong LATE_RESPONSE_COUNT = new AtomicLong();

    private PendingRequests() {
    }

    public static void put(String requestId, CompletableFuture<RpcResponse> future) {
        put(requestId, future, null);
    }

    public static void put(String requestId, CompletableFuture<RpcResponse> future, Channel channel) {
        String channelId = channel == null ? null : channel.id().asLongText();
        REQUEST_MAP.put(requestId, new PendingRequest(future, channelId));
    }

    public static void complete(RpcResponse response) {
        if (response == null || response.getRequestId() == null) {
            return;
        }
        PendingRequest pendingRequest = REQUEST_MAP.remove(response.getRequestId());
        if (pendingRequest != null) {
            pendingRequest.future.complete(response);
            return;
        }
        LATE_RESPONSE_COUNT.incrementAndGet();
        log.debug("No pending request found for requestId={}", response.getRequestId());
    }

    public static void fail(String requestId, Throwable throwable) {
        PendingRequest pendingRequest = REQUEST_MAP.remove(requestId);
        if (pendingRequest != null) {
            pendingRequest.future.completeExceptionally(throwable);
        }
    }

    public static int failChannel(Channel channel, Throwable throwable) {
        if (channel == null) {
            return 0;
        }

        String channelId = channel.id().asLongText();
        int failedCount = 0;
        for (ConcurrentMap.Entry<String, PendingRequest> entry : REQUEST_MAP.entrySet()) {
            PendingRequest pendingRequest = entry.getValue();
            if (channelId.equals(pendingRequest.channelId)
                    && REQUEST_MAP.remove(entry.getKey(), pendingRequest)
                    && pendingRequest.future.completeExceptionally(throwable)) {
                failedCount++;
            }
        }
        return failedCount;
    }

    public static void remove(String requestId) {
        REQUEST_MAP.remove(requestId);
    }

    /**
     * Read-only observability hook for tests: current number of in-flight requests.
     * Not used by production code paths.
     */
    public static int size() {
        return REQUEST_MAP.size();
    }

    public static long lateResponseCount() {
        return LATE_RESPONSE_COUNT.get();
    }

    static void clearForTest() {
        REQUEST_MAP.clear();
        LATE_RESPONSE_COUNT.set(0L);
    }

    private static final class PendingRequest {
        private final CompletableFuture<RpcResponse> future;
        private final String channelId;

        private PendingRequest(CompletableFuture<RpcResponse> future, String channelId) {
            this.future = future;
            this.channelId = channelId;
        }
    }
}
