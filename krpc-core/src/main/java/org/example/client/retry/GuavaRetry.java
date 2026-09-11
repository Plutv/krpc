package org.example.client.retry;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.AttemptTimeLimiters;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import lombok.extern.slf4j.Slf4j;
import org.example.client.rpcClient.RpcClient;
import org.example.common.message.RpcRequest;
import org.example.common.message.RpcResponse;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class GuavaRetry {
    private static final int MAX_ATTEMPTS = 3;
    private static final long WAIT_MILLIS = 500L;
    private static final long ATTEMPT_TIMEOUT_MILLIS = 6000L;

    public RpcResponse sendServiceWithRetry(RpcRequest request, RpcClient client) {
        Retryer<RpcResponse> retryer = RetryerBuilder.<RpcResponse>newBuilder()
                .retryIfException()
                .retryIfResult(response -> response == null || response.getCode() >= 500)
                .withAttemptTimeLimiter(AttemptTimeLimiters.fixedTimeLimit(ATTEMPT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                .withWaitStrategy(WaitStrategies.fixedWait(WAIT_MILLIS, TimeUnit.MILLISECONDS))
                .withStopStrategy(StopStrategies.stopAfterAttempt(MAX_ATTEMPTS))
                .withRetryListener(new RetryListener() {
                    @Override
                    public <V> void onRetry(Attempt<V> attempt) {
                        log.warn("Retrying request, attempt={}, hasException={}",
                                attempt.getAttemptNumber(), attempt.hasException());
                    }
                })
                .build();
        try {
            return retryer.call(() -> client.sendRequest(request));
        } catch (ExecutionException | RetryException e) {
            log.error("Retry failed, interface={}, method={}", request.getInterfaceName(), request.getMethodName(), e);
            return RpcResponse.fail("retry failed");
        }
    }
}
