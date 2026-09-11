package org.example.server.ratelimit;

public class TokenBucketRateLimitImpl implements RateLimit {
    private final int capacity;
    private final int refillRatePerSecond;

    private double currentTokens;
    private long lastRefillNanos;

    public TokenBucketRateLimitImpl(int capacity, int refillRatePerSecond) {
        this.capacity = capacity;
        this.refillRatePerSecond = refillRatePerSecond;
        this.currentTokens = capacity;
        this.lastRefillNanos = System.nanoTime();
    }

    @Override
    public synchronized boolean getToken() {
        refill();
        if (currentTokens < 1d) {
            return false;
        }
        currentTokens -= 1d;
        return true;
    }

    private void refill() {
        long now = System.nanoTime();
        long elapsed = now - lastRefillNanos;
        if (elapsed <= 0) {
            return;
        }
        double tokensToAdd = (elapsed / 1_000_000_000d) * refillRatePerSecond;
        if (tokensToAdd <= 0d) {
            return;
        }
        currentTokens = Math.min(capacity, currentTokens + tokensToAdd);
        lastRefillNanos = now;
    }
}
