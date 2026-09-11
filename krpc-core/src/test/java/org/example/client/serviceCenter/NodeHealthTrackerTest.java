package org.example.client.serviceCenter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeHealthTrackerTest {

    @Test
    void quarantinesOnlyAfterConsecutiveFailures() {
        NodeHealthTracker tracker = new NodeHealthTracker(3);

        assertFalse(tracker.recordFailure("service", "127.0.0.1:9000"));
        assertFalse(tracker.recordFailure("service", "127.0.0.1:9000"));
        assertTrue(tracker.recordFailure("service", "127.0.0.1:9000"));
    }

    @Test
    void successResetsFailureStreak() {
        NodeHealthTracker tracker = new NodeHealthTracker(2);

        assertFalse(tracker.recordFailure("service", "127.0.0.1:9000"));
        tracker.recordSuccess("service", "127.0.0.1:9000");
        assertFalse(tracker.recordFailure("service", "127.0.0.1:9000"));
    }
}
