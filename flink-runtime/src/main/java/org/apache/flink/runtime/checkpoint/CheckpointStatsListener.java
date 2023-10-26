package org.apache.flink.runtime.checkpoint;

public interface CheckpointStatsListener {

    default void onPendingCheckpointStats(PendingCheckpointStats stats) {
        // No-op.
    }

    default void onCompletedCheckpointStats(CompletedCheckpointStats stats) {
        // No-op.
    }

    default void onFailedCheckpointStats(FailedCheckpointStats stats) {
        // No-op.
    }

    default void onRestoredCheckpointStats(RestoredCheckpointStats stats) {
        // No-op.
    }
}
