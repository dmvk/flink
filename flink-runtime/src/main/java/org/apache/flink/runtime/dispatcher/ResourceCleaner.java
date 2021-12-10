/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * {@code GlobalCleanupStage} executes all passed {@link JobCleanup} instances on the given {@code
 * JobID}.
 */
public class ResourceCleaner {

    public static CompletableFuture<Void> asyncCleanup(
            ThrowingRunnable<Exception> runnable, Executor ioExecutor) {
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        runnable.run();
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                },
                ioExecutor);
    }

    public interface CleanupStage {

        /**
         * TODO Remove the JobCleanup interface. We should reason about threading mode of each one
         * of the implementations, so it's better to make an async execution their responsibility.
         */
        static CleanupStage of(JobCleanup jobCleanup) {
            return (jobId, ioExecutor) ->
                    asyncCleanup(() -> jobCleanup.cleanupJobData(jobId), ioExecutor);
        }

        static CleanupStage of(AutoCloseableAsync closeable) {
            return (jobId, ioExecutor) -> closeable.closeAsync();
        }

        CompletableFuture<Void> cleanupJobData(JobID jobId, Executor ioExecutor);
    }

    private final Executor ioExecutor;

    public ResourceCleaner(Executor ioExecutor) {
        this.ioExecutor = ioExecutor;
    }

    public CompletableFuture<Void> cleanup(JobID jobId, List<CleanupStage> cleanupStages) {
        final List<CompletableFuture<Void>> futures =
                cleanupStages.stream()
                        .map(cleanupStage -> cleanupStage.cleanupJobData(jobId, ioExecutor))
                        .collect(Collectors.toList());
        return FutureUtils.waitForAll(futures);
    }
}
