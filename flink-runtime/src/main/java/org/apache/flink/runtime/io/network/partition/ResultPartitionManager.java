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

package org.apache.flink.runtime.io.network.partition;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The result partition manager keeps track of all currently produced/consumed partitions of a
 * task manager.
 */
public class ResultPartitionManager implements ResultPartitionProvider {

	private static final int DEBUG_THRESHOLD_MS = 100;

	private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionManager.class);

	interface LockedFn<T, R> {

		R apply(T value) throws IOException;
	}

	private static class LockedResultPartition {

		private final ReentrantLock lock = new ReentrantLock();
		private final ResultPartition resultPartition;

		LockedResultPartition(ResultPartition resultPartition) {
			this.resultPartition = resultPartition;
		}

		<T> T executeLocked(String action, LockedFn<ResultPartition, T> function) throws IOException {
			final long startTime = System.currentTimeMillis();
//			lock.lock();
			final long lockDuration = System.currentTimeMillis() - startTime;
			try {
				final T result = function.apply(resultPartition);
				final long totalDuration = System.currentTimeMillis() - startTime;
				if (totalDuration >= DEBUG_THRESHOLD_MS) {
					LOG.warn("Action [{}] for [{}] was waiting [{}] for lock and took [{}] to execute.",
						action,
						resultPartition.getPartitionId(),
						lockDuration,
						totalDuration);
				}
				return result;
			} finally {
//				lock.unlock();
			}
		}
	}

	private final ConcurrentMap<ResultPartitionID, LockedResultPartition> registeredPartitions =
		new ConcurrentHashMap<>(16);

	private AtomicBoolean isShutdown = new AtomicBoolean(false);

	public void registerResultPartition(ResultPartition partition) {
		checkState(!isShutdown.get(), "Result partition manager already shut down.");
		final LockedResultPartition previous =
			registeredPartitions.put(
				partition.getPartitionId(),
				new LockedResultPartition(partition));
		if (previous != null) {
			throw new IllegalStateException("Result partition already registered.");
		}
		LOG.info(
			"Registered partition [{}] with [{}] subpartitions. Total number of registered partitions [{}].",
			partition.getPartitionId(),
			partition.getNumberOfSubpartitions(),
			registeredPartitions.size());
	}

	@Override
	public ResultSubpartitionView createSubpartitionView(
			ResultPartitionID partitionId,
			int subpartitionIndex,
			BufferAvailabilityListener availabilityListener) throws IOException {
		final LockedResultPartition lockedPartition = registeredPartitions.get(partitionId);
		if (lockedPartition == null) {
			throw new PartitionNotFoundException(partitionId);
		}
		return lockedPartition.executeLocked("createSubpartitionView", (partition) -> {
			LOG.debug("Requesting subpartition {} of {}.", subpartitionIndex, partition);
			return partition.createSubpartitionView(subpartitionIndex, availabilityListener);
		});
	}

	public void releasePartition(ResultPartitionID partitionId, Throwable cause) {
		final LockedResultPartition lockedPartition = registeredPartitions.remove(partitionId);
		if (lockedPartition != null) {
			try {
				lockedPartition.executeLocked("releasePartition", (partition) -> {
					partition.release(cause);
					LOG.debug("Released partition {} produced by {}.",
						partitionId.getPartitionId(), partitionId.getProducerId());
					return null;
				});
			} catch (IOException e) {
				throw new IllegalStateException("This should never happen...", e);
			}
		}
	}

	public void shutdown() {
		if (!isShutdown.getAndSet(true)) {
			LOG.debug("Releasing {} partitions because of shutdown.",
				registeredPartitions.values().size());
			for (ResultPartitionID partitionId : getUnreleasedPartitions()) {
				releasePartition(partitionId, null);
			}
			Preconditions.checkState(registeredPartitions.isEmpty());
		}
	}

	// ------------------------------------------------------------------------
	// Notifications
	// ------------------------------------------------------------------------

	void onConsumedPartition(ResultPartition partition) {
		LOG.debug("Received consume notification from {}.", partition);
		releasePartition(partition.getPartitionId(), null);
	}

	public Collection<ResultPartitionID> getUnreleasedPartitions() {
		final long startTime = System.currentTimeMillis();
		final Set<ResultPartitionID> keySet = new HashSet<>(registeredPartitions.keySet());
		LOG.info("Block check - getUnreleasedPartitions: {}",
			System.currentTimeMillis() - startTime);
		return keySet;
	}
}
