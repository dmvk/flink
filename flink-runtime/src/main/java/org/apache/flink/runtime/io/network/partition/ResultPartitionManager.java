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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
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

	private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionManager.class);

	private final ConcurrentMap<ResultPartitionID, ResultPartition> registeredPartitions =
		new ConcurrentHashMap<>(16);

	private AtomicBoolean isShutdown = new AtomicBoolean(false);

	public void registerResultPartition(ResultPartition partition) {
		checkState(!isShutdown.get(), "Result partition manager already shut down.");
		final ResultPartition previous =
			registeredPartitions.put(partition.getPartitionId(), partition);
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
		final ResultPartition partition = registeredPartitions.get(partitionId);
		if (partition == null) {
			throw new PartitionNotFoundException(partitionId);
		}
		return partition.createSubpartitionView(subpartitionIndex, availabilityListener);
	}

	public void releasePartition(ResultPartitionID partitionId, Throwable cause) {
		final ResultPartition partition = registeredPartitions.remove(partitionId);
		if (partition != null) {
			partition.release(cause);
			LOG.debug("Released partition {} produced by {}.",
				partitionId.getPartitionId(), partitionId.getProducerId());
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
		return new HashSet<>(registeredPartitions.keySet());
	}
}
