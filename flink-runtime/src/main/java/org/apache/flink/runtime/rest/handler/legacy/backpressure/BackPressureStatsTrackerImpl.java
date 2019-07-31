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

package org.apache.flink.runtime.rest.handler.legacy.backpressure;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


/**
 * Back pressure statistics tracker.
 *
 * <p>Back pressure is determined by sampling running tasks. If a task is
 * slowed down by back pressure it will be stuck in memory requests to a
 * {@link org.apache.flink.runtime.io.network.buffer.LocalBufferPool}.
 *
 * <p>The back pressured stack traces look like this:
 *
 * <pre>
 * java.lang.Object.wait(Native Method)
 * o.a.f.[...].LocalBufferPool.requestBuffer(LocalBufferPool.java:163)
 * o.a.f.[...].LocalBufferPool.requestBufferBlocking(LocalBufferPool.java:133) <--- BLOCKING
 * request
 * [...]
 * </pre>
 */
public class BackPressureStatsTrackerImpl {

	private static final Logger LOG = LoggerFactory.getLogger(BackPressureStatsTrackerImpl.class);

	/** Maximum stack trace depth for samples. */
	public static final int MAX_STACK_TRACE_DEPTH = 3;

	/** Expected class name for back pressure indicating stack trace element. */
	static final String EXPECTED_CLASS_NAME = "org.apache.flink.runtime.io.network.buffer.LocalBufferPool";

	/** Expected method name for back pressure indicating stack trace element. */
	static final String EXPECTED_METHOD_NAME = "requestBufferBuilderBlocking";

	/**
	 * Creates the back pressure stats from a stack trace sample.
	 *
	 * @param sample Stack trace sample to base stats on.
	 *
	 * @return Back pressure stats
	 */
	public static OperatorBackPressureStats createStatsFromSample(ExecutionJobVertex vertex, StackTraceSample sample) {
		Map<ExecutionAttemptID, List<StackTraceElement[]>> traces = sample.getStackTraces();

		// Map task ID to subtask index, because the web interface expects
		// it like that.
		Map<ExecutionAttemptID, Integer> subtaskIndexMap = Maps
				.newHashMapWithExpectedSize(traces.size());

		Set<ExecutionAttemptID> sampledTasks = sample.getStackTraces().keySet();

		for (ExecutionVertex task : vertex.getTaskVertices()) {
			ExecutionAttemptID taskId = task.getCurrentExecutionAttempt().getAttemptId();
			if (sampledTasks.contains(taskId)) {
				subtaskIndexMap.put(taskId, task.getParallelSubtaskIndex());
			} else {
				LOG.debug("Outdated sample. A task, which is part of the " +
						"sample has been reset.");
			}
		}

		// Ratio of blocked samples to total samples per sub task. Array
		// position corresponds to sub task index.
		double[] backPressureRatio = new double[traces.size()];

		for (Entry<ExecutionAttemptID, List<StackTraceElement[]>> entry : traces.entrySet()) {
			int backPressureSamples = 0;

			List<StackTraceElement[]> taskTraces = entry.getValue();

			for (StackTraceElement[] trace : taskTraces) {
				for (int i = trace.length - 1; i >= 0; i--) {
					StackTraceElement elem = trace[i];

					if (elem.getClassName().equals(EXPECTED_CLASS_NAME) &&
							elem.getMethodName().equals(EXPECTED_METHOD_NAME)) {

						backPressureSamples++;
						break; // Continue with next stack trace
					}
				}
			}

			int subtaskIndex = subtaskIndexMap.get(entry.getKey());

			int size = taskTraces.size();
			double ratio = (size > 0)
					? ((double) backPressureSamples) / size
					: 0;

			backPressureRatio[subtaskIndex] = ratio;
		}

		return new OperatorBackPressureStats(
				sample.getSampleId(),
				sample.getEndTime(),
				backPressureRatio);
	}
}
