/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertexInputInfo;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.IntermediateResultInfo;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.*;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.util.JobVertexConnectionUtils;

import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchSchedulerTest.createJobVertex;
import static org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchSchedulerTest.transitionExecutionsState;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AdaptiveBatchScheduler}. */
public class AdaptiveBatchSchedulerForwardPartitioningTest {

    /**
     *
     *
     * <pre>
     *  SRC -------------------hash---> JOIN
     *                                 /
     *  SRC ---hash---> AGG ---fwd----
     * </pre>
     */
    @Test
    void testForwardPartitioning() throws Exception {
        final int sourceParallelism = 5;
        final JobVertex leftSource = createJobVertex("left-source", sourceParallelism);
        final JobVertex rightSource = createJobVertex("right-source", sourceParallelism);
        final JobVertex rightAgg = createJobVertex("right-agg", -1);
        final JobVertex join = createJobVertex("join", -1);

        final Map<JobVertexID, Integer> parallelismDecisions = new HashMap<>();
        parallelismDecisions.put(rightAgg.getID(), 4);
        parallelismDecisions.put(join.getID(), 5);

        connectUsingShuffle(leftSource, join);
        connectUsingShuffle(rightSource, rightAgg);
        connectUsingForward(rightAgg, join);

        final JobGraph jobGraph =
                new JobGraph(new JobID(), "whatever", leftSource, rightSource, rightAgg, join);

        final ComponentMainThreadExecutor mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forMainThread();
        final ManuallyTriggeredScheduledExecutorService scheduledExecutor =
                new ManuallyTriggeredScheduledExecutorService();

        final VertexParallelismAndInputInfosDecider decider =
                DefaultSchedulerBuilder.newParallelismDeciderBuilder()
                        .setParallelismFn(
                                vertexId ->
                                        Objects.requireNonNull(parallelismDecisions.get(vertexId)))
                        .setInputInfosFn(
                                (jobVertexId, parallelism, inputs) -> {
                                    if (rightAgg.getID().equals(jobVertexId)) {
                                        // Reverse partitioning of right agg, so it will no longer
                                        // match the left side of the join
                                        final IntermediateResultInfo input =
                                                Iterables.getOnlyElement(inputs);
                                        final JobVertexInputInfo info =
                                                Objects.requireNonNull(
                                                        VertexInputInfoComputationUtils
                                                                .computeVertexInputInfos(
                                                                        parallelism, inputs, true)
                                                                .get(input.getResultId()));
                                        return Collections.singletonMap(
                                                input.getResultId(), reversePartitioning(info));
                                    }
                                    return VertexInputInfoComputationUtils.computeVertexInputInfos(
                                            parallelism, inputs, true);
                                })
                        .build();

        final SchedulerBase scheduler =
                new DefaultSchedulerBuilder(jobGraph, mainThreadExecutor, scheduledExecutor)
                        .setVertexParallelismAndInputInfosDecider(decider)
                        .setDefaultMaxParallelism(128)
                        .buildAdaptiveBatchJobScheduler();

        final ExecutionJobVertex rightAggExecutionJobVertex =
                getExecutionJobVertex(scheduler, rightAgg);
        final ExecutionJobVertex joinExecutionJobVertex = getExecutionJobVertex(scheduler, join);

        scheduler.startScheduling();
        assertParallelismUndecided(rightAggExecutionJobVertex, joinExecutionJobVertex);

        // finish sources
        transitionExecutionsState(scheduler, ExecutionState.FINISHED, leftSource);
        transitionExecutionsState(scheduler, ExecutionState.FINISHED, rightSource);

        final Map<JobVertexID, Integer> finalParallelismDecisions =
                new HashMap<>(parallelismDecisions);
        finalParallelismDecisions.put(join.getID(), parallelismDecisions.get(rightAgg.getID()));
        assertParallelismDecided(
                finalParallelismDecisions, rightAggExecutionJobVertex, joinExecutionJobVertex);

        // finish right aggregation
        transitionExecutionsState(scheduler, ExecutionState.FINISHED, rightAgg);

        final List<IndexRange> leftInputs =
                collectSubpartitionIndexRanges(joinExecutionJobVertex, leftSource);
        final List<IndexRange> rightInputs =
                collectSubpartitionIndexRanges(rightAggExecutionJobVertex, rightSource);

        assertThat(leftInputs).containsExactlyElementsOf(rightInputs);
    }

    /**
     *
     *
     * <pre>
     *  SRC ---------------------------hash---> MULTI INPUT
     *                                           /     /
     *  SRC -------------------hash---> JOIN ---/     /
     *                                 /             /
     *  SRC ---hash---> AGG ---fwd----/-------------/
     * </pre>
     */
    @Test
    void testForwardPartitioning_multipleConsumers() throws Exception {
        final int sourceParallelism = 5;
        final JobVertex firstSource = createJobVertex("first-source", sourceParallelism);
        final JobVertex secondSource = createJobVertex("second-source", sourceParallelism);
        final JobVertex thirdSource = createJobVertex("third-source", sourceParallelism);
        final JobVertex aggregation = createJobVertex("aggregation", -1);
        final JobVertex join = createJobVertex("join", -1);
        final JobVertex multiJoin = createJobVertex("multi-join", -1);

        final Map<JobVertexID, Integer> parallelismDecisions = new HashMap<>();
        parallelismDecisions.put(aggregation.getID(), 4);
        parallelismDecisions.put(join.getID(), 5);
        parallelismDecisions.put(multiJoin.getID(), 6);

        connectUsingShuffle(firstSource, join);
        connectUsingShuffle(secondSource, multiJoin);
        connectUsingShuffle(thirdSource, aggregation);
        connectUsingForward(aggregation, join);
        connectUsingShuffle(join, multiJoin);
        connectUsingForward(aggregation, multiJoin);

        final JobGraph jobGraph =
                new JobGraph(
                        new JobID(),
                        "whatever",
                        firstSource,
                        secondSource,
                        thirdSource,
                        aggregation,
                        join,
                        multiJoin);

        final ComponentMainThreadExecutor mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forMainThread();
        final ManuallyTriggeredScheduledExecutorService scheduledExecutor =
                new ManuallyTriggeredScheduledExecutorService();

        final VertexParallelismAndInputInfosDecider decider =
                DefaultSchedulerBuilder.newParallelismDeciderBuilder()
                        .setParallelismFn(
                                vertexId ->
                                        Objects.requireNonNull(parallelismDecisions.get(vertexId)))
                        .build();

        final SchedulerBase scheduler =
                new DefaultSchedulerBuilder(jobGraph, mainThreadExecutor, scheduledExecutor)
                        .setVertexParallelismAndInputInfosDecider(decider)
                        .setDefaultMaxParallelism(128)
                        .buildAdaptiveBatchJobScheduler();

        final ExecutionJobVertex aggregationExecutionJobVertex =
                getExecutionJobVertex(scheduler, aggregation);
        final ExecutionJobVertex firstJoinExecutionJobVertex =
                getExecutionJobVertex(scheduler, join);
        final ExecutionJobVertex secondJoinExecutionJobVertex =
                getExecutionJobVertex(scheduler, multiJoin);

        scheduler.startScheduling();
        assertParallelismUndecided(
                aggregationExecutionJobVertex,
                firstJoinExecutionJobVertex,
                secondJoinExecutionJobVertex);

        // finish sources
        transitionExecutionsState(scheduler, ExecutionState.FINISHED, firstSource);
        transitionExecutionsState(scheduler, ExecutionState.FINISHED, secondSource);
        transitionExecutionsState(scheduler, ExecutionState.FINISHED, thirdSource);

        // finish aggregation
        transitionExecutionsState(scheduler, ExecutionState.FINISHED, aggregation);

        final Map<JobVertexID, Integer> finalParallelismDecisions =
                new HashMap<>(parallelismDecisions);
        finalParallelismDecisions.put(join.getID(), 4);
        finalParallelismDecisions.put(multiJoin.getID(), 4);
        assertParallelismDecided(
                finalParallelismDecisions,
                aggregationExecutionJobVertex,
                firstJoinExecutionJobVertex,
                secondJoinExecutionJobVertex);
    }

    private static List<IndexRange> collectSubpartitionIndexRanges(
            ExecutionJobVertex target, JobVertex source) {
        final List<IndexRange> inputs = new ArrayList<>();
        for (int idx = 0; idx < target.getTaskVertices().length; idx++) {
            final ExecutionVertex taskVertex = target.getTaskVertices()[idx];
            final ExecutionVertexInputInfo input =
                    taskVertex.getExecutionVertexInputInfo(
                            Iterables.getOnlyElement(source.getProducedDataSets()).getId());
            inputs.add(Iterables.getOnlyElement(input.getConsumedSubpartitionGroups().values()));
        }
        return inputs;
    }

    private void assertParallelismDecided(
            Map<JobVertexID, Integer> expectedDecisions, ExecutionJobVertex... vertices) {
        final Map<JobVertexID, Integer> actualDecisions = new HashMap<>(vertices.length);
        for (ExecutionJobVertex vertex : vertices) {
            actualDecisions.put(vertex.getJobVertexId(), vertex.getParallelism());
        }
        assertThat(actualDecisions).containsExactlyEntriesOf(expectedDecisions);
    }

    private void assertParallelismUndecided(ExecutionJobVertex... vertices) {
        for (ExecutionJobVertex vertex : vertices) {
            assertThat(vertex.isParallelismDecided())
                    .withFailMessage(
                            "Parallelism decision is incorrect for vertex [%s].", vertex.getName())
                    .isFalse();
        }
    }

    private static JobVertexInputInfo reversePartitioning(JobVertexInputInfo jobVertexInputInfo) {
        final List<ExecutionVertexInputInfo> infos =
                jobVertexInputInfo.getExecutionVertexInputInfos();
        final List<ExecutionVertexInputInfo> reversed = new ArrayList<>();
        for (int idx = 0; idx < infos.size(); idx++) {
            final ExecutionVertexInputInfo info = infos.get(infos.size() - 1 - idx);
            reversed.add(new ExecutionVertexInputInfo(idx, info.getConsumedSubpartitionGroups()));
        }
        return new JobVertexInputInfo(reversed);
    }

    private static void connectUsingShuffle(JobVertex source, JobVertex target) {
        JobVertexConnectionUtils.connectNewDataSetAsInput(
                target, source, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
    }

    private static void connectUsingForward(JobVertex source, JobVertex target) {
        JobVertexConnectionUtils.connectNewDataSetAsInput(
                target,
                source,
                DistributionPattern.POINTWISE,
                ResultPartitionType.BLOCKING,
                false,
                true);
    }

    private static ExecutionJobVertex getExecutionJobVertex(
            SchedulerBase scheduler, JobVertex jobVertex) {
        final DefaultExecutionGraph graph = (DefaultExecutionGraph) scheduler.getExecutionGraph();
        return Objects.requireNonNull(graph.getJobVertex(jobVertex.getID()));
    }
}
