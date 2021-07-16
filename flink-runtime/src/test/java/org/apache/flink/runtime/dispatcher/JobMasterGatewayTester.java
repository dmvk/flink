package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JobMasterGatewayTester implements Closeable {

    private static final Time TIMEOUT = Time.seconds(1);

    private static TaskStateSnapshot createNonEmptyStateSnapshot(TaskInformation taskInformation) {
        final TaskStateSnapshot checkpointStateHandles = new TaskStateSnapshot();
        checkpointStateHandles.putSubtaskStateByOperatorID(
                OperatorID.fromJobVertexID(taskInformation.getJobVertexId()),
                OperatorSubtaskState.builder()
                        .setManagedOperatorState(
                                new OperatorStreamStateHandle(
                                        Collections.emptyMap(),
                                        new ByteStreamStateHandle("foobar", new byte[0])))
                        .build());
        return checkpointStateHandles;
    }

    private static class CheckpointCompletionHandler {

        private final Map<ExecutionAttemptID, CompletableFuture<Void>> completedAttemptFutures;
        private final CompletableFuture<Void> completedFuture;

        public CheckpointCompletionHandler(List<TaskDeploymentDescriptor> descriptors) {
            this.completedAttemptFutures =
                    descriptors.stream()
                            .map(TaskDeploymentDescriptor::getExecutionAttemptId)
                            .collect(
                                    Collectors.toMap(
                                            Function.identity(),
                                            ignored -> new CompletableFuture<>()));
            this.completedFuture =
                    CompletableFuture.allOf(
                            completedAttemptFutures
                                    .values()
                                    .toArray(new CompletableFuture<?>[] {}));
        }

        void completeAttempt(ExecutionAttemptID executionAttemptId) {
            completedAttemptFutures.get(executionAttemptId).complete(null);
        }

        CompletableFuture<Void> getCompletedFuture() {
            return completedFuture;
        }
    }

    private final UnresolvedTaskManagerLocation taskManagerLocation =
            new LocalUnresolvedTaskManagerLocation();
    private final ConcurrentMap<ExecutionAttemptID, TaskDeploymentDescriptor> descriptors =
            new ConcurrentHashMap<>();

    private final TestingRpcService rpcService;
    private final JobID jobId;
    private final JobMasterGateway jobMasterGateway;
    private final TaskExecutorGateway taskExecutorGateway;

    private final CompletableFuture<ExecutionGraphInfo> executionGraphInfoFuture;

    private final CompletableFuture<List<TaskDeploymentDescriptor>> descriptorsFuture =
            new CompletableFuture<>();

    private final ConcurrentMap<Long, CheckpointCompletionHandler> checkpoints =
            new ConcurrentHashMap<>();

    public JobMasterGatewayTester(
            TestingRpcService rpcService, JobID jobId, JobMasterGateway jobMasterGateway) {
        this.rpcService = rpcService;
        this.jobId = jobId;
        this.jobMasterGateway = jobMasterGateway;
        this.taskExecutorGateway = createTaskExecutorGateway();
        executionGraphInfoFuture = jobMasterGateway.requestJob(TIMEOUT);
    }

    public CompletableFuture<Acknowledge> transitionTo(
            List<TaskDeploymentDescriptor> descriptors, ExecutionState state) {
        final List<CompletableFuture<Acknowledge>> futures =
                descriptors.stream()
                        .map(
                                descriptor ->
                                        jobMasterGateway.updateTaskExecutionState(
                                                new TaskExecutionState(
                                                        descriptor.getExecutionAttemptId(), state)))
                        .collect(Collectors.toList());
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[] {}))
                .thenApply(ignored -> Acknowledge.get());
    }

    public CompletableFuture<List<TaskDeploymentDescriptor>> deployVertices(int numSlots) {
        return jobMasterGateway
                .registerTaskManager(
                        taskExecutorGateway.getAddress(), taskManagerLocation, jobId, TIMEOUT)
                .thenCompose(ignored -> offerSlots(numSlots))
                .thenCompose(ignored -> descriptorsFuture);
    }

    public CompletableFuture<Void> awaitCheckpoint(long checkpointId) {
        return descriptorsFuture.thenCompose(
                descriptors ->
                        checkpoints
                                .computeIfAbsent(
                                        checkpointId,
                                        key -> new CheckpointCompletionHandler(descriptors))
                                .getCompletedFuture());
    }

    @Override
    public void close() throws IOException {
        rpcService.unregisterGateway(taskExecutorGateway.getAddress());
    }

    private TaskExecutorGateway createTaskExecutorGateway() {
        final AtomicReference<TaskExecutorGateway> taskExecutorGatewayReference =
                new AtomicReference<>();
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setSubmitTaskConsumer(this::onSubmitTaskConsumer)
                        .setTriggerCheckpointFunction(this::onTriggerCheckpoint)
                        .createTestingTaskExecutorGateway();
        taskExecutorGatewayReference.set(taskExecutorGateway);
        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);
        return taskExecutorGateway;
    }

    private CompletableFuture<TaskInformation> getTaskInformation(
            ExecutionAttemptID executionAttemptId) {
        return descriptorsFuture.thenApply(
                descriptors -> {
                    final TaskDeploymentDescriptor descriptor =
                            descriptors.stream()
                                    .filter(
                                            desc ->
                                                    executionAttemptId.equals(
                                                            desc.getExecutionAttemptId()))
                                    .findAny()
                                    .orElseThrow(
                                            () ->
                                                    new IllegalStateException(
                                                            String.format(
                                                                    "Task descriptor for %s not found.",
                                                                    executionAttemptId)));
                    try {
                        return descriptor
                                .getSerializedTaskInformation()
                                .deserializeValue(Thread.currentThread().getContextClassLoader());
                    } catch (Exception e) {
                        throw new IllegalStateException(
                                String.format(
                                        "Unable to deserialize task information of %s.",
                                        executionAttemptId));
                    }
                });
    }

    private CompletableFuture<Acknowledge> onTriggerCheckpoint(
            ExecutionAttemptID executionAttemptId,
            long checkpointId,
            long checkpointTimestamp,
            CheckpointOptions checkpointOptions) {
        return getTaskInformation(executionAttemptId)
                .thenCompose(
                        taskInformation -> {
                            jobMasterGateway.acknowledgeCheckpoint(
                                    jobId,
                                    executionAttemptId,
                                    checkpointId,
                                    new CheckpointMetrics(),
                                    createNonEmptyStateSnapshot(taskInformation));
                            return completeAttemptCheckpoint(checkpointId, executionAttemptId);
                        });
    }

    private CompletableFuture<Acknowledge> onSubmitTaskConsumer(
            TaskDeploymentDescriptor taskDeploymentDescriptor, JobMasterId jobMasterId) {
        return executionGraphInfoFuture.thenCompose(
                executionGraphInfo -> {
                    final int numVertices =
                            Iterables.size(
                                    executionGraphInfo
                                            .getArchivedExecutionGraph()
                                            .getAllExecutionVertices());
                    descriptors.put(
                            taskDeploymentDescriptor.getExecutionAttemptId(),
                            taskDeploymentDescriptor);
                    if (descriptors.size() == numVertices) {
                        descriptorsFuture.complete(new ArrayList<>(descriptors.values()));
                    }
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });
    }

    private CompletableFuture<Acknowledge> completeAttemptCheckpoint(
            long checkpointId, ExecutionAttemptID executionAttemptId) {
        return descriptorsFuture
                .thenAccept(
                        descriptors ->
                                checkpoints
                                        .computeIfAbsent(
                                                checkpointId,
                                                key -> new CheckpointCompletionHandler(descriptors))
                                        .completeAttempt(executionAttemptId))
                .thenApply(ignored -> Acknowledge.get());
    }

    private CompletableFuture<Collection<SlotOffer>> offerSlots(int numSlots) {
        final List<SlotOffer> offers = new ArrayList<>();
        for (int idx = 0; idx < numSlots; idx++) {
            offers.add(new SlotOffer(new AllocationID(), 0, ResourceProfile.ANY));
        }
        return jobMasterGateway.offerSlots(taskManagerLocation.getResourceID(), offers, TIMEOUT);
    }
}
