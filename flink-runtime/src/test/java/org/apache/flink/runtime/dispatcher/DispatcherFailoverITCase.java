package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.EmbeddedCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.PerJobCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneRunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.TestingJobGraphStore;
import org.apache.flink.util.TimeUtils;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class DispatcherFailoverITCase extends AbstractDispatcherTest {

    private static final Time TIMEOUT = Time.seconds(1);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        final CompletedCheckpointStore completedCheckpointStore =
                new EmbeddedCompletedCheckpointStore();
        haServices.setCheckpointRecoveryFactory(
                PerJobCheckpointRecoveryFactory.useSameServicesForAllJobs(
                        completedCheckpointStore, new StandaloneCheckpointIDCounter()));
    }

    @Test
    public void testRecoverFromCheckpointAfterJobGraphRemovalOfTerminatedJobFailed()
            throws Exception {
        final JobGraph jobGraph = createJobGraph();
        final JobID jobId = jobGraph.getJobID();

        // Construct job graph store.
        final TestingJobGraphStore jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setRemoveJobGraphConsumer(
                                graph -> {
                                    throw new Exception("Unable to remove job graph.");
                                })
                        .build();
        jobGraphStore.start(null);
        haServices.setJobGraphStore(jobGraphStore);

        // Construct leader election service.
        final TestingLeaderElectionService leaderElectionService =
                new TestingLeaderElectionService();
        haServices.setJobMasterLeaderElectionService(jobId, leaderElectionService);

        // Start the first dispatcher and submit the job.
        final Dispatcher dispatcher = createRecoveredDispatcher();
        leaderElectionService.isLeader(UUID.randomUUID());
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        awaitStatus(dispatcherGateway, jobId, JobStatus.RUNNING);

        // Run vertices, checkpoint and finish.
        final JobMasterGateway jobMasterGateway = dispatcher.getJobMasterGateway(jobId).get();
        try (final JobMasterGatewayTester tester =
                new JobMasterGatewayTester(rpcService, jobId, jobMasterGateway)) {
            final List<TaskDeploymentDescriptor> descriptors = tester.deployVertices(2).get();
            tester.transitionTo(descriptors, ExecutionState.INITIALIZING).get();
            tester.transitionTo(descriptors, ExecutionState.RUNNING).get();
            tester.awaitCheckpoint(1L).get();
            tester.transitionTo(descriptors, ExecutionState.FINISHED).get();
        }
        awaitStatus(dispatcherGateway, jobId, JobStatus.FINISHED);

        // Kill the first dispatcher.
        RpcUtils.terminateRpcEndpoint(dispatcher, TIMEOUT);

        // Run a second dispatcher, that restores our finished job.
        final Dispatcher secondDispatcher = createRecoveredDispatcher();
        final DispatcherGateway secondDispatcherGateway =
                secondDispatcher.getSelfGateway(DispatcherGateway.class);
        leaderElectionService.isLeader(UUID.randomUUID());
        awaitStatus(secondDispatcherGateway, jobId, JobStatus.RUNNING);

        // Now make sure that restored job started from checkpoint.
        final JobMasterGateway secondJobMasterGateway =
                secondDispatcher.getJobMasterGateway(jobId).get();
        try (final JobMasterGatewayTester tester =
                new JobMasterGatewayTester(rpcService, jobId, secondJobMasterGateway)) {
            final List<TaskDeploymentDescriptor> descriptors = tester.deployVertices(2).get();
            final Optional<JobManagerTaskRestore> maybeRestore =
                    descriptors.stream()
                            .map(TaskDeploymentDescriptor::getTaskRestore)
                            .filter(Objects::nonNull)
                            .findAny();
            assertTrue("Job has recover from checkpoint.", maybeRestore.isPresent());
        }

        // Kill the second dispatcher.
        RpcUtils.terminateRpcEndpoint(dispatcher, TIMEOUT);
    }

    private JobGraph createJobGraph() {
        final JobVertex firstVertex = new JobVertex("first");
        firstVertex.setInvokableClass(NoOpInvokable.class);
        firstVertex.setParallelism(1);

        final JobVertex secondVertex = new JobVertex("second");
        secondVertex.setInvokableClass(NoOpInvokable.class);
        secondVertex.setParallelism(1);

        final CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                CheckpointCoordinatorConfiguration.builder().build();
        final JobCheckpointingSettings checkpointingSettings =
                new JobCheckpointingSettings(checkpointCoordinatorConfiguration, null);
        return JobGraphBuilder.newStreamingJobGraphBuilder()
                .addJobVertex(firstVertex)
                .addJobVertex(secondVertex)
                .setJobCheckpointingSettings(checkpointingSettings)
                .build();
    }

    private TestingDispatcher createRecoveredDispatcher() throws Exception {
        final List<JobGraph> jobGraphs = new ArrayList<>();
        for (JobID jobId : haServices.getJobGraphStore().getJobIds()) {
            jobGraphs.add(haServices.getJobGraphStore().recoverJobGraph(jobId));
        }
        haServices.setRunningJobsRegistry(new StandaloneRunningJobsRegistry());
        final TestingDispatcher dispatcher =
                new TestingDispatcherBuilder()
                        .setJobManagerRunnerFactory(
                                JobMasterServiceLeadershipRunnerFactory.INSTANCE)
                        .setJobGraphWriter(haServices.getJobGraphStore())
                        .setInitialJobGraphs(jobGraphs)
                        .build();
        dispatcher.start();
        return dispatcher;
    }

    private static void awaitStatus(
            DispatcherGateway dispatcherGateway, JobID jobId, JobStatus status) throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> status.equals(dispatcherGateway.requestJobStatus(jobId, TIMEOUT).get()),
                Deadline.fromNow(TimeUtils.toDuration(TIMEOUT)));
    }
}
