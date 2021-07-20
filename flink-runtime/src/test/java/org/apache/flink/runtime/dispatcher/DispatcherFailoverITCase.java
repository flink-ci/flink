/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.TestingJobGraphStore;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TimeUtils;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** An integration test for various fail-over scenarios of the {@link Dispatcher} component. */
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
        final Error jobGraphRemovalError = new Error("Unable to remove job graph.");
        final TestingJobGraphStore jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setRemoveJobGraphConsumer(
                                graph -> {
                                    throw jobGraphRemovalError;
                                })
                        .build();
        jobGraphStore.start(null);
        haServices.setJobGraphStore(jobGraphStore);

        // Construct leader election service.
        final TestingLeaderElectionService leaderElectionService =
                new TestingLeaderElectionService();
        haServices.setJobMasterLeaderElectionService(jobId, leaderElectionService);

        // Start the first dispatcher and submit the job.
        final CountDownLatch jobGraphRemovalErrorReceived = new CountDownLatch(1);
        final Dispatcher dispatcher =
                createRecoveredDispatcher(
                        throwable -> {
                            final Optional<Error> maybeError =
                                    ExceptionUtils.findThrowable(throwable, Error.class);
                            if (maybeError.isPresent()
                                    && jobGraphRemovalError.equals(maybeError.get())) {
                                jobGraphRemovalErrorReceived.countDown();
                            } else {
                                testingFatalErrorHandlerResource
                                        .getFatalErrorHandler()
                                        .onFatalError(throwable);
                            }
                        });
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
        assertTrue(jobGraphRemovalErrorReceived.await(5, TimeUnit.SECONDS));

        // First dispatcher is in a weird state (in a real world scenario, we'd just kill the whole
        // process), just remove it's leadership for now, so no extra cleanup is performed
        leaderElectionService.stop();

        // Run a second dispatcher, that restores our finished job.
        final Dispatcher secondDispatcher = createRecoveredDispatcher(null);
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
            assertTrue("Job has recovered from checkpoint.", maybeRestore.isPresent());
        }

        // Kill the first dispatcher. This should fail, but we need to cleanup anyway, so the
        // `rpcService` class rule succeeds
        assertThrows(
                ExecutionException.class, () -> RpcUtils.terminateRpcEndpoint(dispatcher, TIMEOUT));

        // Kill the second dispatcher.
        RpcUtils.terminateRpcEndpoint(secondDispatcher, TIMEOUT);
    }

    private JobGraph createJobGraph() {
        final JobVertex firstVertex = new JobVertex("first");
        firstVertex.setInvokableClass(NoOpInvokable.class);
        firstVertex.setParallelism(1);

        final JobVertex secondVertex = new JobVertex("second");
        secondVertex.setInvokableClass(NoOpInvokable.class);
        secondVertex.setParallelism(1);

        final CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                CheckpointCoordinatorConfiguration.builder()
                        .setCheckpointInterval(100L)
                        .setMinPauseBetweenCheckpoints(100L)
                        .setCheckpointTimeout(10_000L)
                        .build();
        final JobCheckpointingSettings checkpointingSettings =
                new JobCheckpointingSettings(checkpointCoordinatorConfiguration, null);
        return JobGraphBuilder.newStreamingJobGraphBuilder()
                .addJobVertex(firstVertex)
                .addJobVertex(secondVertex)
                .setJobCheckpointingSettings(checkpointingSettings)
                .build();
    }

    private TestingDispatcher createRecoveredDispatcher(
            @Nullable FatalErrorHandler fatalErrorHandler) throws Exception {
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
                        .setFatalErrorHandler(
                                fatalErrorHandler == null
                                        ? testingFatalErrorHandlerResource.getFatalErrorHandler()
                                        : fatalErrorHandler)
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
