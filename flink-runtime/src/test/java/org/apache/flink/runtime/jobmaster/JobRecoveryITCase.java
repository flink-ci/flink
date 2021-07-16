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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServicesWithLeadershipControl;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.testutils.TestingUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Tests for the recovery of task failures. */
public class JobRecoveryITCase extends TestLogger {

    private static final int PARALLELISM = 1;

    private static class MyHaServices extends EmbeddedHaServicesWithLeadershipControl {

        public MyHaServices(Executor executor) {
            super(executor);
        }
    }

    @Test
    public void testTaskFailureRecovery() throws Exception {
        final TestingMiniClusterConfiguration configuration =
                new TestingMiniClusterConfiguration.Builder()
                        //                .setNumberDispatcherResourceManagerComponents(2)
                        .setNumTaskManagers(1)
                        .setNumTaskManagers(1)
                        .build();
        final HighAvailabilityServices haServices =
                new MyHaServices(TestingUtils.defaultExecutor());

        try (TestingMiniCluster miniCluster =
                new TestingMiniCluster(configuration, () -> haServices)) {
            miniCluster.start();
            JobGraph jobGraph = createjobGraph(true);
            miniCluster.submitJob(jobGraph).get();
            final CompletableFuture<JobResult> jobResultFuture =
                    miniCluster.requestJobResult(jobGraph.getJobID());
            JobResult jobResult = jobResultFuture.get();
            System.out.println(jobResult.getApplicationStatus());
        }
    }

    private JobGraph createjobGraph(boolean slotSharingEnabled) throws IOException {
        final JobVertex sender = new JobVertex("Sender");
        sender.setParallelism(PARALLELISM);
        sender.setInvokableClass(TestingAbstractInvokables.Sender.class);

        final JobVertex receiver = new JobVertex("Receiver");
        receiver.setParallelism(PARALLELISM);
        receiver.setInvokableClass(FailingOnceReceiver.class);
        FailingOnceReceiver.reset();

        if (slotSharingEnabled) {
            final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
            receiver.setSlotSharingGroup(slotSharingGroup);
            sender.setSlotSharingGroup(slotSharingGroup);
        }

        receiver.connectNewDataSetAsInput(
                sender, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        final ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));

        return JobGraphBuilder.newStreamingJobGraphBuilder()
                .addJobVertices(Arrays.asList(sender, receiver))
                .setJobName(getClass().getSimpleName())
                .setExecutionConfig(executionConfig)
                .build();
    }

    /** Receiver which fails once before successfully completing. */
    public static final class FailingOnceReceiver extends TestingAbstractInvokables.Receiver {

        private static volatile boolean failed = false;

        public FailingOnceReceiver(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            if (!failed && getEnvironment().getTaskInfo().getIndexOfThisSubtask() == 0) {
                failed = true;
                throw new FlinkRuntimeException(getClass().getSimpleName());
            } else {
                super.invoke();
            }
        }

        private static void reset() {
            failed = false;
        }
    }
}
