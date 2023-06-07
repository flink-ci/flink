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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.SerializedShuffleDescriptorAndIndicesID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;

/** Non op implement of {@link ShuffleDescriptorsCache}. */
public class NoOpShuffleDescriptorsCache implements ShuffleDescriptorsCache {

    public static final NoOpShuffleDescriptorsCache INSTANCE = new NoOpShuffleDescriptorsCache();

    @Override
    public void start(ComponentMainThreadExecutor mainThreadExecutor) {}

    @Override
    public void stop() {}

    @Override
    public ShuffleDescriptorCacheEntry get(
            SerializedShuffleDescriptorAndIndicesID serializedShuffleDescriptorsId) {
        return null;
    }

    @Override
    public void put(
            JobID jobId,
            SerializedShuffleDescriptorAndIndicesID serializedShuffleDescriptorsId,
            TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[]
                    shuffleDescriptorAndIndices) {}

    @Override
    public void clearCacheOfJob(JobID jobId) {}
}
