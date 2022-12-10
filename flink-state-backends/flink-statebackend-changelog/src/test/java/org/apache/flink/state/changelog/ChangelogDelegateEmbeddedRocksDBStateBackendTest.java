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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackendTest;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.TestTaskStateManager;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

/** Tests for {@link ChangelogStateBackend} delegating {@link EmbeddedRocksDBStateBackend}. */
public class ChangelogDelegateEmbeddedRocksDBStateBackendTest
        extends EmbeddedRocksDBStateBackendTest {

    @TempDir static File tmPath;

    @Override
    protected TestTaskStateManager getTestTaskStateManager() throws IOException {
        return ChangelogStateBackendTestUtils.createTaskStateManager(tmPath);
    }

    @Override
    protected boolean snapshotUsesStreamFactory() {
        return false;
    }

    @Override
    protected boolean supportsMetaInfoVerification() {
        return false;
    }

    @Override
    protected boolean isSafeToReuseKVState() {
        return true;
    }

    @TestTemplate
    @Disabled("The type of handle returned from snapshot() is not incremental")
    void testSharedIncrementalStateDeRegistration() {}

    @Override
    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            Environment env)
            throws Exception {

        return ChangelogStateBackendTestUtils.createKeyedBackend(
                new ChangelogStateBackend(super.getStateBackend()),
                keySerializer,
                numberOfKeyGroups,
                keyGroupRange,
                env);
    }

    @Override
    protected ConfigurableStateBackend getStateBackend() throws IOException {
        return new ChangelogStateBackend(super.getStateBackend());
    }

    @TestTemplate
    void testMaterializedRestore() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();

        ChangelogStateBackendTestUtils.testMaterializedRestore(
                getStateBackend(), StateTtlConfig.DISABLED, env, streamFactory);
    }

    @TestTemplate
    void testMaterializedRestoreWithWrappedState() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();

        Configuration configuration = new Configuration();
        configuration.set(StateBackendOptions.LATENCY_TRACK_ENABLED, true);
        StateBackend stateBackend =
                getStateBackend()
                        .configure(configuration, Thread.currentThread().getContextClassLoader());
        ChangelogStateBackendTestUtils.testMaterializedRestore(
                stateBackend,
                StateTtlConfig.newBuilder(Time.minutes(1)).build(),
                env,
                streamFactory);
    }

    @TestTemplate
    void testMaterializedRestorePriorityQueue() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();

        ChangelogStateBackendTestUtils.testMaterializedRestoreForPriorityQueue(
                getStateBackend(), env, streamFactory);
    }
}
