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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.util.TestHarnessUtil.buildSubtaskState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

/** Test for {@link StreamingGlobalCommitterOperator}. */
public class StreamingGlobalCommitterOperatorTest extends TestLogger {

    @org.junit.Test(expected = IllegalStateException.class)
    public void throwExceptionWithoutSerializer() throws Exception {
        final OneInputStreamOperatorTestHarness<String, String> testHarness =
                createTestHarness(new Test.DefaultGlobalCommitter(), null);
        testHarness.initializeEmptyState();
        testHarness.open();
    }

    @org.junit.Test(expected = IllegalStateException.class)
    public void throwExceptionWithoutCommitter() throws Exception {
        final OneInputStreamOperatorTestHarness<String, String> testHarness =
                createTestHarness(null, Test.StringCommittableSerializer.INSTANCE);
        testHarness.initializeEmptyState();
        testHarness.open();
    }

    @org.junit.Test(expected = UnsupportedOperationException.class)
    public void doNotSupportRetry() throws Exception {
        final List<String> input = Arrays.asList("lazy", "leaf");

        final OneInputStreamOperatorTestHarness<String, String> testHarness =
                createTestHarness(new Test.AlwaysRetryGlobalCommitter());

        testHarness.initializeEmptyState();
        testHarness.open();
        testHarness.processElements(
                input.stream().map(StreamRecord::new).collect(Collectors.toList()));
        testHarness.snapshot(1L, 1L);
        testHarness.notifyOfCompletedCheckpoint(1L);

        testHarness.close();
    }

    @org.junit.Test
    public void closeCommitter() throws Exception {
        final Test.DefaultGlobalCommitter globalCommitter = new Test.DefaultGlobalCommitter();
        final OneInputStreamOperatorTestHarness<String, String> testHarness =
                createTestHarness(globalCommitter);
        testHarness.initializeEmptyState();
        testHarness.open();
        testHarness.close();
        assertThat(globalCommitter.isClosed(), is(true));
    }

    @org.junit.Test
    public void restoredFromMergedState() throws Exception {

        final List<String> input1 = Arrays.asList("host", "drop");
        final OperatorSubtaskState operatorSubtaskState1 =
                buildSubtaskState(createTestHarness(), input1);

        final List<String> input2 = Arrays.asList("future", "evil", "how");
        final OperatorSubtaskState operatorSubtaskState2 =
                buildSubtaskState(createTestHarness(), input2);

        final Test.DefaultGlobalCommitter globalCommitter = new Test.DefaultGlobalCommitter();
        final OneInputStreamOperatorTestHarness<String, String> testHarness =
                createTestHarness(globalCommitter);

        final OperatorSubtaskState mergedOperatorSubtaskState =
                OneInputStreamOperatorTestHarness.repackageState(
                        operatorSubtaskState1, operatorSubtaskState2);

        testHarness.initializeState(
                OneInputStreamOperatorTestHarness.repartitionOperatorState(
                        mergedOperatorSubtaskState, 2, 2, 1, 0));
        testHarness.open();

        final List<String> expectedOutput = new ArrayList<>();
        expectedOutput.add(Test.DefaultGlobalCommitter.COMBINER.apply(input1));
        expectedOutput.add(Test.DefaultGlobalCommitter.COMBINER.apply(input2));

        testHarness.snapshot(1L, 1L);
        testHarness.notifyOfCompletedCheckpoint(1L);
        testHarness.close();

        assertThat(
                globalCommitter.getCommittedData(), containsInAnyOrder(expectedOutput.toArray()));
    }

    @org.junit.Test
    public void commitMultipleStagesTogether() throws Exception {

        final Test.DefaultGlobalCommitter globalCommitter = new Test.DefaultGlobalCommitter();

        final List<String> input1 = Arrays.asList("cautious", "nature");
        final List<String> input2 = Arrays.asList("count", "over");
        final List<String> input3 = Arrays.asList("lawyer", "grammar");

        final List<String> expectedOutput = new ArrayList<>();

        expectedOutput.add(Test.DefaultGlobalCommitter.COMBINER.apply(input1));
        expectedOutput.add(Test.DefaultGlobalCommitter.COMBINER.apply(input2));
        expectedOutput.add(Test.DefaultGlobalCommitter.COMBINER.apply(input3));

        final OneInputStreamOperatorTestHarness<String, String> testHarness =
                createTestHarness(globalCommitter);
        testHarness.initializeEmptyState();
        testHarness.open();

        testHarness.processElements(
                input1.stream().map(StreamRecord::new).collect(Collectors.toList()));
        testHarness.snapshot(1L, 1L);
        testHarness.processElements(
                input2.stream().map(StreamRecord::new).collect(Collectors.toList()));
        testHarness.snapshot(2L, 2L);
        testHarness.processElements(
                input3.stream().map(StreamRecord::new).collect(Collectors.toList()));
        testHarness.snapshot(3L, 3L);

        testHarness.notifyOfCompletedCheckpoint(3L);

        testHarness.close();

        assertThat(
                testHarness.getOutput(),
                containsInAnyOrder(expectedOutput.stream().map(StreamRecord::new).toArray()));

        assertThat(
                globalCommitter.getCommittedData(), containsInAnyOrder(expectedOutput.toArray()));
    }

    @org.junit.Test
    public void filterRecoveredCommittables() throws Exception {
        final List<String> input = Arrays.asList("silent", "elder", "patience");
        final String successCommittedCommittable =
                Test.DefaultGlobalCommitter.COMBINER.apply(input);

        final OperatorSubtaskState operatorSubtaskState =
                buildSubtaskState(createTestHarness(), input);
        final Test.DefaultGlobalCommitter globalCommitter =
                new Test.DefaultGlobalCommitter(successCommittedCommittable);

        final OneInputStreamOperatorTestHarness<String, String> testHarness =
                createTestHarness(globalCommitter);

        // all data from previous checkpoint are expected to be committed,
        // so we expect no data to be re-committed.
        testHarness.initializeState(operatorSubtaskState);
        testHarness.open();
        testHarness.snapshot(1L, 1L);
        testHarness.notifyOfCompletedCheckpoint(1L);
        assertTrue(globalCommitter.getCommittedData().isEmpty());
        testHarness.close();
    }

    @org.junit.Test
    public void endOfInput() throws Exception {
        final Test.DefaultGlobalCommitter globalCommitter = new Test.DefaultGlobalCommitter();

        final OneInputStreamOperatorTestHarness<String, String> testHarness =
                createTestHarness(globalCommitter);
        testHarness.initializeEmptyState();
        testHarness.open();
        testHarness.snapshot(1L, 1L);
        testHarness.endInput();
        testHarness.notifyOfCompletedCheckpoint(1L);
        testHarness.close();
        assertThat(globalCommitter.getCommittedData(), contains("end of input"));
    }

    private OneInputStreamOperatorTestHarness<String, String> createTestHarness() throws Exception {
        return createTestHarness(
                new Test.DefaultGlobalCommitter(), Test.StringCommittableSerializer.INSTANCE);
    }

    private OneInputStreamOperatorTestHarness<String, String> createTestHarness(
            GlobalCommitter<String, String> globalCommitter) throws Exception {
        return createTestHarness(globalCommitter, Test.StringCommittableSerializer.INSTANCE);
    }

    private OneInputStreamOperatorTestHarness<String, String> createTestHarness(
            GlobalCommitter<String, String> globalCommitter,
            SimpleVersionedSerializer<String> serializer)
            throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new StreamingGlobalCommitterOperatorFactory<>(
                        Test.newBuilder()
                                .setGlobalCommitter(globalCommitter)
                                .setGlobalCommittableSerializer(serializer)
                                .build()),
                StringSerializer.INSTANCE);
    }
}
