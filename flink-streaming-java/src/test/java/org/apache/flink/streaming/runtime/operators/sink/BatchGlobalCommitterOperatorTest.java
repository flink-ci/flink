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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

/** Tests for {@link BatchGlobalCommitterOperator}. */
public class BatchGlobalCommitterOperatorTest extends TestLogger {

    @org.junit.Test(expected = IllegalStateException.class)
    public void throwExceptionWithoutCommitter() throws Exception {
        final OneInputStreamOperatorTestHarness<String, String> testHarness =
                createTestHarness(null);

        testHarness.initializeEmptyState();
    }

    @org.junit.Test(expected = UnsupportedOperationException.class)
    public void doNotSupportRetry() throws Exception {
        final OneInputStreamOperatorTestHarness<String, String> testHarness =
                createTestHarness(new Test.AlwaysRetryGlobalCommitter());

        testHarness.initializeEmptyState();
        testHarness.open();
        testHarness.processElement(new StreamRecord<>("hotel"));
        testHarness.endInput();
        testHarness.close();
    }

    @org.junit.Test
    public void endOfInput() throws Exception {
        final Test.DefaultGlobalCommitter globalCommitter = new Test.DefaultGlobalCommitter();
        final OneInputStreamOperatorTestHarness<String, String> testHarness =
                createTestHarness(globalCommitter);
        final List<String> inputs = Arrays.asList("compete", "swear", "shallow");

        testHarness.initializeEmptyState();
        testHarness.open();

        testHarness.processElements(
                inputs.stream().map(StreamRecord::new).collect(Collectors.toList()));

        testHarness.endInput();

        final List<String> expectedCommittedData =
                Arrays.asList(globalCommitter.combine(inputs), "end of input");

        assertThat(
                globalCommitter.getCommittedData(),
                containsInAnyOrder(expectedCommittedData.toArray()));

        testHarness.close();
    }

    @org.junit.Test
    public void close() throws Exception {
        final Test.DefaultGlobalCommitter globalCommitter = new Test.DefaultGlobalCommitter();
        final OneInputStreamOperatorTestHarness<String, String> testHarness =
                createTestHarness(globalCommitter);
        testHarness.initializeEmptyState();
        testHarness.open();
        testHarness.close();

        assertThat(globalCommitter.isClosed(), is(true));
    }

    private OneInputStreamOperatorTestHarness<String, String> createTestHarness(
            GlobalCommitter<String, String> globalCommitter) throws Exception {

        return new OneInputStreamOperatorTestHarness<>(
                new BatchGlobalCommitterOperatorFactory<>(
                        Test.newBuilder()
                                .setGlobalCommitter(globalCommitter)
                                .setGlobalCommittableSerializer(
                                        Test.StringCommittableSerializer.INSTANCE)
                                .build()),
                StringSerializer.INSTANCE);
    }
}
