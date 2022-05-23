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

package org.apache.flink.table.runtime.operators.window.slicing;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.ZoneId;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SliceAssigners.WindowedSliceAssigner}. */
class WindowedSliceAssignerTest extends SliceAssignerTestBase {

    private static Stream<TestSpec> parameters() {
        return Stream.of(
                new TestSpec(ZoneId.of("America/Los_Angeles")),
                new TestSpec(ZoneId.of("Asia/Shanghai")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void testSliceAssignment(TestSpec testSpec) {
        SliceAssigner assigner = SliceAssigners.windowed(0, testSpec.tumbleAssigner);

        assertThat(assignSliceEnd(assigner, utcMills("1970-01-01T00:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assignSliceEnd(assigner, utcMills("1970-01-01T05:00:00")))
                .isEqualTo(utcMills("1970-01-01T05:00:00"));
        assertThat(assignSliceEnd(assigner, utcMills("1970-01-01T10:00:00")))
                .isEqualTo(utcMills("1970-01-01T10:00:00"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void testGetWindowStartForTumble(TestSpec testSpec) {
        SliceAssigner assigner = SliceAssigners.windowed(0, testSpec.tumbleAssigner);

        assertThat(assigner.getWindowStart(utcMills("1970-01-01T00:00:00")))
                .isEqualTo(utcMills("1969-12-31T20:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T04:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T08:00:00")))
                .isEqualTo(utcMills("1970-01-01T04:00:00"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void testGetWindowStartForHop(TestSpec testSpec) {
        SliceAssigner assigner = SliceAssigners.windowed(0, testSpec.hopAssigner);

        assertThat(assigner.getWindowStart(utcMills("1970-01-01T00:00:00")))
                .isEqualTo(utcMills("1969-12-31T19:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T01:00:00")))
                .isEqualTo(utcMills("1969-12-31T20:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T02:00:00")))
                .isEqualTo(utcMills("1969-12-31T21:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T03:00:00")))
                .isEqualTo(utcMills("1969-12-31T22:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T04:00:00")))
                .isEqualTo(utcMills("1969-12-31T23:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T05:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T06:00:00")))
                .isEqualTo(utcMills("1970-01-01T01:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T10:00:00")))
                .isEqualTo(utcMills("1970-01-01T05:00:00"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void testGetWindowStartForCumulate(TestSpec testSpec) {
        SliceAssigner assigner = SliceAssigners.windowed(0, testSpec.cumulateAssigner);

        assertThat(assigner.getWindowStart(utcMills("1970-01-01T00:00:00")))
                .isEqualTo(utcMills("1969-12-31T19:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T01:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T02:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T03:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T04:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T05:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T06:00:00")))
                .isEqualTo(utcMills("1970-01-01T05:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T10:00:00")))
                .isEqualTo(utcMills("1970-01-01T05:00:00"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void testExpiredSlices(TestSpec testSpec) {
        SliceAssigner assigner = SliceAssigners.windowed(0, testSpec.tumbleAssigner);

        assertThat(expiredSlices(assigner, utcMills("1970-01-01T00:00:00")))
                .containsExactly(utcMills("1970-01-01T00:00:00"));
        assertThat(expiredSlices(assigner, utcMills("1970-01-01T04:00:00")))
                .containsExactly(utcMills("1970-01-01T04:00:00"));
        assertThat(expiredSlices(assigner, utcMills("1970-01-01T10:00:00")))
                .containsExactly(utcMills("1970-01-01T10:00:00"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void testEventTime(TestSpec testSpec) {
        SliceAssigner assigner = SliceAssigners.windowed(0, testSpec.tumbleAssigner);
        assertThat(assigner.isEventTime()).isTrue();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void testInvalidParameters(TestSpec testSpec) {
        assertErrorMessage(
                () -> SliceAssigners.windowed(-1, testSpec.tumbleAssigner),
                "Windowed slice assigner must have a positive window end index.");

        // should pass
        SliceAssigners.windowed(1, testSpec.tumbleAssigner);
    }

    private static class TestSpec {
        private final ZoneId shiftTimeZone;
        private final SliceAssigner tumbleAssigner;
        private final SliceAssigner hopAssigner;
        private final SliceAssigner cumulateAssigner;

        TestSpec(ZoneId shiftTimeZone) {
            this.shiftTimeZone = shiftTimeZone;
            this.tumbleAssigner = SliceAssigners.tumbling(-1, shiftTimeZone, Duration.ofHours(4));
            this.hopAssigner =
                    SliceAssigners.hopping(
                            0, shiftTimeZone, Duration.ofHours(5), Duration.ofHours(1));
            this.cumulateAssigner =
                    SliceAssigners.cumulative(
                            0, shiftTimeZone, Duration.ofHours(5), Duration.ofHours(1));
        }

        @Override
        public String toString() {
            return "timezone = " + shiftTimeZone;
        }
    }
}
