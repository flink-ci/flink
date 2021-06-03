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

package org.apache.flink.table.runtime.operators.join.interval;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.junit.Assert.assertEquals;

/** Test for {@link ProcTimeIntervalJoin}. */
public class ProcTimeIntervalJoinTest extends TimeIntervalStreamJoinTestBase {

    private int keyIdx = 0;
    private RowDataKeySelector keySelector =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {keyIdx}, rowType.toRowFieldTypes());
    private TypeInformation<RowData> keyType = InternalTypeInfo.ofFields();

    /** a.proctime >= b.proctime - 10 and a.proctime <= b.proctime + 20. * */
    @Test
    public void testProcTimeInnerJoinWithCommonBounds() throws Exception {
        ProcTimeIntervalJoin joinProcessFunc =
                new ProcTimeIntervalJoin(
                        FlinkJoinType.INNER, -10, 20, rowType, rowType, joinFunction);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinProcessFunc);
        testHarness.open();
        testHarness.setProcessingTime(1);
        testHarness.processElement1(insertRecord(1L, "1a1"));
        assertEquals(1, testHarness.numProcessingTimeTimers());

        testHarness.setProcessingTime(2);
        testHarness.processElement1(insertRecord(2L, "2a2"));
        // timers for key = 1 and key = 2
        assertEquals(2, testHarness.numProcessingTimeTimers());

        testHarness.setProcessingTime(3);
        testHarness.processElement1(insertRecord(1L, "1a3"));
        assertEquals(4, testHarness.numKeyedStateEntries());
        // The number of timers won't increase.
        assertEquals(2, testHarness.numProcessingTimeTimers());

        testHarness.processElement2(insertRecord(1L, "1b3"));

        testHarness.setProcessingTime(4);
        testHarness.processElement2(insertRecord(2L, "2b4"));
        // The number of states should be doubled.
        assertEquals(8, testHarness.numKeyedStateEntries());
        assertEquals(4, testHarness.numProcessingTimeTimers());

        // Test for -10 boundary (13 - 10 = 3).
        // The left row (key = 1) with timestamp = 1 will be eagerly removed here.
        testHarness.setProcessingTime(13);
        testHarness.processElement2(insertRecord(1L, "1b13"));

        // Test for +20 boundary (13 + 20 = 33).
        testHarness.setProcessingTime(33);
        assertEquals(4, testHarness.numKeyedStateEntries());
        assertEquals(2, testHarness.numProcessingTimeTimers());

        testHarness.processElement1(insertRecord(1L, "1a33"));
        testHarness.processElement1(insertRecord(2L, "2a33"));
        // The left row (key = 2) with timestamp = 2 will be eagerly removed here.
        testHarness.processElement2(insertRecord(2L, "2b33"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1L, "1a1", 1L, "1b3"));
        expectedOutput.add(insertRecord(1L, "1a3", 1L, "1b3"));
        expectedOutput.add(insertRecord(2L, "2a2", 2L, "2b4"));
        expectedOutput.add(insertRecord(1L, "1a3", 1L, "1b13"));
        expectedOutput.add(insertRecord(1L, "1a33", 1L, "1b13"));
        expectedOutput.add(insertRecord(2L, "2a33", 2L, "2b33"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    /** a.proctime >= b.proctime - 10 and a.proctime <= b.proctime - 5. * */
    @Test
    public void testProcTimeInnerJoinWithNegativeBounds() throws Exception {
        ProcTimeIntervalJoin joinProcessFunc =
                new ProcTimeIntervalJoin(
                        FlinkJoinType.INNER, -10, -5, rowType, rowType, joinFunction);

        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinProcessFunc);
        testHarness.open();

        testHarness.setProcessingTime(1);
        testHarness.processElement1(insertRecord(1L, "1a1"));

        testHarness.setProcessingTime(2);
        testHarness.processElement1(insertRecord(2L, "2a2"));

        testHarness.setProcessingTime(3);
        testHarness.processElement1(insertRecord(1L, "1a3"));
        assertEquals(4, testHarness.numKeyedStateEntries());
        assertEquals(2, testHarness.numProcessingTimeTimers());

        // All the right rows will not be cached.
        testHarness.processElement2(insertRecord(1L, "1b3"));
        assertEquals(4, testHarness.numKeyedStateEntries());
        assertEquals(2, testHarness.numProcessingTimeTimers());

        testHarness.setProcessingTime(7);

        // Meets a.proctime <= b.proctime - 5.
        // This row will only be joined without being cached (7 >= 7 - 5).
        testHarness.processElement2(insertRecord(2L, "2b7"));
        assertEquals(4, testHarness.numKeyedStateEntries());
        assertEquals(2, testHarness.numProcessingTimeTimers());

        testHarness.setProcessingTime(12);
        // The left row (key = 1) with timestamp = 1 will be eagerly removed here.
        testHarness.processElement2(insertRecord(1L, "1b12"));

        // We add a delay (relativeWindowSize / 2) for cleaning up state.
        // No timers will be triggered here.
        testHarness.setProcessingTime(13);
        assertEquals(4, testHarness.numKeyedStateEntries());
        assertEquals(2, testHarness.numProcessingTimeTimers());

        // Trigger the timer registered by the left row (key = 1) with timestamp = 1
        // (1 + 10 + 2 + 0 + 1 = 14).
        // The left row (key = 1) with timestamp = 3 will removed here.
        testHarness.setProcessingTime(14);
        assertEquals(2, testHarness.numKeyedStateEntries());
        assertEquals(1, testHarness.numProcessingTimeTimers());

        // Clean up the left row (key = 2) with timestamp = 2.
        testHarness.setProcessingTime(16);
        assertEquals(0, testHarness.numKeyedStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(2L, "2a2", 2L, "2b7"));
        expectedOutput.add(insertRecord(1L, "1a3", 1L, "1b12"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            createTestHarness(ProcTimeIntervalJoin intervalJoinFunc) throws Exception {
        KeyedCoProcessOperator<RowData, RowData, RowData, RowData> operator =
                new KeyedCoProcessOperator<>(intervalJoinFunc);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator, keySelector, keySelector, keyType);
        return testHarness;
    }
}
