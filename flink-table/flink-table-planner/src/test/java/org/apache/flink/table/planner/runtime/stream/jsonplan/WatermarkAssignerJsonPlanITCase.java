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

package org.apache.flink.table.planner.runtime.stream.jsonplan;

import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;

import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;

import static org.apache.flink.table.utils.DateTimeUtils.toLocalDateTime;

/** Test for watermark assigner json plan. */
public class WatermarkAssignerJsonPlanITCase extends JsonPlanTestBase {

    @Test
    public void testWatermarkAssigner() throws Exception {
        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.data3WithTimestamp()),
                new String[] {
                    "a int",
                    "b bigint",
                    "c varchar",
                    "ts timestamp(3)",
                    "watermark for ts as ts - interval '5' second"
                },
                new HashMap<String, String>() {
                    {
                        put("enable-watermark-push-down", "false");
                    }
                });

        File sinkPath = createTestCsvSinkTable("MySink", "a int", "b bigint", "ts timestamp(3)");

        CompiledPlan compiledPlan =
                tableEnv.compilePlanSql(
                        "insert into MySink select a, b, ts from MyTable where b = 3");
        tableEnv.executePlan(compiledPlan).await();

        assertResult(
                Arrays.asList(
                        "4,3," + toLocalDateTime(4000L),
                        "5,3," + toLocalDateTime(5000L),
                        "6,3," + toLocalDateTime(6000L)),
                sinkPath);
    }
}
