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

package org.apache.flink.table.planner.analyze;

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.guava32.com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * A collection of {@link PlanAnalyzer}s for {@link
 * org.apache.flink.table.planner.delegation.StreamPlanner}.
 */
@Internal
public class FlinkStreamPlanAnalyzers {

    public static final List<PlanAnalyzer> ANALYZERS =
            ImmutableList.of(
                    GroupAggregationAnalyzer.INSTANCE, NonDeterministicUpdateAnalyzer.INSTANCE);
}
