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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Flink join reorder rule, which can change the join order of input relNode tree.
 *
 * <p>It is triggered by the ({@link MultiJoin}).
 *
 * <p>In this rule, there are two specific join reorder strategies can be chosen, one is {@link
 * LoptOptimizeJoinRule}, another is {@link FlinkBusyJoinReorderRule}. Which rule is selected
 * depends on the parameter TABLE_OPTIMIZER_BUSY_JOIN_REORDER_THRESHOLD.
 */
public class FlinkJoinReorderRule extends RelRule<FlinkJoinReorderRule.Config>
        implements TransformationRule {

    public static final FlinkJoinReorderRule INSTANCE =
            FlinkJoinReorderRule.Config.DEFAULT.toRule();

    public static final LoptOptimizeJoinRule LOPT_JOIN_REORDER =
            LoptOptimizeJoinRule.Config.DEFAULT.toRule();

    public static final FlinkBusyJoinReorderRule BUSY_JOIN_REORDER =
            FlinkBusyJoinReorderRule.Config.DEFAULT.toRule();

    @Experimental
    public static final ConfigOption<Integer> TABLE_OPTIMIZER_BUSY_JOIN_REORDER_THRESHOLD =
            key("table.optimizer.busy-join-reorder-threshold")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "The maximum number of joined nodes allowed in the busy join reorder algorithm. "
                                    + "If this parameter is set too large, the search space of join reorder "
                                    + "algorithm will be too large, which will spend so much time. the default "
                                    + "value is -1, which means that Flink disable busy join reorder by default.");

    /** Creates an SparkJoinReorderRule. */
    protected FlinkJoinReorderRule(FlinkJoinReorderRule.Config config) {
        super(config);
    }

    @Deprecated // to be removed before 2.0
    public FlinkJoinReorderRule(RelBuilderFactory relBuilderFactory) {
        this(
                FlinkJoinReorderRule.Config.DEFAULT
                        .withRelBuilderFactory(relBuilderFactory)
                        .as(FlinkJoinReorderRule.Config.class));
    }

    @Deprecated // to be removed before 2.0
    public FlinkJoinReorderRule(
            RelFactories.JoinFactory joinFactory,
            RelFactories.ProjectFactory projectFactory,
            RelFactories.FilterFactory filterFactory) {
        this(RelBuilder.proto(joinFactory, projectFactory, filterFactory));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final MultiJoin multiJoinRel = call.rel(0);
        int numJoinInputs = multiJoinRel.getInputs().size();
        int dpThreshold =
                ShortcutUtils.unwrapContext(multiJoinRel)
                        .getTableConfig()
                        .get(FlinkJoinReorderRule.TABLE_OPTIMIZER_BUSY_JOIN_REORDER_THRESHOLD);
        if (numJoinInputs <= dpThreshold) {
            BUSY_JOIN_REORDER.onMatch(call);
        } else {
            LOPT_JOIN_REORDER.onMatch(call);
        }
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                EMPTY.withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs())
                        .as(FlinkJoinReorderRule.Config.class);

        @Override
        default FlinkJoinReorderRule toRule() {
            return new FlinkJoinReorderRule(this);
        }
    }
}
