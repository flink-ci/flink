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
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.functions.BuiltInFunctionDefinitions
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.plan.utils.UnnestConversionUtil
import org.apache.flink.table.planner.utils.ShortcutUtils
import org.apache.flink.table.runtime.functions.table.UnnestRowsFunction
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils.toRowType

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelOptRuleCall, RelOptRuleOperand, RelTraitSet}
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Uncollect
import org.apache.calcite.rel.logical._

import java.util.Collections

/**
 * Planner rule that rewrites UNNEST to explode function.
 *
 * Note: This class can only be used in HepPlanner.
 */
class LogicalUnnestRule(operand: RelOptRuleOperand, description: String)
  extends RelOptRule(operand, description)
  with UnnestConversionUtil {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: LogicalCorrelate = call.rel(0)
    val right = getRel(join.getRight)

    right match {
      // a filter is pushed above the table function
      case filter: LogicalFilter =>
        getRel(filter.getInput) match {
          case u: Uncollect => !u.withOrdinality
          case p: LogicalProject =>
            getRel(p.getInput) match {
              case u: Uncollect => !u.withOrdinality
              case _ => false
            }
          case _ => false
        }
      case project: LogicalProject =>
        getRel(project.getInput) match {
          case u: Uncollect => !u.withOrdinality
          case _ => false
        }
      case u: Uncollect => !u.withOrdinality
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val correlate: LogicalCorrelate = call.rel(0)
    val outer = getRel(correlate.getLeft)
    val array = getRel(correlate.getRight)

    // convert unnest into table function scan
    val tableFunctionScan = convert(array, correlate.getCluster, correlate.getTraitSet)
    // create correlate with table function scan as input
    val newCorrelate =
      correlate.copy(correlate.getTraitSet, ImmutableList.of(outer, tableFunctionScan))
    call.transformTo(newCorrelate)
  }
}

object LogicalUnnestRule {
  val INSTANCE = new LogicalUnnestRule(operand(classOf[LogicalCorrelate], any), "LogicalUnnestRule")
}
