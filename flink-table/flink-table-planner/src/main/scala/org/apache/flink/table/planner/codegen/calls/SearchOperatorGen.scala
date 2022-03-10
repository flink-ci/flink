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

package org.apache.flink.table.planner.codegen.calls

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, CodeGenException, GeneratedExpression}
import org.apache.flink.table.planner.codegen.CodeGenUtils.newNames
import org.apache.flink.table.planner.codegen.GenerateUtils.{generateLiteral, generateNullLiteral}
import org.apache.flink.table.planner.codegen.calls.ScalarOperatorGens._
import org.apache.flink.table.planner.functions.casting.CastRuleProvider
import org.apache.flink.table.planner.plan.utils.RexLiteralUtil.toFlinkInternalValue
import org.apache.flink.table.types.logical.{BooleanType, LogicalType}
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging.findCommonType

import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.util.{RangeSets, Sarg}

import java.util.Arrays.asList

import scala.collection.JavaConverters._

/**
 * Class containing utilities to implement the SEARCH operator.
 *
 * This does not implement [[CallGenerator]] as the interface does not fit, because the [[Sarg]]
 * argument cannot be converted directly to [[GeneratedExpression]].
 */
object SearchOperatorGen {

  /**
   * Generates SEARCH expression using either an HashSet or a concatenation of OR,
   * depending on whether the elements of the haystack are all literals or not.
   *
   * Note that both IN/NOT IN are converted to SEARCH when the set has only constant values,
   * otherwise the IN/NOT IN are converted to a set of disjunctions. See
   * [[org.apache.calcite.rex.RexBuilder#makeIn(org.apache.calcite.rex.RexNode, java.util.List)]].
   */
  def generateSearch(
       ctx: CodeGeneratorContext,
       target: GeneratedExpression,
       sargLiteral: RexLiteral): GeneratedExpression = {
    val sarg: Sarg[Nothing] = sargLiteral.getValueAs(classOf[Sarg[Nothing]])
    val targetType = target.resultType
    val sargType = FlinkTypeFactory.toLogicalType(sargLiteral.getType)

    val commonType: LogicalType = findCommonType(asList(targetType, sargType))
      .orElseThrow(() =>
        new CodeGenException(s"Unable to find common type of $target and $sargLiteral."))

    val needle = generateCast(
      ctx,
      target,
      commonType,
      nullOnFailure = false
    )

    // In case the search is among points we use the hashset implementation
    if (sarg.isPoints || sarg.isComplementedPoints) {
      val rangeSet = if (sarg.isPoints) sarg.rangeSet else sarg.rangeSet.complement()
      val haystack = rangeSet
        .asRanges()
        .asScala
        // We need to go through the generateLiteral to normalize the value from calcite
        .map(r => toFlinkInternalValue(r.lowerEndpoint, sargType))
        // The elements are constant, we perform the cast at priori
        .map(CastRuleProvider.cast(toCastContext(ctx), sargType, commonType, _))
        .map(generateLiteral(ctx, _, commonType))
      if (sarg.containsNull) {
        haystack += generateNullLiteral(commonType, ctx.nullCheck)
      }
      val setTerm = ctx.addReusableHashSet(haystack.toSeq, commonType)
      val negation = if (sarg.isComplementedPoints) "!" else ""

      val Seq(resultTerm, nullTerm) = newNames("result", "isNull")

      val operatorCode = if (ctx.nullCheck) {
        s"""
           |${needle.code}
           |// --- Begin SEARCH ${target.resultTerm}
           |boolean $resultTerm = false;
           |boolean $nullTerm = true;
           |if (!${needle.nullTerm}) {
           |  $resultTerm = $negation$setTerm.contains(${needle.resultTerm});
           |  $nullTerm = !$resultTerm && $setTerm.containsNull();
           |}
           |// --- End SEARCH ${target.resultTerm}
           |""".stripMargin.trim
      }
      else {
        s"""
           |${needle.code}
           |// --- Begin SEARCH ${target.resultTerm}
           |boolean $resultTerm = $negation$setTerm.contains(${needle.resultTerm});
           |// --- End SEARCH ${target.resultTerm}
           |""".stripMargin.trim
      }

      GeneratedExpression(resultTerm, nullTerm, operatorCode, new BooleanType())
    } else {
      // We copy the target to don't re-evaluate on each range check
      val dummyTarget = target.copy(code = "")

      val rangeToExpression = new RangeToExpression(ctx, sargType, dummyTarget)

      // We use a chain of ORs and range comparisons
      var rangeChecks: Seq[GeneratedExpression] = sarg
        .rangeSet
        .asRanges
        .asScala
        .toSeq
        .map(RangeSets.map(_, rangeToExpression))

      if (sarg.containsNull) {
        rangeChecks = Seq(generateIsNull(ctx, target)) ++ rangeChecks
      }

      val generatedRangeChecks = rangeChecks
        .reduce((left, right) => generateOr(ctx, left, right))

      // Add the target expression code
      val finalCode =
        s"""
           |${target.code}
           |// --- Begin SEARCH ${target.resultTerm}
           |${generatedRangeChecks.code}
           |// --- End SEARCH ${target.resultTerm}
           |""".stripMargin.trim
      generatedRangeChecks.copy(code = finalCode)
    }
  }

  private class RangeToExpression[C <: Comparable[C]](
       ctx: CodeGeneratorContext,
       boundType: LogicalType,
       target: GeneratedExpression) extends RangeSets.Handler[C, GeneratedExpression] {

    override def all(): GeneratedExpression = {
      generateLiteral(ctx, true, new BooleanType())
    }

    /**
     * lower <= target
     */
    override def atLeast(lower: C): GeneratedExpression = {
      generateComparison(ctx, "<=", lit(lower), target)
    }

    /**
     * target <= upper
     */
    override def atMost(upper: C): GeneratedExpression = {
      generateComparison(ctx, "<=", target, lit(upper))
    }

    /**
     * lower < target
     */
    override def greaterThan(lower: C): GeneratedExpression = {
      generateComparison(ctx, "<", lit(lower), target)
    }

    /**
     * target < upper
     */
    override def lessThan(upper: C): GeneratedExpression = {
      generateComparison(ctx, "<", target, lit(upper))
    }

    /**
     * value == target
     */
    override def singleton(value: C): GeneratedExpression = {
      generateComparison(ctx, "==", lit(value), target)
    }

    /**
     * lower <= target && target <= upper
     */
    override def closed(lower: C, upper: C): GeneratedExpression = {
      generateAnd(
        ctx,
        generateComparison(ctx, "<=", lit(lower), target),
        generateComparison(ctx, "<=", target, lit(upper))
      )
    }

    /**
     * lower <= target && target < upper
     */
    override def closedOpen(lower: C, upper: C): GeneratedExpression = {
      generateAnd(
        ctx,
        generateComparison(ctx, "<=", lit(lower), target),
        generateComparison(ctx, "<", target, lit(upper))
      )
    }

    /**
     * lower < target && target <= upper
     */
    override def openClosed(lower: C, upper: C): GeneratedExpression = {
      generateAnd(
        ctx,
        generateComparison(ctx, "<", lit(lower), target),
        generateComparison(ctx, "<=", target, lit(upper))
      )
    }

    /**
     * lower < target && target < upper
     */
    override def open(lower: C, upper: C): GeneratedExpression = {
      generateAnd(
        ctx,
        generateComparison(ctx, "<", lit(lower), target),
        generateComparison(ctx, "<", target, lit(upper))
      )
    }

    private def lit(value: C): GeneratedExpression = {
      generateLiteral(ctx, toFlinkInternalValue(value, boundType), boundType)
    }
  }

}
