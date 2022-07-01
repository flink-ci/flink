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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Base test case for built-in FIRST_VALUE and LAST_VALUE (with retract) aggregate function. This
 * class tests `accumulate` method with order and ignore nulls.
 */
public abstract class FirstLastValueIgnoreNullsAggFunctionWithOrderTestBase<T, ACC>
        extends FirstLastValueAggFunctionWithOrderTestBase<T, ACC> {

    protected Method getAccumulateFunc() throws NoSuchMethodException {
        return getAggregator()
                .getClass()
                .getMethod("accumulate", getAccClass(), Object.class, Long.class, boolean.class);
    }

    protected Method getRetractFunc() throws NoSuchMethodException {
        return getAggregator()
                .getClass()
                .getMethod("retract", getAccClass(), Object.class, Long.class, boolean.class);
    }

    protected abstract List<List<Long>> getInputOrderSets();

    protected ACC accumulateValues(List<T> values, List<Long> orders)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Preconditions.checkArgument(
                values.size() == orders.size(),
                "The number of values is not same with the number of orders, "
                        + "\nvalues: "
                        + values
                        + "\norders: "
                        + orders);
        AggregateFunction<T, ACC> aggregator = getAggregator();
        ACC accumulator = getAggregator().createAccumulator();
        Method accumulateFunc = getAccumulateFunc();
        for (int i = 0; i < values.size(); ++i) {
            accumulateFunc.invoke(aggregator, accumulator, values.get(i), orders.get(i), true);
        }
        return accumulator;
    }

    protected void retractValues(ACC accumulator, List<T> values, List<Long> orders)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Preconditions.checkArgument(
                values.size() == orders.size(),
                "The number of values is not same with the number of orders, "
                        + "\nvalues: "
                        + values
                        + "\norders: "
                        + orders);
        AggregateFunction<T, ACC> aggregator = getAggregator();
        Method retractFunc = getRetractFunc();
        for (int i = 0; i < values.size(); ++i) {
            retractFunc.invoke(aggregator, accumulator, values.get(i), orders.get(i), true);
        }
    }
}
