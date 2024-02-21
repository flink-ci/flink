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

package org.apache.flink.process.impl.operators;

import org.apache.flink.process.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.process.impl.common.OutputCollector;
import org.apache.flink.process.impl.common.TimestampCollector;
import org.apache.flink.process.impl.context.DefaultNonPartitionedContext;
import org.apache.flink.process.impl.context.DefaultRuntimeContext;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import static org.apache.flink.util.Preconditions.checkState;

/** Operator for {@link TwoInputNonBroadcastStreamProcessFunction}. */
public class TwoInputNonBroadcastProcessOperator<IN1, IN2, OUT>
        extends AbstractUdfStreamOperator<
                OUT, TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT>, BoundedMultiInput {

    protected transient TimestampCollector<OUT> collector;

    protected transient DefaultRuntimeContext context;

    protected transient DefaultNonPartitionedContext<OUT> nonPartitionedContext;

    public TwoInputNonBroadcastProcessOperator(
            TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> userFunction) {
        super(userFunction);
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.collector = getOutputCollector();
        this.context = new DefaultRuntimeContext();
        this.nonPartitionedContext = new DefaultNonPartitionedContext<>();
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        collector.setTimestamp(element);
        userFunction.processRecordFromFirstInput(element.getValue(), collector, context);
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        collector.setTimestamp(element);
        userFunction.processRecordFromSecondInput(element.getValue(), collector, context);
    }

    protected TimestampCollector<OUT> getOutputCollector() {
        return new OutputCollector<>(output);
    }

    @Override
    public void endInput(int inputId) throws Exception {
        // sanity check.
        checkState(inputId >= 1 && inputId <= 2);
        if (inputId == 1) {
            userFunction.endFirstInput(nonPartitionedContext);
        } else {
            userFunction.endSecondInput(nonPartitionedContext);
        }
    }
}
