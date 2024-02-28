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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.process.api.context.ProcessingTimeManager;
import org.apache.flink.process.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.process.impl.common.OutputCollector;
import org.apache.flink.process.impl.common.TimestampCollector;
import org.apache.flink.process.impl.context.DefaultProcessingTimeManager;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

import java.util.Optional;

/** */
public class KeyedTwoOutputProcessOperator<KEY, IN, OUT_MAIN, OUT_SIDE>
        extends TwoOutputProcessOperator<IN, OUT_MAIN, OUT_SIDE>
        implements Triggerable<KEY, VoidNamespace> {

    private transient InternalTimerService<VoidNamespace> timerService;

    @Nullable private final KeySelector<OUT_MAIN, KEY> mainOutKeySelector;

    @Nullable private final KeySelector<OUT_SIDE, KEY> sideOutKeySelector;

    public KeyedTwoOutputProcessOperator(
            TwoOutputStreamProcessFunction<IN, OUT_MAIN, OUT_SIDE> userFunction,
            OutputTag<OUT_SIDE> outputTag) {
        this(userFunction, outputTag, null, null);
    }

    public KeyedTwoOutputProcessOperator(
            TwoOutputStreamProcessFunction<IN, OUT_MAIN, OUT_SIDE> userFunction,
            OutputTag<OUT_SIDE> outputTag,
            @Nullable KeySelector<OUT_MAIN, KEY> mainOutKeySelector,
            @Nullable KeySelector<OUT_SIDE, KEY> sideOutKeySelector) {
        super(userFunction, outputTag);
        this.mainOutKeySelector = mainOutKeySelector;
        this.sideOutKeySelector = sideOutKeySelector;
    }

    @Override
    public void open() throws Exception {
        this.timerService =
                getInternalTimerService("processing timer", VoidNamespaceSerializer.INSTANCE, this);
        super.open();
    }

    @Override
    protected TimestampCollector<OUT_MAIN> getMainCollector() {
        return mainOutKeySelector != null && sideOutKeySelector != null
                ? new KeyCheckedOutputCollector<>(new OutputCollector<>(output), mainOutKeySelector)
                : new OutputCollector<>(output);
    }

    @Override
    public TimestampCollector<OUT_SIDE> getSideCollector() {
        return mainOutKeySelector != null && sideOutKeySelector != null
                ? new KeyCheckedOutputCollector<>(
                        new SideOutputCollector(output), sideOutKeySelector)
                : new SideOutputCollector(output);
    }

    @Override
    protected Optional<Object> currentKey() {
        return Optional.ofNullable(getCurrentKey());
    }

    protected ProcessingTimeManager getProcessingTimeManager() {
        return new DefaultProcessingTimeManager(timerService);
    }

    @Override
    public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
        // do nothing at the moment.
    }

    @Override
    public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
        // align the key context with the registered timer.
        context.getStateManager().setCurrentKey(timer.getKey());
        userFunction.onProcessingTimer(
                timer.getTimestamp(), getMainCollector(), getSideCollector(), context);
        context.getStateManager().resetCurrentKey();
    }

    private class KeyCheckedOutputCollector<T> extends TimestampCollector<T> {

        private final TimestampCollector<T> outputCollector;

        private final KeySelector<T, KEY> outputKeySelector;

        private KeyCheckedOutputCollector(
                TimestampCollector<T> outputCollector, KeySelector<T, KEY> outputKeySelector) {
            this.outputCollector = outputCollector;
            this.outputKeySelector = outputKeySelector;
        }

        @Override
        public void collect(T outputRecord) {
            checkOutputKey(outputRecord);
            this.outputCollector.collect(outputRecord);
        }

        @Override
        public void collectAndOverwriteTimestamp(T outputRecord, long timestamp) {
            checkOutputKey(outputRecord);
            this.outputCollector.collectAndOverwriteTimestamp(outputRecord, timestamp);
        }

        @SuppressWarnings("unchecked")
        private void checkOutputKey(T outputRecord) {
            try {
                KEY currentKey = (KEY) getCurrentKey();
                KEY outputKey = this.outputKeySelector.getKey(outputRecord);
                if (!outputKey.equals(currentKey)) {
                    throw new IllegalStateException(
                            "Output key must equals to input key if you want the produced stream "
                                    + "is keyed. ");
                }
            } catch (Exception e) {
                // TODO Change Consumer to ThrowingConsumer.
                ExceptionUtils.rethrow(e);
            }
        }
    }
}
