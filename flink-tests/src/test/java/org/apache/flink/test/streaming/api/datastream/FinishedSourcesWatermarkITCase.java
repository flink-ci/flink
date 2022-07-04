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

package org.apache.flink.test.streaming.api.datastream;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.joda.time.DateTime;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

/** Test that ensures watermarks are correctly propagating with finished sources. */
public class FinishedSourcesWatermarkITCase extends TestLogger {

    private static final AtomicLong ACTUAL_DOWNSTREAM_WATERMARK = new AtomicLong(0);

    @Test
    public void testTwoConsecutiveFinishedTasksShouldPropagateMaxWatermark() throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        // disable chaining to make sure we will have two consecutive checkpoints with Task ==
        // FINISHED
        env.disableOperatorChaining();
        env.enableCheckpointing(100);

        // create our sources - one that will want to run forever, and another that finishes
        // immediately
        DataStream<String> runningStreamIn =
                env.addSource(new LongRunningSource(), "Long Running Source");
        DataStream<String> emptyStream =
                env.addSource(new ShortLivedEmptySource(), "Short Lived Source");

        // pass the empty stream through a simple map() function
        DataStream<String> mappedEmptyStream = emptyStream.map(v -> v).name("Empty Stream Map");

        // join the two streams together to see what watermark is reached during startup and after a
        // recovery
        runningStreamIn
                .connect(mappedEmptyStream)
                .process(new MyCoProcessFunction())
                .name("Join")
                .addSink(
                        new SinkFunction<String>() {
                            @Override
                            public void writeWatermark(
                                    org.apache.flink.api.common.eventtime.Watermark watermark) {
                                ACTUAL_DOWNSTREAM_WATERMARK.set(watermark.getTimestamp());
                            }
                        });

        env.execute();
    }

    private static class LongRunningSource extends RichSourceFunction<String>
            implements CheckpointListener {
        private volatile boolean isRunning = true;
        private transient long lastSuccessfulCheckpointId;

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            long expectedWatermark = Watermark.MAX_WATERMARK.getTimestamp();
            long watermark = DateTime.now().getMillis();
            sourceContext.emitWatermark(new Watermark(watermark));

            while (isRunning && expectedWatermark > ACTUAL_DOWNSTREAM_WATERMARK.get()) {
                synchronized (sourceContext.getCheckpointLock()) {
                    watermark = DateTime.now().getMillis();
                    sourceContext.emitWatermark(new Watermark(watermark));
                    if (lastSuccessfulCheckpointId == 5) {
                        throw new RuntimeException("Force recovery");
                    }
                    if (lastSuccessfulCheckpointId > 10 && expectedWatermark > watermark) {
                        expectedWatermark = watermark;
                    }
                    Thread.sleep(1);
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            lastSuccessfulCheckpointId = checkpointId;
        }
    }

    private static class ShortLivedEmptySource extends RichSourceFunction<String> {
        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {}

        public void cancel() {}
    }

    private static class MyCoProcessFunction extends CoProcessFunction<String, String, String> {
        @Override
        public void processElement1(String val, Context context, Collector<String> collector) {}

        @Override
        public void processElement2(String val, Context context, Collector<String> collector) {}
    }
}
