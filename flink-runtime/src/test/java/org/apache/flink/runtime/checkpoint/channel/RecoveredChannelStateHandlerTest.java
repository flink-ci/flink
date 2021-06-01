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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor.MappingType;
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;

import org.junit.Test;

import java.util.HashSet;

import static org.junit.Assert.assertEquals;

/** Test of different implementation of {@link InputChannelRecoveredStateHandler}. */
public class RecoveredChannelStateHandlerTest<Info, Context> {

    @Test
    public void testRecycleBufferForInputChannelBeforeRecoverWasCalled() throws Exception {
        // given: Segment provider with defined number of allocated segments.
        int preAllocatedSegments = 3;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(preAllocatedSegments, 1024);

        // and: Configured input gate with recovered channel.
        SingleInputGate inputGate =
                new SingleInputGateBuilder()
                        .setChannelFactory(InputChannelBuilder::buildLocalRecoveredChannel)
                        .setSegmentProvider(networkBufferPool)
                        .build();

        InputChannelRecoveredStateHandler icsHander = buildInputChannelStateHandler(inputGate);

        InputChannelInfo channelInfo = new InputChannelInfo(0, 0);

        // when: Request the buffer.
        RecoveredChannelStateHandler.BufferWithContext<Buffer> bufferWithContext =
                icsHander.getBuffer(channelInfo);

        // and: Recycle buffer outside.
        bufferWithContext.buffer.close();

        // Close the gate for flushing the cached recycled buffers to the segment provider.
        inputGate.close();

        // then: All pre-allocated segments should be successfully recycled.
        assertEquals(preAllocatedSegments, networkBufferPool.getNumberOfAvailableMemorySegments());
    }

    @Test
    public void testRecycleBufferForInputChannelAfterRecoverWasCalled() throws Exception {
        // given: Segment provider with defined number of allocated segments.
        int preAllocatedSegments = 3;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(preAllocatedSegments, 1024);

        // and: Configured input gate with recovered channel.
        SingleInputGate inputGate =
                new SingleInputGateBuilder()
                        .setChannelFactory(InputChannelBuilder::buildLocalRecoveredChannel)
                        .setSegmentProvider(networkBufferPool)
                        .build();

        InputChannelRecoveredStateHandler icsHander = buildInputChannelStateHandler(inputGate);

        InputChannelInfo channelInfo = new InputChannelInfo(0, 0);

        // when: Request the buffer.
        RecoveredChannelStateHandler.BufferWithContext<Buffer> bufferWithContext =
                icsHander.getBuffer(channelInfo);

        // and: Recycle buffer outside.
        icsHander.recover(channelInfo, 0, bufferWithContext);

        // Close the gate for flushing the cached recycled buffers to the segment provider.
        inputGate.close();

        // then: All pre-allocated segments should be successfully recycled.
        assertEquals(preAllocatedSegments, networkBufferPool.getNumberOfAvailableMemorySegments());
    }

    private InputChannelRecoveredStateHandler buildInputChannelStateHandler(
            SingleInputGate inputGate) {
        return new InputChannelRecoveredStateHandler(
                new InputGate[] {inputGate},
                new InflightDataRescalingDescriptor(
                        new InflightDataGateOrPartitionRescalingDescriptor[] {
                            new InflightDataGateOrPartitionRescalingDescriptor(
                                    new int[] {1},
                                    RescaleMappings.identity(1, 1),
                                    new HashSet<>(),
                                    MappingType.IDENTITY)
                        }));
    }

    @Test
    public void testRecycleBufferForResultSubpartitionChannelBeforeRecoverWasCalled()
            throws Exception {
        // given: Result partition with defined number of allocated segments.
        int preAllocatedSegments = 3;

        NetworkBufferPool networkBufferPool = new NetworkBufferPool(preAllocatedSegments, 1024);
        ResultPartition partition =
                new ResultPartitionBuilder().setNetworkBufferPool(networkBufferPool).build();
        partition.setup();

        ResultSubpartitionRecoveredStateHandler rstHandler = buildResultStateHandler(partition);

        // when: Request the buffer.
        RecoveredChannelStateHandler.BufferWithContext<BufferBuilder> bufferWithContext =
                rstHandler.getBuffer(new ResultSubpartitionInfo(0, 0));

        // and: Recycle buffer outside.
        bufferWithContext.buffer.close();

        // Close the partition for flushing the cached recycled buffers to the segment provider.
        partition.close();

        // then: All pre-allocated segments should be successfully recycled.
        assertEquals(preAllocatedSegments, networkBufferPool.getNumberOfAvailableMemorySegments());
    }

    private ResultSubpartitionRecoveredStateHandler buildResultStateHandler(
            ResultPartition partition) {
        return new ResultSubpartitionRecoveredStateHandler(
                new ResultPartitionWriter[] {partition},
                false,
                new InflightDataRescalingDescriptor(
                        new InflightDataGateOrPartitionRescalingDescriptor[] {
                            new InflightDataGateOrPartitionRescalingDescriptor(
                                    new int[] {1},
                                    RescaleMappings.identity(1, 1),
                                    new HashSet<>(),
                                    MappingType.IDENTITY)
                        }));
    }

    @Test
    public void testRecycleBufferForResultSubpartitionAfterRecoverWasCalled() throws Exception {
        // given: Result partition with defined number of allocated segments.
        int preAllocatedSegments = 3;
        ResultSubpartitionInfo channelInfo = new ResultSubpartitionInfo(0, 0);

        NetworkBufferPool networkBufferPool = new NetworkBufferPool(preAllocatedSegments, 1024);
        ResultPartition partition =
                new ResultPartitionBuilder().setNetworkBufferPool(networkBufferPool).build();
        partition.setup();

        ResultSubpartitionRecoveredStateHandler rsp = buildResultStateHandler(partition);

        // when: Request the buffer.
        RecoveredChannelStateHandler.BufferWithContext<BufferBuilder> bufferWithContext =
                rsp.getBuffer(channelInfo);

        // and: Pass the buffer to recovery.
        rsp.recover(channelInfo, 0, bufferWithContext);

        // Close the partition for flushing the cached recycled buffers to the segment provider.
        partition.close();

        // then: All pre-allocated segments should be successfully recycled.
        assertEquals(preAllocatedSegments, networkBufferPool.getNumberOfAvailableMemorySegments());
    }
}
