/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * {@link ForwardForLocalKeyByPartitioner} is a intermediate partitioner in optimization phase,
 * should be converted to following partitioners after the operator chain creation:
 *
 * <p>1. Be converted to {@link ForwardPartitioner} if this partitioner is intra-chain.
 *
 * <p>2. Be converted to {@link ForwardForLocalKeyByPartitioner#hashPartitioner} if this partitioner
 * is inter-chain.
 *
 * @param <T> Type of the elements in the Stream
 */
public class ForwardForLocalKeyByPartitioner<T> extends ForwardPartitioner<T> {

    private final StreamPartitioner<T> hashPartitioner;

    /**
     * Create a new ForwardForLocalKeyPartitioner.
     *
     * @param hashPartitioner the HashPartitioner
     */
    public ForwardForLocalKeyByPartitioner(StreamPartitioner<T> hashPartitioner) {
        this.hashPartitioner = hashPartitioner;
    }

    @Override
    public StreamPartitioner<T> copy() {
        throw new RuntimeException(
                "ForwardForLocalKeyPartitioner is a intermediate partitioner in optimization phase, "
                        + "should be converted to ForwardPartitioner/HashPartitioner finally.");
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        throw new RuntimeException(
                "ForwardForLocalKeyPartitioner is a intermediate partitioner in optimization phase, "
                        + "should be converted to ForwardPartitioner/HashPartitioner finally.");
    }

    @Override
    public boolean isPointwise() {
        // will be used in StreamGraphGenerator#shouldDisableUnalignedCheckpointing, so can't throw
        // exception.
        return true;
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        throw new RuntimeException(
                "ForwardForLocalKeyPartitioner is a intermediate partitioner in optimization phase, "
                        + "should be converted to ForwardPartitioner/HashPartitioner finally.");
    }

    public StreamPartitioner<T> getHashPartitioner() {
        return hashPartitioner;
    }
}
