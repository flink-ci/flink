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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkRoundRobinPartitioner;

import org.junit.Assert;
import org.junit.Test;

/** Tests for the {@link FlinkRoundRobinPartitioner}. */
public class FlinkRoundRobinPartitionerTest {

    @Test
    public void testRoundRobin() {
        FlinkRoundRobinPartitioner<String> part = new FlinkRoundRobinPartitioner<>();

        int[] partitions = new int[] {0, 1, 2, 3, 4};
        String topic = "topic1";
        part.open(0, 2);

        Assert.assertEquals(0, part.partition("abc1", null, null, topic, partitions));
        Assert.assertEquals(1, part.partition("abc2", null, null, topic, partitions));
        Assert.assertEquals(2, part.partition("abc3", null, null, topic, partitions));
        Assert.assertEquals(3, part.partition("abc4", null, null, topic, partitions));
        Assert.assertEquals(4, part.partition("abc5", null, null, topic, partitions));

        part.open(1, 2);
        Assert.assertEquals(0, part.partition("abc1", null, null, topic, partitions));
        Assert.assertEquals(1, part.partition("abc2", null, null, topic, partitions));
        Assert.assertEquals(2, part.partition("abc3", null, null, topic, partitions));
        Assert.assertEquals(3, part.partition("abc4", null, null, topic, partitions));
        Assert.assertEquals(4, part.partition("abc5", null, null, topic, partitions));
    }
}
