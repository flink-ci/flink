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

package org.apache.flink.queryablestate.client.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

import org.junit.Before;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/** Tests the {@link ImmutableValueState}. */
public class ImmutableValueStateTest {

    private final ValueStateDescriptor<Long> valueStateDesc =
            new ValueStateDescriptor<>("test", BasicTypeInfo.LONG_TYPE_INFO);

    private ValueState<Long> valueState;

    @Before
    public void setUp() throws Exception {
        if (!valueStateDesc.isSerializerInitialized()) {
            valueStateDesc.initializeSerializerUnlessSet(new ExecutionConfig());
        }

        valueState =
                ImmutableValueState.createState(
                        valueStateDesc, ByteBuffer.allocate(Long.BYTES).putLong(42L).array());
    }

    @Test
    public void testUpdate() throws IOException {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
                    long value = valueState.value();
        assertEquals(42L, value);

        valueState.update(54L);
        });
    }

    @Test
    public void testClear() throws IOException {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
                    long value = valueState.value();
        assertEquals(42L, value);

        valueState.clear();
        });
    }
}
