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

package org.apache.flink.api.dag;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests for {@link Transformation}. */
public class TransformationTest extends TestLogger {

    private Transformation<Void> transformation;

    @Before
    public void setUp() {
        transformation = new TestTransformation<>("t", null, 1);
    }

    @Test
    public void testDeclareManagedMemoryUseCase() {
        transformation.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, 123);
        transformation.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.STATE_BACKEND);
        assertThat(
                transformation
                        .getManagedMemoryOperatorScopeUseCaseWeights()
                        .get(ManagedMemoryUseCase.OPERATOR),
                is(123));
        assertThat(
                transformation.getManagedMemorySlotScopeUseCases(),
                contains(ManagedMemoryUseCase.STATE_BACKEND));
    }

    @Test
    public void testDeclareManagedMemoryOperatorScopeUseCaseFailWrongScope() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
                    transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.PYTHON, 123);
        });
    }

    @Test
    public void testDeclareManagedMemoryOperatorScopeUseCaseFailZeroWeight() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
                    transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 0);
        });
    }

    @Test
    public void testDeclareManagedMemoryOperatorScopeUseCaseFailNegativeWeight() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
                    transformation.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, -1);
        });
    }

    @Test
    public void testDeclareManagedMemorySlotScopeUseCaseFailWrongScope() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
                    transformation.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.OPERATOR);
        });
    }

    /** A test implementation of {@link Transformation}. */
    private static class TestTransformation<T> extends Transformation<T> {

        public TestTransformation(String name, TypeInformation<T> outputType, int parallelism) {
            super(name, outputType, parallelism);
        }

        @Override
        public List<Transformation<?>> getTransitivePredecessors() {
            return Collections.emptyList();
        }

        @Override
        public List<Transformation<?>> getInputs() {
            return Collections.emptyList();
        }
    }
}
