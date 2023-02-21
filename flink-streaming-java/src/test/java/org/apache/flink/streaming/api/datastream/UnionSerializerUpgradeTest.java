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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.TaggedUnion;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.UnionSerializer;

import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.is;

/** A {@link TypeSerializerUpgradeTestBase} for {@link UnionSerializer}. */
class UnionSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<
                TaggedUnion<String, Long>, TaggedUnion<String, Long>> {

    public Collection<TestSpecification<?, ?>> createTestSpecifications() throws Exception {
        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        for (FlinkVersion flinkVersion : MIGRATION_VERSIONS) {
            testSpecifications.add(
                    new TestSpecification<>(
                            "union-serializer-one",
                            flinkVersion,
                            UnionSerializerOneSetup.class,
                            UnionSerializerOneVerifier.class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "union-serializer-two",
                            flinkVersion,
                            UnionSerializerTwoSetup.class,
                            UnionSerializerTwoVerifier.class));
        }
        return testSpecifications;
    }

    private static TypeSerializer<TaggedUnion<String, Long>> stringLongRowSupplier() {
        return new UnionSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE);
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "union-serializer-for-TaggedUnion.one"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class UnionSerializerOneSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<TaggedUnion<String, Long>> {
        @Override
        public TypeSerializer<TaggedUnion<String, Long>> createPriorSerializer() {
            return new UnionSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE);
        }

        @Override
        public TaggedUnion<String, Long> createTestData() {
            return TaggedUnion.one("flink");
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class UnionSerializerOneVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<TaggedUnion<String, Long>> {
        @Override
        public TypeSerializer<TaggedUnion<String, Long>> createUpgradedSerializer() {
            return new UnionSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE);
        }

        @Override
        public Matcher<TaggedUnion<String, Long>> testDataMatcher() {
            return is(TaggedUnion.one("flink"));
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<TaggedUnion<String, Long>>>
                schemaCompatibilityMatcher(FlinkVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "union-serializer-for-TaggedUnion.two"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class UnionSerializerTwoSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<TaggedUnion<String, Long>> {
        @Override
        public TypeSerializer<TaggedUnion<String, Long>> createPriorSerializer() {
            return new UnionSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE);
        }

        @Override
        public TaggedUnion<String, Long> createTestData() {
            return TaggedUnion.two(23456L);
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class UnionSerializerTwoVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<TaggedUnion<String, Long>> {
        @Override
        public TypeSerializer<TaggedUnion<String, Long>> createUpgradedSerializer() {
            return new UnionSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE);
        }

        @Override
        public Matcher<TaggedUnion<String, Long>> testDataMatcher() {
            return is(TaggedUnion.two(23456L));
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<TaggedUnion<String, Long>>>
                schemaCompatibilityMatcher(FlinkVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }
}
