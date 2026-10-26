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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VariantType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import java.util.Optional;

/**
 * Type strategy for {@code VARIANT_GET(variant, path [, type])} and {@code TRY_VARIANT_GET}.
 *
 * <p>The result is the type named by the {@code type} literal, or {@code VARIANT} without one. It
 * is always nullable, because a path that points at nothing gives {@code NULL}.
 */
@Internal
public final class VariantGetTypeStrategy implements TypeStrategy {

    static final int ARG_TYPE = 2;

    @Override
    public Optional<DataType> inferType(final CallContext callContext) {
        if (callContext.getArgumentDataTypes().size() <= ARG_TYPE) {
            return Optional.of(DataTypes.VARIANT().nullable());
        }
        return Optional.of(resolveTargetType(callContext));
    }

    /**
     * Resolves the {@code type} literal to a nullable type that {@code VARIANT} can be cast to.
     *
     * @throws ValidationException if the literal is not a type or names a type that {@code VARIANT}
     *     cannot be cast to
     */
    static DataType resolveTargetType(final CallContext callContext) {
        // The input strategy only accepts a non-null literal.
        final String typeString =
                callContext
                        .getArgumentValue(ARG_TYPE, String.class)
                        .orElseThrow(IllegalStateException::new);
        final DataType type;
        try {
            type = callContext.getDataTypeFactory().createDataType(typeString);
        } catch (RuntimeException e) {
            throw new ValidationException(
                    String.format(
                            "Invalid type '%s' for %s: %s",
                            typeString, callContext.getName(), e.getMessage()),
                    e);
        }
        final LogicalType logicalType = type.getLogicalType();
        if (!LogicalTypeCasts.supportsExplicitCast(new VariantType(), logicalType)) {
            throw new ValidationException(
                    String.format(
                            "%s cannot return type %s, because a VARIANT cannot be cast to it.",
                            callContext.getName(), logicalType.asSummaryString()));
        }
        return type.nullable();
    }
}
