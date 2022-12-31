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

package org.apache.flink.table.planner.plan.abilities.sink;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;
import java.util.Objects;

/**
 * A sub-class of {@link SinkAbilitySpec} that can not only serialize/deserialize the row-level
 * update mode & updated columns to/from JSON, but also can update existing data for {@link
 * org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("RowLevelUpdate")
public class RowLevelUpdateSpec implements SinkAbilitySpec {
    public static final String FIELD_NAME_UPDATED_COLUMNS = "updatedColumns";
    public static final String FIELD_NAME_ROW_LEVEL_UPDATE_MODE = "rowLevelUpdateMode";

    @JsonProperty(FIELD_NAME_UPDATED_COLUMNS)
    private final List<Column> updatedColumns;

    @JsonProperty(FIELD_NAME_ROW_LEVEL_UPDATE_MODE)
    private final SupportsRowLevelUpdate.RowLevelUpdateInfo.RowLevelUpdateMode rowLevelUpdateMode;

    @JsonCreator
    public RowLevelUpdateSpec(
            @JsonProperty(FIELD_NAME_UPDATED_COLUMNS) List<Column> updatedColumns,
            @JsonProperty(FIELD_NAME_ROW_LEVEL_UPDATE_MODE)
                    SupportsRowLevelUpdate.RowLevelUpdateInfo.RowLevelUpdateMode
                            rowLevelUpdateMode) {
        this.updatedColumns = updatedColumns;
        this.rowLevelUpdateMode = rowLevelUpdateMode;
    }

    @Override
    public void apply(DynamicTableSink tableSink) {
        if (tableSink instanceof SupportsRowLevelUpdate) {
            ((SupportsRowLevelUpdate) tableSink).applyRowLevelUpdate(updatedColumns);
        } else {
            throw new TableException(
                    String.format(
                            "%s does not support SupportsRowLevelUpdate.",
                            tableSink.getClass().getName()));
        }
    }

    public SupportsRowLevelUpdate.RowLevelUpdateInfo.RowLevelUpdateMode getRowLevelUpdateMode() {
        return rowLevelUpdateMode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowLevelUpdateSpec that = (RowLevelUpdateSpec) o;
        return Objects.equals(updatedColumns, that.updatedColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updatedColumns);
    }
}
