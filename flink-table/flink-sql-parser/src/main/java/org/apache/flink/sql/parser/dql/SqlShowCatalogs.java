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

package org.apache.flink.sql.parser.dql;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/** SHOW CATALOGS sql call. */
public class SqlShowCatalogs extends SqlCall {

    // different like type such as like, ilike
    protected final String likeType;
    protected final SqlCharStringLiteral likeLiteral;
    protected final boolean notLike;

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SHOW CATALOGS", SqlKind.OTHER);

    public SqlShowCatalogs(
            SqlParserPos pos, String likeType, SqlCharStringLiteral likeLiteral, boolean notLike) {
        super(pos);
        if (likeType != null) {
            this.likeType = likeType;
            this.likeLiteral = requireNonNull(likeLiteral, "Like pattern must not be null");
            this.notLike = notLike;
        } else {
            this.likeType = null;
            this.likeLiteral = null;
            this.notLike = false;
        }
    }

    public String getLikeType() {
        return likeType;
    }

    public boolean isWithLike() {
        return likeType != null;
    }

    public String getLikeSqlPattern() {
        return Objects.isNull(likeLiteral) ? null : likeLiteral.getValueAs(String.class);
    }

    public boolean isNotLike() {
        return notLike;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW CATALOGS");
        if (isWithLike()) {
            if (isNotLike()) {
                writer.keyword(String.format("NOT %s '%s'", likeType, getLikeSqlPattern()));
            } else {
                writer.keyword(String.format("%s '%s'", likeType, getLikeSqlPattern()));
            }
        }
    }
}
