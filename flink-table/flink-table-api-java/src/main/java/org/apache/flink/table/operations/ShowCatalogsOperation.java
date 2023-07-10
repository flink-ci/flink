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

package org.apache.flink.table.operations;

import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.functions.SqlLikeUtils;

import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.table.api.internal.TableResultUtils.buildStringArrayResult;

/** Operation to describe a SHOW CATALOGS statement. */
public class ShowCatalogsOperation implements ShowOperation {

    // different like type such as like, ilike
    private final LikeType likeType;
    private final boolean notLike;
    private final String likePattern;

    public ShowCatalogsOperation(String likeType, String likePattern, boolean notLike) {
        if (likeType != null) {
            this.likeType = LikeType.of(likeType);
            this.likePattern = requireNonNull(likePattern, "Like pattern must not be null");
            this.notLike = notLike;
        } else {
            this.likeType = null;
            this.likePattern = null;
            this.notLike = false;
        }
    }

    public boolean isLike() {
        return likeType == LikeType.LIKE;
    }

    public boolean isIlike() {
        return likeType == LikeType.ILIKE;
    }

    public boolean isWithLike() {
        return likeType != null;
    }

    public boolean isNotLike() {
        return notLike;
    }

    @Override
    public String asSummaryString() {
        StringBuilder builder = new StringBuilder().append("SHOW CATALOGS");
        if (isWithLike()) {
            if (isNotLike()) {
                builder.append(String.format(" NOT %s '%s'", likeType.name(), likePattern));
            } else {
                builder.append(String.format(" %s '%s'", likeType.name(), likePattern));
            }
        }
        return builder.toString();
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        Set<String> catalogs = ctx.getCatalogManager().listCatalogs();
        String[] rows;
        if (isWithLike()) {
            rows =
                    catalogs.stream()
                            .filter(
                                    row -> {
                                        if (likeType == LikeType.ILIKE) {
                                            return isNotLike()
                                                    != SqlLikeUtils.ilike(row, likePattern, "\\");
                                        } else {
                                            return isNotLike()
                                                    != SqlLikeUtils.like(row, likePattern, "\\");
                                        }
                                    })
                            .sorted()
                            .toArray(String[]::new);
        } else {
            rows = catalogs.stream().sorted().toArray(String[]::new);
        }
        return buildStringArrayResult("catalog name", rows);
    }
}
