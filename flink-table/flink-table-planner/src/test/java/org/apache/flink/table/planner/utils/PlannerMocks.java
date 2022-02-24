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

package org.apache.flink.table.planner.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerConfig;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.utils.CatalogManagerMocks;

import java.util.Collections;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;

/**
 * A utility class for instantiating and holding mocks for {@link FlinkPlannerImpl}, {@link
 * ParserImpl} and {@link CatalogManager} for testing.
 */
public class PlannerMocks {

    private final FlinkPlannerImpl planner;
    private final ParserImpl parser;
    private final CatalogManager catalogManager;
    private final FunctionCatalog functionCatalog;
    private final PlannerConfig plannerConfig;
    private final PlannerContext plannerContext;

    private PlannerMocks(boolean isBatchMode, PlannerConfig plannerConfig) {
        this.catalogManager = CatalogManagerMocks.createEmptyCatalogManager();
        this.plannerConfig = plannerConfig;

        final ModuleManager moduleManager = new ModuleManager();

        this.functionCatalog =
                new FunctionCatalog(plannerConfig.getTableConfig(), catalogManager, moduleManager);

        this.plannerContext =
                new PlannerContext(
                        isBatchMode,
                        plannerConfig,
                        moduleManager,
                        functionCatalog,
                        catalogManager,
                        asRootSchema(new CatalogManagerCalciteSchema(catalogManager, !isBatchMode)),
                        Collections.emptyList());

        this.planner =
                plannerContext.createFlinkPlanner(
                        catalogManager.getCurrentCatalog(), catalogManager.getCurrentDatabase());
        this.parser =
                new ParserImpl(
                        catalogManager,
                        () -> planner,
                        planner::parser,
                        plannerContext.getSqlExprToRexConverterFactory());

        catalogManager.initSchemaResolver(
                true,
                ExpressionResolver.resolverFor(
                        plannerConfig.getTableConfig(),
                        name -> {
                            throw new UnsupportedOperationException();
                        },
                        functionCatalog.asLookup(parser::parseIdentifier),
                        catalogManager.getDataTypeFactory(),
                        parser::parseSqlExpression));
    }

    public FlinkPlannerImpl getPlanner() {
        return planner;
    }

    public ParserImpl getParser() {
        return parser;
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public FunctionCatalog getFunctionCatalog() {
        return functionCatalog;
    }

    public PlannerConfig getPlannerConfig() {
        return plannerConfig;
    }

    public PlannerContext getPlannerContext() {
        return plannerContext;
    }

    public PlannerMocks registerTemporaryTable(String tableName, Schema tableSchema) {
        final CatalogTable table =
                CatalogTable.of(tableSchema, null, Collections.emptyList(), Collections.emptyMap());

        this.getCatalogManager()
                .createTemporaryTable(
                        table,
                        ObjectIdentifier.of(
                                this.getCatalogManager().getCurrentCatalog(),
                                this.getCatalogManager().getCurrentDatabase(),
                                tableName),
                        false);

        return this;
    }

    public static PlannerMocks create() {
        return create(false);
    }

    public static PlannerMocks create(boolean batchMode) {
        return new PlannerMocks(batchMode, PlannerConfig.getDefault());
    }

    public static PlannerMocks create(Configuration configuration) {
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.addConfiguration(configuration);
        return new PlannerMocks(false, PlannerConfig.of(tableConfig));
    }
}
