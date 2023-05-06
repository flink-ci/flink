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

package org.apache.flink.table.planner.operations;

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.operations.LoadModuleOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ShowFunctionsOperation;
import org.apache.flink.table.operations.ShowModulesOperation;
import org.apache.flink.table.operations.ShowTablesOperation;
import org.apache.flink.table.operations.UnloadModuleOperation;
import org.apache.flink.table.operations.UseCatalogOperation;
import org.apache.flink.table.operations.UseDatabaseOperation;
import org.apache.flink.table.operations.UseModulesOperation;
import org.apache.flink.table.operations.command.AddJarOperation;
import org.apache.flink.table.operations.command.ClearOperation;
import org.apache.flink.table.operations.command.ExecutePlanOperation;
import org.apache.flink.table.operations.command.HelpOperation;
import org.apache.flink.table.operations.command.QuitOperation;
import org.apache.flink.table.operations.command.RemoveJarOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.operations.command.ShowJarsOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.parse.ExtendedParser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test cases for the statements that neither belong to DDL nor DML for {@link
 * SqlNodeToOperationConversion}.
 */
public class SqlOtherOperationConverterTest extends SqlNodeToOperationConversionTestBase {

    @Test
    public void testUseCatalog() {
        final String sql = "USE CATALOG cat1";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(UseCatalogOperation.class);
        assertThat(((UseCatalogOperation) operation).getCatalogName()).isEqualTo("cat1");
        assertThat(operation.asSummaryString()).isEqualTo("USE CATALOG cat1");
    }

    @Test
    public void testUseDatabase() {
        final String sql1 = "USE db1";
        Operation operation1 = parse(sql1);
        assertThat(operation1).isInstanceOf(UseDatabaseOperation.class);
        assertThat(((UseDatabaseOperation) operation1).getCatalogName()).isEqualTo("builtin");
        assertThat(((UseDatabaseOperation) operation1).getDatabaseName()).isEqualTo("db1");

        final String sql2 = "USE cat1.db1";
        Operation operation2 = parse(sql2);
        assertThat(operation2).isInstanceOf(UseDatabaseOperation.class);
        assertThat(((UseDatabaseOperation) operation2).getCatalogName()).isEqualTo("cat1");
        assertThat(((UseDatabaseOperation) operation2).getDatabaseName()).isEqualTo("db1");
    }

    @Test
    public void testUseDatabaseWithException() {
        final String sql = "USE cat1.db1.tbl1";
        assertThatThrownBy(() -> parse(sql)).isInstanceOf(ValidationException.class);
    }

    @Test
    public void testLoadModule() {
        final String sql = "LOAD MODULE dummy WITH ('k1' = 'v1', 'k2' = 'v2')";
        final String expectedModuleName = "dummy";
        final Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("k1", "v1");
        expectedOptions.put("k2", "v2");

        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(LoadModuleOperation.class);
        final LoadModuleOperation loadModuleOperation = (LoadModuleOperation) operation;

        assertThat(loadModuleOperation.getModuleName()).isEqualTo(expectedModuleName);
        assertThat(loadModuleOperation.getOptions()).isEqualTo(expectedOptions);
    }

    @Test
    public void testUnloadModule() {
        final String sql = "UNLOAD MODULE dummy";
        final String expectedModuleName = "dummy";

        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(UnloadModuleOperation.class);

        final UnloadModuleOperation unloadModuleOperation = (UnloadModuleOperation) operation;

        assertThat(unloadModuleOperation.getModuleName()).isEqualTo(expectedModuleName);
    }

    @Test
    public void testUseOneModule() {
        final String sql = "USE MODULES dummy";
        final List<String> expectedModuleNames = Collections.singletonList("dummy");

        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(UseModulesOperation.class);

        final UseModulesOperation useModulesOperation = (UseModulesOperation) operation;

        assertThat(useModulesOperation.getModuleNames()).isEqualTo(expectedModuleNames);
        assertThat(useModulesOperation.asSummaryString()).isEqualTo("USE MODULES: [dummy]");
    }

    @Test
    public void testUseMultipleModules() {
        final String sql = "USE MODULES x, y, z";
        final List<String> expectedModuleNames = Arrays.asList("x", "y", "z");

        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(UseModulesOperation.class);

        final UseModulesOperation useModulesOperation = (UseModulesOperation) operation;

        assertThat(useModulesOperation.getModuleNames()).isEqualTo(expectedModuleNames);
        assertThat(useModulesOperation.asSummaryString()).isEqualTo("USE MODULES: [x, y, z]");
    }

    @Test
    public void testShowModules() {
        final String sql = "SHOW MODULES";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowModulesOperation.class);
        final ShowModulesOperation showModulesOperation = (ShowModulesOperation) operation;

        assertThat(showModulesOperation.requireFull()).isFalse();
        assertThat(showModulesOperation.asSummaryString()).isEqualTo("SHOW MODULES");
    }

    @Test
    public void testShowTables() {
        final String sql = "SHOW TABLES from cat1.db1 not like 't%'";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowTablesOperation.class);

        ShowTablesOperation showTablesOperation = (ShowTablesOperation) operation;
        assertThat(showTablesOperation.getCatalogName()).isEqualTo("cat1");
        assertThat(showTablesOperation.getDatabaseName()).isEqualTo("db1");
        assertThat(showTablesOperation.getPreposition()).isEqualTo("FROM");
        assertThat(showTablesOperation.isUseLike()).isTrue();
        assertThat(showTablesOperation.isNotLike()).isTrue();

        final String sql2 = "SHOW TABLES in db2";
        showTablesOperation = (ShowTablesOperation) parse(sql2);
        assertThat(showTablesOperation.getCatalogName()).isEqualTo("builtin");
        assertThat(showTablesOperation.getDatabaseName()).isEqualTo("db2");
        assertThat(showTablesOperation.getPreposition()).isEqualTo("IN");
        assertThat(showTablesOperation.isUseLike()).isFalse();
        assertThat(showTablesOperation.isNotLike()).isFalse();

        final String sql3 = "SHOW TABLES";
        showTablesOperation = (ShowTablesOperation) parse(sql3);
        assertThat(showTablesOperation.getCatalogName()).isNull();
        assertThat(showTablesOperation.getDatabaseName()).isNull();
        assertThat(showTablesOperation.getPreposition()).isNull();
    }

    @Test
    public void testShowFullModules() {
        final String sql = "SHOW FULL MODULES";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowModulesOperation.class);
        final ShowModulesOperation showModulesOperation = (ShowModulesOperation) operation;

        assertThat(showModulesOperation.requireFull()).isTrue();
        assertThat(showModulesOperation.asSummaryString()).isEqualTo("SHOW FULL MODULES");
    }

    @Test
    public void testShowFunctions() {
        final String sql1 = "SHOW FUNCTIONS";
        assertShowFunctions(sql1, sql1, ShowFunctionsOperation.FunctionScope.ALL);

        final String sql2 = "SHOW USER FUNCTIONS";
        assertShowFunctions(sql2, sql2, ShowFunctionsOperation.FunctionScope.USER);
    }

    @Test
    public void testAddJar() {
        Arrays.asList(
                        "./test.\njar",
                        "file:///path/to/whatever",
                        "../test-jar.jar",
                        "/root/test.jar",
                        "test\\ jar.jar",
                        "oss://path/helloworld.go")
                .forEach(
                        jarPath -> {
                            AddJarOperation operation =
                                    (AddJarOperation)
                                            parser.parse(String.format("ADD JAR '%s'", jarPath))
                                                    .get(0);
                            assertThat(operation.getPath()).isEqualTo(jarPath);
                        });
    }

    @Test
    public void testRemoveJar() {
        Arrays.asList(
                        "./test.\njar",
                        "file:///path/to/whatever",
                        "../test-jar.jar",
                        "/root/test.jar",
                        "test\\ jar.jar",
                        "oss://path/helloworld.go")
                .forEach(
                        jarPath -> {
                            RemoveJarOperation operation =
                                    (RemoveJarOperation)
                                            parser.parse(String.format("REMOVE JAR '%s'", jarPath))
                                                    .get(0);
                            assertThat(operation.getPath()).isEqualTo(jarPath);
                        });
    }

    @Test
    public void testShowJars() {
        final String sql = "SHOW JARS";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowJarsOperation.class);
        final ShowJarsOperation showModulesOperation = (ShowJarsOperation) operation;
        assertThat(showModulesOperation.asSummaryString()).isEqualTo("SHOW JARS");
    }

    @Test
    public void testSet() {
        Operation operation1 = parse("SET");
        assertThat(operation1).isInstanceOf(SetOperation.class);
        SetOperation setOperation1 = (SetOperation) operation1;
        assertThat(setOperation1.getKey()).isNotPresent();
        assertThat(setOperation1.getValue()).isNotPresent();

        Operation operation2 = parse("SET 'test-key' = 'test-value'");
        assertThat(operation2).isInstanceOf(SetOperation.class);
        SetOperation setOperation2 = (SetOperation) operation2;
        assertThat(setOperation2.getKey()).hasValue("test-key");
        assertThat(setOperation2.getValue()).hasValue("test-value");
    }

    @Test
    public void testReset() {
        Operation operation1 = parse("RESET");
        assertThat(operation1).isInstanceOf(ResetOperation.class);
        assertThat(((ResetOperation) operation1).getKey()).isNotPresent();

        Operation operation2 = parse("RESET 'test-key'");
        assertThat(operation2).isInstanceOf(ResetOperation.class);
        assertThat(((ResetOperation) operation2).getKey()).isPresent();
        assertThat(((ResetOperation) operation2).getKey()).hasValue("test-key");
    }

    @ParameterizedTest
    @ValueSource(strings = {"SET", "SET;", "SET ;", "SET\t;", "SET\n;"})
    public void testSetCommands(String command) {
        ExtendedParser extendedParser = new ExtendedParser();
        assertThat(extendedParser.parse(command)).get().isInstanceOf(SetOperation.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"HELP", "HELP;", "HELP ;", "HELP\t;", "HELP\n;"})
    public void testHelpCommands(String command) {
        ExtendedParser extendedParser = new ExtendedParser();
        assertThat(extendedParser.parse(command)).get().isInstanceOf(HelpOperation.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"CLEAR", "CLEAR;", "CLEAR ;", "CLEAR\t;", "CLEAR\n;"})
    public void testClearCommands(String command) {
        ExtendedParser extendedParser = new ExtendedParser();
        assertThat(extendedParser.parse(command)).get().isInstanceOf(ClearOperation.class);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "QUIT;", "QUIT;", "QUIT ;", "QUIT\t;", "QUIT\n;", "EXIT;", "EXIT ;", "EXIT\t;",
                "EXIT\n;", "EXIT ; "
            })
    public void testQuitCommands(String command) {
        ExtendedParser extendedParser = new ExtendedParser();
        assertThat(extendedParser.parse(command)).get().isInstanceOf(QuitOperation.class);
    }

    private void assertShowFunctions(
            String sql,
            String expectedSummary,
            ShowFunctionsOperation.FunctionScope expectedScope) {
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowFunctionsOperation.class);

        final ShowFunctionsOperation showFunctionsOperation = (ShowFunctionsOperation) operation;

        assertThat(showFunctionsOperation.getFunctionScope()).isEqualTo(expectedScope);
        assertThat(showFunctionsOperation.asSummaryString()).isEqualTo(expectedSummary);
    }

    @ParameterizedTest
    @CsvSource({
        "execute plan 'oss:///loalhost:9000/path/to.json', oss:/loalhost:9000/path/to.json",
        "execute plan 'hdfs:///loalhost:9000/path/to.json', hdfs:/loalhost:9000/path/to.json",
        "execute plan 'file:///loalhost:9000/path/to.json', file:/loalhost:9000/path/to.json",
        "execute plan 's3:///loalhost:9000/path/to.json', s3:/loalhost:9000/path/to.json"
    })
    void testSqlExecutePlanPath(String sql, String expected) {
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation;
        operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(ExecutePlanOperation.class);
        assertThat((ExecutePlanOperation) operation)
                .satisfies(
                        execute ->
                                assertThat(execute.getFilePath().toString()).isEqualTo(expected));
    }
}
