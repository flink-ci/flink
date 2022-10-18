/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.sink2.sync;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.sink2.statement.JdbcQueryStatement;
import org.apache.flink.connector.jdbc.sink2.statement.SimpleJdbcWriterStatement;
import org.apache.flink.connector.jdbc.sink2.sync.writer.JdbcSinkWriter;

import java.io.IOException;

/** A JdbcSink implementation for {@link Sink}. */
public class JdbcSink<OUT> implements Sink<OUT> {

    private final JdbcConnectionProvider connectionProvider;
    private final JdbcQueryStatement<OUT> queryStatement;
    private final JdbcExecutionOptions executeOptions;

    public JdbcSink(
            JdbcConnectionProvider connectionProvider,
            JdbcExecutionOptions executeOptions,
            JdbcQueryStatement<OUT> queryStatement) {
        this.queryStatement = queryStatement;
        this.executeOptions = executeOptions;
        this.connectionProvider = connectionProvider;
    }


    @Override
    public SinkWriter<OUT> createWriter(InitContext context) throws IOException {
        SimpleJdbcWriterStatement<OUT> statement =
                new SimpleJdbcWriterStatement<>(connectionProvider, queryStatement);
        return new JdbcSinkWriter<>(statement, executeOptions);
    }
}
