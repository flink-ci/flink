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

package org.apache.flink.connector.jdbc.sink2.async;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.sink2.statement.JdbcQueryStatement;
import org.apache.flink.connector.jdbc.sink2.statement.SimpleJdbcWriterStatement;
import org.apache.flink.connector.jdbc.sink2.async.writer.JdbcSinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

/** A JdbcSink implementation for {@link Sink}. */
public class JdbcSink<OUT extends Serializable> extends AsyncSinkBase<OUT, OUT> {

    private final JdbcConnectionProvider connectionProvider;
    private final JdbcQueryStatement<OUT> queryStatement;
    private final AsyncSinkWriterConfiguration writerOptions;

    public JdbcSink(
            JdbcConnectionProvider connectionProvider,
            AsyncSinkWriterConfiguration writerOptions,
            JdbcQueryStatement<OUT> queryStatement) {
        super(
                (element, context) -> element,
                writerOptions.getMaxBatchSize(),
                writerOptions.getMaxInFlightRequests(),
                writerOptions.getMaxBufferedRequests(),
                writerOptions.getMaxBatchSizeInBytes(),
                writerOptions.getMaxTimeInBufferMS(),
                writerOptions.getMaxRecordSizeInBytes());
        this.queryStatement = queryStatement;
        this.writerOptions = writerOptions;
        this.connectionProvider = connectionProvider;
    }

    @Override
    public StatefulSinkWriter<OUT, BufferedRequestState<OUT>> createWriter(InitContext context)
            throws IOException {
        return restoreWriter(context, Collections.emptyList());
    }

    @Override
    public StatefulSinkWriter<OUT, BufferedRequestState<OUT>> restoreWriter(
            InitContext context, Collection<BufferedRequestState<OUT>> recoveredState)
            throws IOException {
        SimpleJdbcWriterStatement<OUT> statement =
                new SimpleJdbcWriterStatement<>(connectionProvider, queryStatement);
        statement.prepare();
        return new JdbcSinkWriter<>(
                statement, getElementConverter(), context, writerOptions, recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<OUT>> getWriterStateSerializer() {
        return new JdbcSerializer<>();
    }
}
