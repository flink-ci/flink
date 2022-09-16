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

package org.apache.flink.connector.jdbc.sink2.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.jdbc.sink2.statement.JdbcWriterStatement;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/** A JdbcSinkWriter implementation for {@link SinkWriter}. */
public class JdbcSinkWriter<OUT extends Serializable> extends AsyncSinkWriter<OUT, OUT> {

    private final JdbcWriterStatement<OUT> statement;

    public JdbcSinkWriter(
            JdbcWriterStatement<OUT> statement,
            ElementConverter<OUT, OUT> elementConverter,
            Sink.InitContext context,
            AsyncSinkWriterConfiguration configuration,
            Collection<BufferedRequestState<OUT>> bufferedRequestStates) {
        super(elementConverter, context, configuration, bufferedRequestStates);
        this.statement = statement;
    }

    @Override
    protected void submitRequestEntries(List<OUT> entries, Consumer<List<OUT>> toRetry) {
        statement.process(entries);
    }

    @Override
    protected long getSizeInBytes(OUT entry) {
        return 0;
    }

    @Override
    public void close() {
        statement.close();
    }
}
