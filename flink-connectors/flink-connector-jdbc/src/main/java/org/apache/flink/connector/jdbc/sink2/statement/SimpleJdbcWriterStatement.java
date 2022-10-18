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

package org.apache.flink.connector.jdbc.sink2.statement;

import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.sink2.JdbcException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/** A simple implementation for {@link JdbcWriterStatement}. */
public class SimpleJdbcWriterStatement<IN> implements JdbcWriterStatement<IN> {

    private final JdbcConnectionProvider connectionProvider;
    private final JdbcQueryStatement<IN> queryStatement;
    private transient PreparedStatement preparedStatement;

    public SimpleJdbcWriterStatement(
            JdbcConnectionProvider connectionProvider, JdbcQueryStatement<IN> queryStatement) {
        this.connectionProvider = connectionProvider;
        this.queryStatement = queryStatement;
    }

    @Override
    public void prepare() throws JdbcException {
        try {
            Connection connection = connectionProvider.getOrEstablishConnection();
            if (connection == null) {
                throw new JdbcException("cant establish connection");
            }
            preparedStatement = connection.prepareStatement(queryStatement.query());
        } catch (SQLException | ClassNotFoundException ex) {
            throw new JdbcException(ex.getMessage(), ex);
        }
    }

    @Override
    public boolean isValid() throws JdbcException {
        try {
            return connectionProvider.isConnectionValid();
        } catch (SQLException ex) {
            throw new JdbcException(ex.getMessage(), ex);
        }
    }

    @Override
    public void process(List<IN> entries) throws JdbcException {
        if (entries.isEmpty()) {
            return;
        }
        try {
            for (IN elem : entries) {
                queryStatement.map(preparedStatement, elem);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        } catch (SQLException ex) {
            throw new JdbcException(ex.getMessage(), ex);
        }
    }

    @Override
    public void close() throws JdbcException {
        try {
            preparedStatement.close();
            preparedStatement = null;
            connectionProvider.closeConnection();
        } catch (SQLException ex) {
            throw new JdbcException(ex.getMessage(), ex);
        }
    }
}
