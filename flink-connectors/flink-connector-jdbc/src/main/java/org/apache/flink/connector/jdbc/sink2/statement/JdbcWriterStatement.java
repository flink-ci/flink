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

import org.apache.flink.connector.jdbc.sink2.JdbcException;

import java.io.Serializable;
import java.util.List;

/**
 * Allow to generate the process of writing to Jdbc based on a specific type of record.
 */
public interface JdbcWriterStatement<T> extends Serializable, AutoCloseable {

    void prepare() throws JdbcException;
    boolean isValid() throws JdbcException;

    default boolean isNotValid() throws JdbcException {
        return !isValid();
    }

    void process(List<T> entries) throws JdbcException;

    void close() throws JdbcException;
}
