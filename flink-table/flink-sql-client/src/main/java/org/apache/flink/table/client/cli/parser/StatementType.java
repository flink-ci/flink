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

package org.apache.flink.table.client.cli.parser;

/** Enumerates the possible types of input statements. */
public enum StatementType {
    /** command to quit the client. */
    QUIT,
    /** command to clear the terminal's screen. */
    CLEAR,
    /** command to print help message. */
    HELP,
    /** 'EXPLAIN' SQL statement. */
    EXPLAIN,
    /** 'SHOW CREATE TABLE/VIEW' SQL statement. */
    SHOW_CREATE,
    /** 'BEGIN STATEMENT SET;' SQL statement. */
    BEGIN_STATEMENT_SET,
    /** 'END;' SQL statement. */
    END,
    /** type not covered by any other type value. */
    OTHER
}
