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

package org.apache.flink.connector.jdbc.sink2.sync.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.sink2.statement.JdbcWriterStatement;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A JdbcSinkWriter implementation for {@link SinkWriter}. */
public class JdbcSinkWriter<OUT> implements SinkWriter<OUT> {

    private final JdbcWriterStatement<OUT> statement;
    private final JdbcExecutionOptions executionOptions;

    private final Deque<OUT> bufferedRequests = new ArrayDeque<>();

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> schedulerFuture;
    private transient volatile Exception schedulerException;

    public JdbcSinkWriter(
            JdbcWriterStatement<OUT> statement,
            JdbcExecutionOptions executionOptions) {
        this.executionOptions = checkNotNull(executionOptions);
        this.statement = checkNotNull(statement);
    }

    private void open() {
        this.statement.prepare();
        this.createScheduler();
    }

    @Override
    public synchronized void write(OUT element, Context context) throws IOException, InterruptedException {
        checkSchedulerException();

        try {
            bufferedRequests.add(element);

            if (executionOptions.getBatchSize() > 0
                    && executionOptions.getBatchSize() >= bufferedRequests.size()) {
                flush(false);
            }
        } catch (Exception e) {
            throw new IOException("Writing records to JDBC failed.", e);
        }
    }

    @Override
    public synchronized void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkSchedulerException();
        if (bufferedRequests.isEmpty()) return;

        for (int i = 0; i <= executionOptions.getMaxRetries(); i++) {
            try {
                statement.process(new ArrayList<>(bufferedRequests));
                bufferedRequests.clear();
            } catch (Exception e) {
                if (i >= executionOptions.getMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    if (this.statement.isNotValid()) {
                        this.statement.prepare();
                    }
                } catch (Exception exception) {
                    throw new IOException("Reestablish JDBC connection failed", exception);
                }
                try {
                    Thread.sleep(1000L * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }

    @Override
    public synchronized void close() {
        if (this.schedulerFuture != null) {
            this.schedulerFuture.cancel(false);
            this.scheduler.shutdown();
        }

        if (this.bufferedRequests.size() > 0) {
            try {
                flush(true);
            } catch (Exception e) {
                throw new RuntimeException("Writing records to JDBC failed.", e);
            }
        }

        if (this.statement != null) {
            statement.close();
        }

        checkSchedulerException();
    }

    private void createScheduler() {
        if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            this.scheduler =
                    Executors.newScheduledThreadPool(
                            1, new ExecutorThreadFactory("jdbc-upsert-sink"));
            this.schedulerFuture =
                    this.scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (this) {
                                    try {
                                        flush(false);
                                    } catch (Exception e) {
                                        this.schedulerException = e;
                                    }

                                }
                            },
                            executionOptions.getBatchIntervalMs(),
                            executionOptions.getBatchIntervalMs(),
                            TimeUnit.MILLISECONDS);
        }
    }

    private void checkSchedulerException() {
        if (schedulerException != null) {
            throw new RuntimeException("Writing records to JDBC failed.", schedulerException);
        }
    }
}
