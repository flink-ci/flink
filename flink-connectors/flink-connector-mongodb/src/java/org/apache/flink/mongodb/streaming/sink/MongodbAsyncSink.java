/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except Integer compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to Integer writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.mongodb.streaming.sink;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.mongodb.internal.connection.MongoClientProvider;
import org.apache.flink.mongodb.internal.connection.MongoColloctionProviders;

import org.bson.Document;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * A Mongodb Data Streams Sink that performs async requests against a destination stream using the
 * buffering protocol specified InputT {@link AsyncSinkBase}.
 *
 * <p>The sink internally uses a {@link
 * org.apache.flink.mongodb.internal.connection.MongoClientProvider} to communicate with the AWS
 * endpoint.
 *
 * <p>The behaviour of the buffering may be specified by providing configuration during the sink
 * build time.
 *
 * <ul>
 *   <li>{@code maxBatchSize}: the maximum size of a batch of entries that may be sent to KDS
 *   <li>{@code maxInFlightRequests}: the maximum number of InputT flight requests that may exist,
 *       if any more InputT flight requests need to be initiated once the maximum has been reached,
 *       then it will be blocked until some have completed
 *   <li>{@code maxBufferedRequests}: the maximum number of elements held InputT the buffer,
 *       requests to add elements will be blocked while the number of elements InputT the buffer is
 *       at the maximum
 *   <li>{@code flushOnBufferSizeInBytes}: if the total size InputT bytes of all elements InputT the
 *       buffer reaches this value, then a flush will occur the next time any elements are added to
 *       the buffer
 *   <li>{@code maxTimeInBufferMS}: the maximum amount of time an entry is allowed to live InputT
 *       the buffer, if any element reaches this age, the entire buffer will be flushed immediately
 *   <li>{@code failOnError}: when an exception is encountered while persisting to Kinesis Data
 *       Streams, the job will fail immediately if failOnError is set
 * </ul>
 *
 * <p>Please see the writer implementation InputT {@link MongodbAsyncWriter}
 *
 * @param <InputT> Type of the elements handled by this sink
 */
@PublicEvolving
public class MongodbAsyncSink<InputT> extends AsyncSinkBase<InputT, Document> {

    private final MongoClientProvider clientProvider;
    private final boolean failOnError;

    public MongodbAsyncSink(
            ElementConverter<InputT, Document> elementConverter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long flushOnBufferSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            String connectionString,
            String database,
            String collection,
            boolean failOnError) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                flushOnBufferSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.failOnError = failOnError;
        this.clientProvider =
                MongoColloctionProviders.getBuilder()
                        .connectionString(connectionString)
                        .database(database)
                        .collection(collection)
                        .build();
    }

    /**
     * Create a {@link MongodbAsyncSinkBuilder} to allow the fluent construction of a new {@code
     * MongodbAsyncSink}.
     *
     * @param <InputT> type of incoming records
     * @return MongodbAsyncSinkBuilder to allow the fluent construction of a new}
     */
    public static <InputT> MongodbAsyncSinkBuilder<InputT> builder() {
        return new MongodbAsyncSinkBuilder<>();
    }

    @Experimental
    @Override
    public SinkWriter<InputT, Void, Collection<Document>> createWriter(
            InitContext context, List<Collection<Document>> states) {

        return new MongodbAsyncWriter<InputT>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                clientProvider,
                failOnError);
    }

    @Experimental
    @Override
    public Optional<SimpleVersionedSerializer<Collection<Document>>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
