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

package org.apache.flink.api.connector.sink;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.List;

public interface Committing<InputT, CommT, WriterT extends Committing.Writer<InputT, CommT>>
        extends Sink<InputT, WriterT> {
    /**
     * Creates a {@link Committer}.
     *
     * @return A committer.
     * @throws IOException if fail to create a committer.
     */
    Committer<CommT> createCommitter(Sink.InitContext context) throws IOException;

    /** Returns the serializer of the committable type. */
    SimpleVersionedSerializer<CommT> getCommittableSerializer();

    /**
     * The {@code SinkWriter} is responsible for writing data and handling any potential tmp area
     * used to write yet un-staged data, e.g. in-progress files. The data (or metadata pointing to
     * where the actual data is staged) ready to commit is returned to the system by the {@link
     * #prepareCommit(boolean)}.
     *
     * @param <InputT> The type of the sink writer's input
     * @param <CommT> The type of information needed to commit data staged by the sink
     */
    @Experimental
    interface Writer<InputT, CommT> extends SinkWriter<InputT> {

        /**
         * Prepare for a commit.
         *
         * <p>This will be called before we checkpoint the Writer's state in Streaming execution
         * mode.
         *
         * @param flush Whether flushing the un-staged data or not
         * @return The data is ready to commit.
         * @throws IOException if fail to prepare for a commit.
         */
        List<CommT> prepareCommit(boolean flush) throws IOException, InterruptedException;
    }
}
