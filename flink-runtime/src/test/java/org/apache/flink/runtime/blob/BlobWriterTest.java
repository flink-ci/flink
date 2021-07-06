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
 * limitations under the License
 */

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.types.Either;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link BlobWriter}. */
public class BlobWriterTest extends TestLogger {

    @Test
    public void testSerializeAndTryOffload() throws IOException, ClassNotFoundException {
        final TestingBlobWriter blobWriter = new TestingBlobWriter();
        final String value = "TESTCASE";
        final JobID jobId = new JobID();

        Either<SerializedValue<String>, PermanentBlobKey> blobKey =
                BlobWriter.serializeAndTryOffload(value, jobId, blobWriter);
        assertTrue(blobKey.isRight());
        assertEquals(
                value,
                InstantiationUtil.deserializeObject(
                        blobWriter.getBlob(jobId, blobKey.right()),
                        ClassLoader.getSystemClassLoader()));

        blobWriter.setMinOffloadingSize(Integer.MAX_VALUE);
        Either<SerializedValue<String>, PermanentBlobKey> serializedValue =
                BlobWriter.serializeAndTryOffload(value, jobId, blobWriter);
        assertTrue(serializedValue.isLeft());
        assertEquals(
                value, serializedValue.left().deserializeValue(ClassLoader.getSystemClassLoader()));
    }
}
