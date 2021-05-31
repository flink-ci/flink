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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.utils.TestUtil;
import org.apache.flink.types.Row;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.parquet.schema.MessageType;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Test cases for reading Parquet files and convert parquet records to Avro GenericRecords. */
@RunWith(Parameterized.class)
public class ParquetAvroInputFormatTest extends TestUtil {

    @ClassRule public static TemporaryFolder tempRoot = new TemporaryFolder();

    public ParquetAvroInputFormatTest(boolean useLegacyMode) {
        // AvroRowSerializationSchema does not work with parquet.avro.write-old-list-structure set
        // to false
        super(true);
    }

    @Test
    public void testReadFromSimpleRecord() throws IOException {
        Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData =
                getSimpleRecordTestData();
        MessageType messageType = getSchemaConverter().convert(SIMPLE_SCHEMA);
        Path path =
                createTempParquetFile(
                        tempRoot.getRoot(),
                        SIMPLE_SCHEMA,
                        Collections.singletonList(testData.f1),
                        getConfiguration());

        ParquetAvroInputFormat inputFormat = new ParquetAvroInputFormat(path, messageType);
        inputFormat.setRuntimeContext(TestUtil.getMockRuntimeContext());

        FileInputSplit[] splits = inputFormat.createInputSplits(1);
        assertEquals(1, splits.length);
        inputFormat.open(splits[0]);

        final GenericRecord genericRecord = inputFormat.nextRecord(null);
        assertEquals(testData.f2.getField(0), genericRecord.get("foo"));
        assertEquals(testData.f2.getField(1), genericRecord.get("bar").toString());
        assertArrayEquals(
                (Long[]) testData.f2.getField(2),
                ((List<Long>) genericRecord.get("arr")).toArray());
    }

    @Test(expected = AvroRuntimeException.class)
    public void testProjectedReadFromSimpleRecord()
            throws IOException, NoSuchFieldError, AvroRuntimeException {
        Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData =
                TestUtil.getSimpleRecordTestData();
        MessageType messageType = getSchemaConverter().convert(SIMPLE_SCHEMA);
        Path path =
                createTempParquetFile(
                        tempRoot.getRoot(),
                        SIMPLE_SCHEMA,
                        Collections.singletonList(testData.f1),
                        getConfiguration());

        ParquetAvroInputFormat inputFormat = new ParquetAvroInputFormat(path, messageType);
        inputFormat.setRuntimeContext(getMockRuntimeContext());

        FileInputSplit[] splits = inputFormat.createInputSplits(1);
        assertEquals(1, splits.length);

        inputFormat.selectFields(new String[] {"foo", "bar"});
        inputFormat.open(splits[0]);

        final GenericRecord genericRecord = inputFormat.nextRecord(null);
        assertEquals(testData.f2.getField(0), genericRecord.get("foo"));
        assertEquals(testData.f2.getField(1), genericRecord.get("bar").toString());
        // should throw AvroRuntimeException("Not a valid schema field: arr")
        genericRecord.get("arr");
    }
}
