package org.apache.flink.connector.jdbc.sink2.async;

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JdbcSerializerTest {

    @Test
    public void testSerializer() throws IOException {
        TestClass tester = new TestClass("one", 1);
        BufferedRequestState<TestClass> toBeTested = new BufferedRequestState<>(
                Arrays.asList(
                        new RequestEntryWrapper<>(new TestClass("one", 1),-1),
                        new RequestEntryWrapper<>(new TestClass("two", 2),-1)
                ));

        JdbcSerializer<TestClass> serializer = new JdbcSerializer<>();

        byte[] serializedBytes = serializer.serialize(toBeTested);

        BufferedRequestState<TestClass> deserializerClass = serializer.deserialize(1, serializedBytes);

        assertEquals(getEntry(toBeTested, 1), getEntry(deserializerClass, 1));
        assertEquals(getEntry(toBeTested, 2), getEntry(deserializerClass, 2));

    }

    private TestClass getEntry(BufferedRequestState<TestClass> buffer, int position) {
        return buffer.getBufferedRequestEntries().get(position - 1).getRequestEntry();
    }

    private static class TestClass implements Serializable {
        public final String valueString;
        public final Integer valueInteger;

        public TestClass(String valueString, Integer valueInteger) {
            this.valueString = valueString;
            this.valueInteger = valueInteger;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestClass testClass = (TestClass) o;
            return new EqualsBuilder()
                    .append(valueString, testClass.valueString)
                    .append(valueInteger, testClass.valueInteger)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(valueString)
                    .append(valueInteger)
                    .toHashCode();
        }
    }
}
