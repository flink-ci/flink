package org.apache.flink.connector.jdbc.sink2.async;

import org.apache.commons.lang3.SerializationUtils;

import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

public class JdbcSerializer<OUT extends Serializable> extends AsyncSinkWriterStateSerializer<OUT> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    protected void serializeRequestToStream(OUT request, DataOutputStream out) throws IOException {
        byte[] bytes = SerializationUtils.serialize(request);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    protected OUT deserializeRequestFromStream(
            long requestSize,
            DataInputStream in) throws IOException {

        int size = in.readInt();
        byte[] requestData = new byte[size];
        in.read(requestData);
        return SerializationUtils.deserialize(requestData);
    }

}
