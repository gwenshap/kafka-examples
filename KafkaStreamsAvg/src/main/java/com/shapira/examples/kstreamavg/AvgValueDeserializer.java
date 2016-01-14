package com.shapira.examples.kstreamavg;

import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by gwen on 1/12/16.
 */
public class AvgValueDeserializer implements Deserializer<AvgValue> {
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public AvgValue deserialize(String topic, byte[] data) {
        if (data == null)
            return null;
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int count = buffer.getInt();
        int sum =buffer.getInt();
        return new AvgValue(count, sum);
    }

    public void close() {

    }
}
