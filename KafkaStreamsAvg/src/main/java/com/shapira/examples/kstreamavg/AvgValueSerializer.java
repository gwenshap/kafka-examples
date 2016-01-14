package com.shapira.examples.kstreamavg;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class AvgValueSerializer implements Serializer<AvgValue> {

    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    public byte[] serialize(String topic, AvgValue data) {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4);
        buffer.putInt(data.count);
        buffer.putInt(data.sum);
        return buffer.array();
    }

    public void close() {
    }
}
