package com.shapira.examples.kstreamavg;

/**
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
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.streams.KafkaStreaming;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.examples.WallclockTimestampExtractor;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SlidingWindow;
import org.apache.log4j.Logger;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class StreamingAvg {
    static Logger log = Logger.getLogger(StreamingAvg.class.getName());

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();

        props.put(StreamingConfig.JOB_ID_CONFIG, "moving-avg-example");
        props.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        StreamingConfig config = new StreamingConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, Integer> stream = builder.stream("ks_number_source");


        KTable<Windowed<String>, AvgValue> tempTable = stream.<AvgValue, SlidingWindow>aggregateByKey(
                () -> new AvgAggregator<String, Integer, AvgValue>(),
                SlidingWindows.of("avgWindow").with(10),
                new StringSerializer(), new AvgValueSerializer(),
                new StringDeserializer(), new AvgValueDeserializer());

        // Should work after we implement "aggregateByKey
        KTable<Windowed<String>, Double> results = tempTable.<Double>mapValues((v) -> ((double) v.sum / v.count));

        results.to("ks_avg");

        KafkaStreaming kstream = new KafkaStreaming(builder, config);
        kstream.start();
    }
}
