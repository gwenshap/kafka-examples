package com.shapira.examples.kstreamavg;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.IntegerSerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.log4j.Logger;

public class StreamingAvg {
    static Logger log = Logger.getLogger(StreamingAvg.class.getName());

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "moving-avg-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, IntegerSerde.class);
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        StreamsConfig config = new StreamsConfig(props);
        
        final Serde<String> stringSerde = Serdes.String();
        final Serde<AvgValue> avgValueSerde = Serdes.serdeFrom(new AvgValueSerializer(), new AvgValueDeserializer());        

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, Integer> prices = builder.stream("ks_prices");

        KTable<String, String> names = builder.table("ks_names");

        KStream<String, Integer> namedPrices = prices.leftJoin(names, (price, name) -> {
            return new NamedPrice(name, price);
        }).map((ticket, namedPrice) -> new org.apache.kafka.streams.KeyValue<String, Integer>(namedPrice.name, namedPrice.price));



        KTable<Windowed<String>, AvgValue> tempTable = namedPrices.<AvgValue, TimeWindow>aggregateByKey(
        		AvgAggregator.initializer(),
        		AvgAggregator.aggregator(),
                TimeWindows.of("avgWindow",10000),
                stringSerde, avgValueSerde);

        // Should work after we implement "aggregateByKey
        KTable<Windowed<String>, Double> avg = tempTable.<Double>mapValues((v) -> ((double) v.sum / v.count));

        avg.to("ks_avg_prices");


        KafkaStreams kstream = new KafkaStreams(builder, config);
        kstream.start();
    }
}


//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.kafka.common.serialization.IntegerDeserializer;
//import org.apache.kafka.common.serialization.IntegerSerializer;
//import org.apache.kafka.streams.KafkaStreaming;
//import org.apache.kafka.streams.StreamingConfig;
//import org.apache.kafka.streams.examples.WallclockTimestampExtractor;
//import org.apache.kafka.streams.kstream.KStream;
//import org.apache.kafka.streams.kstream.KStreamBuilder;
//import org.apache.kafka.streams.kstream.KTable;
//import org.apache.kafka.streams.kstream.KeyValue;
//import org.apache.kafka.streams.kstream.TumblingWindows;
//import org.apache.kafka.streams.kstream.Windowed;
//import org.apache.kafka.streams.kstream.internals.TumblingWindow;
//import org.apache.log4j.Logger;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.kafka.common.serialization.StringSerializer;
//
//import java.util.Properties;
//
//public class StreamingAvg {
//    static Logger log = Logger.getLogger(StreamingAvg.class.getName());
//
//    public static void main(String[] args) throws Exception {
//        Properties props = new Properties();
//
//        props.put(StreamingConfig.JOB_ID_CONFIG, "moving-avg-example");
//        props.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
//        props.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        props.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
//        props.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        props.put(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
//        StreamingConfig config = new StreamingConfig(props);
//
//        KStreamBuilder builder = new KStreamBuilder();
//
//        KStream<String, Integer> prices = builder.stream("ks_prices");
//
//        KTable<String, String> names = builder.table("ks_names");
//
//        KStream<String, Integer> namedPrices = prices.leftJoin(names, (price, name) -> {
//            return new NamedPrice(name, price);
//        }).map((ticket, namedPrice) -> new KeyValue<String, Integer>(namedPrice.name, namedPrice.price));
//
//
//
//        KTable<Windowed<String>, AvgValue> tempTable = namedPrices.<AvgValue, TumblingWindow>aggregateByKey(
//                () -> new AvgAggregator<String, Integer, AvgValue>(),
//                TumblingWindows.of("avgWindow").with(10000),
//                new StringSerializer(), new AvgValueSerializer(),
//                new StringDeserializer(), new AvgValueDeserializer());
//
//        // Should work after we implement "aggregateByKey
//        KTable<Windowed<String>, Double> avg = tempTable.<Double>mapValues((v) -> ((double) v.sum / v.count));
//
//        avg.to("ks_avg_prices");
//
//
//        KafkaStreaming kstream = new KafkaStreaming(builder, config);
//        kstream.start();
//    }
//}
//
//
