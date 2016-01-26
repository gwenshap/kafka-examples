package com.shapira.examples.consumer.avroclicks;

import JavaSessionize.avro.LogLine;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AvroClicksSessionizer {
    private final KafkaConsumer<String, LogLine> consumer;
    private final KafkaProducer<String, LogLine> producer;
    private final String inputTopic;
    private final String outputTopic;
    private Map<String, SessionState> state = new HashMap<String, SessionState>();


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // currently hardcoding a lot of parameters, for simplicity
        String groupId = "AvroClicksSessionizer";
        String inputTopic = "clicks";
        String outputTopic = "sessionized_clicks";
        String url = "http://localhost:8081";
        String brokers = "localhost:9092";

        AvroClicksSessionizer sessionizer = new AvroClicksSessionizer(brokers, groupId, inputTopic, outputTopic, url);
        sessionizer.run();



    }

    public AvroClicksSessionizer(String brokers, String groupId, String inputTopic, String outputTopic, String url) {

        this.consumer = new KafkaConsumer(createConsumerConfig(brokers, groupId, url));
        this.producer = getProducer(outputTopic, url);
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;

    }

    private Properties createConsumerConfig(String brokers, String groupId, String url) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("auto.commit.enable", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("schema.registry.url", url);
        props.put("specific.avro.reader", true);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");

        return props;
    }

    private void run() throws ExecutionException, InterruptedException {
        consumer.subscribe(Collections.singletonList(inputTopic));

        System.out.println("Reading topic:" + inputTopic);

        while (true) {
            ConsumerRecords<String, LogLine> records = consumer.poll(1000);


            for (ConsumerRecord<String, LogLine> record: records) {
                String ip = record.key();
                LogLine event = record.value();


                SessionState oldState = state.get(ip);
                int sessionId = 0;
                if (oldState == null) {
                    state.put(ip, new SessionState(event.getTimestamp(), 0));
                } else {
                    sessionId = oldState.getSessionId();
                    // if the old timestamp is more than 30 minutes older than new one, we have a new session
                    if (oldState.getLastConnection() < event.getTimestamp() - (30 * 60 * 1000))
                        sessionId = sessionId + 1;
                    SessionState newState = new SessionState(event.getTimestamp(), sessionId);
                    state.put(ip, newState);
                }
                event.setSessionid(sessionId);
                System.out.println(event.toString());
                ProducerRecord<String, LogLine> sessionizedEvent = new ProducerRecord<String, LogLine>(outputTopic, event.getIp().toString(), event);
                producer.send(sessionizedEvent).get();
            }
            consumer.commitSync();
        }

    }

    private KafkaProducer<String, LogLine> getProducer(String topic, String url) {
        Properties props = new Properties();
        // hardcoding the Kafka server URI for this example
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", url);

        KafkaProducer<String, LogLine> producer = new KafkaProducer<String, LogLine>(props);
        return producer;
    }


}

