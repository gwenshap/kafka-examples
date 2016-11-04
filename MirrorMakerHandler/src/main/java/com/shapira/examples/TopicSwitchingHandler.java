package com.shapira.examples;

import kafka.tools.MirrorMaker;
import kafka.consumer.BaseConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Collections;

public class TopicSwitchingHandler implements MirrorMaker.MirrorMakerMessageHandler {

    private final String topicPrefix;

    public TopicSwitchingHandler(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    public List<ProducerRecord<byte[], byte[]>> handle(BaseConsumerRecord record) {
        return Collections.singletonList(new ProducerRecord<byte[], byte[]>(topicPrefix + "." + record.topic(), record.partition(), record.key(), record.value()));
    }
}
