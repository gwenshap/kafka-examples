package com.shapira.examples.newconsumer.simplemovingavg;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.kafka.clients.consumer.*;

import java.util.Collections;
import java.util.Properties;

public class SimpleMovingAvgNewConsumer {

    private Properties kafkaProps = new Properties();
    private String waitTime;
    private KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("SimpleMovingAvgZkConsumer {brokers} {group.id} {topic} {window-size}");
            return;
        }

        SimpleMovingAvgNewConsumer movingAvg = new SimpleMovingAvgNewConsumer();
        String brokers = args[0];
        String groupId = args[1];
        String topic = args[2];
        int window = Integer.parseInt(args[3]);

        CircularFifoBuffer buffer = new CircularFifoBuffer(window);
        movingAvg.configure(brokers, groupId);

       movingAvg.consumer.subscribe(Collections.singletonList(topic));

        // looping until ctrl-c, should add logic to close consumer nicely
        while (true) {
            ConsumerRecords<String, String> records = movingAvg.consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
            {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());

                int sum = 0;

                try {
                    int num = Integer.parseInt(record.value());
                    buffer.add(num);
                } catch (NumberFormatException e) {
                    // just ignore strings
                }

                for (Object o: buffer) {
                    sum += (Integer) o;
                }

                if (buffer.size() > 0) {
                    System.out.println("Moving avg is: " + (sum / buffer.size()));
                }
            }
            // If you are going for at least once, commit after you finished processing everything you got in the last poll
            movingAvg.consumer.commit(CommitType.ASYNC);
        }
    }

    private void configure(String servers, String groupId) {
        kafkaProps.put("group.id",groupId);
        kafkaProps.put("bootstrap.servers",servers);
        // when in doubt, read everything
        kafkaProps.put("auto.offset.reset","earliest");
        kafkaProps.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(kafkaProps);
    }

}
