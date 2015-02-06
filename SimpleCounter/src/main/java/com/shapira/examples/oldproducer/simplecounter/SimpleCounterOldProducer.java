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

package com.shapira.examples.oldproducer.simplecounter;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class SimpleCounterOldProducer {

    private Properties kafkaProps = new Properties();
    private Producer<String, String> producer;
    private ProducerConfig config;
    private String topic;


    public static void main(String[] args) throws InterruptedException {

        if (args.length == 0) {
            System.out.println("SimpleCounterOldProducer {broker-list} {topic} {sync} {delay (ms)} {count}");
            return;
        }

        SimpleCounterOldProducer counter = new SimpleCounterOldProducer();

        /* get arguments */
        String brokerList = args[0];
        counter.topic = args[1];
        String sync = args[2];
        int delay = Integer.parseInt(args[3]);
        int count = Integer.parseInt(args[4]);

        /* start a producer */
        counter.configure(brokerList, sync);
        counter.start();

        long startTime = System.currentTimeMillis();
        System.out.println("Starting...");
        counter.produce("Starting...");

        /* produce the numbers */
        for (int i=0; i < count; i++ ) {
            counter.produce(Integer.toString(i));
            Thread.sleep(delay);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("... and we are done. This took " + (endTime - startTime) + " ms.");
        counter.produce("... and we are done. This took " + (endTime - startTime) + " ms.");

        /* close shop and leave */
        counter.producer.close();
        System.exit(0);
    }

    /* create configuration for the producer
    *  consult Kafka documentation for exact meaning of each configuration parameter */
    private void configure(String brokerList, String sync) {

        kafkaProps.put("metadata.broker.list", brokerList);
        kafkaProps.put("serializer.class", "kafka.serializer.StringEncoder");
        kafkaProps.put("request.required.acks", "1");
        kafkaProps.put("producer.type", sync);

        config = new ProducerConfig(kafkaProps);
    }

    /* start the producer */
    private void start() {
        producer = new Producer<String, String>(config);
    }

    /* create record and send to Kafka
    *  because the key is null, data will be sent to a random partition.
    *  the producer will switch to a different random partition every 10 minutes
    **/
    private void produce(String s) {
        KeyedMessage<String, String> message = new KeyedMessage<String, String>(topic, null, s);
        producer.send(message);
    }
}
