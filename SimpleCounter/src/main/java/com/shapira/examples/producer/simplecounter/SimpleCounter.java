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

package com.shapira.examples.producer.simplecounter;

import java.util.concurrent.ExecutionException;

public class SimpleCounter {

    private static DemoProducer producer;


    public static void main(String[] args) throws InterruptedException, ExecutionException {

        if (args.length == 0) {
            System.out.println("SimpleCounter {broker-list} {topic} {type old/new} {type sync/async} {delay (ms)} {count}");
            return;
        }

        /* get arguments */
        String brokerList = args[0];
        String topic = args[1];
        String age = args[2];
        String sync = args[3];
        int delay = Integer.parseInt(args[4]);
        int count = Integer.parseInt(args[5]);

        if (age.equals("old"))
            producer = new DemoProducerOld(topic);
        else if (age.equals("new"))
            producer = new DemoProducerNewJava(topic);
        else {
            System.out.println("Third argument should be old or new, got " + age);
            System.exit(-1);
        }

        /* start a producer */
        producer.configure(brokerList, sync);
        producer.start();

        long startTime = System.currentTimeMillis();
        System.out.println("Starting...");
        producer.produce("Starting...");

        /* produce the numbers */
        for (int i=0; i < count; i++ ) {
            producer.produce(Integer.toString(i));
            Thread.sleep(delay);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("... and we are done. This took " + (endTime - startTime) + " ms.");
        producer.produce("... and we are done. This took " + (endTime - startTime) + " ms.");

        /* close shop and leave */
        producer.close();
        System.exit(0);
    }

}
