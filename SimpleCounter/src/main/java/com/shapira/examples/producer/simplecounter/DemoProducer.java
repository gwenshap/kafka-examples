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

public interface DemoProducer {

    /**
     * create configuration for the producer
     * consult Kafka documentation for exact meaning of each configuration parameter
     */
    void configure(String brokerList, String sync);

    /* start the producer */
    void start();

    /**
     * create record and send to Kafka
     * because the key is null, data will be sent to a random partition.
     * exact behavior will be different depending on producer implementation
     */
    void produce(String s) throws ExecutionException, InterruptedException;

    void close();
}
