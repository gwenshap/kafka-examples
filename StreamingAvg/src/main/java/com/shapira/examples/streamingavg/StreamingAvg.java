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

package com.shapira.examples.streamingavg;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkConf;
import org.apache.spark.Logging;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;


public class StreamingAvg {
    static Logger log = Logger.getLogger(StreamingAvg.class.getName());

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: StreamingAvg <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }

        //Configure the Streaming Context
        SparkConf sparkConf = new SparkConf().setAppName("StreamingAvg");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(10000));

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(ssc, args[0], args[1], topicMap);


        System.out.println("Got my DStream! connecting to zookeeper "+ args[0] + " group " + args[1] + " topics" +
        topicMap);



        JavaPairDStream<Integer,Integer> nums = messages.mapToPair(new PairFunction<Tuple2<String,String>, Integer, Integer>()
        {
            @Override
            public Tuple2<Integer,Integer> call(Tuple2<String, String> tuple2) {
                return new Tuple2<Integer,Integer>(1,Integer.parseInt(tuple2._2()));
            }
        });

        JavaDStream<Tuple2<Integer,Integer>> countAndSum = nums.reduce(new Function2<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                return new Tuple2<Integer, Integer>(a._1() + b._1(), a._2() + b._2());
            }
        });

        countAndSum.foreachRDD(new Function<JavaRDD<Tuple2<Integer, Integer>>, Void>() {
            @Override
            public Void call(JavaRDD<Tuple2<Integer, Integer>> tuple2JavaRDD) throws Exception {
                if (tuple2JavaRDD.count() > 0) {
                    System.out.println("Current avg: " + tuple2JavaRDD.first()._2() / tuple2JavaRDD.first()._1());
                } else {
                    System.out.println("Got no data in this window");
                }
                return null;
            }
        });

        ssc.start();
        ssc.awaitTermination();

    }

    void setStreamingLogLevels() {
        Boolean log4jInitialized = Logger.getRootLogger().getAllAppenders().hasMoreElements();
        if (!log4jInitialized) {
            // We first log something to initialize Spark's default logging, then we override the
            // logging level.
            log.info("Setting log level to [WARN] for streaming example." +
                    " To override add a custom log4j.properties to the classpath.");
            Logger.getRootLogger().setLevel(Level.WARN);
        }
    }
}
