package com.umesh.streaming;

import com.umesh.receiver.CustomWMQReceiver;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;
import java.util.Properties;

/**
 * Created by uchaudh on 12/31/2015.
 */
public class MQStreaming implements Serializable {


    /**
     * Entry point to start receiving data from WebSphere MQ
     * @param args
     */
    public static void main(String[] args) {

        try {

            SparkConf sparkConf = new SparkConf().setAppName("TestMQStreaming").setMaster("local[2]");

            JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

            //Getting
            JavaDStream<String> customReceiverStream = ssc.receiverStream(new CustomWMQReceiver("host", 1414, "<qmanager>", "<channel>", "qname"));

            Properties props = new Properties();
            props.put("bootstrap.servers", "<kafka-broker:port>");
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("key.serializer", StringSerializer.class.getName());
            props.put("value.serializer", StringSerializer.class.getName());
            final Producer<String, String> producer = new KafkaProducer<String, String>(props);

            customReceiverStream.foreachRDD(new Function<JavaRDD<String>, Void>() {
                @Override
                public Void call(JavaRDD<String> stringJavaRDD) throws Exception {

                    String messageToSend = stringJavaRDD.collect().toString();

                    System.out.println(messageToSend);
                    if(!messageToSend.equals("[]")) {
                        ProducerRecord<String, String> record = new ProducerRecord<String, String>("streaming-data", messageToSend);
                        producer.send(record);
                    }
                    return null;
                }
            });

            ssc.start();
            ssc.awaitTermination();

        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }


    }

}