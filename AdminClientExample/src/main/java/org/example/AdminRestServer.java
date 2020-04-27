package org.example;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collections;
import java.util.Properties;


// Test with:  curl 'localhost:8080?topic=demo-topic'
// To check that it is indeed async, suspend Kafka with SIGSTOP and then run:
// curl 'localhost:8080?topic=demo-topic&timeout=60000' on one terminal and
// curl 'localhost:8080?topic=demo-topic' on another.
// Even though I only have one Vertx thread, the first command will wait 60s and the second will return immediately
// Demonstrating that the second command did not block behind the first
public class AdminRestServer {
    public static Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(1));

    public static void main(String[] args) {
        // initialize admin client
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 1000);
        AdminClient admin = AdminClient.create(props);

        // create web server
        HttpServerOptions options = new HttpServerOptions().setLogActivity(true);
        vertx.createHttpServer().requestHandler(request -> {
            String topic = request.getParam("topic");
            String timeout = request.getParam("timeout");
            int timeoutMs = NumberUtils.toInt(timeout, 1000);

            DescribeTopicsResult demoTopic = admin.describeTopics(
                    Collections.singletonList(topic),
                    new DescribeTopicsOptions().timeoutMs(timeoutMs));
            demoTopic.values().get(topic).whenComplete(
                    new KafkaFuture.BiConsumer<TopicDescription, Throwable>() {
                        @Override
                        public void accept(final TopicDescription topicDescription,
                                           final Throwable throwable) {
                            if (throwable != null) {
                                System.out.println("got exception");
                                request.response().end("Error trying to describe topic "
                                        + topic + " due to " + throwable.getMessage());
                            } else {
                                request.response().end(topicDescription.toString());
                            }
                        }
                    });
        }).listen(8080);
    }
}
