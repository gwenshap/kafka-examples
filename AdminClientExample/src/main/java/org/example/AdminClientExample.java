package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminClientExample {

    public static final String TOPIC_NAME = "demo-topic";
    public static final List<String> TOPIC_LIST = Collections.singletonList(TOPIC_NAME);
    public static final int NUM_PARTITIONS = 6;
    public static final short REP_FACTOR = 1;

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // initialize admin client
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient admin = AdminClient.create(props);
        TopicDescription topicDescription;

        // list topics
        ListTopicsResult topics = admin.listTopics();
        topics.names().get().forEach(System.out::println);

        // check if our demo topic exists, create it if it doesn't
        DescribeTopicsResult demoTopic = admin.describeTopics(TOPIC_LIST);
        try {
            topicDescription = demoTopic.values().get(TOPIC_NAME).get();
            System.out.println("Description of demo topic:" + topicDescription);

            if (topicDescription.partitions().size() != NUM_PARTITIONS) {
                System.out.println("Topic has wrong number of partitions. Exiting.");
                System.exit(-1);
            }
        } catch (ExecutionException e) {
            // exit early for almost all exceptions
            if (! (e.getCause() instanceof UnknownTopicOrPartitionException)) {
                e.printStackTrace();
                throw e;
            }

            // if we are here, topic doesn't exist
            System.out.println("Topic " + TOPIC_NAME + " does not exist. Going to create it now");
            // Note that number of partitions and replicas are optional. If there are
            // not specified, the defaults configured on the Kafka brokers will be used
            CreateTopicsResult newTopic = admin.createTopics(
                    Collections.singletonList(new NewTopic(TOPIC_NAME, NUM_PARTITIONS, REP_FACTOR)));

            // Check that the topic was created correctly:
            if (newTopic.numPartitions(TOPIC_NAME).get() != NUM_PARTITIONS) {
                System.out.println("Topic was created with wrong number of partitions. Exiting.");
                System.exit(-1);
            }
        }

        // Make the topic compacted
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC,TOPIC_NAME);
        DescribeConfigsResult configsResult = admin.describeConfigs(Collections.singleton(configResource));
        Config configs = configsResult.all().get().get(configResource);

        // print non-default configs
        configs.entries().stream().filter(entry -> !entry.isDefault()).forEach(System.out::println);


        // Check if topic is compacted
        ConfigEntry compaction = new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        if (! configs.entries().contains(compaction)) {
            // if topic is not compacted, compact it
            Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs =
                    Collections.singletonMap(
                            configResource,
                            Collections.singleton(new AlterConfigOp(compaction, AlterConfigOp.OpType.SET))
                    );
            admin.incrementalAlterConfigs(alterConfigs).all().get();
        } else {
            System.out.println("Topic " + TOPIC_NAME + " is compacted topic");
        }

        // finish things off by deleting our topic

        admin.deleteTopics(TOPIC_LIST).all().get();

        // Check that it is gone. Note that due to the async nature of deletes,
        // it is possible that at this point the topic still exists
        try {
            demoTopic.values().get(TOPIC_NAME).get(); // this is just to get the exception when topic doesn't exist
            System.out.println("Topic " + TOPIC_NAME + " is still around");
        } catch (ExecutionException e) {
            System.out.println("Topic " + TOPIC_NAME + " is gone");
        }

        admin.close(Duration.ofSeconds(30));
    }
}
