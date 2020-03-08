package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.NoReassignmentInProgressException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.joda.time.DateTime;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminClientExample {

    public static final String TOPIC_NAME = "demo-topic";
    public static final String CONSUMER_GROUP = "AvroClicksSessionizer";
    public static final List<String> TOPIC_LIST = Collections.singletonList(TOPIC_NAME);
    public static final List<String> CONSUMER_GRP_LIST = Collections.singletonList(CONSUMER_GROUP);
    public static final int NUM_PARTITIONS = 6;
    public static final short REP_FACTOR = 1;

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // initialize admin client
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 1000);
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
                //System.exit(-1);
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
            CreateTopicsResult newTopic = admin.createTopics(Collections.singletonList(new NewTopic(TOPIC_NAME, NUM_PARTITIONS, REP_FACTOR)));

            // uncomment to see the topic get created with wrong number of partitions
            //CreateTopicsResult newTopic = admin.createTopics(Collections.singletonList(new NewTopic(TOPIC_NAME, Optional.empty(), Optional.empty())));

            // Check that the topic was created correctly:
            if (newTopic.numPartitions(TOPIC_NAME).get() != NUM_PARTITIONS) {
                System.out.println("Topic was created with wrong number of partitions. Exiting.");
                // System.exit(-1);
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

       // admin.deleteTopics(TOPIC_LIST).all().get();

        // Check that it is gone. Note that due to the async nature of deletes,
        // it is possible that at this point the topic still exists
        try {
            demoTopic.values().get(TOPIC_NAME).get(); // this is just to get the exception when topic doesn't exist
            System.out.println("Topic " + TOPIC_NAME + " is still around");
        } catch (ExecutionException e) {
            System.out.println("Topic " + TOPIC_NAME + " is gone");
        }

        // List consumer groups
        System.out.println("Listing consumer groups, if any exist:");
        admin.listConsumerGroups().valid().get().forEach(System.out::println);

        // Describe a group
        ConsumerGroupDescription groupDescription = admin.describeConsumerGroups(CONSUMER_GRP_LIST)
                .describedGroups().get(CONSUMER_GROUP).get();
        System.out.println("Description of group " + CONSUMER_GROUP + ":" + groupDescription);

        // Get offsets committed by the group
        Map<TopicPartition, OffsetAndMetadata> partitionOffsets =
                admin.listConsumerGroupOffsets(CONSUMER_GROUP).partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        Map<TopicPartition, OffsetSpec> requestEarliestOffsets = new HashMap<>();
        Map<TopicPartition, OffsetSpec> requestOlderOffsets = new HashMap<>();
        DateTime resetTo = new DateTime().minusHours(2);
        // For all topics and partitions that have offsets committed by the group, get their latest offsets, earliest offsets
        // and the offset for 2h ago. Note that I'm populating the request for 2h old offsets, but not using them.
        // You can swap the use of "Earliest" in the `alterConsumerGroupOffset` example with the offsets from 2h ago
        for(TopicPartition tp: partitionOffsets.keySet()) {
            requestLatestOffsets.put(tp, OffsetSpec.latest());
            requestEarliestOffsets.put(tp, OffsetSpec.earliest());
            requestOlderOffsets.put(tp, OffsetSpec.forTimestamp(resetTo.getMillis()));
        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = admin.listOffsets(requestLatestOffsets).all().get();

        for (Map.Entry<TopicPartition, OffsetAndMetadata> e: partitionOffsets.entrySet()) {
            String topic = e.getKey().topic();
            int partition =  e.getKey().partition();
            long committedOffset = e.getValue().offset();
            long latestOffset = latestOffsets.get(e.getKey()).offset();

            System.out.println("Consumer group " + CONSUMER_GROUP +" has committed offset " + committedOffset +
                   " to topic " + topic + " partition " + partition + ". The latest offset in the partition is "
                    +  latestOffset + " so consumer group is " + (latestOffset - committedOffset) + " records behind");
        }

        // Reset offsets to beginning of topic. You can try to reset to 2h ago too by using `requestOlderOffsets`
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets = admin.listOffsets(requestEarliestOffsets).all().get();
        Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e: earliestOffsets.entrySet()) {
            System.out.println("Will reset topic-partition " + e.getKey() + " to offset " + e.getValue().offset());
            resetOffsets.put(e.getKey(), new OffsetAndMetadata(e.getValue().offset()));
        }

        try {
            admin.alterConsumerGroupOffsets(CONSUMER_GROUP, resetOffsets).all().get();
        } catch (ExecutionException e) {
            System.out.println("Failed to update the offsets committed by group " + CONSUMER_GROUP +
                    " with error " + e.getMessage());
            if (e.getCause() instanceof UnknownMemberIdException)
                System.out.println("Check if consumer group is still active.");
        }

        // Who are the brokers? Who is the controller?
        DescribeClusterResult cluster = admin.describeCluster();

        System.out.println("Connected to cluster " + cluster.clusterId().get());
        System.out.println("The brokers in the cluster are:");
        cluster.nodes().get().forEach(node -> System.out.println("    * " + node));
        System.out.println("The controller is: " + cluster.controller().get());

        // add partitions
        Map<String, NewPartitions> newPartitions = new HashMap<>();
        newPartitions.put(TOPIC_NAME, NewPartitions.increaseTo(NUM_PARTITIONS+2));
        try {
            admin.createPartitions(newPartitions).all().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof InvalidPartitionsException) {
                System.out.printf("Couldn't modify number of partitions in topic: " + e.getMessage());
            }
        }

        // delete records
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> olderOffsets = admin.listOffsets(requestOlderOffsets).all().get();
        Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>  e: olderOffsets.entrySet())
            recordsToDelete.put(e.getKey(), RecordsToDelete.beforeOffset(e.getValue().offset()));
        admin.deleteRecords(recordsToDelete).all().get();

        try {
            admin.electLeaders(ElectionType.PREFERRED, Collections.singleton(new TopicPartition(TOPIC_NAME, 0))).all().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ElectionNotNeededException) {
                System.out.println("All leaders are preferred leaders, no need to do anything");
            }
        }

        // reassign partitions to new broker
        Map<TopicPartition, Optional<NewPartitionReassignment>> reassignment = new HashMap<>();
        reassignment.put(new TopicPartition(TOPIC_NAME, 0), Optional.of(new NewPartitionReassignment(Arrays.asList(0,1))));
        reassignment.put(new TopicPartition(TOPIC_NAME, 1), Optional.of(new NewPartitionReassignment(Arrays.asList(0))));
        reassignment.put(new TopicPartition(TOPIC_NAME, 2), Optional.of(new NewPartitionReassignment(Arrays.asList(1,0))));
        reassignment.put(new TopicPartition(TOPIC_NAME, 3), Optional.empty());
        try {
            admin.alterPartitionReassignments(reassignment).all().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NoReassignmentInProgressException) {
                System.out.println("We tried cancelling a reassignment that was not happening anyway. Lets ignore this.");
            }
        }

        System.out.println("currently reassigning: " + admin.listPartitionReassignments().reassignments().get());
        demoTopic = admin.describeTopics(TOPIC_LIST);
        topicDescription = demoTopic.values().get(TOPIC_NAME).get();
        System.out.println("Description of demo topic:" + topicDescription);

        admin.close(Duration.ofSeconds(30));
    }
}
