package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.text.CollationElementIterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class TopicCreator {

    private AdminClient admin;

    public TopicCreator(AdminClient admin) {
        this.admin = admin;
    }

    // silly example of a method that will create a topic if its name start with "test"
    public void maybeCreateTopic(String topicName)
            throws ExecutionException, InterruptedException {
        Collection<NewTopic> topics = new ArrayList<>();
        topics.add(new NewTopic(topicName, 1, (short) 1));
        if (topicName.toLowerCase().startsWith("test")) {
            admin.createTopics(topics);

            // alter configs just to demonstrate a point
            ConfigResource configResource =
                    new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            ConfigEntry compaction =
                    new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG,
                            TopicConfig.CLEANUP_POLICY_COMPACT);

            Collection<AlterConfigOp> configOp = new ArrayList<AlterConfigOp>();
            configOp.add(new AlterConfigOp(compaction, AlterConfigOp.OpType.SET));
            Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = new HashMap<>();
            alterConfigs.put(configResource, configOp);
            admin.incrementalAlterConfigs(alterConfigs).all().get();
        }
    }
}
