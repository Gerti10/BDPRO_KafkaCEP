package com.github.fhuss.kafka.streams.cep;

import com.esotericsoftware.minlog.Log;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicProducer {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient admin = AdminClient.create(props)){
            int partitions = 1;
            short replication = 1;
            Set<String> topicsSet = admin.listTopics().names().get();
            admin.deleteTopics(topicsSet).all().get();
            if (admin.listTopics().names().get().size() == 0) Log.info("Topics are deleted");

            Map<String, String> newTopicConfig = new HashMap<>();
            newTopicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
            NewTopic velocityTopic = new NewTopic("velocities", partitions, replication).configs(newTopicConfig);
            NewTopic quantityTopic = new NewTopic("quantities", partitions, replication).configs(newTopicConfig);
            NewTopic matchesTopic = new NewTopic("Matches", partitions, replication).configs(newTopicConfig);

            Collection<NewTopic> topicCollection = new ArrayList<>();
            topicCollection.add(velocityTopic);
            topicCollection.add(quantityTopic);
            topicCollection.add(matchesTopic);

            CreateTopicsResult result = admin.createTopics(topicCollection);
            result.all().get();

            KafkaFuture<Void> future = result.values().get("velocities");
            future.get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
