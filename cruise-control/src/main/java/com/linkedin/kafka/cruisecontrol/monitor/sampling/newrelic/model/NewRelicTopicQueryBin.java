/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model;

import java.util.ArrayList;

/**
 * This bin is used to be able to properly format topic queries
 * using the contents inside of the bin. Note that
 * the KafkaSize objects stored in this bin should be
 * TopicReplicaCount objects.
 */
public class NewRelicTopicQueryBin extends NewRelicQueryBin {
    public NewRelicTopicQueryBin() {
        super();
    }

    /**
     * Given the list of all topics in this bin,
     * we generate a string of the topics separated by a comma and space
     * @return - String of topics separated by comma and space w/ no trailing comma or space
     */
    @Override
    public String generateStringForQuery() {
        ArrayList<TopicReplicaCount> topics = new ArrayList<>();
        ArrayList<TopicReplicaCount> brokerTopics = new ArrayList<>();

        for (KafkaSize size: getSizes()) {
            TopicReplicaCount topicReplicaCount = (TopicReplicaCount) size;
            if (topicReplicaCount.getIsBrokerTopic()) {
                brokerTopics.add(topicReplicaCount);
            } else {
                topics.add(topicReplicaCount);
            }
        }
        // We want a comma on all but the last element so we will handle the last one separately
        // We want these topics to be in the format:
        // "topic IN ('topic1', 'topic2', ...)"
        StringBuffer topicBuffer = new StringBuffer();
        if (topics.size() > 0) {
            topicBuffer.append("topic IN (");

            for (int i = 0; i < topics.size() - 1; i++) {
                topicBuffer.append(String.format("'%s', ", topics.get(i).getTopic()));
            }
            // Add in last element without a comma or space
            topicBuffer.append(String.format("'%s')", topics.get(topics.size() - 1).getTopic()));
        }

        // We want to combine broker topics into the format
        // "(topic = 'topic1' AND broker = brokerId1) OR (topic = 'topic2' AND broker = brokerId2) ..."
        StringBuffer topicBrokerBuffer = new StringBuffer();
        if (brokerTopics.size() > 0) {
            if (topics.size() > 0) {
                topicBrokerBuffer.append(" OR ");
            }
            for (int i = 0; i < brokerTopics.size() - 1; i++) {
                topicBrokerBuffer.append(String.format("(topic = '%s' AND broker = %s) OR ",
                        brokerTopics.get(i).getTopic(), brokerTopics.get(i).getBrokerId()));
            }
            // Add in last element without OR
            topicBrokerBuffer.append(String.format("(topic = '%s' AND broker = %s)",
                    brokerTopics.get(brokerTopics.size() - 1).getTopic(),
                    brokerTopics.get(brokerTopics.size() - 1).getBrokerId()));
        }

        return topicBuffer + topicBrokerBuffer.toString();
    }
}
