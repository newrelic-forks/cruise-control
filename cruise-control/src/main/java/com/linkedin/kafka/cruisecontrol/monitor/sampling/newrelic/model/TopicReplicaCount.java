/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model;

import java.util.Objects;

/**
 * Used to pair topics together with the count of their replicas.
 * We need this instead of just the count of partitions per topic
 * because we query for replica data separately so each replica
 * will end up contributing to our query limit.
 */
public class TopicReplicaCount extends KafkaSize {
    private String _topic;
    private int _brokerId;
    private boolean _isBrokerTopic;

    public TopicReplicaCount(String topic, int size) {
        super(size);
        _topic = topic;
        _isBrokerTopic = false;
    }

    public TopicReplicaCount(String topic, int size, int brokerId) {
        super(size);
        _topic = topic;
        _brokerId = brokerId;
        _isBrokerTopic = true;
    }

    public String getTopic() {
        return _topic;
    }

    public int getBrokerId() {
        return _brokerId;
    }

    public boolean getIsBrokerTopic() {
        return _isBrokerTopic;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        TopicReplicaCount topicReplicaCountOther = (TopicReplicaCount) other;
        return this.getTopic().equals(topicReplicaCountOther.getTopic())
                && this.getSize() == topicReplicaCountOther.getSize()
                && this.getIsBrokerTopic() == topicReplicaCountOther.getIsBrokerTopic()
                && this.getBrokerId() == topicReplicaCountOther.getBrokerId();
    }

    @Override
    public int hashCode() {
        return Objects.hash(_topic, _brokerId, _isBrokerTopic, getSize());
    }

    @Override
    public String toString() {
        if (_isBrokerTopic) {
            return String.format("TopicSize with topic %s and size: %s",
                    _topic, getSize());
        } else {
            return String.format("TopicSize with brokerId %s, topic %s, and size: %s",
                    _brokerId, _topic, getSize());
        }
    }
}
