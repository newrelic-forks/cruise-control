/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model;

import java.util.Objects;

/**
 * Used to pair topics together with their size.
 * Note that size in this context refers to the number of
 * leaders and replicas of this topic.
 */
public class TopicPartitionCount extends KafkaSize {
    private String _topic;
    private int _brokerId;
    private boolean _isBrokerTopic;

    public TopicPartitionCount(String topic, int size) {
        super(size);
        _topic = topic;
        _isBrokerTopic = false;
    }

    public TopicPartitionCount(String topic, int size, int brokerId) {
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
        TopicPartitionCount topicPartitionCountOther = (TopicPartitionCount) other;
        return hashCode() == topicPartitionCountOther.hashCode();
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
