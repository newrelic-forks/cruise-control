/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model;

import java.util.Objects;

/**
 * Used to store the number of brokers in each topic.
 */
public class BrokerTopicCount extends KafkaSize {
    private int _brokerId;

    public BrokerTopicCount(int size, int brokerId) {
        super(size);
        _brokerId = brokerId;
    }

    public int getBrokerId() {
        return _brokerId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        BrokerTopicCount otherSize = (BrokerTopicCount) other;
        return this.hashCode() == otherSize.hashCode();
    }

    @Override
    public int hashCode() {
        return Objects.hash(_brokerId, getSize());
    }

    @Override
    public String toString() {
        return String.format("BrokerSize with brokerId %s and size: %s", _brokerId, getSize());
    }
}
