/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model;

/**
 * Stores the size of some kafka object type. This class is a comparable
 * because we want to often sort by the size of two different objects of this class.
 *
 * Note that Object.equals method will not always return true when
 * compareTo is 0 because Object.equals will check other metadata object the object.
 */
public abstract class KafkaSize implements Comparable<KafkaSize> {
    private int _size;

    public KafkaSize(int size) {
        _size = size;
    }

    public int getSize() {
        return _size;
    }

    @Override
    public int compareTo(KafkaSize other) {
        return _size - other.getSize();
    }

    @Override
    public String toString() {
        return String.format("KafkaSize with size: %s", _size);
    }
}
