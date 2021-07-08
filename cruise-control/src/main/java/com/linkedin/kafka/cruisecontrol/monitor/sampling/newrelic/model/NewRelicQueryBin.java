/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Stores a list of KafkaSize objects which all have a combined
 * size less than or equal to MAX_SIZE - 1.
 */
public abstract class NewRelicQueryBin {
    private int _currentSize;
    private List<KafkaSize> _kafkaSizes;
    private static int MAX_SIZE;

    public static void setMaxSize(int maxSize) {
        MAX_SIZE = maxSize;
    }

    public NewRelicQueryBin() {
        _kafkaSizes = new ArrayList<>();
        _currentSize = 0;
    }

    /**
     * Attempts to add a new size object to this object. We will only be
     * able to add it if the total size (_currentSize + new size) after
     * adding the new size is less than the overall MAX_SIZE.
     * @param newKafkaSize - The new size that we want to add.
     * @return - Whether or not we were able to add the new size.
     */
    public boolean addKafkaSize(KafkaSize newKafkaSize) {
        int newSize = newKafkaSize.getSize();
        if (_currentSize + newSize >= MAX_SIZE) {
            return false;
        } else {
            _currentSize += newSize;
            _kafkaSizes.add(newKafkaSize);
            return true;
        }
    }

    public int getSize() {
        return _currentSize;
    }

    public List<KafkaSize> getKafkaSizes() {
        return _kafkaSizes;
    }

    /**
     * Combine the values in the list into a properly
     * formatted String that will work as part of a NRQL query for the type of
     * objects that we are using.
     * @return - String which can be used inside the NRQL query we want
     * to represent the items inside this query bin.
     */
    public abstract String generateStringForQuery();
}
