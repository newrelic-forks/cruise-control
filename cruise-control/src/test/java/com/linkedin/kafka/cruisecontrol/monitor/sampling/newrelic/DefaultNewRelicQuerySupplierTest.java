/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic;

import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import org.junit.Test;
import java.util.Map;

public class DefaultNewRelicQuerySupplierTest {
    private NewRelicQuerySupplier _querySupplier = new DefaultNewRelicQuerySupplier();

    @Test(expected = UnsupportedOperationException.class)
    public void testUnmodifiableBrokerMap() {
        Map<String, RawMetricType> brokerMap = _querySupplier.getUnmodifiableBrokerMap();
        brokerMap.put("Test", RawMetricType.ALL_TOPIC_BYTES_IN);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnmodifiableTopicMap() {
        Map<String, RawMetricType> brokerMap = _querySupplier.getUnmodifiableTopicMap();
        brokerMap.put("Test", RawMetricType.TOPIC_BYTES_IN);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnmodifiablePartitionMap() {
        Map<String, RawMetricType> brokerMap = _querySupplier.getUnmodifiablePartitionMap();
        brokerMap.put("Test", RawMetricType.PARTITION_SIZE);
    }
}
