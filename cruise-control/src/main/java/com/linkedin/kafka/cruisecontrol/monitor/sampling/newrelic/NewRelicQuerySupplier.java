/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic;

import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import java.util.Map;

public interface NewRelicQuerySupplier {
    String brokerQuery(String clusterName);

    String topicQuery(String brokerSelect, String clusterName);

    String partitionQuery(String whereClause, String clusterName);

    Map<String, RawMetricType> getBrokerMap();

    Map<String, RawMetricType> getTopicMap();

    Map<String, RawMetricType> getPartitionMap();
}
