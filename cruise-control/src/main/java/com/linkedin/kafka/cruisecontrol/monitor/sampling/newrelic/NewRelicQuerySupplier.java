/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic;

import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import java.util.Map;

/**
 *
 */
public interface NewRelicQuerySupplier {
    /**
     * Returns a NRQL query which will be used to acquire all the information
     * related to brokers of this cluster that cruise control needs. The specific broker metrics
     * that we need for cruise control are in the enum RawMetricType.
     * @param clusterName - Name of the cluster that we are acquiring the broker metrics for
     * @return - The actual NRQL query we can run to acquire the broker data.
     */
    String brokerQuery(String clusterName);

    /**
     * Returns a NRQL query that will be used to acquire all the information
     * related to topics in the specified brokers of this cluster that cruise control needs.
     * The specific topic metrics that we need for cruise control are in the enum RawMetricType.
     * Note that it is not necessary to acquire data for TOPIC_REPLICATION_BYTES_IN and
     * TOPIC_REPLICATION_BYTES_OUT if this data is not being collected.
     * @param brokerSelect - Brokers to select in the situation that there are more than
     *                    NewRelicMetricSampler.MAX_SIZE unique topic/broker combinations in this cluster.
     * @param clusterName - Name of the cluster that we are acquiring the topic metrics for
     * @return - The actual NRQL query we can run to acquire information about the topics in the
     *           specified brokers.
     */
    String topicQuery(String brokerSelect, String clusterName);

    /**
     * Returns a NRQL query that will be used to acquire all the information related
     * to the replicas in the specified topics (or broker and topic) combination of this cluster
     * that cruise control needs. The specific partition metrics that we need for cruise control
     * are in the enum RawMetricType.
     * @param whereClause - Clause to select for replicas since it is typical for a cluster to hav
     *                    more than NewRelicMetricSampler.MAX_SIZE unique replicas per topic/broker
     *                    combinations.
     * @param clusterName - Name of the cluster that we are acquiring the partition metrics for
     * @return - The actual NRQL query we can run to acquire information about the replicas in the
     *           specified whereClause. The query should output one value for each replica that we need.
     */
    String partitionQuery(String whereClause, String clusterName);

    /**
     * Getter for BrokerMap
     * @return - Gets a map of String to RawMetricType which will be used to
     *           look at a NRQL broker query output and map the output values
     *           to different broker RawMetricTypes. This map should be unmodifiable and should
     *           map to all possible broker query output types.
     */
    Map<String, RawMetricType> getUnmodifiableBrokerMap();

    /**
     * Getter for TopicMap
     * @return - Gets a map of String to RawMetricType which will be used to
     *           look at a NRQL topic query output and map the output values
     *           to different topic RawMetricTypes. This map should be unmodifiable and should
     *           map to all possible topic query output types.
     */
    Map<String, RawMetricType> getUnmodifiableTopicMap();

    /**
     * Getter for PartitionMap
     * @return - Gets a map of String to RawMetricType which will be used to
     *           look at NRQL partition query output and map the output values
     *           to different partition RawMetricTypes. This map should be unmodifiable and should
     *           map to all possible partition query output types.
     */
    Map<String, RawMetricType> getUnmodifiablePartitionMap();
}
