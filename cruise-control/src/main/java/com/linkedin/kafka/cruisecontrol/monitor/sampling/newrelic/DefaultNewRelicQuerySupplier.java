/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;

/**
 * Contains the NRQL queries which will output broker, topic, and partition level
 * stats which are used by cruise control.
 */
public final class DefaultNewRelicQuerySupplier implements NewRelicQuerySupplier {
    private static final Map<String, RawMetricType> UNMODIFIABLE_BROKER_MAP = buildBrokerMap();
    private static final Map<String, RawMetricType> UNMODIFIABLE_TOPIC_MAP = buildTopicMap();
    private static final Map<String, RawMetricType> UNMODIFIABLE_PARTITION_MAP = buildPartitionMap();

    private static final String BROKER_QUERY = "FROM KafkaBrokerStats "
            + "SELECT %s "
            + "WHERE cluster = '%s' "
            + "FACET broker "
            + "SINCE 1 minute ago "
            + "LIMIT MAX";

    private static final String TOPIC_QUERY = "FROM KafkaBrokerTopicStats "
            + "SELECT %s "
            + "WHERE cluster = '%s' "
            + "%s"
            + "AND topic is NOT NULL "
            + "FACET broker, topic "
            + "SINCE 1 minute ago "
            + "LIMIT MAX";

    private static final String PARTITION_QUERY = "FROM KafkaPartitionSizeStats "
            + "SELECT max(partitionSize) "
            + "WHERE cluster = '%s' "
            + "WHERE %s "
            + "FACET broker, topic, partition "
            + "SINCE 1 minute ago "
            + "LIMIT MAX";

    @Override
    public String brokerQuery(String clusterName) {
        return brokerQueryFormat(generateFeatures(UNMODIFIABLE_BROKER_MAP), clusterName);
    }

    private static String brokerQueryFormat(String select, String clusterName) {
        return String.format(BROKER_QUERY, select, clusterName);
    }

    @Override
    public String topicQuery(String brokerSelect, String clusterName) {
        return topicQueryFormat(generateFeatures(UNMODIFIABLE_TOPIC_MAP), brokerSelect, clusterName);
    }

    private static String topicQueryFormat(String select, String brokerSelect, String clusterName) {
        return String.format(TOPIC_QUERY, select, clusterName, brokerSelect);
    }

    @Override
    public String partitionQuery(String whereClause, String clusterName) {
        return String.format(PARTITION_QUERY, clusterName, whereClause);
    }

    @Override
    public Map<String, RawMetricType> getUnmodifiableBrokerMap() {
        return UNMODIFIABLE_BROKER_MAP;
    }

    @Override
    public Map<String, RawMetricType> getUnmodifiableTopicMap() {
        return UNMODIFIABLE_TOPIC_MAP;
    }

    @Override
    public Map<String, RawMetricType> getUnmodifiablePartitionMap() {
        return UNMODIFIABLE_PARTITION_MAP;
    }

    /**
     * Generates the sequence of SELECT features we want to use in our queries
     * based on which metrics we are looking to collect in that query.
     * @param metrics - List of queryLabels mapped to their metric types
     * @return - Feature string of the format: "max(feature1), max(feature2), ... max(featureN)"
     */
    private static String generateFeatures(Map<String, RawMetricType> metrics) {
        StringBuffer buffer = new StringBuffer();

        // We want a comma on all but the last element so we will handle the last one separately
        String[] metricLabels = metrics.keySet().toArray(new String [0]);
        for (int i = 0; i < metricLabels.length - 1; i++) {
            buffer.append(String.format("max(%s), ", metricLabels[i]));
        }
        // Add in last element without a comma or space
        buffer.append(String.format("max(%s)", metricLabels[metricLabels.length - 1]));

        return buffer.toString();
    }

    private static Map<String, RawMetricType> buildBrokerMap() {
        Map<String, RawMetricType> brokerMap = new HashMap<>();

        // add in all the broker metrics
        brokerMap.put("bytesInPerSec", ALL_TOPIC_BYTES_IN);
        brokerMap.put("bytesOutPerSec", ALL_TOPIC_BYTES_OUT);
        brokerMap.put("replicationBytesInPerSec", ALL_TOPIC_REPLICATION_BYTES_IN);
        brokerMap.put("replicationBytesOutPerSec", ALL_TOPIC_REPLICATION_BYTES_OUT);
        brokerMap.put("totalFetchRequestsPerSec", ALL_TOPIC_FETCH_REQUEST_RATE);
        brokerMap.put("totalProduceRequestsPerSec", ALL_TOPIC_PRODUCE_REQUEST_RATE);
        brokerMap.put("messagesInPerSec", ALL_TOPIC_MESSAGES_IN_PER_SEC);
        brokerMap.put("cpuTotalUtilizationPercentage / 100", BROKER_CPU_UTIL);
        brokerMap.put("produceRequestsPerSec", BROKER_PRODUCE_REQUEST_RATE);
        brokerMap.put("fetchConsumerRequestsPerSec", BROKER_CONSUMER_FETCH_REQUEST_RATE);
        brokerMap.put("fetchFollowerRequestsPerSec", BROKER_FOLLOWER_FETCH_REQUEST_RATE);
        brokerMap.put("requestQueueSize", BROKER_REQUEST_QUEUE_SIZE);
        brokerMap.put("responseQueueSize", BROKER_RESPONSE_QUEUE_SIZE);
        brokerMap.put("produceQueueTimeMaxMs", BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX);
        brokerMap.put("produceQueueTimeMeanMs", BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN);
        brokerMap.put("produceQueueTime50thPercentileMs", BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH);
        brokerMap.put("produceQueueTime999thPercentileMs", BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH);
        brokerMap.put("fetchConsumerQueueTimeMaxMs", BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX);
        brokerMap.put("fetchConsumerQueueTimeMeanMs", BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN);
        brokerMap.put("fetchConsumerQueueTime50thPercentileMs", BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH);
        brokerMap.put("fetchConsumerQueueTime999thPercentileMs", BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH);
        brokerMap.put("fetchFollowerQueueTimeMaxMs", BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX);
        brokerMap.put("fetchFollowerQueueTimeMeanMs", BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN);
        brokerMap.put("fetchFollowerQueueTime50thPercentileMs", BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH);
        brokerMap.put("fetchFollowerQueueTime999thPercentileMs", BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH);
        brokerMap.put("produceLocalTimeMaxMs", BROKER_PRODUCE_LOCAL_TIME_MS_MAX);
        brokerMap.put("produceLocalTimeMeanMs", BROKER_PRODUCE_LOCAL_TIME_MS_MEAN);
        brokerMap.put("produceLocalTime50thPercentileMs", BROKER_PRODUCE_LOCAL_TIME_MS_50TH);
        brokerMap.put("produceLocalTime999thPercentileMs", BROKER_PRODUCE_LOCAL_TIME_MS_999TH);
        brokerMap.put("fetchConsumerLocalTimeMaxMs", BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX);
        brokerMap.put("fetchConsumerLocalTimeMeanMs", BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN);
        brokerMap.put("fetchConsumerLocalTime50thPercentileMs", BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH);
        brokerMap.put("fetchConsumerLocalTime999thPercentileMs", BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH);
        brokerMap.put("fetchFollowerLocalTimeMaxMs", BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX);
        brokerMap.put("fetchFollowerLocalTimeMeanMs", BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN);
        brokerMap.put("fetchFollowerLocalTime50thPercentileMs", BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH);
        brokerMap.put("fetchFollowerLocalTime999thPercentileMs", BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH);
        brokerMap.put("produceLatencyMaxMs", BROKER_PRODUCE_TOTAL_TIME_MS_MAX);
        brokerMap.put("produceLatencyMeanMs", BROKER_PRODUCE_TOTAL_TIME_MS_MEAN);
        brokerMap.put("produceLatency50thPercentileMs", BROKER_PRODUCE_TOTAL_TIME_MS_50TH);
        brokerMap.put("produceLatency999thPercentileMs", BROKER_PRODUCE_TOTAL_TIME_MS_999TH);
        brokerMap.put("fetchConsumerLatencyMaxMs", BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX);
        brokerMap.put("fetchConsumerLatencyMeanMs", BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN);
        brokerMap.put("fetchConsumerLatency50thPercentileMs", BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH);
        brokerMap.put("fetchConsumerLatency999thPercentileMs", BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH);
        brokerMap.put("fetchFollowerLatencyMaxMs", BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX);
        brokerMap.put("fetchFollowerLatencyMeanMs", BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN);
        brokerMap.put("fetchFollowerLatency50thPercentileMs", BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH);
        brokerMap.put("fetchFollowerLatency999thPercentileMs", BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH);
        brokerMap.put("logFlushOneMinuteRateMs", BROKER_LOG_FLUSH_RATE);
        brokerMap.put("logFlushMaxMs", BROKER_LOG_FLUSH_TIME_MS_MAX);
        brokerMap.put("logFlushMeanMs", BROKER_LOG_FLUSH_TIME_MS_MEAN);
        brokerMap.put("logFlush50thPercentileMs", BROKER_LOG_FLUSH_TIME_MS_50TH);
        brokerMap.put("logFlush999thPercentileMs", BROKER_LOG_FLUSH_TIME_MS_999TH);
        brokerMap.put("requestHandlerAvgIdlePercent", BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT);

        return Collections.unmodifiableMap(brokerMap);
    }

    private static Map<String, RawMetricType> buildTopicMap() {
        Map<String, RawMetricType> topicMap = new HashMap<>();

        // topic level metrics
        topicMap.put("bytesInPerSec", TOPIC_BYTES_IN);
        topicMap.put("bytesOutPerSec", TOPIC_BYTES_OUT);
        // We don't collect the following data on a topic level so I'm not including them
        //topicMap.put("replicationBytesInPerSec", TOPIC_REPLICATION_BYTES_IN);
        //topicMap.put("replicationBytesOutPerSec", TOPIC_REPLICATION_BYTES_OUT);
        topicMap.put("totalFetchRequestsPerSec", TOPIC_FETCH_REQUEST_RATE);
        topicMap.put("totalProduceRequestsPerSec", TOPIC_PRODUCE_REQUEST_RATE);
        topicMap.put("messagesInPerSec", TOPIC_MESSAGES_IN_PER_SEC);

        return Collections.unmodifiableMap(topicMap);
    }

    private static Map<String, RawMetricType> buildPartitionMap() {
        Map<String, RawMetricType> partitionMap = new HashMap<>();

        // partition level metrics
        partitionMap.put("partitionSize", PARTITION_SIZE);

        return Collections.unmodifiableMap(partitionMap);
    }
}
