/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.NewRelicQuerySupplier;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Represents one result from a query to NRDB. 
 */
public class NewRelicQueryResult {

    public static final String BEGIN_TIME_SECONDS_ATTR = "beginTimeSeconds";
    public static final String END_TIME_SECONDS_ATTR = "endTimeSeconds";
    public static final String FACET_ATTR = "facet";
    public static final String CLUSTER = "cluster";
    public static final String BROKER = "broker";
    public static final String TOPIC = "topic";
    public static final String PARTITION = "partition";

    // Values from the query that we don't want to parse directly
    // but may handle separately based on which ones we need
    private static final Set<String> RESERVED_ATTRS = new HashSet<>();
    static {
        RESERVED_ATTRS.add(BEGIN_TIME_SECONDS_ATTR);
        RESERVED_ATTRS.add(END_TIME_SECONDS_ATTR);
        RESERVED_ATTRS.add(FACET_ATTR);
        RESERVED_ATTRS.add(BROKER);
        RESERVED_ATTRS.add(TOPIC);
        RESERVED_ATTRS.add(PARTITION);

        // Note that we don't need to collect this since Cruise Control
        // only looks at data from one cluster
        RESERVED_ATTRS.add(CLUSTER);
    }
    private final int _brokerID;
    private final String _topic;
    private final int _partition;
    private final long _epochTimeMilli;

    private final Map<RawMetricType, Double> _results = new HashMap<>();

    private static NewRelicQuerySupplier _querySupplier;

    public static void setupQuerySupplier(NewRelicQuerySupplier querySupplier) {
        _querySupplier = querySupplier;
    }

    /**
     * Takes a JSON result which represents one of the results from a NRQL query,
     * figures out which type of query it is (broker, topic, or partition),
     * and obtains the value of the query for each of the separate RawMetricTypes
     * which are being used in this query. Finally will store a map
     * of the RawMetricType to the value of that metric.
     * @param result - The JSON result that we want to parse and obtain the value from.
     */
    public NewRelicQueryResult(JsonNode result) {
        _epochTimeMilli = Instant.now().toEpochMilli();

        // If facet is one item, this is a broker level query: facets = broker
        // If length of facets is 2, this is a topic level query: facets = [broker, topic]
        // If length of facets is 3, this is a partition level query: facets = [broker, topic, partition]
        Map<String, RawMetricType> valueToMetricMap;
        JsonNode facets = result.get(FACET_ATTR);
        if (facets.getNodeType() == JsonNodeType.ARRAY) {
            _brokerID = facets.get(0).asInt();
            _topic = facets.get(1).asText();
            if (facets.has(2)) {
                _partition = facets.get(2).asInt();
                valueToMetricMap = _querySupplier.getUnmodifiablePartitionMap();
            } else {
                _partition = -1;
                valueToMetricMap = _querySupplier.getUnmodifiableTopicMap();
            }
        } else {
            valueToMetricMap = _querySupplier.getUnmodifiableBrokerMap();
            _brokerID = facets.asInt();
            _topic = null;
            _partition = -1;
        }

        Iterator<String> fieldNames = result.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();

            if (RESERVED_ATTRS.contains(fieldName)) {
                continue;
            }

            String metricLabel = fieldName.split("\\.")[1];
            _results.put(valueToMetricMap.get(metricLabel), result.get(fieldName).asDouble());
        }
    }

    // Use the following for testing NewRelicAdapter
    public NewRelicQueryResult(int brokerID, Map<RawMetricType, Double> results) {
        _brokerID = brokerID;
        _topic = null;
        _partition = -1;
        _epochTimeMilli = Instant.now().toEpochMilli();
        _results.putAll(results);
    }

    public NewRelicQueryResult(int brokerID, String topic, Map<RawMetricType, Double> results) {
        _brokerID = brokerID;
        _topic = topic;
        _partition = -1;
        _epochTimeMilli = Instant.now().toEpochMilli();
        _results.putAll(results);
    }

    public NewRelicQueryResult(int brokerID, String topic, int partition, Map<RawMetricType, Double> results) {
        _brokerID = brokerID;
        _topic = topic;
        _partition = partition;
        _epochTimeMilli = Instant.now().toEpochMilli();
        _results.putAll(results);
    }

    public int getBrokerID() {
        return _brokerID;
    }

    public String getTopic() {
        return _topic;
    }

    public long getTimeMs() {
        return _epochTimeMilli;
    }

    public int getPartition() {
        return _partition;
    }

    public Map<RawMetricType, Double> getResults() {
        return _results;
    }

    @Override
    public String toString() {
        return "NewRelic Query Result: " + _results.toString();
    }
}
