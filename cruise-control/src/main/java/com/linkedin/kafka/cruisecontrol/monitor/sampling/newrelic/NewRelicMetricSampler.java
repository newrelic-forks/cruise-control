/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic;

import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.BrokerMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.PartitionMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.TopicMetric;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.AbstractMetricSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSamplerOptions;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model.NewRelicBrokerQueryBin;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model.BrokerTopicCount;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model.KafkaSize;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model.NewRelicQueryBin;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model.NewRelicQueryResult;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model.NewRelicTopicQueryBin;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model.TopicReplicaCount;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.error.MissingEnvironmentVariableException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;

/**
 * Metric sampler that fetches Kafka metrics from the New Relic Database (NRDB).
 *
 * Note that this class will default to using the queries directly in NewRelicQuerySupplier
 * and that if you need to query more data, you should update that query supplier.
 *
 * Required configurations for this class.
 * <ul>
 *   <li>{@link #NEWRELIC_ENDPOINT_CONFIG}: The config for the HTTP endpoint of the NRDB server
 *   which is to be used as a source for sampling metrics.</li>
 *   <li>{@link #NEWRELIC_API_KEY_ENVIRONMENT}: The system environment variable location where the API
 *   key to access NRDB should be added. </li>
 *   <li>{@link #NEWRELIC_ACCOUNT_ID_CONFIG}: The account ID for the account that is accessing NRDB. </li>
 *   <li>{@link #NEWRELIC_QUERY_LIMIT_CONFIG}: The query limit for NRDB at the current moment.
 *   At the time of writing this class, that limit is currently 2000. </li>
 *   <li>{@link #CLUSTER_NAME_CONFIG}: The name of the cluster that you want to query in NRDB. </li>
 * </ul>
 */
public class NewRelicMetricSampler extends AbstractMetricSampler {
    private static final Logger LOGGER = LoggerFactory.getLogger(NewRelicMetricSampler.class);

    // Config name visible to tests
    static final String NEWRELIC_ENDPOINT_CONFIG = "newrelic.endpoint";
    static final String NEWRELIC_API_KEY_ENVIRONMENT = "NR_STAGING_ACCOUNT_1_API_KEY";
    static final String NEWRELIC_ACCOUNT_ID_CONFIG = "newrelic.account.id";
    static final String NEWRELIC_QUERY_LIMIT_CONFIG = "newrelic.query.limit";
    static final String CLUSTER_NAME_CONFIG = "newrelic.cell.name";

    // We make this protected so we can set it during the tests
    protected NewRelicAdapter _newRelicAdapter;
    private CloseableHttpClient _httpClient;

    // NRQL Query limit
    // As a note, we will only attempt to run queries of size (MAX_SIZE - 1) or less
    private static int MAX_SIZE;

    // Currently we are hardcoding this in -> later need to make it specific to whatever cluster
    // this cruise control instance is running on
    private static String CLUSTER_NAME = "";

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        configureNewRelicAdapter(configs);
        configureQueries(configs);
    }

    private void configureQueries(Map<String, ?> configs) {
        if (!configs.containsKey(NEWRELIC_QUERY_LIMIT_CONFIG)) {
            throw new ConfigException(String.format(
                    "%s config is required to have a query limit", NEWRELIC_QUERY_LIMIT_CONFIG));
        }
        MAX_SIZE = Integer.parseInt((String) configs.get(NEWRELIC_QUERY_LIMIT_CONFIG));
        NewRelicQueryBin.setMaxSize(MAX_SIZE);

        if (!configs.containsKey(CLUSTER_NAME_CONFIG)) {
            throw new ConfigException(String.format(
                    "%s config is required to have the cluster name", CLUSTER_NAME_CONFIG));
        }
        CLUSTER_NAME = (String) configs.get(CLUSTER_NAME_CONFIG);
    }

    private void configureNewRelicAdapter(Map<String, ?> configs) {
        final String endpoint = (String) configs.get(NEWRELIC_ENDPOINT_CONFIG);
        if (endpoint == null) {
            throw new ConfigException(String.format(
                    "%s config is required to have an endpoint", NEWRELIC_ENDPOINT_CONFIG));
        }

        String apiKey = System.getenv(NEWRELIC_API_KEY_ENVIRONMENT);
        if (apiKey == null) {
            // We do this for testing purposes
            apiKey = (String) configs.get(NEWRELIC_API_KEY_ENVIRONMENT);
            if (apiKey == null) {
                throw new MissingEnvironmentVariableException(String.format(
                        "%s environment variable is required to have an API Key", NEWRELIC_API_KEY_ENVIRONMENT));
            }
        }

        if (!configs.containsKey(NEWRELIC_ACCOUNT_ID_CONFIG)) {
            throw new ConfigException(String.format(
                    "%s config is required to have an account ID", NEWRELIC_ACCOUNT_ID_CONFIG));
        }
        final int accountId = Integer.parseInt((String) configs.get(NEWRELIC_ACCOUNT_ID_CONFIG));

        _httpClient = HttpClients.createDefault();
        _newRelicAdapter = new NewRelicAdapter(_httpClient, endpoint, accountId, apiKey);

    }

    /**
     * Runs the broker queries, then the topic level queries, then the partition level queries and adds
     * those metrics to be processed.
     * @param metricSamplerOptions Object that encapsulates all the options to be used for sampling metrics.
     * @return - Returns the number of metrics that were added.
     */
    @Override
    protected int retrieveMetricsForProcessing(MetricSamplerOptions metricSamplerOptions) {
        ResultCounts counts = new ResultCounts();

        // Run our broker level queries
        runBrokerQueries(counts);

        // Run topic level queries
        runTopicQueries(metricSamplerOptions.cluster(), counts);

        // Run partition level queries
        runPartitionQueries(metricSamplerOptions.cluster(), counts);

        // FIXME - Remove print statements eventually
        System.out.printf("Added %s metric values. Skipped %s invalid query results.%n",
                counts.getMetricsAdded(), counts.getResultsSkipped());
        return counts.getMetricsAdded();
    }

    /**
     * Run the NRQL queries to get our broker level stats.
     * @param counts - Keeps track of the metrics added and the results skipped.
     */
    private void runBrokerQueries(ResultCounts counts) {
        // Run our broker query first
        final String brokerQuery = NewRelicQuerySupplier.brokerQuery(CLUSTER_NAME);
        final List<NewRelicQueryResult> brokerResults;

        try {
            brokerResults = _newRelicAdapter.runQuery(brokerQuery);
        } catch (IOException e) {
            // Note that we could throw an exception here, but we don't want
            // to stop trying all future queries because this one query
            // failed to run
            LOGGER.error("Error when attempting to query NRQL for broker metrics.", e);
            return;
        }

        for (NewRelicQueryResult result : brokerResults) {
            try {
                counts.addMetricsAdded(addBrokerMetrics(result));
            } catch (InvalidNewRelicResultException e) {
                // Unlike PrometheusMetricSampler, this form of exception is probably very unlikely since
                // we will be getting cleaned up and well formed data directly from NRDB, but just keeping
                // this check here anyway to be safe
                LOGGER.trace("Invalid query result received from New Relic for query {}", brokerQuery, e);
                counts.addResultsSkipped(1);
            }
        }
    }

    /**
     * Create a semi-optimal solution to the number of topic level queries
     * to run to NRQL which are split up by different brokers and runs those queries.
     * @param cluster - Cluster object containing information metadata this cluster.
     * @param counts - Keeps track of the metrics added and the results skipped.
     */
    private void runTopicQueries(Cluster cluster, ResultCounts counts) {
        // Get the sorted list of brokers by their topic counts
        List<KafkaSize> brokerSizes = getSortedBrokersByTopicCount(cluster);

        List<NewRelicQueryBin> brokerQueryBins;
        try {
            brokerQueryBins = assignToBins(brokerSizes, NewRelicBrokerQueryBin.class);

            // Generate the queries based on the bins that TopicCounts were assigned to
            List<String> topicQueries = getTopicQueries(brokerQueryBins);

            // Run the topic queries
            for (String query: topicQueries) {
                final List<NewRelicQueryResult> queryResults;

                try {
                    queryResults = _newRelicAdapter.runQuery(query);
                } catch (IOException e) {
                    // Note that we could throw an exception here, but we don't want
                    // to stop trying all future queries because this one query
                    // failed to run
                    LOGGER.error("Error when attempting to query NRQL for metrics.", e);
                    continue;
                }

                for (NewRelicQueryResult result : queryResults) {
                    try {
                        counts.addMetricsAdded(addTopicMetrics(result));
                    } catch (InvalidNewRelicResultException e) {
                        // Unlike PrometheusMetricSampler, this form of exception is probably very unlikely since
                        // we will be getting cleaned up and well formed data directly from NRDB, but just keeping
                        // this check here anyway to be safe
                        LOGGER.trace("Invalid query result received from New Relic for topic query {}", query, e);
                        counts.addResultsSkipped(1);
                    }
                }
            }

        } catch (Exception e) {
            LOGGER.error("Error when converting topics to bins.", e);
        }
    }

    /**
     * Create a semi-optimal solution to the number of partition level queries
     * to run to NRQL which are split up by different brokers and runs those queries.
     * @param cluster - Cluster object containing information metadata this cluster.
     * @param counts - Keeps track of the metrics added and the results skipped.
     */
    private void runPartitionQueries(Cluster cluster, ResultCounts counts) {
        // Get the sorted list of topics by their leader + follower count for each partition
        List<KafkaSize> topicSizes = getSortedTopicsByReplicaCount(cluster);

        // Use FFD algorithm (more info at method header) to assign topicSizes to queries
        List<NewRelicQueryBin> topicQueryBins;
        try {
            topicQueryBins = assignToBins(topicSizes, NewRelicTopicQueryBin.class);

            // Generate the queries based on the bins that PartitionCounts were assigned to
            List<String> partitionQueries = getPartitionQueries(topicQueryBins);

            // Run the partition queries
            for (String query: partitionQueries) {
                final List<NewRelicQueryResult> queryResults;

                try {
                    queryResults = _newRelicAdapter.runQuery(query);
                } catch (IOException e) {
                    // Note that we could throw an exception here, but we don't want
                    // to stop trying all future queries because this one query
                    // failed to run
                    LOGGER.error("Error when attempting to query NRQL for metrics.", e);
                    continue;
                }

                for (NewRelicQueryResult result : queryResults) {
                    try {
                        counts.addMetricsAdded(addPartitionMetrics(result));
                    } catch (InvalidNewRelicResultException e) {
                        // Unlike PrometheusMetricSampler, this form of exception is probably very unlikely since
                        // we will be getting cleaned up and well formed data directly from NRDB, but just keeping
                        // this check here anyway to be safe
                        LOGGER.trace("Invalid query result received from New Relic for partition query {}", query, e);
                        counts.addResultsSkipped(1);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error when converting topics to bins.", e);
        }
    }

    /**
     * Used to keep track of metrics added and results skipped
     * throughout broker, topic, and partition queries.
     */
    private static class ResultCounts {
        private int _metricsAdded;
        private int _resultsSkipped;

        private void addMetricsAdded(int addedMetrics) {
            _metricsAdded += addedMetrics;
        }

        private void addResultsSkipped(int resultsSkipped) {
            _resultsSkipped += resultsSkipped;
        }

        private int getMetricsAdded() {
            return _metricsAdded;
        }

        private int getResultsSkipped() {
            return _resultsSkipped;
        }
    }

    /**
     * Goes through each broker in the cluster and
     * gets the number of topics in that broker sorted from least to greatest.
     * Note that if a topic has a replica in another broker,
     * but no leader, it will not count for that topic being in the other broker.
     * We do make the assumption that no one broker will have more than MAX_SIZE
     * topics in it.
     * @param cluster - Cluster object containing information metadata this cluster.
     * @return - Returns a sorted by count list of KafkaSize objects which store the count of
     * topics in each broker.
     */
    private ArrayList<KafkaSize> getSortedBrokersByTopicCount(Cluster cluster) {
        ArrayList<KafkaSize> brokerSizes = new ArrayList<>();

        for (Node node: cluster.nodes()) {
            HashSet<String> topicsInNode = new HashSet<>();
            List<PartitionInfo> partitions = cluster.partitionsForNode(node.id());
            for (PartitionInfo partition: partitions) {
                topicsInNode.add(partition.topic());
            }
            brokerSizes.add(new BrokerTopicCount(topicsInNode.size(), node.id()));
        }

        Collections.sort(brokerSizes);

        return brokerSizes;
    }

    /**
     * We go through each topic in the cluster and get the replica count that that
     * topic has in sorted order from least to greatest.
     * Note that if a topic has more replicas than MAX_SIZE, we split that topic
     * into its broker, topic combination and get the replica count for that
     * grouped object because otherwise we will not be able to query for that.
     * @param cluster - Cluster object containing information metadata this cluster.
     * @return - Returns a sorted by count list of KafkaSize objects which store the count of
     * replicas in each topic (or potentially topic/broker combination).
     */
    private ArrayList<KafkaSize> getSortedTopicsByReplicaCount(Cluster cluster) {
        Set<String> topics = cluster.topics();

        // Get the total number of leaders + replicas that are for this topic
        // Note that each leader and replica is counted as separately
        // since they are on different brokers and will require a different output from NRQL
        ArrayList<KafkaSize> topicSizes = new ArrayList<>();
        for (String topic: topics) {
            int size = 0;
            for (PartitionInfo partitionInfo: cluster.partitionsForTopic(topic)) {
                size += partitionInfo.replicas().length;
            }

            // If topic has more than 2000 replicas, go through each broker and get
            // the count of replicas in that broker for this topic and create
            // a new topicSize for each broker, topic combination
            if (size >= MAX_SIZE) {
                HashMap<Integer, Integer> brokerToCount = new HashMap<>();
                for (Node node: cluster.nodes()) {
                    brokerToCount.put(node.id(), 0);
                }
                for (PartitionInfo partitionInfo: cluster.partitionsForTopic(topic)) {
                    for (Node broker: partitionInfo.replicas()) {
                        brokerToCount.put(broker.id(), 1 + brokerToCount.get(broker.id()));
                    }
                }
                for (Map.Entry<Integer, Integer> entry: brokerToCount.entrySet()) {
                    if (entry.getValue() != 0) {
                        topicSizes.add(new TopicReplicaCount(topic, entry.getValue(), entry.getKey()));
                    }
                }
            } else {
                topicSizes.add(new TopicReplicaCount(topic, size));
            }
        }

        Collections.sort(topicSizes);

        return topicSizes;
    }

    /**
     * Our problem here is the bin packing problem (see https://en.wikipedia.org/wiki/Bin_packing_problem)
     * and the algorithm that we used to solve this problem is the First Fit Decreasing (FFD) algorithm
     * (see https://www.ics.uci.edu/~goodrich/teach/cs165/notes/BinPacking.pdf).
     * Using the first fit decreasing algorithm, we assign the different KafkaSize objects to bins
     * and return the final bin arrangement we were able to find.
     * @param kafkaSizes - List of KafkaSize which we want to arrange into different bins.
     * @param binType - The type of bin we want to put our size objects into.
     * @return - The bin arrangements which we found using the FFD algorithm. Note that this
     * solution may not be optimal, but finding the optimal solution to this problem is NP-Complete.
     * @throws InstantiationException - If the binType we passed in could not be successfully
     * initialized we will throw this exception.
     * @throws IllegalAccessException - If we don't have access to the binType we want to use,
     * we will throw this exception.
     */
    private List<NewRelicQueryBin> assignToBins(List<KafkaSize> kafkaSizes, Class<?> binType)
            throws InstantiationException, IllegalAccessException {
        List<NewRelicQueryBin> queryBins = new ArrayList<>();

        // Since topicSizes is ordered in ascending order, we traverse it backwards
        for (int i = kafkaSizes.size() - 1; i >= 0; i--) {
            KafkaSize kafkaSize = kafkaSizes.get(i);
            boolean added = false;
            for (NewRelicQueryBin queryBin: queryBins) {
                if (queryBin.addKafkaSize(kafkaSize)) {
                    added = true;
                    break;
                }
            }

            // If we couldn't add the topic to any of the previous bins,
            // create a new bin and add the topic to that bin
            if (!added) {
                NewRelicQueryBin newBin = (NewRelicQueryBin) binType.newInstance();
                boolean addedToNewBin = newBin.addKafkaSize(kafkaSize);
                if (!addedToNewBin) {
                    LOGGER.error("Size object has too many items: {}",
                            kafkaSize);
                } else {
                    queryBins.add(newBin);
                }
            }
        }

        return queryBins;
    }

    /**
     * Given the set of bins that we found, we generate the queries we can use
     * to query NRQL for the topic level stats that we want. There will be one
     * query for each of the queryBins which we pass in to this object.
     * @param queryBins - Bins that we are converting into queries.
     * @return - List of strings to run as queries to NRQL which will output
     * the data we want on a topic level.
     */
    private List<String> getTopicQueries(List<NewRelicQueryBin> queryBins) {
        List<String> queries = new ArrayList<>();
        // When every broker is in one bin, we don't need to include the "WHERE broker IN"
        // and can just select every broker
        if (queryBins.size() == 1) {
            queries.add(NewRelicQuerySupplier.topicQuery("", CLUSTER_NAME));
        } else {
            for (NewRelicQueryBin queryBin : queryBins) {
                queries.add(NewRelicQuerySupplier.topicQuery(queryBin.generateStringForQuery(), CLUSTER_NAME));
            }
        }
        return queries;
    }

    /**
     * Given the set of bins that we found, we generate the queries we can use
     * to query NRQL for the partition level stats that we want. There will be one
     * query for each of the queryBins which we pass in to this object.
     * @param queryBins - Bins that we are converting into queries.
     * @return - List of strings to run as queries to NRQL which will output
     * the data we want on a partition level.
     */
    private List<String> getPartitionQueries(List<NewRelicQueryBin> queryBins) {
        List<String> queries = new ArrayList<>();
        for (NewRelicQueryBin queryBin: queryBins) {
            queries.add(NewRelicQuerySupplier.partitionQuery(queryBin.generateStringForQuery(), CLUSTER_NAME));
        }
        return queries;
    }

    /**
     * Adds all the queryResults which were passed in to be processed
     * as BrokerMetrics.
     * @param queryResult - NRQL query result which should be from a broker
     *                    level query.
     * @return - Number of metrics we were able to add successfully from this
     * query result.
     * @throws InvalidNewRelicResultException - If the metric
     * is of invalid type, we will throw this exception.
     */
    private int addBrokerMetrics(NewRelicQueryResult queryResult)
            throws InvalidNewRelicResultException {
        int brokerID = queryResult.getBrokerID();
        long timeMs = queryResult.getTimeMs();

        int metricsAdded = 0;
        for (Map.Entry<RawMetricType, Double> entry: queryResult.getResults().entrySet()) {
            addMetricForProcessing(new BrokerMetric(entry.getKey(), timeMs,
                    brokerID, entry.getValue()));
            metricsAdded++;
        }
        return metricsAdded;
    }

    /**
     * Adds all the queryResults which were passed in to be processed
     * as TopicMetrics.
     * @param queryResult - NRQL query result which should be from a topic
     *                    level query.
     * @return - Number of metrics we were able to add successfully from this
     * query result.
     * @throws InvalidNewRelicResultException - If the metric
     * is of invalid type, we will throw this exception.
     */
    private int addTopicMetrics(NewRelicQueryResult queryResult)
            throws InvalidNewRelicResultException {
        int brokerID = queryResult.getBrokerID();
        String topic = queryResult.getTopic();
        long timeMs = queryResult.getTimeMs();

        int metricsAdded = 0;
        for (Map.Entry<RawMetricType, Double> entry: queryResult.getResults().entrySet()) {
            addMetricForProcessing(new TopicMetric(entry.getKey(), timeMs,
                    brokerID, topic, entry.getValue()));
            metricsAdded++;
        }
        return metricsAdded;
    }

    /**
     * Adds all the queryResults which were passed in to be processed
     * as PartitionMetrics.
     * @param queryResult - NRQL query result which should be from a partition
     *                    level query.
     * @return - Number of metrics we were able to add successfully from this
     * query result.
     * @throws InvalidNewRelicResultException - If the metric
     * is of invalid type, we will throw this exception.
     */
    private int addPartitionMetrics(NewRelicQueryResult queryResult)
            throws InvalidNewRelicResultException {
        int brokerID = queryResult.getBrokerID();
        String topic = queryResult.getTopic();
        int partition = queryResult.getPartition();
        long timeMs = queryResult.getTimeMs();

        int metricsAdded = 0;
        for (Map.Entry<RawMetricType, Double> entry: queryResult.getResults().entrySet()) {
            addMetricForProcessing(new PartitionMetric(entry.getKey(), timeMs,
                    brokerID, topic, partition, entry.getValue()));
            metricsAdded++;
        }
        return metricsAdded;
    }

    @Override
    public void close() throws Exception {
        _httpClient.close();
    }
}
