/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic;

import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSamplerOptions;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model.NewRelicQueryResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.error.MissingEnvironmentVariableException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.MetricScope.BROKER;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.NewRelicMetricSampler.*;
import static org.easymock.EasyMock.*;

/**
 * Unit tests for NewRelicMetricSampler.
 * We use this class to primarily check that the queries are formatted properly.
 * NewRelicAdapterTest tests whether or not we properly handle those queries.
 */
public class NewRelicMetricSamplerTest {
    private static final long START_EPOCH_SECONDS = 1603301400L;
    private static final long START_TIME_MS = TimeUnit.SECONDS.toMillis(START_EPOCH_SECONDS);
    private static final long END_TIME_MS = START_TIME_MS + TimeUnit.SECONDS.toMillis(59);

    private static final int TOTAL_BROKERS = 3;

    private static final String TEST_TOPIC1 = "test-topic1";
    private static final String TEST_TOPIC2 = "test-topic2";
    private static final String TEST_TOPIC3 = "test-topic3";

    private NewRelicMetricSampler _newRelicMetricSampler;
    private NewRelicAdapter _newRelicAdapter;

    private static final String CLUSTER_NAME = "kafka-test-cluster";

    private final NewRelicQuerySupplier _querySupplier = new DefaultNewRelicQuerySupplier();

    /**
     * Set up mocks
     */
    @Before
    public void setUp() {
        _newRelicAdapter = mock(NewRelicAdapter.class);
        _newRelicMetricSampler = new NewRelicMetricSampler();
    }

    @Test(expected = ConfigException.class)
    public void testNoClusterNameProvided() throws Exception {
        Map<String, Object> config = new HashMap<>();
        addCapacityConfig(config);
        config.put(NEWRELIC_ENDPOINT_CONFIG, "https://staging-api.newrelic.com");
        config.put(NEWRELIC_ACCOUNT_ID_CONFIG, "1");
        config.put(NEWRELIC_QUERY_LIMIT_CONFIG, "10");
        config.put(NEWRELIC_API_KEY_ENVIRONMENT, "ABC");
        _newRelicMetricSampler.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void testNoEndpointProvided() throws Exception {
        Map<String, Object> config = new HashMap<>();
        addCapacityConfig(config);
        config.put(NEWRELIC_ACCOUNT_ID_CONFIG, "1");
        config.put(NEWRELIC_QUERY_LIMIT_CONFIG, "10");
        config.put(CLUSTER_NAME_CONFIG, CLUSTER_NAME);
        config.put(NEWRELIC_API_KEY_ENVIRONMENT, "ABC");
        _newRelicMetricSampler.configure(config);
    }

    @Test(expected = MissingEnvironmentVariableException.class)
    public void testNoAPIKeyProvided() throws Exception {
        Map<String, Object> config = new HashMap<>();
        addCapacityConfig(config);
        config.put(NEWRELIC_ENDPOINT_CONFIG, "https://staging-api.newrelic.com");
        config.put(NEWRELIC_ACCOUNT_ID_CONFIG, "1");
        config.put(NEWRELIC_QUERY_LIMIT_CONFIG, "10");
        config.put(CLUSTER_NAME_CONFIG, CLUSTER_NAME);
        _newRelicMetricSampler.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void testNoAccountIDProvided() throws Exception {
        Map<String, Object> config = new HashMap<>();
        addCapacityConfig(config);
        config.put(NEWRELIC_ENDPOINT_CONFIG, "https://staging-api.newrelic.com");
        config.put(NEWRELIC_QUERY_LIMIT_CONFIG, "10");
        config.put(NEWRELIC_API_KEY_ENVIRONMENT, "ABC");
        config.put(CLUSTER_NAME_CONFIG, CLUSTER_NAME);
        _newRelicMetricSampler.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void testNoQueryLimitProvided() throws Exception {
        Map<String, Object> config = new HashMap<>();
        addCapacityConfig(config);
        config.put(NEWRELIC_ENDPOINT_CONFIG, "https://staging-api.newrelic.com");
        config.put(NEWRELIC_ACCOUNT_ID_CONFIG, "1");
        config.put(CLUSTER_NAME_CONFIG, CLUSTER_NAME);
        config.put(NEWRELIC_API_KEY_ENVIRONMENT, "ABC");
        _newRelicMetricSampler.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void testInvalidNewRelicQuerySupplierClassName() throws Exception {
        Map<String, Object> config = new HashMap<>();
        setConfigs(config, "5");
        addCapacityConfig(config);
        config.put(NEWRELIC_QUERY_SUPPLIER_CONFIG, "InvalidClass");
        _newRelicMetricSampler.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void testInvalidNewRelicQuerySupplierClassType() throws Exception {
        Map<String, Object> config = new HashMap<>();
        setConfigs(config, "5");
        addCapacityConfig(config);
        config.put(NEWRELIC_QUERY_SUPPLIER_CONFIG, String.class.getName());
        _newRelicMetricSampler.configure(config);
    }

    @Test
    public void testGetSamplesSuccess() throws Exception {
        Map<String, Object> config = new HashMap<>();
        setConfigs(config, "15");
        addCapacityConfig(config);

        ArrayList<String> topics = new ArrayList<>();
        topics.add(TEST_TOPIC1);
        topics.add(TEST_TOPIC2);
        topics.add(TEST_TOPIC3);

        ArrayList<Integer> partitions = new ArrayList<>();
        partitions.add(3);
        partitions.add(5);
        partitions.add(1);

        setUp();
        MetricSamplerOptions metricSamplerOptions = buildMetricSamplerOptions(TOTAL_BROKERS, topics, partitions);
        _newRelicMetricSampler.configure(config);
        _newRelicMetricSampler._newRelicAdapter = _newRelicAdapter;

        setupNewRelicAdapterMock(buildBrokerResults(1),
                buildTopicResults(1, new ArrayList<>()),
                buildPartitionResults(TOTAL_BROKERS, new ArrayList<>(), new ArrayList<>()));

        replay(_newRelicAdapter);
        _newRelicMetricSampler.getSamples(metricSamplerOptions);
        verify(_newRelicAdapter);
    }

    @Test
    public void testTooManyReplicas() throws Exception {
        Map<String, Object> config = new HashMap<>();
        // MAX_SIZE = 2
        setConfigs(config, "3");
        addCapacityConfig(config);

        ArrayList<String> topics = new ArrayList<>();
        // Topic is in three brokers -> should be 2 separate queries
        topics.add(TEST_TOPIC1);
        topics.add(TEST_TOPIC2);

        ArrayList<Integer> partitions = new ArrayList<>();
        // 4 replicas total -> should be in two separate queries
        partitions.add(2);
        partitions.add(2);

        setUp();
        MetricSamplerOptions metricSamplerOptions = buildMetricSamplerOptions(TOTAL_BROKERS, topics, partitions);
        _newRelicMetricSampler.configure(config);
        _newRelicMetricSampler._newRelicAdapter = _newRelicAdapter;

        setupNewRelicAdapterMock(buildBrokerResults(1),
                buildTopicResults(1, new ArrayList<>()),
                buildPartitionResults(TOTAL_BROKERS, new ArrayList<>(), new ArrayList<>()));

        replay(_newRelicAdapter);
        _newRelicMetricSampler.getSamples(metricSamplerOptions);
        verify(_newRelicAdapter);
    }

    @Test
    public void testRandomInputs() throws Exception {
        // Generate random value for number of brokers, topics, max partitions per topic, and max size
        Random rand = new Random();

        int maxSize = 4000;
        int numBrokers = rand.nextInt(50);
        int numTopics = rand.nextInt(100);

        ArrayList<String> topics = new ArrayList<>();
        ArrayList<Integer> partitions = new ArrayList<>();
        for (int i = 0; i < numTopics; i++) {
            topics.add(String.format("topic%s", i));
            partitions.add(rand.nextInt(50));
        }

        Map<String, Object> config = new HashMap<>();
        setConfigs(config, String.valueOf(maxSize));
        addCapacityConfig(config);

        setUp();
        MetricSamplerOptions metricSamplerOptions = buildMetricSamplerOptions(numBrokers, topics, partitions);

        _newRelicMetricSampler.configure(config);
        _newRelicMetricSampler._newRelicAdapter = _newRelicAdapter;

        setupNewRelicAdapterMock(buildBrokerResults(1),
                buildTopicResults(1, new ArrayList<>()),
                buildPartitionResults(TOTAL_BROKERS, new ArrayList<>(), new ArrayList<>()));

        replay(_newRelicAdapter);
        _newRelicMetricSampler.getSamples(metricSamplerOptions);
        verify(_newRelicAdapter);
    }

    @Test
    public void testTooLargeQueryOutput() throws Exception {
        Map<String, Object> config = new HashMap<>();
        // MAX_SIZE = 2
        setConfigs(config, "3");
        addCapacityConfig(config);

        ArrayList<String> topics = new ArrayList<>();
        // Topic is in three brokers -> should be 2 separate queries
        topics.add(TEST_TOPIC1);
        topics.add(TEST_TOPIC2);

        ArrayList<Integer> partitions = new ArrayList<>();
        // 4 replicas total -> should be in two separate queries
        partitions.add(2);
        partitions.add(2);

        setUp();
        MetricSamplerOptions metricSamplerOptions = buildMetricSamplerOptions(TOTAL_BROKERS, topics, partitions);
        _newRelicMetricSampler.configure(config);
        _newRelicMetricSampler._newRelicAdapter = _newRelicAdapter;

        setupNewRelicIllegalStateMock(buildBrokerResults(1),
                buildTopicResults(TOTAL_BROKERS, topics));

        replay(_newRelicAdapter);
        _newRelicMetricSampler.getSamples(metricSamplerOptions);
        verify(_newRelicAdapter);
    }

    @Test
    public void testValidNewRelicQuerySupplierGet() throws Exception {
        Map<String, Object> config = new HashMap<>();
        setConfigs(config, "15");
        addCapacityConfig(config);
        config.put(NEWRELIC_QUERY_SUPPLIER_CONFIG, TestQuerySupplier.class.getName());
        _newRelicMetricSampler.configure(config);
        _newRelicMetricSampler._newRelicAdapter = _newRelicAdapter;

        ArrayList<String> topics = new ArrayList<>();
        topics.add(TEST_TOPIC1);
        topics.add(TEST_TOPIC2);
        topics.add(TEST_TOPIC3);

        ArrayList<Integer> partitions = new ArrayList<>();
        partitions.add(3);
        partitions.add(5);
        partitions.add(1);

        setUp();

        MetricSamplerOptions metricSamplerOptions = buildMetricSamplerOptions(TOTAL_BROKERS, topics, partitions);
        _newRelicMetricSampler.configure(config);
        _newRelicMetricSampler._newRelicAdapter = _newRelicAdapter;

        setupTestAdapterMock();

        replay(_newRelicAdapter);
        _newRelicMetricSampler.getSamples(metricSamplerOptions);
        verify(_newRelicAdapter);
    }

    private static MetricSamplerOptions buildMetricSamplerOptions(int numBrokers,
                                                                  ArrayList<String> topics,
                                                                  ArrayList<Integer> partitions) {

        return new MetricSamplerOptions(
                generateCluster(numBrokers, topics, partitions),
                generatePartitions(topics, partitions),
                START_TIME_MS,
                END_TIME_MS,
                MetricSampler.SamplingMode.ALL,
                KafkaMetricDef.commonMetricDef(),
                60000
        );
    }

    private void setConfigs(Map<String, Object> config, String queryLimit) {
        config.put(NEWRELIC_ENDPOINT_CONFIG, "https://staging-api.newrelic.com");
        config.put(NEWRELIC_ACCOUNT_ID_CONFIG, "1");
        config.put(NEWRELIC_QUERY_LIMIT_CONFIG, queryLimit);
        config.put(CLUSTER_NAME_CONFIG, CLUSTER_NAME);
        config.put(NEWRELIC_API_KEY_ENVIRONMENT, "ABC");
    }

    private void setupNewRelicAdapterMock(List<NewRelicQueryResult> brokerResults,
                                          List<NewRelicQueryResult> topicResults,
                                          List<NewRelicQueryResult> partitionResults) throws Exception {
            expect(_newRelicAdapter.runQuery(eq(_querySupplier.brokerQuery(CLUSTER_NAME))))
                    .andReturn(brokerResults);

            String beforeTopicRegex = String.format("FROM KafkaBrokerTopicStats SELECT max\\(messagesInPerSec\\), "
                    + "max\\(bytesInPerSec\\), max\\(bytesOutPerSec\\), max\\(totalProduceRequestsPerSec\\), "
                    + "max\\(totalFetchRequestsPerSec\\) WHERE cluster = '%s'", CLUSTER_NAME);
            String afterTopicRegex = "AND topic is NOT NULL FACET broker, topic SINCE 1 minute ago LIMIT MAX";
            String topicRegex = "(( WHERE broker IN \\((\\d+, )*\\d+\\) )|\\s)";
            // the middle can be either " " or "WHERE broker IN (1, 2, ... 0)
            String topicMatcher = beforeTopicRegex + topicRegex + afterTopicRegex;
            expect(_newRelicAdapter.runQuery(matches(topicMatcher)))
                    .andReturn(topicResults).atLeastOnce();

            String beforePartitionRegex = String.format("FROM KafkaPartitionSizeStats SELECT max\\(partitionSize\\) "
                    + "WHERE cluster = '%s' WHERE ", CLUSTER_NAME);
            String afterPartitionRegex = "FACET broker, topic, partition SINCE 1 minute ago LIMIT MAX";
            // regex portion can be 1. "topic IN ('topic1', 'topic2', ... 'topicN') "
            //                      2. "topic IN ('topic1', 'topic2', ... 'topicN')
            //                              OR (topic = 'brokerTopic1' AND broker = 1)...
            //                              OR (topic = 'brokerTopicN' AND broker = N) "
            //                      3. "(topic = 'brokerTopic1' AND broker = 1) OR ...
            //                              (topic = 'brokerTopicN' AND broker = N) "
            String partitionRegex = "((topic IN \\(('[\\w\\-]*', )*'[\\w\\-]*'\\) )"
                    + "|(topic IN \\(('[\\w\\-]*', )*'[\\w\\-]*'\\) (OR \\(topic = '[\\w\\-]+' "
                    + "AND broker = \\d+\\) )+)|((\\(topic = '[\\w\\-]+' AND broker = \\d+\\) "
                    + "OR )*\\(topic = '[\\w\\-]+' AND broker = \\d+\\) ))";
            String partitionMatcher = beforePartitionRegex + partitionRegex + afterPartitionRegex;
            expect(_newRelicAdapter.runQuery(matches(partitionMatcher)))
                    .andReturn(partitionResults).atLeastOnce();
    }

    private void setupNewRelicIllegalStateMock(List<NewRelicQueryResult> brokerResults,
                                                   List<NewRelicQueryResult> topicResults) throws Exception {
        expect(_newRelicAdapter.runQuery(eq(_querySupplier.brokerQuery(CLUSTER_NAME))))
                .andReturn(brokerResults);

        String beforeTopicRegex = String.format("FROM KafkaBrokerTopicStats SELECT max\\(messagesInPerSec\\), "
                + "max\\(bytesInPerSec\\), max\\(bytesOutPerSec\\), max\\(totalProduceRequestsPerSec\\), "
                + "max\\(totalFetchRequestsPerSec\\) WHERE cluster = '%s'", CLUSTER_NAME);
        String afterTopicRegex = "AND topic is NOT NULL FACET broker, topic SINCE 1 minute ago LIMIT MAX";
        String topicRegex = "(( WHERE broker IN \\((\\d+, )*\\d+\\) )|\\s)";
        // the middle can be either " " or "WHERE broker IN (1, 2, ... 0)
        String topicMatcher = beforeTopicRegex + topicRegex + afterTopicRegex;

        // Return too many results the first time
        expect(_newRelicAdapter.runQuery(matches(topicMatcher)))
                .andReturn(topicResults);
    }

    private void setupTestAdapterMock() throws Exception {
        expect(_newRelicAdapter.runQuery(eq(TestQuerySupplier.TEST_BROKER_QUERY)))
                .andReturn(new ArrayList<>());

        expect(_newRelicAdapter.runQuery(eq(TestQuerySupplier.TEST_TOPIC_QUERY)))
                .andReturn(new ArrayList<>())
                .atLeastOnce();

        expect(_newRelicAdapter.runQuery(eq(TestQuerySupplier.TEST_PARTITION_QUERY)))
                .andReturn(new ArrayList<>())
                .atLeastOnce();
    }

    private static List<NewRelicQueryResult> buildBrokerResults(int numBrokers) {
        List<NewRelicQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
            Map<RawMetricType, Double> results = new HashMap<>();
            for (RawMetricType type: RawMetricType.allMetricTypes()) {
                if (type.metricScope() == BROKER) {
                    results.put(type, Math.random());
                }
            }
            resultList.add(new NewRelicQueryResult(brokerId, results));
        }
        return resultList;
    }

    private static List<NewRelicQueryResult> buildTopicResults(int numBrokers, ArrayList<String> topics) {
        List<NewRelicQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
            for (String topic: topics) {
                Map<RawMetricType, Double> results = new HashMap<>();
                for (RawMetricType type: RawMetricType.topicMetricTypes()) {
                    results.put(type, Math.random());
                }
                resultList.add(new NewRelicQueryResult(brokerId, topic, results));
            }
        }
        return resultList;
    }

    private static List<NewRelicQueryResult> buildPartitionResults(int numBrokers, ArrayList<String> topics,
                                                                   ArrayList<Integer> partitions) {
        List<NewRelicQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
            for (int i = 0; i < topics.size(); i++) {
                for (int partition = 0; partition < partitions.get(i); partition++) {
                    Map<RawMetricType, Double> results = new HashMap<>();
                    for (RawMetricType type : RawMetricType.partitionMetricTypes()) {
                        results.put(type, Math.random());
                    }
                    resultList.add(new NewRelicQueryResult(brokerId, topics.get(i), partition, results));
                }
            }
        }
        return resultList;
    }

    private void addCapacityConfig(Map<String, Object> config) throws IOException {
        File capacityConfigFile = File.createTempFile("capacityConfig", "json");
        FileOutputStream fileOutputStream = new FileOutputStream(capacityConfigFile);
        try (OutputStreamWriter writer = new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8)) {
            writer.write("{\n"
                    + "  \"brokerCapacities\":[\n"
                    + "    {\n"
                    + "      \"brokerId\": \"-1\",\n"
                    + "      \"capacity\": {\n"
                    + "        \"DISK\": \"100000\",\n"
                    + "        \"CPU\": {\"num.cores\": \"4\"},\n"
                    + "        \"NW_IN\": \"5000000\",\n"
                    + "        \"NW_OUT\": \"5000000\"\n"
                    + "      }\n"
                    + "    }\n"
                    + "  ]\n"
                    + "}\n");
        }
        config.put("capacity.config.file", capacityConfigFile.getAbsolutePath());
        BrokerCapacityConfigResolver brokerCapacityConfigResolver = new BrokerCapacityConfigFileResolver();
        config.put("broker.capacity.config.resolver.object", brokerCapacityConfigResolver);
        config.put("sampling.allow.cpu.capacity.estimation", true);
        brokerCapacityConfigResolver.configure(config);
    }

    private static Set<TopicPartition> generatePartitions(ArrayList<String> topics, ArrayList<Integer> partitions) {
        Set<TopicPartition> set = new HashSet<>();
        for (int i = 0; i < topics.size(); i++) {
            for (int partition = 0; partition < partitions.get(i); partition++) {
                TopicPartition topicPartition = new TopicPartition(topics.get(i), partition);
                set.add(topicPartition);
            }
        }
        return set;
    }

    private static Cluster generateCluster(int numBrokers, ArrayList<String> topics, ArrayList<Integer> partitions) {
        Node[] allNodes = new Node[numBrokers];
        Set<PartitionInfo> partitionInfo = new HashSet<>(numBrokers);
        for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
            allNodes[brokerId] = new Node(brokerId, "broker-" + brokerId + ".test-cluster.org", 9092);
        }
        for (int i = 0; i < topics.size(); i++) {
            for (int partitionId = 0; partitionId < partitions.get(i); partitionId++) {
                partitionInfo.add(new PartitionInfo(topics.get(i), partitionId,
                        allNodes[(partitionId + i) % numBrokers], allNodes, allNodes));
            }
        }
        return new Cluster("cluster_id", Arrays.asList(allNodes),
                partitionInfo, Collections.emptySet(), Collections.emptySet());
    }

    public static class TestQuerySupplier implements NewRelicQuerySupplier {

        public static final String TEST_BROKER_QUERY = "test_broker_query";

        public static final String TEST_TOPIC_QUERY = "test_topic_query";

        public static final String TEST_PARTITION_QUERY = "test_partition_query";

        @Override
        public String brokerQuery(String clusterName) {
            return TEST_BROKER_QUERY;
        }

        @Override
        public String topicQuery(String brokerSelect, String clusterName) {
            return TEST_TOPIC_QUERY;
        }

        @Override
        public String partitionQuery(String whereClause, String clusterName) {
            return TEST_PARTITION_QUERY;
        }

        @Override
        public Map<String, RawMetricType> getUnmodifiableBrokerMap() {
            return new HashMap<>();
        }

        @Override
        public Map<String, RawMetricType> getUnmodifiableTopicMap() {
            return new HashMap<>();
        }

        @Override
        public Map<String, RawMetricType> getUnmodifiablePartitionMap() {
            return new HashMap<>();
        }
    }
}
