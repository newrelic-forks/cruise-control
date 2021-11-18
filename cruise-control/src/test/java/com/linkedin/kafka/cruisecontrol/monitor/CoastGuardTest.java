/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static com.linkedin.kafka.cruisecontrol.monitor.CoastGuard.COAST_CLEAR_CHECK_WAIT_DURATION_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.monitor.CoastGuard.COAST_CLEAR_MAX_WAIT_DURATION_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.monitor.CoastGuard.COAST_CLEAR_MIN_DURATION_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.monitor.CoastGuard.IS_ENABLED_CONFIG;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CoastGuardTest {
    private static final Node NODE_1 = new Node(1, "host-1", 9092);
    private static final Node NODE_2 = new Node(2, "host-2", 9092);
    private static final Node NODE_3 = new Node(3, "host-3", 9092);

    @Test
    public void testCoastIsNotClearUnderReplicatedPartitions() throws ExecutionException, InterruptedException {
        @SuppressWarnings("rawtypes")
        KafkaFuture listTopicsNamesFuture = mock(KafkaFuture.class);
        expect(listTopicsNamesFuture.get()).andReturn(Set.of("dummy_topic_1")).times(1);
        replay(listTopicsNamesFuture);

        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        //noinspection unchecked
        expect(listTopicsResult.names()).andReturn(listTopicsNamesFuture).times(1);
        replay(listTopicsResult);

        @SuppressWarnings("rawtypes")
        KafkaFuture describeTopicsAllFuture = mock(KafkaFuture.class);
        expect(describeTopicsAllFuture.get()).andReturn(Map.of(
                "dummy_topic_1", new TopicDescription("dummy_topic_1", false, List.of(
                        new TopicPartitionInfo(0, NODE_1, List.of(NODE_1, NODE_2, NODE_3), List.of(NODE_1, NODE_2, NODE_3)),
                        new TopicPartitionInfo(0, NODE_1, List.of(NODE_1, NODE_2, NODE_3), List.of(NODE_1, NODE_2))
                ))
        )).times(1);
        replay(describeTopicsAllFuture);

        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        //noinspection unchecked
        expect(describeTopicsResult.all()).andReturn(describeTopicsAllFuture).times(1);
        replay(describeTopicsResult);

        AdminClient adminClient = mock(AdminClient.class);
        expect(adminClient.listTopics()).andReturn(listTopicsResult).times(1);
        expect(adminClient.describeTopics(Set.of("dummy_topic_1"))).andReturn(describeTopicsResult).times(1);
        replay(adminClient);

        KafkaCruiseControlConfig config = mock(KafkaCruiseControlConfig.class);
        expect(config.mergedConfigValues()).andReturn(Map.of(
                IS_ENABLED_CONFIG, "true",
                COAST_CLEAR_MIN_DURATION_MS_CONFIG, "30000",
                COAST_CLEAR_MAX_WAIT_DURATION_MS_CONFIG, "120000",
                COAST_CLEAR_CHECK_WAIT_DURATION_MS_CONFIG, "500")).times(1);
        replay(config);

        CoastGuard coastGuard = new CoastGuard(adminClient, mock(Time.class), config);
        assertFalse(coastGuard.isCoastClear());
    }

    @Test
    public void testCoastIsNotClearNonPreferredLeaders() throws ExecutionException, InterruptedException {
        @SuppressWarnings("rawtypes")
        KafkaFuture listTopicsNamesFuture = mock(KafkaFuture.class);
        expect(listTopicsNamesFuture.get()).andReturn(Set.of("dummy_topic_1")).times(1);
        replay(listTopicsNamesFuture);

        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        //noinspection unchecked
        expect(listTopicsResult.names()).andReturn(listTopicsNamesFuture).times(1);
        replay(listTopicsResult);

        @SuppressWarnings("rawtypes")
        KafkaFuture describeTopicsAllFuture = mock(KafkaFuture.class);
        expect(describeTopicsAllFuture.get()).andReturn(Map.of(
                "dummy_topic_1", new TopicDescription("dummy_topic_1", false, List.of(
                        new TopicPartitionInfo(0, NODE_1, List.of(NODE_1, NODE_2, NODE_3), List.of(NODE_1, NODE_2, NODE_3)),
                        new TopicPartitionInfo(0, NODE_2, List.of(NODE_1, NODE_2, NODE_3), List.of(NODE_1, NODE_2, NODE_3))
                ))
        )).times(1);
        replay(describeTopicsAllFuture);

        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        //noinspection unchecked
        expect(describeTopicsResult.all()).andReturn(describeTopicsAllFuture).times(1);
        replay(describeTopicsResult);

        AdminClient adminClient = mock(AdminClient.class);
        expect(adminClient.listTopics()).andReturn(listTopicsResult).times(1);
        expect(adminClient.describeTopics(Set.of("dummy_topic_1"))).andReturn(describeTopicsResult).times(1);
        replay(adminClient);

        KafkaCruiseControlConfig config = mock(KafkaCruiseControlConfig.class);
        expect(config.mergedConfigValues()).andReturn(Map.of(
                IS_ENABLED_CONFIG, "true",
                COAST_CLEAR_MIN_DURATION_MS_CONFIG, "30000",
                COAST_CLEAR_MAX_WAIT_DURATION_MS_CONFIG, "120000",
                COAST_CLEAR_CHECK_WAIT_DURATION_MS_CONFIG, "500")).times(1);
        replay(config);

        CoastGuard coastGuard = new CoastGuard(adminClient, mock(Time.class), config);
        assertFalse(coastGuard.isCoastClear());
    }

    @Test
    public void testCoastIsClear() throws ExecutionException, InterruptedException {
        @SuppressWarnings("rawtypes")
        KafkaFuture listTopicsNamesFuture = mock(KafkaFuture.class);
        expect(listTopicsNamesFuture.get()).andReturn(Set.of("dummy_topic_1")).times(1);
        replay(listTopicsNamesFuture);

        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        //noinspection unchecked
        expect(listTopicsResult.names()).andReturn(listTopicsNamesFuture).times(1);
        replay(listTopicsResult);

        @SuppressWarnings("rawtypes")
        KafkaFuture describeTopicsAllFuture = mock(KafkaFuture.class);
        expect(describeTopicsAllFuture.get()).andReturn(Map.of(
                "dummy_topic_1", new TopicDescription("dummy_topic_1", false, List.of(
                        new TopicPartitionInfo(0, NODE_1, List.of(NODE_1, NODE_2, NODE_3), List.of(NODE_1, NODE_2, NODE_3)),
                        new TopicPartitionInfo(0, NODE_1, List.of(NODE_1, NODE_2, NODE_3), List.of(NODE_1, NODE_2, NODE_3))
                ))
        )).times(1);
        replay(describeTopicsAllFuture);

        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        //noinspection unchecked
        expect(describeTopicsResult.all()).andReturn(describeTopicsAllFuture).times(1);
        replay(describeTopicsResult);

        AdminClient adminClient = mock(AdminClient.class);
        expect(adminClient.listTopics()).andReturn(listTopicsResult).times(1);
        expect(adminClient.describeTopics(Set.of("dummy_topic_1"))).andReturn(describeTopicsResult).times(1);
        replay(adminClient);

        KafkaCruiseControlConfig config = mock(KafkaCruiseControlConfig.class);
        expect(config.mergedConfigValues()).andReturn(Map.of(
                IS_ENABLED_CONFIG, "true",
                COAST_CLEAR_MIN_DURATION_MS_CONFIG, "30000",
                COAST_CLEAR_MAX_WAIT_DURATION_MS_CONFIG, "120000",
                COAST_CLEAR_CHECK_WAIT_DURATION_MS_CONFIG, "500")).times(1);
        replay(config);

        CoastGuard coastGuard = new CoastGuard(adminClient, mock(Time.class), config);
        assertTrue(coastGuard.isCoastClear());
    }

    @Test
    public void testWaitForCoastClearTimesOut() throws ExecutionException, InterruptedException {
        @SuppressWarnings("rawtypes")
        KafkaFuture listTopicsNamesFuture = mock(KafkaFuture.class);
        expect(listTopicsNamesFuture.get()).andReturn(Set.of("dummy_topic_1")).anyTimes();
        replay(listTopicsNamesFuture);

        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        //noinspection unchecked
        expect(listTopicsResult.names()).andReturn(listTopicsNamesFuture).anyTimes();
        replay(listTopicsResult);

        @SuppressWarnings("rawtypes")
        KafkaFuture describeTopicsAllFuture = mock(KafkaFuture.class);
        expect(describeTopicsAllFuture.get()).andReturn(Map.of(
                "dummy_topic_1", new TopicDescription("dummy_topic_1", false, List.of(
                        new TopicPartitionInfo(0, NODE_1, List.of(NODE_1, NODE_2, NODE_3), List.of(NODE_1, NODE_2, NODE_3)),
                        new TopicPartitionInfo(0, NODE_1, List.of(NODE_1, NODE_2, NODE_3), List.of(NODE_1, NODE_2))
                ))
        )).anyTimes();
        replay(describeTopicsAllFuture);

        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        //noinspection unchecked
        expect(describeTopicsResult.all()).andReturn(describeTopicsAllFuture).anyTimes();
        replay(describeTopicsResult);

        AdminClient adminClient = mock(AdminClient.class);
        expect(adminClient.listTopics()).andReturn(listTopicsResult).anyTimes();
        expect(adminClient.describeTopics(Set.of("dummy_topic_1"))).andReturn(describeTopicsResult).anyTimes();
        replay(adminClient);

        KafkaCruiseControlConfig config = mock(KafkaCruiseControlConfig.class);
        expect(config.mergedConfigValues()).andReturn(Map.of(
                IS_ENABLED_CONFIG, "true",
                COAST_CLEAR_MIN_DURATION_MS_CONFIG, "30000",
                COAST_CLEAR_MAX_WAIT_DURATION_MS_CONFIG, "120000",
                COAST_CLEAR_CHECK_WAIT_DURATION_MS_CONFIG, "500")).times(1);
        replay(config);

        CoastGuard coastGuard = new CoastGuard(adminClient, new SystemTime(), config);

        boolean hitTimeout = false;
        try {
            coastGuard.waitForCoastClear();
        } catch (TimeoutException e) {
            hitTimeout = true;
        }
        assertTrue(hitTimeout);
    }

    @Test
    public void testNotEnabled() throws ExecutionException, InterruptedException, TimeoutException {
        @SuppressWarnings("rawtypes")
        KafkaFuture listTopicsNamesFuture = mock(KafkaFuture.class);
        expect(listTopicsNamesFuture.get()).andReturn(Set.of("dummy_topic_1")).anyTimes();
        replay(listTopicsNamesFuture);

        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        //noinspection unchecked
        expect(listTopicsResult.names()).andReturn(listTopicsNamesFuture).anyTimes();
        replay(listTopicsResult);

        @SuppressWarnings("rawtypes")
        KafkaFuture describeTopicsAllFuture = mock(KafkaFuture.class);
        expect(describeTopicsAllFuture.get()).andReturn(Map.of(
                "dummy_topic_1", new TopicDescription("dummy_topic_1", false, List.of(
                        new TopicPartitionInfo(0, NODE_1, List.of(NODE_1, NODE_2, NODE_3), List.of(NODE_1, NODE_2, NODE_3)),
                        new TopicPartitionInfo(0, NODE_1, List.of(NODE_1, NODE_2, NODE_3), List.of(NODE_1, NODE_2))
                ))
        )).anyTimes();
        replay(describeTopicsAllFuture);

        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        //noinspection unchecked
        expect(describeTopicsResult.all()).andReturn(describeTopicsAllFuture).anyTimes();
        replay(describeTopicsResult);

        AdminClient adminClient = mock(AdminClient.class);
        expect(adminClient.listTopics()).andReturn(listTopicsResult).anyTimes();
        expect(adminClient.describeTopics(Set.of("dummy_topic_1"))).andReturn(describeTopicsResult).anyTimes();
        replay(adminClient);

        KafkaCruiseControlConfig config = mock(KafkaCruiseControlConfig.class);
        expect(config.mergedConfigValues()).andReturn(Map.of(
                IS_ENABLED_CONFIG, "false",
                COAST_CLEAR_MIN_DURATION_MS_CONFIG, "30000",
                COAST_CLEAR_MAX_WAIT_DURATION_MS_CONFIG, "120000",
                COAST_CLEAR_CHECK_WAIT_DURATION_MS_CONFIG, "500")).times(1);
        replay(config);

        CoastGuard coastGuard = new CoastGuard(adminClient, new SystemTime(), config);
        coastGuard.waitForCoastClear();
    }

    @Test
    public void testWaitForCoastClear() throws ExecutionException, InterruptedException, TimeoutException {
        @SuppressWarnings("rawtypes")
        KafkaFuture listTopicsNamesFuture = mock(KafkaFuture.class);
        expect(listTopicsNamesFuture.get()).andReturn(Set.of("dummy_topic_1")).times(1);
        replay(listTopicsNamesFuture);

        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        //noinspection unchecked
        expect(listTopicsResult.names()).andReturn(listTopicsNamesFuture).times(1);
        replay(listTopicsResult);

        @SuppressWarnings("rawtypes")
        KafkaFuture describeTopicsAllFuture = mock(KafkaFuture.class);
        expect(describeTopicsAllFuture.get()).andReturn(Map.of(
                "dummy_topic_1", new TopicDescription("dummy_topic_1", false, List.of(
                        new TopicPartitionInfo(0, NODE_1, List.of(NODE_1, NODE_2, NODE_3), List.of(NODE_1, NODE_2, NODE_3)),
                        new TopicPartitionInfo(0, NODE_1, List.of(NODE_1, NODE_2, NODE_3), List.of(NODE_1, NODE_2, NODE_3))
                ))
        )).times(1);
        replay(describeTopicsAllFuture);

        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        //noinspection unchecked
        expect(describeTopicsResult.all()).andReturn(describeTopicsAllFuture).times(1);
        replay(describeTopicsResult);

        AdminClient adminClient = mock(AdminClient.class);
        expect(adminClient.listTopics()).andReturn(listTopicsResult).times(1);
        expect(adminClient.describeTopics(Set.of("dummy_topic_1"))).andReturn(describeTopicsResult).times(1);
        replay(adminClient);

        KafkaCruiseControlConfig config = mock(KafkaCruiseControlConfig.class);
        expect(config.mergedConfigValues()).andReturn(Map.of(
                IS_ENABLED_CONFIG, "true",
                COAST_CLEAR_MIN_DURATION_MS_CONFIG, "30000",
                COAST_CLEAR_MAX_WAIT_DURATION_MS_CONFIG, "120000",
                COAST_CLEAR_CHECK_WAIT_DURATION_MS_CONFIG, "500")).times(1);
        replay(config);

        Time time = new MockTime(List.of(1L, 29999L, 30001L));

        CoastGuard coastGuard = new CoastGuard(adminClient, time, config);
        coastGuard.waitForCoastClear();
    }

    private static final class MockTime implements Time {
        private final List<Long> _milliseconds;
        private int _index = 0;

        private MockTime(List<Long> milliseconds) {
            _milliseconds = milliseconds;
        }

        @Override
        public long milliseconds() {
            return _milliseconds.get(_index++);
        }

        @Override
        public long nanoseconds() {
            return 0;
        }

        @Override
        public void sleep(long ms) {
        }

        @Override
        public void waitObject(Object obj, Supplier<Boolean> condition, long deadlineMs) {
        }
    }
}
