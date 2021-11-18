/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Queries the Kafka cluster directly to ascertain that there are no under-replicated partitions or non-preferred
 * leaders. We do this because Cruise Control's cluster model may be a bit behind; we'd like to use information that is
 * as up-to-date as possible for this purpose.
 */
public class CoastGuard {
    private static final Logger LOG = LoggerFactory.getLogger(CoastGuard.class);

    static final String IS_ENABLED_CONFIG = "coast.guard.enabled";
    static final String COAST_CLEAR_MIN_DURATION_MS_CONFIG = "coast.guard.coast.clear.min.duration.ms";
    static final String COAST_CLEAR_MAX_WAIT_DURATION_MS_CONFIG = "coast.guard.coast.clear.max.wait.duration.ms";
    static final String COAST_CLEAR_CHECK_WAIT_DURATION_MS_CONFIG = "coast.guard.coast.clear.check.wait.duration.ms";

    // 1 minute
    private static final String COAST_CLEAR_MIN_DURATION_MS_DEFAULT = "60000";
    // 1 hour
    private static final String COAST_CLEAR_MAX_WAIT_DURATION_MS_DEFAULT = "3600000";
    // 1 second
    private static final String COAST_CLEAR_CHECK_WAIT_DURATION_MS_DEFAULT = "1000";

    private final AdminClient _adminClient;
    private final Time _time;
    private final ExecutorService _waitExecutor;

    private final boolean _isEnabled;
    private final long _coastClearMinDurationMs;
    private final long _coastClearMaxWaitDurationMs;
    private final long _coastClearCheckWaitDurationMs;

    public CoastGuard(AdminClient adminClient, Time time, KafkaCruiseControlConfig config) {
        _adminClient = adminClient;
        _time = time;
        _waitExecutor = Executors.newSingleThreadExecutor(new KafkaCruiseControlThreadFactory("CoastGuard", false, LOG));

        Map<String, Object> mergedConfig = config.mergedConfigValues();
        _isEnabled = "true".equals(mergedConfig.get(IS_ENABLED_CONFIG));
        _coastClearMinDurationMs = Long.parseLong((String) mergedConfig.getOrDefault(
                COAST_CLEAR_MIN_DURATION_MS_CONFIG, COAST_CLEAR_MIN_DURATION_MS_DEFAULT));
        _coastClearMaxWaitDurationMs = Long.parseLong((String) mergedConfig.getOrDefault(
                COAST_CLEAR_MAX_WAIT_DURATION_MS_CONFIG, COAST_CLEAR_MAX_WAIT_DURATION_MS_DEFAULT));
        _coastClearCheckWaitDurationMs = Long.parseLong((String) mergedConfig.getOrDefault(
                COAST_CLEAR_CHECK_WAIT_DURATION_MS_CONFIG, COAST_CLEAR_CHECK_WAIT_DURATION_MS_DEFAULT));
    }

    /**
     * If there are no under-replicated partitions or non-preferred leaders this method will return after
     * {@code COAST_CLEAR_MIN_DURATION_MS_CONFIG} milliseconds. This method will throw a {@link TimeoutException} if
     * after {@code COAST_CLEAR_MAX_WAIT_DURATION_MS_CONFIG} milliseconds there hasn't been
     * {@code COAST_CLEAR_MIN_DURATION_MS_CONFIG} milliseconds of fully-replicated partitions and fully-preferred
     * leaders.
     */
    public void waitForCoastClear() throws ExecutionException, InterruptedException, TimeoutException {
        if (_isEnabled) {
            _waitExecutor
                    .submit(new WaitForCoastClear())
                    .get(_coastClearMaxWaitDurationMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * @return True if there are no under-replicated partitions or non-preferred leaders, false otherwise.
     */
    public boolean isCoastClear() throws ExecutionException, InterruptedException {
        ListTopicsResult listTopicsResult = _adminClient.listTopics();
        Set<String> topicNames = listTopicsResult.names().get();

        DescribeTopicsResult describeTopicsResult = _adminClient.describeTopics(topicNames);
        Map<String, TopicDescription> topicDescriptions = describeTopicsResult.all().get();

        for (TopicDescription topicDescription : topicDescriptions.values()) {
            for (TopicPartitionInfo topicPartitionInfo : topicDescription.partitions()) {
                if (topicPartitionInfo.isr().size() < topicPartitionInfo.replicas().size()) {
                    LOG.debug("Under-replicated partition of topic {} detected: {}",
                            topicDescription.name(), topicPartitionInfo);
                    return false;
                }
                if (topicPartitionInfo.leader() != topicPartitionInfo.replicas().get(0)) {
                    LOG.debug("Non-preferred leader detected for topic {}: {}",
                            topicDescription.name(), topicPartitionInfo);
                    return false;
                }
            }
        }

        return true;
    }

    private class WaitForCoastClear implements Runnable {
        @Override
        public void run() {
            LOG.info("Waiting for {} ms to establish that the coast is clear", _coastClearMinDurationMs);
            long horizon = setNewHorizon();

            while (_time.milliseconds() < horizon) {
                try {
                    if (!isCoastClear()) {
                        LOG.warn("Coast is not clear; resetting horizon");
                        horizon = setNewHorizon();
                    }

                    //noinspection BusyWait
                    Thread.sleep(_coastClearCheckWaitDurationMs);
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            }

            LOG.info("Coast appears clear");
        }

        private long setNewHorizon() {
            return _time.milliseconds() + _coastClearMinDurationMs;
        }
    }
}
