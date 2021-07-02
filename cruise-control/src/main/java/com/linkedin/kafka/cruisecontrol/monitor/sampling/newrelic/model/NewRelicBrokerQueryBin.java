/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model;

import java.util.List;

/**
 * This bin is used to be able to properly format broker queries
 * using the contents inside of the bin. Note that
 * the KafkaSize objects stored in this bin should be
 * BrokerTopicCount objects.
 */
public class NewRelicBrokerQueryBin extends NewRelicQueryBin {
    public NewRelicBrokerQueryBin() {
        super();
    }

    @Override
    public String generateStringForQuery() {
        if (getSizes().size() == 0) {
            return "";
        } else {
            // We want this to be comma separated list of brokers
            // Example: "WHERE broker IN (broker1, broker2, ...) "
            StringBuffer brokerBuffer = new StringBuffer();
            brokerBuffer.append("WHERE broker IN (");

            List<KafkaSize> sizes = getSizes();
            for (int i = 0; i < sizes.size() - 1; i++) {
                BrokerTopicCount brokerTopicCount = (BrokerTopicCount) sizes.get(i);
                brokerBuffer.append(String.format("%s, ", brokerTopicCount.getBrokerId()));
            }

            // Handle the last broker and add in parentheses + space instead of comma to finish
            BrokerTopicCount brokerTopicCount = (BrokerTopicCount) sizes.get(sizes.size() - 1);
            brokerBuffer.append(String.format("%s) ", brokerTopicCount.getBrokerId()));

            return brokerBuffer.toString();
        }
    }
}
