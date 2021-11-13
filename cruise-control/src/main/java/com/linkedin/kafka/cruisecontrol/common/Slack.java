package com.linkedin.kafka.cruisecontrol.common;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.NotifierUtils;
import com.linkedin.kafka.cruisecontrol.detector.notifier.SlackMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class Slack {
    private static final Logger LOG = LoggerFactory.getLogger(Slack.class);

    private static final String EMOJI = "sunglasses";
    private static final String USERNAME = "kpt-cruise-control";
    private static final String SLACK_CHANNEL = "#kafka-automation";

    private static final String CONFIG_KEY_WEBHOOK_URL = "slack.webhook.url";
    private static final String CONFIG_KEY_NR_CELL_NAME = "newrelic.cell.name";

    private final String _webhookUrl;
    private final String _nrCellName;

    public Slack(KafkaCruiseControlConfig config) {
        Map<String, Object> mergedConfigValues = config.mergedConfigValues();

        _webhookUrl = (String) mergedConfigValues.get(CONFIG_KEY_WEBHOOK_URL);
        _nrCellName = (String) mergedConfigValues.get(CONFIG_KEY_NR_CELL_NAME);
    }

    public void post(String sourceIdentifier, String message) {
        if (_webhookUrl == null || _webhookUrl.isEmpty()) {
            LOG.warn("No Slack webhook configured; will skip posting to Slack: " + message);
            return;
        }

        message += String.format("[%s] [%s] ", _nrCellName, sourceIdentifier);

        try {
            SlackMessage slackMessage = new SlackMessage(USERNAME, message, EMOJI, SLACK_CHANNEL);
            NotifierUtils.sendMessage(slackMessage.toString(), _webhookUrl, null);
        } catch (IOException e) {
            LOG.error("Error posting to slack: {} ({})", e.getMessage(), message, e);
        }
    }
}
