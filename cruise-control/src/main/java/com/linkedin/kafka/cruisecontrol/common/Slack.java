package com.linkedin.kafka.cruisecontrol.common;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class Slack {
    private static final Logger LOG = LoggerFactory.getLogger(Slack.class);

    private static final String REQ_HEADER_KEY_CONTENT_TYPE = "Content-Type";
    private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";

    private static final String REQ_BODY_KEY_TEXT = "text";
    private static final String REQ_BODY_KEY_EMOJI = "icon_emoji";
    private static final String REQ_BODY_KEY_USERNAME = "username";

    private static final String EMOJI = "sunglasses";
    private static final String USERNAME = "kpt-cruise-control";

    private static final String CONFIG_KEY_WEBHOOK_URL = "slack.webhook.url";
    private static final String CONFIG_KEY_CONNECT_TIMEOUT_MS = "slack.connect.timeout.ms";
    private static final String CONFIG_KEY_READ_TIMEOUT_MS = "slack.read.timeout.ms";

    private final String _webhookUrl;
    private final int _connectTimeoutMs;
    private final int _readTimeoutMs;

    public Slack(KafkaCruiseControlConfig config) {
        _webhookUrl = config.getString(CONFIG_KEY_WEBHOOK_URL);
        _connectTimeoutMs = config.getInt(CONFIG_KEY_CONNECT_TIMEOUT_MS);
        _readTimeoutMs = config.getInt(CONFIG_KEY_READ_TIMEOUT_MS);
    }

    public void post(String message) {
        if (_webhookUrl == null || _webhookUrl.isEmpty()) {
            LOG.warn("No Slack webhook configured; will skip posting to Slack: " + message);
            return;
        }

        Map<String, String> requestParameters = Map.of(
                REQ_BODY_KEY_TEXT, message,
                REQ_BODY_KEY_EMOJI, EMOJI,
                REQ_BODY_KEY_USERNAME, USERNAME);

        HttpsURLConnection connection = null;
        BufferedReader in = null;

        try {
            URL url = new URL(_webhookUrl);
            connection = (HttpsURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty(REQ_HEADER_KEY_CONTENT_TYPE, JSON_CONTENT_TYPE);
            connection.setConnectTimeout(_connectTimeoutMs);
            connection.setReadTimeout(_readTimeoutMs);

            connection.setDoOutput(true);
            DataOutputStream out = new DataOutputStream(connection.getOutputStream());
            out.writeBytes(buildRequestParameterString(requestParameters));
            out.flush();
            out.close();

            int status = connection.getResponseCode();

            if (status != 200) {
                Reader streamReader = new InputStreamReader(connection.getErrorStream());
                in = new BufferedReader(streamReader);

                String inputLine;
                StringBuilder errorContent = new StringBuilder();
                while ((inputLine = in.readLine()) != null) {
                    errorContent.append(inputLine);
                }

                LOG.error("Error posting to slack: {} ({})", errorContent, message);
            }
        } catch (IOException e) {
            LOG.error("Error posting to slack: {} ({})", e.getMessage(), message, e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }

            if (in != null) {
                try {
                    in.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

    private String buildRequestParameterString(Map<String, String> requestParameters) {
        StringBuilder result = new StringBuilder();

        for (Map.Entry<String, String> entry : requestParameters.entrySet()) {
            result.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8));
            result.append("=");
            result.append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
            result.append("&");
        }

        String resultString = result.toString();
        return resultString.length() > 0
                ? resultString.substring(0, resultString.length() - 1)
                : resultString;
    }
}
