/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model.NewRelicQueryResult;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;

class NewRelicAdapter {
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String GRAPHQL_API_PATH = "/graphql";

    private final CloseableHttpClient _httpClient;

    private final String _newRelicEndpoint;
    private final long _newRelicAccountId;
    private final String _newRelicApiKey;

    NewRelicAdapter(CloseableHttpClient httpClient, String newRelicEndpoint, int newRelicAccountId, String newRelicApiKey) {
        this._httpClient = validateNotNull(httpClient, "httpClient cannot be null");

        this._newRelicEndpoint = validateNotNull(newRelicEndpoint, "newRelicEndpoint cannot be null");
        this._newRelicAccountId = newRelicAccountId;
        this._newRelicApiKey = validateNotNull(newRelicApiKey, "newRelicApiKey cannot be null");
    }

    List<NewRelicQueryResult> runQuery(String query) throws IOException {
        URI uri = URI.create(_newRelicEndpoint + GRAPHQL_API_PATH);

        HttpPost httpPost = new HttpPost(uri);
        httpPost.addHeader("Api-Key", _newRelicApiKey);
        httpPost.addHeader("Content-Type", "application/json");

        httpPost.setEntity(buildQueryPayload(query));

        try (CloseableHttpResponse response = _httpClient.execute(httpPost)) {
            int responseCode = response.getStatusLine().getStatusCode();

            HttpEntity entity = response.getEntity();
            InputStream content = entity.getContent();
            String responseBody = IOUtils.toString(content, StandardCharsets.UTF_8);

            if (responseCode != HttpServletResponse.SC_OK) {
                throw new IOException(String.format("Received non-success response code on New Relic GraphQL API HTTP call "
                        + "(response code = %s, response body = %s)", responseCode, responseBody));
            } else if (responseBody == null || responseBody.equals("")) {
                throw new IOException(String.format("Received null responseBody or responseBody was empty."
                                + " (response code = %s, response body = %s)", responseCode, responseBody));
            }

            try {
                JsonNode responseJson = JSON.readTree(responseBody);
                JsonNode resultsJson = responseJson.get("data").get("actor").get("account").get("nrql").get("results");
                List<NewRelicQueryResult> results = new ArrayList<>();

                // Can be null upon invalid query or just something else going wrong on server side
                if (resultsJson == null) {
                    throw new IOException(String.format("ResultsJson was empty. "
                            + "(response body = %s)", responseBody));
                }

                for (JsonNode resultJson : resultsJson) {
                    results.add(new NewRelicQueryResult(resultJson));
                }

                return results;
            } catch (NullPointerException e) {
                throw new IOException(String.format("Response body was of invalid format and could not be"
                        + "properly parsed. (response body = %s)", responseBody));
            }
        }
    }

    private HttpEntity buildQueryPayload(String query) throws JsonProcessingException {
        String payload = String.format(
                "{ actor { account(id: %s) { nrql(query: \"%s\") { results } } } }",
                _newRelicAccountId,
                query);

        ObjectNode payloadWrapper = JSON.createObjectNode();
        payloadWrapper.put("query", payload);
        payloadWrapper.put("variables", "");

        return new ByteArrayEntity(JSON.writeValueAsBytes(payloadWrapper));
    }
}
