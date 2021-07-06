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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;

/**
 * We use this class to query the NRQL endpoint, receive the outputs from the endpoint,
 * and parse those outputs into usable values (NewRelicQueryResults).
 */
class NewRelicAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(NewRelicAdapter.class);

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

    /**
     * Runs our query on the NRQL endpoint. Parses the output JSON into
     * different NewRelicQueryResult values which store the metric types and
     * metric values that we are trying to capture. Note that we retry the query
     * once if it were to fail. If the query fails more than once, we will throw
     * an IOException.
     * @param query - The NRQL query that we want to run to obtain our data.
     * @return - List of results from the endpoint. Each QueryResult may have
     * multiple metric types, but will only have one value associated with each metric type
     * that it contains.
     * @throws IOException
     */
    List<NewRelicQueryResult> runQuery(String query) throws IOException, InterruptedException {
        // Creating the query to NRQL
        URI uri = URI.create(_newRelicEndpoint + GRAPHQL_API_PATH);

        HttpPost httpPost = new HttpPost(uri);
        httpPost.addHeader("Api-Key", _newRelicApiKey);
        httpPost.addHeader("Content-Type", "application/json");

        httpPost.setEntity(buildQueryPayload(query));

        try {
            String responseBody = executeQuery(httpPost);
            return processResults(responseBody);
        } catch (IOException e) {
            LOGGER.error("The query {} failed with error {}. Query is being retried.", query, e);

            // Wait a second and then retry
            TimeUnit.SECONDS.sleep(1);

            // We retry the query
            String responseBody = executeQuery(httpPost);
            return processResults(responseBody);
        }
    }

    private String executeQuery(HttpPost httpPost) throws IOException {
        try (CloseableHttpResponse response = _httpClient.execute(httpPost)) {
            int responseCode = response.getStatusLine().getStatusCode();

            HttpEntity entity = response.getEntity();
            InputStream content = entity.getContent();
            String responseBody = IOUtils.toString(content, StandardCharsets.UTF_8);

            // Make sure we get a good response
            if (responseCode != HttpServletResponse.SC_OK) {
                throw new IOException(String.format("Received non-success response code on New Relic GraphQL API HTTP call "
                        + "(response code = %s, response body = %s)", responseCode, responseBody));
            } else if (responseBody == null || responseBody.equals("")) {
                throw new IOException(String.format("Received null responseBody or responseBody was empty."
                        + " (response code = %s, response body = %s)", responseCode, responseBody));
            }

            return responseBody;
        }
    }

    private List<NewRelicQueryResult> processResults(String responseBody) throws IOException {
        // We will have a null pointer exception when the query is not formatted in the way that we want it
        // to be - This will happen most of the time because we fed an invalid query
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
