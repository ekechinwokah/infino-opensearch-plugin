/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.infino;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.rest.RestRequest;

// Transform OpenSearch Infino REST request to an Infino URL.
// 1. Search window defaults to the past 30 days if not specified by the request.
// 2. To access Infino indexes, the REST caller must prefix the index name with '/infino/'.
// 3. If the specified index does not exist in OpenSearch, create it before sending to Infino.
//
// Note that REST paths will normally be in form:
//
//     /infino/logs/<index-name>/_action?parameters OR
//     /infino/metrics/<index-name>/_action?parameters OR
//     /infino/<index-name>/_action?parameters
//
public class InfinoUrl {

    public String prefix; // Path prefix. E.g. /infino/logs/
    public String path; // Path postfix. E.g. /_search
    public InfinoIndexType indexType; // Type of index. E.g. LOGS or METRICS
    public String indexName; // Name of the Infino index
    public Map<String, String> params; // Request parameters. E.g. ?start_time="123"
    public String infinoEndpoint = System.getenv("INFINO_SERVER_URL"); // The Infino endpoint
    public RestRequest.Method method; // The REST method for the request

    private static final Logger logger = LogManager.getLogger(InfinoUrl.class);
    public static final String defaultInfinoEndpoint = "http://localhost:3000";

    // Helper method to get the prefix from the path
    private String getPrefix(int element, String requestPath) {
        // Check if the lastIndex is -1 (indicating "/" was not found)
        if (element == -1) {
            return null;
        } else {
            return requestPath.substring(0, element);
        }
    }

    // Method to get environment variable - makes unit tests easier
    protected String getEnvVariable(String name) {
        return System.getenv(name);
    }

    // Constructor method
    public InfinoUrl(RestRequest request) {

        this.infinoEndpoint = getEnvVariable("INFINO_SERVER_URL");
        if (infinoEndpoint == null || infinoEndpoint.isEmpty()) {
            this.infinoEndpoint = defaultInfinoEndpoint;
        }

        this.method = request.method();

        // Split the request path
        String requestPath = request.param("infinoPath");
        int pathElement = requestPath.lastIndexOf("/");
        this.prefix = getPrefix(pathElement, requestPath);
        this.path = requestPath.substring(pathElement + 1);

        // Get the index name
        this.indexName = request.param("infinoIndex");

        // Determine the index type
        int typeIndex = requestPath.lastIndexOf("/");
        String index = getPrefix(typeIndex, requestPath);
        if ("logs".equals(index)) {
            this.indexType = InfinoIndexType.LOGS;
        } else if ("metrics".equals(index)) {
            this.indexType = InfinoIndexType.METRICS;
        } else {
            this.indexType = InfinoIndexType.UNDEFINED;
        }

        // Set the Infino service endpoint
        if (infinoEndpoint == null || infinoEndpoint.isEmpty()) {
            this.infinoEndpoint = defaultInfinoEndpoint;
            logger.debug("Setting Infino Server URL to its default value: 'localhost:3000'");
        }

        // Read all the request parameters otherwise OpenSearch complains about invalid arguments.
        // Note that we also need to explictly read wildcard parameters for the paths
        // defined in the routes() override in the REST handler
        this.params = getInfinoParams(request);
    }

    // The type of index in Infino. Infino has a different index for each telemetry data
    // type: logs, metrics, and traces (traces are not yet supported Dec 2023).
    public static enum InfinoIndexType {
        UNDEFINED,
        LOGS,
        METRICS
    }

    // With no explicit time boundaries in OpenSearch searches, we should expect queries with
    // no time arguments so the default time range is set to 30 days.
    private class InfinoTimeBoundaries {
        public String startBoundary;
        public String endBoundary;

        // Constructor method
        public InfinoTimeBoundaries(String startTime, String endTime) {

            // Get current time
            Instant now = Instant.now();

            // Default start_time to 30 days before now if not provided
            if (startTime == null || startTime.isEmpty()) {
                startTime = now.minus(30, ChronoUnit.DAYS).toString();
            }

            // Default end_time to now if not provided
            if (endTime == null || endTime.isEmpty()) {
                endTime = now.toString();
            }

            startBoundary = startTime;
            endBoundary = endTime;
        }
    }

    // Helper method to build query strings
    private String buildQueryString(String... params) {
        StringBuilder queryBuilder = new StringBuilder();
        for (int i = 0; i < params.length; i += 2) {
            if (i > 0) {
                queryBuilder.append("&");
            }
            queryBuilder.append(params[i]).append("=").append(encodeParam(params[i + 1]));
        }
        return queryBuilder.toString();
    }

    // Helper method to encode URL parameters
    private String encodeParam(String param) {
        try {
            return URLEncoder.encode(param, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            logger.error("Error encoding parameter: " + param, e);
            return param;
        }
    }

    // Get the parameters from the REST request (JVM complains otherwise)
    private Map<String, String> getInfinoParams(RestRequest request) {

            // Initialize a new HashMap to store the parameters
            Map<String, String> requestParamsMap = new HashMap<>();

            // Iterate over the request parameters and add them to the map
            request.params().forEach((key, value) -> {
                requestParamsMap.put(key, value);
                logger.info("Query parameters from OpenSearch to Infino: " + key + " is " + value);
            });

            return requestParamsMap;
    }

    // Returns a validated URL for sending requests to Infino
    public String getInfinoUrl() {

        String baseUrl = infinoEndpoint + "/" + indexName;

        logger.info("Constructing Infino URL for: " + baseUrl + "/" + this.path);

        switch (method) {
            case GET -> {
                if (this.path.endsWith("_ping")) {
                    return infinoEndpoint + "/ping";
                }

                InfinoTimeBoundaries boundaries = new InfinoTimeBoundaries(
                    params.get("start_time"),
                    params.get("end_time"));

                if (this.path.endsWith("_search")) {
                    return switch(this.indexType) {
                        case LOGS ->  baseUrl + "/search_logs" + buildQueryString(
                            "text", params.get("text"),
                            "start_time", boundaries.startBoundary,
                            "end_time", boundaries.endBoundary);
                        case METRICS -> baseUrl + "/search_metrics" + buildQueryString(
                            "name", params.get("name"),
                            "value", params.get("value"),
                            "start_time", boundaries.startBoundary,
                            "end_time", boundaries.endBoundary);
                        // TODO: Allow searches to UNDEFINED, which will span multiple index types in Infino
                        default -> throw new IllegalArgumentException("Error constructing Infino URL for: " + baseUrl + "/" + this.path);
                    };
                }
                if (this.path.endsWith("_summarize")) {
                    return switch(this.indexType) {
                        case LOGS ->  baseUrl + "/summarize" + buildQueryString(
                            "text", params.get("text"),
                            "start_time", boundaries.startBoundary,
                            "end_time", boundaries.endBoundary);
                        default -> throw new IllegalArgumentException("Error constructing Infino URL for: " + baseUrl + "/" + this.path);
                    };
                }
            }
            case POST -> {
                return switch(indexType) {
                    case LOGS ->  baseUrl + "/append_log";
                    case METRICS -> baseUrl + "/append_metric";
                    default -> throw new IllegalArgumentException("Error constructing Infino URL for: " + baseUrl + "/" + this.path);
                };
            }
            case PUT, DELETE -> {
                return baseUrl + "/:" + indexName; 
            }
        }
        throw new IllegalArgumentException("Error constructing Infino URL for: " + baseUrl + "/" + this.path);
    }

}
