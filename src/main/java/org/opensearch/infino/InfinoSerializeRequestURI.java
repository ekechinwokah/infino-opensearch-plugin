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

/**
 * Serialize OpenSearch Infino REST request to an Infino URL.
 * 1. Search window defaults to the past 30 days if not specified by the request.
 * 2. To access Infino indexes, the REST caller must prefix the index name with '/infino/'.
 * 3. If the specified index does not exist in OpenSearch, create it before sending to Infino.
 *
 * Note that REST paths will normally be in form:
 *
 * /infino/logs/<index-name>/_action?parameters OR
 * /infino/metrics/<index-name>/_action?parameters OR
 * /infino/<index-name>/_action?parameters
 */
public class InfinoSerializeRequestURI {

    protected String prefix; // Path prefix. E.g. /infino/logs/
    protected String path; // Path postfix. E.g. /_search
    protected Map<String, String> params; // Request parameters. E.g. ?start_time="123"
    protected InfinoIndexType indexType; // Type of index. E.g. LOGS or METRICS
    protected String indexName; // Name of the Infino index
    protected RestRequest.Method method; // The REST method for the request
    protected String startTime; // Start time for search queries
    protected String endTime; // End time for search queries
    protected String finalUrl; // final URL to be sent to Infino

    protected static String infinoEndpoint = System.getenv("INFINO_SERVER_URL"); // The Infino endpoint
    protected static int DEFAULT_SEARCH_TIME_RANGE = 7; // Default time range for Infino searches is 7 days

    private static final Logger logger = LogManager.getLogger(InfinoSerializeRequestURI.class);
    private static final String defaultInfinoEndpoint = "http://localhost:3000";

    /**
     * Constructor
     *
     * Takes a request and serializes to the protected member, finalUrl.
     *
     * @param request - the request to be serialized.
     */
    public InfinoSerializeRequestURI(RestRequest request) throws IllegalArgumentException {

        // Consume the parameters
        params = getInfinoParams(request);

        // Get the requested method
        method = request.method();

        // Get the Infino endpoint
        infinoEndpoint = getEnvVariable("INFINO_SERVER_URL");
        if (infinoEndpoint == null || infinoEndpoint.isEmpty()) {
            infinoEndpoint = defaultInfinoEndpoint;
            logger.info("Setting Infino Server URL to its default value.");
        }

        setDefaultTimeRange();

        // Split the request path
        String requestPath = params.get("infinoPath");
        logger.info("Request path is " + requestPath);
        if (requestPath != null) {
            int pathElement = requestPath.lastIndexOf("/");
            prefix = getPrefix(pathElement, requestPath);
            path = requestPath.substring(pathElement + 1);
        }

        logger.info("Path is now " + path);

        // Get the index name
        indexName = params.get("infinoIndex");

        // This should be caught be the security manager, but catch here in case
        if (indexName == null) throw new IllegalArgumentException("Index name must be specified");

        // Determine the index type
        if (requestPath != null) {
            int typeIndex = requestPath.lastIndexOf("/");
            String index = getPrefix(typeIndex, requestPath);
            logger.info("index is " + index);
            if ("logs".equals(index)) {
                indexType = InfinoIndexType.LOGS;
            } else if ("metrics".equals(index)) {
                indexType = InfinoIndexType.METRICS;
            } else {
                indexType = InfinoIndexType.UNDEFINED;
            }
        }

        constructInfinoRequestURI();

        logger.info("Serialized REST request for Infino to: " + finalUrl);
    }

    // Helper function to construct Infino URL
    private void constructInfinoRequestURI() {

        String errorString =  "Error constructing Infino URL for: " + infinoEndpoint + "/" + indexName + "/" + this.path;
        logger.info("Constructing Infino URL for: " + infinoEndpoint + "/" + indexName);

        switch (method) {
            case GET -> {
                // We shouldn't have a null path at this point
                if (this.path == null) throw new IllegalArgumentException(errorString);

                if (this.path.endsWith("_ping")) {
                    finalUrl =  infinoEndpoint + "/ping";
                }

                // Default to LOGS for search
                if (this.path.endsWith("_search")) {
                    finalUrl = switch(this.indexType) {
                        case METRICS -> infinoEndpoint + "/" + indexName + "/search_metrics?" + buildQueryString(
                            "name", params.get("name"),
                            "value", params.get("value"),
                            "start_time", startTime,
                            "end_time", endTime);
                        default ->  infinoEndpoint + "/" + indexName + "/search_logs?" + buildQueryString(
                            "text", params.get("text"),
                            "start_time", startTime,
                            "end_time", endTime);
                    };
                }

                if (this.path.endsWith("_summarize")) {
                    finalUrl =  infinoEndpoint + "/" + indexName + "/summarize?" + buildQueryString(
                            "text", params.get("text"),
                            "start_time", startTime,
                            "end_time", endTime);
                    };
                }
            case POST -> {
                finalUrl =  switch(indexType) {
                    case LOGS ->  infinoEndpoint + "/" + indexName + "/append_log";
                    case METRICS -> infinoEndpoint + "/" + indexName + "/append_metric";
                    default -> throw new IllegalArgumentException(errorString);
                };
            }
            case PUT, DELETE -> {
                finalUrl =  infinoEndpoint + "/:" + indexName;
            }
        }
        // If request contains an unsupported path for Infino, finalUrl will be null
        if (finalUrl == null) throw new IllegalArgumentException(errorString);
    }

    // Set the default time range for searches
    private void setDefaultTimeRange() {
        if (method == RestRequest.Method.GET) {

            // Get current time
            Instant now = Instant.now();

            // get time range arguments
            startTime = params.get("startTime");
            endTime = params.get("endTime");

            // Default start_time to 30 days before now if not provided
            if (startTime == null || startTime.isEmpty()) {
                startTime = now.minus(DEFAULT_SEARCH_TIME_RANGE, ChronoUnit.DAYS).toString();
            }

            // Default end_time to now if not provided
            if (endTime == null || endTime.isEmpty()) {
                endTime = now.toString();
            }
        }
    }

    // Helper method to get the prefix from the path
    private String getPrefix(int element, String requestPath) {
        // Check if the lastIndex is -1 (indicating "/" was not found)
        if (element == -1) {
            return null;
        } else {
            return requestPath.substring(0, element);
        }
    }

    // Helper method for unit tests etc.
    protected String getEnvVariable(String name) {
        return System.getenv(name);
    }

    // Helper method for unit tests etc.
    protected String getFinalUrl() {
        return this.finalUrl;
    }

    // Helper method for unit tests etc.
    protected RestRequest.Method getMethod() {
        return this.method;
    }

    // Helper method for unit tests etc.
    protected String getIndexName() {
        return this.indexName;
    }

    // Helper method for unit tests etc.
    protected InfinoIndexType getIndexType() {
        return this.indexType;
    }
    /**
     *  Consumes all the request parameters after the "?" in the URI
     *  otherwise the Rest handler will fail. We also need to explictly
     *  read wildcard parameters for the paths defined by routes() in
     *  the Infino Rest handler as they are not consumed by params()
     *  in the RestRequest class.
     *
     *  @param request the request to execute
     *  @return a string hashmap of the request parameters
     */
     protected Map<String, String> getInfinoParams(RestRequest request) {

        // Initialize a new HashMap to store the parameters
        Map<String, String> requestParamsMap = new HashMap<>();

        // Iterate over the request parameters and add them to the map
        request.params().forEach((key, value) -> {
            requestParamsMap.put(key, value);
            logger.info("Query parameters from OpenSearch to Infino: " + key + " is " + value);
        });

        requestParamsMap.put("infinoIndex", request.param("infinoIndex"));
        requestParamsMap.put("infinoPath", request.param("infinoPath"));

        return requestParamsMap;
    }

    /**
     * The type of index in Infino. Infino has a different index for each telemetry data
     * type: logs, metrics, and traces (traces are not yet supported as of Dec 2023).
     */
    public static enum InfinoIndexType {
        UNDEFINED,
        LOGS,
        METRICS
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
}
