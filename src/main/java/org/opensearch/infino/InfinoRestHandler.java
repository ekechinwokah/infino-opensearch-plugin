/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
*/

package org.opensearch.infino;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.threadpool.ThreadPool;

import static org.opensearch.rest.RestRequest.Method.*;

public class InfinoRestHandler extends BaseRestHandler {

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private static final Logger logger = LogManager.getLogger(InfinoRestHandler.class);

    // Get HTTP Client
    public HttpClient getHttpClient() {
        return httpClient;
    }

    // Name of this REST handler
    @Override
    public String getName() {
        return "rest_handler_infino";
    }

    // Handle REST calls for the /infino index.
    //
    // By explicitly listing all the possible paths, we let OpenSearch handle
    // illegal path expections rather than wait to send to Infino and translate
    // the error response for the user.
    //
    // Note that we need to explictly read wildcard parameters for the paths
    // defined here. I.e. somewhere before the handler completes we need to
    //
    // String someVar = request.param("infinoIndex");
    // etc.
    //
    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/infino/{infinoIndex}/{infinoPath}"),
            new Route(GET, "/infino/{infinoIndex}/logs/{infinoPath}"),
            new Route(GET, "/infino/{infinoIndex}/metrics/{infinoPath}"),
            new Route(GET, "/_cat/infino/{infinoIndex}"),
            new Route(HEAD, "/infino/{infinoIndex}/{infinoPath}"),
            new Route(POST, "/infino/{infinoIndex}/{infinoPath}"),
            new Route(PUT, "/infino/{infinoIndex}"),
            new Route(DELETE, "/infino/{infinoIndex}"),
            new Route(HEAD, "/infino/{infinoIndex}")
        ));
    }

    // Create an empty Lucene index with the same name as the Infino index
    private void createLuceneIndexIfNeeded(NodeClient client, String indexName) {
        IndicesExistsRequest getIndexRequest = new IndicesExistsRequest(new String[]{indexName});
        client.admin().indices().exists(getIndexRequest, new ActionListener<>() {
            @Override
            public void onResponse(IndicesExistsResponse response) {
                if (!response.isExists()) {
                    // Create the index if it doesn't exist
                    CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                    createIndexRequest.settings(Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                    );
                    client.admin().indices().create(createIndexRequest, new ActionListener<>() {
                        @Override
                        public void onResponse(CreateIndexResponse response) {
                            logger.info("Successfully created '" + indexName + "' Lucene index on local node");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error("Failed to create '" + indexName + "' Lucene index on local node", e);
                        }
                    });
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Error checking existence of '" + indexName + "' index", e);
            }
        });
    }

    // Forward REST requests to the Infino Server
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {

        InfinoUrl infinoUrl = null;
        HttpClient httpClient = getHttpClient();

        // Serialize the request to an Infino URL
        try {
            infinoUrl = new InfinoUrl(request);
        } catch (Exception e) {
            logger.error("Error serializing REST request for Infino: ", e);
            return null;
        }

        String targetUrl = infinoUrl.getInfinoUrl();

        logger.info("We got URL " + infinoUrl.infinoEndpoint + " " + infinoUrl.path + " and target " + targetUrl);

        // Create Lucene mirror as needed
        if (infinoUrl.method == PUT) createLuceneIndexIfNeeded(client, infinoUrl.indexName);

        // Use OpenSearch thread pool to handle requests
        ThreadPool threadPool = client.threadPool();

        // Create the HTTP request to forward to Infino Server
        HttpRequest forwardRequest = HttpRequest.newBuilder()
            .uri(URI.create(targetUrl))
            .method(infinoUrl.method.toString(), HttpRequest.BodyPublishers.ofString(request.content().utf8ToString()))
            .build();

        logger.info("We constructed the HTTP Request for " + infinoUrl.getInfinoUrl());

        // Send request to Infino server and create a listener to handle the response
        return channel -> {
            // Execute the HTTP request using OpenSearch's thread pool
            threadPool.generic().execute(() -> {
                httpClient.sendAsync(forwardRequest, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(response -> {
                        try {
                            channel.sendResponse(new BytesRestResponse(RestStatus.OK, response.body()));
                        } catch (Exception e) {
                            logger.error("Error sending response", e);
                            channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                        }
                    })
                    .exceptionally(e -> {
                        logger.error("Error in async HTTP call", e);
                        channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                        return null;
                    });
            });
        };
    }
};
