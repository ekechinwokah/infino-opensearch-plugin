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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

import static org.opensearch.rest.RestRequest.Method.*;


/**
 * Handle REST calls for the /infino index.
 * This effectively serves as the public API for Infino.
 *
 * 1. Search window defaults to the past 30 days if not specified by the request.
 * 2. To access Infino indexes, the REST caller must prefix the index name with '/infino/'.
 * 3. If the specified index does not exist in OpenSearch, create it before sending to Infino.
 *
 * Note that URIs will normally be in form:
 *
 * http://endpoint/infino/logs/<index-name>/_action?parameters OR
 * http://endpoint/infino/metrics/<index-name>/_action?parameters OR
 * http://endpoint/infino/<index-name>/_action?parameters
 */
public class InfinoRestHandler extends BaseRestHandler {

    private static CreateIndexRequest createIndexRequest;

    private static final int THREADPOOLSIZE = 10;
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final Logger logger = LogManager.getLogger(InfinoRestHandler.class);

    protected InfinoSerializeRequestURI getInfinoSerializeRequestURI(RestRequest request) {
        return new InfinoSerializeRequestURI(request);
    }

    // List of futures we need to clear on close
    private static List<CompletableFuture<?>> futures = new ArrayList<>();

    protected HttpClient getHttpClient() {
        return httpClient;
    }

    /**
     * Name of this REST handler
     */
    @Override
    public String getName() {
        return "rest_handler_infino";
    }

    // We use our own thread pool to avoid impacting OpenSearch's pool
    private static final ExecutorService infinoThreadPool = Executors.newFixedThreadPool(
        THREADPOOLSIZE, new CustomThreadFactory("InfinoPluginThread"));

    // Get thread pool
    protected ExecutorService getInfinoThreadPool () {
        return infinoThreadPool;
    }

    /**
     * Shutdown the thread pool and futures when the plugin is stopped
     */
    public static void close() {
        // Wait for all futures to complete
        CompletableFuture<Void> allDone = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        try {
            allDone.get(30, TimeUnit.SECONDS); // Adjust the timeout as needed
        } catch (Exception e) {
            logger.error("Error shutting down futures w/ HTTP client", e);
        }
        // Clear the list of futures
        futures.clear();
        infinoThreadPool.shutdown();
    }

    // Use a privileged custom thread factory since Security Manager blocks access
    // to thread groups.
    //
    // https://github.com/opensearch-project/OpenSearch/issues/5359
    protected static final class CustomThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        CustomThreadFactory(String baseName) {
            namePrefix = baseName + "-";
        }

        // TODO: Security Manager is being deprecated after Java 17 - not sure what OpenSearch
        // will decide to do but VMs, containers, etc. can lock down plugin code these days
        // better than a JVM so perhaps they remove Security Manager completely and we can change
        // this code to a simple threadpool or use OpenSearch's threadpool passed in to the REST
        // handler from the client (i.e. client.Threadpool()).
        public Thread newThread(Runnable r) {
            return AccessController.doPrivileged((PrivilegedAction<Thread>) () -> {
                Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
                if (t.isDaemon()) t.setDaemon(false);
                if (t.getPriority() != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY);
                return t;
            });
        }
    }

    // Create an empty Lucene index with the same name as the Infino index if it doesn't exist
    protected void createLuceneIndexIfNeeded(NodeClient client, String indexName) {
        IndicesExistsRequest getIndexRequest = new IndicesExistsRequest(new String[]{indexName});
        client.admin().indices().exists(getIndexRequest, new ActionListener<>() {
            @Override
            public void onResponse(IndicesExistsResponse response) {
                if (!response.isExists()) {
                    // Create the index if it doesn't exist
                    createIndexRequest = new CreateIndexRequest(indexName);
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

    /**
     * Handle REST routes for the /infino index.
     *
     * By explicitly listing all the possible paths, we let OpenSearch handle
     * illegal path expections rather than wait to send to Infino and translate
     * the error response for the user.
     *
     * Note that we need to explictly read wildcard parameters for the paths
     * defined here. I.e. somewhere before the handler completes we need to do
     * something like the following:
     *
     * String someVar = request.param("infinoIndex");
     *
     * etc.
     */
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

    /**
     * Prepare the request for execution. Implementations should consume all request params before
     * returning the runnable for actual execution. Unconsumed params will immediately terminate
     * execution of the request.
     *
     * The first half of the method (before the thread executor) is parallellized by OpenSearch's
     * REST thread pool so we can serialize in parallel. However network calls use our own
     * privileged thread factory.
     *
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     * @return the action to execute
     * @throws IOException if an I/O exception occurred parsing the request and preparing for execution
     */
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        InfinoSerializeRequestURI infinoSerializeRequestURI = null;
        HttpClient httpClient = getHttpClient();

        logger.info("Serializing REST request for Infino");

        // Serialize the request to a valid Infino URL
        try {
            infinoSerializeRequestURI = getInfinoSerializeRequestURI(request);
        } catch (Exception e) {
            logger.error("Error serializing REST URI for Infino: ", e);
            return null;
        }

        logger.info("Serialized REST request for Infino to " + infinoSerializeRequestURI.getFinalUrl());

        // Create Lucene mirror for the Infino collection if it doesn't exist
        if (infinoSerializeRequestURI.getMethod() == PUT) createLuceneIndexIfNeeded(client, infinoSerializeRequestURI.getIndexName());

        // Create the HTTP request to forward to Infino Server
        HttpRequest forwardRequest = HttpRequest.newBuilder()
            .uri(URI.create(infinoSerializeRequestURI.getFinalUrl()))
            .method(infinoSerializeRequestURI.getMethod().toString(), HttpRequest.BodyPublishers.ofString(request.content().utf8ToString()))
            .build();

        logger.info("Sending HTTP Request to Infino: " + infinoSerializeRequestURI.getFinalUrl());

        // Send request to Infino server and create a listener to handle the response.
        // Execute the HTTP request using our own thread factory
        return channel -> {
            infinoThreadPool.execute(() -> {
                CompletableFuture<Void> future = httpClient.sendAsync(forwardRequest, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(response -> {
                        if (Thread.currentThread().isInterrupted()) {
                            logger.debug("Infino Plugin Rest handler thread interrupted. Exiting.");
                            // Handle the interrupted status of the thread if needed
                            return;
                        }
                        try {
                            logger.info("Receieved HTTP response from Infino: " + response.body().toString());
                            channel.sendResponse(new BytesRestResponse(RestStatus.OK, response.body()));
                            logger.info("Sent HTTP response to OpenSearch Rest Channel: " + channel);
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

                // Add the future to the list of futures to clear, protected by a thread lock
                synchronized (futures) {
                    futures.add(future);
                }
            });
        };
    }
};
